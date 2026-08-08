import io
import os
import re
import json
import time
import threading
import traceback
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import boto3
import pandas as pd
import requests
import websocket


# ==================================================
# Config
# ==================================================

S3_BUCKET = "bin-tickers-yev"

RECENT_ACTIVITY_PREFIX = "general-filters"

# Multi-interval structure:
# candles/interval=3m/candle_date=YYYY-MM-DD/candle_time=HHMM/candles.csv
CANDLES_PREFIX = "candles"
SWING_LOW_OUTPUT_PREFIX = "swing-low-search"
TRIGGER_RESULTS_PREFIX = "trigger-1-results"
TRIGGER_2_RESULTS_PREFIX = "trigger-2-results"
TG_ALERTS_SENT_PREFIX = "tg-alerts-sent"

# Binance-native intervals we want to analyze.
INTERVALS = ["3m", "5m"]
INTERVAL_MINUTES = {
    "3m": 3,
    "5m": 5,
}
# INTERVALS = ["3m", "5m", "15m", "30m"]
# INTERVAL_MINUTES = {
#     "3m": 3,
#     "5m": 5,
#     "15m": 15,
#     "30m": 30,
# }

# Active symbol list is now read from the combined general-filters output.
# The combined filter script still produces active-cycle metadata for the live engine.
# The list is loaded immediately on startup, then refreshed daily after the
# scheduled crypto-general-filters Airflow run.
SYMBOL_REFRESH_HOUR_UTC = 1
SYMBOL_REFRESH_MINUTE_UTC = 9
SYMBOL_REFRESH_CHECK_SLEEP_SECONDS = 60

# Wait after the first closed candle for the same interval/time before writing the batch.
# This gives other symbols for that same interval close a few seconds to arrive.
BATCH_WAIT_SECONDS = 5

BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream?streams="
SOURCE = "websocket"

SAVE_CANDLES_TO_S3 = True
SAVE_SWING_LOWS_TO_S3 = True
SAVE_TRIGGERS_TO_S3 = True
SAVE_SENT_ALERTS_TO_S3 = True
SEND_TELEGRAM_ALERTS = True

# 7-candle swing-low structure:
# C1.close > C2.close > C3.close
# C4.close is the lowest close in the 7-candle window
# C5.close < C6.close < C7.close
# No lower-shadow/body condition is required.
# To avoid tiny local pivots, swing depth must be at least 2x
# the average candle body of previous max 40 candles before C1.
SWING_LOW_PATTERN_CANDLES = 7
SWING_LOW_BODY_LOOKBACK_CANDLES = 40
SWING_LOW_CLOSE_LOOKBACK_CANDLES = 40
SWING_LOW_DEPTH_BODY_RATIO_MIN = 2.0
TRIGGER_1_TYPE = "SHORT_SWING_LOW_BREAK"
TRIGGER_2_TYPE = "SHORT_HIGH_CLOSE_REJECTION"
TRIGGER_TYPE = TRIGGER_1_TYPE

# Trigger 2 structure:
# Last 3 closed candles: C1.close < C2.close < C3.close.
# C3 is the latest closed candle.
# C3.close must be higher than all previous closes in the 40-candle lookback window.
# C3 upper shadow must be at least 2x its body.
# C3 lower shadow must be <= configured max ratio of its body.
# C1 and C2 quote volume must be >= average quote volume of previous max 40 candles before C1.
# C1 and C2 body length must be > average body length of previous max 40 candles before C1.
TRIGGER_2_CLOSE_LOOKBACK_CANDLES = 40
TRIGGER_2_VOLUME_LOOKBACK_CANDLES = 40
TRIGGER_2_BODY_LOOKBACK_CANDLES = 40
TRIGGER_2_UPPER_SHADOW_BODY_RATIO_MIN = 2.0
TRIGGER_2_LOWER_SHADOW_BODY_RATIO_MAX = 0.10
TRIGGER_2_BODY_AVG_RATIO_MIN = 1.0

# Minimal logging controls.
LOG_CLOSED_CANDLES = False
LOG_INELIGIBLE_CANDLES = False
LOG_S3_SAVE_DETAILS = False
LOG_SYMBOL_LISTS = False
LOG_ALERT_PREVIEW = False

# WebSocket self-healing.
# If the Python process stays alive but the Binance WebSocket stops receiving
# closed candles, exit intentionally and let systemd restart the service.
WEBSOCKET_NO_CANDLE_TIMEOUT_SECONDS = 10 * 60
WEBSOCKET_HEALTHCHECK_SECONDS = 60

# Volume confirmation:
# trigger candle quote volume must be greater than average quote volume of previous max N candles.
REQUIRE_TRIGGER_VOLUME_CONFIRMATION = True
VOLUME_CONFIRMATION_MAX_CANDLES = 40

# S3 lookup optimization.
# Existing triggers/sent logs are loaded from min(active_cycle_date) - this lookback.
EXISTING_HISTORY_LOOKBACK_DAYS = 1
SENT_ALERT_LOOKBACK_DAYS = 1

# Startup recovery.
# Startup loads recent S3 candle history so 40-candle lookbacks are available after restart.
# Swing-low rebuild/catch-up stays disabled by default to avoid firing old signals
# across long maintenance/test gaps.
LOAD_S3_CANDLE_HISTORY_ON_STARTUP = True
REBUILD_SWING_LOWS_AND_CATCH_UP_ON_STARTUP = False

# Startup history/backfill optimization.
# We do not need to load all historical candle files on every restart.
# For current trigger logic, the largest lookback is 40 candles, so the latest
# 60 saved candle folders per interval is enough context with a safety buffer.
STARTUP_S3_CANDLE_HISTORY_MAX_FILES_PER_INTERVAL = 60

# On startup/restart, check the most recent completed candle folders.
# If a folder is missing or has too few rows, fetch missing rows from Binance REST,
# save them to S3, and add them to in-memory history before live WebSocket starts.
STARTUP_BACKFILL_RECENT_CANDLES = True
STARTUP_BACKFILL_LOOKBACK_CANDLES = 20
STARTUP_BACKFILL_MIN_COMPLETENESS_RATIO = 0.80
BINANCE_REST_BASE = "https://api.binance.com"
BINANCE_REST_KLINES_ENDPOINT = "/api/v3/klines"
BINANCE_REST_TIMEOUT_SECONDS = 20
BINANCE_REST_SLEEP_SECONDS = 0.05

# Telegram credentials. Recommended: set as environment variables.
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ==================================================
# Global runtime state
# ==================================================

state_lock = threading.Lock()
batch_lock = threading.Lock()
history_lock = threading.Lock()
stop_event = threading.Event()

current_ws_app = None
current_ws_thread = None
active_connection_id = 0

current_state = {
    "symbols": set(),
    "symbols_list": [],
    "active_cycle_start_by_symbol": {},
    "first_valid_candle_by_interval_symbol": {},
    "latest_recent_key": None,
    "recent_run_date": None,
    "recent_run_time": None,
    "activation_source_column": None,
}

# Candle batches waiting for S3 save.
# Key: (interval, candle_open_time_iso)
candle_batches = {}

# In-memory candle history.
# candle_history[interval][symbol][open_time_iso] = candle row dict
candle_history = {
    interval: defaultdict(dict)
    for interval in INTERVALS
}

# swing_lows_by_interval_symbol[interval][symbol][swing_low_id] = swing row dict
swing_lows_by_interval_symbol = {
    interval: defaultdict(dict)
    for interval in INTERVALS
}

# Existing or newly triggered swing lows.
triggered_swing_low_ids_by_interval = {
    interval: set()
    for interval in INTERVALS
}

# Existing or newly sent Telegram alerts.
sent_trigger_ids_by_interval = {
    interval: set()
    for interval in INTERVALS
}

# Existing or newly processed Trigger 2 IDs.
processed_trigger_2_ids_by_interval = {
    interval: set()
    for interval in INTERVALS
}

closed_candle_messages_received = 0
saved_candle_batches_count = 0

# WebSocket health state.
last_websocket_started_at_utc = None
last_websocket_opened_at_utc = None
last_closed_candle_received_at_utc = None
last_websocket_error = None
last_websocket_closed_at_utc = None


# ==================================================
# S3 helpers
# ==================================================

def list_s3_keys(bucket, prefix):
    s3_client = boto3.client("s3")

    keys = []
    continuation_token = None

    while True:
        kwargs = {
            "Bucket": bucket,
            "Prefix": prefix,
        }

        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        response = s3_client.list_objects_v2(**kwargs)

        for obj in response.get("Contents", []):
            keys.append(obj["Key"])

        if response.get("IsTruncated"):
            continuation_token = response.get("NextContinuationToken")
        else:
            break

    return keys


def read_csv_from_s3(bucket, key):
    s3_client = boto3.client("s3")

    response = s3_client.get_object(
        Bucket=bucket,
        Key=key,
    )

    body = response["Body"].read().decode("utf-8")

    return pd.read_csv(io.StringIO(body))


def upload_dataframe_to_s3(df, bucket, key):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue(),
    )

    if LOG_S3_SAVE_DETAILS:
        print(f"Saved: s3://{bucket}/{key}")


def append_dataframe_to_s3(df_new, bucket, key, dedupe_subset=None, sort_cols=None):
    """
    Appends to an S3 CSV partition by reading existing file if present,
    concatenating, de-duplicating, then overwriting the same key.
    """

    try:
        df_existing = read_csv_from_s3(bucket, key)
        df_to_save = pd.concat([df_existing, df_new], ignore_index=True)
    except Exception:
        df_to_save = df_new.copy()

    if dedupe_subset:
        available_subset = [col for col in dedupe_subset if col in df_to_save.columns]
        if available_subset:
            df_to_save = df_to_save.drop_duplicates(
                subset=available_subset,
                keep="last",
            )

    if sort_cols:
        available_sort_cols = [col for col in sort_cols if col in df_to_save.columns]
        if available_sort_cols:
            df_to_save = df_to_save.sort_values(available_sort_cols)

    df_to_save = df_to_save.reset_index(drop=True)

    upload_dataframe_to_s3(df_to_save, bucket, key)

    return df_to_save


# ==================================================
# Time helpers
# ==================================================

def parse_utc_datetime(value):
    dt = pd.to_datetime(value, utc=True)

    if pd.isna(dt):
        return None

    return dt.to_pydatetime()


def ms_to_utc_datetime(ms_value):
    return datetime.fromtimestamp(ms_value / 1000, tz=timezone.utc)


def dt_to_iso(value):
    if value is None:
        return None

    dt = parse_utc_datetime(value)
    if dt is None:
        return None

    return dt.isoformat()


def next_full_interval_candle_after(dt, interval_minutes):
    """
    Use only candles whose open_time_utc is >= the next full interval boundary
    after activation time.

    Examples for 5m:
      activation 14:32:10 -> first valid candle 14:35:00
      activation 14:35:00 -> first valid candle 14:35:00

    Examples for 15m:
      activation 14:32:10 -> first valid candle 14:45:00
      activation 14:30:00 -> first valid candle 14:30:00
    """

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    floored_minute = (dt.minute // interval_minutes) * interval_minutes

    floored = dt.replace(
        minute=floored_minute,
        second=0,
        microsecond=0,
    )

    if dt == floored:
        return floored

    return floored + timedelta(minutes=interval_minutes)


def get_partition_from_open_time(open_time_utc):
    dt = parse_utc_datetime(open_time_utc)
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H%M")


def are_adjacent_candles(interval, left, middle, right):
    """
    Validates that the 3 candles used for a swing low are truly adjacent
    for the selected interval.

    This prevents a restart/maintenance gap from creating a false structure like:
      yesterday 22:06, yesterday 22:09, today 21:09
    """

    interval_delta = timedelta(minutes=INTERVAL_MINUTES[interval])

    left_time = parse_utc_datetime(left["open_time_utc"])
    middle_time = parse_utc_datetime(middle["open_time_utc"])
    right_time = parse_utc_datetime(right["open_time_utc"])

    if left_time is None or middle_time is None or right_time is None:
        return False

    return (
        middle_time - left_time == interval_delta
        and right_time - middle_time == interval_delta
    )


def are_consecutive_candles(interval, rows):
    """
    Validates that every candle in a window is exactly one interval apart.

    This is required for the 7-candle swing-low rule so we never build a
    structure across restart, maintenance, missing-data, or S3-history gaps.
    """

    if len(rows) < 2:
        return True

    interval_delta = timedelta(minutes=INTERVAL_MINUTES[interval])

    times = [parse_utc_datetime(row["open_time_utc"]) for row in rows]

    if any(t is None for t in times):
        return False

    for previous_time, current_time in zip(times[:-1], times[1:]):
        if current_time - previous_time != interval_delta:
            return False

    return True


def now_utc():
    return datetime.now(timezone.utc)


# ==================================================
# Recent activity helpers
# ==================================================

def get_latest_recent_activity_key(bucket, recent_activity_prefix):
    keys = list_s3_keys(bucket, recent_activity_prefix)

    pattern = re.compile(
        rf"^{re.escape(recent_activity_prefix)}/"
        r"run_date=(\d{4}-\d{2}-\d{2})/"
        r"run_time=(\d{4})/"
        r"general_filters\.csv$"
    )

    candidates = []

    for key in keys:
        match = pattern.match(key)

        if match:
            run_date = match.group(1)
            run_time = match.group(2)

            run_dt = datetime.strptime(
                f"{run_date} {run_time}",
                "%Y-%m-%d %H%M",
            ).replace(tzinfo=timezone.utc)

            candidates.append({
                "key": key,
                "run_date": run_date,
                "run_time": run_time,
                "run_dt": run_dt,
            })

    if not candidates:
        raise FileNotFoundError(
            f"No general_filters.csv files found under s3://{bucket}/{recent_activity_prefix}/"
        )

    latest = sorted(candidates, key=lambda x: x["run_dt"], reverse=True)[0]

    return latest


def load_latest_recent_activity_context():
    latest_recent = get_latest_recent_activity_key(
        bucket=S3_BUCKET,
        recent_activity_prefix=RECENT_ACTIVITY_PREFIX,
    )

    latest_recent_key = latest_recent["key"]

    df_recent = read_csv_from_s3(
        bucket=S3_BUCKET,
        key=latest_recent_key,
    )

    if "symbol" not in df_recent.columns:
        raise ValueError("general_filters.csv does not contain required column: symbol")

    if "active_cycle_start_datetime_utc" in df_recent.columns:
        activation_source_column = "active_cycle_start_datetime_utc"
    elif "recent_activity_run_datetime_utc" in df_recent.columns:
        activation_source_column = "recent_activity_run_datetime_utc"
    else:
        raise ValueError(
            "general_filters.csv must contain either active_cycle_start_datetime_utc "
            "or recent_activity_run_datetime_utc."
        )

    df_unique = df_recent.dropna(subset=["symbol", activation_source_column]).copy()
    df_unique["symbol"] = df_unique["symbol"].astype(str)
    df_unique = df_unique.drop_duplicates("symbol", keep="last")

    symbols = sorted(df_unique["symbol"].unique().tolist())

    active_cycle_start_by_symbol = {}
    first_valid_candle_by_interval_symbol = {
        interval: {}
        for interval in INTERVALS
    }

    for _, row in df_unique.iterrows():
        symbol = str(row["symbol"])
        activation_time = parse_utc_datetime(row[activation_source_column])

        if activation_time is None:
            continue

        active_cycle_start_by_symbol[symbol] = activation_time

        for interval in INTERVALS:
            first_valid = next_full_interval_candle_after(
                activation_time,
                INTERVAL_MINUTES[interval],
            )
            first_valid_candle_by_interval_symbol[interval][symbol] = first_valid

    context = {
        "symbols": set(symbols),
        "symbols_list": symbols,
        "active_cycle_start_by_symbol": active_cycle_start_by_symbol,
        "first_valid_candle_by_interval_symbol": first_valid_candle_by_interval_symbol,
        "latest_recent_key": latest_recent_key,
        "recent_run_date": latest_recent["run_date"],
        "recent_run_time": latest_recent["run_time"],
        "activation_source_column": activation_source_column,
    }

    return context


def context_signature(context):
    """
    Used to decide whether we must reload history and reconnect.
    If symbols are the same and active-cycle starts are the same, we can keep current streams.
    """

    rows = []
    for symbol in sorted(context["symbols"]):
        active_start = context["active_cycle_start_by_symbol"].get(symbol)
        rows.append((symbol, dt_to_iso(active_start)))

    return tuple(rows)


def update_current_state(context):
    with state_lock:
        current_state["symbols"] = set(context["symbols"])
        current_state["symbols_list"] = list(context["symbols_list"])
        current_state["active_cycle_start_by_symbol"] = dict(context["active_cycle_start_by_symbol"])
        current_state["first_valid_candle_by_interval_symbol"] = {
            interval: dict(context["first_valid_candle_by_interval_symbol"][interval])
            for interval in INTERVALS
        }
        current_state["latest_recent_key"] = context["latest_recent_key"]
        current_state["recent_run_date"] = context["recent_run_date"]
        current_state["recent_run_time"] = context["recent_run_time"]
        current_state["activation_source_column"] = context["activation_source_column"]


def get_state_snapshot():
    with state_lock:
        return {
            "symbols": set(current_state["symbols"]),
            "symbols_list": list(current_state["symbols_list"]),
            "active_cycle_start_by_symbol": dict(current_state["active_cycle_start_by_symbol"]),
            "first_valid_candle_by_interval_symbol": {
                interval: dict(current_state["first_valid_candle_by_interval_symbol"].get(interval, {}))
                for interval in INTERVALS
            },
            "latest_recent_key": current_state["latest_recent_key"],
            "recent_run_date": current_state["recent_run_date"],
            "recent_run_time": current_state["recent_run_time"],
            "activation_source_column": current_state["activation_source_column"],
        }


# ==================================================
# S3 key discovery for interval-partitioned outputs
# ==================================================

def make_candles_s3_key(interval, candle_open_time_utc):
    candle_date, candle_time = get_partition_from_open_time(candle_open_time_utc)

    return (
        f"{CANDLES_PREFIX}/"
        f"interval={interval}/"
        f"candle_date={candle_date}/"
        f"candle_time={candle_time}/"
        f"candles.csv"
    )


def make_swing_low_s3_key(interval, discovered_dt_utc):
    run_date = discovered_dt_utc.strftime("%Y-%m-%d")
    run_time = discovered_dt_utc.strftime("%H%M")

    return (
        f"{SWING_LOW_OUTPUT_PREFIX}/"
        f"interval={interval}/"
        f"run_date={run_date}/"
        f"run_time={run_time}/"
        f"swing_lows.csv"
    )


def make_trigger_s3_key(interval, entry_dt_utc, trigger_results_prefix=TRIGGER_RESULTS_PREFIX):
    """
    Trigger result partition uses entry time, which is the trigger candle close time.
    It does not use script save/discovery time.
    """

    trigger_date = entry_dt_utc.strftime("%Y-%m-%d")
    trigger_time = entry_dt_utc.strftime("%H%M")

    return (
        f"{trigger_results_prefix}/"
        f"interval={interval}/"
        f"trigger_date={trigger_date}/"
        f"trigger_time={trigger_time}/"
        f"triggers.csv"
    )



def make_sent_alert_s3_key(interval, sent_dt_utc):
    sent_date = sent_dt_utc.strftime("%Y-%m-%d")
    sent_time = sent_dt_utc.strftime("%H%M")

    return (
        f"{TG_ALERTS_SENT_PREFIX}/"
        f"interval={interval}/"
        f"sent_date={sent_date}/"
        f"sent_time={sent_time}/"
        f"sent_alerts.csv"
    )


def get_interval_candle_keys(bucket, candles_prefix, interval, min_candle_time=None):
    prefix = f"{candles_prefix}/interval={interval}/"

    keys = list_s3_keys(bucket, prefix)

    pattern = re.compile(
        rf"^{re.escape(candles_prefix)}/"
        rf"interval={re.escape(interval)}/"
        r"candle_date=(\d{4}-\d{2}-\d{2})/"
        r"candle_time=(\d{4})/"
        r"candles\.csv$"
    )

    candidates = []

    for key in keys:
        match = pattern.match(key)

        if match:
            candle_date = match.group(1)
            candle_time = match.group(2)

            candle_open_dt = datetime.strptime(
                f"{candle_date} {candle_time}",
                "%Y-%m-%d %H%M",
            ).replace(tzinfo=timezone.utc)

            if min_candle_time is not None and candle_open_dt < min_candle_time:
                continue

            candidates.append({
                "key": key,
                "candle_date": candle_date,
                "candle_time": candle_time,
                "candle_open_dt": candle_open_dt,
            })

    return sorted(candidates, key=lambda x: x["candle_open_dt"])


def get_existing_trigger_keys(bucket, trigger_results_prefix, interval, min_trigger_date=None):
    prefix = f"{trigger_results_prefix}/interval={interval}/"

    keys = list_s3_keys(bucket, prefix)

    pattern = re.compile(
        rf"^{re.escape(trigger_results_prefix)}/"
        rf"interval={re.escape(interval)}/"
        r"trigger_date=(\d{4}-\d{2}-\d{2})/"
        r"trigger_time=(\d{4})/"
        r"triggers\.csv$"
    )

    candidates = []

    for key in keys:
        match = pattern.match(key)

        if match:
            trigger_date_str = match.group(1)
            trigger_time = match.group(2)

            trigger_date = datetime.strptime(trigger_date_str, "%Y-%m-%d").date()

            if min_trigger_date is not None and trigger_date < min_trigger_date:
                continue

            trigger_dt = datetime.strptime(
                f"{trigger_date_str} {trigger_time}",
                "%Y-%m-%d %H%M",
            ).replace(tzinfo=timezone.utc)

            candidates.append({
                "key": key,
                "trigger_date": trigger_date_str,
                "trigger_time": trigger_time,
                "trigger_date_obj": trigger_date,
                "trigger_dt": trigger_dt,
            })

    return sorted(candidates, key=lambda x: x["trigger_dt"])


def get_sent_alert_keys(bucket, sent_prefix, interval, min_sent_date=None):
    prefix = f"{sent_prefix}/interval={interval}/"

    keys = list_s3_keys(bucket, prefix)

    pattern = re.compile(
        rf"^{re.escape(sent_prefix)}/"
        rf"interval={re.escape(interval)}/"
        r"sent_date=(\d{4}-\d{2}-\d{2})/"
        r"sent_time=(\d{4})/"
        r"sent_alerts\.csv$"
    )

    candidates = []

    for key in keys:
        match = pattern.match(key)

        if match:
            sent_date_str = match.group(1)
            sent_time = match.group(2)
            sent_date = datetime.strptime(sent_date_str, "%Y-%m-%d").date()

            if min_sent_date is not None and sent_date < min_sent_date:
                continue

            sent_dt = datetime.strptime(
                f"{sent_date_str} {sent_time}",
                "%Y-%m-%d %H%M",
            ).replace(tzinfo=timezone.utc)

            candidates.append({
                "key": key,
                "sent_date": sent_date_str,
                "sent_time": sent_time,
                "sent_date_obj": sent_date,
                "sent_dt": sent_dt,
            })

    return sorted(candidates, key=lambda x: x["sent_dt"])


# ==================================================
# IDs
# ==================================================

def make_swing_low_id_from_values(interval, symbol, swing_low_open_time_utc, swing_low_price, confirmation_candle_open_time_utc):
    return (
        str(interval)
        + "|"
        + str(symbol)
        + "|"
        + dt_to_iso(swing_low_open_time_utc)
        + "|"
        + str(swing_low_price)
        + "|"
        + dt_to_iso(confirmation_candle_open_time_utc)
    )


def make_trigger_id(row):
    return (
        str(row["interval"])
        + "|"
        + str(row["swing_low_id"])
        + "|"
        + dt_to_iso(row["entry_time_utc"])
        + "|"
        + str(row["entry_price"])
    )


def make_trigger_2_id(row):
    return (
        str(row["interval"])
        + "|"
        + str(row["trigger_type"])
        + "|"
        + str(row["symbol"])
        + "|"
        + dt_to_iso(row["entry_time_utc"])
        + "|"
        + str(row["entry_price"])
    )


# ==================================================
# WebSocket URL and connection helpers
# ==================================================

def build_ws_url(symbols, intervals):
    streams = []

    for symbol in sorted(symbols):
        symbol_lower = symbol.lower()
        for interval in intervals:
            streams.append(f"{symbol_lower}@kline_{interval}")

    return BINANCE_WS_BASE + "/".join(streams)


def get_interval_label(interval):
    minutes = INTERVAL_MINUTES.get(interval)
    if minutes is None:
        return interval
    return f"{minutes}-Min"


# ==================================================
# Candle saving and history
# ==================================================

def make_batch_id(interval, candle_open_time_utc):
    return (interval, dt_to_iso(candle_open_time_utc))


def schedule_batch_save(batch_id):
    timer = threading.Timer(
        BATCH_WAIT_SECONDS,
        save_batch_if_ready,
        args=[batch_id],
    )
    timer.daemon = True
    timer.start()


def save_batch_if_ready(batch_id):
    global saved_candle_batches_count

    interval, candle_open_time_iso = batch_id

    with batch_lock:
        rows = candle_batches.pop(batch_id, [])

    if not rows:
        return

    df_batch = pd.DataFrame(rows)

    candle_open_time = parse_utc_datetime(candle_open_time_iso)
    output_key = make_candles_s3_key(interval, candle_open_time)

    if SAVE_CANDLES_TO_S3:
        append_dataframe_to_s3(
            df_new=df_batch,
            bucket=S3_BUCKET,
            key=output_key,
            dedupe_subset=["interval", "symbol", "open_time_utc"],
            sort_cols=["symbol"],
        )

    saved_candle_batches_count += 1

    print(
        f"Saved candle batch: interval={interval}, "
        f"candle_open={candle_open_time_iso}, rows={len(df_batch)}"
    )


def queue_candle_for_s3_save(candle_row):
    batch_id = make_batch_id(
        candle_row["interval"],
        candle_row["open_time_utc"],
    )

    should_schedule = False

    with batch_lock:
        if batch_id not in candle_batches:
            candle_batches[batch_id] = []
            should_schedule = True

        candle_batches[batch_id].append(candle_row)

    if should_schedule:
        schedule_batch_save(batch_id)


def build_candle_row_from_kline(kline, event_time_ms):
    interval = str(kline["i"])
    symbol = str(kline["s"])

    open_time_utc = ms_to_utc_datetime(kline["t"])
    close_time_utc_raw = ms_to_utc_datetime(kline["T"])
    event_close_time_utc = ms_to_utc_datetime(event_time_ms)
    candle_close_time_utc = open_time_utc + timedelta(minutes=INTERVAL_MINUTES[interval])

    state_snapshot = get_state_snapshot()
    active_cycle_start = state_snapshot["active_cycle_start_by_symbol"].get(symbol)
    first_valid = state_snapshot["first_valid_candle_by_interval_symbol"].get(interval, {}).get(symbol)

    return {
        "interval": interval,
        "symbol": symbol,
        "open_time_utc": open_time_utc,
        "candle_close_time_utc": candle_close_time_utc,
        "close_time_utc_raw": close_time_utc_raw,
        "event_close_time_utc": event_close_time_utc,
        "open": float(kline["o"]),
        "high": float(kline["h"]),
        "low": float(kline["l"]),
        "close": float(kline["c"]),
        "volume": float(kline["v"]),
        "quote_asset_volume": float(kline["q"]),
        "number_of_trades": int(kline["n"]),
        "first_trade_id": kline.get("f"),
        "last_trade_id": kline.get("L"),
        "taker_buy_base_volume": float(kline["V"]),
        "taker_buy_quote_volume": float(kline["Q"]),
        "is_closed": bool(kline["x"]),
        "source": SOURCE,
        "received_at_utc": now_utc(),
        "active_cycle_start_datetime_utc": active_cycle_start,
        "first_valid_candle_open_time_utc": first_valid,
        "activation_source_column": state_snapshot["activation_source_column"],
        "recent_activity_s3_key": state_snapshot["latest_recent_key"],
        "recent_activity_run_date": state_snapshot["recent_run_date"],
        "recent_activity_run_time": state_snapshot["recent_run_time"],
    }


def clean_candle_dataframe(df):
    if df.empty:
        return df

    required_cols = [
        "interval",
        "symbol",
        "open_time_utc",
        "open",
        "high",
        "low",
        "close",
        "quote_asset_volume",
    ]

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required candle columns: {missing_cols}")

    df = df.copy()
    df["interval"] = df["interval"].astype(str)
    df["symbol"] = df["symbol"].astype(str)
    df["open_time_utc"] = pd.to_datetime(df["open_time_utc"], utc=True)

    datetime_cols = [
        "candle_close_time_utc",
        "close_time_utc_raw",
        "event_close_time_utc",
        "received_at_utc",
        "active_cycle_start_datetime_utc",
        "first_valid_candle_open_time_utc",
    ]

    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["interval", "symbol", "open_time_utc", "open", "high", "low", "close"])

    return df


def candle_row_to_history_row(row):
    if isinstance(row, pd.Series):
        row = row.to_dict()

    result = dict(row)

    for col in [
        "open_time_utc",
        "candle_close_time_utc",
        "close_time_utc_raw",
        "event_close_time_utc",
        "received_at_utc",
        "active_cycle_start_datetime_utc",
        "first_valid_candle_open_time_utc",
    ]:
        if col in result and result[col] is not None and not pd.isna(result[col]):
            result[col] = parse_utc_datetime(result[col])

    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "number_of_trades",
    ]:
        if col in result and result[col] is not None and not pd.isna(result[col]):
            result[col] = float(result[col])

    return result


def get_sorted_history_rows(interval, symbol):
    rows = list(candle_history[interval][symbol].values())
    rows = sorted(rows, key=lambda x: parse_utc_datetime(x["open_time_utc"]))
    return rows


def add_candle_to_history(candle_row):
    interval = candle_row["interval"]
    symbol = candle_row["symbol"]
    open_time_iso = dt_to_iso(candle_row["open_time_utc"])

    with history_lock:
        candle_history[interval][symbol][open_time_iso] = candle_row_to_history_row(candle_row)


def is_candle_eligible_for_active_cycle(candle_row):
    interval = candle_row["interval"]
    symbol = candle_row["symbol"]
    open_time = parse_utc_datetime(candle_row["open_time_utc"])

    state_snapshot = get_state_snapshot()

    if symbol not in state_snapshot["symbols"]:
        return False

    first_valid = state_snapshot["first_valid_candle_by_interval_symbol"].get(interval, {}).get(symbol)

    if first_valid is None:
        return False

    return open_time >= first_valid


# ==================================================
# Swing-low and trigger logic
# ==================================================


def is_7_candle_swing_low_window(window, previous_rows_before_c1=None):
    """
    7-candle swing-low rule.

    We analyze 7 consecutive closed candles:

      C1 C2 C3 C4 C5 C6 C7

    Conditions:
      1. C1.close > C2.close > C3.close
      2. C4.close is lower than every other close in the 7-candle window
      3. C4.close is lower than each of the previous 40 candle closes before C4
      4. C5.close < C6.close < C7.close
      5. swing_depth >= 2 x avg_body_prev_40

    swing_depth = max(C1.close, C2.close, C3.close) - C4.close
    avg_body_prev_40 = average abs(open - close) of previous max 40 candles before C1.

    The swing-low level used for trigger is C4.close.
    There is no lower-shadow/body condition.
    """

    if len(window) != SWING_LOW_PATTERN_CANDLES:
        return False, {}

    c1, c2, c3, c4, c5, c6, c7 = window

    closes = [float(candle["close"]) for candle in window]

    closes_down_into_c4 = closes[0] > closes[1] > closes[2]
    c4_is_lowest_close = closes[3] < min(closes[:3] + closes[4:])
    closes_up_after_c4 = closes[4] < closes[5] < closes[6]

    previous_rows_before_c1 = previous_rows_before_c1 or []

    # C4 should also be lower than each of the previous 40 candle closes before C4.
    # This check requires exactly 40 previous candles before C4.
    # The previous candles before C4 are: historical rows before C1 + C1 + C2 + C3.
    previous_rows_before_c4 = list(previous_rows_before_c1) + [c1, c2, c3]
    previous_rows_before_c4 = sorted(
        previous_rows_before_c4,
        key=lambda x: parse_utc_datetime(x["open_time_utc"]),
    )[-SWING_LOW_CLOSE_LOOKBACK_CANDLES:]

    previous_closes_before_c4 = [
        float(row["close"])
        for row in previous_rows_before_c4
    ]

    c4_previous_close_candles_used = len(previous_closes_before_c4)
    c4_previous_min_close_last_40_excluding_c4 = (
        min(previous_closes_before_c4)
        if c4_previous_close_candles_used > 0
        else None
    )
    c4_has_required_previous_40_closes = (
        c4_previous_close_candles_used == SWING_LOW_CLOSE_LOOKBACK_CANDLES
    )
    c4_is_lowest_close_last_40_excluding_c4 = (
        closes[3] < c4_previous_min_close_last_40_excluding_c4
        if c4_has_required_previous_40_closes
        and c4_previous_min_close_last_40_excluding_c4 is not None
        else False
    )
    previous_body_rows = sorted(
        previous_rows_before_c1,
        key=lambda x: parse_utc_datetime(x["open_time_utc"]),
    )[-SWING_LOW_BODY_LOOKBACK_CANDLES:]

    previous_body_lengths = [
        abs(float(row["close"]) - float(row["open"]))
        for row in previous_body_rows
    ]

    previous_body_lengths = [value for value in previous_body_lengths if value >= 0]

    bodies_available = len(previous_body_lengths)
    avg_body_prev_40 = (
        sum(previous_body_lengths) / bodies_available
        if bodies_available > 0
        else None
    )

    swing_depth = max(closes[0], closes[1], closes[2]) - closes[3]

    if avg_body_prev_40 is None:
        swing_depth_vs_avg_body = None
        swing_depth_pass = False
    elif avg_body_prev_40 == 0:
        swing_depth_vs_avg_body = None
        swing_depth_pass = swing_depth > 0
    else:
        swing_depth_vs_avg_body = swing_depth / avg_body_prev_40
        swing_depth_pass = swing_depth >= SWING_LOW_DEPTH_BODY_RATIO_MIN * avg_body_prev_40

    diagnostics = {
        "c1_close": closes[0],
        "c2_close": closes[1],
        "c3_close": closes[2],
        "c4_close": closes[3],
        "c5_close": closes[4],
        "c6_close": closes[5],
        "c7_close": closes[6],
        "closes_down_into_c4": closes_down_into_c4,
        "c4_is_lowest_close": c4_is_lowest_close,
        "c4_is_lowest_close_last_40_excluding_c4": c4_is_lowest_close_last_40_excluding_c4,
        "c4_has_required_previous_40_closes": c4_has_required_previous_40_closes,
        "c4_lowest_close_lookback_candles": SWING_LOW_CLOSE_LOOKBACK_CANDLES,
        "c4_previous_close_candles_used": c4_previous_close_candles_used,
        "c4_previous_min_close_last_40_excluding_c4": c4_previous_min_close_last_40_excluding_c4,
        "closes_up_after_c4": closes_up_after_c4,
        "swing_depth": swing_depth,
        "avg_body_prev_40": avg_body_prev_40,
        "swing_depth_vs_avg_body": swing_depth_vs_avg_body,
        "swing_depth_pass": swing_depth_pass,
        "swing_depth_body_ratio_min": SWING_LOW_DEPTH_BODY_RATIO_MIN,
        "swing_depth_body_lookback_candles": SWING_LOW_BODY_LOOKBACK_CANDLES,
        "swing_depth_body_candles_used": bodies_available,
    }

    is_swing_low = (
        closes_down_into_c4
        and c4_is_lowest_close
        and c4_is_lowest_close_last_40_excluding_c4
        and closes_up_after_c4
        and swing_depth_pass
    )

    return is_swing_low, diagnostics

def build_swing_low_row(interval, window, discovery_dt_utc, latest_recent_key=None, diagnostics=None):
    c1, c2, c3, c4, c5, c6, c7 = window

    symbol = c4["symbol"]

    # Swing-low level is the C4 close price.
    swing_low_price = float(c4["close"])

    swing_low_id = make_swing_low_id_from_values(
        interval=interval,
        symbol=symbol,
        swing_low_open_time_utc=c4["open_time_utc"],
        swing_low_price=swing_low_price,
        confirmation_candle_open_time_utc=c7["open_time_utc"],
    )

    if diagnostics is None:
        diagnostics = {}

    row = {
        "interval": interval,
        "swing_low_id": swing_low_id,
        "symbol": symbol,

        # C4 is the swing-low candle.
        "swing_low_open_time_utc": c4["open_time_utc"],
        "swing_low_close_time_utc": c4.get("event_close_time_utc") or c4.get("candle_close_time_utc"),
        "swing_low_price": swing_low_price,
        "swing_low_price_source": "c4_close",

        # Immediate neighbors retained for compatibility/debugging.
        "left_candle_open_time_utc": c3["open_time_utc"],
        "left_low": c3["low"],
        "left_close": c3["close"],
        "right_candle_open_time_utc": c5["open_time_utc"],
        "right_low": c5["low"],
        "right_close": c5["close"],

        # C7 confirms the full 7-candle pattern.
        "confirmation_candle_open_time_utc": c7["open_time_utc"],
        "confirmation_candle_close_time_utc": c7.get("event_close_time_utc") or c7.get("candle_close_time_utc"),

        # C4 OHLC.
        "swing_low_candle_open": c4["open"],
        "swing_low_candle_high": c4["high"],
        "swing_low_candle_low": c4["low"],
        "swing_low_candle_close": c4["close"],
        "swing_low_candle_quote_volume": c4.get("quote_asset_volume"),

        # Full 7-candle audit fields.
        "c1_open_time_utc": c1["open_time_utc"],
        "c1_close": c1["close"],
        "c2_open_time_utc": c2["open_time_utc"],
        "c2_close": c2["close"],
        "c3_open_time_utc": c3["open_time_utc"],
        "c3_close": c3["close"],
        "c4_open_time_utc": c4["open_time_utc"],
        "c4_close": c4["close"],
        "c5_open_time_utc": c5["open_time_utc"],
        "c5_close": c5["close"],
        "c6_open_time_utc": c6["open_time_utc"],
        "c6_close": c6["close"],
        "c7_open_time_utc": c7["open_time_utc"],
        "c7_close": c7["close"],

        # Pattern diagnostics.
        "closes_down_into_c4": diagnostics.get("closes_down_into_c4"),
        "c4_is_lowest_close": diagnostics.get("c4_is_lowest_close"),
        "c4_is_lowest_close_last_40_excluding_c4": diagnostics.get("c4_is_lowest_close_last_40_excluding_c4"),
        "c4_has_required_previous_40_closes": diagnostics.get("c4_has_required_previous_40_closes"),
        "c4_lowest_close_lookback_candles": diagnostics.get("c4_lowest_close_lookback_candles"),
        "c4_previous_close_candles_used": diagnostics.get("c4_previous_close_candles_used"),
        "c4_previous_min_close_last_40_excluding_c4": diagnostics.get("c4_previous_min_close_last_40_excluding_c4"),
        "closes_up_after_c4": diagnostics.get("closes_up_after_c4"),
        "swing_depth": diagnostics.get("swing_depth"),
        "avg_body_prev_40": diagnostics.get("avg_body_prev_40"),
        "swing_depth_vs_avg_body": diagnostics.get("swing_depth_vs_avg_body"),
        "swing_depth_pass": diagnostics.get("swing_depth_pass"),
        "swing_depth_body_ratio_min": diagnostics.get("swing_depth_body_ratio_min"),
        "swing_depth_body_lookback_candles": diagnostics.get("swing_depth_body_lookback_candles"),
        "swing_depth_body_candles_used": diagnostics.get("swing_depth_body_candles_used"),
        "source_s3_key": c4.get("source_s3_key"),
        "swing_low_search_run_datetime_utc": discovery_dt_utc,
        "recent_activity_s3_key": latest_recent_key,
        "active_cycle_start_datetime_utc": c4.get("active_cycle_start_datetime_utc"),
        "first_valid_candle_open_time_utc": c4.get("first_valid_candle_open_time_utc"),
        "swing_low_status": "active_latest",
        "superseded_previous_swing_lows_count": 0,
        "superseded_previous_swing_low_ids": "",
        "swing_low_rule": (
            "7-candle close-based structure: C1.close > C2.close > C3.close, "
            "C4.close is lowest close across the 7-candle window and lower than previous 40 closes, "
            "C5.close < C6.close < C7.close; "
            "swing_depth >= 2 x avg_body_prev_40; no lower-shadow/body condition"
        ),
        "activation_rule": "open_time_utc >= next full interval candle after active_cycle_start_datetime_utc",
    }

    return row


def detect_latest_confirmed_swing_low(interval, symbol):
    rows = get_sorted_history_rows(interval, symbol)

    if len(rows) < SWING_LOW_PATTERN_CANDLES:
        return None

    window = rows[-SWING_LOW_PATTERN_CANDLES:]
    previous_rows_before_c1 = rows[:-SWING_LOW_PATTERN_CANDLES]

    if not are_consecutive_candles(interval, window):
        times = " -> ".join(dt_to_iso(row["open_time_utc"]) for row in window)
        print(
            f"Skipping 7-candle swing-low check across candle gap: "
            f"{symbol} {interval} {times}"
        )
        return None

    is_swing_low, diagnostics = is_7_candle_swing_low_window(
        window,
        previous_rows_before_c1=previous_rows_before_c1,
    )

    if not is_swing_low:
        return None

    latest_recent_key = get_state_snapshot()["latest_recent_key"]

    return build_swing_low_row(
        interval=interval,
        window=window,
        discovery_dt_utc=now_utc(),
        latest_recent_key=latest_recent_key,
        diagnostics=diagnostics,
    )



def calculate_previous_quote_volume_stats_from_rows(rows, trigger_candle_open_time_utc, max_lookback_count):
    trigger_time = parse_utc_datetime(trigger_candle_open_time_utc)

    previous_rows = [
        row for row in rows
        if parse_utc_datetime(row["open_time_utc"]) < trigger_time
    ]

    previous_rows = sorted(previous_rows, key=lambda x: parse_utc_datetime(x["open_time_utc"]))
    previous_rows = previous_rows[-max_lookback_count:]

    candles_used = len(previous_rows)

    if candles_used == 0:
        return {
            "candles_used": 0,
            "avg_quote_volume": None,
            "first_previous_candle_open_time_utc": None,
            "last_previous_candle_open_time_utc": None,
        }

    quote_volumes = [float(row["quote_asset_volume"]) for row in previous_rows]
    avg_quote_volume = sum(quote_volumes) / len(quote_volumes)

    return {
        "candles_used": candles_used,
        "avg_quote_volume": avg_quote_volume,
        "first_previous_candle_open_time_utc": previous_rows[0]["open_time_utc"],
        "last_previous_candle_open_time_utc": previous_rows[-1]["open_time_utc"],
    }


def volume_confirmation_passes(rows, candidate_candle):
    volume_stats = calculate_previous_quote_volume_stats_from_rows(
        rows=rows,
        trigger_candle_open_time_utc=candidate_candle["open_time_utc"],
        max_lookback_count=VOLUME_CONFIRMATION_MAX_CANDLES,
    )

    avg_previous_quote_volume = volume_stats["avg_quote_volume"]
    candidate_quote_volume = candidate_candle.get("quote_asset_volume")

    if not REQUIRE_TRIGGER_VOLUME_CONFIRMATION:
        return True, volume_stats

    if avg_previous_quote_volume is None:
        return False, volume_stats

    if candidate_quote_volume is None or pd.isna(candidate_quote_volume):
        return False, volume_stats

    return float(candidate_quote_volume) > float(avg_previous_quote_volume), volume_stats


def build_trigger_row(interval, swing_low_row, trigger_candle, volume_stats, discovery_dt_utc):
    trigger_candle_open_time_utc = parse_utc_datetime(trigger_candle["open_time_utc"])

    trigger_candle_close_time_utc = parse_utc_datetime(
        trigger_candle.get("candle_close_time_utc")
    )

    if trigger_candle_close_time_utc is None:
        trigger_candle_close_time_utc = trigger_candle_open_time_utc + timedelta(
            minutes=INTERVAL_MINUTES[interval]
        )

    # Entry time is when the trigger candle closes.
    # The signal is only actionable after the candle close is known.
    entry_time_utc = trigger_candle_close_time_utc

    trigger_candle_date = trigger_candle_open_time_utc.strftime("%Y-%m-%d")
    trigger_candle_time = trigger_candle_open_time_utc.strftime("%H%M")

    entry_date = entry_time_utc.strftime("%Y-%m-%d")
    entry_time = entry_time_utc.strftime("%H%M")

    trigger_candle_quote_volume = trigger_candle.get("quote_asset_volume")
    avg_previous_quote_volume = volume_stats["avg_quote_volume"]

    if avg_previous_quote_volume not in [None, 0] and not pd.isna(avg_previous_quote_volume):
        quote_volume_vs_avg_ratio = float(trigger_candle_quote_volume) / float(avg_previous_quote_volume)
    else:
        quote_volume_vs_avg_ratio = None

    row = {
        "trigger_id": None,
        "swing_low_id": swing_low_row["swing_low_id"],
        "interval": interval,
        "trigger_type": TRIGGER_TYPE,
        "symbol": swing_low_row["symbol"],

        # Trigger result partitions use entry time, not script discovery/save time.
        "trigger_date": entry_date,
        "trigger_time": entry_time,

        # Separate metadata for when the engine actually found/saved the trigger.
        "trigger_discovered_datetime_utc": discovery_dt_utc,

        # Actual trigger candle timing.
        # trigger_candle_date/time are the candle OPEN time, kept for chart/audit.
        "trigger_candle_date": trigger_candle_date,
        "trigger_candle_time": trigger_candle_time,
        "trigger_candle_open_time_utc": trigger_candle_open_time_utc,
        "trigger_candle_close_time_utc": trigger_candle_close_time_utc,
        "trigger_candle_event_close_time_utc": trigger_candle.get("event_close_time_utc"),

        # Entry timing shown in alerts and used for S3 trigger partitioning.
        "entry_time_utc": entry_time_utc,
        "entry_date": entry_date,
        "entry_time": entry_time,

        # Swing low info.
        "swing_low_open_time_utc": swing_low_row["swing_low_open_time_utc"],
        "swing_low_price": swing_low_row["swing_low_price"],
        "confirmation_candle_open_time_utc": swing_low_row["confirmation_candle_open_time_utc"],

        # Trigger candle info.
        "trigger_candle_open": trigger_candle["open"],
        "trigger_candle_high": trigger_candle["high"],
        "trigger_candle_low": trigger_candle["low"],
        "trigger_candle_close": trigger_candle["close"],

        # Entry logic.
        "entry_price": trigger_candle["close"],

        # Volume / activity info.
        "trigger_candle_volume": trigger_candle.get("volume"),
        "trigger_candle_quote_volume": trigger_candle_quote_volume,
        "trigger_candle_number_of_trades": trigger_candle.get("number_of_trades"),

        # Volume confirmation info.
        "volume_confirmation_required": REQUIRE_TRIGGER_VOLUME_CONFIRMATION,
        "volume_confirmation_rule": (
            "trigger_candle_quote_volume > avg quote volume of previous max 40 available candles"
        ),
        "volume_confirmation_pass": True,
        "volume_lookback_candles_max": VOLUME_CONFIRMATION_MAX_CANDLES,
        "volume_lookback_candles_used": volume_stats["candles_used"],
        "avg_quote_volume_previous_max_40_candles": avg_previous_quote_volume,
        "trigger_quote_volume_vs_avg_previous_max_40_ratio": quote_volume_vs_avg_ratio,
        "volume_lookback_first_candle_open_time_utc": volume_stats["first_previous_candle_open_time_utc"],
        "volume_lookback_last_candle_open_time_utc": volume_stats["last_previous_candle_open_time_utc"],

        # S3/debug metadata.
        "trigger_candle_source_s3_key": trigger_candle.get("source_s3_key"),
        "swing_low_source_s3_key": swing_low_row.get("source_s3_key"),
        "recent_activity_s3_key": get_state_snapshot()["latest_recent_key"],

        # Run metadata.
        "trigger_checker_run_datetime_utc": discovery_dt_utc,
        "trigger_rule": (
            "first candle after confirmation where close < swing_low_price "
            "and trigger_candle_quote_volume > avg quote volume of previous max 40 available candles"
        ),
    }

    row["trigger_id"] = make_trigger_id(row)

    return row



def save_swing_low_row(interval, swing_low_row):
    if not SAVE_SWING_LOWS_TO_S3:
        return

    discovered_dt = parse_utc_datetime(swing_low_row["swing_low_search_run_datetime_utc"])
    output_key = make_swing_low_s3_key(interval, discovered_dt)

    df_new = pd.DataFrame([swing_low_row])

    append_dataframe_to_s3(
        df_new=df_new,
        bucket=S3_BUCKET,
        key=output_key,
        dedupe_subset=["swing_low_id"],
        sort_cols=["symbol", "swing_low_open_time_utc"],
    )


def save_trigger_row(interval, trigger_row):
    if not SAVE_TRIGGERS_TO_S3:
        return

    entry_dt = parse_utc_datetime(
        trigger_row.get("entry_time_utc")
        or trigger_row.get("trigger_candle_close_time_utc")
    )

    output_key = make_trigger_s3_key(interval, entry_dt)

    df_new = pd.DataFrame([trigger_row])

    append_dataframe_to_s3(
        df_new=df_new,
        bucket=S3_BUCKET,
        key=output_key,
        dedupe_subset=["trigger_id"],
        sort_cols=["symbol", "entry_time_utc", "trigger_candle_open_time_utc"],
    )


def save_trigger_2_row(interval, trigger_row):
    if not SAVE_TRIGGERS_TO_S3:
        return

    entry_dt = parse_utc_datetime(
        trigger_row.get("entry_time_utc")
        or trigger_row.get("trigger_candle_close_time_utc")
    )

    output_key = make_trigger_s3_key(
        interval=interval,
        entry_dt_utc=entry_dt,
        trigger_results_prefix=TRIGGER_2_RESULTS_PREFIX,
    )

    df_new = pd.DataFrame([trigger_row])

    append_dataframe_to_s3(
        df_new=df_new,
        bucket=S3_BUCKET,
        key=output_key,
        dedupe_subset=["trigger_id"],
        sort_cols=["symbol", "entry_time_utc", "trigger_candle_open_time_utc"],
    )



def replace_active_swing_low(interval, symbol, swing_low_row):
    """
    Keeps only the latest active swing low per symbol + interval.

    When a newer swing low is confirmed, all previous untriggered swing lows for
    the same symbol + interval are superseded and cannot trigger later.
    """

    swing_low_id = swing_low_row["swing_low_id"]

    with history_lock:
        existing = swing_lows_by_interval_symbol[interval][symbol]

        if swing_low_id in existing:
            return False, []

        previous_ids = list(existing.keys())

        swing_low_row["swing_low_status"] = "active_latest"
        swing_low_row["superseded_previous_swing_lows_count"] = len(previous_ids)
        swing_low_row["superseded_previous_swing_low_ids"] = "|".join(previous_ids)

        existing.clear()
        existing[swing_low_id] = swing_low_row

    return True, previous_ids



def check_current_candle_against_untriggered_swing_lows(interval, symbol, current_candle):
    rows = get_sorted_history_rows(interval, symbol)
    current_open_time = parse_utc_datetime(current_candle["open_time_utc"])

    trigger_rows = []

    with history_lock:
        swing_low_rows = list(swing_lows_by_interval_symbol[interval][symbol].values())

    # Only the latest active swing low per symbol + interval can trigger.
    # Older swing lows are superseded when a new swing low appears.
    swing_low_rows = [
        row for row in swing_low_rows
        if row["swing_low_id"] not in triggered_swing_low_ids_by_interval[interval]
    ]

    if not swing_low_rows:
        return trigger_rows

    swing_low_row = max(
        swing_low_rows,
        key=lambda x: parse_utc_datetime(x["swing_low_open_time_utc"]),
    )

    swing_low_id = swing_low_row["swing_low_id"]
    confirmation_time = parse_utc_datetime(swing_low_row["confirmation_candle_open_time_utc"])

    if current_open_time <= confirmation_time:
        return trigger_rows

    if float(current_candle["close"]) >= float(swing_low_row["swing_low_price"]):
        return trigger_rows

    volume_pass, volume_stats = volume_confirmation_passes(
        rows=rows,
        candidate_candle=current_candle,
    )

    if not volume_pass:
        return trigger_rows

    trigger_row = build_trigger_row(
        interval=interval,
        swing_low_row=swing_low_row,
        trigger_candle=current_candle,
        volume_stats=volume_stats,
        discovery_dt_utc=now_utc(),
    )

    triggered_swing_low_ids_by_interval[interval].add(swing_low_id)
    trigger_rows.append(trigger_row)

    return trigger_rows




def calculate_trigger_2_volume_stats(rows, c1):
    return calculate_previous_quote_volume_stats_from_rows(
        rows=rows,
        trigger_candle_open_time_utc=c1["open_time_utc"],
        max_lookback_count=TRIGGER_2_VOLUME_LOOKBACK_CANDLES,
    )


def calculate_trigger_2_body_stats(rows, c1):
    c1_open_time = parse_utc_datetime(c1["open_time_utc"])

    previous_rows = [
        row for row in rows
        if parse_utc_datetime(row["open_time_utc"]) < c1_open_time
    ]

    previous_rows = sorted(
        previous_rows,
        key=lambda x: parse_utc_datetime(x["open_time_utc"]),
    )[-TRIGGER_2_BODY_LOOKBACK_CANDLES:]

    body_lengths = [
        abs(float(row["close"]) - float(row["open"]))
        for row in previous_rows
    ]

    body_lengths = [value for value in body_lengths if value >= 0]

    candles_used = len(body_lengths)
    avg_body_length = (
        sum(body_lengths) / candles_used
        if candles_used > 0
        else None
    )

    return {
        "avg_body_length": avg_body_length,
        "candles_used": candles_used,
        "first_previous_candle_open_time_utc": previous_rows[0]["open_time_utc"] if previous_rows else None,
        "last_previous_candle_open_time_utc": previous_rows[-1]["open_time_utc"] if previous_rows else None,
    }


def build_trigger_2_row(interval, symbol, c1, c2, c3, previous_39_rows, volume_stats, body_stats, discovery_dt_utc):
    c3_open_time_utc = parse_utc_datetime(c3["open_time_utc"])
    c3_close_time_utc = parse_utc_datetime(c3.get("candle_close_time_utc"))

    if c3_close_time_utc is None:
        c3_close_time_utc = c3_open_time_utc + timedelta(minutes=INTERVAL_MINUTES[interval])

    entry_time_utc = c3_close_time_utc
    entry_date = entry_time_utc.strftime("%Y-%m-%d")
    entry_time = entry_time_utc.strftime("%H%M")

    c3_open = float(c3["open"])
    c3_close = float(c3["close"])
    c3_high = float(c3["high"])
    c3_low = float(c3["low"])

    c1_open = float(c1["open"])
    c1_close = float(c1["close"])
    c2_open = float(c2["open"])
    c2_close = float(c2["close"])

    c1_body_length = abs(c1_close - c1_open)
    c2_body_length = abs(c2_close - c2_open)
    c3_body_length = abs(c3_close - c3_open)
    c3_upper_shadow_length = c3_high - max(c3_open, c3_close)
    c3_lower_shadow_length = min(c3_open, c3_close) - c3_low

    avg_previous_body_length = body_stats["avg_body_length"]

    if avg_previous_body_length is None:
        c1_body_vs_avg_ratio = None
        c2_body_vs_avg_ratio = None
    elif avg_previous_body_length == 0:
        c1_body_vs_avg_ratio = None
        c2_body_vs_avg_ratio = None
    else:
        c1_body_vs_avg_ratio = c1_body_length / avg_previous_body_length
        c2_body_vs_avg_ratio = c2_body_length / avg_previous_body_length

    if c3_body_length == 0:
        c3_upper_shadow_to_body_ratio = float("inf") if c3_upper_shadow_length > 0 else 0.0
        c3_lower_shadow_to_body_ratio = float("inf") if c3_lower_shadow_length > 0 else 0.0
    else:
        c3_upper_shadow_to_body_ratio = c3_upper_shadow_length / c3_body_length
        c3_lower_shadow_to_body_ratio = c3_lower_shadow_length / c3_body_length

    previous_39_closes = [float(row["close"]) for row in previous_39_rows]
    highest_previous_39_close = max(previous_39_closes) if previous_39_closes else None

    avg_previous_quote_volume = volume_stats["avg_quote_volume"]

    c1_quote_volume = c1.get("quote_asset_volume")
    c2_quote_volume = c2.get("quote_asset_volume")

    if avg_previous_quote_volume not in [None, 0] and not pd.isna(avg_previous_quote_volume):
        c1_quote_volume_vs_avg_ratio = float(c1_quote_volume) / float(avg_previous_quote_volume)
        c2_quote_volume_vs_avg_ratio = float(c2_quote_volume) / float(avg_previous_quote_volume)
    else:
        c1_quote_volume_vs_avg_ratio = None
        c2_quote_volume_vs_avg_ratio = None

    row = {
        "trigger_id": None,
        "interval": interval,
        "trigger_type": TRIGGER_2_TYPE,
        "symbol": symbol,

        # Trigger result partitions use entry time, not script discovery/save time.
        "trigger_date": entry_date,
        "trigger_time": entry_time,
        "trigger_discovered_datetime_utc": discovery_dt_utc,

        # C3 is the latest closed candle and trigger candle.
        "trigger_candle_date": c3_open_time_utc.strftime("%Y-%m-%d"),
        "trigger_candle_time": c3_open_time_utc.strftime("%H%M"),
        "trigger_candle_open_time_utc": c3_open_time_utc,
        "trigger_candle_close_time_utc": c3_close_time_utc,
        "trigger_candle_event_close_time_utc": c3.get("event_close_time_utc"),
        "entry_time_utc": entry_time_utc,
        "entry_date": entry_date,
        "entry_time": entry_time,
        "entry_price": c3["close"],

        # Three-candle structure.
        "c1_open_time_utc": c1["open_time_utc"],
        "c1_open": c1["open"],
        "c1_high": c1["high"],
        "c1_low": c1["low"],
        "c1_close": c1["close"],
        "c1_body_length": c1_body_length,
        "c1_quote_volume": c1_quote_volume,
        "c2_open_time_utc": c2["open_time_utc"],
        "c2_open": c2["open"],
        "c2_high": c2["high"],
        "c2_low": c2["low"],
        "c2_close": c2["close"],
        "c2_body_length": c2_body_length,
        "c2_quote_volume": c2_quote_volume,
        "c3_open_time_utc": c3["open_time_utc"],
        "c3_open": c3["open"],
        "c3_high": c3["high"],
        "c3_low": c3["low"],
        "c3_close": c3["close"],
        "c3_quote_volume": c3.get("quote_asset_volume"),
        "c1_close_less_than_c2_close": float(c1["close"]) < float(c2["close"]),
        "c2_close_less_than_c3_close": float(c2["close"]) < float(c3["close"]),

        # Highest-close breakout check.
        "close_lookback_candles_total_including_c3": TRIGGER_2_CLOSE_LOOKBACK_CANDLES,
        "close_lookback_previous_candles_used": len(previous_39_rows),
        "highest_close_previous_39_candles": highest_previous_39_close,
        "c3_close_higher_than_previous_39_closes": (
            highest_previous_39_close is not None and float(c3["close"]) > float(highest_previous_39_close)
        ),
        "close_lookback_first_candle_open_time_utc": previous_39_rows[0]["open_time_utc"] if previous_39_rows else None,
        "close_lookback_last_candle_open_time_utc": previous_39_rows[-1]["open_time_utc"] if previous_39_rows else None,

        # Upper-shadow rejection check.
        "c3_body_length": c3_body_length,
        "c3_upper_shadow_length": c3_upper_shadow_length,
        "c3_upper_shadow_to_body_ratio": c3_upper_shadow_to_body_ratio,
        "c3_upper_shadow_to_body_min_ratio": TRIGGER_2_UPPER_SHADOW_BODY_RATIO_MIN,
        "c3_upper_shadow_pass": (
            c3_body_length > 0 and
            c3_upper_shadow_length >= TRIGGER_2_UPPER_SHADOW_BODY_RATIO_MIN * c3_body_length
        ),

        # Lower-shadow filter.
        "c3_lower_shadow_length": c3_lower_shadow_length,
        "c3_lower_shadow_to_body_ratio": c3_lower_shadow_to_body_ratio,
        "c3_lower_shadow_to_body_max_ratio": TRIGGER_2_LOWER_SHADOW_BODY_RATIO_MAX,
        "c3_lower_shadow_pass": (
            c3_body_length > 0 and
            c3_lower_shadow_length <= TRIGGER_2_LOWER_SHADOW_BODY_RATIO_MAX * c3_body_length
        ),

        # Volume confirmation for C1 and C2.
        "volume_confirmation_rule": (
            "C1 and C2 quote volume >= avg quote volume of previous max 40 candles before C1"
        ),
        "volume_lookback_candles_max": TRIGGER_2_VOLUME_LOOKBACK_CANDLES,
        "volume_lookback_candles_used": volume_stats["candles_used"],
        "avg_quote_volume_previous_max_40_before_c1": avg_previous_quote_volume,
        "c1_quote_volume_vs_avg_previous_max_40_ratio": c1_quote_volume_vs_avg_ratio,
        "c2_quote_volume_vs_avg_previous_max_40_ratio": c2_quote_volume_vs_avg_ratio,
        "c1_volume_confirmation_pass": (
            avg_previous_quote_volume is not None and float(c1_quote_volume) >= float(avg_previous_quote_volume)
        ),
        "c2_volume_confirmation_pass": (
            avg_previous_quote_volume is not None and float(c2_quote_volume) >= float(avg_previous_quote_volume)
        ),
        "volume_lookback_first_candle_open_time_utc": volume_stats["first_previous_candle_open_time_utc"],
        "volume_lookback_last_candle_open_time_utc": volume_stats["last_previous_candle_open_time_utc"],

        # C1/C2 body strength confirmation.
        "body_confirmation_rule": (
            "C1 and C2 body length > avg body length of previous max 40 candles before C1"
        ),
        "body_lookback_candles_max": TRIGGER_2_BODY_LOOKBACK_CANDLES,
        "body_lookback_candles_used": body_stats["candles_used"],
        "avg_body_previous_max_40_before_c1": avg_previous_body_length,
        "c1_body_vs_avg_previous_max_40_ratio": c1_body_vs_avg_ratio,
        "c2_body_vs_avg_previous_max_40_ratio": c2_body_vs_avg_ratio,
        "c1_body_confirmation_pass": (
            avg_previous_body_length is not None and c1_body_length > float(avg_previous_body_length)
        ),
        "c2_body_confirmation_pass": (
            avg_previous_body_length is not None and c2_body_length > float(avg_previous_body_length)
        ),
        "body_lookback_first_candle_open_time_utc": body_stats["first_previous_candle_open_time_utc"],
        "body_lookback_last_candle_open_time_utc": body_stats["last_previous_candle_open_time_utc"],

        # S3/debug metadata.
        "trigger_candle_source_s3_key": c3.get("source_s3_key"),
        "recent_activity_s3_key": get_state_snapshot()["latest_recent_key"],
        "trigger_checker_run_datetime_utc": discovery_dt_utc,
        "trigger_rule": (
            "C1.close < C2.close < C3.close; C3.close > max close of previous 39 candles; "
            "C3 body > 0; C3 upper shadow >= 2x body; C3 lower shadow <= 20% of body; "
            "C1 and C2 quote volume >= avg quote volume of previous max 40 candles before C1; "
            "C1 and C2 body length > avg body length of previous max 40 candles before C1"
        ),
    }

    row["trigger_id"] = make_trigger_2_id(row)

    return row


def detect_trigger_2_candidate(interval, symbol):
    rows = get_sorted_history_rows(interval, symbol)

    if len(rows) < TRIGGER_2_CLOSE_LOOKBACK_CANDLES:
        return None

    # Last 40 candles = previous 39 candles + latest closed candle C3.
    close_lookback_window = rows[-TRIGGER_2_CLOSE_LOOKBACK_CANDLES:]

    if not are_consecutive_candles(interval, close_lookback_window):
        return None

    c1, c2, c3 = rows[-3:]

    if not (float(c1["close"]) < float(c2["close"]) < float(c3["close"])):
        return None

    previous_39_rows = close_lookback_window[:-1]
    highest_previous_39_close = max(float(row["close"]) for row in previous_39_rows)

    if float(c3["close"]) <= highest_previous_39_close:
        return None

    c3_open = float(c3["open"])
    c3_close = float(c3["close"])
    c3_high = float(c3["high"])
    c3_low = float(c3["low"])

    c3_body_length = abs(c3_close - c3_open)
    c3_upper_shadow_length = c3_high - max(c3_open, c3_close)
    c3_lower_shadow_length = min(c3_open, c3_close) - c3_low

    # Body must be greater than zero, otherwise shadow/body is undefined.
    # This prevents zero-body candles from passing because 2 * body = 0.
    if c3_body_length <= 0:
        return None

    if c3_upper_shadow_length < TRIGGER_2_UPPER_SHADOW_BODY_RATIO_MIN * c3_body_length:
        return None

    if c3_lower_shadow_length > TRIGGER_2_LOWER_SHADOW_BODY_RATIO_MAX * c3_body_length:
        return None

    volume_stats = calculate_trigger_2_volume_stats(rows, c1)
    avg_previous_quote_volume = volume_stats["avg_quote_volume"]

    if avg_previous_quote_volume is None:
        return None

    c1_quote_volume = c1.get("quote_asset_volume")
    c2_quote_volume = c2.get("quote_asset_volume")

    if c1_quote_volume is None or c2_quote_volume is None:
        return None

    if pd.isna(c1_quote_volume) or pd.isna(c2_quote_volume):
        return None

    if float(c1_quote_volume) < float(avg_previous_quote_volume):
        return None

    if float(c2_quote_volume) < float(avg_previous_quote_volume):
        return None

    body_stats = calculate_trigger_2_body_stats(rows, c1)
    avg_previous_body_length = body_stats["avg_body_length"]

    if avg_previous_body_length is None:
        return None

    c1_body_length = abs(float(c1["close"]) - float(c1["open"]))
    c2_body_length = abs(float(c2["close"]) - float(c2["open"]))

    if avg_previous_body_length == 0:
        if c1_body_length <= 0 or c2_body_length <= 0:
            return None
    else:
        if c1_body_length <= TRIGGER_2_BODY_AVG_RATIO_MIN * float(avg_previous_body_length):
            return None
        if c2_body_length <= TRIGGER_2_BODY_AVG_RATIO_MIN * float(avg_previous_body_length):
            return None

    trigger_row = build_trigger_2_row(
        interval=interval,
        symbol=symbol,
        c1=c1,
        c2=c2,
        c3=c3,
        previous_39_rows=previous_39_rows,
        volume_stats=volume_stats,
        body_stats=body_stats,
        discovery_dt_utc=now_utc(),
    )

    trigger_id = trigger_row["trigger_id"]

    if trigger_id in processed_trigger_2_ids_by_interval[interval]:
        return None

    if trigger_id in sent_trigger_ids_by_interval[interval]:
        processed_trigger_2_ids_by_interval[interval].add(trigger_id)
        return None

    return trigger_row


def process_trigger_2_row(interval, trigger_row):
    print(
        f"New trigger 2: {trigger_row['symbol']} {interval}, "
        f"entry={trigger_row['entry_price']}, "
        f"entry_time={dt_to_iso(trigger_row.get('entry_time_utc'))}, "
        f"upper_shadow/body={trigger_row.get('c3_upper_shadow_to_body_ratio'):.2f}x, "
        f"lower_shadow/body={trigger_row.get('c3_lower_shadow_to_body_ratio'):.2f}x, "
        f"c1_body_vs_avg={trigger_row.get('c1_body_vs_avg_previous_max_40_ratio'):.2f}x, "
        f"c2_body_vs_avg={trigger_row.get('c2_body_vs_avg_previous_max_40_ratio'):.2f}x"
    )

    processed_trigger_2_ids_by_interval[interval].add(trigger_row["trigger_id"])

    save_trigger_2_row(interval, trigger_row)

    if SEND_TELEGRAM_ALERTS:
        send_alert_if_unsent(interval, trigger_row)


def process_trigger_row(interval, trigger_row):
    print(
        f"New trigger 1: {trigger_row['symbol']} {interval}, "
        f"entry={trigger_row['entry_price']}, "
        f"entry_time={dt_to_iso(trigger_row.get('entry_time_utc'))}, "
        f"swing_low={dt_to_iso(trigger_row['swing_low_open_time_utc'])}"
    )

    save_trigger_row(interval, trigger_row)

    if SEND_TELEGRAM_ALERTS:
        send_alert_if_unsent(interval, trigger_row)



def process_closed_candle(candle_row):
    interval = candle_row["interval"]
    symbol = candle_row["symbol"]

    if not is_candle_eligible_for_active_cycle(candle_row):
        if LOG_INELIGIBLE_CANDLES:
            print(
                f"Skipping ineligible candle: {symbol} {interval} "
                f"{dt_to_iso(candle_row['open_time_utc'])}"
            )
        return

    add_candle_to_history(candle_row)

    # First, check whether the current closed candle confirms a new 7-candle swing low.
    # If a new swing low appears, it supersedes all previous swing lows for
    # the same symbol + interval before trigger checking happens.
    swing_low_row = detect_latest_confirmed_swing_low(interval, symbol)

    if swing_low_row is not None:
        was_added, superseded_ids = replace_active_swing_low(
            interval=interval,
            symbol=symbol,
            swing_low_row=swing_low_row,
        )

        if was_added:
            depth_ratio = swing_low_row.get("swing_depth_vs_avg_body")
            depth_ratio_text = (
                f"{float(depth_ratio):.2f}x"
                if depth_ratio is not None
                else "N/A"
            )
            summary = (
                f"New swing low: {symbol} {interval}, "
                f"time={dt_to_iso(swing_low_row['swing_low_open_time_utc'])}, "
                f"price={swing_low_row['swing_low_price']}, "
                f"depth_vs_avg_body={depth_ratio_text}"
            )
            if superseded_ids:
                summary += f", superseded={len(superseded_ids)}"
            print(summary)

            save_swing_low_row(interval, swing_low_row)

    # Then check whether the current candle triggers the latest active swing low.
    # If this candle just confirmed a new swing low, it will not trigger that
    # same swing low because current_open_time == confirmation_candle_open_time.
    trigger_rows = check_current_candle_against_untriggered_swing_lows(
        interval=interval,
        symbol=symbol,
        current_candle=candle_row,
    )

    for trigger_row in trigger_rows:
        process_trigger_row(interval, trigger_row)

    # Trigger 2: latest 3-candle high-close rejection pattern.
    trigger_2_row = detect_trigger_2_candidate(
        interval=interval,
        symbol=symbol,
    )

    if trigger_2_row is not None:
        process_trigger_2_row(interval, trigger_2_row)


# ==================================================
# Catch-up / recovery from S3
# ==================================================

def load_existing_triggered_and_sent_ids(context):
    if not context["active_cycle_start_by_symbol"]:
        return

    min_active_start = min(context["active_cycle_start_by_symbol"].values())
    min_date = (min_active_start - timedelta(days=EXISTING_HISTORY_LOOKBACK_DAYS)).date()

    print("\nLoading existing trigger/sent history from date:", min_date)

    for interval in INTERVALS:
        trigger_1_files = get_existing_trigger_keys(
            bucket=S3_BUCKET,
            trigger_results_prefix=TRIGGER_RESULTS_PREFIX,
            interval=interval,
            min_trigger_date=min_date,
        )

        trigger_1_ids = set()
        swing_low_ids = set()

        for item in trigger_1_files:
            try:
                df = read_csv_from_s3(S3_BUCKET, item["key"])
                if "trigger_id" in df.columns:
                    trigger_1_ids.update(df["trigger_id"].dropna().astype(str).tolist())
                if "swing_low_id" in df.columns:
                    swing_low_ids.update(df["swing_low_id"].dropna().astype(str).tolist())
            except Exception as e:
                print(f"WARNING: Could not read trigger 1 file {item['key']}: {e}")

        triggered_swing_low_ids_by_interval[interval] = swing_low_ids

        trigger_2_files = get_existing_trigger_keys(
            bucket=S3_BUCKET,
            trigger_results_prefix=TRIGGER_2_RESULTS_PREFIX,
            interval=interval,
            min_trigger_date=min_date,
        )

        trigger_2_ids = set()

        for item in trigger_2_files:
            try:
                df = read_csv_from_s3(S3_BUCKET, item["key"])
                if "trigger_id" in df.columns:
                    trigger_2_ids.update(df["trigger_id"].dropna().astype(str).tolist())
            except Exception as e:
                print(f"WARNING: Could not read trigger 2 file {item['key']}: {e}")

        processed_trigger_2_ids_by_interval[interval] = trigger_2_ids

        sent_files = get_sent_alert_keys(
            bucket=S3_BUCKET,
            sent_prefix=TG_ALERTS_SENT_PREFIX,
            interval=interval,
            min_sent_date=min_date,
        )

        sent_trigger_ids = set()

        for item in sent_files:
            try:
                df = read_csv_from_s3(S3_BUCKET, item["key"])
                if "trigger_id" in df.columns:
                    sent_trigger_ids.update(df["trigger_id"].dropna().astype(str).tolist())
            except Exception as e:
                print(f"WARNING: Could not read sent alert file {item['key']}: {e}")

        sent_trigger_ids_by_interval[interval] = sent_trigger_ids

        print(
            f"Interval {interval}: existing trigger1 swing lows={len(swing_low_ids)}, "
            f"trigger2={len(trigger_2_ids)}, sent alerts={len(sent_trigger_ids)}"
        )


def reset_runtime_history_for_context(context):
    with history_lock:
        for interval in INTERVALS:
            candle_history[interval] = defaultdict(dict)
            swing_lows_by_interval_symbol[interval] = defaultdict(dict)


def load_candle_history_from_s3_for_context(context):
    """
    Loads the latest recent interval candle files from S3.

    This gives the engine enough 40-candle lookback context after restart without
    reading every historical candle file for the active cycle.
    """

    reset_runtime_history_for_context(context)

    symbols = set(context["symbols"])

    if not symbols:
        print("No active symbols. No candle history loaded.")
        return

    for interval in INTERVALS:
        first_valid_values = [
            value
            for symbol, value in context["first_valid_candle_by_interval_symbol"][interval].items()
            if symbol in symbols and value is not None
        ]

        if not first_valid_values:
            continue

        earliest_first_valid = min(first_valid_values)

        print(f"\nLoading S3 candle history for interval {interval} from {earliest_first_valid}")

        candle_keys = get_interval_candle_keys(
            bucket=S3_BUCKET,
            candles_prefix=CANDLES_PREFIX,
            interval=interval,
            min_candle_time=earliest_first_valid,
        )

        total_candle_keys = len(candle_keys)

        if STARTUP_S3_CANDLE_HISTORY_MAX_FILES_PER_INTERVAL is not None:
            candle_keys = candle_keys[-STARTUP_S3_CANDLE_HISTORY_MAX_FILES_PER_INTERVAL:]

        print(
            f"Candle files found for {interval}: {total_candle_keys}; "
            f"loading latest {len(candle_keys)}"
        )

        for item in candle_keys:
            try:
                df_file = read_csv_from_s3(S3_BUCKET, item["key"])
                if df_file.empty:
                    continue

                if "interval" not in df_file.columns:
                    df_file["interval"] = interval

                df_file["source_s3_key"] = item["key"]

                df_file = clean_candle_dataframe(df_file)

                df_file = df_file[
                    df_file["symbol"].isin(symbols)
                ].copy()

                if df_file.empty:
                    continue

                for _, row in df_file.iterrows():
                    symbol = str(row["symbol"])
                    open_time = parse_utc_datetime(row["open_time_utc"])
                    first_valid = context["first_valid_candle_by_interval_symbol"][interval].get(symbol)

                    if first_valid is None or open_time < first_valid:
                        continue

                    history_row = candle_row_to_history_row(row)
                    open_time_iso = dt_to_iso(history_row["open_time_utc"])
                    candle_history[interval][symbol][open_time_iso] = history_row

            except Exception as e:
                print(f"WARNING: Could not load candle file {item['key']}: {e}")

        for symbol in sorted(symbols):
            count = len(candle_history[interval][symbol])
            if count > 0:
                if LOG_SYMBOL_LISTS:
                    print(f"Loaded candles: {interval} {symbol}: {count}")


# ==================================================
# Startup recent candle backfill
# ==================================================

def floor_datetime_to_interval(dt, interval_minutes):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    floored_minute = (dt.minute // interval_minutes) * interval_minutes

    return dt.replace(
        minute=floored_minute,
        second=0,
        microsecond=0,
    )


def get_recent_completed_candle_open_times(interval, count, reference_dt_utc=None):
    if reference_dt_utc is None:
        reference_dt_utc = now_utc()

    interval_delta = timedelta(minutes=INTERVAL_MINUTES[interval])
    current_open = floor_datetime_to_interval(reference_dt_utc, INTERVAL_MINUTES[interval])
    last_completed_open = current_open - interval_delta

    return [
        last_completed_open - interval_delta * offset
        for offset in reversed(range(count))
    ]


def get_existing_symbols_for_candle_file(interval, candle_open_time_utc):
    key = make_candles_s3_key(interval, candle_open_time_utc)

    try:
        df_existing = read_csv_from_s3(S3_BUCKET, key)
    except Exception:
        return set(), 0, key

    if df_existing.empty or "symbol" not in df_existing.columns:
        return set(), 0, key

    existing_symbols = set(df_existing["symbol"].dropna().astype(str).unique().tolist())

    return existing_symbols, len(df_existing), key


def build_candle_row_from_rest_kline(interval, symbol, kline, context):
    open_time_utc = ms_to_utc_datetime(int(kline[0]))
    close_time_utc_raw = ms_to_utc_datetime(int(kline[6]))
    candle_close_time_utc = open_time_utc + timedelta(minutes=INTERVAL_MINUTES[interval])

    active_cycle_start = context["active_cycle_start_by_symbol"].get(symbol)
    first_valid = context["first_valid_candle_by_interval_symbol"].get(interval, {}).get(symbol)

    return {
        "interval": interval,
        "symbol": symbol,
        "open_time_utc": open_time_utc,
        "candle_close_time_utc": candle_close_time_utc,
        "close_time_utc_raw": close_time_utc_raw,
        "event_close_time_utc": None,
        "open": float(kline[1]),
        "high": float(kline[2]),
        "low": float(kline[3]),
        "close": float(kline[4]),
        "volume": float(kline[5]),
        "quote_asset_volume": float(kline[7]),
        "number_of_trades": int(kline[8]),
        "first_trade_id": None,
        "last_trade_id": None,
        "taker_buy_base_volume": float(kline[9]),
        "taker_buy_quote_volume": float(kline[10]),
        "is_closed": True,
        "source": "binance_rest_startup_backfill",
        "received_at_utc": now_utc(),
        "active_cycle_start_datetime_utc": active_cycle_start,
        "first_valid_candle_open_time_utc": first_valid,
        "activation_source_column": context["activation_source_column"],
        "recent_activity_s3_key": context["latest_recent_key"],
        "recent_activity_run_date": context["recent_run_date"],
        "recent_activity_run_time": context["recent_run_time"],
    }


def fetch_binance_klines_for_backfill(symbol, interval, start_open_time_utc, end_open_time_utc):
    """
    Fetches Binance klines for open times in [start_open_time_utc, end_open_time_utc].
    endTime is exclusive, so we add one interval.
    """
    interval_delta = timedelta(minutes=INTERVAL_MINUTES[interval])

    start_ms = int(parse_utc_datetime(start_open_time_utc).timestamp() * 1000)
    end_exclusive = parse_utc_datetime(end_open_time_utc) + interval_delta
    end_ms = int(end_exclusive.timestamp() * 1000)

    response = requests.get(
        BINANCE_REST_BASE + BINANCE_REST_KLINES_ENDPOINT,
        params={
            "symbol": symbol,
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 1000,
        },
        timeout=BINANCE_REST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()

    return response.json()


def save_backfilled_candles_and_update_history(interval, rows):
    if not rows:
        return 0

    df_rows = pd.DataFrame(rows)
    df_rows = clean_candle_dataframe(df_rows)

    saved_count = 0

    for open_time_utc, df_group in df_rows.groupby("open_time_utc"):
        open_dt = parse_utc_datetime(open_time_utc)
        output_key = make_candles_s3_key(interval, open_dt)

        if SAVE_CANDLES_TO_S3:
            append_dataframe_to_s3(
                df_new=df_group,
                bucket=S3_BUCKET,
                key=output_key,
                dedupe_subset=["interval", "symbol", "open_time_utc"],
                sort_cols=["symbol"],
            )

        for _, row in df_group.iterrows():
            add_candle_to_history(candle_row_to_history_row(row))
            saved_count += 1

    return saved_count


def backfill_recent_missing_or_incomplete_candles(context):
    """
    Repairs short gaps after restart.

    This function checks the latest expected completed candle folders in S3 for
    each interval. If a file is missing or contains too few active symbols, it
    fetches the missing symbol candles from Binance REST, saves them to the same
    S3 candle partition, and updates in-memory candle history.

    It intentionally does not run trigger detection or send Telegram alerts for
    backfilled candles. Its job is to repair context and prevent future trigger
    logic from seeing false candle gaps after WebSocket restarts.
    """
    symbols = sorted(context["symbols"])

    if not symbols:
        print("Startup backfill skipped: no active symbols.")
        return

    reference_dt_utc = now_utc()

    print(
        "\nStartup recent candle backfill check:",
        f"lookback={STARTUP_BACKFILL_LOOKBACK_CANDLES} completed candles per interval,",
        f"min completeness={STARTUP_BACKFILL_MIN_COMPLETENESS_RATIO:.0%}",
    )

    for interval in INTERVALS:
        expected_open_times = get_recent_completed_candle_open_times(
            interval=interval,
            count=STARTUP_BACKFILL_LOOKBACK_CANDLES,
            reference_dt_utc=reference_dt_utc,
        )

        missing_by_symbol = defaultdict(list)
        files_checked = 0
        files_needing_backfill = 0

        for candle_open_time in expected_open_times:
            eligible_symbols = []

            for symbol in symbols:
                first_valid = context["first_valid_candle_by_interval_symbol"].get(interval, {}).get(symbol)
                if first_valid is not None and candle_open_time >= first_valid:
                    eligible_symbols.append(symbol)

            if not eligible_symbols:
                continue

            files_checked += 1
            existing_symbols, existing_row_count, s3_key = get_existing_symbols_for_candle_file(
                interval=interval,
                candle_open_time_utc=candle_open_time,
            )

            required_min_rows = max(
                1,
                int(len(eligible_symbols) * STARTUP_BACKFILL_MIN_COMPLETENESS_RATIO),
            )

            missing_symbols = sorted(set(eligible_symbols) - existing_symbols)
            needs_backfill = (
                existing_row_count == 0
                or existing_row_count < required_min_rows
                or bool(missing_symbols)
            )

            if needs_backfill:
                files_needing_backfill += 1
                for symbol in missing_symbols:
                    missing_by_symbol[symbol].append(candle_open_time)

                print(
                    f"Backfill needed: {interval} {dt_to_iso(candle_open_time)}; "
                    f"existing_rows={existing_row_count}; eligible_symbols={len(eligible_symbols)}; "
                    f"missing_symbols={len(missing_symbols)}; s3://{S3_BUCKET}/{s3_key}"
                )

        if not missing_by_symbol:
            print(
                f"Startup backfill {interval}: checked {files_checked} files; "
                "no missing/incomplete recent candles found."
            )
            continue

        total_rest_rows = 0
        total_saved_rows = 0
        expected_times_set_by_symbol = {
            symbol: {dt_to_iso(value) for value in open_times}
            for symbol, open_times in missing_by_symbol.items()
        }

        for symbol, open_times in sorted(missing_by_symbol.items()):
            start_time = min(open_times)
            end_time = max(open_times)

            try:
                klines = fetch_binance_klines_for_backfill(
                    symbol=symbol,
                    interval=interval,
                    start_open_time_utc=start_time,
                    end_open_time_utc=end_time,
                )
            except Exception as e:
                print(f"WARNING: REST backfill failed: {symbol} {interval}: {e}")
                continue

            rows_to_save = []
            expected_times_for_symbol = expected_times_set_by_symbol[symbol]

            for kline in klines:
                open_time = ms_to_utc_datetime(int(kline[0]))
                if dt_to_iso(open_time) not in expected_times_for_symbol:
                    continue

                first_valid = context["first_valid_candle_by_interval_symbol"].get(interval, {}).get(symbol)
                if first_valid is not None and open_time < first_valid:
                    continue

                rows_to_save.append(
                    build_candle_row_from_rest_kline(
                        interval=interval,
                        symbol=symbol,
                        kline=kline,
                        context=context,
                    )
                )

            total_rest_rows += len(rows_to_save)
            total_saved_rows += save_backfilled_candles_and_update_history(interval, rows_to_save)

            if BINANCE_REST_SLEEP_SECONDS:
                time.sleep(BINANCE_REST_SLEEP_SECONDS)

        print(
            f"Startup backfill {interval}: checked {files_checked} files; "
            f"files needing backfill={files_needing_backfill}; "
            f"REST rows fetched/saved={total_rest_rows}/{total_saved_rows}."
        )


def rebuild_swing_lows_and_catch_up_triggers(context):
    """
    Optional warm-start recovery.

    Disabled by default for current testing. If enabled later, it keeps the same
    live rule: only the latest active swing low per symbol + interval can trigger,
    and swing lows are never built across candle gaps.
    """

    print("\nRebuilding swing lows and checking catch-up triggers from loaded history...")

    for interval in INTERVALS:
        for symbol in sorted(context["symbols"]):
            rows = get_sorted_history_rows(interval, symbol)

            if len(rows) < SWING_LOW_PATTERN_CANDLES:
                continue

            # Rebuild confirmed 7-candle swing lows from continuous history.
            for i in range(SWING_LOW_PATTERN_CANDLES - 1, len(rows)):
                window = rows[i - SWING_LOW_PATTERN_CANDLES + 1:i + 1]

                if not are_consecutive_candles(interval, window):
                    continue

                previous_rows_before_c1 = rows[:i - SWING_LOW_PATTERN_CANDLES + 1]

                is_swing_low, diagnostics = is_7_candle_swing_low_window(
                    window,
                    previous_rows_before_c1=previous_rows_before_c1,
                )

                if not is_swing_low:
                    continue

                swing_low_row = build_swing_low_row(
                    interval=interval,
                    window=window,
                    discovery_dt_utc=now_utc(),
                    latest_recent_key=context["latest_recent_key"],
                    diagnostics=diagnostics,
                )

                replace_active_swing_low(
                    interval=interval,
                    symbol=symbol,
                    swing_low_row=swing_low_row,
                )

            # Catch-up trigger check: only the latest active swing low can trigger.
            with history_lock:
                swing_low_rows = list(swing_lows_by_interval_symbol[interval][symbol].values())

            swing_low_rows = [
                row for row in swing_low_rows
                if row["swing_low_id"] not in triggered_swing_low_ids_by_interval[interval]
            ]

            if not swing_low_rows:
                continue

            swing_low_row = max(
                swing_low_rows,
                key=lambda x: parse_utc_datetime(x["swing_low_open_time_utc"]),
            )

            swing_low_id = swing_low_row["swing_low_id"]
            confirmation_time = parse_utc_datetime(swing_low_row["confirmation_candle_open_time_utc"])
            swing_low_price = float(swing_low_row["swing_low_price"])

            later_rows = [
                row for row in rows
                if parse_utc_datetime(row["open_time_utc"]) > confirmation_time
            ]

            for candidate in later_rows:
                if float(candidate["close"]) >= swing_low_price:
                    continue

                volume_pass, volume_stats = volume_confirmation_passes(rows, candidate)

                if not volume_pass:
                    continue

                trigger_row = build_trigger_row(
                    interval=interval,
                    swing_low_row=swing_low_row,
                    trigger_candle=candidate,
                    volume_stats=volume_stats,
                    discovery_dt_utc=now_utc(),
                )

                if trigger_row["trigger_id"] in sent_trigger_ids_by_interval[interval]:
                    triggered_swing_low_ids_by_interval[interval].add(swing_low_id)
                    break

                print("\nCatch-up trigger found")
                process_trigger_row(interval, trigger_row)
                triggered_swing_low_ids_by_interval[interval].add(swing_low_id)
                break

            swing_count = len(swing_lows_by_interval_symbol[interval][symbol])
            if swing_count > 0:
                if LOG_SYMBOL_LISTS:
                    print(f"Rebuilt active swing lows: {interval} {symbol}: {swing_count}")


# ==================================================
# Telegram
# ==================================================

def safe_float(value):
    try:
        return float(value)
    except Exception:
        return None


def format_price(value):
    value = safe_float(value)

    if value is None:
        return "N/A"

    if value >= 1:
        return f"{value:.4f}"
    elif value >= 0.01:
        return f"{value:.5f}"
    else:
        return f"{value:.8f}".rstrip("0").rstrip(".")


def format_utc_time(value):
    dt = parse_utc_datetime(value)
    if dt is None:
        return "N/A"
    return dt.strftime("%H:%M")


def format_utc_date(value):
    dt = parse_utc_datetime(value)
    if dt is None:
        return "N/A"
    return dt.strftime("%Y-%m-%d")


def build_trigger_alert(row):
    symbol = row.get("symbol", "N/A")
    interval = row.get("interval", "N/A")
    interval_label = get_interval_label(interval).replace("-Min", "-min")
    trigger_type = row.get("trigger_type")

    entry_price = format_price(row.get("entry_price"))

    entry_time_value = (
        row.get("entry_time_utc")
        or row.get("trigger_candle_close_time_utc")
        or row.get("trigger_candle_open_time_utc")
    )

    entry_time = format_utc_time(entry_time_value)
    entry_date = format_utc_date(entry_time_value)

    if trigger_type == TRIGGER_2_TYPE:
        upper_shadow_ratio = row.get("c3_upper_shadow_to_body_ratio")
        c1_volume_ratio = row.get("c1_quote_volume_vs_avg_previous_max_40_ratio")
        c2_volume_ratio = row.get("c2_quote_volume_vs_avg_previous_max_40_ratio")

        if upper_shadow_ratio is not None and not pd.isna(upper_shadow_ratio):
            upper_shadow_line = f"Upper shadow/body: {float(upper_shadow_ratio):.2f}x"
        else:
            upper_shadow_line = "Upper shadow/body: N/A"

        if c1_volume_ratio is not None and not pd.isna(c1_volume_ratio):
            c1_volume_line = f"C-1 volume vs avg: {float(c1_volume_ratio):.2f}x"
        else:
            c1_volume_line = "C-1 volume vs avg: N/A"

        if c2_volume_ratio is not None and not pd.isna(c2_volume_ratio):
            c2_volume_line = f"C-2 volume vs avg: {float(c2_volume_ratio):.2f}x"
        else:
            c2_volume_line = "C-2 volume vs avg: N/A"

        message = (
            f"🚨 {interval_label} candles\n"
            f"Coin: {symbol}\n"
            f"Trigger time UTC: {entry_time}\n"
            f"Trigger price: {entry_price}\n\n"
            f"{upper_shadow_line}\n"
            f"{c1_volume_line}\n"
            f"{c2_volume_line}\n"
            f"Date: {entry_date}"
        )

        return message

    swing_low_time = format_utc_time(row.get("swing_low_open_time_utc"))
    volume_ratio = row.get("trigger_quote_volume_vs_avg_previous_max_40_ratio")

    if volume_ratio is not None and not pd.isna(volume_ratio):
        volume_line = f"Volume vs Avg: {float(volume_ratio):.2f}x"
    else:
        volume_line = "Volume vs Avg: N/A"

    message = (
        f"🚨 {interval_label} candles\n"
        f"Coin: {symbol}\n"
        f"Trigger time UTC: {entry_time}\n"
        f"Trigger price: {entry_price}\n\n"
        f"{volume_line}\n"
        f"Swing low time UTC: {swing_low_time}\n"
        f"Date UTC: {entry_date}"
    )

    return message


def send_telegram_message(bot_token, chat_id, text):
    if not bot_token:
        raise ValueError("TELEGRAM_BOT_TOKEN is empty.")

    if not chat_id:
        raise ValueError("TELEGRAM_CHAT_ID is empty.")

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }

    response = requests.post(url, json=payload, timeout=20)

    if response.status_code != 200:
        print("Telegram status:", response.status_code)
        print(response.text)

    response.raise_for_status()

    return response.json()


def save_sent_alert_row(interval, sent_row):
    if not SAVE_SENT_ALERTS_TO_S3:
        return

    sent_dt = parse_utc_datetime(sent_row["sent_datetime_utc"])
    output_key = make_sent_alert_s3_key(interval, sent_dt)

    df_new = pd.DataFrame([sent_row])

    append_dataframe_to_s3(
        df_new=df_new,
        bucket=S3_BUCKET,
        key=output_key,
        dedupe_subset=["trigger_id"],
        sort_cols=["symbol", "entry_time_utc", "trigger_candle_open_time_utc"],
    )


def send_alert_if_unsent(interval, trigger_row):
    trigger_id = str(trigger_row["trigger_id"])

    if trigger_id in sent_trigger_ids_by_interval[interval]:
        print("Alert already sent, skipping:", trigger_id)
        return

    message = build_trigger_alert(trigger_row)

    if LOG_ALERT_PREVIEW:
        print("\nAlert preview:")
        print(message)

    telegram_message_id = None

    if SEND_TELEGRAM_ALERTS:
        telegram_response = send_telegram_message(
            bot_token=TELEGRAM_BOT_TOKEN,
            chat_id=TELEGRAM_CHAT_ID,
            text=message,
        )

        try:
            telegram_message_id = telegram_response.get("result", {}).get("message_id")
        except Exception:
            telegram_message_id = None

        print(
            f"Telegram alert sent: {trigger_row['symbol']} {interval}, "
            f"entry_time={dt_to_iso(trigger_row.get('entry_time_utc'))}"
        )

    sent_dt = now_utc()

    sent_row = {
        "trigger_id": trigger_id,
        "trigger_type": trigger_row.get("trigger_type"),
        "interval": interval,
        "symbol": trigger_row.get("symbol"),
        "entry_price": trigger_row.get("entry_price"),
        "entry_time_utc": trigger_row.get("entry_time_utc"),
        "trigger_candle_open_time_utc": trigger_row.get("trigger_candle_open_time_utc"),
        "trigger_candle_close_time_utc": trigger_row.get("trigger_candle_close_time_utc"),
        "swing_low_open_time_utc": trigger_row.get("swing_low_open_time_utc"),
        "telegram_chat_id": TELEGRAM_CHAT_ID,
        "telegram_message_id": telegram_message_id,
        "sent_datetime_utc": sent_dt,
        "alert_message": message,
    }

    save_sent_alert_row(interval, sent_row)

    sent_trigger_ids_by_interval[interval].add(trigger_id)


# ==================================================
# WebSocket callbacks
# ==================================================

def make_on_open(connection_id):
    def on_open(ws):
        global last_websocket_opened_at_utc

        with state_lock:
            last_websocket_opened_at_utc = now_utc()

        print("\nWebSocket connection opened.")
        print("Connection ID:", connection_id)
    return on_open


def make_on_message(connection_id):
    def on_message(ws, message):
        global closed_candle_messages_received
        global last_closed_candle_received_at_utc

        try:
            with state_lock:
                if connection_id != active_connection_id:
                    # Stale connection after reconnect.
                    return

            data = json.loads(message)
            payload = data.get("data", data)

            if payload.get("e") != "kline":
                return

            kline = payload.get("k", {})

            # Only process fully closed candles.
            if not kline.get("x"):
                return

            interval = str(kline.get("i"))
            symbol = str(kline.get("s"))

            if interval not in INTERVALS:
                return

            state_snapshot = get_state_snapshot()

            if symbol not in state_snapshot["symbols"]:
                return

            candle_row = build_candle_row_from_kline(
                kline=kline,
                event_time_ms=payload.get("E"),
            )

            closed_candle_messages_received += 1
            with state_lock:
                last_closed_candle_received_at_utc = now_utc()

            if LOG_CLOSED_CANDLES:
                print(
                    "\nClosed candle received:",
                    symbol,
                    interval,
                    dt_to_iso(candle_row["open_time_utc"]),
                    "close=",
                    candle_row["close"],
                )

            queue_candle_for_s3_save(candle_row)
            process_closed_candle(candle_row)

        except Exception as e:
            print("\nERROR in on_message:", e)
            traceback.print_exc()

    return on_message


def make_on_error(connection_id):
    def on_error(ws, error):
        global last_websocket_error

        with state_lock:
            last_websocket_error = str(error)

        print("\nWebSocket error.")
        print("Connection ID:", connection_id)
        print(error)
    return on_error


def make_on_close(connection_id):
    def on_close(ws, close_status_code, close_msg):
        global last_websocket_closed_at_utc

        with state_lock:
            last_websocket_closed_at_utc = now_utc()

        print("\nWebSocket closed.")
        print("Connection ID:", connection_id)
        print("Close status code:", close_status_code)
        print("Close message:", close_msg)
    return on_close


# ==================================================
# Connection management
# ==================================================

def start_websocket_connection(context):
    global current_ws_app
    global current_ws_thread
    global active_connection_id
    global last_websocket_started_at_utc
    global last_websocket_opened_at_utc
    global last_closed_candle_received_at_utc
    global last_websocket_error
    global last_websocket_closed_at_utc

    symbols = context["symbols"]

    if not symbols:
        print("No active symbols. WebSocket connection not started.")
        return

    update_current_state(context)

    ws_url = build_ws_url(symbols, INTERVALS)

    with state_lock:
        active_connection_id += 1
        connection_id = active_connection_id
        last_websocket_started_at_utc = now_utc()
        last_websocket_opened_at_utc = None
        last_closed_candle_received_at_utc = None
        last_websocket_error = None
        last_websocket_closed_at_utc = None

    stream_count = len(symbols) * len(INTERVALS)

    print("\nStarting WebSocket connection")
    print("Connection ID:", connection_id)
    print("Symbols:", len(symbols))
    print("Intervals:", INTERVALS)
    print("Streams:", stream_count)
    print(f"Latest recent activity: s3://{S3_BUCKET}/{context['latest_recent_key']}")

    ws_app = websocket.WebSocketApp(
        ws_url,
        on_open=make_on_open(connection_id),
        on_message=make_on_message(connection_id),
        on_error=make_on_error(connection_id),
        on_close=make_on_close(connection_id),
    )

    ws_thread = threading.Thread(
        target=ws_app.run_forever,
        kwargs={
            "ping_interval": 20,
            "ping_timeout": 10,
        },
        daemon=True,
    )

    with state_lock:
        current_ws_app = ws_app
        current_ws_thread = ws_thread

    ws_thread.start()


def stop_current_websocket(mark_stale=True):
    global active_connection_id

    with state_lock:
        ws_app = current_ws_app
        ws_thread = current_ws_thread

        if mark_stale:
            active_connection_id += 1

    if ws_app is not None:
        try:
            print("\nClosing current WebSocket connection...")
            ws_app.close()
        except Exception as e:
            print("Error closing WebSocket:", e)

    if ws_thread is not None and ws_thread.is_alive():
        ws_thread.join(timeout=10)


def prepare_context_for_live_processing(context):
    update_current_state(context)
    load_existing_triggered_and_sent_ids(context)

    # For live testing, start from fresh WebSocket candles.
    # Old S3 candle history/rebuild is optional and disabled by default to avoid
    # false swing lows across long restart/maintenance gaps.
    if LOAD_S3_CANDLE_HISTORY_ON_STARTUP:
        load_candle_history_from_s3_for_context(context)

        if STARTUP_BACKFILL_RECENT_CANDLES:
            backfill_recent_missing_or_incomplete_candles(context)

        if REBUILD_SWING_LOWS_AND_CATCH_UP_ON_STARTUP:
            rebuild_swing_lows_and_catch_up_triggers(context)
        else:
            print("\nS3 candle history loaded/backfilled for 40-candle lookbacks, but swing-low rebuild/catch-up is disabled.")
    else:
        reset_runtime_history_for_context(context)
        print("\nCold start mode: S3 candle history loading is disabled.")
        print("Swing lows/triggers will use only newly received closed candles.")



def refresh_recent_activity_and_reconnect_if_needed(last_context):
    try:
        new_context = load_latest_recent_activity_context()
    except Exception as e:
        print("\nERROR refreshing recent activity file:")
        print(e)
        traceback.print_exc()
        return last_context

    old_signature = context_signature(last_context)
    new_signature = context_signature(new_context)

    old_key = last_context["latest_recent_key"]
    new_key = new_context["latest_recent_key"]

    old_symbols = set(last_context["symbols"])
    new_symbols = set(new_context["symbols"])

    added_symbols = sorted(new_symbols - old_symbols)
    removed_symbols = sorted(old_symbols - new_symbols)

    if new_signature != old_signature:
        print("\nGeneral-filter active universe changed.")
        print("Old symbols:", len(old_symbols))
        print("New symbols:", len(new_symbols))
        print("Added symbols:", len(added_symbols))
        print("Removed symbols:", len(removed_symbols))
        if LOG_SYMBOL_LISTS:
            print("Added symbol list:", added_symbols if added_symbols else "None")
            print("Removed symbol list:", removed_symbols if removed_symbols else "None")
        print("Reloading active-cycle history and reconnecting streams.")

        stop_current_websocket(mark_stale=True)
        time.sleep(3)

        prepare_context_for_live_processing(new_context)
        start_websocket_connection(new_context)

        return new_context

    if new_key != old_key:
        print("\nGeneral filters file changed, but symbols/active cycles are the same.")
        print(f"Old file: s3://{S3_BUCKET}/{old_key}")
        print(f"New file: s3://{S3_BUCKET}/{new_key}")
        print("Keeping current WebSocket connection open and updating metadata only.")

        update_current_state(new_context)

        return new_context

    print("\nGeneral filters refresh checked. No changes.")
    return last_context





def exit_for_websocket_health(reason):
    """
    Exit the process intentionally when the WebSocket is no longer healthy.

    systemd has Restart=always for this service, so exiting is safer than
    keeping a Python process alive while no candles are being received/saved.
    """
    print("\nWEBSOCKET SELF-HEALING: unhealthy live engine detected.", flush=True)
    print("Reason:", reason, flush=True)
    print("Exiting process now so systemd can restart crypto-live-engine-triggers.", flush=True)
    os._exit(1)


def check_websocket_health_or_exit():
    if stop_event.is_set():
        return

    with state_lock:
        ws_thread = current_ws_thread
        connection_id = active_connection_id
        started_at = last_websocket_started_at_utc
        opened_at = last_websocket_opened_at_utc
        last_closed_candle_at = last_closed_candle_received_at_utc
        last_error = last_websocket_error
        closed_at = last_websocket_closed_at_utc

    if ws_thread is None:
        return

    now_dt = now_utc()

    if not ws_thread.is_alive():
        exit_for_websocket_health(
            f"WebSocket thread is not alive. connection_id={connection_id}; "
            f"last_error={last_error}; closed_at={dt_to_iso(closed_at)}"
        )

    heartbeat_start = opened_at or started_at

    if heartbeat_start is None:
        return

    if last_closed_candle_at is None:
        silence_seconds = (now_dt - heartbeat_start).total_seconds()
        if silence_seconds > WEBSOCKET_NO_CANDLE_TIMEOUT_SECONDS:
            exit_for_websocket_health(
                f"No closed candles received for {int(silence_seconds)} seconds "
                f"after WebSocket start/open. connection_id={connection_id}; "
                f"last_error={last_error}; closed_at={dt_to_iso(closed_at)}"
            )
        return

    silence_seconds = (now_dt - last_closed_candle_at).total_seconds()

    if silence_seconds > WEBSOCKET_NO_CANDLE_TIMEOUT_SECONDS:
        exit_for_websocket_health(
            f"No closed candles received for {int(silence_seconds)} seconds. "
            f"Last closed candle message at {dt_to_iso(last_closed_candle_at)}. "
            f"connection_id={connection_id}; last_error={last_error}; "
            f"closed_at={dt_to_iso(closed_at)}"
        )

def get_next_symbol_refresh_datetime_utc(after_dt_utc=None):
    """
    Returns the next scheduled general-filter refresh timestamp in UTC.

    The active symbol list is still loaded immediately on startup. This function
    controls only the recurring daily refresh after startup.
    """
    if after_dt_utc is None:
        after_dt_utc = now_utc()

    if after_dt_utc.tzinfo is None:
        after_dt_utc = after_dt_utc.replace(tzinfo=timezone.utc)
    else:
        after_dt_utc = after_dt_utc.astimezone(timezone.utc)

    refresh_dt = after_dt_utc.replace(
        hour=SYMBOL_REFRESH_HOUR_UTC,
        minute=SYMBOL_REFRESH_MINUTE_UTC,
        second=0,
        microsecond=0,
    )

    if refresh_dt <= after_dt_utc:
        refresh_dt = refresh_dt + timedelta(days=1)

    return refresh_dt


def wait_until_symbol_refresh_time(refresh_dt_utc):
    """
    Waits until the scheduled refresh time, but wakes up periodically so Ctrl+C
    and systemd stop requests are handled quickly.

    Returns True when refresh time is reached.
    Returns False when stop_event is set before refresh time.
    """
    while not stop_event.is_set():
        seconds_until_refresh = (refresh_dt_utc - now_utc()).total_seconds()

        if seconds_until_refresh <= 0:
            return True

        wait_seconds = min(
            SYMBOL_REFRESH_CHECK_SLEEP_SECONDS,
            WEBSOCKET_HEALTHCHECK_SECONDS,
            max(1, seconds_until_refresh),
        )
        stop_requested = stop_event.wait(wait_seconds)

        if stop_requested:
            return False

        check_websocket_health_or_exit()

    return False

# ==================================================
# Main
# ==================================================

def main():
    print("Starting crypto-live-engine-trigger-1-2-general-filters-all-lookbacks-40-s3-history")
    print("Intervals:", INTERVALS)
    print(
        "Daily symbol refresh UTC:",
        f"{SYMBOL_REFRESH_HOUR_UTC:02d}:{SYMBOL_REFRESH_MINUTE_UTC:02d}",
    )
    print("Candle S3 prefix:", CANDLES_PREFIX)
    print("Volume confirmation:", REQUIRE_TRIGGER_VOLUME_CONFIRMATION)
    print("Trigger 1 volume lookback max candles:", VOLUME_CONFIRMATION_MAX_CANDLES)
    print("Trigger 1 swing-depth filter: swing_depth >= 2 x avg body of previous max 40 candles before C1")
    print("Trigger 1 C4 close filter: C4.close must be lower than each of the previous 40 closes before C4; 40 previous candles required")
    print(
        "Trigger 2: 3 rising closes + 40-candle highest close + upper shadow/body >= 2x "
        f"+ lower shadow/body <= {TRIGGER_2_LOWER_SHADOW_BODY_RATIO_MAX:.2f}x"
    )
    print("Trigger 2 volume: C1 and C2 >= avg previous max 40 candles before C1")
    print("Trigger 2 body: C1 and C2 body > avg body of previous max 40 candles before C1")
    print("Load S3 candle history on startup:", LOAD_S3_CANDLE_HISTORY_ON_STARTUP)
    print("Startup S3 candle history max files per interval:", STARTUP_S3_CANDLE_HISTORY_MAX_FILES_PER_INTERVAL)
    print("Startup recent candle backfill:", STARTUP_BACKFILL_RECENT_CANDLES)
    print("Startup backfill lookback candles:", STARTUP_BACKFILL_LOOKBACK_CANDLES)
    print("Startup backfill min completeness ratio:", STARTUP_BACKFILL_MIN_COMPLETENESS_RATIO)
    print("Rebuild swing lows/catch-up on startup:", REBUILD_SWING_LOWS_AND_CATCH_UP_ON_STARTUP)
    print("WebSocket self-healing no-candle timeout seconds:", WEBSOCKET_NO_CANDLE_TIMEOUT_SECONDS)
    print("WebSocket healthcheck seconds:", WEBSOCKET_HEALTHCHECK_SECONDS)

    initial_context = load_latest_recent_activity_context()

    print("\nInitial general filters file:")
    print(f"s3://{S3_BUCKET}/{initial_context['latest_recent_key']}")
    print("Initial symbols:", len(initial_context["symbols"]))
    if LOG_SYMBOL_LISTS:
        print(sorted(initial_context["symbols"]))
    print("Activation source column:", initial_context["activation_source_column"])

    prepare_context_for_live_processing(initial_context)
    start_websocket_connection(initial_context)

    last_context = initial_context
    next_refresh_dt_utc = get_next_symbol_refresh_datetime_utc()
    print("Next scheduled general-filter refresh UTC:", next_refresh_dt_utc.isoformat())

    try:
        while not stop_event.is_set():
            refresh_time_reached = wait_until_symbol_refresh_time(next_refresh_dt_utc)

            if not refresh_time_reached:
                break

            print("\nScheduled daily refresh of general-filter symbols...")
            print("Refresh time UTC:", now_utc().isoformat())
            last_context = refresh_recent_activity_and_reconnect_if_needed(last_context)

            next_refresh_dt_utc = get_next_symbol_refresh_datetime_utc()
            print("Next scheduled general-filter refresh UTC:", next_refresh_dt_utc.isoformat())

    except KeyboardInterrupt:
        print("\nInterrupted manually. Closing WebSocket.")
        stop_event.set()
        stop_current_websocket(mark_stale=True)

    print("\ncrypto-live-engine-trigger-1-2-general-filters stopped.")


if __name__ == "__main__":
    main()
