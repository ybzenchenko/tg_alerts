import io
import json
import re
import time
import traceback
import requests
import pandas as pd
import boto3
from datetime import datetime, timezone

# --------------------------------------------------
# Config
# --------------------------------------------------

BASE_URL = "https://api.binance.com"

S3_BUCKET = "bin-tickers-yev"

# Combined final output prefix
GENERAL_FILTERS_PREFIX = "general-filters"

# Tabular monitoring/reporting logs prefix
FILTER_LOGS_PREFIX = "general-filters-logs"

QUOTE_ASSET = "USDT"

# Base filter thresholds
VOLUME_THRESHOLD_USDT = 1_000_000
RANGE_6H_THRESHOLD_PCT = 3
RANGE_12H_THRESHOLD_PCT = 6

# Recent activity filter thresholds
# Binance does not provide native 10m klines.
# We request 5m klines and aggregate every two completed 5m candles into one custom 10m candle.
KLINE_SOURCE_INTERVAL = "5m"
CUSTOM_CANDLE_INTERVAL = "10m"
CUSTOM_CANDLE_MINUTES = 10
SOURCE_CANDLE_MINUTES = 5
SOURCE_CANDLES_PER_CUSTOM_CANDLE = CUSTOM_CANDLE_MINUTES // SOURCE_CANDLE_MINUTES

PREVIOUS_CANDLES_COUNT = 12
AVG_10M_RANGE_THRESHOLD_PCT = 0.5

# Request extra 5m candles so we can safely exclude open/incomplete 10m windows
# and still keep the previous 12 completed custom 10m candles.
KLINE_LIMIT = (PREVIOUS_CANDLES_COUNT * SOURCE_CANDLES_PER_CUSTOM_CANDLE) + 6

REQUEST_SLEEP_SECONDS = 0.05

STABLE_OR_FIAT_BASE_ASSETS = {
    "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDP", "USDD",
    "EUR", "AEUR", "EURI", "TRY", "BRL", "GBP", "AUD", "JPY",
    "UAH", "RUB", "ZAR", "IDRT", "BIDR"
}

# Optional diagnostic comparison with the previous base-filter version.
# Previous version logic:
#   24h quoteVolume >= 800,000
#   AND (abs 24h price change >= 5% OR 24h high-low range >= 8%)
PRINT_COMPARISON_WITH_PREVIOUS_VERSION = True
PREVIOUS_VERSION_VOLUME_THRESHOLD_USDT = 800_000
PREVIOUS_VERSION_PRICE_CHANGE_THRESHOLD_PCT = 5
PREVIOUS_VERSION_24H_RANGE_THRESHOLD_PCT = 8


# --------------------------------------------------
# Helpers
# --------------------------------------------------

def now_utc():
    return datetime.now(timezone.utc)


def binance_get(path, params=None, timeout=20, print_error=True):
    """Quiet Binance GET helper."""
    url = BASE_URL + path

    response = requests.get(
        url,
        params=params,
        timeout=timeout
    )

    if response.status_code >= 400 and print_error:
        print(f"Binance request failed: {response.status_code} {response.reason}")
        if response.text:
            print("Response body:")
            print(response.text[:500])

    response.raise_for_status()

    if response.text:
        return response.json()

    return {}


def upload_dataframe_to_s3(df, bucket, key, print_location=True):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue()
    )

    if print_location:
        print(f"Saved: s3://{bucket}/{key}")


def read_csv_from_s3(bucket, key):
    s3_client = boto3.client("s3")

    response = s3_client.get_object(
        Bucket=bucket,
        Key=key
    )

    body = response["Body"].read().decode("utf-8")
    return pd.read_csv(io.StringIO(body))


def list_s3_keys(bucket, prefix):
    s3_client = boto3.client("s3")

    keys = []
    continuation_token = None

    while True:
        kwargs = {
            "Bucket": bucket,
            "Prefix": prefix
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


def get_latest_general_filters_key(bucket, general_filters_prefix, exclude_key=None):
    """
    Finds latest previous general_filters.csv based on partition values:

    general-filters/run_date=YYYY-MM-DD/run_time=HHMM/general_filters.csv
    """
    keys = list_s3_keys(bucket, general_filters_prefix)

    pattern = re.compile(
        rf"^{re.escape(general_filters_prefix)}/"
        r"run_date=(\d{4}-\d{2}-\d{2})/"
        r"run_time=(\d{4})/"
        r"general_filters\.csv$"
    )

    candidates = []

    for key in keys:
        if exclude_key is not None and key == exclude_key:
            continue

        match = pattern.match(key)
        if match:
            run_date = match.group(1)
            run_time = match.group(2)
            run_dt = datetime.strptime(
                f"{run_date} {run_time}",
                "%Y-%m-%d %H%M"
            ).replace(tzinfo=timezone.utc)

            candidates.append({
                "key": key,
                "run_date": run_date,
                "run_time": run_time,
                "run_dt": run_dt
            })

    if not candidates:
        return None

    return sorted(candidates, key=lambda x: x["run_dt"], reverse=True)[0]


def load_previous_active_cycle_context(bucket, general_filters_prefix, current_output_key):
    """
    Loads previous general_filters.csv and returns active-cycle context.

    Active-cycle rule:
    - If symbol was present in previous general_filters.csv and is still present now,
      keep its previous active_cycle_start_datetime_utc.
    - If symbol is new now, start a new active cycle now.
    - If symbol disappeared previously and later reappears, treat it as new.
    """
    latest_previous = get_latest_general_filters_key(
        bucket=bucket,
        general_filters_prefix=general_filters_prefix,
        exclude_key=current_output_key
    )

    if latest_previous is None:
        return {
            "previous_general_filters_s3_key": None,
            "previous_active_symbols": set(),
            "previous_cycle_start_by_symbol": {}
        }

    previous_key = latest_previous["key"]

    df_previous = read_csv_from_s3(bucket=bucket, key=previous_key)

    if df_previous.empty or "symbol" not in df_previous.columns:
        return {
            "previous_general_filters_s3_key": previous_key,
            "previous_active_symbols": set(),
            "previous_cycle_start_by_symbol": {}
        }

    df_previous = df_previous.dropna(subset=["symbol"]).copy()
    df_previous["symbol"] = df_previous["symbol"].astype(str)
    df_previous = df_previous.drop_duplicates("symbol", keep="last")

    previous_partition_datetime_utc = latest_previous["run_dt"].strftime("%Y-%m-%d %H:%M:%S%z")

    if "active_cycle_start_datetime_utc" in df_previous.columns:
        cycle_col = "active_cycle_start_datetime_utc"
    elif "general_filters_run_datetime_utc" in df_previous.columns:
        cycle_col = "general_filters_run_datetime_utc"
    elif "recent_activity_run_datetime_utc" in df_previous.columns:
        cycle_col = "recent_activity_run_datetime_utc"
    else:
        cycle_col = None

    previous_cycle_start_by_symbol = {}

    for _, row in df_previous.iterrows():
        symbol = row["symbol"]

        if cycle_col is not None and pd.notna(row.get(cycle_col)):
            cycle_start = str(row.get(cycle_col))
        else:
            cycle_start = previous_partition_datetime_utc

        previous_cycle_start_by_symbol[symbol] = cycle_start

    previous_active_symbols = set(previous_cycle_start_by_symbol.keys())

    return {
        "previous_general_filters_s3_key": previous_key,
        "previous_active_symbols": previous_active_symbols,
        "previous_cycle_start_by_symbol": previous_cycle_start_by_symbol
    }


def to_dataframe(api_response):
    """Binance can return either a list of tickers or a single dict."""
    if isinstance(api_response, list):
        return pd.DataFrame(api_response)
    if isinstance(api_response, dict):
        return pd.DataFrame([api_response])
    return pd.DataFrame()


def convert_ticker_numeric_columns(df):
    numeric_cols = [
        "priceChange", "priceChangePercent", "weightedAvgPrice",
        "prevClosePrice", "lastPrice", "lastQty", "bidPrice", "bidQty",
        "askPrice", "askQty", "openPrice", "highPrice", "lowPrice",
        "volume", "quoteVolume", "openTime", "closeTime", "firstId",
        "lastId", "count",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def add_high_low_range_pct(df, output_col_name):
    df[output_col_name] = (
        (df["highPrice"] - df["lowPrice"])
        / df["lowPrice"]
        * 100
    )
    return df


def chunk_list(values, batch_size):
    for i in range(0, len(values), batch_size):
        yield values[i:i + batch_size]


def format_symbols_param(symbol_batch):
    """
    Binance expects compact JSON. ensure_ascii=False supports Unicode symbols.
    """
    return json.dumps(symbol_batch, ensure_ascii=False, separators=(",", ":"))


def load_single_rolling_window_ticker(window_size, symbol):
    raw = binance_get(
        "/api/v3/ticker",
        params={
            "symbol": symbol,
            "windowSize": window_size,
            "type": "FULL",
        },
        print_error=False
    )
    return to_dataframe(raw)


def load_rolling_window_ticker(window_size, symbols, batch_size=50):
    symbols = [s for s in dict.fromkeys(symbols) if pd.notna(s)]

    if not symbols:
        return pd.DataFrame(columns=["symbol"]), {
            "window_size": window_size,
            "rows_loaded": 0,
            "failed_batches": 0,
            "skipped_symbols": 0,
        }

    all_batches = []
    skipped_symbols = []
    failed_batches = 0

    for symbol_batch in chunk_list(symbols, batch_size):
        try:
            raw = binance_get(
                "/api/v3/ticker",
                params={
                    "symbols": format_symbols_param(symbol_batch),
                    "windowSize": window_size,
                    "type": "FULL",
                },
                print_error=False
            )
            df_batch = to_dataframe(raw)
            if not df_batch.empty:
                all_batches.append(df_batch)

        except requests.exceptions.HTTPError:
            failed_batches += 1
            for symbol in symbol_batch:
                try:
                    df_single = load_single_rolling_window_ticker(window_size, symbol)
                    if not df_single.empty:
                        all_batches.append(df_single)
                except requests.exceptions.HTTPError:
                    skipped_symbols.append(symbol)

    if not all_batches:
        return pd.DataFrame(columns=["symbol"]), {
            "window_size": window_size,
            "rows_loaded": 0,
            "failed_batches": failed_batches,
            "skipped_symbols": len(skipped_symbols),
        }

    df = pd.concat(all_batches, ignore_index=True)
    df = convert_ticker_numeric_columns(df)
    df = add_high_low_range_pct(df, "high_low_range_pct")

    suffix = window_size.lower()

    keep_cols = [
        "symbol", "openPrice", "lastPrice", "highPrice", "lowPrice",
        "priceChangePercent", "high_low_range_pct", "volume", "quoteVolume",
        "count", "openTime", "closeTime",
    ]

    available_keep_cols = [col for col in keep_cols if col in df.columns]
    df = df[available_keep_cols].copy()

    rename_map = {
        col: f"{col}_{suffix}"
        for col in df.columns
        if col != "symbol"
    }
    df = df.rename(columns=rename_map)

    return df, {
        "window_size": window_size,
        "rows_loaded": len(df),
        "failed_batches": failed_batches,
        "skipped_symbols": len(skipped_symbols),
    }


def calculate_recent_10m_range(symbol):
    """
    Calculates average custom 10m candle range over previous 12 completed candles.
    Candle range % = (high - low) / open * 100
    """
    klines = binance_get(
        "/api/v3/klines",
        params={
            "symbol": symbol,
            "interval": KLINE_SOURCE_INTERVAL,
            "limit": KLINE_LIMIT
        },
        timeout=20
    )

    columns = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
    ]

    df = pd.DataFrame(klines, columns=columns)

    if df.empty:
        return None

    numeric_cols = [
        "open", "high", "low", "close", "volume", "quote_asset_volume",
        "number_of_trades", "taker_buy_base_volume", "taker_buy_quote_volume"
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)

    # Exclude current/open 5m candle if Binance returned one.
    now_ts_utc = pd.Timestamp.now(tz="UTC")
    df = df[df["close_time"].le(now_ts_utc)].copy()

    if df.empty:
        return None

    # Align 5m candles into UTC 10m buckets:
    # 10:00 + 10:05 -> 10:00 custom 10m candle
    # 10:10 + 10:15 -> 10:10 custom 10m candle
    df["custom_10m_open_time"] = df["open_time"].dt.floor("10min")

    df_10m = (
        df.groupby("custom_10m_open_time", as_index=False)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            quote_asset_volume=("quote_asset_volume", "sum"),
            number_of_trades=("number_of_trades", "sum"),
            taker_buy_base_volume=("taker_buy_base_volume", "sum"),
            taker_buy_quote_volume=("taker_buy_quote_volume", "sum"),
            first_source_open_time=("open_time", "min"),
            last_source_open_time=("open_time", "max"),
            close_time=("close_time", "max"),
            source_candles_count=("open_time", "count")
        )
        .rename(columns={"custom_10m_open_time": "open_time"})
    )

    # Keep only fully completed 10m candles made from exactly two adjacent 5m candles.
    df_10m = df_10m[
        (df_10m["source_candles_count"].eq(SOURCE_CANDLES_PER_CUSTOM_CANDLE)) &
        (
            (df_10m["last_source_open_time"] - df_10m["first_source_open_time"])
            .eq(pd.Timedelta(minutes=SOURCE_CANDLE_MINUTES))
        )
    ].copy()

    df_10m = df_10m.sort_values("open_time").reset_index(drop=True)

    if len(df_10m) < PREVIOUS_CANDLES_COUNT:
        return None

    previous_12 = df_10m.tail(PREVIOUS_CANDLES_COUNT).copy()

    previous_12["candle_range_pct"] = (
        (previous_12["high"] - previous_12["low"])
        / previous_12["open"]
        * 100
    )

    return {
        "symbol": symbol,
        "avg_10m_range_pct_prev_12": previous_12["candle_range_pct"].mean(),
        "avg_10m_quote_volume_prev_12": previous_12["quote_asset_volume"].mean(),
        "avg_10m_base_volume_prev_12": previous_12["volume"].mean(),
        "total_10m_quote_volume_prev_12": previous_12["quote_asset_volume"].sum(),
        "candles_used": len(previous_12),
        "first_candle_time_utc": previous_12["open_time"].min(),
        "last_candle_time_utc": previous_12["open_time"].max()
    }


def build_base_universe(run_datetime_utc, run_date, run_time, run_summary):
    exchange_info = binance_get("/api/v3/exchangeInfo")
    df_symbols = pd.DataFrame(exchange_info["symbols"])

    count_all_symbols = len(df_symbols)

    df_step_2 = df_symbols[df_symbols["quoteAsset"].eq(QUOTE_ASSET)].copy()
    df_step_3 = df_step_2[df_step_2["status"].eq("TRADING")].copy()
    df_step_4 = df_step_3[df_step_3["isSpotTradingAllowed"].eq(True)].copy()
    df_step_5 = df_step_4[~df_step_4["baseAsset"].isin(STABLE_OR_FIAT_BASE_ASSETS)].copy()

    ticker_24h = binance_get("/api/v3/ticker/24hr")
    df_24h = to_dataframe(ticker_24h)
    df_24h = convert_ticker_numeric_columns(df_24h)
    df_24h = add_high_low_range_pct(df_24h, "high_low_range_pct")
    df_24h["abs_price_change_pct"] = df_24h["priceChangePercent"].abs()

    df_merged = df_step_5.merge(
        df_24h[
            [
                "symbol", "openPrice", "lastPrice", "highPrice", "lowPrice",
                "priceChangePercent", "abs_price_change_pct", "high_low_range_pct",
                "volume", "quoteVolume", "count"
            ]
        ],
        on="symbol",
        how="left"
    )

    df_merged["high_low_range_pct_24h"] = df_merged["high_low_range_pct"]
    df_merged["quoteVolume_24h"] = df_merged["quoteVolume"]

    previous_version_count = None
    if PRINT_COMPARISON_WITH_PREVIOUS_VERSION:
        df_previous_version_reference = df_merged[
            df_merged["quoteVolume"].ge(PREVIOUS_VERSION_VOLUME_THRESHOLD_USDT) &
            (
                df_merged["abs_price_change_pct"].ge(PREVIOUS_VERSION_PRICE_CHANGE_THRESHOLD_PCT) |
                df_merged["high_low_range_pct_24h"].ge(PREVIOUS_VERSION_24H_RANGE_THRESHOLD_PCT)
            )
        ].copy()
        previous_version_count = len(df_previous_version_reference)

    df_step_7 = df_merged[df_merged["quoteVolume"].ge(VOLUME_THRESHOLD_USDT)].copy()

    symbols_for_rolling_stats = df_step_7["symbol"].dropna().tolist()

    df_6h, rolling_6h_stats = load_rolling_window_ticker("6h", symbols_for_rolling_stats)
    df_12h, rolling_12h_stats = load_rolling_window_ticker("12h", symbols_for_rolling_stats)

    df_step_8 = df_step_7.merge(df_6h, on="symbol", how="left")
    df_step_8 = df_step_8.merge(df_12h, on="symbol", how="left")

    df_step_8["passes_6h_range_condition"] = df_step_8["high_low_range_pct_6h"].ge(RANGE_6H_THRESHOLD_PCT)
    df_step_8["passes_12h_range_condition"] = df_step_8["high_low_range_pct_12h"].ge(RANGE_12H_THRESHOLD_PCT)
    df_step_8["passes_movement_condition"] = (
        df_step_8["passes_6h_range_condition"] |
        df_step_8["passes_12h_range_condition"]
    )

    df_base_universe = df_step_8[df_step_8["passes_movement_condition"]].copy()
    df_base_universe = df_base_universe.sort_values("quoteVolume", ascending=False).reset_index(drop=True)

    df_base_universe["base_filters_run_datetime_utc"] = run_datetime_utc
    df_base_universe["base_filters_run_date"] = run_date
    df_base_universe["base_filters_run_time"] = run_time
    df_base_universe["volume_threshold_usdt"] = VOLUME_THRESHOLD_USDT
    df_base_universe["range_6h_threshold_pct"] = RANGE_6H_THRESHOLD_PCT
    df_base_universe["range_12h_threshold_pct"] = RANGE_12H_THRESHOLD_PCT
    df_base_universe["movement_condition"] = (
        f"high_low_range_pct_6h >= {RANGE_6H_THRESHOLD_PCT} "
        f"OR high_low_range_pct_12h >= {RANGE_12H_THRESHOLD_PCT}"
    )

    run_summary.update({
        "all_symbols_count": count_all_symbols,
        "quote_asset_usdt_count": len(df_step_2),
        "trading_status_count": len(df_step_3),
        "spot_trading_allowed_count": len(df_step_4),
        "non_stable_fiat_count": len(df_step_5),
        "previous_version_logic_count": previous_version_count,
        "volume_24h_pass_count": len(df_step_7),
        "rolling_stats_symbols_count": len(symbols_for_rolling_stats),
        "rolling_6h_rows_loaded": rolling_6h_stats["rows_loaded"],
        "rolling_6h_failed_batches": rolling_6h_stats["failed_batches"],
        "rolling_6h_skipped_symbols": rolling_6h_stats["skipped_symbols"],
        "rolling_12h_rows_loaded": rolling_12h_stats["rows_loaded"],
        "rolling_12h_failed_batches": rolling_12h_stats["failed_batches"],
        "rolling_12h_skipped_symbols": rolling_12h_stats["skipped_symbols"],
        "base_movement_filter_count": len(df_base_universe),
    })

    return df_base_universe


def apply_recent_activity_filter(df_base_universe, run_datetime_utc, run_date, run_time, output_key, run_summary):
    symbols = df_base_universe["symbol"].dropna().drop_duplicates().tolist()

    range_results = []
    symbol_log_rows = []
    error_count = 0
    not_enough_candles_count = 0

    for index, symbol in enumerate(symbols, start=1):
        symbol_started_at = time.time()
        status = "ok"
        error_message = None
        stats = None

        try:
            stats = calculate_recent_10m_range(symbol)

            if stats is None:
                not_enough_candles_count += 1
                status = "not_enough_completed_10m_candles"
            else:
                range_results.append(stats)

        except Exception as exc:
            error_count += 1
            status = "error"
            error_message = str(exc)[:500]

        symbol_log_row = {
            "run_datetime_utc": run_datetime_utc,
            "run_date": run_date,
            "run_time": run_time,
            "symbol": symbol,
            "recent_activity_calc_status": status,
            "error_message": error_message,
            "elapsed_seconds": round(time.time() - symbol_started_at, 4),
        }

        if stats is not None:
            symbol_log_row.update({
                "avg_10m_range_pct_prev_12": stats.get("avg_10m_range_pct_prev_12"),
                "candles_used": stats.get("candles_used"),
                "first_candle_time_utc": stats.get("first_candle_time_utc"),
                "last_candle_time_utc": stats.get("last_candle_time_utc"),
            })

        symbol_log_rows.append(symbol_log_row)

        if REQUEST_SLEEP_SECONDS > 0 and index < len(symbols):
            time.sleep(REQUEST_SLEEP_SECONDS)

    df_ranges = pd.DataFrame(range_results)
    df_symbol_log = pd.DataFrame(symbol_log_rows)

    if df_ranges.empty:
        df_ranges = pd.DataFrame(columns=["symbol"])

    df_recent = df_base_universe.merge(df_ranges, on="symbol", how="left")

    df_general_filters = df_recent[
        df_recent["avg_10m_range_pct_prev_12"].ge(AVG_10M_RANGE_THRESHOLD_PCT)
    ].copy()

    df_general_filters = df_general_filters.sort_values(
        "avg_10m_range_pct_prev_12",
        ascending=False
    ).reset_index(drop=True)

    previous_context = load_previous_active_cycle_context(
        bucket=S3_BUCKET,
        general_filters_prefix=GENERAL_FILTERS_PREFIX,
        current_output_key=output_key
    )

    previous_active_symbols = previous_context["previous_active_symbols"]
    previous_cycle_start_by_symbol = previous_context["previous_cycle_start_by_symbol"]
    previous_general_filters_s3_key = previous_context["previous_general_filters_s3_key"]

    active_cycle_start_values = []
    active_cycle_status_values = []

    for symbol in df_general_filters["symbol"].astype(str):
        if symbol in previous_active_symbols:
            active_cycle_start_values.append(previous_cycle_start_by_symbol.get(symbol, run_datetime_utc))
            active_cycle_status_values.append("continued")
        else:
            active_cycle_start_values.append(run_datetime_utc)
            active_cycle_status_values.append("new")

    df_general_filters["active_cycle_start_datetime_utc"] = active_cycle_start_values
    df_general_filters["active_cycle_status"] = active_cycle_status_values
    df_general_filters["previous_general_filters_s3_key"] = previous_general_filters_s3_key

    # Backward-compatible columns for downstream/live-engine code that used recent_activity names.
    df_general_filters["recent_activity_run_datetime_utc"] = run_datetime_utc
    df_general_filters["recent_activity_run_date"] = run_date
    df_general_filters["recent_activity_run_time"] = run_time
    df_general_filters["previous_recent_activity_s3_key"] = previous_general_filters_s3_key

    # New combined run metadata.
    df_general_filters["general_filters_run_datetime_utc"] = run_datetime_utc
    df_general_filters["general_filters_run_date"] = run_date
    df_general_filters["general_filters_run_time"] = run_time

    df_general_filters["kline_source_interval"] = KLINE_SOURCE_INTERVAL
    df_general_filters["custom_candle_interval"] = CUSTOM_CANDLE_INTERVAL
    df_general_filters["previous_candles_count"] = PREVIOUS_CANDLES_COUNT
    df_general_filters["avg_10m_range_threshold_pct"] = AVG_10M_RANGE_THRESHOLD_PCT

    run_summary.update({
        "recent_activity_symbols_processed": len(symbols),
        "recent_activity_stats_calculated_count": len(df_ranges),
        "recent_activity_not_enough_candles_count": not_enough_candles_count,
        "recent_activity_error_count": error_count,
        "final_general_filters_count": len(df_general_filters),
        "active_cycle_new_count": int((df_general_filters.get("active_cycle_status") == "new").sum()) if not df_general_filters.empty else 0,
        "active_cycle_continued_count": int((df_general_filters.get("active_cycle_status") == "continued").sum()) if not df_general_filters.empty else 0,
        "previous_general_filters_s3_key": previous_general_filters_s3_key,
    })

    return df_general_filters, df_symbol_log


def save_logs(run_summary, df_symbol_log, summary_key, symbol_log_key):
    df_run_summary = pd.DataFrame([run_summary])

    upload_dataframe_to_s3(
        df=df_run_summary,
        bucket=S3_BUCKET,
        key=summary_key,
        print_location=True
    )

    upload_dataframe_to_s3(
        df=df_symbol_log,
        bucket=S3_BUCKET,
        key=symbol_log_key,
        print_location=True
    )


def main():
    script_started_at = time.time()

    run_dt_utc = now_utc()
    run_date = run_dt_utc.strftime("%Y-%m-%d")
    run_time = run_dt_utc.strftime("%H%M")
    run_datetime_utc = run_dt_utc.strftime("%Y-%m-%d %H:%M:%S%z")

    output_key = (
        f"{GENERAL_FILTERS_PREFIX}/"
        f"run_date={run_date}/"
        f"run_time={run_time}/"
        f"general_filters.csv"
    )

    summary_log_key = (
        f"{FILTER_LOGS_PREFIX}/"
        f"run_date={run_date}/"
        f"run_time={run_time}/"
        f"run_summary.csv"
    )

    symbol_log_key = (
        f"{FILTER_LOGS_PREFIX}/"
        f"run_date={run_date}/"
        f"run_time={run_time}/"
        f"symbol_processing_log.csv"
    )

    run_summary = {
        "run_datetime_utc": run_datetime_utc,
        "run_date": run_date,
        "run_time": run_time,
        "script_name": "crypto-general-filters-combined",
        "status": "started",
        "general_filters_s3_key": output_key,
        "run_summary_log_s3_key": summary_log_key,
        "symbol_processing_log_s3_key": symbol_log_key,
        "quote_asset": QUOTE_ASSET,
        "volume_threshold_usdt": VOLUME_THRESHOLD_USDT,
        "range_6h_threshold_pct": RANGE_6H_THRESHOLD_PCT,
        "range_12h_threshold_pct": RANGE_12H_THRESHOLD_PCT,
        "kline_source_interval": KLINE_SOURCE_INTERVAL,
        "custom_candle_interval": CUSTOM_CANDLE_INTERVAL,
        "previous_candles_count": PREVIOUS_CANDLES_COUNT,
        "avg_10m_range_threshold_pct": AVG_10M_RANGE_THRESHOLD_PCT,
    }

    df_symbol_log = pd.DataFrame()

    print(f"Run datetime UTC: {run_datetime_utc}")

    try:
        df_base_universe = build_base_universe(
            run_datetime_utc=run_datetime_utc,
            run_date=run_date,
            run_time=run_time,
            run_summary=run_summary,
        )

        print(f"Base universe count: {len(df_base_universe)}")

        df_general_filters, df_symbol_log = apply_recent_activity_filter(
            df_base_universe=df_base_universe,
            run_datetime_utc=run_datetime_utc,
            run_date=run_date,
            run_time=run_time,
            output_key=output_key,
            run_summary=run_summary,
        )

        print(f"Final general filters count: {len(df_general_filters)}")

        upload_dataframe_to_s3(
            df=df_general_filters,
            bucket=S3_BUCKET,
            key=output_key,
            print_location=True
        )

        run_summary["status"] = "success"

    except Exception as exc:
        run_summary["status"] = "failed"
        run_summary["error_type"] = type(exc).__name__
        run_summary["error_message"] = str(exc)[:1000]
        run_summary["traceback"] = traceback.format_exc()[:4000]
        print(f"Run failed: {type(exc).__name__}: {exc}")
        raise

    finally:
        run_summary["elapsed_seconds"] = round(time.time() - script_started_at, 3)

        # Save logs even if the run fails. If df_symbol_log is empty, still save a valid CSV.
        if df_symbol_log.empty:
            df_symbol_log = pd.DataFrame([
                {
                    "run_datetime_utc": run_datetime_utc,
                    "run_date": run_date,
                    "run_time": run_time,
                    "symbol": None,
                    "recent_activity_calc_status": "not_started_or_no_symbols",
                }
            ])

        save_logs(
            run_summary=run_summary,
            df_symbol_log=df_symbol_log,
            summary_key=summary_log_key,
            symbol_log_key=symbol_log_key,
        )

        print(f"Run status: {run_summary['status']}")
        print(f"Elapsed seconds: {run_summary['elapsed_seconds']}")


if __name__ == "__main__":
    main()
