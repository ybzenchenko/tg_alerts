from __future__ import annotations

import os
import subprocess
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_DIR = os.environ.get("TG_ALERTS_PROJECT_DIR", "/home/ubuntu/tg-alerts")
VENV_PYTHON = os.environ.get("TG_ALERTS_PYTHON", f"{PROJECT_DIR}/venv/bin/python")
GENERAL_FILTERS_SCRIPT = os.environ.get(
    "TG_ALERTS_GENERAL_FILTERS_SCRIPT",
    f"{PROJECT_DIR}/scripts/crypto-general-filters.py",
)


def run_python_script(script_path: str) -> None:
    """Run a project Python script and stream output into Airflow logs."""
    script = Path(script_path)
    if not script.exists():
        raise FileNotFoundError(f"Script does not exist: {script}")

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    process = subprocess.Popen(
        [VENV_PYTHON, str(script)],
        cwd=PROJECT_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    )

    assert process.stdout is not None
    for line in process.stdout:
        print(line, end="")

    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(f"Script failed with return code {return_code}: {script}")


with DAG(
    dag_id="tg_alerts_general_filters_daily",
    description="Run crypto general filters once per day at 01:00 UTC.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "tg-alerts",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["tg-alerts", "crypto", "filters"],
) as dag:
    general_filters = PythonOperator(
        task_id="crypto_general_filters",
        python_callable=run_python_script,
        op_kwargs={"script_path": GENERAL_FILTERS_SCRIPT},
    )
