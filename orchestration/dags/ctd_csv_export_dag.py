from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.CSV_FIle_Exporters.CTDDataExport import run_ctd_export


def _run_ctd_export_for_airflow(**context):
    cutoff_datetime = context["data_interval_end"]
    filename = "CTDExport_{0}.csv".format(cutoff_datetime.strftime("%Y%m%dT%H%M%S"))
    return run_ctd_export(cutoff_datetime=cutoff_datetime, filename=filename)


default_args = {
    "owner": "ihvn",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id=os.getenv("AIRFLOW_CTD_DAG_ID", "ctd_csv_export"),
    default_args=default_args,
    description="Run the CTD CSV export ETL on a schedule.",
    start_date=datetime(2026, 1, 1),
    schedule=os.getenv("AIRFLOW_CTD_SCHEDULE", "@daily"),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "ctd", "csv"],
) as dag:
    run_ctd_csv_export = PythonOperator(
        task_id="run_ctd_csv_export",
        python_callable=_run_ctd_export_for_airflow,
    )
