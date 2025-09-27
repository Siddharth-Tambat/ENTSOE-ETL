from airflow.decorators import dag, task
from airflow.models import Variable
#from airflow.sdk import get_current_context  # airflow >3
from airflow.operators.python import get_current_context  # airflow=2.7.1
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from psycopg2.extras import execute_values

from bnc_parser import parse_balancing_market_document


# Constants
BASE_URL = "https://web-api.tp.entsoe.eu/api"
API_KEY = Variable.get("entsoe-api-key")

# Germany control areas
DE_CONTROL_AREAS = [
    "10YDE-VE-------2",
    "10YDE-RWENET---I",
    "10YDE-EON------1",
    "10YDE-ENBW-----N",
    "10Y1001C--00002H",
    "10YCB-GERMANY--8",
    "10Y1001A1001A82H",
]

PROCESS_TYPES = {
    "A52": "FCR",
    "A51": "aFRR",
    "A47": "mFRR",
    "A46": "RR",
}

# DAG definition
@dag(
    dag_id="germany_bnc_reserves",
    description="Fetch ENTSO-E balancing reserves volumes and prices for Germany (all reserve types)",
    schedule="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    max_active_runs=5,
    catchup=False,
    default_args={
        "owner": "termi",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["entsoe", "balancing", "germany"],
)
def germany_bnc_reserves():

    @task
    def fetch_entsoe_data(fetch_date: str = None) -> dict:
        """Fetch balancing reserves data for each German control area and reserve type for yesterday."""

         # Prefer explicit fetch_date if passed, otherwise use the DAG run context 'ds'
        if fetch_date is None:
            ctx = get_current_context()
            # 'ds' is YYYY-MM-DD (logical date of the DAG run) — use that as the day to fetch
            fetch_date = ctx.get("ds")
            if fetch_date is None:
                # fallback to yesterday UTC (safe default if something is unexpectedly missing)
                fetch_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

        dt = datetime.strptime(fetch_date, "%Y-%m-%d")
        period_start = dt.strftime("%Y%m%d0000")
        period_end = dt.strftime("%Y%m%d2300")

        results = []

        for process_code, reserve_name in PROCESS_TYPES.items():
            for domain in DE_CONTROL_AREAS:
                params = {
                    "securityToken": API_KEY,
                    "documentType": "A81",
                    "businessType": "B95",
                    "processType": process_code,
                    "Type_MarketAgreement.Type": "A01",
                    "controlArea_Domain": domain,
                    "periodStart": period_start,
                    "periodEnd": period_end,
                }
                response = requests.get(BASE_URL, params=params, timeout=60)

                if response.status_code == 200:
                    print(f"✅ API Success for domain: {domain}, reserve: {reserve_name}, fetch_date {fetch_date}")
                    results.append({
                        "xml": response.text,
                        "reserve_type": reserve_name,
                        "domain": domain,
                    })
                else:
                    print( f"❌ API Failed for domain: {domain}, reserve: {reserve_name}, fetch_date: {fetch_date}")
                    print(f"error: ({response.status_code}) - {response.text}")

        return {"results": results, "fetch_date": fetch_date}

    @task
    def process_data(fetch_result: dict) -> dict:
        """Process XML results into a single DataFrame."""

        results = fetch_result["results"]
        fetch_date = fetch_result["fetch_date"]
        final_df = pd.DataFrame()

        for item in results:
            reserve = item["reserve_type"]
            domain = item["domain"]
            xml = item["xml"]

            df = parse_balancing_market_document(xml)
            if df.empty:
                print(f"⚠️ No data parsed for {domain}, {reserve}")
                continue

            final_df = pd.concat([final_df, df], ignore_index=True)

        return {"final_df": final_df, "fetch_date": fetch_date}

    @task
    def upload_to_postgres(process_result: dict) -> None:
        """
        Upsert parsed CSV into Postgres (table entsoe.public.germany_bnc_reserves).
        Assumes the table & indexes already exist.
        """
        df = process_result["final_df"]

        if df is None or df.empty:
            print("⚠️ No data to upload to Postgres.")
            return None

        pg_hook = PostgresHook(postgres_conn_id="azure_postgres")
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        table = "entsoe.public.germany_bnc_reserves"
        cols = list(df.columns)
        cols_sql = ", ".join(cols)

        # Exclude PK columns from update
        excluded_cols = [
            c for c in cols
            if c not in ("area_domain", "series_mrid", "interval_start", "process_type_code")
        ]
        update_sql = ", ".join([f"{c} = EXCLUDED.{c}" for c in excluded_cols])

        sql = f"""
            INSERT INTO {table} ({cols_sql})
            VALUES %s
            ON CONFLICT (area_domain, series_mrid, interval_start, process_type_code)
            DO UPDATE SET {update_sql}, ingestion_time = now();
        """

        try:
            values = [tuple(x) for x in df.to_numpy()]
            execute_values(cur, sql, values, page_size=5000)
            conn.commit()
            print(f"✅ Upserted {len(values)} rows into {table}")

        except Exception as e:
            conn.rollback()
            print(f"❌ Error during upsert: {e}")
            raise

        finally:
            cur.close()
            conn.close()

        return None

    @task
    def upload_to_adls(process_result: dict) -> str | None:
        """
        Save the processed DataFrame as a Parquet file and upload
        to Azure Data Lake Storage in a partitioned path.
        """

        df = process_result["final_df"]
        fetch_date = process_result["fetch_date"]

        if df is None or df.empty:
            print("⚠️ No data to upload to ADLS.")
            return None

        # Partition path: year=YYYY/month=MM/day=DD/region=DE/
        dt = datetime.strptime(fetch_date, "%Y-%m-%d")
        year, month, day = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")

        # Save Parquet locally
        load_path = f"/tmp/germany_bnc_reserves_{fetch_date}.parquet"
        df.to_parquet(load_path, engine="pyarrow", index=False)

        blob_name = (
            f"bnc_reserves/year={year}/month={month}/day={day}/region=DE/bnc_reserves.parquet"
        )

        # Upload to ADLS
        wasb_hook = WasbHook(wasb_conn_id="azure_data_lake")
        wasb_hook.load_file(
            file_path=load_path,
            container_name="processed",
            blob_name=blob_name,
            overwrite=True,
        )

        print(f"✅ Uploaded {load_path} → ADLS (processed/{blob_name})")
        
        return load_path
    
    @task
    def cleanup_local_file(load_path: str) -> None:
        """Delete the local CSV file after upload to ADLS"""

        if load_path and os.path.exists(load_path):
            os.remove(load_path)
            print(f"🗑️ Deleted local file {load_path}")

        return None

    # Orchestration
    fetch_result = fetch_entsoe_data()
    process_result = process_data(fetch_result)
    # Run in parallel
    upload_to_postgres(process_result)
    load_path = upload_to_adls(process_result)
    # Cleanup
    cleanup_local_file(load_path) << load_path

# Instantiate the DAG
dag = germany_bnc_reserves()
