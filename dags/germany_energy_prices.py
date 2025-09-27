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

from energy_prices_parser import parse_energy_prices_document


# Constants
BASE_URL = "https://web-api.tp.entsoe.eu/api"
API_KEY = Variable.get("entsoe-api-key")

# Germany in out domains
DE_BIDDING_ZONES = [
    "10Y1001A1001A82H",
    "10Y1001A1001A63L",
    "10YDOM-CZ-DE-SKK"
]


# DAG definition
@dag(
    dag_id="germany_energy_prices",
    description="Fetch ENTSO-E Day-ahead prices (A44) for Germany domains and write to Postgres + ADLS",
    schedule="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    max_active_runs=5,
    catchup=False,
    default_args={
        "owner": "termi",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["entsoe", "day-ahead", "germany"],
)
def germany_energy_prices():

    @task
    def fetch_entsoe_data(fetch_date: str = None) -> dict:
        """
        Fetch ENTSO-E energy day-ahead prices for Germany for the given date (default: yesterday).
        """
        if fetch_date is None:
            ctx = get_current_context()
            fetch_date = ctx.get("ds")  # YYYY-MM-DD
            if fetch_date is None:
                fetch_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

        dt = datetime.strptime(fetch_date, "%Y-%m-%d")
        period_start = dt.strftime("%Y%m%d0000")
        period_end = dt.strftime("%Y%m%d0000")

        results = []

        for domain in DE_BIDDING_ZONES:
            params = {
                "securityToken": API_KEY,
                "documentType": "A44",
                "periodStart": period_start,
                "periodEnd": period_end,
                "contract_MarketAgreement.type": "A01",  # Day-ahead, "A07"=intraday
                "out_Domain": domain, 
                "in_Domain": domain, # must be same as out_Domain
            }
            resp = requests.get(BASE_URL, params=params, timeout=60)
            if resp.status_code == 200 and resp.text:
                results.append({
                    "xml": resp.text,
                    "domain": domain,
                })
                print(f"✅ API Success for domain: {domain}, fetch_date: {fetch_date}")
            else:
                print(f"❌ API Failed for domain: {domain}, fetch_date: {fetch_date}")
                print(f"error: {resp.status_code} {resp.text}")

        return {"results": results, "fetch_date": fetch_date}

    @task
    def process_data(fetch_result: dict) -> dict:
        """Process XML results into a single DataFrame."""

        results = fetch_result["results"]
        fetch_date = fetch_result["fetch_date"]
        final_df = pd.DataFrame()

        for item in results:
            xml = item["xml"]
            df = parse_energy_prices_document(xml)
            if df.empty:
                print("⚠️ No data parsed from document for domain:", item.get("domain"))
                continue
            
            final_df = pd.concat([final_df, df], ignore_index=True)

        return {"final_df": final_df, "fetch_date": fetch_date}

    @task
    def upload_to_postgres(process_result: dict) -> None:
        """
        Upsert parsed CSV into Postgres (table entsoe.public.germany_energy_prices).
        Assumes the table & indexes already exist.
        """
        df = process_result["final_df"]

        if df is None or df.empty:
            print("⚠️ No data to upload to Postgres.")
            return None

        pg_hook = PostgresHook(postgres_conn_id="azure_postgres")
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        table = "entsoe.public.germany_energy_prices"
        cols = list(df.columns)
        cols_sql = ", ".join(cols)

        # update all except PK columns (we'll use interval_start, area_domain, resolution as PK)
        excluded_cols = [c for c in cols if c not in ("area_domain", "interval_start", "resolution", "document_mrid")]
        update_sql = ", ".join([f"{c} = EXCLUDED.{c}" for c in excluded_cols])

        sql = f"""
            INSERT INTO {table} ({cols_sql})
            VALUES %s
            ON CONFLICT (area_domain, interval_start, resolution, document_mrid)
            DO UPDATE SET {update_sql}, ingestion_time = now();
        """

        try:
            values = [tuple(x) for x in df.to_numpy()]
            execute_values(cur, sql, values, page_size=5000)
            conn.commit()
            print(f"✅ Upserted {len(values)} rows into {table}")

        except Exception as e:
            conn.rollback()
            print(f"❌ Error upserting to Postgres: {e}")
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
        load_path = f"/tmp/germany_energy_prices_{fetch_date}.parquet"
        df.to_parquet(load_path, engine="pyarrow", index=False)

        blob_name = (
            f"day_ahead_prices/year={year}/month={month}/day={day}/region=DE/data.parquet"
        )

        # Upload to ADLS
        wasb_hook = WasbHook(wasb_conn_id="azure_data_lake")
        wasb_hook.load_file(
            file_path=load_path,
            container_name="processed",
            blob_name=blob_name,
            overwrite=True,
        )

        print(f"✅ Uploaded {load_path} → ADLS processed/{blob_name}")

        return load_path

    @task
    def cleanup_local_file(load_path: str) -> None:
        """Delete the local CSV file after upload to ADLS"""

        if load_path and os.path.exists(load_path):
            os.remove(load_path)
            print(f"🗑️ Deleted local {load_path}")
        
        return None

    # Orchestration
    fetch_result = fetch_entsoe_data()
    process_result = process_data(fetch_result)
    # Run in parallel
    upload_to_postgres(process_result)
    path = upload_to_adls(process_result)
    # Cleanup
    cleanup_local_file(path) << path

# Instantiate the DAG
dag = germany_energy_prices()
