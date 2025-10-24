from airflow.decorators import dag, task
from airflow.models import Variable
#from airflow.sdk import get_current_context  # airflow >3
from airflow.operators.python import get_current_context  # airflow=2.7.1
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException

import logging
logger = logging.getLogger(__name__)
from typing import Optional
from datetime import datetime, timedelta, timezone


# Germany Region Domains
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
    max_active_runs=8,
    catchup=False,
    default_args={
        "owner": "termi",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    },
    tags=["entsoe", "day-ahead", "germany", "v2"],
)
def germany_energy_prices():

    @task(
            retries=3,
            retry_delay=timedelta(minutes=5),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=30)
    )
    def fetch_entsoe_data(fetch_date: str = None) -> dict:
        """
        Fetch ENTSO-E energy day-ahead prices for Germany for the given date (default: yesterday).
        """
        import requests

        if fetch_date is None:
            ctx = get_current_context()
            fetch_date = ctx.get("ds")  # YYYY-MM-DD
            if fetch_date is None:
                logger.info("fetch_date not found in current context, falling back to UTC yesterday")
                fetch_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

        dt = datetime.strptime(fetch_date, "%Y-%m-%d")
        period_start = dt.strftime("%Y%m%d0000")
        period_end = dt.strftime("%Y%m%d0000")

        BASE_URL = "https://web-api.tp.entsoe.eu/api"
        API_KEY = Variable.get("entsoe-api-key")

        results = []

        for domain in DE_BIDDING_ZONES:
            params = {
                "securityToken": API_KEY,
                "documentType": "A44",
                "periodStart": period_start,
                "periodEnd": period_end,
                "contract_MarketAgreement.type": "A01",  # "A01"=Day-ahead, "A07"=intraday
                "out_Domain": domain, 
                "in_Domain": domain, # must be same as out_Domain
            }
            resp = requests.get(BASE_URL, params=params, timeout=60)
            if resp.status_code == 200 and resp.text:
                results.append({
                    "xml": resp.text,
                    "domain": domain,
                })
                logger.info(f"✅ API Success for domain: {domain}, fetch_date: {fetch_date}")
            else:
                logger.error(f"❌ API Failed for domain: {domain}, fetch_date: {fetch_date}")
                logger.error(f"API Failed error: {resp.status_code} {resp.text}")
                raise AirflowException(f"API Failed error: {resp.status_code} {resp.text}")


        return {"results": results, "fetch_date": fetch_date}

    @task
    def process_data(fetch_result: dict) -> dict:
        """Process XML results into a single DataFrame."""
        import pandas as pd
        from parser.energy_prices_parser_v2 import energy_prices_parser as ep_parser

        results = fetch_result["results"]
        fetch_date = fetch_result["fetch_date"]
        final_df = pd.DataFrame()

        for item in results:
            xml = item["xml"]
            df = ep_parser(xml)
            if df.empty:
                logger.warning("⚠️ No data parsed from document for domain:", item.get("domain"))
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
            logger.warning("⚠️ No data to upload to Postgres.")
            return None

        from psycopg2.extras import execute_values

        pg_hook = PostgresHook(postgres_conn_id="azure_postgres")
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        table = "entsoe.public.energy_prices_v2"
        cols = list(df.columns)
        cols_sql = ", ".join(cols)

        # update all except PK columns (we'll use interval_start, area_domain, resolution as PK)
        excluded_cols = [c for c in cols if c not in ("area_domain", "resolution", "interval_start")]
        update_sql = ", ".join([f"{c} = EXCLUDED.{c}" for c in excluded_cols])

        sql = f"""
            INSERT INTO {table} ({cols_sql})
            VALUES %s
            ON CONFLICT (area_domain, resolution, interval_start)
            DO UPDATE SET {update_sql}, ingestion_time = now();
        """

        try:
            values = [tuple(x) for x in df.to_numpy()]
            execute_values(cur, sql, values, page_size=5000)
            conn.commit()
            logger.info(f"✅ Upserted {cur.rowcount} rows into {table}")

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Error upserting to Postgres: {e}")
            raise AirflowException(f"Error upserting to Postgres: {e}")

        finally:
            cur.close()
            conn.close()

        return None
    
    @task
    def upload_to_adls(process_result: dict) -> None:
        """
        Save the processed DataFrame as a Parquet file and upload
        to Azure Data Lake Storage in a partitioned path.
        """
        df = process_result["final_df"]
        fetch_date = process_result["fetch_date"]

        if df is None or df.empty:
            logger.warning("⚠️ No data to upload to ADLS.")
            return None
        
        import pyarrow as pa
        import pyarrow.parquet as pq
        import tempfile

        PARQUET_SCHEMA = pa.schema(
            [
                pa.field("curve_type_code", pa.string()),
                pa.field("area_domain", pa.string()),
                pa.field("currency", pa.string()),
                pa.field("price_unit", pa.string()),
                pa.field("resolution", pa.string()),
                pa.field("position", pa.int32()),
                pa.field("price", pa.float64()),
                pa.field("interval_start", pa.timestamp('ns', tz='UTC')),
                pa.field("interval_end", pa.timestamp('ns', tz='UTC'))
            ]
        )

        # Create the partitioned path for ADLS
        try:
            dt = datetime.strptime(fetch_date, "%Y-%m-%d")
            year, month, day = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")

            processed_container = "processedv2"
            blob_name = f"day-ahead-prices/year={year}/month={month}/day={day}/region=DE/data.parquet"

        except Exception as e:
            logger.error(f"❌ Error creating partitioned path: {e}")
            raise AirflowException(f"Error creating partitioned path: {e}")
        
        # Connect to ADLS
        try:
            wasb_hook = WasbHook(wasb_conn_id="azure_data_lake")
        except Exception as e:
            logger.error(f"❌ Error connecting to ADLS: {e}")
            raise AirflowException(f"Error connecting to ADLS: {e}")

        # Write Parquet to a temporary location -> ADLS
        with tempfile.NamedTemporaryFile(suffix=".parquet") as temp_file:
            
            try:
                table = pa.Table.from_pandas(
                    df, 
                    schema=PARQUET_SCHEMA, 
                    preserve_index=False
                )
                pq.write_table(table, temp_file.name, compression="SNAPPY")
                logger.info(f"✅ Written Parquet file locally at {temp_file.name}")
            except Exception as e:
                logger.error(f"❌ Error writing Parquet file: {e}")
                raise AirflowException(f"Error writing Parquet file: {e}")

            try:
                # Upload to ADLS container
                wasb_hook.load_file(
                    file_path=temp_file.name,
                    container_name=processed_container,
                    blob_name=blob_name,
                    overwrite=True
                )
                logger.info(f"✅ Uploaded data to ADLS at processed/{blob_name}")
            except Exception as e:
                logger.error(f"❌ Error uploading to ADLS at processed/{blob_name}: {e}")
                raise AirflowException(f"Error uploading to ADLS at processed/{blob_name}: {e}")

        return None
    

    # Orchestration
    fetch_result = fetch_entsoe_data()
    process_result = process_data(fetch_result)
    # Run in parallel
    upload_to_postgres(process_result)
    upload_to_adls(process_result)


# Instantiate the DAG
dag = germany_energy_prices()
