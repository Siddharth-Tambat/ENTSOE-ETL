import re
from datetime import timedelta
import xml.etree.ElementTree as ET
import pandas as pd
from typing import Optional
import logging
logger = logging.getLogger(__name__)

from airflow.exceptions import AirflowException


# Helper Functions
def parse_iso_duration(resolution_str: str) -> int:
    """Parse ISO 8601 duration string and return resolution in minutes."""
    if not resolution_str:
        logger.error("Invalid resolution string.")
        raise AirflowException("Invalid resolution string.")
    
    resolution_str = resolution_str.strip().upper()
    if resolution_str == "PT15M":
        resolution = 15
    elif resolution_str == "PT60M":
        resolution = 60
    else:
        logger.error(f"Unsupported resolution: {resolution_str}")
        raise AirflowException(f"Unsupported resolution: {resolution_str}")
    
    return resolution


def expected_points(start_ts: pd.Timestamp, end_ts: pd.Timestamp, resolution_str: str) -> int:
    """Calculate expected number of points based on time interval and resolution."""
    if not start_ts or not end_ts or not resolution_str:
        logger.error("Invalid input parameters for expected_points calculation.")
        raise AirflowException("Invalid input parameters for expected_points calculation.")
    
    resolution = parse_iso_duration(resolution_str)
    
    delta = end_ts - start_ts
    total_minutes = int(delta.total_seconds() / 60)
    
    if total_minutes <= 0:
        logger.error("Invalid time interval start and end timestamps.")
        raise AirflowException("Invalid time interval start and end timestamps.")

    if total_minutes % resolution != 0:
        logger.error(f"Time interval {total_minutes} is not divisible by resolution {resolution}.")
        raise AirflowException(f"Time interval {total_minutes} is not divisible by resolution {resolution}.")
    
    expected_points = int(total_minutes // resolution)
    return expected_points


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Enforce schema on the DataFrame."""
    expected_columns = {
        "curve_type_code": "string",
        "area_domain": "string",
        "currency": "string",
        "price_unit": "string",
        "resolution": "string",
        "position": "Int32",
        "price": "numeric",
        "interval_start": "datetime64[ns, UTC]",
        "interval_end": "datetime64[ns, UTC]"
    }

    for col, dtype in expected_columns.items():
        if col not in df.columns:
            logger.error(f"Missing expected column: {col}")
            raise AirflowException(f"Missing expected column: {col}")
        try:
            if col == "price":
                df[col] = pd.to_numeric(df[col], errors='raise')
            else:
                df[col] = df[col].astype(dtype)
        except Exception as e:
            logger.error(f"Error converting column {col} to {dtype}: {e}")
            raise AirflowException(f"Error converting column {col} to {dtype}: {e}")

    return df


def deduplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate rows in the DataFrame based on primary key columns."""
    key_columns = ["area_domain", "resolution", "interval_start"]
    before_count = len(df)
    df = df.drop_duplicates(subset=key_columns, keep='first').reset_index(drop=True)
    after_count = len(df)
    if before_count != after_count:
        logger.info(f"Deduplicated {before_count - after_count} rows.")
    return df


def dq_check(df: pd.DataFrame) -> bool:
    """Perform data quality checks on the DataFrame."""
    if df.empty:
        logger.error("DataFrame is empty after parsing.")
        return False

    key_columns = ["area_domain", "resolution", "interval_start"]
    for col in key_columns:
        if df[col].isnull().any():
            logger.error(f"Null values found in primary key column: {col}")
            return False

    return True


# Main Parsing Function
def energy_prices_parser(xml) -> pd.DataFrame:
    """
    Parse ENTSOE A44 / day-ahead prices XML and return a DataFrame with one row per Point.
    """
    ns = ""
    try:
        root = ET.parse(xml).getroot()
        m = re.match(r'\{(.*)\}', root.tag)
        ns = m.group(1)
    except ET.ParseError as e:
        logger.error(f"Error in processing XML root ns, error: {e}")
        raise AirflowException(f"Error in processing XML root ns, error: {e}")

    namespaces = {"ns": ns}

    try:
        timeseries = root.findall(".//ns:TimeSeries", namespaces)
        if not timeseries:
            logger.error("No TimeSeries elements found in the XML document.")
            raise AirflowException("No TimeSeries elements found in the XML document.")
    except ET.ParseError as e:
        logger.error(f"Error in processing XML TimeSeries, error: {e}")
        raise AirflowException(f"Error in processing XML TimeSeries, error: {e}")
    
    rows = []
    for ts in timeseries:

        try:
            curve_type_code = ts.find(".//ns:curveType", namespaces).text
            area_domain = ts.find(".//ns:in_Domain.mRID", namespaces).text
            currency = ts.find(".//ns:currency_Unit.name", namespaces).text
            price_unit = ts.find(".//ns:price_Measure_Unit.name", namespaces).text
        except AttributeError as e:
            logger.error(f"Error in fetching metadata from TimeSeries, error: {e}")
            raise AirflowException(f"Error in fetching metadata from TimeSeries, error: {e}")
        
        try:
            period = ts.find(".//ns:Period", namespaces)
            ti = period.find(".//ns:timeInterval", namespaces)
            ti_start_text = ti.find(".//ns:start", namespaces).text
            ti_end_text = ti.find(".//ns:end", namespaces).text
            ti_start_ts = pd.to_datetime(ti_start_text, utc=True)
            ti_end_ts = pd.to_datetime(ti_end_text, utc=True)
            resolution = period.find(".//ns:resolution", namespaces).text
            points = period.findall(".//ns:Point", namespaces)
        except AttributeError as e:
            logger.error(f"Error in fetching Period data from TimeSeries, error: {e}")
            raise AirflowException(f"Error in fetching Period data from TimeSeries, error: {e}")

        expected_points_count = expected_points(ti_start_ts, ti_end_ts, resolution)

        if len(points) != expected_points_count:
            logger.error(f"Mismatch in points count: found {len(points)}, expected {expected_points_count}")
            raise AirflowException(f"Mismatch in points count: found {len(points)}, expected {expected_points_count}")
        
        delta = timedelta(minutes=parse_iso_duration(resolution))

        for point in points:
            position_text = point.find(".//ns:position", namespaces).text
            price_text = point.find(".//ns:price.amount", namespaces).text

            try:
                position = int(position_text)
                price = float(price_text)
            except (ValueError, TypeError) as e:
                logger.error(f"Error in parsing Point data, error: {e}")
                raise AirflowException(f"Error in parsing Point data, error: {e}")

            if position < 1 or price is None:
                logger.error(f"Invalid position or price value: position={position}, price={price}")
                raise AirflowException(f"Invalid position or price value: position={position}, price={price}")

            interval_start = ti_start_ts + delta * (position - 1)
            interval_end = interval_start + delta

            rows.append({
                "curve_type_code": curve_type_code,
                "area_domain": area_domain,
                "currency": currency,
                "price_unit": price_unit,
                "resolution": resolution,
                "position": position,
                "price": price,
                "interval_start": interval_start,
                "interval_end": interval_end
            })

    if not rows:
        logger.warning("No valid data points found in the XML document.")
        raise AirflowException("No valid data points found in the XML document.")
    
    df = pd.DataFrame(rows)
    df = enforce_schema(df)
    df = deduplicate_rows(df)

    if not dq_check(df):
        logger.error("Data quality checks failed.")
        raise AirflowException("Data quality checks failed.")
    
    return df
