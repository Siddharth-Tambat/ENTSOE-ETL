import re
from datetime import timedelta
import xml.etree.ElementTree as ET
import pandas as pd


# Constants for mapping codes to descriptions
FLOW_DIRECTION_MAP = {
    "A01": "Up",
    "A02": "Down",
    "A03": "Symmetric"
}

CURVE_TYPE_MAP = {
    "A01": "Sequential fixed block",
    "A03": "Variable sized blocks"
}

PSR_TYPE_MAP = {
    "A03": "Resource object",
    "A04": "Generation",
    "A05": "Load"
}

BUSINESS_TYPE_MAP = {
    "B95": "procured capacity"
}

MARKET_AGREEMENT_TYPE_MAP = {
    "A13": "Hourly",
    "A01": "Daily",
    "A02": "Weekly",
    "A03": "Monthly",
    "A04": "Yearly",
    "A06": "Long term contract"
}

PROCESS_TYPE_MAP = {
    "A46": "Replacement reserve (RR)",
    "A47": "Manual frequency restoration reserve (mFRR)",
    "A51": "Automatic frequency restoration reserve (aFRR)",
    "A52": "Frequency containment reserve (FCR)"
}

# Helper functions
def _ns_q(ns, tag: str) -> str:
    """Return qualified tag name for ElementTree (handles default namespace)."""
    return f"{{{ns}}}{tag}" if ns else tag


def _parse_iso_duration_to_timedelta(res: str) -> timedelta:
    """
    Parse simple ISO 8601 durations like PT15M, PT60M, PT1H, PT30S.
    Returns datetime.timedelta.
    """
    if not res:
        return timedelta(minutes=0)
    m = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', res)
    if not m:
        raise ValueError(f"Unsupported resolution format: {res}")
    hours = int(m.group(1) or 0)
    minutes = int(m.group(2) or 0)
    seconds = int(m.group(3) or 0)
    return timedelta(hours=hours, minutes=minutes, seconds=seconds)


def _get_text(elem, ns, *tag_variants) -> str | None:
    """Return first non-none text for given tag variants (handles dot chars)."""
    for tag in tag_variants:
        found = elem.find(_ns_q(ns, tag))
        if found is not None and found.text is not None:
            return found.text.strip()
    return None


# Main Parsing Function
def parse_balancing_market_document(xml_bytes_or_str) -> pd.DataFrame:
    """
    Parse 17.1.B&C Volumes and Prices of Contracted Reserves API XML response 
    and return a pandas DataFrame with one row per Point (time interval).
    """
    try:
        root = ET.fromstring(xml_bytes_or_str)
    except ET.ParseError as e:
        print(f"Failed to parse XML, error: {e}")
        return pd.DataFrame()  # empty -> caller can handle

    # detect namespace (if any)
    ns = ''
    m = re.match(r'\{(.+)\}', root.tag)
    if m:
        ns = m.group(1)

    # common top-level metadata
    document_mrid = _get_text(root, ns, 'mRID')
    process_type_code = _get_text(root, ns, 'process.processType')
    created_datetime = _get_text(root, ns, 'createdDateTime')
    allocation_decision_dt = _get_text(root, ns, 'allocationDecision_DateAndOrTime.dateTime')
    top_area = _get_text(root, ns, 'area_Domain.mRID')

    time_series_elems = root.findall('.//' + _ns_q(ns, 'TimeSeries'))

    if not time_series_elems:
        print("No TimeSeries found in document (no data). document_mrid=%s", document_mrid)
        return pd.DataFrame()

    rows = []
    for ts in time_series_elems:
        # series-level metadata (use helper to handle tag naming variability)
        series_mrid = _get_text(ts, ns, 'mRID')
        business_type_code = _get_text(ts, ns, 'businessType')
        market_agreement_type_code = _get_text(ts, ns, 'type_MarketAgreement.type', 'type_MarketAgreement.Type')
        market_product = _get_text(ts, ns, 'standard_MarketProduct.marketProductType')
        psr_type_code = _get_text(ts, ns, 'mktPSRType.psrType')
        flow_direction_code = _get_text(ts, ns, 'flowDirection.direction')
        currency = _get_text(ts, ns, 'currency_Unit.name')
        quantity_unit = _get_text(ts, ns, 'quantity_Measure_Unit.name')
        curve_type_code = _get_text(ts, ns, 'curveType')

        period = ts.find(_ns_q(ns, 'Period'))
        if period is None:
            print("TimeSeries without Period; skipping series_mrid=%s", series_mrid)
            continue

        ti = period.find(_ns_q(ns, 'timeInterval'))
        if ti is None:
            print("Period without timeInterval; skipping series_mrid=%s", series_mrid)
            continue

        period_start_text = _get_text(ti, ns, 'start')
        period_end_text = _get_text(ti, ns, 'end')
        resolution_text = _get_text(period, ns, 'resolution')

        # parse period start as UTC timestamp
        try:
            period_start_ts = pd.to_datetime(period_start_text, utc=True)
        except Exception:
            print("Failed to parse period start: %s", period_start_text)
            continue

        # parse resolution
        try:
            delta = _parse_iso_duration_to_timedelta(resolution_text)
        except Exception:
            print("Failed to parse resolution '%s' for series %s", resolution_text, series_mrid)
            delta = timedelta(minutes=15)  # fallback to 15 min

        # iterate points
        point_elems = period.findall(_ns_q(ns, 'Point'))
        if not point_elems:
            print("No Points inside Period for series_mrid=%s", series_mrid)
            continue

        for p in point_elems:
            # position may be missing sometimes; if so we will attempt to enumerate
            pos_text = _get_text(p, ns, 'position')
            try:
                position = int(pos_text) if pos_text is not None else None
            except Exception:
                position = None
            if position is None:
                # fallback to index in list
                position = point_elems.index(p) + 1

            # Calculate interval start and end
            interval_start = period_start_ts + timedelta(seconds=delta.total_seconds() * (position - 1))
            interval_end = interval_start + delta

            # point-level values
            quantity_text = _get_text(p, ns, 'quantity')
            try:
                quantity = float(quantity_text) if quantity_text is not None else None
            except Exception:
                # sometimes quantity = '' or 'N/A'
                quantity = None

            procurement_price_text = _get_text(p, ns, 'procurement_Price.amount', 'procurement_Price.Amount')
            try:
                procurement_price = float(procurement_price_text) if procurement_price_text is not None else None
            except Exception:
                procurement_price = None

            imbalance_cat = _get_text(p, ns, 'imbalance_Price.category', 'imbalance_Price.Category')

            # map codes to descriptions
            flow_direction = FLOW_DIRECTION_MAP.get(flow_direction_code, None)
            curve_type = CURVE_TYPE_MAP.get(curve_type_code, None)
            psr_type = PSR_TYPE_MAP.get(psr_type_code, None)
            business_type = BUSINESS_TYPE_MAP.get(business_type_code, None)
            market_agreement_type = MARKET_AGREEMENT_TYPE_MAP.get(market_agreement_type_code, None)
            process_type = PROCESS_TYPE_MAP.get(process_type_code, None)

            # point-level values
            rows.append({
                "document_mrid": document_mrid,
                "process_type_code": process_type_code,
                "process_type": process_type,
                "created_datetime": created_datetime,
                "allocation_decision_datetime": allocation_decision_dt,
                "area_domain": top_area,
                "series_mrid": series_mrid,
                "business_type_code": business_type_code,
                "business_type": business_type,
                "market_agreement_type_code": market_agreement_type_code,
                "market_agreement_type": market_agreement_type,
                "market_product_type": market_product,
                "psr_type_code": psr_type_code,
                "psr_type": psr_type,
                "flow_direction_code": flow_direction_code,
                "flow_direction": flow_direction,
                "currency": currency,
                "quantity_unit": quantity_unit,
                "curve_type_code": curve_type_code,
                "curve_type": curve_type,
                "period_start": period_start_text,
                "period_end": period_end_text,
                "resolution": resolution_text,
                "position": position,
                "interval_start": interval_start,
                "interval_end": interval_end,
                "quantity": quantity,
                "procurement_price": procurement_price,
                "imbalance_category": imbalance_cat,
            })

    if not rows:
        print("Parsed document but got zero rows (no Points parsed).")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # normalize types
    df["interval_start"] = pd.to_datetime(df["interval_start"], utc=True)
    df["interval_end"] = pd.to_datetime(df["interval_end"], utc=True)
    # numeric columns
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["procurement_price"] = pd.to_numeric(df["procurement_price"], errors="coerce")

    return df.drop_duplicates()  # dedupe just in case
