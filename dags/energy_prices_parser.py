import re
from datetime import timedelta
import xml.etree.ElementTree as ET
import pandas as pd
from typing import Optional


# Constants for mapping codes to descriptions
CURVE_TYPE_MAP = {
    "A01": "Sequential fixed block",
    "A03": "Variable sized blocks"
}

# --- helpers (same style as bnc_parser) ---
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


def _get_text(elem, ns, *tag_variants) -> Optional[str]:
    """Return first non-none text for given tag variants (handles dot chars)."""
    for tag in tag_variants:
        found = elem.find(_ns_q(ns, tag))
        if found is not None and found.text is not None:
            return found.text.strip()
    return None


# Main Parsing Function
def parse_energy_prices_document(xml_bytes_or_str) -> pd.DataFrame:
    """
    Parse ENTSOE A44 / day-ahead prices XML and return a DataFrame with one row per Point.
    """
    try:
        root = ET.fromstring(xml_bytes_or_str)
    except ET.ParseError as e:
        print(f"[energy_prices_parser] XML parse error: {e}")
        return pd.DataFrame()

    ns = ''
    m = re.match(r'\{(.+)\}', root.tag)
    if m:
        ns = m.group(1)

    document_mrid = _get_text(root, ns, 'mRID')
    created_datetime = _get_text(root, ns, 'createdDateTime')

    time_series_elems = root.findall('.//' + _ns_q(ns, 'TimeSeries'))
    if not time_series_elems:
        print("[energy_prices_parser] No TimeSeries found.")
        return pd.DataFrame()

    rows = []
    for ts in time_series_elems:
        series_mrid = _get_text(ts, ns, 'mRID')
        classification_sequence = _get_text(ts, ns, 'classificationSequence_AttributeInstanceComponent.position')
        contract_type = _get_text(ts, ns, 'contract_MarketAgreement.type', 'contract_MarketAgreement.Type')
        in_domain = _get_text(ts, ns, 'in_Domain.mRID', 'in_Domain')
        out_domain = _get_text(ts, ns, 'out_Domain.mRID', 'out_Domain')
        currency = _get_text(ts, ns, 'currency_Unit.name')
        curve_type_code = _get_text(ts, ns, 'curveType')
        price_unit = _get_text(ts, ns, 'price_Measure_Unit.name', 'price_Measure_Unit')
        period = ts.find(_ns_q(ns, 'Period'))
        if period is None:
            continue

        ti = period.find(_ns_q(ns, 'timeInterval'))
        if ti is None:
            continue

        period_start_text = _get_text(ti, ns, 'start')
        period_end_text = _get_text(ti, ns, 'end')
        resolution_text = _get_text(period, ns, 'resolution')
        if resolution_text is None:
            continue

        try:
            period_start_ts = pd.to_datetime(period_start_text, utc=True)
        except Exception:
            # if it fails, skip this series
            print(f"[energy_prices_parser] Failed to parse period start: {period_start_text}")
            continue

        try:
            delta = _parse_iso_duration_to_timedelta(resolution_text)
        except Exception:
            print(f"[energy_prices_parser] Failed to parse resolution '{resolution_text}', skipping series_mrid={series_mrid}")
            continue

        point_elems = period.findall(_ns_q(ns, 'Point'))
        if not point_elems:
            continue

        for p in point_elems:
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

            price_text = _get_text(p, ns, 'price.amount', 'price.Amount', 'price_amount')
            try:
                price = float(price_text) if price_text is not None else None
            except Exception:
                price = None
            
            curve_type = CURVE_TYPE_MAP.get(curve_type_code, None)

            # point-level values
            rows.append({
                "document_mrid": document_mrid,
                "created_datetime": created_datetime,
                "series_mrid": series_mrid,
                "classification_sequence": classification_sequence,
                "contract_type": contract_type,
                "curve_type_code": curve_type_code,
                "curve_type": curve_type,
                "area_domain": in_domain or out_domain,
                "in_domain": in_domain,
                "out_domain": out_domain,
                "currency": currency,
                "price_unit": price_unit,
                "resolution": resolution_text,
                "position": position,
                "price": price,
                "interval_start": interval_start,
                "interval_end": interval_end,
                "period_start": period_start_text,
                "period_end": period_end_text,
            })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # normalize types
    df["interval_start"] = pd.to_datetime(df["interval_start"], utc=True)
    df["interval_end"] = pd.to_datetime(df["interval_end"], utc=True)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    return df.drop_duplicates()  # dedupe just in case
