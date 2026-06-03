import math
from typing import Any, Dict, Iterable, List, Optional, Tuple


RAW_COLUMNS = [
    "Dst Port", "Protocol", "Timestamp", "Flow Duration", "Tot Fwd Pkts",
    "Tot Bwd Pkts", "TotLen Fwd Pkts", "TotLen Bwd Pkts", "Fwd Pkt Len Max",
    "Fwd Pkt Len Min", "Fwd Pkt Len Mean", "Fwd Pkt Len Std", "Bwd Pkt Len Max",
    "Bwd Pkt Len Min", "Bwd Pkt Len Mean", "Bwd Pkt Len Std", "Flow Byts/s",
    "Flow Pkts/s", "Flow IAT Mean", "Flow IAT Std", "Flow IAT Max",
    "Flow IAT Min", "Fwd IAT Tot", "Fwd IAT Mean", "Fwd IAT Std",
    "Fwd IAT Max", "Fwd IAT Min", "Bwd IAT Tot", "Bwd IAT Mean",
    "Bwd IAT Std", "Bwd IAT Max", "Bwd IAT Min", "Fwd PSH Flags",
    "Bwd PSH Flags", "Fwd URG Flags", "Bwd URG Flags", "Fwd Header Len",
    "Bwd Header Len", "Fwd Pkts/s", "Bwd Pkts/s", "Pkt Len Min",
    "Pkt Len Max", "Pkt Len Mean", "Pkt Len Std", "Pkt Len Var",
    "FIN Flag Cnt", "SYN Flag Cnt", "RST Flag Cnt", "PSH Flag Cnt",
    "ACK Flag Cnt", "URG Flag Cnt", "CWE Flag Count", "ECE Flag Cnt",
    "Down/Up Ratio", "Pkt Size Avg", "Fwd Seg Size Avg", "Bwd Seg Size Avg",
    "Fwd Byts/b Avg", "Fwd Pkts/b Avg", "Fwd Blk Rate Avg", "Bwd Byts/b Avg",
    "Bwd Pkts/b Avg", "Bwd Blk Rate Avg", "Subflow Fwd Pkts",
    "Subflow Fwd Byts", "Subflow Bwd Pkts", "Subflow Bwd Byts",
    "Init Fwd Win Byts", "Init Bwd Win Byts", "Fwd Act Data Pkts",
    "Fwd Seg Size Min", "Active Mean", "Active Std", "Active Max",
    "Active Min", "Idle Mean", "Idle Std", "Idle Max", "Idle Min", "Label",
]

NUMERIC_COLUMNS = [column for column in RAW_COLUMNS if column not in {"Timestamp", "Label"}]
DERIVED_FEATURES = [
    "is_well_known_port",
    "is_ssh_port",
    "is_web_port",
    "pkt_ratio",
    "byte_ratio",
    "bytes_per_pkt",
]


def _to_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value or value.lower() in {"nan", "none", "null"}:
            return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _actual_label(row: Dict[str, Any]) -> Optional[str]:
    label = row.get("Label")
    if label is None:
        return None
    label = str(label).strip()
    if not label or label == "Label":
        return None
    return label


def build_feature_record(
    row: Dict[str, Any],
    feature_columns: Iterable[str],
) -> Tuple[Optional[Dict[str, float]], Dict[str, Any], Optional[Dict[str, Any]]]:
    numeric: Dict[str, float] = {}
    invalid_columns: List[str] = []

    for column in NUMERIC_COLUMNS:
        number = _to_number(row.get(column))
        if number is None:
            invalid_columns.append(column)
        else:
            numeric[column] = number

    metadata = {
        "event_time": row.get("Timestamp"),
        "actual_label": _actual_label(row),
        "source": {
            "dst_port": int(numeric["Dst Port"]) if "Dst Port" in numeric else None,
            "protocol": int(numeric["Protocol"]) if "Protocol" in numeric else None,
        },
    }

    if invalid_columns:
        return None, metadata, {
            "error": "missing_or_invalid_raw_columns",
            "missing_features": invalid_columns[:10],
        }

    numeric["is_well_known_port"] = 1.0 if numeric["Dst Port"] < 1024 else 0.0
    numeric["is_ssh_port"] = 1.0 if int(numeric["Dst Port"]) == 22 else 0.0
    numeric["is_web_port"] = 1.0 if int(numeric["Dst Port"]) in {80, 443} else 0.0
    numeric["pkt_ratio"] = numeric["Tot Fwd Pkts"] / (numeric["Tot Bwd Pkts"] + 1.0)
    numeric["byte_ratio"] = numeric["TotLen Fwd Pkts"] / (numeric["TotLen Bwd Pkts"] + 1.0)
    numeric["bytes_per_pkt"] = (
        numeric["TotLen Fwd Pkts"] + numeric["TotLen Bwd Pkts"]
    ) / (numeric["Tot Fwd Pkts"] + numeric["Tot Bwd Pkts"] + 1.0)

    feature_columns = list(feature_columns)
    missing_features = [column for column in feature_columns if column not in numeric]
    if missing_features:
        return None, metadata, {
            "error": "missing_or_invalid_feature",
            "missing_features": missing_features,
        }

    feature_record = {column: float(numeric[column]) for column in feature_columns}
    for column, value in feature_record.items():
        if not math.isfinite(value):
            return None, metadata, {
                "error": "non_finite_feature",
                "missing_features": [column],
            }

    return feature_record, metadata, None
