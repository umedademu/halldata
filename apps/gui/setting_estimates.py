from __future__ import annotations

from functools import lru_cache
import json
import math
from pathlib import Path
import re
from typing import Any
import unicodedata


ROOT_DIR = Path(__file__).resolve().parents[2]
SETTING_ESTIMATES_PATH = ROOT_DIR / "config" / "setting_estimates.json"
SETTING_ESTIMATE_VALUE_VERSION = 1


@lru_cache(maxsize=1)
def load_setting_estimate_definitions() -> list[dict[str, Any]]:
    if not SETTING_ESTIMATES_PATH.exists():
        return []

    payload = json.loads(SETTING_ESTIMATES_PATH.read_text(encoding="utf-8"))
    definitions = payload.get("setting_estimates", [])
    if not isinstance(definitions, list):
        return []
    return [_build_definition(definition) for definition in definitions if isinstance(definition, dict)]


def get_setting_estimate_definition(machine_name: str) -> dict[str, Any] | None:
    normalized_machine_name = _normalize_machine_name(machine_name)
    if not normalized_machine_name:
        return None

    for definition in load_setting_estimate_definitions():
        for match_name in definition.get("normalized_match_names", []):
            if match_name and match_name in normalized_machine_name:
                return definition
    return None


def calculate_setting_estimate(
    definition: dict[str, Any] | None,
    record: dict[str, Any],
) -> dict[str, Any] | None:
    games = _read_int(record, "games_count", "G数")
    bb_count = _read_int(record, "bb_count", "BB")
    rb_count = _read_int(record, "rb_count", "RB")
    if (
        not definition
        or games is None
        or games <= 0
        or not _is_valid_count(bb_count, games)
        or not _is_valid_count(rb_count, games)
    ):
        return None

    setting_rates = definition.get("setting_rates", [])
    if not isinstance(setting_rates, list) or not setting_rates:
        return None

    log_rows: list[dict[str, Any]] = []
    for row in setting_rates:
        setting = _read_number(row.get("setting"))
        bb = _read_number(row.get("bb"))
        rb = _read_number(row.get("rb"))
        if setting is None or bb is None or rb is None:
            continue
        log_rows.append(
            {
                "setting": setting,
                "label": str(row.get("label") or ""),
                "log_value": _calculate_log_binomial_probability(bb_count, games, bb)
                + _calculate_log_binomial_probability(rb_count, games, rb),
            }
        )
    if not log_rows:
        return None

    max_log_value = max(row["log_value"] for row in log_rows)
    if not math.isfinite(max_log_value):
        return None

    weighted_rows = [
        {
            **row,
            "weight": math.exp(row["log_value"] - max_log_value),
        }
        for row in log_rows
    ]
    total_weight = sum(row["weight"] for row in weighted_rows)
    if not math.isfinite(total_weight) or total_weight <= 0:
        return None

    probabilities = [
        {
            "setting": row["setting"],
            "label": row["label"],
            "probability": row["weight"] / total_weight,
        }
        for row in weighted_rows
    ]
    average = sum(row["setting"] * row["probability"] for row in probabilities)
    return {
        "average": average,
        "probabilities": probabilities,
    }


def _build_definition(definition: dict[str, Any]) -> dict[str, Any]:
    settings = definition.get("settings", [])
    setting_rates: list[dict[str, Any]] = []
    if isinstance(settings, list):
        for row in settings:
            if not isinstance(row, dict):
                continue
            bb = _parse_rate_text(row.get("bbText"))
            rb = _parse_rate_text(row.get("rbText"))
            setting_rates.append(
                {
                    **row,
                    "bb": bb,
                    "rb": rb,
                    "combined": bb + rb,
                }
            )

    match_names = [definition.get("displayName"), *(definition.get("matchNames") or [])]
    return {
        **definition,
        "normalized_match_names": [_normalize_machine_name(value) for value in match_names],
        "setting_rates": setting_rates,
    }


def _parse_rate_text(value: Any) -> float:
    denominator = _read_number(str(value or "").replace("1/", ""))
    if denominator is None or denominator <= 0:
        return 0
    return 1 / denominator


def _normalize_machine_name(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    return re.sub(r"[\s\u3000・･_-]+", "", text).upper()


def _read_int(record: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        if key not in record:
            continue
        number = _read_number(record.get(key))
        if number is not None and float(number).is_integer():
            return int(number)
    return None


def _read_number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(str(value).strip().replace(",", ""))
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _is_valid_count(value: int | None, base: int) -> bool:
    return value is not None and value >= 0 and value <= base


def _calculate_log_binomial_probability(
    success_count: int,
    total_count: int,
    probability: float,
) -> float:
    if (
        total_count < 0
        or success_count < 0
        or success_count > total_count
        or probability < 0
        or probability > 1
    ):
        return -math.inf

    if total_count == 0:
        return 0 if success_count == 0 else -math.inf
    if probability == 0:
        return 0 if success_count == 0 else -math.inf
    if probability == 1:
        return 0 if success_count == total_count else -math.inf

    log_combination = (
        math.lgamma(total_count + 1)
        - math.lgamma(success_count + 1)
        - math.lgamma(total_count - success_count + 1)
    )
    return (
        log_combination
        + success_count * math.log(probability)
        + (total_count - success_count) * math.log1p(-probability)
    )
