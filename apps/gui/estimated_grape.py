from __future__ import annotations

import math
import re
from typing import Any
import unicodedata

from setting_estimates import calculate_setting_estimate, get_setting_estimate_definition


ESTIMATED_GRAPE_VALUE_VERSION = 1
REPLAY_DENOMINATOR = 7.30
REPLAY_PAYOUT = 3
GRAPE_PAYOUT = 8
CHERRY_PAYOUT = 2
BB_PAYOUT = 252
RB_PAYOUT = 96

CHERRY_DENOMINATORS_BY_SETTING = (
    (1.0, 36.36),
    (2.0, 35.92),
    (3.0, 36.00),
    (4.0, 36.35),
    (5.0, 35.92),
    (6.0, 35.73),
)


def is_aim_juggler_ex_machine(machine_name: Any) -> bool:
    normalized_name = _normalize_machine_name(machine_name)
    return "アイムジャグラーEX" in normalized_name


def calculate_estimated_grape_value(
    machine_name: str,
    row_values: dict[str, Any],
    *,
    setting_average: float | int | None = None,
) -> dict[str, float] | None:
    if not is_aim_juggler_ex_machine(machine_name):
        return None

    games_count = _read_number(row_values, "G数", "games_count")
    difference_value = _read_number(row_values, "差枚", "difference_value")
    bb_count = _read_number(row_values, "BB", "bb_count")
    rb_count = _read_number(row_values, "RB", "rb_count")
    if (
        games_count is None
        or games_count <= 0
        or difference_value is None
        or bb_count is None
        or rb_count is None
        or bb_count < 0
        or rb_count < 0
    ):
        return None

    if setting_average is None:
        definition = get_setting_estimate_definition(machine_name)
        setting_estimate = calculate_setting_estimate(definition, row_values) if definition else None
        setting_average = setting_estimate.get("average") if setting_estimate else None
    setting_average_number = _read_raw_number(setting_average)
    cherry_probability = _interpolate_setting_probability(setting_average_number)
    if cherry_probability is None:
        return None

    total_investment = games_count * 3
    total_bonus_payout = bb_count * BB_PAYOUT + rb_count * RB_PAYOUT
    total_small_payout = difference_value + total_investment - total_bonus_payout
    replay_payout = games_count * REPLAY_PAYOUT / REPLAY_DENOMINATOR
    cherry_payout = games_count * CHERRY_PAYOUT * cherry_probability
    grape_payout = total_small_payout - replay_payout - cherry_payout
    grape_count = grape_payout / GRAPE_PAYOUT
    if not math.isfinite(grape_count) or grape_count <= 0:
        return None

    grape_denominator = games_count / grape_count
    grape_probability = grape_count / games_count
    if (
        not math.isfinite(grape_denominator)
        or not math.isfinite(grape_probability)
        or grape_denominator <= 0
        or grape_probability <= 0
    ):
        return None

    return {
        "count": round(grape_count, 3),
        "denominator": round(grape_denominator, 4),
        "probability": round(grape_probability, 8),
    }


def _interpolate_setting_probability(setting_average: float | None) -> float | None:
    if setting_average is None or not math.isfinite(setting_average):
        return None

    rows = [(setting, 1 / denominator) for setting, denominator in CHERRY_DENOMINATORS_BY_SETTING]
    first_setting, first_probability = rows[0]
    last_setting, last_probability = rows[-1]
    if setting_average <= first_setting:
        return first_probability
    if setting_average >= last_setting:
        return last_probability

    for index in range(len(rows) - 1):
        left_setting, left_probability = rows[index]
        right_setting, right_probability = rows[index + 1]
        if setting_average < left_setting or setting_average > right_setting:
            continue
        setting_width = right_setting - left_setting
        if setting_width <= 0:
            return left_probability
        progress = (setting_average - left_setting) / setting_width
        return left_probability + (right_probability - left_probability) * progress

    return None


def _normalize_machine_name(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    return re.sub(r"[\s\u3000・･_-]+", "", text).upper()


def _read_number(row_values: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key not in row_values:
            continue
        number = _read_raw_number(row_values.get(key))
        if number is not None:
            return number
    return None


def _read_raw_number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(str(value).strip().replace(",", ""))
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None
