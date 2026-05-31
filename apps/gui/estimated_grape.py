from __future__ import annotations

import math
import re
from typing import Any
import unicodedata

from setting_estimates import calculate_setting_estimate, get_setting_estimate_definition


ESTIMATED_GRAPE_VALUE_VERSION = 6
REPLAY_DENOMINATOR = 7.30
REPLAY_PAYOUT = 3
GRAPE_PAYOUT = 8
CHERRY_PAYOUT = 2
DEFAULT_MINREPO_ONE_BET_GAME_FACTOR = 0.725
MINREPO_CUMULATIVE_ONE_BET_GAME_FACTOR = 1 / 3
ONE_BET_GRAPE_DENOMINATOR = 10.3
ONE_BET_REPLAY_DENOMINATOR = 7.3
ONE_BET_GRAPE_PAYOUT = 8
ONE_BET_REPLAY_PAYOUT = 1

ESTIMATED_GRAPE_MACHINE_SPECS = (
    {
        "key": "aim-juggler-ex",
        "keywords": ("アイムジャグラーEX",),
        "bb_payout": 252,
        "rb_payout": 96,
        "post_announcement_bonus_ratio": 0.75,
        "minrepo_one_bet_game_factor": MINREPO_CUMULATIVE_ONE_BET_GAME_FACTOR,
        "cherry_denominators_by_setting": (
            (1.0, 36.36),
            (2.0, 35.92),
            (3.0, 36.00),
            (4.0, 36.35),
            (5.0, 35.92),
            (6.0, 35.73),
        ),
    },
    {
        "key": "gogo-juggler-3",
        "keywords": ("ゴーゴージャグラー3",),
        "bb_payout": 240,
        "rb_payout": 96,
        "post_announcement_bonus_ratio": 1.0,
        "minrepo_one_bet_game_factor": MINREPO_CUMULATIVE_ONE_BET_GAME_FACTOR,
        "cherry_denominators_by_setting": (
            (1.0, 33.40),
            (2.0, 33.30),
            (3.0, 33.20),
            (4.0, 33.10),
            (5.0, 32.90),
            (6.0, 32.80),
        ),
    },
    {
        "key": "funky-juggler-2",
        "keywords": ("ファンキージャグラー2",),
        "bb_payout": 240,
        "rb_payout": 96,
        "post_announcement_bonus_ratio": 0.75,
        "minrepo_one_bet_game_factor": 1.443,
        "cherry_denominators_by_setting": (
            (1.0, 35.83),
            (2.0, 35.46),
            (3.0, 36.27),
            (4.0, 35.68),
            (5.0, 35.64),
            (6.0, 36.02),
        ),
    },
    {
        "key": "juggler-girls",
        "keywords": ("ジャグラーガールズ",),
        "bb_payout": 240,
        "rb_payout": 96,
        "post_announcement_bonus_ratio": 0.75,
        "minrepo_one_bet_game_factor": -1.013,
        "cherry_denominators_by_setting": (
            (1.0, 33.56),
            (2.0, 33.47),
            (3.0, 33.32),
            (4.0, 33.15),
            (5.0, 33.10),
            (6.0, 32.97),
        ),
    },
    {
        "key": "my-juggler-v",
        "keywords": ("マイジャグラー",),
        "bb_payout": 240,
        "rb_payout": 96,
        "post_announcement_bonus_ratio": 0.75,
        "minrepo_one_bet_game_factor": DEFAULT_MINREPO_ONE_BET_GAME_FACTOR,
        "cherry_denominators_by_setting": (
            (1.0, 38.43),
            (2.0, 38.29),
            (3.0, 37.04),
            (4.0, 35.89),
            (5.0, 35.82),
            (6.0, 35.79),
        ),
    },
    {
        "key": "mr-juggler",
        "keywords": ("ミスタージャグラー",),
        "bb_payout": 240,
        "rb_payout": 96,
        "post_announcement_bonus_ratio": 0.75,
        "minrepo_one_bet_game_factor": DEFAULT_MINREPO_ONE_BET_GAME_FACTOR,
        "cherry_payout": 4,
        "cherry_acquisition_rate": 0.97,
        "bell_denominator": 655.36,
        "bell_payout": 14,
        "piero_denominator": 655.36,
        "piero_payout": 10,
        "high_setting_threshold": 3.5,
        "high_setting_bell_piero_acquisition_rate": 0.75,
        "low_setting_bell_piero_acquisition_rate": 0.0458817500129305,
        "cherry_denominators_by_setting": (
            (1.0, 37.24),
            (2.0, 37.24),
            (3.0, 37.24),
            (4.0, 37.24),
            (5.0, 37.24),
            (6.0, 37.24),
        ),
    },
)


def find_estimated_grape_machine_spec(machine_name: Any) -> dict[str, Any] | None:
    normalized_name = _normalize_machine_name(machine_name)
    if not normalized_name:
        return None

    for spec in ESTIMATED_GRAPE_MACHINE_SPECS:
        for keyword in spec["keywords"]:
            if _normalize_machine_name(keyword) in normalized_name:
                return spec
    return None


def is_estimated_grape_machine(machine_name: Any) -> bool:
    return find_estimated_grape_machine_spec(machine_name) is not None


def is_aim_juggler_ex_machine(machine_name: Any) -> bool:
    spec = find_estimated_grape_machine_spec(machine_name)
    return spec is not None and spec["key"] == "aim-juggler-ex"


def calculate_estimated_grape_value(
    machine_name: str,
    row_values: dict[str, Any],
    *,
    setting_average: float | int | None = None,
) -> dict[str, float] | None:
    machine_spec = find_estimated_grape_machine_spec(machine_name)
    if machine_spec is None:
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
    cherry_probability = _interpolate_setting_probability(
        setting_average_number,
        machine_spec["cherry_denominators_by_setting"],
    )
    if cherry_probability is None:
        return None

    post_announcement_bonus_count = (bb_count + rb_count) * machine_spec["post_announcement_bonus_ratio"]
    one_bet_games = _calculate_one_bet_games(
        bb_count + rb_count,
        machine_spec["post_announcement_bonus_ratio"],
    )
    minrepo_one_bet_games = one_bet_games * machine_spec.get(
        "minrepo_one_bet_game_factor",
        DEFAULT_MINREPO_ONE_BET_GAME_FACTOR,
    )
    normal_games_count = games_count - minrepo_one_bet_games
    if not math.isfinite(normal_games_count) or normal_games_count <= 0:
        return None

    corrected_difference_value = difference_value - (post_announcement_bonus_count / ONE_BET_REPLAY_DENOMINATOR)
    total_investment = (normal_games_count * 3) + one_bet_games
    total_bonus_payout = bb_count * machine_spec["bb_payout"] + rb_count * machine_spec["rb_payout"]
    total_small_payout = corrected_difference_value + total_investment - total_bonus_payout
    replay_payout = normal_games_count * REPLAY_PAYOUT / REPLAY_DENOMINATOR
    cherry_payout = (
        normal_games_count
        * machine_spec.get("cherry_payout", CHERRY_PAYOUT)
        * cherry_probability
        * machine_spec.get("cherry_acquisition_rate", 1.0)
    )
    extra_normal_small_payout = _calculate_extra_normal_small_payout(
        machine_spec,
        normal_games_count,
        setting_average_number,
    )
    one_bet_grape_payout = one_bet_games * ONE_BET_GRAPE_PAYOUT / ONE_BET_GRAPE_DENOMINATOR
    one_bet_replay_payout = one_bet_games * ONE_BET_REPLAY_PAYOUT / ONE_BET_REPLAY_DENOMINATOR
    grape_payout = (
        total_small_payout
        - replay_payout
        - cherry_payout
        - extra_normal_small_payout
        - one_bet_grape_payout
        - one_bet_replay_payout
    )
    grape_count = grape_payout / GRAPE_PAYOUT
    if not math.isfinite(grape_count) or grape_count <= 0:
        return None

    grape_denominator = normal_games_count / grape_count
    grape_probability = grape_count / normal_games_count
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


def _calculate_one_bet_games(bonus_count: float, post_announcement_bonus_ratio: float) -> float:
    if bonus_count <= 0:
        return 0.0

    one_bet_start_count = bonus_count * post_announcement_bonus_ratio
    settle_probability = 1 - (1 / ONE_BET_GRAPE_DENOMINATOR) - (1 / ONE_BET_REPLAY_DENOMINATOR)
    if settle_probability <= 0:
        return 0.0
    return one_bet_start_count / settle_probability


def _calculate_extra_normal_small_payout(
    machine_spec: dict[str, Any],
    normal_games_count: float,
    setting_average: float | None,
) -> float:
    bell_denominator = _read_raw_number(machine_spec.get("bell_denominator"))
    bell_payout = _read_raw_number(machine_spec.get("bell_payout"))
    piero_denominator = _read_raw_number(machine_spec.get("piero_denominator"))
    piero_payout = _read_raw_number(machine_spec.get("piero_payout"))
    if (
        bell_denominator is None
        or bell_denominator <= 0
        or bell_payout is None
        or piero_denominator is None
        or piero_denominator <= 0
        or piero_payout is None
    ):
        return 0.0

    acquisition_rate = _select_extra_normal_small_acquisition_rate(machine_spec, setting_average)
    return normal_games_count * acquisition_rate * (
        bell_payout / bell_denominator + piero_payout / piero_denominator
    )


def _select_extra_normal_small_acquisition_rate(
    machine_spec: dict[str, Any],
    setting_average: float | None,
) -> float:
    threshold = _read_raw_number(machine_spec.get("high_setting_threshold"))
    high_rate = _read_raw_number(machine_spec.get("high_setting_bell_piero_acquisition_rate"))
    low_rate = _read_raw_number(machine_spec.get("low_setting_bell_piero_acquisition_rate"))
    if (
        threshold is None
        or high_rate is None
        or low_rate is None
        or setting_average is None
        or not math.isfinite(setting_average)
    ):
        return 1.0

    return high_rate if setting_average >= threshold else low_rate


def _interpolate_setting_probability(
    setting_average: float | None,
    denominator_rows: tuple[tuple[float, float], ...],
) -> float | None:
    if setting_average is None or not math.isfinite(setting_average):
        return None

    rows = [(setting, 1 / denominator) for setting, denominator in denominator_rows]
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
