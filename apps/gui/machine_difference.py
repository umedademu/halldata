from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from functools import lru_cache
import json
from pathlib import Path
import re
from typing import Any

from minrepo_scraper import normalize_text
from setting_estimates import calculate_setting_estimate, get_setting_estimate_definition


ROOT_DIR = Path(__file__).resolve().parents[2]
MACHINE_DIFFERENCE_RULES_PATH = ROOT_DIR / "config" / "machine_difference_rules.json"
MINREPO_ONE_BET_GAME_FACTOR = Decimal("0.3333333333333333333333333333")
ONE_BET_GRAPE_DENOMINATOR = Decimal("10.3")
ONE_BET_REPLAY_DENOMINATOR = Decimal("7.3")
ONE_BET_GRAPE_PAYOUT = Decimal("8")
ONE_BET_REPLAY_PAYOUT = Decimal("1")
ONE_BET_TARGET_MACHINE_RATIOS = (
    ("アイムジャグラーex", Decimal("0.75")),
    ("ゴーゴージャグラー3", Decimal("1")),
    ("マイジャグラー", Decimal("0.75")),
)


@lru_cache(maxsize=1)
def load_machine_difference_rules() -> list[dict[str, Any]]:
    if not MACHINE_DIFFERENCE_RULES_PATH.exists():
        return []

    payload = json.loads(MACHINE_DIFFERENCE_RULES_PATH.read_text(encoding="utf-8"))
    rules = payload.get("machine_rules", [])
    if not isinstance(rules, list):
        return []
    return [rule for rule in rules if isinstance(rule, dict)]


def find_machine_difference_rule(machine_name: str, site7_only: bool = False) -> dict[str, Any] | None:
    normalized_machine_name = _normalize_machine_name(machine_name)
    if not normalized_machine_name:
        return None

    for rule in load_machine_difference_rules():
        if site7_only and not bool(rule.get("site7_enabled")):
            continue
        if _machine_name_matches_rule(normalized_machine_name, rule):
            return rule
    return None


def calculate_machine_difference_value(machine_name: str, row_values: dict[str, Any]) -> int | None:
    rule = find_machine_difference_rule(machine_name)
    if rule is None:
        return None

    investment_coins = _parse_decimal_value(rule.get("investment_coins"))
    games_per_investment = _parse_decimal_value(rule.get("games_per_investment"))
    games_count = _read_decimal_value(row_values, "G数", "games_count")
    if (
        investment_coins is None
        or games_per_investment is None
        or games_per_investment == 0
        or games_count is None
    ):
        return None

    bonus_values = _calculate_bonus_payout_and_count(rule, row_values)
    if bonus_values is None:
        return None
    total_bonus_payout, total_bonus_count = bonus_values

    return _calculate_coin_hold_difference_value(
        rule,
        games_count=games_count,
        investment_coins=investment_coins,
        coin_hold=games_per_investment,
        total_bonus_payout=total_bonus_payout,
        total_bonus_count=total_bonus_count,
    )


def calculate_estimated_coin_hold_difference_value(
    machine_name: str,
    row_values: dict[str, Any],
    *,
    setting_average: float | int | None = None,
) -> int | None:
    rule = find_machine_difference_rule(machine_name)
    if rule is None:
        return None

    if setting_average is None:
        definition = get_setting_estimate_definition(machine_name)
        setting_estimate = calculate_setting_estimate(definition, row_values) if definition else None
        setting_average = setting_estimate.get("average") if setting_estimate else None
    setting_average_decimal = _parse_decimal_value(setting_average)
    coin_hold = _interpolate_setting_coin_hold(rule, setting_average_decimal)
    investment_coins = _parse_decimal_value(rule.get("investment_coins"))
    games_count = _read_decimal_value(row_values, "G数", "games_count")
    bonus_values = _calculate_bonus_payout_and_count(rule, row_values)
    if (
        coin_hold is None
        or investment_coins is None
        or games_count is None
        or bonus_values is None
    ):
        return None
    total_bonus_payout, total_bonus_count = bonus_values

    return _calculate_coin_hold_difference_value(
        rule,
        games_count=games_count,
        investment_coins=investment_coins,
        coin_hold=coin_hold,
        total_bonus_payout=total_bonus_payout,
        total_bonus_count=total_bonus_count,
    )


def _calculate_coin_hold_difference_value(
    rule: dict[str, Any],
    *,
    games_count: Decimal,
    investment_coins: Decimal,
    coin_hold: Decimal,
    total_bonus_payout: Decimal,
    total_bonus_count: Decimal,
) -> int | None:
    if coin_hold <= 0:
        return None

    one_bet_bonus_ratio = _read_one_bet_bonus_ratio(rule)
    if one_bet_bonus_ratio is not None:
        one_bet_games = _calculate_one_bet_games(total_bonus_count, one_bet_bonus_ratio)
        normal_games_count = games_count - one_bet_games * MINREPO_ONE_BET_GAME_FACTOR
        if normal_games_count <= 0:
            return None
        one_bet_small_payout = one_bet_games * (
            ONE_BET_GRAPE_PAYOUT / ONE_BET_GRAPE_DENOMINATOR
            + ONE_BET_REPLAY_PAYOUT / ONE_BET_REPLAY_DENOMINATOR
        )
        difference_value = (
            total_bonus_payout
            - normal_games_count * investment_coins / coin_hold
            + one_bet_small_payout
            - one_bet_games
        ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    else:
        difference_value = (total_bonus_payout - games_count * investment_coins / coin_hold).quantize(
            Decimal("1"),
            rounding=ROUND_HALF_UP,
        )

    if difference_value == Decimal("-0"):
        difference_value = Decimal("0")
    return int(difference_value)


def format_machine_difference_value(value: int | None) -> str:
    if value is None:
        return "-"
    return str(value)


def format_machine_difference_for_row(machine_name: str, row_values: dict[str, Any]) -> str:
    return format_machine_difference_value(calculate_machine_difference_value(machine_name, row_values))


def canonical_machine_name(machine_name: str, site7_only: bool = False) -> str:
    rule = find_machine_difference_rule(machine_name, site7_only=site7_only)
    if rule is None:
        return str(machine_name).strip()

    canonical_name = str(rule.get("canonical_name", "")).strip()
    if canonical_name:
        return canonical_name

    for candidate_name in rule.get("machine_names", []):
        text = str(candidate_name).strip()
        if text:
            return text

    return str(machine_name).strip()


def list_site7_target_machine_keywords() -> list[str]:
    keywords: list[str] = []
    seen_keywords: set[str] = set()
    for rule in load_machine_difference_rules():
        if not bool(rule.get("site7_enabled")):
            continue
        for keyword in _rule_keyword_texts(rule):
            if keyword in seen_keywords:
                continue
            seen_keywords.add(keyword)
            keywords.append(keyword)
    return keywords


def list_site7_target_machine_names() -> list[str]:
    machine_names: list[str] = []
    seen_machine_names: set[str] = set()
    for rule in load_machine_difference_rules():
        if not bool(rule.get("site7_enabled")):
            continue

        machine_name = str(rule.get("canonical_name", "")).strip()
        if not machine_name:
            for candidate_name in rule.get("machine_names", []):
                machine_name = str(candidate_name).strip()
                if machine_name:
                    break
        if not machine_name or machine_name in seen_machine_names:
            continue

        seen_machine_names.add(machine_name)
        machine_names.append(machine_name)
    return machine_names


def machine_is_site7_target(machine_name: str) -> bool:
    return find_machine_difference_rule(machine_name, site7_only=True) is not None


def _calculate_bonus_payout_and_count(
    rule: dict[str, Any],
    row_values: dict[str, Any],
) -> tuple[Decimal, Decimal] | None:
    bonus_payouts = rule.get("bonus_payouts", {})
    if not isinstance(bonus_payouts, dict) or not bonus_payouts:
        return None

    total_bonus_payout = Decimal("0")
    total_bonus_count = Decimal("0")
    for bonus_label, payout_value in bonus_payouts.items():
        payout_coins = _parse_decimal_value(payout_value)
        hit_count = _read_decimal_value(
            row_values,
            str(bonus_label),
            f"{str(bonus_label).lower()}_count",
        )
        if payout_coins is None or hit_count is None:
            return None
        total_bonus_payout += hit_count * payout_coins
        total_bonus_count += hit_count
    return total_bonus_payout, total_bonus_count


def _calculate_one_bet_games(total_bonus_count: Decimal, post_announcement_bonus_ratio: Decimal) -> Decimal:
    if total_bonus_count <= 0:
        return Decimal("0")
    settle_probability = (
        Decimal("1") - Decimal("1") / ONE_BET_GRAPE_DENOMINATOR - Decimal("1") / ONE_BET_REPLAY_DENOMINATOR
    )
    if settle_probability <= 0:
        return Decimal("0")
    return total_bonus_count * post_announcement_bonus_ratio / settle_probability


def _read_one_bet_bonus_ratio(rule: dict[str, Any]) -> Decimal | None:
    candidate_texts = [
        str(rule.get("canonical_name") or ""),
        *[str(value) for value in rule.get("machine_names", [])],
        *[str(value) for value in rule.get("match_keywords", [])],
    ]
    normalized_texts = [_normalize_machine_name(text) for text in candidate_texts]
    for keyword, ratio in ONE_BET_TARGET_MACHINE_RATIOS:
        if any(keyword in normalized_text for normalized_text in normalized_texts):
            return ratio
    return None


def _read_setting_coin_hold_rows(rule: dict[str, Any]) -> list[tuple[Decimal, Decimal]]:
    setting_coin_holds = rule.get("setting_coin_holds", {})
    if not isinstance(setting_coin_holds, dict):
        return []

    rows: list[tuple[Decimal, Decimal]] = []
    for setting, coin_hold in setting_coin_holds.items():
        setting_value = _parse_decimal_value(setting)
        coin_hold_value = _parse_decimal_value(coin_hold)
        if setting_value is None or coin_hold_value is None or coin_hold_value <= 0:
            continue
        rows.append((setting_value, coin_hold_value))
    return sorted(rows, key=lambda row: row[0])


def _interpolate_setting_coin_hold(
    rule: dict[str, Any],
    setting_average: Decimal | None,
) -> Decimal | None:
    coin_hold_rows = _read_setting_coin_hold_rows(rule)
    if setting_average is None or not coin_hold_rows:
        return None

    first_setting, first_coin_hold = coin_hold_rows[0]
    last_setting, last_coin_hold = coin_hold_rows[-1]
    if setting_average <= first_setting:
        return first_coin_hold
    if setting_average >= last_setting:
        return last_coin_hold

    for index in range(len(coin_hold_rows) - 1):
        left_setting, left_coin_hold = coin_hold_rows[index]
        right_setting, right_coin_hold = coin_hold_rows[index + 1]
        if setting_average < left_setting or setting_average > right_setting:
            continue
        setting_width = right_setting - left_setting
        if setting_width <= 0:
            return left_coin_hold
        progress = (setting_average - left_setting) / setting_width
        return left_coin_hold + (right_coin_hold - left_coin_hold) * progress

    return None


def _machine_name_matches_rule(normalized_machine_name: str, rule: dict[str, Any]) -> bool:
    for candidate_name in _rule_exact_names(rule):
        if candidate_name == normalized_machine_name:
            return True

    for keyword in _rule_keyword_texts(rule):
        normalized_keyword = _normalize_machine_name(keyword)
        if normalized_keyword and normalized_keyword in normalized_machine_name:
            return True

    return False


def _rule_exact_names(rule: dict[str, Any]) -> list[str]:
    exact_names: list[str] = []
    canonical_name = str(rule.get("canonical_name", "")).strip()
    if canonical_name:
        exact_names.append(_normalize_machine_name(canonical_name))

    for candidate_name in rule.get("machine_names", []):
        exact_names.append(_normalize_machine_name(str(candidate_name)))

    return [name for name in exact_names if name]


def _rule_keyword_texts(rule: dict[str, Any]) -> list[str]:
    keyword_texts: list[str] = []
    for keyword in rule.get("match_keywords", []):
        text = str(keyword).strip()
        if text:
            keyword_texts.append(text)
    return keyword_texts


def _normalize_machine_name(value: str) -> str:
    return normalize_text(str(value)).casefold()


def _read_decimal_value(row_values: dict[str, Any], *keys: str) -> Decimal | None:
    for key in keys:
        if key in row_values:
            parsed_value = _parse_decimal_value(row_values.get(key))
            if parsed_value is not None:
                return parsed_value
    return None


def _parse_decimal_value(value: Any) -> Decimal | None:
    normalized = str(value).strip().replace(",", "")
    if not normalized or normalized == "-":
        return None
    if re.fullmatch(r"-?\d+(?:\.\d+)?", normalized) is None:
        return None

    try:
        return Decimal(normalized)
    except InvalidOperation:
        return None
