from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from functools import lru_cache
import json
from pathlib import Path
import re
from typing import Any

from minrepo_scraper import normalize_text


ROOT_DIR = Path(__file__).resolve().parents[2]
MACHINE_DIFFERENCE_RULES_PATH = ROOT_DIR / "config" / "machine_difference_rules.json"
DIFFERENCE_QUANTIZE = Decimal("0.1")


@lru_cache(maxsize=1)
def load_machine_difference_rules() -> list[dict[str, Any]]:
    if not MACHINE_DIFFERENCE_RULES_PATH.exists():
        return []

    payload = json.loads(MACHINE_DIFFERENCE_RULES_PATH.read_text(encoding="utf-8"))
    rules = payload.get("machine_rules", [])
    if not isinstance(rules, list):
        return []
    return [rule for rule in rules if isinstance(rule, dict)]


def find_machine_difference_rule(machine_name: str) -> dict[str, Any] | None:
    normalized_machine_name = normalize_text(str(machine_name))
    if not normalized_machine_name:
        return None

    for rule in load_machine_difference_rules():
        for candidate_name in rule.get("machine_names", []):
            if normalize_text(str(candidate_name)) == normalized_machine_name:
                return rule
    return None


def calculate_machine_difference_value(machine_name: str, row_values: dict[str, Any]) -> float | None:
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

    bonus_payouts = rule.get("bonus_payouts", {})
    if not isinstance(bonus_payouts, dict) or not bonus_payouts:
        return None

    total_bonus_payout = Decimal("0")
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

    used_coins = games_count * investment_coins / games_per_investment
    difference_value = (total_bonus_payout - used_coins).quantize(DIFFERENCE_QUANTIZE, rounding=ROUND_HALF_UP)
    if difference_value == Decimal("-0.0"):
        difference_value = Decimal("0.0")
    return float(difference_value)


def format_machine_difference_value(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.1f}"


def format_machine_difference_for_row(machine_name: str, row_values: dict[str, Any]) -> str:
    return format_machine_difference_value(calculate_machine_difference_value(machine_name, row_values))


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
