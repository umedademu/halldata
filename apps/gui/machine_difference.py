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
    difference_value = (total_bonus_payout - used_coins).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
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


def machine_is_site7_target(machine_name: str) -> bool:
    return find_machine_difference_rule(machine_name, site7_only=True) is not None


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
