from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
import math
import os
from pathlib import Path
import re
from typing import Any
import unicodedata
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import requests

from daidata_online_scraper import daidata_store_is_beam_hikari
from machine_difference import (
    calculate_machine_difference_value,
    canonical_machine_name,
    list_equivalent_machine_names,
)
from minrepo_scraper import MachineHistoryResult, normalize_text
from r2_storage import R2JsonStorage, R2StorageError
from site7_scraper import (
    DEFAULT_SITE7_PREFECTURE_NAME,
    SITE7_DATE_BOUNDARY_HOUR,
    SITE7_DIFFERENCE_SOURCE_GRAPH,
    dataset_has_site7_graph_difference,
    default_site7_store_settings,
    site7_dataset_updated_at,
    site7_store_is_known_unavailable,
)
from web_data_export import (
    WEB_DATA_VERSION,
    StoreSource,
    build_index_store_entry,
    build_machine_data_file,
    build_store_id,
    build_store_payload,
    export_registered_store_payloads,
    export_store_payloads,
    update_index,
)


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_LOCAL_SAVE_DIR = ROOT_DIR / "local_data"
DEFAULT_SCHEMA = "public"
DEFAULT_STORES_TABLE = "stores"
DEFAULT_RESULTS_TABLE = "machine_daily_results"
DEFAULT_MACHINE_SUMMARIES_TABLE = "store_machine_summaries"
DEFAULT_MACHINE_DAILY_DETAILS_TABLE = "store_machine_daily_details"
REGISTERED_STORES_FILE_NAME = "registered_stores.json"
FETCH_FREQUENCY_VALUES = {"高頻度", "毎日", "低頻度", "停止"}
FETCH_SOURCE_VALUES = {"みんレポ", "サイセ", "両方"}
GUI_SETTINGS_FILE_NAME = "gui_settings.json"
REGISTERED_STORE_EXCLUDED_URLS_KEY = "excluded_store_urls"
STORE_COLUMNS = {"機種", "機種名"}
WINDOWS_FORBIDDEN_CHARS = re.compile(r'[<>:"/\\|?*]+')
DATA_SOURCE_MINREPO = "minrepo"
DATA_SOURCE_SITE7 = "site7"
SITE7_SAVED_TIMEZONE = timezone(timedelta(hours=9))
SITE7_COMPLETE_FETCH_HOUR = 23
R2_SNAPSHOT_PREFIX = "snapshots"
R2_FULL_DAY_INDEX_FILE_NAME = "full-day-index.json"
FULL_DAY_INCOMPLETE_RECORD_RATIO = 0.8
FULL_DAY_INCOMPLETE_MACHINE_RATIO = 0.8
FULL_DAY_INCOMPLETE_MIN_REFERENCE_RECORD_COUNT = 20
FULL_DAY_INCOMPLETE_MIN_REFERENCE_MACHINE_COUNT = 5


@dataclass
class PersistenceSummary:
    local_file_path: str | None = None
    local_record_count: int = 0
    supabase_saved: bool = False
    supabase_record_count: int = 0
    web_data_saved: bool = False
    web_data_file_path: str | None = None
    web_data_record_count: int = 0
    messages: list[str] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return bool(self.messages)


@dataclass
class RegisteredStoresPersistenceSummary:
    local_saved: bool = False
    local_store_count: int = 0
    supabase_saved: bool = False
    supabase_store_count: int = 0
    web_data_saved: bool = False
    web_data_store_count: int = 0
    messages: list[str] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return bool(self.messages)


@dataclass
class SavedMachineTargetsSummary:
    saved_targets: set[tuple[str, str]] = field(default_factory=set)
    replaceable_targets: set[tuple[str, str]] = field(default_factory=set)
    messages: list[str] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return bool(self.messages)


@dataclass
class SavedMachineSlotsSummary:
    protected_slots: set[tuple[str, str]] = field(default_factory=set)
    replaceable_slots: set[tuple[str, str]] = field(default_factory=set)
    messages: list[str] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return bool(self.messages)


@dataclass
class SavedFullDayDatesSummary:
    saved_dates: set[str] = field(default_factory=set)
    incomplete_dates: set[str] = field(default_factory=set)
    messages: list[str] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return bool(self.messages)


@dataclass
class FullDaySite7CleanupSummary:
    checked_store_count: int = 0
    updated_store_count: int = 0
    removed_date_count: int = 0
    removed_dates_by_store: dict[str, list[str]] = field(default_factory=dict)
    messages: list[str] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return bool(self.messages)


def normalize_store_url(value: str) -> str:
    text = str(value).strip()
    if not text:
        return ""

    parts = urlsplit(text)
    normalized_scheme = parts.scheme.lower()
    normalized_netloc = parts.netloc.lower()
    normalized_path = quote(unquote(parts.path or "/"), safe="/-_.~")
    if normalized_path != "/":
        normalized_path = normalized_path.rstrip("/") + "/"

    return urlunsplit((normalized_scheme, normalized_netloc, normalized_path, parts.query, ""))


def normalize_store_name_key(value: str) -> str:
    normalized_value = unicodedata.normalize("NFKC", str(value))
    return normalize_text(normalized_value).casefold()


def normalize_machine_name_key(value: str) -> str:
    canonical_name = canonical_machine_name(str(value)).strip()
    return normalize_text(canonical_name)


def normalize_saved_target_machine_name_keys(machine_names: list[str]) -> set[str]:
    normalized_names: set[str] = set()
    for machine_name in machine_names:
        text = str(machine_name).strip()
        if not text:
            continue
        normalized_name = normalize_machine_name_key(text)
        if normalized_name:
            normalized_names.add(normalized_name)
    return normalized_names


def _normalize_data_source(value: Any) -> str:
    text = str(value or "").strip().casefold()
    if text == DATA_SOURCE_SITE7:
        return DATA_SOURCE_SITE7
    return DATA_SOURCE_MINREPO


def _infer_history_data_source(*urls: str) -> str:
    has_minrepo_url = False
    for url in urls:
        normalized_url = str(url or "").strip().lower()
        if (
            "d-deltanet.com" in normalized_url
            or "daidata.goraggio.com" in normalized_url
            or "/site7" in normalized_url
            or "site7" in normalized_url
        ):
            return DATA_SOURCE_SITE7
        if "min-repo.com" in normalized_url:
            has_minrepo_url = True
    if has_minrepo_url:
        return DATA_SOURCE_MINREPO
    return DATA_SOURCE_MINREPO


def _infer_saved_result_data_source(row: dict[str, Any]) -> str:
    data_source = _normalize_data_source(row.get("data_source"))
    payout_rate = row.get("payout_rate")
    if data_source == DATA_SOURCE_SITE7:
        return DATA_SOURCE_SITE7
    if data_source == DATA_SOURCE_MINREPO and str(row.get("data_source", "")).strip() and payout_rate not in (None, ""):
        return DATA_SOURCE_MINREPO
    if payout_rate in (None, ""):
        return DATA_SOURCE_SITE7
    return DATA_SOURCE_MINREPO


def _record_has_site7_data_source(row: dict[str, Any]) -> bool:
    return str(row.get("data_source", "")).strip().casefold() == DATA_SOURCE_SITE7


def _record_has_site7_source(row: dict[str, Any]) -> bool:
    if _record_has_site7_data_source(row):
        return True
    return bool(str(row.get("site7_fetched_at", "")).strip())


def _parse_site7_saved_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=SITE7_SAVED_TIMEZONE)
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _site7_business_date_from_updated_at(updated_at_utc: datetime) -> str:
    updated_at = updated_at_utc.replace(tzinfo=timezone.utc).astimezone(SITE7_SAVED_TIMEZONE)
    if updated_at.hour < SITE7_DATE_BOUNDARY_HOUR:
        updated_at -= timedelta(days=1)
    return updated_at.strftime("%Y-%m-%d")


def _site7_complete_fetch_threshold(target_date: Any) -> datetime | None:
    target_date_text = str(target_date or "").strip()
    if not target_date_text:
        return None
    try:
        parsed_date = datetime.strptime(target_date_text, "%Y-%m-%d")
    except ValueError:
        return None
    threshold = parsed_date.replace(
        hour=SITE7_COMPLETE_FETCH_HOUR,
        minute=0,
        second=0,
        microsecond=0,
        tzinfo=SITE7_SAVED_TIMEZONE,
    )
    return threshold.astimezone(timezone.utc).replace(tzinfo=None)


def _site7_record_update_is_current_or_newer(
    record: dict[str, Any],
    site7_updated_at: str | datetime | None,
) -> bool:
    current_updated_at = _parse_site7_saved_datetime(site7_updated_at)
    if current_updated_at is None:
        return True

    saved_updated_at = _parse_site7_saved_datetime(
        record.get("site7_fetched_at") or record.get("site7FetchedAt")
    )
    if saved_updated_at is None:
        return False

    target_date = str(record.get("target_date", "")).strip()
    current_business_date = _site7_business_date_from_updated_at(current_updated_at)
    if target_date and target_date < current_business_date:
        complete_fetch_threshold = _site7_complete_fetch_threshold(target_date)
        if complete_fetch_threshold is not None:
            return saved_updated_at >= complete_fetch_threshold

    return saved_updated_at >= current_updated_at


def choose_preferred_store(candidates: list[dict[str, Any]]) -> dict[str, str] | None:
    ranked_candidates: list[tuple[int, int, str, str]] = []
    for candidate in candidates:
        store_name = str(candidate.get("store_name", "")).strip()
        store_url = normalize_store_url(str(candidate.get("store_url", "")).strip())
        if not store_name or not store_url:
            continue

        try:
            record_count = int(candidate.get("record_count", 0) or 0)
        except (TypeError, ValueError):
            record_count = 0

        ranked_candidates.append(
            (
                record_count,
                1 if "min-repo.com" in store_url.lower() else 0,
                store_name,
                store_url,
            )
        )

    if not ranked_candidates:
        return None

    _, _, store_name, store_url = max(ranked_candidates, key=lambda item: (item[0], item[1], item[3]))
    return {
        "store_name": store_name,
        "store_url": store_url,
    }


def build_machine_daily_records(history_result: MachineHistoryResult) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    for dataset in history_result.datasets:
        source_columns = [column for column in dataset.columns if normalize_text(column) not in STORE_COLUMNS]
        stored_machine_name = canonical_machine_name(dataset.machine_name).strip() or dataset.machine_name.strip()
        data_source = _infer_history_data_source(dataset.store_url, dataset.date_url, dataset.machine_url)
        for row in dataset.rows:
            row_values = dict(zip(source_columns, row, strict=False))
            slot_number = row_values.get("台番", "").strip()
            if not slot_number:
                continue

            source_difference_value = _parse_difference_value(row_values.get("差枚", ""))
            bonus_difference_value = calculate_machine_difference_value(stored_machine_name, row_values)
            has_site7_graph_difference = (
                data_source == DATA_SOURCE_SITE7
                and source_difference_value is not None
                and dataset_has_site7_graph_difference(dataset, slot_number)
            )
            if data_source == DATA_SOURCE_SITE7:
                difference_value = source_difference_value if has_site7_graph_difference else None
            else:
                difference_value = (
                    source_difference_value
                    if source_difference_value is not None
                    else bonus_difference_value
                )
            games_count = _parse_int_value(row_values.get("G数", ""))
            payout_rate = _parse_percent_value(row_values.get("出率", ""))
            bb_count = _parse_int_value(row_values.get("BB", ""))
            rb_count = _parse_int_value(row_values.get("RB", ""))
            combined_ratio_text = _parse_text_value(row_values.get("合成", ""))
            bb_ratio_text = _parse_text_value(row_values.get("BB率", ""))
            rb_ratio_text = _parse_text_value(row_values.get("RB率", ""))
            site7_difference_source = ""
            if has_site7_graph_difference:
                site7_difference_source = SITE7_DIFFERENCE_SOURCE_GRAPH
            if data_source == DATA_SOURCE_SITE7 and not _site7_record_has_meaningful_data(
                difference_value=difference_value,
                bonus_difference_value=bonus_difference_value,
                games_count=games_count,
                payout_rate=payout_rate,
                bb_count=bb_count,
                rb_count=rb_count,
                combined_ratio_text=combined_ratio_text,
                bb_ratio_text=bb_ratio_text,
                rb_ratio_text=rb_ratio_text,
            ):
                continue

            record = {
                "target_date": dataset.target_date,
                "slot_number": slot_number,
                "machine_name": stored_machine_name,
                "data_source": data_source,
                "difference_value": difference_value,
                "bonus_difference_value": bonus_difference_value,
                "games_count": games_count,
                "payout_rate": payout_rate,
                "bb_count": bb_count,
                "rb_count": rb_count,
                "combined_ratio_text": combined_ratio_text,
                "bb_ratio_text": bb_ratio_text,
                "rb_ratio_text": rb_ratio_text,
            }
            if site7_difference_source:
                record["site7_difference_source"] = site7_difference_source
            site7_updated_at = site7_dataset_updated_at(dataset)
            if data_source == DATA_SOURCE_SITE7 and site7_updated_at:
                record["site7_fetched_at"] = site7_updated_at
            records.append(record)

    return records


def _site7_record_has_meaningful_data(
    *,
    difference_value: Any = None,
    bonus_difference_value: Any = None,
    games_count: Any = None,
    payout_rate: Any = None,
    bb_count: Any = None,
    rb_count: Any = None,
    combined_ratio_text: Any = None,
    bb_ratio_text: Any = None,
    rb_ratio_text: Any = None,
) -> bool:
    numeric_values = (
        difference_value,
        bonus_difference_value,
        games_count,
        payout_rate,
        bb_count,
        rb_count,
    )
    if any(value is not None for value in numeric_values):
        return True

    text_values = (combined_ratio_text, bb_ratio_text, rb_ratio_text)
    return any(str(value or "").strip() not in {"", "-", "--"} for value in text_values)


def _saved_value_is_filled(value: Any) -> bool:
    if value is None:
        return False
    return str(value).strip() not in {"", "-", "--"}


def _site7_record_has_complete_fetch_data(
    record: dict[str, Any],
    *,
    require_source_difference: bool = True,
) -> bool:
    has_required_counts = all(
        _saved_value_is_filled(record.get(field_name))
        for field_name in ("games_count", "bb_count", "rb_count")
    )
    if not has_required_counts:
        return False
    if require_source_difference:
        return _site7_record_has_source_difference_value(record)
    return True


def _site7_record_has_source_difference_value(record: dict[str, Any]) -> bool:
    if not _saved_value_is_filled(record.get("difference_value")):
        return False
    difference_source = str(
        record.get("site7_difference_source") or record.get("site7DifferenceSource") or ""
    ).strip().casefold()
    if difference_source == SITE7_DIFFERENCE_SOURCE_GRAPH:
        return True

    bonus_difference_value = record.get("bonus_difference_value")
    if not _saved_value_is_filled(bonus_difference_value):
        return True

    difference_value = _parse_difference_value(str(record.get("difference_value")))
    parsed_bonus_difference_value = _parse_difference_value(str(bonus_difference_value))
    if difference_value is None or parsed_bonus_difference_value is None:
        return str(record.get("difference_value")).strip() != str(bonus_difference_value).strip()
    return difference_value != parsed_bonus_difference_value


def _saved_record_should_be_kept(record: dict[str, Any]) -> bool:
    if _infer_saved_result_data_source(record) != DATA_SOURCE_SITE7:
        return True
    return _site7_record_has_meaningful_data(
        difference_value=record.get("difference_value"),
        bonus_difference_value=record.get("bonus_difference_value"),
        games_count=record.get("games_count"),
        payout_rate=record.get("payout_rate"),
        bb_count=record.get("bb_count"),
        rb_count=record.get("rb_count"),
        combined_ratio_text=record.get("combined_ratio_text"),
        bb_ratio_text=record.get("bb_ratio_text"),
        rb_ratio_text=record.get("rb_ratio_text"),
    )


def _with_site7_fetched_at(record: dict[str, Any], fetched_at: str) -> dict[str, Any]:
    if not _record_has_site7_data_source(record):
        return record

    normalized_fetched_at = str(
        record.get("site7_fetched_at") or record.get("site7FetchedAt") or fetched_at
    ).strip()
    if not normalized_fetched_at:
        return record

    updated_record = dict(record)
    updated_record["site7_fetched_at"] = normalized_fetched_at
    return updated_record


def build_supabase_result_payload(record: dict[str, Any], store_id: str, updated_at: str) -> dict[str, Any]:
    payload = dict(record)
    payload["difference_value"] = _normalize_difference_value_for_supabase(payload.get("difference_value"))
    payload["bonus_difference_value"] = _normalize_difference_value_for_supabase(payload.get("bonus_difference_value"))
    payload["data_source"] = _normalize_data_source(payload.get("data_source"))
    payload["store_id"] = store_id
    payload["updated_at"] = updated_at
    return payload


def build_store_machine_summary_payloads(
    records: list[dict[str, Any]],
    store_id: str,
    updated_at: str,
) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}

    for record in records:
        if not isinstance(record, dict):
            continue

        machine_name = str(record.get("machine_name", "")).strip()
        target_date = str(record.get("target_date", "")).strip()
        slot_number = str(record.get("slot_number", "")).strip()
        if not machine_name or not target_date or not slot_number:
            continue

        bucket = buckets.get(machine_name)
        if bucket is None or target_date > bucket["latest_date"]:
            buckets[machine_name] = {
                "machine_name": machine_name,
                "latest_date": target_date,
                "slot_numbers": {slot_number},
                "rows": [record],
            }
            continue

        if target_date != bucket["latest_date"]:
            continue

        bucket["slot_numbers"].add(slot_number)
        bucket["rows"].append(record)

    payloads: list[dict[str, Any]] = []
    for bucket in buckets.values():
        rows = bucket["rows"]
        payloads.append(
            {
                "store_id": store_id,
                "machine_name": bucket["machine_name"],
                "latest_date": bucket["latest_date"],
                "slot_count": len(bucket["slot_numbers"]),
                "average_difference": _average_summary_numbers(row.get("difference_value") for row in rows),
                "average_games": _average_summary_numbers(row.get("games_count") for row in rows),
                "average_payout": _average_summary_numbers(row.get("payout_rate") for row in rows),
                "updated_at": updated_at,
            }
        )

    payloads.sort(
        key=lambda payload: (
            str(payload.get("latest_date", "")),
            int(payload.get("slot_count", 0) or 0),
            normalize_text(str(payload.get("machine_name", ""))),
        ),
        reverse=True,
    )
    return payloads


def build_store_machine_daily_detail_payloads(
    records: list[dict[str, Any]],
    store_id: str,
    updated_at: str,
) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str], dict[str, Any]] = {}

    for record in records:
        if not isinstance(record, dict):
            continue

        machine_name = str(record.get("machine_name", "")).strip()
        target_date = str(record.get("target_date", "")).strip()
        slot_number = str(record.get("slot_number", "")).strip()
        if not machine_name or not target_date or not slot_number:
            continue

        bucket_key = (machine_name, target_date)
        bucket = buckets.get(bucket_key)
        if bucket is None:
            bucket = {
                "machine_name": machine_name,
                "target_date": target_date,
                "records_by_slot": {},
                "rows": [],
            }
            buckets[bucket_key] = bucket

        slot_payload = {
            "difference_value": _normalize_difference_value_for_supabase(record.get("difference_value")),
            "bonus_difference_value": _normalize_difference_value_for_supabase(
                record.get("bonus_difference_value")
            ),
            "games_count": _parse_numeric_value(record.get("games_count")),
            "payout_rate": _parse_numeric_value(record.get("payout_rate")),
            "bb_count": _parse_numeric_value(record.get("bb_count")),
            "rb_count": _parse_numeric_value(record.get("rb_count")),
            "combined_ratio_text": _parse_text_value(record.get("combined_ratio_text") or ""),
            "bb_ratio_text": _parse_text_value(record.get("bb_ratio_text") or ""),
            "rb_ratio_text": _parse_text_value(record.get("rb_ratio_text") or ""),
        }
        data_source = _normalize_data_source(record.get("data_source"))
        if data_source == DATA_SOURCE_SITE7:
            slot_payload["data_source"] = data_source
            site7_difference_source = str(
                record.get("site7_difference_source") or record.get("site7DifferenceSource") or ""
            ).strip()
            if site7_difference_source:
                slot_payload["site7_difference_source"] = site7_difference_source
        site7_fetched_at = str(record.get("site7_fetched_at") or record.get("site7FetchedAt") or "").strip()
        if data_source == DATA_SOURCE_SITE7 and site7_fetched_at:
            slot_payload["site7_fetched_at"] = site7_fetched_at
        bucket["records_by_slot"][slot_number] = slot_payload
        bucket["rows"].append(record)

    payloads: list[dict[str, Any]] = []
    for bucket in buckets.values():
        records_by_slot = {
            slot_number: bucket["records_by_slot"][slot_number]
            for slot_number in sorted(bucket["records_by_slot"].keys(), key=_slot_number_sort_key)
        }
        rows = bucket["rows"]
        payloads.append(
            {
                "store_id": store_id,
                "machine_name": bucket["machine_name"],
                "target_date": bucket["target_date"],
                "slot_count": len(records_by_slot),
                "average_difference": _average_summary_numbers(row.get("difference_value") for row in rows),
                "average_games": _average_summary_numbers(row.get("games_count") for row in rows),
                "average_payout": _average_summary_numbers(row.get("payout_rate") for row in rows),
                "records_by_slot": records_by_slot,
                "updated_at": updated_at,
            }
        )

    payloads.sort(
        key=lambda payload: (
            str(payload.get("target_date", "")),
            normalize_text(str(payload.get("machine_name", ""))),
        ),
        reverse=True,
    )
    return payloads


class HistoryPersistenceService:
    def __init__(self, root_dir: Path | None = None, r2_storage: R2JsonStorage | None = None) -> None:
        self.root_dir = root_dir or ROOT_DIR
        self.r2_storage = r2_storage or R2JsonStorage.from_environment(self.root_dir)
        self._registered_store_index_load_failed = False
        self._registered_store_index_load_error = ""

    def save_history_result(self, history_result: MachineHistoryResult, full_day: bool = False) -> PersistenceSummary:
        snapshot = self._build_local_snapshot(history_result)
        summary = PersistenceSummary(local_record_count=len(snapshot["records"]))

        try:
            snapshot_key = self._save_r2_snapshot(snapshot)
            entry = self._save_r2_web_data(snapshot)
            if full_day:
                if self._snapshot_is_minrepo_only(snapshot):
                    self._mark_full_day_saved_r2(
                        snapshot,
                        snapshot_key,
                        verified_current_counts_by_date=self._full_day_saved_counts_by_date(snapshot),
                        verified_site7_dates=set(),
                    )
                else:
                    self._mark_full_day_saved_r2(snapshot, snapshot_key)
            else:
                self._clear_full_day_saved_r2_for_snapshot_site7_dates(snapshot)
            summary.web_data_saved = True
            summary.web_data_file_path = self._format_r2_path(str(entry.get("dataFile", "")))
            summary.web_data_record_count = int(entry.get("recordCount") or len(snapshot["records"]))
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"R2保存に失敗しました。\n{exc}")

        return summary

    def save_history_result_local_checkpoint(
        self,
        history_result: MachineHistoryResult,
        full_day: bool = False,
    ) -> PersistenceSummary:
        snapshot = self._build_local_snapshot(history_result)
        summary = PersistenceSummary(local_record_count=len(snapshot["records"]))

        try:
            local_path = self._save_local_snapshot(snapshot)
            if full_day:
                self._mark_full_day_saved(snapshot, local_path)
            summary.local_file_path = str(local_path)
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"ローカル退避に失敗しました。\n{exc}")

        return summary

    def delete_local_checkpoint_files(self, file_paths: list[str]) -> PersistenceSummary:
        summary = PersistenceSummary()
        local_dir = self._local_save_dir().resolve()
        deleted_count = 0

        for file_path_text in file_paths:
            file_path = Path(file_path_text)
            try:
                resolved_path = file_path.resolve()
            except OSError as exc:
                summary.messages.append(f"ローカル退避の確認に失敗しました。\n{file_path}\n{exc}")
                continue

            if not _is_relative_to(resolved_path, local_dir):
                summary.messages.append(f"ローカル退避以外のファイルは削除しませんでした。\n{resolved_path}")
                continue

            if resolved_path.name in {REGISTERED_STORES_FILE_NAME, GUI_SETTINGS_FILE_NAME}:
                summary.messages.append(f"設定ファイルは削除しませんでした。\n{resolved_path}")
                continue

            if resolved_path.name == "_full_day_index.json" or resolved_path.parent.name == "backups":
                summary.messages.append(f"管理用ファイルは削除しませんでした。\n{resolved_path}")
                continue

            if not resolved_path.exists():
                continue
            if not resolved_path.is_file() or resolved_path.suffix.lower() != ".json":
                summary.messages.append(f"JSON退避ファイル以外は削除しませんでした。\n{resolved_path}")
                continue

            try:
                resolved_path.unlink()
                deleted_count += 1
            except OSError as exc:
                summary.messages.append(f"ローカル退避の削除に失敗しました。\n{resolved_path}\n{exc}")

        summary.local_record_count = deleted_count
        return summary

    def mark_full_day_saved(self, history_result: MachineHistoryResult) -> PersistenceSummary:
        snapshot = self._build_local_snapshot(history_result)
        summary = PersistenceSummary()

        try:
            snapshot_key = self._save_r2_snapshot(snapshot)
            self._mark_full_day_saved_r2(snapshot, snapshot_key)
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"R2の全機種取得済み記録に失敗しました。\n{exc}")

        return summary

    def find_saved_full_day_dates(
        self,
        store_name: str,
        store_url: str,
        start_date: str,
        end_date: str,
    ) -> SavedFullDayDatesSummary:
        summary = SavedFullDayDatesSummary()
        try:
            all_saved_date_entries = self._find_saved_full_day_date_entries_r2(
                store_name=store_name,
                store_url=store_url,
                start_date="0000-00-00",
                end_date="9999-99-99",
            )
            saved_date_entries = {
                target_date: entry
                for target_date, entry in all_saved_date_entries.items()
                if start_date <= target_date <= end_date
            }
            incomplete_dates = self._find_incomplete_full_day_dates(all_saved_date_entries)
            summary.incomplete_dates.update(incomplete_dates.intersection(saved_date_entries))
            summary.saved_dates.update(set(saved_date_entries).difference(summary.incomplete_dates))
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"R2の全機種取得済み確認に失敗しました。\n{exc}")
            return summary

        return summary

    def clear_full_day_saved_dates_with_site7(
        self,
        store_name: str = "",
        store_url: str = "",
        start_date: str = "0000-00-00",
        end_date: str = "9999-99-99",
    ) -> FullDaySite7CleanupSummary:
        summary = FullDaySite7CleanupSummary()
        try:
            stores = self._full_day_site7_cleanup_target_stores(
                store_name=store_name,
                store_url=store_url,
            )
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"R2の店舗一覧確認に失敗しました。\n{exc}")
            return summary

        for store in stores:
            current_store_name = str(store.get("store_name", "")).strip()
            current_store_url = normalize_store_url(str(store.get("store_url", "")))
            if not current_store_name and not current_store_url:
                continue
            summary.checked_store_count += 1
            try:
                saved_entries = self._find_saved_full_day_date_entries_r2(
                    store_name=current_store_name,
                    store_url=current_store_url,
                    start_date=start_date,
                    end_date=end_date,
                )
                removed_dates = self._clear_full_day_saved_r2_for_current_site7_dates(
                    store_name=current_store_name,
                    store_url=current_store_url,
                    candidate_dates=set(saved_entries),
                )
            except Exception as exc:  # noqa: BLE001
                display_name = current_store_name or current_store_url
                summary.messages.append(f"{display_name} の取得済み印整理に失敗しました。\n{exc}")
                continue

            if not removed_dates:
                continue
            display_name = current_store_name or current_store_url
            summary.updated_store_count += 1
            summary.removed_date_count += len(removed_dates)
            summary.removed_dates_by_store[display_name] = removed_dates

        return summary

    def find_saved_machine_targets(
        self,
        store_name: str,
        store_url: str,
        start_date: str,
        end_date: str,
        machine_names: list[str],
    ) -> SavedMachineTargetsSummary:
        target_machine_names = normalize_saved_target_machine_name_keys(machine_names)
        summary = SavedMachineTargetsSummary()
        if not target_machine_names:
            return summary

        try:
            protected_targets, replaceable_targets = self._find_saved_machine_target_sources_r2(
                store_name=store_name,
                store_url=store_url,
                start_date=start_date,
                end_date=end_date,
                target_machine_names=target_machine_names,
            )
            summary.saved_targets.update(protected_targets)
            summary.replaceable_targets.update(replaceable_targets)
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"R2の取得済み確認に失敗しました。\n{exc}")

        return summary

    def find_saved_machine_slots(
        self,
        store_name: str,
        store_url: str,
        start_date: str,
        end_date: str,
        slot_numbers: list[str],
        require_source_difference: bool = True,
        site7_updated_at: str | datetime | None = None,
    ) -> SavedMachineSlotsSummary:
        normalized_slot_numbers = {
            str(slot_number).strip()
            for slot_number in slot_numbers
            if str(slot_number).strip()
        }
        summary = SavedMachineSlotsSummary()
        if not normalized_slot_numbers:
            return summary

        try:
            protected_slots, replaceable_slots = self._find_saved_machine_slot_sources_r2(
                store_name=store_name,
                store_url=store_url,
                start_date=start_date,
                end_date=end_date,
                target_slot_numbers=normalized_slot_numbers,
                require_source_difference=require_source_difference,
                site7_updated_at=site7_updated_at,
            )
            summary.protected_slots.update(protected_slots)
            summary.replaceable_slots.update(replaceable_slots)
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"R2の取得済み確認に失敗しました。\n{exc}")

        return summary

    def find_saved_machine_targets_supabase(
        self,
        store_url: str,
        start_date: str,
        end_date: str,
        machine_names: list[str],
    ) -> SavedMachineTargetsSummary:
        return self.find_saved_machine_targets(
            store_name="",
            store_url=store_url,
            start_date=start_date,
            end_date=end_date,
            machine_names=machine_names,
        )

    def find_saved_machine_slots_supabase(
        self,
        store_url: str,
        start_date: str,
        end_date: str,
        slot_numbers: list[str],
        require_source_difference: bool = True,
        site7_updated_at: str | datetime | None = None,
    ) -> SavedMachineSlotsSummary:
        return self.find_saved_machine_slots(
            store_name="",
            store_url=store_url,
            start_date=start_date,
            end_date=end_date,
            slot_numbers=slot_numbers,
            require_source_difference=require_source_difference,
            site7_updated_at=site7_updated_at,
        )

    def delete_machine_targets_from_supabase(
        self,
        store_url: str,
        target_pairs: set[tuple[str, str]],
        data_source: str | None = None,
    ) -> int:
        return 0

    def delete_machine_slots_from_supabase(
        self,
        store_url: str,
        target_slots: set[tuple[str, str]],
        data_source: str | None = None,
    ) -> int:
        return 0

    def load_registered_stores(self) -> list[dict[str, Any]]:
        self._registered_store_index_load_failed = False
        self._registered_store_index_load_error = ""
        local_stores = self._load_registered_stores_local()
        excluded_store_urls = self._load_registered_store_excluded_urls()
        fallback_stores = self._merge_registered_store_sources(
            self._load_registered_stores_from_static_web_data(),
            [],
            excluded_store_urls=set(),
        )
        return self._merge_registered_store_sources(
            local_stores,
            fallback_stores,
            excluded_store_urls=excluded_store_urls,
        )

    def save_registered_stores(self, stores: list[dict[str, Any]]) -> RegisteredStoresPersistenceSummary:
        normalized_stores = self._normalize_registered_stores(stores)
        summary = RegisteredStoresPersistenceSummary()

        try:
            saved_count = self._save_registered_stores_local(normalized_stores)
            summary.local_saved = True
            summary.local_store_count = saved_count
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"登録店舗のローカル保存に失敗しました。\n{exc}")

        try:
            if self._registered_store_index_load_failed:
                summary.messages.append(self._registered_store_index_update_blocked_message())
            else:
                entries = self._save_registered_stores_to_r2_web_data(normalized_stores)
                summary.web_data_saved = bool(entries)
                summary.web_data_store_count = len(entries)
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"Web表示用店舗索引の更新に失敗しました。\n{exc}")

        return summary

    def sync_registered_stores_to_web_data(
        self,
        stores: list[dict[str, Any]] | None = None,
    ) -> RegisteredStoresPersistenceSummary:
        source_stores = self.load_registered_stores() if stores is None else stores
        normalized_stores = self._normalize_registered_stores(source_stores)
        summary = RegisteredStoresPersistenceSummary(local_store_count=len(normalized_stores))

        try:
            if self._registered_store_index_load_failed:
                summary.messages.append(self._registered_store_index_update_blocked_message())
            else:
                entries = self._save_registered_stores_to_r2_web_data(normalized_stores)
                summary.web_data_saved = bool(entries)
                summary.web_data_store_count = len(entries)
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"Web表示用店舗索引の更新に失敗しました。\n{exc}")

        return summary

    def delete_registered_stores(self, store_urls: list[str]) -> int:
        normalized_store_urls = sorted(
            {
                normalized_store_url
                for store_url in store_urls
                if (normalized_store_url := normalize_store_url(store_url))
            }
        )
        if not normalized_store_urls:
            return 0

        return self._delete_registered_stores_local(normalized_store_urls)

    def refresh_web_data_for_store(self, store_name: str) -> dict[str, Any] | None:
        return self._find_r2_store_entry(store_name=store_name, store_url="")

    def _build_local_snapshot(self, history_result: MachineHistoryResult) -> dict[str, Any]:
        saved_at = datetime.now().astimezone().isoformat(timespec="seconds")
        records = [
            _with_site7_fetched_at(record, saved_at)
            for record in build_machine_daily_records(history_result)
        ]
        registered_location = self._registered_store_location_for(
            history_result.store_name,
            history_result.store_url,
        )
        store_payload = {
            "store_name": history_result.store_name,
            "store_url": normalize_store_url(history_result.store_url),
        }
        if registered_location.get("prefecture_name"):
            store_payload["site7_prefecture"] = registered_location["prefecture_name"]
        if registered_location.get("area_name"):
            store_payload["site7_area"] = registered_location["area_name"]
        if registered_location.get("event_day_tails"):
            store_payload["event_day_tails"] = registered_location["event_day_tails"]
        if registered_location.get("event_month_days"):
            store_payload["event_month_days"] = registered_location["event_month_days"]
        if registered_location.get("event_zoro"):
            store_payload["event_zoro"] = True
        if registered_location.get("event_weekdays"):
            store_payload["event_weekdays"] = registered_location["event_weekdays"]
        if registered_location.get("event_source_text"):
            store_payload["event_source_text"] = registered_location["event_source_text"]

        return {
            "saved_at": saved_at,
            "store": store_payload,
            "period": {
                "start_date": history_result.start_date,
                "end_date": history_result.end_date,
            },
            "date_pages": [
                {
                    "target_date": date_page.target_date,
                    "date_url": date_page.date_url,
                }
                for date_page in history_result.date_pages
            ],
            "machine_names": sorted(
                {
                    str(record.get("machine_name", "")).strip()
                    for record in records
                    if str(record.get("machine_name", "")).strip()
                },
                key=normalize_text,
            ),
            "records": records,
        }

    def _r2_store_source_from_snapshot(self, snapshot: dict[str, Any]) -> StoreSource:
        store = snapshot.get("store", {})
        store_name = str(store.get("store_name", "")).strip() if isinstance(store, dict) else ""
        store_url = normalize_store_url(str(store.get("store_url", "")).strip()) if isinstance(store, dict) else ""
        prefecture_name = ""
        area_name = ""
        if isinstance(store, dict):
            prefecture_name = str(
                store.get("prefectureName")
                or store.get("prefecture_name")
                or store.get("site7_prefecture")
                or store.get("site7Prefecture")
                or ""
            ).strip()
            area_name = str(
                store.get("areaName")
                or store.get("area_name")
                or store.get("site7_area")
                or store.get("site7Area")
                or ""
            ).strip()

        registered_location = self._registered_store_location_for(store_name, store_url)
        return StoreSource(
            store_name=store_name or store_url,
            store_url=store_url,
            prefecture_name=prefecture_name or registered_location.get("prefecture_name", ""),
            area_name=area_name or registered_location.get("area_name", ""),
            event_day_tails=_normalize_event_values(
                store.get("event_day_tails", registered_location.get("event_day_tails", []))
                if isinstance(store, dict)
                else registered_location.get("event_day_tails", []),
                0,
                9,
            ),
            event_month_days=_normalize_event_values(
                store.get("event_month_days", registered_location.get("event_month_days", []))
                if isinstance(store, dict)
                else registered_location.get("event_month_days", []),
                1,
                31,
            ),
            event_zoro=_coerce_bool(
                store.get("event_zoro", registered_location.get("event_zoro", False))
                if isinstance(store, dict)
                else registered_location.get("event_zoro", False),
            ),
            event_weekdays=_normalize_event_values(
                store.get("event_weekdays", registered_location.get("event_weekdays", []))
                if isinstance(store, dict)
                else registered_location.get("event_weekdays", []),
                0,
                6,
            ),
            event_source_text=str(
                (
                    store.get("event_source_text", registered_location.get("event_source_text", ""))
                    if isinstance(store, dict)
                    else registered_location.get("event_source_text", "")
                )
            ).strip(),
        )

    def _r2_store_id(self, store_name: str, store_url: str) -> str:
        return build_store_id(store_name, normalize_store_url(store_url))

    def _r2_store_key(self, store_id: str) -> str:
        return f"stores/{store_id}.json"

    def _r2_full_day_index_key(self, store_name: str, store_url: str) -> str:
        store_id = self._r2_store_id(store_name, store_url)
        return f"stores/{store_id}/{R2_FULL_DAY_INDEX_FILE_NAME}"

    def _format_r2_path(self, key: str) -> str:
        config = self.r2_storage.require_config()
        normalized_key = str(key).replace("\\", "/").lstrip("/")
        return f"r2://{config.bucket_name}/{normalized_key}"

    def _save_r2_snapshot(self, snapshot: dict[str, Any]) -> str:
        store_source = self._r2_store_source_from_snapshot(snapshot)
        store_id = self._r2_store_id(store_source.store_name, store_source.store_url)
        period = snapshot.get("period", {})
        start_date = str(period.get("start_date", "")).strip() if isinstance(period, dict) else ""
        end_date = str(period.get("end_date", "")).strip() if isinstance(period, dict) else ""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        key = f"{R2_SNAPSHOT_PREFIX}/{store_id}/{start_date}_{end_date}_{timestamp}.json"
        return self.r2_storage.write_json(key, snapshot)

    def _save_r2_web_data(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        store_source = self._r2_store_source_from_snapshot(snapshot)
        incoming_records = [
            record
            for record in snapshot.get("records", [])
            if isinstance(record, dict) and _saved_record_should_be_kept(record)
        ]
        if incoming_records and self._records_are_minrepo_only(incoming_records):
            return self._save_r2_web_data_minrepo_incremental(
                store_source=store_source,
                incoming_records=incoming_records,
            )

        existing_records = self._load_r2_store_records(
            store_name=store_source.store_name,
            store_url=store_source.store_url,
        )
        records = self._merge_r2_records(existing_records, incoming_records)
        store_payload = build_store_payload(store_source, records)
        entries = export_store_payloads(
            self.root_dir / "apps" / "web" / "public" / "halldata-static",
            [store_payload],
            r2_storage=self.r2_storage,
            allow_missing_r2_index=False,
        )
        return entries[0] if entries else {}

    def _records_are_minrepo_only(self, records: list[dict[str, Any]]) -> bool:
        for record in records:
            if _infer_saved_result_data_source(record) != DATA_SOURCE_MINREPO:
                return False
            if _record_has_site7_source(record):
                return False
        return True

    def _snapshot_is_minrepo_only(self, snapshot: dict[str, Any]) -> bool:
        records = [
            record
            for record in snapshot.get("records", [])
            if isinstance(record, dict) and _saved_record_should_be_kept(record)
        ]
        return bool(records) and self._records_are_minrepo_only(records)

    def _save_r2_web_data_minrepo_incremental(
        self,
        *,
        store_source: StoreSource,
        incoming_records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        existing_store_payload = self._load_r2_store_payload(
            store_name=store_source.store_name,
            store_url=store_source.store_url,
        )
        if existing_store_payload is None:
            store_payload = build_store_payload(store_source, incoming_records)
            entries = export_store_payloads(
                self.root_dir / "apps" / "web" / "public" / "halldata-static",
                [store_payload],
                r2_storage=self.r2_storage,
                allow_missing_r2_index=False,
            )
            return entries[0] if entries else {}

        incoming_records_by_machine: dict[str, list[dict[str, Any]]] = {}
        for record in incoming_records:
            machine_name = str(record.get("machine_name", "")).strip()
            machine_key = normalize_machine_name_key(machine_name)
            if not machine_key:
                continue
            incoming_records_by_machine.setdefault(machine_key, []).append(record)

        if not incoming_records_by_machine:
            store_id = self._r2_store_id(store_source.store_name, store_source.store_url)
            return build_index_store_entry(existing_store_payload, self._r2_store_key(store_id))

        existing_machine_summaries = [
            machine
            for machine in existing_store_payload.get("machines", [])
            if isinstance(machine, dict)
        ]
        existing_machine_summaries_by_key = {
            normalize_machine_name_key(str(machine.get("machineName", "")).strip()): machine
            for machine in existing_machine_summaries
            if normalize_machine_name_key(str(machine.get("machineName", "")).strip())
        }
        updated_machine_summaries_by_key: dict[str, dict[str, Any]] = {
            machine_key: dict(machine)
            for machine_key, machine in existing_machine_summaries_by_key.items()
        }
        existing_total_record_count = self._read_store_record_count(existing_store_payload)
        replaced_record_count = 0
        added_record_count = 0

        for machine_key, machine_incoming_records in incoming_records_by_machine.items():
            existing_summary = existing_machine_summaries_by_key.get(machine_key)
            existing_data_file = (
                str(existing_summary.get("dataFile", "")).strip()
                if isinstance(existing_summary, dict)
                else ""
            )
            incoming_machine_name = str(machine_incoming_records[0].get("machine_name", "")).strip()
            canonical_incoming_machine_name = (
                canonical_machine_name(incoming_machine_name).strip()
                or incoming_machine_name
            )
            existing_machine_records = self._load_r2_equivalent_machine_records(
                store_source=store_source,
                machine_name=canonical_incoming_machine_name,
                primary_data_file=existing_data_file,
            )
            original_existing_machine_record_count = len(existing_machine_records)
            if self._machine_records_need_full_day_backfill(existing_machine_records, machine_incoming_records):
                backfill_records = self._load_r2_full_day_machine_records(
                    store_source=store_source,
                    machine_key=machine_key,
                    existing_records=existing_machine_records + machine_incoming_records,
                )
                existing_machine_records = self._merge_minrepo_machine_records(
                    existing_machine_records,
                    backfill_records,
                )
            replaced_record_count += original_existing_machine_record_count
            merged_machine_records = self._merge_minrepo_machine_records(
                existing_machine_records,
                machine_incoming_records,
            )
            merged_machine_records = self._rewrite_machine_records_name(
                merged_machine_records,
                canonical_incoming_machine_name,
            )
            machine_payload, machine_summary = self._build_single_machine_payload(
                store_source,
                merged_machine_records,
            )
            data_file = str(machine_summary.get("dataFile", "")).strip()
            if not data_file:
                continue
            added_record_count += len(machine_payload.get("records", []))
            self.r2_storage.write_json(data_file, machine_payload)
            updated_machine_summaries_by_key[machine_key] = machine_summary

        store_payload = self._build_incremental_store_payload(
            store_source=store_source,
            machine_summaries=list(updated_machine_summaries_by_key.values()),
            record_count=max(0, existing_total_record_count - replaced_record_count + added_record_count),
        )
        store_id = self._r2_store_id(store_source.store_name, store_source.store_url)
        data_file = self._r2_store_key(store_id)
        self.r2_storage.write_json(data_file, store_payload)
        entry = build_index_store_entry(store_payload, data_file)
        update_index(
            self.root_dir / "apps" / "web" / "public" / "halldata-static",
            [entry],
            r2_storage=self.r2_storage,
            allow_missing_r2_index=False,
        )
        return entry

    def _load_r2_machine_records(self, data_file: str) -> list[dict[str, Any]]:
        if not data_file:
            return []
        machine_payload = self.r2_storage.read_json(data_file)
        if not isinstance(machine_payload, dict):
            return []
        records = machine_payload.get("records", [])
        if not isinstance(records, list):
            return []
        return [
            record
            for record in records
            if isinstance(record, dict) and _saved_record_should_be_kept(record)
        ]

    def _load_r2_equivalent_machine_records(
        self,
        *,
        store_source: StoreSource,
        machine_name: str,
        primary_data_file: str,
    ) -> list[dict[str, Any]]:
        store_id = self._r2_store_id(store_source.store_name, store_source.store_url)
        data_files: list[str] = []
        seen_data_files: set[str] = set()

        for candidate_name in list_equivalent_machine_names(machine_name):
            data_file = build_machine_data_file(store_id, candidate_name)
            if not data_file or data_file in seen_data_files:
                continue
            seen_data_files.add(data_file)
            data_files.append(data_file)

        if primary_data_file and primary_data_file not in seen_data_files:
            data_files.append(primary_data_file)
            seen_data_files.add(primary_data_file)
        elif primary_data_file:
            data_files = [data_file for data_file in data_files if data_file != primary_data_file]
            data_files.append(primary_data_file)

        records: list[dict[str, Any]] = []
        for data_file in data_files:
            records = self._merge_minrepo_machine_records(
                records,
                self._load_r2_machine_records(data_file),
            )
        return records

    def _rewrite_machine_records_name(
        self,
        records: list[dict[str, Any]],
        machine_name: str,
    ) -> list[dict[str, Any]]:
        normalized_machine_name = str(machine_name or "").strip()
        if not normalized_machine_name:
            return records
        return [
            {
                **record,
                "machine_name": normalized_machine_name,
            }
            for record in records
        ]

    def _machine_records_need_full_day_backfill(
        self,
        existing_records: list[dict[str, Any]],
        incoming_records: list[dict[str, Any]],
    ) -> bool:
        incoming_dates = {
            str(record.get("target_date", "")).strip()
            for record in incoming_records
            if str(record.get("target_date", "")).strip()
        }
        if not incoming_dates:
            return False
        existing_dates = {
            str(record.get("target_date", "")).strip()
            for record in existing_records
            if str(record.get("target_date", "")).strip()
        }
        return len(existing_dates) <= len(incoming_dates)

    def _load_r2_full_day_machine_records(
        self,
        *,
        store_source: StoreSource,
        machine_key: str,
        existing_records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        index_key = self._r2_full_day_index_key(store_source.store_name, store_source.store_url)
        index_payload = self.r2_storage.read_json(index_key)
        if not isinstance(index_payload, dict):
            return []
        full_day_dates = index_payload.get("full_day_dates", {})
        if not isinstance(full_day_dates, dict):
            return []

        existing_dates = {
            str(record.get("target_date", "")).strip()
            for record in existing_records
            if str(record.get("target_date", "")).strip()
        }
        existing_slots = {
            str(record.get("slot_number", "")).strip()
            for record in existing_records
            if str(record.get("slot_number", "")).strip()
        }
        snapshot_dates_by_key: dict[str, set[str]] = {}
        for target_date, raw_entry in full_day_dates.items():
            normalized_date = str(target_date).strip()
            if not normalized_date or normalized_date in existing_dates:
                continue
            entry = raw_entry if isinstance(raw_entry, dict) else {}
            snapshot_key = str(entry.get("snapshot_key", "")).strip()
            if not snapshot_key:
                continue
            snapshot_dates_by_key.setdefault(snapshot_key, set()).add(normalized_date)

        records_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for snapshot_key, target_dates in snapshot_dates_by_key.items():
            snapshot = self.r2_storage.read_json(snapshot_key)
            if not isinstance(snapshot, dict):
                continue
            for record in snapshot.get("records", []):
                if not isinstance(record, dict) or not _saved_record_should_be_kept(record):
                    continue
                target_date = str(record.get("target_date", "")).strip()
                if target_date not in target_dates:
                    continue
                record_machine_key = normalize_machine_name_key(str(record.get("machine_name", "")).strip())
                if record_machine_key != machine_key:
                    continue
                slot_number = str(record.get("slot_number", "")).strip()
                if existing_slots and slot_number not in existing_slots:
                    continue
                key = self._record_replace_key(record)
                if key is not None:
                    records_by_key[key] = record
        return list(records_by_key.values())

    def _merge_minrepo_machine_records(
        self,
        existing_records: list[dict[str, Any]],
        incoming_records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        records_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for record in existing_records:
            key = self._record_replace_key(record)
            if key is not None:
                records_by_key[key] = record
        for record in incoming_records:
            key = self._record_replace_key(record)
            if key is not None:
                records_by_key[key] = record
        return list(records_by_key.values())

    def _build_single_machine_payload(
        self,
        store_source: StoreSource,
        records: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        store_payload = build_store_payload(store_source, records)
        machine_records_by_file = store_payload.get("_machineRecordsByFile", {})
        machines = [
            machine
            for machine in store_payload.get("machines", [])
            if isinstance(machine, dict)
        ]
        if not machines:
            return {}, {}
        machine_summary = machines[0]
        data_file = str(machine_summary.get("dataFile", "")).strip()
        machine_payload = (
            machine_records_by_file.get(data_file, {})
            if isinstance(machine_records_by_file, dict)
            else {}
        )
        return (
            machine_payload if isinstance(machine_payload, dict) else {},
            machine_summary,
        )

    def _build_incremental_store_payload(
        self,
        *,
        store_source: StoreSource,
        machine_summaries: list[dict[str, Any]],
        record_count: int,
    ) -> dict[str, Any]:
        generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
        store_id = self._r2_store_id(store_source.store_name, store_source.store_url)
        normalized_machine_summaries = [
            dict(machine)
            for machine in machine_summaries
            if str(machine.get("machineName", "")).strip()
        ]
        normalized_machine_summaries.sort(
            key=lambda machine: (
                str(machine.get("latestDate") or ""),
                int(machine.get("slotCount") or 0),
                str(machine.get("machineName") or ""),
            ),
            reverse=True,
        )
        latest_date = max(
            (str(machine.get("latestDate") or "") for machine in normalized_machine_summaries),
            default=None,
        )
        return {
            "version": WEB_DATA_VERSION,
            "generatedAt": generated_at,
            "store": {
                "id": store_id,
                "legacyIds": sorted(store_source.legacy_ids),
                "storeName": store_source.store_name,
                "storeUrl": normalize_store_url(store_source.store_url),
                "prefectureName": str(store_source.prefecture_name or "").strip(),
                "areaName": str(store_source.area_name or "").strip(),
                "eventDayTails": _normalize_event_values(store_source.event_day_tails, 0, 9),
                "eventMonthDays": _normalize_event_values(store_source.event_month_days, 1, 31),
                "eventZoro": bool(store_source.event_zoro),
                "eventWeekdays": _normalize_event_values(store_source.event_weekdays, 0, 6),
                "eventSourceText": str(store_source.event_source_text or "").strip(),
            },
            "summary": {
                "machineCount": len(normalized_machine_summaries),
                "latestDate": latest_date,
                "recordCount": record_count,
            },
            "machines": normalized_machine_summaries,
        }

    def _read_store_record_count(self, store_payload: dict[str, Any]) -> int:
        summary = store_payload.get("summary", {})
        if not isinstance(summary, dict):
            return 0
        try:
            return int(summary.get("recordCount") or 0)
        except (TypeError, ValueError):
            return 0

    def _merge_r2_records(
        self,
        existing_records: list[dict[str, Any]],
        incoming_records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        records_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for record in existing_records:
            if not _saved_record_should_be_kept(record):
                continue
            key = self._record_replace_key(record)
            if key is not None:
                records_by_key[key] = record

        for record in incoming_records:
            if not _saved_record_should_be_kept(record):
                continue
            key = self._record_replace_key(record)
            if key is None:
                continue
            existing_record = records_by_key.get(key)
            if existing_record is None or self._incoming_record_should_replace(existing_record, record):
                records_by_key[key] = record

        return list(records_by_key.values())

    def _record_replace_key(self, record: dict[str, Any]) -> tuple[str, str] | None:
        target_date = str(record.get("target_date", "")).strip()
        slot_number = str(record.get("slot_number", "")).strip()
        if not target_date or not slot_number:
            return None
        return target_date, slot_number

    def _incoming_record_should_replace(self, existing_record: dict[str, Any], incoming_record: dict[str, Any]) -> bool:
        existing_source = _infer_saved_result_data_source(existing_record)
        incoming_source = _infer_saved_result_data_source(incoming_record)
        if existing_source == DATA_SOURCE_MINREPO and incoming_source == DATA_SOURCE_SITE7:
            return False
        return True

    def _load_r2_index_payload(self) -> dict[str, Any]:
        payload = self.r2_storage.read_json("index.json")
        if isinstance(payload, dict):
            return payload
        return {"version": WEB_DATA_VERSION, "stores": []}

    def _find_r2_store_entry(self, *, store_name: str, store_url: str) -> dict[str, Any] | None:
        payload = self._load_r2_index_payload()
        stores = payload.get("stores", [])
        if not isinstance(stores, list):
            return None

        normalized_url = normalize_store_url(store_url)
        requested_id = self._r2_store_id(store_name, normalized_url) if (store_name or normalized_url) else ""
        normalized_name = normalize_store_name_key(store_name) if store_name else ""
        for store in stores:
            if not isinstance(store, dict):
                continue
            store_id = str(store.get("id", "")).strip()
            legacy_ids = {str(legacy_id).strip() for legacy_id in store.get("legacyIds", []) if str(legacy_id).strip()}
            store_entry_url = normalize_store_url(str(store.get("storeUrl", "")).strip())
            store_entry_name = normalize_store_name_key(str(store.get("storeName", "")).strip())
            if requested_id and (store_id == requested_id or requested_id in legacy_ids):
                return store
            if normalized_url and store_entry_url == normalized_url:
                return store
            if normalized_name and store_entry_name == normalized_name:
                return store
        return None

    def _load_r2_store_payload(self, *, store_name: str, store_url: str) -> dict[str, Any] | None:
        entry = self._find_r2_store_entry(store_name=store_name, store_url=store_url)
        data_file = str(entry.get("dataFile", "")).strip() if isinstance(entry, dict) else ""
        if not data_file:
            store_id = self._r2_store_id(store_name, store_url)
            data_file = self._r2_store_key(store_id)
        payload = self.r2_storage.read_json(data_file)
        return payload if isinstance(payload, dict) else None

    def _load_r2_store_records(
        self,
        *,
        store_name: str,
        store_url: str,
        include_empty_site7: bool = False,
    ) -> list[dict[str, Any]]:
        store_payload = self._load_r2_store_payload(store_name=store_name, store_url=store_url)
        if not store_payload:
            return []

        records: list[dict[str, Any]] = []
        for record in store_payload.get("records", []):
            if isinstance(record, dict) and (include_empty_site7 or _saved_record_should_be_kept(record)):
                records.append(record)

        for machine in store_payload.get("machines", []):
            if not isinstance(machine, dict):
                continue
            data_file = str(machine.get("dataFile", "")).strip()
            if not data_file:
                continue
            machine_payload = self.r2_storage.read_json(data_file)
            if not isinstance(machine_payload, dict):
                continue
            for record in machine_payload.get("records", []):
                if isinstance(record, dict) and (include_empty_site7 or _saved_record_should_be_kept(record)):
                    records.append(record)
        return records

    def _mark_full_day_saved_r2(
        self,
        snapshot: dict[str, Any],
        snapshot_key: str,
        *,
        verified_current_counts_by_date: dict[str, dict[str, int]] | None = None,
        verified_site7_dates: set[str] | None = None,
    ) -> None:
        store = snapshot.get("store", {})
        if not isinstance(store, dict):
            return

        store_name = str(store.get("store_name", "")).strip()
        store_url = normalize_store_url(str(store.get("store_url", "")))
        if not store_name and not store_url:
            return

        index_key = self._r2_full_day_index_key(store_name, store_url)
        index_payload = self.r2_storage.read_json(index_key) or {"version": 1, "store": {}, "full_day_dates": {}}
        if not isinstance(index_payload, dict):
            index_payload = {"version": 1, "store": {}, "full_day_dates": {}}

        index_store = {
            "store_name": store_name,
            "store_url": store_url,
        }
        event_day_tails = _normalize_event_values(store.get("event_day_tails", []), 0, 9)
        event_month_days = _normalize_event_values(store.get("event_month_days", []), 1, 31)
        event_zoro = _coerce_bool(store.get("event_zoro", False))
        event_weekdays = _normalize_event_values(store.get("event_weekdays", []), 0, 6)
        event_source_text = str(store.get("event_source_text", "")).strip()
        if event_day_tails:
            index_store["event_day_tails"] = event_day_tails
        if event_month_days:
            index_store["event_month_days"] = event_month_days
        if event_zoro:
            index_store["event_zoro"] = True
        if event_weekdays:
            index_store["event_weekdays"] = event_weekdays
        if event_source_text:
            index_store["event_source_text"] = event_source_text
        index_payload["store"] = index_store
        full_day_dates = index_payload.setdefault("full_day_dates", {})
        if not isinstance(full_day_dates, dict):
            full_day_dates = {}
            index_payload["full_day_dates"] = full_day_dates

        now_text = datetime.now().astimezone().isoformat(timespec="seconds")
        saved_counts_by_date = self._full_day_saved_counts_by_date(snapshot)
        source_counts_by_date = self._full_day_source_counts_by_date(snapshot)
        target_dates: list[str] = []
        for date_page in snapshot.get("date_pages", []):
            if not isinstance(date_page, dict):
                continue
            target_date = str(date_page.get("target_date", "")).strip()
            if not target_date:
                continue
            target_dates.append(target_date)

        if verified_current_counts_by_date is not None and verified_site7_dates is not None:
            current_counts_by_date = verified_current_counts_by_date
            current_site7_dates = verified_site7_dates
        else:
            current_counts_by_date, current_site7_dates = self._full_day_current_saved_state_by_date(
                store_name=store_name,
                store_url=store_url,
                target_dates=set(target_dates),
            )

        for target_date in target_dates:
            saved_counts = saved_counts_by_date.get(target_date, {"machine_count": 0, "record_count": 0})
            source_counts = source_counts_by_date.get(target_date, {})
            current_counts = current_counts_by_date.get(target_date, {"machine_count": 0, "record_count": 0})
            if (
                source_counts.get(DATA_SOURCE_SITE7, 0) > 0
                or target_date in current_site7_dates
                or saved_counts["record_count"] <= 0
                or current_counts["record_count"] < saved_counts["record_count"]
                or current_counts["machine_count"] < saved_counts["machine_count"]
            ):
                full_day_dates.pop(target_date, None)
                continue

            full_day_dates[target_date] = {
                "saved_at": now_text,
                "machine_count": current_counts["machine_count"],
                "record_count": current_counts["record_count"],
                "snapshot_key": snapshot_key,
                "data_source": DATA_SOURCE_MINREPO,
            }

        self.r2_storage.write_json(index_key, index_payload)

    def _full_day_site7_cleanup_target_stores(
        self,
        *,
        store_name: str = "",
        store_url: str = "",
    ) -> list[dict[str, str]]:
        normalized_store_url = normalize_store_url(store_url)
        if store_name or normalized_store_url:
            return [{"store_name": str(store_name).strip(), "store_url": normalized_store_url}]

        payload = self._load_r2_index_payload()
        stores = payload.get("stores", [])
        if not isinstance(stores, list):
            return []

        target_stores: list[dict[str, str]] = []
        seen_keys: set[tuple[str, str]] = set()
        for store in stores:
            if not isinstance(store, dict):
                continue
            current_store_name = str(store.get("storeName", store.get("store_name", ""))).strip()
            current_store_url = normalize_store_url(str(store.get("storeUrl", store.get("store_url", ""))))
            if not current_store_name and not current_store_url:
                continue
            key = (normalize_store_name_key(current_store_name), current_store_url)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            target_stores.append({"store_name": current_store_name, "store_url": current_store_url})
        return target_stores

    def _clear_full_day_saved_r2_for_snapshot_site7_dates(self, snapshot: dict[str, Any]) -> list[str]:
        site7_dates = self._full_day_snapshot_site7_dates(snapshot)
        if not site7_dates:
            return []

        store_source = self._r2_store_source_from_snapshot(snapshot)
        return self._clear_full_day_saved_r2_for_current_site7_dates(
            store_name=store_source.store_name,
            store_url=store_source.store_url,
            candidate_dates=site7_dates,
        )

    def _clear_full_day_saved_r2_for_current_site7_dates(
        self,
        *,
        store_name: str,
        store_url: str,
        candidate_dates: set[str],
    ) -> list[str]:
        normalized_dates = {str(target_date).strip() for target_date in candidate_dates if str(target_date).strip()}
        if not normalized_dates:
            return []

        index_key = self._r2_full_day_index_key(store_name, store_url)
        index_payload = self.r2_storage.read_json(index_key)
        if not isinstance(index_payload, dict):
            return []

        full_day_dates = index_payload.get("full_day_dates", {})
        if not isinstance(full_day_dates, dict):
            return []

        indexed_dates = {target_date for target_date in normalized_dates if target_date in full_day_dates}
        if not indexed_dates:
            return []

        current_counts_by_date, current_site7_dates = self._full_day_current_saved_state_by_date(
            store_name=store_name,
            store_url=store_url,
            target_dates=indexed_dates,
        )
        removed_dates = [
            target_date
            for target_date in sorted(indexed_dates.intersection(current_site7_dates))
            if self._full_day_index_entry_should_clear_for_site7(
                full_day_dates.get(target_date),
                current_counts_by_date.get(target_date, {"machine_count": 0, "record_count": 0}),
            )
        ]
        if not removed_dates:
            return []

        for target_date in removed_dates:
            full_day_dates.pop(target_date, None)
        self.r2_storage.write_json(index_key, index_payload)
        return removed_dates

    def _full_day_index_entry_should_clear_for_site7(
        self,
        entry: Any,
        current_minrepo_counts: dict[str, int],
    ) -> bool:
        if not isinstance(entry, dict):
            return True

        data_source = str(entry.get("data_source", "")).strip()
        if data_source and data_source != DATA_SOURCE_MINREPO:
            return True
        if entry.get("has_site7_records") is True:
            return True

        indexed_record_count = self._coerce_saved_full_day_record_count(entry.get("record_count"))
        if indexed_record_count is not None and current_minrepo_counts.get("record_count", 0) < indexed_record_count:
            return True

        indexed_machine_count = self._coerce_saved_full_day_machine_count(entry.get("machine_count"))
        if indexed_machine_count is not None and current_minrepo_counts.get("machine_count", 0) < indexed_machine_count:
            return True

        return False

    def resolve_preferred_store_by_name(self, store_name: str) -> dict[str, str] | None:
        store_name_key = normalize_store_name_key(store_name)
        if not store_name_key:
            return None

        candidates: list[dict[str, Any]] = []
        for store in self.load_registered_stores():
            candidate_name = str(store.get("store_name", "")).strip()
            if normalize_store_name_key(candidate_name) != store_name_key:
                continue
            candidate_url = normalize_store_url(str(store.get("store_url", "")).strip())
            if not candidate_url:
                continue
            candidates.append(
                {
                    "store_name": candidate_name,
                    "store_url": candidate_url,
                    "record_count": len(
                        self._iter_r2_store_records(
                            store_name=candidate_name,
                            store_url=candidate_url,
                            start_date="0000-00-00",
                            end_date="9999-99-99",
                        )
                    ),
                }
            )
        return choose_preferred_store(candidates)

    def _save_local_snapshot(self, snapshot: dict[str, Any]) -> Path:
        local_dir = self._local_save_dir()
        store_name = str(snapshot["store"]["store_name"])
        store_dir = local_dir / _sanitize_file_name(store_name)
        store_dir.mkdir(parents=True, exist_ok=True)

        period = snapshot["period"]
        file_name = (
            f"{period['start_date']}_{period['end_date']}_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.json"
        )
        file_path = store_dir / file_name
        file_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
        return file_path

    def _mark_full_day_saved(self, snapshot: dict[str, Any], local_path: Path) -> None:
        store = snapshot.get("store", {})
        if not isinstance(store, dict):
            return

        store_name = str(store.get("store_name", "")).strip()
        if not store_name:
            return

        index_path = self._full_day_index_path(store_name)
        index_payload = self._load_full_day_index(index_path)
        index_store = {
            "store_name": store_name,
            "store_url": normalize_store_url(str(store.get("store_url", ""))),
        }
        event_day_tails = _normalize_event_values(store.get("event_day_tails", []), 0, 9)
        event_month_days = _normalize_event_values(store.get("event_month_days", []), 1, 31)
        event_zoro = _coerce_bool(store.get("event_zoro", False))
        event_weekdays = _normalize_event_values(store.get("event_weekdays", []), 0, 6)
        event_source_text = str(store.get("event_source_text", "")).strip()
        if event_day_tails:
            index_store["event_day_tails"] = event_day_tails
        if event_month_days:
            index_store["event_month_days"] = event_month_days
        if event_zoro:
            index_store["event_zoro"] = True
        if event_weekdays:
            index_store["event_weekdays"] = event_weekdays
        if event_source_text:
            index_store["event_source_text"] = event_source_text
        index_payload["store"] = index_store
        full_day_dates = index_payload.setdefault("full_day_dates", {})
        if not isinstance(full_day_dates, dict):
            full_day_dates = {}
            index_payload["full_day_dates"] = full_day_dates

        now_text = datetime.now().astimezone().isoformat(timespec="seconds")
        saved_counts_by_date = self._full_day_saved_counts_by_date(snapshot)
        for date_page in snapshot.get("date_pages", []):
            if not isinstance(date_page, dict):
                continue
            target_date = str(date_page.get("target_date", "")).strip()
            if not target_date:
                continue
            saved_counts = saved_counts_by_date.get(target_date, {"machine_count": 0, "record_count": 0})
            full_day_dates[target_date] = {
                "saved_at": now_text,
                "machine_count": saved_counts["machine_count"],
                "record_count": saved_counts["record_count"],
                "local_file_path": str(local_path),
            }

        index_path.parent.mkdir(parents=True, exist_ok=True)
        index_path.write_text(json.dumps(index_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _find_saved_full_day_dates_local(
        self,
        store_name: str,
        store_url: str,
        start_date: str,
        end_date: str,
    ) -> set[str]:
        return set(
            self._find_saved_full_day_date_entries_local(
                store_name=store_name,
                store_url=store_url,
                start_date=start_date,
                end_date=end_date,
            )
        )

    def _find_saved_full_day_date_entries_r2(
        self,
        store_name: str,
        store_url: str,
        start_date: str,
        end_date: str,
    ) -> dict[str, dict[str, Any]]:
        index_key = self._r2_full_day_index_key(store_name, store_url)
        payload = self.r2_storage.read_json(index_key)
        if not isinstance(payload, dict):
            return {}

        store_payload = payload.get("store", {})
        if isinstance(store_payload, dict):
            saved_store_url = normalize_store_url(str(store_payload.get("store_url", "")).strip())
            if saved_store_url and saved_store_url != normalize_store_url(store_url):
                return {}

        full_day_dates = payload.get("full_day_dates", {})
        if not isinstance(full_day_dates, dict):
            return {}

        saved_date_entries: dict[str, dict[str, Any]] = {}
        for target_date, entry in full_day_dates.items():
            target_date_text = str(target_date).strip()
            if not target_date_text or target_date_text < start_date or target_date_text > end_date:
                continue
            saved_date_entries[target_date_text] = entry if isinstance(entry, dict) else {}
        return saved_date_entries

    def _iter_r2_store_records(
        self,
        *,
        store_name: str = "",
        store_url: str = "",
        start_date: str,
        end_date: str,
        include_empty_site7: bool = False,
    ) -> list[dict[str, Any]]:
        records = self._load_r2_store_records(
            store_name=store_name,
            store_url=store_url,
            include_empty_site7=include_empty_site7,
        )
        filtered_records: list[dict[str, Any]] = []
        for row in records:
            target_date = str(row.get("target_date", "")).strip()
            if not target_date or target_date < start_date or target_date > end_date:
                continue
            filtered_records.append(row)
        return filtered_records

    def _find_saved_machine_target_sources_r2(
        self,
        store_name: str,
        store_url: str,
        start_date: str,
        end_date: str,
        target_machine_names: set[str],
    ) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
        protected_targets: set[tuple[str, str]] = set()
        replaceable_targets: set[tuple[str, str]] = set()
        for row in self._iter_r2_store_records(
            store_name=store_name,
            store_url=store_url,
            start_date=start_date,
            end_date=end_date,
            include_empty_site7=True,
        ):
            target_date = str(row.get("target_date", "")).strip()
            machine_name = normalize_machine_name_key(str(row.get("machine_name", "")).strip())
            if not target_date or machine_name not in target_machine_names:
                continue

            target_key = (target_date, machine_name)
            if _infer_saved_result_data_source(row) == DATA_SOURCE_SITE7:
                if target_key not in protected_targets:
                    replaceable_targets.add(target_key)
                continue

            protected_targets.add(target_key)
            replaceable_targets.discard(target_key)

        return protected_targets, replaceable_targets

    def _find_saved_machine_slot_sources_r2(
        self,
        store_name: str,
        store_url: str,
        start_date: str,
        end_date: str,
        target_slot_numbers: set[str],
        require_source_difference: bool = True,
        site7_updated_at: str | datetime | None = None,
    ) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
        protected_slots: set[tuple[str, str]] = set()
        replaceable_slots: set[tuple[str, str]] = set()
        for row in self._iter_r2_store_records(
            store_name=store_name,
            store_url=store_url,
            start_date=start_date,
            end_date=end_date,
            include_empty_site7=True,
        ):
            target_date = str(row.get("target_date", "")).strip()
            slot_number = str(row.get("slot_number", "")).strip()
            if not target_date or slot_number not in target_slot_numbers:
                continue

            target_key = (target_date, slot_number)
            if _infer_saved_result_data_source(row) == DATA_SOURCE_SITE7:
                if _site7_record_has_complete_fetch_data(
                    row,
                    require_source_difference=require_source_difference,
                ) and _site7_record_update_is_current_or_newer(row, site7_updated_at):
                    protected_slots.add(target_key)
                    replaceable_slots.discard(target_key)
                elif target_key not in protected_slots:
                    replaceable_slots.add(target_key)
                continue

            protected_slots.add(target_key)
            replaceable_slots.discard(target_key)

        return protected_slots, replaceable_slots

    def _find_saved_full_day_date_entries_local(
        self,
        store_name: str,
        store_url: str,
        start_date: str,
        end_date: str,
    ) -> dict[str, dict[str, Any]]:
        index_path = self._full_day_index_path(store_name)
        if not index_path.exists():
            return {}

        payload = self._load_full_day_index(index_path)
        store_payload = payload.get("store", {})
        if isinstance(store_payload, dict):
            saved_store_url = normalize_store_url(str(store_payload.get("store_url", "")).strip())
            if saved_store_url and saved_store_url != normalize_store_url(store_url):
                return {}

        full_day_dates = payload.get("full_day_dates", {})
        if not isinstance(full_day_dates, dict):
            return {}

        saved_date_entries: dict[str, dict[str, Any]] = {}
        for target_date, entry in full_day_dates.items():
            target_date_text = str(target_date).strip()
            if not target_date_text or target_date_text < start_date or target_date_text > end_date:
                continue
            if isinstance(entry, dict):
                saved_date_entries[target_date_text] = entry
            else:
                saved_date_entries[target_date_text] = {}

        return saved_date_entries

    def _find_saved_full_day_dates_from_supabase(
        self,
        store_url: str,
        start_date: str,
        end_date: str,
        saved_date_entries: dict[str, dict[str, Any]],
    ) -> set[str]:
        return set()

        if not saved_date_entries:
            return set()

        supabase_url, _, schema, stores_table, _ = self._supabase_config()
        session = self._create_supabase_session(schema)
        store_id = self._find_store_id(session, supabase_url, stores_table, normalize_store_url(store_url))
        if not store_id:
            return set()

        saved_detail_counts = self._fetch_saved_full_day_detail_counts_by_date(
            session=session,
            supabase_url=supabase_url,
            machine_daily_details_table=self._machine_daily_details_table(),
            store_id=store_id,
            start_date=start_date,
            end_date=end_date,
        )
        verified_dates: set[str] = set()
        for target_date, saved_entry in saved_date_entries.items():
            saved_machine_count = self._coerce_saved_full_day_machine_count(saved_entry.get("machine_count"))
            actual_detail_count = saved_detail_counts.get(target_date, 0)
            if saved_machine_count is None:
                if actual_detail_count > 0:
                    verified_dates.add(target_date)
                continue
            if actual_detail_count == saved_machine_count:
                verified_dates.add(target_date)

        return verified_dates

    def _fetch_saved_full_day_detail_counts_by_date(
        self,
        session: requests.Session,
        supabase_url: str,
        machine_daily_details_table: str,
        store_id: str,
        start_date: str,
        end_date: str,
    ) -> dict[str, int]:
        detail_counts: dict[str, int] = {}
        offset = 0
        page_size = 1000
        endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{quote(machine_daily_details_table, safe='')}"

        while True:
            try:
                response = session.get(
                    endpoint,
                    params={
                        "select": "target_date,machine_name",
                        "store_id": f"eq.{store_id}",
                        "target_date": [f"gte.{start_date}", f"lte.{end_date}"],
                        "order": "target_date.asc,machine_name.asc",
                        "limit": str(page_size),
                        "offset": str(offset),
                    },
                    timeout=30,
                )
                response.raise_for_status()
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code in {400, 404}:
                    raise RuntimeError(
                        "台データページ用の詳細テーブルがありません。"
                        " 追加用SQLを適用してから再度保存してください。"
                    ) from exc
                raise

            rows = response.json()
            if not rows:
                break

            for row in rows:
                if not isinstance(row, dict):
                    continue
                target_date = str(row.get("target_date", "")).strip()
                machine_name = str(row.get("machine_name", "")).strip()
                if not target_date or not machine_name:
                    continue
                detail_counts[target_date] = detail_counts.get(target_date, 0) + 1

            if len(rows) < page_size:
                break
            offset += page_size

        return detail_counts

    def _coerce_saved_full_day_machine_count(self, value: Any) -> int | None:
        try:
            machine_count = int(value)
        except (TypeError, ValueError):
            return None
        if machine_count < 0:
            return None
        return machine_count

    def _coerce_saved_full_day_record_count(self, value: Any) -> int | None:
        try:
            record_count = int(value)
        except (TypeError, ValueError):
            return None
        if record_count < 0:
            return None
        return record_count

    def _full_day_saved_counts_by_date(self, snapshot: dict[str, Any]) -> dict[str, dict[str, int]]:
        counts_by_date: dict[str, dict[str, int]] = {}
        machine_names_by_date: dict[str, set[str]] = {}
        records = snapshot.get("records", [])
        if not isinstance(records, list):
            return counts_by_date

        records_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for record in records:
            if not isinstance(record, dict):
                continue
            if not _saved_record_should_be_kept(record):
                continue
            key = self._record_replace_key(record)
            if key is None:
                continue
            existing_record = records_by_key.get(key)
            if existing_record is None or self._incoming_record_should_replace(existing_record, record):
                records_by_key[key] = record

        for record in records_by_key.values():
            target_date = str(record.get("target_date", "")).strip()
            if not target_date:
                continue
            counts = counts_by_date.setdefault(target_date, {"machine_count": 0, "record_count": 0})
            counts["record_count"] += 1

            machine_name = str(record.get("machine_name", "")).strip()
            if machine_name:
                machine_names_by_date.setdefault(target_date, set()).add(machine_name)

        for target_date, machine_names in machine_names_by_date.items():
            counts_by_date.setdefault(target_date, {"machine_count": 0, "record_count": 0})
            counts_by_date[target_date]["machine_count"] = len(machine_names)

        return counts_by_date

    def _full_day_source_counts_by_date(self, snapshot: dict[str, Any]) -> dict[str, dict[str, int]]:
        counts_by_date: dict[str, dict[str, int]] = {}
        records = snapshot.get("records", [])
        if not isinstance(records, list):
            return counts_by_date

        for record in records:
            if not isinstance(record, dict):
                continue
            target_date = str(record.get("target_date", "")).strip()
            if not target_date:
                continue
            data_source = DATA_SOURCE_SITE7 if _record_has_site7_source(record) else DATA_SOURCE_MINREPO
            counts = counts_by_date.setdefault(
                target_date,
                {DATA_SOURCE_MINREPO: 0, DATA_SOURCE_SITE7: 0},
            )
            counts[data_source] = counts.get(data_source, 0) + 1
        return counts_by_date

    def _full_day_snapshot_site7_dates(self, snapshot: dict[str, Any]) -> set[str]:
        return {
            target_date
            for target_date, source_counts in self._full_day_source_counts_by_date(snapshot).items()
            if source_counts.get(DATA_SOURCE_SITE7, 0) > 0
        }

    def _full_day_current_saved_state_by_date(
        self,
        *,
        store_name: str,
        store_url: str,
        target_dates: set[str],
    ) -> tuple[dict[str, dict[str, int]], set[str]]:
        normalized_dates = {str(target_date).strip() for target_date in target_dates if str(target_date).strip()}
        if not normalized_dates:
            return {}, set()

        counts_by_date: dict[str, dict[str, int]] = {}
        machine_names_by_date: dict[str, set[str]] = {}
        site7_dates: set[str] = set()
        for record in self._iter_r2_store_records(
            store_name=store_name,
            store_url=store_url,
            start_date=min(normalized_dates),
            end_date=max(normalized_dates),
            include_empty_site7=True,
        ):
            target_date = str(record.get("target_date", "")).strip()
            if target_date not in normalized_dates:
                continue
            if _record_has_site7_source(record):
                site7_dates.add(target_date)
                continue

            counts = counts_by_date.setdefault(target_date, {"machine_count": 0, "record_count": 0})
            counts["record_count"] += 1
            machine_name = str(record.get("machine_name", "")).strip()
            if machine_name:
                machine_names_by_date.setdefault(target_date, set()).add(machine_name)

        for target_date, machine_names in machine_names_by_date.items():
            counts_by_date.setdefault(target_date, {"machine_count": 0, "record_count": 0})
            counts_by_date[target_date]["machine_count"] = len(machine_names)
        return counts_by_date, site7_dates

    def _find_incomplete_full_day_dates(
        self,
        saved_date_entries: dict[str, dict[str, Any]],
    ) -> set[str]:
        record_counts: dict[str, int] = {}
        machine_counts: dict[str, int] = {}
        incomplete_dates: set[str] = set()
        for target_date, entry in saved_date_entries.items():
            if not isinstance(entry, dict):
                continue
            data_source = str(entry.get("data_source", "")).strip()
            if data_source and data_source != DATA_SOURCE_MINREPO:
                incomplete_dates.add(target_date)
            if entry.get("has_site7_records") is True:
                incomplete_dates.add(target_date)
            record_count = self._coerce_saved_full_day_record_count(entry.get("record_count"))
            if record_count is not None:
                record_counts[target_date] = record_count
            machine_count = self._coerce_saved_full_day_machine_count(entry.get("machine_count"))
            if machine_count is not None:
                machine_counts[target_date] = machine_count

        incomplete_dates.update(
            self._find_low_saved_count_dates(
                record_counts,
                ratio=FULL_DAY_INCOMPLETE_RECORD_RATIO,
                min_reference_count=FULL_DAY_INCOMPLETE_MIN_REFERENCE_RECORD_COUNT,
            )
        )
        incomplete_dates.update(
            self._find_low_saved_count_dates(
                machine_counts,
                ratio=FULL_DAY_INCOMPLETE_MACHINE_RATIO,
                min_reference_count=FULL_DAY_INCOMPLETE_MIN_REFERENCE_MACHINE_COUNT,
            )
        )
        return incomplete_dates

    def _find_low_saved_count_dates(
        self,
        counts_by_date: dict[str, int],
        *,
        ratio: float,
        min_reference_count: int,
    ) -> set[str]:
        if len(counts_by_date) < 2:
            return set()

        sorted_counts = sorted(counts_by_date.values())
        reference_count = sorted_counts[len(sorted_counts) // 2] if len(sorted_counts) >= 3 else sorted_counts[-1]
        if reference_count < min_reference_count:
            return set()

        threshold = int(reference_count * ratio)
        return {
            target_date
            for target_date, count in counts_by_date.items()
            if count < threshold
        }

    def _full_day_index_path(self, store_name: str) -> Path:
        return self._local_save_dir() / _sanitize_file_name(store_name) / "_full_day_index.json"

    def _load_full_day_index(self, index_path: Path) -> dict[str, Any]:
        if not index_path.exists():
            return {"version": 1, "store": {}, "full_day_dates": {}}

        payload = json.loads(index_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return {"version": 1, "store": {}, "full_day_dates": {}}
        payload.setdefault("version", 1)
        payload.setdefault("store", {})
        payload.setdefault("full_day_dates", {})
        return payload

    def _registered_stores_path(self) -> Path:
        return self._local_save_dir() / REGISTERED_STORES_FILE_NAME

    def _save_registered_stores_local(
        self,
        stores: list[dict[str, Any]],
        excluded_store_urls: set[str] | None = None,
    ) -> int:
        normalized_stores = self._normalize_registered_stores(stores)
        saved_store_urls = {
            normalize_store_url(str(store.get("store_url", "")))
            for store in normalized_stores
            if normalize_store_url(str(store.get("store_url", "")))
        }
        normalized_excluded_store_urls = {
            normalize_store_url(store_url)
            for store_url in (excluded_store_urls if excluded_store_urls is not None else self._load_registered_store_excluded_urls())
            if normalize_store_url(store_url)
        }
        normalized_excluded_store_urls.difference_update(saved_store_urls)

        payload = {
            "version": 1,
            "saved_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "stores": normalized_stores,
        }
        if normalized_excluded_store_urls:
            payload[REGISTERED_STORE_EXCLUDED_URLS_KEY] = sorted(normalized_excluded_store_urls)

        path = self._registered_stores_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return len(normalized_stores)

    def _load_registered_stores_local(self) -> list[dict[str, Any]]:
        payload = self._load_registered_stores_payload()
        stores = payload.get("stores", [])
        return self._normalize_registered_stores(stores if isinstance(stores, list) else [])

    def _load_registered_stores_payload(self) -> dict[str, Any]:
        path = self._registered_stores_path()
        if not path.exists():
            return {}

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
        if not isinstance(payload, dict):
            return {}
        return payload

    def _load_registered_store_excluded_urls(self) -> set[str]:
        payload = self._load_registered_stores_payload()
        raw_urls = payload.get(REGISTERED_STORE_EXCLUDED_URLS_KEY, [])
        if not isinstance(raw_urls, list):
            return set()
        return {
            normalized_url
            for raw_url in raw_urls
            if (normalized_url := normalize_store_url(str(raw_url)))
        }

    def _merge_registered_store_sources(
        self,
        primary_stores: list[dict[str, Any]],
        fallback_stores: list[dict[str, Any]],
        excluded_store_urls: set[str],
    ) -> list[dict[str, Any]]:
        excluded_urls = {
            normalized_url
            for store_url in excluded_store_urls
            if (normalized_url := normalize_store_url(store_url))
        }
        merged_stores: list[dict[str, Any]] = []
        seen_store_urls: set[str] = set()

        for source_stores in (primary_stores, fallback_stores):
            for store in self._normalize_registered_stores(source_stores):
                store_url = normalize_store_url(str(store.get("store_url", "")))
                if not store_url or store_url in excluded_urls or store_url in seen_store_urls:
                    continue
                seen_store_urls.add(store_url)
                merged_stores.append(store)

        return merged_stores

    def _load_registered_stores_from_static_web_data(self) -> list[dict[str, Any]]:
        if not self.r2_storage.is_configured:
            return []

        try:
            payload = self.r2_storage.read_json("index.json")
        except R2StorageError as exc:
            self._mark_registered_store_index_load_failed(f"R2のindex.json読込に失敗しました。\n{exc}")
            return []
        if payload is None:
            self._mark_registered_store_index_load_failed("R2のindex.jsonが見つかりませんでした。")
            return []
        if not isinstance(payload, dict):
            self._mark_registered_store_index_load_failed("R2のindex.jsonの形式が不正です。")
            return []

        stores = []
        for store in payload.get("stores", []):
            if not isinstance(store, dict):
                continue
            store_name = str(store.get("storeName", "")).strip()
            store_url = str(store.get("storeUrl", "")).strip()
            if not store_url:
                continue
            stores.append(
                {
                    "store_name": store_name,
                    "store_url": store_url,
                    "site7_prefecture": str(
                        store.get("prefectureName")
                        or store.get("site7_prefecture")
                        or store.get("site7Prefecture")
                        or ""
                    ).strip(),
                    "site7_area": str(
                        store.get("areaName")
                        or store.get("site7_area")
                        or store.get("site7Area")
                        or ""
                    ).strip(),
                    "event_day_tails": _normalize_event_values(store.get("eventDayTails", []), 0, 9),
                    "event_month_days": _normalize_event_values(store.get("eventMonthDays", []), 1, 31),
                    "event_zoro": _coerce_bool(store.get("eventZoro", False)),
                    "event_weekdays": _normalize_event_values(store.get("eventWeekdays", []), 0, 6),
                    "event_source_text": str(store.get("eventSourceText", "")).strip(),
                }
            )
        return self._normalize_registered_stores(stores)

    def _mark_registered_store_index_load_failed(self, message: str) -> None:
        self._registered_store_index_load_failed = True
        self._registered_store_index_load_error = message

    def _registered_store_index_update_blocked_message(self) -> str:
        detail = self._registered_store_index_load_error.strip()
        message = "R2の公開用店舗一覧を確認できなかったため、欠けた一覧での上書きを防ぐ目的で更新を中止しました。"
        return f"{message}\n{detail}" if detail else message

    def _registered_store_location_for(self, store_name: str, store_url: str) -> dict[str, Any]:
        normalized_url = normalize_store_url(store_url)
        store_name_key = normalize_store_name_key(store_name)
        name_match: dict[str, str] = {}

        try:
            registered_stores = self._load_registered_stores_local()
        except Exception:  # noqa: BLE001
            registered_stores = []

        for registered_store in registered_stores:
            registered_location = {
                "prefecture_name": str(registered_store.get("site7_prefecture", "")).strip(),
                "area_name": str(registered_store.get("site7_area", "")).strip(),
            }
            event_day_tails = _normalize_event_values(registered_store.get("event_day_tails", []), 0, 9)
            event_month_days = _normalize_event_values(registered_store.get("event_month_days", []), 1, 31)
            event_zoro = _coerce_bool(registered_store.get("event_zoro", False))
            event_weekdays = _normalize_event_values(registered_store.get("event_weekdays", []), 0, 6)
            event_source_text = str(registered_store.get("event_source_text", "")).strip()
            if event_day_tails:
                registered_location["event_day_tails"] = event_day_tails
            if event_month_days:
                registered_location["event_month_days"] = event_month_days
            if event_zoro:
                registered_location["event_zoro"] = event_zoro
            if event_weekdays:
                registered_location["event_weekdays"] = event_weekdays
            if event_source_text:
                registered_location["event_source_text"] = event_source_text
            registered_url = normalize_store_url(str(registered_store.get("store_url", "")))
            if normalized_url and registered_url == normalized_url:
                return registered_location

            registered_name_key = normalize_store_name_key(str(registered_store.get("store_name", "")))
            if store_name_key and registered_name_key == store_name_key:
                name_match = registered_location

        return name_match

    def _save_registered_stores_to_r2_web_data(self, stores: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not self.r2_storage.is_configured:
            return []

        store_sources = [
            StoreSource(
                store_name=str(store.get("store_name", "")).strip(),
                store_url=normalize_store_url(str(store.get("store_url", ""))),
                prefecture_name=str(store.get("site7_prefecture", "")).strip(),
                area_name=str(store.get("site7_area", "")).strip(),
                event_day_tails=_normalize_event_values(store.get("event_day_tails", []), 0, 9),
                event_month_days=_normalize_event_values(store.get("event_month_days", []), 1, 31),
                event_zoro=_coerce_bool(store.get("event_zoro", False)),
                event_weekdays=_normalize_event_values(store.get("event_weekdays", []), 0, 6),
                event_source_text=str(store.get("event_source_text", "")).strip(),
            )
            for store in stores
            if normalize_store_url(str(store.get("store_url", "")))
        ]
        if not store_sources:
            return []

        return export_registered_store_payloads(
            self.root_dir / "apps" / "web" / "public" / "halldata-static",
            store_sources,
            r2_storage=self.r2_storage,
        )

    def _load_registered_stores_from_local_snapshots(self) -> list[dict[str, Any]]:
        local_dir = self._local_save_dir()
        if not local_dir.exists():
            return []

        stores: list[dict[str, Any]] = []
        for store_dir in sorted((path for path in local_dir.iterdir() if path.is_dir()), key=lambda path: path.name):
            store = self._load_registered_store_from_local_snapshot_dir(store_dir)
            if store is not None:
                stores.append(store)

        return self._normalize_registered_stores(stores)

    def _load_registered_store_from_local_snapshot_dir(self, store_dir: Path) -> dict[str, Any] | None:
        for snapshot_path in [store_dir / "_full_day_index.json", *sorted(store_dir.glob("*.json"))]:
            if snapshot_path.name == REGISTERED_STORES_FILE_NAME:
                continue
            if snapshot_path.name != "_full_day_index.json" and snapshot_path.name.startswith("_"):
                continue

            payload = self._load_json_dict(snapshot_path)
            if not payload:
                continue

            store_payload = payload.get("store", {})
            if not isinstance(store_payload, dict):
                continue

            store_name = str(store_payload.get("store_name", "")).strip() or store_dir.name
            store_url = normalize_store_url(str(store_payload.get("store_url", "")).strip())
            if not store_url:
                continue

            registered_store = {
                "store_name": store_name,
                "store_url": store_url,
            }
            event_day_tails = _normalize_event_values(store_payload.get("event_day_tails", []), 0, 9)
            event_month_days = _normalize_event_values(store_payload.get("event_month_days", []), 1, 31)
            event_zoro = _coerce_bool(store_payload.get("event_zoro", False))
            event_weekdays = _normalize_event_values(store_payload.get("event_weekdays", []), 0, 6)
            event_source_text = str(store_payload.get("event_source_text", "")).strip()
            if event_day_tails:
                registered_store["event_day_tails"] = event_day_tails
            if event_month_days:
                registered_store["event_month_days"] = event_month_days
            if event_zoro:
                registered_store["event_zoro"] = True
            if event_weekdays:
                registered_store["event_weekdays"] = event_weekdays
            if event_source_text:
                registered_store["event_source_text"] = event_source_text
            return registered_store

        return None

    def _load_json_dict(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
        return payload if isinstance(payload, dict) else {}

    def _delete_registered_stores_local(self, store_urls: list[str]) -> int:
        target_urls = {normalize_store_url(store_url) for store_url in store_urls if normalize_store_url(store_url)}
        if not target_urls:
            return 0

        stores = self.load_registered_stores()
        remaining_stores = [
            store
            for store in stores
            if normalize_store_url(str(store.get("store_url", ""))) not in target_urls
        ]
        deleted_count = len(stores) - len(remaining_stores)
        excluded_store_urls = self._load_registered_store_excluded_urls()
        excluded_store_urls.update(target_urls)
        self._save_registered_stores_local(remaining_stores, excluded_store_urls=excluded_store_urls)
        return deleted_count

    def _iter_local_snapshot_records(
        self,
        *,
        store_name: str = "",
        store_url: str = "",
        start_date: str,
        end_date: str,
    ) -> list[dict[str, Any]]:
        local_dir = self._local_save_dir()
        if not local_dir.exists():
            return []

        candidate_dirs: list[Path] = []
        if store_name.strip():
            candidate_dir = local_dir / _sanitize_file_name(store_name)
            if candidate_dir.exists():
                candidate_dirs.append(candidate_dir)
        if not candidate_dirs:
            candidate_dirs = sorted([path for path in local_dir.iterdir() if path.is_dir()])

        normalized_store_url = normalize_store_url(store_url)
        records: list[dict[str, Any]] = []
        for store_dir in candidate_dirs:
            for file_path in sorted(store_dir.glob("*.json")):
                if file_path.name in {"_full_day_index.json", REGISTERED_STORES_FILE_NAME}:
                    continue
                try:
                    payload = json.loads(file_path.read_text(encoding="utf-8"))
                except (json.JSONDecodeError, OSError):
                    continue
                if not isinstance(payload, dict):
                    continue

                snapshot_store = payload.get("store", {})
                snapshot_store_url = ""
                if isinstance(snapshot_store, dict):
                    snapshot_store_url = normalize_store_url(str(snapshot_store.get("store_url", "")))
                if normalized_store_url and snapshot_store_url and snapshot_store_url != normalized_store_url:
                    continue

                for row in payload.get("records", []):
                    if not isinstance(row, dict):
                        continue
                    target_date = str(row.get("target_date", "")).strip()
                    if not target_date or target_date < start_date or target_date > end_date:
                        continue
                    records.append(row)

        return records

    def _find_saved_machine_target_sources_local(
        self,
        store_name: str,
        store_url: str,
        start_date: str,
        end_date: str,
        target_machine_names: set[str],
    ) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
        protected_targets: set[tuple[str, str]] = set()
        replaceable_targets: set[tuple[str, str]] = set()
        for row in self._iter_local_snapshot_records(
            store_name=store_name,
            store_url=store_url,
            start_date=start_date,
            end_date=end_date,
        ):
            target_date = str(row.get("target_date", "")).strip()
            machine_name = normalize_machine_name_key(str(row.get("machine_name", "")).strip())
            if not target_date or machine_name not in target_machine_names:
                continue

            target_key = (target_date, machine_name)
            if _infer_saved_result_data_source(row) == DATA_SOURCE_SITE7:
                if target_key not in protected_targets:
                    replaceable_targets.add(target_key)
                continue

            protected_targets.add(target_key)
            replaceable_targets.discard(target_key)

        return protected_targets, replaceable_targets

    def _find_saved_machine_slot_sources_local(
        self,
        store_name: str,
        store_url: str,
        start_date: str,
        end_date: str,
        target_slot_numbers: set[str],
        require_source_difference: bool = True,
        site7_updated_at: str | datetime | None = None,
    ) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
        protected_slots: set[tuple[str, str]] = set()
        replaceable_slots: set[tuple[str, str]] = set()
        for row in self._iter_local_snapshot_records(
            store_name=store_name,
            store_url=store_url,
            start_date=start_date,
            end_date=end_date,
        ):
            target_date = str(row.get("target_date", "")).strip()
            slot_number = str(row.get("slot_number", "")).strip()
            if not target_date or slot_number not in target_slot_numbers:
                continue

            target_key = (target_date, slot_number)
            if _infer_saved_result_data_source(row) == DATA_SOURCE_SITE7:
                if _site7_record_has_complete_fetch_data(
                    row,
                    require_source_difference=require_source_difference,
                ) and _site7_record_update_is_current_or_newer(row, site7_updated_at):
                    protected_slots.add(target_key)
                    replaceable_slots.discard(target_key)
                elif target_key not in protected_slots:
                    replaceable_slots.add(target_key)
                continue

            protected_slots.add(target_key)
            replaceable_slots.discard(target_key)

        return protected_slots, replaceable_slots

    def _find_saved_machine_targets_local(
        self,
        store_name: str,
        store_url: str,
        start_date: str,
        end_date: str,
        target_machine_names: set[str],
    ) -> set[tuple[str, str]]:
        if not target_machine_names:
            return set()

        store_dir = self._local_save_dir() / _sanitize_file_name(store_name)
        if not store_dir.exists():
            return set()

        normalized_store_url = normalize_store_url(store_url)
        saved_targets: set[tuple[str, str]] = set()

        for file_path in store_dir.glob("*.json"):
            payload = json.loads(file_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                continue

            store_payload = payload.get("store", {})
            if not isinstance(store_payload, dict):
                continue

            saved_store_url = normalize_store_url(str(store_payload.get("store_url", "")).strip())
            if saved_store_url and saved_store_url != normalized_store_url:
                continue

            records = payload.get("records", [])
            if not isinstance(records, list):
                continue

            for record in records:
                if not isinstance(record, dict):
                    continue

                target_date = str(record.get("target_date", "")).strip()
                machine_name = normalize_machine_name_key(str(record.get("machine_name", "")).strip())
                if not target_date or not machine_name:
                    continue
                if target_date < start_date or target_date > end_date:
                    continue
                if machine_name not in target_machine_names:
                    continue
                saved_targets.add((target_date, machine_name))

        return saved_targets

    def _save_to_supabase(self, snapshot: dict[str, Any]) -> int:
        raise RuntimeError("Supabase保存は無効です。")

        supabase_url, _, schema, stores_table, results_table = self._supabase_config()
        machine_summaries_table = self._machine_summaries_table()
        machine_daily_details_table = self._machine_daily_details_table()
        now_text = datetime.now().astimezone().isoformat(timespec="seconds")
        session = self._create_supabase_session(schema)

        store_payload = {
            "store_name": snapshot["store"]["store_name"],
            "store_url": normalize_store_url(snapshot["store"]["store_url"]),
            "updated_at": now_text,
        }
        store_id = self._upsert_store(session, supabase_url, stores_table, store_payload)

        records = snapshot["records"]
        if not records:
            return 0

        result_payloads = []
        for record in records:
            result_payloads.append(build_supabase_result_payload(record, store_id=store_id, updated_at=now_text))

        for payload_chunk in _chunk_items(result_payloads, 500):
            endpoint = (
                f"{supabase_url.rstrip('/')}/rest/v1/{quote(results_table, safe='')}"
                "?on_conflict=store_id,target_date,slot_number"
            )
            try:
                response = session.post(
                    endpoint,
                    headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
                    json=payload_chunk,
                    timeout=30,
                )
                response.raise_for_status()
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 400:
                    raise RuntimeError(
                        "machine_daily_results テーブルに取得元を保存する列がありません。"
                        " 追加用SQLを適用してから再度保存してください。"
                    ) from exc
                raise

        source_rows = self._fetch_store_result_source_rows(
            session=session,
            supabase_url=supabase_url,
            results_table=results_table,
            store_id=store_id,
        )

        self._refresh_store_machine_summaries(
            session=session,
            supabase_url=supabase_url,
            machine_summaries_table=machine_summaries_table,
            store_id=store_id,
            updated_at=now_text,
            source_rows=source_rows,
        )
        self._refresh_store_machine_daily_details(
            session=session,
            supabase_url=supabase_url,
            machine_daily_details_table=machine_daily_details_table,
            store_id=store_id,
            updated_at=now_text,
            source_rows=source_rows,
        )

        return len(result_payloads)

    def _refresh_store_machine_summaries(
        self,
        session: requests.Session,
        supabase_url: str,
        machine_summaries_table: str,
        store_id: str,
        updated_at: str,
        source_rows: list[dict[str, Any]],
    ) -> int:
        payloads = build_store_machine_summary_payloads(
            source_rows,
            store_id=store_id,
            updated_at=updated_at,
        )
        endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{quote(machine_summaries_table, safe='')}"

        try:
            response = session.delete(
                endpoint,
                params={
                    "store_id": f"eq.{store_id}",
                },
                headers={"Prefer": "return=minimal"},
                timeout=30,
            )
            response.raise_for_status()
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in {400, 404}:
                raise RuntimeError(
                    "機種一覧用の要約テーブルがありません。"
                    " 追加用SQLを適用してから再度保存してください。"
                ) from exc
            raise

        if not payloads:
            return 0

        endpoint = f"{endpoint}?on_conflict=store_id,machine_name"
        try:
            for payload_chunk in _chunk_items(payloads, 500):
                response = session.post(
                    endpoint,
                    headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
                    json=payload_chunk,
                    timeout=30,
                )
                response.raise_for_status()
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in {400, 404}:
                raise RuntimeError(
                    "機種一覧用の要約テーブルがありません。"
                    " 追加用SQLを適用してから再度保存してください。"
                ) from exc
            raise

        return len(payloads)

    def _refresh_store_machine_daily_details(
        self,
        session: requests.Session,
        supabase_url: str,
        machine_daily_details_table: str,
        store_id: str,
        updated_at: str,
        source_rows: list[dict[str, Any]],
    ) -> int:
        payloads = build_store_machine_daily_detail_payloads(
            source_rows,
            store_id=store_id,
            updated_at=updated_at,
        )
        endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{quote(machine_daily_details_table, safe='')}"

        try:
            response = session.delete(
                endpoint,
                params={
                    "store_id": f"eq.{store_id}",
                },
                headers={"Prefer": "return=minimal"},
                timeout=30,
            )
            response.raise_for_status()
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in {400, 404}:
                raise RuntimeError(
                    "台データページ用の詳細テーブルがありません。"
                    " 追加用SQLを適用してから再度保存してください。"
                ) from exc
            raise

        if not payloads:
            return 0

        endpoint = f"{endpoint}?on_conflict=store_id,machine_name,target_date"
        try:
            for payload_chunk in _chunk_items(payloads, 500):
                response = session.post(
                    endpoint,
                    headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
                    json=payload_chunk,
                    timeout=30,
                )
                response.raise_for_status()
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in {400, 404}:
                raise RuntimeError(
                    "台データページ用の詳細テーブルがありません。"
                    " 追加用SQLを適用してから再度保存してください。"
                ) from exc
            raise

        return len(payloads)

    def _fetch_store_result_source_rows(
        self,
        session: requests.Session,
        supabase_url: str,
        results_table: str,
        store_id: str,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        page_size = 1000
        endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{quote(results_table, safe='')}"

        while True:
            response = session.get(
                endpoint,
                params={
                    "select": (
                        "machine_name,target_date,slot_number,difference_value,bonus_difference_value,"
                        "games_count,payout_rate,bb_count,rb_count,combined_ratio_text,bb_ratio_text,rb_ratio_text"
                    ),
                    "store_id": f"eq.{store_id}",
                    "order": "target_date.desc,slot_number.asc",
                    "limit": str(page_size),
                    "offset": str(offset),
                },
                timeout=30,
            )
            response.raise_for_status()
            chunk = response.json()
            if not chunk:
                break

            rows.extend(chunk)
            if len(chunk) < page_size:
                break
            offset += page_size

        return rows

    def _save_registered_stores_to_supabase(self, stores: list[dict[str, Any]]) -> int:
        raise RuntimeError("Supabase保存は無効です。")

        if not stores:
            return 0

        supabase_url, _, schema, stores_table, _ = self._supabase_config()
        session = self._create_supabase_session(schema)
        now_text = datetime.now().astimezone().isoformat(timespec="seconds")
        payloads = [
            {
                "store_name": store["store_name"],
                "store_url": normalize_store_url(store["store_url"]),
                "site7_enabled": bool(store.get("site7_enabled", False)),
                "site7_prefecture": str(store.get("site7_prefecture", "")).strip() or DEFAULT_SITE7_PREFECTURE_NAME,
                "site7_area": str(store.get("site7_area", "")).strip(),
                "site7_store_name": str(store.get("site7_store_name", "")).strip() or str(store["store_name"]).strip(),
                "site7_hall_id": str(store.get("site7_hall_id", "")).strip(),
                "site7_address": str(store.get("site7_address", "")).strip(),
                "event_day_tails": _normalize_event_values(store.get("event_day_tails", []), 0, 9),
                "event_month_days": _normalize_event_values(store.get("event_month_days", []), 1, 31),
                "event_zoro": _coerce_bool(store.get("event_zoro", False)),
                "event_weekdays": _normalize_event_values(store.get("event_weekdays", []), 0, 6),
                "event_source_text": str(store.get("event_source_text", "")).strip(),
                "updated_at": now_text,
            }
            for store in stores
        ]
        endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{quote(stores_table, safe='')}?on_conflict=store_url"
        try:
            for payload_chunk in _chunk_items(payloads, 500):
                response = session.post(
                    endpoint,
                    headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
                    json=payload_chunk,
                    timeout=30,
                )
                response.raise_for_status()
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 400:
                raise RuntimeError(
                    "stores テーブルにサイトセブン用の列がありません。"
                    " 追加用SQLを適用してから再度保存してください。"
                ) from exc
            raise
        return len(payloads)

    def _collect_machine_names_from_records(self, records: list[dict[str, Any]]) -> list[str]:
        return sorted(
            {
                str(record.get("machine_name", "")).strip()
                for record in records
                if isinstance(record, dict) and str(record.get("machine_name", "")).strip()
            },
            key=normalize_text,
        )

    def _load_registered_stores_from_supabase(self) -> list[dict[str, Any]]:
        return []

        supabase_url, _, schema, stores_table, _ = self._supabase_config()
        session = self._create_supabase_session(schema)
        endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{quote(stores_table, safe='')}"
        rows: list[dict[str, Any]] = []
        offset = 0
        page_size = 1000

        try:
            while True:
                response = session.get(
                    endpoint,
                    params={
                        "select": (
                            "store_name,store_url,site7_enabled,site7_prefecture,site7_area,"
                            "site7_store_name,site7_hall_id,site7_address,"
                            "event_day_tails,event_month_days,event_zoro,event_weekdays,event_source_text"
                        ),
                        "order": "store_name.asc",
                        "limit": str(page_size),
                        "offset": str(offset),
                    },
                    timeout=30,
                )
                response.raise_for_status()
                chunk = response.json()
                if not chunk:
                    break
                rows.extend(chunk)
                if len(chunk) < page_size:
                    break
                offset += page_size
        except requests.HTTPError as exc:
            if exc.response is None or exc.response.status_code != 400:
                raise

            rows = self._load_registered_stores_with_select(
                session=session,
                endpoint=endpoint,
                select_columns="store_name,store_url,site7_enabled,site7_prefecture,site7_area,site7_store_name",
                page_size=page_size,
            )

        return self._normalize_registered_stores(rows)

    def _load_registered_stores_with_select(
        self,
        session: requests.Session,
        endpoint: str,
        select_columns: str,
        page_size: int,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        try:
            while True:
                response = session.get(
                    endpoint,
                    params={
                        "select": select_columns,
                        "order": "store_name.asc",
                        "limit": str(page_size),
                        "offset": str(offset),
                    },
                    timeout=30,
                )
                response.raise_for_status()
                chunk = response.json()
                if not chunk:
                    break
                rows.extend(chunk)
                if len(chunk) < page_size:
                    break
                offset += page_size
        except requests.HTTPError as exc:
            if exc.response is None or exc.response.status_code != 400:
                raise
            rows = []
            offset = 0
            while True:
                response = session.get(
                    endpoint,
                    params={
                        "select": "store_name,store_url",
                        "order": "store_name.asc",
                        "limit": str(page_size),
                        "offset": str(offset),
                    },
                    timeout=30,
                )
                response.raise_for_status()
                chunk = response.json()
                if not chunk:
                    break
                rows.extend(chunk)
                if len(chunk) < page_size:
                    break
                offset += page_size
        return rows

    def _delete_registered_stores_from_supabase(self, store_urls: list[str]) -> int:
        return 0

        supabase_url, _, schema, stores_table, results_table = self._supabase_config()
        session = self._create_supabase_session(schema)
        stores_endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{quote(stores_table, safe='')}"
        results_endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{quote(results_table, safe='')}"
        deleted_store_count = 0

        for store_url in store_urls:
            store_id = self._find_store_id(session, supabase_url, stores_table, store_url)
            if not store_id:
                continue

            response = session.delete(
                results_endpoint,
                params={
                    "store_id": f"eq.{store_id}",
                },
                headers={"Prefer": "return=minimal"},
                timeout=30,
            )
            response.raise_for_status()

            response = session.delete(
                stores_endpoint,
                params={
                    "id": f"eq.{store_id}",
                },
                headers={"Prefer": "return=minimal"},
                timeout=30,
            )
            response.raise_for_status()
            deleted_store_count += 1

        return deleted_store_count

    def _find_saved_machine_targets_from_supabase(
        self,
        store_url: str,
        start_date: str,
        end_date: str,
        target_machine_names: set[str],
    ) -> set[tuple[str, str]]:
        return set()

        if not target_machine_names:
            return set()

        try:
            supabase_url, _, schema, stores_table, results_table = self._supabase_config()
        except RuntimeError:
            return set()

        session = self._create_supabase_session(schema)
        store_id = self._find_store_id(session, supabase_url, stores_table, normalize_store_url(store_url))
        if not store_id:
            return set()

        saved_targets: set[tuple[str, str]] = set()
        offset = 0
        page_size = 1000
        endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{quote(results_table, safe='')}"

        while True:
            response = session.get(
                endpoint,
                params={
                    "select": "target_date,machine_name",
                    "store_id": f"eq.{store_id}",
                    "target_date": [f"gte.{start_date}", f"lte.{end_date}"],
                    "order": "target_date.asc",
                    "limit": str(page_size),
                    "offset": str(offset),
                },
                timeout=30,
            )
            response.raise_for_status()
            rows = response.json()
            if not rows:
                break

            for row in rows:
                if not isinstance(row, dict):
                    continue

                target_date = str(row.get("target_date", "")).strip()
                machine_name = normalize_machine_name_key(str(row.get("machine_name", "")).strip())
                if not target_date or machine_name not in target_machine_names:
                    continue
                saved_targets.add((target_date, machine_name))

            if len(rows) < page_size:
                break
            offset += page_size

        return saved_targets

    def _find_saved_machine_target_sources_from_supabase(
        self,
        store_url: str,
        start_date: str,
        end_date: str,
        target_machine_names: set[str],
    ) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
        return set(), set()

        if not target_machine_names:
            return set(), set()

        try:
            supabase_url, _, schema, stores_table, results_table = self._supabase_config()
        except RuntimeError:
            return set(), set()

        session = self._create_supabase_session(schema)
        store_id = self._find_store_id(session, supabase_url, stores_table, normalize_store_url(store_url))
        if not store_id:
            return set(), set()

        protected_targets: set[tuple[str, str]] = set()
        replaceable_targets: set[tuple[str, str]] = set()
        offset = 0
        page_size = 1000
        endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{quote(results_table, safe='')}"
        select_clause = "target_date,machine_name,data_source,payout_rate"

        while True:
            try:
                response = session.get(
                    endpoint,
                    params={
                        "select": select_clause,
                        "store_id": f"eq.{store_id}",
                        "target_date": [f"gte.{start_date}", f"lte.{end_date}"],
                        "order": "target_date.asc",
                        "limit": str(page_size),
                        "offset": str(offset),
                    },
                    timeout=30,
                )
                response.raise_for_status()
            except requests.HTTPError as exc:
                if exc.response is None or exc.response.status_code != 400 or select_clause == "target_date,machine_name,payout_rate":
                    raise
                select_clause = "target_date,machine_name,payout_rate"
                continue

            rows = response.json()
            if not rows:
                break

            for row in rows:
                if not isinstance(row, dict):
                    continue

                target_date = str(row.get("target_date", "")).strip()
                machine_name = normalize_machine_name_key(str(row.get("machine_name", "")).strip())
                if not target_date or machine_name not in target_machine_names:
                    continue

                target_key = (target_date, machine_name)
                data_source = _infer_saved_result_data_source(row)
                if data_source == DATA_SOURCE_SITE7:
                    if target_key not in protected_targets:
                        replaceable_targets.add(target_key)
                    continue

                protected_targets.add(target_key)
                replaceable_targets.discard(target_key)

            if len(rows) < page_size:
                break
            offset += page_size

        return protected_targets, replaceable_targets

    def _find_saved_machine_slot_sources_from_supabase(
        self,
        store_url: str,
        start_date: str,
        end_date: str,
        target_slot_numbers: set[str],
        site7_updated_at: str | datetime | None = None,
    ) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
        return set(), set()

        if not target_slot_numbers:
            return set(), set()

        try:
            supabase_url, _, schema, stores_table, results_table = self._supabase_config()
        except RuntimeError:
            return set(), set()

        session = self._create_supabase_session(schema)
        store_id = self._find_store_id(session, supabase_url, stores_table, normalize_store_url(store_url))
        if not store_id:
            return set(), set()

        protected_slots: set[tuple[str, str]] = set()
        replaceable_slots: set[tuple[str, str]] = set()
        offset = 0
        page_size = 1000
        endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{quote(results_table, safe='')}"
        select_clause = "target_date,slot_number,data_source,payout_rate"

        while True:
            try:
                response = session.get(
                    endpoint,
                    params={
                        "select": select_clause,
                        "store_id": f"eq.{store_id}",
                        "target_date": [f"gte.{start_date}", f"lte.{end_date}"],
                        "order": "target_date.asc",
                        "limit": str(page_size),
                        "offset": str(offset),
                    },
                    timeout=30,
                )
                response.raise_for_status()
            except requests.HTTPError as exc:
                if exc.response is None or exc.response.status_code != 400 or select_clause == "target_date,slot_number,payout_rate":
                    raise
                select_clause = "target_date,slot_number,payout_rate"
                continue

            rows = response.json()
            if not rows:
                break

            for row in rows:
                if not isinstance(row, dict):
                    continue

                target_date = str(row.get("target_date", "")).strip()
                slot_number = str(row.get("slot_number", "")).strip()
                if not target_date or slot_number not in target_slot_numbers:
                    continue

                target_key = (target_date, slot_number)
                data_source = _infer_saved_result_data_source(row)
                if data_source == DATA_SOURCE_SITE7:
                    if target_key not in protected_slots:
                        replaceable_slots.add(target_key)
                    continue

                protected_slots.add(target_key)
                replaceable_slots.discard(target_key)

            if len(rows) < page_size:
                break
            offset += page_size

        return protected_slots, replaceable_slots

    def _find_store_candidates_by_name_key(
        self,
        session: requests.Session,
        supabase_url: str,
        stores_table: str,
        results_table: str,
        store_name_key: str,
    ) -> list[dict[str, Any]]:
        if not store_name_key:
            return []

        candidates: list[dict[str, Any]] = []
        offset = 0
        page_size = 1000
        endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{quote(stores_table, safe='')}"

        while True:
            response = session.get(
                endpoint,
                params={
                    "select": "id,store_name,store_url",
                    "order": "id.asc",
                    "limit": str(page_size),
                    "offset": str(offset),
                },
                timeout=30,
            )
            response.raise_for_status()
            rows = response.json()
            if not rows:
                break

            for row in rows:
                if not isinstance(row, dict):
                    continue

                candidate_name = str(row.get("store_name", "")).strip()
                if normalize_store_name_key(candidate_name) != store_name_key:
                    continue

                store_id = str(row.get("id", "")).strip()
                if not store_id:
                    continue

                candidates.append(
                    {
                        "store_name": candidate_name,
                        "store_url": normalize_store_url(str(row.get("store_url", "")).strip()),
                        "record_count": self._count_supabase_results(session, supabase_url, results_table, store_id),
                    }
                )

            if len(rows) < page_size:
                break
            offset += page_size

        return candidates

    def _count_supabase_results(
        self,
        session: requests.Session,
        supabase_url: str,
        results_table: str,
        store_id: str,
    ) -> int:
        endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{quote(results_table, safe='')}"
        response = session.get(
            endpoint,
            params={
                "select": "id",
                "store_id": f"eq.{store_id}",
                "limit": "1",
            },
            headers={"Prefer": "count=exact"},
            timeout=30,
        )
        response.raise_for_status()
        content_range = response.headers.get("Content-Range", "")
        if "/" not in content_range:
            return 0

        try:
            return int(content_range.rsplit("/", 1)[1])
        except ValueError:
            return 0

    def _upsert_store(
        self,
        session: requests.Session,
        supabase_url: str,
        stores_table: str,
        store_payload: dict[str, Any],
    ) -> str:
        normalized_store_url = normalize_store_url(str(store_payload.get("store_url", "")))
        existing_store_id = self._find_store_id(session, supabase_url, stores_table, normalized_store_url)
        if existing_store_id:
            store_payload = dict(store_payload)
            store_payload["store_url"] = normalized_store_url
            endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{quote(stores_table, safe='')}?id=eq.{quote(existing_store_id, safe='')}"
            response = session.patch(
                endpoint,
                headers={"Prefer": "return=minimal"},
                json=store_payload,
                timeout=30,
            )
            response.raise_for_status()
            return existing_store_id

        store_payload = dict(store_payload)
        store_payload["store_url"] = normalized_store_url
        endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{quote(stores_table, safe='')}?on_conflict=store_url&select=id"
        response = session.post(
            endpoint,
            headers={"Prefer": "resolution=merge-duplicates,return=representation"},
            json=[store_payload],
            timeout=30,
        )
        response.raise_for_status()
        body = response.json()
        if not body or "id" not in body[0]:
            raise RuntimeError("Supabase 側で店舗IDを取得できませんでした。")
        return str(body[0]["id"])

    def _find_store_id(
        self,
        session: requests.Session,
        supabase_url: str,
        stores_table: str,
        store_url: str,
    ) -> str | None:
        endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{quote(stores_table, safe='')}"
        response = session.get(
            endpoint,
            params={
                "select": "id",
                "store_url": f"eq.{store_url}",
                "limit": "1",
            },
            timeout=30,
        )
        response.raise_for_status()
        body = response.json()
        if not body:
            return None
        return str(body[0].get("id") or "")

    def _local_save_dir(self) -> Path:
        settings = self._load_settings()
        local_dir_text = settings.get("SUPABASE_LOCAL_SAVE_DIR") or settings.get("LOCAL_SAVE_DIR")
        local_dir = Path(local_dir_text) if local_dir_text else self.root_dir / "local_data"
        if not local_dir.is_absolute():
            local_dir = self.root_dir / local_dir
        return local_dir

    def _normalize_registered_stores(self, stores: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized_stores: list[dict[str, Any]] = []
        seen_store_urls: set[str] = set()

        for store in stores:
            if not isinstance(store, dict):
                continue

            store_name = str(store.get("store_name", store.get("name", ""))).strip()
            store_url = normalize_store_url(str(store.get("store_url", store.get("url", ""))).strip())
            if not store_url:
                continue

            site7_defaults = default_site7_store_settings(store_name)
            if store_url in seen_store_urls:
                continue
            seen_store_urls.add(store_url)
            site7_enabled = _coerce_bool(store.get("site7_enabled", site7_defaults["site7_enabled"]))
            raw_site7_store_name = str(store.get("site7_store_name", "")).strip()
            is_daidata_online_store = daidata_store_is_beam_hikari(
                store_name,
                store_url,
            ) or daidata_store_is_beam_hikari(
                raw_site7_store_name,
                store_url,
            )
            is_known_unavailable = (
                not is_daidata_online_store
                and (
                    site7_store_is_known_unavailable(store_name)
                    or site7_store_is_known_unavailable(raw_site7_store_name)
                )
            )
            if is_known_unavailable:
                site7_enabled = False
            should_fill_site7_defaults = site7_enabled or is_known_unavailable
            fetch_order = _normalize_positive_int_or_none(store.get("fetch_order"))
            site7_difference_enabled = bool(
                site7_enabled
                and _coerce_bool(store.get("site7_difference_enabled", fetch_order is not None))
            )
            site7_area = str(store.get("site7_area", "")).strip()
            site7_store_name = raw_site7_store_name
            site7_hall_id = str(store.get("site7_hall_id", "")).strip()
            site7_address = str(store.get("site7_address", "")).strip()
            if should_fill_site7_defaults:
                site7_area = site7_area or str(site7_defaults["site7_area"]).strip()
                site7_store_name = site7_store_name or str(site7_defaults["site7_store_name"]).strip()
                site7_hall_id = site7_hall_id or str(site7_defaults["site7_hall_id"]).strip()
                site7_address = site7_address or str(site7_defaults["site7_address"]).strip()
            site7_store_name = site7_store_name or store_name
            normalized_store = {
                "store_name": store_name,
                "store_url": store_url,
                "site7_enabled": site7_enabled,
                "site7_difference_enabled": site7_difference_enabled,
                "site7_prefecture": str(
                    store.get("site7_prefecture", site7_defaults["site7_prefecture"])
                ).strip()
                or DEFAULT_SITE7_PREFECTURE_NAME,
                "site7_area": site7_area,
                "site7_store_name": site7_store_name,
                "site7_hall_id": site7_hall_id,
                "site7_address": site7_address,
            }
            fetch_frequency = str(store.get("fetch_frequency", "")).strip()
            if fetch_frequency in FETCH_FREQUENCY_VALUES:
                normalized_store["fetch_frequency"] = fetch_frequency
            fetch_source = str(store.get("fetch_source", "")).strip()
            if fetch_source in FETCH_SOURCE_VALUES:
                normalized_store["fetch_source"] = fetch_source
            if fetch_order is not None:
                normalized_store["fetch_order"] = fetch_order
            event_day_tails = _normalize_event_values(store.get("event_day_tails", []), 0, 9)
            event_month_days = _normalize_event_values(store.get("event_month_days", []), 1, 31)
            event_zoro = _coerce_bool(store.get("event_zoro", False))
            event_weekdays = _normalize_event_values(store.get("event_weekdays", []), 0, 6)
            event_source_text = str(store.get("event_source_text", "")).strip()
            if event_day_tails:
                normalized_store["event_day_tails"] = event_day_tails
            if event_month_days:
                normalized_store["event_month_days"] = event_month_days
            if event_zoro:
                normalized_store["event_zoro"] = event_zoro
            if event_weekdays:
                normalized_store["event_weekdays"] = event_weekdays
            if event_source_text:
                normalized_store["event_source_text"] = event_source_text
            normalized_stores.append(normalized_store)

        return normalized_stores

    def _supabase_config(self) -> tuple[str, str, str, str, str]:
        raise RuntimeError("Supabase接続は無効です。")

        settings = self._load_settings()
        supabase_url = settings.get("SUPABASE_URL", "").strip()
        supabase_key = (
            settings.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
            or settings.get("SUPABASE_SECRET_KEY", "").strip()
        )
        if not supabase_url or not supabase_key:
            raise RuntimeError(".env.local に SUPABASE_URL と SUPABASE_SERVICE_ROLE_KEY を設定してください。")

        schema = settings.get("SUPABASE_SCHEMA", DEFAULT_SCHEMA).strip() or DEFAULT_SCHEMA
        stores_table = settings.get("SUPABASE_STORES_TABLE", DEFAULT_STORES_TABLE).strip() or DEFAULT_STORES_TABLE
        results_table = settings.get("SUPABASE_MACHINE_RESULTS_TABLE", DEFAULT_RESULTS_TABLE).strip() or DEFAULT_RESULTS_TABLE
        return supabase_url, supabase_key, schema, stores_table, results_table

    def _supabase_is_configured(self) -> bool:
        return False

        settings = self._load_settings()
        supabase_url = settings.get("SUPABASE_URL", "").strip()
        supabase_key = (
            settings.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
            or settings.get("SUPABASE_SECRET_KEY", "").strip()
        )
        return bool(supabase_url and supabase_key)

    def _machine_summaries_table(self) -> str:
        settings = self._load_settings()
        return (
            settings.get("SUPABASE_MACHINE_SUMMARIES_TABLE", "").strip()
            or DEFAULT_MACHINE_SUMMARIES_TABLE
        )

    def _machine_daily_details_table(self) -> str:
        settings = self._load_settings()
        return (
            settings.get("SUPABASE_MACHINE_DAILY_DETAILS_TABLE", "").strip()
            or DEFAULT_MACHINE_DAILY_DETAILS_TABLE
        )

    def _create_supabase_session(self, schema: str) -> requests.Session:
        raise RuntimeError("Supabase接続は無効です。")

        _, supabase_key, _, _, _ = self._supabase_config()
        session = requests.Session()
        session.headers.update(
            {
                "apikey": supabase_key,
                "Authorization": f"Bearer {supabase_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Accept-Profile": schema,
                "Content-Profile": schema,
            }
        )
        return session

    def _load_settings(self) -> dict[str, str]:
        settings = dict(os.environ)
        for env_path in (self.root_dir / "env.local", self.root_dir / ".env.local"):
            if not env_path.exists():
                continue

            for raw_line in env_path.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("export "):
                    line = line[7:].strip()
                if "=" not in line:
                    continue

                name, value = line.split("=", 1)
                name = name.strip()
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                    value = value[1:-1]
                settings[name] = value

        return settings


def _parse_int_value(value: str) -> int | None:
    normalized = str(value).strip().replace(",", "")
    if not normalized or normalized == "-":
        return None
    if re.fullmatch(r"-?\d+", normalized) is None:
        return None
    return int(normalized)


def _parse_difference_value(value: str) -> int | float | None:
    normalized = str(value).strip().replace(",", "")
    if not normalized or normalized == "-":
        return None
    if re.fullmatch(r"-?\d+(?:\.\d+)?", normalized) is None:
        return None
    if "." in normalized:
        return float(normalized)
    return int(normalized)


def _parse_percent_value(value: str) -> float | None:
    normalized = str(value).strip().replace("%", "")
    if not normalized or normalized == "-":
        return None
    if re.fullmatch(r"-?\d+(?:\.\d+)?", normalized) is None:
        return None
    return float(normalized)


def _average_summary_numbers(values: Any) -> float | None:
    numeric_values: list[float] = []
    for value in values:
        if value is None or value == "":
            continue
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            if isinstance(value, float) and not math.isfinite(value):
                continue
            numeric_values.append(float(value))
            continue

        normalized = str(value).strip().replace(",", "")
        if not normalized or normalized == "-":
            continue
        if re.fullmatch(r"-?\d+(?:\.\d+)?", normalized) is None:
            continue
        numeric_values.append(float(normalized))

    if not numeric_values:
        return None

    return sum(numeric_values) / len(numeric_values)


def _parse_numeric_value(value: Any) -> int | float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return value

    normalized = str(value).strip().replace(",", "")
    if not normalized or normalized == "-":
        return None
    if re.fullmatch(r"-?\d+", normalized):
        return int(normalized)
    if re.fullmatch(r"-?\d+(?:\.\d+)?", normalized):
        return float(normalized)
    return None


def _slot_number_sort_key(value: str) -> tuple[int, int | str]:
    text = str(value).strip()
    if re.fullmatch(r"\d+", text):
        return (0, int(text))
    return (1, text)


def _parse_text_value(value: str) -> str | None:
    normalized = str(value).strip()
    return normalized or None


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)

    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on", "t"}:
        return True
    if text in {"0", "false", "no", "off", "f", ""}:
        return False
    return bool(text)


def _normalize_positive_int_or_none(value: Any) -> int | None:
    text = str(value).strip()
    if not text:
        return None
    if not re.fullmatch(r"\d+", text):
        return None
    number = int(text)
    return number if number > 0 else None


def _normalize_event_values(value: Any, minimum: int, maximum: int) -> list[int]:
    raw_values = value if isinstance(value, (list, tuple, set)) else []
    normalized_values: set[int] = set()
    for raw_value in raw_values:
        try:
            numeric_value = int(raw_value)
        except (TypeError, ValueError):
            continue
        if minimum <= numeric_value <= maximum:
            normalized_values.add(numeric_value)
    return sorted(normalized_values)


def _normalize_difference_value_for_supabase(value: Any) -> int | None:
    if value is None:
        return None

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return _round_half_up_to_int(str(value))

    parsed_value = _parse_difference_value(str(value))
    if isinstance(parsed_value, int):
        return parsed_value
    if isinstance(parsed_value, float):
        return _round_half_up_to_int(str(parsed_value))
    return None


def _round_half_up_to_int(value: str) -> int | None:
    normalized = str(value).strip().replace(",", "")
    if not normalized or normalized == "-":
        return None
    if re.fullmatch(r"-?\d+(?:\.\d+)?", normalized) is None:
        return None

    try:
        return int(Decimal(normalized).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    except InvalidOperation:
        return None


def _sanitize_file_name(value: str) -> str:
    text = WINDOWS_FORBIDDEN_CHARS.sub("_", value.strip())
    text = re.sub(r"\s+", "_", text)
    return text or "store"


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _chunk_items(items: list[dict[str, Any]], chunk_size: int) -> list[list[dict[str, Any]]]:
    return [items[index:index + chunk_size] for index in range(0, len(items), chunk_size)]
