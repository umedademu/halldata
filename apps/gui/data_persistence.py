from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
import os
from pathlib import Path
import re
from typing import Any
import unicodedata
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import requests

from machine_difference import calculate_machine_difference_value, canonical_machine_name
from minrepo_scraper import MachineHistoryResult, normalize_text
from site7_scraper import DEFAULT_SITE7_PREFECTURE_NAME, default_site7_store_settings


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_LOCAL_SAVE_DIR = ROOT_DIR / "local_data"
DEFAULT_SCHEMA = "public"
DEFAULT_STORES_TABLE = "stores"
DEFAULT_RESULTS_TABLE = "machine_daily_results"
STORE_COLUMNS = {"機種", "機種名"}
WINDOWS_FORBIDDEN_CHARS = re.compile(r'[<>:"/\\|?*]+')


@dataclass
class PersistenceSummary:
    local_file_path: str | None = None
    local_record_count: int = 0
    supabase_saved: bool = False
    supabase_record_count: int = 0
    messages: list[str] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return bool(self.messages)


@dataclass
class RegisteredStoresPersistenceSummary:
    supabase_saved: bool = False
    supabase_store_count: int = 0
    messages: list[str] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return bool(self.messages)


@dataclass
class SavedMachineTargetsSummary:
    saved_targets: set[tuple[str, str]] = field(default_factory=set)
    messages: list[str] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return bool(self.messages)


@dataclass
class SavedFullDayDatesSummary:
    saved_dates: set[str] = field(default_factory=set)
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
        for row in dataset.rows:
            row_values = dict(zip(source_columns, row, strict=False))
            slot_number = row_values.get("台番", "").strip()
            if not slot_number:
                continue

            difference_value = _parse_difference_value(row_values.get("差枚", ""))
            if difference_value is None:
                difference_value = calculate_machine_difference_value(stored_machine_name, row_values)

            records.append(
                {
                    "target_date": dataset.target_date,
                    "slot_number": slot_number,
                    "machine_name": stored_machine_name,
                    "difference_value": difference_value,
                    "games_count": _parse_int_value(row_values.get("G数", "")),
                    "payout_rate": _parse_percent_value(row_values.get("出率", "")),
                    "bb_count": _parse_int_value(row_values.get("BB", "")),
                    "rb_count": _parse_int_value(row_values.get("RB", "")),
                    "combined_ratio_text": _parse_text_value(row_values.get("合成", "")),
                    "bb_ratio_text": _parse_text_value(row_values.get("BB率", "")),
                    "rb_ratio_text": _parse_text_value(row_values.get("RB率", "")),
                }
            )

    return records


def build_supabase_result_payload(record: dict[str, Any], store_id: str, updated_at: str) -> dict[str, Any]:
    payload = dict(record)
    payload["difference_value"] = _normalize_difference_value_for_supabase(payload.get("difference_value"))
    payload["store_id"] = store_id
    payload["updated_at"] = updated_at
    return payload


class HistoryPersistenceService:
    def __init__(self, root_dir: Path | None = None) -> None:
        self.root_dir = root_dir or ROOT_DIR

    def save_history_result(self, history_result: MachineHistoryResult, full_day: bool = False) -> PersistenceSummary:
        snapshot = self._build_local_snapshot(history_result)
        summary = PersistenceSummary(local_record_count=len(snapshot["records"]))

        try:
            local_path = self._save_local_snapshot(snapshot)
            summary.local_file_path = str(local_path)
            if full_day:
                self._mark_full_day_saved(snapshot, local_path)
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"ローカル保存に失敗しました。\n{exc}")

        try:
            supabase_count = self._save_to_supabase(snapshot)
            summary.supabase_saved = True
            summary.supabase_record_count = supabase_count
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"Supabase 保存に失敗しました。\n{exc}")

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
            summary.saved_dates.update(
                self._find_saved_full_day_dates_local(
                    store_name=store_name,
                    store_url=store_url,
                    start_date=start_date,
                    end_date=end_date,
                )
            )
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"ローカルの全機種取得済み確認に失敗しました。\n{exc}")
        return summary

    def find_saved_machine_targets(
        self,
        store_name: str,
        store_url: str,
        start_date: str,
        end_date: str,
        machine_names: list[str],
    ) -> SavedMachineTargetsSummary:
        target_machine_names = {
            normalize_machine_name_key(machine_name)
            for machine_name in machine_names
            if machine_name.strip()
        }
        summary = SavedMachineTargetsSummary()
        if not target_machine_names:
            return summary

        try:
            summary.saved_targets.update(
                self._find_saved_machine_targets_local(
                    store_name=store_name,
                    store_url=store_url,
                    start_date=start_date,
                    end_date=end_date,
                    target_machine_names=target_machine_names,
                )
            )
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"ローカルの取得済み確認に失敗しました。\n{exc}")

        try:
            summary.saved_targets.update(
                self._find_saved_machine_targets_from_supabase(
                    store_url=store_url,
                    start_date=start_date,
                    end_date=end_date,
                    target_machine_names=target_machine_names,
                )
            )
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"Supabase の取得済み確認に失敗しました。\n{exc}")

        return summary

    def find_saved_machine_targets_supabase(
        self,
        store_url: str,
        start_date: str,
        end_date: str,
        machine_names: list[str],
    ) -> SavedMachineTargetsSummary:
        target_machine_names = {
            normalize_machine_name_key(machine_name)
            for machine_name in machine_names
            if machine_name.strip()
        }
        summary = SavedMachineTargetsSummary()
        if not target_machine_names:
            return summary

        try:
            summary.saved_targets.update(
                self._find_saved_machine_targets_from_supabase(
                    store_url=store_url,
                    start_date=start_date,
                    end_date=end_date,
                    target_machine_names=target_machine_names,
                )
            )
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"Supabase の取得済み確認に失敗しました。\n{exc}")

        return summary

    def resolve_preferred_store_by_name(self, store_name: str) -> dict[str, str] | None:
        store_name_key = normalize_store_name_key(store_name)
        if not store_name_key:
            return None

        try:
            supabase_url, _, schema, stores_table, results_table = self._supabase_config()
        except RuntimeError:
            return None

        session = self._create_supabase_session(schema)
        candidates = self._find_store_candidates_by_name_key(
            session=session,
            supabase_url=supabase_url,
            stores_table=stores_table,
            results_table=results_table,
            store_name_key=store_name_key,
        )
        return choose_preferred_store(candidates)

    def delete_machine_targets_from_supabase(
        self,
        store_url: str,
        target_pairs: set[tuple[str, str]],
    ) -> int:
        normalized_target_pairs = {
            (str(target_date).strip(), str(machine_name).strip())
            for target_date, machine_name in target_pairs
            if str(target_date).strip() and str(machine_name).strip()
        }
        if not normalized_target_pairs:
            return 0

        try:
            supabase_url, _, schema, stores_table, results_table = self._supabase_config()
        except RuntimeError:
            return 0

        session = self._create_supabase_session(schema)
        store_id = self._find_store_id(session, supabase_url, stores_table, normalize_store_url(store_url))
        if not store_id:
            return 0

        endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{quote(results_table, safe='')}"
        deleted_target_count = 0
        for target_date, machine_name in sorted(normalized_target_pairs):
            response = session.delete(
                endpoint,
                params={
                    "store_id": f"eq.{store_id}",
                    "target_date": f"eq.{target_date}",
                    "machine_name": f"eq.{machine_name}",
                },
                headers={"Prefer": "return=minimal"},
                timeout=30,
            )
            response.raise_for_status()
            deleted_target_count += 1

        return deleted_target_count

    def load_registered_stores(self) -> list[dict[str, Any]]:
        return self._load_registered_stores_from_supabase()

    def save_registered_stores(self, stores: list[dict[str, Any]]) -> RegisteredStoresPersistenceSummary:
        normalized_stores = self._normalize_registered_stores(stores)
        summary = RegisteredStoresPersistenceSummary()

        try:
            saved_count = self._save_registered_stores_to_supabase(normalized_stores)
            summary.supabase_saved = True
            summary.supabase_store_count = saved_count
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"登録店舗の Supabase 保存に失敗しました。\n{exc}")

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

        return self._delete_registered_stores_from_supabase(normalized_store_urls)

    def _build_local_snapshot(self, history_result: MachineHistoryResult) -> dict[str, Any]:
        records = build_machine_daily_records(history_result)
        return {
            "saved_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "store": {
                "store_name": history_result.store_name,
                "store_url": normalize_store_url(history_result.store_url),
            },
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

    def _save_local_snapshot(self, snapshot: dict[str, Any]) -> Path:
        local_dir = self._local_save_dir()
        store_name = str(snapshot["store"]["store_name"])
        store_dir = local_dir / _sanitize_file_name(store_name)
        store_dir.mkdir(parents=True, exist_ok=True)

        period = snapshot["period"]
        file_name = (
            f"{period['start_date']}_{period['end_date']}_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
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
        index_payload["store"] = {
            "store_name": store_name,
            "store_url": normalize_store_url(str(store.get("store_url", ""))),
        }
        full_day_dates = index_payload.setdefault("full_day_dates", {})
        if not isinstance(full_day_dates, dict):
            full_day_dates = {}
            index_payload["full_day_dates"] = full_day_dates

        now_text = datetime.now().astimezone().isoformat(timespec="seconds")
        machine_names = snapshot.get("machine_names", [])
        records = snapshot.get("records", [])
        for date_page in snapshot.get("date_pages", []):
            if not isinstance(date_page, dict):
                continue
            target_date = str(date_page.get("target_date", "")).strip()
            if not target_date:
                continue
            full_day_dates[target_date] = {
                "saved_at": now_text,
                "machine_count": len(machine_names) if isinstance(machine_names, list) else 0,
                "record_count": len(records) if isinstance(records, list) else 0,
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
        index_path = self._full_day_index_path(store_name)
        if not index_path.exists():
            return set()

        payload = self._load_full_day_index(index_path)
        store_payload = payload.get("store", {})
        if isinstance(store_payload, dict):
            saved_store_url = normalize_store_url(str(store_payload.get("store_url", "")).strip())
            if saved_store_url and saved_store_url != normalize_store_url(store_url):
                return set()

        full_day_dates = payload.get("full_day_dates", {})
        if not isinstance(full_day_dates, dict):
            return set()

        return {
            target_date
            for target_date in full_day_dates
            if start_date <= target_date <= end_date
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
        supabase_url, _, schema, stores_table, results_table = self._supabase_config()
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
            response = session.post(
                endpoint,
                headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
                json=payload_chunk,
                timeout=30,
            )
            response.raise_for_status()

        return len(result_payloads)

    def _save_registered_stores_to_supabase(self, stores: list[dict[str, Any]]) -> int:
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

    def _load_registered_stores_from_supabase(self) -> list[dict[str, Any]]:
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
                        "select": "store_name,store_url,site7_enabled,site7_prefecture,site7_area,site7_store_name",
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

        return self._normalize_registered_stores(rows)

    def _delete_registered_stores_from_supabase(self, store_urls: list[str]) -> int:
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
            normalized_stores.append(
                {
                    "store_name": store_name,
                    "store_url": store_url,
                    "site7_enabled": _coerce_bool(store.get("site7_enabled", site7_defaults["site7_enabled"])),
                    "site7_prefecture": str(
                        store.get("site7_prefecture", site7_defaults["site7_prefecture"])
                    ).strip()
                    or DEFAULT_SITE7_PREFECTURE_NAME,
                    "site7_area": str(store.get("site7_area", site7_defaults["site7_area"])).strip(),
                    "site7_store_name": str(
                        store.get("site7_store_name", site7_defaults["site7_store_name"])
                    ).strip()
                    or store_name,
                }
            )

        return normalized_stores

    def _supabase_config(self) -> tuple[str, str, str, str, str]:
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

    def _create_supabase_session(self, schema: str) -> requests.Session:
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


def _chunk_items(items: list[dict[str, Any]], chunk_size: int) -> list[list[dict[str, Any]]]:
    return [items[index:index + chunk_size] for index in range(0, len(items), chunk_size)]
