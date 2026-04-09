from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
import os
from pathlib import Path
import re
from typing import Any
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import requests

from minrepo_scraper import MachineHistoryResult, normalize_text


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_LOCAL_SAVE_DIR = ROOT_DIR / "local_data"
DEFAULT_REGISTERED_STORES_FILE = DEFAULT_LOCAL_SAVE_DIR / "registered_stores.json"
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
    local_file_path: str | None = None
    local_store_count: int = 0
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


def build_machine_daily_records(history_result: MachineHistoryResult) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    for dataset in history_result.datasets:
        source_columns = [column for column in dataset.columns if normalize_text(column) not in STORE_COLUMNS]
        for row in dataset.rows:
            row_values = dict(zip(source_columns, row, strict=False))
            slot_number = row_values.get("台番", "").strip()
            if not slot_number:
                continue

            records.append(
                {
                    "target_date": dataset.target_date,
                    "slot_number": slot_number,
                    "machine_name": dataset.machine_name.strip(),
                    "difference_value": _parse_int_value(row_values.get("差枚", "")),
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


class HistoryPersistenceService:
    def __init__(self, root_dir: Path | None = None) -> None:
        self.root_dir = root_dir or ROOT_DIR

    def save_history_result(self, history_result: MachineHistoryResult) -> PersistenceSummary:
        snapshot = self._build_local_snapshot(history_result)
        summary = PersistenceSummary(local_record_count=len(snapshot["records"]))

        try:
            local_path = self._save_local_snapshot(snapshot)
            summary.local_file_path = str(local_path)
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"ローカル保存に失敗しました。\n{exc}")

        try:
            supabase_count = self._save_to_supabase(snapshot)
            summary.supabase_saved = True
            summary.supabase_record_count = supabase_count
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"Supabase 保存に失敗しました。\n{exc}")

        return summary

    def find_saved_machine_targets(
        self,
        store_name: str,
        store_url: str,
        start_date: str,
        end_date: str,
        machine_names: list[str],
    ) -> SavedMachineTargetsSummary:
        target_machine_names = {normalize_text(machine_name) for machine_name in machine_names if machine_name.strip()}
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

    def load_registered_stores(self) -> list[dict[str, str]]:
        file_path = self._registered_stores_file_path()
        if not file_path.exists():
            return []

        payload = json.loads(file_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            raw_stores = payload.get("stores", [])
        else:
            raw_stores = payload

        if not isinstance(raw_stores, list):
            raise RuntimeError("登録店舗ファイルの形式が不正です。")

        return self._normalize_registered_stores(raw_stores)

    def save_registered_stores(self, stores: list[dict[str, str]]) -> RegisteredStoresPersistenceSummary:
        normalized_stores = self._normalize_registered_stores(stores)
        summary = RegisteredStoresPersistenceSummary(local_store_count=len(normalized_stores))

        try:
            local_path = self._save_registered_stores_local(normalized_stores)
            summary.local_file_path = str(local_path)
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"登録店舗のローカル保存に失敗しました。\n{exc}")

        try:
            saved_count = self._save_registered_stores_to_supabase(normalized_stores)
            summary.supabase_saved = True
            summary.supabase_store_count = saved_count
        except Exception as exc:  # noqa: BLE001
            summary.messages.append(f"登録店舗の Supabase 保存に失敗しました。\n{exc}")

        return summary

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
            "machine_names": sorted({dataset.machine_name for dataset in history_result.datasets}, key=normalize_text),
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

    def _save_registered_stores_local(self, stores: list[dict[str, str]]) -> Path:
        file_path = self._registered_stores_file_path()
        file_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "saved_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "stores": stores,
        }
        file_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return file_path

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
                machine_name = normalize_text(str(record.get("machine_name", "")).strip())
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
            payload = dict(record)
            payload["store_id"] = store_id
            payload["updated_at"] = now_text
            result_payloads.append(payload)

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

    def _save_registered_stores_to_supabase(self, stores: list[dict[str, str]]) -> int:
        if not stores:
            return 0

        supabase_url, _, schema, stores_table, _ = self._supabase_config()
        session = self._create_supabase_session(schema)
        now_text = datetime.now().astimezone().isoformat(timespec="seconds")
        payloads = [
            {
                "store_name": store["store_name"],
                "store_url": normalize_store_url(store["store_url"]),
                "updated_at": now_text,
            }
            for store in stores
        ]
        endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{quote(stores_table, safe='')}?on_conflict=store_url"
        for payload_chunk in _chunk_items(payloads, 500):
            response = session.post(
                endpoint,
                headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
                json=payload_chunk,
                timeout=30,
            )
            response.raise_for_status()
        return len(payloads)

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
                machine_name = normalize_text(str(row.get("machine_name", "")).strip())
                if not target_date or machine_name not in target_machine_names:
                    continue
                saved_targets.add((target_date, machine_name))

            if len(rows) < page_size:
                break
            offset += page_size

        return saved_targets

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
        local_dir = Path(local_dir_text) if local_dir_text else DEFAULT_LOCAL_SAVE_DIR
        if not local_dir.is_absolute():
            local_dir = self.root_dir / local_dir
        return local_dir

    def _registered_stores_file_path(self) -> Path:
        settings = self._load_settings()
        file_text = settings.get("REGISTERED_STORES_FILE") or settings.get("SUPABASE_REGISTERED_STORES_FILE")
        file_path = Path(file_text) if file_text else DEFAULT_REGISTERED_STORES_FILE
        if not file_path.is_absolute():
            file_path = self.root_dir / file_path
        return file_path

    def _normalize_registered_stores(self, stores: list[dict[str, Any]]) -> list[dict[str, str]]:
        normalized_stores: list[dict[str, str]] = []
        seen_keys: set[tuple[str, str]] = set()

        for store in stores:
            if not isinstance(store, dict):
                continue

            store_name = str(store.get("store_name", store.get("name", ""))).strip()
            store_url = normalize_store_url(str(store.get("store_url", store.get("url", ""))).strip())
            if not store_name or not store_url:
                continue

            dedupe_key = (store_url,)
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            normalized_stores.append(
                {
                    "store_name": store_name,
                    "store_url": store_url,
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
            raise RuntimeError("env.local に SUPABASE_URL と SUPABASE_SERVICE_ROLE_KEY を設定してください。")

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
        env_path = self.root_dir / "env.local"
        if not env_path.exists():
            return settings

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


def _sanitize_file_name(value: str) -> str:
    text = WINDOWS_FORBIDDEN_CHARS.sub("_", value.strip())
    text = re.sub(r"\s+", "_", text)
    return text or "store"


def _chunk_items(items: list[dict[str, Any]], chunk_size: int) -> list[list[dict[str, Any]]]:
    return [items[index:index + chunk_size] for index in range(0, len(items), chunk_size)]
