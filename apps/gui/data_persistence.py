from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
import os
from pathlib import Path
import re
from typing import Any
from urllib.parse import quote

import requests

from minrepo_scraper import MachineHistoryResult, normalize_text


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

    def _build_local_snapshot(self, history_result: MachineHistoryResult) -> dict[str, Any]:
        records = build_machine_daily_records(history_result)
        return {
            "saved_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "store": {
                "store_name": history_result.store_name,
                "store_url": history_result.store_url,
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
        settings = self._load_settings()
        local_dir_text = settings.get("SUPABASE_LOCAL_SAVE_DIR") or settings.get("LOCAL_SAVE_DIR")
        local_dir = Path(local_dir_text) if local_dir_text else DEFAULT_LOCAL_SAVE_DIR
        if not local_dir.is_absolute():
            local_dir = self.root_dir / local_dir

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

    def _save_to_supabase(self, snapshot: dict[str, Any]) -> int:
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
        now_text = datetime.now().astimezone().isoformat(timespec="seconds")

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

        store_payload = {
            "store_name": snapshot["store"]["store_name"],
            "store_url": snapshot["store"]["store_url"],
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

    def _upsert_store(
        self,
        session: requests.Session,
        supabase_url: str,
        stores_table: str,
        store_payload: dict[str, Any],
    ) -> str:
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
