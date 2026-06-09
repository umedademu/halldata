from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from r2_storage import R2JsonStorage
from web_data_export import normalize_store_url


ROOT_DIR = Path(__file__).resolve().parents[2]
STATIC_WEB_DATA_DIR = ROOT_DIR / "apps" / "web" / "public" / "halldata-static"
FULL_DAY_INDEX_FILE_NAME = "full-day-index.json"
DATA_SOURCE_MINREPO = "minrepo"
DATA_SOURCE_SITE7 = "site7"


def record_has_site7_source(record: dict[str, Any]) -> bool:
    data_source = str(record.get("data_source", "")).strip().casefold()
    return data_source == DATA_SOURCE_SITE7 or bool(str(record.get("site7_fetched_at", "")).strip())


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    return payload if isinstance(payload, dict) else None


def build_full_day_index(static_dir: Path, store_entry: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    store_id = str(store_entry.get("id", "")).strip()
    data_file = str(store_entry.get("dataFile", "")).strip()
    if not store_id or not data_file:
        return None

    store_payload = read_json(static_dir / data_file)
    if not store_payload:
        return None

    records_by_date: dict[str, int] = {}
    machines_by_date: dict[str, set[str]] = {}
    site7_dates: set[str] = set()
    for machine in store_payload.get("machines", []):
        if not isinstance(machine, dict):
            continue
        machine_name = str(machine.get("machineName", "")).strip()
        machine_file = str(machine.get("dataFile", "")).strip()
        if not machine_file:
            continue
        machine_payload = read_json(static_dir / machine_file)
        if not machine_payload:
            continue
        for record in machine_payload.get("records", []):
            if not isinstance(record, dict):
                continue
            target_date = str(record.get("target_date", "")).strip()
            record_machine_name = str(record.get("machine_name", "")).strip() or machine_name
            if not target_date:
                continue
            if record_has_site7_source(record):
                site7_dates.add(target_date)
                continue
            records_by_date[target_date] = records_by_date.get(target_date, 0) + 1
            machines_by_date.setdefault(target_date, set()).add(record_machine_name)

    if not records_by_date:
        return None

    generated_at = str(store_payload.get("generatedAt") or datetime.now().astimezone().isoformat(timespec="seconds"))
    store_payload_body = store_payload.get("store", {})
    store_name = str(store_payload_body.get("storeName") or store_entry.get("storeName") or "").strip()
    store_url = normalize_store_url(str(store_payload_body.get("storeUrl") or store_entry.get("storeUrl") or ""))
    index_payload = {
        "version": 1,
        "store": {
            "store_name": store_name,
            "store_url": store_url,
        },
        "full_day_dates": {
            target_date: {
                "saved_at": generated_at,
                "machine_count": len(machines_by_date.get(target_date, set())),
                "record_count": record_count,
                "snapshot_key": "",
                "data_source": DATA_SOURCE_MINREPO,
            }
            for target_date, record_count in sorted(records_by_date.items())
            if target_date not in site7_dates
        },
    }
    return f"stores/{store_id}/{FULL_DAY_INDEX_FILE_NAME}", index_payload


def upload_static_web_data_to_r2(static_dir: Path = STATIC_WEB_DATA_DIR) -> tuple[int, int]:
    if not static_dir.exists():
        raise FileNotFoundError(f"表示用Jsonのフォルダが見つかりません。{static_dir}")

    storage = R2JsonStorage.from_environment(ROOT_DIR)
    if not storage.is_configured:
        raise RuntimeError(".env.local に R2 の接続情報を設定してください。")

    json_files = sorted(path for path in static_dir.rglob("*.json") if path.is_file())
    index_file = static_dir / "index.json"
    uploaded_count = 0

    for path in json_files:
        if path == index_file:
            continue
        relative_key = path.relative_to(static_dir).as_posix()
        storage.write_bytes(
            relative_key,
            path.read_bytes(),
            content_type="application/json; charset=utf-8",
        )
        uploaded_count += 1

    full_day_index_count = 0
    index_payload = read_json(index_file) if index_file.exists() else None
    if index_payload and isinstance(index_payload.get("stores"), list):
        for store_entry in index_payload["stores"]:
            if not isinstance(store_entry, dict):
                continue
            result = build_full_day_index(static_dir, store_entry)
            if result is None:
                continue
            key, payload = result
            storage.write_json(key, payload)
            full_day_index_count += 1

    if index_file.exists():
        storage.write_bytes(
            "index.json",
            index_file.read_bytes(),
            content_type="application/json; charset=utf-8",
        )
        uploaded_count += 1

    return uploaded_count, full_day_index_count


def main() -> None:
    uploaded_count, full_day_index_count = upload_static_web_data_to_r2()
    print(f"{uploaded_count}件の表示用Jsonと{full_day_index_count}件の取得済み索引をR2へ移しました。")


if __name__ == "__main__":
    main()
