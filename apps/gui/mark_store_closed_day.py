from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from r2_storage import R2JsonStorage
from web_data_export import (
    WEB_DATA_VERSION,
    build_index_store_entry,
    build_machine_summaries,
    build_store_id,
    normalize_store_day_status_payloads,
    normalize_store_url,
    store_day_status_is_closed,
    update_index,
)


ROOT_DIR = Path(__file__).resolve().parents[2]
STORE_DAY_STATUS_CLOSED = "closed"


@dataclass
class ClosedDayResult:
    store_id: str
    store_data_file: str
    target_date: str
    removed_record_count: int = 0
    changed_machine_files: list[str] = field(default_factory=list)
    deleted_machine_files: list[str] = field(default_factory=list)
    snapshot_key: str = ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="R2上の指定店舗・指定日データを店休日扱いへ変更します。",
    )
    parser.add_argument("--store-name", required=True)
    parser.add_argument("--store-url", required=True)
    parser.add_argument("--target-date", required=True)
    parser.add_argument("--reason", default="manual_closed_day")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="R2へ保存せず、削除対象件数だけを確認します。",
    )
    return parser.parse_args()


def read_text(value: Any) -> str:
    return str(value or "").strip()


def validate_target_date(value: str) -> str:
    target_date = read_text(value)
    datetime.strptime(target_date, "%Y-%m-%d")
    return target_date


def find_store_entry(
    storage: R2JsonStorage,
    *,
    store_name: str,
    store_url: str,
) -> dict[str, Any] | None:
    store_id = build_store_id(store_name, normalize_store_url(store_url))
    normalized_url = normalize_store_url(store_url)
    index_payload = storage.read_json("index.json") or {}
    for entry in index_payload.get("stores", []):
        if not isinstance(entry, dict):
            continue
        legacy_ids = {read_text(legacy_id) for legacy_id in entry.get("legacyIds", [])}
        if read_text(entry.get("id")) == store_id or store_id in legacy_ids:
            return entry
        if normalized_url and normalize_store_url(read_text(entry.get("storeUrl"))) == normalized_url:
            return entry
        if store_name and read_text(entry.get("storeName")) == store_name:
            return entry
    return None


def status_for_web(target_date: str, reason: str, now_text: str) -> dict[str, Any]:
    return {
        "targetDate": target_date,
        "status": STORE_DAY_STATUS_CLOSED,
        "source": "manual",
        "reason": reason,
        "checkedAt": now_text,
    }


def status_for_index(status: dict[str, Any], *, saved_at: str, snapshot_key: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "target_date": read_text(status.get("targetDate") or status.get("target_date")),
        "status": read_text(status.get("status")),
        "saved_at": saved_at,
        "snapshot_key": snapshot_key,
    }
    for web_key, index_key in (
        ("source", "source"),
        ("reason", "reason"),
        ("checkedAt", "checked_at"),
        ("sourceUpdatedAt", "source_updated_at"),
    ):
        value = read_text(status.get(web_key))
        if value:
            payload[index_key] = value
    for web_key, index_key in (
        ("observedSlotCount", "observed_slot_count"),
        ("observedNoPlaySlotCount", "observed_no_play_slot_count"),
    ):
        value = status.get(web_key)
        if isinstance(value, int) and value >= 0:
            payload[index_key] = value
    return payload


def remove_date_records(records: list[Any], target_date: str) -> tuple[list[dict[str, Any]], int]:
    kept_records: list[dict[str, Any]] = []
    removed_count = 0
    for record in records:
        if not isinstance(record, dict):
            continue
        if read_text(record.get("target_date") or record.get("targetDate")) == target_date:
            removed_count += 1
            continue
        kept_records.append(record)
    return kept_records, removed_count


def rebuild_machine_summaries(
    store_payload: dict[str, Any],
    *,
    storage: R2JsonStorage,
    target_date: str,
    now_text: str,
    dry_run: bool,
    result: ClosedDayResult,
) -> list[dict[str, Any]]:
    rebuilt_summaries: list[dict[str, Any]] = []
    for machine_summary in store_payload.get("machines", []):
        if not isinstance(machine_summary, dict):
            continue
        data_file = read_text(machine_summary.get("dataFile"))
        if not data_file:
            rebuilt_summaries.append(machine_summary)
            continue

        machine_payload = storage.read_json(data_file) or {}
        raw_records = machine_payload.get("records", [])
        records = raw_records if isinstance(raw_records, list) else []
        kept_records, removed_count = remove_date_records(records, target_date)
        if removed_count <= 0:
            rebuilt_summaries.append(machine_summary)
            continue

        result.removed_record_count += removed_count
        if kept_records:
            machine_payload["records"] = kept_records
            machine_payload["generatedAt"] = now_text
            summaries = build_machine_summaries(kept_records)
            if summaries:
                for summary in summaries:
                    summary["dataFile"] = data_file
                    rebuilt_summaries.append(summary)
            if not dry_run:
                storage.write_json(data_file, machine_payload)
            result.changed_machine_files.append(data_file)
            continue

        if not dry_run:
            storage.delete_object(data_file)
        result.deleted_machine_files.append(data_file)

    return sorted(
        rebuilt_summaries,
        key=lambda machine: (
            read_text(machine.get("latestDate")),
            int(machine.get("slotCount") or 0),
            read_text(machine.get("machineName")),
        ),
        reverse=True,
    )


def build_snapshot(
    store_payload: dict[str, Any],
    *,
    store_name: str,
    store_url: str,
    target_date: str,
    status: dict[str, Any],
    result: ClosedDayResult,
) -> dict[str, Any]:
    store = store_payload.get("store", {})
    store = store if isinstance(store, dict) else {}
    return {
        "version": 1,
        "store": {
            "store_name": store_name,
            "store_url": normalize_store_url(store_url),
            "prefecture_name": read_text(store.get("prefectureName")),
            "area_name": read_text(store.get("areaName")),
            "event_day_tails": store.get("eventDayTails", []),
            "event_month_days": store.get("eventMonthDays", []),
            "event_zoro": bool(store.get("eventZoro", False)),
            "event_weekdays": store.get("eventWeekdays", []),
            "event_source_text": read_text(store.get("eventSourceText")),
        },
        "period": {
            "start_date": target_date,
            "end_date": target_date,
        },
        "records": [],
        "date_pages": [],
        "store_day_statuses": [
            status_for_index(status, saved_at=read_text(status.get("checkedAt")), snapshot_key=result.snapshot_key)
        ],
        "maintenance": {
            "action": "mark_store_closed_day",
            "removed_record_count": result.removed_record_count,
            "changed_machine_files": result.changed_machine_files,
            "deleted_machine_files": result.deleted_machine_files,
        },
    }


def update_full_day_index(
    storage: R2JsonStorage,
    *,
    store_payload: dict[str, Any],
    store_id: str,
    store_name: str,
    store_url: str,
    target_date: str,
    status: dict[str, Any],
    saved_at: str,
    snapshot_key: str,
) -> None:
    index_key = f"stores/{store_id}/full-day-index.json"
    index_payload = storage.read_json(index_key) or {
        "version": 1,
        "store": {},
        "full_day_dates": {},
        "store_day_statuses": {},
    }
    if not isinstance(index_payload, dict):
        index_payload = {
            "version": 1,
            "store": {},
            "full_day_dates": {},
            "store_day_statuses": {},
        }

    store = store_payload.get("store", {})
    store = store if isinstance(store, dict) else {}
    index_payload["version"] = int(index_payload.get("version") or 1)
    index_payload["store"] = {
        "store_name": store_name,
        "store_url": normalize_store_url(store_url),
        "event_day_tails": store.get("eventDayTails", []),
        "event_month_days": store.get("eventMonthDays", []),
        "event_zoro": bool(store.get("eventZoro", False)),
        "event_weekdays": store.get("eventWeekdays", []),
        "event_source_text": read_text(store.get("eventSourceText")),
    }
    full_day_dates = index_payload.setdefault("full_day_dates", {})
    if not isinstance(full_day_dates, dict):
        full_day_dates = {}
        index_payload["full_day_dates"] = full_day_dates
    full_day_dates.pop(target_date, None)

    status_entries = index_payload.setdefault("store_day_statuses", {})
    if not isinstance(status_entries, dict):
        status_entries = {}
        index_payload["store_day_statuses"] = status_entries
    status_entries[target_date] = status_for_index(status, saved_at=saved_at, snapshot_key=snapshot_key)

    storage.write_json(index_key, index_payload)


def mark_store_closed_day(
    *,
    storage: R2JsonStorage,
    store_name: str,
    store_url: str,
    target_date: str,
    reason: str,
    dry_run: bool = False,
) -> ClosedDayResult:
    store_url = normalize_store_url(store_url)
    store_id = build_store_id(store_name, store_url)
    store_entry = find_store_entry(storage, store_name=store_name, store_url=store_url)
    store_data_file = read_text(store_entry.get("dataFile")) if isinstance(store_entry, dict) else ""
    if not store_data_file:
        store_data_file = f"stores/{store_id}.json"

    store_payload = storage.read_json(store_data_file)
    if not isinstance(store_payload, dict):
        raise RuntimeError(f"店舗データを読めませんでした: {store_data_file}")

    now_text = datetime.now().astimezone().isoformat(timespec="seconds")
    result = ClosedDayResult(store_id=store_id, store_data_file=store_data_file, target_date=target_date)
    machine_summaries = rebuild_machine_summaries(
        store_payload,
        storage=storage,
        target_date=target_date,
        now_text=now_text,
        dry_run=dry_run,
        result=result,
    )

    top_records = store_payload.get("records", [])
    if isinstance(top_records, list):
        kept_top_records, removed_top_count = remove_date_records(top_records, target_date)
        if removed_top_count > 0:
            result.removed_record_count += removed_top_count
            store_payload["records"] = kept_top_records

    status = status_for_web(target_date, reason, now_text)
    statuses = normalize_store_day_status_payloads(
        [
            *(
                store_payload.get("storeDayStatuses", [])
                if isinstance(store_payload.get("storeDayStatuses"), list)
                else []
            ),
            status,
        ]
    )
    closed_date_count = sum(1 for item in statuses if store_day_status_is_closed(item))
    latest_date = max((read_text(machine.get("latestDate")) for machine in machine_summaries), default=None)
    previous_record_count = int((store_payload.get("summary") or {}).get("recordCount") or 0)
    record_count = max(0, previous_record_count - result.removed_record_count)

    store_payload["version"] = int(store_payload.get("version") or WEB_DATA_VERSION)
    store_payload["generatedAt"] = now_text
    store_payload["storeDayStatuses"] = statuses
    store_payload["machines"] = machine_summaries
    store_payload["summary"] = {
        "machineCount": len(machine_summaries),
        "latestDate": latest_date,
        "recordCount": record_count,
        "closedDateCount": closed_date_count,
    }

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    result.snapshot_key = f"snapshots/{store_id}/{target_date}_{target_date}_{timestamp}_manual_closed_day.json"
    snapshot_payload = build_snapshot(
        store_payload,
        store_name=store_name,
        store_url=store_url,
        target_date=target_date,
        status=status,
        result=result,
    )

    if not dry_run:
        storage.write_json(result.snapshot_key, snapshot_payload)
        storage.write_json(store_data_file, store_payload)
        update_index(
            ROOT_DIR / "apps" / "web" / "public" / "halldata-static",
            [build_index_store_entry(store_payload, store_data_file)],
            r2_storage=storage,
            allow_missing_r2_index=False,
        )
        update_full_day_index(
            storage,
            store_payload=store_payload,
            store_id=store_id,
            store_name=store_name,
            store_url=store_url,
            target_date=target_date,
            status=status,
            saved_at=now_text,
            snapshot_key=result.snapshot_key,
        )

    return result


def main() -> None:
    args = parse_args()
    target_date = validate_target_date(args.target_date)
    storage = R2JsonStorage.from_environment(ROOT_DIR)
    result = mark_store_closed_day(
        storage=storage,
        store_name=read_text(args.store_name),
        store_url=read_text(args.store_url),
        target_date=target_date,
        reason=read_text(args.reason) or "manual_closed_day",
        dry_run=bool(args.dry_run),
    )
    mode = "確認のみ" if args.dry_run else "保存完了"
    print(f"{mode}: {args.store_name} {target_date}")
    print(f"削除レコード: {result.removed_record_count}件")
    print(f"更新機種ファイル: {len(result.changed_machine_files)}件")
    print(f"削除機種ファイル: {len(result.deleted_machine_files)}件")
    if result.snapshot_key:
        print(f"記録: {result.snapshot_key}")


if __name__ == "__main__":
    main()
