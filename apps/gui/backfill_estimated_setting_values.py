from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from r2_storage import R2JsonStorage
from web_data_export import add_setting_estimate_fields


ROOT_DIR = Path(__file__).resolve().parents[2]
SETTING_ESTIMATE_FIELD_NAMES = (
    "setting_estimate_average",
    "setting_estimate_status",
    "setting_estimate_source",
    "setting_estimate_version",
    "estimated_difference_value",
    "estimated_difference_status",
    "estimated_difference_source",
    "estimated_difference_version",
)


@dataclass
class BackfillSummary:
    store_count: int = 0
    machine_file_count: int = 0
    changed_machine_file_count: int = 0
    record_count: int = 0
    updated_record_count: int = 0
    skipped_record_count: int = 0


@dataclass(frozen=True)
class MachineFileTarget:
    store_name: str
    machine_name: str
    data_file: str


def backfill_record(machine_name: str, record: dict[str, Any]) -> tuple[bool, bool]:
    before_values = {field_name: record.get(field_name) for field_name in SETTING_ESTIMATE_FIELD_NAMES}
    for field_name in SETTING_ESTIMATE_FIELD_NAMES:
        record.pop(field_name, None)

    data_source = str(record.get("data_source") or "").strip()
    add_setting_estimate_fields(record, machine_name, data_source)
    updated = record.get("setting_estimate_average") not in (None, "")
    after_values = {field_name: record.get(field_name) for field_name in SETTING_ESTIMATE_FIELD_NAMES}
    changed = before_values != after_values
    return updated, changed


def backfill_machine_payload(machine_payload: dict[str, Any]) -> tuple[int, int, int, bool]:
    machine_name = str(machine_payload.get("machineName") or "").strip()
    records = machine_payload.get("records")
    if not machine_name or not isinstance(records, list):
        return 0, 0, 0, False

    record_count = 0
    updated_count = 0
    skipped_count = 0
    changed_count = 0
    for record in records:
        if not isinstance(record, dict):
            continue
        record_count += 1
        updated, changed = backfill_record(machine_name, record)
        if updated and changed:
            updated_count += 1
        elif not updated:
            skipped_count += 1
        if changed:
            changed_count += 1

    return record_count, updated_count, skipped_count, changed_count > 0


def collect_machine_file_targets(storage: R2JsonStorage) -> tuple[int, list[MachineFileTarget]]:
    index_payload = storage.read_json("index.json")
    stores = index_payload.get("stores", []) if isinstance(index_payload, dict) else []
    targets: list[MachineFileTarget] = []

    for store_entry in stores:
        if not isinstance(store_entry, dict):
            continue
        store_name = str(store_entry.get("storeName") or "").strip()
        store_data_file = str(store_entry.get("dataFile") or "").strip()
        if not store_data_file:
            continue

        store_payload = storage.read_json(store_data_file)
        machines = store_payload.get("machines", []) if isinstance(store_payload, dict) else []
        for machine_entry in machines:
            if not isinstance(machine_entry, dict):
                continue
            machine_data_file = str(machine_entry.get("dataFile") or "").strip()
            machine_name = str(machine_entry.get("machineName") or "").strip()
            if not machine_data_file:
                continue
            targets.append(MachineFileTarget(store_name, machine_name, machine_data_file))

    store_count = len([store for store in stores if isinstance(store, dict)])
    return store_count, targets


def backfill_machine_file(root_dir: Path, target: MachineFileTarget) -> tuple[int, int, int, bool]:
    storage = R2JsonStorage.from_environment(root_dir)
    machine_payload = storage.read_json(target.data_file)
    if not isinstance(machine_payload, dict):
        return 0, 0, 0, False

    record_count, updated_count, skipped_count, changed = backfill_machine_payload(machine_payload)
    if changed:
        storage.write_json(target.data_file, machine_payload)
    return record_count, updated_count, skipped_count, changed


def backfill_r2_web_data(root_dir: Path = ROOT_DIR, workers: int = 12) -> BackfillSummary:
    storage = R2JsonStorage.from_environment(root_dir)
    storage.require_config()
    store_count, targets = collect_machine_file_targets(storage)
    summary = BackfillSummary(store_count=store_count, machine_file_count=len(targets))
    safe_workers = max(1, workers)

    processed_count = 0
    with ThreadPoolExecutor(max_workers=safe_workers) as executor:
        futures = [executor.submit(backfill_machine_file, root_dir, target) for target in targets]
        for future in as_completed(futures):
            record_count, updated_count, skipped_count, changed = future.result()
            processed_count += 1
            summary.record_count += record_count
            summary.updated_record_count += updated_count
            summary.skipped_record_count += skipped_count
            if changed:
                summary.changed_machine_file_count += 1
            if processed_count % 100 == 0 or processed_count == len(targets):
                print(
                    f"{processed_count}/{len(targets)}件の機種ファイルを確認しました。"
                    f" 補完行{summary.updated_record_count}件。",
                    flush=True,
                )

    return summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="R2上のWeb表示用JSONへ推定設定と推定設定基準差枚を追加します。",
    )
    parser.add_argument("--workers", type=int, default=12, help="同時に処理する機種ファイル数")
    args = parser.parse_args()
    summary = backfill_r2_web_data(ROOT_DIR, workers=args.workers)
    print(
        "R2の既存Web表示用JSONを更新しました。"
        f" 店舗{summary.store_count}件、機種ファイル{summary.machine_file_count}件、"
        f"更新ファイル{summary.changed_machine_file_count}件、"
        f"確認行{summary.record_count}件、補完行{summary.updated_record_count}件、"
        f"計算不可{summary.skipped_record_count}件。"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
