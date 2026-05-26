from __future__ import annotations

import argparse
import copy
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import Any

from r2_storage import R2JsonStorage, normalize_r2_key
from setting_estimates import get_setting_estimate_definition
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
    changed_record_count: int = 0
    backup_file_count: int = 0


@dataclass(frozen=True)
class MachineFileTarget:
    store_id: str
    store_name: str
    machine_name: str
    data_file: str


@dataclass(frozen=True)
class MachineFileResult:
    record_count: int
    updated_count: int
    skipped_count: int
    changed_count: int
    changed: bool
    backed_up: bool


@dataclass(frozen=True)
class StoreTargetsResult:
    targets: list[MachineFileTarget]


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

    return record_count, updated_count, skipped_count, changed_count, changed_count > 0


def collect_machine_file_targets(
    storage: R2JsonStorage,
    *,
    root_dir: Path = ROOT_DIR,
    workers: int = 12,
    store_id_filter: str = "",
    store_name_filter: str = "",
    machine_name_filter: str = "",
) -> tuple[int, list[MachineFileTarget]]:
    index_payload = storage.read_json("index.json")
    stores = index_payload.get("stores", []) if isinstance(index_payload, dict) else []
    matched_stores: list[dict[str, Any]] = []
    targets: list[MachineFileTarget] = []

    for store_entry in stores:
        if not isinstance(store_entry, dict):
            continue
        store_id = str(store_entry.get("id") or store_entry.get("storeId") or "").strip()
        store_name = str(store_entry.get("storeName") or "").strip()
        store_data_file = str(store_entry.get("dataFile") or "").strip()
        if not store_data_file:
            continue
        if store_id_filter and store_id != store_id_filter:
            continue
        if store_name_filter and store_name_filter not in store_name:
            continue
        matched_stores.append(store_entry)

    safe_workers = max(1, workers)
    processed_count = 0
    with ThreadPoolExecutor(max_workers=safe_workers) as executor:
        futures = [
            executor.submit(collect_store_machine_file_targets, root_dir, store_entry, machine_name_filter)
            for store_entry in matched_stores
        ]
        for future in as_completed(futures):
            result = future.result()
            targets.extend(result.targets)
            processed_count += 1
            if processed_count % 25 == 0 or processed_count == len(matched_stores):
                print(
                    f"{processed_count}/{len(matched_stores)}件の店舗から機種一覧を読み込みました。",
                    flush=True,
                )

    return len(matched_stores), targets


def collect_store_machine_file_targets(
    root_dir: Path,
    store_entry: dict[str, Any],
    machine_name_filter: str = "",
) -> StoreTargetsResult:
    storage = R2JsonStorage.from_environment(root_dir)
    store_id = str(store_entry.get("id") or store_entry.get("storeId") or "").strip()
    store_name = str(store_entry.get("storeName") or "").strip()
    store_data_file = str(store_entry.get("dataFile") or "").strip()
    if not store_data_file:
        return StoreTargetsResult([])

    store_payload = storage.read_json(store_data_file)
    machines = store_payload.get("machines", []) if isinstance(store_payload, dict) else []
    targets: list[MachineFileTarget] = []
    for machine_entry in machines:
        if not isinstance(machine_entry, dict):
            continue
        machine_data_file = str(machine_entry.get("dataFile") or "").strip()
        machine_name = str(machine_entry.get("machineName") or "").strip()
        if not machine_data_file:
            continue
        if machine_name_filter and machine_name_filter not in machine_name:
            continue
        if not get_setting_estimate_definition(machine_name):
            continue
        targets.append(MachineFileTarget(store_id, store_name, machine_name, machine_data_file))
    return StoreTargetsResult(targets)


def make_default_backup_dir(root_dir: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return root_dir / "local_data" / "backups" / f"estimated-setting-values-{timestamp}"


def write_backup_payload(backup_dir: Path, target: MachineFileTarget, payload: dict[str, Any]) -> None:
    normalized_key = normalize_r2_key(target.data_file)
    backup_path = backup_dir.joinpath(*normalized_key.split("/"))
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    backup_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def backfill_machine_file(
    root_dir: Path,
    target: MachineFileTarget,
    *,
    dry_run: bool = False,
    backup_dir: Path | None = None,
) -> MachineFileResult:
    storage = R2JsonStorage.from_environment(root_dir)
    machine_payload = storage.read_json(target.data_file)
    if not isinstance(machine_payload, dict):
        return MachineFileResult(0, 0, 0, 0, False, False)

    original_payload = copy.deepcopy(machine_payload) if backup_dir is not None and not dry_run else None
    record_count, updated_count, skipped_count, changed_count, changed = backfill_machine_payload(machine_payload)
    backed_up = False
    if changed and not dry_run:
        if backup_dir is not None and original_payload is not None:
            write_backup_payload(backup_dir, target, original_payload)
            backed_up = True
        storage.write_json(target.data_file, machine_payload)
    return MachineFileResult(record_count, updated_count, skipped_count, changed_count, changed, backed_up)


def backfill_r2_web_data(
    root_dir: Path = ROOT_DIR,
    workers: int = 12,
    *,
    dry_run: bool = False,
    backup_dir: Path | None = None,
    store_id_filter: str = "",
    store_name_filter: str = "",
    machine_name_filter: str = "",
) -> BackfillSummary:
    storage = R2JsonStorage.from_environment(root_dir)
    storage.require_config()
    safe_workers = max(1, workers)
    store_count, targets = collect_machine_file_targets(
        storage,
        root_dir=root_dir,
        workers=safe_workers,
        store_id_filter=store_id_filter,
        store_name_filter=store_name_filter,
        machine_name_filter=machine_name_filter,
    )
    summary = BackfillSummary(store_count=store_count, machine_file_count=len(targets))

    processed_count = 0
    with ThreadPoolExecutor(max_workers=safe_workers) as executor:
        futures = [
            executor.submit(
                backfill_machine_file,
                root_dir,
                target,
                dry_run=dry_run,
                backup_dir=backup_dir,
            )
            for target in targets
        ]
        for future in as_completed(futures):
            result = future.result()
            processed_count += 1
            summary.record_count += result.record_count
            summary.updated_record_count += result.updated_count
            summary.skipped_record_count += result.skipped_count
            summary.changed_record_count += result.changed_count
            if result.changed:
                summary.changed_machine_file_count += 1
            if result.backed_up:
                summary.backup_file_count += 1
            if processed_count % 100 == 0 or processed_count == len(targets):
                action = "確認" if dry_run else "更新"
                print(
                    f"{processed_count}/{len(targets)}件の機種ファイルを{action}しました。"
                    f" 変更予定行{summary.changed_record_count}件。",
                    flush=True,
                )

    return summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="R2上のWeb表示用JSONへ推定設定と推定設定基準差枚を追加します。",
    )
    parser.add_argument("--workers", type=int, default=12, help="同時に処理する機種ファイル数")
    parser.add_argument("--dry-run", action="store_true", help="R2へ保存せず、変更対象の件数だけ確認します")
    parser.add_argument("--backup-dir", type=Path, default=None, help="更新前の機種JSONを退避するフォルダー")
    parser.add_argument("--store-id", default="", help="指定した店舗IDだけ処理します")
    parser.add_argument("--store-name", default="", help="指定した文字列を含む店舗名だけ処理します")
    parser.add_argument("--machine-name", default="", help="指定した文字列を含む機種名だけ処理します")
    args = parser.parse_args()
    backup_dir = None if args.dry_run else args.backup_dir or make_default_backup_dir(ROOT_DIR)
    summary = backfill_r2_web_data(
        ROOT_DIR,
        workers=args.workers,
        dry_run=args.dry_run,
        backup_dir=backup_dir,
        store_id_filter=args.store_id.strip(),
        store_name_filter=args.store_name.strip(),
        machine_name_filter=args.machine_name.strip(),
    )
    action = "確認しました" if args.dry_run else "更新しました"
    print(
        f"R2の既存Web表示用JSONを{action}。"
        f" 店舗{summary.store_count}件、機種ファイル{summary.machine_file_count}件、"
        f"更新ファイル{summary.changed_machine_file_count}件、"
        f"確認行{summary.record_count}件、補完行{summary.updated_record_count}件、"
        f"変更行{summary.changed_record_count}件、計算不可{summary.skipped_record_count}件。"
    )
    if backup_dir is not None:
        print(f"更新前バックアップ: {backup_dir} ({summary.backup_file_count}件)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
