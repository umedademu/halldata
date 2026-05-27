from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import tempfile
from typing import Any
from urllib.parse import quote, unquote, urlsplit, urlunsplit

from estimated_grape import ESTIMATED_GRAPE_VALUE_VERSION, calculate_estimated_grape_value
from machine_difference import (
    calculate_estimated_coin_hold_difference_value,
    calculate_machine_difference_value,
)
from r2_storage import R2JsonStorage
from setting_estimates import (
    SETTING_ESTIMATE_GRAPE_VALUE_VERSION,
    SETTING_ESTIMATE_MODE_GRAPE,
    SETTING_ESTIMATE_VALUE_VERSION,
    calculate_setting_estimate,
    get_setting_estimate_definition,
)


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_LOCAL_SAVE_DIR = ROOT_DIR / "local_data"
DEFAULT_WEB_DATA_DIR = ROOT_DIR / "apps" / "web" / "public" / "halldata-static"
DEFAULT_STORES_CSV = ROOT_DIR / "stores_rows.csv"
DEFAULT_RESULTS_CSV = ROOT_DIR / "machine_daily_results_rows.csv"
WEB_DATA_VERSION = 1
DATA_SOURCE_SITE7 = "site7"
SETTING_ESTIMATE_STATUS_CONFIRMED = "confirmed"
SETTING_ESTIMATE_STATUS_PROVISIONAL = "provisional"
ESTIMATED_GRAPE_FIELD_NAMES = (
    "estimated_grape_count",
    "estimated_grape_denominator",
    "estimated_grape_probability",
    "estimated_grape_status",
    "estimated_grape_source",
    "estimated_grape_version",
)
SETTING_ESTIMATE_GRAPE_FIELD_NAMES = (
    "setting_estimate_grape_average",
    "setting_estimate_grape_status",
    "setting_estimate_grape_source",
    "setting_estimate_grape_version",
)


class WebDataIndexMissingError(RuntimeError):
    pass


@dataclass
class StoreSource:
    store_name: str
    store_url: str
    legacy_ids: set[str] = field(default_factory=set)
    prefecture_name: str = ""
    area_name: str = ""
    event_day_tails: list[int] = field(default_factory=list)
    event_month_days: list[int] = field(default_factory=list)
    event_zoro: bool = False
    event_weekdays: list[int] = field(default_factory=list)
    event_source_text: str = ""


def normalize_store_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    parts = urlsplit(text)
    normalized_scheme = parts.scheme.lower()
    normalized_netloc = parts.netloc.lower()
    normalized_path = quote(unquote(parts.path or "/"), safe="/-_.~")
    if normalized_path != "/":
        normalized_path = normalized_path.rstrip("/") + "/"

    return urlunsplit((normalized_scheme, normalized_netloc, normalized_path, parts.query, ""))


def store_key(store_name: str, store_url: str) -> str:
    normalized_url = normalize_store_url(store_url)
    if normalized_url:
        return f"url:{normalized_url}"
    return f"name:{store_name.strip()}"


def build_store_id(store_name: str, store_url: str) -> str:
    key = store_key(store_name, store_url)
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
    return f"store-{digest}"


def build_machine_data_file(store_id: str, machine_name: str) -> str:
    digest = hashlib.sha1(str(machine_name or "").encode("utf-8")).hexdigest()[:12]
    return f"stores/{store_id}/machines/machine-{digest}.json"


def read_number(value: Any) -> float | int | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        number = Decimal(text)
    except (InvalidOperation, ValueError):
        return None
    if not number.is_finite():
        return None
    if number == number.to_integral_value():
        return int(number)
    return float(number)


def read_text(value: Any) -> str:
    return str(value or "").strip()


def site7_record_has_meaningful_data(record: dict[str, Any]) -> bool:
    numeric_keys = (
        "difference_value",
        "bonus_difference_value",
        "games_count",
        "payout_rate",
        "bb_count",
        "rb_count",
    )
    if any(record.get(key) is not None for key in numeric_keys):
        return True

    text_keys = ("combined_ratio_text", "bb_ratio_text", "rb_ratio_text")
    return any(str(record.get(key) or "").strip() not in {"", "-", "--"} for key in text_keys)


def record_should_be_exported(record: dict[str, Any]) -> bool:
    data_source = read_text(record.get("data_source"))
    if data_source.casefold() == DATA_SOURCE_SITE7 and not site7_record_has_meaningful_data(record):
        return False
    return True


def add_setting_estimate_fields(
    record: dict[str, Any],
    machine_name: str,
    data_source: str,
) -> None:
    definition = get_setting_estimate_definition(machine_name)
    setting_estimate = calculate_setting_estimate(definition, record) if definition else None
    setting_average = setting_estimate.get("average") if setting_estimate else None
    if not isinstance(setting_average, (int, float)):
        return

    is_site7 = data_source.casefold() == DATA_SOURCE_SITE7
    status = SETTING_ESTIMATE_STATUS_PROVISIONAL if is_site7 else SETTING_ESTIMATE_STATUS_CONFIRMED
    source = data_source if data_source else "minrepo"
    record["setting_estimate_average"] = setting_average
    record["setting_estimate_status"] = status
    record["setting_estimate_source"] = source
    record["setting_estimate_version"] = SETTING_ESTIMATE_VALUE_VERSION

    estimated_difference_value = calculate_estimated_coin_hold_difference_value(
        machine_name,
        record,
        setting_average=setting_average,
    )
    if estimated_difference_value is None:
        return

    record["estimated_difference_value"] = estimated_difference_value
    record["estimated_difference_status"] = status
    record["estimated_difference_source"] = source
    record["estimated_difference_version"] = SETTING_ESTIMATE_VALUE_VERSION


def add_estimated_grape_fields(
    record: dict[str, Any],
    machine_name: str,
    data_source: str,
) -> None:
    if data_source.casefold() == DATA_SOURCE_SITE7:
        return

    setting_average = record.get("setting_estimate_average")
    estimated_grape = calculate_estimated_grape_value(
        machine_name,
        record,
        setting_average=setting_average if isinstance(setting_average, (int, float)) else None,
    )
    if estimated_grape is None:
        return

    source = data_source if data_source else "minrepo"
    record["estimated_grape_count"] = estimated_grape["count"]
    record["estimated_grape_denominator"] = estimated_grape["denominator"]
    record["estimated_grape_probability"] = estimated_grape["probability"]
    record["estimated_grape_status"] = SETTING_ESTIMATE_STATUS_CONFIRMED
    record["estimated_grape_source"] = source
    record["estimated_grape_version"] = ESTIMATED_GRAPE_VALUE_VERSION


def add_grape_setting_estimate_fields(
    record: dict[str, Any],
    machine_name: str,
    data_source: str,
) -> None:
    definition = get_setting_estimate_definition(machine_name)
    setting_estimate = (
        calculate_setting_estimate(definition, record, mode=SETTING_ESTIMATE_MODE_GRAPE)
        if definition
        else None
    )
    if not setting_estimate or setting_estimate.get("source_mode") != SETTING_ESTIMATE_MODE_GRAPE:
        return

    setting_average = setting_estimate.get("average")
    if not isinstance(setting_average, (int, float)):
        return

    is_site7 = data_source.casefold() == DATA_SOURCE_SITE7
    status = SETTING_ESTIMATE_STATUS_PROVISIONAL if is_site7 else SETTING_ESTIMATE_STATUS_CONFIRMED
    source = data_source if data_source else "minrepo"
    record["setting_estimate_grape_average"] = setting_average
    record["setting_estimate_grape_status"] = status
    record["setting_estimate_grape_source"] = source
    record["setting_estimate_grape_version"] = SETTING_ESTIMATE_GRAPE_VALUE_VERSION


def read_prefecture_name(value: dict[str, Any]) -> str:
    return read_text(
        value.get("prefectureName")
        or value.get("prefecture_name")
        or value.get("site7Prefecture")
        or value.get("site7_prefecture")
        or value.get("prefecture")
        or value.get("都道府県")
    )


def read_area_name(value: dict[str, Any]) -> str:
    return read_text(
        value.get("areaName")
        or value.get("area_name")
        or value.get("site7Area")
        or value.get("site7_area")
        or value.get("area")
        or value.get("地域")
    )


def read_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on", "t"}:
        return True
    if text in {"0", "false", "no", "off", "f", ""}:
        return False
    return bool(text)


def normalize_event_values(value: Any, minimum: int, maximum: int) -> list[int]:
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("["):
            try:
                parsed_value = json.loads(text)
            except json.JSONDecodeError:
                parsed_value = []
            raw_values = parsed_value if isinstance(parsed_value, (list, tuple, set)) else []
        else:
            raw_values = [item.strip() for item in text.split(",") if item.strip()]
    else:
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


def apply_event_fields(store_source: StoreSource, value: dict[str, Any]) -> None:
    if not store_source.event_day_tails:
        store_source.event_day_tails = normalize_event_values(
            value.get("eventDayTails", value.get("event_day_tails", [])),
            0,
            9,
        )
    if not store_source.event_month_days:
        store_source.event_month_days = normalize_event_values(
            value.get("eventMonthDays", value.get("event_month_days", [])),
            1,
            31,
        )
    if not store_source.event_zoro:
        store_source.event_zoro = read_bool(value.get("eventZoro", value.get("event_zoro", False)))
    if not store_source.event_weekdays:
        store_source.event_weekdays = normalize_event_values(
            value.get("eventWeekdays", value.get("event_weekdays", [])),
            0,
            6,
        )
    if not store_source.event_source_text:
        store_source.event_source_text = read_text(
            value.get("eventSourceText", value.get("event_source_text", "")),
        )


def build_store_event_payload(store_source: StoreSource) -> dict[str, Any]:
    return {
        "eventDayTails": normalize_event_values(store_source.event_day_tails, 0, 9),
        "eventMonthDays": normalize_event_values(store_source.event_month_days, 1, 31),
        "eventZoro": bool(store_source.event_zoro),
        "eventWeekdays": normalize_event_values(store_source.event_weekdays, 0, 6),
        "eventSourceText": read_text(store_source.event_source_text),
    }


def average(values: list[float | int | None]) -> float | None:
    numeric_values = [float(value) for value in values if isinstance(value, (int, float))]
    if not numeric_values:
        return None
    return sum(numeric_values) / len(numeric_values)


def compare_slot_key(slot_number: str) -> tuple[int, int | str]:
    try:
        return (0, int(slot_number))
    except ValueError:
        return (1, slot_number)


def safe_record(
    raw_record: dict[str, Any],
    store_id: str | None = None,
    site7_fetched_at: str | None = None,
) -> dict[str, Any] | None:
    machine_name = read_text(raw_record.get("machine_name"))
    target_date = read_text(raw_record.get("target_date"))
    slot_number = read_text(raw_record.get("slot_number"))
    if not machine_name or not target_date or not slot_number:
        return None

    record = {
        "machine_name": machine_name,
        "target_date": target_date,
        "slot_number": slot_number,
        "difference_value": read_number(raw_record.get("difference_value")),
        "bonus_difference_value": read_number(raw_record.get("bonus_difference_value")),
        "games_count": read_number(raw_record.get("games_count")),
        "payout_rate": read_number(raw_record.get("payout_rate")),
        "bb_count": read_number(raw_record.get("bb_count")),
        "rb_count": read_number(raw_record.get("rb_count")),
        "combined_ratio_text": read_text(raw_record.get("combined_ratio_text")) or None,
        "bb_ratio_text": read_text(raw_record.get("bb_ratio_text")) or None,
        "rb_ratio_text": read_text(raw_record.get("rb_ratio_text")) or None,
    }
    if record["bonus_difference_value"] is None:
        record["bonus_difference_value"] = calculate_machine_difference_value(machine_name, raw_record)
    data_source = read_text(raw_record.get("data_source"))
    if data_source:
        record["data_source"] = data_source
    fetched_at = read_text(
        raw_record.get("site7_fetched_at")
        or raw_record.get("site7FetchedAt")
        or raw_record.get("saved_at")
        or site7_fetched_at
    )
    if data_source.casefold() == DATA_SOURCE_SITE7 and fetched_at:
        record["site7_fetched_at"] = fetched_at
    if not record_should_be_exported(record):
        return None
    add_setting_estimate_fields(record, machine_name, data_source)
    add_estimated_grape_fields(record, machine_name, data_source)
    add_grape_setting_estimate_fields(record, machine_name, data_source)
    if store_id:
        record["store_id"] = store_id
    return record


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        delete=False,
        suffix=".tmp",
    ) as temp_file:
        json.dump(payload, temp_file, ensure_ascii=False, separators=(",", ":"))
        temp_path = Path(temp_file.name)
    temp_path.replace(path)


def write_json_payload(
    web_data_dir: Path,
    relative_path: str,
    payload: dict[str, Any],
    *,
    r2_storage: R2JsonStorage | None = None,
) -> None:
    normalized_path = str(relative_path).replace("\\", "/").lstrip("/")
    if r2_storage is not None:
        r2_storage.write_json(normalized_path, payload)
        return
    write_json_atomic(web_data_dir / normalized_path, payload)


def load_existing_index(
    web_data_dir: Path,
    *,
    r2_storage: R2JsonStorage | None = None,
    allow_missing_r2_index: bool = False,
) -> dict[str, Any]:
    if r2_storage is not None:
        payload = r2_storage.read_json("index.json")
        if payload is None:
            if allow_missing_r2_index:
                return {"version": WEB_DATA_VERSION, "stores": []}
            raise WebDataIndexMissingError(
                "R2上のindex.jsonが見つからないため、公開用店舗一覧の更新を中止しました。"
            )
        if not isinstance(payload, dict):
            raise ValueError("R2上のindex.jsonの形式が不正です。")
        if isinstance(payload.get("stores"), list):
            return payload
        raise ValueError("R2上のindex.jsonの形式が不正です。")

    index_path = web_data_dir / "index.json"
    if not index_path.exists():
        return {"version": WEB_DATA_VERSION, "stores": []}
    try:
        payload = json.loads(index_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"version": WEB_DATA_VERSION, "stores": []}
    if not isinstance(payload, dict) or not isinstance(payload.get("stores"), list):
        return {"version": WEB_DATA_VERSION, "stores": []}
    return payload


def load_existing_payload(
    web_data_dir: Path,
    relative_path: str,
    *,
    r2_storage: R2JsonStorage | None = None,
) -> dict[str, Any] | None:
    normalized_path = str(relative_path).replace("\\", "/").lstrip("/")
    if not normalized_path:
        return None
    if r2_storage is not None:
        return r2_storage.read_json(normalized_path)

    payload_path = web_data_dir / normalized_path
    if not payload_path.exists():
        return None
    try:
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    return payload if isinstance(payload, dict) else None


def build_store_payload(store_source: StoreSource, records: list[dict[str, Any]]) -> dict[str, Any]:
    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    store_id = build_store_id(store_source.store_name, store_source.store_url)
    export_records = [
        record
        for record in records
        if isinstance(record, dict) and record_should_be_exported(record)
    ]
    sorted_records = sorted(
        export_records,
        key=lambda record: (
            str(record.get("target_date", "")),
            str(record.get("machine_name", "")),
            compare_slot_key(str(record.get("slot_number", ""))),
        ),
        reverse=True,
    )

    machines = build_machine_summaries(sorted_records)
    machine_records_by_file: dict[str, dict[str, Any]] = {}
    for machine in machines:
        machine_name = str(machine.get("machineName", "")).strip()
        machine_records = [
            record
            for record in sorted_records
            if str(record.get("machine_name", "")).strip() == machine_name
        ]
        data_file = build_machine_data_file(store_id, machine_name)
        machine["dataFile"] = data_file
        machine_records_by_file[data_file] = {
            "version": WEB_DATA_VERSION,
            "generatedAt": generated_at,
            "store": {
                "id": store_id,
                "legacyIds": sorted(store_source.legacy_ids),
                "storeName": store_source.store_name,
                "storeUrl": normalize_store_url(store_source.store_url),
                "prefectureName": read_text(store_source.prefecture_name),
                "areaName": read_text(store_source.area_name),
                **build_store_event_payload(store_source),
            },
            "machineName": machine_name,
            "records": machine_records,
        }

    latest_date = max((str(record.get("target_date", "")) for record in sorted_records), default=None)
    return {
        "version": WEB_DATA_VERSION,
        "generatedAt": generated_at,
        "_machineRecordsByFile": machine_records_by_file,
        "store": {
            "id": store_id,
            "legacyIds": sorted(store_source.legacy_ids),
            "storeName": store_source.store_name,
            "storeUrl": normalize_store_url(store_source.store_url),
            "prefectureName": read_text(store_source.prefecture_name),
            "areaName": read_text(store_source.area_name),
            **build_store_event_payload(store_source),
        },
        "summary": {
            "machineCount": len(machines),
            "latestDate": latest_date,
            "recordCount": len(sorted_records),
        },
        "machines": machines,
    }


def build_machine_summaries(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest_date_by_machine: dict[str, str] = {}
    records_by_machine_latest_date: dict[str, list[dict[str, Any]]] = {}

    for record in records:
        machine_name = str(record.get("machine_name", "")).strip()
        target_date = str(record.get("target_date", "")).strip()
        if not machine_name or not target_date:
            continue
        if target_date > latest_date_by_machine.get(machine_name, ""):
            latest_date_by_machine[machine_name] = target_date
            records_by_machine_latest_date[machine_name] = [record]
        elif target_date == latest_date_by_machine.get(machine_name):
            records_by_machine_latest_date.setdefault(machine_name, []).append(record)

    machines = []
    for machine_name, latest_date in latest_date_by_machine.items():
        latest_records = records_by_machine_latest_date.get(machine_name, [])
        machines.append(
            {
                "machineName": machine_name,
                "slotCount": len({str(record.get("slot_number", "")).strip() for record in latest_records}),
                "latestDate": latest_date,
                "latestAverageDifference": average([record.get("difference_value") for record in latest_records]),
                "latestAverageGames": average([record.get("games_count") for record in latest_records]),
                "latestAveragePayout": average([record.get("payout_rate") for record in latest_records]),
            }
        )

    return sorted(
        machines,
        key=lambda machine: (
            str(machine.get("latestDate") or ""),
            int(machine.get("slotCount") or 0),
            str(machine.get("machineName") or ""),
        ),
        reverse=True,
    )


def build_index_store_entry(store_payload: dict[str, Any], data_file: str) -> dict[str, Any]:
    store = store_payload.get("store", {})
    summary = store_payload.get("summary", {})
    return {
        "id": store.get("id"),
        "legacyIds": store.get("legacyIds", []),
        "storeName": store.get("storeName"),
        "storeUrl": store.get("storeUrl"),
        "prefectureName": store.get("prefectureName"),
        "areaName": store.get("areaName"),
        "eventDayTails": store.get("eventDayTails", []),
        "eventMonthDays": store.get("eventMonthDays", []),
        "eventZoro": bool(store.get("eventZoro", False)),
        "eventWeekdays": store.get("eventWeekdays", []),
        "eventSourceText": store.get("eventSourceText", ""),
        "machineCount": summary.get("machineCount", 0),
        "latestDate": summary.get("latestDate"),
        "recordCount": summary.get("recordCount", 0),
        "dataFile": data_file,
    }


def build_registered_store_index_entry(
    store_source: StoreSource,
    store_id: str,
    *,
    existing_entry: dict[str, Any] | None = None,
    data_file: str = "",
) -> dict[str, Any]:
    existing_entry = existing_entry if isinstance(existing_entry, dict) else {}
    legacy_ids = {
        str(legacy_id).strip()
        for legacy_id in existing_entry.get("legacyIds", [])
        if str(legacy_id).strip()
    }
    legacy_ids.update(store_source.legacy_ids)
    event_payload = build_store_event_payload(store_source)
    if not (
        event_payload["eventDayTails"] or
        event_payload["eventMonthDays"] or
        event_payload["eventZoro"] or
        event_payload["eventWeekdays"] or
        event_payload["eventSourceText"]
    ):
        event_payload = {
            "eventDayTails": normalize_event_values(existing_entry.get("eventDayTails", []), 0, 9),
            "eventMonthDays": normalize_event_values(existing_entry.get("eventMonthDays", []), 1, 31),
            "eventZoro": read_bool(existing_entry.get("eventZoro", False)),
            "eventWeekdays": normalize_event_values(existing_entry.get("eventWeekdays", []), 0, 6),
            "eventSourceText": read_text(existing_entry.get("eventSourceText", "")),
        }
    return {
        "id": store_id,
        "legacyIds": sorted(legacy_ids),
        "storeName": store_source.store_name,
        "storeUrl": normalize_store_url(store_source.store_url),
        "prefectureName": read_text(store_source.prefecture_name),
        "areaName": read_text(store_source.area_name),
        **event_payload,
        "machineCount": int(existing_entry.get("machineCount") or 0),
        "latestDate": existing_entry.get("latestDate"),
        "recordCount": int(existing_entry.get("recordCount") or 0),
        "dataFile": data_file or str(existing_entry.get("dataFile") or "").strip(),
    }


def update_index(
    web_data_dir: Path,
    store_entries: list[dict[str, Any]],
    *,
    r2_storage: R2JsonStorage | None = None,
    allow_missing_r2_index: bool = False,
) -> None:
    index_payload = load_existing_index(
        web_data_dir,
        r2_storage=r2_storage,
        allow_missing_r2_index=allow_missing_r2_index,
    )
    existing_entries = {
        str(entry.get("id")): entry
        for entry in index_payload.get("stores", [])
        if isinstance(entry, dict) and entry.get("id")
    }
    for entry in store_entries:
        existing_entry = existing_entries.get(str(entry["id"]))
        legacy_ids = {
            str(legacy_id).strip()
            for legacy_id in entry.get("legacyIds", [])
            if str(legacy_id).strip()
        }
        if isinstance(existing_entry, dict):
            legacy_ids.update(
                str(legacy_id).strip()
                for legacy_id in existing_entry.get("legacyIds", [])
                if str(legacy_id).strip()
            )
        entry["legacyIds"] = sorted(legacy_ids)
        existing_entries[str(entry["id"])] = entry

    stores = sorted(
        existing_entries.values(),
        key=lambda entry: (
            not bool(str(entry.get("storeName") or "").strip()),
            str(entry.get("storeName") or entry.get("storeUrl") or ""),
        ),
    )
    write_json_payload(
        web_data_dir,
        "index.json",
        {
            "version": WEB_DATA_VERSION,
            "generatedAt": datetime.now().astimezone().isoformat(timespec="seconds"),
            "stores": stores,
        },
        r2_storage=r2_storage,
    )


def export_store_payloads(
    web_data_dir: Path,
    store_payloads: list[dict[str, Any]],
    *,
    r2_storage: R2JsonStorage | None = None,
    allow_missing_r2_index: bool = False,
) -> list[dict[str, Any]]:
    store_entries = []
    for store_payload in store_payloads:
        machine_records_by_file = store_payload.pop("_machineRecordsByFile", {})
        for data_file, machine_payload in machine_records_by_file.items():
            write_json_payload(web_data_dir, data_file, machine_payload, r2_storage=r2_storage)

        store = store_payload["store"]
        data_file = f"stores/{store['id']}.json"
        write_json_payload(web_data_dir, data_file, store_payload, r2_storage=r2_storage)
        store_entries.append(build_index_store_entry(store_payload, data_file))
    update_index(
        web_data_dir,
        store_entries,
        r2_storage=r2_storage,
        allow_missing_r2_index=allow_missing_r2_index,
    )
    return store_entries


def export_registered_store_payloads(
    web_data_dir: Path,
    store_sources: list[StoreSource],
    *,
    r2_storage: R2JsonStorage | None = None,
) -> list[dict[str, Any]]:
    index_payload = load_existing_index(
        web_data_dir,
        r2_storage=r2_storage,
    )
    existing_entries = {
        str(entry.get("id")): entry
        for entry in index_payload.get("stores", [])
        if isinstance(entry, dict) and entry.get("id")
    }
    metadata_only_entries: list[dict[str, Any]] = []

    for store_source in store_sources:
        store_id = build_store_id(store_source.store_name, store_source.store_url)
        existing_entry = existing_entries.get(store_id)
        if isinstance(existing_entry, dict) and existing_entry.get("dataFile"):
            metadata_only_entries.append(
                build_registered_store_index_entry(
                    store_source,
                    store_id,
                    existing_entry=existing_entry,
                    data_file=str(existing_entry.get("dataFile") or "").strip(),
                )
            )
            continue

        data_file = f"stores/{store_id}.json"
        existing_payload = load_existing_payload(
            web_data_dir,
            data_file,
            r2_storage=r2_storage,
        )
        if existing_payload is not None:
            existing_entry = build_index_store_entry(existing_payload, data_file)

        metadata_only_entries.append(
            build_registered_store_index_entry(
                store_source,
                store_id,
                existing_entry=existing_entry,
                data_file=data_file if existing_payload is not None else "",
            )
        )

    exported_entries = []
    if metadata_only_entries:
        update_index(
            web_data_dir,
            metadata_only_entries,
            r2_storage=r2_storage,
        )
        exported_entries.extend(metadata_only_entries)

    return exported_entries


def load_store_sources_from_csv(stores_csv: Path) -> dict[str, StoreSource]:
    store_sources: dict[str, StoreSource] = {}
    if not stores_csv.exists():
        return store_sources

    with stores_csv.open("r", encoding="utf-8-sig", newline="") as csv_file:
        for row in csv.DictReader(csv_file):
            store_name = read_text(row.get("store_name"))
            store_url = normalize_store_url(read_text(row.get("store_url")))
            if not store_name and not store_url:
                continue
            key = store_key(store_name, store_url)
            store_source = store_sources.setdefault(
                key,
                StoreSource(
                    store_name=store_name or store_url,
                    store_url=store_url,
                    prefecture_name=read_prefecture_name(row),
                    area_name=read_area_name(row),
                    event_day_tails=normalize_event_values(row.get("event_day_tails", []), 0, 9),
                    event_month_days=normalize_event_values(row.get("event_month_days", []), 1, 31),
                    event_zoro=read_bool(row.get("event_zoro", False)),
                    event_weekdays=normalize_event_values(row.get("event_weekdays", []), 0, 6),
                    event_source_text=read_text(row.get("event_source_text")),
                ),
            )
            if store_name and not store_source.store_name:
                store_source.store_name = store_name
            if not store_source.prefecture_name:
                store_source.prefecture_name = read_prefecture_name(row)
            if not store_source.area_name:
                store_source.area_name = read_area_name(row)
            apply_event_fields(store_source, row)
            legacy_id = read_text(row.get("id"))
            if legacy_id:
                store_source.legacy_ids.add(legacy_id)
    return store_sources


def export_from_csv(
    *,
    stores_csv: Path = DEFAULT_STORES_CSV,
    results_csv: Path = DEFAULT_RESULTS_CSV,
    web_data_dir: Path = DEFAULT_WEB_DATA_DIR,
    r2_storage: R2JsonStorage | None = None,
) -> list[dict[str, Any]]:
    store_sources = load_store_sources_from_csv(stores_csv)
    legacy_store_id_to_key = {
        legacy_id: key
        for key, store_source in store_sources.items()
        for legacy_id in store_source.legacy_ids
    }
    records_by_store_key: dict[str, dict[tuple[str, str], dict[str, Any]]] = {}

    with results_csv.open("r", encoding="utf-8-sig", newline="") as csv_file:
        for row in csv.DictReader(csv_file):
            legacy_store_id = read_text(row.get("store_id"))
            key = legacy_store_id_to_key.get(legacy_store_id)
            if not key:
                continue
            record = safe_record(row, store_id=legacy_store_id)
            if record is None:
                continue
            record_key = (str(record["target_date"]), str(record["slot_number"]))
            records_by_store_key.setdefault(key, {})[record_key] = record

    store_payloads = [
        build_store_payload(store_source, list(records_by_key.values()))
        for key, store_source in store_sources.items()
        if (records_by_key := records_by_store_key.get(key))
    ]
    return export_store_payloads(web_data_dir, store_payloads, r2_storage=r2_storage)


def load_snapshot(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    return payload if isinstance(payload, dict) else None


def collect_store_records_from_local_store_dir(store_dir: Path) -> tuple[StoreSource | None, list[dict[str, Any]]]:
    store_source: StoreSource | None = None
    records_by_key: dict[tuple[str, str], tuple[str, dict[str, Any]]] = {}

    for snapshot_path in sorted(store_dir.glob("*.json")):
        if snapshot_path.name == "_full_day_index.json":
            continue
        snapshot = load_snapshot(snapshot_path)
        if not snapshot:
            continue

        store_payload = snapshot.get("store") if isinstance(snapshot.get("store"), dict) else {}
        store_name = read_text(store_payload.get("store_name")) or store_dir.name
        store_url = normalize_store_url(read_text(store_payload.get("store_url")))
        store_source = store_source or StoreSource(
            store_name=store_name,
            store_url=store_url,
            prefecture_name=read_prefecture_name(store_payload),
            area_name=read_area_name(store_payload),
            event_day_tails=normalize_event_values(store_payload.get("event_day_tails", []), 0, 9),
            event_month_days=normalize_event_values(store_payload.get("event_month_days", []), 1, 31),
            event_zoro=read_bool(store_payload.get("event_zoro", False)),
            event_weekdays=normalize_event_values(store_payload.get("event_weekdays", []), 0, 6),
            event_source_text=read_text(store_payload.get("event_source_text")),
        )
        if not store_source.prefecture_name:
            store_source.prefecture_name = read_prefecture_name(store_payload)
        if not store_source.area_name:
            store_source.area_name = read_area_name(store_payload)
        apply_event_fields(store_source, store_payload)
        saved_at = read_text(snapshot.get("saved_at")) or datetime.fromtimestamp(
            snapshot_path.stat().st_mtime,
        ).isoformat()

        for raw_record in snapshot.get("records", []):
            if not isinstance(raw_record, dict):
                continue
            record = safe_record(raw_record, site7_fetched_at=saved_at)
            if record is None:
                continue
            record_key = (str(record["target_date"]), str(record["slot_number"]))
            current_saved_at = records_by_key.get(record_key, ("", {}))[0]
            if saved_at >= current_saved_at:
                records_by_key[record_key] = (saved_at, record)

    return store_source, [entry[1] for entry in records_by_key.values()]


def export_store_from_local_data(
    store_name: str,
    *,
    local_save_dir: Path = DEFAULT_LOCAL_SAVE_DIR,
    web_data_dir: Path = DEFAULT_WEB_DATA_DIR,
    r2_storage: R2JsonStorage | None = None,
) -> dict[str, Any] | None:
    store_dir = local_save_dir / sanitize_file_name(store_name)
    if not store_dir.exists():
        return None
    store_source, records = collect_store_records_from_local_store_dir(store_dir)
    if store_source is None or not records:
        return None
    store_payload = build_store_payload(store_source, records)
    entries = export_store_payloads(web_data_dir, [store_payload], r2_storage=r2_storage)
    return entries[0] if entries else None


def export_from_local_data(
    *,
    local_save_dir: Path = DEFAULT_LOCAL_SAVE_DIR,
    web_data_dir: Path = DEFAULT_WEB_DATA_DIR,
    r2_storage: R2JsonStorage | None = None,
) -> list[dict[str, Any]]:
    store_payloads = []
    for store_dir in sorted([path for path in local_save_dir.iterdir() if path.is_dir()]):
        store_source, records = collect_store_records_from_local_store_dir(store_dir)
        if store_source is None or not records:
            continue
        store_payloads.append(build_store_payload(store_source, records))
    return export_store_payloads(web_data_dir, store_payloads, r2_storage=r2_storage)


def sanitize_file_name(value: str) -> str:
    return "".join("_" if char in '<>:"/\\|?*' else char for char in str(value)).strip() or "store"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Web表示用データを生成します。")
    parser.add_argument("--source", choices=["csv", "local"], default="csv")
    parser.add_argument("--destination", choices=["r2", "local"], default="r2")
    parser.add_argument("--stores-csv", type=Path, default=DEFAULT_STORES_CSV)
    parser.add_argument("--results-csv", type=Path, default=DEFAULT_RESULTS_CSV)
    parser.add_argument("--local-save-dir", type=Path, default=DEFAULT_LOCAL_SAVE_DIR)
    parser.add_argument("--web-data-dir", type=Path, default=DEFAULT_WEB_DATA_DIR)
    parser.add_argument("--store-name", default="")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    r2_storage = None
    if args.destination == "r2":
        r2_storage = R2JsonStorage.from_environment(ROOT_DIR)
        if not r2_storage.is_configured:
            raise SystemExit(".env.local に R2 の接続情報を設定してください。")

    if args.source == "csv":
        entries = export_from_csv(
            stores_csv=args.stores_csv,
            results_csv=args.results_csv,
            web_data_dir=args.web_data_dir,
            r2_storage=r2_storage,
        )
    elif args.store_name:
        entry = export_store_from_local_data(
            args.store_name,
            local_save_dir=args.local_save_dir,
            web_data_dir=args.web_data_dir,
            r2_storage=r2_storage,
        )
        entries = [entry] if entry else []
    else:
        entries = export_from_local_data(
            local_save_dir=args.local_save_dir,
            web_data_dir=args.web_data_dir,
            r2_storage=r2_storage,
        )

    print(f"{len(entries)}店舗分のWeb表示用データを生成しました。")


if __name__ == "__main__":
    main()
