from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import queue
import re
import threading
import time
import tkinter as tk
from tkinter import messagebox, ttk
from typing import Callable, TypeVar
from urllib.parse import urlparse

try:
    import winsound
except ImportError:  # pragma: no cover
    winsound = None

try:
    import pystray
    from PIL import Image, ImageDraw
except ImportError:  # pragma: no cover
    pystray = None
    Image = None
    ImageDraw = None

from data_persistence import (
    HistoryPersistenceService,
    PersistenceSummary,
    RegisteredStoresPersistenceSummary,
    SavedFullDayDatesSummary,
    normalize_store_url,
)
from daidata_online_scraper import (
    DaidataOnlineMachineEntry,
    DaidataOnlineScraper,
    daidata_store_config_for,
)
from minrepo_scraper import (
    FetchProgress,
    MachineDataset,
    MachineHistoryResult,
    MinRepoScraper,
    ScraperError,
    StoreDatePage,
    StoreEventSettings,
    normalize_text,
    parse_date_range_input,
)
from machine_difference import canonical_machine_name, list_site7_target_machine_names
from site7_scraper import (
    DEFAULT_SITE7_PREFECTURE_NAME,
    SITE7_DATE_BOUNDARY_HOUR,
    SITE7_MAX_RECENT_DAYS,
    SITE7_TARGET_MACHINE_KEYWORDS,
    Site7FetchCancelled,
    Site7MachineEntry,
    Site7Scraper,
    Site7TargetStore,
    clamp_site7_recent_days,
    copy_site7_dataset_metadata,
    default_site7_store_settings,
    enrich_site7_target_store,
    site7_dataset_updated_at,
    site7_store_is_known_unavailable,
)


DEFAULT_STORE_NAME = "MJアリーナ箱崎店"
DEFAULT_STORE_URL = "https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/"
DEFAULT_RECENT_DAYS = "90"
DEFAULT_RETRY_DELAY_SECONDS = "10"
MAX_FETCH_RETRY_COUNT = 3
DEFAULT_MINREPO_DAY_PROGRESS_STEPS = 40
FETCH_PROGRESS_GLOBAL_SCALE = 1000
FETCH_PROGRESS_QUEUE_MIN_INTERVAL_SECONDS = 0.2
FETCH_PROGRESS_BAR_ANIMATION_INTERVAL_MS = 100
MINREPO_FETCH_MODE_NORMAL = "通常"
MINREPO_FETCH_MODE_FAST = "高速"
MINREPO_FETCH_MODE_STRONG = "強並列"
MINREPO_FETCH_MODE_OPTIONS = (
    MINREPO_FETCH_MODE_NORMAL,
    MINREPO_FETCH_MODE_FAST,
    MINREPO_FETCH_MODE_STRONG,
)
DEFAULT_MINREPO_FETCH_MODE = MINREPO_FETCH_MODE_STRONG
WEB_PUBLISH_MODE_DAYS = "days"
WEB_PUBLISH_MODE_STORE = "store"
DEFAULT_WEB_PUBLISH_INTERVAL_DAYS = 1
DEFAULT_SCHEDULE_HOUR = 2
DEFAULT_SCHEDULE_ALL_STORES_INTERVAL_DAYS = 14
MINREPO_PRIORITY_WATCH_START_HOUR = 0
MINREPO_PRIORITY_WATCH_START_MINUTE = 30
MINREPO_PRIORITY_WATCH_END_HOUR = 10
MINREPO_PRIORITY_WATCH_CHECK_INTERVAL_MINUTES = 15
SITE7_SCHEDULE_HOUR_OPTIONS = (0, 1, *range(10, 24))
DEFAULT_SITE7_SCHEDULE_HOURS = (12, 15, 18, 21)
SITE7_SCHEDULE_INITIAL_CHECK_MINUTE = 20
SITE7_FINAL_UPDATE_HOUR = 23
SITE7_MORNING_SCHEDULE_LAST_HOUR = 10
SITE7_MINREPO_FALLBACK_HOUR = 10
SITE7_SCHEDULE_RECHECK_INTERVAL_MINUTES = 10
SITE7_SCHEDULE_RECHECK_LIMIT_MINUTES = 60
GUI_SETTINGS_FILE_NAME = "gui_settings.json"
SITE7_BROWSER_MODE_VISIBLE = "visible"
SITE7_BROWSER_MODE_HIDDEN = "hidden"
DEFAULT_SITE7_SKIP_JUGGLER_DIFFERENCE = False
FETCH_ORDER_REGION_MODE_AS_IS = "現状のまま"
FETCH_ORDER_REGION_MODE_FUKUOKA = "福岡の店舗を優先"
FETCH_ORDER_REGION_MODE_TOKYO = "東京の店舗を優先"
FETCH_ORDER_REGION_MODE_OPTIONS = (
    FETCH_ORDER_REGION_MODE_AS_IS,
    FETCH_ORDER_REGION_MODE_FUKUOKA,
    FETCH_ORDER_REGION_MODE_TOKYO,
)
DEFAULT_FETCH_ORDER_REGION_MODE = FETCH_ORDER_REGION_MODE_FUKUOKA
JST = timezone(timedelta(hours=9))
REGISTERED_STORE_COLUMNS = (
    "頻度",
    "取得元",
    "S差枚",
    "取得順",
    "店舗名",
    "URL",
    "都道府県",
    "地域",
    "SS店舗名",
    "SS ID",
    "SS住所",
)
FETCH_FREQUENCY_HIGH = "高頻度"
FETCH_FREQUENCY_DAILY = "毎日"
FETCH_FREQUENCY_LOW = "低頻度"
FETCH_FREQUENCY_STOP = "停止"
FETCH_FREQUENCY_OPTIONS = (
    FETCH_FREQUENCY_HIGH,
    FETCH_FREQUENCY_DAILY,
    FETCH_FREQUENCY_LOW,
    FETCH_FREQUENCY_STOP,
)
FETCH_SOURCE_MINREPO = "みんレポ"
FETCH_SOURCE_SITE7 = "サイセ"
FETCH_SOURCE_BOTH = "両方"
FETCH_SOURCE_OPTIONS = (
    FETCH_SOURCE_MINREPO,
    FETCH_SOURCE_SITE7,
    FETCH_SOURCE_BOTH,
)
SITE7_MACHINE_SOURCE_GROUPS = (
    FETCH_SOURCE_BOTH,
    FETCH_SOURCE_SITE7,
)
SITE7_MACHINE_SOURCE_GROUP_TITLES = {
    FETCH_SOURCE_BOTH: "取得元が両方の店舗",
    FETCH_SOURCE_SITE7: "取得元がサイセのみの店舗",
}
SITE7_MACHINE_SOURCE_GROUP_HELP = {
    FETCH_SOURCE_BOTH: "登録店舗の取得元が両方の店舗に使います。",
    FETCH_SOURCE_SITE7: "登録店舗の取得元がサイセの店舗に使います。",
}
SITE7_NEO_IM_MACHINE_NAME = "ネオアイムジャグラーEX"
T = TypeVar("T")


@dataclass(frozen=True)
class MinRepoFetchParallelOptions:
    date_workers: int
    machine_workers: int


@dataclass(frozen=True)
class WebPublishOptions:
    mode: str
    interval_days: int = DEFAULT_WEB_PUBLISH_INTERVAL_DAYS


MINREPO_FETCH_PARALLEL_OPTIONS = {
    MINREPO_FETCH_MODE_NORMAL: MinRepoFetchParallelOptions(date_workers=1, machine_workers=1),
    MINREPO_FETCH_MODE_FAST: MinRepoFetchParallelOptions(date_workers=1, machine_workers=4),
    MINREPO_FETCH_MODE_STRONG: MinRepoFetchParallelOptions(date_workers=3, machine_workers=6),
}


def parse_recent_days(value: str) -> int:
    text = value.strip()
    if not re.fullmatch(r"\d+", text):
        raise ScraperError("直近日数は 1 以上の整数で入力してください。")

    recent_days = int(text)
    if recent_days <= 0:
        raise ScraperError("直近日数は 1 以上の整数で入力してください。")

    return recent_days


def build_recent_date_range_input(value: str, today: datetime | None = None) -> str:
    recent_days = parse_recent_days(value)
    today_date = (today or datetime.now(JST)).astimezone(JST).date()
    start_date = today_date - timedelta(days=recent_days - 1)
    return f"{start_date.strftime('%Y-%m-%d')} ～ {today_date.strftime('%Y-%m-%d')}"


def parse_retry_delay_seconds(value: str) -> int:
    text = value.strip()
    if not re.fullmatch(r"\d+", text):
        raise ScraperError("再試行の休止秒数は 0 以上の整数で入力してください。")

    return int(text)


def normalize_web_publish_mode(value: object) -> str:
    if str(value).strip() == WEB_PUBLISH_MODE_STORE:
        return WEB_PUBLISH_MODE_STORE
    return WEB_PUBLISH_MODE_DAYS


def parse_web_publish_interval_days(value: str) -> int:
    text = value.strip()
    if not re.fullmatch(r"\d+", text):
        raise ScraperError("Web反映の日数は 1 以上の整数で入力してください。")

    interval_days = int(text)
    if interval_days <= 0:
        raise ScraperError("Web反映の日数は 1 以上の整数で入力してください。")

    return interval_days


def normalize_web_publish_interval_days(value: object) -> int:
    try:
        return parse_web_publish_interval_days(str(value))
    except ScraperError:
        return DEFAULT_WEB_PUBLISH_INTERVAL_DAYS


def matches_day_tail(date_text: str, day_tail: str) -> bool:
    if day_tail == "全て":
        return True

    match = re.fullmatch(r"\d{4}-\d{2}-(\d{2})", date_text.strip())
    if match is None:
        return False

    return match.group(1).endswith(day_tail)


def normalize_site7_browser_mode(value: object) -> str:
    text = str(value).strip().lower()
    if text == SITE7_BROWSER_MODE_HIDDEN:
        return SITE7_BROWSER_MODE_HIDDEN
    return SITE7_BROWSER_MODE_VISIBLE


def normalize_fetch_order_region_mode(value: object) -> str:
    text = str(value).strip()
    if text in FETCH_ORDER_REGION_MODE_OPTIONS:
        return text
    return DEFAULT_FETCH_ORDER_REGION_MODE


def normalize_schedule_enabled(value: object, default: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default

    text = str(value).strip().lower()
    if text in {"0", "false", "off", "no", "停止"}:
        return False
    if text in {"1", "true", "on", "yes", "有効"}:
        return True
    return default


def normalize_int_tuple(value: object, minimum: int, maximum: int) -> tuple[int, ...]:
    values = value if isinstance(value, (list, tuple, set)) else []
    normalized_values: set[int] = set()
    for raw_value in values:
        try:
            numeric_value = int(raw_value)
        except (TypeError, ValueError):
            continue
        if minimum <= numeric_value <= maximum:
            normalized_values.add(numeric_value)
    return tuple(sorted(normalized_values))


def current_jst_date_text(now: datetime | None = None) -> str:
    return (now or datetime.now(JST)).astimezone(JST).strftime("%Y-%m-%d")


def scheduled_fetch_due_date(
    scheduled_fetch_hour: int | None,
    scheduled_last_run_date: str | None,
    now: datetime | None = None,
) -> str | None:
    if scheduled_fetch_hour is None:
        return None

    current_time = (now or datetime.now(JST)).astimezone(JST)
    today_text = current_time.date().isoformat()
    if current_time.hour != scheduled_fetch_hour or scheduled_last_run_date == today_text:
        return None

    return today_text


def parse_schedule_all_stores_interval_days(value: str) -> int:
    text = value.strip()
    if not re.fullmatch(r"\d+", text):
        raise ScraperError("全店舗取得の日数は 1 以上の整数で入力してください。")

    interval_days = int(text)
    if interval_days <= 0:
        raise ScraperError("全店舗取得の日数は 1 以上の整数で入力してください。")

    return interval_days


def normalize_schedule_all_stores_interval_days(value: object) -> int:
    try:
        return parse_schedule_all_stores_interval_days(str(value))
    except ScraperError:
        return DEFAULT_SCHEDULE_ALL_STORES_INTERVAL_DAYS


def normalize_schedule_store_run_dates(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}

    normalized_dates: dict[str, str] = {}
    for raw_url, raw_date in value.items():
        store_url = normalize_store_url(str(raw_url))
        date_text = str(raw_date).strip()
        if store_url and re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_text):
            normalized_dates[store_url] = date_text
    return normalized_dates


def minrepo_error_is_no_date_pages(error: Exception) -> bool:
    error_text = str(error)
    return (
        "日付ページが見つかりませんでした。" in error_text
        or "取得可能な日付ページが見つかりませんでした。" in error_text
    )


def filter_history_result_dates(
    history_result: MachineHistoryResult,
    target_dates: set[str],
) -> MachineHistoryResult:
    date_pages = [
        date_page
        for date_page in history_result.date_pages
        if date_page.target_date in target_dates
    ]
    datasets = [
        dataset
        for dataset in history_result.datasets
        if dataset.target_date in target_dates
    ]
    skipped_targets = [
        skipped_target
        for skipped_target in history_result.skipped_targets
        if skipped_target[0] in target_dates
    ]
    skipped_dates = [
        skipped_date
        for skipped_date in history_result.skipped_dates
        if skipped_date in target_dates
    ]
    store_day_statuses = [
        status
        for status in history_result.store_day_statuses
        if status.target_date in target_dates
    ]
    start_date = date_pages[0].target_date if date_pages else history_result.start_date
    end_date = date_pages[-1].target_date if date_pages else history_result.end_date
    return MachineHistoryResult(
        store_name=history_result.store_name,
        store_url=history_result.store_url,
        start_date=start_date,
        end_date=end_date,
        date_pages=date_pages,
        datasets=datasets,
        skipped_targets=skipped_targets,
        skipped_dates=skipped_dates,
        store_day_statuses=store_day_statuses,
    )


def scheduled_supplemental_store_limit(store_count: int, interval_days: int) -> int:
    if store_count <= 0:
        return 0
    normalized_interval_days = max(1, interval_days)
    return max(1, (store_count + normalized_interval_days - 1) // normalized_interval_days)


def minrepo_priority_watch_target_date(now: datetime | None = None) -> str:
    current_time = (now or datetime.now(JST)).astimezone(JST)
    return (current_time.date() - timedelta(days=1)).isoformat()


def minrepo_priority_watch_is_active(now: datetime | None = None) -> bool:
    current_time = (now or datetime.now(JST)).astimezone(JST)
    start_time = current_time.replace(
        hour=MINREPO_PRIORITY_WATCH_START_HOUR,
        minute=MINREPO_PRIORITY_WATCH_START_MINUTE,
        second=0,
        microsecond=0,
    )
    end_time = current_time.replace(
        hour=MINREPO_PRIORITY_WATCH_END_HOUR,
        minute=0,
        second=0,
        microsecond=0,
    )
    return start_time <= current_time <= end_time


def normalize_fetch_frequency(value: object, default: str = FETCH_FREQUENCY_DAILY) -> str:
    text = str(value).strip()
    if text in FETCH_FREQUENCY_OPTIONS:
        return text
    return default if default in FETCH_FREQUENCY_OPTIONS else FETCH_FREQUENCY_DAILY


def normalize_fetch_source(value: object, default: str = FETCH_SOURCE_MINREPO) -> str:
    text = str(value).strip()
    if text in FETCH_SOURCE_OPTIONS:
        return text
    return default if default in FETCH_SOURCE_OPTIONS else FETCH_SOURCE_MINREPO


def site7_machine_source_group(fetch_source: object) -> str:
    normalized_source = normalize_fetch_source(fetch_source, FETCH_SOURCE_BOTH)
    if normalized_source == FETCH_SOURCE_SITE7:
        return FETCH_SOURCE_SITE7
    return FETCH_SOURCE_BOTH


def site7_machine_is_juggler(machine_name: object) -> bool:
    machine_text = normalize_text(canonical_machine_name(str(machine_name or ""), site7_only=True))
    return "ジャグラー" in machine_text


def normalize_fetch_order(value: object) -> int | None:
    text = str(value).strip()
    if not text:
        return None
    if not re.fullmatch(r"\d+", text):
        return None
    order = int(text)
    return order if order > 0 else None


def store_uses_minrepo(fetch_source: str) -> bool:
    return fetch_source in {FETCH_SOURCE_MINREPO, FETCH_SOURCE_BOTH}


def store_uses_site7(fetch_source: str) -> bool:
    return fetch_source in {FETCH_SOURCE_SITE7, FETCH_SOURCE_BOTH}


def registered_store_uses_daidata_online(registered_store: "RegisteredStore") -> bool:
    return daidata_store_config_for(registered_store.name, registered_store.url) is not None


def normalize_site7_schedule_hours(value: object) -> tuple[int, ...]:
    if not isinstance(value, (list, tuple, set)):
        return DEFAULT_SITE7_SCHEDULE_HOURS

    hours: set[int] = set()
    for raw_hour in value:
        try:
            hour = int(raw_hour)
        except (TypeError, ValueError):
            continue
        if hour in SITE7_SCHEDULE_HOUR_OPTIONS:
            hours.add(hour)
    return tuple(sorted(hours))


def normalize_site7_schedule_run_dates(value: object) -> dict[int, str]:
    if not isinstance(value, dict):
        return {}

    run_dates: dict[int, str] = {}
    for raw_hour, raw_date in value.items():
        try:
            hour = int(raw_hour)
        except (TypeError, ValueError):
            continue
        date_text = str(raw_date).strip()
        if hour in SITE7_SCHEDULE_HOUR_OPTIONS and re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_text):
            run_dates[hour] = date_text
    return run_dates


def normalize_site7_enabled_machine_names(value: object, available_machine_names: tuple[str, ...]) -> set[str]:
    available_names = tuple(str(machine_name).strip() for machine_name in available_machine_names if str(machine_name).strip())
    if not available_names:
        return set()
    if not isinstance(value, (list, tuple, set)):
        return set(available_names)

    available_by_key = {
        canonical_machine_name(machine_name, site7_only=True).casefold(): machine_name
        for machine_name in available_names
    }
    enabled_names: set[str] = set()
    for raw_machine_name in value:
        machine_name = str(raw_machine_name).strip()
        if not machine_name:
            continue
        machine_key = canonical_machine_name(machine_name, site7_only=True).casefold()
        if machine_key in available_by_key:
            enabled_names.add(available_by_key[machine_key])
    return enabled_names


def site7_schedule_due_hour(
    schedule_hours: tuple[int, ...] | list[int] | set[int],
    last_run_dates_by_hour: dict[int, str],
    now: datetime | None = None,
) -> int | None:
    current_time = (now or datetime.now(JST)).astimezone(JST)
    current_hour = current_time.hour
    if current_hour not in set(schedule_hours):
        return None
    if current_time.minute < SITE7_SCHEDULE_INITIAL_CHECK_MINUTE:
        return None

    today_text = current_time.date().isoformat()
    if last_run_dates_by_hour.get(current_hour) == today_text:
        return None
    return current_hour


def _as_jst_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=JST)
    return value.astimezone(JST)


def site7_update_satisfies_scheduled_hour(
    updated_at: datetime,
    scheduled_hour: int,
    checked_at: datetime | None = None,
) -> bool:
    update_time = _as_jst_datetime(updated_at)
    check_time = _as_jst_datetime(checked_at or datetime.now(JST))
    if update_time > check_time:
        return False

    if 0 <= scheduled_hour <= SITE7_MORNING_SCHEDULE_LAST_HOUR:
        previous_final_threshold = check_time.replace(
            hour=SITE7_FINAL_UPDATE_HOUR,
            minute=0,
            second=0,
            microsecond=0,
        ) - timedelta(days=1)
        return update_time >= previous_final_threshold

    scheduled_threshold = check_time.replace(
        hour=scheduled_hour,
        minute=0,
        second=0,
        microsecond=0,
    )
    return update_time.date() == check_time.date() and update_time >= scheduled_threshold


def site7_business_date_from_updated_at(updated_at: datetime) -> date:
    update_time = _as_jst_datetime(updated_at)
    if update_time.hour < SITE7_DATE_BOUNDARY_HOUR:
        update_time -= timedelta(days=1)
    return update_time.date()


def site7_target_date_texts(
    recent_days: int,
    *,
    latest_date: date,
) -> list[str]:
    normalized_days = max(1, recent_days)
    return [
        (latest_date - timedelta(days=day_index)).isoformat()
        for day_index in range(normalized_days)
    ]


def minrepo_fallback_date_texts_for_site7(
    fetch_source: str,
    recent_days: int,
    *,
    now: datetime | None = None,
    site7_updated_at: datetime | None = None,
) -> list[str]:
    if not store_uses_minrepo(fetch_source):
        return []

    current_time = (now or datetime.now(JST)).astimezone(JST)
    if current_time.hour < SITE7_MINREPO_FALLBACK_HOUR:
        return []

    latest_date = (
        site7_business_date_from_updated_at(site7_updated_at)
        if site7_updated_at is not None
        else current_time.date()
    )
    previous_date = current_time.date() - timedelta(days=1)
    return [
        target_date
        for target_date in site7_target_date_texts(recent_days, latest_date=latest_date)
        if datetime.strptime(target_date, "%Y-%m-%d").date() <= previous_date
    ]


def date_range_input_from_date_texts(date_texts: list[str] | set[str] | tuple[str, ...]) -> str:
    normalized_dates = sorted({date_text for date_text in date_texts if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_text)})
    if not normalized_dates:
        raise ScraperError("対象日付がありません。")
    return f"{normalized_dates[0]} ～ {normalized_dates[-1]}"


def replace_dataset_preserving_site7_metadata(dataset: MachineDataset, **changes: object) -> MachineDataset:
    return copy_site7_dataset_metadata(dataset, replace(dataset, **changes))


def rewrite_history_result_store(
    history_result: MachineHistoryResult,
    store_name: str,
    store_url: str,
) -> MachineHistoryResult:
    rewritten_datasets = [
        replace_dataset_preserving_site7_metadata(
            dataset,
            store_name=store_name,
            store_url=store_url,
        )
        for dataset in history_result.datasets
    ]
    return replace(
        history_result,
        store_name=store_name,
        store_url=store_url,
        datasets=rewritten_datasets,
    )


def filter_site7_history_result_by_saved_targets(
    history_result: MachineHistoryResult,
    saved_targets: set[tuple[str, str]],
) -> MachineHistoryResult:
    if not saved_targets:
        return history_result

    filtered_datasets: list[MachineDataset] = []
    skipped_targets = list(history_result.skipped_targets)
    skipped_dates = list(history_result.skipped_dates)
    skipped_target_dates: set[str] = set()

    for dataset in history_result.datasets:
        target_key = (dataset.target_date, normalize_text(dataset.machine_name))
        if target_key in saved_targets:
            skipped_targets.append((dataset.target_date, dataset.machine_name))
            skipped_target_dates.add(dataset.target_date)
            continue
        filtered_datasets.append(dataset)

    remaining_dates = {dataset.target_date for dataset in filtered_datasets}
    filtered_date_pages = [date_page for date_page in history_result.date_pages if date_page.target_date in remaining_dates]
    for skipped_date in sorted(skipped_target_dates - remaining_dates):
        if skipped_date not in skipped_dates:
            skipped_dates.append(skipped_date)

    return replace(
        history_result,
        date_pages=filtered_date_pages,
        datasets=filtered_datasets,
        skipped_targets=skipped_targets,
        skipped_dates=skipped_dates,
    )


def _find_slot_column_index(columns: list[str]) -> int | None:
    target_name = normalize_text("台番")
    for index, column_name in enumerate(columns):
        if normalize_text(column_name) == target_name:
            return index
    return None


def collect_history_result_slot_keys(history_result: MachineHistoryResult) -> set[tuple[str, str]]:
    slot_keys: set[tuple[str, str]] = set()
    for dataset in history_result.datasets:
        slot_column_index = _find_slot_column_index(dataset.columns)
        if slot_column_index is None:
            continue

        for row in dataset.rows:
            if slot_column_index >= len(row):
                continue
            slot_number = str(row[slot_column_index]).strip()
            if slot_number:
                slot_keys.add((dataset.target_date, slot_number))

    return slot_keys


def site7_history_result_updated_at(history_result: MachineHistoryResult) -> str | None:
    updated_at_values = [
        updated_at
        for dataset in history_result.datasets
        if (updated_at := site7_dataset_updated_at(dataset))
    ]
    if not updated_at_values:
        return None
    return max(updated_at_values)


def filter_site7_history_result_by_saved_slots(
    history_result: MachineHistoryResult,
    protected_slots: set[tuple[str, str]],
) -> MachineHistoryResult:
    if not protected_slots:
        return history_result

    filtered_datasets: list[MachineDataset] = []
    skipped_targets = list(history_result.skipped_targets)
    skipped_dates = list(history_result.skipped_dates)
    skipped_target_dates: set[str] = set()

    for dataset in history_result.datasets:
        slot_column_index = _find_slot_column_index(dataset.columns)
        if slot_column_index is None:
            filtered_datasets.append(dataset)
            continue

        filtered_rows: list[list[str]] = []
        removed_row_count = 0
        for row in dataset.rows:
            if slot_column_index >= len(row):
                filtered_rows.append(row)
                continue

            slot_number = str(row[slot_column_index]).strip()
            if slot_number and (dataset.target_date, slot_number) in protected_slots:
                removed_row_count += 1
                continue
            filtered_rows.append(row)

        if not filtered_rows and removed_row_count > 0:
            skipped_targets.append((dataset.target_date, dataset.machine_name))
            skipped_target_dates.add(dataset.target_date)
            continue

        if removed_row_count > 0:
            filtered_datasets.append(replace_dataset_preserving_site7_metadata(dataset, rows=filtered_rows))
            continue

        filtered_datasets.append(dataset)

    remaining_dates = {dataset.target_date for dataset in filtered_datasets}
    filtered_date_pages = [date_page for date_page in history_result.date_pages if date_page.target_date in remaining_dates]
    for skipped_date in sorted(skipped_target_dates - remaining_dates):
        if skipped_date not in skipped_dates:
            skipped_dates.append(skipped_date)

    return replace(
        history_result,
        date_pages=filtered_date_pages,
        datasets=filtered_datasets,
        skipped_targets=skipped_targets,
        skipped_dates=skipped_dates,
    )


def strip_site7_history_result_source_differences(history_result: MachineHistoryResult) -> MachineHistoryResult:
    stripped_datasets: list[MachineDataset] = []
    for dataset in history_result.datasets:
        try:
            difference_index = dataset.columns.index("差枚")
        except ValueError:
            stripped_datasets.append(dataset)
            continue

        stripped_rows: list[list[str]] = []
        for row in dataset.rows:
            stripped_row = list(row)
            if len(stripped_row) > difference_index:
                stripped_row[difference_index] = "-"
            stripped_rows.append(stripped_row)

        stripped_datasets.append(replace_dataset_preserving_site7_metadata(dataset, rows=stripped_rows))

    return replace(history_result, datasets=stripped_datasets)


@dataclass
class RegisteredStore:
    name: str
    url: str
    fetch_frequency: str = FETCH_FREQUENCY_DAILY
    fetch_source: str = FETCH_SOURCE_MINREPO
    fetch_order: int | None = None
    site7_enabled: bool = False
    site7_difference_enabled: bool = False
    site7_prefecture: str = DEFAULT_SITE7_PREFECTURE_NAME
    site7_area: str = ""
    site7_store_name: str = ""
    site7_hall_id: str = ""
    site7_address: str = ""
    event_day_tails: tuple[int, ...] = ()
    event_month_days: tuple[int, ...] = ()
    event_zoro: bool = False
    event_weekdays: tuple[int, ...] = ()
    event_source_text: str = ""

    def __post_init__(self) -> None:
        self.fetch_frequency = normalize_fetch_frequency(self.fetch_frequency)
        if self.site7_enabled and self.fetch_source == FETCH_SOURCE_MINREPO:
            self.fetch_source = FETCH_SOURCE_BOTH
        self.fetch_source = normalize_fetch_source(self.fetch_source)
        self.fetch_order = normalize_fetch_order(self.fetch_order)
        self.site7_enabled = self.uses_site7()
        self.site7_difference_enabled = bool(self.site7_enabled and self.site7_difference_enabled)

    def resolved_site7_store_name(self) -> str:
        return self.site7_store_name.strip() or self.name.strip()

    def has_event_settings(self) -> bool:
        return bool(self.event_day_tails or self.event_month_days or self.event_zoro or self.event_weekdays)

    def uses_minrepo(self) -> bool:
        return store_uses_minrepo(self.fetch_source)

    def uses_site7(self) -> bool:
        return store_uses_site7(self.fetch_source)

    def to_site7_target_store(self) -> Site7TargetStore:
        return enrich_site7_target_store(
            Site7TargetStore(
                display_name=self.name.strip() or self.resolved_site7_store_name(),
                site7_hall_name=self.resolved_site7_store_name(),
                prefecture_name=self.site7_prefecture.strip() or DEFAULT_SITE7_PREFECTURE_NAME,
                area_name=self.site7_area.strip(),
                hall_id=self.site7_hall_id.strip(),
                hall_address=self.site7_address.strip(),
                hall_name_aliases=(self.name.strip(),) if self.name.strip() else (),
            )
        )


class FetchCancelled(Exception):
    pass


MINREPO_OPERATION_KINDS = {"fetch", "scheduled_fetch", "minrepo_priority_watch"}
SITE7_OPERATION_KINDS = {"site7_fetch", "scheduled_site7_fetch"}
FETCH_OPERATION_KINDS = MINREPO_OPERATION_KINDS | SITE7_OPERATION_KINDS
PROGRESS_KIND_MINREPO = "minrepo"
PROGRESS_KIND_SITE7 = "site7"
FETCH_PROGRESS_STARTED_AT_UNSET = object()


class OperationResultQueue(queue.Queue):
    def __init__(self, operation_id_getter: Callable[[], int | None]) -> None:
        super().__init__()
        self.operation_id_getter = operation_id_getter
        self.last_operation_id: int | None = None

    def put(self, item: object, block: bool = True, timeout: float | None = None) -> None:  # type: ignore[override]
        super().put((self.operation_id_getter(), item), block=block, timeout=timeout)

    def get_nowait(self) -> object:  # type: ignore[override]
        operation_id, item = super().get_nowait()
        self.last_operation_id = operation_id
        return item


@dataclass
class StoreFetchResult:
    history_result: MachineHistoryResult
    save_summary: PersistenceSummary | None
    saved_full_day_summary: SavedFullDayDatesSummary
    pending_save_futures: list[Future[PersistenceSummary]] = field(default_factory=list)


@dataclass
class StoreFetchFailure:
    store: RegisteredStore
    error: Exception


@dataclass
class FetchManyResult:
    results: list[StoreFetchResult]
    failures: list[StoreFetchFailure]
    cancelled: bool = False


@dataclass
class MinRepoPriorityWatchResult:
    registered_stores: list[RegisteredStore]
    target_date: str
    checked_store_count: int
    available_store_count: int
    fetch_many_result: FetchManyResult | None = None


@dataclass
class StoreRefreshResult:
    registered_stores: list[RegisteredStore]
    save_summary: RegisteredStoresPersistenceSummary | None = None


@dataclass
class ScheduledFetchResult:
    refresh_result: StoreRefreshResult
    fetch_many_result: FetchManyResult
    supplemental_store_urls: set[str]
    run_date: str


@dataclass
class ScheduledSite7FetchResult:
    registered_stores: list[RegisteredStore]
    fetch_many_result: FetchManyResult
    store_run_urls: set[str]
    run_date: str
    scheduled_hour: int | None = None
    waiting_store_urls: set[str] = field(default_factory=set)
    waiting_started_at: datetime | None = None


@dataclass
class ScheduledSite7UpdateWaitingResult:
    registered_stores: list[RegisteredStore]
    scheduled_hour: int
    waiting_store_urls: set[str]
    run_date: str
    waiting_started_at: datetime


@dataclass
class Site7ScheduleRecheckRequest:
    scheduled_hour: int
    store_urls: set[str]
    run_date: str
    first_checked_at: datetime
    next_check_at: datetime
    expires_at: datetime


@dataclass
class StoreDeleteResult:
    registered_stores: list[RegisteredStore]
    deleted_store_count: int


class MinRepoApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Halldata Prototype")
        self.root.geometry("1320x900")

        self.scraper = MinRepoScraper()
        self.persistence_service = HistoryPersistenceService()
        self.site7_scraper = Site7Scraper(self.persistence_service.root_dir)
        self.daidata_online_scraper = DaidataOnlineScraper(self.persistence_service.root_dir)
        self._worker_context = threading.local()
        self._next_operation_id = 1
        self.active_operations: dict[int, str] = {}
        self.result_queue: queue.Queue[tuple[str, object]] = OperationResultQueue(
            lambda: getattr(self._worker_context, "operation_id", None)
        )
        self.result_polling_active = False
        self.persistence_lock = threading.Lock()
        self.current_results: list[MachineDataset] = []
        self.current_history_result: MachineHistoryResult | None = None
        self.startup_store_warning: str | None = None
        self.registered_stores: list[RegisteredStore] = self._load_registered_stores_on_startup()
        self.selected_store_urls: set[str] = self._load_saved_selected_store_urls(self.registered_stores)
        self.registered_store_sort_column: str | None = None
        self.registered_store_sort_descending = False
        self.is_busy = False
        self.active_operation_kind = ""
        self.minrepo_cancel_event = threading.Event()
        self.site7_cancel_event = threading.Event()
        self.fetch_cancel_event = self.minrepo_cancel_event
        self.minrepo_schedule_enabled = self._load_saved_minrepo_schedule_enabled()
        self.scheduled_fetch_hour: int | None = self._load_saved_schedule_hour()
        self.schedule_all_stores_interval_days = self._load_saved_schedule_all_stores_interval_days()
        self.schedule_supplemental_store_last_run_dates = self._load_saved_schedule_supplemental_store_last_run_dates()
        self.web_publish_mode = self._load_saved_web_publish_mode()
        self.web_publish_interval_days = self._load_saved_web_publish_interval_days()
        self.fetch_order_region_mode = self._load_saved_fetch_order_region_mode()
        self.site7_browser_mode: str = self._load_saved_site7_browser_mode()
        self.site7_skip_juggler_difference = self._load_saved_site7_skip_juggler_difference()
        self.site7_target_machine_names = tuple(list_site7_target_machine_names())
        self.site7_enabled_machine_names_by_source = self._load_saved_site7_enabled_machine_names_by_source()
        self.site7_schedule_enabled = self._load_saved_site7_schedule_enabled()
        self.site7_schedule_hours = self._load_saved_site7_schedule_hours()
        self.site7_schedule_last_run_dates_by_hour = self._load_saved_site7_schedule_run_dates()
        self.site7_schedule_store_last_run_dates = self._load_saved_site7_schedule_store_last_run_dates()
        self.scheduled_last_run_date: str | None = None
        self.scheduled_pending_date: str | None = None
        self.scheduled_startup_prompt_date: str | None = (
            scheduled_fetch_due_date(
                self.scheduled_fetch_hour,
                self.scheduled_last_run_date,
            )
            if self.minrepo_schedule_enabled
            else None
        )
        self.site7_schedule_startup_prompt_hour: int | None = (
            site7_schedule_due_hour(
                self.site7_schedule_hours,
                self.site7_schedule_last_run_dates_by_hour,
            )
            if self.site7_schedule_enabled
            else None
        )
        self.site7_schedule_pending_hours: set[int] = set()
        self.site7_schedule_recheck_requests: dict[int, Site7ScheduleRecheckRequest] = {}
        self.minrepo_priority_watch_next_check_at: datetime | None = None
        self.minrepo_priority_watch_pending = False
        self.minrepo_priority_watch_target_date: str | None = None
        self.minrepo_priority_watch_completed_store_dates: set[tuple[str, str]] = set()
        self.tray_icon: object | None = None
        self.tray_thread: threading.Thread | None = None

        self.target_date_var = tk.StringVar(value=DEFAULT_RECENT_DAYS)
        self.minrepo_schedule_enabled_var = tk.BooleanVar(value=self.minrepo_schedule_enabled)
        self.schedule_hour_var = tk.StringVar(value=str(self.scheduled_fetch_hour))
        self.schedule_status_var = tk.StringVar(value=self._schedule_status_text())
        if self.scheduled_startup_prompt_date is not None:
            self.schedule_status_var.set(f"本日 {self.scheduled_fetch_hour} 時の定期実行を確認待ち")
        self.schedule_all_stores_interval_days_var = tk.StringVar(value=str(self.schedule_all_stores_interval_days))
        self.schedule_all_stores_status_var = tk.StringVar(value=self._schedule_all_stores_status_text())
        self.retry_delay_seconds_var = tk.StringVar(value=DEFAULT_RETRY_DELAY_SECONDS)
        self.minrepo_fetch_mode_var = tk.StringVar(value=DEFAULT_MINREPO_FETCH_MODE)
        self.web_publish_mode_var = tk.StringVar(value=self.web_publish_mode)
        self.web_publish_interval_days_var = tk.StringVar(value=str(self.web_publish_interval_days))
        self.fetch_order_region_mode_var = tk.StringVar(value=self.fetch_order_region_mode)
        self.status_var = tk.StringVar(value="待機中")
        self.summary_var = tk.StringVar(value="未取得")
        self.fetch_progress_value_var = tk.DoubleVar(value=0.0)
        self.fetch_progress_text_var = tk.StringVar(value="未開始")
        self.site7_fetch_progress_value_var = tk.DoubleVar(value=0.0)
        self.site7_fetch_progress_text_var = tk.StringVar(value="未開始")
        self.notify_fetch_complete_var = tk.BooleanVar(value=True)
        self.register_store_url_var = tk.StringVar()
        self.register_store_frequency_var = tk.StringVar(value=FETCH_FREQUENCY_DAILY)
        self.register_store_source_var = tk.StringVar(value=FETCH_SOURCE_MINREPO)
        self.register_store_order_var = tk.StringVar()
        self.register_store_site7_enabled_var = tk.BooleanVar(value=False)
        self.register_store_site7_difference_enabled_var = tk.BooleanVar(value=False)
        self.register_store_prefecture_var = tk.StringVar(value=DEFAULT_SITE7_PREFECTURE_NAME)
        self.register_store_area_var = tk.StringVar()
        self.register_store_site7_store_name_var = tk.StringVar()
        self.register_store_site7_hall_id_var = tk.StringVar()
        self.register_store_site7_address_var = tk.StringVar()
        self.register_store_status_var = tk.StringVar(value="未登録")
        self.registered_store_filter_var = tk.StringVar()
        self.registered_store_filter_status_var = tk.StringVar()
        self.site7_browser_mode_var = tk.StringVar(value=self.site7_browser_mode)
        self.site7_skip_juggler_difference_var = tk.BooleanVar(value=self.site7_skip_juggler_difference)
        self.site7_machine_enabled_vars_by_source = {
            source_group: {
                machine_name: tk.BooleanVar(
                    value=machine_name in self.site7_enabled_machine_names_by_source.get(source_group, set())
                )
                for machine_name in self.site7_target_machine_names
            }
            for source_group in SITE7_MACHINE_SOURCE_GROUPS
        }
        self.site7_machine_settings_status_var = tk.StringVar(value=self._site7_machine_settings_status_text())
        self.site7_status_var = tk.StringVar(
            value="保存済みのログイン情報あり" if self.site7_scraper.has_saved_login_state() else "初回ログインが必要"
        )
        self.site7_schedule_enabled_var = tk.BooleanVar(value=self.site7_schedule_enabled)
        self.site7_schedule_hour_vars = {
            hour: tk.BooleanVar(value=hour in self.site7_schedule_hours)
            for hour in SITE7_SCHEDULE_HOUR_OPTIONS
        }
        self.site7_schedule_status_var = tk.StringVar(value=self._site7_schedule_status_text())
        if self.site7_schedule_startup_prompt_hour is not None:
            self.site7_schedule_status_var.set(
                f"本日 {self.site7_schedule_startup_prompt_hour} 時のサイトセブン定期実行を確認待ち"
            )
        self.fetch_progress_current = 0
        self.fetch_progress_total = 0
        self.fetch_progress_started_at: float | None = None
        self.fetch_progress_last_message = "未開始"
        self.site7_fetch_progress_current = 0
        self.site7_fetch_progress_total = 0
        self.site7_fetch_progress_started_at: float | None = None
        self.site7_fetch_progress_last_message = "未開始"
        self._last_queued_fetch_progress_by_operation: dict[object, tuple[float, int, int, str]] = {}
        self._fetch_progress_bar_modes: dict[str, str] = {}

        self._build_ui()
        self._reset_fetch_progress()
        self._update_button_states()
        self._refresh_registered_store_table()
        self.root.protocol("WM_DELETE_WINDOW", self._on_window_close)
        self._schedule_timer_tick()
        if self.startup_store_warning:
            self.root.after(100, lambda: messagebox.showwarning("登録店舗", self.startup_store_warning))
        self.root.after(250, self._prompt_scheduled_fetch_on_startup_if_needed)
        self.root.after(350, self._prompt_scheduled_site7_fetch_on_startup_if_needed)
        self.root.after(500, self._prompt_site7_login_on_startup_if_needed)

    def _build_ui(self) -> None:
        container = ttk.Frame(self.root, padding=16)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)

        self.notebook = ttk.Notebook(container)
        self.notebook.grid(row=0, column=0, sticky="nsew")

        self.fetch_tab = ttk.Frame(self.notebook, padding=12)
        self.fetch_tab.columnconfigure(0, weight=1)
        self.fetch_tab.rowconfigure(2, weight=0)
        self.notebook.add(self.fetch_tab, text="データ取得")

        self.register_tab = ttk.Frame(self.notebook, padding=12)
        self.register_tab.columnconfigure(0, weight=1)
        self.register_tab.rowconfigure(1, weight=1)
        self.notebook.add(self.register_tab, text="登録店舗")

        self.site7_machine_tab = ttk.Frame(self.notebook, padding=12)
        self.site7_machine_tab.columnconfigure(0, weight=1)
        self.site7_machine_tab.rowconfigure(2, weight=1)
        self.notebook.add(self.site7_machine_tab, text="サイトセブン取得機種")
        self._register_tab_built = False
        self._site7_machine_settings_tab_built = False
        self.notebook.bind("<<NotebookTabChanged>>", self._on_notebook_tab_changed)

        self.fetch_form = ttk.LabelFrame(self.fetch_tab, text="取得条件", padding=12)
        self.fetch_form.grid(row=0, column=0, sticky="ew")
        self.fetch_form.columnconfigure(1, weight=1)

        ttk.Label(self.fetch_form, text="直近日数").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        self.target_date_entry = ttk.Entry(self.fetch_form, textvariable=self.target_date_var, width=8)
        self.target_date_entry.grid(row=0, column=1, sticky="w", pady=4)
        ttk.Label(self.fetch_form, text="日（日本時間の今日まで）").grid(row=0, column=1, sticky="w", padx=(72, 0), pady=4)

        ttk.Label(self.fetch_form, text="再試行休止").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        self.retry_delay_entry = ttk.Entry(self.fetch_form, textvariable=self.retry_delay_seconds_var, width=8)
        self.retry_delay_entry.grid(row=1, column=1, sticky="w", pady=4)
        ttk.Label(self.fetch_form, text="秒（取得失敗時は最大3回まで再試行）").grid(
            row=1,
            column=1,
            sticky="w",
            padx=(72, 0),
            pady=4,
        )

        ttk.Label(self.fetch_form, text="みんレポ取得モード").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=4)
        self.minrepo_fetch_mode_selector = ttk.Combobox(
            self.fetch_form,
            textvariable=self.minrepo_fetch_mode_var,
            values=MINREPO_FETCH_MODE_OPTIONS,
            state="readonly",
            width=8,
        )
        self.minrepo_fetch_mode_selector.grid(row=2, column=1, sticky="w", pady=4)
        ttk.Label(self.fetch_form, text="通常 / 高速 / 強並列").grid(row=2, column=1, sticky="w", padx=(84, 0), pady=4)

        ttk.Label(self.fetch_form, text="店舗取得順").grid(row=3, column=0, sticky="w", padx=(0, 8), pady=4)
        self.fetch_order_region_selector = ttk.Combobox(
            self.fetch_form,
            textvariable=self.fetch_order_region_mode_var,
            values=FETCH_ORDER_REGION_MODE_OPTIONS,
            state="readonly",
            width=18,
        )
        self.fetch_order_region_selector.grid(row=3, column=1, sticky="w", pady=4)
        self.fetch_order_region_selector.bind("<<ComboboxSelected>>", self._on_fetch_order_region_mode_changed)
        ttk.Label(self.fetch_form, text="取得順の数字が最優先").grid(
            row=3,
            column=1,
            sticky="w",
            padx=(164, 0),
            pady=4,
        )

        ttk.Label(self.fetch_form, text="Web反映").grid(row=4, column=0, sticky="w", padx=(0, 8), pady=4)
        web_publish_row = ttk.Frame(self.fetch_form)
        web_publish_row.grid(row=4, column=1, sticky="w", pady=4)
        self.web_publish_days_radio = ttk.Radiobutton(
            web_publish_row,
            text="日数ごと",
            value=WEB_PUBLISH_MODE_DAYS,
            variable=self.web_publish_mode_var,
            command=self._on_web_publish_mode_changed,
        )
        self.web_publish_days_radio.grid(row=0, column=0, sticky="w")
        self.web_publish_interval_days_entry = ttk.Entry(
            web_publish_row,
            textvariable=self.web_publish_interval_days_var,
            width=4,
        )
        self.web_publish_interval_days_entry.grid(row=0, column=1, sticky="w", padx=(8, 4))
        ttk.Label(web_publish_row, text="日ごと").grid(row=0, column=2, sticky="w")
        self.web_publish_store_radio = ttk.Radiobutton(
            web_publish_row,
            text="店舗ごと",
            value=WEB_PUBLISH_MODE_STORE,
            variable=self.web_publish_mode_var,
            command=self._on_web_publish_mode_changed,
        )
        self.web_publish_store_radio.grid(row=0, column=3, sticky="w", padx=(16, 0))

        button_row = ttk.Frame(self.fetch_form)
        button_row.grid(row=5, column=1, sticky="w", pady=(8, 0))

        self.fetch_button = ttk.Button(button_row, text="取得", command=self.fetch_data)
        self.fetch_button.grid(row=0, column=0, sticky="w")

        self.cancel_fetch_button = ttk.Button(button_row, text="中止", command=self.cancel_fetch)
        self.cancel_fetch_button.grid(row=0, column=1, sticky="w", padx=(8, 0))

        self.notify_fetch_complete_button = ttk.Checkbutton(
            button_row,
            text="取得完了時に音を鳴らす",
            variable=self.notify_fetch_complete_var,
        )
        self.notify_fetch_complete_button.grid(row=0, column=2, sticky="w", padx=(12, 0))

        schedule_row = ttk.Frame(self.fetch_form)
        schedule_row.grid(row=6, column=1, sticky="w", pady=(8, 0))
        self.minrepo_schedule_enabled_checkbutton = ttk.Checkbutton(
            schedule_row,
            text="みんレポ定期ON",
            variable=self.minrepo_schedule_enabled_var,
            command=self._on_minrepo_schedule_enabled_changed,
        )
        self.minrepo_schedule_enabled_checkbutton.grid(row=0, column=0, sticky="w")
        ttk.Label(schedule_row, text="毎日").grid(row=0, column=1, sticky="w", padx=(12, 0))
        self.schedule_hour_entry = ttk.Entry(schedule_row, textvariable=self.schedule_hour_var, width=4)
        self.schedule_hour_entry.grid(row=0, column=2, sticky="w", padx=(6, 4))
        ttk.Label(schedule_row, text="時に実行").grid(row=0, column=3, sticky="w")
        self.apply_schedule_button = ttk.Button(schedule_row, text="設定", command=self.apply_daily_schedule)
        self.apply_schedule_button.grid(row=0, column=4, sticky="w", padx=(8, 0))
        self.clear_schedule_button = ttk.Button(schedule_row, text="解除", command=self.clear_daily_schedule)
        self.clear_schedule_button.grid(row=0, column=5, sticky="w", padx=(8, 0))
        ttk.Label(schedule_row, textvariable=self.schedule_status_var).grid(row=0, column=6, sticky="w", padx=(12, 0))

        all_store_schedule_row = ttk.Frame(self.fetch_form)
        all_store_schedule_row.grid(row=7, column=1, sticky="w", pady=(8, 0))
        ttk.Label(all_store_schedule_row, text="低頻度").grid(row=0, column=0, sticky="w")
        self.schedule_all_stores_interval_days_entry = ttk.Entry(
            all_store_schedule_row,
            textvariable=self.schedule_all_stores_interval_days_var,
            width=4,
        )
        self.schedule_all_stores_interval_days_entry.grid(row=0, column=1, sticky="w", padx=(8, 4))
        ttk.Label(all_store_schedule_row, text="日周期で分散").grid(row=0, column=2, sticky="w")
        self.apply_schedule_all_stores_button = ttk.Button(
            all_store_schedule_row,
            text="設定",
            command=self.apply_schedule_all_stores_interval,
        )
        self.apply_schedule_all_stores_button.grid(row=0, column=3, sticky="w", padx=(8, 0))
        ttk.Label(all_store_schedule_row, textvariable=self.schedule_all_stores_status_var).grid(
            row=0,
            column=4,
            sticky="w",
            padx=(12, 0),
        )

        site7_row = ttk.LabelFrame(self.fetch_form, text="サイトセブン", padding=12)
        site7_row.grid(row=8, column=0, columnspan=2, sticky="ew", pady=(12, 0))
        site7_row.columnconfigure(0, weight=1)

        self.site7_login_button = ttk.Button(
            site7_row,
            text="サイトセブンにログイン",
            command=self.site7_login,
        )
        self.site7_login_button.grid(row=0, column=0, sticky="w")

        self.site7_fetch_button = ttk.Button(
            site7_row,
            text="サイトセブン取得",
            command=self.fetch_site7_data,
        )
        self.site7_fetch_button.grid(row=0, column=1, sticky="w", padx=(8, 0))

        self.site7_neo_im_fetch_button = ttk.Button(
            site7_row,
            text="ネオアイムのみ取得",
            command=self.fetch_site7_neo_im_data,
        )
        self.site7_neo_im_fetch_button.grid(row=0, column=2, sticky="w", padx=(8, 0))

        self.site7_cancel_button = ttk.Button(site7_row, text="中止", command=self.cancel_site7_fetch)
        self.site7_cancel_button.grid(row=0, column=3, sticky="w", padx=(8, 0))

        ttk.Label(site7_row, textvariable=self.site7_status_var).grid(row=0, column=4, sticky="w", padx=(12, 0))

        site7_schedule_row = ttk.Frame(site7_row)
        site7_schedule_row.grid(row=1, column=0, columnspan=5, sticky="w", pady=(8, 0))
        self.site7_schedule_enabled_checkbutton = ttk.Checkbutton(
            site7_schedule_row,
            text="定期取得ON",
            variable=self.site7_schedule_enabled_var,
            command=self._on_site7_schedule_enabled_changed,
        )
        self.site7_schedule_enabled_checkbutton.grid(row=0, column=0, sticky="w")
        ttk.Label(site7_schedule_row, text="実行時刻").grid(row=0, column=1, sticky="w", padx=(8, 4))
        self.site7_schedule_hour_buttons: dict[int, ttk.Checkbutton] = {}
        for index, hour in enumerate(SITE7_SCHEDULE_HOUR_OPTIONS):
            row_index = index // 7
            column_index = 2 + (index % 7)
            hour_button = ttk.Checkbutton(
                site7_schedule_row,
                text=f"{hour}時",
                variable=self.site7_schedule_hour_vars[hour],
            )
            hour_button.grid(row=row_index, column=column_index, sticky="w", padx=(4, 0))
            self.site7_schedule_hour_buttons[hour] = hour_button
        self.apply_site7_schedule_button = ttk.Button(
            site7_schedule_row,
            text="設定",
            command=self.apply_site7_schedule,
        )
        self.apply_site7_schedule_button.grid(row=0, column=9, sticky="w", padx=(8, 0))
        self.clear_site7_schedule_button = ttk.Button(
            site7_schedule_row,
            text="全解除",
            command=self.clear_site7_schedule,
        )
        self.clear_site7_schedule_button.grid(row=0, column=10, sticky="w", padx=(8, 0))
        ttk.Label(site7_schedule_row, textvariable=self.site7_schedule_status_var).grid(
            row=1,
            column=9,
            columnspan=2,
            sticky="w",
            padx=(12, 0),
        )

        mode_row = ttk.Frame(site7_row)
        mode_row.grid(row=2, column=0, columnspan=5, sticky="w", pady=(8, 0))
        ttk.Label(mode_row, text="取得時のブラウザ").grid(row=0, column=0, sticky="w")
        self.site7_browser_visible_radio = ttk.Radiobutton(
            mode_row,
            text="表示",
            value=SITE7_BROWSER_MODE_VISIBLE,
            variable=self.site7_browser_mode_var,
            command=self._on_site7_browser_mode_changed,
        )
        self.site7_browser_visible_radio.grid(row=0, column=1, sticky="w", padx=(12, 0))
        self.site7_browser_hidden_radio = ttk.Radiobutton(
            mode_row,
            text="非表示",
            value=SITE7_BROWSER_MODE_HIDDEN,
            variable=self.site7_browser_mode_var,
            command=self._on_site7_browser_mode_changed,
        )
        self.site7_browser_hidden_radio.grid(row=0, column=2, sticky="w", padx=(8, 0))
        ttk.Label(mode_row, text="初期値は表示").grid(row=0, column=3, sticky="w", padx=(12, 0))
        self.site7_skip_juggler_difference_checkbutton = ttk.Checkbutton(
            mode_row,
            text="ジャグ系機種は差枚を取得しない",
            variable=self.site7_skip_juggler_difference_var,
            command=self._on_site7_skip_juggler_difference_changed,
        )
        self.site7_skip_juggler_difference_checkbutton.grid(row=0, column=4, sticky="w", padx=(16, 0))

        self.fetch_info = ttk.Frame(self.fetch_tab, padding=(0, 12, 0, 12))
        self.fetch_info.grid(row=1, column=0, sticky="ew")
        self.fetch_info.columnconfigure(1, weight=1)
        self.fetch_info.columnconfigure(3, weight=1)

        ttk.Label(self.fetch_info, text="状態").grid(row=0, column=0, sticky="w")
        ttk.Label(self.fetch_info, textvariable=self.status_var).grid(row=0, column=1, sticky="w", padx=(8, 24))
        ttk.Label(self.fetch_info, text="概要").grid(row=0, column=2, sticky="w")
        ttk.Label(self.fetch_info, textvariable=self.summary_var).grid(row=0, column=3, sticky="w", padx=(8, 0))

        ttk.Label(self.fetch_info, text="みんレポ進捗").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.fetch_progress_bar = ttk.Progressbar(
            self.fetch_info,
            variable=self.fetch_progress_value_var,
            maximum=100,
            mode="determinate",
        )
        self.fetch_progress_bar.grid(row=1, column=1, columnspan=2, sticky="ew", padx=(8, 12), pady=(8, 0))
        ttk.Label(self.fetch_info, textvariable=self.fetch_progress_text_var).grid(row=1, column=3, sticky="w", pady=(8, 0))

        ttk.Label(self.fetch_info, text="サイセ進捗").grid(row=2, column=0, sticky="w", pady=(8, 0))
        self.site7_fetch_progress_bar = ttk.Progressbar(
            self.fetch_info,
            variable=self.site7_fetch_progress_value_var,
            maximum=100,
            mode="determinate",
        )
        self.site7_fetch_progress_bar.grid(row=2, column=1, columnspan=2, sticky="ew", padx=(8, 12), pady=(8, 0))
        ttk.Label(self.fetch_info, textvariable=self.site7_fetch_progress_text_var).grid(
            row=2,
            column=3,
            sticky="w",
            pady=(8, 0),
        )

    def _on_notebook_tab_changed(self, _: tk.Event[tk.Misc] | None = None) -> None:
        selected_tab = self.notebook.select()
        if selected_tab == str(self.register_tab):
            self._ensure_register_tab_built()
            return
        if selected_tab == str(self.site7_machine_tab):
            self._ensure_site7_machine_settings_tab_built()

    def _ensure_register_tab_built(self) -> None:
        if getattr(self, "_register_tab_built", False):
            return
        self._build_register_tab(self.register_tab)
        self._register_tab_built = True
        self._refresh_registered_store_table()
        self._update_button_states()

    def _ensure_site7_machine_settings_tab_built(self) -> None:
        if getattr(self, "_site7_machine_settings_tab_built", False):
            return
        self._build_site7_machine_settings_tab(self.site7_machine_tab)
        self._site7_machine_settings_tab_built = True
        self._update_button_states()

    def _prompt_site7_login_on_startup_if_needed(self) -> None:
        if self.site7_scraper.has_saved_login_state():
            self.site7_status_var.set("保存済みのログイン情報あり")
            return

        if self._has_active_operations():
            self.root.after(30_000, self._prompt_site7_login_on_startup_if_needed)
            return

        self.site7_status_var.set("初回ログインが必要")
        if not messagebox.askyesno(
            "サイトセブン",
            "サイトセブンは初回ログインが必要です。\n"
            "いまブラウザを開いてログインしますか？\n"
            "ログイン完了後の画面が見えたら、数秒待つと自動で反映します。",
        ):
            return

        self.site7_login()

    def _prompt_scheduled_fetch_on_startup_if_needed(self) -> None:
        if not self.minrepo_schedule_enabled:
            self.scheduled_startup_prompt_date = None
            return

        prompt_date = self.scheduled_startup_prompt_date
        if prompt_date is None:
            return

        if self._minrepo_start_blocked():
            self.root.after(1_000, self._prompt_scheduled_fetch_on_startup_if_needed)
            return

        due_date = scheduled_fetch_due_date(
            self.scheduled_fetch_hour,
            self.scheduled_last_run_date,
        )
        if due_date != prompt_date:
            self.scheduled_startup_prompt_date = None
            return

        scheduled_hour = self.scheduled_fetch_hour
        self.scheduled_startup_prompt_date = None
        should_start = messagebox.askyesno(
            "定期実行",
            f"いまは {scheduled_hour} 時台です。\n"
            "起動直後のため、自動取得をすぐには始めません。\n"
            "本日の定期実行をいま開始しますか？",
        )
        if not should_start:
            self.scheduled_last_run_date = prompt_date
            self.scheduled_pending_date = None
            self.schedule_status_var.set(f"本日 {scheduled_hour} 時の定期実行は見送りました")
            return

        self.scheduled_last_run_date = prompt_date
        self._start_scheduled_fetch()

    def _prompt_scheduled_site7_fetch_on_startup_if_needed(self) -> None:
        if not self.site7_schedule_enabled:
            self.site7_schedule_startup_prompt_hour = None
            return

        prompt_hour = self.site7_schedule_startup_prompt_hour
        if prompt_hour is None:
            return

        if self._site7_start_blocked():
            self.root.after(1_000, self._prompt_scheduled_site7_fetch_on_startup_if_needed)
            return

        due_hour = site7_schedule_due_hour(
            self.site7_schedule_hours,
            self.site7_schedule_last_run_dates_by_hour,
        )
        if due_hour != prompt_hour:
            self.site7_schedule_startup_prompt_hour = None
            return

        self.site7_schedule_startup_prompt_hour = None
        should_start = messagebox.askyesno(
            "サイトセブン定期実行",
            f"いまは {prompt_hour} 時台です。\n"
            "起動直後のため、サイトセブン取得をすぐには始めません。\n"
            "本日のサイトセブン定期実行をいま開始しますか？",
        )
        if not should_start:
            self.site7_schedule_pending_hours.discard(prompt_hour)
            self.site7_schedule_last_run_dates_by_hour[prompt_hour] = current_jst_date_text()
            try:
                self._save_site7_schedule_run_dates()
            except Exception as exc:  # noqa: BLE001
                self.site7_schedule_status_var.set(f"サイトセブン定期実行の記録保存に失敗しました: {exc}")
                return
            self.site7_schedule_status_var.set(f"本日 {prompt_hour} 時のサイトセブン定期実行は見送りました")
            return

        self._start_scheduled_site7_fetch(prompt_hour)

    def site7_login(self) -> None:
        if self._has_active_operations():
            return

        messagebox.showinfo(
            "サイトセブン",
            "これからサイトセブンのログイン画面を開きます。\n"
            "ブラウザでログインしたあと、ログイン後の画面が見えるまで進めてください。\n"
            "画面が切り替わったら、数秒待つと自動で反映します。",
        )
        self.status_var.set("サイトセブンのログイン確認中")
        self.site7_status_var.set("ログイン確認中")
        self._start_worker(self._worker_site7_login, operation_kind="site7_login")

    def _worker_site7_login(self) -> None:
        try:
            self.site7_scraper.login_interactively()
            self.result_queue.put(("site7_login_success", None))
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("site7_login_error", exc))

    def _on_site7_browser_mode_changed(self) -> None:
        self.site7_browser_mode = normalize_site7_browser_mode(self.site7_browser_mode_var.get())
        self.site7_browser_mode_var.set(self.site7_browser_mode)
        try:
            self._save_site7_browser_mode(self.site7_browser_mode)
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("設定保存", f"サイトセブンの表示設定保存に失敗しました。\n{exc}")

    def _site7_browser_visible(self) -> bool:
        browser_mode = normalize_site7_browser_mode(self.site7_browser_mode_var.get())
        self.site7_browser_mode = browser_mode
        return browser_mode == SITE7_BROWSER_MODE_VISIBLE

    def _on_site7_skip_juggler_difference_changed(self) -> None:
        self.site7_skip_juggler_difference = self._site7_skip_juggler_difference_enabled()
        try:
            self._save_site7_skip_juggler_difference(self.site7_skip_juggler_difference)
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("設定保存", f"サイトセブン差枚設定の保存に失敗しました。\n{exc}")

    def _site7_skip_juggler_difference_enabled(self) -> bool:
        if hasattr(self, "site7_skip_juggler_difference_var"):
            self.site7_skip_juggler_difference = bool(self.site7_skip_juggler_difference_var.get())
        return bool(getattr(self, "site7_skip_juggler_difference", DEFAULT_SITE7_SKIP_JUGGLER_DIFFERENCE))

    def _schedule_status_text(self) -> str:
        if self.scheduled_fetch_hour is None:
            base_text = "定期実行なし"
        else:
            base_text = f"毎日 {self.scheduled_fetch_hour} 時に実行"
        if not self.minrepo_schedule_enabled:
            return f"みんレポ定期OFF（{base_text}）"
        return base_text

    def _site7_schedule_status_text(self) -> str:
        if not self.site7_schedule_enabled:
            if not self.site7_schedule_hours:
                return "サイトセブン定期OFF（実行時刻なし）"
            hours_text = "、".join(f"{hour}時" for hour in self.site7_schedule_hours)
            return f"サイトセブン定期OFF（{hours_text} 設定）"
        if not self.site7_schedule_hours:
            return "サイトセブン定期実行なし"
        hours_text = "、".join(f"{hour}時" for hour in self.site7_schedule_hours)
        return f"毎日 {hours_text} に実行"

    def _on_minrepo_schedule_enabled_changed(self) -> None:
        self.minrepo_schedule_enabled = bool(self.minrepo_schedule_enabled_var.get())
        if not self.minrepo_schedule_enabled:
            self.scheduled_pending_date = None
            self.scheduled_startup_prompt_date = None
            self.minrepo_priority_watch_pending = False
            self.minrepo_priority_watch_next_check_at = None
        try:
            self._save_minrepo_schedule_enabled(self.minrepo_schedule_enabled)
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("設定保存", f"みんレポ定期実行のON/OFF保存に失敗しました。\n{exc}")
        self.schedule_status_var.set(self._schedule_status_text())

    def _on_site7_schedule_enabled_changed(self) -> None:
        self.site7_schedule_enabled = bool(self.site7_schedule_enabled_var.get())
        if not self.site7_schedule_enabled:
            self.site7_schedule_startup_prompt_hour = None
            self.site7_schedule_pending_hours = set()
            self.site7_schedule_recheck_requests = {}
        try:
            self._save_site7_schedule_enabled(self.site7_schedule_enabled)
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("設定保存", f"サイトセブン定期実行のON/OFF保存に失敗しました。\n{exc}")
        self.site7_schedule_status_var.set(self._site7_schedule_status_text())

    def apply_site7_schedule(self) -> None:
        self.site7_schedule_hours = tuple(
            hour
            for hour in SITE7_SCHEDULE_HOUR_OPTIONS
            if self.site7_schedule_hour_vars[hour].get()
        )
        self.site7_schedule_pending_hours = {
            hour for hour in self.site7_schedule_pending_hours if hour in self.site7_schedule_hours
        }
        self.site7_schedule_recheck_requests = {
            hour: request
            for hour, request in getattr(self, "site7_schedule_recheck_requests", {}).items()
            if hour in self.site7_schedule_hours
        }
        try:
            self._save_site7_schedule_hours(self.site7_schedule_hours)
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("設定保存", f"サイトセブン定期実行の設定保存に失敗しました。\n{exc}")
        self.site7_schedule_status_var.set(self._site7_schedule_status_text())

    def clear_site7_schedule(self) -> None:
        self.site7_schedule_hours = ()
        self.site7_schedule_pending_hours = set()
        self.site7_schedule_recheck_requests = {}
        for hour_var in self.site7_schedule_hour_vars.values():
            hour_var.set(False)
        try:
            self._save_site7_schedule_hours(self.site7_schedule_hours)
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("設定保存", f"サイトセブン定期実行の設定保存に失敗しました。\n{exc}")
        self.site7_schedule_status_var.set(self._site7_schedule_status_text())

    def apply_daily_schedule(self) -> None:
        try:
            scheduled_hour = self._parse_schedule_hour()
        except ScraperError as exc:
            messagebox.showwarning("入力不正", str(exc))
            return

        self.scheduled_fetch_hour = scheduled_hour
        self.scheduled_last_run_date = None
        self.scheduled_pending_date = None
        self.scheduled_startup_prompt_date = None
        try:
            self._save_schedule_hour(scheduled_hour)
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("設定保存", f"定期実行の時刻保存に失敗しました。\n{exc}")
        self.schedule_status_var.set(self._schedule_status_text())

    def apply_schedule_all_stores_interval(self) -> None:
        try:
            interval_days = parse_schedule_all_stores_interval_days(
                self.schedule_all_stores_interval_days_var.get()
            )
        except ScraperError as exc:
            messagebox.showwarning("入力不正", str(exc))
            return

        self.schedule_all_stores_interval_days = interval_days
        self.schedule_all_stores_interval_days_var.set(str(interval_days))
        try:
            self._save_schedule_all_stores_interval_days(interval_days)
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("設定保存", f"低頻度取得の設定保存に失敗しました。\n{exc}")
        self.schedule_all_stores_status_var.set(self._schedule_all_stores_status_text())

    def clear_daily_schedule(self) -> None:
        self.scheduled_fetch_hour = None
        self.scheduled_last_run_date = None
        self.scheduled_pending_date = None
        self.scheduled_startup_prompt_date = None
        self.schedule_status_var.set(self._schedule_status_text())

    def _parse_schedule_hour(self) -> int:
        text = self.schedule_hour_var.get().strip()
        if not re.fullmatch(r"\d{1,2}", text):
            raise ScraperError("定期実行の時刻は 0 から 23 の整数で入力してください。")

        scheduled_hour = int(text)
        if not 0 <= scheduled_hour <= 23:
            raise ScraperError("定期実行の時刻は 0 から 23 の整数で入力してください。")

        return scheduled_hour

    def _settings_file_path(self) -> Path:
        return self.persistence_service.root_dir / "local_data" / GUI_SETTINGS_FILE_NAME

    def _load_gui_settings(self) -> dict[str, object]:
        settings_path = self._settings_file_path()
        if not settings_path.exists():
            return {}

        try:
            payload = json.loads(settings_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return {}

        return payload if isinstance(payload, dict) else {}

    def _save_gui_settings(self, **updates: object) -> None:
        settings_path = self._settings_file_path()
        settings_path.parent.mkdir(parents=True, exist_ok=True)
        payload = self._load_gui_settings()
        payload.update(updates)
        settings_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _load_saved_minrepo_schedule_enabled(self) -> bool:
        try:
            payload = self._load_gui_settings()
        except Exception:  # noqa: BLE001
            return True
        return normalize_schedule_enabled(payload.get("minrepo_schedule_enabled"), True)

    def _save_minrepo_schedule_enabled(self, enabled: bool) -> None:
        self._save_gui_settings(minrepo_schedule_enabled=bool(enabled))

    def _load_saved_schedule_hour(self) -> int:
        try:
            payload = self._load_gui_settings()
            scheduled_hour = int(payload.get("scheduled_fetch_hour", DEFAULT_SCHEDULE_HOUR))
        except Exception:  # noqa: BLE001
            return DEFAULT_SCHEDULE_HOUR

        if not 0 <= scheduled_hour <= 23:
            return DEFAULT_SCHEDULE_HOUR
        return scheduled_hour

    def _save_schedule_hour(self, scheduled_hour: int) -> None:
        self._save_gui_settings(scheduled_fetch_hour=scheduled_hour)

    def _load_saved_schedule_all_stores_interval_days(self) -> int:
        try:
            payload = self._load_gui_settings()
        except Exception:  # noqa: BLE001
            return DEFAULT_SCHEDULE_ALL_STORES_INTERVAL_DAYS
        return normalize_schedule_all_stores_interval_days(
            payload.get("schedule_all_stores_interval_days", DEFAULT_SCHEDULE_ALL_STORES_INTERVAL_DAYS)
        )

    def _save_schedule_all_stores_interval_days(self, interval_days: int) -> None:
        self._save_gui_settings(
            schedule_all_stores_interval_days=normalize_schedule_all_stores_interval_days(interval_days)
        )

    def _load_saved_schedule_supplemental_store_last_run_dates(self) -> dict[str, str]:
        try:
            payload = self._load_gui_settings()
        except Exception:  # noqa: BLE001
            return {}
        return normalize_schedule_store_run_dates(payload.get("schedule_supplemental_store_last_run_dates"))

    def _save_schedule_supplemental_store_last_run_dates(self) -> None:
        self._save_gui_settings(
            schedule_supplemental_store_last_run_dates=dict(
                sorted(self.schedule_supplemental_store_last_run_dates.items())
            )
        )

    def _schedule_all_stores_status_text(self) -> str:
        interval_days = getattr(
            self,
            "schedule_all_stores_interval_days",
            DEFAULT_SCHEDULE_ALL_STORES_INTERVAL_DAYS,
        )
        run_dates = getattr(self, "schedule_supplemental_store_last_run_dates", {})
        run_count = len(run_dates) if isinstance(run_dates, dict) else 0
        return f"{interval_days}日周期で分散 / 記録{run_count}店舗"

    def _load_saved_selected_store_urls(self, registered_stores: list[RegisteredStore]) -> set[str]:
        return {
            normalize_store_url(registered_store.url)
            for registered_store in registered_stores
            if registered_store.fetch_frequency in {FETCH_FREQUENCY_HIGH, FETCH_FREQUENCY_DAILY}
            and registered_store.uses_minrepo()
        }

    def _save_selected_store_urls(self) -> None:
        selected_store_urls = {
            normalize_store_url(registered_store.url)
            for registered_store in self.registered_stores
            if registered_store.fetch_frequency in {FETCH_FREQUENCY_HIGH, FETCH_FREQUENCY_DAILY}
            and registered_store.uses_minrepo()
        }
        self.selected_store_urls = selected_store_urls
        self._save_gui_settings(selected_store_urls=sorted(selected_store_urls))

    def _load_saved_web_publish_mode(self) -> str:
        try:
            payload = self._load_gui_settings()
        except Exception:  # noqa: BLE001
            return WEB_PUBLISH_MODE_DAYS
        return normalize_web_publish_mode(payload.get("web_publish_mode", WEB_PUBLISH_MODE_DAYS))

    def _load_saved_web_publish_interval_days(self) -> int:
        try:
            payload = self._load_gui_settings()
        except Exception:  # noqa: BLE001
            return DEFAULT_WEB_PUBLISH_INTERVAL_DAYS
        return normalize_web_publish_interval_days(
            payload.get("web_publish_interval_days", DEFAULT_WEB_PUBLISH_INTERVAL_DAYS)
        )

    def _save_web_publish_settings(self, options: WebPublishOptions) -> None:
        self._save_gui_settings(
            web_publish_mode=normalize_web_publish_mode(options.mode),
            web_publish_interval_days=options.interval_days,
        )

    def _load_saved_fetch_order_region_mode(self) -> str:
        try:
            payload = self._load_gui_settings()
        except Exception:  # noqa: BLE001
            return DEFAULT_FETCH_ORDER_REGION_MODE
        return normalize_fetch_order_region_mode(
            payload.get("fetch_order_region_mode", DEFAULT_FETCH_ORDER_REGION_MODE)
        )

    def _save_fetch_order_region_mode(self, fetch_order_region_mode: str) -> None:
        self._save_gui_settings(
            fetch_order_region_mode=normalize_fetch_order_region_mode(fetch_order_region_mode)
        )

    def _on_fetch_order_region_mode_changed(self, *_: object) -> None:
        self.fetch_order_region_mode = normalize_fetch_order_region_mode(
            self.fetch_order_region_mode_var.get()
        )
        self.fetch_order_region_mode_var.set(self.fetch_order_region_mode)
        try:
            self._save_fetch_order_region_mode(self.fetch_order_region_mode)
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("設定保存", f"店舗取得順設定の保存に失敗しました。\n{exc}")

    def _on_web_publish_mode_changed(self) -> None:
        self.web_publish_mode = normalize_web_publish_mode(self.web_publish_mode_var.get())
        interval_days = normalize_web_publish_interval_days(self.web_publish_interval_days_var.get())
        self.web_publish_interval_days_var.set(str(interval_days))
        try:
            self._save_web_publish_settings(
                WebPublishOptions(
                    mode=self.web_publish_mode,
                    interval_days=interval_days,
                )
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("設定保存", f"Web反映設定の保存に失敗しました。\n{exc}")
        self._update_button_states()

    def _load_saved_site7_browser_mode(self) -> str:
        try:
            payload = self._load_gui_settings()
        except Exception:  # noqa: BLE001
            return SITE7_BROWSER_MODE_VISIBLE
        return normalize_site7_browser_mode(payload.get("site7_browser_mode", SITE7_BROWSER_MODE_VISIBLE))

    def _save_site7_browser_mode(self, browser_mode: str) -> None:
        self._save_gui_settings(site7_browser_mode=normalize_site7_browser_mode(browser_mode))

    def _load_saved_site7_skip_juggler_difference(self) -> bool:
        try:
            payload = self._load_gui_settings()
        except Exception:  # noqa: BLE001
            return DEFAULT_SITE7_SKIP_JUGGLER_DIFFERENCE
        return normalize_schedule_enabled(
            payload.get("site7_skip_juggler_difference", DEFAULT_SITE7_SKIP_JUGGLER_DIFFERENCE),
            DEFAULT_SITE7_SKIP_JUGGLER_DIFFERENCE,
        )

    def _save_site7_skip_juggler_difference(self, enabled: bool) -> None:
        self._save_gui_settings(site7_skip_juggler_difference=bool(enabled))

    def _load_saved_site7_enabled_machine_names_by_source(self) -> dict[str, set[str]]:
        try:
            payload = self._load_gui_settings()
        except Exception:  # noqa: BLE001
            return {
                source_group: set(self.site7_target_machine_names)
                for source_group in SITE7_MACHINE_SOURCE_GROUPS
            }

        legacy_enabled_machine_names = normalize_site7_enabled_machine_names(
            payload.get("site7_enabled_machine_names", list(self.site7_target_machine_names)),
            self.site7_target_machine_names,
        )
        source_payload = payload.get("site7_enabled_machine_names_by_source")
        enabled_names_by_source: dict[str, set[str]] = {}
        for source_group in SITE7_MACHINE_SOURCE_GROUPS:
            if isinstance(source_payload, dict) and source_group in source_payload:
                raw_machine_names = source_payload.get(source_group)
            else:
                raw_machine_names = list(legacy_enabled_machine_names)
            enabled_names_by_source[source_group] = normalize_site7_enabled_machine_names(
                raw_machine_names,
                self.site7_target_machine_names,
            )
        return enabled_names_by_source

    def _save_site7_enabled_machine_names(self) -> None:
        source_payload = {
            source_group: sorted(
                self.site7_enabled_machine_names_by_source.get(source_group, set()),
                key=normalize_text,
            )
            for source_group in SITE7_MACHINE_SOURCE_GROUPS
        }
        legacy_machine_names = sorted(
            {
                machine_name
                for enabled_machine_names in source_payload.values()
                for machine_name in enabled_machine_names
            },
            key=normalize_text,
        )
        self._save_gui_settings(
            site7_enabled_machine_names=legacy_machine_names,
            site7_enabled_machine_names_by_source=source_payload,
        )

    def _current_site7_enabled_machine_names(self, source_group: str) -> set[str]:
        normalized_source_group = site7_machine_source_group(source_group)
        if not hasattr(self, "site7_machine_enabled_vars_by_source"):
            return set(
                getattr(self, "site7_enabled_machine_names_by_source", {}).get(
                    normalized_source_group,
                    set(),
                )
            )
        return {
            machine_name
            for machine_name, enabled_var in self.site7_machine_enabled_vars_by_source.get(
                normalized_source_group,
                {},
            ).items()
            if enabled_var.get()
        }

    def _site7_enabled_machine_names_for_fetch(self, registered_store: RegisteredStore | None = None) -> set[str] | None:
        if not hasattr(self, "site7_target_machine_names"):
            return None
        source_group = site7_machine_source_group(
            registered_store.fetch_source if registered_store is not None else FETCH_SOURCE_BOTH
        )
        enabled_machine_names = set(
            getattr(self, "site7_enabled_machine_names_by_source", {}).get(
                source_group,
                set(self.site7_target_machine_names),
            )
        )
        if enabled_machine_names == set(self.site7_target_machine_names):
            return None
        return enabled_machine_names

    def _site7_machine_requires_source_difference(
        self,
        machine_name: object,
        *,
        site7_difference_enabled: bool,
        skip_juggler_graph_differences: bool,
    ) -> bool:
        if not site7_difference_enabled:
            return False
        return not (skip_juggler_graph_differences and site7_machine_is_juggler(machine_name))

    def _site7_history_result_requires_source_difference(
        self,
        history_result: MachineHistoryResult,
        *,
        site7_difference_enabled: bool,
        skip_juggler_graph_differences: bool,
    ) -> bool:
        machine_names = {
            dataset.machine_name
            for dataset in history_result.datasets
            if str(dataset.machine_name or "").strip()
        }
        if not machine_names:
            return bool(site7_difference_enabled)
        return any(
            self._site7_machine_requires_source_difference(
                machine_name,
                site7_difference_enabled=site7_difference_enabled,
                skip_juggler_graph_differences=skip_juggler_graph_differences,
            )
            for machine_name in machine_names
        )

    def _site7_graph_difference_machine_names_for_fetch(
        self,
        enabled_machine_names: set[str] | None,
        *,
        skip_juggler_graph_differences: bool,
    ) -> set[str] | None:
        if not skip_juggler_graph_differences:
            return None
        if enabled_machine_names is None:
            machine_names = set(
                getattr(
                    self,
                    "site7_target_machine_names",
                    tuple(list_site7_target_machine_names()),
                )
            )
        else:
            machine_names = set(enabled_machine_names)
        return {
            machine_name
            for machine_name in machine_names
            if not site7_machine_is_juggler(machine_name)
        }

    def _site7_fetch_requires_login(self, target_stores: list[RegisteredStore]) -> bool:
        return any(not registered_store_uses_daidata_online(registered_store) for registered_store in target_stores)

    def _site7_has_enabled_target_machines(self, target_stores: list[RegisteredStore] | None = None) -> bool:
        if not hasattr(self, "site7_target_machine_names"):
            return True
        enabled_by_source = getattr(self, "site7_enabled_machine_names_by_source", {})
        if target_stores is None:
            source_groups = SITE7_MACHINE_SOURCE_GROUPS
        else:
            real_site7_stores = [
                registered_store
                for registered_store in target_stores
                if not registered_store_uses_daidata_online(registered_store)
            ]
            if not real_site7_stores:
                return True
            source_groups = tuple(
                sorted(
                    {
                        site7_machine_source_group(registered_store.fetch_source)
                        for registered_store in real_site7_stores
                    },
                    key=lambda source_group: SITE7_MACHINE_SOURCE_GROUPS.index(source_group),
                )
            )
        return any(bool(enabled_by_source.get(source_group, set())) for source_group in source_groups)

    def _site7_machine_settings_status_text(self) -> str:
        total_count = len(getattr(self, "site7_target_machine_names", ()))
        enabled_by_source = getattr(self, "site7_enabled_machine_names_by_source", {})
        parts = []
        for source_group in SITE7_MACHINE_SOURCE_GROUPS:
            source_label = "両方" if source_group == FETCH_SOURCE_BOTH else "サイセのみ"
            enabled_count = len(enabled_by_source.get(source_group, set()))
            parts.append(f"{source_label} {enabled_count}/{total_count}")
        return "、".join(parts) + " 機種を取得対象にしています"

    def _update_site7_machine_settings_status(self) -> None:
        if hasattr(self, "site7_machine_settings_status_var"):
            self.site7_machine_settings_status_var.set(self._site7_machine_settings_status_text())

    def _on_site7_machine_setting_changed(self, source_group: str) -> None:
        normalized_source_group = site7_machine_source_group(source_group)
        self.site7_enabled_machine_names_by_source[normalized_source_group] = (
            self._current_site7_enabled_machine_names(normalized_source_group)
        )
        try:
            self._save_site7_enabled_machine_names()
            self._update_site7_machine_settings_status()
        except Exception as exc:  # noqa: BLE001
            self.site7_machine_settings_status_var.set(f"保存に失敗しました: {exc}")
        self._update_button_states()

    def _select_all_site7_target_machines(self, source_group: str) -> None:
        normalized_source_group = site7_machine_source_group(source_group)
        for enabled_var in self.site7_machine_enabled_vars_by_source.get(normalized_source_group, {}).values():
            enabled_var.set(True)
        self._on_site7_machine_setting_changed(normalized_source_group)

    def _clear_site7_target_machines(self, source_group: str) -> None:
        normalized_source_group = site7_machine_source_group(source_group)
        for enabled_var in self.site7_machine_enabled_vars_by_source.get(normalized_source_group, {}).values():
            enabled_var.set(False)
        self._on_site7_machine_setting_changed(normalized_source_group)

    def _load_saved_site7_schedule_hours(self) -> tuple[int, ...]:
        try:
            payload = self._load_gui_settings()
        except Exception:  # noqa: BLE001
            return DEFAULT_SITE7_SCHEDULE_HOURS
        return normalize_site7_schedule_hours(payload.get("site7_schedule_hours", DEFAULT_SITE7_SCHEDULE_HOURS))

    def _load_saved_site7_schedule_enabled(self) -> bool:
        try:
            payload = self._load_gui_settings()
        except Exception:  # noqa: BLE001
            return True
        return normalize_schedule_enabled(payload.get("site7_schedule_enabled"), True)

    def _save_site7_schedule_enabled(self, enabled: bool) -> None:
        self._save_gui_settings(site7_schedule_enabled=bool(enabled))

    def _save_site7_schedule_hours(self, schedule_hours: tuple[int, ...]) -> None:
        self._save_gui_settings(site7_schedule_hours=list(schedule_hours))

    def _load_saved_site7_schedule_run_dates(self) -> dict[int, str]:
        try:
            payload = self._load_gui_settings()
        except Exception:  # noqa: BLE001
            return {}
        return normalize_site7_schedule_run_dates(payload.get("site7_schedule_last_run_dates", {}))

    def _save_site7_schedule_run_dates(self) -> None:
        payload = {
            str(hour): date_text
            for hour, date_text in sorted(self.site7_schedule_last_run_dates_by_hour.items())
            if hour in SITE7_SCHEDULE_HOUR_OPTIONS
        }
        self._save_gui_settings(site7_schedule_last_run_dates=payload)

    def _load_saved_site7_schedule_store_last_run_dates(self) -> dict[str, str]:
        try:
            payload = self._load_gui_settings()
        except Exception:  # noqa: BLE001
            return {}
        return normalize_schedule_store_run_dates(payload.get("site7_schedule_store_last_run_dates"))

    def _save_site7_schedule_store_last_run_dates(self) -> None:
        self._save_gui_settings(
            site7_schedule_store_last_run_dates=dict(
                sorted(self.site7_schedule_store_last_run_dates.items())
            )
        )

    def _mark_site7_schedule_hour_started(self, scheduled_hour: int, started_at: datetime | None = None) -> None:
        if scheduled_hour not in SITE7_SCHEDULE_HOUR_OPTIONS:
            return
        started_time = (started_at or datetime.now(JST)).astimezone(JST)
        self.site7_schedule_last_run_dates_by_hour[scheduled_hour] = started_time.date().isoformat()
        try:
            self._save_site7_schedule_run_dates()
        except Exception as exc:  # noqa: BLE001
            self.site7_schedule_status_var.set(f"サイトセブン定期実行の記録保存に失敗しました: {exc}")

    def _schedule_timer_tick(self) -> None:
        self._run_minrepo_priority_watch_if_due()
        self._run_scheduled_fetch_if_due()
        self._run_scheduled_site7_fetch_if_due()
        self.root.after(30_000, self._schedule_timer_tick)

    def _run_minrepo_priority_watch_if_due(self, now: datetime | None = None) -> None:
        if not self.minrepo_schedule_enabled:
            self.minrepo_priority_watch_pending = False
            self.minrepo_priority_watch_next_check_at = None
            return

        current_time = (now or datetime.now(JST)).astimezone(JST)
        target_date = minrepo_priority_watch_target_date(current_time)
        if self.minrepo_priority_watch_target_date != target_date:
            self.minrepo_priority_watch_target_date = target_date
            self.minrepo_priority_watch_pending = False
            self.minrepo_priority_watch_next_check_at = None
            self.minrepo_priority_watch_completed_store_dates = set()

        if not minrepo_priority_watch_is_active(current_time):
            self.minrepo_priority_watch_pending = False
            self.minrepo_priority_watch_next_check_at = None
            return

        if not self._minrepo_priority_watch_registered_stores(
            self.registered_stores,
            target_date=target_date,
            completed_store_dates=self.minrepo_priority_watch_completed_store_dates,
        ):
            return

        if self.minrepo_priority_watch_pending:
            if self._minrepo_start_blocked():
                self.schedule_status_var.set(f"{target_date} の取得順店舗みんレポ確認を待機中")
                return
            self._start_minrepo_priority_watch(current_time, target_date)
            return

        if self.minrepo_priority_watch_next_check_at is not None and current_time < self.minrepo_priority_watch_next_check_at:
            return

        if self._minrepo_start_blocked():
            self.minrepo_priority_watch_pending = True
            self.schedule_status_var.set(f"{target_date} の取得順店舗みんレポ確認を待機中")
            return

        self._start_minrepo_priority_watch(current_time, target_date)

    def _start_minrepo_priority_watch(self, now: datetime, target_date: str) -> None:
        if not self.minrepo_schedule_enabled:
            self.minrepo_priority_watch_pending = False
            self.minrepo_priority_watch_next_check_at = None
            self.schedule_status_var.set(self._schedule_status_text())
            return

        try:
            retry_delay_seconds = self._retry_delay_seconds_input()
            fetch_parallel_options = self._minrepo_fetch_parallel_options()
            web_publish_options = self._web_publish_options_input()
        except ScraperError as exc:
            self.schedule_status_var.set("早朝みんレポ確認を開始できません")
            self._show_error(exc)
            return

        self.minrepo_priority_watch_pending = False
        self.minrepo_priority_watch_next_check_at = now.astimezone(JST) + timedelta(
            minutes=MINREPO_PRIORITY_WATCH_CHECK_INTERVAL_MINUTES
        )
        self._begin_fetch_run(
            progress_message="早朝みんレポ更新を確認中...",
            status_message="みんレポ確認中...",
            summary_message=f"取得順店舗の {target_date} を確認中",
        )
        self.schedule_status_var.set(f"{target_date} の取得順店舗みんレポ確認中")
        self.minrepo_cancel_event.clear()
        self._start_worker(
            self._worker_minrepo_priority_watch,
            target_date,
            retry_delay_seconds,
            fetch_parallel_options,
            web_publish_options,
            set(self.minrepo_priority_watch_completed_store_dates),
            operation_kind="minrepo_priority_watch",
        )

    def _run_scheduled_fetch_if_due(self) -> None:
        if not self.minrepo_schedule_enabled:
            self.scheduled_pending_date = None
            self.scheduled_startup_prompt_date = None
            return

        now = datetime.now(JST)
        due_date = scheduled_fetch_due_date(
            self.scheduled_fetch_hour,
            self.scheduled_last_run_date,
            now,
        )
        if self.scheduled_pending_date is not None:
            if self._minrepo_start_blocked():
                self.schedule_status_var.set(f"{self.scheduled_pending_date} の定期実行を待機中")
                return
            self.scheduled_last_run_date = self.scheduled_pending_date
            self.scheduled_pending_date = None
            self._start_scheduled_fetch()
            return

        if due_date is None:
            return

        if self.scheduled_startup_prompt_date == due_date:
            self.schedule_status_var.set(f"本日 {self.scheduled_fetch_hour} 時の定期実行を確認待ち")
            return

        if self._minrepo_start_blocked():
            self.scheduled_pending_date = due_date
            self.schedule_status_var.set(f"本日 {self.scheduled_fetch_hour} 時の定期実行を待機中")
            return

        self.scheduled_last_run_date = due_date
        self._start_scheduled_fetch()

    def _schedule_site7_update_recheck(
        self,
        *,
        scheduled_hour: int,
        waiting_store_urls: set[str],
        run_date: str,
        waiting_started_at: datetime,
        now: datetime | None = None,
    ) -> None:
        normalized_urls = {
            normalized_store_url
            for store_url in waiting_store_urls
            if (normalized_store_url := normalize_store_url(store_url))
        }
        if not normalized_urls:
            return

        if not hasattr(self, "site7_schedule_recheck_requests"):
            self.site7_schedule_recheck_requests = {}
        current_time = (now or datetime.now(JST)).astimezone(JST)
        first_checked_at = _as_jst_datetime(waiting_started_at)
        expires_at = first_checked_at + timedelta(minutes=SITE7_SCHEDULE_RECHECK_LIMIT_MINUTES)
        if current_time >= expires_at:
            self.site7_schedule_recheck_requests.pop(scheduled_hour, None)
            self.site7_schedule_status_var.set(f"{scheduled_hour}時台のサイトセブン更新待ちは終了しました")
            return

        existing_request = getattr(self, "site7_schedule_recheck_requests", {}).get(scheduled_hour)
        if existing_request is not None:
            first_checked_at = min(existing_request.first_checked_at, first_checked_at)
            expires_at = first_checked_at + timedelta(minutes=SITE7_SCHEDULE_RECHECK_LIMIT_MINUTES)
            normalized_urls.update(existing_request.store_urls)

        request = Site7ScheduleRecheckRequest(
            scheduled_hour=scheduled_hour,
            store_urls=normalized_urls,
            run_date=run_date,
            first_checked_at=first_checked_at,
            next_check_at=current_time + timedelta(minutes=SITE7_SCHEDULE_RECHECK_INTERVAL_MINUTES),
            expires_at=expires_at,
        )
        self.site7_schedule_recheck_requests[scheduled_hour] = request
        self.site7_schedule_status_var.set(
            f"{scheduled_hour}時台のサイトセブン更新待ち: 10分後に再確認"
        )

    def _scheduled_site7_recheck_due_request(self, now: datetime) -> Site7ScheduleRecheckRequest | None:
        requests = getattr(self, "site7_schedule_recheck_requests", {})
        if not requests:
            return None

        current_time = now.astimezone(JST)
        expired_hours = [
            hour
            for hour, request in requests.items()
            if current_time >= request.expires_at
        ]
        for hour in expired_hours:
            requests.pop(hour, None)
        if expired_hours:
            self.site7_schedule_status_var.set("サイトセブン更新待ちを1時間で終了しました")

        due_requests = [
            request
            for request in requests.values()
            if current_time >= request.next_check_at
        ]
        if not due_requests:
            return None
        return min(due_requests, key=lambda request: request.next_check_at)

    def _mark_successful_supplemental_stores(
        self,
        fetch_many_result: FetchManyResult,
        supplemental_store_urls: set[str],
        run_date: str,
    ) -> None:
        normalized_supplemental_urls = {
            normalized_store_url
            for store_url in supplemental_store_urls
            if (normalized_store_url := normalize_store_url(store_url))
        }
        if not normalized_supplemental_urls:
            self.schedule_all_stores_status_var.set(self._schedule_all_stores_status_text())
            return

        successful_urls = {
            normalize_store_url(store_result.history_result.store_url)
            for store_result in fetch_many_result.results
        }
        completed_urls = normalized_supplemental_urls.intersection(successful_urls)
        if not completed_urls:
            self.schedule_all_stores_status_var.set(self._schedule_all_stores_status_text())
            return

        for store_url in completed_urls:
            self.schedule_supplemental_store_last_run_dates[store_url] = run_date
        self._save_schedule_supplemental_store_last_run_dates()
        self.schedule_all_stores_status_var.set(self._schedule_all_stores_status_text())

    def _mark_successful_site7_schedule_stores(
        self,
        fetch_many_result: FetchManyResult,
        store_run_urls: set[str],
        run_date: str,
    ) -> None:
        normalized_store_run_urls = {
            normalized_store_url
            for store_url in store_run_urls
            if (normalized_store_url := normalize_store_url(store_url))
        }
        if not normalized_store_run_urls:
            return

        successful_urls = {
            normalize_store_url(store_result.history_result.store_url)
            for store_result in fetch_many_result.results
        }
        completed_urls = normalized_store_run_urls.intersection(successful_urls)
        if not completed_urls:
            return

        for store_url in completed_urls:
            self.site7_schedule_store_last_run_dates[store_url] = run_date
        self._save_site7_schedule_store_last_run_dates()

    def _start_scheduled_fetch(self) -> None:
        if not self.minrepo_schedule_enabled:
            self.scheduled_pending_date = None
            self.schedule_status_var.set(self._schedule_status_text())
            return

        try:
            target_date_input = self._target_date_input_from_recent_days()
            retry_delay_seconds = self._retry_delay_seconds_input()
            fetch_parallel_options = self._minrepo_fetch_parallel_options()
            web_publish_options = self._web_publish_options_input()
        except ScraperError as exc:
            self.schedule_status_var.set("定期実行を開始できません")
            self._show_error(exc)
            return

        selected_store_urls = set(self.selected_store_urls)
        supplemental_store_last_run_dates = dict(self.schedule_supplemental_store_last_run_dates)
        supplemental_run_date = current_jst_date_text()
        self.current_results = []
        self.current_history_result = None
        self._clear_fetch_result_details()
        self._begin_fetch_progress("定期実行: 登録店舗を更新中...")
        self.status_var.set("定期実行中...")
        self.summary_var.set("登録店舗を更新してから毎日店舗と低頻度店舗を取得します")
        self.schedule_status_var.set("定期実行中")
        self.minrepo_cancel_event.clear()
        self._start_worker(
            self._worker_scheduled_fetch,
            target_date_input,
            retry_delay_seconds,
            fetch_parallel_options,
            web_publish_options,
            selected_store_urls,
            supplemental_store_last_run_dates,
            self.schedule_all_stores_interval_days,
            supplemental_run_date,
            operation_kind="scheduled_fetch",
        )

    def _run_scheduled_site7_fetch_if_due(self) -> None:
        if not self.site7_schedule_enabled:
            self.site7_schedule_startup_prompt_hour = None
            self.site7_schedule_pending_hours = set()
            self.site7_schedule_recheck_requests = {}
            return

        now = datetime.now(JST)
        if not self.site7_schedule_hours:
            return

        recheck_request = self._scheduled_site7_recheck_due_request(now)
        if recheck_request is not None:
            if self._site7_start_blocked():
                self.site7_schedule_status_var.set(
                    f"{recheck_request.scheduled_hour}時台のサイトセブン更新再確認を待機中"
                )
                return
            self.site7_schedule_recheck_requests.pop(recheck_request.scheduled_hour, None)
            self._start_scheduled_site7_fetch(
                recheck_request.scheduled_hour,
                now,
                target_store_urls=set(recheck_request.store_urls),
                run_date=recheck_request.run_date,
                waiting_started_at=recheck_request.first_checked_at,
            )
            return

        due_hour = site7_schedule_due_hour(
            self.site7_schedule_hours,
            self.site7_schedule_last_run_dates_by_hour,
            now,
        )
        if due_hour is not None:
            if getattr(self, "site7_schedule_startup_prompt_hour", None) == due_hour:
                self.site7_schedule_status_var.set(f"本日 {due_hour} 時のサイトセブン定期実行を確認待ち")
                return
            self.site7_schedule_pending_hours.add(due_hour)

        self.site7_schedule_pending_hours = {
            hour for hour in self.site7_schedule_pending_hours if hour in self.site7_schedule_hours
        }
        if not self.site7_schedule_pending_hours:
            return

        if self._site7_start_blocked():
            pending_hours_text = "、".join(f"{hour}時台" for hour in sorted(self.site7_schedule_pending_hours))
            self.site7_schedule_status_var.set(f"{pending_hours_text}のサイトセブン定期実行を待機中")
            return

        pending_hour = min(self.site7_schedule_pending_hours)
        self._start_scheduled_site7_fetch(pending_hour, now)

    def _start_scheduled_site7_fetch(
        self,
        scheduled_hour: int,
        started_at: datetime | None = None,
        *,
        target_store_urls: set[str] | None = None,
        run_date: str | None = None,
        waiting_started_at: datetime | None = None,
    ) -> None:
        if not self.site7_schedule_enabled:
            self.site7_schedule_pending_hours.discard(scheduled_hour)
            self.site7_schedule_status_var.set(self._site7_schedule_status_text())
            return

        self.site7_schedule_pending_hours.discard(scheduled_hour)
        if target_store_urls is None:
            self._mark_site7_schedule_hour_started(scheduled_hour, started_at)
        try:
            recent_days = parse_recent_days(self.target_date_var.get())
            retry_delay_seconds = self._retry_delay_seconds_input()
            fetch_parallel_options = self._minrepo_fetch_parallel_options()
            web_publish_options = self._web_publish_options_input()
        except ScraperError as exc:
            self.site7_schedule_status_var.set("サイトセブン定期実行を開始できません")
            self._show_error(exc)
            return

        recent_days = min(recent_days, SITE7_MAX_RECENT_DAYS)
        if not self.site7_scraper.has_saved_login_state():
            self.site7_schedule_status_var.set("サイトセブン定期実行はログイン情報待ち")
            return
        if not self._site7_has_enabled_target_machines():
            self.site7_schedule_status_var.set("サイトセブン定期実行は取得機種なし")
            return

        self.current_results = []
        self.current_history_result = None
        self._clear_fetch_result_details()
        self._begin_fetch_progress(
            "サイトセブン定期実行: 登録店舗を更新中...",
            progress_kind=PROGRESS_KIND_SITE7,
        )
        self.status_var.set("サイトセブン定期実行中...")
        self.summary_var.set("登録店舗を更新してからサイトセブン取得します")
        self.site7_schedule_status_var.set("サイトセブン定期実行中")
        self.site7_cancel_event.clear()
        browser_visible = self._site7_browser_visible()
        skip_juggler_graph_differences = self._site7_skip_juggler_difference_enabled()
        self._start_worker(
            self._worker_scheduled_site7_fetch,
            recent_days,
            retry_delay_seconds,
            browser_visible,
            fetch_parallel_options,
            web_publish_options,
            scheduled_hour,
            dict(self.site7_schedule_store_last_run_dates),
            run_date or current_jst_date_text(started_at),
            target_store_urls,
            waiting_started_at,
            skip_juggler_graph_differences,
            operation_kind="scheduled_site7_fetch",
        )

    def _hide_to_resident(self) -> None:
        if not self._ensure_tray_icon():
            messagebox.showwarning(
                "常駐",
                "常駐アイコンを表示できません。requirements.txt の内容を入れ直してください。",
            )
            return

        self.root.withdraw()
        if not self.is_busy:
            self.status_var.set("常駐中")

    def _on_window_close(self) -> None:
        result = self._ask_window_close_action()
        if result == "quit":
            self._quit_application()
            return
        if result == "resident":
            self._hide_to_resident()

    def _ask_window_close_action(self) -> str | None:
        selected_action: str | None = None
        dialog = tk.Toplevel(self.root)
        dialog.title("終了確認")
        dialog.transient(self.root)
        dialog.resizable(False, False)

        def close_dialog() -> None:
            dialog.destroy()

        def choose_action(action: str) -> None:
            nonlocal selected_action
            selected_action = action
            dialog.destroy()

        dialog.protocol("WM_DELETE_WINDOW", close_dialog)
        dialog.bind("<Escape>", lambda event: close_dialog())

        container = ttk.Frame(dialog, padding=16)
        container.pack(fill="both", expand=True)

        ttk.Label(
            container,
            text="閉じる時の動作を選んでください。",
            justify="left",
        ).pack(anchor="w")

        button_row = ttk.Frame(container)
        button_row.pack(anchor="e", pady=(12, 0))

        resident_button = ttk.Button(
            button_row,
            text="常駐",
            command=lambda: choose_action("resident"),
        )
        resident_button.pack(side="right")

        quit_button = ttk.Button(
            button_row,
            text="終了",
            command=lambda: choose_action("quit"),
        )
        quit_button.pack(side="right", padx=(0, 8))

        self._position_dialog_near_root(dialog)
        dialog.grab_set()
        quit_button.focus_set()
        dialog.wait_window()
        return selected_action

    def _position_dialog_near_root(self, dialog: tk.Toplevel) -> None:
        dialog.update_idletasks()
        dialog_width = max(dialog.winfo_reqwidth(), 1)
        dialog_height = max(dialog.winfo_reqheight(), 1)
        root_width = max(self.root.winfo_width(), 1)
        root_height = max(self.root.winfo_height(), 1)
        x = self.root.winfo_rootx() + max((root_width - dialog_width) // 2, 0)
        y = self.root.winfo_rooty() + max((root_height - dialog_height) // 2, 0)
        dialog.geometry(f"+{x}+{y}")

    def _ensure_tray_icon(self) -> bool:
        if self.tray_icon is not None:
            return True

        if pystray is None or Image is None or ImageDraw is None:
            return False

        try:
            icon_image = self._create_tray_icon_image()
            self.tray_icon = pystray.Icon(
                "halldata",
                icon_image,
                "Halldata",
                menu=pystray.Menu(
                    pystray.MenuItem("表示", self._on_tray_show, default=True),
                    pystray.MenuItem("終了", self._on_tray_exit),
                ),
            )
            self.tray_thread = threading.Thread(target=self.tray_icon.run, daemon=True)
            self.tray_thread.start()
            return True
        except Exception as exc:  # noqa: BLE001
            self.tray_icon = None
            self.tray_thread = None
            messagebox.showwarning("常駐", f"常駐アイコンを表示できませんでした。\n{exc}")
            return False

    def _create_tray_icon_image(self) -> object:
        image = Image.new("RGBA", (64, 64), (255, 255, 255, 0))
        draw = ImageDraw.Draw(image)
        draw.rectangle((8, 8, 56, 56), fill=(48, 126, 204, 255))
        draw.rectangle((14, 14, 50, 50), outline=(255, 255, 255, 255), width=4)
        draw.text((24, 22), "H", fill=(255, 255, 255, 255))
        return image

    def _on_tray_show(self, *_: object) -> None:
        self.root.after(0, self._show_from_tray)

    def _show_from_tray(self) -> None:
        self.root.deiconify()
        self.root.lift()
        self.root.focus_force()
        if self.status_var.get() == "常駐中":
            self.status_var.set("待機中")

    def _on_tray_exit(self, *_: object) -> None:
        self.root.after(0, self._quit_from_tray)

    def _quit_from_tray(self) -> None:
        self._quit_application()

    def _quit_application(self) -> None:
        if self.tray_icon is not None:
            self.tray_icon.stop()
            self.tray_icon = None
        self.tray_thread = None
        self.site7_scraper.close_visible_browser()
        self.root.destroy()

    def _build_site7_machine_settings_tab(self, tab: ttk.Frame) -> None:
        guide = ttk.LabelFrame(tab, text="案内", padding=12)
        guide.grid(row=0, column=0, sticky="ew")
        guide.columnconfigure(0, weight=1)
        ttk.Label(
            guide,
            text=(
                "サイトセブン取得で読む機種を選びます。"
                "取得元が両方の店舗と、サイセのみの店舗で別々に使います。"
                "チェックを外した機種は、対応する店舗のサイトセブン取得時に開きません。"
            ),
            wraplength=900,
            justify="left",
        ).grid(row=0, column=0, sticky="w")

        ttk.Label(tab, textvariable=self.site7_machine_settings_status_var).grid(
            row=1,
            column=0,
            sticky="w",
            pady=(12, 0),
        )

        machine_container = ttk.Frame(tab)
        machine_container.grid(row=2, column=0, sticky="nsew", pady=(12, 0))
        machine_container.columnconfigure(0, weight=1)

        self.site7_machine_checkbuttons: dict[tuple[str, str], ttk.Checkbutton] = {}
        self.site7_machine_action_buttons: list[ttk.Button] = []
        for group_index, source_group in enumerate(SITE7_MACHINE_SOURCE_GROUPS):
            machine_frame = ttk.LabelFrame(
                machine_container,
                text=SITE7_MACHINE_SOURCE_GROUP_TITLES[source_group],
                padding=12,
            )
            machine_frame.grid(row=group_index, column=0, sticky="ew", pady=(0 if group_index == 0 else 12, 0))
            for column_index in range(3):
                machine_frame.columnconfigure(column_index, weight=1)

            ttk.Label(
                machine_frame,
                text=SITE7_MACHINE_SOURCE_GROUP_HELP[source_group],
                justify="left",
            ).grid(row=0, column=0, columnspan=3, sticky="w")

            action_row = ttk.Frame(machine_frame)
            action_row.grid(row=1, column=0, columnspan=3, sticky="w", pady=(8, 6))
            select_button = ttk.Button(
                action_row,
                text="全選択",
                command=lambda source_group=source_group: self._select_all_site7_target_machines(source_group),
            )
            select_button.grid(row=0, column=0, sticky="w")
            clear_button = ttk.Button(
                action_row,
                text="全解除",
                command=lambda source_group=source_group: self._clear_site7_target_machines(source_group),
            )
            clear_button.grid(row=0, column=1, sticky="w", padx=(8, 0))
            self.site7_machine_action_buttons.extend([select_button, clear_button])

            for index, machine_name in enumerate(self.site7_target_machine_names):
                row_index = 2 + index // 3
                column_index = index % 3
                checkbutton = ttk.Checkbutton(
                    machine_frame,
                    text=machine_name,
                    variable=self.site7_machine_enabled_vars_by_source[source_group][machine_name],
                    command=lambda source_group=source_group: self._on_site7_machine_setting_changed(source_group),
                )
                checkbutton.grid(row=row_index, column=column_index, sticky="w", padx=(0, 24), pady=3)
                self.site7_machine_checkbuttons[(source_group, machine_name)] = checkbutton

    def _build_register_tab(self, register_tab: ttk.Frame) -> None:
        guide = ttk.LabelFrame(register_tab, text="案内", padding=12)
        guide.grid(row=0, column=0, sticky="ew")
        guide.columnconfigure(0, weight=1)

        ttk.Label(
            guide,
            text=(
                "ここでは店舗URLを入れて店舗名を自動取得し、一覧へ登録できます。"
                "頻度が毎日または高頻度の店舗を、データ取得タブの取得ボタンで順番に取得します。"
                "登録済み一覧の行を右クリックすると、その店舗だけを個別取得できます。"
                "登録した店舗一覧はローカルJSONに保存されます。"
                "一覧で行を選ぶと、選んだ店舗を登録一覧から削除できます。"
            ),
            wraplength=900,
            justify="left",
        ).grid(row=0, column=0, sticky="w")

        form = ttk.LabelFrame(register_tab, text="店舗を登録", padding=12)
        form.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        form.columnconfigure(1, weight=1)
        form.rowconfigure(8, weight=1)

        ttk.Label(form, text="店舗URL").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        self.register_store_url_entry = ttk.Entry(form, textvariable=self.register_store_url_var)
        self.register_store_url_entry.grid(row=0, column=1, sticky="ew", pady=4)

        ttk.Label(form, text="取得設定").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        fetch_option_row = ttk.Frame(form)
        fetch_option_row.grid(row=1, column=1, sticky="w", pady=4)
        ttk.Label(fetch_option_row, text="頻度").grid(row=0, column=0, sticky="w")
        self.register_store_frequency_selector = ttk.Combobox(
            fetch_option_row,
            textvariable=self.register_store_frequency_var,
            values=FETCH_FREQUENCY_OPTIONS,
            state="readonly",
            width=8,
        )
        self.register_store_frequency_selector.grid(row=0, column=1, sticky="w", padx=(6, 14))
        ttk.Label(fetch_option_row, text="取得元").grid(row=0, column=2, sticky="w")
        self.register_store_source_selector = ttk.Combobox(
            fetch_option_row,
            textvariable=self.register_store_source_var,
            values=FETCH_SOURCE_OPTIONS,
            state="readonly",
            width=8,
        )
        self.register_store_source_selector.grid(row=0, column=3, sticky="w", padx=(6, 14))
        ttk.Label(fetch_option_row, text="取得順").grid(row=0, column=4, sticky="w")
        self.register_store_order_entry = ttk.Entry(
            fetch_option_row,
            textvariable=self.register_store_order_var,
            width=8,
        )
        self.register_store_order_entry.grid(row=0, column=5, sticky="w", padx=(6, 14))
        self.register_store_site7_difference_checkbutton = ttk.Checkbutton(
            fetch_option_row,
            text="S差枚",
            variable=self.register_store_site7_difference_enabled_var,
        )
        self.register_store_site7_difference_checkbutton.grid(row=0, column=6, sticky="w")

        ttk.Label(form, text="都道府県").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=4)
        self.register_store_prefecture_entry = ttk.Entry(form, textvariable=self.register_store_prefecture_var)
        self.register_store_prefecture_entry.grid(row=2, column=1, sticky="ew", pady=4)

        ttk.Label(form, text="地域").grid(row=3, column=0, sticky="w", padx=(0, 8), pady=4)
        self.register_store_area_entry = ttk.Entry(form, textvariable=self.register_store_area_var)
        self.register_store_area_entry.grid(row=3, column=1, sticky="ew", pady=4)

        ttk.Label(form, text="SS店舗名").grid(row=4, column=0, sticky="w", padx=(0, 8), pady=4)
        self.register_store_site7_store_name_entry = ttk.Entry(form, textvariable=self.register_store_site7_store_name_var)
        self.register_store_site7_store_name_entry.grid(row=4, column=1, sticky="ew", pady=4)

        ttk.Label(form, text="SS ID").grid(row=5, column=0, sticky="w", padx=(0, 8), pady=4)
        self.register_store_site7_hall_id_entry = ttk.Entry(form, textvariable=self.register_store_site7_hall_id_var)
        self.register_store_site7_hall_id_entry.grid(row=5, column=1, sticky="ew", pady=4)

        ttk.Label(form, text="SS住所").grid(row=6, column=0, sticky="w", padx=(0, 8), pady=4)
        self.register_store_site7_address_entry = ttk.Entry(form, textvariable=self.register_store_site7_address_var)
        self.register_store_site7_address_entry.grid(row=6, column=1, sticky="ew", pady=4)

        action_row = ttk.Frame(form)
        action_row.grid(row=7, column=1, sticky="w", pady=(8, 8))
        self.register_store_button = ttk.Button(action_row, text="登録する", command=self.register_store)
        self.register_store_button.grid(row=0, column=0, sticky="w")
        self.update_registered_store_button = ttk.Button(
            action_row,
            text="選択行を更新",
            command=self.update_registered_store,
        )
        self.update_registered_store_button.grid(row=0, column=1, sticky="w", padx=(8, 0))
        self.clear_register_store_form_button = ttk.Button(
            action_row,
            text="入力欄をクリア",
            command=self.clear_register_store_form,
        )
        self.clear_register_store_form_button.grid(row=0, column=2, sticky="w", padx=(8, 0))

        ttk.Label(action_row, textvariable=self.register_store_status_var).grid(row=0, column=3, sticky="w", padx=(12, 0))

        table_frame = ttk.LabelFrame(form, text="登録済み一覧", padding=8)
        table_frame.grid(row=8, column=0, columnspan=2, sticky="nsew", pady=(8, 0))
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(2, weight=1)

        target_action_row = ttk.Frame(table_frame)
        target_action_row.grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))
        self.refresh_registered_stores_button = ttk.Button(
            target_action_row,
            text="最新に更新",
            command=self.refresh_registered_stores,
        )
        self.refresh_registered_stores_button.grid(row=0, column=0, sticky="w")
        self.delete_registered_stores_button = ttk.Button(
            target_action_row,
            text="選択した店舗を削除",
            command=self.delete_registered_stores,
        )
        self.delete_registered_stores_button.grid(row=0, column=1, sticky="w", padx=(8, 0))
        self.apply_my_hall_stores_button = ttk.Button(
            target_action_row,
            text="Webマイホールを毎日に反映",
            command=self.apply_shared_my_hall_to_registered_stores,
        )
        self.apply_my_hall_stores_button.grid(row=0, column=2, sticky="w", padx=(8, 0))

        filter_row = ttk.Frame(table_frame)
        filter_row.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        filter_row.columnconfigure(1, weight=1)
        ttk.Label(filter_row, text="店舗名検索").grid(row=0, column=0, sticky="w", padx=(0, 8))
        self.registered_store_filter_entry = ttk.Entry(
            filter_row,
            textvariable=self.registered_store_filter_var,
        )
        self.registered_store_filter_entry.grid(row=0, column=1, sticky="ew")
        self.clear_registered_store_filter_button = ttk.Button(
            filter_row,
            text="クリア",
            command=self._clear_registered_store_filter,
        )
        self.clear_registered_store_filter_button.grid(row=0, column=2, sticky="w", padx=(8, 0))
        ttk.Label(filter_row, textvariable=self.registered_store_filter_status_var).grid(
            row=0,
            column=3,
            sticky="w",
            padx=(12, 0),
        )
        self.registered_store_filter_var.trace_add("write", self._on_registered_store_filter_changed)

        self.registered_store_tree = ttk.Treeview(
            table_frame,
            columns=REGISTERED_STORE_COLUMNS,
            show="headings",
            selectmode="extended",
        )
        self.registered_store_tree.grid(row=2, column=0, sticky="nsew")

        for column in REGISTERED_STORE_COLUMNS:
            self.registered_store_tree.heading(
                column,
                text=self._registered_store_heading_text(column),
                command=lambda current_column=column: self._sort_registered_store_table_by(current_column),
            )
            if column in {"頻度", "取得元", "S差枚", "取得順"}:
                self.registered_store_tree.column(column, width=80, minwidth=80, anchor="center")
                continue
            self.registered_store_tree.column(
                column,
                width=220 if column == "店舗名" else 180 if column in {"都道府県", "地域", "SS店舗名", "SS ID"} else 520,
                minwidth=120 if column != "URL" else 280,
                anchor="w",
            )
        self.registered_store_tree.bind("<Button-1>", self._on_registered_store_tree_click)
        self.registered_store_tree.bind("<Button-3>", self._on_registered_store_tree_right_click)
        self.registered_store_tree.bind("<<TreeviewSelect>>", self._on_registered_store_selection_changed)

        y_scroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.registered_store_tree.yview)
        y_scroll.grid(row=2, column=1, sticky="ns")
        self.registered_store_tree.configure(yscrollcommand=y_scroll.set)

        x_scroll = ttk.Scrollbar(table_frame, orient="horizontal", command=self.registered_store_tree.xview)
        x_scroll.grid(row=3, column=0, sticky="ew")
        self.registered_store_tree.configure(xscrollcommand=x_scroll.set)

    def _load_registered_stores_on_startup(self) -> list[RegisteredStore]:
        try:
            registered_stores = self._load_latest_registered_stores()
            sync_summary = self._persist_registered_store_list(registered_stores)
            if sync_summary.has_errors:
                self.startup_store_warning = (
                    "登録店舗は読み込めましたが、Web表示用店舗索引の更新に失敗しました。\n"
                    + "\n\n".join(sync_summary.messages)
                )
            return registered_stores
        except Exception as exc:  # noqa: BLE001
            self.startup_store_warning = f"登録店舗の読込に失敗したため、初期店舗だけを表示します。\n{exc}"
            return self._default_registered_stores()

    def _default_registered_stores(self) -> list[RegisteredStore]:
        return [self._build_registered_store(DEFAULT_STORE_NAME, DEFAULT_STORE_URL)]

    def _load_latest_registered_stores(self) -> list[RegisteredStore]:
        saved_stores = self.persistence_service.load_registered_stores()
        legacy_selected_store_urls = self._load_legacy_selected_store_urls_from_settings()
        return [
            self._build_registered_store(
                store_name=store["store_name"],
                store_url=store["store_url"],
                fetch_frequency=self._saved_store_fetch_frequency(store, legacy_selected_store_urls),
                fetch_source=self._saved_store_fetch_source(store),
                fetch_order=normalize_fetch_order(store.get("fetch_order")),
                site7_enabled=bool(store.get("site7_enabled", False)),
                site7_difference_enabled=bool(store.get("site7_difference_enabled", False)),
                site7_prefecture=str(store.get("site7_prefecture", DEFAULT_SITE7_PREFECTURE_NAME)),
                site7_area=str(store.get("site7_area", "")),
                site7_store_name=str(store.get("site7_store_name", "")),
                site7_hall_id=str(store.get("site7_hall_id", "")),
                site7_address=str(store.get("site7_address", "")),
                event_day_tails=normalize_int_tuple(store.get("event_day_tails", []), 0, 9),
                event_month_days=normalize_int_tuple(store.get("event_month_days", []), 1, 31),
                event_zoro=bool(store.get("event_zoro", False)),
                event_weekdays=normalize_int_tuple(store.get("event_weekdays", []), 0, 6),
                event_source_text=str(store.get("event_source_text", "")),
            )
            for store in saved_stores
        ]

    def _load_legacy_selected_store_urls_from_settings(self) -> set[str]:
        try:
            payload = self._load_gui_settings()
        except Exception:  # noqa: BLE001
            return set()

        raw_urls = payload.get("selected_store_urls")
        if not isinstance(raw_urls, list):
            return set()

        return {
            normalize_store_url(str(raw_url).strip())
            for raw_url in raw_urls
            if str(raw_url).strip()
        }

    def _saved_store_fetch_frequency(self, store: dict[str, object], selected_store_urls: set[str]) -> str:
        raw_frequency = store.get("fetch_frequency")
        if raw_frequency:
            return normalize_fetch_frequency(raw_frequency)

        store_url = normalize_store_url(str(store.get("store_url", store.get("url", ""))).strip())
        if selected_store_urls and store_url not in selected_store_urls:
            return FETCH_FREQUENCY_LOW
        return FETCH_FREQUENCY_DAILY

    def _saved_store_fetch_source(self, store: dict[str, object]) -> str:
        raw_source = store.get("fetch_source")
        if raw_source:
            return normalize_fetch_source(raw_source)
        return FETCH_SOURCE_BOTH if bool(store.get("site7_enabled", False)) else FETCH_SOURCE_MINREPO

    def refresh_registered_stores(self) -> None:
        if self._is_general_busy():
            return

        self.register_store_status_var.set("登録店舗を更新中...")
        self._start_worker(self._worker_refresh_registered_stores, operation_kind="refresh_stores")

    def _worker_refresh_registered_stores(self) -> None:
        try:
            refresh_result = self._load_and_complete_registered_stores()
            self.result_queue.put(("refresh_registered_stores_success", refresh_result))
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("refresh_registered_stores_error", exc))

    def delete_registered_stores(self) -> None:
        if self._is_general_busy():
            return

        target_stores = self._selected_registered_store_rows()
        if not target_stores:
            messagebox.showwarning("入力不足", "削除する店舗を一覧から選んでください。")
            return

        if not self._confirm_registered_store_deletion(target_stores):
            return

        self.register_store_status_var.set("登録店舗を削除中...")
        self._start_worker(
            self._worker_delete_registered_stores,
            [registered_store.url for registered_store in target_stores],
            operation_kind="delete_stores",
        )

    def _worker_delete_registered_stores(self, store_urls: list[str]) -> None:
        try:
            deleted_store_count = self.persistence_service.delete_registered_stores(store_urls)
            registered_stores = self._load_latest_registered_stores()
            self.result_queue.put(
                (
                    "delete_registered_stores_success",
                    StoreDeleteResult(
                        registered_stores=registered_stores,
                        deleted_store_count=deleted_store_count,
                    ),
                )
            )
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("delete_registered_stores_error", exc))

    def _worker_scheduled_fetch(
        self,
        target_date_input: str,
        retry_delay_seconds: int,
        fetch_parallel_options: MinRepoFetchParallelOptions,
        web_publish_options: WebPublishOptions,
        selected_store_urls: set[str],
        supplemental_store_last_run_dates: dict[str, str],
        supplemental_interval_days: int,
        supplemental_run_date: str,
    ) -> None:
        try:
            refresh_result = self._load_and_complete_registered_stores()
            self._raise_if_fetch_cancelled()
            target_stores, supplemental_store_urls = self._scheduled_minrepo_registered_stores(
                refresh_result.registered_stores,
                selected_store_urls=selected_store_urls,
                supplemental_store_last_run_dates=supplemental_store_last_run_dates,
                supplemental_interval_days=supplemental_interval_days,
            )
            if not target_stores:
                raise ScraperError("定期実行の対象店舗がありません。")
            fetch_many_result = self._run_fetch_many(
                target_stores,
                target_date_input,
                retry_delay_seconds,
                fetch_parallel_options,
                web_publish_options,
                preserve_order=True,
            )
            if fetch_many_result.cancelled and not fetch_many_result.results:
                self.result_queue.put(("fetch_cancelled", None))
                return
            self.result_queue.put(
                (
                    "scheduled_fetch_many_success",
                    ScheduledFetchResult(
                        refresh_result=refresh_result,
                        fetch_many_result=fetch_many_result,
                        supplemental_store_urls=supplemental_store_urls,
                        run_date=supplemental_run_date,
                    ),
                )
            )
        except FetchCancelled:
            self.result_queue.put(("fetch_cancelled", None))
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("fetch_error", exc))

    def _worker_scheduled_site7_fetch(
        self,
        recent_days: int,
        retry_delay_seconds: int,
        browser_visible: bool,
        fetch_parallel_options: MinRepoFetchParallelOptions,
        web_publish_options: WebPublishOptions,
        scheduled_hour: int,
        store_last_run_dates: dict[str, str],
        run_date: str,
        target_store_urls: set[str] | None = None,
        waiting_started_at: datetime | None = None,
        skip_juggler_graph_differences: bool = DEFAULT_SITE7_SKIP_JUGGLER_DIFFERENCE,
    ) -> None:
        try:
            registered_stores = self._load_latest_registered_stores()
            self._raise_if_fetch_cancelled()
            target_stores, store_run_urls = self._scheduled_site7_registered_stores(
                registered_stores,
                scheduled_hour=scheduled_hour,
                store_last_run_dates=store_last_run_dates,
                run_date=run_date,
                target_store_urls=target_store_urls,
            )
            if not target_stores:
                self.result_queue.put(("scheduled_site7_fetch_skipped", registered_stores))
                return
            if not self._site7_has_enabled_target_machines(target_stores):
                self.result_queue.put(("scheduled_site7_fetch_skipped", registered_stores))
                return

            checked_at = datetime.now(JST)
            target_stores, waiting_store_urls, site7_updated_at_by_store_url = self._filter_scheduled_site7_stores_by_update_time(
                target_stores=target_stores,
                scheduled_hour=scheduled_hour,
                checked_at=checked_at,
                browser_visible=browser_visible,
            )
            if waiting_store_urls:
                store_run_urls.difference_update(waiting_store_urls)
            if not target_stores:
                self.result_queue.put(
                    (
                        "scheduled_site7_fetch_waiting",
                        ScheduledSite7UpdateWaitingResult(
                            registered_stores=registered_stores,
                            scheduled_hour=scheduled_hour,
                            waiting_store_urls=waiting_store_urls,
                            run_date=run_date,
                            waiting_started_at=waiting_started_at or checked_at,
                        ),
                    )
                )
                return
            if not self._site7_has_enabled_target_machines(target_stores):
                self.result_queue.put(("scheduled_site7_fetch_skipped", registered_stores))
                return

            fetch_many_result = self._run_site7_fetch_many(
                target_stores=target_stores,
                recent_days=recent_days,
                retry_delay_seconds=retry_delay_seconds,
                browser_visible=browser_visible,
                fetch_parallel_options=fetch_parallel_options,
                web_publish_options=web_publish_options,
                site7_updated_at_by_store_url=site7_updated_at_by_store_url,
                now=checked_at,
                skip_juggler_graph_differences=skip_juggler_graph_differences,
            )
            if fetch_many_result.cancelled and not fetch_many_result.results:
                self.result_queue.put(("fetch_cancelled", None))
                return
            self.result_queue.put(
                (
                    "scheduled_site7_fetch_many_success",
                    ScheduledSite7FetchResult(
                        registered_stores=registered_stores,
                        fetch_many_result=fetch_many_result,
                        store_run_urls=store_run_urls,
                        run_date=run_date,
                        scheduled_hour=scheduled_hour,
                        waiting_store_urls=waiting_store_urls,
                        waiting_started_at=waiting_started_at or checked_at,
                    ),
                )
            )
        except FetchCancelled:
            self.result_queue.put(("fetch_cancelled", None))
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("fetch_error", exc))

    def _filter_scheduled_site7_stores_by_update_time(
        self,
        *,
        target_stores: list[RegisteredStore],
        scheduled_hour: int,
        checked_at: datetime,
        browser_visible: bool,
    ) -> tuple[list[RegisteredStore], set[str], dict[str, datetime]]:
        waiting_store_urls: set[str] = set()
        updated_at_by_store_url: dict[str, datetime] = {}
        for registered_store in target_stores:
            self._raise_if_fetch_cancelled()
            try:
                updated_at = self.site7_scraper.fetch_mobile_hall_updated_datetime(
                    target_store=registered_store.to_site7_target_store(),
                    browser_visible=browser_visible,
                    cancel_requested=self._cancel_requested,
                )
            except Site7FetchCancelled as exc:
                raise FetchCancelled from exc
            except Exception:
                return list(target_stores), set(), updated_at_by_store_url

            normalized_url = normalize_store_url(registered_store.url)
            if normalized_url:
                updated_at_by_store_url[normalized_url] = updated_at
            if site7_update_satisfies_scheduled_hour(updated_at, scheduled_hour, checked_at):
                return list(target_stores), set(), updated_at_by_store_url
            else:
                if normalized_url:
                    waiting_store_urls.add(normalized_url)

        return [], waiting_store_urls, updated_at_by_store_url

    def _load_and_complete_registered_stores(self) -> StoreRefreshResult:
        registered_stores = self._load_latest_registered_stores()
        completed_stores: list[RegisteredStore] = []
        save_summary: RegisteredStoresPersistenceSummary | None = None
        changed = False
        messages: list[str] = []

        for registered_store in registered_stores:
            if registered_store.name.strip():
                completed_stores.append(registered_store)
                continue

            try:
                registration_info = self.scraper.fetch_store_registration_info(registered_store.url)
            except Exception as exc:  # noqa: BLE001
                messages.append(f"{registered_store.url} の店舗名取得に失敗しました。\n{exc}")
                completed_stores.append(registered_store)
                continue

            completed_stores.append(
                self._build_registered_store(
                    store_name=registration_info.store_name,
                    store_url=registered_store.url,
                    fetch_frequency=registered_store.fetch_frequency,
                    fetch_source=registered_store.fetch_source,
                    fetch_order=registered_store.fetch_order,
                    site7_enabled=registered_store.site7_enabled,
                    site7_difference_enabled=registered_store.site7_difference_enabled,
                    site7_prefecture=registered_store.site7_prefecture or registration_info.prefecture_name,
                    site7_area=registered_store.site7_area or registration_info.area_name,
                    site7_store_name=registered_store.site7_store_name,
                    site7_hall_id=registered_store.site7_hall_id,
                    site7_address=registered_store.site7_address,
                    event_day_tails=tuple(registration_info.event_settings.day_tails),
                    event_month_days=tuple(registration_info.event_settings.month_days),
                    event_zoro=registration_info.event_settings.zoro,
                    event_weekdays=tuple(registration_info.event_settings.weekdays),
                    event_source_text=registration_info.event_settings.source_text,
                )
            )
            changed = True

        if changed:
            save_summary = self._persist_registered_store_list(completed_stores)
        else:
            save_summary = self._sync_registered_store_web_data(completed_stores)

        if save_summary is not None and messages:
            save_summary.messages.extend(messages)

        return StoreRefreshResult(registered_stores=completed_stores, save_summary=save_summary)

    def register_store(self) -> None:
        if self._is_general_busy():
            return

        try:
            (
                store_url,
                fetch_frequency,
                fetch_source,
                fetch_order,
                site7_enabled,
                site7_difference_enabled,
                site7_prefecture,
                site7_area,
                site7_store_name,
                site7_hall_id,
                site7_address,
            ) = self._validated_register_store_form_input()
        except ScraperError as exc:
            self._show_error(exc)
            return

        normalized_url = normalize_store_url(store_url)
        for registered_store in self.registered_stores:
            if normalize_store_url(registered_store.url) == normalized_url:
                messagebox.showwarning("重複", "同じURLがすでに登録されています。")
                return

        self.register_store_status_var.set("店舗情報を取得中...")
        self._start_worker(
            self._worker_register_store,
            store_url,
            fetch_frequency,
            fetch_source,
            fetch_order,
            site7_enabled,
            site7_difference_enabled,
            site7_prefecture,
            site7_area,
            site7_store_name,
            site7_hall_id,
            site7_address,
        )

    def update_registered_store(self) -> None:
        if self._is_general_busy():
            return

        target_stores = self._selected_registered_store_rows()
        if len(target_stores) != 1:
            messagebox.showwarning("入力不足", "更新する店舗を一覧から1つだけ選んでください。")
            return

        try:
            (
                store_url,
                fetch_frequency,
                fetch_source,
                fetch_order,
                site7_enabled,
                site7_difference_enabled,
                site7_prefecture,
                site7_area,
                site7_store_name,
                site7_hall_id,
                site7_address,
            ) = self._validated_register_store_form_input()
        except ScraperError as exc:
            self._show_error(exc)
            return

        target_store = target_stores[0]
        self.register_store_status_var.set("更新先URLの店舗情報を取得中...")
        self._start_worker(
            self._worker_update_registered_store,
            target_store.url,
            store_url,
            fetch_frequency,
            fetch_source,
            fetch_order,
            site7_enabled,
            site7_difference_enabled,
            site7_prefecture,
            site7_area,
            site7_store_name,
            site7_hall_id,
            site7_address,
        )

    def clear_register_store_form(self) -> None:
        self.register_store_url_var.set("")
        self.register_store_frequency_var.set(FETCH_FREQUENCY_DAILY)
        self.register_store_source_var.set(FETCH_SOURCE_MINREPO)
        self.register_store_order_var.set("")
        self.register_store_site7_enabled_var.set(False)
        self.register_store_site7_difference_enabled_var.set(False)
        self.register_store_prefecture_var.set(DEFAULT_SITE7_PREFECTURE_NAME)
        self.register_store_area_var.set("")
        self.register_store_site7_store_name_var.set("")
        self.register_store_site7_hall_id_var.set("")
        self.register_store_site7_address_var.set("")
        if hasattr(self, "registered_store_tree"):
            selected_items = self.registered_store_tree.selection()
            if selected_items:
                self.registered_store_tree.selection_remove(*selected_items)
        self.register_store_status_var.set("入力欄をクリアしました")
        self._update_button_states()

    def _worker_register_store(
        self,
        store_url: str,
        fetch_frequency: str,
        fetch_source: str,
        fetch_order: int | None,
        site7_enabled: bool,
        site7_difference_enabled: bool,
        site7_prefecture: str,
        site7_area: str,
        site7_store_name: str,
        site7_hall_id: str,
        site7_address: str,
    ) -> None:
        try:
            registration_info = self.scraper.fetch_store_registration_info(store_url)
            event_settings = getattr(registration_info, "event_settings", StoreEventSettings())
            resolved_site7_prefecture, resolved_site7_area = self._resolve_store_region_input(
                site7_enabled=site7_enabled,
                site7_prefecture=site7_prefecture,
                site7_area=site7_area,
                fetched_prefecture=registration_info.prefecture_name,
                fetched_area=registration_info.area_name,
            )
            self.result_queue.put(
                (
                    "register_store_success",
                    (
                        registration_info.store_name,
                        store_url,
                        fetch_frequency,
                        fetch_source,
                        fetch_order,
                        site7_enabled,
                        site7_difference_enabled,
                        resolved_site7_prefecture,
                        resolved_site7_area,
                        site7_store_name,
                        site7_hall_id,
                        site7_address,
                        event_settings,
                    ),
                )
            )
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("register_store_error", exc))

    def _worker_update_registered_store(
        self,
        original_store_url: str,
        store_url: str,
        fetch_frequency: str,
        fetch_source: str,
        fetch_order: int | None,
        site7_enabled: bool,
        site7_difference_enabled: bool,
        site7_prefecture: str,
        site7_area: str,
        site7_store_name: str,
        site7_hall_id: str,
        site7_address: str,
    ) -> None:
        try:
            registration_info = self.scraper.fetch_store_registration_info(store_url)
            event_settings = getattr(registration_info, "event_settings", StoreEventSettings())
            resolved_site7_prefecture, resolved_site7_area = self._resolve_store_region_input(
                site7_enabled=site7_enabled,
                site7_prefecture=site7_prefecture,
                site7_area=site7_area,
                fetched_prefecture=registration_info.prefecture_name,
                fetched_area=registration_info.area_name,
            )
            self.result_queue.put(
                (
                    "update_registered_store_success",
                    (
                        original_store_url,
                        registration_info.store_name,
                        store_url,
                        fetch_frequency,
                        fetch_source,
                        fetch_order,
                        site7_enabled,
                        site7_difference_enabled,
                        resolved_site7_prefecture,
                        resolved_site7_area,
                        site7_store_name,
                        site7_hall_id,
                        site7_address,
                        event_settings,
                    ),
                )
            )
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("update_registered_store_error", exc))

    def fetch_site7_data(self) -> None:
        if self._site7_start_blocked():
            return

        try:
            recent_days = parse_recent_days(self.target_date_var.get())
            retry_delay_seconds = self._retry_delay_seconds_input()
            fetch_parallel_options = self._minrepo_fetch_parallel_options()
            web_publish_options = self._web_publish_options_input()
        except ScraperError as exc:
            self._show_error(exc)
            return

        recent_days = clamp_site7_recent_days(recent_days)

        try:
            target_stores = self._selected_site7_registered_stores()
        except ScraperError as exc:
            self._show_error(exc)
            return
        if not target_stores:
            messagebox.showwarning("入力不足", "登録店舗タブで取得元にサイセを含む店舗を1つ以上用意してください。")
            return
        if self._site7_fetch_requires_login(target_stores) and not self.site7_scraper.has_saved_login_state():
            if messagebox.askyesno(
                "サイトセブン",
                "サイトセブンのログイン情報がまだありません。\n先にログイン画面を開きますか？",
            ):
                self.site7_login()
            return
        if not self._site7_has_enabled_target_machines(target_stores):
            messagebox.showwarning("入力不足", "対象店舗の取得元に対応するサイトセブン取得機種を1つ以上選択してください。")
            return

        self._begin_fetch_run(
            progress_message="サイトセブンへ接続中...",
            status_message="サイトセブン取得中...",
            summary_message=f"{len(target_stores)}店舗の対象機種をサイトセブンから取得中",
            progress_kind=PROGRESS_KIND_SITE7,
        )
        browser_visible = self._site7_browser_visible()
        skip_juggler_graph_differences = self._site7_skip_juggler_difference_enabled()
        self._start_worker(
            self._worker_fetch_site7,
            target_stores,
            recent_days,
            retry_delay_seconds,
            browser_visible,
            fetch_parallel_options,
            web_publish_options,
            None,
            True,
            False,
            skip_juggler_graph_differences,
            operation_kind="site7_fetch",
        )

    def fetch_site7_neo_im_data(self) -> None:
        if self._site7_start_blocked():
            return

        try:
            recent_days = parse_recent_days(self.target_date_var.get())
            retry_delay_seconds = self._retry_delay_seconds_input()
            fetch_parallel_options = self._minrepo_fetch_parallel_options()
            web_publish_options = self._web_publish_options_input()
        except ScraperError as exc:
            self._show_error(exc)
            return

        recent_days = clamp_site7_recent_days(recent_days)

        try:
            target_stores = self._selected_site7_registered_stores()
        except ScraperError as exc:
            self._show_error(exc)
            return
        if not target_stores:
            messagebox.showwarning("入力不足", "登録店舗タブで取得元にサイセを含む店舗を1つ以上用意してください。")
            return
        if self._site7_fetch_requires_login(target_stores) and not self.site7_scraper.has_saved_login_state():
            if messagebox.askyesno(
                "サイトセブン",
                "サイトセブンのログイン情報がまだありません。\n先にログイン画面を開きますか？",
            ):
                self.site7_login()
            return

        self._begin_fetch_run(
            progress_message="サイトセブンへ接続中...",
            status_message="サイトセブン取得中...",
            summary_message=f"{len(target_stores)}店舗のネオアイムをサイトセブンから取得中",
            progress_kind=PROGRESS_KIND_SITE7,
        )
        browser_visible = self._site7_browser_visible()
        skip_juggler_graph_differences = self._site7_skip_juggler_difference_enabled()
        self._start_worker(
            self._worker_fetch_site7,
            target_stores,
            recent_days,
            retry_delay_seconds,
            browser_visible,
            fetch_parallel_options,
            web_publish_options,
            {SITE7_NEO_IM_MACHINE_NAME},
            False,
            False,
            skip_juggler_graph_differences,
            operation_kind="site7_fetch",
        )

    def fetch_registered_store_site7_data(self, registered_store: RegisteredStore) -> None:
        if self._site7_start_blocked():
            return

        try:
            recent_days = parse_recent_days(self.target_date_var.get())
            retry_delay_seconds = self._retry_delay_seconds_input()
            fetch_parallel_options = self._minrepo_fetch_parallel_options()
            web_publish_options = self._web_publish_options_input()
            target_store = self._site7_registered_store_for_single_fetch(registered_store)
        except ScraperError as exc:
            self._show_error(exc)
            return

        recent_days = clamp_site7_recent_days(recent_days)

        if self._site7_fetch_requires_login([target_store]) and not self.site7_scraper.has_saved_login_state():
            if messagebox.askyesno(
                "サイトセブン",
                "サイトセブンのログイン情報がまだありません。\n先にログイン画面を開きますか？",
            ):
                self.site7_login()
            return
        if not self._site7_has_enabled_target_machines([target_store]):
            messagebox.showwarning("入力不足", "対象店舗の取得元に対応するサイトセブン取得機種を1つ以上選択してください。")
            return

        display_name = self._registered_store_display_name(target_store)
        self._begin_fetch_run(
            progress_message="サイトセブンへ接続中...",
            status_message="サイトセブン取得中...",
            summary_message=f"{display_name} をサイトセブンから取得中",
            progress_kind=PROGRESS_KIND_SITE7,
        )
        browser_visible = self._site7_browser_visible()
        skip_juggler_graph_differences = self._site7_skip_juggler_difference_enabled()
        self._start_worker(
            self._worker_fetch_site7,
            [target_store],
            recent_days,
            retry_delay_seconds,
            browser_visible,
            fetch_parallel_options,
            web_publish_options,
            None,
            True,
            False,
            skip_juggler_graph_differences,
            operation_kind="site7_fetch",
        )

    def fetch_registered_store_site7_neo_im_data(self, registered_store: RegisteredStore) -> None:
        if self._site7_start_blocked():
            return

        try:
            recent_days = parse_recent_days(self.target_date_var.get())
            retry_delay_seconds = self._retry_delay_seconds_input()
            fetch_parallel_options = self._minrepo_fetch_parallel_options()
            web_publish_options = self._web_publish_options_input()
            target_store = self._site7_registered_store_for_single_fetch(
                registered_store,
                require_site7_source=True,
            )
        except ScraperError as exc:
            self._show_error(exc)
            return

        recent_days = clamp_site7_recent_days(recent_days)

        if self._site7_fetch_requires_login([target_store]) and not self.site7_scraper.has_saved_login_state():
            if messagebox.askyesno(
                "サイトセブン",
                "サイトセブンのログイン情報がまだありません。\n先にログイン画面を開きますか？",
            ):
                self.site7_login()
            return

        display_name = self._registered_store_display_name(target_store)
        self._begin_fetch_run(
            progress_message="サイトセブンへ接続中...",
            status_message="サイトセブン取得中...",
            summary_message=f"{display_name} のネオアイムをサイトセブンから取得中",
            progress_kind=PROGRESS_KIND_SITE7,
        )
        browser_visible = self._site7_browser_visible()
        skip_juggler_graph_differences = self._site7_skip_juggler_difference_enabled()
        self._start_worker(
            self._worker_fetch_site7,
            [target_store],
            recent_days,
            retry_delay_seconds,
            browser_visible,
            fetch_parallel_options,
            web_publish_options,
            {SITE7_NEO_IM_MACHINE_NAME},
            False,
            False,
            skip_juggler_graph_differences,
            operation_kind="site7_fetch",
        )

    def _worker_fetch_site7(
        self,
        target_stores: list[RegisteredStore],
        recent_days: int,
        retry_delay_seconds: int,
        browser_visible: bool,
        fetch_parallel_options: MinRepoFetchParallelOptions,
        web_publish_options: WebPublishOptions,
        enabled_machine_names: set[str] | None = None,
        minrepo_prefetch_enabled: bool = True,
        force_site7_difference: bool = False,
        skip_juggler_graph_differences: bool = DEFAULT_SITE7_SKIP_JUGGLER_DIFFERENCE,
    ) -> None:
        try:
            fetch_many_result = self._run_site7_fetch_many(
                target_stores=target_stores,
                recent_days=recent_days,
                retry_delay_seconds=retry_delay_seconds,
                browser_visible=browser_visible,
                fetch_parallel_options=fetch_parallel_options,
                web_publish_options=web_publish_options,
                enabled_machine_names=enabled_machine_names,
                minrepo_prefetch_enabled=minrepo_prefetch_enabled,
                force_site7_difference=force_site7_difference,
                skip_juggler_graph_differences=skip_juggler_graph_differences,
            )
            if fetch_many_result.cancelled and not fetch_many_result.results:
                self.result_queue.put(("fetch_cancelled", None))
                return
            self.result_queue.put(("fetch_many_success", fetch_many_result))
        except FetchCancelled:
            self.result_queue.put(("fetch_cancelled", None))
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("fetch_error", exc))

    def _run_site7_fetch_many(
        self,
        target_stores: list[RegisteredStore],
        recent_days: int,
        retry_delay_seconds: int,
        browser_visible: bool,
        fetch_parallel_options: MinRepoFetchParallelOptions | None = None,
        web_publish_options: WebPublishOptions | None = None,
        site7_updated_at_by_store_url: dict[str, datetime] | None = None,
        now: datetime | None = None,
        enabled_machine_names: set[str] | None = None,
        minrepo_prefetch_enabled: bool = True,
        force_site7_difference: bool = False,
        skip_juggler_graph_differences: bool = DEFAULT_SITE7_SKIP_JUGGLER_DIFFERENCE,
    ) -> FetchManyResult:
        fetch_parallel_options = fetch_parallel_options or MINREPO_FETCH_PARALLEL_OPTIONS[MINREPO_FETCH_MODE_NORMAL]
        web_publish_options = web_publish_options or WebPublishOptions(mode=WEB_PUBLISH_MODE_DAYS)
        site7_updated_at_by_store_url = site7_updated_at_by_store_url or {}
        results: list[StoreFetchResult] = []
        failures: list[StoreFetchFailure] = []
        target_stores = self._registered_store_fetch_ordered(target_stores)
        total_stores = len(target_stores)
        cancelled = False
        save_executor = (
            ThreadPoolExecutor(max_workers=1, thread_name_prefix="r2-save")
            if total_stores > 1
            else None
        )

        try:
            for store_index, registered_store in enumerate(target_stores, start=1):
                if self._cancel_requested():
                    cancelled = True
                    break

                try:
                    normalized_store_url = normalize_store_url(registered_store.url)
                    if minrepo_prefetch_enabled:
                        minrepo_prefetch_result, site7_should_be_skipped = self._try_minrepo_before_site7_fetch(
                            registered_store=registered_store,
                            recent_days=recent_days,
                            store_index=store_index,
                            total_stores=total_stores,
                            retry_delay_seconds=retry_delay_seconds,
                            fetch_parallel_options=fetch_parallel_options,
                            web_publish_options=web_publish_options,
                            site7_updated_at=site7_updated_at_by_store_url.get(normalized_store_url),
                            now=now,
                        )
                        if minrepo_prefetch_result is not None:
                            self._refresh_web_data_for_store_result(minrepo_prefetch_result)
                            if site7_should_be_skipped:
                                results.append(minrepo_prefetch_result)
                                continue

                    store_result = self._fetch_single_site7_store(
                        registered_store=registered_store,
                        recent_days=recent_days,
                        store_index=store_index,
                        total_stores=total_stores,
                        retry_delay_seconds=retry_delay_seconds,
                        browser_visible=browser_visible,
                        enabled_machine_names=enabled_machine_names,
                        force_site7_difference=force_site7_difference,
                        skip_juggler_graph_differences=skip_juggler_graph_differences,
                        async_save_executor=save_executor,
                    )
                    self._refresh_web_data_for_store_result(store_result)
                    results.append(store_result)
                except FetchCancelled:
                    cancelled = True
                    break
                except Exception as exc:  # noqa: BLE001
                    failures.append(StoreFetchFailure(store=registered_store, error=exc))
                    self._queue_fetch_progress(
                        FetchProgress(
                            current_step=1,
                            total_steps=1,
                            message=f"{store_index}/{total_stores} {registered_store.name} は取得失敗",
                        ),
                        store_index=store_index,
                        total_stores=total_stores,
                    )
        finally:
            self._wait_for_pending_store_saves(results)
            if save_executor is not None:
                save_executor.shutdown(wait=True)

        if self._cancel_requested():
            cancelled = True

        if not results and failures:
            failure_lines = "\n".join(f"{failure.store.name}: {failure.error}" for failure in failures)
            raise ScraperError(f"サイトセブンの対象店舗を取得できませんでした。\n{failure_lines}")

        return FetchManyResult(results=results, failures=failures, cancelled=cancelled)

    def _try_minrepo_before_site7_fetch(
        self,
        *,
        registered_store: RegisteredStore,
        recent_days: int,
        store_index: int,
        total_stores: int,
        retry_delay_seconds: int,
        fetch_parallel_options: MinRepoFetchParallelOptions,
        web_publish_options: WebPublishOptions,
        site7_updated_at: datetime | None = None,
        now: datetime | None = None,
    ) -> tuple[StoreFetchResult | None, bool]:
        self._raise_if_fetch_cancelled()
        fallback_dates = minrepo_fallback_date_texts_for_site7(
            registered_store.fetch_source,
            recent_days,
            now=now,
            site7_updated_at=site7_updated_at,
        )
        if not fallback_dates:
            return None, False

        requested_dates = set(fallback_dates)
        site7_latest_date = (
            site7_business_date_from_updated_at(site7_updated_at)
            if site7_updated_at is not None
            else (now or datetime.now(JST)).astimezone(JST).date()
        )
        site7_target_dates = set(site7_target_date_texts(recent_days, latest_date=site7_latest_date))
        target_date_input = date_range_input_from_date_texts(fallback_dates)
        try:
            minrepo_result = self._fetch_single_store(
                registered_store=registered_store,
                target_date_input=target_date_input,
                store_index=store_index,
                total_stores=total_stores,
                retry_delay_seconds=retry_delay_seconds,
                fetch_parallel_options=fetch_parallel_options,
                web_publish_options=web_publish_options,
                required_target_dates=requested_dates,
            )
        except FetchCancelled:
            raise
        except Exception:
            return None, False

        successful_dates = self._successful_minrepo_dates_for_site7_fallback(
            minrepo_result,
            requested_dates=requested_dates,
        )
        if not successful_dates:
            return minrepo_result, False

        return minrepo_result, site7_target_dates.issubset(successful_dates)

    def _successful_minrepo_dates_for_site7_fallback(
        self,
        store_result: StoreFetchResult,
        *,
        requested_dates: set[str],
    ) -> set[str]:
        successful_dates = set(store_result.saved_full_day_summary.saved_dates).intersection(requested_dates)
        save_summary = store_result.save_summary
        if save_summary is not None and save_summary.web_data_saved:
            successful_dates.update(
                dataset.target_date
                for dataset in store_result.history_result.datasets
                if dataset.target_date in requested_dates and dataset.rows
            )
        return successful_dates

    def _fetch_single_daidata_online_store(
        self,
        registered_store: RegisteredStore,
        recent_days: int,
        store_index: int,
        total_stores: int,
        retry_delay_seconds: int,
        browser_visible: bool,
        enabled_machine_names: set[str] | None = None,
        async_save_executor: ThreadPoolExecutor | None = None,
    ) -> StoreFetchResult:
        self._raise_if_fetch_cancelled()
        store_label = f"{store_index}/{total_stores} {registered_store.name}"
        daidata_store_config = daidata_store_config_for(registered_store.name, registered_store.url)
        if daidata_store_config is None:
            raise ScraperError(f"{registered_store.name} は台データオンライン取得の対象店舗ではありません。")

        def queue_progress(progress: FetchProgress) -> None:
            self._queue_fetch_progress(progress, store_index=store_index, total_stores=total_stores)

        fetch_enabled_machine_names = enabled_machine_names
        if fetch_enabled_machine_names is None:
            fetch_enabled_machine_names = self._site7_enabled_machine_names_for_fetch(registered_store)
        if fetch_enabled_machine_names == set():
            fetch_enabled_machine_names = None

        saved_lookup_store_cache: tuple[str, str] | None = None
        minrepo_saved_dates_cache: dict[tuple[str, str], set[str]] = {}
        store_closed_dates_cache: dict[tuple[str, str], set[str]] = {}
        warning_summary = SavedFullDayDatesSummary()

        def saved_lookup_store() -> tuple[str, str]:
            nonlocal saved_lookup_store_cache
            if saved_lookup_store_cache is not None:
                return saved_lookup_store_cache

            lookup_store_name = registered_store.name
            lookup_store_url = registered_store.url
            preferred_store = self.persistence_service.resolve_preferred_store_by_name(lookup_store_name)
            if preferred_store is not None:
                preferred_store_name = str(preferred_store.get("store_name", "")).strip()
                preferred_store_url = str(preferred_store.get("store_url", "")).strip()
                if preferred_store_name and preferred_store_url:
                    lookup_store_name = preferred_store_name
                    lookup_store_url = preferred_store_url

            saved_lookup_store_cache = (lookup_store_name, lookup_store_url)
            return saved_lookup_store_cache

        def minrepo_saved_full_day_dates(target_dates: list[str]) -> set[str]:
            nonlocal warning_summary
            normalized_dates = [
                target_date
                for target_date in target_dates
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(target_date or ""))
            ]
            if not normalized_dates:
                return set()

            start_date = min(normalized_dates)
            end_date = max(normalized_dates)
            cache_key = (start_date, end_date)
            cached_dates = minrepo_saved_dates_cache.get(cache_key)
            if cached_dates is not None:
                return cached_dates

            lookup_store_name, lookup_store_url = saved_lookup_store()
            saved_full_day_summary = self.persistence_service.find_saved_full_day_dates(
                store_name=lookup_store_name,
                store_url=lookup_store_url,
                start_date=start_date,
                end_date=end_date,
            )
            warning_summary.messages.extend(saved_full_day_summary.messages)
            saved_dates = set(saved_full_day_summary.saved_dates)
            minrepo_saved_dates_cache[cache_key] = saved_dates
            return saved_dates

        def saved_store_closed_dates(target_dates: list[str]) -> set[str]:
            normalized_dates = [
                target_date
                for target_date in target_dates
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(target_date or ""))
            ]
            if not normalized_dates:
                return set()

            start_date = min(normalized_dates)
            end_date = max(normalized_dates)
            cache_key = (start_date, end_date)
            cached_dates = store_closed_dates_cache.get(cache_key)
            if cached_dates is not None:
                return cached_dates

            lookup_store_name, lookup_store_url = saved_lookup_store()
            find_closed_dates = getattr(self.persistence_service, "find_store_closed_dates", None)
            closed_dates = (
                set(
                    find_closed_dates(
                        store_name=lookup_store_name,
                        store_url=lookup_store_url,
                        start_date=start_date,
                        end_date=end_date,
                    )
                )
                if callable(find_closed_dates)
                else set()
            )
            store_closed_dates_cache[cache_key] = closed_dates
            return closed_dates

        def find_protected_slots_before_fetch(
            machine_entry: DaidataOnlineMachineEntry,
            target_dates: list[str],
            slot_numbers: list[str],
            daidata_updated_at: str | None = None,
        ) -> set[tuple[str, str]]:
            nonlocal warning_summary
            self._raise_if_fetch_cancelled()
            if not target_dates or not slot_numbers:
                return set()

            target_date_set = set(target_dates)
            minrepo_saved_dates = minrepo_saved_full_day_dates(target_dates).intersection(target_date_set)
            store_closed_dates = saved_store_closed_dates(target_dates).intersection(target_date_set)
            fully_protected_dates = minrepo_saved_dates | store_closed_dates
            protected_slots = {
                (target_date, slot_number)
                for target_date in fully_protected_dates
                for slot_number in slot_numbers
            }
            remaining_dates = [
                target_date
                for target_date in target_dates
                if target_date not in fully_protected_dates
            ]
            if not remaining_dates:
                return protected_slots

            lookup_store_name, lookup_store_url = saved_lookup_store()
            saved_slots_summary = self.persistence_service.find_saved_machine_slots(
                store_name=lookup_store_name,
                store_url=lookup_store_url,
                start_date=min(remaining_dates),
                end_date=max(remaining_dates),
                slot_numbers=slot_numbers,
                require_source_difference=False,
                site7_updated_at=daidata_updated_at,
            )
            warning_summary.messages.extend(saved_slots_summary.messages)
            remaining_date_set = set(remaining_dates)
            protected_slots.update(
                (target_date, slot_number)
                for target_date, slot_number in saved_slots_summary.protected_slots
                if target_date in remaining_date_set
            )
            return protected_slots

        def run_daidata_fetch() -> MachineHistoryResult:
            return self.daidata_online_scraper.fetch_store_juggler_history(
                store_config=daidata_store_config,
                recent_days=recent_days,
                browser_visible=browser_visible,
                progress_callback=lambda progress: queue_progress(
                    FetchProgress(
                        current_step=progress.current_step,
                        total_steps=progress.total_steps,
                        message=f"{store_label}: {progress.message}",
                    )
                ),
                enabled_machine_names=fetch_enabled_machine_names,
                machine_protected_slots_callback=find_protected_slots_before_fetch,
                cancel_requested=self._cancel_requested,
            )

        history_result = self._run_with_fetch_retries(
            run_daidata_fetch,
            retry_delay_seconds=retry_delay_seconds,
            retry_status_callback=lambda retry_number, max_retries, delay_seconds: queue_progress(
                FetchProgress(
                    current_step=0,
                    total_steps=4,
                    message=(
                        f"{store_label}: 台データオンライン取得に失敗しました。"
                        f"{delay_seconds}秒後に再試行します（{retry_number}/{max_retries}）"
                    ),
                ),
            ),
        )
        self._raise_if_fetch_cancelled()
        history_result = rewrite_history_result_store(
            history_result,
            store_name=registered_store.name,
            store_url=registered_store.url,
        )
        prefetch_warning_summary = warning_summary
        history_result, post_filter_warning_summary = self._prepare_site7_history_result_for_save(
            history_result,
            require_source_difference=False,
        )
        prefetch_warning_summary.messages.extend(post_filter_warning_summary.messages)
        warning_summary = prefetch_warning_summary
        self._raise_if_fetch_cancelled()
        save_summary: PersistenceSummary | None = None
        pending_save_futures: list[Future[PersistenceSummary]] = []
        if history_result.datasets or history_result.store_day_statuses:
            queue_progress(
                FetchProgress(
                    current_step=3,
                    total_steps=4,
                    message=(
                        f"{store_label}: R2保存を開始"
                        if async_save_executor is not None
                        else f"{store_label}: 保存中"
                    ),
                )
            )

            def run_save() -> PersistenceSummary:
                return self._run_with_persistence_lock(
                    lambda: (
                        self.persistence_service.save_history_result(history_result)
                        if history_result.datasets
                        else self.persistence_service.save_store_day_statuses(history_result)
                    )
                )

            if async_save_executor is not None:
                pending_save_futures.append(async_save_executor.submit(run_save))
            else:
                save_summary = run_save()
        return StoreFetchResult(
            history_result=history_result,
            save_summary=save_summary,
            saved_full_day_summary=warning_summary,
            pending_save_futures=pending_save_futures,
        )

    def _fetch_single_site7_store(
        self,
        registered_store: RegisteredStore,
        recent_days: int,
        store_index: int,
        total_stores: int,
        retry_delay_seconds: int,
        browser_visible: bool,
        enabled_machine_names: set[str] | None = None,
        force_site7_difference: bool = False,
        skip_juggler_graph_differences: bool = DEFAULT_SITE7_SKIP_JUGGLER_DIFFERENCE,
        async_save_executor: ThreadPoolExecutor | None = None,
    ) -> StoreFetchResult:
        self._raise_if_fetch_cancelled()
        if registered_store_uses_daidata_online(registered_store):
            return self._fetch_single_daidata_online_store(
                registered_store=registered_store,
                recent_days=recent_days,
                store_index=store_index,
                total_stores=total_stores,
                retry_delay_seconds=retry_delay_seconds,
                browser_visible=browser_visible,
                enabled_machine_names=enabled_machine_names,
                async_save_executor=async_save_executor,
            )

        target_store = registered_store.to_site7_target_store()
        site7_difference_enabled = bool(
            force_site7_difference
            or (registered_store.site7_enabled and registered_store.site7_difference_enabled)
        )
        store_label = f"{store_index}/{total_stores} {registered_store.name}"

        def queue_progress(progress: FetchProgress) -> None:
            self._queue_fetch_progress(progress, store_index=store_index, total_stores=total_stores)

        saved_lookup_store_cache: tuple[str, str] | None = None
        minrepo_saved_dates_cache: dict[tuple[str, str], set[str]] = {}
        store_closed_dates_cache: dict[tuple[str, str], set[str]] = {}

        def saved_lookup_store() -> tuple[str, str]:
            nonlocal saved_lookup_store_cache
            if saved_lookup_store_cache is not None:
                return saved_lookup_store_cache

            lookup_store_name = registered_store.name
            lookup_store_url = registered_store.url
            preferred_store = self.persistence_service.resolve_preferred_store_by_name(lookup_store_name)
            if preferred_store is not None:
                preferred_store_name = str(preferred_store.get("store_name", "")).strip()
                preferred_store_url = str(preferred_store.get("store_url", "")).strip()
                if preferred_store_name and preferred_store_url:
                    lookup_store_name = preferred_store_name
                    lookup_store_url = preferred_store_url

            saved_lookup_store_cache = (lookup_store_name, lookup_store_url)
            return saved_lookup_store_cache

        def minrepo_saved_full_day_dates(target_dates: list[str]) -> set[str]:
            nonlocal warning_summary
            if not target_dates:
                return set()

            start_date = min(target_dates)
            end_date = max(target_dates)
            cache_key = (start_date, end_date)
            cached_dates = minrepo_saved_dates_cache.get(cache_key)
            if cached_dates is not None:
                return cached_dates

            lookup_store_name, lookup_store_url = saved_lookup_store()
            saved_full_day_summary = self.persistence_service.find_saved_full_day_dates(
                store_name=lookup_store_name,
                store_url=lookup_store_url,
                start_date=start_date,
                end_date=end_date,
            )
            warning_summary.messages.extend(saved_full_day_summary.messages)
            saved_dates = set(saved_full_day_summary.saved_dates)
            minrepo_saved_dates_cache[cache_key] = saved_dates
            return saved_dates

        def saved_store_closed_dates(target_dates: list[str]) -> set[str]:
            if not target_dates:
                return set()

            start_date = min(target_dates)
            end_date = max(target_dates)
            cache_key = (start_date, end_date)
            cached_dates = store_closed_dates_cache.get(cache_key)
            if cached_dates is not None:
                return cached_dates

            lookup_store_name, lookup_store_url = saved_lookup_store()
            find_closed_dates = getattr(self.persistence_service, "find_store_closed_dates", None)
            closed_dates = (
                set(
                    find_closed_dates(
                        store_name=lookup_store_name,
                        store_url=lookup_store_url,
                        start_date=start_date,
                        end_date=end_date,
                    )
                )
                if callable(find_closed_dates)
                else set()
            )
            store_closed_dates_cache[cache_key] = closed_dates
            return closed_dates

        def filter_machine_result_for_fetch(machine_result: MachineHistoryResult) -> MachineHistoryResult:
            nonlocal warning_summary
            self._raise_if_fetch_cancelled()
            partial_result = rewrite_history_result_store(
                machine_result,
                store_name=registered_store.name,
                store_url=registered_store.url,
            )
            partial_result, partial_warning_summary = self._prepare_site7_history_result_for_save(
                partial_result,
                require_source_difference=self._site7_history_result_requires_source_difference(
                    partial_result,
                    site7_difference_enabled=site7_difference_enabled,
                    skip_juggler_graph_differences=skip_juggler_graph_differences,
                ),
            )
            warning_summary.messages.extend(partial_warning_summary.messages)
            return partial_result

        def find_protected_slots_before_fetch(
            machine_entry: Site7MachineEntry,
            target_dates: list[str],
            slot_numbers: list[str],
            site7_updated_at: str | None = None,
        ) -> set[tuple[str, str]]:
            nonlocal warning_summary
            self._raise_if_fetch_cancelled()
            if not target_dates or not slot_numbers:
                return set()

            target_date_set = set(target_dates)
            minrepo_saved_dates = minrepo_saved_full_day_dates(target_dates).intersection(target_date_set)
            store_closed_dates = saved_store_closed_dates(target_dates).intersection(target_date_set)
            fully_protected_dates = minrepo_saved_dates | store_closed_dates
            protected_slots = {
                (target_date, slot_number)
                for target_date in fully_protected_dates
                for slot_number in slot_numbers
            }
            remaining_dates = [
                target_date
                for target_date in target_dates
                if target_date not in fully_protected_dates
            ]
            if not remaining_dates:
                return protected_slots

            lookup_store_name, lookup_store_url = saved_lookup_store()
            saved_slots_summary = self.persistence_service.find_saved_machine_slots(
                store_name=lookup_store_name,
                store_url=lookup_store_url,
                start_date=min(remaining_dates),
                end_date=max(remaining_dates),
                slot_numbers=slot_numbers,
                require_source_difference=self._site7_machine_requires_source_difference(
                    machine_entry.machine_name,
                    site7_difference_enabled=site7_difference_enabled,
                    skip_juggler_graph_differences=skip_juggler_graph_differences,
                ),
                site7_updated_at=site7_updated_at,
            )
            warning_summary.messages.extend(saved_slots_summary.messages)
            remaining_date_set = set(remaining_dates)
            protected_slots.update(
                (target_date, slot_number)
                for target_date, slot_number in saved_slots_summary.protected_slots
                if target_date in remaining_date_set
            )
            return protected_slots

        warning_summary = SavedFullDayDatesSummary()

        def run_site7_fetch() -> MachineHistoryResult:
            try:
                fetch_kwargs = {
                    "recent_days": recent_days,
                    "browser_visible": browser_visible,
                    "progress_callback": lambda progress: queue_progress(
                        FetchProgress(
                            current_step=progress.current_step,
                            total_steps=progress.total_steps,
                            message=f"{store_label}: {progress.message}",
                        )
                    ),
                    "target_store": target_store,
                    "cancel_requested": self._cancel_requested,
                    "machine_base_result_callback": None,
                    "machine_result_callback": None,
                    "machine_result_filter_callback": filter_machine_result_for_fetch,
                    "machine_protected_slots_callback": find_protected_slots_before_fetch,
                    "include_graph_differences": site7_difference_enabled,
                    "defer_graph_differences": site7_difference_enabled,
                }
                fetch_enabled_machine_names = enabled_machine_names
                if fetch_enabled_machine_names is None:
                    fetch_enabled_machine_names = self._site7_enabled_machine_names_for_fetch(registered_store)
                if fetch_enabled_machine_names == set():
                    source_group = site7_machine_source_group(registered_store.fetch_source)
                    raise ScraperError(
                        f"{SITE7_MACHINE_SOURCE_GROUP_TITLES[source_group]}の取得機種が未選択です。"
                    )
                if fetch_enabled_machine_names is not None:
                    fetch_kwargs["enabled_machine_names"] = fetch_enabled_machine_names
                graph_difference_machine_names = self._site7_graph_difference_machine_names_for_fetch(
                    fetch_enabled_machine_names,
                    skip_juggler_graph_differences=skip_juggler_graph_differences,
                )
                if graph_difference_machine_names is not None:
                    fetch_kwargs["graph_difference_machine_names"] = graph_difference_machine_names
                return self.site7_scraper.fetch_target_machine_history(**fetch_kwargs)
            except Site7FetchCancelled as exc:
                raise FetchCancelled from exc

        history_result = self._run_with_fetch_retries(
            run_site7_fetch,
            retry_delay_seconds=retry_delay_seconds,
            retry_status_callback=lambda retry_number, max_retries, delay_seconds: queue_progress(
                FetchProgress(
                    current_step=0,
                    total_steps=4,
                    message=(
                        f"{store_label}: サイトセブン取得に失敗しました。"
                        f"{delay_seconds}秒後に再試行します（{retry_number}/{max_retries}）"
                    ),
                ),
            ),
        )
        self._raise_if_fetch_cancelled()
        history_result = rewrite_history_result_store(
            history_result,
            store_name=registered_store.name,
            store_url=registered_store.url,
        )
        self._raise_if_fetch_cancelled()
        save_summary: PersistenceSummary | None = None
        pending_save_futures: list[Future[PersistenceSummary]] = []
        if (history_result.datasets or history_result.store_day_statuses) and save_summary is None:
            queue_progress(
                FetchProgress(
                    current_step=3,
                    total_steps=4,
                    message=(
                        f"{store_label}: R2保存を開始"
                        if async_save_executor is not None
                        else f"{store_label}: 保存中"
                    ),
                )
            )

            def run_save() -> PersistenceSummary:
                return self._run_with_persistence_lock(
                    lambda: (
                        self.persistence_service.save_history_result(history_result)
                        if history_result.datasets
                        else self.persistence_service.save_store_day_statuses(history_result)
                    )
                )

            if async_save_executor is not None:
                pending_save_futures.append(async_save_executor.submit(run_save))
            else:
                save_summary = run_save()
        return StoreFetchResult(
            history_result=history_result,
            save_summary=save_summary,
            saved_full_day_summary=warning_summary,
            pending_save_futures=pending_save_futures,
        )

    def _prepare_site7_history_result_for_save(
        self,
        history_result: MachineHistoryResult,
        require_source_difference: bool = True,
    ) -> tuple[MachineHistoryResult, SavedFullDayDatesSummary]:
        warning_messages: list[str] = []
        preferred_store = self.persistence_service.resolve_preferred_store_by_name(history_result.store_name)
        if preferred_store is not None:
            preferred_store_name = str(preferred_store.get("store_name", "")).strip()
            preferred_store_url = str(preferred_store.get("store_url", "")).strip()
            if preferred_store_name and preferred_store_url:
                history_result = rewrite_history_result_store(
                    history_result,
                    store_name=preferred_store_name,
                    store_url=preferred_store_url,
                )

        slot_keys = collect_history_result_slot_keys(history_result)
        slot_numbers = sorted({slot_number for _, slot_number in slot_keys}, key=self._slot_sort_key)
        saved_slots_summary = self.persistence_service.find_saved_machine_slots(
            store_name=history_result.store_name,
            store_url=history_result.store_url,
            start_date=history_result.start_date,
            end_date=history_result.end_date,
            slot_numbers=slot_numbers,
            require_source_difference=require_source_difference,
            site7_updated_at=site7_history_result_updated_at(history_result),
        )
        warning_messages.extend(saved_slots_summary.messages)
        history_result = filter_site7_history_result_by_saved_slots(
            history_result,
            protected_slots=saved_slots_summary.protected_slots,
        )

        return history_result, SavedFullDayDatesSummary(messages=warning_messages)

    def _ensure_operation_tracking(self) -> None:
        if not hasattr(self, "_worker_context"):
            self._worker_context = threading.local()
        if not hasattr(self, "_next_operation_id"):
            self._next_operation_id = 1
        if not hasattr(self, "active_operations"):
            self.active_operations = {}
        if not hasattr(self, "active_operation_kind"):
            self.active_operation_kind = ""
        if not hasattr(self, "is_busy"):
            self.is_busy = False
        if not hasattr(self, "result_polling_active"):
            self.result_polling_active = False
        if not hasattr(self, "minrepo_cancel_event"):
            self.minrepo_cancel_event = getattr(self, "fetch_cancel_event", threading.Event())
        if not hasattr(self, "site7_cancel_event"):
            self.site7_cancel_event = threading.Event()
        if not hasattr(self, "fetch_cancel_event"):
            self.fetch_cancel_event = self.minrepo_cancel_event
        if not hasattr(self, "persistence_lock"):
            self.persistence_lock = threading.Lock()
        if not hasattr(self, "_last_queued_fetch_progress_by_operation"):
            self._last_queued_fetch_progress_by_operation = {}
        if not hasattr(self, "_fetch_progress_bar_modes"):
            self._fetch_progress_bar_modes = {}

    def _operation_kind_for_result(self, operation_id: int | None) -> str:
        self._ensure_operation_tracking()
        if operation_id is not None:
            operation_kind = self.active_operations.get(operation_id)
            if operation_kind:
                return operation_kind
        return self.active_operation_kind

    def _progress_kind_for_operation(self, operation_kind: str) -> str:
        if operation_kind in SITE7_OPERATION_KINDS:
            return PROGRESS_KIND_SITE7
        return PROGRESS_KIND_MINREPO

    def _refresh_busy_state(self) -> None:
        self._ensure_operation_tracking()
        active_kinds = list(self.active_operations.values())
        self.is_busy = bool(active_kinds)
        if not active_kinds:
            self.active_operation_kind = ""
        elif len(active_kinds) == 1:
            self.active_operation_kind = active_kinds[0]
        else:
            self.active_operation_kind = "multiple"

    def _has_active_operations(self) -> bool:
        self._ensure_operation_tracking()
        return bool(self.active_operations) or bool(getattr(self, "is_busy", False))

    def _is_operation_kind_running(self, operation_kinds: set[str]) -> bool:
        self._ensure_operation_tracking()
        if any(operation_kind in operation_kinds for operation_kind in self.active_operations.values()):
            return True
        return self.active_operation_kind in operation_kinds

    def _is_minrepo_busy(self) -> bool:
        return self._is_operation_kind_running(MINREPO_OPERATION_KINDS)

    def _is_site7_busy(self) -> bool:
        return self._is_operation_kind_running(SITE7_OPERATION_KINDS)

    def _is_general_busy(self) -> bool:
        self._ensure_operation_tracking()
        if self.active_operations:
            return any(operation_kind not in FETCH_OPERATION_KINDS for operation_kind in self.active_operations.values())
        return bool(self.is_busy and self.active_operation_kind not in FETCH_OPERATION_KINDS)

    def _minrepo_start_blocked(self) -> bool:
        return self._is_minrepo_busy() or self._is_general_busy()

    def _site7_start_blocked(self) -> bool:
        return self._is_site7_busy() or self._is_general_busy()

    def _operation_cancel_event(self, operation_kind: str | None = None) -> threading.Event:
        self._ensure_operation_tracking()
        selected_kind = operation_kind or getattr(self._worker_context, "operation_kind", self.active_operation_kind)
        if selected_kind in SITE7_OPERATION_KINDS:
            return self.site7_cancel_event
        return self.minrepo_cancel_event

    def _cancel_requested(self) -> bool:
        return self._operation_cancel_event().is_set()

    def _run_with_persistence_lock(self, action: Callable[[], T]) -> T:
        self._ensure_operation_tracking()
        with self.persistence_lock:
            return action()

    def _pending_save_error_summary(self, exc: Exception) -> PersistenceSummary:
        return PersistenceSummary(messages=[f"R2保存の完了待ちに失敗しました。\n{exc}"])

    def _wait_for_pending_store_saves(self, store_results: list[StoreFetchResult]) -> None:
        pending_saves = [
            (store_result, future)
            for store_result in store_results
            for future in store_result.pending_save_futures
        ]
        if not pending_saves:
            return

        total_saves = len(pending_saves)
        for save_index, (store_result, future) in enumerate(pending_saves, start=1):
            self._queue_fetch_progress(
                FetchProgress(
                    current_step=save_index - 1,
                    total_steps=total_saves,
                    message=f"R2保存の完了待ち {save_index}/{total_saves}",
                )
            )
            try:
                save_summary = future.result()
            except Exception as exc:  # noqa: BLE001
                save_summary = self._pending_save_error_summary(exc)
            store_result.save_summary = self._merge_persistence_summary(
                store_result.save_summary,
                save_summary,
            )
        for store_result in store_results:
            store_result.pending_save_futures.clear()

    def _begin_fetch_run(
        self,
        *,
        progress_message: str,
        status_message: str,
        summary_message: str,
        progress_kind: str = PROGRESS_KIND_MINREPO,
    ) -> None:
        self.current_results = []
        self.current_history_result = None
        self._clear_fetch_result_details()
        self._begin_fetch_progress(progress_message, progress_kind=progress_kind)
        self.status_var.set(status_message)
        self.summary_var.set(summary_message)

    def fetch_data(self) -> None:
        if self._minrepo_start_blocked():
            return

        try:
            target_date_input = self._target_date_input_from_recent_days()
            retry_delay_seconds = self._retry_delay_seconds_input()
            fetch_parallel_options = self._minrepo_fetch_parallel_options()
            web_publish_options = self._web_publish_options_input()
        except ScraperError as exc:
            self._show_error(exc)
            return

        target_stores = self._selected_registered_stores()
        if not target_stores:
            messagebox.showwarning("入力不足", "登録店舗タブで頻度を毎日または高頻度にした店舗を1つ以上選んでください。")
            return

        self._begin_fetch_run(
            progress_message="対象期間を確認中...",
            status_message="取得中...",
            summary_message=f"{len(target_stores)}店舗を期間取得中",
        )
        self._start_worker(
            self._worker_fetch_many,
            target_stores,
            target_date_input,
            retry_delay_seconds,
            fetch_parallel_options,
            web_publish_options,
            operation_kind="fetch",
        )

    def fetch_registered_store_data(self, registered_store: RegisteredStore) -> None:
        if self._minrepo_start_blocked():
            return

        try:
            target_date_input = self._target_date_input_from_recent_days()
            retry_delay_seconds = self._retry_delay_seconds_input()
            fetch_parallel_options = self._minrepo_fetch_parallel_options()
            web_publish_options = self._web_publish_options_input()
        except ScraperError as exc:
            self._show_error(exc)
            return

        display_name = self._registered_store_display_name(registered_store)
        self._begin_fetch_run(
            progress_message="対象期間を確認中...",
            status_message="取得中...",
            summary_message=f"{display_name} をみんレポから取得中",
        )
        self._start_worker(
            self._worker_fetch_many,
            [registered_store],
            target_date_input,
            retry_delay_seconds,
            fetch_parallel_options,
            web_publish_options,
            operation_kind="fetch",
        )

    def cancel_fetch(self) -> None:
        if not self._is_minrepo_busy() or self.minrepo_cancel_event.is_set():
            return

        self.minrepo_cancel_event.set()
        self.status_var.set("中止中...")
        self._set_fetch_progress_text(
            "みんレポ取得は処理中の店舗を破棄して中止します",
            progress_kind=PROGRESS_KIND_MINREPO,
        )
        self._update_button_states()

    def cancel_site7_fetch(self) -> None:
        if not self._is_site7_busy() or self.site7_cancel_event.is_set():
            return

        self.site7_cancel_event.set()
        self.status_var.set("中止中...")
        self._set_fetch_progress_text(
            "サイトセブン取得は処理中の店舗を破棄して中止します",
            progress_kind=PROGRESS_KIND_SITE7,
        )
        self._update_button_states()

    def _start_worker(self, target: object, *args: object, operation_kind: str = "general") -> None:
        self._ensure_operation_tracking()
        operation_id = self._next_operation_id
        self._next_operation_id += 1
        self.active_operations[operation_id] = operation_kind
        if operation_kind in MINREPO_OPERATION_KINDS:
            self.minrepo_cancel_event.clear()
        elif operation_kind in SITE7_OPERATION_KINDS:
            self.site7_cancel_event.clear()
        self._refresh_busy_state()
        self._update_button_states()

        def run_worker() -> None:
            self._worker_context.operation_id = operation_id
            self._worker_context.operation_kind = operation_kind
            try:
                target(*args)
            finally:
                self._worker_context.operation_id = None
                self._worker_context.operation_kind = None

        worker = threading.Thread(target=run_worker, daemon=True)
        worker.start()
        if not self.result_polling_active:
            self.result_polling_active = True
            self.root.after(100, self._poll_queue)

    def _raise_if_fetch_cancelled(self) -> None:
        if self._cancel_requested():
            raise FetchCancelled

    def _run_with_fetch_retries(
        self,
        action: Callable[[], T],
        retry_delay_seconds: int,
        retry_status_callback: Callable[[int, int, int], None],
    ) -> T:
        for failed_count in range(MAX_FETCH_RETRY_COUNT + 1):
            self._raise_if_fetch_cancelled()
            try:
                return action()
            except FetchCancelled:
                raise
            except Exception as exc:
                if isinstance(exc, ScraperError) and minrepo_error_is_no_date_pages(exc):
                    raise
                if failed_count >= MAX_FETCH_RETRY_COUNT:
                    raise

                retry_number = failed_count + 1
                retry_status_callback(retry_number, MAX_FETCH_RETRY_COUNT, retry_delay_seconds)
                if retry_delay_seconds > 0 and self._operation_cancel_event().wait(retry_delay_seconds):
                    raise FetchCancelled

        raise ScraperError("取得の再試行に失敗しました。")

    def _worker_fetch_many(
        self,
        target_stores: list[RegisteredStore],
        target_date_input: str,
        retry_delay_seconds: int,
        fetch_parallel_options: MinRepoFetchParallelOptions,
        web_publish_options: WebPublishOptions,
    ) -> None:
        try:
            fetch_many_result = self._run_fetch_many(
                target_stores,
                target_date_input,
                retry_delay_seconds,
                fetch_parallel_options,
                web_publish_options,
            )
            if fetch_many_result.cancelled and not fetch_many_result.results:
                self.result_queue.put(("fetch_cancelled", None))
                return
            self.result_queue.put(("fetch_many_success", fetch_many_result))
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("fetch_error", exc))

    def _worker_minrepo_priority_watch(
        self,
        target_date: str,
        retry_delay_seconds: int,
        fetch_parallel_options: MinRepoFetchParallelOptions,
        web_publish_options: WebPublishOptions,
        completed_store_dates: set[tuple[str, str]],
    ) -> None:
        try:
            registered_stores = self._load_latest_registered_stores()
            target_stores = self._minrepo_priority_watch_registered_stores(
                registered_stores,
                target_date=target_date,
                completed_store_dates=completed_store_dates,
            )
            if not target_stores:
                self.result_queue.put(
                    (
                        "minrepo_priority_watch_no_update",
                        MinRepoPriorityWatchResult(
                            registered_stores=registered_stores,
                            target_date=target_date,
                            checked_store_count=0,
                            available_store_count=0,
                        ),
                    )
                )
                return

            available_stores: list[RegisteredStore] = []
            checked_count = 0
            for store_index, registered_store in enumerate(target_stores, start=1):
                self._raise_if_fetch_cancelled()
                checked_count += 1
                self.result_queue.put(
                    (
                        "fetch_progress",
                        FetchProgress(
                            current_step=store_index - 1,
                            total_steps=max(1, len(target_stores)),
                            message=f"{registered_store.name}: みんレポの {target_date} を確認中",
                        ),
                    )
                )
                if self._minrepo_store_has_target_date(
                    registered_store,
                    target_date=target_date,
                    retry_delay_seconds=retry_delay_seconds,
                ):
                    available_stores.append(registered_store)

            self.result_queue.put(
                (
                    "fetch_progress",
                    FetchProgress(
                        current_step=checked_count,
                        total_steps=max(1, len(target_stores)),
                        message=f"{target_date} の更新確認完了: {len(available_stores)}店舗",
                    ),
                )
            )

            if not available_stores:
                self.result_queue.put(
                    (
                        "minrepo_priority_watch_no_update",
                        MinRepoPriorityWatchResult(
                            registered_stores=registered_stores,
                            target_date=target_date,
                            checked_store_count=checked_count,
                            available_store_count=0,
                        ),
                    )
                )
                return

            fetch_many_result = self._run_fetch_many(
                available_stores,
                target_date,
                retry_delay_seconds,
                fetch_parallel_options,
                web_publish_options,
                preserve_order=True,
                required_target_dates={target_date},
            )
            if fetch_many_result.cancelled and not fetch_many_result.results:
                self.result_queue.put(("fetch_cancelled", None))
                return
            self.result_queue.put(
                (
                    "minrepo_priority_watch_success",
                    MinRepoPriorityWatchResult(
                        registered_stores=registered_stores,
                        target_date=target_date,
                        checked_store_count=checked_count,
                        available_store_count=len(available_stores),
                        fetch_many_result=fetch_many_result,
                    ),
                )
            )
        except FetchCancelled:
            self.result_queue.put(("fetch_cancelled", None))
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("fetch_error", exc))

    def _run_fetch_many(
        self,
        target_stores: list[RegisteredStore],
        target_date_input: str,
        retry_delay_seconds: int,
        fetch_parallel_options: MinRepoFetchParallelOptions | None = None,
        web_publish_options: WebPublishOptions | None = None,
        preserve_order: bool = False,
        required_target_dates: set[str] | None = None,
    ) -> FetchManyResult:
        fetch_parallel_options = fetch_parallel_options or MINREPO_FETCH_PARALLEL_OPTIONS[MINREPO_FETCH_MODE_NORMAL]
        web_publish_options = web_publish_options or WebPublishOptions(mode=WEB_PUBLISH_MODE_DAYS)
        target_stores = list(target_stores) if preserve_order else self._minrepo_fetch_ordered_stores(target_stores)
        results: list[StoreFetchResult] = []
        failures: list[StoreFetchFailure] = []
        total_stores = len(target_stores)
        cancelled = False
        save_executor = (
            ThreadPoolExecutor(max_workers=1, thread_name_prefix="r2-save")
            if total_stores > 1
            else None
        )

        try:
            for store_index, registered_store in enumerate(target_stores, start=1):
                if self._cancel_requested():
                    cancelled = True
                    break

                try:
                    store_result = self._fetch_single_store(
                        registered_store=registered_store,
                        target_date_input=target_date_input,
                        store_index=store_index,
                        total_stores=total_stores,
                        retry_delay_seconds=retry_delay_seconds,
                        fetch_parallel_options=fetch_parallel_options,
                        web_publish_options=web_publish_options,
                        required_target_dates=required_target_dates,
                        async_save_executor=save_executor,
                    )
                    self._refresh_web_data_for_store_result(store_result)
                    results.append(store_result)
                except FetchCancelled:
                    cancelled = True
                    break
                except Exception as exc:  # noqa: BLE001
                    failures.append(StoreFetchFailure(store=registered_store, error=exc))
                    self._queue_fetch_progress(
                        FetchProgress(
                            current_step=1,
                            total_steps=1,
                            message=f"{store_index}/{total_stores} {registered_store.name} は取得失敗",
                        ),
                        store_index=store_index,
                        total_stores=total_stores,
                    )
        finally:
            self._wait_for_pending_store_saves(results)
            if save_executor is not None:
                save_executor.shutdown(wait=True)

        if self._cancel_requested():
            cancelled = True

        if not results and failures:
            failure_lines = "\n".join(f"{failure.store.name}: {failure.error}" for failure in failures)
            raise ScraperError(f"選択した店舗を取得できませんでした。\n{failure_lines}")

        return FetchManyResult(results=results, failures=failures, cancelled=cancelled)

    def _queue_fetch_progress(
        self,
        progress: FetchProgress,
        *,
        store_index: int | None = None,
        total_stores: int | None = None,
    ) -> None:
        scaled_progress = self._scaled_fetch_progress(progress, store_index, total_stores)
        if not self._should_queue_fetch_progress(scaled_progress):
            return
        self.result_queue.put(("fetch_progress", scaled_progress))

    def _fetch_progress_queue_key(self) -> object:
        operation_id = getattr(self._worker_context, "operation_id", None)
        if operation_id is not None:
            return operation_id

        operation_kind = getattr(self._worker_context, "operation_kind", None) or self.active_operation_kind
        if operation_kind:
            return operation_kind

        return threading.get_ident()

    def _should_queue_fetch_progress(self, progress: FetchProgress) -> bool:
        total_steps = max(1, progress.total_steps)
        current_step = min(max(0, progress.current_step), total_steps)
        message = progress.message
        if current_step <= 0 or current_step >= total_steps:
            return True

        progress_key = self._fetch_progress_queue_key()
        now = time.monotonic()
        last_progress = self._last_queued_fetch_progress_by_operation.get(progress_key)
        if last_progress is None:
            self._last_queued_fetch_progress_by_operation[progress_key] = (
                now,
                current_step,
                total_steps,
                message,
            )
            return True

        last_queued_at, last_current_step, last_total_steps, last_message = last_progress
        if (
            current_step == last_current_step
            and total_steps == last_total_steps
            and message == last_message
        ):
            return False
        if now - last_queued_at < FETCH_PROGRESS_QUEUE_MIN_INTERVAL_SECONDS:
            return False

        self._last_queued_fetch_progress_by_operation[progress_key] = (
            now,
            current_step,
            total_steps,
            message,
        )
        return True

    def _scaled_fetch_progress(
        self,
        progress: FetchProgress,
        store_index: int | None,
        total_stores: int | None,
    ) -> FetchProgress:
        if store_index is None or total_stores is None or total_stores <= 1:
            return progress

        local_total = max(1, progress.total_steps)
        local_current = min(max(0, progress.current_step), local_total)
        total_steps = max(1, total_stores * FETCH_PROGRESS_GLOBAL_SCALE)
        store_fraction = max(0, store_index - 1) + (local_current / local_total)
        current_step = int(round(store_fraction * FETCH_PROGRESS_GLOBAL_SCALE))
        return FetchProgress(
            current_step=min(max(0, current_step), total_steps),
            total_steps=total_steps,
            message=progress.message,
        )

    def _minrepo_fetch_ordered_stores(self, target_stores: list[RegisteredStore]) -> list[RegisteredStore]:
        return self._registered_store_fetch_ordered(target_stores)

    def _registered_store_fetch_ordered(self, target_stores: list[RegisteredStore]) -> list[RegisteredStore]:
        return sorted(target_stores, key=self._registered_store_fetch_order_key)

    def _registered_store_fetch_order_key(self, registered_store: RegisteredStore) -> tuple[int, int, int, str, str]:
        fetch_order = normalize_fetch_order(registered_store.fetch_order)
        region_priority = self._registered_store_region_order_priority(registered_store)
        if fetch_order is not None:
            return (
                0,
                fetch_order,
                region_priority,
                normalize_text(registered_store.name),
                normalize_store_url(registered_store.url),
            )
        return (
            1,
            0,
            region_priority,
            normalize_text(registered_store.name),
            normalize_store_url(registered_store.url),
        )

    def _registered_store_region_order_priority(self, registered_store: RegisteredStore) -> int:
        fetch_order_region_mode = normalize_fetch_order_region_mode(
            getattr(self, "fetch_order_region_mode", DEFAULT_FETCH_ORDER_REGION_MODE)
        )
        if fetch_order_region_mode == FETCH_ORDER_REGION_MODE_AS_IS:
            return 0

        region_text = normalize_text(
            " ".join(
                [
                    registered_store.site7_prefecture,
                    registered_store.site7_address,
                ]
            )
        )
        preferred_region_text = "福岡" if fetch_order_region_mode == FETCH_ORDER_REGION_MODE_FUKUOKA else "東京"
        return 0 if preferred_region_text in region_text else 1

    def _minrepo_priority_watch_registered_stores(
        self,
        registered_stores: list[RegisteredStore],
        *,
        target_date: str,
        completed_store_dates: set[tuple[str, str]] | None = None,
    ) -> list[RegisteredStore]:
        completed_store_dates = completed_store_dates or set()
        target_stores = [
            registered_store
            for registered_store in registered_stores
            if registered_store.uses_minrepo()
            and registered_store.fetch_frequency != FETCH_FREQUENCY_STOP
            and normalize_fetch_order(registered_store.fetch_order) is not None
            and (normalize_store_url(registered_store.url), target_date) not in completed_store_dates
        ]
        return self._registered_store_fetch_ordered(target_stores)

    def _minrepo_store_has_target_date(
        self,
        registered_store: RegisteredStore,
        *,
        target_date: str,
        retry_delay_seconds: int,
    ) -> bool:
        store_url = registered_store.url
        try:
            context = self._run_with_fetch_retries(
                lambda: self.scraper.prepare_machine_history_context(store_url, target_date),
                retry_delay_seconds=retry_delay_seconds,
                retry_status_callback=lambda _retry_number, _max_retries, _delay_seconds: None,
            )
        except FetchCancelled:
            raise
        except Exception:  # noqa: BLE001
            return False

        return any(date_page.target_date == target_date for date_page in context.date_pages)

    def _mark_minrepo_priority_watch_completed(
        self,
        fetch_many_result: FetchManyResult,
        target_date: str,
    ) -> None:
        if not hasattr(self, "minrepo_priority_watch_completed_store_dates"):
            self.minrepo_priority_watch_completed_store_dates = set()

        for store_result in fetch_many_result.results:
            store_url = normalize_store_url(store_result.history_result.store_url)
            if not store_url:
                continue
            if self._store_result_covers_target_date(store_result, target_date):
                self.minrepo_priority_watch_completed_store_dates.add((store_url, target_date))

    def _store_result_covers_target_date(self, store_result: StoreFetchResult, target_date: str) -> bool:
        if target_date in store_result.saved_full_day_summary.saved_dates:
            return True
        if target_date in store_result.history_result.skipped_dates:
            return True
        if any(
            status.target_date == target_date and str(status.status).strip().casefold() == "closed"
            for status in store_result.history_result.store_day_statuses
        ):
            return True
        if any(date_page.target_date == target_date for date_page in store_result.history_result.date_pages):
            save_summary = store_result.save_summary
            if save_summary is None or not save_summary.has_errors:
                return True
        return any(dataset.target_date == target_date for dataset in store_result.history_result.datasets)

    def _scheduled_minrepo_registered_stores(
        self,
        registered_stores: list[RegisteredStore],
        *,
        selected_store_urls: set[str],
        supplemental_store_last_run_dates: dict[str, str] | None = None,
        supplemental_interval_days: int | None = None,
    ) -> tuple[list[RegisteredStore], set[str]]:
        normalized_selected_urls = {
            normalize_store_url(store_url)
            for store_url in selected_store_urls
        }
        primary_stores = [
            registered_store
            for registered_store in registered_stores
            if registered_store.uses_minrepo()
            and (
                registered_store.fetch_frequency in {FETCH_FREQUENCY_HIGH, FETCH_FREQUENCY_DAILY}
                or (
                    not registered_store.fetch_frequency
                    and normalize_store_url(registered_store.url) in normalized_selected_urls
                )
            )
        ]
        supplemental_candidates = [
            registered_store
            for registered_store in registered_stores
            if registered_store.uses_minrepo()
            and registered_store.fetch_frequency == FETCH_FREQUENCY_LOW
        ]
        interval_days = supplemental_interval_days or getattr(
            self,
            "schedule_all_stores_interval_days",
            DEFAULT_SCHEDULE_ALL_STORES_INTERVAL_DAYS,
        )
        supplemental_limit = scheduled_supplemental_store_limit(len(supplemental_candidates), interval_days)
        run_dates = supplemental_store_last_run_dates or {}

        def supplemental_sort_key(registered_store: RegisteredStore) -> tuple[str, int, int, int, str, str]:
            store_url = normalize_store_url(registered_store.url)
            last_run_date = run_dates.get(store_url, "")
            fetch_order = normalize_fetch_order(registered_store.fetch_order)
            return (
                last_run_date,
                0 if fetch_order is not None else 1,
                fetch_order or 0,
                self._registered_store_region_order_priority(registered_store),
                normalize_text(registered_store.name),
                store_url,
            )

        supplemental_stores = sorted(supplemental_candidates, key=supplemental_sort_key)[:supplemental_limit]
        supplemental_store_urls = {
            normalize_store_url(registered_store.url)
            for registered_store in supplemental_stores
        }
        target_stores = self._minrepo_fetch_ordered_stores(primary_stores) + supplemental_stores
        return target_stores, supplemental_store_urls

    def _fetch_single_store(
        self,
        registered_store: RegisteredStore,
        target_date_input: str,
        store_index: int,
        total_stores: int,
        retry_delay_seconds: int,
        fetch_parallel_options: MinRepoFetchParallelOptions | None = None,
        web_publish_options: WebPublishOptions | None = None,
        required_target_dates: set[str] | None = None,
        async_save_executor: ThreadPoolExecutor | None = None,
    ) -> StoreFetchResult:
        fetch_parallel_options = fetch_parallel_options or MINREPO_FETCH_PARALLEL_OPTIONS[MINREPO_FETCH_MODE_NORMAL]
        self._raise_if_fetch_cancelled()
        store_url = registered_store.url
        store_label = f"{store_index}/{total_stores} {self._registered_store_display_name(registered_store)}"

        def queue_progress(progress: FetchProgress) -> None:
            self._queue_fetch_progress(progress, store_index=store_index, total_stores=total_stores)

        try:
            context = self._run_with_fetch_retries(
                lambda: self.scraper.prepare_machine_history_context(store_url, target_date_input),
                retry_delay_seconds=retry_delay_seconds,
                retry_status_callback=lambda retry_number, max_retries, delay_seconds: queue_progress(
                    FetchProgress(
                        current_step=0,
                        total_steps=1,
                        message=(
                            f"{store_label}: 対象期間の確認に失敗しました。"
                            f"{delay_seconds}秒後に再試行します（{retry_number}/{max_retries}）"
                        ),
                    )
                ),
            )
        except ScraperError as exc:
            if not minrepo_error_is_no_date_pages(exc):
                raise

            start_date, end_date = parse_date_range_input(target_date_input)
            queue_progress(
                FetchProgress(
                    current_step=1,
                    total_steps=1,
                    message=f"{store_label}: 対象期間に取得できる日付なし",
                )
            )
            return StoreFetchResult(
                history_result=MachineHistoryResult(
                    store_name=registered_store.name,
                    store_url=store_url,
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d"),
                    date_pages=[],
                    datasets=[],
                ),
                save_summary=None,
                saved_full_day_summary=SavedFullDayDatesSummary(),
            )
        if required_target_dates is not None:
            context_date_pages = [
                date_page
                for date_page in context.date_pages
                if date_page.target_date in required_target_dates
            ]
            if context_date_pages:
                context = replace(
                    context,
                    start_date=context_date_pages[0].target_date,
                    end_date=context_date_pages[-1].target_date,
                    date_pages=context_date_pages,
                )
            else:
                sorted_required_dates = sorted(required_target_dates)
                context = replace(
                    context,
                    start_date=sorted_required_dates[0] if sorted_required_dates else context.start_date,
                    end_date=sorted_required_dates[-1] if sorted_required_dates else context.end_date,
                    date_pages=[],
                )
        self._raise_if_fetch_cancelled()
        saved_full_day_summary = self.persistence_service.find_saved_full_day_dates(
            store_name=context.store_name,
            store_url=store_url,
            start_date=context.start_date,
            end_date=context.end_date,
        )
        self._raise_if_fetch_cancelled()
        skipped_dates = [
            date_page.target_date
            for date_page in context.date_pages
            if date_page.target_date in saved_full_day_summary.saved_dates
        ]
        pending_date_pages = [
            date_page
            for date_page in context.date_pages
            if date_page.target_date not in saved_full_day_summary.saved_dates
        ]
        day_progress_steps = {
            date_page.target_date: DEFAULT_MINREPO_DAY_PROGRESS_STEPS
            for date_page in pending_date_pages
        }
        total_steps = max(1, sum(day_progress_steps.values()) + 1)
        current_step = 0
        progress_lock = threading.Lock()
        save_summary_lock = threading.Lock()
        date_parallel_workers = max(1, min(fetch_parallel_options.date_workers, len(pending_date_pages) or 1))
        machine_parallel_workers = max(1, fetch_parallel_options.machine_workers)
        incomplete_date_count = len(saved_full_day_summary.incomplete_dates)
        incomplete_date_text = f"、保存台数不足の{incomplete_date_count}日を再取得" if incomplete_date_count else ""
        queue_progress(
            FetchProgress(
                current_step=current_step,
                total_steps=total_steps,
                message=(
                    f"{store_label}: "
                    f"{len(context.date_pages)}日分のうち"
                    f"{len(skipped_dates)}日を日付ごとスキップ"
                    f"{incomplete_date_text}"
                ),
            )
        )

        def step_callback(message: str) -> None:
            nonlocal current_step, total_steps
            self._raise_if_fetch_cancelled()
            with progress_lock:
                current_step += 1
                total_steps = max(total_steps, current_step)
                progress = FetchProgress(
                    current_step=current_step,
                    total_steps=total_steps,
                    message=f"{store_label}: {message}",
                )
            queue_progress(
                progress
            )

        def day_total_callback(target_date: str, machine_count: int) -> None:
            nonlocal current_step, total_steps
            with progress_lock:
                current_estimate = day_progress_steps.get(target_date, DEFAULT_MINREPO_DAY_PROGRESS_STEPS)
                exact_steps = max(2, 2 + max(0, machine_count) * 2)
                if exact_steps == current_estimate:
                    return
                day_progress_steps[target_date] = exact_steps
                total_steps = max(current_step + 1, total_steps + exact_steps - current_estimate)

        def queue_retry_progress(target_date: str, retry_number: int, max_retries: int, delay_seconds: int) -> None:
            with progress_lock:
                progress = FetchProgress(
                    current_step=current_step,
                    total_steps=total_steps,
                    message=(
                        f"{store_label}: {target_date} の取得に失敗しました。"
                        f"{delay_seconds}秒後に再試行します（{retry_number}/{max_retries}）"
                    ),
                )
            queue_progress(progress)

        save_summary: PersistenceSummary | None = None
        pending_save_futures: list[Future[PersistenceSummary]] = []
        checkpoint_paths_by_date: dict[str, list[str]] = {}
        checkpoint_lock = threading.Lock()

        def merge_checkpoint_summary(checkpoint_summary: PersistenceSummary) -> None:
            nonlocal save_summary
            if checkpoint_summary.has_errors:
                with save_summary_lock:
                    save_summary = self._merge_persistence_summary(save_summary, checkpoint_summary)

        def save_dataset_checkpoint(dataset_result: MachineHistoryResult) -> None:
            self._raise_if_fetch_cancelled()
            dataset = dataset_result.datasets[0] if dataset_result.datasets else None
            if dataset is not None:
                step_callback(f"{dataset.target_date} の {dataset.machine_name} をローカル退避中")
            checkpoint_summary = self.persistence_service.save_history_result_local_checkpoint(dataset_result)
            if checkpoint_summary.local_file_path:
                checkpoint_dates = {date_page.target_date for date_page in dataset_result.date_pages}
                if not checkpoint_dates and dataset is not None:
                    checkpoint_dates = {dataset.target_date}
                with checkpoint_lock:
                    for checkpoint_date in checkpoint_dates:
                        checkpoint_paths_by_date.setdefault(checkpoint_date, []).append(
                            checkpoint_summary.local_file_path
                        )
            merge_checkpoint_summary(checkpoint_summary)

        def fetch_date_page(date_index: int, date_page: object) -> MachineHistoryResult:
            return self._run_with_fetch_retries(
                lambda: self.scraper.fetch_all_machine_history_for_date_page(
                    context=context,
                    date_page=date_page,
                    step_callback=step_callback,
                    date_index=date_index,
                    total_dates=len(pending_date_pages),
                    dataset_callback=save_dataset_checkpoint,
                    day_total_callback=day_total_callback,
                    machine_parallel_workers=machine_parallel_workers,
                ),
                retry_delay_seconds=retry_delay_seconds,
                retry_status_callback=lambda retry_number, max_retries, delay_seconds, target_date=date_page.target_date: queue_retry_progress(
                    target_date,
                    retry_number,
                    max_retries,
                    delay_seconds,
                ),
            )

        day_results_by_date: dict[str, MachineHistoryResult] = {}
        publish_batch_results: list[MachineHistoryResult] = []

        def build_publish_result(batch_results: list[MachineHistoryResult]) -> MachineHistoryResult:
            date_pages_by_date: dict[str, StoreDatePage] = {}
            datasets: list[MachineDataset] = []
            skipped_targets: list[tuple[str, str]] = []
            skipped_batch_dates: list[str] = []

            for batch_result in batch_results:
                for batch_date_page in batch_result.date_pages:
                    date_pages_by_date[batch_date_page.target_date] = batch_date_page
                datasets.extend(batch_result.datasets)
                skipped_targets.extend(batch_result.skipped_targets)
                for skipped_date in batch_result.skipped_dates:
                    if skipped_date not in skipped_batch_dates:
                        skipped_batch_dates.append(skipped_date)

            date_pages = sorted(
                date_pages_by_date.values(),
                key=lambda batch_date_page: batch_date_page.target_date,
            )
            batch_start_date = date_pages[0].target_date if date_pages else context.start_date
            batch_end_date = date_pages[-1].target_date if date_pages else context.end_date
            return MachineHistoryResult(
                store_name=context.store_name,
                store_url=context.store_url,
                start_date=batch_start_date,
                end_date=batch_end_date,
                date_pages=date_pages,
                datasets=datasets,
                skipped_targets=skipped_targets,
                skipped_dates=skipped_batch_dates,
            )

        def complete_dates_for_full_day_mark(publish_result: MachineHistoryResult) -> set[str]:
            incomplete_dates = {
                target_date
                for target_date, _machine_name in publish_result.skipped_targets
            }
            return {
                date_page.target_date
                for date_page in publish_result.date_pages
                if date_page.target_date not in incomplete_dates
            }

        def take_checkpoint_paths_for_publish_result(publish_result: MachineHistoryResult) -> list[str]:
            publish_dates = [date_page.target_date for date_page in publish_result.date_pages]
            with checkpoint_lock:
                checkpoint_paths: list[str] = []
                for publish_date in publish_dates:
                    checkpoint_paths.extend(checkpoint_paths_by_date.pop(publish_date, []))
            return checkpoint_paths

        def take_all_checkpoint_paths() -> list[str]:
            with checkpoint_lock:
                checkpoint_paths = [
                    checkpoint_path
                    for checkpoint_paths in checkpoint_paths_by_date.values()
                    for checkpoint_path in checkpoint_paths
                ]
                checkpoint_paths_by_date.clear()
            return checkpoint_paths

        def mark_checkpoints_left_on_failure(
            batch_save_summary: PersistenceSummary,
            checkpoint_paths: list[str],
        ) -> None:
            if not checkpoint_paths or batch_save_summary.web_data_saved:
                return
            batch_save_summary.local_file_path = checkpoint_paths[-1]
            batch_save_summary.messages.append("R2保存に失敗したため、取得中のローカル退避を復旧用に残しました。")

        def publish_batch(label: str) -> None:
            nonlocal save_summary, publish_batch_results
            if not publish_batch_results:
                return

            publish_result = build_publish_result(publish_batch_results)
            if not publish_result.datasets:
                publish_batch_results = []
                return

            step_callback(
                f"{label}のR2保存とWeb更新を開始"
                if async_save_executor is not None
                else f"{label}のR2保存とWeb更新中"
            )
            batch_checkpoint_paths = take_checkpoint_paths_for_publish_result(publish_result)
            complete_dates = complete_dates_for_full_day_mark(publish_result)
            full_day_save = len(complete_dates) == len(publish_result.date_pages)

            def run_batch_save() -> PersistenceSummary:
                batch_save_summary = self._run_with_persistence_lock(
                    lambda: self.persistence_service.save_history_result(publish_result, full_day=full_day_save)
                )
                if batch_save_summary.web_data_saved and complete_dates and not full_day_save:
                    complete_result = filter_history_result_dates(publish_result, complete_dates)
                    mark_summary = self._run_with_persistence_lock(
                        lambda: self.persistence_service.mark_full_day_saved(complete_result)
                    )
                    batch_save_summary = self._merge_persistence_summary(batch_save_summary, mark_summary)
                if batch_save_summary.web_data_saved:
                    delete_summary = self.persistence_service.delete_local_checkpoint_files(batch_checkpoint_paths)
                    batch_save_summary.messages.extend(delete_summary.messages)
                else:
                    mark_checkpoints_left_on_failure(batch_save_summary, batch_checkpoint_paths)
                return batch_save_summary

            if async_save_executor is not None:
                pending_save_futures.append(async_save_executor.submit(run_batch_save))
            else:
                batch_save_summary = run_batch_save()
                with save_summary_lock:
                    save_summary = self._merge_persistence_summary(save_summary, batch_save_summary)
            publish_batch_results = []

        def save_day_result(date_page: object, day_result: MachineHistoryResult) -> None:
            nonlocal publish_batch_results
            day_results_by_date[date_page.target_date] = day_result
            if day_result.datasets:
                publish_batch_results.append(day_result)
            else:
                step_callback(f"{date_page.target_date} は保存対象なし")

        try:
            if date_parallel_workers <= 1:
                for date_index, date_page in enumerate(pending_date_pages, start=1):
                    self._raise_if_fetch_cancelled()
                    day_result = fetch_date_page(date_index, date_page)
                    self._raise_if_fetch_cancelled()
                    save_day_result(date_page, day_result)
            else:
                with ThreadPoolExecutor(max_workers=date_parallel_workers, thread_name_prefix="minrepo-day") as executor:
                    futures_by_date_page = {
                        executor.submit(fetch_date_page, date_index, date_page): date_page
                        for date_index, date_page in enumerate(pending_date_pages, start=1)
                    }
                    try:
                        for future in as_completed(futures_by_date_page):
                            date_page = futures_by_date_page[future]
                            day_result = future.result()
                            self._raise_if_fetch_cancelled()
                            save_day_result(date_page, day_result)
                    except Exception:
                        for future in futures_by_date_page:
                            future.cancel()
                        raise

            self._raise_if_fetch_cancelled()
            if publish_batch_results:
                publish_batch("店舗完了分")
        except FetchCancelled:
            checkpoint_paths = take_all_checkpoint_paths()
            if checkpoint_paths:
                self.persistence_service.delete_local_checkpoint_files(checkpoint_paths)
            raise

        datasets: list[MachineDataset] = []
        skipped_targets: list[tuple[str, str]] = []
        for date_page in pending_date_pages:
            day_result = day_results_by_date.get(date_page.target_date)
            if day_result is None:
                continue
            datasets.extend(day_result.datasets)
            skipped_targets.extend(day_result.skipped_targets)

        result = MachineHistoryResult(
            store_name=context.store_name,
            store_url=context.store_url,
            start_date=context.start_date,
            end_date=context.end_date,
            date_pages=pending_date_pages,
            datasets=datasets,
            skipped_targets=skipped_targets,
            skipped_dates=skipped_dates,
        )
        return StoreFetchResult(
            history_result=result,
            save_summary=save_summary,
            saved_full_day_summary=saved_full_day_summary,
            pending_save_futures=pending_save_futures,
        )

    def _poll_queue(self) -> None:
        try:
            kind, payload = self.result_queue.get_nowait()
        except queue.Empty:
            if self._has_active_operations():
                self.root.after(100, self._poll_queue)
            else:
                self.result_polling_active = False
            return

        operation_id = getattr(self.result_queue, "last_operation_id", None)
        operation_kind = self._operation_kind_for_result(operation_id)
        progress_kind = self._progress_kind_for_operation(operation_kind)
        if kind == "fetch_progress":
            if isinstance(payload, FetchProgress):
                self._apply_fetch_progress(payload, progress_kind=progress_kind)
            if self._has_active_operations():
                self.root.after(100, self._poll_queue)
            else:
                self.result_polling_active = False
            return

        if operation_id is not None:
            self._last_queued_fetch_progress_by_operation.pop(operation_id, None)
        if operation_kind:
            self._last_queued_fetch_progress_by_operation.pop(operation_kind, None)
        if operation_id is not None:
            self.active_operations.pop(operation_id, None)
        else:
            self.active_operations = {
                active_operation_id: active_operation_kind
                for active_operation_id, active_operation_kind in self.active_operations.items()
                if active_operation_kind != operation_kind
            }
        self._refresh_busy_state()
        if operation_kind in MINREPO_OPERATION_KINDS and not self._is_minrepo_busy():
            self.minrepo_cancel_event.clear()
        if operation_kind in SITE7_OPERATION_KINDS and not self._is_site7_busy():
            self.site7_cancel_event.clear()
        self._update_button_states()
        if self._has_active_operations():
            self.root.after(100, self._poll_queue)
        else:
            self.result_polling_active = False

        if kind == "site7_login_error":
            self.site7_status_var.set("ログイン未完了")
            self.status_var.set("待機中")
            self._show_error(payload)
            return

        if kind == "site7_login_success":
            self.site7_status_var.set("ログイン情報を保存しました")
            self.status_var.set("待機中")
            messagebox.showinfo("サイトセブン", "サイトセブンのログイン状態を確認して保存しました。次回以降は再ログインを省ける場合があります。")
            return

        if kind == "register_store_error":
            self.register_store_status_var.set("店舗登録に失敗しました")
            self._show_error(payload)
            return

        if kind == "register_store_success":
            if (
                not isinstance(payload, tuple)
                or len(payload) != 13
                or not isinstance(payload[0], str)
                or not isinstance(payload[1], str)
            ):
                messagebox.showerror("エラー", "登録店舗の形式が不正です。")
                return
            (
                store_name,
                store_url,
                fetch_frequency,
                fetch_source,
                fetch_order,
                site7_enabled,
                site7_difference_enabled,
                site7_prefecture,
                site7_area,
                site7_store_name,
                site7_hall_id,
                site7_address,
                event_settings,
            ) = payload
            self._apply_registered_store(
                store_name,
                store_url,
                str(fetch_frequency),
                str(fetch_source),
                normalize_fetch_order(fetch_order),
                bool(site7_enabled),
                bool(site7_difference_enabled),
                str(site7_prefecture),
                str(site7_area),
                str(site7_store_name),
                str(site7_hall_id),
                str(site7_address),
                event_settings if isinstance(event_settings, StoreEventSettings) else None,
            )
            return

        if kind == "update_registered_store_error":
            self.register_store_status_var.set("店舗更新に失敗しました")
            self._show_error(payload)
            return

        if kind == "update_registered_store_success":
            if (
                not isinstance(payload, tuple)
                or len(payload) != 14
                or not isinstance(payload[0], str)
                or not isinstance(payload[1], str)
                or not isinstance(payload[2], str)
            ):
                messagebox.showerror("エラー", "更新店舗の形式が不正です。")
                return
            (
                original_store_url,
                store_name,
                store_url,
                fetch_frequency,
                fetch_source,
                fetch_order,
                site7_enabled,
                site7_difference_enabled,
                site7_prefecture,
                site7_area,
                site7_store_name,
                site7_hall_id,
                site7_address,
                event_settings,
            ) = payload
            original_store = next(
                (
                    registered_store
                    for registered_store in self.registered_stores
                    if normalize_store_url(registered_store.url) == normalize_store_url(original_store_url)
                ),
                None,
            )
            if original_store is None:
                messagebox.showerror("エラー", "更新対象の店舗が見つかりませんでした。")
                return
            self._replace_registered_store_entry(
                original_store=original_store,
                store_name=store_name,
                store_url=store_url,
                fetch_frequency=str(fetch_frequency),
                fetch_source=str(fetch_source),
                fetch_order=normalize_fetch_order(fetch_order),
                site7_enabled=bool(site7_enabled),
                site7_difference_enabled=bool(site7_difference_enabled),
                site7_prefecture=str(site7_prefecture),
                site7_area=str(site7_area),
                site7_store_name=str(site7_store_name),
                site7_hall_id=str(site7_hall_id),
                site7_address=str(site7_address),
                event_settings=event_settings if isinstance(event_settings, StoreEventSettings) else None,
            )
            return

        if kind == "refresh_registered_stores_error":
            self.register_store_status_var.set("登録店舗の更新に失敗しました")
            messagebox.showerror("登録店舗", f"登録店舗の更新に失敗しました。\n{payload}")
            return

        if kind == "refresh_registered_stores_success":
            if not isinstance(payload, StoreRefreshResult):
                self.register_store_status_var.set("登録店舗の更新に失敗しました")
                messagebox.showerror("登録店舗", "登録店舗の形式が不正です。")
                return
            self._replace_registered_stores(payload.registered_stores, select_all=False)
            self.register_store_status_var.set(f"{len(payload.registered_stores)}店舗を読み込みました")
            if payload.save_summary is not None and payload.save_summary.has_errors:
                messagebox.showwarning("登録店舗", "\n\n".join(payload.save_summary.messages))
            return

        if kind == "delete_registered_stores_error":
            self.register_store_status_var.set("登録店舗の削除に失敗しました")
            messagebox.showerror("登録店舗", f"登録店舗の削除に失敗しました。\n{payload}")
            return

        if kind == "delete_registered_stores_success":
            if not isinstance(payload, StoreDeleteResult):
                self.register_store_status_var.set("登録店舗の削除に失敗しました")
                messagebox.showerror("登録店舗", "削除結果の形式が不正です。")
                return
            self._replace_registered_stores(payload.registered_stores, select_all=False)
            if payload.deleted_store_count > 0:
                self.register_store_status_var.set(f"{payload.deleted_store_count}店舗を削除しました")
            else:
                self.register_store_status_var.set("削除結果を反映しました")
            return

        if kind == "fetch_error":
            self._finish_fetch_progress(success=False, message="取得失敗", progress_kind=progress_kind)
            self.status_var.set("失敗")
            self.summary_var.set("取得できませんでした")
            if operation_kind == "scheduled_fetch":
                self.schedule_status_var.set("定期実行に失敗しました")
            elif operation_kind == "minrepo_priority_watch":
                self.schedule_status_var.set("早朝みんレポ確認に失敗しました")
            elif operation_kind == "scheduled_site7_fetch":
                self.site7_schedule_status_var.set("サイトセブン定期実行に失敗しました")
            self._show_error(payload)
            return

        if kind == "fetch_cancelled":
            self._finish_fetch_progress(success=False, message="中止しました", progress_kind=progress_kind)
            self.status_var.set("中止")
            self.summary_var.set("取得を中止しました")
            if operation_kind == "scheduled_fetch":
                self.schedule_status_var.set("定期実行を中止しました")
            elif operation_kind == "minrepo_priority_watch":
                self.schedule_status_var.set("早朝みんレポ確認を中止しました")
            elif operation_kind == "scheduled_site7_fetch":
                self.site7_schedule_status_var.set("サイトセブン定期実行を中止しました")
            return

        if kind == "minrepo_priority_watch_no_update":
            if not isinstance(payload, MinRepoPriorityWatchResult):
                self._finish_fetch_progress(success=False, message="確認失敗", progress_kind=progress_kind)
                self.status_var.set("失敗")
                self.summary_var.set("不明な結果")
                self.schedule_status_var.set("早朝みんレポ確認に失敗しました")
                messagebox.showerror("エラー", "早朝みんレポ確認の結果形式が不正です。")
                return
            self._replace_registered_stores(payload.registered_stores, select_all=False, reset_fetch_display=False)
            self._finish_fetch_progress(success=True, message="更新なし", progress_kind=progress_kind)
            self.status_var.set("待機中")
            self.summary_var.set(
                f"{payload.target_date} は未更新 / 確認{payload.checked_store_count}店舗"
            )
            self.schedule_status_var.set(
                f"{payload.target_date} の取得順店舗は未更新。15分後に再確認"
            )
            return

        if kind == "minrepo_priority_watch_success":
            if (
                not isinstance(payload, MinRepoPriorityWatchResult)
                or not isinstance(payload.fetch_many_result, FetchManyResult)
                or not payload.fetch_many_result.results
            ):
                self._finish_fetch_progress(success=False, message="取得失敗", progress_kind=progress_kind)
                self.status_var.set("失敗")
                self.summary_var.set("不明な結果")
                self.schedule_status_var.set("早朝みんレポ取得に失敗しました")
                messagebox.showerror("エラー", "早朝みんレポ取得の結果形式が不正です。")
                return
            self._replace_registered_stores(payload.registered_stores, select_all=False, reset_fetch_display=False)
            self._apply_fetch_many_result(payload.fetch_many_result, progress_kind=progress_kind)
            self._mark_minrepo_priority_watch_completed(payload.fetch_many_result, payload.target_date)
            if payload.fetch_many_result.cancelled:
                self.schedule_status_var.set("早朝みんレポ確認を中止しました")
            else:
                self.schedule_status_var.set(
                    f"{payload.target_date} の取得順店舗を取得しました: {payload.available_store_count}店舗"
                )
            return

        if kind == "fetch_many_success":
            if not isinstance(payload, FetchManyResult) or not payload.results:
                self._finish_fetch_progress(success=False, message="取得失敗", progress_kind=progress_kind)
                self.status_var.set("失敗")
                self.summary_var.set("不明な結果")
                messagebox.showerror("エラー", "取得結果の形式が不正です。")
                return
            self._apply_fetch_many_result(payload, progress_kind=progress_kind)
            return

        if kind == "scheduled_fetch_many_success":
            if not isinstance(payload, ScheduledFetchResult):
                self._finish_fetch_progress(success=False, message="取得失敗", progress_kind=progress_kind)
                self.status_var.set("失敗")
                self.summary_var.set("不明な結果")
                self.schedule_status_var.set("定期実行に失敗しました")
                messagebox.showerror("エラー", "定期実行の結果形式が不正です。")
                return
            refresh_result = payload.refresh_result
            fetch_many_result = payload.fetch_many_result
            self._replace_registered_stores(refresh_result.registered_stores, select_all=False, reset_fetch_display=False)
            self._apply_fetch_many_result(fetch_many_result, progress_kind=progress_kind)
            if refresh_result.save_summary is not None and refresh_result.save_summary.has_errors:
                messagebox.showwarning("登録店舗", "\n\n".join(refresh_result.save_summary.messages))
            try:
                self._mark_successful_supplemental_stores(
                    fetch_many_result,
                    payload.supplemental_store_urls,
                    payload.run_date,
                )
            except Exception as exc:  # noqa: BLE001
                self.schedule_status_var.set(f"低頻度取得の記録保存に失敗しました: {exc}")
                return
            if fetch_many_result.cancelled:
                self.schedule_status_var.set("定期実行を中止しました")
            else:
                self.schedule_status_var.set(f"定期実行完了: 毎日 {self.scheduled_fetch_hour} 時")
            return

        if kind == "scheduled_site7_fetch_many_success":
            if not isinstance(payload, ScheduledSite7FetchResult):
                self._finish_fetch_progress(success=False, message="取得失敗", progress_kind=progress_kind)
                self.status_var.set("失敗")
                self.summary_var.set("不明な結果")
                self.site7_schedule_status_var.set("サイトセブン定期実行に失敗しました")
                messagebox.showerror("エラー", "サイトセブン定期実行の結果形式が不正です。")
                return
            registered_stores = payload.registered_stores
            fetch_many_result = payload.fetch_many_result
            self._replace_registered_stores(registered_stores, select_all=False, reset_fetch_display=False)
            self._apply_fetch_many_result(fetch_many_result, progress_kind=progress_kind)
            try:
                self._mark_successful_site7_schedule_stores(
                    fetch_many_result,
                    payload.store_run_urls,
                    payload.run_date,
                )
            except Exception as exc:  # noqa: BLE001
                self.site7_schedule_status_var.set(f"サイトセブン定期実行の記録保存に失敗しました: {exc}")
                return
            if fetch_many_result.cancelled:
                self.site7_schedule_status_var.set("サイトセブン定期実行を中止しました")
            else:
                if payload.waiting_store_urls and payload.scheduled_hour is not None and payload.waiting_started_at is not None:
                    self._schedule_site7_update_recheck(
                        scheduled_hour=payload.scheduled_hour,
                        waiting_store_urls=payload.waiting_store_urls,
                        run_date=payload.run_date,
                        waiting_started_at=payload.waiting_started_at,
                    )
                else:
                    self.site7_schedule_status_var.set(f"サイトセブン定期実行完了: {self._site7_schedule_status_text()}")
            return

        if kind == "scheduled_site7_fetch_waiting":
            if not isinstance(payload, ScheduledSite7UpdateWaitingResult):
                self._finish_fetch_progress(success=False, message="取得失敗", progress_kind=progress_kind)
                self.status_var.set("失敗")
                self.summary_var.set("不明な結果")
                self.site7_schedule_status_var.set("サイトセブン定期実行に失敗しました")
                messagebox.showerror("エラー", "サイトセブン更新待ちの結果形式が不正です。")
                return
            self._replace_registered_stores(payload.registered_stores, select_all=False, reset_fetch_display=False)
            self._finish_fetch_progress(success=True, message="更新待ち", progress_kind=progress_kind)
            self.status_var.set("待機中")
            self.summary_var.set("サイトセブン更新待ちの店舗を10分後に再確認します")
            self._schedule_site7_update_recheck(
                scheduled_hour=payload.scheduled_hour,
                waiting_store_urls=payload.waiting_store_urls,
                run_date=payload.run_date,
                waiting_started_at=payload.waiting_started_at,
            )
            return

        if kind == "scheduled_site7_fetch_skipped":
            if isinstance(payload, list):
                self._replace_registered_stores(payload, select_all=False, reset_fetch_display=False)
            self._finish_fetch_progress(success=True, message="取得対象なし", progress_kind=progress_kind)
            self.status_var.set("完了")
            self.summary_var.set("サイトセブン定期実行の対象店舗はありません")
            self.site7_schedule_status_var.set(f"サイトセブン定期実行完了: {self._site7_schedule_status_text()}")
            return

        history_result = payload
        save_summary: PersistenceSummary | None = None
        saved_full_day_summary = SavedFullDayDatesSummary()
        if (
            isinstance(payload, tuple)
            and len(payload) == 3
            and isinstance(payload[0], MachineHistoryResult)
            and (isinstance(payload[1], PersistenceSummary) or payload[1] is None)
            and isinstance(payload[2], SavedFullDayDatesSummary)
        ):
            history_result = payload[0]
            save_summary = payload[1]
            saved_full_day_summary = payload[2]
        if not isinstance(history_result, MachineHistoryResult):
            self._finish_fetch_progress(success=False, message="取得失敗", progress_kind=progress_kind)
            self.status_var.set("失敗")
            self.summary_var.set("不明な結果")
            messagebox.showerror("エラー", "取得結果の形式が不正です。")
            return

        self.current_history_result = history_result
        self.current_results = history_result.datasets
        self._clear_fetch_result_details()
        self._apply_fetch_result_layout()
        store_name = history_result.store_name
        self._finish_fetch_progress(
            success=True,
            message="取得完了（保存に注意）" if save_summary is not None and save_summary.has_errors else "取得完了",
            progress_kind=progress_kind,
        )
        if save_summary is not None and save_summary.has_errors:
            self.status_var.set("完了（保存に注意）")
        elif not history_result.datasets and (history_result.skipped_targets or history_result.skipped_dates):
            self.status_var.set("完了（取得済みをスキップ）")
        else:
            self.status_var.set("完了")
        skipped_count = len(history_result.skipped_targets)
        skipped_date_count = len(history_result.skipped_dates)
        fetched_machine_count = len({dataset.machine_name for dataset in history_result.datasets})
        self.summary_var.set(
            f"{store_name} / {history_result.start_date} ～ {history_result.end_date} / "
            f"{fetched_machine_count}機種 / {len(history_result.date_pages)}日取得 / "
            f"{self._save_status_text(save_summary)}"
            f"{f' / 日付スキップ{skipped_date_count}日' if skipped_date_count else ''}"
            f"{f' / スキップ{skipped_count}件' if skipped_count else ''}"
        )
        self._update_button_states()
        self._notify_fetch_complete()
        warning_messages = list(saved_full_day_summary.messages)
        if save_summary is not None and save_summary.has_errors:
            warning_messages.extend(save_summary.messages)
        if warning_messages:
            messagebox.showwarning("自動処理", "\n\n".join(warning_messages))

    def _apply_fetch_many_result(
        self,
        fetch_many_result: FetchManyResult,
        *,
        progress_kind: str = PROGRESS_KIND_MINREPO,
    ) -> None:
        last_store_result = fetch_many_result.results[-1]
        history_result = last_store_result.history_result

        self.current_history_result = history_result
        self.current_results = history_result.datasets
        self._clear_fetch_result_details()
        self._apply_fetch_result_layout()

        has_save_errors = any(
            store_result.save_summary is not None and store_result.save_summary.has_errors
            for store_result in fetch_many_result.results
        )
        all_skipped = all(
            not store_result.history_result.datasets
            and (store_result.history_result.skipped_targets or store_result.history_result.skipped_dates)
            for store_result in fetch_many_result.results
        )

        if fetch_many_result.cancelled:
            finish_message = "中止しました（保存に注意）" if has_save_errors else "中止しました"
        else:
            finish_message = "取得完了（保存に注意）" if has_save_errors else "取得完了"

        self._finish_fetch_progress(success=True, message=finish_message, progress_kind=progress_kind)
        if fetch_many_result.cancelled:
            self.status_var.set("中止（一部取得済み）")
        elif fetch_many_result.failures:
            self.status_var.set("完了（一部失敗）")
        elif has_save_errors:
            self.status_var.set("完了（保存に注意）")
        elif all_skipped:
            self.status_var.set("完了（取得済みをスキップ）")
        else:
            self.status_var.set("完了")

        self.summary_var.set(self._fetch_many_summary_text(fetch_many_result))
        self._update_button_states()
        if not fetch_many_result.cancelled:
            self._notify_fetch_complete()

        warning_messages: list[str] = []
        for store_result in fetch_many_result.results:
            warning_messages.extend(store_result.saved_full_day_summary.messages)
            if store_result.save_summary is not None and store_result.save_summary.has_errors:
                warning_messages.extend(store_result.save_summary.messages)
        for failure in fetch_many_result.failures:
            warning_messages.append(f"{failure.store.name} の取得に失敗しました。\n{failure.error}")
        if warning_messages:
            messagebox.showwarning("自動処理", "\n\n".join(warning_messages))

    def _fetch_many_summary_text(self, fetch_many_result: FetchManyResult) -> str:
        if len(fetch_many_result.results) == 1 and not fetch_many_result.failures:
            store_result = fetch_many_result.results[0]
            return self._single_fetch_summary_text(store_result.history_result, store_result.save_summary)

        first_history_result = fetch_many_result.results[0].history_result
        fetched_machine_count = sum(
            len({dataset.machine_name for dataset in store_result.history_result.datasets})
            for store_result in fetch_many_result.results
        )
        fetched_day_count = sum(
            len(store_result.history_result.date_pages)
            for store_result in fetch_many_result.results
        )
        skipped_count = sum(
            len(store_result.history_result.skipped_targets)
            for store_result in fetch_many_result.results
        )
        skipped_date_count = sum(
            len(store_result.history_result.skipped_dates)
            for store_result in fetch_many_result.results
        )
        failed_text = f" / 失敗{len(fetch_many_result.failures)}店舗" if fetch_many_result.failures else ""
        cancelled_text = " / 中止" if fetch_many_result.cancelled else ""
        return (
            f"{len(fetch_many_result.results)}店舗完了{failed_text}{cancelled_text} / "
            f"{first_history_result.start_date} ～ {first_history_result.end_date} / "
            f"{fetched_machine_count}機種 / {fetched_day_count}日取得 / "
            f"{self._many_save_status_text(fetch_many_result.results)}"
            f"{f' / 日付スキップ{skipped_date_count}日' if skipped_date_count else ''}"
            f"{f' / スキップ{skipped_count}件' if skipped_count else ''}"
        )

    def _single_fetch_summary_text(
        self,
        history_result: MachineHistoryResult,
        save_summary: PersistenceSummary | None,
    ) -> str:
        skipped_count = len(history_result.skipped_targets)
        skipped_date_count = len(history_result.skipped_dates)
        fetched_machine_count = len({dataset.machine_name for dataset in history_result.datasets})
        return (
            f"{history_result.store_name} / {history_result.start_date} ～ {history_result.end_date} / "
            f"{fetched_machine_count}機種 / {len(history_result.date_pages)}日取得 / "
            f"{self._save_status_text(save_summary)}"
            f"{f' / 日付スキップ{skipped_date_count}日' if skipped_date_count else ''}"
            f"{f' / スキップ{skipped_count}件' if skipped_count else ''}"
        )

    def _many_save_status_text(self, store_results: list[StoreFetchResult]) -> str:
        save_summaries = [store_result.save_summary for store_result in store_results]
        if any(save_summary is not None and save_summary.has_errors for save_summary in save_summaries):
            return "保存に注意"

        if any(
            save_summary is not None
            and (save_summary.local_file_path or save_summary.supabase_saved or save_summary.web_data_saved)
            for save_summary in save_summaries
        ):
            return "保存あり"

        return "保存なし"

    def _clear_fetch_result_details(self) -> None:
        return

    def _on_registered_store_filter_changed(self, *_: object) -> None:
        if hasattr(self, "registered_store_tree"):
            self._refresh_registered_store_table(preserve_selection=True)

    def _clear_registered_store_filter(self) -> None:
        self.registered_store_filter_var.set("")

    def _registered_store_filter_keyword(self) -> str:
        if not hasattr(self, "registered_store_filter_var"):
            return ""
        return normalize_text(self.registered_store_filter_var.get())

    def _refresh_registered_store_table(self, preserve_selection: bool = False) -> None:
        if not hasattr(self, "registered_store_tree"):
            return

        selected_item_ids = set(self.registered_store_tree.selection()) if preserve_selection else set()
        self.registered_store_tree.delete(*self.registered_store_tree.get_children())
        self._refresh_registered_store_headings()
        filter_keyword = self._registered_store_filter_keyword()
        visible_count = 0
        rows = list(enumerate(self.registered_stores))
        rows = self._sorted_registered_store_rows(rows)
        for index, registered_store in rows:
            display_name = self._registered_store_display_name(registered_store)
            if filter_keyword and filter_keyword not in normalize_text(display_name):
                continue

            item_id = f"registered_store_{index}"
            self.registered_store_tree.insert(
                "",
                "end",
                iid=item_id,
                values=(
                    self._registered_store_frequency_text(registered_store),
                    self._registered_store_source_text(registered_store),
                    self._registered_store_site7_difference_text(registered_store),
                    self._registered_store_order_text(registered_store),
                    display_name,
                    registered_store.url,
                    registered_store.site7_prefecture,
                    registered_store.site7_area,
                    registered_store.resolved_site7_store_name(),
                    registered_store.site7_hall_id,
                    registered_store.site7_address,
                ),
            )
            visible_count += 1
            if item_id in selected_item_ids:
                self.registered_store_tree.selection_add(item_id)
        if hasattr(self, "registered_store_filter_status_var"):
            total_count = len(self.registered_stores)
            if filter_keyword:
                self.registered_store_filter_status_var.set(f"{visible_count} / {total_count} 店舗を表示")
            else:
                self.registered_store_filter_status_var.set(f"{total_count} 店舗を表示")
        self._update_button_states()

    def _registered_store_heading_text(self, column: str) -> str:
        if column != self.registered_store_sort_column:
            return column
        return f"{column} {'↓' if self.registered_store_sort_descending else '↑'}"

    def _refresh_registered_store_headings(self) -> None:
        for column in REGISTERED_STORE_COLUMNS:
            self.registered_store_tree.heading(
                column,
                text=self._registered_store_heading_text(column),
                command=lambda current_column=column: self._sort_registered_store_table_by(current_column),
            )

    def _sort_registered_store_table_by(self, column: str) -> None:
        if column == self.registered_store_sort_column:
            self.registered_store_sort_descending = not self.registered_store_sort_descending
        else:
            self.registered_store_sort_column = column
            self.registered_store_sort_descending = False
        self._refresh_registered_store_table(preserve_selection=True)

    def _sorted_registered_store_rows(
        self,
        rows: list[tuple[int, RegisteredStore]],
    ) -> list[tuple[int, RegisteredStore]]:
        sort_column = self.registered_store_sort_column
        if sort_column not in REGISTERED_STORE_COLUMNS:
            return rows
        return self._sort_records(
            rows,
            lambda row: self._registered_store_sort_value(row[1], sort_column),
            self.registered_store_sort_descending,
        )

    def _registered_store_sort_value(self, registered_store: RegisteredStore, column: str) -> object:
        if column == "頻度":
            return self._registered_store_frequency_text(registered_store)
        if column == "取得元":
            return self._registered_store_source_text(registered_store)
        if column == "S差枚":
            return self._registered_store_site7_difference_text(registered_store)
        if column == "取得順":
            return self._registered_store_order_text(registered_store)
        if column == "店舗名":
            return self._registered_store_display_name(registered_store)
        if column == "URL":
            return registered_store.url
        if column == "都道府県":
            return registered_store.site7_prefecture
        if column == "地域":
            return registered_store.site7_area
        if column == "SS店舗名":
            return registered_store.resolved_site7_store_name()
        if column == "SS ID":
            return registered_store.site7_hall_id
        if column == "SS住所":
            return registered_store.site7_address
        return ""

    def _replace_registered_stores(
        self,
        registered_stores: list[RegisteredStore],
        select_all: bool,
        reset_fetch_display: bool = True,
    ) -> None:
        previous_urls = {
            normalize_store_url(registered_store.url)
            for registered_store in self.registered_stores
        }
        next_urls = {
            normalize_store_url(registered_store.url)
            for registered_store in registered_stores
        }

        if select_all:
            for registered_store in registered_stores:
                registered_store.fetch_frequency = FETCH_FREQUENCY_DAILY
        else:
            new_urls = next_urls - previous_urls
            for registered_store in registered_stores:
                if normalize_store_url(registered_store.url) in new_urls:
                    registered_store.fetch_frequency = FETCH_FREQUENCY_DAILY

        self.registered_stores = registered_stores
        self.selected_store_urls = self._load_saved_selected_store_urls(self.registered_stores)

        self._refresh_registered_store_table()
        try:
            self._save_selected_store_urls()
        except Exception as exc:  # noqa: BLE001
            if hasattr(self, "register_store_status_var"):
                self.register_store_status_var.set(f"頻度の保存に失敗しました: {exc}")
        if reset_fetch_display:
            self._reset_fetch_display_for_store_change()

    def _registered_store_frequency_text(self, registered_store: RegisteredStore) -> str:
        return normalize_fetch_frequency(registered_store.fetch_frequency)

    def _registered_store_source_text(self, registered_store: RegisteredStore) -> str:
        return normalize_fetch_source(registered_store.fetch_source)

    def _registered_store_site7_difference_text(self, registered_store: RegisteredStore) -> str:
        return "ON" if registered_store.site7_enabled and registered_store.site7_difference_enabled else "OFF"

    def _registered_store_order_text(self, registered_store: RegisteredStore) -> str:
        return "" if registered_store.fetch_order is None else str(registered_store.fetch_order)

    def _registered_store_display_name(self, registered_store: RegisteredStore) -> str:
        return registered_store.name.strip() or "（店舗名未取得）"

    def _load_registered_store_form(self, registered_store: RegisteredStore) -> None:
        self.register_store_url_var.set(registered_store.url)
        self.register_store_frequency_var.set(self._registered_store_frequency_text(registered_store))
        self.register_store_source_var.set(self._registered_store_source_text(registered_store))
        self.register_store_order_var.set(self._registered_store_order_text(registered_store))
        self.register_store_site7_enabled_var.set(registered_store.site7_enabled)
        self.register_store_site7_difference_enabled_var.set(registered_store.site7_difference_enabled)
        self.register_store_prefecture_var.set(registered_store.site7_prefecture or DEFAULT_SITE7_PREFECTURE_NAME)
        self.register_store_area_var.set(registered_store.site7_area)
        self.register_store_site7_store_name_var.set(registered_store.resolved_site7_store_name())
        self.register_store_site7_hall_id_var.set(registered_store.site7_hall_id)
        self.register_store_site7_address_var.set(registered_store.site7_address)
        self.register_store_status_var.set(f"{self._registered_store_display_name(registered_store)} を編集中")

    def _selected_registered_stores(self) -> list[RegisteredStore]:
        return self._minrepo_fetch_ordered_stores([
            registered_store
            for registered_store in self.registered_stores
            if registered_store.uses_minrepo()
            and registered_store.fetch_frequency in {FETCH_FREQUENCY_HIGH, FETCH_FREQUENCY_DAILY}
        ])

    def _selected_registered_store_rows(self) -> list[RegisteredStore]:
        return [
            registered_store
            for item_id in self.registered_store_tree.selection()
            if (registered_store := self._registered_store_from_item_id(item_id)) is not None
        ]

    def _registered_store_from_item_id(self, item_id: str) -> RegisteredStore | None:
        prefix = "registered_store_"
        if not item_id.startswith(prefix):
            return None

        index_text = item_id[len(prefix):]
        if not index_text.isdigit():
            return None

        index = int(index_text)
        if index < 0 or index >= len(self.registered_stores):
            return None

        return self.registered_stores[index]

    def _confirm_registered_store_deletion(self, registered_stores: list[RegisteredStore]) -> bool:
        store_lines = [
            self._registered_store_display_name(registered_store)
            for registered_store in registered_stores[:5]
        ]
        if len(registered_stores) > 5:
            store_lines.append(f"ほか {len(registered_stores) - 5} 店舗")

        return messagebox.askyesno(
            "登録店舗",
            (
                "選択した店舗を登録一覧から削除します。\n"
                "保存済みの台データは削除しません。\n\n"
                + "\n".join(store_lines)
            ),
        )

    def _on_registered_store_selection_changed(self, _: tk.Event[tk.Misc]) -> None:
        selected_rows = self._selected_registered_store_rows()
        if len(selected_rows) == 1:
            self._load_registered_store_form(selected_rows[0])
        self._update_button_states()

    def _on_registered_store_tree_click(self, event: tk.Event[tk.Misc]) -> str | None:
        if self.registered_store_tree.identify_region(event.x, event.y) != "cell":
            return None

        column_id = self.registered_store_tree.identify_column(event.x)
        if column_id not in {"#1", "#2", "#3", "#4"}:
            return None

        item_id = self.registered_store_tree.identify_row(event.y)
        if not item_id:
            return None

        if column_id == "#1":
            self._cycle_registered_store_frequency(item_id)
        elif column_id == "#2":
            self._cycle_registered_store_source(item_id)
        elif column_id == "#3":
            self._toggle_registered_store_site7_difference(item_id)
        else:
            self._edit_registered_store_order(item_id)
        return "break"

    def _on_registered_store_tree_right_click(self, event: tk.Event[tk.Misc]) -> str | None:
        item_id = self.registered_store_tree.identify_row(event.y)
        if not item_id:
            return None

        registered_store = self._registered_store_from_item_id(item_id)
        if registered_store is None:
            return None

        self.registered_store_tree.focus(item_id)
        self.registered_store_tree.selection_set(item_id)
        self._load_registered_store_form(registered_store)

        menu = tk.Menu(self.root, tearoff=False)
        menu.add_command(
            label="この店舗だけをみんレポ取得",
            command=lambda store=registered_store: self.fetch_registered_store_data(store),
        )
        menu.add_command(
            label="この店舗だけをサイトセブン取得",
            command=lambda store=registered_store: self.fetch_registered_store_site7_data(store),
        )
        menu.add_command(
            label="この店舗のネオアイムを取得",
            command=lambda store=registered_store: self.fetch_registered_store_site7_neo_im_data(store),
        )
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            menu.grab_release()
        return "break"

    def _cycle_registered_store_frequency(self, item_id: str) -> None:
        registered_store = self._registered_store_from_item_id(item_id)
        if registered_store is None:
            return

        current_index = FETCH_FREQUENCY_OPTIONS.index(normalize_fetch_frequency(registered_store.fetch_frequency))
        registered_store.fetch_frequency = FETCH_FREQUENCY_OPTIONS[(current_index + 1) % len(FETCH_FREQUENCY_OPTIONS)]
        self.registered_store_tree.set(item_id, "頻度", self._registered_store_frequency_text(registered_store))
        self._save_selected_store_urls()
        save_summary = self._persist_registered_stores()
        if save_summary.has_errors:
            messagebox.showwarning("登録店舗", "\n\n".join(save_summary.messages))
        self._load_registered_store_form(registered_store)
        self._reset_fetch_display_for_store_change()

    def _cycle_registered_store_source(self, item_id: str) -> None:
        registered_store = self._registered_store_from_item_id(item_id)
        if registered_store is None:
            return

        was_site7_enabled = registered_store.site7_enabled
        current_index = FETCH_SOURCE_OPTIONS.index(normalize_fetch_source(registered_store.fetch_source))
        registered_store.fetch_source = FETCH_SOURCE_OPTIONS[(current_index + 1) % len(FETCH_SOURCE_OPTIONS)]
        registered_store.site7_enabled = registered_store.uses_site7()
        if not registered_store.site7_enabled:
            registered_store.site7_difference_enabled = False
        elif not was_site7_enabled:
            registered_store.site7_difference_enabled = registered_store.fetch_order is not None
        self.registered_store_tree.set(item_id, "取得元", self._registered_store_source_text(registered_store))
        self.registered_store_tree.set(item_id, "S差枚", self._registered_store_site7_difference_text(registered_store))
        save_summary = self._persist_registered_stores()
        if save_summary.has_errors:
            messagebox.showwarning("登録店舗", "\n\n".join(save_summary.messages))
        self._load_registered_store_form(registered_store)
        self._update_button_states()

    def _toggle_registered_store_site7_difference(self, item_id: str) -> None:
        registered_store = self._registered_store_from_item_id(item_id)
        if registered_store is None:
            return
        if not registered_store.site7_enabled:
            self.register_store_status_var.set("取得元にサイセを含む店舗だけS差枚をONにできます")
            return

        registered_store.site7_difference_enabled = not registered_store.site7_difference_enabled
        self.registered_store_tree.set(item_id, "S差枚", self._registered_store_site7_difference_text(registered_store))
        save_summary = self._persist_registered_stores()
        if save_summary.has_errors:
            messagebox.showwarning("登録店舗", "\n\n".join(save_summary.messages))
        self._load_registered_store_form(registered_store)
        self._update_button_states()

    def _edit_registered_store_order(self, item_id: str) -> None:
        registered_store = self._registered_store_from_item_id(item_id)
        if registered_store is None:
            return

        bbox = self.registered_store_tree.bbox(item_id, "取得順")
        if not bbox:
            return

        x, y, width, height = bbox
        order_var = tk.StringVar(value=self._registered_store_order_text(registered_store))
        editor = ttk.Entry(self.registered_store_tree, textvariable=order_var, justify="center")
        editor.place(x=x, y=y, width=width, height=height)
        editor.focus_set()
        editor.select_range(0, "end")

        completed = False

        def finish(save: bool) -> None:
            nonlocal completed
            if completed:
                return
            completed = True
            try:
                editor.destroy()
            except tk.TclError:
                pass
            if not save:
                return

            text = order_var.get().strip()
            if text and normalize_fetch_order(text) is None:
                self.register_store_status_var.set("取得順は1以上の整数、または空欄で入力してください")
                return

            registered_store.fetch_order = normalize_fetch_order(text)
            self.registered_store_tree.set(item_id, "取得順", self._registered_store_order_text(registered_store))
            save_summary = self._persist_registered_stores()
            if save_summary.has_errors:
                messagebox.showwarning("登録店舗", "\n\n".join(save_summary.messages))
            self._load_registered_store_form(registered_store)

        editor.bind("<Return>", lambda _: finish(True))
        editor.bind("<FocusOut>", lambda _: finish(True))
        editor.bind("<Escape>", lambda _: finish(False))

    def apply_shared_my_hall_to_registered_stores(self) -> None:
        try:
            my_hall_store_urls, missing_store_ids = self.persistence_service.load_shared_my_hall_store_urls()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Webマイホール", f"Webマイホールを読めませんでした。\n{exc}")
            return

        if not my_hall_store_urls:
            messagebox.showwarning("Webマイホール", "Webマイホールに店舗がありません。")
            return

        my_hall_store_url_set = set(my_hall_store_urls)
        registered_store_url_set = {
            normalize_store_url(registered_store.url)
            for registered_store in self.registered_stores
        }
        if not (my_hall_store_url_set & registered_store_url_set):
            messagebox.showwarning(
                "Webマイホール",
                "Webマイホールに一致する登録店舗がありません。最新に更新してから再実行してください。",
            )
            return

        matched_count = 0
        for registered_store in self.registered_stores:
            if normalize_store_url(registered_store.url) in my_hall_store_url_set:
                registered_store.fetch_frequency = FETCH_FREQUENCY_DAILY
                matched_count += 1
            else:
                registered_store.fetch_frequency = FETCH_FREQUENCY_LOW

        self.selected_store_urls = self._load_saved_selected_store_urls(self.registered_stores)
        self._refresh_registered_store_table()
        try:
            self._save_selected_store_urls()
            save_summary = self._persist_registered_stores()
            if save_summary.has_errors:
                messagebox.showwarning("登録店舗", "\n\n".join(save_summary.messages))
        except Exception as exc:  # noqa: BLE001
            if hasattr(self, "register_store_status_var"):
                self.register_store_status_var.set(f"頻度の保存に失敗しました: {exc}")
            return

        status = f"Webマイホール {matched_count} 店舗を毎日にしました"
        if missing_store_ids:
            status += f"（未照合 {len(missing_store_ids)} 件）"
        self.register_store_status_var.set(status)
        self._reset_fetch_display_for_store_change()

    def _resolve_store_region_input(
        self,
        *,
        site7_enabled: bool,
        site7_prefecture: str,
        site7_area: str,
        fetched_prefecture: str,
        fetched_area: str,
    ) -> tuple[str, str]:
        resolved_site7_prefecture = site7_prefecture.strip()
        resolved_site7_area = site7_area.strip()
        normalized_fetched_prefecture = fetched_prefecture.strip()
        normalized_fetched_area = fetched_area.strip()

        if normalized_fetched_prefecture and (
            not resolved_site7_prefecture or resolved_site7_prefecture == DEFAULT_SITE7_PREFECTURE_NAME
        ):
            resolved_site7_prefecture = normalized_fetched_prefecture

        if not resolved_site7_area and normalized_fetched_area:
            resolved_site7_area = normalized_fetched_area

        if site7_enabled and not resolved_site7_area:
            raise ScraperError("サイトセブン取得を使う場合は地域を入力してください。みんレポから取れない時だけ手入力してください。")

        return resolved_site7_prefecture or DEFAULT_SITE7_PREFECTURE_NAME, resolved_site7_area

    def _validated_register_store_form_input(self) -> tuple[str, str, str, int | None, bool, bool, str, str, str, str, str]:
        store_url = self.register_store_url_var.get().strip()
        fetch_frequency = normalize_fetch_frequency(
            self.register_store_frequency_var.get()
            if hasattr(self, "register_store_frequency_var")
            else FETCH_FREQUENCY_DAILY
        )
        fetch_source = normalize_fetch_source(
            self.register_store_source_var.get()
            if hasattr(self, "register_store_source_var")
            else FETCH_SOURCE_BOTH if bool(self.register_store_site7_enabled_var.get()) else FETCH_SOURCE_MINREPO
        )
        raw_fetch_order = self.register_store_order_var.get().strip() if hasattr(self, "register_store_order_var") else ""
        fetch_order = normalize_fetch_order(raw_fetch_order)
        if raw_fetch_order and fetch_order is None:
            raise ScraperError("取得順は1以上の整数、または空欄で入力してください。")
        site7_enabled = store_uses_site7(fetch_source)
        site7_difference_enabled = bool(site7_enabled and self.register_store_site7_difference_enabled_var.get())
        site7_prefecture = self.register_store_prefecture_var.get().strip() or DEFAULT_SITE7_PREFECTURE_NAME
        site7_area = self.register_store_area_var.get().strip()
        site7_store_name = self.register_store_site7_store_name_var.get().strip()
        site7_hall_id = self.register_store_site7_hall_id_var.get().strip()
        site7_address = self.register_store_site7_address_var.get().strip()

        if not store_url:
            raise ScraperError("店舗URLを入力してください。")
        if not self._is_valid_url(store_url):
            raise ScraperError("店舗URLは http:// または https:// から入力してください。")
        return (
            store_url,
            fetch_frequency,
            fetch_source,
            fetch_order,
            site7_enabled,
            site7_difference_enabled,
            site7_prefecture,
            site7_area,
            site7_store_name,
            site7_hall_id,
            site7_address,
        )

    def _build_registered_store(
        self,
        store_name: str,
        store_url: str,
        fetch_frequency: str = FETCH_FREQUENCY_DAILY,
        fetch_source: str | None = None,
        fetch_order: int | None = None,
        site7_enabled: bool | None = None,
        site7_difference_enabled: bool = False,
        site7_prefecture: str = "",
        site7_area: str = "",
        site7_store_name: str = "",
        site7_hall_id: str = "",
        site7_address: str = "",
        event_day_tails: tuple[int, ...] = (),
        event_month_days: tuple[int, ...] = (),
        event_zoro: bool = False,
        event_weekdays: tuple[int, ...] = (),
        event_source_text: str = "",
    ) -> RegisteredStore:
        defaults = default_site7_store_settings(store_name)
        resolved_site7_enabled = defaults["site7_enabled"] if site7_enabled is None else bool(site7_enabled)
        resolved_fetch_source = normalize_fetch_source(
            fetch_source,
            FETCH_SOURCE_BOTH if resolved_site7_enabled else FETCH_SOURCE_MINREPO,
        )
        resolved_site7_enabled = store_uses_site7(resolved_fetch_source)
        resolved_site7_difference_enabled = bool(resolved_site7_enabled and site7_difference_enabled)
        resolved_site7_prefecture = site7_prefecture.strip() or str(defaults["site7_prefecture"]).strip() or DEFAULT_SITE7_PREFECTURE_NAME
        resolved_site7_area = site7_area.strip() or str(defaults["site7_area"]).strip()
        resolved_site7_store_name = site7_store_name.strip() or str(defaults["site7_store_name"]).strip() or store_name.strip()
        resolved_site7_hall_id = site7_hall_id.strip() or str(defaults["site7_hall_id"]).strip()
        resolved_site7_address = site7_address.strip() or str(defaults["site7_address"]).strip()
        return RegisteredStore(
            name=store_name,
            url=normalize_store_url(store_url),
            fetch_frequency=normalize_fetch_frequency(fetch_frequency),
            fetch_source=resolved_fetch_source,
            fetch_order=normalize_fetch_order(fetch_order),
            site7_enabled=bool(resolved_site7_enabled),
            site7_difference_enabled=resolved_site7_difference_enabled,
            site7_prefecture=resolved_site7_prefecture,
            site7_area=resolved_site7_area,
            site7_store_name=resolved_site7_store_name,
            site7_hall_id=resolved_site7_hall_id,
            site7_address=resolved_site7_address,
            event_day_tails=normalize_int_tuple(event_day_tails, 0, 9),
            event_month_days=normalize_int_tuple(event_month_days, 1, 31),
            event_zoro=bool(event_zoro),
            event_weekdays=normalize_int_tuple(event_weekdays, 0, 6),
            event_source_text=event_source_text.strip(),
        )

    def _apply_registered_store(
        self,
        store_name: str,
        store_url: str,
        fetch_frequency: str = FETCH_FREQUENCY_DAILY,
        fetch_source: str = FETCH_SOURCE_MINREPO,
        fetch_order: int | None = None,
        site7_enabled: bool = False,
        site7_difference_enabled: bool = False,
        site7_prefecture: str = DEFAULT_SITE7_PREFECTURE_NAME,
        site7_area: str = "",
        site7_store_name: str = "",
        site7_hall_id: str = "",
        site7_address: str = "",
        event_settings: StoreEventSettings | None = None,
    ) -> None:
        normalized_name = normalize_text(store_name)
        normalized_url = normalize_store_url(store_url)
        for registered_store in self.registered_stores:
            if normalize_text(registered_store.name) == normalized_name or normalize_store_url(registered_store.url) == normalized_url:
                messagebox.showwarning("重複", "同じ店舗名またはURLがすでに登録されています。")
                self.register_store_status_var.set("登録済みの店舗です")
                return

        registered_store = self._build_registered_store(
            store_name=store_name,
            store_url=normalized_url,
            fetch_frequency=fetch_frequency,
            fetch_source=fetch_source,
            fetch_order=fetch_order,
            site7_enabled=site7_enabled,
            site7_difference_enabled=site7_difference_enabled,
            site7_prefecture=site7_prefecture,
            site7_area=site7_area,
            site7_store_name=site7_store_name,
            site7_hall_id=site7_hall_id,
            site7_address=site7_address,
            event_day_tails=tuple(event_settings.day_tails) if event_settings else (),
            event_month_days=tuple(event_settings.month_days) if event_settings else (),
            event_zoro=event_settings.zoro if event_settings else False,
            event_weekdays=tuple(event_settings.weekdays) if event_settings else (),
            event_source_text=event_settings.source_text if event_settings else "",
        )
        self.registered_stores.append(registered_store)
        self.selected_store_urls = self._load_saved_selected_store_urls(self.registered_stores)
        self.clear_register_store_form()
        self._refresh_registered_store_table()
        try:
            self._save_selected_store_urls()
        except Exception as exc:  # noqa: BLE001
            if hasattr(self, "register_store_status_var"):
                self.register_store_status_var.set(f"頻度の保存に失敗しました: {exc}")
        save_summary = self._persist_registered_stores()
        if save_summary.has_errors:
            self.register_store_status_var.set(f"{store_name} を登録しました（保存に注意）")
            messagebox.showwarning("登録店舗", "\n\n".join(save_summary.messages))
            return

        self.register_store_status_var.set(f"{store_name} を登録しました")

    def _replace_registered_store_entry(
        self,
        original_store: RegisteredStore,
        store_name: str,
        store_url: str,
        fetch_frequency: str,
        fetch_source: str,
        fetch_order: int | None,
        site7_enabled: bool,
        site7_difference_enabled: bool,
        site7_prefecture: str,
        site7_area: str,
        site7_store_name: str,
        site7_hall_id: str,
        site7_address: str,
        event_settings: StoreEventSettings | None = None,
    ) -> None:
        normalized_name = normalize_text(store_name)
        normalized_url = normalize_store_url(store_url)
        for registered_store in self.registered_stores:
            if registered_store is original_store:
                continue
            if normalize_text(registered_store.name) == normalized_name or normalize_store_url(registered_store.url) == normalized_url:
                messagebox.showwarning("重複", "同じ店舗名またはURLがすでに登録されています。")
                self.register_store_status_var.set("登録済みの店舗です")
                return

        updated_store = self._build_registered_store(
            store_name=store_name,
            store_url=store_url,
            fetch_frequency=fetch_frequency,
            fetch_source=fetch_source,
            fetch_order=fetch_order,
            site7_enabled=site7_enabled,
            site7_difference_enabled=site7_difference_enabled,
            site7_prefecture=site7_prefecture,
            site7_area=site7_area,
            site7_store_name=site7_store_name,
            site7_hall_id=site7_hall_id,
            site7_address=site7_address,
            event_day_tails=tuple(event_settings.day_tails) if event_settings else original_store.event_day_tails,
            event_month_days=tuple(event_settings.month_days) if event_settings else original_store.event_month_days,
            event_zoro=event_settings.zoro if event_settings else original_store.event_zoro,
            event_weekdays=tuple(event_settings.weekdays) if event_settings else original_store.event_weekdays,
            event_source_text=event_settings.source_text if event_settings else original_store.event_source_text,
        )
        updated_registered_stores = [
            updated_store if registered_store is original_store else registered_store
            for registered_store in self.registered_stores
        ]
        self.registered_stores = updated_registered_stores
        self.selected_store_urls = self._load_saved_selected_store_urls(self.registered_stores)
        self._refresh_registered_store_table()
        try:
            self._save_selected_store_urls()
        except Exception as exc:  # noqa: BLE001
            if hasattr(self, "register_store_status_var"):
                self.register_store_status_var.set(f"頻度の保存に失敗しました: {exc}")
        save_summary = self._persist_registered_stores()
        if save_summary.has_errors:
            self.register_store_status_var.set(f"{store_name} を更新しました（保存に注意）")
            messagebox.showwarning("登録店舗", "\n\n".join(save_summary.messages))
            return

        self.register_store_status_var.set(f"{store_name} を更新しました")

    def _selected_site7_registered_stores(self) -> list[RegisteredStore]:
        return self._site7_registered_stores_from(self.registered_stores)

    def _scheduled_site7_registered_stores(
        self,
        registered_stores: list[RegisteredStore],
        *,
        scheduled_hour: int,
        store_last_run_dates: dict[str, str],
        run_date: str,
        target_store_urls: set[str] | None = None,
    ) -> tuple[list[RegisteredStore], set[str]]:
        candidates = self._site7_registered_stores_from(registered_stores)
        if target_store_urls is not None:
            normalized_target_urls = {
                normalized_store_url
                for store_url in target_store_urls
                if (normalized_store_url := normalize_store_url(store_url))
            }
            candidates = [
                registered_store
                for registered_store in candidates
                if normalize_store_url(registered_store.url) in normalized_target_urls
            ]
        high_frequency_stores = [
            registered_store
            for registered_store in candidates
            if registered_store.fetch_frequency == FETCH_FREQUENCY_HIGH
        ]
        daily_stores = [
            registered_store
            for registered_store in candidates
            if registered_store.fetch_frequency == FETCH_FREQUENCY_DAILY
            and store_last_run_dates.get(normalize_store_url(registered_store.url)) != run_date
        ]
        low_frequency_candidates = [
            registered_store
            for registered_store in candidates
            if registered_store.fetch_frequency == FETCH_FREQUENCY_LOW
        ]
        interval_days = getattr(
            self,
            "schedule_all_stores_interval_days",
            DEFAULT_SCHEDULE_ALL_STORES_INTERVAL_DAYS,
        )
        low_frequency_limit = scheduled_supplemental_store_limit(len(low_frequency_candidates), interval_days)

        def low_frequency_sort_key(registered_store: RegisteredStore) -> tuple[str, int, int, int, str, str]:
            store_url = normalize_store_url(registered_store.url)
            fetch_order = normalize_fetch_order(registered_store.fetch_order)
            return (
                store_last_run_dates.get(store_url, ""),
                0 if fetch_order is not None else 1,
                fetch_order or 0,
                self._registered_store_region_order_priority(registered_store),
                normalize_text(registered_store.name),
                store_url,
            )

        low_frequency_stores = sorted(low_frequency_candidates, key=low_frequency_sort_key)[:low_frequency_limit]
        store_run_urls = {
            normalize_store_url(registered_store.url)
            for registered_store in [*daily_stores, *low_frequency_stores]
        }
        target_stores = self._registered_store_fetch_ordered(
            [*high_frequency_stores, *daily_stores, *low_frequency_stores]
        )
        return target_stores, store_run_urls

    def _site7_registered_store_for_single_fetch(
        self,
        registered_store: RegisteredStore,
        *,
        require_site7_source: bool = False,
    ) -> RegisteredStore:
        if require_site7_source and not registered_store.uses_site7():
            display_name = self._registered_store_display_name(registered_store)
            raise ScraperError(f"{display_name} の取得元にサイセを含めてください。")
        if registered_store_uses_daidata_online(registered_store):
            return registered_store
        if self._registered_store_is_known_site7_unavailable(registered_store):
            display_name = self._registered_store_display_name(registered_store)
            raise ScraperError(
                f"{display_name} は現在サイトセブンの店舗一覧にないため、サイトセブン取得の対象外です。"
            )
        if not registered_store.site7_area.strip():
            display_name = self._registered_store_display_name(registered_store)
            raise ScraperError(f"{display_name} をサイトセブン取得するには地域を入力してください。")
        return registered_store

    def _site7_registered_stores_from(self, registered_stores: list[RegisteredStore]) -> list[RegisteredStore]:
        target_stores = [
            registered_store
            for registered_store in registered_stores
            if registered_store.uses_site7() and registered_store.fetch_frequency != FETCH_FREQUENCY_STOP
        ]
        unavailable_stores = [
            registered_store.name
            for registered_store in target_stores
            if not registered_store_uses_daidata_online(registered_store)
            and self._registered_store_is_known_site7_unavailable(registered_store)
        ]
        target_stores = [
            registered_store
            for registered_store in target_stores
            if registered_store_uses_daidata_online(registered_store)
            or not self._registered_store_is_known_site7_unavailable(registered_store)
        ]
        if unavailable_stores and not target_stores:
            raise ScraperError(
                "次の店舗は現在サイトセブンの店舗一覧にないため、サイトセブン取得の対象外です。\n"
                + "\n".join(unavailable_stores)
            )
        invalid_stores = [
            registered_store.name
            for registered_store in target_stores
            if not registered_store_uses_daidata_online(registered_store)
            and not registered_store.site7_area.strip()
        ]
        if invalid_stores:
            raise ScraperError("サイトセブン取得を使う店舗は地域を入力してください。\n" + "\n".join(invalid_stores))
        return self._registered_store_fetch_ordered(target_stores)

    def _registered_store_is_known_site7_unavailable(self, registered_store: RegisteredStore) -> bool:
        return site7_store_is_known_unavailable(registered_store.name) or site7_store_is_known_unavailable(
            registered_store.resolved_site7_store_name()
        )

    def _persist_registered_stores(self) -> RegisteredStoresPersistenceSummary:
        return self._persist_registered_store_list(self.registered_stores)

    def _persist_registered_store_list(self, registered_stores: list[RegisteredStore]) -> RegisteredStoresPersistenceSummary:
        payloads = self._registered_store_payloads(registered_stores)
        return self._run_with_persistence_lock(
            lambda: self.persistence_service.save_registered_stores(payloads)
        )

    def _sync_registered_store_web_data(self, registered_stores: list[RegisteredStore]) -> RegisteredStoresPersistenceSummary:
        payloads = self._registered_store_payloads(registered_stores)
        return self._run_with_persistence_lock(
            lambda: self.persistence_service.sync_registered_stores_to_web_data(payloads)
        )

    def _registered_store_payloads(self, registered_stores: list[RegisteredStore]) -> list[dict[str, object]]:
        return [
            {
                "store_name": registered_store.name,
                "store_url": registered_store.url,
                "fetch_frequency": normalize_fetch_frequency(registered_store.fetch_frequency),
                "fetch_source": normalize_fetch_source(registered_store.fetch_source),
                "fetch_order": registered_store.fetch_order,
                "site7_enabled": registered_store.site7_enabled,
                "site7_difference_enabled": registered_store.site7_difference_enabled,
                "site7_prefecture": registered_store.site7_prefecture,
                "site7_area": registered_store.site7_area,
                "site7_store_name": registered_store.resolved_site7_store_name(),
                "site7_hall_id": registered_store.site7_hall_id,
                "site7_address": registered_store.site7_address,
                "event_day_tails": list(registered_store.event_day_tails),
                "event_month_days": list(registered_store.event_month_days),
                "event_zoro": registered_store.event_zoro,
                "event_weekdays": list(registered_store.event_weekdays),
                "event_source_text": registered_store.event_source_text,
            }
            for registered_store in registered_stores
        ]

    def _apply_fetch_result_layout(self) -> None:
        self.fetch_form.grid()
        self.fetch_info.grid()

    def _reset_fetch_display_for_store_change(self) -> None:
        self.current_results = []
        self.current_history_result = None
        self._clear_fetch_result_details()
        self.summary_var.set("未取得")
        self.status_var.set("待機中")
        self._reset_fetch_progress()
        self._apply_fetch_result_layout()
        self._update_button_states()

    def _fetch_progress_bar_for(self, progress_kind: str) -> object:
        if progress_kind == PROGRESS_KIND_SITE7:
            return self.site7_fetch_progress_bar
        return self.fetch_progress_bar

    def _fetch_progress_value_var_for(self, progress_kind: str) -> object:
        if progress_kind == PROGRESS_KIND_SITE7:
            return self.site7_fetch_progress_value_var
        return self.fetch_progress_value_var

    def _fetch_progress_text_var_for(self, progress_kind: str) -> object:
        if progress_kind == PROGRESS_KIND_SITE7:
            return self.site7_fetch_progress_text_var
        return self.fetch_progress_text_var

    def _fetch_progress_controls_ready(self, progress_kind: str) -> bool:
        if progress_kind == PROGRESS_KIND_SITE7:
            return (
                hasattr(self, "site7_fetch_progress_bar")
                and hasattr(self, "site7_fetch_progress_value_var")
                and hasattr(self, "site7_fetch_progress_text_var")
            )
        return (
            hasattr(self, "fetch_progress_bar")
            and hasattr(self, "fetch_progress_value_var")
            and hasattr(self, "fetch_progress_text_var")
        )

    def _set_fetch_progress_bar_mode(self, progress_kind: str, mode: str) -> None:
        progress_bar = self._fetch_progress_bar_for(progress_kind)
        if not hasattr(self, "_fetch_progress_bar_modes"):
            self._fetch_progress_bar_modes = {}
        current_mode = self._fetch_progress_bar_modes.get(progress_kind)
        if current_mode == mode:
            return

        if mode == "determinate":
            progress_bar.stop()
            progress_bar.configure(mode="determinate", maximum=100)
        else:
            progress_bar.stop()
            progress_bar.configure(mode="indeterminate", maximum=100)
            progress_bar.start(FETCH_PROGRESS_BAR_ANIMATION_INTERVAL_MS)
        self._fetch_progress_bar_modes[progress_kind] = mode

    def _set_fetch_progress_value(self, progress_kind: str, value: float) -> None:
        progress_value_var = self._fetch_progress_value_var_for(progress_kind)
        try:
            current_value = float(progress_value_var.get())
        except (tk.TclError, TypeError, ValueError):
            current_value = None
        if current_value is None or abs(current_value - value) >= 0.05:
            progress_value_var.set(value)

    def _set_fetch_progress_display_text(self, progress_kind: str, text: str) -> None:
        progress_text_var = self._fetch_progress_text_var_for(progress_kind)
        try:
            current_text = progress_text_var.get()
        except tk.TclError:
            current_text = None
        if current_text != text:
            progress_text_var.set(text)

    def _set_fetch_progress_state(
        self,
        progress_kind: str,
        *,
        current: int | None = None,
        total: int | None = None,
        started_at: float | None | object = FETCH_PROGRESS_STARTED_AT_UNSET,
        last_message: str | None = None,
    ) -> None:
        is_site7_progress = progress_kind == PROGRESS_KIND_SITE7
        if current is not None:
            if is_site7_progress:
                self.site7_fetch_progress_current = current
            else:
                self.fetch_progress_current = current
        if total is not None:
            if is_site7_progress:
                self.site7_fetch_progress_total = total
            else:
                self.fetch_progress_total = total
        if started_at is not FETCH_PROGRESS_STARTED_AT_UNSET:
            if is_site7_progress:
                self.site7_fetch_progress_started_at = started_at if isinstance(started_at, float) else None
            else:
                self.fetch_progress_started_at = started_at if isinstance(started_at, float) else None
        if last_message is not None:
            if is_site7_progress:
                self.site7_fetch_progress_last_message = last_message
            else:
                self.fetch_progress_last_message = last_message

    def _fetch_progress_total_for(self, progress_kind: str) -> int:
        if progress_kind == PROGRESS_KIND_SITE7:
            return getattr(self, "site7_fetch_progress_total", 0)
        return getattr(self, "fetch_progress_total", 0)

    def _fetch_progress_started_at_for(self, progress_kind: str) -> float | None:
        if progress_kind == PROGRESS_KIND_SITE7:
            return getattr(self, "site7_fetch_progress_started_at", None)
        return getattr(self, "fetch_progress_started_at", None)

    def _fetch_progress_last_message_for(self, progress_kind: str) -> str:
        if progress_kind == PROGRESS_KIND_SITE7:
            return getattr(self, "site7_fetch_progress_last_message", "未開始")
        return getattr(self, "fetch_progress_last_message", "未開始")

    def _begin_fetch_progress(self, message: str, *, progress_kind: str = PROGRESS_KIND_MINREPO) -> None:
        self._set_fetch_progress_state(
            progress_kind,
            current=0,
            total=0,
            started_at=time.monotonic(),
            last_message=message,
        )
        self._set_fetch_progress_bar_mode(progress_kind, "indeterminate")
        self._set_fetch_progress_value(progress_kind, 0.0)
        self._set_fetch_progress_text(message, progress_kind=progress_kind)
        self._schedule_fetch_elapsed_tick()

    def _apply_fetch_progress(self, progress: FetchProgress, *, progress_kind: str = PROGRESS_KIND_MINREPO) -> None:
        total_steps = max(1, progress.total_steps)
        current_step = min(max(0, progress.current_step), total_steps)
        progress_percent = current_step * 100 / total_steps
        self._set_fetch_progress_state(progress_kind, current=current_step, total=total_steps)
        self._set_fetch_progress_bar_mode(progress_kind, "determinate")
        self._set_fetch_progress_value(progress_kind, progress_percent)
        self._set_fetch_progress_text(f"{progress_percent:.1f}% {progress.message}", progress_kind=progress_kind)

    def _finish_fetch_progress(
        self,
        success: bool,
        message: str,
        *,
        progress_kind: str = PROGRESS_KIND_MINREPO,
    ) -> None:
        self._set_fetch_progress_bar_mode(progress_kind, "determinate")
        if success:
            total_steps = self._fetch_progress_total_for(progress_kind) or 1
            self._set_fetch_progress_state(progress_kind, current=total_steps, total=total_steps)
            self._set_fetch_progress_value(progress_kind, 100.0)
            self._set_fetch_progress_text(f"100.0% {message}", progress_kind=progress_kind)
            self._set_fetch_progress_state(progress_kind, started_at=None)
            return

        self._set_fetch_progress_state(progress_kind, current=0, total=0, started_at=None)
        self._set_fetch_progress_value(progress_kind, 0.0)
        self._set_fetch_progress_text(message, progress_kind=progress_kind)

    def _reset_fetch_progress(self, *, progress_kind: str | None = None) -> None:
        progress_kinds = (
            (PROGRESS_KIND_MINREPO, PROGRESS_KIND_SITE7)
            if progress_kind is None
            else (progress_kind,)
        )
        for current_progress_kind in progress_kinds:
            if not self._fetch_progress_controls_ready(current_progress_kind):
                continue
            self._set_fetch_progress_bar_mode(current_progress_kind, "determinate")
            self._set_fetch_progress_state(
                current_progress_kind,
                current=0,
                total=0,
                started_at=None,
                last_message="未開始",
            )
            self._set_fetch_progress_value(current_progress_kind, 0.0)
            self._set_fetch_progress_display_text(current_progress_kind, "未開始")

    def _set_fetch_progress_text(
        self,
        message: str,
        *,
        progress_kind: str = PROGRESS_KIND_MINREPO,
    ) -> None:
        self._set_fetch_progress_state(progress_kind, last_message=message)
        elapsed_text = self._fetch_elapsed_text(progress_kind=progress_kind)
        if elapsed_text:
            self._set_fetch_progress_display_text(progress_kind, f"{message} / {elapsed_text}")
            return
        self._set_fetch_progress_display_text(progress_kind, message)

    def _fetch_elapsed_text(self, *, progress_kind: str = PROGRESS_KIND_MINREPO) -> str:
        fetch_progress_started_at = self._fetch_progress_started_at_for(progress_kind)
        if fetch_progress_started_at is None:
            return ""
        elapsed_seconds = max(0, int(time.monotonic() - fetch_progress_started_at))
        hours, remainder = divmod(elapsed_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours:
            return f"経過 {hours}:{minutes:02d}:{seconds:02d}"
        return f"経過 {minutes:02d}:{seconds:02d}"

    def _schedule_fetch_elapsed_tick(self) -> None:
        if not hasattr(self, "root"):
            return
        self.root.after(1000, self._refresh_fetch_elapsed_text)

    def _refresh_fetch_elapsed_text(self) -> None:
        has_active_progress = False
        for progress_kind in (PROGRESS_KIND_MINREPO, PROGRESS_KIND_SITE7):
            if (
                self._fetch_progress_controls_ready(progress_kind)
                and self._fetch_progress_started_at_for(progress_kind) is not None
            ):
                self._set_fetch_progress_text(
                    self._fetch_progress_last_message_for(progress_kind),
                    progress_kind=progress_kind,
                )
                has_active_progress = True
        if has_active_progress:
            self._schedule_fetch_elapsed_tick()

    def _notify_fetch_complete(self) -> None:
        if not self.notify_fetch_complete_var.get():
            return

        if winsound is not None:
            try:
                winsound.MessageBeep(winsound.MB_ICONASTERISK)
                return
            except RuntimeError:
                pass

        try:
            self.root.bell()
        except tk.TclError:
            pass

    def _target_date_input_from_recent_days(self) -> str:
        return build_recent_date_range_input(self.target_date_var.get())

    def _retry_delay_seconds_input(self) -> int:
        return parse_retry_delay_seconds(self.retry_delay_seconds_var.get())

    def _web_publish_options_input(self) -> WebPublishOptions:
        mode = normalize_web_publish_mode(self.web_publish_mode_var.get())
        if mode == WEB_PUBLISH_MODE_STORE:
            interval_days = normalize_web_publish_interval_days(self.web_publish_interval_days_var.get())
            self.web_publish_interval_days_var.set(str(interval_days))
        else:
            interval_days = parse_web_publish_interval_days(self.web_publish_interval_days_var.get())

        options = WebPublishOptions(mode=mode, interval_days=interval_days)
        self.web_publish_mode = mode
        self.web_publish_interval_days = interval_days
        try:
            self._save_web_publish_settings(options)
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("設定保存", f"Web反映設定の保存に失敗しました。\n{exc}")
        return options

    def _minrepo_fetch_parallel_options(self) -> MinRepoFetchParallelOptions:
        return MINREPO_FETCH_PARALLEL_OPTIONS.get(
            self.minrepo_fetch_mode_var.get(),
            MINREPO_FETCH_PARALLEL_OPTIONS[MINREPO_FETCH_MODE_NORMAL],
        )

    def _save_status_text(self, save_summary: PersistenceSummary | None) -> str:
        if save_summary is None:
            return "保存なし"

        saved_targets: list[str] = []
        if save_summary.local_file_path:
            saved_targets.append("ローカル")
        if save_summary.web_data_saved:
            saved_targets.append("R2")

        if not saved_targets:
            return "保存失敗"
        return "保存:" + "+".join(saved_targets)

    def _refresh_web_data_for_store_result(self, store_result: StoreFetchResult) -> None:
        save_summary = store_result.save_summary
        if save_summary is None or save_summary.web_data_saved:
            return
        if not save_summary.local_file_path:
            return

        try:
            entry = self._run_with_persistence_lock(
                lambda: self.persistence_service.refresh_web_data_for_store(
                    store_result.history_result.store_name,
                )
            )
        except Exception as exc:  # noqa: BLE001
            save_summary.messages.append(f"Web表示用データの生成に失敗しました。\n{exc}")
            return

        if not entry:
            return
        save_summary.web_data_saved = True
        save_summary.web_data_file_path = str(
            self.persistence_service.root_dir
            / "apps"
            / "web"
            / "public"
            / "halldata-static"
            / str(entry.get("dataFile", ""))
        )
        save_summary.web_data_record_count = int(entry.get("recordCount") or 0)

    def _merge_persistence_summary(
        self,
        current_summary: PersistenceSummary | None,
        day_summary: PersistenceSummary,
    ) -> PersistenceSummary:
        if current_summary is None:
            return PersistenceSummary(
                local_file_path=day_summary.local_file_path,
                local_record_count=day_summary.local_record_count,
                supabase_saved=day_summary.supabase_saved,
                supabase_record_count=day_summary.supabase_record_count,
                web_data_saved=day_summary.web_data_saved,
                web_data_file_path=day_summary.web_data_file_path,
                web_data_record_count=day_summary.web_data_record_count,
                messages=list(day_summary.messages),
            )

        if day_summary.local_file_path:
            current_summary.local_file_path = day_summary.local_file_path
        current_summary.local_record_count += day_summary.local_record_count
        current_summary.supabase_saved = current_summary.supabase_saved or day_summary.supabase_saved
        current_summary.supabase_record_count += day_summary.supabase_record_count
        current_summary.web_data_saved = current_summary.web_data_saved or day_summary.web_data_saved
        if day_summary.web_data_file_path:
            current_summary.web_data_file_path = day_summary.web_data_file_path
        current_summary.web_data_record_count = max(
            current_summary.web_data_record_count,
            day_summary.web_data_record_count,
        )
        current_summary.messages.extend(day_summary.messages)
        return current_summary

    def _sort_records(
        self,
        records: list[object],
        value_getter: Callable[[object], object],
        descending: bool,
    ) -> list[object]:
        filled_records: list[object] = []
        blank_records: list[object] = []

        for record in records:
            value = value_getter(record)
            if self._is_blank_value(value):
                blank_records.append(record)
            else:
                filled_records.append(record)

        filled_records.sort(key=lambda record: self._sortable_value(value_getter(record)), reverse=descending)
        return filled_records + blank_records

    def _is_blank_value(self, value: object) -> bool:
        return str(value).strip() in {"", "-"}

    def _is_valid_url(self, value: str) -> bool:
        parsed = urlparse(value)
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)

    def _sortable_value(self, value: object) -> tuple[int, float | str]:
        text = str(value).strip()
        if text in {"", "-"}:
            return (2, "")

        if isinstance(value, int):
            return (0, float(value))

        normalized = text.replace(",", "").replace("台", "").replace("%", "")
        if re.fullmatch(r"-?\d+(?:\.\d+)?", normalized):
            return (0, float(normalized))

        ratio_match = re.fullmatch(r"(-?\d+(?:\.\d+)?)/(-?\d+(?:\.\d+)?)", text.replace(",", ""))
        if ratio_match:
            numerator = float(ratio_match.group(1))
            denominator = float(ratio_match.group(2))
            if denominator != 0:
                return (0, numerator / denominator)

        return (1, normalize_text(text))

    def _slot_sort_key(self, slot_number: str) -> tuple[int, int | str]:
        normalized = slot_number.replace(",", "").strip()
        if normalized.isdigit():
            return (0, int(normalized))
        return (1, normalize_text(slot_number))

    def _configure_widget_state(self, widget: object, state: str) -> None:
        try:
            if str(widget.cget("state")) == state:
                return
        except (AttributeError, tk.TclError):
            pass
        widget.configure(state=state)

    def _configure_named_widget_state(self, widget_name: str, state: str) -> None:
        widget = getattr(self, widget_name, None)
        if widget is None:
            return
        self._configure_widget_state(widget, state)

    def _update_button_states(self) -> None:
        registered_store_selection = (
            self.registered_store_tree.selection()
            if hasattr(self, "registered_store_tree")
            else ()
        )
        has_registered_store_row_selection = bool(registered_store_selection)
        has_single_registered_store_row_selection = len(registered_store_selection) == 1

        minrepo_busy = self._is_minrepo_busy()
        site7_busy = self._is_site7_busy()
        general_busy = self._is_general_busy()
        self._configure_widget_state(self.fetch_button, "disabled" if minrepo_busy or general_busy else "normal")
        can_cancel_fetch = (
            minrepo_busy
            and not self.minrepo_cancel_event.is_set()
        )
        self._configure_widget_state(self.cancel_fetch_button, "normal" if can_cancel_fetch else "disabled")
        self._configure_widget_state(self.target_date_entry, "normal")
        self._configure_widget_state(self.retry_delay_entry, "normal")
        if hasattr(self, "minrepo_fetch_mode_selector"):
            self._configure_widget_state(self.minrepo_fetch_mode_selector, "readonly")
        if hasattr(self, "fetch_order_region_selector"):
            self._configure_widget_state(self.fetch_order_region_selector, "readonly")
        web_publish_days_selected = normalize_web_publish_mode(self.web_publish_mode_var.get()) == WEB_PUBLISH_MODE_DAYS
        self._configure_widget_state(self.web_publish_days_radio, "normal")
        self._configure_widget_state(self.web_publish_store_radio, "normal")
        self._configure_widget_state(
            self.web_publish_interval_days_entry,
            "normal" if web_publish_days_selected else "disabled",
        )
        self._configure_widget_state(self.schedule_hour_entry, "normal")
        if hasattr(self, "minrepo_schedule_enabled_checkbutton"):
            self._configure_widget_state(self.minrepo_schedule_enabled_checkbutton, "normal")
        self._configure_widget_state(self.apply_schedule_button, "normal")
        self._configure_widget_state(self.clear_schedule_button, "normal")
        self._configure_widget_state(self.schedule_all_stores_interval_days_entry, "normal")
        self._configure_widget_state(self.apply_schedule_all_stores_button, "normal")
        self._configure_widget_state(self.notify_fetch_complete_button, "normal")
        self._configure_widget_state(
            self.site7_login_button,
            "disabled" if site7_busy or general_busy else "normal",
        )
        self._configure_widget_state(
            self.site7_fetch_button,
            "disabled" if site7_busy or general_busy else "normal",
        )
        self._configure_widget_state(
            self.site7_neo_im_fetch_button,
            "disabled" if site7_busy or general_busy else "normal",
        )
        can_cancel_site7_fetch = (
            site7_busy
            and not self.site7_cancel_event.is_set()
        )
        self._configure_widget_state(self.site7_cancel_button, "normal" if can_cancel_site7_fetch else "disabled")
        for hour_button in self.site7_schedule_hour_buttons.values():
            self._configure_widget_state(hour_button, "normal")
        if hasattr(self, "site7_schedule_enabled_checkbutton"):
            self._configure_widget_state(self.site7_schedule_enabled_checkbutton, "normal")
        self._configure_widget_state(self.apply_site7_schedule_button, "normal")
        self._configure_widget_state(self.clear_site7_schedule_button, "normal")
        self._configure_widget_state(self.site7_browser_visible_radio, "normal")
        self._configure_widget_state(self.site7_browser_hidden_radio, "normal")
        if hasattr(self, "site7_skip_juggler_difference_checkbutton"):
            self._configure_widget_state(self.site7_skip_juggler_difference_checkbutton, "normal")
        if hasattr(self, "site7_machine_checkbuttons"):
            for checkbutton in self.site7_machine_checkbuttons.values():
                self._configure_widget_state(checkbutton, "normal")
        if hasattr(self, "site7_machine_action_buttons"):
            for button in self.site7_machine_action_buttons:
                self._configure_widget_state(button, "normal")
        self._configure_named_widget_state("register_store_button", "disabled" if general_busy else "normal")
        self._configure_named_widget_state("register_store_url_entry", "normal")
        self._configure_named_widget_state("register_store_frequency_selector", "readonly")
        self._configure_named_widget_state("register_store_source_selector", "readonly")
        self._configure_named_widget_state("register_store_order_entry", "normal")
        self._configure_named_widget_state("register_store_site7_difference_checkbutton", "normal")
        self._configure_named_widget_state("register_store_prefecture_entry", "normal")
        self._configure_named_widget_state("register_store_area_entry", "normal")
        self._configure_named_widget_state("register_store_site7_store_name_entry", "normal")
        self._configure_named_widget_state("register_store_site7_hall_id_entry", "normal")
        self._configure_named_widget_state("register_store_site7_address_entry", "normal")
        self._configure_named_widget_state(
            "update_registered_store_button",
            "disabled" if general_busy or not has_single_registered_store_row_selection else "normal",
        )
        self._configure_named_widget_state("clear_register_store_form_button", "normal")
        self._configure_named_widget_state("registered_store_filter_entry", "normal")
        self._configure_named_widget_state("clear_registered_store_filter_button", "normal")
        self._configure_named_widget_state("refresh_registered_stores_button", "disabled" if general_busy else "normal")
        self._configure_named_widget_state("apply_my_hall_stores_button", "disabled" if general_busy else "normal")
        self._configure_named_widget_state(
            "delete_registered_stores_button",
            "disabled" if general_busy or not has_registered_store_row_selection else "normal",
        )

    def _show_error(self, exc: object) -> None:
        if isinstance(exc, ScraperError):
            message = str(exc)
        else:
            message = f"想定外のエラーが発生しました。\n{exc}"
        messagebox.showerror("取得失敗", message)


def main() -> None:
    root = tk.Tk()
    MinRepoApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
