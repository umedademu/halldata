from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
import unicodedata
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit, unquote

from bs4 import BeautifulSoup, Tag

from machine_difference import canonical_machine_name, list_equivalent_machine_names
from minrepo_scraper import FetchProgress, MachineDataset, MachineHistoryResult, ScraperError, StoreDatePage, normalize_text
from site7_scraper import (
    SITE7_DATE_BOUNDARY_HOUR,
    SITE7_MOBILE_USER_AGENT,
    SITE7_MOBILE_VIEWPORT,
    build_site7_transition_wait_milliseconds,
    format_site7_ratio_text,
    site7_dataset_updated_at,
    set_site7_dataset_updated_at,
)

try:
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import sync_playwright
except ImportError:  # pragma: no cover
    PlaywrightError = RuntimeError  # type: ignore[assignment]
    sync_playwright = None  # type: ignore[assignment]


ROOT_DIR = Path(__file__).resolve().parents[2]
DAIDATA_BEAM_HIKARI_STORE_NAME = "ビームヒカリ"
DAIDATA_BEAM_HIKARI_STORE_ID = "100619"
DAIDATA_BEAM_HIKARI_URL = f"https://daidata.goraggio.com/{DAIDATA_BEAM_HIKARI_STORE_ID}"
DAIDATA_WONDERLAND_SUE_STORE_NAME = "ワンダーランド須恵店"
DAIDATA_WONDERLAND_SUE_STORE_ID = "101221"
DAIDATA_WONDERLAND_SUE_URL = f"https://daidata.goraggio.com/{DAIDATA_WONDERLAND_SUE_STORE_ID}"
DAIDATA_BROWSER_STATE_DIR_NAME = "daidata_online_browser"
DAIDATA_JST = timezone(timedelta(hours=9))
DAIDATA_COLUMNS = ["台番", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率"]
DAIDATA_UPDATED_AT_PATTERN = re.compile(
    r"(\d{4})[./年](\d{1,2})[./月](\d{1,2})日?\s*(\d{1,2}):(\d{2})"
)
DAIDATA_FULL_DATE_PATTERN = re.compile(r"(\d{4})[./年-](\d{1,2})[./月-](\d{1,2})")
DAIDATA_SHORT_DATE_PATTERN = re.compile(r"(\d{1,2})[./月](\d{1,2})")
DAIDATA_ACCEPT_AUTO_WAIT_SECONDS = 60


@dataclass(frozen=True)
class DaidataOnlineMachineEntry:
    machine_name: str
    raw_machine_name: str
    url: str
    machine_count: int = 0


@dataclass(frozen=True)
class DaidataOnlineStoreConfig:
    store_name: str
    store_id: str
    name_keys: tuple[str, ...]
    aliases: tuple[str, ...] = ()

    @property
    def url(self) -> str:
        return f"https://daidata.goraggio.com/{self.store_id}"


DAIDATA_ONLINE_STORE_CONFIGS = (
    DaidataOnlineStoreConfig(
        store_name=DAIDATA_BEAM_HIKARI_STORE_NAME,
        store_id=DAIDATA_BEAM_HIKARI_STORE_ID,
        name_keys=("ビームヒカリ",),
        aliases=("beamhikari", "beambyhikari"),
    ),
    DaidataOnlineStoreConfig(
        store_name=DAIDATA_WONDERLAND_SUE_STORE_NAME,
        store_id=DAIDATA_WONDERLAND_SUE_STORE_ID,
        name_keys=("ワンダーランド須恵", "ワンダーランド須惠"),
        aliases=("wonderlandsue",),
    ),
)


def _daidata_store_name_key(value: str) -> str:
    return normalize_text(unicodedata.normalize("NFKC", str(value or "")).casefold())


def daidata_store_config_for(store_name: str, store_url: str = "") -> DaidataOnlineStoreConfig | None:
    compact_name = _daidata_store_name_key(store_name)
    decoded_url = unquote(str(store_url or "")).casefold()
    for config in DAIDATA_ONLINE_STORE_CONFIGS:
        if any(_daidata_store_name_key(name_key) in compact_name for name_key in config.name_keys):
            return config
        if compact_name in config.aliases:
            return config
        if "daidata.goraggio.com" in decoded_url and f"/{config.store_id}" in decoded_url:
            return config
    return None


def daidata_store_uses_daidata_online(store_name: str, store_url: str = "") -> bool:
    return daidata_store_config_for(store_name, store_url) is not None


def daidata_store_is_beam_hikari(store_name: str, store_url: str = "") -> bool:
    config = daidata_store_config_for(store_name, store_url)
    return config is not None and config.store_id == DAIDATA_BEAM_HIKARI_STORE_ID


def build_daidata_transition_wait_milliseconds(
    random_seconds_fn: Callable[[float, float], float] | None = None,
) -> int:
    return build_site7_transition_wait_milliseconds(random_seconds_fn)


def _raise_if_cancel_requested(cancel_requested: Callable[[], bool] | None) -> None:
    if cancel_requested is not None and cancel_requested():
        raise ScraperError("台データオンライン取得を中止しました。")


def _clean_machine_name(value: str) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).strip()
    text = text.split("|", 1)[0].strip()
    text = re.sub(r"\s+\d+(?:\.\d+)?\s*円\s*スロット.*$", "", text).strip()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[（(]\s*\d+\s*台?\s*[)）]\s*$", "", text).strip()
    text = re.sub(r"\s+\d+\s*台\s*$", "", text).strip()
    return text


def _enabled_machine_keys(enabled_machine_names: set[str] | None) -> set[str] | None:
    if enabled_machine_names is None:
        return None
    keys: set[str] = set()
    for machine_name in enabled_machine_names:
        for candidate_name in list_equivalent_machine_names(machine_name):
            key = normalize_text(candidate_name)
            if key:
                keys.add(key)
        canonical_name = canonical_machine_name(machine_name)
        key = normalize_text(canonical_name)
        if key:
            keys.add(key)
    return keys


def _machine_entry_keys(raw_machine_name: str, machine_name: str) -> set[str]:
    keys = {normalize_text(raw_machine_name), normalize_text(machine_name)}
    for candidate_name in list_equivalent_machine_names(machine_name):
        keys.add(normalize_text(candidate_name))
    return {key for key in keys if key}


def _machine_is_juggler(raw_machine_name: str, machine_name: str) -> bool:
    search_text = unicodedata.normalize("NFKC", f"{raw_machine_name} {machine_name}")
    return "ジャグラー" in search_text


def _value_has_data(value: str) -> bool:
    text = str(value or "").strip()
    return bool(text and text not in {"-", "--"})


def _page_requires_accept_terms(html: str, current_url: str = "") -> bool:
    parts = urlsplit(str(current_url or ""))
    if parts.netloc != "daidata.goraggio.com" or not parts.path.rstrip("/").endswith("/accept"):
        return False
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    return "利用規約に同意する" in text


def _clean_cell_text(value: str) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).strip()
    text = re.sub(r"\s+", "", text)
    return text or "-"


def _normalize_count_text(value: str) -> str:
    text = _clean_cell_text(value).replace(",", "")
    if text in {"", "-", "--"}:
        return "-"
    match = re.search(r"-?\d+", text)
    return match.group(0) if match else text


def _normalize_ratio_text(value: str) -> str:
    text = _clean_cell_text(value).replace(",", "")
    if text in {"", "-", "--"}:
        return "-"
    return format_site7_ratio_text(text)


def _column_index(headers: list[str], *keywords: str, require_all: bool = True) -> int | None:
    normalized_keywords = [normalize_text(keyword) for keyword in keywords]
    for index, header in enumerate(headers):
        normalized_header = normalize_text(header)
        if require_all:
            if all(keyword in normalized_header for keyword in normalized_keywords):
                return index
        elif any(keyword in normalized_header for keyword in normalized_keywords):
            return index
    return None


def _read_cells(row: Tag, cell_names: tuple[str, ...] = ("td", "th")) -> list[str]:
    return [cell.get_text(" ", strip=True) for cell in row.find_all(cell_names)]


def _parse_updated_datetime(html: str) -> datetime | None:
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    match = DAIDATA_UPDATED_AT_PATTERN.search(text)
    if match is None:
        return None
    year, month, day, hour, minute = (int(group) for group in match.groups())
    return datetime(year, month, day, hour, minute, tzinfo=DAIDATA_JST)


def _business_date_from_updated_at(updated_at: datetime | None, hist_num: int) -> str:
    base_time = updated_at.astimezone(DAIDATA_JST) if updated_at is not None else datetime.now(DAIDATA_JST)
    if base_time.hour < SITE7_DATE_BOUNDARY_HOUR:
        base_time -= timedelta(days=1)
    return (base_time.date() - timedelta(days=hist_num)).isoformat()


def _date_text_minus_days(date_text: str, days: int) -> str:
    try:
        base_date = datetime.strptime(str(date_text), "%Y-%m-%d").date()
    except ValueError:
        return ""
    return (base_date - timedelta(days=days)).isoformat()


def _parse_date_label(text: str, updated_at: datetime | None, hist_num: int) -> str | None:
    label = unicodedata.normalize("NFKC", str(text or "")).strip()
    match = DAIDATA_FULL_DATE_PATTERN.search(label)
    if match is not None:
        year, month, day = (int(group) for group in match.groups())
        return datetime(year, month, day, tzinfo=DAIDATA_JST).date().isoformat()

    match = DAIDATA_SHORT_DATE_PATTERN.search(label)
    if match is None:
        return None

    month, day = int(match.group(1)), int(match.group(2))
    base_date = (
        updated_at.astimezone(DAIDATA_JST).date()
        if updated_at is not None
        else datetime.now(DAIDATA_JST).date()
    )
    candidate = base_date.replace(month=month, day=day)
    if candidate > base_date + timedelta(days=31):
        candidate = candidate.replace(year=candidate.year - 1)
    return candidate.isoformat()


def _extract_selected_date(html: str, hist_num: int, updated_at: datetime | None) -> str:
    soup = BeautifulSoup(html, "html.parser")
    hist_num_text = str(hist_num)
    for option in soup.find_all("option"):
        value = str(option.get("value", "")).strip()
        if value == hist_num_text or (hist_num == 0 and option.has_attr("selected")):
            date_text = _parse_date_label(option.get_text(" ", strip=True), updated_at, hist_num)
            if date_text:
                return date_text
    return _business_date_from_updated_at(updated_at, hist_num)


def _with_hist_num(url: str, hist_num: int) -> str:
    parts = urlsplit(url)
    query = [(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True) if key != "hist_num"]
    query.append(("hist_num", str(hist_num)))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _daidata_day_is_fully_protected(
    target_date: str,
    slot_numbers: list[str],
    protected_slots: set[tuple[str, str]],
) -> bool:
    return bool(target_date and slot_numbers) and all(
        (target_date, slot_number) in protected_slots
        for slot_number in slot_numbers
    )


def _find_unit_table(soup: BeautifulSoup) -> Tag | None:
    for table in soup.find_all("table"):
        header_rows = table.find_all("tr")
        if not header_rows:
            continue
        headers = _read_cells(header_rows[0])
        header_key = " ".join(normalize_text(header) for header in headers).casefold()
        if "台番号" in header_key and "累計スタート" in header_key and "bb" in header_key and "rb" in header_key:
            return table
    return None


def build_daidata_machine_dataset(
    html: str,
    *,
    store_name: str,
    store_url: str,
    machine_name: str,
    machine_url: str,
    hist_num: int = 0,
) -> MachineDataset:
    soup = BeautifulSoup(html, "html.parser")
    updated_at = _parse_updated_datetime(html)
    target_date = _extract_selected_date(html, hist_num, updated_at)
    table = _find_unit_table(soup)
    rows: list[list[str]] = []

    if table is not None:
        table_rows = table.find_all("tr")
        headers = _read_cells(table_rows[0])
        slot_index = _column_index(headers, "台番号", require_all=False)
        games_index = _column_index(headers, "累計", "スタート")
        if games_index is None:
            games_index = _column_index(headers, "スタート", "回数")
        bb_index = _column_index(headers, "BB", require_all=False)
        rb_index = _column_index(headers, "RB", require_all=False)
        combined_index = _column_index(headers, "合成", require_all=False)
        bb_ratio_index = _column_index(headers, "BB", "確率")
        rb_ratio_index = _column_index(headers, "RB", "確率")

        for table_row in table_rows[1:]:
            cells = _read_cells(table_row, ("td",))
            if not cells:
                continue
            if slot_index is None or slot_index >= len(cells):
                continue

            slot_number = _normalize_count_text(cells[slot_index])
            if not slot_number or slot_number in {"-", "--"} or slot_number == "平均":
                continue

            games_count = _normalize_count_text(cells[games_index]) if games_index is not None and games_index < len(cells) else "-"
            bb_count = _normalize_count_text(cells[bb_index]) if bb_index is not None and bb_index < len(cells) else "-"
            rb_count = _normalize_count_text(cells[rb_index]) if rb_index is not None and rb_index < len(cells) else "-"
            combined_ratio = (
                _normalize_ratio_text(cells[combined_index])
                if combined_index is not None and combined_index < len(cells)
                else "-"
            )
            bb_ratio = (
                _normalize_ratio_text(cells[bb_ratio_index])
                if bb_ratio_index is not None and bb_ratio_index < len(cells)
                else "-"
            )
            rb_ratio = (
                _normalize_ratio_text(cells[rb_ratio_index])
                if rb_ratio_index is not None and rb_ratio_index < len(cells)
                else "-"
            )
            if not any(_value_has_data(value) for value in (games_count, bb_count, rb_count, combined_ratio, bb_ratio, rb_ratio)):
                continue

            rows.append(
                [
                    slot_number,
                    "-",
                    games_count,
                    "-",
                    bb_count,
                    rb_count,
                    combined_ratio,
                    bb_ratio,
                    rb_ratio,
                ]
            )

    dataset = MachineDataset(
        store_name=store_name,
        store_url=store_url,
        target_date=target_date,
        date_url=_with_hist_num(machine_url, hist_num),
        machine_name=machine_name,
        machine_url=machine_url,
        columns=list(DAIDATA_COLUMNS),
        rows=rows,
    )
    if updated_at is not None:
        set_site7_dataset_updated_at(dataset, updated_at)
    return dataset


class DaidataOnlineScraper:
    def __init__(self, root_dir: Path | None = None) -> None:
        self.root_dir = root_dir or ROOT_DIR
        self.browser_state_dir = self.root_dir / "local_data" / DAIDATA_BROWSER_STATE_DIR_NAME

    def fetch_beam_hikari_juggler_history(
        self,
        recent_days: int,
        browser_visible: bool = False,
        progress_callback: Callable[[FetchProgress], None] | None = None,
        enabled_machine_names: set[str] | None = None,
        machine_protected_slots_callback: Callable[
            [DaidataOnlineMachineEntry, list[str], list[str], str | None],
            set[tuple[str, str]],
        ] | None = None,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> MachineHistoryResult:
        return self.fetch_store_juggler_history(
            store_config=DAIDATA_ONLINE_STORE_CONFIGS[0],
            recent_days=recent_days,
            browser_visible=browser_visible,
            progress_callback=progress_callback,
            enabled_machine_names=enabled_machine_names,
            machine_protected_slots_callback=machine_protected_slots_callback,
            cancel_requested=cancel_requested,
        )

    def fetch_store_juggler_history(
        self,
        *,
        store_config: DaidataOnlineStoreConfig,
        recent_days: int,
        browser_visible: bool = False,
        progress_callback: Callable[[FetchProgress], None] | None = None,
        enabled_machine_names: set[str] | None = None,
        machine_protected_slots_callback: Callable[
            [DaidataOnlineMachineEntry, list[str], list[str], str | None],
            set[tuple[str, str]],
        ] | None = None,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> MachineHistoryResult:
        target_days = max(1, min(int(recent_days), 8))
        self._require_playwright()
        _raise_if_cancel_requested(cancel_requested)
        self._notify_progress(progress_callback, 0, 1, f"{store_config.store_name}の台データオンラインへ接続しています")

        playwright = None
        context = None
        machine_results: list[MachineHistoryResult] = []
        try:
            playwright, context = self._launch_mobile_browser_context(browser_visible=browser_visible)
            page = self._prepare_page(context)
            self._goto_daidata_page(
                page,
                store_config.url,
                browser_visible=browser_visible,
                progress_callback=progress_callback,
                cancel_requested=cancel_requested,
            )
            _raise_if_cancel_requested(cancel_requested)
            list_url = urljoin(f"{store_config.url}/", "list?mode=psModelNameSearch&ps=S")
            self._goto_daidata_page(
                page,
                list_url,
                browser_visible=browser_visible,
                progress_callback=progress_callback,
                cancel_requested=cancel_requested,
            )
            machine_list_html = page.content()
            machine_entries = self.extract_juggler_machine_links(
                machine_list_html,
                list_url,
                enabled_machine_names=enabled_machine_names,
            )
            if not machine_entries:
                raise ScraperError(f"台データオンラインで{store_config.store_name}のジャグラー系機種が見つかりませんでした。")

            total_steps = len(machine_entries) + 1
            for machine_index, machine_entry in enumerate(machine_entries, start=1):
                _raise_if_cancel_requested(cancel_requested)
                self._notify_progress(
                    progress_callback,
                    machine_index,
                    total_steps,
                    f"{store_config.store_name} / {machine_entry.machine_name} の台データオンラインを読んでいます",
                )
                machine_results.append(
                    self._fetch_machine_history_result(
                        store_config=store_config,
                        page=page,
                        machine_entry=machine_entry,
                        recent_days=target_days,
                        browser_visible=browser_visible,
                        progress_callback=progress_callback,
                        machine_protected_slots_callback=machine_protected_slots_callback,
                        cancel_requested=cancel_requested,
                    )
                )
        except PlaywrightError as exc:
            raise ScraperError(f"台データオンラインの取得に失敗しました。\n{exc}") from exc
        finally:
            self._release_browser_context(playwright, context, browser_visible=browser_visible)

        return self._merge_machine_history_results(store_config, machine_results)

    def extract_juggler_machine_links(
        self,
        html: str,
        base_url: str,
        *,
        enabled_machine_names: set[str] | None = None,
    ) -> list[DaidataOnlineMachineEntry]:
        soup = BeautifulSoup(html, "html.parser")
        enabled_keys = _enabled_machine_keys(enabled_machine_names)
        entries: list[DaidataOnlineMachineEntry] = []
        seen_machine_names: set[str] = set()

        for link in soup.find_all("a", href=True):
            href = str(link.get("href", "")).strip()
            if "unit_list" not in href:
                continue

            raw_machine_name = _clean_machine_name(link.get_text(" ", strip=True))
            if not raw_machine_name:
                continue
            machine_name = canonical_machine_name(raw_machine_name)
            if not _machine_is_juggler(raw_machine_name, machine_name):
                continue
            if enabled_keys is not None and not (_machine_entry_keys(raw_machine_name, machine_name) & enabled_keys):
                continue
            if machine_name in seen_machine_names:
                continue
            seen_machine_names.add(machine_name)
            entries.append(
                DaidataOnlineMachineEntry(
                    machine_name=machine_name,
                    raw_machine_name=raw_machine_name,
                    url=urljoin(base_url, href),
                    machine_count=self._extract_machine_count(link.get_text(" ", strip=True)),
                )
            )

        return entries

    def _fetch_machine_history_result(
        self,
        *,
        store_config: DaidataOnlineStoreConfig = DAIDATA_ONLINE_STORE_CONFIGS[0],
        page: object,
        machine_entry: DaidataOnlineMachineEntry,
        recent_days: int,
        browser_visible: bool,
        progress_callback: Callable[[FetchProgress], None] | None,
        machine_protected_slots_callback: Callable[
            [DaidataOnlineMachineEntry, list[str], list[str], str | None],
            set[tuple[str, str]],
        ] | None = None,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> MachineHistoryResult:
        datasets: list[MachineDataset] = []
        date_pages: list[StoreDatePage] = []
        skipped_targets: list[tuple[str, str]] = []
        skipped_dates: list[str] = []

        first_target_url = _with_hist_num(machine_entry.url, 0)
        self._goto_daidata_page(
            page,
            first_target_url,
            browser_visible=browser_visible,
            progress_callback=progress_callback,
            cancel_requested=cancel_requested,
        )
        first_html = page.content()
        first_dataset = build_daidata_machine_dataset(
            first_html,
            store_name=store_config.store_name,
            store_url=store_config.url,
            machine_name=machine_entry.machine_name,
            machine_url=machine_entry.url,
            hist_num=0,
        )
        target_dates = [
            _date_text_minus_days(first_dataset.target_date, hist_num)
            for hist_num in range(recent_days)
        ]
        first_day_slot_numbers = self._dataset_slot_numbers(first_dataset)
        protected_slots: set[tuple[str, str]] = set()
        if machine_protected_slots_callback is not None and first_day_slot_numbers:
            protected_slots = set(
                machine_protected_slots_callback(
                    machine_entry,
                    target_dates,
                    first_day_slot_numbers,
                    site7_dataset_updated_at(first_dataset) or None,
                )
            )

        for hist_num in range(recent_days):
            _raise_if_cancel_requested(cancel_requested)
            target_date = target_dates[hist_num] if hist_num < len(target_dates) else ""
            target_url = _with_hist_num(machine_entry.url, hist_num)
            if _daidata_day_is_fully_protected(target_date, first_day_slot_numbers, protected_slots):
                skipped_targets.append((target_date, machine_entry.machine_name))
                if target_date not in skipped_dates:
                    skipped_dates.append(target_date)
                continue

            if hist_num == 0:
                dataset = first_dataset
            else:
                self._goto_daidata_page(
                    page,
                    target_url,
                    browser_visible=browser_visible,
                    progress_callback=progress_callback,
                    cancel_requested=cancel_requested,
                )
                html = page.content()
                dataset = build_daidata_machine_dataset(
                    html,
                    store_name=store_config.store_name,
                    store_url=store_config.url,
                    machine_name=machine_entry.machine_name,
                    machine_url=machine_entry.url,
                    hist_num=hist_num,
                )
            if _daidata_day_is_fully_protected(dataset.target_date, first_day_slot_numbers, protected_slots):
                skipped_targets.append((dataset.target_date, machine_entry.machine_name))
                if dataset.target_date not in skipped_dates:
                    skipped_dates.append(dataset.target_date)
                continue
            if dataset.rows:
                datasets.append(dataset)
                date_pages.append(StoreDatePage(target_date=dataset.target_date, date_url=dataset.date_url))
            else:
                skipped_targets.append((dataset.target_date, machine_entry.machine_name))

        candidate_dates = [date_page.target_date for date_page in date_pages] or [target_date for target_date, _ in skipped_targets]
        start_date = min(candidate_dates) if candidate_dates else ""
        end_date = max(candidate_dates) if candidate_dates else ""
        return MachineHistoryResult(
            store_name=store_config.store_name,
            store_url=store_config.url,
            start_date=start_date,
            end_date=end_date,
            date_pages=date_pages,
            datasets=datasets,
            skipped_targets=skipped_targets,
            skipped_dates=skipped_dates,
        )

    def _merge_machine_history_results(
        self,
        store_config: DaidataOnlineStoreConfig,
        machine_results: list[MachineHistoryResult],
    ) -> MachineHistoryResult:
        datasets: list[MachineDataset] = []
        date_pages_by_date: dict[str, StoreDatePage] = {}
        skipped_targets: list[tuple[str, str]] = []
        skipped_dates: list[str] = []

        for machine_result in machine_results:
            datasets.extend(machine_result.datasets)
            skipped_targets.extend(machine_result.skipped_targets)
            skipped_dates.extend(date for date in machine_result.skipped_dates if date not in skipped_dates)
            for date_page in machine_result.date_pages:
                date_pages_by_date.setdefault(date_page.target_date, date_page)

        date_pages = sorted(date_pages_by_date.values(), key=lambda date_page: date_page.target_date)
        datasets.sort(key=lambda dataset: (dataset.target_date, dataset.machine_name.casefold()))
        if not datasets and not skipped_dates:
            raise ScraperError(f"台データオンラインで{store_config.store_name}の台データが見つかりませんでした。")

        candidate_dates = [date_page.target_date for date_page in date_pages] or [target_date for target_date, _ in skipped_targets]
        return MachineHistoryResult(
            store_name=store_config.store_name,
            store_url=store_config.url,
            start_date=min(candidate_dates) if candidate_dates else "",
            end_date=max(candidate_dates) if candidate_dates else "",
            date_pages=date_pages,
            datasets=datasets,
            skipped_targets=skipped_targets,
            skipped_dates=skipped_dates,
        )

    def _dataset_slot_numbers(self, dataset: MachineDataset) -> list[str]:
        try:
            slot_index = next(
                index
                for index, column_name in enumerate(dataset.columns)
                if normalize_text(column_name) == normalize_text("台番")
            )
        except StopIteration:
            return []

        slot_numbers = [
            str(row[slot_index]).strip()
            for row in dataset.rows
            if slot_index < len(row) and str(row[slot_index]).strip()
        ]
        return sorted(set(slot_numbers), key=lambda value: int(value) if value.isdigit() else value)

    def _launch_mobile_browser_context(self, browser_visible: bool) -> tuple[object, object]:
        playwright = sync_playwright().start()
        context = playwright.chromium.launch_persistent_context(
            str(self.browser_state_dir),
            headless=not browser_visible,
            locale="ja-JP",
            viewport=SITE7_MOBILE_VIEWPORT,
            user_agent=SITE7_MOBILE_USER_AGENT,
            is_mobile=True,
            has_touch=True,
        )
        return playwright, context

    def _prepare_page(self, context: object) -> object:
        try:
            pages = list(context.pages)
        except Exception:  # noqa: BLE001
            pages = []
        return pages[-1] if pages else context.new_page()

    def _goto_daidata_page(
        self,
        page: object,
        target_url: str,
        *,
        browser_visible: bool,
        progress_callback: Callable[[FetchProgress], None] | None,
        cancel_requested: Callable[[], bool] | None,
    ) -> None:
        page.goto(target_url, wait_until="domcontentloaded", timeout=60_000)
        accepted_terms = self._wait_for_accept_terms_if_needed(
            page,
            browser_visible=browser_visible,
            progress_callback=progress_callback,
            cancel_requested=cancel_requested,
        )
        if accepted_terms:
            _raise_if_cancel_requested(cancel_requested)
            page.goto(target_url, wait_until="domcontentloaded", timeout=60_000)
            self._wait_for_accept_terms_if_needed(
                page,
                browser_visible=browser_visible,
                progress_callback=progress_callback,
                cancel_requested=cancel_requested,
            )
        self._wait_between_transitions(page, cancel_requested=cancel_requested)

    def _wait_for_accept_terms_if_needed(
        self,
        page: object,
        *,
        browser_visible: bool,
        progress_callback: Callable[[FetchProgress], None] | None,
        cancel_requested: Callable[[], bool] | None,
    ) -> bool:
        html = page.content()
        if not _page_requires_accept_terms(html, str(page.url)):
            return False

        self._notify_progress(
            progress_callback,
            0,
            1,
            "台データオンラインの利用規約画面で自動同意しています",
        )
        self._click_accept_terms_button(page)

        deadline = datetime.now(DAIDATA_JST) + timedelta(seconds=DAIDATA_ACCEPT_AUTO_WAIT_SECONDS)
        while datetime.now(DAIDATA_JST) < deadline:
            _raise_if_cancel_requested(cancel_requested)
            html = page.content()
            if not _page_requires_accept_terms(html, str(page.url)):
                return True
            page.wait_for_timeout(500)
        return True

    def _click_accept_terms_button(self, page: object) -> None:
        button_locator = self._find_accept_terms_button(page)
        if button_locator is None:
            raise ScraperError("台データオンラインの利用規約画面で同意ボタンが見つかりませんでした。")

        try:
            button_locator.click(timeout=5_000)
            try:
                page.wait_for_load_state("domcontentloaded", timeout=60_000)
            except Exception:  # noqa: BLE001
                pass
            page.wait_for_timeout(500)
        except Exception as exc:  # noqa: BLE001
            raise ScraperError("台データオンラインの利用規約画面で同意ボタンを押せませんでした。") from exc

    def _wait_between_transitions(self, page: object, cancel_requested: Callable[[], bool] | None = None) -> None:
        remaining_milliseconds = build_daidata_transition_wait_milliseconds()
        while remaining_milliseconds > 0:
            _raise_if_cancel_requested(cancel_requested)
            wait_milliseconds = min(100, remaining_milliseconds)
            page.wait_for_timeout(wait_milliseconds)
            remaining_milliseconds -= wait_milliseconds
        _raise_if_cancel_requested(cancel_requested)

    def _find_accept_terms_button(self, page: object) -> object | None:
        locator_builders = (
            lambda: page.locator("button").filter(has_text="利用規約に同意する").first,
            lambda: page.locator("input[type='submit'][value='利用規約に同意する']").first,
            lambda: page.locator("input[type='button'][value='利用規約に同意する']").first,
        )
        for build_locator in locator_builders:
            try:
                locator = build_locator()
                if locator.count() > 0 and locator.is_visible():
                    return locator
            except Exception:  # noqa: BLE001
                continue
        return None

    def _release_browser_context(self, playwright: object, context: object, *, browser_visible: bool) -> None:
        if context is not None:
            try:
                context.close()
            except Exception:  # noqa: BLE001
                pass
        if playwright is not None:
            try:
                playwright.stop()
            except Exception:  # noqa: BLE001
                pass

    def _notify_progress(
        self,
        progress_callback: Callable[[FetchProgress], None] | None,
        current_step: int,
        total_steps: int,
        message: str,
    ) -> None:
        if progress_callback is None:
            return
        progress_callback(
            FetchProgress(
                current_step=current_step,
                total_steps=max(1, total_steps),
                message=message,
            )
        )

    def _require_playwright(self) -> None:
        if sync_playwright is None:
            raise ScraperError("Playwright が見つかりません。`pip install playwright` を実行してください。")

    def _extract_machine_count(self, text: str) -> int:
        match = re.search(r"(\d+)\s*台", unicodedata.normalize("NFKC", str(text or "")))
        return int(match.group(1)) if match is not None else 0
