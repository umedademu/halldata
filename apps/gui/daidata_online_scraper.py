from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import unicodedata
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit, unquote

from bs4 import BeautifulSoup, Tag

from machine_difference import canonical_machine_name, list_equivalent_machine_names
from minrepo_scraper import (
    FetchProgress,
    MachineDataset,
    MachineHistoryResult,
    ScraperError,
    StoreDatePage,
    StoreDayStatus,
    normalize_text,
)
from site7_scraper import (
    SITE7_DATE_BOUNDARY_HOUR,
    SITE7_STORE_CLOSED_CHECK_HOUR,
    SITE7_STORE_CLOSED_CHECK_MINUTE,
    SITE7_STORE_CLOSED_STALE_UPDATE_HOUR,
    SITE7_STORE_DAY_STATUS_CLOSED,
    SITE7_MOBILE_USER_AGENT,
    SITE7_MOBILE_VIEWPORT,
    Site7NoPlayDayStats,
    build_site7_transition_wait_milliseconds,
    format_site7_updated_datetime,
    format_site7_ratio_text,
    site7_dataset_updated_at,
    site7_result_no_play_day_stats,
    set_site7_dataset_updated_at,
    set_site7_result_no_play_day_stats,
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
DAIDATA_WONDERLAND_MINAMIGAOKA_STORE_NAME = "ワンダーランド南ヶ丘店"
DAIDATA_WONDERLAND_MINAMIGAOKA_STORE_ID = "101220"
DAIDATA_WONDERLAND_MINAMIGAOKA_URL = f"https://daidata.goraggio.com/{DAIDATA_WONDERLAND_MINAMIGAOKA_STORE_ID}"
DAIDATA_BROWSER_STATE_DIR_NAME = "daidata_online_browser"
DAIDATA_AT_COUNTER_RULES_FILE_NAME = "daidata_online_at_counter_rules.json"
DAIDATA_JST = timezone(timedelta(hours=9))
DAIDATA_COLUMNS = ["台番", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率"]
DAIDATA_AT_COLUMNS = ["台番", "AT", "AT表示枠", "AT取得元", "AT取得日時"]
DAIDATA_AT_SOURCE = "daidata_online"
DAIDATA_AT_RATE_KEY_20_YEN = "20yen"
DAIDATA_AT_DECISION_TO_BB = "at_to_bb"
DAIDATA_AT_DECISION_TO_RB = "at_to_rb"
DAIDATA_AT_DECISION_IGNORE_THREE_COUNTERS = "ignore_three_counters"
DAIDATA_AT_DECISION_NO_AT = "no_at"
DAIDATA_AT_DECISION_UNKNOWN = "unknown"
DAIDATA_AT_DECISIONS = frozenset(
    {
        DAIDATA_AT_DECISION_TO_BB,
        DAIDATA_AT_DECISION_TO_RB,
        DAIDATA_AT_DECISION_IGNORE_THREE_COUNTERS,
        DAIDATA_AT_DECISION_NO_AT,
        DAIDATA_AT_DECISION_UNKNOWN,
    }
)
DAIDATA_AT_SKIP_DETAIL_DECISIONS = frozenset(
    {
        DAIDATA_AT_DECISION_NO_AT,
    }
)
DAIDATA_AT_MAX_RECENT_DAYS = 8
DAIDATA_AT_STANDARD_SLOT_PRICE_MIN = 15.0
DAIDATA_AT_STANDARD_SLOT_PRICE_MAX = 25.0
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
    ball_price: float | None = None
    rate_key: str = ""


@dataclass(frozen=True)
class DaidataOnlineUnitEntry:
    slot_number: str
    url: str


@dataclass(frozen=True)
class DaidataWeeklyCounterRow:
    target_date: str
    bb_count: int
    rb_count: int
    at_count: int


@dataclass(frozen=True)
class DaidataWeeklyCounterGraph:
    graph_found: bool
    bb_series_present: bool
    rb_series_present: bool
    at_series_present: bool
    bb_positive: bool
    rb_positive: bool
    at_positive: bool
    rows: tuple[DaidataWeeklyCounterRow, ...] = ()


@dataclass(frozen=True)
class DaidataAtCounterEvidence:
    graph_found: bool = False
    at_series_present: bool = False
    bb_positive: bool = False
    rb_positive: bool = False
    at_positive: bool = False


@dataclass(frozen=True)
class DaidataAtDetailObservation:
    slot_number: str
    detail_url: str
    fetched_at: str
    graph: DaidataWeeklyCounterGraph


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
    DaidataOnlineStoreConfig(
        store_name=DAIDATA_WONDERLAND_MINAMIGAOKA_STORE_NAME,
        store_id=DAIDATA_WONDERLAND_MINAMIGAOKA_STORE_ID,
        name_keys=("ワンダーランド南ヶ丘", "ワンダーランド南ケ丘", "ワンダーランド南が丘"),
        aliases=("wonderlandminamigaoka",),
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


def normalize_daidata_at_machine_name(machine_name: str) -> str:
    canonical_name = canonical_machine_name(unicodedata.normalize("NFKC", str(machine_name or "")))
    return normalize_text(canonical_name).casefold()


def build_daidata_at_rule_key(
    store_id: str,
    machine_name: str,
    rate_key: str = DAIDATA_AT_RATE_KEY_20_YEN,
) -> str:
    return "|".join(
        (
            str(store_id or "").strip(),
            normalize_daidata_at_machine_name(machine_name),
            str(rate_key or "").strip(),
        )
    )


def _parse_daidata_ball_price(machine_url: str, link_text: str = "") -> float | None:
    query = dict(parse_qsl(urlsplit(str(machine_url or "")).query, keep_blank_values=True))
    candidates = [query.get("ballPrice", "")]
    normalized_link_text = unicodedata.normalize("NFKC", str(link_text or ""))
    match = re.search(r"(\d+(?:\.\d+)?)\s*円\s*スロット", normalized_link_text)
    if match is not None:
        candidates.append(match.group(1))

    for candidate in candidates:
        text = unicodedata.normalize("NFKC", str(candidate or "")).replace(",", "").strip()
        if not text:
            continue
        try:
            return float(text)
        except ValueError:
            continue
    return None


def daidata_ball_price_is_twenty_yen_equivalent(ball_price: float | None) -> bool:
    if ball_price is None:
        return False
    return DAIDATA_AT_STANDARD_SLOT_PRICE_MIN <= ball_price <= DAIDATA_AT_STANDARD_SLOT_PRICE_MAX


def daidata_at_counter_evidence_from_graph(
    graph: DaidataWeeklyCounterGraph,
) -> DaidataAtCounterEvidence:
    return DaidataAtCounterEvidence(
        graph_found=graph.graph_found,
        at_series_present=graph.at_series_present,
        bb_positive=graph.bb_positive,
        rb_positive=graph.rb_positive,
        at_positive=graph.at_positive,
    )


def merge_daidata_at_counter_evidence(
    evidences: Iterable[DaidataAtCounterEvidence],
) -> DaidataAtCounterEvidence:
    graph_found = False
    at_series_present = False
    bb_positive = False
    rb_positive = False
    at_positive = False
    for evidence in evidences:
        graph_found = graph_found or bool(evidence.graph_found)
        at_series_present = at_series_present or bool(evidence.at_series_present)
        bb_positive = bb_positive or bool(evidence.bb_positive)
        rb_positive = rb_positive or bool(evidence.rb_positive)
        at_positive = at_positive or bool(evidence.at_positive)
    return DaidataAtCounterEvidence(
        graph_found=graph_found,
        at_series_present=at_series_present,
        bb_positive=bb_positive,
        rb_positive=rb_positive,
        at_positive=at_positive,
    )


def classify_daidata_at_counter_usage(evidence: DaidataAtCounterEvidence) -> str:
    if not evidence.graph_found:
        return DAIDATA_AT_DECISION_UNKNOWN
    if not evidence.at_series_present:
        return DAIDATA_AT_DECISION_NO_AT
    if not evidence.at_positive:
        return DAIDATA_AT_DECISION_UNKNOWN
    if evidence.bb_positive and evidence.rb_positive:
        return DAIDATA_AT_DECISION_IGNORE_THREE_COUNTERS
    if not evidence.bb_positive and evidence.rb_positive:
        return DAIDATA_AT_DECISION_TO_BB
    if evidence.bb_positive and not evidence.rb_positive:
        return DAIDATA_AT_DECISION_TO_RB
    return DAIDATA_AT_DECISION_TO_BB


def daidata_at_display_slot(decision: str) -> str:
    if decision == DAIDATA_AT_DECISION_TO_BB:
        return "bb"
    if decision == DAIDATA_AT_DECISION_TO_RB:
        return "rb"
    if decision == DAIDATA_AT_DECISION_IGNORE_THREE_COUNTERS:
        return "ignore"
    return "unknown"


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
    try:
        candidate = base_date.replace(month=month, day=day)
    except ValueError:
        return None
    if candidate > base_date + timedelta(days=31):
        candidate = candidate.replace(year=candidate.year - 1)
    return candidate.isoformat()


def _extract_balanced_javascript_array(script_text: str, assignment_end: int) -> str:
    array_start = script_text.find("[", assignment_end)
    if array_start < 0:
        return ""

    depth = 0
    quote = ""
    escaped = False
    for index in range(array_start, len(script_text)):
        character = script_text[index]
        if quote:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                quote = ""
            continue
        if character in {'"', "'"}:
            quote = character
            continue
        if character == "[":
            depth += 1
        elif character == "]":
            depth -= 1
            if depth == 0:
                return script_text[array_start : index + 1]
    return ""


def _parse_weekly_counter_series(
    raw_series: object,
    reference_datetime: datetime,
) -> dict[str, int]:
    if not isinstance(raw_series, list):
        return {}
    parsed: dict[str, int] = {}
    for raw_point in raw_series:
        if not isinstance(raw_point, (list, tuple)) or len(raw_point) < 2:
            continue
        target_date = _parse_date_label(str(raw_point[0]), reference_datetime, 0)
        count = _parse_daidata_count(raw_point[1])
        if target_date and count is not None and count >= 0:
            parsed[target_date] = count
    return parsed


def parse_daidata_weekly_counter_graph(
    html: str,
    *,
    reference_datetime: datetime | None = None,
) -> DaidataWeeklyCounterGraph:
    reference = reference_datetime or _parse_updated_datetime(html) or datetime.now(DAIDATA_JST)
    if reference.tzinfo is None or reference.utcoffset() is None:
        reference = reference.replace(tzinfo=DAIDATA_JST)
    else:
        reference = reference.astimezone(DAIDATA_JST)

    soup = BeautifulSoup(html, "html.parser")
    payload: list[object] | None = None
    assignment_pattern = re.compile(r"\b(?:const|let|var)\s+data\s*=")
    for script in soup.find_all("script"):
        script_text = script.string if script.string is not None else script.get_text("\n", strip=False)
        if "weekly-jackpot-1" not in script_text:
            continue
        for assignment_match in assignment_pattern.finditer(script_text):
            raw_array = _extract_balanced_javascript_array(script_text, assignment_match.end())
            if not raw_array:
                continue
            try:
                candidate = json.loads(re.sub(r",\s*([\]}])", r"\1", raw_array))
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(candidate, list):
                payload = candidate
        if payload is not None:
            break

    if payload is None:
        return DaidataWeeklyCounterGraph(
            graph_found=False,
            bb_series_present=False,
            rb_series_present=False,
            at_series_present=False,
            bb_positive=False,
            rb_positive=False,
            at_positive=False,
        )

    bb_by_date = _parse_weekly_counter_series(payload[0], reference) if len(payload) >= 1 else {}
    rb_by_date = _parse_weekly_counter_series(payload[1], reference) if len(payload) >= 2 else {}
    at_by_date = _parse_weekly_counter_series(payload[2], reference) if len(payload) >= 3 else {}
    target_dates = sorted(
        set(at_by_date)
        if at_by_date
        else set(bb_by_date) | set(rb_by_date)
    )
    rows = tuple(
        DaidataWeeklyCounterRow(
            target_date=target_date,
            bb_count=bb_by_date.get(target_date, 0),
            rb_count=rb_by_date.get(target_date, 0),
            at_count=at_by_date.get(target_date, 0),
        )
        for target_date in target_dates
    )
    return DaidataWeeklyCounterGraph(
        graph_found=True,
        bb_series_present=bool(bb_by_date),
        rb_series_present=bool(rb_by_date),
        at_series_present=bool(at_by_date),
        bb_positive=any(value > 0 for value in bb_by_date.values()),
        rb_positive=any(value > 0 for value in rb_by_date.values()),
        at_positive=any(value > 0 for value in at_by_date.values()),
        rows=rows,
    )


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


def _parse_daidata_updated_at(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=DAIDATA_JST)
    return parsed.astimezone(DAIDATA_JST)


def _parse_daidata_count(value: object) -> int | None:
    text = str(value or "").strip().replace(",", "")
    if text in {"", "-", "--"}:
        return 0
    match = re.search(r"-?\d+", text)
    if match is None:
        return None
    return int(match.group(0))


def _daidata_stat_value_is_empty_or_zero(value: object) -> bool:
    return _parse_daidata_count(value) == 0


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


def daidata_at_counter_evidence_from_rule(rule: object) -> DaidataAtCounterEvidence:
    if not isinstance(rule, dict):
        return DaidataAtCounterEvidence()
    decision = str(rule.get("decision", "")).strip()
    evidence = DaidataAtCounterEvidence(
        graph_found=bool(rule.get("graph_found", False)),
        at_series_present=bool(rule.get("at_series_present", False)),
        bb_positive=bool(rule.get("bb_positive", False)),
        rb_positive=bool(rule.get("rb_positive", False)),
        at_positive=bool(rule.get("at_positive", False)),
    )
    if any(
        (
            evidence.graph_found,
            evidence.at_series_present,
            evidence.bb_positive,
            evidence.rb_positive,
            evidence.at_positive,
        )
    ):
        return evidence
    if decision == DAIDATA_AT_DECISION_NO_AT:
        return DaidataAtCounterEvidence(graph_found=True)
    if decision == DAIDATA_AT_DECISION_TO_BB:
        return DaidataAtCounterEvidence(
            graph_found=True,
            at_series_present=True,
            rb_positive=True,
            at_positive=True,
        )
    if decision == DAIDATA_AT_DECISION_TO_RB:
        return DaidataAtCounterEvidence(
            graph_found=True,
            at_series_present=True,
            bb_positive=True,
            at_positive=True,
        )
    if decision == DAIDATA_AT_DECISION_IGNORE_THREE_COUNTERS:
        return DaidataAtCounterEvidence(
            graph_found=True,
            at_series_present=True,
            bb_positive=True,
            rb_positive=True,
            at_positive=True,
        )
    return evidence


def build_daidata_at_counter_rule(
    *,
    store_id: str,
    machine_name: str,
    rate_key: str,
    evidence: DaidataAtCounterEvidence,
    updated_at: str,
) -> dict[str, object]:
    return {
        "store_id": str(store_id or "").strip(),
        "machine_name": canonical_machine_name(machine_name),
        "machine_name_key": normalize_daidata_at_machine_name(machine_name),
        "rate_key": str(rate_key or "").strip(),
        "decision": classify_daidata_at_counter_usage(evidence),
        "graph_found": evidence.graph_found,
        "at_series_present": evidence.at_series_present,
        "bb_positive": evidence.bb_positive,
        "rb_positive": evidence.rb_positive,
        "at_positive": evidence.at_positive,
        "updated_at": str(updated_at or "").strip(),
    }


def build_daidata_at_supplement_history_result(
    *,
    store_name: str,
    store_url: str,
    machine_name: str,
    machine_url: str,
    observations: Iterable[DaidataAtDetailObservation],
    decision: str,
    recent_days: int,
) -> MachineHistoryResult:
    target_days = max(1, min(int(recent_days), DAIDATA_AT_MAX_RECENT_DAYS))
    display_slot = daidata_at_display_slot(decision)
    observations_list = list(observations)
    all_target_dates = sorted(
        {
            row.target_date
            for observation in observations_list
            if observation.graph.at_series_present
            for row in observation.graph.rows
        },
        reverse=True,
    )
    selected_dates = set(all_target_dates[:target_days])
    rows_by_date: dict[str, dict[str, list[str]]] = {}
    detail_url_by_date: dict[str, str] = {}

    for observation in observations_list:
        if not observation.graph.at_series_present:
            continue
        for graph_row in observation.graph.rows:
            if graph_row.target_date not in selected_dates:
                continue
            rows_by_slot = rows_by_date.setdefault(graph_row.target_date, {})
            rows_by_slot[observation.slot_number] = [
                observation.slot_number,
                str(graph_row.at_count),
                display_slot,
                DAIDATA_AT_SOURCE,
                observation.fetched_at,
            ]
            detail_url_by_date.setdefault(graph_row.target_date, observation.detail_url)

    datasets: list[MachineDataset] = []
    date_pages: list[StoreDatePage] = []
    for target_date in sorted(rows_by_date):
        rows_by_slot = rows_by_date[target_date]
        rows = sorted(
            rows_by_slot.values(),
            key=lambda row: int(row[0]) if row[0].isdigit() else row[0],
        )
        if not rows:
            continue
        date_url = detail_url_by_date.get(target_date, machine_url)
        datasets.append(
            MachineDataset(
                store_name=store_name,
                store_url=store_url,
                target_date=target_date,
                date_url=date_url,
                machine_name=machine_name,
                machine_url=machine_url,
                columns=list(DAIDATA_AT_COLUMNS),
                rows=rows,
            )
        )
        date_pages.append(StoreDatePage(target_date=target_date, date_url=date_url))

    return MachineHistoryResult(
        store_name=store_name,
        store_url=store_url,
        start_date=min(rows_by_date) if rows_by_date else "",
        end_date=max(rows_by_date) if rows_by_date else "",
        date_pages=date_pages,
        datasets=datasets,
    )


class DaidataOnlineScraper:
    def __init__(
        self,
        root_dir: Path | None = None,
        current_datetime_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self.root_dir = root_dir or ROOT_DIR
        self.browser_state_dir = self.root_dir / "local_data" / DAIDATA_BROWSER_STATE_DIR_NAME
        self.at_counter_rules_path = self.root_dir / "local_data" / DAIDATA_AT_COUNTER_RULES_FILE_NAME
        self._current_datetime_fn = current_datetime_fn

    def _current_daidata_datetime(self) -> datetime:
        current_datetime = (
            self._current_datetime_fn()
            if self._current_datetime_fn is not None
            else datetime.now(DAIDATA_JST)
        )
        if current_datetime.tzinfo is None or current_datetime.utcoffset() is None:
            return current_datetime.replace(tzinfo=DAIDATA_JST)
        return current_datetime.astimezone(DAIDATA_JST)

    def fetch_store_at_supplement_history(
        self,
        *,
        store_config: DaidataOnlineStoreConfig,
        recent_days: int,
        enabled_machine_names: set[str] | None = None,
        browser_visible: bool = False,
        progress_callback: Callable[[FetchProgress], None] | None = None,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> MachineHistoryResult:
        target_days = max(1, min(int(recent_days), DAIDATA_AT_MAX_RECENT_DAYS))
        self._require_playwright()
        _raise_if_cancel_requested(cancel_requested)
        self._notify_progress(
            progress_callback,
            0,
            1,
            f"{store_config.store_name}のAT回数補完対象を確認しています",
        )

        playwright = None
        context = None
        machine_results: list[MachineHistoryResult] = []
        rules = self._load_at_counter_rules()
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
            list_url = urljoin(f"{store_config.url}/", "list?mode=psModelNameSearch&ps=S")
            self._goto_daidata_page(
                page,
                list_url,
                browser_visible=browser_visible,
                progress_callback=progress_callback,
                cancel_requested=cancel_requested,
            )
            machine_entries = self.extract_at_candidate_machine_links(
                page.content(),
                list_url,
                enabled_machine_names=enabled_machine_names,
            )

            for machine_index, machine_entry in enumerate(machine_entries, start=1):
                _raise_if_cancel_requested(cancel_requested)
                rule_key = build_daidata_at_rule_key(
                    store_config.store_id,
                    machine_entry.machine_name,
                    machine_entry.rate_key,
                )
                saved_rule = rules.get(rule_key, {})
                saved_decision = (
                    str(saved_rule.get("decision", "")).strip()
                    if isinstance(saved_rule, dict)
                    else ""
                )
                if saved_decision in DAIDATA_AT_SKIP_DETAIL_DECISIONS:
                    self._notify_progress(
                        progress_callback,
                        machine_index,
                        len(machine_entries),
                        f"{store_config.store_name} / {machine_entry.machine_name} は保存済み判定によりAT詳細を省略します",
                    )
                    continue

                self._notify_progress(
                    progress_callback,
                    machine_index,
                    len(machine_entries),
                    f"{store_config.store_name} / {machine_entry.machine_name} のAT回数を確認しています",
                )
                self._goto_daidata_page(
                    page,
                    _with_hist_num(machine_entry.url, 0),
                    browser_visible=browser_visible,
                    progress_callback=progress_callback,
                    cancel_requested=cancel_requested,
                )
                unit_entries = self.extract_unit_detail_links(page.content(), machine_entry.url)
                if not unit_entries:
                    continue

                observations: list[DaidataAtDetailObservation] = []
                current_evidences: list[DaidataAtCounterEvidence] = []
                for unit_entry in unit_entries:
                    _raise_if_cancel_requested(cancel_requested)
                    self._goto_daidata_page(
                        page,
                        unit_entry.url,
                        browser_visible=browser_visible,
                        progress_callback=progress_callback,
                        cancel_requested=cancel_requested,
                    )
                    detail_html = page.content()
                    fetched_at = format_site7_updated_datetime(self._current_daidata_datetime())
                    graph = parse_daidata_weekly_counter_graph(
                        detail_html,
                        reference_datetime=_parse_updated_datetime(detail_html)
                        or self._current_daidata_datetime(),
                    )
                    observations.append(
                        DaidataAtDetailObservation(
                            slot_number=unit_entry.slot_number,
                            detail_url=unit_entry.url,
                            fetched_at=fetched_at,
                            graph=graph,
                        )
                    )
                    current_evidences.append(daidata_at_counter_evidence_from_graph(graph))
                    if (
                        saved_decision not in DAIDATA_AT_DECISIONS
                        and graph.graph_found
                        and not graph.at_series_present
                    ):
                        break

                evidence = merge_daidata_at_counter_evidence(
                    [
                        daidata_at_counter_evidence_from_rule(saved_rule),
                        *current_evidences,
                    ]
                )
                updated_at = format_site7_updated_datetime(self._current_daidata_datetime())
                rule = build_daidata_at_counter_rule(
                    store_id=store_config.store_id,
                    machine_name=machine_entry.machine_name,
                    rate_key=machine_entry.rate_key,
                    evidence=evidence,
                    updated_at=updated_at,
                )
                rules[rule_key] = rule
                self._save_at_counter_rules(rules)

                machine_result = build_daidata_at_supplement_history_result(
                    store_name=store_config.store_name,
                    store_url=store_config.url,
                    machine_name=machine_entry.machine_name,
                    machine_url=machine_entry.url,
                    observations=observations,
                    decision=str(rule["decision"]),
                    recent_days=target_days,
                )
                if machine_result.datasets:
                    machine_results.append(machine_result)
        except PlaywrightError as exc:
            raise ScraperError(f"台データオンラインのAT回数取得に失敗しました。\n{exc}") from exc
        finally:
            self._release_browser_context(playwright, context, browser_visible=browser_visible)

        return self._merge_at_supplement_results(store_config, machine_results)

    def fetch_beam_hikari_juggler_history(
        self,
        recent_days: int,
        browser_visible: bool = False,
        progress_callback: Callable[[FetchProgress], None] | None = None,
        enabled_machine_names: set[str] | None = None,
        include_twenty_yen_non_juggler_machines: bool = False,
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
            include_twenty_yen_non_juggler_machines=include_twenty_yen_non_juggler_machines,
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
        include_twenty_yen_non_juggler_machines: bool = False,
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
        machine_entries: list[DaidataOnlineMachineEntry] = []
        store_closed_dates: set[str] = set()
        store_closed_statuses: dict[str, StoreDayStatus] = {}
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
            if include_twenty_yen_non_juggler_machines:
                seen_machine_keys = {
                    normalize_daidata_at_machine_name(machine_entry.machine_name)
                    for machine_entry in machine_entries
                }
                for machine_entry in self.extract_at_candidate_machine_links(
                    machine_list_html,
                    list_url,
                    enabled_machine_names=None,
                ):
                    machine_key = normalize_daidata_at_machine_name(machine_entry.machine_name)
                    if machine_key in seen_machine_keys:
                        continue
                    seen_machine_keys.add(machine_key)
                    machine_entries.append(machine_entry)
            if not machine_entries:
                target_label = (
                    "20円相当の対象機種"
                    if include_twenty_yen_non_juggler_machines
                    else "ジャグラー系機種"
                )
                raise ScraperError(
                    f"台データオンラインで{store_config.store_name}の{target_label}が見つかりませんでした。"
                )

            total_steps = len(machine_entries) + 1
            for machine_index, machine_entry in enumerate(machine_entries, start=1):
                _raise_if_cancel_requested(cancel_requested)
                self._notify_progress(
                    progress_callback,
                    machine_index,
                    total_steps,
                    f"{store_config.store_name} / {machine_entry.machine_name} の台データオンラインを読んでいます",
                )
                machine_result = self._fetch_machine_history_result(
                    store_config=store_config,
                    page=page,
                    machine_entry=machine_entry,
                    recent_days=target_days,
                    browser_visible=browser_visible,
                    progress_callback=progress_callback,
                    machine_protected_slots_callback=machine_protected_slots_callback,
                    cancel_requested=cancel_requested,
                )
                machine_results.append(machine_result)
                detected_store_closed = (
                    self._detect_store_closed_date_from_first_machine(machine_result)
                    if machine_index == 1
                    else None
                )
                if detected_store_closed is not None:
                    detected_closed_date, no_play_stats, checked_at = detected_store_closed
                    store_closed_dates.add(detected_closed_date)
                    store_closed_statuses[detected_closed_date] = self._build_store_closed_day_status(
                        detected_closed_date,
                        no_play_stats,
                        checked_at,
                        reason="first_machine_stale_1am_no_play",
                    )
                    self._notify_progress(
                        progress_callback,
                        machine_index,
                        total_steps,
                        f"{store_config.store_name} / {detected_closed_date} は店休日扱いでスキップします",
                    )
                    break
        except PlaywrightError as exc:
            raise ScraperError(f"台データオンラインの取得に失敗しました。\n{exc}") from exc
        finally:
            self._release_browser_context(playwright, context, browser_visible=browser_visible)

        self._apply_store_closed_date_skips(
            machine_results=machine_results,
            machine_entries=machine_entries,
            closed_dates=store_closed_dates,
            closed_statuses=store_closed_statuses,
            store_config=store_config,
        )
        return self._merge_machine_history_results(store_config, machine_results)

    def _detect_store_closed_date_from_first_machine(
        self,
        history_result: MachineHistoryResult,
    ) -> tuple[str, Site7NoPlayDayStats, datetime] | None:
        current_datetime = self._current_daidata_datetime()
        for stats in site7_result_no_play_day_stats(history_result).values():
            detected_store_closed = self._detect_store_closed_date_from_no_play_stats(stats, current_datetime)
            if detected_store_closed is not None:
                return detected_store_closed
        return None

    def _detect_store_closed_date_from_no_play_stats(
        self,
        stats: Site7NoPlayDayStats,
        current_datetime: datetime | None = None,
    ) -> tuple[str, Site7NoPlayDayStats, datetime] | None:
        updated_at = stats.updated_at
        if updated_at is None or not stats.all_slots_no_play:
            return None
        updated_at_jst = (
            updated_at.astimezone(DAIDATA_JST)
            if updated_at.tzinfo is not None and updated_at.utcoffset() is not None
            else updated_at.replace(tzinfo=DAIDATA_JST)
        )
        if updated_at_jst.hour != SITE7_STORE_CLOSED_STALE_UPDATE_HOUR:
            return None

        current_datetime = current_datetime or self._current_daidata_datetime()
        check_datetime = updated_at_jst.replace(
            hour=SITE7_STORE_CLOSED_CHECK_HOUR,
            minute=SITE7_STORE_CLOSED_CHECK_MINUTE,
            second=0,
            microsecond=0,
        )
        if current_datetime < check_datetime:
            return None

        return updated_at_jst.strftime("%Y-%m-%d"), stats, current_datetime

    def _build_store_closed_day_status(
        self,
        target_date: str,
        stats: Site7NoPlayDayStats,
        checked_at: datetime,
        *,
        reason: str,
    ) -> StoreDayStatus:
        return StoreDayStatus(
            target_date=target_date,
            status=SITE7_STORE_DAY_STATUS_CLOSED,
            source="daidata_online",
            reason=reason,
            checked_at=format_site7_updated_datetime(checked_at),
            source_updated_at=(
                format_site7_updated_datetime(stats.updated_at)
                if stats.updated_at is not None
                else ""
            ),
            observed_slot_count=stats.slot_count,
            observed_no_play_slot_count=stats.no_play_slot_count,
        )

    def _apply_store_closed_date_skips(
        self,
        *,
        machine_results: list[MachineHistoryResult],
        machine_entries: list[DaidataOnlineMachineEntry],
        closed_dates: set[str],
        closed_statuses: dict[str, StoreDayStatus],
        store_config: DaidataOnlineStoreConfig,
    ) -> None:
        if not closed_dates:
            return
        closed_date_set = set(closed_dates)
        for machine_result in machine_results:
            machine_result.datasets = [
                dataset
                for dataset in machine_result.datasets
                if not self._dataset_matches_store_closed_date(dataset, closed_date_set)
            ]
            machine_result.date_pages = [
                date_page for date_page in machine_result.date_pages if date_page.target_date not in closed_date_set
            ]

        machine_names = [machine_entry.machine_name for machine_entry in machine_entries]
        skipped_targets = [
            (target_date, machine_name)
            for target_date in sorted(closed_date_set)
            for machine_name in machine_names
        ]
        machine_results.append(
            MachineHistoryResult(
                store_name=store_config.store_name,
                store_url=store_config.url,
                start_date=min(closed_date_set),
                end_date=max(closed_date_set),
                date_pages=[],
                datasets=[],
                skipped_targets=skipped_targets,
                skipped_dates=sorted(closed_date_set),
                store_day_statuses=[
                    closed_statuses[target_date]
                    for target_date in sorted(closed_date_set)
                    if target_date in closed_statuses
                ],
            )
        )

    def _dataset_matches_store_closed_date(
        self,
        dataset: MachineDataset,
        closed_dates: set[str],
    ) -> bool:
        if dataset.target_date in closed_dates:
            return True
        stats = self._build_daidata_no_play_day_stats(dataset)
        if not stats.all_slots_no_play or stats.updated_at is None:
            return False
        updated_at = (
            stats.updated_at.astimezone(DAIDATA_JST)
            if stats.updated_at.tzinfo is not None and stats.updated_at.utcoffset() is not None
            else stats.updated_at.replace(tzinfo=DAIDATA_JST)
        )
        return updated_at.strftime("%Y-%m-%d") in closed_dates

    def extract_at_candidate_machine_links(
        self,
        html: str,
        base_url: str,
        *,
        enabled_machine_names: set[str] | None = None,
    ) -> list[DaidataOnlineMachineEntry]:
        soup = BeautifulSoup(html, "html.parser")
        enabled_keys = _enabled_machine_keys(enabled_machine_names)
        entries: list[DaidataOnlineMachineEntry] = []
        seen_rule_names: set[str] = set()

        for link in soup.find_all("a", href=True):
            href = str(link.get("href", "")).strip()
            if "unit_list" not in href:
                continue
            raw_link_text = link.get_text(" ", strip=True)
            raw_machine_name = _clean_machine_name(raw_link_text)
            if not raw_machine_name:
                continue
            machine_name = canonical_machine_name(raw_machine_name)
            if _machine_is_juggler(raw_machine_name, machine_name):
                continue
            if enabled_keys is not None and not (_machine_entry_keys(raw_machine_name, machine_name) & enabled_keys):
                continue

            machine_url = urljoin(base_url, href)
            ball_price = _parse_daidata_ball_price(machine_url, raw_link_text)
            if not daidata_ball_price_is_twenty_yen_equivalent(ball_price):
                continue
            normalized_machine_name = normalize_daidata_at_machine_name(machine_name)
            if normalized_machine_name in seen_rule_names:
                continue
            seen_rule_names.add(normalized_machine_name)
            entries.append(
                DaidataOnlineMachineEntry(
                    machine_name=machine_name,
                    raw_machine_name=raw_machine_name,
                    url=machine_url,
                    machine_count=self._extract_machine_count(raw_link_text),
                    ball_price=ball_price,
                    rate_key=DAIDATA_AT_RATE_KEY_20_YEN,
                )
            )

        return entries

    def extract_unit_detail_links(
        self,
        html: str,
        base_url: str,
    ) -> list[DaidataOnlineUnitEntry]:
        soup = BeautifulSoup(html, "html.parser")
        table = _find_unit_table(soup)
        search_root = table if table is not None else soup
        entries: list[DaidataOnlineUnitEntry] = []
        seen_slots: set[str] = set()
        for link in search_root.find_all("a", href=True):
            href = str(link.get("href", "")).strip()
            href_parts = urlsplit(urljoin(base_url, href))
            query = dict(parse_qsl(href_parts.query, keep_blank_values=True))
            if not href_parts.path.rstrip("/").endswith("/detail") or not query.get("unit"):
                continue
            slot_number = _normalize_count_text(link.get_text(" ", strip=True))
            if slot_number in {"", "-", "--"}:
                continue
            if slot_number in seen_slots:
                continue
            seen_slots.add(slot_number)
            entries.append(
                DaidataOnlineUnitEntry(
                    slot_number=slot_number,
                    url=urljoin(base_url, href),
                )
            )
        return sorted(
            entries,
            key=lambda entry: (
                0,
                int(entry.slot_number),
            )
            if entry.slot_number.isdigit()
            else (1, entry.slot_number),
        )

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
        result = MachineHistoryResult(
            store_name=store_config.store_name,
            store_url=store_config.url,
            start_date=start_date,
            end_date=end_date,
            date_pages=date_pages,
            datasets=datasets,
            skipped_targets=skipped_targets,
            skipped_dates=skipped_dates,
        )
        no_play_day_stats = {
            dataset.target_date: stats
            for dataset in datasets
            if (stats := self._build_daidata_no_play_day_stats(dataset)).slot_count > 0
        }
        if no_play_day_stats:
            set_site7_result_no_play_day_stats(result, no_play_day_stats)
        return result

    def _merge_machine_history_results(
        self,
        store_config: DaidataOnlineStoreConfig,
        machine_results: list[MachineHistoryResult],
    ) -> MachineHistoryResult:
        datasets: list[MachineDataset] = []
        date_pages_by_date: dict[str, StoreDatePage] = {}
        skipped_targets: list[tuple[str, str]] = []
        skipped_dates: list[str] = []
        store_day_statuses: list[StoreDayStatus] = []

        for machine_result in machine_results:
            datasets.extend(machine_result.datasets)
            skipped_targets.extend(machine_result.skipped_targets)
            skipped_dates.extend(date for date in machine_result.skipped_dates if date not in skipped_dates)
            store_day_statuses.extend(machine_result.store_day_statuses)
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
            store_day_statuses=store_day_statuses,
        )

    def _merge_at_supplement_results(
        self,
        store_config: DaidataOnlineStoreConfig,
        machine_results: list[MachineHistoryResult],
    ) -> MachineHistoryResult:
        datasets = [
            dataset
            for machine_result in machine_results
            for dataset in machine_result.datasets
        ]
        datasets.sort(key=lambda dataset: (dataset.target_date, dataset.machine_name.casefold()))
        date_pages_by_date: dict[str, StoreDatePage] = {}
        for machine_result in machine_results:
            for date_page in machine_result.date_pages:
                date_pages_by_date.setdefault(date_page.target_date, date_page)
        date_pages = sorted(date_pages_by_date.values(), key=lambda date_page: date_page.target_date)
        target_dates = [dataset.target_date for dataset in datasets if dataset.target_date]
        return MachineHistoryResult(
            store_name=store_config.store_name,
            store_url=store_config.url,
            start_date=min(target_dates) if target_dates else "",
            end_date=max(target_dates) if target_dates else "",
            date_pages=date_pages,
            datasets=datasets,
        )

    def _load_at_counter_rules(self) -> dict[str, dict[str, object]]:
        if not self.at_counter_rules_path.exists():
            return {}
        try:
            payload = json.loads(self.at_counter_rules_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        raw_rules = payload.get("rules", {}) if isinstance(payload, dict) else {}
        if not isinstance(raw_rules, dict):
            return {}
        rules: dict[str, dict[str, object]] = {}
        for raw_key, raw_rule in raw_rules.items():
            if not isinstance(raw_rule, dict):
                continue
            decision = str(raw_rule.get("decision", "")).strip()
            if decision not in DAIDATA_AT_DECISIONS:
                continue
            rules[str(raw_key)] = dict(raw_rule)
        return rules

    def _save_at_counter_rules(self, rules: dict[str, dict[str, object]]) -> None:
        payload = {
            "version": 1,
            "rules": {
                rule_key: rules[rule_key]
                for rule_key in sorted(rules)
            },
        }
        try:
            self.at_counter_rules_path.parent.mkdir(parents=True, exist_ok=True)
            temporary_path = self.at_counter_rules_path.with_suffix(
                f"{self.at_counter_rules_path.suffix}.tmp"
            )
            temporary_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            temporary_path.replace(self.at_counter_rules_path)
        except OSError as exc:
            raise ScraperError("台データオンラインのAT回数判定を保存できませんでした。") from exc

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

    def _build_daidata_no_play_day_stats(self, dataset: MachineDataset) -> Site7NoPlayDayStats:
        no_play_slot_count = sum(
            1
            for row in dataset.rows
            if self._daidata_machine_day_row_is_no_play(dataset, row)
        )
        return Site7NoPlayDayStats(
            slot_count=len(dataset.rows),
            no_play_slot_count=no_play_slot_count,
            has_play_data=no_play_slot_count < len(dataset.rows),
            updated_at=_parse_daidata_updated_at(site7_dataset_updated_at(dataset)),
        )

    def _daidata_machine_day_row_is_no_play(self, dataset: MachineDataset, row: list[str]) -> bool:
        column_indexes: list[int] = []
        for target_column in ("G数", "BB", "RB"):
            try:
                column_indexes.append(
                    next(
                        index
                        for index, column_name in enumerate(dataset.columns)
                        if normalize_text(column_name) == normalize_text(target_column)
                    )
                )
            except StopIteration:
                return False
        return all(
            index < len(row) and _daidata_stat_value_is_empty_or_zero(row[index])
            for index in column_indexes
        )

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
