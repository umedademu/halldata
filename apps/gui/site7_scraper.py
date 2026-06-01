from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
import random
import re
import statistics
import time
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup, Tag
from PIL import Image, UnidentifiedImageError

from machine_difference import (
    canonical_machine_name,
    format_machine_difference_for_row,
    list_site7_target_machine_keywords,
    machine_is_site7_target,
)
from minrepo_scraper import FetchProgress, MachineDataset, MachineHistoryResult, ScraperError, StoreDatePage

try:
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
except ImportError:  # pragma: no cover
    PlaywrightError = RuntimeError  # type: ignore[assignment]
    PlaywrightTimeoutError = RuntimeError  # type: ignore[assignment]
    sync_playwright = None  # type: ignore[assignment]


ROOT_DIR = Path(__file__).resolve().parents[2]
SITE7_TOP_URL = "https://www.d-deltanet.com/pc/Top.do"
SITE7_LOGIN_URL = "https://www.d-deltanet.com/pc/MypageLoginTop.do?redirectLogin=0&skskb="
SITE7_MOBILE_TOP_URL = "https://m.site777.jp/db/A0100.do"
SITE7_MOBILE_USER_AGENT = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
)
SITE7_MOBILE_VIEWPORT = {"width": 390, "height": 844}
DEFAULT_SITE7_PREFECTURE_NAME = "福岡県"
SITE7_TARGET_MACHINE_NAME = "ネオアイムジャグラーEX"
SITE7_TARGET_MACHINE_KEYWORDS = tuple(list_site7_target_machine_keywords())
SITE7_MAX_RECENT_DAYS = 8
SITE7_TRANSITION_WAIT_MIN_SECONDS = 2.0
SITE7_TRANSITION_WAIT_MAX_SECONDS = 4.0
SITE7_GRAPH_LIST_DETAIL_THRESHOLD = 4500
SITE7_GRAPH_LIST_MAX_PAGES = 20
SITE7_BROWSER_STATE_DIR_NAME = "site7_browser"
SITE7_DATE_BOUNDARY_HOUR = 4
SITE7_UPDATE_DATE_PATTERN = re.compile(
    r"データ更新日時：\s*(\d{4})/(\d{1,2})/(\d{1,2})(?:\s+(\d{1,2}):(\d{2}))?"
)
SITE7_SLOT_NUMBER_PATTERN = re.compile(r"(\d+)")
SITE7_HALL_CLICK_PATTERN = re.compile(r"hallClick\('([^']+)'\)")
SITE7_LOGIN_URL_PATTERN = re.compile(r"(?:Mypage)?Login", re.IGNORECASE)
SITE7_LOGGED_IN_URL_KEYWORDS = (
    "PCCreditAuth.do",
    "MypageTop.do",
    "MypageRegistProfile.do",
)
SITE7_LOOKUP_DROP_PATTERN = re.compile(r"[\s\u3000'\"`´’‘“”.,，．:：;；/／\\|｜!?！？\-_－ー―ｰ~〜～・･·•()\[\]{}（）［］｛｝【】「」『』〈〉<>]")


def site7_value_has_data(value: str) -> bool:
    text = str(value).strip()
    return bool(text and text not in {"-", "--"})


def clamp_site7_recent_days(recent_days: int) -> int:
    if recent_days <= 0:
        raise ScraperError("直近日数は 1 以上の整数で入力してください。")
    return min(recent_days, SITE7_MAX_RECENT_DAYS)


def format_site7_ratio_text(value: str) -> str:
    text = str(value).strip()
    if not text or text in {"-", "--"}:
        return "-"
    if text.startswith("1/"):
        return text
    return f"1/{text}"


def build_site7_transition_wait_milliseconds(
    random_seconds_fn: Callable[[float, float], float] | None = None,
) -> int:
    seconds_fn = random_seconds_fn or random.uniform
    seconds = float(seconds_fn(SITE7_TRANSITION_WAIT_MIN_SECONDS, SITE7_TRANSITION_WAIT_MAX_SECONDS))
    seconds = max(SITE7_TRANSITION_WAIT_MIN_SECONDS, min(SITE7_TRANSITION_WAIT_MAX_SECONDS, seconds))
    return int(seconds * 1000)


def parse_site7_graph_difference_value(image_bytes: bytes) -> int | None:
    try:
        image = Image.open(BytesIO(image_bytes)).convert("RGB")
    except (UnidentifiedImageError, OSError):
        return None

    axis_x, graph_top, graph_bottom = _detect_site7_graph_axis(image)
    if axis_x is None or graph_top is None or graph_bottom is None:
        return None
    zero_y = _detect_site7_graph_zero_y(image, axis_x, graph_top, graph_bottom)
    if zero_y is None:
        return None
    grid_spacing = _detect_site7_graph_grid_spacing(image, axis_x, graph_top, graph_bottom, zero_y)
    if grid_spacing is None or grid_spacing <= 0:
        return None
    final_line_y = _detect_site7_graph_final_line_y(image, axis_x, graph_top, graph_bottom)
    if final_line_y is None:
        return None

    return int(round((zero_y - final_line_y) * 1000 / grid_spacing))


def _detect_site7_graph_axis(image: Image.Image) -> tuple[int | None, int | None, int | None]:
    width, height = image.size
    candidates: list[tuple[int, int, int, int]] = []
    for x in range(min(60, width)):
        dark_rows = [y for y in range(height) if _site7_pixel_is_dark(image.getpixel((x, y)))]
        if len(dark_rows) < 20:
            continue
        candidates.append((x, len(dark_rows), min(dark_rows), max(dark_rows)))
    if not candidates:
        return None, None, None

    axis_x, _, graph_top, graph_bottom = max(candidates, key=lambda item: item[1])
    return axis_x, graph_top, graph_bottom


def _detect_site7_graph_zero_y(
    image: Image.Image,
    axis_x: int,
    graph_top: int,
    graph_bottom: int,
) -> int | None:
    width, _ = image.size
    graph_width = max(1, width - axis_x - 16)
    minimum_line_pixels = max(20, int(graph_width * 0.35))
    candidates: list[tuple[int, int]] = []
    for y in range(graph_top, graph_bottom + 1):
        dark_count = sum(
            1
            for x in range(axis_x + 1, max(axis_x + 2, width - 15))
            if _site7_pixel_is_dark(image.getpixel((x, y)))
        )
        if dark_count > minimum_line_pixels:
            candidates.append((y, dark_count))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[1])[0]


def _detect_site7_graph_grid_spacing(
    image: Image.Image,
    axis_x: int,
    graph_top: int,
    graph_bottom: int,
    zero_y: int,
) -> float | None:
    width, _ = image.size
    graph_width = max(1, width - axis_x - 16)
    minimum_grid_pixels = max(20, int(graph_width * 0.35))
    grid_rows: list[int] = []
    for y in range(graph_top, graph_bottom + 1):
        grid_count = sum(
            1
            for x in range(axis_x + 1, max(axis_x + 2, width - 15))
            if _site7_pixel_is_grid(image.getpixel((x, y)))
        )
        if grid_count > minimum_grid_pixels:
            grid_rows.append(y)

    grid_rows = [row for row in grid_rows if abs(row - zero_y) > 2]
    gaps = [
        next_row - current_row
        for current_row, next_row in zip(grid_rows, grid_rows[1:])
        if 10 <= next_row - current_row <= 35
    ]
    if not gaps:
        return None
    return float(statistics.median(gaps))


def _detect_site7_graph_final_line_y(
    image: Image.Image,
    axis_x: int,
    graph_top: int,
    graph_bottom: int,
) -> float | None:
    width, _ = image.size
    line_pixels: dict[int, list[int]] = {}
    for y in range(graph_top, graph_bottom + 1):
        for x in range(axis_x + 1, max(axis_x + 2, width - 15)):
            if not _site7_pixel_is_graph_line(image.getpixel((x, y))):
                continue
            line_pixels.setdefault(x, []).append(y)
    if not line_pixels:
        return None

    max_x = max(line_pixels)
    candidate_rows = [
        y
        for x, rows in line_pixels.items()
        if max_x - 4 <= x <= max_x
        for y in rows
    ]
    if not candidate_rows:
        return None
    return float(statistics.fmean(candidate_rows))


def _site7_pixel_is_dark(pixel: tuple[int, int, int]) -> bool:
    return max(pixel) < 130


def _site7_pixel_is_grid(pixel: tuple[int, int, int]) -> bool:
    red, green, blue = pixel
    return 190 <= red <= 235 and 185 <= green <= 230 and 180 <= blue <= 225 and abs(red - green) < 12 and abs(green - blue) < 12


def _site7_pixel_is_graph_line(pixel: tuple[int, int, int]) -> bool:
    red, green, blue = pixel
    if red > 170 and green < 150 and blue < 140 and red - green > 45 and red - blue > 45:
        return True
    return green > 110 and red < 170 and blue < 170 and green - red > 25 and green - blue > 20


@dataclass(frozen=True)
class Site7MachineEntry:
    display_name: str
    machine_name: str


@dataclass(frozen=True)
class Site7TargetStore:
    display_name: str
    site7_hall_name: str
    prefecture_name: str = DEFAULT_SITE7_PREFECTURE_NAME
    area_name: str = ""
    hall_id: str = ""
    hall_address: str = ""
    direct_hall_url: str = ""
    hall_name_aliases: tuple[str, ...] = ()

    @property
    def store_name_match_keys(self) -> tuple[str, ...]:
        return _collect_site7_lookup_keys(self.display_name, self.site7_hall_name, *self.hall_name_aliases)

    @property
    def hall_match_keys(self) -> tuple[str, ...]:
        return _collect_site7_lookup_keys(
            self.display_name,
            self.site7_hall_name,
            *self.hall_name_aliases,
            self.hall_address,
        )

    @property
    def prefecture_link_text(self) -> str:
        return _normalize_site7_prefecture_link_text(self.prefecture_name)


@dataclass(frozen=True)
class Site7UnavailableStore:
    display_name: str
    prefecture_name: str = DEFAULT_SITE7_PREFECTURE_NAME
    area_name: str = ""
    hall_name_aliases: tuple[str, ...] = ()

    @property
    def store_name_match_keys(self) -> tuple[str, ...]:
        return _collect_site7_lookup_keys(self.display_name, *self.hall_name_aliases)


SITE7_TARGET_STORES = (
    Site7TargetStore(
        display_name="Aパーク春日店",
        site7_hall_name="Ａパーク春日店",
        prefecture_name=DEFAULT_SITE7_PREFECTURE_NAME,
        hall_id="",
        hall_address="福岡県春日市日の出町５－２４",
        area_name="春日市",
        direct_hall_url="https://www.d-deltanet.com/pc/HallSelectLink.do?hallcode=235def7f3ed0c81275a2bc47dc5b839a",
        hall_name_aliases=("Aパーク春日店",),
    ),
    Site7TargetStore(
        display_name="123博多店",
        site7_hall_name="１２３博多店",
        prefecture_name=DEFAULT_SITE7_PREFECTURE_NAME,
        hall_id="27038079",
        hall_address="福岡県福岡市博多区住吉２丁目６番２４号",
        area_name="福岡市博多区",
        direct_hall_url="https://www.d-deltanet.com/pc/HallSelectLink.do?hallcode=27038079",
        hall_name_aliases=("123博多店",),
    ),
    Site7TargetStore(
        display_name="スーパーハリウッド1120",
        site7_hall_name="スーパーハリウッド１１２０",
        prefecture_name=DEFAULT_SITE7_PREFECTURE_NAME,
        hall_id="",
        hall_address="福岡県春日市星見ヶ丘６丁目３２番地",
        area_name="春日市",
        direct_hall_url="https://www.d-deltanet.com/pc/HallSelectLink.do?hallcode=7c0196b036b9225e520c13b81e969c84",
        hall_name_aliases=("スーパーハリウッド1120",),
    ),
    Site7TargetStore(
        display_name="BOOM天神本店",
        site7_hall_name="ブーム天神本店",
        prefecture_name=DEFAULT_SITE7_PREFECTURE_NAME,
        hall_id="40001007",
        hall_address="福岡県福岡市中央区今泉１丁目１３番１号",
        area_name="福岡市中央区",
        direct_hall_url="https://www.d-deltanet.com/pc/HallSelectLink.do?hallcode=40001007",
        hall_name_aliases=("BOOM天神本店", "ＢＯＯＭ天神本店"),
    ),
    Site7TargetStore(
        display_name="GOGOアリーナ天神",
        site7_hall_name="ＧＯＧＯアリーナ天神",
        prefecture_name=DEFAULT_SITE7_PREFECTURE_NAME,
        hall_id="",
        hall_address="福岡県福岡市中央区天神２－６－３７",
        area_name="福岡市中央区",
        direct_hall_url="https://www.d-deltanet.com/pc/HallSelectLink.do?hallcode=40056006",
        hall_name_aliases=("GOGOアリーナ天神",),
    ),
    Site7TargetStore(
        display_name="MJアリーナ井尻店",
        site7_hall_name="MJアリーナ井尻店",
        prefecture_name=DEFAULT_SITE7_PREFECTURE_NAME,
        hall_id="40056001",
        hall_address="福岡県春日市桜ケ丘４－１４",
        area_name="春日市",
        direct_hall_url="https://www.d-deltanet.com/pc/HallSelectLink.do?hallcode=40056001",
        hall_name_aliases=("ＭＪアリーナ井尻店",),
    ),
    Site7TargetStore(
        display_name="HINODE大野城店",
        site7_hall_name="HINODE大野城店",
        prefecture_name=DEFAULT_SITE7_PREFECTURE_NAME,
        hall_id="40101001",
        hall_address="福岡県大野城市瓦田４－１２－５",
        area_name="大野城市",
        direct_hall_url="https://www.d-deltanet.com/pc/HallSelectLink.do?hallcode=40101001",
        hall_name_aliases=("ヒノデ大野城店", "日の出大野城店"),
    ),
    Site7TargetStore(
        display_name="スーパーDステーション39筑紫野店",
        site7_hall_name="スーパーＤ’ステーション３９筑紫野店",
        prefecture_name=DEFAULT_SITE7_PREFECTURE_NAME,
        hall_id="42006007",
        hall_address="福岡県筑紫野市筑紫９６８番２",
        area_name="筑紫野市",
        direct_hall_url="https://www.d-deltanet.com/pc/HallSelectLink.do?hallcode=42006007",
        hall_name_aliases=(
            "スーパーDステーション筑紫野店",
            "スーパーDステーション39筑紫野店",
            "スーパーＤステーション筑紫野店",
            "スーパーＤ’ステーション３９筑紫野店",
        ),
    ),
    Site7TargetStore(
        display_name="アミューズ浅草店",
        site7_hall_name="アミューズ浅草店",
        prefecture_name="東京都",
        hall_id="13777725",
        hall_address="東京都台東区浅草１－４３－１",
        area_name="台東区",
        direct_hall_url="https://www.d-deltanet.com/pc/HallSelectLink.do?hallcode=13777725",
        hall_name_aliases=("アミューズ浅草", "AMUSE浅草店", "AMUSE浅草", "ＡＭＵＳＥ浅草店", "ＡＭＵＳＥ浅草"),
    ),
)
SITE7_UNAVAILABLE_STORES = (
    Site7UnavailableStore(
        display_name="ビームヒカリ",
        prefecture_name=DEFAULT_SITE7_PREFECTURE_NAME,
        area_name="大野城市",
        hall_name_aliases=("BEAM BY HIKARI", "BEAM HIKARI", "ビームヒカリ店"),
    ),
)
SITE7_TARGET_STORE_DISPLAY_NAMES = tuple(store.display_name for store in SITE7_TARGET_STORES)
SITE7_DEFAULT_TARGET_STORE = SITE7_TARGET_STORES[0]


def _normalize_site7_lookup_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(value)).casefold()
    normalized = SITE7_LOOKUP_DROP_PATTERN.sub("", normalized)
    return normalized.strip()


def _build_site7_lookup_keys(value: str) -> tuple[str, ...]:
    normalized = _normalize_site7_lookup_text(value)
    if not normalized:
        return ()

    candidates = [normalized]
    if normalized.endswith("店") and len(normalized) > 1:
        candidates.append(normalized[:-1])
    if normalized.endswith(("都", "府", "県")) and len(normalized) > 1:
        candidates.append(normalized[:-1])

    seen_keys: set[str] = set()
    keys: list[str] = []
    for candidate in candidates:
        if not candidate or candidate in seen_keys:
            continue
        seen_keys.add(candidate)
        keys.append(candidate)
    return tuple(keys)


def _collect_site7_lookup_keys(*values: str) -> tuple[str, ...]:
    seen_keys: set[str] = set()
    keys: list[str] = []
    for value in values:
        for key in _build_site7_lookup_keys(value):
            if key in seen_keys:
                continue
            seen_keys.add(key)
            keys.append(key)
    return tuple(keys)


def _site7_lookup_keys_match(
    left_keys: tuple[str, ...],
    right_keys: tuple[str, ...],
    *,
    allow_partial: bool = False,
    partial_min_length: int = 4,
) -> bool:
    for left_key in left_keys:
        for right_key in right_keys:
            if left_key == right_key:
                return True
            if not allow_partial:
                continue
            if min(len(left_key), len(right_key)) < partial_min_length:
                continue
            if left_key in right_key or right_key in left_key:
                return True
    return False


def _normalize_site7_hall_id(value: str) -> str:
    return unicodedata.normalize("NFKC", str(value)).strip().casefold()


def _site7_hall_id_matches(left_value: str, right_value: str) -> bool:
    left_id = _normalize_site7_hall_id(left_value)
    right_id = _normalize_site7_hall_id(right_value)
    return bool(left_id and right_id and left_id == right_id)


class Site7FetchCancelled(ScraperError):
    pass


def _raise_if_site7_cancel_requested(cancel_requested: Callable[[], bool] | None) -> None:
    if cancel_requested is not None and cancel_requested():
        raise Site7FetchCancelled("サイトセブン取得を中止しました。")


def _normalize_site7_prefecture_link_text(value: str) -> str:
    text = str(value).strip()
    if text == "北海道":
        return text
    if text.endswith(("都", "府", "県")):
        return text[:-1]
    return text


def find_known_site7_target_store(store_name: str) -> Site7TargetStore | None:
    candidate_names = (store_name,)
    for candidate_name in candidate_names:
        store_name_keys = _build_site7_lookup_keys(candidate_name)
        if not store_name_keys:
            continue

        for target_store in SITE7_TARGET_STORES:
            if _site7_lookup_keys_match(target_store.store_name_match_keys, store_name_keys):
                return target_store
    return None


def find_known_unavailable_site7_store(store_name: str) -> Site7UnavailableStore | None:
    store_name_keys = _build_site7_lookup_keys(store_name)
    if not store_name_keys:
        return None

    for unavailable_store in SITE7_UNAVAILABLE_STORES:
        if _site7_lookup_keys_match(unavailable_store.store_name_match_keys, store_name_keys):
            return unavailable_store
    return None


def site7_store_is_known_unavailable(store_name: str) -> bool:
    return find_known_unavailable_site7_store(store_name) is not None


def enrich_site7_target_store(target_store: Site7TargetStore) -> Site7TargetStore:
    known_target_store = (
        find_known_site7_target_store(target_store.site7_hall_name)
        or find_known_site7_target_store(target_store.display_name)
        or next(
            (
                known_store
                for alias in target_store.hall_name_aliases
                for known_store in [find_known_site7_target_store(alias)]
                if known_store is not None
            ),
            None,
        )
    )
    if known_target_store is None:
        return target_store

    merged_aliases: list[str] = []
    for alias in (
        *target_store.hall_name_aliases,
        target_store.display_name,
        target_store.site7_hall_name,
        known_target_store.display_name,
        known_target_store.site7_hall_name,
        *known_target_store.hall_name_aliases,
    ):
        stripped_alias = str(alias).strip()
        if not stripped_alias or stripped_alias in merged_aliases:
            continue
        merged_aliases.append(stripped_alias)

    return Site7TargetStore(
        display_name=target_store.display_name.strip() or known_target_store.display_name,
        site7_hall_name=target_store.site7_hall_name.strip() or known_target_store.site7_hall_name,
        prefecture_name=target_store.prefecture_name.strip() or known_target_store.prefecture_name,
        area_name=target_store.area_name.strip() or known_target_store.area_name,
        hall_id=target_store.hall_id.strip() or known_target_store.hall_id,
        hall_address=target_store.hall_address.strip() or known_target_store.hall_address,
        direct_hall_url=target_store.direct_hall_url.strip() or known_target_store.direct_hall_url,
        hall_name_aliases=tuple(merged_aliases),
    )


def default_site7_store_settings(store_name: str) -> dict[str, object]:
    known_target_store = find_known_site7_target_store(store_name)
    if known_target_store is not None:
        return {
            "site7_enabled": True,
            "site7_prefecture": known_target_store.prefecture_name,
            "site7_area": known_target_store.area_name,
            "site7_store_name": known_target_store.site7_hall_name,
            "site7_hall_id": known_target_store.hall_id,
            "site7_address": known_target_store.hall_address,
        }

    known_unavailable_store = find_known_unavailable_site7_store(store_name)
    if known_unavailable_store is not None:
        return {
            "site7_enabled": False,
            "site7_prefecture": known_unavailable_store.prefecture_name,
            "site7_area": known_unavailable_store.area_name,
            "site7_store_name": known_unavailable_store.display_name,
            "site7_hall_id": "",
            "site7_address": "",
        }

    stripped_store_name = str(store_name).strip()
    return {
        "site7_enabled": False,
        "site7_prefecture": DEFAULT_SITE7_PREFECTURE_NAME,
        "site7_area": "",
        "site7_store_name": stripped_store_name,
        "site7_hall_id": "",
        "site7_address": "",
    }


class Site7Scraper:
    def __init__(self, root_dir: Path | None = None) -> None:
        self.root_dir = root_dir or ROOT_DIR
        self.browser_state_dir = self.root_dir / "local_data" / SITE7_BROWSER_STATE_DIR_NAME
        self._visible_playwright: object | None = None
        self._visible_context: object | None = None

    def has_saved_login_state(self) -> bool:
        return self.browser_state_dir.exists() and any(self.browser_state_dir.iterdir())

    def close_visible_browser(self) -> None:
        try:
            self._release_browser_context(self._visible_playwright, self._visible_context)
        finally:
            self._visible_playwright = None
            self._visible_context = None

    def _launch_browser_context(self, browser_visible: bool) -> tuple[object, object]:
        playwright = sync_playwright().start()
        context = playwright.chromium.launch_persistent_context(
            str(self.browser_state_dir),
            headless=not browser_visible,
            locale="ja-JP",
            viewport={"width": 1440, "height": 960},
        )
        return playwright, context

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

    def _open_fetch_browser_context(self, browser_visible: bool) -> tuple[object, object]:
        return self._launch_browser_context(browser_visible)

    def _prepare_fetch_page(self, context: object, browser_visible: bool) -> object:
        try:
            pages = list(context.pages)
        except Exception:  # noqa: BLE001
            pages = []
        page = pages[-1] if pages else context.new_page()
        if browser_visible:
            page.bring_to_front()
        return page

    def login_interactively(self, timeout_seconds: int = 300) -> None:
        self._require_playwright()
        self.close_visible_browser()
        self.browser_state_dir.mkdir(parents=True, exist_ok=True)
        timed_out = False

        try:
            with sync_playwright() as playwright:
                context = playwright.chromium.launch_persistent_context(
                    str(self.browser_state_dir),
                    headless=False,
                    locale="ja-JP",
                    viewport={"width": 1440, "height": 960},
                )
                try:
                    page = context.pages[0] if context.pages else context.new_page()
                    page.goto(SITE7_LOGIN_URL, wait_until="domcontentloaded", timeout=60_000)
                    page.bring_to_front()
                    timed_out = self._wait_for_login_success(context, timeout_seconds=timeout_seconds)
                finally:
                    try:
                        context.close()
                    except Exception:  # noqa: BLE001
                        pass
        except PlaywrightError as exc:
            raise self._wrap_playwright_error(exc) from exc

        if self.is_logged_in():
            return

        if timed_out:
            raise ScraperError("ログイン待機がタイムアウトしました。ログイン完了後の画面が開いたままなら数秒待ってください。")

        raise ScraperError("ログイン状態を確認できませんでした。ログイン後の画面が開いたままなら数秒待ってください。")

    def is_logged_in(self, browser_visible: bool = False) -> bool:
        if not self.has_saved_login_state():
            return False

        self._require_playwright()
        self.close_visible_browser()
        try:
            with sync_playwright() as playwright:
                context = playwright.chromium.launch_persistent_context(
                    str(self.browser_state_dir),
                    headless=not browser_visible,
                    locale="ja-JP",
                    viewport={"width": 1440, "height": 960},
                )
                try:
                    page = context.new_page()
                    if browser_visible:
                        page.bring_to_front()
                    hall_page_url, hall_html = self._open_target_hall_page(page, SITE7_DEFAULT_TARGET_STORE)
                    if self._page_is_login_required(hall_page_url, hall_html):
                        return False
                    return self._page_has_target_hall_page(hall_page_url, hall_html, SITE7_DEFAULT_TARGET_STORE)
                finally:
                    context.close()
        except Exception:  # noqa: BLE001
            return False

    def fetch_target_machine_history(
        self,
        recent_days: int,
        browser_visible: bool = False,
        progress_callback: Callable[[FetchProgress], None] | None = None,
        target_store: Site7TargetStore | None = None,
        cancel_requested: Callable[[], bool] | None = None,
        machine_result_callback: Callable[[MachineHistoryResult], None] | None = None,
        include_graph_differences: bool = False,
    ) -> MachineHistoryResult:
        resolved_target_store = enrich_site7_target_store(target_store or SITE7_DEFAULT_TARGET_STORE)
        target_days = clamp_site7_recent_days(recent_days)
        self._notify_progress(
            progress_callback,
            0,
            1,
            f"{resolved_target_store.display_name} の店舗ページへ移動しています",
        )
        self._require_playwright()
        _raise_if_site7_cancel_requested(cancel_requested)

        playwright = None
        context = None
        machine_results: list[MachineHistoryResult] = []
        try:
            playwright, context = self._open_fetch_browser_context(browser_visible)
            page = self._prepare_fetch_page(context, browser_visible)
            hall_page_url, hall_html = self._open_target_hall_page(
                page,
                resolved_target_store,
                cancel_requested=cancel_requested,
            )
            store_name = self.extract_store_name(hall_html)
            target_machine_entries = self.extract_target_machine_entries(hall_html)
            total_steps = len(target_machine_entries) + 2

            for machine_index, machine_entry in enumerate(target_machine_entries, start=1):
                _raise_if_site7_cancel_requested(cancel_requested)
                if machine_index > 1:
                    self._wait_between_transitions(page, cancel_requested=cancel_requested)
                    _raise_if_site7_cancel_requested(cancel_requested)
                    page.goto(hall_page_url, wait_until="domcontentloaded", timeout=60_000)
                    self._accept_cookie_banner_if_present(page)

                self._notify_progress(
                    progress_callback,
                    machine_index,
                    total_steps,
                    f"{resolved_target_store.display_name} / {machine_entry.machine_name} のページを開いています",
                )
                self._wait_between_transitions(page, cancel_requested=cancel_requested)
                self._open_target_machine_page(page, machine_entry, cancel_requested=cancel_requested)
                _raise_if_site7_cancel_requested(cancel_requested)
                page.wait_for_selector("#ata0", timeout=60_000)
                machine_page_url = str(page.url)
                machine_html = page.content()
                machine_result = self.parse_machine_history_html(
                    machine_html,
                    store_url=hall_page_url,
                    page_url=machine_page_url,
                    recent_days=target_days,
                    fallback_store_name=store_name,
                    machine_name_override=machine_entry.machine_name,
                )
                machine_results.append(machine_result)
                if machine_result_callback is not None and not include_graph_differences:
                    machine_result_callback(machine_result)
        except PlaywrightError as exc:
            raise self._wrap_playwright_error(exc) from exc
        finally:
            self._release_browser_context(playwright, context)

        _raise_if_site7_cancel_requested(cancel_requested)
        if include_graph_differences and machine_results:
            self._notify_progress(
                progress_callback,
                len(machine_results),
                len(machine_results) + 2,
                f"{resolved_target_store.display_name} の出玉推移グラフを読み取っています",
            )
            machine_results = self._apply_mobile_graph_differences(
                machine_results,
                target_store=resolved_target_store,
                browser_visible=browser_visible,
                cancel_requested=cancel_requested,
                progress_callback=progress_callback,
            )
            if machine_result_callback is not None:
                for machine_result in machine_results:
                    machine_result_callback(machine_result)

        _raise_if_site7_cancel_requested(cancel_requested)
        self._notify_progress(
            progress_callback,
            len(machine_results) + 1,
            len(machine_results) + 2,
            f"{resolved_target_store.display_name} の台データを読み取っています",
        )
        return self._merge_machine_history_results(
            machine_results,
            fallback_store_name=store_name if "store_name" in locals() else resolved_target_store.display_name,
            store_url=hall_page_url if "hall_page_url" in locals() else resolved_target_store.direct_hall_url,
        )

    def _apply_mobile_graph_differences(
        self,
        machine_results: list[MachineHistoryResult],
        target_store: Site7TargetStore,
        browser_visible: bool,
        cancel_requested: Callable[[], bool] | None = None,
        progress_callback: Callable[[FetchProgress], None] | None = None,
    ) -> list[MachineHistoryResult]:
        self._require_playwright()
        _raise_if_site7_cancel_requested(cancel_requested)

        total_graph_count = sum(len(dataset.rows) for result in machine_results for dataset in result.datasets)
        if total_graph_count <= 0:
            return machine_results

        playwright = None
        context = None
        current_graph_count = 0
        try:
            playwright, context = self._launch_mobile_browser_context(browser_visible=browser_visible)
            page = self._prepare_fetch_page(context, browser_visible=browser_visible)
            hall_html = self._open_mobile_target_hall_page(page, target_store, cancel_requested=cancel_requested)
            machine_list_link = self.extract_mobile_slot_machine_list_link(hall_html)
            self._wait_between_transitions(page, cancel_requested=cancel_requested)
            _raise_if_site7_cancel_requested(cancel_requested)
            page.goto(machine_list_link, wait_until="domcontentloaded", timeout=60_000)
            self._accept_cookie_banner_if_present(page)
            machine_list_html = page.content()
            self._wait_between_transitions(page, cancel_requested=cancel_requested)
            machine_link_items = self.extract_mobile_target_machine_links(machine_list_html)
            if not machine_link_items:
                raise ScraperError("スマホ版サイトセブンで対象機種のリンクが見つかりませんでした。")
            machine_links = {entry.machine_name: machine_link for entry, machine_link in machine_link_items}

            for machine_result in machine_results:
                _raise_if_site7_cancel_requested(cancel_requested)
                machine_name = canonical_machine_name(machine_result.datasets[0].machine_name if machine_result.datasets else "")
                machine_link = machine_links.get(machine_name)
                if not machine_link:
                    raise ScraperError(f"スマホ版サイトセブンで {machine_name} の出玉推移グラフ入口が見つかりませんでした。")

                page.goto(machine_link, wait_until="domcontentloaded", timeout=60_000)
                self._accept_cookie_banner_if_present(page)
                machine_html = page.content()
                self._wait_between_transitions(page, cancel_requested=cancel_requested)
                graph_list_link = self.extract_mobile_machine_graph_list_link(machine_html)
                graph_index_link = self.extract_mobile_machine_graph_index_link(machine_html)
                latest_date = self._machine_result_latest_date(machine_result)

                for dataset in machine_result.datasets:
                    day_index = self._mobile_graph_day_index(latest_date, dataset.target_date)
                    if day_index is None:
                        current_graph_count += len(dataset.rows)
                        continue

                    graph_list_url = self._replace_mobile_query_param(graph_list_link, "dtdd", str(day_index))
                    target_slot_numbers = self._dataset_slot_numbers(dataset)
                    list_difference_values, slot_graph_links = self._fetch_mobile_graph_list_page_data(
                        page=page,
                        context=context,
                        start_url=graph_list_url,
                        target_slot_numbers=target_slot_numbers,
                        cancel_requested=cancel_requested,
                    )
                    detail_slot_numbers = {
                        slot_number
                        for slot_number, difference_value in list_difference_values.items()
                        if self._mobile_graph_difference_needs_detail(difference_value)
                    }
                    if not slot_graph_links or detail_slot_numbers:
                        graph_index_url = self._replace_mobile_query_param(graph_index_link, "dtdd", str(day_index))
                        _raise_if_site7_cancel_requested(cancel_requested)
                        page.goto(graph_index_url, wait_until="domcontentloaded", timeout=60_000)
                        self._accept_cookie_banner_if_present(page)
                        graph_index_html = page.content()
                        self._wait_between_transitions(page, cancel_requested=cancel_requested)
                        slot_graph_links.update(self.extract_mobile_slot_graph_links(graph_index_html))

                    self._apply_mobile_graph_differences_to_dataset(
                        page=page,
                        context=context,
                        dataset=dataset,
                        list_difference_values=list_difference_values,
                        detail_slot_numbers=detail_slot_numbers,
                        slot_graph_links=slot_graph_links,
                        day_index=day_index,
                        cancel_requested=cancel_requested,
                        progress_callback=progress_callback,
                        current_graph_count_ref=lambda: current_graph_count,
                        total_graph_count=total_graph_count,
                    )
                    current_graph_count += len(dataset.rows)
        except PlaywrightError as exc:
            raise self._wrap_playwright_error(exc) from exc
        finally:
            self._release_browser_context(playwright, context)

        return machine_results

    def _fetch_mobile_graph_list_page_data(
        self,
        *,
        page: object,
        context: object,
        start_url: str,
        target_slot_numbers: set[str],
        cancel_requested: Callable[[], bool] | None = None,
    ) -> tuple[dict[str, int], dict[str, str]]:
        list_difference_values: dict[str, int] = {}
        slot_graph_links: dict[str, str] = {}
        visited_urls: set[str] = set()
        pending_urls = [start_url]
        seen_page_slots: set[str] = set()

        while pending_urls and len(visited_urls) < SITE7_GRAPH_LIST_MAX_PAGES:
            _raise_if_site7_cancel_requested(cancel_requested)
            graph_list_url = pending_urls.pop(0)
            normalized_url = urljoin(SITE7_MOBILE_TOP_URL, graph_list_url)
            if normalized_url in visited_urls:
                continue
            visited_urls.add(normalized_url)

            page.goto(normalized_url, wait_until="domcontentloaded", timeout=60_000)
            self._accept_cookie_banner_if_present(page)
            graph_list_html = page.content()
            self._wait_between_transitions(page, cancel_requested=cancel_requested)

            page_slot_graph_links = self.extract_mobile_slot_graph_links(graph_list_html)
            page_difference_values = self._fetch_mobile_graph_list_difference_values(
                page=page,
                context=context,
                cancel_requested=cancel_requested,
            )
            page_slot_numbers = set(page_slot_graph_links) | set(page_difference_values)
            new_page_slots = page_slot_numbers - seen_page_slots
            if not page_slot_numbers:
                break
            if len(visited_urls) > 1 and not new_page_slots:
                break

            seen_page_slots.update(page_slot_numbers)
            slot_graph_links.update(page_slot_graph_links)
            list_difference_values.update(page_difference_values)
            covered_slot_numbers = set(slot_graph_links) | set(list_difference_values)
            if target_slot_numbers and target_slot_numbers.issubset(covered_slot_numbers):
                break

            next_urls = self.extract_mobile_graph_list_next_page_links(graph_list_html, normalized_url)
            if not next_urls:
                next_url = self._mobile_next_graph_list_page_url(normalized_url)
                next_urls = [next_url] if next_url else []
            for next_url in next_urls:
                absolute_next_url = urljoin(SITE7_MOBILE_TOP_URL, next_url)
                if absolute_next_url not in visited_urls and absolute_next_url not in pending_urls:
                    pending_urls.append(absolute_next_url)

        return list_difference_values, slot_graph_links

    def _fetch_mobile_graph_list_difference_values(
        self,
        *,
        page: object,
        context: object,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> dict[str, int]:
        difference_values: dict[str, int] = {}
        for entry in self._extract_mobile_graph_list_image_entries(page):
            _raise_if_site7_cancel_requested(cancel_requested)
            slot_number = str(entry.get("slot_number") or "").strip()
            image_url = str(entry.get("image_url") or "").strip()
            if not slot_number or not image_url:
                continue

            image_bytes = self._download_mobile_graph_image(context, image_url, prefer_big=False)
            difference_value = parse_site7_graph_difference_value(image_bytes)
            if difference_value is None:
                continue
            difference_values[slot_number] = difference_value

        return difference_values

    def _extract_mobile_graph_list_image_entries(self, page: object) -> list[dict[str, str]]:
        image_entries = page.evaluate(
            """() => Array.from(document.querySelectorAll("a"))
                .flatMap((anchor) => {
                    const href = anchor.href || anchor.getAttribute("href") || "";
                    if (!href.includes("D3000.do?")) {
                        return [];
                    }
                    const images = Array.from(anchor.querySelectorAll("img"));
                    return images.map((img) => {
                        const rawSrc = img.currentSrc
                            || img.src
                            || img.getAttribute("src")
                            || img.getAttribute("data-src")
                            || img.getAttribute("data-original")
                            || "";
                        let slotNumber = "";
                        try {
                            slotNumber = new URL(href, document.baseURI).searchParams.get("dn") || "";
                        } catch (error) {
                            slotNumber = "";
                        }
                        if (!slotNumber) {
                            const text = [
                                anchor.textContent || "",
                                img.getAttribute("alt") || "",
                                img.getAttribute("title") || ""
                            ].join(" ");
                            const match = text.match(/\\d+/);
                            slotNumber = match ? match[0] : "";
                        }
                        return {
                            slot_number: slotNumber,
                            graph_url: new URL(href, document.baseURI).href,
                            image_url: rawSrc ? new URL(rawSrc, document.baseURI).href : ""
                        };
                    });
                })
                .filter((entry) => entry.slot_number && entry.image_url.includes("RequestSPDedamaTransitionChartForPortal"))"""
        )
        if not isinstance(image_entries, list):
            return []

        result: list[dict[str, str]] = []
        seen_slots: set[str] = set()
        for entry in image_entries:
            if not isinstance(entry, dict):
                continue
            slot_number = str(entry.get("slot_number") or "").strip()
            image_url = str(entry.get("image_url") or "").strip()
            if not slot_number or not image_url or slot_number in seen_slots:
                continue
            seen_slots.add(slot_number)
            result.append(
                {
                    "slot_number": slot_number,
                    "graph_url": str(entry.get("graph_url") or "").strip(),
                    "image_url": image_url,
                }
            )
        return result

    def _mobile_graph_difference_needs_detail(self, difference_value: int) -> bool:
        return abs(difference_value) >= SITE7_GRAPH_LIST_DETAIL_THRESHOLD

    def _dataset_slot_numbers(self, dataset: MachineDataset) -> set[str]:
        try:
            slot_index = dataset.columns.index("台番")
        except ValueError:
            return set()
        return {
            str(row[slot_index]).strip()
            for row in dataset.rows
            if len(row) > slot_index and str(row[slot_index]).strip()
        }

    def _apply_mobile_graph_differences_to_dataset(
        self,
        *,
        page: object,
        context: object,
        dataset: MachineDataset,
        list_difference_values: dict[str, int],
        detail_slot_numbers: set[str],
        slot_graph_links: dict[str, str],
        day_index: int,
        cancel_requested: Callable[[], bool] | None,
        progress_callback: Callable[[FetchProgress], None] | None,
        current_graph_count_ref: Callable[[], int],
        total_graph_count: int,
    ) -> None:
        try:
            slot_index = dataset.columns.index("台番")
            difference_index = dataset.columns.index("差枚")
        except ValueError:
            return

        for row_index, row in enumerate(dataset.rows, start=1):
            _raise_if_site7_cancel_requested(cancel_requested)
            if len(row) <= max(slot_index, difference_index):
                continue

            slot_number = str(row[slot_index]).strip()
            list_difference_value = list_difference_values.get(slot_number)
            if list_difference_value is not None and slot_number not in detail_slot_numbers:
                row[difference_index] = str(list_difference_value)
                continue

            graph_link = slot_graph_links.get(slot_number)
            if not graph_link:
                if list_difference_value is not None:
                    row[difference_index] = str(list_difference_value)
                continue

            current_step = current_graph_count_ref() + row_index
            self._notify_progress(
                progress_callback,
                current_step,
                total_graph_count,
                f"{dataset.machine_name} {dataset.target_date} {slot_number}番台の出玉推移グラフを読み取っています",
            )
            graph_page_url = self._replace_mobile_query_param(graph_link, "dtdd", str(day_index))
            difference_value = self._fetch_mobile_graph_difference_value(
                page,
                context,
                graph_page_url,
                cancel_requested=cancel_requested,
            )
            if difference_value is not None:
                row[difference_index] = str(difference_value)
            elif list_difference_value is not None:
                row[difference_index] = str(list_difference_value)

    def _open_mobile_target_hall_page(
        self,
        page: object,
        target_store: Site7TargetStore,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> str:
        page.goto(SITE7_MOBILE_TOP_URL, wait_until="domcontentloaded", timeout=60_000)
        self._accept_cookie_banner_if_present(page)
        top_html = page.content()
        self._wait_between_transitions(page, cancel_requested=cancel_requested)

        prefecture_link = self.extract_mobile_prefecture_link(top_html, target_store)
        _raise_if_site7_cancel_requested(cancel_requested)
        page.goto(prefecture_link, wait_until="domcontentloaded", timeout=60_000)
        self._accept_cookie_banner_if_present(page)
        prefecture_html = page.content()
        self._wait_between_transitions(page, cancel_requested=cancel_requested)

        area_link = self.extract_mobile_area_link(prefecture_html, target_store)
        _raise_if_site7_cancel_requested(cancel_requested)
        page.goto(area_link, wait_until="domcontentloaded", timeout=60_000)
        self._accept_cookie_banner_if_present(page)
        area_html = page.content()
        self._wait_between_transitions(page, cancel_requested=cancel_requested)

        hall_link = self.extract_mobile_target_hall_link(area_html, target_store)
        _raise_if_site7_cancel_requested(cancel_requested)
        page.goto(hall_link, wait_until="domcontentloaded", timeout=60_000)
        self._accept_cookie_banner_if_present(page)
        return page.content()

    def _fetch_mobile_graph_difference_value(
        self,
        page: object,
        context: object,
        graph_page_url: str,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> int | None:
        page.goto(graph_page_url, wait_until="domcontentloaded", timeout=60_000)
        self._accept_cookie_banner_if_present(page)
        self._wait_between_transitions(page, cancel_requested=cancel_requested)
        page.wait_for_selector("img[src*='RequestSPDedamaTransitionChartForPortal']", timeout=60_000)
        try:
            page.wait_for_load_state("networkidle", timeout=10_000)
        except PlaywrightTimeoutError:
            pass
        image_url = self._extract_visible_mobile_graph_image_url(page)
        if not image_url:
            return None

        image_bytes = self._download_mobile_graph_image(context, image_url, prefer_big=True)
        return parse_site7_graph_difference_value(image_bytes)

    def _extract_visible_mobile_graph_image_url(self, page: object) -> str:
        image_entries = page.evaluate(
            """() => Array.from(document.images)
                .map((img) => {
                    const rect = img.getBoundingClientRect();
                    return {
                        src: img.currentSrc || img.src,
                        width: rect.width,
                        height: rect.height,
                        left: rect.left,
                        top: rect.top
                    };
                })
                .filter((entry) => entry.src.includes("RequestSPDedamaTransitionChartForPortal"))"""
        )
        if not isinstance(image_entries, list):
            return ""

        visible_entries = [
            entry
            for entry in image_entries
            if isinstance(entry, dict)
            and float(entry.get("width") or 0) > 50
            and float(entry.get("height") or 0) > 50
            and -20 <= float(entry.get("left") or 0) <= SITE7_MOBILE_VIEWPORT["width"]
        ]
        selected_entries = visible_entries or [entry for entry in image_entries if isinstance(entry, dict)]
        if not selected_entries:
            return ""
        selected_entry = min(
            selected_entries,
            key=lambda entry: abs(float(entry.get("left") or 0) - SITE7_MOBILE_VIEWPORT["width"] / 2),
        )
        return str(selected_entry.get("src") or "").strip()

    def _download_mobile_graph_image(self, context: object, image_url: str, prefer_big: bool = True) -> bytes:
        target_url = image_url
        if prefer_big and "big=1" not in target_url:
            separator = "&" if "?" in target_url else "?"
            target_url = f"{target_url}{separator}big=1"
        response = context.request.get(target_url)
        if not response.ok and target_url != image_url:
            response = context.request.get(image_url)
        if not response.ok:
            raise ScraperError("スマホ版サイトセブンの出玉推移グラフ画像を取得できませんでした。")
        return response.body()

    def _machine_result_latest_date(self, machine_result: MachineHistoryResult) -> datetime | None:
        parsed_dates: list[datetime] = []
        for dataset in machine_result.datasets:
            try:
                parsed_dates.append(datetime.strptime(dataset.target_date, "%Y-%m-%d"))
            except ValueError:
                continue
        return max(parsed_dates) if parsed_dates else None

    def _mobile_graph_day_index(self, latest_date: datetime | None, target_date: str) -> int | None:
        if latest_date is None:
            return None
        try:
            parsed_target_date = datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            return None
        day_index = (latest_date - parsed_target_date).days
        if day_index < 0 or day_index >= SITE7_MAX_RECENT_DAYS:
            return None
        return day_index

    def _mobile_graph_list_page_number(self, url: str) -> int:
        return self._mobile_graph_list_page_number_or_none(url) or 1

    def _mobile_graph_list_page_number_or_none(self, url: str) -> int | None:
        parts = urlsplit(urljoin(SITE7_MOBILE_TOP_URL, url))
        query_items = dict(parse_qsl(parts.query, keep_blank_values=True))
        for name in ("pan", "page", "pg", "p"):
            value = str(query_items.get(name) or "").strip()
            if not value.isdigit():
                continue
            return max(1, int(value))
        return None

    def _mobile_next_graph_list_page_url(self, url: str) -> str:
        current_page_number = self._mobile_graph_list_page_number(url)
        if current_page_number >= SITE7_GRAPH_LIST_MAX_PAGES:
            return ""
        return self._replace_mobile_query_param(url, "pan", str(current_page_number + 1))

    def _replace_mobile_query_param(self, url: str, name: str, value: str) -> str:
        parts = urlsplit(urljoin(SITE7_MOBILE_TOP_URL, url))
        query_items = parse_qsl(parts.query, keep_blank_values=True)
        replaced = False
        updated_items: list[tuple[str, str]] = []
        for key, current_value in query_items:
            if key == name:
                updated_items.append((key, value))
                replaced = True
            else:
                updated_items.append((key, current_value))
        if not replaced:
            updated_items.append((name, value))
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(updated_items), parts.fragment))

    def extract_store_name(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        hall_name = soup.select_one("#hall_name")
        if hall_name is not None:
            text = hall_name.get_text(strip=True)
            if text:
                return text

        for heading in soup.find_all("h1"):
            text = heading.get_text(strip=True)
            if text:
                return text

        raise ScraperError("サイトセブンの店舗名が見つかりませんでした。")

    def parse_machine_history_html(
        self,
        html: str,
        store_url: str,
        page_url: str,
        recent_days: int,
        fallback_store_name: str = "",
        machine_name_override: str = "",
    ) -> MachineHistoryResult:
        target_days = clamp_site7_recent_days(recent_days)
        soup = BeautifulSoup(html, "html.parser")
        store_name = fallback_store_name.strip() or self.extract_store_name(html)
        machine_name = machine_name_override.strip() or self.extract_machine_name(html)
        base_date = self.extract_updated_date(html)

        datasets: list[MachineDataset] = []
        date_pages: list[StoreDatePage] = []
        for day_index in range(target_days):
            day_container = soup.find(id=f"ata{day_index}")
            if not isinstance(day_container, Tag):
                continue

            target_date = (base_date - timedelta(days=day_index)).strftime("%Y-%m-%d")
            dataset = self._build_dataset_for_day(
                day_container=day_container,
                store_name=store_name,
                store_url=store_url,
                target_date=target_date,
                machine_name=machine_name,
                machine_url=page_url,
            )
            if not dataset.rows:
                continue

            datasets.append(dataset)
            date_pages.append(StoreDatePage(target_date=target_date, date_url=f"{page_url}#ata{day_index}"))

        if not datasets:
            raise ScraperError("サイトセブンの台データが見つかりませんでした。")

        datasets.sort(key=lambda dataset: dataset.target_date)
        date_pages.sort(key=lambda date_page: date_page.target_date)
        return MachineHistoryResult(
            store_name=store_name,
            store_url=store_url,
            start_date=date_pages[0].target_date,
            end_date=date_pages[-1].target_date,
            date_pages=date_pages,
            datasets=datasets,
        )

    def extract_machine_name(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        heading = soup.find("h2")
        if heading is None:
            return SITE7_TARGET_MACHINE_NAME

        text = heading.get_text(" ", strip=True)
        if "【" in text:
            return text.split("【", 1)[0].strip()
        return text or SITE7_TARGET_MACHINE_NAME

    def extract_updated_date(self, html: str) -> datetime:
        soup = BeautifulSoup(html, "html.parser")
        hall_date = soup.select_one("#hall_date")
        search_text = hall_date.get_text(" ", strip=True) if hall_date is not None else soup.get_text(" ", strip=True)
        match = SITE7_UPDATE_DATE_PATTERN.search(search_text)
        if match is None:
            raise ScraperError("サイトセブンの更新日が見つかりませんでした。")

        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))
        hour_text = match.group(4)
        minute_text = match.group(5)
        if hour_text is None or minute_text is None:
            return datetime(year, month, day)

        updated_at = datetime(year, month, day, int(hour_text), int(minute_text))
        if updated_at.hour < SITE7_DATE_BOUNDARY_HOUR:
            updated_at -= timedelta(days=1)
        return updated_at.replace(hour=0, minute=0, second=0, microsecond=0)

    def extract_target_machine_entries(self, html: str) -> list[Site7MachineEntry]:
        soup = BeautifulSoup(html, "html.parser")
        entries: list[Site7MachineEntry] = []
        seen_machine_names: set[str] = set()

        for row in soup.find_all("tr"):
            if row.find("input", attrs={"name": "select"}) is None and row.find("input", attrs={"type": "button"}) is None:
                continue

            display_name = self._extract_machine_label_from_row(row)
            if not display_name or not machine_is_site7_target(display_name):
                continue

            machine_name = canonical_machine_name(display_name, site7_only=True)
            machine_key = machine_name.casefold()
            if machine_key in seen_machine_names:
                continue

            seen_machine_names.add(machine_key)
            entries.append(
                Site7MachineEntry(
                    display_name=display_name,
                    machine_name=machine_name,
                )
            )

        if not entries:
            raise ScraperError(
                "サイトセブンで対象機種の行が見つかりませんでした。\n"
                f"対象語: {'、'.join(SITE7_TARGET_MACHINE_KEYWORDS)}"
            )

        return entries

    def _build_dataset_for_day(
        self,
        day_container: Tag,
        store_name: str,
        store_url: str,
        target_date: str,
        machine_name: str,
        machine_url: str,
    ) -> MachineDataset:
        table = day_container.find("table")
        if table is None:
            return MachineDataset(
                store_name=store_name,
                store_url=store_url,
                target_date=target_date,
                date_url=machine_url,
                machine_name=machine_name,
                machine_url=machine_url,
                columns=["台番", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率"],
                rows=[],
            )

        rows: list[list[str]] = []
        for row in table.find_all("tr")[1:]:
            cells = [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
            if len(cells) < 7:
                continue

            slot_number = self._extract_slot_number(cells[0])
            if not slot_number:
                continue

            row_values = {
                "G数": cells[1] or "-",
                "BB": cells[2] or "-",
                "RB": cells[3] or "-",
            }
            ratio_values = [format_site7_ratio_text(cells[index]) for index in (4, 5, 6)]
            if not any(site7_value_has_data(value) for value in [*row_values.values(), *ratio_values]):
                continue

            rows.append(
                [
                    slot_number,
                    format_machine_difference_for_row(machine_name, row_values),
                    row_values["G数"],
                    "-",
                    row_values["BB"],
                    row_values["RB"],
                    *ratio_values,
                ]
            )

        return MachineDataset(
            store_name=store_name,
            store_url=store_url,
            target_date=target_date,
            date_url=machine_url,
            machine_name=machine_name,
            machine_url=machine_url,
            columns=["台番", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率"],
            rows=rows,
        )

    def _extract_slot_number(self, cell_text: str) -> str:
        match = SITE7_SLOT_NUMBER_PATTERN.search(str(cell_text))
        return match.group(1) if match is not None else ""

    def extract_prefecture_link(self, html: str, target_store: Site7TargetStore | None = None) -> str:
        resolved_target_store = target_store or SITE7_DEFAULT_TARGET_STORE
        return self._extract_link_from_html(
            html,
            link_text=resolved_target_store.prefecture_link_text,
            href_keyword="HallMapSearch.do?",
        )

    def extract_mobile_prefecture_link(self, html: str, target_store: Site7TargetStore | None = None) -> str:
        resolved_target_store = target_store or SITE7_DEFAULT_TARGET_STORE
        return self._extract_mobile_link_from_html(
            html,
            link_text=resolved_target_store.prefecture_name,
            href_keyword="H0810.do?",
        )

    def extract_area_link(self, html: str, target_store: Site7TargetStore | None = None) -> str:
        resolved_target_store = target_store or SITE7_DEFAULT_TARGET_STORE
        return self._extract_link_from_html(
            html,
            link_text=resolved_target_store.area_name,
            href_keyword="HallSearchByArea.do?",
        )

    def extract_mobile_area_link(self, html: str, target_store: Site7TargetStore | None = None) -> str:
        resolved_target_store = target_store or SITE7_DEFAULT_TARGET_STORE
        return self._extract_mobile_link_from_html(
            html,
            link_text=resolved_target_store.area_name,
            href_keyword="H0800.do?",
        )

    def extract_target_hall_search_code(self, html: str, target_store: Site7TargetStore | None = None) -> str:
        resolved_target_store = target_store or SITE7_DEFAULT_TARGET_STORE
        soup = BeautifulSoup(html, "html.parser")
        for hall_link in soup.find_all("a", onclick=True):
            onclick = str(hall_link.get("onclick") or "")
            match = SITE7_HALL_CLICK_PATTERN.search(onclick)
            if match is None:
                continue

            hall_search_code = match.group(1)
            if _site7_hall_id_matches(hall_search_code, resolved_target_store.hall_id):
                return hall_search_code

            hall_container = hall_link.find_parent(class_="hall")
            hall_text = ""
            if hall_container is not None:
                hall_text = hall_container.get_text(" ", strip=True)
            if not hall_text:
                hall_row = hall_link.find_parent(["tr", "li", "div"])
                hall_text = hall_row.get_text(" ", strip=True) if hall_row is not None else hall_link.get_text(" ", strip=True)

            if _site7_lookup_keys_match(
                _build_site7_lookup_keys(hall_text),
                resolved_target_store.hall_match_keys,
                allow_partial=True,
            ):
                return hall_search_code

        raise ScraperError(
            f"サイトセブンで {resolved_target_store.display_name} を選ぶための情報が見つかりませんでした。"
        )

    def extract_mobile_target_hall_link(self, html: str, target_store: Site7TargetStore | None = None) -> str:
        resolved_target_store = target_store or SITE7_DEFAULT_TARGET_STORE
        soup = BeautifulSoup(html, "html.parser")
        for hall_link in soup.find_all("a"):
            href = str(hall_link.get("href") or "").strip()
            if "D0100.do?" not in href or "pmc=" not in href:
                continue

            if _site7_hall_id_matches(href, resolved_target_store.hall_id):
                return urljoin(SITE7_MOBILE_TOP_URL, href)

            hall_text = hall_link.get_text(" ", strip=True)
            if _site7_lookup_keys_match(
                _build_site7_lookup_keys(hall_text),
                resolved_target_store.hall_match_keys,
                allow_partial=True,
            ):
                return urljoin(SITE7_MOBILE_TOP_URL, href)

        raise ScraperError(
            f"スマホ版サイトセブンで {resolved_target_store.display_name} の店舗リンクが見つかりませんでした。"
        )

    def extract_mobile_store_name(
        self,
        html: str,
        target_store: Site7TargetStore | None = None,
    ) -> str:
        resolved_target_store = target_store or SITE7_DEFAULT_TARGET_STORE
        soup = BeautifulSoup(html, "html.parser")
        title_text = soup.title.get_text(" ", strip=True) if soup.title is not None else ""
        if "｜" in title_text:
            title_parts = [part.strip() for part in title_text.split("｜") if part.strip()]
            if len(title_parts) >= 2:
                return title_parts[1]

        page_text = soup.get_text("\n", strip=True)
        for key_source in (resolved_target_store.site7_hall_name, resolved_target_store.display_name):
            key = str(key_source).strip()
            if key and key in page_text:
                return key
        return resolved_target_store.site7_hall_name or resolved_target_store.display_name

    def extract_mobile_slot_machine_list_link(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        fallback_rate_link = ""
        fallback_all_link = ""
        for anchor in soup.find_all("a"):
            href = str(anchor.get("href") or "").strip()
            if "D0300.do?" not in href or "clc=03" not in href:
                continue

            text = anchor.get_text(" ", strip=True)
            normalized_text = _normalize_site7_lookup_text(text)
            absolute_href = urljoin(SITE7_MOBILE_TOP_URL, href)
            if "urt=2173" in href or "1000円46枚" in normalized_text:
                return absolute_href
            if "urt=-1" not in href and not fallback_rate_link:
                fallback_rate_link = absolute_href
            if ("パチスロ" in text and "すべて" in text) or "urt=-1" in href:
                fallback_all_link = absolute_href

        if fallback_rate_link:
            return fallback_rate_link
        if fallback_all_link:
            return fallback_all_link
        raise ScraperError("スマホ版サイトセブンでパチスロ機種一覧のリンクが見つかりませんでした。")

    def extract_mobile_target_machine_link(self, html: str) -> tuple[Site7MachineEntry, str]:
        machine_links = self.extract_mobile_target_machine_links(html)
        fallback_result: tuple[Site7MachineEntry, str] | None = None
        for machine_entry, machine_link in machine_links:
            result = machine_entry, machine_link
            if machine_entry.machine_name == SITE7_TARGET_MACHINE_NAME:
                return result
            if fallback_result is None:
                fallback_result = result

        if fallback_result is not None:
            return fallback_result

        raise ScraperError(
            "スマホ版サイトセブンで対象機種のリンクが見つかりませんでした。\n"
            f"対象語: {'、'.join(SITE7_TARGET_MACHINE_KEYWORDS)}"
        )

    def extract_mobile_target_machine_links(self, html: str) -> list[tuple[Site7MachineEntry, str]]:
        soup = BeautifulSoup(html, "html.parser")
        machine_links: list[tuple[Site7MachineEntry, str]] = []
        seen_machine_names: set[str] = set()
        for anchor in soup.find_all("a"):
            href = str(anchor.get("href") or "").strip()
            if "D2300.do?" not in href:
                continue

            display_name = self._extract_mobile_machine_label(anchor.get_text(" ", strip=True))
            if not display_name or not machine_is_site7_target(display_name):
                continue

            machine_entry = Site7MachineEntry(
                display_name=display_name,
                machine_name=canonical_machine_name(display_name, site7_only=True),
            )
            machine_key = machine_entry.machine_name.casefold()
            if machine_key in seen_machine_names:
                continue
            seen_machine_names.add(machine_key)
            machine_links.append((machine_entry, urljoin(SITE7_MOBILE_TOP_URL, href)))

        return machine_links

    def extract_mobile_machine_graph_list_link(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        fallback_link = ""
        graph_link = ""
        for anchor in soup.find_all("a"):
            href = str(anchor.get("href") or "").strip()
            if not href:
                continue

            text = anchor.get_text(" ", strip=True)
            absolute_href = urljoin(SITE7_MOBILE_TOP_URL, href)
            if "出玉推移一覧" in text:
                return absolute_href
            if "D2400.do?" not in href:
                continue
            if "gc=1" in href:
                return absolute_href
            if "出玉推移グラフ" in text or "gc=2" in href:
                graph_link = absolute_href
            if not fallback_link:
                fallback_link = absolute_href

        if graph_link:
            return self._replace_mobile_query_param(graph_link, "gc", "1")
        if fallback_link:
            return fallback_link
        raise ScraperError("スマホ版サイトセブンで出玉推移一覧のリンクが見つかりませんでした。")

    def extract_mobile_machine_graph_index_link(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        fallback_link = ""
        for anchor in soup.find_all("a"):
            href = str(anchor.get("href") or "").strip()
            if "D2400.do?" not in href:
                continue

            text = anchor.get_text(" ", strip=True)
            absolute_href = urljoin(SITE7_MOBILE_TOP_URL, href)
            if "出玉推移グラフ" in text or "gc=2" in href:
                return absolute_href
            if not fallback_link:
                fallback_link = absolute_href

        if fallback_link:
            return fallback_link
        raise ScraperError("スマホ版サイトセブンで出玉推移グラフのリンクが見つかりませんでした。")

    def extract_mobile_graph_list_next_page_links(self, html: str, current_url: str) -> list[str]:
        current_parts = urlsplit(urljoin(SITE7_MOBILE_TOP_URL, current_url))
        current_query = dict(parse_qsl(current_parts.query, keep_blank_values=True))
        current_page_number = self._mobile_graph_list_page_number(current_url)
        current_day_index = current_query.get("dtdd")
        next_links: list[tuple[int, str]] = []
        seen_links: set[str] = set()
        soup = BeautifulSoup(html, "html.parser")
        for anchor in soup.find_all("a"):
            href = str(anchor.get("href") or "").strip()
            if not href:
                continue
            absolute_href = urljoin(SITE7_MOBILE_TOP_URL, href)
            parts = urlsplit(absolute_href)
            if parts.path != current_parts.path:
                continue

            query = dict(parse_qsl(parts.query, keep_blank_values=True))
            query_gc = query.get("gc")
            current_gc = current_query.get("gc")
            if query_gc not in {None, "", "1", current_gc}:
                continue
            if current_day_index is not None and query.get("dtdd") not in {None, "", current_day_index}:
                continue

            page_number = self._mobile_graph_list_page_number_or_none(absolute_href)
            text = anchor.get_text(" ", strip=True)
            normalized_text = re.sub(r"\s+", "", text).casefold()
            if page_number is None and normalized_text.isdigit():
                page_number = int(normalized_text)
            if page_number is None:
                if normalized_text in {"次", "次へ", "next", "＞", ">", ">>", "≫"} or "次" in normalized_text:
                    page_number = current_page_number + 1
                else:
                    continue
            if page_number <= current_page_number:
                continue
            if absolute_href in seen_links:
                continue
            seen_links.add(absolute_href)
            next_links.append((page_number, absolute_href))

        next_links.sort(key=lambda item: item[0])
        return [link for _, link in next_links]

    def extract_mobile_slot_graph_link(self, html: str) -> tuple[str, str]:
        slot_graph_links = self.extract_mobile_slot_graph_links(html)
        for slot_number in sorted(slot_graph_links, key=lambda value: int(value) if value.isdigit() else value):
            return slot_number, slot_graph_links[slot_number]
        raise ScraperError("スマホ版サイトセブンで台別の出玉推移グラフへのリンクが見つかりませんでした。")

    def extract_mobile_slot_graph_links(self, html: str) -> dict[str, str]:
        soup = BeautifulSoup(html, "html.parser")
        slot_graph_links: dict[str, str] = {}
        for anchor in soup.find_all("a"):
            href = str(anchor.get("href") or "").strip()
            if "D3000.do?" not in href:
                continue

            text = anchor.get_text(" ", strip=True)
            slot_number = self._extract_slot_number(text)
            if not slot_number:
                continue
            slot_graph_links.setdefault(slot_number, urljoin(SITE7_MOBILE_TOP_URL, href))

        return slot_graph_links

    def _extract_mobile_machine_label(self, text: str) -> str:
        label = re.sub(r"\s+", " ", str(text)).strip()
        label = re.sub(r"\s*[\[［]\d+[\]］]\s*$", "", label).strip()
        return label

    def _extract_link_from_html(self, html: str, link_text: str, href_keyword: str = "") -> str:
        target_link_keys = _build_site7_lookup_keys(link_text)
        soup = BeautifulSoup(html, "html.parser")
        for anchor in soup.find_all("a"):
            text = anchor.get_text(" ", strip=True)
            href = str(anchor.get("href") or "").strip()
            if not _site7_lookup_keys_match(
                _build_site7_lookup_keys(text),
                target_link_keys,
                allow_partial=True,
                partial_min_length=2,
            ):
                continue
            if not href:
                continue
            if href_keyword and href_keyword not in href:
                continue
            return urljoin(SITE7_TOP_URL, href)

        raise ScraperError(f"サイトセブンで {link_text} のリンクが見つかりませんでした。")

    def _extract_mobile_link_from_html(self, html: str, link_text: str, href_keyword: str = "") -> str:
        target_link_keys = _build_site7_lookup_keys(link_text)
        soup = BeautifulSoup(html, "html.parser")
        for anchor in soup.find_all("a"):
            text = anchor.get_text(" ", strip=True)
            href = str(anchor.get("href") or "").strip()
            if not _site7_lookup_keys_match(
                _build_site7_lookup_keys(text),
                target_link_keys,
                allow_partial=True,
                partial_min_length=2,
            ):
                continue
            if not href:
                continue
            if href_keyword and href_keyword not in href:
                continue
            return urljoin(SITE7_MOBILE_TOP_URL, href)

        raise ScraperError(f"スマホ版サイトセブンで {link_text} のリンクが見つかりませんでした。")

    def _open_target_hall_page(
        self,
        page: object,
        target_store: Site7TargetStore | None = None,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> tuple[str, str]:
        resolved_target_store = target_store or SITE7_DEFAULT_TARGET_STORE
        _raise_if_site7_cancel_requested(cancel_requested)
        page.goto(SITE7_TOP_URL, wait_until="domcontentloaded", timeout=60_000)
        self._accept_cookie_banner_if_present(page)
        top_html = page.content()
        self._wait_between_transitions(page, cancel_requested=cancel_requested)

        prefecture_link = self.extract_prefecture_link(top_html, resolved_target_store)
        _raise_if_site7_cancel_requested(cancel_requested)
        page.goto(prefecture_link, wait_until="domcontentloaded", timeout=60_000)
        self._accept_cookie_banner_if_present(page)
        prefecture_html = page.content()
        self._wait_between_transitions(page, cancel_requested=cancel_requested)

        area_link = self.extract_area_link(prefecture_html, resolved_target_store)
        _raise_if_site7_cancel_requested(cancel_requested)
        page.goto(area_link, wait_until="domcontentloaded", timeout=60_000)
        self._accept_cookie_banner_if_present(page)
        area_html = page.content()
        target_hall_search_code = self.extract_target_hall_search_code(area_html, resolved_target_store)
        self._wait_between_transitions(page, cancel_requested=cancel_requested)

        try:
            _raise_if_site7_cancel_requested(cancel_requested)
            with page.expect_navigation(wait_until="domcontentloaded", timeout=60_000):
                page.evaluate("(hallCode) => hallClick(hallCode)", target_hall_search_code)
        except PlaywrightTimeoutError:
            pass

        self._wait_between_transitions(page, cancel_requested=cancel_requested)
        self._accept_cookie_banner_if_present(page)
        hall_html = page.content()
        hall_page_url = str(page.url)

        if self._page_is_login_required(hall_page_url, hall_html):
            raise ScraperError("サイトセブンのログインが必要です。先にサイトセブンにログインしてください。")
        if not self._page_has_target_hall_page(hall_page_url, hall_html, resolved_target_store):
            raise ScraperError(f"サイトセブンで {resolved_target_store.display_name} の店舗ページを開けませんでした。")

        return hall_page_url, hall_html

    def _wait_between_transitions(self, page: object, cancel_requested: Callable[[], bool] | None = None) -> None:
        remaining_milliseconds = build_site7_transition_wait_milliseconds()
        while remaining_milliseconds > 0:
            _raise_if_site7_cancel_requested(cancel_requested)
            wait_milliseconds = min(100, remaining_milliseconds)
            page.wait_for_timeout(wait_milliseconds)
            remaining_milliseconds -= wait_milliseconds
        _raise_if_site7_cancel_requested(cancel_requested)

    def _release_browser_context(self, playwright: object | None, context: object | None) -> None:
        self._close_browser_context(context)
        self._stop_playwright(playwright)

    def _close_browser_context(self, context: object | None) -> None:
        if context is None:
            return
        try:
            context.close()
        except Exception:  # noqa: BLE001
            pass

    def _stop_playwright(self, playwright: object | None) -> None:
        if playwright is None:
            return
        try:
            playwright.stop()
        except Exception:  # noqa: BLE001
            pass

    def _accept_cookie_banner_if_present(self, page: object) -> None:
        try:
            button_locator = page.locator("button").filter(has_text="承諾する").first
            if button_locator.count() == 0 or not button_locator.is_visible():
                return
            button_locator.click(timeout=2_000)
            page.wait_for_timeout(300)
        except Exception:  # noqa: BLE001
            pass

    def _open_target_machine_page(
        self,
        page: object,
        machine_entry: Site7MachineEntry,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> None:
        row_locator = page.locator("tr").filter(has_text=machine_entry.display_name).first
        if row_locator.count() == 0:
            raise ScraperError(f"サイトセブンで {machine_entry.machine_name} の行が見つかりませんでした。")

        button_locator = row_locator.locator("input[name='select']").first
        if button_locator.count() == 0:
            button_locator = row_locator.locator("input[value='出玉データ']").first
        if button_locator.count() == 0:
            button_locator = row_locator.locator("input[type='button']").first
        if button_locator.count() == 0:
            raise ScraperError(f"サイトセブンで {machine_entry.machine_name} の出玉ボタンが見つかりませんでした。")

        try:
            _raise_if_site7_cancel_requested(cancel_requested)
            with page.expect_navigation(wait_until="domcontentloaded", timeout=60_000):
                button_locator.click()
        except PlaywrightTimeoutError:
            pass
        self._wait_between_transitions(page, cancel_requested=cancel_requested)

    def _extract_machine_label_from_row(self, row: Tag) -> str:
        paragraph = row.find("p")
        text = paragraph.get_text(" ", strip=True) if paragraph is not None else row.get_text(" ", strip=True)
        text = text.replace("FREE", " ").replace("free", " ")
        text = re.sub(r"\s+", " ", text).strip()
        text = re.sub(r"[\(（]\d+[\)）]\s*$", "", text).strip()
        return text

    def _merge_machine_history_results(
        self,
        machine_results: list[MachineHistoryResult],
        fallback_store_name: str,
        store_url: str,
    ) -> MachineHistoryResult:
        if not machine_results:
            raise ScraperError("サイトセブンの対象機種データが見つかりませんでした。")

        datasets: list[MachineDataset] = []
        date_pages_by_date: dict[str, StoreDatePage] = {}
        skipped_targets: list[tuple[str, str]] = []
        skipped_dates: list[str] = []

        for machine_result in machine_results:
            datasets.extend(machine_result.datasets)
            skipped_targets.extend(machine_result.skipped_targets)
            for skipped_date in machine_result.skipped_dates:
                if skipped_date not in skipped_dates:
                    skipped_dates.append(skipped_date)
            for date_page in machine_result.date_pages:
                date_pages_by_date.setdefault(date_page.target_date, date_page)

        datasets.sort(key=lambda dataset: (dataset.target_date, dataset.machine_name.casefold()))
        date_pages = sorted(date_pages_by_date.values(), key=lambda date_page: date_page.target_date)
        return MachineHistoryResult(
            store_name=fallback_store_name,
            store_url=store_url,
            start_date=date_pages[0].target_date,
            end_date=date_pages[-1].target_date,
            date_pages=date_pages,
            datasets=datasets,
            skipped_targets=skipped_targets,
            skipped_dates=skipped_dates,
        )

    def _wait_for_login_success(self, context: object, timeout_seconds: int) -> bool:
        deadline = time.time() + timeout_seconds

        while time.time() < deadline:
            pages = list(context.pages)
            if not pages:
                return False

            for page in pages:
                page_url = self._safe_page_url(page)
                page_html = self._safe_page_content(page)
                if not page_html:
                    continue

                if self._page_has_hall_content(page_html):
                    return False

                if self._page_indicates_logged_in(page_url, page_html):
                    try:
                        page.wait_for_timeout(1_500)
                    except Exception:  # noqa: BLE001
                        pass
                    return False

            time.sleep(1)

        return True

    def _page_indicates_logged_in(self, page_url: str, html: str) -> bool:
        if any(keyword in (page_url or "") for keyword in SITE7_LOGGED_IN_URL_KEYWORDS):
            return True

        normalized_html = re.sub(r"\s+", "", html)
        if "MypageTop.do" in html and "プロフィール" in normalized_html:
            return True
        if "MypageRegistProfile.do" in html:
            return True
        if "プロフィール変更" in normalized_html and "マイページ" in normalized_html:
            return True
        if "のプロフィール" in normalized_html and "マイページ" in normalized_html:
            return True
        return False

    def _page_has_hall_content(self, html: str) -> bool:
        return 'id="hall_name"' in html or ('id="hall_contents"' in html and "HallSelectLink.do?hallcode=" in html)

    def _page_has_target_hall_page(
        self,
        page_url: str,
        html: str,
        target_store: Site7TargetStore | None = None,
    ) -> bool:
        resolved_target_store = target_store or SITE7_DEFAULT_TARGET_STORE
        if not self._page_has_hall_content(html):
            return False
        hall_id = _normalize_site7_hall_id(resolved_target_store.hall_id)
        if hall_id and hall_id in _normalize_site7_hall_id(f"{page_url} {html}"):
            return True
        return _site7_lookup_keys_match(
            _build_site7_lookup_keys(html),
            resolved_target_store.hall_match_keys,
            allow_partial=True,
        )

    def _safe_page_url(self, page: object) -> str:
        try:
            return str(page.url)
        except Exception:  # noqa: BLE001
            return ""

    def _safe_page_content(self, page: object) -> str:
        try:
            return str(page.content())
        except Exception:  # noqa: BLE001
            return ""

    def _page_is_login_required(self, page_url: str, html: str) -> bool:
        if SITE7_LOGIN_URL_PATTERN.search(page_url or ""):
            return True
        return "MypageLoginTop.do" in html or "ログイン" in html and not self._page_has_hall_content(html)

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
            raise ScraperError(
                "サイトセブン取得に必要な画面操作部品が見つかりません。requirements.txt の内容を入れ直してください。"
            )

    def _wrap_playwright_error(self, exc: Exception) -> ScraperError:
        return ScraperError(
            "サイトセブン用のブラウザを起動できませんでした。\n"
            f"{exc}\n"
            "必要に応じて python -m playwright install chromium を実行してください。"
        )
