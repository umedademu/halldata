from __future__ import annotations

from dataclasses import dataclass
import random
import re
import time
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

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
DEFAULT_SITE7_PREFECTURE_NAME = "福岡県"
SITE7_TARGET_MACHINE_NAME = "ネオアイムジャグラーEX"
SITE7_TARGET_MACHINE_KEYWORDS = tuple(list_site7_target_machine_keywords())
SITE7_MAX_RECENT_DAYS = 8
SITE7_TRANSITION_WAIT_MIN_SECONDS = 2.0
SITE7_TRANSITION_WAIT_MAX_SECONDS = 4.0
SITE7_BROWSER_STATE_DIR_NAME = "site7_browser"
SITE7_UPDATE_DATE_PATTERN = re.compile(r"データ更新日時：\s*(\d{4})/(\d{1,2})/(\d{1,2})")
SITE7_SLOT_NUMBER_PATTERN = re.compile(r"(\d+)")
SITE7_HALL_CLICK_PATTERN = re.compile(r"hallClick\('([^']+)'\)")
SITE7_LOGIN_URL_PATTERN = re.compile(r"(?:Mypage)?Login", re.IGNORECASE)
SITE7_LOGGED_IN_URL_KEYWORDS = (
    "PCCreditAuth.do",
    "MypageTop.do",
    "MypageRegistProfile.do",
)


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
    hall_address: str = ""
    direct_hall_url: str = ""
    hall_name_aliases: tuple[str, ...] = ()

    @property
    def hall_match_texts(self) -> tuple[str, ...]:
        match_texts: list[str] = []
        seen_texts: set[str] = set()
        for candidate in (self.display_name, self.site7_hall_name, *self.hall_name_aliases, self.hall_address):
            normalized_candidate = _normalize_site7_lookup_text(candidate)
            if not normalized_candidate or normalized_candidate in seen_texts:
                continue
            seen_texts.add(normalized_candidate)
            match_texts.append(str(candidate).strip())
        return tuple(match_texts)

    @property
    def prefecture_link_text(self) -> str:
        return _normalize_site7_prefecture_link_text(self.prefecture_name)


SITE7_TARGET_STORES = (
    Site7TargetStore(
        display_name="Aパーク春日店",
        site7_hall_name="Ａパーク春日店",
        prefecture_name=DEFAULT_SITE7_PREFECTURE_NAME,
        hall_address="福岡県春日市日の出町５－２４",
        area_name="春日市",
        direct_hall_url="https://www.d-deltanet.com/pc/HallSelectLink.do?hallcode=235def7f3ed0c81275a2bc47dc5b839a",
        hall_name_aliases=("Aパーク春日店",),
    ),
    Site7TargetStore(
        display_name="GOGOアリーナ天神",
        site7_hall_name="ＧＯＧＯアリーナ天神",
        prefecture_name=DEFAULT_SITE7_PREFECTURE_NAME,
        hall_address="福岡県福岡市中央区天神２－６－３７",
        area_name="福岡市中央区",
        direct_hall_url="https://www.d-deltanet.com/pc/HallSelectLink.do?hallcode=40056006",
        hall_name_aliases=("GOGOアリーナ天神",),
    ),
)
SITE7_TARGET_STORE_DISPLAY_NAMES = tuple(store.display_name for store in SITE7_TARGET_STORES)
SITE7_DEFAULT_TARGET_STORE = SITE7_TARGET_STORES[0]


def _normalize_site7_lookup_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(value))
    return re.sub(r"\s+", "", normalized).casefold()


def _normalize_site7_prefecture_link_text(value: str) -> str:
    text = str(value).strip()
    if text == "北海道":
        return text
    if text.endswith(("都", "府", "県")):
        return text[:-1]
    return text


def find_known_site7_target_store(store_name: str) -> Site7TargetStore | None:
    normalized_store_name = _normalize_site7_lookup_text(store_name)
    if not normalized_store_name:
        return None

    for target_store in SITE7_TARGET_STORES:
        for candidate in (target_store.display_name, target_store.site7_hall_name, *target_store.hall_name_aliases):
            if _normalize_site7_lookup_text(candidate) == normalized_store_name:
                return target_store
    return None


def default_site7_store_settings(store_name: str) -> dict[str, object]:
    known_target_store = find_known_site7_target_store(store_name)
    if known_target_store is not None:
        return {
            "site7_enabled": True,
            "site7_prefecture": known_target_store.prefecture_name,
            "site7_area": known_target_store.area_name,
            "site7_store_name": known_target_store.site7_hall_name,
        }

    stripped_store_name = str(store_name).strip()
    return {
        "site7_enabled": False,
        "site7_prefecture": DEFAULT_SITE7_PREFECTURE_NAME,
        "site7_area": "",
        "site7_store_name": stripped_store_name,
    }


class Site7Scraper:
    def __init__(self, root_dir: Path | None = None) -> None:
        self.root_dir = root_dir or ROOT_DIR
        self.browser_state_dir = self.root_dir / "local_data" / SITE7_BROWSER_STATE_DIR_NAME
        self._visible_browser_playwright: object | None = None
        self._visible_browser_context: object | None = None

    def has_saved_login_state(self) -> bool:
        return self.browser_state_dir.exists() and any(self.browser_state_dir.iterdir())

    def close_visible_browser(self) -> None:
        retained_context = self._visible_browser_context
        retained_playwright = self._visible_browser_playwright
        self._visible_browser_context = None
        self._visible_browser_playwright = None
        self._close_browser_context(retained_context)
        self._stop_playwright(retained_playwright)

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
    ) -> MachineHistoryResult:
        resolved_target_store = target_store or SITE7_DEFAULT_TARGET_STORE
        target_days = clamp_site7_recent_days(recent_days)
        self._notify_progress(
            progress_callback,
            0,
            1,
            f"{resolved_target_store.display_name} の店舗ページへ移動しています",
        )
        self._require_playwright()
        self.close_visible_browser()

        playwright = None
        context = None
        keep_browser_open = False
        machine_results: list[MachineHistoryResult] = []
        try:
            playwright = sync_playwright().start()
            context = playwright.chromium.launch_persistent_context(
                str(self.browser_state_dir),
                headless=not browser_visible,
                locale="ja-JP",
                viewport={"width": 1440, "height": 960},
            )
            page = context.new_page()
            if browser_visible:
                page.bring_to_front()
            hall_page_url, hall_html = self._open_target_hall_page(page, resolved_target_store)
            store_name = self.extract_store_name(hall_html)
            target_machine_entries = self.extract_target_machine_entries(hall_html)
            total_steps = len(target_machine_entries) + 2

            for machine_index, machine_entry in enumerate(target_machine_entries, start=1):
                if machine_index > 1:
                    self._wait_between_transitions(page)
                    page.goto(hall_page_url, wait_until="domcontentloaded", timeout=60_000)
                    self._accept_cookie_banner_if_present(page)

                self._notify_progress(
                    progress_callback,
                    machine_index,
                    total_steps,
                    f"{resolved_target_store.display_name} / {machine_entry.machine_name} のページを開いています",
                )
                self._wait_between_transitions(page)
                self._open_target_machine_page(page, machine_entry)
                page.wait_for_selector("#ata0", timeout=60_000)
                machine_page_url = str(page.url)
                machine_html = page.content()
                machine_results.append(
                    self.parse_machine_history_html(
                        machine_html,
                        store_url=hall_page_url,
                        page_url=machine_page_url,
                        recent_days=target_days,
                        fallback_store_name=store_name,
                        machine_name_override=machine_entry.machine_name,
                    )
                )
            keep_browser_open = browser_visible
        except PlaywrightError as exc:
            raise self._wrap_playwright_error(exc) from exc
        finally:
            self._release_browser_context(playwright, context, keep_open=keep_browser_open)

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
        return datetime(year, month, day)

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

            rows.append(
                [
                    slot_number,
                    format_machine_difference_for_row(machine_name, row_values),
                    row_values["G数"],
                    "-",
                    row_values["BB"],
                    row_values["RB"],
                    format_site7_ratio_text(cells[4]),
                    format_site7_ratio_text(cells[5]),
                    format_site7_ratio_text(cells[6]),
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

    def extract_area_link(self, html: str, target_store: Site7TargetStore | None = None) -> str:
        resolved_target_store = target_store or SITE7_DEFAULT_TARGET_STORE
        return self._extract_link_from_html(
            html,
            link_text=resolved_target_store.area_name,
            href_keyword="HallSearchByArea.do?",
        )

    def extract_target_hall_search_code(self, html: str, target_store: Site7TargetStore | None = None) -> str:
        resolved_target_store = target_store or SITE7_DEFAULT_TARGET_STORE
        normalized_match_texts = {
            _normalize_site7_lookup_text(match_text)
            for match_text in resolved_target_store.hall_match_texts
            if _normalize_site7_lookup_text(match_text)
        }
        soup = BeautifulSoup(html, "html.parser")
        for hall_link in soup.find_all("a", onclick=True):
            onclick = str(hall_link.get("onclick") or "")
            match = SITE7_HALL_CLICK_PATTERN.search(onclick)
            if match is None:
                continue

            hall_container = hall_link.find_parent(class_="hall")
            hall_text = ""
            if hall_container is not None:
                hall_text = hall_container.get_text(" ", strip=True)
            if not hall_text:
                hall_row = hall_link.find_parent(["tr", "li", "div"])
                hall_text = hall_row.get_text(" ", strip=True) if hall_row is not None else hall_link.get_text(" ", strip=True)

            normalized_hall_text = _normalize_site7_lookup_text(hall_text)
            if any(match_text in normalized_hall_text for match_text in normalized_match_texts):
                return match.group(1)

        raise ScraperError(
            f"サイトセブンで {resolved_target_store.display_name} を選ぶための情報が見つかりませんでした。"
        )

    def _extract_link_from_html(self, html: str, link_text: str, href_keyword: str = "") -> str:
        soup = BeautifulSoup(html, "html.parser")
        for anchor in soup.find_all("a"):
            text = anchor.get_text(" ", strip=True)
            href = str(anchor.get("href") or "").strip()
            if text != link_text:
                continue
            if not href:
                continue
            if href_keyword and href_keyword not in href:
                continue
            return urljoin(SITE7_TOP_URL, href)

        raise ScraperError(f"サイトセブンで {link_text} のリンクが見つかりませんでした。")

    def _open_target_hall_page(
        self,
        page: object,
        target_store: Site7TargetStore | None = None,
    ) -> tuple[str, str]:
        resolved_target_store = target_store or SITE7_DEFAULT_TARGET_STORE
        page.goto(SITE7_TOP_URL, wait_until="domcontentloaded", timeout=60_000)
        self._accept_cookie_banner_if_present(page)
        top_html = page.content()
        self._wait_between_transitions(page)

        prefecture_link = self.extract_prefecture_link(top_html, resolved_target_store)
        page.goto(prefecture_link, wait_until="domcontentloaded", timeout=60_000)
        self._accept_cookie_banner_if_present(page)
        prefecture_html = page.content()
        self._wait_between_transitions(page)

        area_link = self.extract_area_link(prefecture_html, resolved_target_store)
        page.goto(area_link, wait_until="domcontentloaded", timeout=60_000)
        self._accept_cookie_banner_if_present(page)
        area_html = page.content()
        target_hall_search_code = self.extract_target_hall_search_code(area_html, resolved_target_store)
        self._wait_between_transitions(page)

        try:
            with page.expect_navigation(wait_until="domcontentloaded", timeout=60_000):
                page.evaluate("(hallCode) => hallClick(hallCode)", target_hall_search_code)
        except PlaywrightTimeoutError:
            pass

        page.wait_for_timeout(1_000)
        self._accept_cookie_banner_if_present(page)
        hall_html = page.content()
        hall_page_url = str(page.url)

        if self._page_is_login_required(hall_page_url, hall_html):
            raise ScraperError("サイトセブンのログインが必要です。先にサイトセブンにログインしてください。")
        if not self._page_has_target_hall_page(hall_page_url, hall_html, resolved_target_store):
            raise ScraperError(f"サイトセブンで {resolved_target_store.display_name} の店舗ページを開けませんでした。")

        return hall_page_url, hall_html

    def _wait_between_transitions(self, page: object) -> None:
        page.wait_for_timeout(build_site7_transition_wait_milliseconds())

    def _release_browser_context(self, playwright: object | None, context: object | None, keep_open: bool = False) -> None:
        if keep_open and playwright is not None and context is not None:
            self.close_visible_browser()
            self._visible_browser_playwright = playwright
            self._visible_browser_context = context
            return

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

    def _open_target_machine_page(self, page: object, machine_entry: Site7MachineEntry) -> None:
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
            with page.expect_navigation(wait_until="domcontentloaded", timeout=60_000):
                button_locator.click()
        except PlaywrightTimeoutError:
            pass
        page.wait_for_timeout(1_000)

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
        normalized_html = _normalize_site7_lookup_text(html)
        for match_text in resolved_target_store.hall_match_texts:
            normalized_match_text = _normalize_site7_lookup_text(match_text)
            if normalized_match_text and normalized_match_text in normalized_html:
                return True
        return False

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
