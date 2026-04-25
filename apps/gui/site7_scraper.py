from __future__ import annotations

import random
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from machine_difference import format_machine_difference_for_row
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
SITE7_TARGET_HALL_URL = "https://www.d-deltanet.com/pc/HallSelectLink.do?hallcode=235def7f3ed0c81275a2bc47dc5b839a"
SITE7_TARGET_PREFECTURE_NAME = "福岡"
SITE7_TARGET_PREFECTURE_URL_KEYWORD = "HallMapSearch.do?prefecturecode=40"
SITE7_TARGET_AREA_NAME = "春日市"
SITE7_TARGET_AREA_URL_KEYWORD = "HallSearchByArea.do?prefecturecode=40&district=40218"
SITE7_TARGET_HALL_NAME = "Ａパーク春日店"
SITE7_TARGET_HALL_ADDRESS = "福岡県春日市日の出町５－２４"
SITE7_TARGET_MACHINE_NAME = "ネオアイムジャグラーEX"
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


class Site7Scraper:
    def __init__(self, root_dir: Path | None = None) -> None:
        self.root_dir = root_dir or ROOT_DIR
        self.browser_state_dir = self.root_dir / "local_data" / SITE7_BROWSER_STATE_DIR_NAME

    def has_saved_login_state(self) -> bool:
        return self.browser_state_dir.exists() and any(self.browser_state_dir.iterdir())

    def login_interactively(self, timeout_seconds: int = 300) -> None:
        self._require_playwright()
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
                    hall_page_url, hall_html = self._open_target_hall_page(page)
                    if self._page_is_login_required(hall_page_url, hall_html):
                        return False
                    return self._page_has_target_hall_page(hall_page_url, hall_html)
                finally:
                    context.close()
        except Exception:  # noqa: BLE001
            return False

    def fetch_target_machine_history(
        self,
        recent_days: int,
        browser_visible: bool = False,
        progress_callback: Callable[[FetchProgress], None] | None = None,
    ) -> MachineHistoryResult:
        target_days = clamp_site7_recent_days(recent_days)
        self._notify_progress(progress_callback, 0, 4, "サイトセブンのトップから店舗ページへ移動しています")
        self._require_playwright()

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
                    hall_page_url, hall_html = self._open_target_hall_page(page)
                    store_name = self.extract_store_name(hall_html)
                    self._notify_progress(progress_callback, 1, 4, "対象機種ページを開いています")
                    self._wait_between_transitions(page)
                    self._open_target_machine_page(page)
                    page.wait_for_selector("#ata0", timeout=60_000)
                    machine_page_url = page.url
                    machine_html = page.content()
                finally:
                    context.close()
        except PlaywrightError as exc:
            raise self._wrap_playwright_error(exc) from exc

        self._notify_progress(progress_callback, 2, 4, "台データを読み取っています")
        return self.parse_machine_history_html(
            machine_html,
            store_url=hall_page_url if "hall_page_url" in locals() else SITE7_TARGET_HALL_URL,
            page_url=machine_page_url if "machine_page_url" in locals() else SITE7_TARGET_HALL_URL,
            recent_days=target_days,
            fallback_store_name=store_name,
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
    ) -> MachineHistoryResult:
        target_days = clamp_site7_recent_days(recent_days)
        soup = BeautifulSoup(html, "html.parser")
        store_name = fallback_store_name.strip() or self.extract_store_name(html)
        machine_name = self.extract_machine_name(html)
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

    def extract_prefecture_link(self, html: str) -> str:
        return self._extract_link_from_html(
            html,
            link_text=SITE7_TARGET_PREFECTURE_NAME,
            href_keyword=SITE7_TARGET_PREFECTURE_URL_KEYWORD,
        )

    def extract_area_link(self, html: str) -> str:
        return self._extract_link_from_html(
            html,
            link_text=SITE7_TARGET_AREA_NAME,
            href_keyword=SITE7_TARGET_AREA_URL_KEYWORD,
        )

    def extract_target_hall_search_code(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        for hall_block in soup.select("div.hall"):
            hall_text = hall_block.get_text(" ", strip=True)
            if SITE7_TARGET_HALL_NAME not in hall_text and SITE7_TARGET_HALL_ADDRESS not in hall_text:
                continue

            hall_link = hall_block.find("a", onclick=True)
            if hall_link is None:
                continue

            onclick = str(hall_link.get("onclick") or "")
            match = SITE7_HALL_CLICK_PATTERN.search(onclick)
            if match is not None:
                return match.group(1)

        raise ScraperError(f"サイトセブンで {SITE7_TARGET_HALL_NAME} を選ぶための情報が見つかりませんでした。")

    def _extract_link_from_html(self, html: str, link_text: str, href_keyword: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        for anchor in soup.find_all("a"):
            text = anchor.get_text(" ", strip=True)
            href = str(anchor.get("href") or "").strip()
            if text != link_text:
                continue
            if not href or href_keyword not in href:
                continue
            return urljoin(SITE7_TOP_URL, href)

        raise ScraperError(f"サイトセブンで {link_text} のリンクが見つかりませんでした。")

    def _open_target_hall_page(self, page: object) -> tuple[str, str]:
        page.goto(SITE7_TOP_URL, wait_until="domcontentloaded", timeout=60_000)
        self._accept_cookie_banner_if_present(page)
        top_html = page.content()
        self._wait_between_transitions(page)

        prefecture_link = self.extract_prefecture_link(top_html)
        page.goto(prefecture_link, wait_until="domcontentloaded", timeout=60_000)
        self._accept_cookie_banner_if_present(page)
        prefecture_html = page.content()
        self._wait_between_transitions(page)

        area_link = self.extract_area_link(prefecture_html)
        page.goto(area_link, wait_until="domcontentloaded", timeout=60_000)
        self._accept_cookie_banner_if_present(page)
        area_html = page.content()
        target_hall_search_code = self.extract_target_hall_search_code(area_html)
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
        if not self._page_has_target_hall_page(hall_page_url, hall_html):
            raise ScraperError(f"サイトセブンで {SITE7_TARGET_HALL_NAME} の店舗ページを開けませんでした。")

        return hall_page_url, hall_html

    def _wait_between_transitions(self, page: object) -> None:
        page.wait_for_timeout(build_site7_transition_wait_milliseconds())

    def _accept_cookie_banner_if_present(self, page: object) -> None:
        try:
            button_locator = page.locator("button").filter(has_text="承諾する").first
            if button_locator.count() == 0 or not button_locator.is_visible():
                return
            button_locator.click(timeout=2_000)
            page.wait_for_timeout(300)
        except Exception:  # noqa: BLE001
            pass

    def _open_target_machine_page(self, page: object) -> None:
        row_locator = page.locator("tr").filter(has_text=SITE7_TARGET_MACHINE_NAME).first
        if row_locator.count() == 0:
            raise ScraperError(f"サイトセブンで {SITE7_TARGET_MACHINE_NAME} の行が見つかりませんでした。")

        button_locator = row_locator.locator("input[value='出玉データ']").first
        if button_locator.count() == 0:
            raise ScraperError(f"サイトセブンで {SITE7_TARGET_MACHINE_NAME} の出玉ボタンが見つかりませんでした。")

        try:
            with page.expect_navigation(wait_until="domcontentloaded", timeout=60_000):
                button_locator.click()
        except PlaywrightTimeoutError:
            pass
        page.wait_for_timeout(1_000)

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

    def _page_has_target_hall_page(self, page_url: str, html: str) -> bool:
        if not self._page_has_hall_content(html):
            return False
        return SITE7_TARGET_HALL_NAME in html or "id=\"hall_name\"" in html and SITE7_TARGET_HALL_ADDRESS in html

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
