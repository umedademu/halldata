from __future__ import annotations

import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

from bs4 import BeautifulSoup, Tag

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
SITE7_LOGIN_URL = "https://www.d-deltanet.com/pc/MypageLoginTop.do?redirectLogin=0&skskb="
SITE7_TARGET_HALL_URL = "https://www.d-deltanet.com/pc/HallSelectLink.do?hallcode=235def7f3ed0c81275a2bc47dc5b839a"
SITE7_TARGET_MACHINE_NAME = "ネオアイムジャグラーEX"
SITE7_MAX_RECENT_DAYS = 8
SITE7_BROWSER_STATE_DIR_NAME = "site7_browser"
SITE7_UPDATE_DATE_PATTERN = re.compile(r"データ更新日時：\s*(\d{4})/(\d{1,2})/(\d{1,2})")
SITE7_SLOT_NUMBER_PATTERN = re.compile(r"(\d+)")
SITE7_LOGIN_URL_PATTERN = re.compile(r"(?:Mypage)?Login", re.IGNORECASE)


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


class Site7Scraper:
    def __init__(self, root_dir: Path | None = None) -> None:
        self.root_dir = root_dir or ROOT_DIR
        self.browser_state_dir = self.root_dir / "local_data" / SITE7_BROWSER_STATE_DIR_NAME

    def has_saved_login_state(self) -> bool:
        return self.browser_state_dir.exists() and any(self.browser_state_dir.iterdir())

    def login_interactively(self, timeout_seconds: int = 300) -> None:
        self._require_playwright()
        self.browser_state_dir.mkdir(parents=True, exist_ok=True)

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
                    deadline = time.time() + timeout_seconds
                    while time.time() < deadline:
                        if not context.pages:
                            break
                        time.sleep(1)
                    else:
                        raise ScraperError("ログイン待機がタイムアウトしました。ログイン後にブラウザを閉じてください。")
                finally:
                    try:
                        context.close()
                    except Exception:  # noqa: BLE001
                        pass
        except PlaywrightError as exc:
            raise self._wrap_playwright_error(exc) from exc

        if not self.is_logged_in():
            raise ScraperError("ログイン状態を確認できませんでした。ログイン後にブラウザを閉じてください。")

    def is_logged_in(self) -> bool:
        if not self.has_saved_login_state():
            return False

        self._require_playwright()
        try:
            with sync_playwright() as playwright:
                context = playwright.chromium.launch_persistent_context(
                    str(self.browser_state_dir),
                    headless=True,
                    locale="ja-JP",
                    viewport={"width": 1440, "height": 960},
                )
                try:
                    page = context.new_page()
                    page.goto(SITE7_TARGET_HALL_URL, wait_until="domcontentloaded", timeout=60_000)
                    page.wait_for_timeout(1_000)
                    if self._page_is_login_required(page.url, page.content()):
                        return False
                    return bool(page.locator("#hall_name").count())
                finally:
                    context.close()
        except Exception:  # noqa: BLE001
            return False

    def fetch_target_machine_history(
        self,
        recent_days: int,
        progress_callback: Callable[[FetchProgress], None] | None = None,
    ) -> MachineHistoryResult:
        target_days = clamp_site7_recent_days(recent_days)
        self._notify_progress(progress_callback, 0, 4, "サイトセブンの店舗ページを開いています")
        self._require_playwright()

        try:
            with sync_playwright() as playwright:
                context = playwright.chromium.launch_persistent_context(
                    str(self.browser_state_dir),
                    headless=True,
                    locale="ja-JP",
                    viewport={"width": 1440, "height": 960},
                )
                try:
                    page = context.new_page()
                    page.goto(SITE7_TARGET_HALL_URL, wait_until="domcontentloaded", timeout=60_000)
                    page.wait_for_timeout(1_000)
                    hall_html = page.content()
                    if self._page_is_login_required(page.url, hall_html):
                        raise ScraperError("サイトセブンのログインが必要です。先にサイトセブンにログインしてください。")

                    store_name = self.extract_store_name(hall_html)
                    self._notify_progress(progress_callback, 1, 4, "対象機種ページを開いています")
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
            store_url=SITE7_TARGET_HALL_URL,
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

            rows.append(
                [
                    slot_number,
                    "-",
                    cells[1] or "-",
                    "-",
                    cells[2] or "-",
                    cells[3] or "-",
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

    def _page_is_login_required(self, page_url: str, html: str) -> bool:
        if SITE7_LOGIN_URL_PATTERN.search(page_url or ""):
            return True
        return "MypageLoginTop.do" in html or "ログイン" in html and "hall_name" not in html

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
