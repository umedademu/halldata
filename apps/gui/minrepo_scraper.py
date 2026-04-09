from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag


WEEKDAYS_JP = "月火水木金土日"
INLINE_COOKIE_PATTERN = re.compile(r"\$\.cookie\('([^']+)', '([^']+)'")
DATE_RANGE_PATTERN = re.compile(r"\s*[～〜~]\s*")
STORE_DATE_PATTERN = re.compile(r"^(?:(\d{4})/)?(\d{1,2})/(\d{1,2})")
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    )
}


class ScraperError(RuntimeError):
    pass


@dataclass
class MachineEntry:
    name: str
    url: str
    section_name: str
    machine_count: int
    average_difference: str
    average_games: str
    win_rate: str
    payout_rate: str


@dataclass
class MachineListResult:
    store_name: str
    store_url: str
    target_date: str
    date_url: str
    machine_entries: List[MachineEntry]


@dataclass
class MachineDataset:
    store_name: str
    store_url: str
    target_date: str
    date_url: str
    machine_name: str
    machine_url: str
    columns: List[str]
    rows: List[List[str]]


@dataclass
class StoreDatePage:
    target_date: str
    date_url: str


@dataclass
class MachineHistoryResult:
    store_name: str
    store_url: str
    start_date: str
    end_date: str
    date_pages: List[StoreDatePage]
    datasets: List[MachineDataset]
    skipped_targets: List[tuple[str, str]] = field(default_factory=list)


@dataclass
class MachineHistoryContext:
    store_name: str
    store_url: str
    start_date: str
    end_date: str
    date_pages: List[StoreDatePage]


@dataclass
class FetchProgress:
    current_step: int
    total_steps: int
    message: str


def parse_date_input(value: str) -> datetime:
    text = value.strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise ScraperError("日付は YYYY-MM-DD 形式で入力してください。")


def parse_date_range_input(value: str) -> tuple[datetime, datetime]:
    text = value.strip()
    if not text:
        raise ScraperError("期間は YYYY-MM-DD ～ YYYY-MM-DD 形式で入力してください。")

    parts = [part.strip() for part in DATE_RANGE_PATTERN.split(text) if part.strip()]
    if len(parts) == 1:
        start_date = parse_date_input(parts[0])
        return start_date, start_date

    if len(parts) != 2:
        raise ScraperError("期間は YYYY-MM-DD ～ YYYY-MM-DD 形式で入力してください。")

    start_date = parse_date_input(parts[0])
    end_date = parse_date_input(parts[1])
    if start_date > end_date:
        raise ScraperError("期間の開始日は終了日以前で入力してください。")

    return start_date, end_date


def format_minrepo_date(date_value: datetime) -> str:
    weekday = WEEKDAYS_JP[date_value.weekday()]
    return f"{date_value.month}/{date_value.day}({weekday})"


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", "", value.replace("\u3000", " ").strip())


class MinRepoScraper:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def fetch_store_name(self, store_url: str) -> str:
        store_html = self.fetch_html(store_url)
        store_soup = BeautifulSoup(store_html, "html.parser")
        return self.extract_store_name(store_soup)

    def fetch_machine_dataset(
        self,
        store_url: str,
        target_date_input: str,
        machine_name: str,
    ) -> MachineDataset:
        machine_list = self.fetch_machine_list(store_url, target_date_input)
        machine_entry = self.find_machine_entry(machine_list.machine_entries, machine_name)
        return self.fetch_machine_dataset_from_entry(machine_list, machine_entry)

    def fetch_machine_list(
        self,
        store_url: str,
        target_date_input: str,
    ) -> MachineListResult:
        store_name, target_date, date_url, date_soup = self._load_date_page(store_url, target_date_input)
        machine_entries = self.extract_machine_entries(date_soup, date_url)

        return MachineListResult(
            store_name=store_name,
            store_url=store_url,
            target_date=target_date.strftime("%Y-%m-%d"),
            date_url=date_url,
            machine_entries=machine_entries,
        )

    def fetch_machine_history_datasets(
        self,
        store_url: str,
        target_date_input: str,
        machine_names: List[str],
        skip_targets: set[tuple[str, str]] | None = None,
        progress_callback: Callable[[FetchProgress], None] | None = None,
    ) -> MachineHistoryResult:
        context = self.prepare_machine_history_context(store_url, target_date_input)
        datasets: List[MachineDataset] = []
        skipped_targets: List[tuple[str, str]] = []
        normalized_skip_targets = skip_targets or set()
        total_steps = max(1, len(context.date_pages) * (len(machine_names) + 1) + 1)
        current_step = 0

        self._notify_progress(
            progress_callback,
            current_step,
            total_steps,
            f"{len(context.date_pages)}日分の取得を準備中",
        )

        def step_callback(message: str) -> None:
            nonlocal current_step
            current_step += 1
            self._notify_progress(progress_callback, current_step, total_steps, message)

        for date_index, date_page in enumerate(context.date_pages, start=1):
            day_result = self.fetch_machine_history_for_date_page(
                context=context,
                date_page=date_page,
                machine_names=machine_names,
                skip_targets=normalized_skip_targets,
                step_callback=step_callback,
                date_index=date_index,
                total_dates=len(context.date_pages),
            )
            datasets.extend(day_result.datasets)
            skipped_targets.extend(day_result.skipped_targets)

        self._notify_progress(
            progress_callback,
            max(0, total_steps - 1),
            total_steps,
            "台データの取得完了、自動保存中",
        )

        return MachineHistoryResult(
            store_name=context.store_name,
            store_url=context.store_url,
            start_date=context.start_date,
            end_date=context.end_date,
            date_pages=context.date_pages,
            datasets=datasets,
            skipped_targets=skipped_targets,
        )

    def prepare_machine_history_context(
        self,
        store_url: str,
        target_date_input: str,
    ) -> MachineHistoryContext:
        store_name, store_soup = self._load_store_page(store_url)
        start_date, end_date = parse_date_range_input(target_date_input)
        date_pages = self.find_date_pages_in_range(store_soup, store_url, target_date_input)
        return MachineHistoryContext(
            store_name=store_name,
            store_url=store_url,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            date_pages=date_pages,
        )

    def fetch_machine_history_for_date_page(
        self,
        context: MachineHistoryContext,
        date_page: StoreDatePage,
        machine_names: List[str],
        skip_targets: set[tuple[str, str]] | None = None,
        step_callback: Callable[[str], None] | None = None,
        date_index: int | None = None,
        total_dates: int | None = None,
    ) -> MachineHistoryResult:
        date_html = self.fetch_html(date_page.date_url)
        date_soup = BeautifulSoup(date_html, "html.parser")
        machine_entries = self.extract_machine_entries(date_soup, date_page.date_url)
        machine_list = MachineListResult(
            store_name=context.store_name,
            store_url=context.store_url,
            target_date=date_page.target_date,
            date_url=date_page.date_url,
            machine_entries=machine_entries,
        )

        day_prefix = ""
        if date_index is not None and total_dates is not None:
            day_prefix = f"{date_index}/{total_dates}日目 "

        if step_callback is not None:
            step_callback(f"{day_prefix}機種一覧を確認中")

        datasets: List[MachineDataset] = []
        skipped_targets_for_day: List[tuple[str, str]] = []
        normalized_skip_targets = skip_targets or set()

        for machine_index, machine_name in enumerate(machine_names, start=1):
            if (date_page.target_date, normalize_text(machine_name)) in normalized_skip_targets:
                skipped_targets_for_day.append((date_page.target_date, machine_name))
                if step_callback is not None:
                    step_callback(f"{date_page.target_date} の {machine_index}/{len(machine_names)}機種目は取得済みのためスキップ")
                continue

            try:
                machine_entry = self.find_machine_entry(machine_entries, machine_name)
            except ScraperError:
                if step_callback is not None:
                    step_callback(f"{date_page.target_date} の {machine_index}/{len(machine_names)}機種目は見つかりませんでした")
                continue

            datasets.append(self.fetch_machine_dataset_from_entry(machine_list, machine_entry))
            if step_callback is not None:
                step_callback(f"{date_page.target_date} の {machine_index}/{len(machine_names)}機種目を取得中")

        return MachineHistoryResult(
            store_name=context.store_name,
            store_url=context.store_url,
            start_date=date_page.target_date,
            end_date=date_page.target_date,
            date_pages=[date_page],
            datasets=datasets,
            skipped_targets=skipped_targets_for_day,
        )

    def fetch_machine_datasets(
        self,
        machine_list: MachineListResult,
        machine_names: List[str],
    ) -> List[MachineDataset]:
        datasets: List[MachineDataset] = []

        for machine_name in machine_names:
            machine_entry = self.find_machine_entry(machine_list.machine_entries, machine_name)
            datasets.append(self.fetch_machine_dataset_from_entry(machine_list, machine_entry))

        return datasets

    def fetch_machine_dataset_from_entry(
        self,
        machine_list: MachineListResult,
        machine_entry: MachineEntry,
    ) -> MachineDataset:
        machine_html = self.fetch_html(machine_entry.url)
        machine_soup = BeautifulSoup(machine_html, "html.parser")
        columns, rows = self.extract_machine_table(machine_soup)

        if not rows:
            raise ScraperError("台データが見つかりませんでした。")

        return MachineDataset(
            store_name=machine_list.store_name,
            store_url=machine_list.store_url,
            target_date=machine_list.target_date,
            date_url=machine_list.date_url,
            machine_name=machine_entry.name,
            machine_url=machine_entry.url,
            columns=columns,
            rows=rows,
        )

    def fetch_html(self, url: str) -> str:
        response = self.session.get(url, timeout=30)
        response.raise_for_status()

        if self._apply_inline_cookies(response.text):
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

        return response.text

    def _apply_inline_cookies(self, html: str) -> bool:
        changed = False
        for name, value in INLINE_COOKIE_PATTERN.findall(html):
            if not name.startswith("_d"):
                continue
            if self.session.cookies.get(name) == value:
                continue
            self.session.cookies.set(name, value, domain=".min-repo.com", path="/")
            changed = True
        return changed

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

    def extract_store_name(self, soup: BeautifulSoup) -> str:
        heading = soup.find("h1")
        if not heading:
            raise ScraperError("店舗名が見つかりませんでした。")
        return heading.get_text(strip=True)

    def _load_date_page(
        self,
        store_url: str,
        target_date_input: str,
    ) -> tuple[str, datetime, str, BeautifulSoup]:
        _, target_date = parse_date_range_input(target_date_input)
        store_name, store_soup = self._load_store_page(store_url)
        date_url = self.find_date_url(store_soup, store_url, target_date)

        date_html = self.fetch_html(date_url)
        date_soup = BeautifulSoup(date_html, "html.parser")
        return store_name, target_date, date_url, date_soup

    def _load_store_page(
        self,
        store_url: str,
    ) -> tuple[str, BeautifulSoup]:
        store_html = self.fetch_html(store_url)
        store_soup = BeautifulSoup(store_html, "html.parser")
        store_name = self.extract_store_name(store_soup)
        return store_name, store_soup

    def find_date_pages_in_range(
        self,
        soup: BeautifulSoup,
        base_url: str,
        target_date_input: str,
    ) -> List[StoreDatePage]:
        start_date, end_date = parse_date_range_input(target_date_input)
        date_pages = [
            date_page
            for date_page in self._collect_store_date_pages(soup, base_url)
            if start_date.strftime("%Y-%m-%d") <= date_page.target_date <= end_date.strftime("%Y-%m-%d")
        ]

        if not date_pages:
            raise ScraperError("指定期間の日付ページが見つかりませんでした。")

        date_pages.sort(key=lambda date_page: date_page.target_date)
        return date_pages

    def find_date_url(
        self,
        soup: BeautifulSoup,
        base_url: str,
        target_date: datetime,
    ) -> str:
        for date_page in self._collect_store_date_pages(soup, base_url):
            if date_page.target_date == target_date.strftime("%Y-%m-%d"):
                return date_page.date_url

        raise ScraperError(f"{target_date.strftime('%Y-%m-%d')} の日付ページが見つかりませんでした。")

    def find_machine_url(
        self,
        soup: BeautifulSoup,
        base_url: str,
        machine_name: str,
    ) -> str:
        machine_entries = self.extract_machine_entries(soup, base_url)
        machine_entry = self.find_machine_entry(machine_entries, machine_name)
        return machine_entry.url

    def find_machine_entry(
        self,
        machine_entries: List[MachineEntry],
        machine_name: str,
    ) -> MachineEntry:
        target = normalize_text(machine_name)
        for machine_entry in machine_entries:
            if normalize_text(machine_entry.name) == target:
                return machine_entry
        raise ScraperError(f"{machine_name} の機種ページが見つかりませんでした。")

    def extract_machine_entries(
        self,
        soup: BeautifulSoup,
        base_url: str,
    ) -> List[MachineEntry]:
        machine_entries: List[MachineEntry] = []
        seen_names: set[str] = set()

        for tab_content in soup.select("div.tab_content"):
            heading = tab_content.find("h2")
            heading_text = heading.get_text(strip=True) if heading else ""

            if "機種別データ" in heading_text:
                section_name = "機種別"
            elif "バラエティ" in heading_text:
                section_name = "バラエティ"
            else:
                continue

            table = tab_content.find("table")
            if not table:
                continue

            for row in table.find_all("tr"):
                cells = row.find_all("td")
                if not cells:
                    continue

                link = cells[0].find("a")
                if link is None:
                    continue

                href = link.get("href")
                if not href:
                    continue

                name = link.get_text(" ", strip=True)
                normalized_name = normalize_text(name)
                if normalized_name in seen_names:
                    continue

                machine_count = self._extract_machine_count(row, section_name)
                average_difference, average_games, win_rate, payout_rate = self._extract_machine_summary(row, section_name)
                machine_entries.append(
                    MachineEntry(
                        name=name,
                        url=urljoin(base_url, href),
                        section_name=section_name,
                        machine_count=machine_count,
                        average_difference=average_difference,
                        average_games=average_games,
                        win_rate=win_rate,
                        payout_rate=payout_rate,
                    )
                )
                seen_names.add(normalized_name)

        if not machine_entries:
            raise ScraperError("機種一覧が見つかりませんでした。")

        return machine_entries

    def _extract_machine_count(self, row: Tag, section_name: str) -> int:
        if section_name == "バラエティ":
            return 1

        count_text = row.get("data-count", "").strip()
        if count_text.isdigit():
            return int(count_text)

        return 0

    def _extract_machine_summary(self, row: Tag, section_name: str) -> tuple[str, str, str, str]:
        cells = [cell.get_text(strip=True) for cell in row.find_all("td")]

        if section_name == "バラエティ":
            if len(cells) < 5:
                return "-", "-", "-", "-"
            return cells[2], cells[3], "-", cells[4]

        if len(cells) < 5:
            return "-", "-", "-", "-"

        return cells[1], cells[2], cells[3], cells[4]

    def extract_machine_table(self, soup: BeautifulSoup) -> tuple[List[str], List[List[str]]]:
        table = self._find_machine_data_table(soup)

        header_row = table.find("tr")
        if not header_row:
            raise ScraperError("テーブルのヘッダーが見つかりませんでした。")

        columns = [cell.get_text(strip=True) for cell in header_row.find_all("th")]
        rows: List[List[str]] = []

        for row in table.find_all("tr"):
            if "avg_row" in (row.get("class") or []):
                continue

            values = [cell.get_text(strip=True) for cell in row.find_all("td")]
            if not values:
                continue

            if len(values) != len(columns):
                continue

            if values[0] == "平均":
                continue

            rows.append(values)

        return columns, rows

    def _find_machine_data_table(self, soup: BeautifulSoup) -> Tag:
        heading = self._find_data_list_heading(soup)
        if heading:
            table = heading.find_next("table")
            if table:
                return table

        for table in soup.find_all("table"):
            headers = [cell.get_text(strip=True) for cell in table.find_all("th")]
            header_set = set(headers)

            if {"差枚", "G数", "出率"}.issubset(header_set) and ("台番" in header_set or "機種" in header_set):
                return table

        raise ScraperError("機種別のデータ一覧テーブルが見つかりませんでした。")

    def _find_data_list_heading(self, soup: BeautifulSoup) -> Tag | None:
        for heading in soup.find_all("h2"):
            if "データ一覧" in heading.get_text(strip=True):
                return heading
        return None

    def _extract_store_page_year(self, soup: BeautifulSoup) -> int:
        time_tag = soup.find("time", class_="date")
        if time_tag:
            match = re.search(r"(\d{4})年", time_tag.get_text(strip=True))
            if match:
                return int(match.group(1))
        return datetime.now().year

    def _collect_store_date_pages(
        self,
        soup: BeautifulSoup,
        base_url: str,
    ) -> List[StoreDatePage]:
        fallback_year = self._extract_store_page_year(soup)
        current_year = fallback_year
        previous_month: int | None = None
        date_pages: List[StoreDatePage] = []

        for link in soup.select("div.table_wrap table tr td:first-child a"):
            label = link.get_text(strip=True)
            match = STORE_DATE_PATTERN.match(label)
            if not match:
                continue

            explicit_year = match.group(1)
            month = int(match.group(2))
            day = int(match.group(3))
            if explicit_year:
                current_year = int(explicit_year)
            elif previous_month is not None and month > previous_month:
                current_year -= 1

            href = link.get("href")
            if not href:
                previous_month = month
                continue

            parsed_date = datetime(current_year, month, day)
            date_pages.append(
                StoreDatePage(
                    target_date=parsed_date.strftime("%Y-%m-%d"),
                    date_url=urljoin(base_url, href),
                )
            )
            previous_month = month

        return date_pages

    def _parse_store_date_label(
        self,
        label: str,
        fallback_year: int,
    ) -> datetime | None:
        match = STORE_DATE_PATTERN.match(label)
        if not match:
            return None

        year = int(match.group(1)) if match.group(1) else fallback_year
        month = int(match.group(2))
        day = int(match.group(3))
        return datetime(year, month, day)
