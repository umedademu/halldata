from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag


WEEKDAYS_JP = "月火水木金土日"
INLINE_COOKIE_PATTERN = re.compile(r"\$\.cookie\('([^']+)', '([^']+)'")
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


def parse_date_input(value: str) -> datetime:
    text = value.strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise ScraperError("日付は YYYY-MM-DD 形式で入力してください。")


def format_minrepo_date(date_value: datetime) -> str:
    weekday = WEEKDAYS_JP[date_value.weekday()]
    return f"{date_value.month}/{date_value.day}({weekday})"


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", "", value.replace("\u3000", " ").strip())


class MinRepoScraper:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

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
        target_date = parse_date_input(target_date_input)
        target_date_label = format_minrepo_date(target_date)

        store_html = self.fetch_html(store_url)
        store_soup = BeautifulSoup(store_html, "html.parser")
        store_name = self.extract_store_name(store_soup)
        date_url = self.find_date_url(store_soup, store_url, target_date_label)

        date_html = self.fetch_html(date_url)
        date_soup = BeautifulSoup(date_html, "html.parser")
        return store_name, target_date, date_url, date_soup

    def find_date_url(
        self,
        soup: BeautifulSoup,
        base_url: str,
        target_date_label: str,
    ) -> str:
        for link in soup.select("div.table_wrap table tr td:first-child a"):
            if link.get_text(strip=True) == target_date_label:
                href = link.get("href")
                if href:
                    return urljoin(base_url, href)
        raise ScraperError(f"{target_date_label} の日付ページが見つかりませんでした。")

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
