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
        target_date = parse_date_input(target_date_input)
        target_date_label = format_minrepo_date(target_date)

        store_html = self.fetch_html(store_url)
        store_soup = BeautifulSoup(store_html, "html.parser")
        store_name = self.extract_store_name(store_soup)
        date_url = self.find_date_url(store_soup, store_url, target_date_label)

        date_html = self.fetch_html(date_url)
        date_soup = BeautifulSoup(date_html, "html.parser")
        machine_url = self.find_machine_url(date_soup, date_url, machine_name)

        machine_html = self.fetch_html(machine_url)
        machine_soup = BeautifulSoup(machine_html, "html.parser")
        columns, rows = self.extract_machine_table(machine_soup)

        if not rows:
            raise ScraperError("台データが見つかりませんでした。")

        return MachineDataset(
            store_name=store_name,
            store_url=store_url,
            target_date=target_date.strftime("%Y-%m-%d"),
            date_url=date_url,
            machine_name=machine_name,
            machine_url=machine_url,
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
        target = normalize_text(machine_name)
        for link in soup.select("table.kishu._2dai td a"):
            if normalize_text(link.get_text(strip=True)) == target:
                href = link.get("href")
                if href:
                    return urljoin(base_url, href)
        raise ScraperError(f"{machine_name} の機種ページが見つかりませんでした。")

    def extract_machine_table(self, soup: BeautifulSoup) -> tuple[List[str], List[List[str]]]:
        heading = self._find_data_list_heading(soup)
        table = heading.find_next("table")
        if not table:
            raise ScraperError("機種別のデータ一覧テーブルが見つかりませんでした。")

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

    def _find_data_list_heading(self, soup: BeautifulSoup) -> Tag:
        for heading in soup.find_all("h2"):
            if "データ一覧" in heading.get_text(strip=True):
                return heading
        raise ScraperError("データ一覧セクションが見つかりませんでした。")
