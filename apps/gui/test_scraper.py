from __future__ import annotations

import unittest
from datetime import datetime
from pathlib import Path

from bs4 import BeautifulSoup

from main import matches_day_tail
from minrepo_scraper import MinRepoScraper, parse_date_range_input


ROOT_DIR = Path(__file__).resolve().parents[2]
HTML_DIR = ROOT_DIR / "html"


def find_html(folder_name: str) -> str:
    folder = HTML_DIR / folder_name
    html_file = next(folder.glob("*.html"))
    return html_file.read_text(encoding="utf-8")


class FixtureScraper(MinRepoScraper):
    def __init__(self) -> None:
        super().__init__()
        self.store_html = find_html("店舗ページトップ")
        self.date_html = find_html("日付別ページ")
        self.machine_html = find_html("機種別データページ")

    def fetch_html(self, url: str) -> str:
        if "?kishu=" in url:
            return self.machine_html
        if "/tag/" in url:
            return self.store_html
        return self.date_html


class MinRepoScraperTests(unittest.TestCase):
    def test_matches_day_tail(self) -> None:
        self.assertTrue(matches_day_tail("2026-03-07", "7"))
        self.assertTrue(matches_day_tail("2026-03-17", "7"))
        self.assertTrue(matches_day_tail("2026-04-07", "7"))
        self.assertFalse(matches_day_tail("2026-03-08", "7"))
        self.assertTrue(matches_day_tail("2026-03-08", "全て"))

    def test_parse_date_range_input(self) -> None:
        start_date, end_date = parse_date_range_input("2025-12-30 ～ 2026-04-08")

        self.assertEqual(start_date, datetime(2025, 12, 30))
        self.assertEqual(end_date, datetime(2026, 4, 8))

    def test_fetch_store_name_from_saved_html(self) -> None:
        scraper = FixtureScraper()
        result = scraper.fetch_store_name(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
        )

        self.assertEqual(result, "MJアリーナ箱崎店")

    def test_fetch_machine_list_from_saved_html(self) -> None:
        scraper = FixtureScraper()
        result = scraper.fetch_machine_list(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-08",
        )

        self.assertEqual(result.store_name, "MJアリーナ箱崎店")
        self.assertEqual(result.target_date, "2026-04-08")
        self.assertGreater(len(result.machine_entries), 10)
        self.assertIn("ネオアイムジャグラーEX", [machine.name for machine in result.machine_entries])
        self.assertIn("パチスロ 転生したら剣でした", [machine.name for machine in result.machine_entries])
        machine_counts = {machine.name: machine.machine_count for machine in result.machine_entries}
        machine_summaries = {
            machine.name: (
                machine.average_difference,
                machine.average_games,
                machine.win_rate,
                machine.payout_rate,
            )
            for machine in result.machine_entries
        }
        self.assertEqual(machine_counts["ネオアイムジャグラーEX"], 40)
        self.assertEqual(machine_counts["パチスロ 転生したら剣でした"], 1)
        self.assertEqual(machine_summaries["ネオアイムジャグラーEX"], ("227", "3,907", "21/40", "101.9%"))
        self.assertEqual(machine_summaries["パチスロ 転生したら剣でした"], ("613", "389", "-", "152.5%"))

    def test_fetch_machine_dataset_from_saved_html(self) -> None:
        scraper = FixtureScraper()
        result = scraper.fetch_machine_dataset(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-08",
            machine_name="ネオアイムジャグラーEX",
        )

        self.assertEqual(result.store_name, "MJアリーナ箱崎店")
        self.assertEqual(result.target_date, "2026-04-08")
        self.assertEqual(result.machine_name, "ネオアイムジャグラーEX")
        self.assertEqual(
            result.columns,
            ["台番", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率"],
        )
        self.assertEqual(result.rows[0], ["687", "-562", "5,931", "96.8%", "22", "14", "1/165", "1/270", "1/424"])
        self.assertEqual(len(result.rows), 40)

    def test_fetch_machine_history_datasets_from_saved_html(self) -> None:
        scraper = FixtureScraper()
        result = scraper.fetch_machine_history_datasets(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-07 ～ 2026-04-08",
            machine_names=["ネオアイムジャグラーEX"],
        )

        self.assertEqual(result.store_name, "MJアリーナ箱崎店")
        self.assertEqual(result.start_date, "2026-04-07")
        self.assertEqual(result.end_date, "2026-04-08")
        self.assertEqual([page.target_date for page in result.date_pages], ["2026-04-07", "2026-04-08"])
        self.assertEqual([dataset.target_date for dataset in result.datasets], ["2026-04-07", "2026-04-08"])
        self.assertTrue(all(dataset.machine_name == "ネオアイムジャグラーEX" for dataset in result.datasets))

    def test_find_date_pages_handles_year_rollover_without_year_label(self) -> None:
        scraper = MinRepoScraper()
        soup = BeautifulSoup(
            """
            <html>
              <body>
                <time class="date">2026年4月9日</time>
                <div class="table_wrap">
                  <table>
                    <tr><td><a href="/a">1/2(木)</a></td></tr>
                    <tr><td><a href="/b">1/1(水)</a></td></tr>
                    <tr><td><a href="/c">12/31(火)</a></td></tr>
                    <tr><td><a href="/d">12/30(月)</a></td></tr>
                  </table>
                </div>
              </body>
            </html>
            """,
            "html.parser",
        )

        result = scraper.find_date_pages_in_range(
            soup=soup,
            base_url="https://example.com/tag/store/",
            target_date_input="2025-12-30 ～ 2026-01-02",
        )

        self.assertEqual(
            [page.target_date for page in result],
            ["2025-12-30", "2025-12-31", "2026-01-01", "2026-01-02"],
        )


if __name__ == "__main__":
    unittest.main()
