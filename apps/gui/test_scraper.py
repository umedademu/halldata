from __future__ import annotations

import unittest
from pathlib import Path

from minrepo_scraper import MinRepoScraper


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
        self.assertEqual(machine_counts["ネオアイムジャグラーEX"], 40)
        self.assertEqual(machine_counts["パチスロ 転生したら剣でした"], 1)

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


if __name__ == "__main__":
    unittest.main()
