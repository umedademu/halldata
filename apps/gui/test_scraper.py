from __future__ import annotations

import queue
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from bs4 import BeautifulSoup

from data_persistence import HistoryPersistenceService, build_machine_daily_records, normalize_store_url
from main import (
    SITE7_BROWSER_MODE_HIDDEN,
    SITE7_BROWSER_MODE_VISIBLE,
    MinRepoApp,
    build_recent_date_range_input,
    matches_day_tail,
    normalize_site7_browser_mode,
    parse_recent_days,
    parse_retry_delay_seconds,
)
from minrepo_scraper import FetchProgress, MinRepoScraper, normalize_text, parse_date_range_input
from site7_scraper import SITE7_TARGET_MACHINE_NAME, Site7Scraper, clamp_site7_recent_days


ROOT_DIR = Path(__file__).resolve().parents[2]
HTML_DIR = ROOT_DIR / "html"
GUI_FIXTURE_DIR = Path(__file__).resolve().parent / "test_fixtures"


def find_html(folder_name: str) -> str:
    folder = HTML_DIR / folder_name
    html_file = next(folder.glob("*.html"))
    return html_file.read_text(encoding="utf-8")


def find_gui_fixture(file_name: str) -> str:
    return (GUI_FIXTURE_DIR / file_name).read_text(encoding="utf-8")


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

    def test_build_recent_date_range_input_uses_jst_today(self) -> None:
        result = build_recent_date_range_input("90", datetime(2026, 4, 14, 0, 30, tzinfo=timezone.utc))

        self.assertEqual(result, "2026-01-15 ～ 2026-04-14")

    def test_parse_retry_delay_seconds(self) -> None:
        self.assertEqual(parse_retry_delay_seconds("10"), 10)
        self.assertEqual(parse_retry_delay_seconds("0"), 0)

        with self.assertRaisesRegex(Exception, "再試行の休止秒数"):
            parse_retry_delay_seconds("1.5")

    def test_parse_recent_days(self) -> None:
        self.assertEqual(parse_recent_days("90"), 90)

        with self.assertRaisesRegex(Exception, "直近日数"):
            parse_recent_days("0")

    def test_clamp_site7_recent_days(self) -> None:
        self.assertEqual(clamp_site7_recent_days(3), 3)
        self.assertEqual(clamp_site7_recent_days(90), 8)

    def test_normalize_site7_browser_mode(self) -> None:
        self.assertEqual(normalize_site7_browser_mode("visible"), SITE7_BROWSER_MODE_VISIBLE)
        self.assertEqual(normalize_site7_browser_mode("hidden"), SITE7_BROWSER_MODE_HIDDEN)
        self.assertEqual(normalize_site7_browser_mode("anything"), SITE7_BROWSER_MODE_VISIBLE)

    def test_site7_browser_mode_defaults_to_visible(self) -> None:
        with TemporaryDirectory() as temp_dir:
            app = MinRepoApp.__new__(MinRepoApp)
            app.persistence_service = HistoryPersistenceService(root_dir=Path(temp_dir))

            self.assertEqual(app._load_saved_site7_browser_mode(), SITE7_BROWSER_MODE_VISIBLE)

    def test_gui_settings_keep_schedule_and_site7_browser_mode(self) -> None:
        with TemporaryDirectory() as temp_dir:
            app = MinRepoApp.__new__(MinRepoApp)
            app.persistence_service = HistoryPersistenceService(root_dir=Path(temp_dir))

            app._save_schedule_hour(5)
            app._save_site7_browser_mode(SITE7_BROWSER_MODE_HIDDEN)

            self.assertEqual(app._load_saved_schedule_hour(), 5)
            self.assertEqual(app._load_saved_site7_browser_mode(), SITE7_BROWSER_MODE_HIDDEN)

    def test_run_with_fetch_retries_retries_three_times(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.fetch_cancel_event = threading.Event()
        app.result_queue = queue.Queue()
        calls = 0
        retry_messages: list[tuple[int, int, int]] = []

        def flaky_fetch() -> str:
            nonlocal calls
            calls += 1
            if calls < 4:
                raise RuntimeError("temporary failure")
            return "ok"

        result = app._run_with_fetch_retries(
            flaky_fetch,
            retry_delay_seconds=0,
            retry_status_callback=lambda retry_number, max_retries, delay_seconds: retry_messages.append(
                (retry_number, max_retries, delay_seconds)
            ),
        )

        self.assertEqual(result, "ok")
        self.assertEqual(calls, 4)
        self.assertEqual(retry_messages, [(1, 3, 0), (2, 3, 0), (3, 3, 0)])

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

    def test_fetch_machine_list_uses_latest_available_date(self) -> None:
        scraper = FixtureScraper()
        result = scraper.fetch_machine_list(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-09",
        )

        self.assertEqual(result.target_date, "2026-04-08")

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
        self.assertEqual(result.skipped_targets, [])

    def test_fetch_machine_history_skips_saved_targets(self) -> None:
        scraper = FixtureScraper()
        result = scraper.fetch_machine_history_datasets(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-07 ～ 2026-04-08",
            machine_names=["ネオアイムジャグラーEX"],
            skip_targets={("2026-04-07", normalize_text("ネオアイムジャグラーEX"))},
        )

        self.assertEqual([page.target_date for page in result.date_pages], ["2026-04-07", "2026-04-08"])
        self.assertEqual([dataset.target_date for dataset in result.datasets], ["2026-04-08"])
        self.assertEqual(result.skipped_targets, [("2026-04-07", "ネオアイムジャグラーEX")])

    def test_prepare_machine_history_context_from_saved_html(self) -> None:
        scraper = FixtureScraper()
        context = scraper.prepare_machine_history_context(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-07 ～ 2026-04-08",
        )

        self.assertEqual(context.store_name, "MJアリーナ箱崎店")
        self.assertEqual(context.start_date, "2026-04-07")
        self.assertEqual(context.end_date, "2026-04-08")
        self.assertEqual([page.target_date for page in context.date_pages], ["2026-04-07", "2026-04-08"])

    def test_fetch_machine_history_for_date_page_from_saved_html(self) -> None:
        scraper = FixtureScraper()
        context = scraper.prepare_machine_history_context(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-07 ～ 2026-04-08",
        )

        day_result = scraper.fetch_machine_history_for_date_page(
            context=context,
            date_page=context.date_pages[0],
            machine_names=["ネオアイムジャグラーEX"],
        )

        self.assertEqual(day_result.start_date, "2026-04-07")
        self.assertEqual(day_result.end_date, "2026-04-07")
        self.assertEqual([page.target_date for page in day_result.date_pages], ["2026-04-07"])
        self.assertEqual([dataset.target_date for dataset in day_result.datasets], ["2026-04-07"])
        self.assertEqual(day_result.skipped_targets, [])

    def test_fetch_all_machine_history_for_date_page_from_saved_html(self) -> None:
        scraper = FixtureScraper()
        context = scraper.prepare_machine_history_context(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-07 ～ 2026-04-08",
        )

        day_result = scraper.fetch_all_machine_history_for_date_page(
            context=context,
            date_page=context.date_pages[0],
        )

        self.assertEqual(day_result.start_date, "2026-04-07")
        self.assertEqual(day_result.end_date, "2026-04-07")
        self.assertEqual([page.target_date for page in day_result.date_pages], ["2026-04-07"])
        self.assertGreater(len({dataset.machine_name for dataset in day_result.datasets}), 10)

    def test_fetch_machine_history_progress_from_saved_html(self) -> None:
        scraper = FixtureScraper()
        progress_updates: list[FetchProgress] = []

        scraper.fetch_machine_history_datasets(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-07 ～ 2026-04-08",
            machine_names=["ネオアイムジャグラーEX"],
            progress_callback=progress_updates.append,
        )

        self.assertGreaterEqual(len(progress_updates), 5)
        self.assertEqual(progress_updates[0].current_step, 0)
        self.assertEqual(progress_updates[0].total_steps, 5)
        self.assertEqual(progress_updates[-1].current_step, 4)
        self.assertIn("自動保存中", progress_updates[-1].message)

    def test_build_machine_daily_records_from_history_result(self) -> None:
        scraper = FixtureScraper()
        history_result = scraper.fetch_machine_history_datasets(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-07 ～ 2026-04-08",
            machine_names=["ネオアイムジャグラーEX"],
        )

        records = build_machine_daily_records(history_result)

        self.assertEqual(len(records), 80)
        self.assertEqual(
            records[0],
            {
                "target_date": "2026-04-07",
                "slot_number": "687",
                "machine_name": "ネオアイムジャグラーEX",
                "difference_value": -562,
                "games_count": 5931,
                "payout_rate": 96.8,
                "bb_count": 22,
                "rb_count": 14,
                "combined_ratio_text": "1/165",
                "bb_ratio_text": "1/270",
                "rb_ratio_text": "1/424",
            },
        )

    def test_site7_extract_store_name_from_saved_html(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = find_gui_fixture("site7_machine.html")

        self.assertEqual(scraper.extract_store_name(html), "Ａパーク春日店")

    def test_site7_detects_logged_in_page_from_saved_html(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = find_gui_fixture("site7_logged_in.html")

        self.assertTrue(
            scraper._page_indicates_logged_in(
                "https://www.d-deltanet.com/pc/PCCreditAuth.do?skskb=3",
                html,
            )
        )
        self.assertFalse(
            scraper._page_is_login_required(
                "https://www.d-deltanet.com/pc/PCCreditAuth.do?skskb=3",
                html,
            )
        )

    def test_site7_extract_prefecture_link_from_saved_html(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = find_gui_fixture("site7_top.html")

        self.assertEqual(
            scraper.extract_prefecture_link(html),
            "https://www.d-deltanet.com/pc/HallMapSearch.do?prefecturecode=40",
        )

    def test_site7_extract_area_link_from_saved_html(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = find_gui_fixture("site7_fukuoka.html")

        self.assertEqual(
            scraper.extract_area_link(html),
            "https://www.d-deltanet.com/pc/HallSearchByArea.do?prefecturecode=40&district=40218",
        )

    def test_site7_extract_target_hall_search_code_from_saved_html(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = find_gui_fixture("site7_kasuga.html")

        self.assertEqual(
            scraper.extract_target_hall_search_code(html),
            "ff3cd2a71a6cbc459c80f25b44423ba6",
        )

    def test_site7_parse_machine_history_from_saved_html(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = find_gui_fixture("site7_machine.html")

        history_result = scraper.parse_machine_history_html(
            html,
            store_url="https://example.com/site7",
            page_url="https://example.com/site7/machine",
            recent_days=2,
        )

        self.assertEqual(history_result.store_name, "Ａパーク春日店")
        self.assertEqual(history_result.start_date, "2026-04-24")
        self.assertEqual(history_result.end_date, "2026-04-25")
        self.assertEqual([page.target_date for page in history_result.date_pages], ["2026-04-24", "2026-04-25"])
        self.assertEqual([dataset.target_date for dataset in history_result.datasets], ["2026-04-24", "2026-04-25"])
        self.assertTrue(all(dataset.machine_name == SITE7_TARGET_MACHINE_NAME for dataset in history_result.datasets))
        self.assertEqual(
            history_result.datasets[1].rows[0],
            ["821", "-", "2163", "-", "10", "5", "1/144", "1/216", "1/432"],
        )

    def test_site7_build_machine_daily_records_from_history_result(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = find_gui_fixture("site7_machine.html")
        history_result = scraper.parse_machine_history_html(
            html,
            store_url="https://example.com/site7",
            page_url="https://example.com/site7/machine",
            recent_days=2,
        )

        records = build_machine_daily_records(history_result)

        self.assertEqual(len(records), 4)
        self.assertEqual(
            records[0],
            {
                "target_date": "2026-04-24",
                "slot_number": "821",
                "machine_name": SITE7_TARGET_MACHINE_NAME,
                "difference_value": None,
                "games_count": 5454,
                "payout_rate": None,
                "bb_count": 25,
                "rb_count": 12,
                "combined_ratio_text": "1/147",
                "bb_ratio_text": "1/218",
                "rb_ratio_text": "1/454",
            },
        )

    def test_save_history_result_writes_local_file(self) -> None:
        scraper = FixtureScraper()
        history_result = scraper.fetch_machine_history_datasets(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-07 ～ 2026-04-08",
            machine_names=["ネオアイムジャグラーEX"],
        )

        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
            service._save_to_supabase = lambda snapshot: len(snapshot["records"])  # type: ignore[method-assign]

            summary = service.save_history_result(history_result)

            self.assertFalse(summary.has_errors)
            self.assertTrue(summary.supabase_saved)
            self.assertEqual(summary.supabase_record_count, 80)
            self.assertIsNotNone(summary.local_file_path)
            self.assertTrue(Path(summary.local_file_path).exists())

    def test_save_history_result_marks_full_day_index(self) -> None:
        scraper = FixtureScraper()
        context = scraper.prepare_machine_history_context(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-07 ～ 2026-04-08",
        )
        history_result = scraper.fetch_all_machine_history_for_date_page(
            context=context,
            date_page=context.date_pages[0],
        )

        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
            service._save_to_supabase = lambda snapshot: len(snapshot["records"])  # type: ignore[method-assign]

            summary = service.save_history_result(history_result, full_day=True)
            saved_dates_summary = service.find_saved_full_day_dates(
                store_name="MJアリーナ箱崎店",
                store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
                start_date="2026-04-07",
                end_date="2026-04-08",
            )

            self.assertFalse(summary.has_errors)
            self.assertEqual(saved_dates_summary.saved_dates, {"2026-04-07"})

    def test_save_and_load_registered_stores(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
            stored_stores: list[dict[str, str]] = []

            def fake_save_registered_stores_to_supabase(stores: list[dict[str, str]]) -> int:
                stored_stores[:] = list(stores)
                return len(stores)

            service._save_registered_stores_to_supabase = fake_save_registered_stores_to_supabase  # type: ignore[method-assign]
            service._load_registered_stores_from_supabase = (  # type: ignore[method-assign]
                lambda: service._normalize_registered_stores(stored_stores)
            )

            summary = service.save_registered_stores(
                [
                    {"store_name": "MJアリーナ箱崎店", "store_url": "https://example.com/a"},
                    {"store_name": "ABCホール", "store_url": "https://example.com/b"},
                ]
            )
            loaded_stores = service.load_registered_stores()

            self.assertFalse(summary.has_errors)
            self.assertTrue(summary.supabase_saved)
            self.assertEqual(summary.supabase_store_count, 2)
            self.assertFalse((Path(temp_dir) / "local_data" / "registered_stores.json").exists())
            self.assertEqual(
                loaded_stores,
                [
                    {"store_name": "MJアリーナ箱崎店", "store_url": "https://example.com/a/"},
                    {"store_name": "ABCホール", "store_url": "https://example.com/b/"},
                ],
            )

    def test_normalize_store_url_unifies_percent_case(self) -> None:
        self.assertEqual(
            normalize_store_url("https://min-repo.com/tag/mj%e5%a4%a9%e7%a5%9eiii/"),
            "https://min-repo.com/tag/mj%E5%A4%A9%E7%A5%9Eiii/",
        )

    def test_save_registered_stores_deduplicates_normalized_url(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
            captured_stores: list[dict[str, str]] = []

            def fake_save_registered_stores_to_supabase(stores: list[dict[str, str]]) -> int:
                captured_stores.extend(stores)
                return len(stores)

            service._save_registered_stores_to_supabase = fake_save_registered_stores_to_supabase  # type: ignore[method-assign]
            service._load_registered_stores_from_supabase = (  # type: ignore[method-assign]
                lambda: service._normalize_registered_stores(captured_stores)
            )

            summary = service.save_registered_stores(
                [
                    {"store_name": "GOGOアリーナ天神", "store_url": "https://min-repo.com/tag/mj%e5%a4%a9%e7%a5%9eiii/"},
                    {"store_name": "GOGOアリーナ天神", "store_url": "https://min-repo.com/tag/mj%E5%A4%A9%E7%A5%9Eiii/"},
                ]
            )
            loaded_stores = service.load_registered_stores()

            self.assertFalse(summary.has_errors)
            self.assertEqual(len(captured_stores), 1)
            self.assertEqual(
                loaded_stores,
                [
                    {
                        "store_name": "GOGOアリーナ天神",
                        "store_url": "https://min-repo.com/tag/mj%E5%A4%A9%E7%A5%9Eiii/",
                    }
                ],
            )

    def test_delete_registered_stores_deduplicates_normalized_url(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
            captured_store_urls: list[str] = []

            def fake_delete_registered_stores_from_supabase(store_urls: list[str]) -> int:
                captured_store_urls.extend(store_urls)
                return len(store_urls)

            service._delete_registered_stores_from_supabase = fake_delete_registered_stores_from_supabase  # type: ignore[method-assign]

            deleted_count = service.delete_registered_stores(
                [
                    "https://min-repo.com/tag/mj%e5%a4%a9%e7%a5%9eiii/",
                    "https://min-repo.com/tag/mj%E5%A4%A9%E7%A5%9Eiii/",
                ]
            )

            self.assertEqual(deleted_count, 1)
            self.assertEqual(
                captured_store_urls,
                ["https://min-repo.com/tag/mj%E5%A4%A9%E7%A5%9Eiii/"],
            )

    def test_find_saved_machine_targets_uses_local_snapshot(self) -> None:
        scraper = FixtureScraper()
        history_result = scraper.fetch_machine_history_datasets(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-07 ～ 2026-04-08",
            machine_names=["ネオアイムジャグラーEX"],
        )

        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
            service._save_to_supabase = lambda snapshot: len(snapshot["records"])  # type: ignore[method-assign]
            service._find_saved_machine_targets_from_supabase = lambda **kwargs: set()  # type: ignore[method-assign]

            service.save_history_result(history_result)
            summary = service.find_saved_machine_targets(
                store_name="MJアリーナ箱崎店",
                store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
                start_date="2026-04-07",
                end_date="2026-04-08",
                machine_names=["ネオアイムジャグラーEX", "マイジャグラー"],
            )

            self.assertFalse(summary.has_errors)
            self.assertEqual(
                summary.saved_targets,
                {
                    ("2026-04-07", normalize_text("ネオアイムジャグラーEX")),
                    ("2026-04-08", normalize_text("ネオアイムジャグラーEX")),
                },
            )

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

    def test_find_date_pages_falls_back_to_latest_before_end_date(self) -> None:
        scraper = MinRepoScraper()
        soup = BeautifulSoup(
            """
            <html>
              <body>
                <time class="date">2026年4月14日</time>
                <div class="table_wrap">
                  <table>
                    <tr><td><a href="/a">4/13(月)</a></td></tr>
                    <tr><td><a href="/b">4/12(日)</a></td></tr>
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
            target_date_input="2026-04-14",
        )

        self.assertEqual([page.target_date for page in result], ["2026-04-13"])


if __name__ == "__main__":
    unittest.main()
