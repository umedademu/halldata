from __future__ import annotations

import json
import queue
import threading
import unittest
from unittest import mock
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from bs4 import BeautifulSoup

from data_persistence import (
    DATA_SOURCE_MINREPO,
    DATA_SOURCE_SITE7,
    HistoryPersistenceService,
    build_machine_daily_records,
    build_store_machine_daily_detail_payloads,
    build_store_machine_summary_payloads,
    build_supabase_result_payload,
    choose_preferred_store,
    normalize_store_name_key,
    normalize_store_url,
)
from main import (
    SITE7_BROWSER_MODE_HIDDEN,
    SITE7_BROWSER_MODE_VISIBLE,
    MinRepoApp,
    RegisteredStore,
    build_recent_date_range_input,
    filter_site7_history_result_by_saved_slots,
    filter_site7_history_result_by_saved_targets,
    matches_day_tail,
    normalize_site7_browser_mode,
    parse_recent_days,
    parse_retry_delay_seconds,
    scheduled_fetch_due_date,
)
from machine_difference import calculate_machine_difference_value, canonical_machine_name, machine_requires_slot_resolution
from machine_difference import machine_slot_resolution_group
from minrepo_scraper import FetchProgress, MachineHistoryResult, MinRepoScraper, ScraperError, normalize_text, parse_date_range_input
from site7_scraper import (
    DEFAULT_SITE7_PREFECTURE_NAME,
    SITE7_TARGET_MACHINE_NAME,
    SITE7_TARGET_MACHINE_KEYWORDS,
    SITE7_TARGET_STORE_DISPLAY_NAMES,
    SITE7_TARGET_STORES,
    Site7MachineEntry,
    Site7FetchCancelled,
    Site7Scraper,
    Site7TargetStore,
    clamp_site7_recent_days,
    default_site7_store_settings,
    enrich_site7_target_store,
)
from site7_scraper import build_site7_transition_wait_milliseconds


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


class MappingScraper(MinRepoScraper):
    def __init__(self, html_by_url: dict[str, str]) -> None:
        super().__init__()
        self.html_by_url = html_by_url

    def fetch_html(self, url: str) -> str:
        if url not in self.html_by_url:
            raise AssertionError(f"未定義のURLです: {url}")
        return self.html_by_url[url]


class FakeClosableContext:
    def __init__(self) -> None:
        self.close_count = 0

    def close(self) -> None:
        self.close_count += 1


class FakePlayableBrowser:
    def __init__(self) -> None:
        self.stop_count = 0

    def stop(self) -> None:
        self.stop_count += 1


class FakeJsonResponse:
    def __init__(self, body: object) -> None:
        self._body = body

    def raise_for_status(self) -> None:
        return None

    def json(self) -> object:
        return self._body


class FakeStateWidget:
    def __init__(self) -> None:
        self.state = ""

    def configure(self, **kwargs: object) -> None:
        if "state" in kwargs:
            self.state = str(kwargs["state"])


class FakeVariable:
    def __init__(self, value: bool = False) -> None:
        self.value = value

    def get(self) -> bool:
        return self.value


class FakeTextVariable:
    def __init__(self, value: str = "") -> None:
        self.value = value

    def get(self) -> str:
        return self.value

    def set(self, value: str) -> None:
        self.value = value


class FakeTreeview:
    def __init__(self, selected_items: tuple[str, ...] = ()) -> None:
        self.selected_items = selected_items

    def selection(self) -> tuple[str, ...]:
        return self.selected_items


class FakeWaitingPage:
    def __init__(self) -> None:
        self.wait_calls: list[int] = []

    def wait_for_timeout(self, milliseconds: int) -> None:
        self.wait_calls.append(milliseconds)


class FakeRetainedPage:
    def __init__(self) -> None:
        self.bring_to_front_count = 0
        self.wait_selector_calls: list[tuple[str, int]] = []
        self.url = "https://example.com/machine"

    def bring_to_front(self) -> None:
        self.bring_to_front_count += 1

    def wait_for_selector(self, selector: str, timeout: int = 0) -> None:
        self.wait_selector_calls.append((selector, timeout))

    def content(self) -> str:
        return "<html></html>"


class FakeRetainedContext(FakeClosableContext):
    def __init__(self, page: FakeRetainedPage | None = None) -> None:
        super().__init__()
        self.pages = [page or FakeRetainedPage()]

    def new_page(self) -> FakeRetainedPage:
        page = FakeRetainedPage()
        self.pages.append(page)
        return page


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

    def test_scheduled_fetch_due_date_returns_today_only_when_due(self) -> None:
        now = datetime(2026, 4, 28, 1, 5, tzinfo=timezone.utc)

        self.assertEqual(scheduled_fetch_due_date(10, None, now), "2026-04-28")
        self.assertIsNone(scheduled_fetch_due_date(9, None, now))
        self.assertIsNone(scheduled_fetch_due_date(10, "2026-04-28", now))

    def test_clamp_site7_recent_days(self) -> None:
        self.assertEqual(clamp_site7_recent_days(3), 3)
        self.assertEqual(clamp_site7_recent_days(90), 8)

    def test_site7_transition_wait_milliseconds_uses_given_value(self) -> None:
        self.assertEqual(build_site7_transition_wait_milliseconds(lambda start, end: 2.5), 2500)

    def test_site7_transition_wait_milliseconds_clamps_min_and_max(self) -> None:
        self.assertEqual(build_site7_transition_wait_milliseconds(lambda start, end: 1.0), 2000)
        self.assertEqual(build_site7_transition_wait_milliseconds(lambda start, end: 9.0), 4000)

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

    def test_window_close_can_choose_exit(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app._quit_application = mock.Mock()
        app._hide_to_resident = mock.Mock()

        with mock.patch("main.messagebox.askyesnocancel", return_value=True):
            app._on_window_close()

        app._quit_application.assert_called_once_with()
        app._hide_to_resident.assert_not_called()

    def test_window_close_can_choose_resident(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app._quit_application = mock.Mock()
        app._hide_to_resident = mock.Mock()

        with mock.patch("main.messagebox.askyesnocancel", return_value=False):
            app._on_window_close()

        app._hide_to_resident.assert_called_once_with()
        app._quit_application.assert_not_called()

    def test_window_close_can_cancel(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app._quit_application = mock.Mock()
        app._hide_to_resident = mock.Mock()

        with mock.patch("main.messagebox.askyesnocancel", return_value=None):
            app._on_window_close()

        app._quit_application.assert_not_called()
        app._hide_to_resident.assert_not_called()

    def test_quit_application_stops_tray_and_closes_browser(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        tray_icon = mock.Mock()
        app.tray_icon = tray_icon
        app.tray_thread = object()
        app.site7_scraper = mock.Mock()
        app.root = mock.Mock()

        app._quit_application()

        tray_icon.stop.assert_called_once_with()
        self.assertIsNone(app.tray_icon)
        self.assertIsNone(app.tray_thread)
        app.site7_scraper.close_visible_browser.assert_called_once_with()
        app.root.destroy.assert_called_once_with()

    def test_run_scheduled_fetch_if_due_waits_for_startup_confirmation(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.scheduled_fetch_hour = 10
        app.scheduled_last_run_date = None
        app.scheduled_pending_date = None
        app.scheduled_startup_prompt_date = "2026-04-28"
        app.is_busy = False
        app.schedule_status_var = FakeTextVariable()
        start_calls: list[str] = []
        app._start_scheduled_fetch = lambda: start_calls.append("started")

        with mock.patch("main.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime(2026, 4, 28, 1, 5, tzinfo=timezone.utc)
            app._run_scheduled_fetch_if_due()

        self.assertEqual(start_calls, [])
        self.assertEqual(app.schedule_status_var.get(), "本日 10 時の定期実行を確認待ち")

    def test_prompt_scheduled_fetch_on_startup_can_skip_today(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.scheduled_fetch_hour = 10
        app.scheduled_last_run_date = None
        app.scheduled_pending_date = None
        app.scheduled_startup_prompt_date = "2026-04-28"
        app.is_busy = False
        app.schedule_status_var = FakeTextVariable()
        app._start_scheduled_fetch = mock.Mock()

        with (
            mock.patch("main.scheduled_fetch_due_date", return_value="2026-04-28"),
            mock.patch("main.messagebox.askyesno", return_value=False),
        ):
            app._prompt_scheduled_fetch_on_startup_if_needed()

        self.assertEqual(app.scheduled_last_run_date, "2026-04-28")
        self.assertIsNone(app.scheduled_startup_prompt_date)
        self.assertEqual(app.schedule_status_var.get(), "本日 10 時の定期実行は見送りました")
        app._start_scheduled_fetch.assert_not_called()

    def test_prompt_scheduled_fetch_on_startup_can_start_now(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.scheduled_fetch_hour = 10
        app.scheduled_last_run_date = None
        app.scheduled_pending_date = None
        app.scheduled_startup_prompt_date = "2026-04-28"
        app.is_busy = False
        app.schedule_status_var = FakeTextVariable()
        app._start_scheduled_fetch = mock.Mock()

        with (
            mock.patch("main.scheduled_fetch_due_date", return_value="2026-04-28"),
            mock.patch("main.messagebox.askyesno", return_value=True),
        ):
            app._prompt_scheduled_fetch_on_startup_if_needed()

        self.assertEqual(app.scheduled_last_run_date, "2026-04-28")
        self.assertIsNone(app.scheduled_startup_prompt_date)
        app._start_scheduled_fetch.assert_called_once_with()

    def test_update_button_states_enables_site7_cancel_button_while_site7_fetching(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.current_history_result = None
        app.skip_comparison_display_var = FakeVariable(False)
        app.fetch_cancel_event = threading.Event()
        app.is_busy = True
        app.active_operation_kind = "site7_fetch"
        app.registered_store_tree = FakeTreeview()

        widget_names = (
            "fetch_button",
            "cancel_fetch_button",
            "target_date_entry",
            "retry_delay_entry",
            "schedule_hour_entry",
            "apply_schedule_button",
            "clear_schedule_button",
            "comparison_day_tail_selector",
            "comparison_focus_button",
            "skip_comparison_display_button",
            "notify_fetch_complete_button",
            "site7_login_button",
            "site7_fetch_button",
            "site7_cancel_button",
            "site7_browser_visible_radio",
            "site7_browser_hidden_radio",
            "register_store_button",
            "register_store_url_entry",
            "register_store_site7_button",
            "register_store_prefecture_entry",
            "register_store_area_entry",
            "register_store_site7_store_name_entry",
            "update_registered_store_button",
            "clear_register_store_form_button",
            "select_all_stores_button",
            "clear_store_selection_button",
            "refresh_registered_stores_button",
            "delete_registered_stores_button",
        )
        for widget_name in widget_names:
            setattr(app, widget_name, FakeStateWidget())

        app._update_button_states()

        self.assertEqual(app.cancel_fetch_button.state, "normal")
        self.assertEqual(app.site7_fetch_button.state, "disabled")
        self.assertEqual(app.site7_cancel_button.state, "normal")

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

    def test_fetch_machine_list_skips_same_day_placeholder_page(self) -> None:
        store_url = "https://example.com/tag/test-store/"
        unavailable_date_url = "https://example.com/20260428/"
        available_date_url = "https://example.com/20260427/"
        scraper = MappingScraper(
            {
                store_url: """
                    <html>
                      <body>
                        <h1>テスト店</h1>
                        <time class="date">2026年4月28日</time>
                        <div class="table_wrap">
                          <table>
                            <tr><td><a href="https://example.com/20260428/">2026/4/28(火)</a></td></tr>
                            <tr><td><a href="https://example.com/20260427/">2026/4/27(月)</a></td></tr>
                          </table>
                        </div>
                      </body>
                    </html>
                """,
                unavailable_date_url: """
                    <html>
                      <body>
                        <div class="tab_content">
                          <h2>機種別データ（2台以上設置機種）</h2>
                        </div>
                      </body>
                    </html>
                """,
                available_date_url: """
                    <html>
                      <body>
                        <div class="tab_content">
                          <h2>機種別データ（2台以上設置機種）</h2>
                          <table>
                            <tr data-count="1">
                              <td><a href="https://example.com/machine">テスト機</a></td>
                              <td>100</td>
                              <td>2000</td>
                              <td>1/1</td>
                              <td>101%</td>
                            </tr>
                          </table>
                        </div>
                      </body>
                    </html>
                """,
            }
        )

        result = scraper.fetch_machine_list(
            store_url=store_url,
            target_date_input="2026-04-28",
        )

        self.assertEqual(result.target_date, "2026-04-27")
        self.assertEqual([machine.name for machine in result.machine_entries], ["テスト機"])

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

    def test_prepare_machine_history_context_trims_unavailable_latest_date(self) -> None:
        store_url = "https://example.com/tag/test-store/"
        unavailable_date_url = "https://example.com/20260428/"
        available_date_url = "https://example.com/20260427/"
        scraper = MappingScraper(
            {
                store_url: """
                    <html>
                      <body>
                        <h1>テスト店</h1>
                        <time class="date">2026年4月28日</time>
                        <div class="table_wrap">
                          <table>
                            <tr><td><a href="https://example.com/20260428/">2026/4/28(火)</a></td></tr>
                            <tr><td><a href="https://example.com/20260427/">2026/4/27(月)</a></td></tr>
                          </table>
                        </div>
                      </body>
                    </html>
                """,
                unavailable_date_url: """
                    <html>
                      <body>
                        <div class="tab_content">
                          <h2>機種別データ（2台以上設置機種）</h2>
                        </div>
                      </body>
                    </html>
                """,
                available_date_url: """
                    <html>
                      <body>
                        <div class="tab_content">
                          <h2>機種別データ（2台以上設置機種）</h2>
                          <table>
                            <tr data-count="1">
                              <td><a href="https://example.com/machine">テスト機</a></td>
                              <td>100</td>
                              <td>2000</td>
                              <td>1/1</td>
                              <td>101%</td>
                            </tr>
                          </table>
                        </div>
                      </body>
                    </html>
                """,
            }
        )

        context = scraper.prepare_machine_history_context(
            store_url=store_url,
            target_date_input="2026-04-27 ～ 2026-04-28",
        )

        self.assertEqual(context.start_date, "2026-04-27")
        self.assertEqual(context.end_date, "2026-04-27")
        self.assertEqual([page.target_date for page in context.date_pages], ["2026-04-27"])

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
                "data_source": DATA_SOURCE_MINREPO,
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

    def test_calculate_machine_difference_value_for_registered_machine(self) -> None:
        difference_value = calculate_machine_difference_value(
            "ネオアイムジャグラーEX",
            {
                "G数": "8000",
                "BB": "30",
                "RB": "15",
            },
        )

        self.assertEqual(difference_value, -852)

    def test_canonical_machine_name_matches_site7_keyword(self) -> None:
        self.assertEqual(canonical_machine_name("SアイムジャグラーＥＸ", site7_only=True), "SアイムジャグラーＥＸ")
        self.assertEqual(canonical_machine_name("ネオアイムジャグラーEX", site7_only=True), "ネオアイムジャグラーEX")
        self.assertEqual(canonical_machine_name("マイジャグラー", site7_only=True), "マイジャグラーV")
        self.assertEqual(canonical_machine_name("ゴーゴージャグラー3", site7_only=True), "ゴーゴージャグラー３")
        self.assertEqual(canonical_machine_name("ファンキージャグラー2", site7_only=True), "ファンキージャグラー２ＫＴ")
        self.assertEqual(canonical_machine_name("ハッピージャグラーV", site7_only=True), "ハッピージャグラーＶＩＩＩ")

    def test_machine_requires_slot_resolution_for_neo_and_s(self) -> None:
        self.assertTrue(machine_requires_slot_resolution("ネオアイムジャグラーEX"))
        self.assertTrue(machine_requires_slot_resolution("SアイムジャグラーＥＸ"))
        self.assertFalse(machine_requires_slot_resolution("ゴーゴージャグラー３"))

    def test_machine_slot_resolution_group_is_shared_by_neo_and_s(self) -> None:
        self.assertEqual(
            machine_slot_resolution_group("ネオアイムジャグラーEX"),
            machine_slot_resolution_group("SアイムジャグラーＥＸ"),
        )

    def test_site7_extract_store_name_from_saved_html(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = find_gui_fixture("site7_machine.html")

        self.assertEqual(scraper.extract_store_name(html), "Ａパーク春日店")

    def test_site7_target_store_names_include_gogo_arena_tenjin(self) -> None:
        self.assertEqual(
            SITE7_TARGET_STORE_DISPLAY_NAMES,
            ("Aパーク春日店", "GOGOアリーナ天神"),
        )
        self.assertEqual(SITE7_TARGET_STORES[1].area_name, "福岡市中央区")
        self.assertEqual(SITE7_TARGET_STORES[0].prefecture_link_text, "福岡")
        self.assertEqual(
            default_site7_store_settings("GOGOアリーナ天神"),
            {
                "site7_enabled": True,
                "site7_prefecture": "福岡県",
                "site7_area": "福岡市中央区",
                "site7_store_name": "ＧＯＧＯアリーナ天神",
            },
        )

    def test_default_site7_store_settings_accepts_store_name_variation(self) -> None:
        self.assertEqual(
            default_site7_store_settings("Ａパーク春日"),
            {
                "site7_enabled": True,
                "site7_prefecture": "福岡県",
                "site7_area": "春日市",
                "site7_store_name": "Ａパーク春日店",
            },
        )

    def test_site7_extract_target_machine_entries_from_saved_html(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = """
<!DOCTYPE html>
<html lang="ja">
  <body>
    <table class="slot">
      <tr>
        <td class="clear">
          <p><span>ネオアイムジャグラーEX(25)</span></p>
          <ul><li><input type="button" name="select" value="出玉データ"></li></ul>
        </td>
      </tr>
      <tr>
        <td class="clear">
          <p><span>SアイムジャグラーＥＸ(20)</span></p>
          <ul><li><input type="button" name="select" value="出玉データ"></li></ul>
        </td>
      </tr>
      <tr>
        <td class="clear">
          <p><span>マイジャグラーV(18)</span></p>
          <ul><li><input type="button" name="select" value="出玉データ"></li></ul>
        </td>
      </tr>
      <tr>
        <td class="clear">
          <p><span>ゴーゴージャグラー3(12)</span></p>
          <ul><li><input type="button" name="select" value="出玉データ"></li></ul>
        </td>
      </tr>
      <tr>
        <td class="clear">
          <p><span>ハナハナホウオウ(10)</span></p>
          <ul><li><input type="button" name="select" value="出玉データ"></li></ul>
        </td>
      </tr>
    </table>
  </body>
</html>
"""

        entries = scraper.extract_target_machine_entries(html)

        self.assertEqual(
            [(entry.display_name, entry.machine_name) for entry in entries],
            [
                ("ネオアイムジャグラーEX", "ネオアイムジャグラーEX"),
                ("SアイムジャグラーＥＸ", "SアイムジャグラーＥＸ"),
                ("マイジャグラーV", "マイジャグラーV"),
                ("ゴーゴージャグラー3", "ゴーゴージャグラー３"),
            ],
        )
        self.assertIn("マイジャグラー", SITE7_TARGET_MACHINE_KEYWORDS)
        self.assertIn("ネオアイムジャグラー", SITE7_TARGET_MACHINE_KEYWORDS)
        self.assertIn("SアイムジャグラーＥＸ", SITE7_TARGET_MACHINE_KEYWORDS)

    def test_site7_release_browser_context_keeps_visible_browser_open(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        context = FakeClosableContext()
        playwright = FakePlayableBrowser()

        scraper._release_browser_context(playwright, context, keep_open=True)

        self.assertIs(scraper._visible_browser_context, context)
        self.assertIs(scraper._visible_browser_playwright, playwright)
        self.assertEqual(context.close_count, 0)
        self.assertEqual(playwright.stop_count, 0)

        scraper.close_visible_browser()

        self.assertEqual(context.close_count, 1)
        self.assertEqual(playwright.stop_count, 1)

    def test_site7_replacing_visible_browser_closes_previous_one(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        first_context = FakeClosableContext()
        first_playwright = FakePlayableBrowser()
        second_context = FakeClosableContext()
        second_playwright = FakePlayableBrowser()

        scraper._release_browser_context(first_playwright, first_context, keep_open=True)
        scraper._release_browser_context(second_playwright, second_context, keep_open=True)

        self.assertEqual(first_context.close_count, 1)
        self.assertEqual(first_playwright.stop_count, 1)
        self.assertIs(scraper._visible_browser_context, second_context)
        self.assertIs(scraper._visible_browser_playwright, second_playwright)

    def test_site7_fetch_reuses_visible_browser_when_available(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        page = FakeRetainedPage()
        context = FakeRetainedContext(page)
        playwright = FakePlayableBrowser()
        expected_result = MachineHistoryResult(
            store_name="Aパーク春日店",
            store_url="https://example.com/hall",
            start_date="2026-04-25",
            end_date="2026-04-25",
            date_pages=[],
            datasets=[],
        )
        scraper._visible_browser_context = context
        scraper._visible_browser_playwright = playwright
        scraper._require_playwright = mock.Mock()
        scraper._open_target_hall_page = mock.Mock(return_value=("https://example.com/hall", "<html></html>"))
        scraper.extract_store_name = mock.Mock(return_value="Aパーク春日店")
        scraper.extract_target_machine_entries = mock.Mock(
            return_value=[Site7MachineEntry(display_name=SITE7_TARGET_MACHINE_NAME, machine_name=SITE7_TARGET_MACHINE_NAME)]
        )
        scraper._wait_between_transitions = mock.Mock()
        scraper._accept_cookie_banner_if_present = mock.Mock()
        scraper._open_target_machine_page = mock.Mock()
        scraper.parse_machine_history_html = mock.Mock(return_value=expected_result)
        scraper._merge_machine_history_results = mock.Mock(return_value=expected_result)

        with mock.patch("site7_scraper.sync_playwright") as sync_playwright_mock:
            result = scraper.fetch_target_machine_history(recent_days=1, browser_visible=True)

        self.assertIs(result, expected_result)
        sync_playwright_mock.assert_not_called()
        self.assertEqual(page.bring_to_front_count, 1)
        self.assertEqual(page.wait_selector_calls, [("#ata0", 60_000)])
        self.assertIs(scraper._visible_browser_context, context)
        self.assertIs(scraper._visible_browser_playwright, playwright)
        self.assertEqual(context.close_count, 0)
        self.assertEqual(playwright.stop_count, 0)

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

    def test_site7_extract_prefecture_link_accepts_suffix_variation(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = """
<!DOCTYPE html>
<html lang="ja">
  <body>
    <a href="HallMapSearch.do?prefecturecode=40">福岡県</a>
  </body>
</html>
"""

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

    def test_site7_extract_area_link_for_gogo_store(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = """
<!DOCTYPE html>
<html lang="ja">
  <body>
    <a href="HallSearchByArea.do?prefecturecode=40&district=40133">福岡市中央区</a>
    <a href="HallSearchByArea.do?prefecturecode=40&district=40218">春日市</a>
  </body>
</html>
"""

        self.assertEqual(
            scraper.extract_area_link(html, SITE7_TARGET_STORES[1]),
            "https://www.d-deltanet.com/pc/HallSearchByArea.do?prefecturecode=40&district=40133",
        )

    def test_site7_extract_area_link_accepts_spacing_variation(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = """
<!DOCTYPE html>
<html lang="ja">
  <body>
    <a href="HallSearchByArea.do?prefecturecode=40&district=40133">福岡市 中央区</a>
  </body>
</html>
"""

        self.assertEqual(
            scraper.extract_area_link(html, SITE7_TARGET_STORES[1]),
            "https://www.d-deltanet.com/pc/HallSearchByArea.do?prefecturecode=40&district=40133",
        )

    def test_site7_extract_target_hall_search_code_from_saved_html(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = find_gui_fixture("site7_kasuga.html")

        self.assertEqual(
            scraper.extract_target_hall_search_code(html),
            "ff3cd2a71a6cbc459c80f25b44423ba6",
        )

    def test_site7_extract_target_hall_search_code_accepts_registered_store_input(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = find_gui_fixture("site7_kasuga.html")
        target_store = RegisteredStore(
            name="Aパーク春日店",
            url="https://example.com/kasuga",
            site7_enabled=True,
            site7_prefecture="福岡県",
            site7_area="春日市",
            site7_store_name="Aパーク春日店",
        ).to_site7_target_store()

        self.assertEqual(
            scraper.extract_target_hall_search_code(html, target_store),
            "ff3cd2a71a6cbc459c80f25b44423ba6",
        )

    def test_enrich_site7_target_store_restores_known_store_address(self) -> None:
        target_store = enrich_site7_target_store(
            Site7TargetStore(
                display_name="Aパーク春日店",
                site7_hall_name="Aパーク春日店",
                prefecture_name="福岡県",
                area_name="春日市",
                hall_name_aliases=("Aパーク春日店",),
            )
        )

        self.assertEqual(target_store.hall_address, "福岡県春日市日の出町５－２４")
        self.assertIn("Ａパーク春日店", target_store.hall_name_aliases)

    def test_site7_extract_target_hall_search_code_for_gogo_store(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = """
<!DOCTYPE html>
<html lang="ja">
  <body>
    <div class="hall">
      <a onclick="javascript:hallClick('11111111111111111111111111111111')">店舗詳細</a>
      <p>福岡県福岡市中央区天神２－６－４１</p>
    </div>
    <div class="hall">
      <a onclick="javascript:hallClick('22222222222222222222222222222222')">店舗詳細</a>
      <p>福岡県福岡市中央区天神２－６－３７</p>
    </div>
  </body>
</html>
"""

        self.assertEqual(
            scraper.extract_target_hall_search_code(html, SITE7_TARGET_STORES[1]),
            "22222222222222222222222222222222",
        )

    def test_site7_wait_between_transitions_can_be_cancelled(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        page = FakeWaitingPage()
        calls = 0

        def cancel_requested() -> bool:
            nonlocal calls
            calls += 1
            return calls >= 3

        with mock.patch("site7_scraper.build_site7_transition_wait_milliseconds", return_value=350):
            with self.assertRaises(Site7FetchCancelled):
                scraper._wait_between_transitions(page, cancel_requested=cancel_requested)

        self.assertEqual(page.wait_calls, [100, 100])

    def test_run_site7_fetch_many_reports_registered_store_name_when_store_fetch_fails(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.fetch_cancel_event = threading.Event()
        app.result_queue = queue.Queue()
        app._fetch_single_site7_store = mock.Mock(side_effect=ScraperError("前回のブラウザを閉じられません"))
        target_store = RegisteredStore(name="Aパーク春日店", url="https://example.com/store")

        with self.assertRaisesRegex(ScraperError, "Aパーク春日店: 前回のブラウザを閉じられません"):
            app._run_site7_fetch_many(
                target_stores=[target_store],
                recent_days=1,
                retry_delay_seconds=0,
                browser_visible=True,
            )

        kind, progress = app.result_queue.get_nowait()
        self.assertEqual(kind, "fetch_progress")
        self.assertEqual(progress.message, "1/1 Aパーク春日店 は取得失敗")

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
            ["821", "336", "2163", "-", "10", "5", "1/144", "1/216", "1/432"],
        )

    def test_site7_extract_updated_date_uses_previous_day_before_four(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = '<p id="hall_date">データ更新日時：2026/04/28 03:59</p>'

        updated_date = scraper.extract_updated_date(html)

        self.assertEqual(updated_date, datetime(2026, 4, 27))

    def test_site7_extract_updated_date_keeps_same_day_from_four(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = '<p id="hall_date">データ更新日時：2026/04/28 04:00</p>'

        updated_date = scraper.extract_updated_date(html)

        self.assertEqual(updated_date, datetime(2026, 4, 28))

    def test_site7_parse_machine_history_uses_four_oclock_boundary(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = find_gui_fixture("site7_machine.html").replace("2026/04/25 15:15", "2026/04/28 01:00")

        history_result = scraper.parse_machine_history_html(
            html,
            store_url="https://example.com/site7",
            page_url="https://example.com/site7/machine",
            recent_days=2,
        )

        self.assertEqual(history_result.start_date, "2026-04-26")
        self.assertEqual(history_result.end_date, "2026-04-27")
        self.assertEqual([page.target_date for page in history_result.date_pages], ["2026-04-26", "2026-04-27"])

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
                "machine_name": "ネオアイムジャグラーEX",
                "data_source": DATA_SOURCE_SITE7,
                "difference_value": 735,
                "games_count": 5454,
                "payout_rate": None,
                "bb_count": 25,
                "rb_count": 12,
                "combined_ratio_text": "1/147",
                "bb_ratio_text": "1/218",
                "rb_ratio_text": "1/454",
            },
        )

    def test_build_supabase_result_payload_rounds_fractional_difference_value(self) -> None:
        payload = build_supabase_result_payload(
            {
                "target_date": "2026-04-24",
                "slot_number": "821",
                "machine_name": SITE7_TARGET_MACHINE_NAME,
                "data_source": DATA_SOURCE_SITE7,
                "difference_value": 735.3,
                "games_count": 5454,
                "payout_rate": None,
                "bb_count": 25,
                "rb_count": 12,
                "combined_ratio_text": "1/147",
                "bb_ratio_text": "1/218",
                "rb_ratio_text": "1/454",
            },
            store_id="store-1",
            updated_at="2026-04-25T12:34:56+09:00",
        )

        self.assertEqual(payload["difference_value"], 735)
        self.assertEqual(payload["data_source"], DATA_SOURCE_SITE7)
        self.assertEqual(payload["store_id"], "store-1")

    def test_build_store_machine_summary_payloads_uses_latest_date_only(self) -> None:
        payloads = build_store_machine_summary_payloads(
            [
                {
                    "target_date": "2026-04-24",
                    "slot_number": "101",
                    "machine_name": "ゴーゴージャグラー３",
                    "difference_value": 100,
                    "games_count": 2000,
                    "payout_rate": 101.5,
                },
                {
                    "target_date": "2026-04-25",
                    "slot_number": "101",
                    "machine_name": "ゴーゴージャグラー３",
                    "difference_value": 300,
                    "games_count": 4000,
                    "payout_rate": 104.0,
                },
                {
                    "target_date": "2026-04-25",
                    "slot_number": "102",
                    "machine_name": "ゴーゴージャグラー３",
                    "difference_value": 500,
                    "games_count": 6000,
                    "payout_rate": 106.0,
                },
            ],
            store_id="store-1",
            updated_at="2026-04-25T12:34:56+09:00",
        )

        self.assertEqual(len(payloads), 1)
        self.assertEqual(
            payloads[0],
            {
                "store_id": "store-1",
                "machine_name": "ゴーゴージャグラー３",
                "latest_date": "2026-04-25",
                "slot_count": 2,
                "average_difference": 400.0,
                "average_games": 5000.0,
                "average_payout": 105.0,
                "updated_at": "2026-04-25T12:34:56+09:00",
            },
        )

    def test_build_store_machine_daily_detail_payloads_groups_by_machine_and_date(self) -> None:
        payloads = build_store_machine_daily_detail_payloads(
            [
                {
                    "target_date": "2026-04-25",
                    "slot_number": "102",
                    "machine_name": "ゴーゴージャグラー３",
                    "difference_value": 500,
                    "games_count": 6000,
                    "payout_rate": 106.0,
                    "bb_count": 28,
                    "rb_count": 18,
                    "combined_ratio_text": "1/130",
                    "bb_ratio_text": "1/214",
                    "rb_ratio_text": "1/333",
                },
                {
                    "target_date": "2026-04-25",
                    "slot_number": "101",
                    "machine_name": "ゴーゴージャグラー３",
                    "difference_value": 300.4,
                    "games_count": 4000,
                    "payout_rate": 104.0,
                    "bb_count": 21,
                    "rb_count": 14,
                    "combined_ratio_text": "1/144",
                    "bb_ratio_text": "1/191",
                    "rb_ratio_text": "1/286",
                },
                {
                    "target_date": "2026-04-24",
                    "slot_number": "101",
                    "machine_name": "ゴーゴージャグラー３",
                    "difference_value": 100,
                    "games_count": 2000,
                    "payout_rate": 101.0,
                    "bb_count": 10,
                    "rb_count": 5,
                },
            ],
            store_id="store-1",
            updated_at="2026-04-25T12:34:56+09:00",
        )

        self.assertEqual(len(payloads), 2)
        self.assertEqual(payloads[0]["machine_name"], "ゴーゴージャグラー３")
        self.assertEqual(payloads[0]["target_date"], "2026-04-25")
        self.assertEqual(payloads[0]["slot_count"], 2)
        self.assertEqual(payloads[0]["average_difference"], 400.2)
        self.assertEqual(
            payloads[0]["records_by_slot"],
            {
                "101": {
                    "difference_value": 300,
                    "games_count": 4000,
                    "payout_rate": 104.0,
                    "bb_count": 21,
                    "rb_count": 14,
                    "combined_ratio_text": "1/144",
                    "bb_ratio_text": "1/191",
                    "rb_ratio_text": "1/286",
                },
                "102": {
                    "difference_value": 500,
                    "games_count": 6000,
                    "payout_rate": 106.0,
                    "bb_count": 28,
                    "rb_count": 18,
                    "combined_ratio_text": "1/130",
                    "bb_ratio_text": "1/214",
                    "rb_ratio_text": "1/333",
                },
            },
        )

    def test_save_to_supabase_refreshes_machine_summary_table(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
            captured_result_posts: list[list[dict[str, object]]] = []
            captured_summary_posts: list[list[dict[str, object]]] = []
            captured_daily_detail_posts: list[list[dict[str, object]]] = []
            captured_summary_deletes: list[dict[str, str]] = []
            captured_daily_detail_deletes: list[dict[str, str]] = []

            class FakeSession:
                def post(
                    self,
                    endpoint: str,
                    headers: dict[str, str],
                    json: list[dict[str, object]],
                    timeout: int = 30,
                ) -> FakeJsonResponse:
                    if "store_machine_daily_details" in endpoint:
                        captured_daily_detail_posts.append(json)
                    elif "store_machine_summaries" in endpoint:
                        captured_summary_posts.append(json)
                    else:
                        captured_result_posts.append(json)
                    return FakeJsonResponse([])

                def delete(
                    self,
                    endpoint: str,
                    params: dict[str, str],
                    headers: dict[str, str],
                    timeout: int = 30,
                ) -> FakeJsonResponse:
                    if "store_machine_daily_details" in endpoint:
                        captured_daily_detail_deletes.append(params)
                    else:
                        captured_summary_deletes.append(params)
                    return FakeJsonResponse([])

                def get(self, endpoint: str, params: dict[str, str], timeout: int = 30) -> FakeJsonResponse:
                    if params.get("offset") != "0":
                        return FakeJsonResponse([])
                    return FakeJsonResponse(
                        [
                            {
                                "machine_name": "ゴーゴージャグラー３",
                                "target_date": "2026-04-24",
                                "slot_number": "101",
                                "difference_value": 100,
                                "games_count": 2000,
                                "payout_rate": 101.0,
                            },
                            {
                                "machine_name": "ゴーゴージャグラー３",
                                "target_date": "2026-04-25",
                                "slot_number": "101",
                                "difference_value": 300,
                                "games_count": 4000,
                                "payout_rate": 104.0,
                            },
                            {
                                "machine_name": "ゴーゴージャグラー３",
                                "target_date": "2026-04-25",
                                "slot_number": "102",
                                "difference_value": 500,
                                "games_count": 6000,
                                "payout_rate": 106.0,
                            },
                        ]
                    )

            service._supabase_config = lambda: (  # type: ignore[method-assign]
                "https://example.supabase.co",
                "service-key",
                "public",
                "stores",
                "machine_daily_results",
            )
            service._machine_summaries_table = lambda: "store_machine_summaries"  # type: ignore[method-assign]
            service._machine_daily_details_table = lambda: "store_machine_daily_details"  # type: ignore[method-assign]
            service._create_supabase_session = lambda schema: FakeSession()  # type: ignore[method-assign]
            service._upsert_store = lambda session, supabase_url, stores_table, payload: "store-1"  # type: ignore[method-assign]

            saved_count = service._save_to_supabase(  # type: ignore[attr-defined]
                {
                    "store": {
                        "store_name": "Aパーク春日店",
                        "store_url": "https://example.com/kasuga/",
                    },
                    "records": [
                        {
                            "target_date": "2026-04-25",
                            "slot_number": "101",
                            "machine_name": "ゴーゴージャグラー３",
                            "difference_value": 300,
                            "games_count": 4000,
                            "payout_rate": 104.0,
                            "data_source": DATA_SOURCE_MINREPO,
                        },
                        {
                            "target_date": "2026-04-25",
                            "slot_number": "102",
                            "machine_name": "ゴーゴージャグラー３",
                            "difference_value": 500,
                            "games_count": 6000,
                            "payout_rate": 106.0,
                            "data_source": DATA_SOURCE_MINREPO,
                        },
                    ],
                }
            )

            self.assertEqual(saved_count, 2)
            self.assertEqual(len(captured_result_posts), 1)
            self.assertEqual(captured_summary_deletes, [{"store_id": "eq.store-1"}])
            self.assertEqual(captured_daily_detail_deletes, [{"store_id": "eq.store-1"}])
            self.assertEqual(len(captured_summary_posts), 1)
            self.assertEqual(len(captured_daily_detail_posts), 1)
            self.assertEqual(captured_summary_posts[0][0]["store_id"], "store-1")
            self.assertEqual(captured_summary_posts[0][0]["machine_name"], "ゴーゴージャグラー３")
            self.assertEqual(captured_summary_posts[0][0]["latest_date"], "2026-04-25")
            self.assertEqual(captured_summary_posts[0][0]["slot_count"], 2)
            self.assertEqual(captured_summary_posts[0][0]["average_difference"], 400.0)
            self.assertEqual(captured_summary_posts[0][0]["average_games"], 5000.0)
            self.assertEqual(captured_summary_posts[0][0]["average_payout"], 105.0)
            self.assertEqual(captured_daily_detail_posts[0][0]["store_id"], "store-1")
            self.assertEqual(captured_daily_detail_posts[0][0]["machine_name"], "ゴーゴージャグラー３")
            self.assertEqual(captured_daily_detail_posts[0][0]["target_date"], "2026-04-25")
            self.assertEqual(captured_daily_detail_posts[0][0]["slot_count"], 2)
            self.assertEqual(
                captured_daily_detail_posts[0][0]["records_by_slot"],
                {
                    "101": {
                        "difference_value": 300,
                        "games_count": 4000,
                        "payout_rate": 104.0,
                        "bb_count": None,
                        "rb_count": None,
                        "combined_ratio_text": None,
                        "bb_ratio_text": None,
                        "rb_ratio_text": None,
                    },
                    "102": {
                        "difference_value": 500,
                        "games_count": 6000,
                        "payout_rate": 106.0,
                        "bb_count": None,
                        "rb_count": None,
                        "combined_ratio_text": None,
                        "bb_ratio_text": None,
                        "rb_ratio_text": None,
                    },
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

    def test_apply_slot_resolution_history_updates_past_snapshot_and_local_file(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
            service._apply_slot_resolution_to_supabase = lambda snapshot, resolution_map: 0  # type: ignore[method-assign]
            store_dir = Path(temp_dir) / "local_data" / "Aパーク春日店"
            store_dir.mkdir(parents=True, exist_ok=True)
            existing_file = store_dir / "existing.json"
            existing_file.write_text(
                json.dumps(
                    {
                        "store": {
                            "store_name": "Aパーク春日店",
                            "store_url": "https://example.com/kasuga/",
                        },
                        "machine_names": ["ネオアイムジャグラーEX"],
                        "records": [
                            {
                                "target_date": "2026-04-24",
                                "slot_number": "101",
                                "machine_name": "ネオアイムジャグラーEX",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            snapshot = {
                "store": {
                    "store_name": "Aパーク春日店",
                    "store_url": "https://example.com/kasuga/",
                },
                "machine_names": ["ネオアイムジャグラーEX", "SアイムジャグラーＥＸ"],
                "records": [
                    {
                        "target_date": "2026-04-24",
                        "slot_number": "101",
                        "machine_name": "ネオアイムジャグラーEX",
                    },
                    {
                        "target_date": "2026-04-25",
                        "slot_number": "101",
                        "machine_name": "SアイムジャグラーＥＸ",
                    },
                ],
            }

            service._apply_slot_resolution_history(snapshot)  # type: ignore[attr-defined]

            self.assertEqual(
                [record["machine_name"] for record in snapshot["records"]],
                ["SアイムジャグラーＥＸ", "SアイムジャグラーＥＸ"],
            )
            saved_payload = json.loads(existing_file.read_text(encoding="utf-8"))
            self.assertEqual(saved_payload["records"][0]["machine_name"], "SアイムジャグラーＥＸ")
            self.assertEqual(saved_payload["machine_names"], ["SアイムジャグラーＥＸ"])

    def test_apply_slot_resolution_to_supabase_moves_past_rows(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
            captured_patches: list[tuple[dict[str, str], dict[str, str]]] = []

            class FakeSession:
                def get(self, endpoint: str, params: dict[str, str], timeout: int = 30) -> FakeJsonResponse:
                    if params.get("offset") == "0":
                        return FakeJsonResponse(
                            [
                                {
                                    "target_date": "2026-04-24",
                                    "slot_number": "101",
                                    "machine_name": "ネオアイムジャグラーEX",
                                },
                                {
                                    "target_date": "2026-04-25",
                                    "slot_number": "101",
                                    "machine_name": "SアイムジャグラーＥＸ",
                                },
                            ]
                        )
                    return FakeJsonResponse([])

                def patch(
                    self,
                    endpoint: str,
                    params: dict[str, str],
                    headers: dict[str, str],
                    json: dict[str, str],
                    timeout: int = 30,
                ) -> FakeJsonResponse:
                    captured_patches.append((params, json))
                    return FakeJsonResponse([])

            service._supabase_config = lambda: (  # type: ignore[method-assign]
                "https://example.supabase.co",
                "service-key",
                "public",
                "stores",
                "machine_daily_results",
            )
            service._create_supabase_session = lambda schema: FakeSession()  # type: ignore[method-assign]
            service._find_store_id = lambda session, supabase_url, stores_table, store_url: "store-1"  # type: ignore[method-assign]

            updated_count = service._apply_slot_resolution_to_supabase(  # type: ignore[attr-defined]
                {
                    "store": {
                        "store_name": "Aパーク春日店",
                        "store_url": "https://example.com/kasuga/",
                    }
                },
                {
                    ("101", machine_slot_resolution_group("SアイムジャグラーＥＸ")): (
                        "SアイムジャグラーＥＸ",
                        "2026-04-25",
                    )
                },
            )

            self.assertEqual(updated_count, 1)
            self.assertEqual(captured_patches[0][0]["target_date"], "eq.2026-04-24")
            self.assertEqual(captured_patches[0][1]["machine_name"], "SアイムジャグラーＥＸ")

    def test_save_and_load_registered_stores(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
            stored_stores: list[dict[str, object]] = []

            def fake_save_registered_stores_to_supabase(stores: list[dict[str, object]]) -> int:
                stored_stores[:] = list(stores)
                return len(stores)

            service._save_registered_stores_to_supabase = fake_save_registered_stores_to_supabase  # type: ignore[method-assign]
            service._load_registered_stores_from_supabase = (  # type: ignore[method-assign]
                lambda: service._normalize_registered_stores(stored_stores)
            )

            summary = service.save_registered_stores(
                [
                    {
                        "store_name": "MJアリーナ箱崎店",
                        "store_url": "https://example.com/a",
                        "site7_enabled": True,
                        "site7_prefecture": "福岡県",
                        "site7_area": "東区",
                        "site7_store_name": "ＭＪアリーナ箱崎店",
                    },
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
                    {
                        "store_name": "MJアリーナ箱崎店",
                        "store_url": "https://example.com/a/",
                        "site7_enabled": True,
                        "site7_prefecture": "福岡県",
                        "site7_area": "東区",
                        "site7_store_name": "ＭＪアリーナ箱崎店",
                    },
                    {
                        "store_name": "ABCホール",
                        "store_url": "https://example.com/b/",
                        "site7_enabled": False,
                        "site7_prefecture": DEFAULT_SITE7_PREFECTURE_NAME,
                        "site7_area": "",
                        "site7_store_name": "ABCホール",
                    },
                ],
            )

    def test_normalize_registered_stores_applies_site7_defaults(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))

            self.assertEqual(
                service._normalize_registered_stores(  # type: ignore[attr-defined]
                    [{"store_name": "Aパーク春日店", "store_url": "https://example.com/kasuga"}]
                ),
                [
                    {
                        "store_name": "Aパーク春日店",
                        "store_url": "https://example.com/kasuga/",
                        "site7_enabled": True,
                        "site7_prefecture": "福岡県",
                        "site7_area": "春日市",
                        "site7_store_name": "Ａパーク春日店",
                    }
                ],
            )

    def test_normalize_store_url_unifies_percent_case(self) -> None:
        self.assertEqual(
            normalize_store_url("https://min-repo.com/tag/mj%e5%a4%a9%e7%a5%9eiii/"),
            "https://min-repo.com/tag/mj%E5%A4%A9%E7%A5%9Eiii/",
        )

    def test_normalize_store_name_key_unifies_halfwidth_and_fullwidth(self) -> None:
        self.assertEqual(
            normalize_store_name_key("Aパーク春日店"),
            normalize_store_name_key("Ａパーク春日店"),
        )

    def test_normalize_store_name_key_unifies_halfwidth_and_fullwidth_gogo(self) -> None:
        self.assertEqual(
            normalize_store_name_key("GOGOアリーナ天神"),
            normalize_store_name_key("ＧＯＧＯアリーナ天神"),
        )

    def test_choose_preferred_store_uses_most_records(self) -> None:
        preferred_store = choose_preferred_store(
            [
                {
                    "store_name": "Ａパーク春日店",
                    "store_url": "https://www.d-deltanet.com/pc/HallSelectLink.do/?hallcode=abc",
                    "record_count": 200,
                },
                {
                    "store_name": "Aパーク春日店",
                    "store_url": "https://min-repo.com/tag/a-park-kasuga/",
                    "record_count": 999,
                },
            ]
        )

        self.assertEqual(
            preferred_store,
            {
                "store_name": "Aパーク春日店",
                "store_url": "https://min-repo.com/tag/a-park-kasuga/",
            },
        )

    def test_save_registered_stores_deduplicates_normalized_url(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
            captured_stores: list[dict[str, object]] = []

            def fake_save_registered_stores_to_supabase(stores: list[dict[str, object]]) -> int:
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
                        "site7_enabled": True,
                        "site7_prefecture": "福岡県",
                        "site7_area": "福岡市中央区",
                        "site7_store_name": "ＧＯＧＯアリーナ天神",
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
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
            service._find_saved_machine_targets_from_supabase = lambda **kwargs: set()  # type: ignore[method-assign]
            store_dir = Path(temp_dir) / "local_data" / "テスト店"
            store_dir.mkdir(parents=True, exist_ok=True)
            (store_dir / "sample.json").write_text(
                json.dumps(
                    {
                        "store": {
                            "store_name": "テスト店",
                            "store_url": "https://example.com/store/",
                        },
                        "records": [
                            {"target_date": "2026-04-07", "machine_name": "ゴーゴージャグラー3"},
                            {"target_date": "2026-04-08", "machine_name": "ゴーゴージャグラー３"},
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            summary = service.find_saved_machine_targets(
                store_name="テスト店",
                store_url="https://example.com/store/",
                start_date="2026-04-07",
                end_date="2026-04-08",
                machine_names=["ゴーゴージャグラー"],
            )

            self.assertFalse(summary.has_errors)
            self.assertEqual(
                summary.saved_targets,
                {
                    ("2026-04-07", normalize_text("ゴーゴージャグラー３")),
                    ("2026-04-08", normalize_text("ゴーゴージャグラー３")),
                },
            )

    def test_find_saved_machine_targets_ignores_slot_resolved_machines(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
            service._find_saved_machine_targets_from_supabase = lambda **kwargs: set()  # type: ignore[method-assign]
            store_dir = Path(temp_dir) / "local_data" / "テスト店"
            store_dir.mkdir(parents=True, exist_ok=True)
            (store_dir / "sample.json").write_text(
                json.dumps(
                    {
                        "store": {
                            "store_name": "テスト店",
                            "store_url": "https://example.com/store/",
                        },
                        "records": [
                            {"target_date": "2026-04-07", "machine_name": "ネオアイムジャグラーEX"},
                            {"target_date": "2026-04-08", "machine_name": "SアイムジャグラーＥＸ"},
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            summary = service.find_saved_machine_targets(
                store_name="テスト店",
                store_url="https://example.com/store/",
                start_date="2026-04-07",
                end_date="2026-04-08",
                machine_names=["ネオアイムジャグラーEX", "SアイムジャグラーＥＸ"],
            )

            self.assertFalse(summary.has_errors)
            self.assertEqual(summary.saved_targets, set())

    def test_find_saved_machine_targets_supabase_uses_supabase_only(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
            service._find_saved_machine_target_sources_from_supabase = (  # type: ignore[method-assign]
                lambda **kwargs: (
                    {("2026-04-25", normalize_text("ゴーゴージャグラー３"))},
                    {("2026-04-24", normalize_text("ゴーゴージャグラー３"))},
                )
            )

            summary = service.find_saved_machine_targets_supabase(
                store_url="https://example.com/store",
                start_date="2026-04-24",
                end_date="2026-04-25",
                machine_names=["ゴーゴージャグラー"],
            )

            self.assertFalse(summary.has_errors)
            self.assertEqual(
                summary.saved_targets,
                {("2026-04-25", normalize_text("ゴーゴージャグラー３"))},
            )
            self.assertEqual(
                summary.replaceable_targets,
                {("2026-04-24", normalize_text("ゴーゴージャグラー３"))},
            )

    def test_filter_site7_history_result_skips_saved_targets(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = find_gui_fixture("site7_machine.html")
        history_result = scraper.parse_machine_history_html(
            html,
            store_url="https://example.com/site7",
            page_url="https://example.com/site7/machine",
            recent_days=2,
        )

        filtered_result = filter_site7_history_result_by_saved_targets(
            history_result,
            saved_targets={
                ("2026-04-24", normalize_text(SITE7_TARGET_MACHINE_NAME)),
                ("2026-04-25", normalize_text(SITE7_TARGET_MACHINE_NAME)),
            },
        )

        self.assertEqual(filtered_result.date_pages, [])
        self.assertEqual(filtered_result.datasets, [])
        self.assertEqual(filtered_result.skipped_dates, ["2026-04-24", "2026-04-25"])
        self.assertEqual(
            filtered_result.skipped_targets,
            [("2026-04-24", SITE7_TARGET_MACHINE_NAME), ("2026-04-25", SITE7_TARGET_MACHINE_NAME)],
        )

    def test_filter_site7_history_result_skips_saved_slots(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = find_gui_fixture("site7_machine.html")
        history_result = scraper.parse_machine_history_html(
            html,
            store_url="https://example.com/site7",
            page_url="https://example.com/site7/machine",
            recent_days=2,
        )

        filtered_result = filter_site7_history_result_by_saved_slots(
            history_result,
            protected_slots={
                ("2026-04-24", "821"),
                ("2026-04-25", "821"),
            },
        )

        self.assertEqual([len(dataset.rows) for dataset in filtered_result.datasets], [1, 1])
        self.assertEqual(
            [dataset.rows[0][0] for dataset in filtered_result.datasets],
            ["822", "822"],
        )

    def test_find_saved_machine_target_sources_from_supabase_prefers_minrepo(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
            service._supabase_config = lambda: ("https://example.supabase.co", "anon", "public", "stores", "results")  # type: ignore[method-assign]
            service._find_store_id = lambda *args, **kwargs: "store-1"  # type: ignore[method-assign]

            class FakeSession:
                def get(self, endpoint: str, params: dict[str, object], timeout: int) -> object:
                    class Response:
                        def raise_for_status(self) -> None:
                            return None

                        def json(self) -> list[dict[str, object]]:
                            return [
                                {
                                    "target_date": "2026-04-24",
                                    "machine_name": "ゴーゴージャグラー３",
                                    "data_source": DATA_SOURCE_SITE7,
                                    "payout_rate": None,
                                },
                                {
                                    "target_date": "2026-04-25",
                                    "machine_name": "ゴーゴージャグラー３",
                                    "data_source": DATA_SOURCE_MINREPO,
                                    "payout_rate": 101.2,
                                },
                            ]

                    return Response()

            service._create_supabase_session = lambda schema: FakeSession()  # type: ignore[method-assign]

            protected_targets, replaceable_targets = service._find_saved_machine_target_sources_from_supabase(  # type: ignore[attr-defined]
                store_url="https://example.com/store",
                start_date="2026-04-24",
                end_date="2026-04-25",
                target_machine_names={normalize_text("ゴーゴージャグラー３")},
            )

            self.assertEqual(protected_targets, {("2026-04-25", normalize_text("ゴーゴージャグラー３"))})
            self.assertEqual(replaceable_targets, {("2026-04-24", normalize_text("ゴーゴージャグラー３"))})

    def test_find_saved_machine_slot_sources_from_supabase_prefers_minrepo(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
            service._supabase_config = lambda: ("https://example.supabase.co", "anon", "public", "stores", "results")  # type: ignore[method-assign]
            service._find_store_id = lambda *args, **kwargs: "store-1"  # type: ignore[method-assign]

            class FakeSession:
                def get(self, endpoint: str, params: dict[str, object], timeout: int) -> object:
                    class Response:
                        def raise_for_status(self) -> None:
                            return None

                        def json(self) -> list[dict[str, object]]:
                            return [
                                {
                                    "target_date": "2026-04-24",
                                    "slot_number": "737",
                                    "data_source": DATA_SOURCE_SITE7,
                                    "payout_rate": 98.4,
                                },
                                {
                                    "target_date": "2026-04-25",
                                    "slot_number": "737",
                                    "data_source": DATA_SOURCE_MINREPO,
                                    "payout_rate": None,
                                },
                            ]

                    return Response()

            service._create_supabase_session = lambda schema: FakeSession()  # type: ignore[method-assign]

            protected_slots, replaceable_slots = service._find_saved_machine_slot_sources_from_supabase(  # type: ignore[attr-defined]
                store_url="https://example.com/store",
                start_date="2026-04-24",
                end_date="2026-04-25",
                target_slot_numbers={"737"},
            )

            self.assertEqual(protected_slots, {("2026-04-25", "737")})
            self.assertEqual(replaceable_slots, {("2026-04-24", "737")})

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
