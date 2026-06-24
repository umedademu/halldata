from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import json
import queue
from types import SimpleNamespace
import threading
import time
import unittest
from unittest import mock
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from bs4 import BeautifulSoup
from PIL import Image, ImageDraw

from data_persistence import (
    DATA_SOURCE_MINREPO,
    DATA_SOURCE_SITE7,
    HistoryPersistenceService,
    PersistenceSummary,
    SavedFullDayDatesSummary,
    SavedMachineSlotsSummary,
    build_machine_daily_records,
    build_store_machine_daily_detail_payloads,
    build_store_machine_summary_payloads,
    build_supabase_result_payload,
    choose_preferred_store,
    normalize_store_name_key,
    normalize_store_url,
)
from daidata_online_scraper import (
    DAIDATA_BEAM_HIKARI_URL,
    DaidataOnlineScraper,
    build_daidata_machine_dataset,
    daidata_store_is_beam_hikari,
)
from main import (
    DEFAULT_MINREPO_FETCH_MODE,
    FETCH_FREQUENCY_DAILY,
    FETCH_FREQUENCY_LOW,
    FETCH_FREQUENCY_STOP,
    FETCH_SOURCE_BOTH,
    FETCH_SOURCE_MINREPO,
    FETCH_SOURCE_SITE7,
    SITE7_BROWSER_MODE_HIDDEN,
    SITE7_BROWSER_MODE_VISIBLE,
    SITE7_NEO_IM_MACHINE_NAME,
    MINREPO_FETCH_MODE_STRONG,
    FetchCancelled,
    MinRepoApp,
    MinRepoFetchParallelOptions,
    OperationResultQueue,
    FetchManyResult,
    RegisteredStore,
    StoreFetchResult,
    build_recent_date_range_input,
    filter_site7_history_result_by_saved_slots,
    filter_site7_history_result_by_saved_targets,
    matches_day_tail,
    minrepo_fallback_date_texts_for_site7,
    minrepo_priority_watch_is_active,
    minrepo_priority_watch_target_date,
    normalize_site7_browser_mode,
    normalize_site7_enabled_machine_names,
    parse_recent_days,
    parse_retry_delay_seconds,
    rewrite_history_result_store,
    scheduled_fetch_due_date,
    scheduled_supplemental_store_limit,
    site7_schedule_due_hour,
    site7_update_satisfies_scheduled_hour,
    strip_site7_history_result_source_differences,
)
from machine_difference import (
    calculate_estimated_coin_hold_difference_value,
    calculate_machine_difference_value,
    canonical_machine_name,
    list_site7_target_machine_names,
    machine_is_site7_target,
)
from estimated_grape import ESTIMATED_GRAPE_VALUE_VERSION, calculate_estimated_grape_value
from minrepo_scraper import (
    FetchProgress,
    MachineDataset,
    MachineHistoryResult,
    MinRepoScraper,
    ScraperError,
    StoreDatePage,
    StoreEventSettings,
    normalize_text,
    parse_date_range_input,
    parse_old_event_days_text,
)
from r2_storage import R2StorageError
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
    Site7NoPlayDayStats,
    clamp_site7_recent_days,
    default_site7_store_settings,
    enrich_site7_target_store,
    dataset_has_site7_graph_difference,
    set_site7_dataset_updated_at,
    set_site7_result_no_play_day_stats,
    site7_dataset_updated_at,
    site7_result_no_play_day_stats,
    mark_site7_dataset_graph_difference,
    parse_site7_graph_difference_value,
    site7_store_is_known_unavailable,
)
from site7_scraper import build_site7_transition_wait_milliseconds
from setting_estimates import SETTING_ESTIMATE_GRAPE_VALUE_VERSION, SETTING_ESTIMATE_VALUE_VERSION
from web_data_export import (
    StoreSource,
    build_machine_data_file,
    build_store_id,
    build_store_payload,
    collect_store_records_from_local_store_dir,
    export_store_payloads,
    safe_record,
)


ROOT_DIR = Path(__file__).resolve().parents[2]
HTML_DIR = ROOT_DIR / "html"
GUI_FIXTURE_DIR = Path(__file__).resolve().parent / "test_fixtures"


class FakeR2JsonStorage:
    def __init__(self) -> None:
        self.objects: dict[str, object] = {}
        self.read_keys: list[str] = []
        self.is_configured = True

    def require_config(self) -> SimpleNamespace:
        return SimpleNamespace(bucket_name="test-bucket")

    def read_json(self, key: str) -> dict[str, object] | None:
        normalized_key = self._normalize_key(key)
        self.read_keys.append(normalized_key)
        if normalized_key not in self.objects:
            return None
        payload = self.objects.get(normalized_key)
        if isinstance(payload, bytes):
            payload = json.loads(payload.decode("utf-8"))
        if not isinstance(payload, dict):
            raise R2StorageError(f"R2上のJson形式が不正です。{key}")
        return json.loads(json.dumps(payload, ensure_ascii=False))

    def write_json(self, key: str, payload: dict[str, object]) -> str:
        normalized_key = self._normalize_key(key)
        self.objects[normalized_key] = json.loads(json.dumps(payload, ensure_ascii=False))
        return normalized_key

    def write_bytes(self, key: str, body: bytes, content_type: str = "application/json") -> str:
        normalized_key = self._normalize_key(key)
        self.objects[normalized_key] = bytes(body)
        return normalized_key

    def delete_object(self, key: str) -> None:
        self.objects.pop(self._normalize_key(key), None)

    @staticmethod
    def _normalize_key(key: str) -> str:
        return str(key).replace("\\", "/").lstrip("/")


class FailingIndexReadStorage(FakeR2JsonStorage):
    def read_json(self, key: str) -> dict[str, object] | None:
        if self._normalize_key(key) == "index.json":
            raise R2StorageError("読込失敗")
        return super().read_json(key)


def make_r2_service(root_dir: Path) -> tuple[HistoryPersistenceService, FakeR2JsonStorage]:
    storage = FakeR2JsonStorage()
    storage.write_json("index.json", {"version": 1, "stores": []})
    return HistoryPersistenceService(root_dir=root_dir, r2_storage=storage), storage


def seed_r2_store(
    storage: FakeR2JsonStorage,
    *,
    store_name: str,
    store_url: str,
    records: list[dict[str, object]],
) -> None:
    store_payload = build_store_payload(
        StoreSource(store_name=store_name, store_url=normalize_store_url(store_url)),
        records,
    )
    export_store_payloads(Path("."), [store_payload], r2_storage=storage)  # type: ignore[arg-type]


def find_html(folder_name: str) -> str:
    folder = HTML_DIR / folder_name
    html_file = next(folder.glob("*.html"))
    return html_file.read_text(encoding="utf-8")


def find_gui_fixture(file_name: str) -> str:
    return (GUI_FIXTURE_DIR / file_name).read_text(encoding="utf-8")


def find_site7_target_store(display_name: str) -> Site7TargetStore:
    for target_store in SITE7_TARGET_STORES:
        if target_store.display_name == display_name:
            return target_store
    raise AssertionError(f"未登録のサイトセブン店舗です: {display_name}")


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


class FakeProgressbar:
    def __init__(self) -> None:
        self.config: dict[str, object] = {}
        self.started = False
        self.stopped = False

    def stop(self) -> None:
        self.stopped = True
        self.started = False

    def start(self, interval: int) -> None:
        self.config["interval"] = interval
        self.started = True

    def configure(self, **kwargs: object) -> None:
        self.config.update(kwargs)


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


class FakeNumberVariable:
    def __init__(self, value: float = 0.0) -> None:
        self.value = value

    def get(self) -> float:
        return self.value

    def set(self, value: float) -> None:
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


class FakeSite7ManualConfirmationPage:
    def __init__(self, release_after_waits: int | None = 1) -> None:
        self.release_after_waits = release_after_waits
        self.wait_calls: list[int] = []
        self.bring_to_front_count = 0
        self.url = "https://m.site777.jp/db/D2300.do"

    def content(self) -> str:
        if self.release_after_waits is None or len(self.wait_calls) < self.release_after_waits:
            return """
<html>
  <body>
    <p>サイトセブンでは利用規約第9条に記載のとおり、プログラムや自動取得ツールでのアクセスを禁止しております。</p>
    <p>識別番号：00000000-235225732</p>
    <div>私はロボットではありません</div>
    <input type="submit" value="利用規約に同意する">
    <script src="https://www.google.com/recaptcha/api.js"></script>
  </body>
</html>
"""
        return "<html><body><a href=\"D2300.do\">大当り一覧</a></body></html>"

    def wait_for_timeout(self, milliseconds: int) -> None:
        self.wait_calls.append(milliseconds)

    def bring_to_front(self) -> None:
        self.bring_to_front_count += 1


class FakeRetainedPage:
    def __init__(self, closed: bool = False) -> None:
        self.closed = closed
        self.bring_to_front_count = 0
        self.wait_selector_calls: list[tuple[str, int]] = []
        self.goto_calls: list[str] = []
        self.url = "https://example.com/machine"

    def bring_to_front(self) -> None:
        self.bring_to_front_count += 1

    def is_closed(self) -> bool:
        return self.closed

    def wait_for_selector(self, selector: str, timeout: int = 0) -> None:
        self.wait_selector_calls.append((selector, timeout))

    def goto(self, url: str, wait_until: str = "", timeout: int = 0) -> None:
        self.goto_calls.append(url)
        self.url = url

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

    def test_parse_old_event_days_text(self) -> None:
        event_settings = parse_old_event_days_text("5のつく日、9のつく日、11日、22日、月と日がゾロ目の日")

        self.assertEqual(event_settings.day_tails, [5, 9])
        self.assertEqual(event_settings.month_days, [11, 22])
        self.assertTrue(event_settings.zoro)
        self.assertEqual(event_settings.weekdays, [])

    def test_parse_old_event_days_text_accepts_weekdays(self) -> None:
        event_settings = parse_old_event_days_text("毎週土曜日、日曜日")

        self.assertEqual(event_settings.day_tails, [])
        self.assertEqual(event_settings.month_days, [])
        self.assertFalse(event_settings.zoro)
        self.assertEqual(event_settings.weekdays, [0, 6])

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

    def test_parallel_thread_sessions_inherit_minrepo_cookies(self) -> None:
        scraper = MinRepoScraper()
        scraper.session.cookies.set("_dparallel", "base", domain=".min-repo.com", path="/")
        worker_cookie_values: list[str | None] = []

        def collect_worker_cookie() -> None:
            session = scraper._get_session()
            worker_cookie_values.append(
                session.cookies.get("_dparallel", domain=".min-repo.com", path="/")
            )

        thread = threading.Thread(target=collect_worker_cookie)
        thread.start()
        thread.join()

        self.assertEqual(worker_cookie_values, ["base"])

    def test_parallel_inline_cookies_sync_to_base_session(self) -> None:
        scraper = MinRepoScraper()
        changed_values: list[bool] = []

        def apply_worker_cookie() -> None:
            changed_values.append(scraper._apply_inline_cookies("$.cookie('_dparallel', 'worker')"))

        thread = threading.Thread(target=apply_worker_cookie)
        thread.start()
        thread.join()

        self.assertEqual(changed_values, [True])
        self.assertEqual(
            scraper.session.cookies.get("_dparallel", domain=".min-repo.com", path="/"),
            "worker",
        )

    def test_scheduled_fetch_due_date_returns_today_only_when_due(self) -> None:
        now = datetime(2026, 4, 28, 1, 5, tzinfo=timezone.utc)

        self.assertEqual(scheduled_fetch_due_date(10, None, now), "2026-04-28")
        self.assertIsNone(scheduled_fetch_due_date(9, None, now))
        self.assertIsNone(scheduled_fetch_due_date(10, "2026-04-28", now))

    def test_scheduled_supplemental_store_limit_divides_stores_by_interval(self) -> None:
        self.assertEqual(scheduled_supplemental_store_limit(0, 14), 0)
        self.assertEqual(scheduled_supplemental_store_limit(1, 14), 1)
        self.assertEqual(scheduled_supplemental_store_limit(350, 14), 25)
        self.assertEqual(scheduled_supplemental_store_limit(351, 14), 26)

    def test_minrepo_priority_watch_target_date_uses_previous_day(self) -> None:
        one_oclock_jst = datetime(2026, 6, 4, 16, 0, tzinfo=timezone.utc)

        self.assertEqual(minrepo_priority_watch_target_date(one_oclock_jst), "2026-06-04")

    def test_minrepo_priority_watch_is_active_from_half_past_midnight_to_ten(self) -> None:
        self.assertFalse(minrepo_priority_watch_is_active(datetime(2026, 6, 4, 15, 29, tzinfo=timezone.utc)))
        self.assertTrue(minrepo_priority_watch_is_active(datetime(2026, 6, 4, 15, 30, tzinfo=timezone.utc)))
        self.assertTrue(minrepo_priority_watch_is_active(datetime(2026, 6, 5, 1, 0, tzinfo=timezone.utc)))
        self.assertFalse(minrepo_priority_watch_is_active(datetime(2026, 6, 5, 1, 1, tzinfo=timezone.utc)))

    def test_minrepo_priority_watch_registered_stores_uses_fetch_order_only(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        later_store = RegisteredStore(
            name="後の店",
            url="https://example.com/later/",
            fetch_source=FETCH_SOURCE_MINREPO,
            fetch_order=2,
        )
        first_store = RegisteredStore(
            name="先の店",
            url="https://example.com/first/",
            fetch_source=FETCH_SOURCE_BOTH,
            fetch_order=1,
        )
        no_order_store = RegisteredStore(
            name="順番なし",
            url="https://example.com/no-order/",
            fetch_source=FETCH_SOURCE_MINREPO,
        )
        site7_store = RegisteredStore(
            name="サイセのみ",
            url="https://example.com/site7/",
            fetch_source=FETCH_SOURCE_SITE7,
            fetch_order=1,
        )
        stopped_store = RegisteredStore(
            name="停止店",
            url="https://example.com/stopped/",
            fetch_source=FETCH_SOURCE_MINREPO,
            fetch_frequency=FETCH_FREQUENCY_STOP,
            fetch_order=1,
        )

        target_stores = app._minrepo_priority_watch_registered_stores(
            [later_store, no_order_store, site7_store, first_store, stopped_store],
            target_date="2026-06-04",
            completed_store_dates=set(),
        )

        self.assertEqual(target_stores, [first_store, later_store])

    def test_run_minrepo_priority_watch_if_due_starts_in_active_window(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.minrepo_schedule_enabled = True
        app.registered_stores = [
            RegisteredStore(
                name="優先店",
                url="https://example.com/priority/",
                fetch_source=FETCH_SOURCE_MINREPO,
                fetch_order=1,
            )
        ]
        app.is_busy = False
        app.minrepo_priority_watch_next_check_at = None
        app.minrepo_priority_watch_pending = False
        app.minrepo_priority_watch_target_date = None
        app.minrepo_priority_watch_completed_store_dates = set()
        app.schedule_status_var = FakeTextVariable()
        app._start_minrepo_priority_watch = mock.Mock()

        app._run_minrepo_priority_watch_if_due(datetime(2026, 6, 4, 16, 0, tzinfo=timezone.utc))

        app._start_minrepo_priority_watch.assert_called_once()
        self.assertEqual(app._start_minrepo_priority_watch.call_args.args[1], "2026-06-04")

    def test_worker_minrepo_priority_watch_fetches_only_updated_stores(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.result_queue = queue.Queue()
        app.fetch_cancel_event = threading.Event()
        updated_store = RegisteredStore(
            name="更新店",
            url="https://example.com/updated/",
            fetch_source=FETCH_SOURCE_MINREPO,
            fetch_order=1,
        )
        waiting_store = RegisteredStore(
            name="未更新店",
            url="https://example.com/waiting/",
            fetch_source=FETCH_SOURCE_MINREPO,
            fetch_order=2,
        )
        app._load_latest_registered_stores = mock.Mock(return_value=[updated_store, waiting_store])
        app._minrepo_store_has_target_date = mock.Mock(
            side_effect=lambda registered_store, **_kwargs: registered_store is updated_store
        )
        fetch_many_result = FetchManyResult(
            results=[
                StoreFetchResult(
                    history_result=MachineHistoryResult(
                        store_name="更新店",
                        store_url=updated_store.url,
                        start_date="2026-06-04",
                        end_date="2026-06-04",
                        date_pages=[StoreDatePage(target_date="2026-06-04", date_url="https://example.com/date")],
                        datasets=[],
                    ),
                    save_summary=None,
                    saved_full_day_summary=SavedFullDayDatesSummary(),
                )
            ],
            failures=[],
        )
        app._run_fetch_many = mock.Mock(return_value=fetch_many_result)

        app._worker_minrepo_priority_watch(
            "2026-06-04",
            0,
            mock.Mock(),
            mock.Mock(),
            set(),
        )

        app._run_fetch_many.assert_called_once()
        call_args = app._run_fetch_many.call_args
        self.assertEqual(call_args.args[0], [updated_store])
        self.assertEqual(call_args.kwargs["required_target_dates"], {"2026-06-04"})
        queued_kinds = []
        queued_payload = None
        while not app.result_queue.empty():
            queued_kind, queued_payload = app.result_queue.get_nowait()
            queued_kinds.append(queued_kind)
        self.assertIn("minrepo_priority_watch_success", queued_kinds)
        self.assertEqual(queued_payload.fetch_many_result, fetch_many_result)

    def test_worker_minrepo_priority_watch_skips_when_no_store_updated(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.result_queue = queue.Queue()
        app.fetch_cancel_event = threading.Event()
        target_store = RegisteredStore(
            name="未更新店",
            url="https://example.com/waiting/",
            fetch_source=FETCH_SOURCE_MINREPO,
            fetch_order=1,
        )
        app._load_latest_registered_stores = mock.Mock(return_value=[target_store])
        app._minrepo_store_has_target_date = mock.Mock(return_value=False)
        app._run_fetch_many = mock.Mock()

        app._worker_minrepo_priority_watch(
            "2026-06-04",
            0,
            mock.Mock(),
            mock.Mock(),
            set(),
        )

        app._run_fetch_many.assert_not_called()
        queued_kinds = []
        queued_payload = None
        while not app.result_queue.empty():
            queued_kind, queued_payload = app.result_queue.get_nowait()
            queued_kinds.append(queued_kind)
        self.assertIn("minrepo_priority_watch_no_update", queued_kinds)
        self.assertEqual(queued_payload.available_store_count, 0)

    def test_mark_minrepo_priority_watch_completed_records_target_date(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.minrepo_priority_watch_completed_store_dates = set()
        store_url = "https://example.com/completed/"
        fetch_many_result = FetchManyResult(
            results=[
                StoreFetchResult(
                    history_result=MachineHistoryResult(
                        store_name="完了店",
                        store_url=store_url,
                        start_date="2026-06-04",
                        end_date="2026-06-04",
                        date_pages=[StoreDatePage(target_date="2026-06-04", date_url="https://example.com/date")],
                        datasets=[],
                    ),
                    save_summary=None,
                    saved_full_day_summary=SavedFullDayDatesSummary(),
                )
            ],
            failures=[],
        )

        app._mark_minrepo_priority_watch_completed(fetch_many_result, "2026-06-04")

        self.assertEqual(
            app.minrepo_priority_watch_completed_store_dates,
            {(normalize_store_url(store_url), "2026-06-04")},
        )

    def test_scheduled_minrepo_registered_stores_adds_old_supplemental_stores_after_daily_targets(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        daily_store = RegisteredStore(name="毎日店", url="https://example.com/daily/", fetch_frequency=FETCH_FREQUENCY_DAILY)
        never_store = RegisteredStore(name="未取得店", url="https://example.com/never/", fetch_frequency=FETCH_FREQUENCY_LOW)
        old_site7_store = RegisteredStore(
            name="古いサイセ店",
            url="https://example.com/old-site7/",
            fetch_frequency=FETCH_FREQUENCY_LOW,
            site7_enabled=True,
        )
        fresh_store = RegisteredStore(name="新しい店", url="https://example.com/fresh/", fetch_frequency=FETCH_FREQUENCY_LOW)
        stores = [fresh_store, never_store, daily_store, old_site7_store]
        selected_store_urls = {normalize_store_url(daily_store.url)}

        target_stores, supplemental_store_urls = app._scheduled_minrepo_registered_stores(
            stores,
            selected_store_urls=selected_store_urls,
            supplemental_store_last_run_dates={
                normalize_store_url(old_site7_store.url): "2026-04-20",
                normalize_store_url(fresh_store.url): "2026-04-27",
            },
            supplemental_interval_days=2,
        )

        self.assertEqual(target_stores, [daily_store, never_store, old_site7_store])
        self.assertEqual(
            supplemental_store_urls,
            {normalize_store_url(never_store.url), normalize_store_url(old_site7_store.url)},
        )

    def test_mark_successful_supplemental_stores_updates_only_completed_stores(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.schedule_all_stores_interval_days = 14
        app.schedule_supplemental_store_last_run_dates = {}
        app.schedule_all_stores_status_var = FakeTextVariable()
        save_calls: list[str] = []
        app._save_schedule_supplemental_store_last_run_dates = lambda: save_calls.append("saved")
        completed_url = "https://example.com/completed/"
        failed_url = "https://example.com/failed/"
        fetch_many_result = FetchManyResult(
            results=[
                StoreFetchResult(
                    history_result=MachineHistoryResult(
                        store_name="完了店",
                        store_url=completed_url,
                        start_date="2026-04-28",
                        end_date="2026-04-28",
                        date_pages=[],
                        datasets=[],
                    ),
                    save_summary=None,
                    saved_full_day_summary=SavedFullDayDatesSummary(),
                )
            ],
            failures=[],
        )

        app._mark_successful_supplemental_stores(
            fetch_many_result,
            {normalize_store_url(completed_url), normalize_store_url(failed_url)},
            "2026-04-28",
        )

        self.assertEqual(
            app.schedule_supplemental_store_last_run_dates,
            {normalize_store_url(completed_url): "2026-04-28"},
        )
        self.assertEqual(save_calls, ["saved"])

    def test_site7_schedule_due_hour_runs_checked_hour_once_per_day(self) -> None:
        noon_now = datetime(2026, 4, 28, 3, 30, tzinfo=timezone.utc)
        evening_before_update_margin = datetime(2026, 4, 28, 9, 19, tzinfo=timezone.utc)
        evening_now = datetime(2026, 4, 28, 9, 20, tzinfo=timezone.utc)
        morning_now = datetime(2026, 4, 28, 0, 0, tzinfo=timezone.utc)

        self.assertEqual(site7_schedule_due_hour((12, 15, 18, 21), {}, noon_now), 12)
        self.assertIsNone(site7_schedule_due_hour((12, 15, 18, 21), {12: "2026-04-28"}, noon_now))
        self.assertEqual(site7_schedule_due_hour((12, 15, 18, 21), {12: "2026-04-27"}, noon_now), 12)
        self.assertIsNone(site7_schedule_due_hour((12, 15, 18, 21), {}, evening_before_update_margin))
        self.assertEqual(site7_schedule_due_hour((12, 15, 18, 21), {}, evening_now), 18)
        self.assertIsNone(site7_schedule_due_hour((12, 15, 18, 21), {}, morning_now))

    def test_site7_update_satisfies_scheduled_hour_uses_source_update_time(self) -> None:
        checked_at = datetime(2026, 6, 5, 14, 5, tzinfo=timezone.utc)

        self.assertFalse(
            site7_update_satisfies_scheduled_hour(
                datetime(2026, 6, 5, 22, 30),
                23,
                checked_at,
            )
        )
        self.assertTrue(
            site7_update_satisfies_scheduled_hour(
                datetime(2026, 6, 5, 23, 8),
                23,
                datetime(2026, 6, 5, 14, 10, tzinfo=timezone.utc),
            )
        )

    def test_scheduled_site7_update_check_starts_fetch_from_first_store_when_later_store_is_updated(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app._raise_if_fetch_cancelled = mock.Mock()
        first_store = RegisteredStore(name="1番店", url="https://example.com/first/", fetch_order=1)
        second_store = RegisteredStore(name="2番店", url="https://example.com/second/", fetch_order=2)
        third_store = RegisteredStore(name="3番店", url="https://example.com/third/", fetch_order=3)
        checked_updates = [
            datetime(2026, 6, 5, 22, 40),
            datetime(2026, 6, 5, 22, 50),
            datetime(2026, 6, 5, 23, 5),
        ]
        app.site7_scraper = SimpleNamespace(
            fetch_mobile_hall_updated_datetime=mock.Mock(side_effect=checked_updates)
        )

        target_stores, waiting_store_urls, updated_at_by_store_url = app._filter_scheduled_site7_stores_by_update_time(
            target_stores=[first_store, second_store, third_store],
            scheduled_hour=23,
            checked_at=datetime(2026, 6, 5, 23, 20),
            browser_visible=False,
        )

        self.assertEqual(target_stores, [first_store, second_store, third_store])
        self.assertEqual(waiting_store_urls, set())
        self.assertEqual(
            updated_at_by_store_url,
            {
                normalize_store_url(first_store.url): checked_updates[0],
                normalize_store_url(second_store.url): checked_updates[1],
                normalize_store_url(third_store.url): checked_updates[2],
            },
        )

    def test_scheduled_site7_update_check_waits_when_no_store_is_updated(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app._raise_if_fetch_cancelled = mock.Mock()
        first_store = RegisteredStore(name="1番店", url="https://example.com/first/", fetch_order=1)
        second_store = RegisteredStore(name="2番店", url="https://example.com/second/", fetch_order=2)
        app.site7_scraper = SimpleNamespace(
            fetch_mobile_hall_updated_datetime=mock.Mock(
                side_effect=[datetime(2026, 6, 5, 22, 40), datetime(2026, 6, 5, 22, 50)]
            )
        )

        target_stores, waiting_store_urls, _updated_at_by_store_url = app._filter_scheduled_site7_stores_by_update_time(
            target_stores=[first_store, second_store],
            scheduled_hour=23,
            checked_at=datetime(2026, 6, 5, 23, 20),
            browser_visible=False,
        )

        self.assertEqual(target_stores, [])
        self.assertEqual(
            waiting_store_urls,
            {normalize_store_url(first_store.url), normalize_store_url(second_store.url)},
        )

    def test_site7_update_satisfies_morning_hour_after_previous_final_update(self) -> None:
        midnight_checked_at = datetime(2026, 6, 5, 15, 5, tzinfo=timezone.utc)
        one_oclock_checked_at = datetime(2026, 6, 5, 16, 5, tzinfo=timezone.utc)

        self.assertTrue(
            site7_update_satisfies_scheduled_hour(
                datetime(2026, 6, 5, 23, 8),
                0,
                midnight_checked_at,
            )
        )
        self.assertTrue(
            site7_update_satisfies_scheduled_hour(
                datetime(2026, 6, 6, 0, 45),
                1,
                one_oclock_checked_at,
            )
        )
        self.assertFalse(
            site7_update_satisfies_scheduled_hour(
                datetime(2026, 6, 5, 22, 50),
                1,
                one_oclock_checked_at,
            )
        )

    def test_minrepo_fallback_date_texts_for_site7_uses_previous_dates_after_ten(self) -> None:
        fallback_dates = minrepo_fallback_date_texts_for_site7(
            FETCH_SOURCE_BOTH,
            3,
            now=datetime(2026, 6, 5, 5, 5, tzinfo=timezone.utc),
            site7_updated_at=datetime(2026, 6, 5, 13, 0),
        )

        self.assertEqual(fallback_dates, ["2026-06-04", "2026-06-03"])

    def test_minrepo_fallback_date_texts_for_site7_uses_previous_site7_business_day(self) -> None:
        fallback_dates = minrepo_fallback_date_texts_for_site7(
            FETCH_SOURCE_BOTH,
            1,
            now=datetime(2026, 6, 6, 1, 5, tzinfo=timezone.utc),
            site7_updated_at=datetime(2026, 6, 5, 23, 8),
        )

        self.assertEqual(fallback_dates, ["2026-06-05"])

    def test_run_scheduled_site7_fetch_if_due_queues_checked_hours_while_busy(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.site7_schedule_enabled = True
        app.site7_schedule_hours = (12, 15, 18, 21)
        app.site7_schedule_last_run_dates_by_hour = {}
        app.site7_schedule_pending_hours = set()
        app.site7_schedule_status_var = FakeTextVariable()
        app._start_scheduled_site7_fetch = mock.Mock()
        app.is_busy = True

        with mock.patch("main.datetime") as mocked_datetime:
            mocked_datetime.now.return_value = datetime(2026, 4, 28, 3, 20, tzinfo=timezone.utc)
            app._run_scheduled_site7_fetch_if_due()
            mocked_datetime.now.return_value = datetime(2026, 4, 28, 6, 20, tzinfo=timezone.utc)
            app._run_scheduled_site7_fetch_if_due()

        self.assertEqual(app.site7_schedule_pending_hours, {12, 15})
        app._start_scheduled_site7_fetch.assert_not_called()

        app.is_busy = False
        with mock.patch("main.datetime") as mocked_datetime:
            current_time = datetime(2026, 4, 28, 7, 20, tzinfo=timezone.utc)
            mocked_datetime.now.return_value = current_time
            app._run_scheduled_site7_fetch_if_due()

        app._start_scheduled_site7_fetch.assert_called_once_with(12, current_time)

    def test_site7_schedule_recheck_request_waits_ten_minutes_and_expires(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.site7_schedule_recheck_requests = {}
        app.site7_schedule_status_var = FakeTextVariable()
        first_checked_at = datetime(2026, 6, 5, 14, 5, tzinfo=timezone.utc)

        app._schedule_site7_update_recheck(
            scheduled_hour=23,
            waiting_store_urls={"https://example.com/store"},
            run_date="2026-06-05",
            waiting_started_at=first_checked_at,
            now=first_checked_at,
        )

        self.assertIsNone(
            app._scheduled_site7_recheck_due_request(
                datetime(2026, 6, 5, 14, 14, tzinfo=timezone.utc)
            )
        )
        due_request = app._scheduled_site7_recheck_due_request(
            datetime(2026, 6, 5, 14, 15, tzinfo=timezone.utc)
        )
        self.assertIsNotNone(due_request)
        self.assertEqual(due_request.scheduled_hour, 23)

        self.assertIsNone(
            app._scheduled_site7_recheck_due_request(
                datetime(2026, 6, 5, 15, 6, tzinfo=timezone.utc)
            )
        )
        self.assertEqual(app.site7_schedule_recheck_requests, {})

    def test_clamp_site7_recent_days(self) -> None:
        self.assertEqual(clamp_site7_recent_days(3), 3)
        self.assertEqual(clamp_site7_recent_days(90), 8)

    def test_default_minrepo_fetch_mode_is_strong_parallel(self) -> None:
        self.assertEqual(DEFAULT_MINREPO_FETCH_MODE, MINREPO_FETCH_MODE_STRONG)

    def test_site7_transition_wait_milliseconds_uses_given_value(self) -> None:
        self.assertEqual(build_site7_transition_wait_milliseconds(lambda start, end: 2.5), 2500)

    def test_site7_transition_wait_milliseconds_clamps_min_and_max(self) -> None:
        self.assertEqual(build_site7_transition_wait_milliseconds(lambda start, end: 1.0), 2000)
        self.assertEqual(build_site7_transition_wait_milliseconds(lambda start, end: 9.0), 4000)

    def test_site7_debug_log_writes_to_local_data(self) -> None:
        with TemporaryDirectory() as temp_dir:
            scraper = Site7Scraper(root_dir=Path(temp_dir))
            scraper._start_debug_log(
                Site7TargetStore(display_name="Aパーク春日店", site7_hall_name="Ａパーク春日店"),
                recent_days=3,
                browser_visible=False,
            )
            scraper._write_debug_log("graph_list_image_parsed", slot="1026", difference=120)

            log_files = list((Path(temp_dir) / "local_data" / "logs" / "site7").glob("*.log"))
            self.assertEqual(len(log_files), 1)
            log_text = log_files[0].read_text(encoding="utf-8")
            self.assertIn("fetch_start", log_text)
            self.assertIn("graph_list_image_parsed", log_text)
            self.assertIn("slot=1026", log_text)
            self.assertIn("difference=120", log_text)

    def test_site7_graph_list_image_entries_keep_lazy_image_url(self) -> None:
        class FakeGraphListPage:
            def __init__(self) -> None:
                self.script = ""

            def evaluate(self, script: str) -> list[dict[str, str]]:
                self.script = script
                return [
                    {
                        "slot_number": "1033",
                        "graph_url": "https://m.site777.jp/db/D3000.do?dn=1033",
                        "image_url": (
                            "https://m.site777.jp/chart/"
                            "RequestSPDedamaTransitionChartForPortal.do?param=lazy&list=1"
                        ),
                        "image_source": "data-src",
                    }
                ]

        scraper = Site7Scraper(root_dir=ROOT_DIR)
        page = FakeGraphListPage()

        entries = scraper._extract_mobile_graph_list_image_entries(page)

        self.assertLess(page.script.index('["data-src"'), page.script.index('["currentSrc"'))
        self.assertEqual(
            entries,
            [
                {
                    "slot_number": "1033",
                    "graph_url": "https://m.site777.jp/db/D3000.do?dn=1033",
                    "image_url": (
                        "https://m.site777.jp/chart/"
                        "RequestSPDedamaTransitionChartForPortal.do?param=lazy&list=1"
                    ),
                    "image_source": "data-src",
                }
            ],
        )

    def test_site7_graph_list_waits_after_reading_images(self) -> None:
        events: list[str] = []

        class FakeGraphListPage(FakeRetainedPage):
            def goto(self, url: str, wait_until: str = "", timeout: int = 0) -> None:
                events.append("goto")
                super().goto(url, wait_until=wait_until, timeout=timeout)

            def content(self) -> str:
                events.append("content")
                return "<a href='D3000.do?dn=1026'>1026</a>"

        scraper = Site7Scraper(root_dir=ROOT_DIR)
        page = FakeGraphListPage()
        scraper._accept_cookie_banner_if_present = mock.Mock()
        scraper.extract_mobile_slot_graph_links = mock.Mock(
            side_effect=lambda html: events.append("extract_links")
            or {"1026": "https://m.site777.jp/db/D3000.do?dn=1026"}
        )
        scraper._fetch_mobile_graph_list_difference_values = mock.Mock(
            side_effect=lambda **kwargs: events.append("read_images") or {"1026": 120}
        )
        scraper._wait_between_transitions = mock.Mock(
            side_effect=lambda page, cancel_requested=None: events.append("wait")
        )

        difference_values, slot_graph_links = scraper._fetch_mobile_graph_list_page_data(
            page=page,
            context=object(),
            start_url="https://m.site777.jp/db/D4300.do?pan=1",
            target_slot_numbers={"1026"},
        )

        self.assertEqual(difference_values, {"1026": 120})
        self.assertEqual(slot_graph_links, {"1026": "https://m.site777.jp/db/D3000.do?dn=1026"})
        self.assertLess(events.index("read_images"), events.index("wait"))

    def test_site7_parse_graph_difference_value_from_image(self) -> None:
        image = Image.new("RGB", (260, 226), (245, 236, 231))
        draw = ImageDraw.Draw(image)
        axis_x = 9
        graph_top = 15
        graph_bottom = 207
        zero_y = 112
        grid_spacing = 19
        for y in range(graph_top + 1, graph_bottom, grid_spacing):
            draw.line((axis_x + 1, y, 244, y), fill=(204, 204, 204))
        draw.line((axis_x, graph_top, axis_x, graph_bottom), fill=(0, 0, 0))
        draw.line((axis_x, zero_y, 244, zero_y), fill=(108, 100, 100))
        line_y = zero_y - grid_spacing
        draw.line((12, zero_y, 80, line_y, 150, line_y), fill=(255, 51, 0), width=2)

        buffer = BytesIO()
        image.save(buffer, format="PNG")

        self.assertAlmostEqual(parse_site7_graph_difference_value(buffer.getvalue()), 1000, delta=100)

    def test_site7_parse_graph_difference_value_from_small_image(self) -> None:
        image = Image.new("RGB", (150, 120), (245, 236, 231))
        draw = ImageDraw.Draw(image)
        axis_x = 7
        graph_top = 10
        graph_bottom = 108
        zero_y = 59
        grid_spacing = 12
        for y in range(graph_top + 1, graph_bottom, grid_spacing):
            draw.line((axis_x + 1, y, 139, y), fill=(204, 204, 204))
        draw.line((axis_x, graph_top, axis_x, graph_bottom), fill=(0, 0, 0))
        draw.line((axis_x, zero_y, 139, zero_y), fill=(108, 100, 100))
        line_y = zero_y + grid_spacing
        draw.line((10, zero_y, 70, line_y, 120, line_y), fill=(40, 150, 80), width=2)

        buffer = BytesIO()
        image.save(buffer, format="PNG")

        self.assertAlmostEqual(parse_site7_graph_difference_value(buffer.getvalue()), -1000, delta=120)

    def test_site7_parse_graph_difference_value_from_date_colored_detail_images(self) -> None:
        line_colors = [
            (0, 174, 239),
            (180, 55, 255),
            (255, 51, 0),
            (40, 150, 80),
            (220, 160, 30),
            (255, 70, 210),
        ]
        for line_color in line_colors:
            with self.subTest(line_color=line_color):
                image = Image.new("RGB", (260, 226), (245, 236, 231))
                draw = ImageDraw.Draw(image)
                axis_x = 9
                graph_top = 15
                graph_bottom = 207
                zero_y = 112
                grid_spacing = 19
                for y in range(graph_top + 1, graph_bottom, grid_spacing):
                    draw.line((axis_x + 1, y, 244, y), fill=(204, 204, 204))
                draw.line((axis_x, graph_top, axis_x, graph_bottom), fill=(0, 0, 0))
                draw.line((axis_x, zero_y, 244, zero_y), fill=(108, 100, 100))
                line_y = zero_y - grid_spacing
                draw.line((12, zero_y, 70, line_y + 4, 150, line_y), fill=line_color, width=2)

                buffer = BytesIO()
                image.save(buffer, format="PNG")

                self.assertAlmostEqual(parse_site7_graph_difference_value(buffer.getvalue()), 1000, delta=120)

    def test_site7_parse_graph_difference_value_from_dark_list_image(self) -> None:
        image = Image.new("RGB", (170, 170), (10, 17, 14))
        draw = ImageDraw.Draw(image)
        zero_y = 84
        grid_spacing = 18
        for y in range(zero_y - grid_spacing * 5, zero_y + grid_spacing * 6, grid_spacing):
            draw.line((14, y, 154, y), fill=(65, 75, 70))
        draw.line((14, zero_y, 154, zero_y), fill=(220, 220, 220))
        line_y = zero_y - grid_spacing
        draw.line((18, zero_y, 70, line_y + 4, 145, line_y), fill=(255, 245, 0), width=2)
        draw.text((126, 144), "785", fill=(255, 245, 0))

        buffer = BytesIO()
        image.save(buffer, format="PNG")

        self.assertAlmostEqual(parse_site7_graph_difference_value(buffer.getvalue()), 1000, delta=140)

    def test_site7_parse_graph_difference_value_uses_dark_list_right_edge_segment(self) -> None:
        image = Image.new("RGB", (170, 170), (10, 17, 14))
        draw = ImageDraw.Draw(image)
        zero_y = 84
        grid_spacing = 18
        for y in range(zero_y - grid_spacing * 5, zero_y + grid_spacing * 6, grid_spacing):
            draw.line((14, y, 154, y), fill=(65, 75, 70))
        draw.line((14, zero_y, 154, zero_y), fill=(220, 220, 220))
        draw.line((18, zero_y - grid_spacing, 100, zero_y - grid_spacing), fill=(255, 245, 0), width=2)
        draw.line((116, zero_y - grid_spacing * 2, 145, zero_y - grid_spacing * 2), fill=(255, 245, 0), width=2)
        draw.text((126, 158), "2969", fill=(255, 245, 0))

        buffer = BytesIO()
        image.save(buffer, format="PNG")

        self.assertAlmostEqual(parse_site7_graph_difference_value(buffer.getvalue()), 2000, delta=140)

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
            app._save_site7_schedule_hours((0, 1, 12, 21))

            self.assertEqual(app._load_saved_schedule_hour(), 5)
            self.assertEqual(app._load_saved_site7_browser_mode(), SITE7_BROWSER_MODE_HIDDEN)
            self.assertEqual(app._load_saved_site7_schedule_hours(), (0, 1, 12, 21))

            app.site7_schedule_last_run_dates_by_hour = {0: "2026-04-28", 1: "2026-04-28", 24: "2026-04-28"}
            app._save_site7_schedule_run_dates()
            app._save_site7_schedule_hours(())

            self.assertEqual(app._load_saved_schedule_hour(), 5)
            self.assertEqual(app._load_saved_site7_browser_mode(), SITE7_BROWSER_MODE_HIDDEN)
            self.assertEqual(app._load_saved_site7_schedule_hours(), ())
            self.assertEqual(app._load_saved_site7_schedule_run_dates(), {0: "2026-04-28", 1: "2026-04-28"})

    def test_window_close_can_choose_exit(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app._quit_application = mock.Mock()
        app._hide_to_resident = mock.Mock()

        app._ask_window_close_action = mock.Mock(return_value="quit")
        app._on_window_close()

        app._quit_application.assert_called_once_with()
        app._hide_to_resident.assert_not_called()

    def test_window_close_can_choose_resident(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app._quit_application = mock.Mock()
        app._hide_to_resident = mock.Mock()

        app._ask_window_close_action = mock.Mock(return_value="resident")
        app._on_window_close()

        app._hide_to_resident.assert_called_once_with()
        app._quit_application.assert_not_called()

    def test_window_close_keeps_window_open_when_dialog_is_closed(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app._quit_application = mock.Mock()
        app._hide_to_resident = mock.Mock()

        app._ask_window_close_action = mock.Mock(return_value=None)
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
        app.minrepo_schedule_enabled = True
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

    def test_run_scheduled_site7_fetch_if_due_waits_for_startup_confirmation(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.site7_schedule_enabled = True
        app.site7_schedule_hours = (12, 15, 18, 21)
        app.site7_schedule_last_run_dates_by_hour = {}
        app.site7_schedule_pending_hours = set()
        app.site7_schedule_startup_prompt_hour = 12
        app.site7_schedule_status_var = FakeTextVariable()
        app._start_scheduled_site7_fetch = mock.Mock()
        app.is_busy = False

        with mock.patch("main.datetime") as mocked_datetime:
            mocked_datetime.now.return_value = datetime(2026, 4, 28, 3, 20, tzinfo=timezone.utc)
            app._run_scheduled_site7_fetch_if_due()

        self.assertEqual(app.site7_schedule_pending_hours, set())
        self.assertEqual(app.site7_schedule_status_var.get(), "本日 12 時のサイトセブン定期実行を確認待ち")
        app._start_scheduled_site7_fetch.assert_not_called()

    def test_prompt_scheduled_fetch_on_startup_can_skip_today(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.minrepo_schedule_enabled = True
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

    def test_prompt_scheduled_site7_fetch_on_startup_can_skip_today(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.site7_schedule_enabled = True
        app.site7_schedule_hours = (12, 15, 18, 21)
        app.site7_schedule_last_run_dates_by_hour = {}
        app.site7_schedule_pending_hours = {12}
        app.site7_schedule_startup_prompt_hour = 12
        app.is_busy = False
        app.site7_schedule_status_var = FakeTextVariable()
        app._save_site7_schedule_run_dates = mock.Mock()
        app._start_scheduled_site7_fetch = mock.Mock()

        with (
            mock.patch("main.site7_schedule_due_hour", return_value=12),
            mock.patch("main.current_jst_date_text", return_value="2026-04-28"),
            mock.patch("main.messagebox.askyesno", return_value=False),
        ):
            app._prompt_scheduled_site7_fetch_on_startup_if_needed()

        self.assertEqual(app.site7_schedule_last_run_dates_by_hour, {12: "2026-04-28"})
        self.assertEqual(app.site7_schedule_pending_hours, set())
        self.assertIsNone(app.site7_schedule_startup_prompt_hour)
        self.assertEqual(app.site7_schedule_status_var.get(), "本日 12 時のサイトセブン定期実行は見送りました")
        app._save_site7_schedule_run_dates.assert_called_once_with()
        app._start_scheduled_site7_fetch.assert_not_called()

    def test_prompt_scheduled_fetch_on_startup_can_start_now(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.minrepo_schedule_enabled = True
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

    def test_prompt_scheduled_site7_fetch_on_startup_can_start_now(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.site7_schedule_enabled = True
        app.site7_schedule_hours = (12, 15, 18, 21)
        app.site7_schedule_last_run_dates_by_hour = {}
        app.site7_schedule_pending_hours = set()
        app.site7_schedule_startup_prompt_hour = 12
        app.is_busy = False
        app.site7_schedule_status_var = FakeTextVariable()
        app._start_scheduled_site7_fetch = mock.Mock()

        with (
            mock.patch("main.site7_schedule_due_hour", return_value=12),
            mock.patch("main.messagebox.askyesno", return_value=True),
        ):
            app._prompt_scheduled_site7_fetch_on_startup_if_needed()

        self.assertIsNone(app.site7_schedule_startup_prompt_hour)
        app._start_scheduled_site7_fetch.assert_called_once_with(12)

    def test_update_button_states_separates_site7_and_minrepo_fetch_buttons(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.current_history_result = None
        app.skip_comparison_display_var = FakeVariable(False)
        app.fetch_cancel_event = threading.Event()
        app.minrepo_cancel_event = threading.Event()
        app.site7_cancel_event = threading.Event()
        app.is_busy = True
        app.active_operation_kind = "site7_fetch"
        app.registered_store_tree = FakeTreeview(("row1",))
        app.web_publish_mode_var = FakeTextVariable("days")

        widget_names = (
            "fetch_button",
            "cancel_fetch_button",
            "target_date_entry",
            "retry_delay_entry",
            "minrepo_fetch_mode_selector",
            "web_publish_days_radio",
            "web_publish_store_radio",
            "web_publish_interval_days_entry",
            "schedule_hour_entry",
            "apply_schedule_button",
            "clear_schedule_button",
            "schedule_all_stores_interval_days_entry",
            "apply_schedule_all_stores_button",
            "comparison_day_tail_selector",
            "comparison_focus_button",
            "skip_comparison_display_button",
            "notify_fetch_complete_button",
            "site7_login_button",
            "site7_fetch_button",
            "site7_neo_im_fetch_button",
            "site7_cancel_button",
            "apply_site7_schedule_button",
            "clear_site7_schedule_button",
            "site7_browser_visible_radio",
            "site7_browser_hidden_radio",
            "register_store_button",
            "register_store_url_entry",
            "register_store_frequency_selector",
            "register_store_source_selector",
            "register_store_order_entry",
            "register_store_site7_button",
            "register_store_site7_difference_checkbutton",
            "register_store_prefecture_entry",
            "register_store_area_entry",
            "register_store_site7_store_name_entry",
            "register_store_site7_hall_id_entry",
            "register_store_site7_address_entry",
            "update_registered_store_button",
            "clear_register_store_form_button",
            "registered_store_filter_entry",
            "clear_registered_store_filter_button",
            "select_all_stores_button",
            "clear_store_selection_button",
            "refresh_registered_stores_button",
            "delete_registered_stores_button",
            "select_all_site7_machines_button",
            "clear_site7_machines_button",
        )
        for widget_name in widget_names:
            setattr(app, widget_name, FakeStateWidget())
        app.site7_machine_checkbuttons = {"machine": FakeStateWidget()}
        app.site7_machine_action_buttons = [
            app.select_all_site7_machines_button,
            app.clear_site7_machines_button,
        ]
        app.site7_schedule_hour_buttons = {
            hour: FakeStateWidget()
            for hour in range(10, 24)
        }

        app._update_button_states()

        self.assertEqual(app.fetch_button.state, "normal")
        self.assertEqual(app.cancel_fetch_button.state, "disabled")
        self.assertEqual(app.site7_fetch_button.state, "disabled")
        self.assertEqual(app.site7_cancel_button.state, "normal")
        self.assertEqual(app.target_date_entry.state, "normal")
        self.assertEqual(app.retry_delay_entry.state, "normal")
        self.assertEqual(app.minrepo_fetch_mode_selector.state, "readonly")
        self.assertEqual(app.web_publish_days_radio.state, "normal")
        self.assertEqual(app.web_publish_store_radio.state, "normal")
        self.assertEqual(app.web_publish_interval_days_entry.state, "normal")
        self.assertEqual(app.schedule_hour_entry.state, "normal")
        self.assertEqual(app.apply_schedule_button.state, "normal")
        self.assertEqual(app.clear_schedule_button.state, "normal")
        self.assertEqual(app.schedule_all_stores_interval_days_entry.state, "normal")
        self.assertEqual(app.apply_schedule_all_stores_button.state, "normal")
        self.assertEqual(app.notify_fetch_complete_button.state, "normal")
        self.assertEqual(app.site7_login_button.state, "disabled")
        self.assertTrue(all(widget.state == "normal" for widget in app.site7_schedule_hour_buttons.values()))
        self.assertEqual(app.apply_site7_schedule_button.state, "normal")
        self.assertEqual(app.clear_site7_schedule_button.state, "normal")
        self.assertEqual(app.site7_browser_visible_radio.state, "normal")
        self.assertEqual(app.site7_browser_hidden_radio.state, "normal")
        self.assertEqual(app.site7_machine_checkbuttons["machine"].state, "normal")
        self.assertEqual(app.select_all_site7_machines_button.state, "normal")
        self.assertEqual(app.clear_site7_machines_button.state, "normal")
        self.assertEqual(app.register_store_button.state, "normal")
        self.assertEqual(app.register_store_url_entry.state, "normal")
        self.assertEqual(app.register_store_frequency_selector.state, "readonly")
        self.assertEqual(app.register_store_source_selector.state, "readonly")
        self.assertEqual(app.register_store_order_entry.state, "normal")
        self.assertEqual(app.register_store_site7_difference_checkbutton.state, "normal")
        self.assertEqual(app.register_store_prefecture_entry.state, "normal")
        self.assertEqual(app.register_store_area_entry.state, "normal")
        self.assertEqual(app.register_store_site7_store_name_entry.state, "normal")
        self.assertEqual(app.register_store_site7_hall_id_entry.state, "normal")
        self.assertEqual(app.register_store_site7_address_entry.state, "normal")
        self.assertEqual(app.update_registered_store_button.state, "normal")
        self.assertEqual(app.clear_register_store_form_button.state, "normal")
        self.assertEqual(app.registered_store_filter_entry.state, "normal")
        self.assertEqual(app.clear_registered_store_filter_button.state, "normal")
        self.assertEqual(app.select_all_stores_button.state, "normal")
        self.assertEqual(app.clear_store_selection_button.state, "normal")
        self.assertEqual(app.refresh_registered_stores_button.state, "normal")
        self.assertEqual(app.delete_registered_stores_button.state, "normal")

    def test_registered_store_table_click_allows_edit_while_fetching(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.is_busy = True
        app._cycle_registered_store_frequency = mock.Mock()

        class ClickableTree:
            def identify_region(self, x: int, y: int) -> str:
                return "cell"

            def identify_column(self, x: int) -> str:
                return "#1"

            def identify_row(self, y: int) -> str:
                return "row1"

        app.registered_store_tree = ClickableTree()

        result = app._on_registered_store_tree_click(SimpleNamespace(x=1, y=1))

        self.assertEqual(result, "break")
        app._cycle_registered_store_frequency.assert_called_once_with("row1")

    def test_refresh_registered_stores_can_start_while_fetching(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.is_busy = True
        app.active_operation_kind = "site7_fetch"
        app.register_store_status_var = FakeTextVariable()
        app._start_worker = mock.Mock()

        app.refresh_registered_stores()

        self.assertEqual(app.register_store_status_var.get(), "登録店舗を更新中...")
        app._start_worker.assert_called_once_with(app._worker_refresh_registered_stores, operation_kind="refresh_stores")

    def test_fetch_start_blocking_allows_minrepo_and_site7_to_run_together(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.is_busy = True
        app.active_operation_kind = "site7_fetch"

        self.assertFalse(app._minrepo_start_blocked())
        self.assertTrue(app._site7_start_blocked())

        app.active_operation_kind = "fetch"

        self.assertTrue(app._minrepo_start_blocked())
        self.assertFalse(app._site7_start_blocked())

    def test_fetch_site7_data_clamps_recent_days_without_prompt(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        target_store = RegisteredStore(
            name="Aパーク春日店",
            url="https://example.com/store",
            fetch_source=FETCH_SOURCE_SITE7,
        )
        app.target_date_var = FakeTextVariable("90")
        app.site7_scraper = SimpleNamespace(has_saved_login_state=mock.Mock(return_value=True))
        app._site7_start_blocked = mock.Mock(return_value=False)
        app._retry_delay_seconds_input = mock.Mock(return_value=0)
        app._minrepo_fetch_parallel_options = mock.Mock(
            return_value=MinRepoFetchParallelOptions(date_workers=1, machine_workers=1)
        )
        app._web_publish_options_input = mock.Mock(return_value=object())
        app._selected_site7_registered_stores = mock.Mock(return_value=[target_store])
        app._site7_has_enabled_target_machines = mock.Mock(return_value=True)
        app._begin_fetch_run = mock.Mock()
        app._site7_browser_visible = mock.Mock(return_value=False)
        app._start_worker = mock.Mock()

        with mock.patch("main.messagebox.askyesno") as askyesno:
            app.fetch_site7_data()

        askyesno.assert_not_called()
        app._start_worker.assert_called_once()
        self.assertEqual(app._start_worker.call_args.args[1], [target_store])
        self.assertEqual(app._start_worker.call_args.args[2], 8)

    def test_fetch_registered_store_site7_data_clamps_recent_days_without_prompt(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        target_store = RegisteredStore(
            name="Aパーク春日店",
            url="https://example.com/store",
            fetch_source=FETCH_SOURCE_SITE7,
        )
        app.target_date_var = FakeTextVariable("90")
        app.site7_scraper = SimpleNamespace(has_saved_login_state=mock.Mock(return_value=True))
        app._site7_start_blocked = mock.Mock(return_value=False)
        app._retry_delay_seconds_input = mock.Mock(return_value=0)
        app._minrepo_fetch_parallel_options = mock.Mock(
            return_value=MinRepoFetchParallelOptions(date_workers=1, machine_workers=1)
        )
        app._web_publish_options_input = mock.Mock(return_value=object())
        app._site7_registered_store_for_single_fetch = mock.Mock(return_value=target_store)
        app._site7_has_enabled_target_machines = mock.Mock(return_value=True)
        app._registered_store_display_name = mock.Mock(return_value=target_store.name)
        app._begin_fetch_run = mock.Mock()
        app._site7_browser_visible = mock.Mock(return_value=False)
        app._start_worker = mock.Mock()

        with mock.patch("main.messagebox.askyesno") as askyesno:
            app.fetch_registered_store_site7_data(target_store)

        askyesno.assert_not_called()
        app._start_worker.assert_called_once()
        self.assertEqual(app._start_worker.call_args.args[1], [target_store])
        self.assertEqual(app._start_worker.call_args.args[2], 8)

    def test_fetch_site7_neo_im_data_does_not_force_difference_fetch(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        target_store = RegisteredStore(
            name="Aパーク春日店",
            url="https://example.com/store",
            fetch_source=FETCH_SOURCE_SITE7,
        )
        app.target_date_var = FakeTextVariable("90")
        app.site7_scraper = SimpleNamespace(has_saved_login_state=mock.Mock(return_value=True))
        app._site7_start_blocked = mock.Mock(return_value=False)
        app._retry_delay_seconds_input = mock.Mock(return_value=0)
        app._minrepo_fetch_parallel_options = mock.Mock(
            return_value=MinRepoFetchParallelOptions(date_workers=1, machine_workers=1)
        )
        app._web_publish_options_input = mock.Mock(return_value=object())
        app._selected_site7_registered_stores = mock.Mock(return_value=[target_store])
        app._begin_fetch_run = mock.Mock()
        app._site7_browser_visible = mock.Mock(return_value=False)
        app._start_worker = mock.Mock()

        app.fetch_site7_neo_im_data()

        app._start_worker.assert_called_once()
        self.assertEqual(app._start_worker.call_args.args[7], {SITE7_NEO_IM_MACHINE_NAME})
        self.assertFalse(app._start_worker.call_args.args[9])

    def test_fetch_registered_store_site7_neo_im_data_does_not_force_difference_fetch(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        target_store = RegisteredStore(
            name="Aパーク春日店",
            url="https://example.com/store",
            fetch_source=FETCH_SOURCE_SITE7,
        )
        app.target_date_var = FakeTextVariable("90")
        app.site7_scraper = SimpleNamespace(has_saved_login_state=mock.Mock(return_value=True))
        app._site7_start_blocked = mock.Mock(return_value=False)
        app._retry_delay_seconds_input = mock.Mock(return_value=0)
        app._minrepo_fetch_parallel_options = mock.Mock(
            return_value=MinRepoFetchParallelOptions(date_workers=1, machine_workers=1)
        )
        app._web_publish_options_input = mock.Mock(return_value=object())
        app._site7_registered_store_for_single_fetch = mock.Mock(return_value=target_store)
        app._registered_store_display_name = mock.Mock(return_value=target_store.name)
        app._begin_fetch_run = mock.Mock()
        app._site7_browser_visible = mock.Mock(return_value=False)
        app._start_worker = mock.Mock()

        app.fetch_registered_store_site7_neo_im_data(target_store)

        app._start_worker.assert_called_once()
        self.assertEqual(app._start_worker.call_args.args[7], {SITE7_NEO_IM_MACHINE_NAME})
        self.assertFalse(app._start_worker.call_args.args[9])

    def test_run_with_persistence_lock_serializes_actions(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.persistence_lock = threading.Lock()
        events: list[str] = []

        def locked_action(label: str) -> str:
            def action() -> str:
                events.append(f"{label}:start")
                time.sleep(0.01)
                events.append(f"{label}:end")
                return label

            return app._run_with_persistence_lock(action)

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(locked_action, ["a", "b"]))

        self.assertEqual(sorted(results), ["a", "b"])
        self.assertIn(
            events,
            [
                ["a:start", "a:end", "b:start", "b:end"],
                ["b:start", "b:end", "a:start", "a:end"],
            ],
        )

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

    def test_validated_register_store_form_input_allows_blank_area_for_auto_fill(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.register_store_url_var = FakeTextVariable("https://example.com/store")
        app.register_store_frequency_var = FakeTextVariable(FETCH_FREQUENCY_DAILY)
        app.register_store_source_var = FakeTextVariable(FETCH_SOURCE_BOTH)
        app.register_store_order_var = FakeTextVariable("")
        app.register_store_site7_enabled_var = FakeVariable(True)
        app.register_store_site7_difference_enabled_var = FakeVariable(False)
        app.register_store_prefecture_var = FakeTextVariable(DEFAULT_SITE7_PREFECTURE_NAME)
        app.register_store_area_var = FakeTextVariable("")
        app.register_store_site7_store_name_var = FakeTextVariable("")
        app.register_store_site7_hall_id_var = FakeTextVariable("")
        app.register_store_site7_address_var = FakeTextVariable("")
        app._is_valid_url = mock.Mock(return_value=True)

        result = app._validated_register_store_form_input()

        self.assertEqual(
            result,
            (
                "https://example.com/store",
                FETCH_FREQUENCY_DAILY,
                FETCH_SOURCE_BOTH,
                None,
                True,
                False,
                DEFAULT_SITE7_PREFECTURE_NAME,
                "",
                "",
                "",
                "",
            ),
        )

    def test_worker_register_store_auto_fills_prefecture_and_area(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.scraper = mock.Mock()
        app.scraper.fetch_store_registration_info.return_value = SimpleNamespace(
            store_name="BIGディッパー門前仲町店",
            prefecture_name="東京都",
            area_name="江東区",
        )
        app.result_queue = queue.Queue()

        app._worker_register_store(
            "https://min-repo.com/tag/big-dipper/",
            FETCH_FREQUENCY_DAILY,
            FETCH_SOURCE_BOTH,
            None,
            True,
            False,
            DEFAULT_SITE7_PREFECTURE_NAME,
            "",
            "",
            "",
            "",
        )

        kind, payload = app.result_queue.get_nowait()
        self.assertEqual(kind, "register_store_success")
        self.assertEqual(
            payload[:12],
            (
                "BIGディッパー門前仲町店",
                "https://min-repo.com/tag/big-dipper/",
                FETCH_FREQUENCY_DAILY,
                FETCH_SOURCE_BOTH,
                None,
                True,
                False,
                "東京都",
                "江東区",
                "",
                "",
                "",
            ),
        )
        self.assertIsInstance(payload[12], StoreEventSettings)

    def test_worker_update_registered_store_uses_auto_fill_but_keeps_manual_region(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.scraper = mock.Mock()
        app.scraper.fetch_store_registration_info.return_value = SimpleNamespace(
            store_name="ワンダーランド三潴店",
            prefecture_name="福岡県",
            area_name="久留米市",
        )
        app.result_queue = queue.Queue()

        app._worker_update_registered_store(
            "https://min-repo.com/tag/old-store/",
            "https://min-repo.com/tag/new-store/",
            FETCH_FREQUENCY_DAILY,
            FETCH_SOURCE_BOTH,
            None,
            True,
            False,
            "佐賀県",
            "佐賀市",
            "",
            "",
            "",
        )

        kind, payload = app.result_queue.get_nowait()
        self.assertEqual(kind, "update_registered_store_success")
        self.assertEqual(
            payload[:13],
            (
                "https://min-repo.com/tag/old-store/",
                "ワンダーランド三潴店",
                "https://min-repo.com/tag/new-store/",
                FETCH_FREQUENCY_DAILY,
                FETCH_SOURCE_BOTH,
                None,
                True,
                False,
                "佐賀県",
                "佐賀市",
                "",
                "",
                "",
            ),
        )
        self.assertIsInstance(payload[13], StoreEventSettings)

    def test_update_registered_store_same_url_uses_worker_for_auto_fill(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        target_store = RegisteredStore(name="123博多店", url="https://min-repo.com/tag/123-hakata/")
        app._selected_registered_store_rows = mock.Mock(return_value=[target_store])
        app._is_valid_url = mock.Mock(return_value=True)
        app._start_worker = mock.Mock()
        app.register_store_status_var = FakeTextVariable("")
        app.register_store_url_var = FakeTextVariable("https://min-repo.com/tag/123-hakata/")
        app.register_store_frequency_var = FakeTextVariable(FETCH_FREQUENCY_DAILY)
        app.register_store_source_var = FakeTextVariable(FETCH_SOURCE_BOTH)
        app.register_store_order_var = FakeTextVariable("")
        app.register_store_site7_enabled_var = FakeVariable(True)
        app.register_store_site7_difference_enabled_var = FakeVariable(False)
        app.register_store_prefecture_var = FakeTextVariable(DEFAULT_SITE7_PREFECTURE_NAME)
        app.register_store_area_var = FakeTextVariable("")
        app.register_store_site7_store_name_var = FakeTextVariable("")
        app.register_store_site7_hall_id_var = FakeTextVariable("")
        app.register_store_site7_address_var = FakeTextVariable("")

        app.update_registered_store()

        app._start_worker.assert_called_once_with(
            app._worker_update_registered_store,
            "https://min-repo.com/tag/123-hakata/",
            "https://min-repo.com/tag/123-hakata/",
            FETCH_FREQUENCY_DAILY,
            FETCH_SOURCE_BOTH,
            None,
            True,
            False,
            DEFAULT_SITE7_PREFECTURE_NAME,
            "",
            "",
            "",
            "",
        )
        self.assertEqual(app.register_store_status_var.get(), "更新先URLの店舗情報を取得中...")

    def test_fetch_store_name_from_saved_html(self) -> None:
        scraper = FixtureScraper()
        result = scraper.fetch_store_name(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
        )

        self.assertEqual(result, "MJアリーナ箱崎店")

    def test_fetch_store_registration_info_from_saved_html(self) -> None:
        scraper = FixtureScraper()
        result = scraper.fetch_store_registration_info(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
        )

        self.assertEqual(result.store_name, "MJアリーナ箱崎店")
        self.assertEqual(result.prefecture_name, "福岡県")
        self.assertEqual(result.area_name, "福岡市東区")

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
        partial_results: list[MachineHistoryResult] = []

        day_result = scraper.fetch_all_machine_history_for_date_page(
            context=context,
            date_page=context.date_pages[0],
            dataset_callback=partial_results.append,
        )

        self.assertEqual(day_result.start_date, "2026-04-07")
        self.assertEqual(day_result.end_date, "2026-04-07")
        self.assertEqual([page.target_date for page in day_result.date_pages], ["2026-04-07"])
        self.assertGreater(len({dataset.machine_name for dataset in day_result.datasets}), 10)
        self.assertEqual(len(partial_results), len(day_result.datasets))
        self.assertTrue(all(len(result.datasets) == 1 for result in partial_results))

    def test_fetch_all_machine_history_for_date_page_parallel_from_saved_html(self) -> None:
        scraper = FixtureScraper()
        context = scraper.prepare_machine_history_context(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-07 ～ 2026-04-08",
        )
        partial_results: list[MachineHistoryResult] = []

        day_result = scraper.fetch_all_machine_history_for_date_page(
            context=context,
            date_page=context.date_pages[0],
            dataset_callback=partial_results.append,
            machine_parallel_workers=4,
        )

        self.assertEqual(day_result.start_date, "2026-04-07")
        self.assertGreater(len(day_result.datasets), 10)
        self.assertEqual(len(partial_results), len(day_result.datasets))
        self.assertEqual(
            sorted(dataset.machine_name for dataset in day_result.datasets),
            sorted(result.datasets[0].machine_name for result in partial_results),
        )

    def test_fetch_all_machine_history_skips_machine_without_data_table(self) -> None:
        store_url = "https://example.com/tag/test-store/"
        date_url = "https://example.com/20260428/"
        ok_machine_url = "https://example.com/machine-ok/"
        empty_machine_url = "https://example.com/machine-empty/"
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
                          </table>
                        </div>
                      </body>
                    </html>
                """,
                date_url: """
                    <html>
                      <body>
                        <div class="tab_content">
                          <h2>機種別データ（2台以上設置機種）</h2>
                          <table>
                            <tr data-count="1">
                              <td><a href="https://example.com/machine-ok/">取れる機種</a></td>
                              <td>100</td><td>2000</td><td>1/1</td><td>101%</td>
                            </tr>
                            <tr data-count="1">
                              <td><a href="https://example.com/machine-empty/">空の機種</a></td>
                              <td>-</td><td>-</td><td>-</td><td>-</td>
                            </tr>
                          </table>
                        </div>
                      </body>
                    </html>
                """,
                ok_machine_url: """
                    <html>
                      <body>
                        <h2>データ一覧</h2>
                        <table>
                          <tr><th>台番</th><th>差枚</th><th>G数</th><th>出率</th></tr>
                          <tr><td>101</td><td>100</td><td>2000</td><td>101%</td></tr>
                        </table>
                      </body>
                    </html>
                """,
                empty_machine_url: """
                    <html>
                      <body>
                        <p>現在表示できる台データはありません。</p>
                      </body>
                    </html>
                """,
            }
        )
        context = scraper.prepare_machine_history_context(store_url=store_url, target_date_input="2026-04-28")
        partial_results: list[MachineHistoryResult] = []

        result = scraper.fetch_all_machine_history_for_date_page(
            context=context,
            date_page=context.date_pages[0],
            dataset_callback=partial_results.append,
            machine_parallel_workers=2,
        )

        self.assertEqual([dataset.machine_name for dataset in result.datasets], ["取れる機種"])
        self.assertEqual(result.skipped_targets, [("2026-04-28", "空の機種")])
        self.assertEqual(len(partial_results), 1)

    def test_fetch_single_store_uses_local_checkpoints_and_store_r2_save(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.scraper = FixtureScraper()
        app.fetch_cancel_event = threading.Event()
        app.result_queue = queue.Queue()

        class FakePersistenceService:
            def __init__(self) -> None:
                self.checkpoint_results: list[MachineHistoryResult] = []
                self.saved_results: list[tuple[MachineHistoryResult, bool]] = []
                self.deleted_checkpoint_paths: list[str] = []

            def find_saved_full_day_dates(
                self,
                store_name: str,
                store_url: str,
                start_date: str,
                end_date: str,
            ) -> SavedFullDayDatesSummary:
                return SavedFullDayDatesSummary()

            def save_history_result_local_checkpoint(
                self,
                history_result: MachineHistoryResult,
            ) -> PersistenceSummary:
                self.checkpoint_results.append(history_result)
                return PersistenceSummary(local_file_path="checkpoint.json", local_record_count=1)

            def save_history_result(
                self,
                history_result: MachineHistoryResult,
                full_day: bool = False,
            ) -> PersistenceSummary:
                self.saved_results.append((history_result, full_day))
                return PersistenceSummary(web_data_saved=True, web_data_record_count=1)

            def delete_local_checkpoint_files(self, file_paths: list[str]) -> PersistenceSummary:
                self.deleted_checkpoint_paths.extend(file_paths)
                return PersistenceSummary()

        persistence_service = FakePersistenceService()
        app.persistence_service = persistence_service

        result = app._fetch_single_store(
            registered_store=RegisteredStore(
                name="MJアリーナ箱崎店",
                url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            ),
            target_date_input="2026-04-07 ～ 2026-04-07",
            store_index=1,
            total_stores=1,
            retry_delay_seconds=0,
        )

        self.assertEqual(len(persistence_service.checkpoint_results), len(result.history_result.datasets))
        self.assertEqual(len(persistence_service.deleted_checkpoint_paths), len(result.history_result.datasets))
        self.assertEqual(len(persistence_service.saved_results), 1)
        self.assertTrue(persistence_service.saved_results[0][1])
        self.assertEqual(
            len(persistence_service.saved_results[0][0].datasets),
            len(result.history_result.datasets),
        )
        progress_updates = [
            payload
            for kind, payload in list(app.result_queue.queue)
            if kind == "fetch_progress" and isinstance(payload, FetchProgress)
        ]
        day_progress = [
            progress
            for progress in progress_updates
            if "全機種一覧を確認中" in progress.message
        ]
        self.assertTrue(day_progress)
        self.assertGreater(day_progress[0].total_steps, 41)
        self.assertLess(day_progress[0].current_step, day_progress[0].total_steps)

    def test_fetch_single_store_strong_parallel_saves_after_store_complete(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.scraper = FixtureScraper()
        app.fetch_cancel_event = threading.Event()
        app.result_queue = queue.Queue()

        class FakePersistenceService:
            def __init__(self) -> None:
                self.lock = threading.Lock()
                self.checkpoint_results: list[MachineHistoryResult] = []
                self.saved_results: list[tuple[MachineHistoryResult, bool]] = []
                self.deleted_checkpoint_paths: list[str] = []

            def find_saved_full_day_dates(
                self,
                store_name: str,
                store_url: str,
                start_date: str,
                end_date: str,
            ) -> SavedFullDayDatesSummary:
                return SavedFullDayDatesSummary()

            def save_history_result_local_checkpoint(
                self,
                history_result: MachineHistoryResult,
            ) -> PersistenceSummary:
                with self.lock:
                    self.checkpoint_results.append(history_result)
                return PersistenceSummary(local_file_path="checkpoint.json", local_record_count=1)

            def save_history_result(
                self,
                history_result: MachineHistoryResult,
                full_day: bool = False,
            ) -> PersistenceSummary:
                with self.lock:
                    self.saved_results.append((history_result, full_day))
                return PersistenceSummary(web_data_saved=True, web_data_record_count=1)

            def delete_local_checkpoint_files(self, file_paths: list[str]) -> PersistenceSummary:
                with self.lock:
                    self.deleted_checkpoint_paths.extend(file_paths)
                return PersistenceSummary()

        persistence_service = FakePersistenceService()
        app.persistence_service = persistence_service

        result = app._fetch_single_store(
            registered_store=RegisteredStore(
                name="MJアリーナ箱崎店",
                url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            ),
            target_date_input="2026-04-07 ～ 2026-04-08",
            store_index=1,
            total_stores=1,
            retry_delay_seconds=0,
            fetch_parallel_options=MinRepoFetchParallelOptions(date_workers=2, machine_workers=4),
        )

        self.assertEqual([page.target_date for page in result.history_result.date_pages], ["2026-04-07", "2026-04-08"])
        self.assertEqual(
            sorted({dataset.target_date for dataset in result.history_result.datasets}),
            ["2026-04-07", "2026-04-08"],
        )
        self.assertEqual(len(persistence_service.saved_results), 1)
        self.assertTrue(all(full_day for _, full_day in persistence_service.saved_results))
        self.assertEqual(
            sorted({dataset.target_date for dataset in persistence_service.saved_results[0][0].datasets}),
            ["2026-04-07", "2026-04-08"],
        )
        self.assertEqual(len(persistence_service.checkpoint_results), len(result.history_result.datasets))
        self.assertEqual(len(persistence_service.deleted_checkpoint_paths), len(result.history_result.datasets))

    def test_fetch_single_store_treats_no_recent_date_pages_as_empty_result(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.fetch_cancel_event = threading.Event()
        app.minrepo_cancel_event = app.fetch_cancel_event
        app.site7_cancel_event = threading.Event()
        app.result_queue = queue.Queue()

        class FakeScraper:
            def prepare_machine_history_context(self, store_url: str, target_date_input: str) -> MachineHistoryResult:
                raise ScraperError("2026-06-18 ～ 2026-06-21 の日付ページが見つかりませんでした。")

        app.scraper = FakeScraper()

        result = app._fetch_single_store(
            registered_store=RegisteredStore(name="古い店舗", url="https://example.com/store"),
            target_date_input="2026-03-25 ～ 2026-06-21",
            store_index=1,
            total_stores=1,
            retry_delay_seconds=0,
            fetch_parallel_options=MinRepoFetchParallelOptions(date_workers=1, machine_workers=1),
        )

        self.assertEqual(result.history_result.store_name, "古い店舗")
        self.assertEqual(result.history_result.start_date, "2026-03-25")
        self.assertEqual(result.history_result.end_date, "2026-06-21")
        self.assertEqual(result.history_result.date_pages, [])
        self.assertEqual(result.history_result.datasets, [])
        self.assertIsNone(result.save_summary)

    def test_fetch_single_store_does_not_mark_partial_day_as_full_day_saved(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.fetch_cancel_event = threading.Event()
        app.minrepo_cancel_event = app.fetch_cancel_event
        app.site7_cancel_event = threading.Event()
        app.result_queue = queue.Queue()
        app.persistence_lock = threading.Lock()
        date_page = StoreDatePage(target_date="2026-04-28", date_url="https://example.com/day")
        dataset = MachineDataset(
            store_name="一部欠け店舗",
            store_url="https://example.com/store",
            target_date="2026-04-28",
            date_url="https://example.com/day",
            machine_name="取れる機種",
            machine_url="https://example.com/machine",
            columns=["台番", "差枚", "G数"],
            rows=[["101", "100", "2000"]],
        )

        class FakeScraper:
            def prepare_machine_history_context(self, store_url: str, target_date_input: str) -> MachineHistoryResult:
                return MachineHistoryResult(
                    store_name="一部欠け店舗",
                    store_url=store_url,
                    start_date="2026-04-28",
                    end_date="2026-04-28",
                    date_pages=[date_page],
                    datasets=[],
                )

            def fetch_all_machine_history_for_date_page(
                self,
                *,
                context: MachineHistoryResult,
                date_page: StoreDatePage,
                step_callback: object,
                date_index: int,
                total_dates: int,
                dataset_callback: object,
                day_total_callback: object,
                machine_parallel_workers: int,
            ) -> MachineHistoryResult:
                day_total_callback(date_page.target_date, 2)
                dataset_result = MachineHistoryResult(
                    store_name=context.store_name,
                    store_url=context.store_url,
                    start_date=date_page.target_date,
                    end_date=date_page.target_date,
                    date_pages=[date_page],
                    datasets=[dataset],
                )
                dataset_callback(dataset_result)
                return MachineHistoryResult(
                    store_name=context.store_name,
                    store_url=context.store_url,
                    start_date=date_page.target_date,
                    end_date=date_page.target_date,
                    date_pages=[date_page],
                    datasets=[dataset],
                    skipped_targets=[(date_page.target_date, "空の機種")],
                )

        class FakePersistenceService:
            def __init__(self) -> None:
                self.saved_full_day_flags: list[bool] = []
                self.marked_results: list[MachineHistoryResult] = []

            def find_saved_full_day_dates(
                self,
                store_name: str,
                store_url: str,
                start_date: str,
                end_date: str,
            ) -> SavedFullDayDatesSummary:
                return SavedFullDayDatesSummary()

            def save_history_result_local_checkpoint(self, history_result: MachineHistoryResult) -> PersistenceSummary:
                return PersistenceSummary(local_file_path="checkpoint.json", local_record_count=1)

            def save_history_result(
                self,
                history_result: MachineHistoryResult,
                full_day: bool = False,
            ) -> PersistenceSummary:
                self.saved_full_day_flags.append(full_day)
                return PersistenceSummary(web_data_saved=True, web_data_record_count=1)

            def mark_full_day_saved(self, history_result: MachineHistoryResult) -> PersistenceSummary:
                self.marked_results.append(history_result)
                return PersistenceSummary()

            def delete_local_checkpoint_files(self, file_paths: list[str]) -> PersistenceSummary:
                return PersistenceSummary()

        persistence_service = FakePersistenceService()
        app.scraper = FakeScraper()
        app.persistence_service = persistence_service

        result = app._fetch_single_store(
            registered_store=RegisteredStore(name="一部欠け店舗", url="https://example.com/store"),
            target_date_input="2026-04-28",
            store_index=1,
            total_stores=1,
            retry_delay_seconds=0,
            fetch_parallel_options=MinRepoFetchParallelOptions(date_workers=1, machine_workers=1),
        )

        self.assertEqual(result.history_result.skipped_targets, [("2026-04-28", "空の機種")])
        self.assertEqual(persistence_service.saved_full_day_flags, [False])
        self.assertEqual(persistence_service.marked_results, [])

    def test_fetch_single_store_discards_checkpoints_when_cancelled_mid_store(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.fetch_cancel_event = threading.Event()
        app.result_queue = queue.Queue()

        date_page = StoreDatePage(target_date="2026-04-07", date_url="https://example.com/day")
        dataset_result = MachineHistoryResult(
            store_name="途中店舗",
            store_url="https://example.com/store",
            start_date="2026-04-07",
            end_date="2026-04-07",
            date_pages=[date_page],
            datasets=[
                MachineDataset(
                    store_name="途中店舗",
                    store_url="https://example.com/store",
                    target_date="2026-04-07",
                    date_url="https://example.com/day",
                    machine_name="ネオアイムジャグラーEX",
                    machine_url="https://example.com/machine",
                    columns=["台番", "差枚", "G数"],
                    rows=[["821", "100", "1000"]],
                )
            ],
        )

        class FakeScraper:
            def prepare_machine_history_context(self, store_url: str, target_date_input: str) -> MachineHistoryResult:
                return MachineHistoryResult(
                    store_name="途中店舗",
                    store_url=store_url,
                    start_date="2026-04-07",
                    end_date="2026-04-07",
                    date_pages=[date_page],
                    datasets=[],
                )

            def fetch_all_machine_history_for_date_page(
                self,
                *,
                context: MachineHistoryResult,
                date_page: StoreDatePage,
                step_callback: object,
                date_index: int,
                total_dates: int,
                dataset_callback: object,
                day_total_callback: object,
                machine_parallel_workers: int,
            ) -> MachineHistoryResult:
                dataset_callback(dataset_result)
                app.fetch_cancel_event.set()
                step_callback("中止確認")
                return dataset_result

        class FakePersistenceService:
            def __init__(self) -> None:
                self.deleted_checkpoint_paths: list[str] = []
                self.saved_results: list[MachineHistoryResult] = []

            def find_saved_full_day_dates(
                self,
                store_name: str,
                store_url: str,
                start_date: str,
                end_date: str,
            ) -> SavedFullDayDatesSummary:
                return SavedFullDayDatesSummary()

            def save_history_result_local_checkpoint(self, history_result: MachineHistoryResult) -> PersistenceSummary:
                return PersistenceSummary(local_file_path="checkpoint-1.json", local_record_count=1)

            def save_history_result(
                self,
                history_result: MachineHistoryResult,
                full_day: bool = False,
            ) -> PersistenceSummary:
                self.saved_results.append(history_result)
                return PersistenceSummary(web_data_saved=True, web_data_record_count=1)

            def delete_local_checkpoint_files(self, file_paths: list[str]) -> PersistenceSummary:
                self.deleted_checkpoint_paths.extend(file_paths)
                return PersistenceSummary()

        persistence_service = FakePersistenceService()
        app.scraper = FakeScraper()
        app.persistence_service = persistence_service

        with self.assertRaises(FetchCancelled):
            app._fetch_single_store(
                registered_store=RegisteredStore(name="途中店舗", url="https://example.com/store"),
                target_date_input="2026-04-07",
                store_index=1,
                total_stores=1,
                retry_delay_seconds=0,
            )

        self.assertEqual(persistence_service.deleted_checkpoint_paths, ["checkpoint-1.json"])
        self.assertEqual(persistence_service.saved_results, [])

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

    def test_scaled_fetch_progress_keeps_multi_store_progress_global(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)

        progress = app._scaled_fetch_progress(
            FetchProgress(current_step=50, total_steps=100, message="2店舗目"),
            store_index=2,
            total_stores=4,
        )

        self.assertEqual(progress.total_steps, 4000)
        self.assertEqual(progress.current_step, 1500)
        self.assertEqual(progress.message, "2店舗目")

    def test_apply_fetch_progress_shows_percent_and_elapsed_time(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.fetch_progress_bar = FakeProgressbar()
        app.fetch_progress_value_var = FakeNumberVariable()
        app.fetch_progress_text_var = FakeTextVariable()
        app.fetch_progress_started_at = time.monotonic() - 65

        app._apply_fetch_progress(FetchProgress(current_step=25, total_steps=100, message="取得中"))

        self.assertEqual(app.fetch_progress_value_var.get(), 25.0)
        self.assertIn("25.0%", app.fetch_progress_text_var.get())
        self.assertIn("経過 01:05", app.fetch_progress_text_var.get())

    def test_apply_fetch_progress_can_update_site7_progress_separately(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.fetch_progress_bar = FakeProgressbar()
        app.fetch_progress_value_var = FakeNumberVariable()
        app.fetch_progress_text_var = FakeTextVariable("みんレポ進捗")
        app.fetch_progress_started_at = time.monotonic() - 65
        app.site7_fetch_progress_bar = FakeProgressbar()
        app.site7_fetch_progress_value_var = FakeNumberVariable()
        app.site7_fetch_progress_text_var = FakeTextVariable()
        app.site7_fetch_progress_started_at = time.monotonic() - 5

        app._apply_fetch_progress(
            FetchProgress(current_step=40, total_steps=100, message="サイセ取得中"),
            progress_kind="site7",
        )

        self.assertEqual(app.site7_fetch_progress_value_var.get(), 40.0)
        self.assertIn("40.0%", app.site7_fetch_progress_text_var.get())
        self.assertIn("サイセ取得中", app.site7_fetch_progress_text_var.get())
        self.assertEqual(app.fetch_progress_value_var.get(), 0.0)
        self.assertEqual(app.fetch_progress_text_var.get(), "みんレポ進捗")

    def test_poll_queue_routes_site7_progress_to_site7_bar(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.fetch_progress_bar = FakeProgressbar()
        app.fetch_progress_value_var = FakeNumberVariable()
        app.fetch_progress_text_var = FakeTextVariable("みんレポ進捗")
        app.fetch_progress_started_at = time.monotonic() - 65
        app.site7_fetch_progress_bar = FakeProgressbar()
        app.site7_fetch_progress_value_var = FakeNumberVariable()
        app.site7_fetch_progress_text_var = FakeTextVariable()
        app.site7_fetch_progress_started_at = time.monotonic() - 5
        app._worker_context = threading.local()
        app._next_operation_id = 3
        app.active_operations = {1: "fetch", 2: "site7_fetch"}
        app.active_operation_kind = "multiple"
        app.is_busy = True
        app.result_polling_active = True
        app.minrepo_cancel_event = threading.Event()
        app.site7_cancel_event = threading.Event()
        app.fetch_cancel_event = app.minrepo_cancel_event
        app.persistence_lock = threading.Lock()
        app.root = SimpleNamespace(after=lambda *args: None)
        app.result_queue = OperationResultQueue(lambda: 2)

        app.result_queue.put(("fetch_progress", FetchProgress(50, 100, "サイセ取得中")))
        app._poll_queue()

        self.assertEqual(app.site7_fetch_progress_value_var.get(), 50.0)
        self.assertIn("サイセ取得中", app.site7_fetch_progress_text_var.get())
        self.assertEqual(app.fetch_progress_value_var.get(), 0.0)
        self.assertEqual(app.fetch_progress_text_var.get(), "みんレポ進捗")

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
                "bonus_difference_value": -300,
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

        self.assertEqual(difference_value, -697)

    def test_calculate_machine_difference_value_for_gogo_juggler_uses_one_bet(self) -> None:
        difference_value = calculate_machine_difference_value(
            "ゴーゴージャグラー３",
            {
                "G数": "8000",
                "BB": "30",
                "RB": "15",
            },
        )

        self.assertEqual(difference_value, -1334)

    def test_calculate_machine_difference_value_for_my_juggler_uses_one_bet(self) -> None:
        difference_value = calculate_machine_difference_value(
            "マイジャグラーV",
            {
                "G数": "8000",
                "BB": "30",
                "RB": "15",
            },
        )

        self.assertEqual(difference_value, -795)

    def test_calculate_estimated_coin_hold_difference_value_for_registered_machine(self) -> None:
        difference_value = calculate_estimated_coin_hold_difference_value(
            "マイジャグラーV",
            {
                "games_count": 5454,
                "bb_count": 25,
                "rb_count": 12,
            },
        )

        self.assertEqual(difference_value, 782)

    def test_calculate_estimated_coin_hold_difference_value_keeps_stronger_bonus_result(self) -> None:
        difference_value = calculate_estimated_coin_hold_difference_value(
            "ネオアイムジャグラーEX",
            {
                "games_count": 25500,
                "bb_count": 150,
                "rb_count": 150,
            },
            setting_average=6,
        )

        self.assertEqual(difference_value, 22867)

    def test_canonical_machine_name_matches_site7_keyword(self) -> None:
        self.assertEqual(canonical_machine_name("SアイムジャグラーＥＸ", site7_only=True), "SアイムジャグラーＥＸ")
        self.assertEqual(canonical_machine_name("ネオアイムジャグラーEX", site7_only=True), "ネオアイムジャグラーEX")
        self.assertEqual(canonical_machine_name("マイジャグラー", site7_only=True), "マイジャグラーV")
        self.assertEqual(canonical_machine_name("ゴーゴージャグラー3", site7_only=True), "ゴーゴージャグラー３")
        self.assertEqual(canonical_machine_name("ファンキージャグラー2", site7_only=True), "ファンキージャグラー２ＫＴ")
        self.assertEqual(canonical_machine_name("ハッピージャグラーV", site7_only=True), "ハッピージャグラーＶＩＩＩ")
        self.assertEqual(canonical_machine_name("ニューキングハナハナV‐30", site7_only=True), "ニューキングハナハナ")
        self.assertEqual(canonical_machine_name("キングハナハナ-30", site7_only=True), "キングハナハナ")
        self.assertEqual(canonical_machine_name("ハナハナホウオウ～天翔～-30", site7_only=True), "ハナハナホウオウ")
        self.assertEqual(canonical_machine_name("スマスロハナビ", site7_only=True), "スマスロ ハナビ")
        self.assertEqual(canonical_machine_name("新ハナビ", site7_only=True), "新ハナビ")
        self.assertEqual(canonical_machine_name("スターハナハナ-30", site7_only=True), "スターハナハナ")
        self.assertEqual(canonical_machine_name("ドラゴンハナハナ～閃光～‐30", site7_only=True), "ドラゴンハナハナ～閃光～")
        site7_a_park_kasuga_targets = {
            "スマスロ 北斗の拳 転生の章2": "スマスロ北斗の拳 転生の章2",
            "Lスマスロ　モンキーターンＶ": "スマスロモンキーターンV",
            "スマスロ 沖ドキ!DUO アンコール": "スマスロ 沖ドキ!DUO アンコール",
            "Lミリオンゴッド－神々の軌跡－": "スマスロ ミリオンゴッド",
            "L 東京喰種": "L東京喰種",
            "Lパチスロ 炎炎ノ消防隊2": "Lパチスロ炎炎ノ消防隊2",
            "スマスロ 新鬼武者3": "スマスロ 新鬼武者3",
            "沖ドキ！ＢＬＡＣＫ": "沖ドキ！ＢＬＡＣＫ",
            "Lスマスロ北斗の拳": "Lスマスロ北斗の拳",
            "沖ドキ！ＧＯＬＤ-30": "沖ドキ！ＧＯＬＤ-30",
            "L機動戦士ガンダムユニコーン 覚醒DRIVE": "L機動戦士ガンダムユニコーン 覚醒DRIVE",
            "LバイオハザードRE：3": "スマスロ バイオハザードRE:3",
            "L真打吉宗": "L真打吉宗",
            "L甲鉄城のカバネリ海門決戦": "スマスロ 甲鉄城のカバネリ 海門決戦",
            "スマスロ 攻殻機動隊": "スマスロ 攻殻機動隊",
            "スマスロ鉄拳6": "スマスロ鉄拳6",
        }
        for source_name, expected_name in site7_a_park_kasuga_targets.items():
            with self.subTest(source_name=source_name):
                self.assertEqual(canonical_machine_name(source_name, site7_only=True), expected_name)
                self.assertTrue(machine_is_site7_target(source_name))
        self.assertTrue(machine_is_site7_target("ドラゴンハナハナ"))

    def test_site7_target_machine_names_are_listed_for_gui_settings(self) -> None:
        machine_names = list_site7_target_machine_names()

        self.assertIn("マイジャグラーV", machine_names)
        self.assertIn("ネオアイムジャグラーEX", machine_names)
        self.assertIn("ニューキングハナハナ", machine_names)
        self.assertIn("スマスロ北斗の拳 転生の章2", machine_names)
        self.assertIn("スマスロモンキーターンV", machine_names)
        self.assertIn("スマスロ 沖ドキ!DUO アンコール", machine_names)
        self.assertIn("L機動戦士ガンダムユニコーン 覚醒DRIVE", machine_names)
        self.assertIn("スマスロ鉄拳6", machine_names)

    def test_normalize_site7_enabled_machine_names_defaults_to_all_and_accepts_aliases(self) -> None:
        available_machine_names = ("マイジャグラーV", "ネオアイムジャグラーEX")

        self.assertEqual(
            normalize_site7_enabled_machine_names(None, available_machine_names),
            {"マイジャグラーV", "ネオアイムジャグラーEX"},
        )
        self.assertEqual(
            normalize_site7_enabled_machine_names(["マイジャグラー", "対象外"], available_machine_names),
            {"マイジャグラーV"},
        )
        self.assertEqual(normalize_site7_enabled_machine_names([], available_machine_names), set())

    def test_site7_enabled_machine_names_for_fetch_only_returns_partial_selection(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.site7_target_machine_names = ("マイジャグラーV", "ネオアイムジャグラーEX")
        app.site7_enabled_machine_names_by_source = {
            FETCH_SOURCE_BOTH: {"マイジャグラーV", "ネオアイムジャグラーEX"},
            FETCH_SOURCE_SITE7: {"マイジャグラーV", "ネオアイムジャグラーEX"},
        }

        self.assertIsNone(app._site7_enabled_machine_names_for_fetch())

        app.site7_enabled_machine_names_by_source[FETCH_SOURCE_BOTH] = {"マイジャグラーV"}
        self.assertEqual(app._site7_enabled_machine_names_for_fetch(), {"マイジャグラーV"})

    def test_site7_machine_select_all_and_clear_updates_saved_selection(self) -> None:
        class FakeBoolVar:
            def __init__(self, value: bool) -> None:
                self.value = value

            def get(self) -> bool:
                return self.value

            def set(self, value: bool) -> None:
                self.value = value

        class FakeStringVar:
            def __init__(self) -> None:
                self.value = ""

            def set(self, value: str) -> None:
                self.value = value

        saved_values: list[set[str]] = []
        app = MinRepoApp.__new__(MinRepoApp)
        app.site7_target_machine_names = ("マイジャグラーV", "ネオアイムジャグラーEX")
        app.site7_enabled_machine_names_by_source = {
            FETCH_SOURCE_BOTH: {"マイジャグラーV"},
            FETCH_SOURCE_SITE7: set(),
        }
        app.site7_machine_enabled_vars_by_source = {
            FETCH_SOURCE_BOTH: {
                "マイジャグラーV": FakeBoolVar(True),
                "ネオアイムジャグラーEX": FakeBoolVar(False),
            },
            FETCH_SOURCE_SITE7: {},
        }
        app.site7_machine_settings_status_var = FakeStringVar()
        app._save_site7_enabled_machine_names = lambda: saved_values.append(
            set(app.site7_enabled_machine_names_by_source[FETCH_SOURCE_BOTH])
        )
        app._update_button_states = lambda: None

        app._select_all_site7_target_machines(FETCH_SOURCE_BOTH)

        self.assertEqual(app.site7_enabled_machine_names_by_source[FETCH_SOURCE_BOTH], {"マイジャグラーV", "ネオアイムジャグラーEX"})
        self.assertEqual(saved_values[-1], {"マイジャグラーV", "ネオアイムジャグラーEX"})
        self.assertEqual(app.site7_machine_settings_status_var.value, "両方 2/2、サイセのみ 0/2 機種を取得対象にしています")

        app._clear_site7_target_machines(FETCH_SOURCE_BOTH)

        self.assertEqual(app.site7_enabled_machine_names_by_source[FETCH_SOURCE_BOTH], set())
        self.assertEqual(saved_values[-1], set())
        self.assertEqual(app.site7_machine_settings_status_var.value, "両方 0/2、サイセのみ 0/2 機種を取得対象にしています")

    def test_site7_extract_store_name_from_saved_html(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = find_gui_fixture("site7_machine.html")

        self.assertEqual(scraper.extract_store_name(html), "Ａパーク春日店")

    def test_site7_target_store_names_include_gogo_arena_tenjin(self) -> None:
        self.assertEqual(
            SITE7_TARGET_STORE_DISPLAY_NAMES,
            (
                "Aパーク春日店",
                "123博多店",
                "スーパーハリウッド1120",
                "BOOM天神本店",
                "GOGOアリーナ天神",
                "MJアリーナ井尻店",
                "HINODE大野城店",
                "スーパーDステーション39筑紫野店",
                "アミューズ浅草店",
            ),
        )
        self.assertEqual(find_site7_target_store("スーパーハリウッド1120").area_name, "春日市")
        self.assertEqual(find_site7_target_store("GOGOアリーナ天神").area_name, "福岡市中央区")
        self.assertEqual(find_site7_target_store("スーパーDステーション39筑紫野店").hall_id, "42006007")
        self.assertEqual(find_site7_target_store("アミューズ浅草店").hall_id, "13777725")
        self.assertEqual(find_site7_target_store("Aパーク春日店").prefecture_link_text, "福岡")
        self.assertEqual(find_site7_target_store("アミューズ浅草店").prefecture_link_text, "東京")
        self.assertEqual(
            default_site7_store_settings("スーパーハリウッド1120"),
            {
                "site7_enabled": True,
                "site7_prefecture": "福岡県",
                "site7_area": "春日市",
                "site7_store_name": "スーパーハリウッド１１２０",
                "site7_hall_id": "",
                "site7_address": "福岡県春日市星見ヶ丘６丁目３２番地",
            },
        )
        self.assertEqual(
            default_site7_store_settings("GOGOアリーナ天神"),
            {
                "site7_enabled": True,
                "site7_prefecture": "福岡県",
                "site7_area": "福岡市中央区",
                "site7_store_name": "ＧＯＧＯアリーナ天神",
                "site7_hall_id": "",
                "site7_address": "福岡県福岡市中央区天神２－６－３７",
            },
        )

    def test_default_site7_store_settings_for_amuse_asakusa(self) -> None:
        self.assertEqual(
            default_site7_store_settings("アミューズ浅草店"),
            {
                "site7_enabled": True,
                "site7_prefecture": "東京都",
                "site7_area": "台東区",
                "site7_store_name": "アミューズ浅草店",
                "site7_hall_id": "13777725",
                "site7_address": "東京都台東区浅草１－４３－１",
            },
        )
        self.assertEqual(default_site7_store_settings("AMUSE浅草")["site7_hall_id"], "13777725")

    def test_default_site7_store_settings_for_recently_checked_stores(self) -> None:
        self.assertEqual(
            default_site7_store_settings("123博多店"),
            {
                "site7_enabled": True,
                "site7_prefecture": "福岡県",
                "site7_area": "福岡市博多区",
                "site7_store_name": "１２３博多店",
                "site7_hall_id": "27038079",
                "site7_address": "福岡県福岡市博多区住吉２丁目６番２４号",
            },
        )
        self.assertEqual(
            default_site7_store_settings("BOOM天神本店"),
            {
                "site7_enabled": True,
                "site7_prefecture": "福岡県",
                "site7_area": "福岡市中央区",
                "site7_store_name": "ブーム天神本店",
                "site7_hall_id": "40001007",
                "site7_address": "福岡県福岡市中央区今泉１丁目１３番１号",
            },
        )
        self.assertEqual(
            default_site7_store_settings("MJアリーナ井尻店"),
            {
                "site7_enabled": True,
                "site7_prefecture": "福岡県",
                "site7_area": "春日市",
                "site7_store_name": "MJアリーナ井尻店",
                "site7_hall_id": "40056001",
                "site7_address": "福岡県春日市桜ケ丘４－１４",
            },
        )
        self.assertEqual(
            default_site7_store_settings("HINODE大野城店"),
            {
                "site7_enabled": True,
                "site7_prefecture": "福岡県",
                "site7_area": "大野城市",
                "site7_store_name": "HINODE大野城店",
                "site7_hall_id": "40101001",
                "site7_address": "福岡県大野城市瓦田４－１２－５",
            },
        )

    def test_default_site7_store_settings_disables_unlisted_beam_hikari(self) -> None:
        self.assertTrue(site7_store_is_known_unavailable("ビームヒカリ"))
        self.assertEqual(
            default_site7_store_settings("ビームヒカリ"),
            {
                "site7_enabled": False,
                "site7_prefecture": "福岡県",
                "site7_area": "大野城市",
                "site7_store_name": "ビームヒカリ",
                "site7_hall_id": "",
                "site7_address": "",
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
                "site7_hall_id": "",
                "site7_address": "福岡県春日市日の出町５－２４",
            },
        )

    def test_default_site7_store_settings_for_super_d_chikushino(self) -> None:
        self.assertEqual(
            default_site7_store_settings("スーパーDステーション39筑紫野店"),
            {
                "site7_enabled": True,
                "site7_prefecture": "福岡県",
                "site7_area": "筑紫野市",
                "site7_store_name": "スーパーＤ’ステーション３９筑紫野店",
                "site7_hall_id": "42006007",
                "site7_address": "福岡県筑紫野市筑紫９６８番２",
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
      <tr>
        <td class="clear">
          <p><span>ニューキングハナハナV‐30(12)</span></p>
          <ul><li><input type="button" name="select" value="出玉データ"></li></ul>
        </td>
      </tr>
      <tr>
        <td class="clear">
          <p><span>キングハナハナ-30(12)</span></p>
          <ul><li><input type="button" name="select" value="出玉データ"></li></ul>
        </td>
      </tr>
      <tr>
        <td class="clear">
          <p><span>スマスロ ハナビ(8)</span></p>
          <ul><li><input type="button" name="select" value="出玉データ"></li></ul>
        </td>
      </tr>
      <tr>
        <td class="clear">
          <p><span>新ハナビ(4)</span></p>
          <ul><li><input type="button" name="select" value="出玉データ"></li></ul>
        </td>
      </tr>
      <tr>
        <td class="clear">
          <p><span>スターハナハナ-30(5)</span></p>
          <ul><li><input type="button" name="select" value="出玉データ"></li></ul>
        </td>
      </tr>
      <tr>
        <td class="clear">
          <p><span>ドラゴンハナハナ～閃光～‐30(6)</span></p>
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
                ("ハナハナホウオウ", "ハナハナホウオウ"),
                ("ニューキングハナハナV‐30", "ニューキングハナハナ"),
                ("キングハナハナ-30", "キングハナハナ"),
                ("スマスロ ハナビ", "スマスロ ハナビ"),
                ("新ハナビ", "新ハナビ"),
                ("スターハナハナ-30", "スターハナハナ"),
                ("ドラゴンハナハナ～閃光～‐30", "ドラゴンハナハナ～閃光～"),
            ],
        )
        self.assertIn("マイジャグラー", SITE7_TARGET_MACHINE_KEYWORDS)
        self.assertIn("ネオアイムジャグラー", SITE7_TARGET_MACHINE_KEYWORDS)
        self.assertIn("SアイムジャグラーＥＸ", SITE7_TARGET_MACHINE_KEYWORDS)
        self.assertIn("ニューキングハナハナ", SITE7_TARGET_MACHINE_KEYWORDS)
        self.assertIn("ハナハナホウオウ", SITE7_TARGET_MACHINE_KEYWORDS)
        self.assertIn("スマスロ ハナビ", SITE7_TARGET_MACHINE_KEYWORDS)
        self.assertIn("ドラゴンハナハナ", SITE7_TARGET_MACHINE_KEYWORDS)

    def test_site7_release_browser_context_closes_browser(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        context = FakeClosableContext()
        playwright = FakePlayableBrowser()

        scraper._release_browser_context(playwright, context)

        self.assertEqual(context.close_count, 1)
        self.assertEqual(playwright.stop_count, 1)

    def test_site7_fetch_opens_and_closes_visible_browser(self) -> None:
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
        scraper._launch_mobile_browser_context = mock.Mock(return_value=(playwright, context))
        scraper._require_playwright = mock.Mock()
        scraper._open_mobile_target_hall_page = mock.Mock(return_value="<html></html>")
        scraper.extract_mobile_store_name = mock.Mock(return_value="Aパーク春日店")
        scraper.extract_mobile_slot_machine_list_link = mock.Mock(return_value="https://example.com/mobile-machines")
        scraper.extract_mobile_target_machine_links = mock.Mock(
            return_value=[
                (
                    Site7MachineEntry(display_name=SITE7_TARGET_MACHINE_NAME, machine_name=SITE7_TARGET_MACHINE_NAME),
                    "https://example.com/mobile-machine",
                )
            ]
        )
        scraper._wait_between_transitions = mock.Mock()
        scraper._accept_cookie_banner_if_present = mock.Mock()
        scraper._fetch_mobile_machine_history_result = mock.Mock(return_value=expected_result)
        scraper._merge_machine_history_results = mock.Mock(return_value=expected_result)
        partial_results: list[MachineHistoryResult] = []

        result = scraper.fetch_target_machine_history(
            recent_days=1,
            browser_visible=True,
            machine_result_callback=partial_results.append,
        )

        self.assertIs(result, expected_result)
        self.assertEqual(partial_results, [expected_result])
        scraper._launch_mobile_browser_context.assert_called_once_with(browser_visible=True)
        self.assertEqual(page.bring_to_front_count, 1)
        self.assertEqual(page.goto_calls, ["https://example.com/mobile-machines"])
        scraper._fetch_mobile_machine_history_result.assert_called_once()
        self.assertEqual(context.close_count, 1)
        self.assertEqual(playwright.stop_count, 1)

    def test_site7_fetch_filters_machine_result_before_callback(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        page = FakeRetainedPage()
        context = FakeRetainedContext(page)
        playwright = FakePlayableBrowser()
        raw_result = MachineHistoryResult(
            store_name="Aパーク春日店",
            store_url="https://example.com/hall",
            start_date="2026-04-25",
            end_date="2026-04-25",
            date_pages=[StoreDatePage(target_date="2026-04-25", date_url="https://example.com/hall#ata0")],
            datasets=[
                MachineDataset(
                    store_name="Aパーク春日店",
                    store_url="https://example.com/hall",
                    target_date="2026-04-25",
                    date_url="https://example.com/hall#ata0",
                    machine_name=SITE7_TARGET_MACHINE_NAME,
                    machine_url="https://example.com/machine",
                    columns=["台番", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率"],
                    rows=[["821", "100", "1000", "-", "5", "2", "1/143", "1/200", "1/500"]],
                )
            ],
        )
        filtered_result = MachineHistoryResult(
            store_name="Aパーク春日店",
            store_url="https://example.com/hall",
            start_date="2026-04-25",
            end_date="2026-04-25",
            date_pages=[],
            datasets=[],
            skipped_targets=[("2026-04-25", SITE7_TARGET_MACHINE_NAME)],
            skipped_dates=["2026-04-25"],
        )
        scraper._launch_mobile_browser_context = mock.Mock(return_value=(playwright, context))
        scraper._require_playwright = mock.Mock()
        scraper._open_mobile_target_hall_page = mock.Mock(return_value="<html></html>")
        scraper.extract_mobile_store_name = mock.Mock(return_value="Aパーク春日店")
        scraper.extract_mobile_slot_machine_list_link = mock.Mock(return_value="https://example.com/mobile-machines")
        scraper.extract_mobile_target_machine_links = mock.Mock(
            return_value=[
                (
                    Site7MachineEntry(display_name=SITE7_TARGET_MACHINE_NAME, machine_name=SITE7_TARGET_MACHINE_NAME),
                    "https://example.com/mobile-machine",
                )
            ]
        )
        scraper._wait_between_transitions = mock.Mock()
        scraper._accept_cookie_banner_if_present = mock.Mock()
        scraper._fetch_mobile_machine_history_result = mock.Mock(return_value=raw_result)
        scraper._merge_machine_history_results = mock.Mock(return_value=filtered_result)
        filter_callback = mock.Mock(return_value=filtered_result)
        partial_results: list[MachineHistoryResult] = []

        result = scraper.fetch_target_machine_history(
            recent_days=1,
            browser_visible=True,
            machine_result_callback=partial_results.append,
            machine_result_filter_callback=filter_callback,
        )

        self.assertIs(result, filtered_result)
        filter_callback.assert_called_once_with(raw_result)
        self.assertEqual(partial_results, [filtered_result])

    def test_site7_fetch_detects_store_closed_after_first_no_play_stale_1am_update(self) -> None:
        scraper = Site7Scraper(
            root_dir=ROOT_DIR,
            current_datetime_fn=lambda: datetime(2026, 6, 8, 11, 15),
        )
        page = FakeRetainedPage()
        context = FakeRetainedContext(page)
        playwright = FakePlayableBrowser()
        site7_target_date = "2026-06-07"
        closed_date = "2026-06-08"
        target_items = [
            (Site7MachineEntry(display_name="マイジャグラーV", machine_name="マイジャグラーV"), "https://example.com/my"),
            (Site7MachineEntry(display_name="SアイムジャグラーＥＸ", machine_name="SアイムジャグラーＥＸ"), "https://example.com/im"),
            (
                Site7MachineEntry(display_name="ファンキージャグラー２ＫＴ", machine_name="ファンキージャグラー２ＫＴ"),
                "https://example.com/funky",
            ),
        ]

        def no_play_result(machine_name: str, slot_count: int) -> MachineHistoryResult:
            result = MachineHistoryResult(
                store_name="Aパーク春日店",
                store_url="https://example.com/hall",
                start_date=site7_target_date,
                end_date=site7_target_date,
                date_pages=[],
                datasets=[],
            )
            set_site7_result_no_play_day_stats(
                result,
                {
                    site7_target_date: Site7NoPlayDayStats(
                        slot_count=slot_count,
                        no_play_slot_count=slot_count,
                        has_play_data=False,
                        updated_at=datetime(2026, 6, 8, 1, 0),
                    )
                },
            )
            return result

        scraper._launch_mobile_browser_context = mock.Mock(return_value=(playwright, context))
        scraper._require_playwright = mock.Mock()
        scraper._open_mobile_target_hall_page = mock.Mock(return_value="<html></html>")
        scraper.extract_mobile_store_name = mock.Mock(return_value="Aパーク春日店")
        scraper.extract_mobile_slot_machine_list_link = mock.Mock(return_value="https://example.com/mobile-machines")
        scraper.extract_mobile_target_machine_links = mock.Mock(return_value=target_items)
        scraper._wait_between_transitions = mock.Mock()
        scraper._accept_cookie_banner_if_present = mock.Mock()
        scraper._fetch_mobile_machine_history_result = mock.Mock(
            side_effect=[
                no_play_result("マイジャグラーV", 4),
                no_play_result("SアイムジャグラーＥＸ", 10),
            ]
        )

        result = scraper.fetch_target_machine_history(recent_days=1)

        self.assertEqual(scraper._fetch_mobile_machine_history_result.call_count, 1)
        self.assertEqual(result.datasets, [])
        self.assertEqual(result.start_date, closed_date)
        self.assertEqual(result.end_date, closed_date)
        self.assertEqual(result.skipped_dates, [closed_date])
        self.assertEqual(
            set(result.skipped_targets),
            {(closed_date, machine_entry.machine_name) for machine_entry, _ in target_items},
        )

    def test_site7_fetch_does_not_store_closed_skip_before_1115(self) -> None:
        scraper = Site7Scraper(
            root_dir=ROOT_DIR,
            current_datetime_fn=lambda: datetime(2026, 6, 8, 11, 14),
        )
        page = FakeRetainedPage()
        context = FakeRetainedContext(page)
        playwright = FakePlayableBrowser()
        target_date = "2026-06-07"

        def no_play_result(slot_count: int) -> MachineHistoryResult:
            result = MachineHistoryResult(
                store_name="Aパーク春日店",
                store_url="https://example.com/hall",
                start_date=target_date,
                end_date=target_date,
                date_pages=[],
                datasets=[],
            )
            set_site7_result_no_play_day_stats(
                result,
                {
                    target_date: Site7NoPlayDayStats(
                        slot_count=slot_count,
                        no_play_slot_count=slot_count,
                        has_play_data=False,
                        updated_at=datetime(2026, 6, 8, 1, 0),
                    )
                },
            )
            return result

        scraper._launch_mobile_browser_context = mock.Mock(return_value=(playwright, context))
        scraper._require_playwright = mock.Mock()
        scraper._open_mobile_target_hall_page = mock.Mock(return_value="<html></html>")
        scraper.extract_mobile_store_name = mock.Mock(return_value="Aパーク春日店")
        scraper.extract_mobile_slot_machine_list_link = mock.Mock(return_value="https://example.com/mobile-machines")
        scraper.extract_mobile_target_machine_links = mock.Mock(
            return_value=[
                (Site7MachineEntry(display_name="マイジャグラーV", machine_name="マイジャグラーV"), "https://example.com/my"),
                (Site7MachineEntry(display_name="SアイムジャグラーＥＸ", machine_name="SアイムジャグラーＥＸ"), "https://example.com/im"),
            ]
        )
        scraper._wait_between_transitions = mock.Mock()
        scraper._accept_cookie_banner_if_present = mock.Mock()
        scraper._fetch_mobile_machine_history_result = mock.Mock(
            side_effect=[
                no_play_result(30),
                no_play_result(10),
            ]
        )

        def drop_no_play_stats(history_result: MachineHistoryResult) -> MachineHistoryResult:
            return MachineHistoryResult(
                store_name=history_result.store_name,
                store_url=history_result.store_url,
                start_date=history_result.start_date,
                end_date=history_result.end_date,
                date_pages=[],
                datasets=[],
            )

        with self.assertRaisesRegex(ScraperError, "有効な台データ"):
            scraper.fetch_target_machine_history(recent_days=1, machine_result_filter_callback=drop_no_play_stats)
        self.assertEqual(scraper._fetch_mobile_machine_history_result.call_count, 2)

    def test_site7_mobile_machine_history_skips_fully_protected_old_day_pages(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)

        def day_html(date_text: str, games_821: str, games_822: str) -> str:
            return f"""
<p id="hall_date">データ更新日時：{date_text} 23:20</p>
<table>
<tr><th>台番</th><th>累計ｹﾞｰﾑ</th><th>BB回数</th><th>RB回数</th></tr>
<tr><td>821</td><td>{games_821}</td><td>5</td><td>2</td></tr>
<tr><td>822</td><td>{games_822}</td><td>6</td><td>3</td></tr>
</table>
"""

        machine_link = "https://m.site777.jp/db/D3310.do?pmc=40100003&mdc=120312&bn=1&pan=1&urt=2173"
        bonus_link = "https://m.site777.jp/db/D3300.do?pmc=40100003&mdc=120312&bn=1&dtdd=0&pan=1&urt=2173"
        machine_html = f'<a href="{bonus_link}">大当り一覧</a>'

        class FakeMobilePage:
            def __init__(self) -> None:
                self.url = machine_link
                self.goto_calls: list[str] = []
                self.html_by_url = {
                    machine_link: machine_html,
                    bonus_link: day_html("2026/06/03", "1000", "2000"),
                    bonus_link.replace("dtdd=0", "dtdd=1"): day_html("2026/06/03", "1100", "2100"),
                    bonus_link.replace("dtdd=0", "dtdd=2"): day_html("2026/06/03", "1200", "2200"),
                }

            def goto(self, url: str, wait_until: str = "", timeout: int = 0) -> None:
                self.goto_calls.append(url)
                self.url = url

            def content(self) -> str:
                return self.html_by_url.get(self.url, "")

        page = FakeMobilePage()
        scraper._wait_between_transitions = mock.Mock()
        scraper._accept_cookie_banner_if_present = mock.Mock()
        callback_calls: list[tuple[Site7MachineEntry, list[str], list[str], str | None]] = []

        def protected_slots_callback(
            machine_entry: Site7MachineEntry,
            target_dates: list[str],
            slot_numbers: list[str],
            site7_updated_at: str | None,
        ) -> set[tuple[str, str]]:
            callback_calls.append((machine_entry, target_dates, slot_numbers, site7_updated_at))
            return {("2026-06-01", "821"), ("2026-06-01", "822")}

        result = scraper._fetch_mobile_machine_history_result(
            page=page,
            store_url="https://example.com/store",
            store_name="Aパーク春日店",
            machine_entry=Site7MachineEntry(display_name="マイジャグラーV", machine_name="マイジャグラーV"),
            machine_link=machine_link,
            recent_days=3,
            machine_protected_slots_callback=protected_slots_callback,
        )

        self.assertEqual(callback_calls[0][1], ["2026-06-03", "2026-06-02", "2026-06-01"])
        self.assertEqual(callback_calls[0][2], ["821", "822"])
        self.assertEqual(callback_calls[0][3], "2026-06-03T23:20:00+09:00")
        self.assertEqual([dataset.target_date for dataset in result.datasets], ["2026-06-02", "2026-06-03"])
        self.assertEqual(result.skipped_dates, ["2026-06-01"])
        self.assertEqual(result.skipped_targets, [("2026-06-01", "マイジャグラーV")])
        self.assertFalse(any("dtdd=2" in url for url in page.goto_calls))

    def test_site7_mobile_machine_history_stops_on_first_day_store_closed(self) -> None:
        scraper = Site7Scraper(
            root_dir=ROOT_DIR,
            current_datetime_fn=lambda: datetime(2026, 6, 9, 12, 0),
        )

        def day_html(date_text: str, games_text: str = "--") -> str:
            return f"""
<p id="hall_date">データ更新日時：{date_text}</p>
<table>
<tr><th>台番</th><th>累計ｹﾞｰﾑ</th><th>BB回数</th><th>RB回数</th></tr>
<tr><td>1026</td><td>{games_text}</td><td>--</td><td>--</td></tr>
<tr><td>1027</td><td>{games_text}</td><td>--</td><td>--</td></tr>
</table>
"""

        machine_link = "https://m.site777.jp/db/D3310.do?pmc=40100003&mdc=120312&bn=1&pan=1&urt=2173"
        bonus_link = "https://m.site777.jp/db/D3300.do?pmc=40100003&mdc=120312&bn=1&dtdd=0&pan=1&urt=2173"
        machine_html = f'<a href="{bonus_link}">大当り一覧</a>'

        class FakeMobilePage:
            def __init__(self) -> None:
                self.url = machine_link
                self.goto_calls: list[str] = []
                self.html_by_url = {
                    machine_link: machine_html,
                    bonus_link: day_html("2026/06/08 01:00"),
                    bonus_link.replace("dtdd=0", "dtdd=1"): day_html("2026/06/07 23:20", "1200"),
                }

            def goto(self, url: str, wait_until: str = "", timeout: int = 0) -> None:
                self.goto_calls.append(url)
                self.url = url

            def content(self) -> str:
                return self.html_by_url.get(self.url, "")

        page = FakeMobilePage()
        scraper._wait_between_transitions = mock.Mock()
        scraper._accept_cookie_banner_if_present = mock.Mock()

        result = scraper._fetch_mobile_machine_history_result(
            page=page,
            store_url="https://example.com/store",
            store_name="Aパーク春日店",
            machine_entry=Site7MachineEntry(display_name="マイジャグラーV", machine_name="マイジャグラーV"),
            machine_link=machine_link,
            recent_days=3,
            stop_on_first_day_store_closed=True,
        )

        self.assertEqual(result.start_date, "2026-06-08")
        self.assertEqual(result.end_date, "2026-06-08")
        self.assertEqual(result.datasets, [])
        self.assertEqual(result.skipped_dates, [])
        self.assertEqual(set(site7_result_no_play_day_stats(result)), {"2026-06-08"})
        self.assertFalse(any("dtdd=1" in url for url in page.goto_calls))

    def test_site7_visible_browser_stays_open_when_fetch_is_cancelled(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        page = FakeRetainedPage()
        context = FakeRetainedContext(page)
        playwright = FakePlayableBrowser()
        cancel_event = threading.Event()
        scraper._launch_mobile_browser_context = mock.Mock(return_value=(playwright, context))
        scraper._require_playwright = mock.Mock()

        def cancel_after_browser_launch(*args: object, **kwargs: object) -> str:
            cancel_event.set()
            raise Site7FetchCancelled("中止しました")

        scraper._open_mobile_target_hall_page = mock.Mock(side_effect=cancel_after_browser_launch)

        with self.assertRaises(Site7FetchCancelled):
            scraper.fetch_target_machine_history(
                recent_days=1,
                browser_visible=True,
                cancel_requested=cancel_event.is_set,
            )

        self.assertEqual(context.close_count, 0)
        self.assertEqual(playwright.stop_count, 0)
        self.assertIs(scraper._visible_context, context)
        self.assertIs(scraper._visible_playwright, playwright)

    def test_site7_hidden_browser_closes_when_fetch_is_cancelled(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        context = FakeRetainedContext(FakeRetainedPage())
        playwright = FakePlayableBrowser()
        cancel_event = threading.Event()
        scraper._launch_mobile_browser_context = mock.Mock(return_value=(playwright, context))
        scraper._require_playwright = mock.Mock()

        def cancel_after_browser_launch(*args: object, **kwargs: object) -> str:
            cancel_event.set()
            raise Site7FetchCancelled("中止しました")

        scraper._open_mobile_target_hall_page = mock.Mock(side_effect=cancel_after_browser_launch)

        with self.assertRaises(Site7FetchCancelled):
            scraper.fetch_target_machine_history(
                recent_days=1,
                browser_visible=False,
                cancel_requested=cancel_event.is_set,
            )

        self.assertEqual(context.close_count, 1)
        self.assertEqual(playwright.stop_count, 1)

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
            scraper.extract_area_link(html, find_site7_target_store("GOGOアリーナ天神")),
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
            scraper.extract_area_link(html, find_site7_target_store("GOGOアリーナ天神")),
            "https://www.d-deltanet.com/pc/HallSearchByArea.do?prefecturecode=40&district=40133",
        )

    def test_site7_extract_mobile_graph_links_from_html(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        target_store = find_site7_target_store("Aパーク春日店")

        self.assertEqual(
            scraper.extract_mobile_prefecture_link(
                '<a href="H0810.do?pan=1&spr=kc&kc=40">福岡</a>',
                target_store,
            ),
            "https://m.site777.jp/db/H0810.do?pan=1&spr=kc&kc=40",
        )
        self.assertEqual(
            scraper.extract_mobile_area_link(
                '<a href="H0800.do?myj=&pan=1&spr=kc&ctc=40218">春日市</a>',
                target_store,
            ),
            "https://m.site777.jp/db/H0800.do?myj=&pan=1&spr=kc&ctc=40218",
        )
        self.assertEqual(
            scraper.extract_mobile_target_hall_link(
                '<a href="D0100.do?pmc=40100003">Ａパーク春日店</a>',
                target_store,
            ),
            "https://m.site777.jp/db/D0100.do?pmc=40100003",
        )

        hall_html = """
<!DOCTYPE html>
<html lang="ja">
  <head><title>Site777｜Ａパーク春日店｜ホールTOP</title></head>
  <body>
    <a href="D0300.do?pmc=40100003&clc=01&urt=-1&pan=1">パチンコ すべて</a>
    <a href="D0300.do?pmc=40100003&clc=03&urt=-1&pan=1">パチスロ すべて</a>
    <a href="D0300.do?pmc=40100003&clc=03&urt=2173&pan=1">【1000円/46枚】スロ</a>
  </body>
</html>
"""
        self.assertEqual(scraper.extract_mobile_store_name(hall_html, target_store), "Ａパーク春日店")
        self.assertEqual(
            scraper.extract_mobile_slot_machine_list_link(hall_html),
            "https://m.site777.jp/db/D0300.do?pmc=40100003&clc=03&urt=2173&pan=1",
        )

        machine_entry, machine_link = scraper.extract_mobile_target_machine_link(
            """
<a href="D2300.do?pmc=40100003&clc=03&urt=2173&mdc=120010&bn=1">マイジャグラーV [30]</a>
<a href="D3310.do?pmc=40100003&clc=03&urt=2173&mdc=120312&bn=1">ネオアイムジャグラーEX [25]</a>
"""
        )
        self.assertEqual(machine_entry.display_name, "ネオアイムジャグラーEX")
        self.assertEqual(machine_entry.machine_name, "ネオアイムジャグラーEX")
        self.assertEqual(
            machine_link,
            "https://m.site777.jp/db/D3310.do?pmc=40100003&clc=03&urt=2173&mdc=120312&bn=1",
        )
        self.assertEqual(
            scraper._filter_mobile_target_machine_links(
                scraper.extract_mobile_target_machine_links(
                    """
<a href="D2300.do?pmc=40100003&clc=03&urt=2173&mdc=120010&bn=1">マイジャグラーV [30]</a>
<a href="D3310.do?pmc=40100003&clc=03&urt=2173&mdc=120312&bn=1">ネオアイムジャグラーEX [25]</a>
"""
                ),
                {"マイジャグラーV"},
            )[0][0].machine_name,
            "マイジャグラーV",
        )

        self.assertEqual(
            scraper.extract_mobile_machine_bonus_list_link(
                '<a href="D3300.do?pmc=40100003&mdc=120312&bn=1&dtdd=0&pan=1&urt=2173">大当り一覧</a>',
                "https://m.site777.jp/db/D3310.do?pmc=40100003&mdc=120312&bn=1&dtdd=0&pan=1&urt=2173",
            ),
            "https://m.site777.jp/db/D3300.do?pmc=40100003&mdc=120312&bn=1&dtdd=0&pan=1&urt=2173",
        )

        self.assertEqual(
            scraper.extract_mobile_machine_graph_list_link(
                '<a href="D2400.do?pmc=40100003&mdc=120312&bn=1&gc=1&pan=1&clc=03&urt=2173">出玉推移一覧</a>'
            ),
            "https://m.site777.jp/db/D2400.do?pmc=40100003&mdc=120312&bn=1&gc=1&pan=1&clc=03&urt=2173",
        )
        self.assertEqual(
            scraper.extract_mobile_machine_graph_list_link(
                '<a href="D2500.do?pmc=40100003&mdc=120312&bn=1&pan=1&clc=03&urt=2173">出玉推移一覧</a>'
            ),
            "https://m.site777.jp/db/D2500.do?pmc=40100003&mdc=120312&bn=1&pan=1&clc=03&urt=2173",
        )
        self.assertEqual(
            scraper.extract_mobile_machine_graph_list_link(
                '<a href="D4300.do?pmc=40100003&mdc=120312&bn=1&pan=1&clc=03&urt=2173">出玉推移一覧</a>'
            ),
            "https://m.site777.jp/db/D4300.do?pmc=40100003&mdc=120312&bn=1&pan=1&clc=03&urt=2173",
        )
        self.assertEqual(
            scraper.extract_mobile_machine_graph_list_link(
                '<a href="D2400.do?pmc=40100003&mdc=120312&bn=1&gc=2&pan=1&clc=03&urt=2173">出玉推移グラフ</a>'
            ),
            "https://m.site777.jp/db/D2400.do?pmc=40100003&mdc=120312&bn=1&gc=1&pan=1&clc=03&urt=2173",
        )

        self.assertEqual(
            scraper.extract_mobile_machine_graph_index_link(
                '<a href="D2400.do?pmc=40100003&mdc=120312&bn=1&gc=2&pan=1&clc=03&urt=2173">出玉推移グラフ</a>'
            ),
            "https://m.site777.jp/db/D2400.do?pmc=40100003&mdc=120312&bn=1&gc=2&pan=1&clc=03&urt=2173",
        )

        self.assertEqual(
            scraper.extract_mobile_slot_graph_link(
                '<a href="D3000.do?pmc=40100003&gc=2&dtdd=0&urt=2173&mdc=120312&dsgk=0&dn=827">827</a>'
            ),
            ("827", "https://m.site777.jp/db/D3000.do?pmc=40100003&gc=2&dtdd=0&urt=2173&mdc=120312&dsgk=0&dn=827"),
        )
        self.assertEqual(
            scraper.extract_mobile_slot_graph_links(
                '<a href="D3000.do?pmc=40100003&gc=2&dtdd=0&urt=2173&mdc=120345&dsgk=0&dn=1151">'
                '<img alt="最大値:634"></a>'
            ),
            {
                "1151": (
                    "https://m.site777.jp/db/D3000.do?"
                    "pmc=40100003&gc=2&dtdd=0&urt=2173&mdc=120345&dsgk=0&dn=1151"
                )
            },
        )
        self.assertEqual(
            scraper.extract_mobile_graph_list_next_page_links(
                """
<a href="D2400.do?pmc=40100003&mdc=120312&bn=1&gc=1&dtdd=0&pan=1">1</a>
<a href="D2400.do?pmc=40100003&mdc=120312&bn=1&gc=1&dtdd=0&pan=2">2</a>
<a href="D2400.do?pmc=40100003&mdc=120312&bn=1&gc=1&dtdd=1&pan=2">別日</a>
""",
                "https://m.site777.jp/db/D2400.do?pmc=40100003&mdc=120312&bn=1&gc=1&dtdd=0&pan=1",
            ),
            ["https://m.site777.jp/db/D2400.do?pmc=40100003&mdc=120312&bn=1&gc=1&dtdd=0&pan=2"],
        )
        self.assertEqual(
            scraper.extract_mobile_graph_list_next_page_links(
                '<a href="D2400.do?pmc=40100003&mdc=120312&bn=1&dtdd=0&pan=3">次へ</a>',
                "https://m.site777.jp/db/D2400.do?pmc=40100003&mdc=120312&bn=1&gc=1&dtdd=0&pan=2",
            ),
            ["https://m.site777.jp/db/D2400.do?pmc=40100003&mdc=120312&bn=1&dtdd=0&pan=3"],
        )
        self.assertEqual(
            scraper._mobile_next_graph_list_page_url(
                "https://m.site777.jp/db/D2400.do?pmc=40100003&mdc=120312&bn=1&gc=1&dtdd=0&pan=2"
            ),
            "https://m.site777.jp/db/D2400.do?pmc=40100003&mdc=120312&bn=1&gc=1&dtdd=0&pan=3",
        )

    def test_site7_graph_phase_uses_bonus_list_page_graph_link(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        machine_link = "https://m.site777.jp/db/D3310.do?pmc=40100003&clc=03&urt=2173&mdc=120312&bn=1"
        bonus_url = (
            "https://m.site777.jp/db/D3300.do?"
            "pmc=40100003&clc=03&urt=2173&mdc=120312&bn=1&dtdd=0&pan=1"
        )
        graph_list_url = (
            "https://m.site777.jp/db/D4300.do?"
            "pmc=40100003&clc=03&urt=2173&mdc=120312&bn=1&dtdd=0&pan=1"
        )

        class FakeGraphSourcePage(FakeRetainedPage):
            def __init__(self) -> None:
                super().__init__()
                self.html_by_url = {
                    bonus_url: (
                        '<a href="D4300.do?pmc=40100003&clc=03&urt=2173&mdc=120312&bn=1&dtdd=0&pan=1">'
                        "出玉推移一覧</a>"
                    )
                }
                self.current_html = ""

            def goto(self, url: str, wait_until: str = "", timeout: int = 0) -> None:
                super().goto(url, wait_until=wait_until, timeout=timeout)
                self.current_html = self.html_by_url.get(url, "")

            def content(self) -> str:
                return self.current_html

        page = FakeGraphSourcePage()
        dataset = MachineDataset(
            store_name="Ａパーク春日店",
            store_url="https://example.com/site7-hall",
            target_date="2026-06-03",
            date_url=bonus_url,
            machine_name=SITE7_TARGET_MACHINE_NAME,
            machine_url=machine_link,
            columns=["台番", "差枚", "G数", "BB", "RB"],
            rows=[["821", "-", "122", "0", "2"]],
        )
        machine_result = MachineHistoryResult(
            store_name="Ａパーク春日店",
            store_url="https://example.com/site7-hall",
            start_date="2026-06-03",
            end_date="2026-06-03",
            date_pages=[StoreDatePage(target_date="2026-06-03", date_url=bonus_url)],
            datasets=[dataset],
        )
        scraper._accept_cookie_banner_if_present = mock.Mock()
        scraper._wait_between_transitions = mock.Mock()
        scraper._fetch_mobile_graph_list_page_data = mock.Mock(return_value=({"821": 123}, {"821": "graph"}))

        scraper._apply_mobile_graph_differences_to_machine_result(
            page=page,
            context=object(),
            machine_result=machine_result,
            machine_link=machine_link,
        )

        self.assertEqual(page.goto_calls, [bonus_url])
        self.assertNotIn(machine_link, page.goto_calls)
        self.assertEqual(
            scraper._fetch_mobile_graph_list_page_data.call_args.kwargs["start_url"],
            graph_list_url,
        )
        self.assertEqual(dataset.rows[0][1], "123")

    def test_site7_extract_mobile_machine_stat_links_and_values(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        latest_date = datetime(2026, 6, 1)
        machine_html = """
<a href="D2900.do?pmc=40100003&clc=03&urt=2173&mdc=120312&bn=1&dt=1">累計ゲーム</a>
<a href="D2900.do?pmc=40100003&clc=03&urt=2173&mdc=120312&bn=1&dt=2">BB回数</a>
<a href="D2900.do?pmc=40100003&clc=03&urt=2173&mdc=120312&bn=1&dt=4">RB回数</a>
"""

        self.assertEqual(
            scraper.extract_mobile_machine_stat_list_links(machine_html),
            {
                "G数": "https://m.site777.jp/db/D2900.do?pmc=40100003&clc=03&urt=2173&mdc=120312&bn=1&dt=1",
                "BB": "https://m.site777.jp/db/D2900.do?pmc=40100003&clc=03&urt=2173&mdc=120312&bn=1&dt=2",
                "RB": "https://m.site777.jp/db/D2900.do?pmc=40100003&clc=03&urt=2173&mdc=120312&bn=1&dt=4",
            },
        )

        stat_html = """
<table>
  <tr><th>台番</th><th>6/1</th><th>5/31</th><th>5/30</th></tr>
  <tr><td>827</td><td>7,867回</td><td>7470</td><td>7055</td></tr>
  <tr><td>828</td><td>-</td><td>5517</td><td>4282</td></tr>
</table>
"""

        self.assertEqual(
            scraper.extract_mobile_machine_stat_values(stat_html, latest_date=latest_date),
            {
                ("2026-06-01", "827"): "7867",
                ("2026-05-31", "827"): "7470",
                ("2026-05-30", "827"): "7055",
                ("2026-05-31", "828"): "5517",
                ("2026-05-30", "828"): "4282",
            },
        )
        self.assertEqual(
            scraper.extract_mobile_machine_stat_next_page_links(
                '<a href="D2900.do?pmc=40100003&clc=03&urt=2173&mdc=120312&bn=1&dt=1&pan=2">データ２&gt;&gt;</a>',
                "https://m.site777.jp/db/D2900.do?pmc=40100003&clc=03&urt=2173&mdc=120312&bn=1&dt=1",
            ),
            ["https://m.site777.jp/db/D2900.do?pmc=40100003&clc=03&urt=2173&mdc=120312&bn=1&dt=1&pan=2"],
        )

    def test_site7_extract_mobile_machine_day_rows(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = """
<table>
  <tr><td></td><td>台番</td><td>累計ｹﾞｰﾑ</td><td>BB回数</td><td>RB回数</td><td>合成確率</td><td>ART回数</td></tr>
  <tr><td></td><td>821</td><td>122</td><td>0</td><td>2</td><td>61</td><td>--</td></tr>
  <tr><td></td><td>827</td><td>7,867</td><td>19</td><td>40</td><td>133</td><td>--</td></tr>
  <tr><td></td><td>平均</td><td>121</td><td>0</td><td>0</td><td>160</td><td>--</td></tr>
</table>
"""

        self.assertEqual(
            scraper.extract_mobile_machine_day_rows(html),
            {
                "821": {"G数": "122", "BB": "0", "RB": "2"},
                "827": {"G数": "7867", "BB": "19", "RB": "40"},
            },
        )

        dataset = scraper._build_mobile_dataset_for_day(
            html=html,
            store_name="Ａパーク春日店",
            store_url="https://example.com/hall",
            target_date="2026-06-01",
            date_url="https://example.com/day",
            machine_name=SITE7_TARGET_MACHINE_NAME,
            machine_url="https://example.com/machine",
        )

        self.assertEqual(dataset.rows[1][0], "827")
        self.assertEqual(dataset.rows[1][2:9], ["7867", "-", "19", "40", "1/133", "1/414", "1/196"])

    def test_site7_extract_mobile_machine_stat_values_from_text(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        latest_date = datetime(2026, 6, 1)
        stat_text = """
台番
6/1
5/31
5/30
827
7867
7470
7055
828
4280
5517
4282
"""

        self.assertEqual(
            scraper.extract_mobile_machine_stat_values(stat_text, latest_date=latest_date),
            {
                ("2026-06-01", "827"): "7867",
                ("2026-05-31", "827"): "7470",
                ("2026-05-30", "827"): "7055",
                ("2026-06-01", "828"): "4280",
                ("2026-05-31", "828"): "5517",
                ("2026-05-30", "828"): "4282",
            },
        )

    def test_site7_apply_mobile_machine_stat_values_refreshes_ratios(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        dataset = MachineDataset(
            store_name="Ａパーク春日店",
            store_url="https://example.com/site7-hall",
            target_date="2026-06-01",
            date_url="https://example.com/site7-hall#ata0",
            machine_name=SITE7_TARGET_MACHINE_NAME,
            machine_url="https://example.com/site7-machine",
            columns=["台番", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率"],
            rows=[["827", "-", "1000", "-", "1", "1", "1/500", "1/1000", "1/1000"]],
        )

        scraper._apply_mobile_machine_stat_values_to_dataset(
            dataset,
            {("2026-06-01", "827"): {"G数": "7867", "BB": "19", "RB": "40"}},
        )

        self.assertEqual(dataset.rows[0][2:9], ["7867", "-", "19", "40", "1/133", "1/414", "1/196"])

    def test_site7_apply_mobile_graph_differences_uses_zero_when_graph_is_unreadable(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        dataset = MachineDataset(
            store_name="Ａパーク春日店",
            store_url="https://example.com/site7-hall",
            target_date="2026-06-01",
            date_url="https://example.com/site7-hall#ata0",
            machine_name="スマスロ ハナビ",
            machine_url="https://example.com/site7-machine",
            columns=["台番", "差枚", "G数", "BB", "RB"],
            rows=[["718", "-", "183", "0", "0"]],
        )
        scraper._fetch_mobile_graph_page_data = mock.Mock(return_value=(None, {}))

        scraper._apply_mobile_graph_differences_to_dataset(
            page=object(),
            context=object(),
            dataset=dataset,
            list_difference_values={},
            detail_slot_numbers=set(),
            slot_graph_links={"718": "https://example.com/graph?dn=718"},
            day_index=0,
            cancel_requested=None,
            progress_callback=None,
            current_graph_count_ref=lambda: 0,
            total_graph_count=1,
        )

        self.assertEqual(dataset.rows[0][1], "0")
        self.assertTrue(dataset_has_site7_graph_difference(dataset, "718"))

    def test_site7_apply_mobile_graph_differences_uses_zero_when_graph_link_is_missing(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        dataset = MachineDataset(
            store_name="Ａパーク春日店",
            store_url="https://example.com/site7-hall",
            target_date="2026-06-01",
            date_url="https://example.com/site7-hall#ata0",
            machine_name="スマスロ ハナビ",
            machine_url="https://example.com/site7-machine",
            columns=["台番", "差枚", "G数", "BB", "RB"],
            rows=[["718", "-", "183", "0", "0"]],
        )

        scraper._apply_mobile_graph_differences_to_dataset(
            page=object(),
            context=object(),
            dataset=dataset,
            list_difference_values={},
            detail_slot_numbers=set(),
            slot_graph_links={},
            day_index=0,
            cancel_requested=None,
            progress_callback=None,
            current_graph_count_ref=lambda: 0,
            total_graph_count=1,
        )

        self.assertEqual(dataset.rows[0][1], "0")
        self.assertTrue(dataset_has_site7_graph_difference(dataset, "718"))

    def test_site7_extract_mobile_slot_graph_page_stat_values_uses_target_slot_block(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = """
<section>
  <h3>821番台 /【1000円/46枚】スロ</h3>
  <p>累計ゲーム 5285回</p>
  <p>BB回数 15回</p>
  <p>RB回数 13回</p>
</section>
<section>
  <h3>827番台 /【1000円/46枚】スロ</h3>
  <p>累計ゲーム 7,867回</p>
  <p>最高出玉 1099</p>
  <p>BB回数 19回</p>
  <p>RB回数 40回</p>
</section>
"""

        self.assertEqual(
            scraper.extract_mobile_slot_graph_page_stat_values(html, "827"),
            {"G数": "7867", "BB": "19", "RB": "40"},
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

    def test_site7_extract_target_hall_search_code_prefers_registered_hall_id(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = """
<!DOCTYPE html>
<html lang="ja">
  <body>
    <div class="hall">
      <p class="hall-name">
        <a href="#" onclick="javascript:hallClick('42006007');">
          <img src="HallName.do?hn=41944978" border="0" alt="ホール名称">
        </a>
      </p>
      <p class="address">福岡県筑紫野市筑紫９６８番２</p>
    </div>
  </body>
</html>
"""
        target_store = RegisteredStore(
            name="スーパーDステーション39筑紫野店",
            url="https://example.com/chikushino",
            site7_enabled=True,
            site7_prefecture="福岡県",
            site7_area="筑紫野市",
            site7_store_name="スーパーＤ’ステーション３９筑紫野店",
            site7_hall_id="42006007",
            site7_address="福岡県筑紫野市筑紫９６８番２",
        ).to_site7_target_store()

        self.assertEqual(scraper.extract_target_hall_search_code(html, target_store), "42006007")

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
        self.assertEqual(target_store.hall_id, "")

    def test_registered_store_target_store_keeps_hall_id_and_address(self) -> None:
        target_store = RegisteredStore(
            name="スーパーDステーション39筑紫野店",
            url="https://example.com/chikushino",
            site7_enabled=True,
            site7_prefecture="福岡県",
            site7_area="筑紫野市",
            site7_store_name="スーパーＤ’ステーション３９筑紫野店",
            site7_hall_id="42006007",
            site7_address="福岡県筑紫野市筑紫９６８番２",
        ).to_site7_target_store()

        self.assertEqual(target_store.hall_id, "42006007")
        self.assertEqual(target_store.hall_address, "福岡県筑紫野市筑紫９６８番２")

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
            scraper.extract_target_hall_search_code(html, find_site7_target_store("GOGOアリーナ天神")),
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

        self.assertEqual(page.wait_calls, [100])

    def test_site7_manual_confirmation_waits_until_screen_is_cleared(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        page = FakeSite7ManualConfirmationPage(release_after_waits=2)

        scraper._wait_for_site7_manual_confirmation_if_present(page, cancel_requested=None)

        self.assertEqual(page.wait_calls, [500, 500])
        self.assertEqual(page.bring_to_front_count, 1)

    def test_site7_manual_confirmation_wait_can_be_cancelled(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        page = FakeSite7ManualConfirmationPage(release_after_waits=None)
        calls = 0

        def cancel_requested() -> bool:
            nonlocal calls
            calls += 1
            return calls >= 2

        with self.assertRaises(Site7FetchCancelled):
            scraper._wait_for_site7_manual_confirmation_if_present(page, cancel_requested=cancel_requested)

        self.assertEqual(page.wait_calls, [500])

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

    def test_fetch_single_site7_store_filters_saved_slots_before_saving(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.fetch_cancel_event = threading.Event()
        app.result_queue = queue.Queue()

        raw_result = MachineHistoryResult(
            store_name="サイトセブン店",
            store_url="https://example.com/site7-hall",
            start_date="2026-04-25",
            end_date="2026-04-25",
            date_pages=[StoreDatePage(target_date="2026-04-25", date_url="https://example.com/site7-hall#ata0")],
            datasets=[
                MachineDataset(
                    store_name="サイトセブン店",
                    store_url="https://example.com/site7-hall",
                    target_date="2026-04-25",
                    date_url="https://example.com/site7-hall#ata0",
                    machine_name=SITE7_TARGET_MACHINE_NAME,
                    machine_url="https://example.com/site7-machine",
                    columns=["台番", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率"],
                    rows=[
                        ["821", "100", "1000", "-", "5", "2", "1/143", "1/200", "1/500"],
                        ["822", "200", "2000", "-", "8", "3", "1/182", "1/250", "1/666"],
                    ],
                )
            ],
        )

        class FakeSite7Scraper:
            def fetch_target_machine_history(
                self,
                *,
                recent_days: int,
                browser_visible: bool,
                progress_callback: object,
                target_store: object,
                cancel_requested: object,
                machine_base_result_callback: object,
                machine_result_callback: object,
                machine_result_filter_callback: object,
                machine_protected_slots_callback: object,
                include_graph_differences: bool,
                defer_graph_differences: bool,
            ) -> MachineHistoryResult:
                filtered_result = machine_result_filter_callback(raw_result)
                if machine_base_result_callback is not None:
                    machine_base_result_callback(filtered_result)
                if machine_result_callback is not None:
                    machine_result_callback(filtered_result)
                return filtered_result

        class FakePersistenceService:
            def __init__(self) -> None:
                self.saved_results: list[MachineHistoryResult] = []

            def resolve_preferred_store_by_name(self, store_name: str) -> None:
                return None

            def find_saved_full_day_dates(
                self,
                store_name: str,
                store_url: str,
                start_date: str,
                end_date: str,
            ) -> SavedFullDayDatesSummary:
                return SavedFullDayDatesSummary()

            def find_saved_machine_slots(
                self,
                store_name: str,
                store_url: str,
                start_date: str,
                end_date: str,
                slot_numbers: list[str],
                require_source_difference: bool = True,
                site7_updated_at: str | datetime | None = None,
            ) -> SavedMachineSlotsSummary:
                self.checked_slot_numbers = slot_numbers
                self.require_source_difference = require_source_difference
                self.site7_updated_at = site7_updated_at
                return SavedMachineSlotsSummary(protected_slots={("2026-04-25", "821")})

            def save_history_result(self, history_result: MachineHistoryResult) -> PersistenceSummary:
                self.saved_results.append(history_result)
                return PersistenceSummary(web_data_saved=True, web_data_record_count=len(history_result.datasets))

        persistence_service = FakePersistenceService()
        app.site7_scraper = FakeSite7Scraper()
        app.persistence_service = persistence_service

        store_result = app._fetch_single_site7_store(
            registered_store=RegisteredStore(
                name="Aパーク春日店",
                url="https://example.com/minrepo-store",
                site7_enabled=True,
                site7_difference_enabled=True,
            ),
            recent_days=1,
            store_index=1,
            total_stores=1,
            retry_delay_seconds=0,
            browser_visible=True,
        )

        self.assertEqual(len(persistence_service.saved_results), 1)
        self.assertEqual(persistence_service.checked_slot_numbers, ["821", "822"])
        self.assertEqual(persistence_service.saved_results[0].datasets[0].rows, [raw_result.datasets[0].rows[1]])
        self.assertEqual(store_result.history_result.datasets[0].rows, [raw_result.datasets[0].rows[1]])
        self.assertEqual(store_result.save_summary.web_data_record_count, 1)

    def test_fetch_single_site7_store_saves_after_fetch_completes(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.fetch_cancel_event = threading.Event()
        app.result_queue = queue.Queue()
        events: list[str] = []

        raw_result = MachineHistoryResult(
            store_name="サイトセブン店",
            store_url="https://example.com/site7-hall",
            start_date="2026-04-25",
            end_date="2026-04-25",
            date_pages=[StoreDatePage(target_date="2026-04-25", date_url="https://example.com/site7-hall#ata0")],
            datasets=[
                MachineDataset(
                    store_name="サイトセブン店",
                    store_url="https://example.com/site7-hall",
                    target_date="2026-04-25",
                    date_url="https://example.com/site7-hall#ata0",
                    machine_name=SITE7_TARGET_MACHINE_NAME,
                    machine_url="https://example.com/site7-machine",
                    columns=["台番", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率"],
                    rows=[["821", "100", "1000", "-", "5", "2", "1/143", "1/200", "1/500"]],
                )
            ],
        )

        class FakeSite7Scraper:
            def fetch_target_machine_history(
                self,
                *,
                recent_days: int,
                browser_visible: bool,
                progress_callback: object,
                target_store: object,
                cancel_requested: object,
                machine_base_result_callback: object,
                machine_result_callback: object,
                machine_result_filter_callback: object,
                machine_protected_slots_callback: object,
                include_graph_differences: bool,
                defer_graph_differences: bool,
            ) -> MachineHistoryResult:
                self.machine_base_result_callback = machine_base_result_callback
                self.machine_result_callback = machine_result_callback
                events.append("fetch_start")
                filtered_result = machine_result_filter_callback(raw_result)
                events.append("fetch_finish")
                return filtered_result

        class FakePersistenceService:
            def resolve_preferred_store_by_name(self, store_name: str) -> None:
                return None

            def find_saved_full_day_dates(
                self,
                store_name: str,
                store_url: str,
                start_date: str,
                end_date: str,
            ) -> SavedFullDayDatesSummary:
                return SavedFullDayDatesSummary()

            def find_saved_machine_slots(
                self,
                store_name: str,
                store_url: str,
                start_date: str,
                end_date: str,
                slot_numbers: list[str],
                require_source_difference: bool = True,
                site7_updated_at: str | datetime | None = None,
            ) -> SavedMachineSlotsSummary:
                return SavedMachineSlotsSummary()

            def save_history_result(self, history_result: MachineHistoryResult) -> PersistenceSummary:
                events.append("save")
                return PersistenceSummary(web_data_saved=True, web_data_record_count=len(history_result.datasets))

        site7_scraper = FakeSite7Scraper()
        app.site7_scraper = site7_scraper
        app.persistence_service = FakePersistenceService()

        store_result = app._fetch_single_site7_store(
            registered_store=RegisteredStore(
                name="Aパーク春日店",
                url="https://example.com/minrepo-store",
                site7_enabled=True,
                site7_difference_enabled=True,
            ),
            recent_days=1,
            store_index=1,
            total_stores=1,
            retry_delay_seconds=0,
            browser_visible=True,
        )

        self.assertIsNone(site7_scraper.machine_base_result_callback)
        self.assertIsNone(site7_scraper.machine_result_callback)
        self.assertEqual(events, ["fetch_start", "fetch_finish", "save"])
        self.assertEqual(store_result.save_summary.web_data_record_count, 1)

    def test_fetch_single_site7_store_discards_partial_result_when_cancelled_mid_store(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.fetch_cancel_event = threading.Event()
        app.site7_cancel_event = threading.Event()
        app.active_operation_kind = "site7_fetch"
        app.result_queue = queue.Queue()

        raw_result = MachineHistoryResult(
            store_name="サイトセブン店",
            store_url="https://example.com/site7-hall",
            start_date="2026-04-25",
            end_date="2026-04-25",
            date_pages=[StoreDatePage(target_date="2026-04-25", date_url="https://example.com/site7-hall#ata0")],
            datasets=[
                MachineDataset(
                    store_name="サイトセブン店",
                    store_url="https://example.com/site7-hall",
                    target_date="2026-04-25",
                    date_url="https://example.com/site7-hall#ata0",
                    machine_name=SITE7_TARGET_MACHINE_NAME,
                    machine_url="https://example.com/site7-machine",
                    columns=["台番", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率"],
                    rows=[["821", "100", "1000", "-", "5", "2", "1/143", "1/200", "1/500"]],
                )
            ],
        )

        class FakeSite7Scraper:
            def fetch_target_machine_history(
                self,
                *,
                recent_days: int,
                browser_visible: bool,
                progress_callback: object,
                target_store: object,
                cancel_requested: object,
                machine_base_result_callback: object,
                machine_result_callback: object,
                machine_result_filter_callback: object,
                machine_protected_slots_callback: object,
                include_graph_differences: bool,
                defer_graph_differences: bool,
            ) -> MachineHistoryResult:
                machine_result_filter_callback(raw_result)
                app.site7_cancel_event.set()
                raise Site7FetchCancelled("サイトセブン取得を中止しました。")

        class FakePersistenceService:
            def __init__(self) -> None:
                self.saved_results: list[MachineHistoryResult] = []

            def resolve_preferred_store_by_name(self, store_name: str) -> None:
                return None

            def find_saved_machine_slots(
                self,
                store_name: str,
                store_url: str,
                start_date: str,
                end_date: str,
                slot_numbers: list[str],
                require_source_difference: bool = True,
                site7_updated_at: str | datetime | None = None,
            ) -> SavedMachineSlotsSummary:
                return SavedMachineSlotsSummary()

            def save_history_result(self, history_result: MachineHistoryResult) -> PersistenceSummary:
                self.saved_results.append(history_result)
                return PersistenceSummary(web_data_saved=True, web_data_record_count=len(history_result.datasets))

        persistence_service = FakePersistenceService()
        app.site7_scraper = FakeSite7Scraper()
        app.persistence_service = persistence_service

        with self.assertRaises(FetchCancelled):
            app._fetch_single_site7_store(
                registered_store=RegisteredStore(
                    name="Aパーク春日店",
                    url="https://example.com/minrepo-store",
                    site7_enabled=True,
                    site7_difference_enabled=True,
                ),
                recent_days=1,
                store_index=1,
                total_stores=1,
                retry_delay_seconds=0,
                browser_visible=True,
            )

        self.assertEqual(persistence_service.saved_results, [])

    def test_fetch_single_site7_store_skips_graph_when_store_difference_is_off(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.fetch_cancel_event = threading.Event()
        app.result_queue = queue.Queue()

        raw_result = MachineHistoryResult(
            store_name="サイトセブン店",
            store_url="https://example.com/site7-hall",
            start_date="2026-04-25",
            end_date="2026-04-25",
            date_pages=[StoreDatePage(target_date="2026-04-25", date_url="https://example.com/site7-hall#ata0")],
            datasets=[
                MachineDataset(
                    store_name="サイトセブン店",
                    store_url="https://example.com/site7-hall",
                    target_date="2026-04-25",
                    date_url="https://example.com/site7-hall#ata0",
                    machine_name=SITE7_TARGET_MACHINE_NAME,
                    machine_url="https://example.com/site7-machine",
                    columns=["台番", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率"],
                    rows=[["821", "-", "1000", "-", "5", "2", "1/143", "1/200", "1/500"]],
                )
            ],
        )

        class FakeSite7Scraper:
            def fetch_target_machine_history(
                self,
                *,
                recent_days: int,
                browser_visible: bool,
                progress_callback: object,
                target_store: object,
                cancel_requested: object,
                machine_base_result_callback: object,
                machine_result_callback: object,
                machine_result_filter_callback: object,
                machine_protected_slots_callback: object,
                include_graph_differences: bool,
                defer_graph_differences: bool,
            ) -> MachineHistoryResult:
                self.machine_base_result_callback = machine_base_result_callback
                self.include_graph_differences = include_graph_differences
                self.defer_graph_differences = defer_graph_differences
                machine_protected_slots_callback(
                    Site7MachineEntry(display_name=SITE7_TARGET_MACHINE_NAME, machine_name=SITE7_TARGET_MACHINE_NAME),
                    ["2026-04-25"],
                    ["821"],
                    "2026-04-25T23:20:00+09:00",
                )
                filtered_result = machine_result_filter_callback(raw_result)
                if machine_result_callback is not None:
                    machine_result_callback(filtered_result)
                return filtered_result

        class FakePersistenceService:
            def __init__(self) -> None:
                self.require_source_difference_values: list[bool] = []
                self.saved_results: list[MachineHistoryResult] = []

            def resolve_preferred_store_by_name(self, store_name: str) -> None:
                return None

            def find_saved_full_day_dates(
                self,
                store_name: str,
                store_url: str,
                start_date: str,
                end_date: str,
            ) -> SavedFullDayDatesSummary:
                return SavedFullDayDatesSummary()

            def find_saved_machine_slots(
                self,
                store_name: str,
                store_url: str,
                start_date: str,
                end_date: str,
                slot_numbers: list[str],
                require_source_difference: bool = True,
                site7_updated_at: str | datetime | None = None,
            ) -> SavedMachineSlotsSummary:
                self.require_source_difference_values.append(require_source_difference)
                return SavedMachineSlotsSummary()

            def save_history_result(self, history_result: MachineHistoryResult) -> PersistenceSummary:
                self.saved_results.append(history_result)
                return PersistenceSummary(web_data_saved=True, web_data_record_count=len(history_result.datasets))

        site7_scraper = FakeSite7Scraper()
        persistence_service = FakePersistenceService()
        app.site7_scraper = site7_scraper
        app.persistence_service = persistence_service

        store_result = app._fetch_single_site7_store(
            registered_store=RegisteredStore(
                name="Aパーク春日店",
                url="https://example.com/minrepo-store",
                fetch_source=FETCH_SOURCE_BOTH,
                site7_difference_enabled=False,
            ),
            recent_days=1,
            store_index=1,
            total_stores=1,
            retry_delay_seconds=0,
            browser_visible=True,
        )

        self.assertFalse(site7_scraper.include_graph_differences)
        self.assertFalse(site7_scraper.defer_graph_differences)
        self.assertIsNone(site7_scraper.machine_base_result_callback)
        self.assertEqual(persistence_service.require_source_difference_values, [False, False])
        self.assertEqual(len(persistence_service.saved_results), 1)
        self.assertEqual(store_result.save_summary.web_data_record_count, 1)

    def test_fetch_single_site7_store_can_force_neo_im_graph_fetch(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.fetch_cancel_event = threading.Event()
        app.result_queue = queue.Queue()

        raw_result = MachineHistoryResult(
            store_name="サイトセブン店",
            store_url="https://example.com/site7-hall",
            start_date="2026-04-25",
            end_date="2026-04-25",
            date_pages=[StoreDatePage(target_date="2026-04-25", date_url="https://example.com/site7-hall#ata0")],
            datasets=[
                MachineDataset(
                    store_name="サイトセブン店",
                    store_url="https://example.com/site7-hall",
                    target_date="2026-04-25",
                    date_url="https://example.com/site7-hall#ata0",
                    machine_name=SITE7_NEO_IM_MACHINE_NAME,
                    machine_url="https://example.com/site7-machine",
                    columns=["台番", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率"],
                    rows=[["821", "100", "1000", "-", "5", "2", "1/143", "1/200", "1/500"]],
                )
            ],
        )

        class FakeSite7Scraper:
            def fetch_target_machine_history(
                self,
                *,
                recent_days: int,
                browser_visible: bool,
                progress_callback: object,
                target_store: object,
                cancel_requested: object,
                machine_base_result_callback: object,
                machine_result_callback: object,
                machine_result_filter_callback: object,
                machine_protected_slots_callback: object,
                include_graph_differences: bool,
                defer_graph_differences: bool,
                enabled_machine_names: set[str] | None = None,
            ) -> MachineHistoryResult:
                self.enabled_machine_names = enabled_machine_names
                self.include_graph_differences = include_graph_differences
                self.defer_graph_differences = defer_graph_differences
                return machine_result_filter_callback(raw_result)

        class FakePersistenceService:
            def __init__(self) -> None:
                self.require_source_difference_values: list[bool] = []

            def resolve_preferred_store_by_name(self, store_name: str) -> None:
                return None

            def find_saved_machine_slots(
                self,
                store_name: str,
                store_url: str,
                start_date: str,
                end_date: str,
                slot_numbers: list[str],
                require_source_difference: bool = True,
                site7_updated_at: str | datetime | None = None,
            ) -> SavedMachineSlotsSummary:
                self.require_source_difference_values.append(require_source_difference)
                return SavedMachineSlotsSummary()

            def save_history_result(self, history_result: MachineHistoryResult) -> PersistenceSummary:
                return PersistenceSummary(web_data_saved=True, web_data_record_count=len(history_result.datasets))

        site7_scraper = FakeSite7Scraper()
        persistence_service = FakePersistenceService()
        app.site7_scraper = site7_scraper
        app.persistence_service = persistence_service

        store_result = app._fetch_single_site7_store(
            registered_store=RegisteredStore(
                name="Aパーク春日店",
                url="https://example.com/minrepo-store",
                fetch_source=FETCH_SOURCE_BOTH,
                site7_difference_enabled=False,
            ),
            recent_days=1,
            store_index=1,
            total_stores=1,
            retry_delay_seconds=0,
            browser_visible=True,
            enabled_machine_names={SITE7_NEO_IM_MACHINE_NAME},
            force_site7_difference=True,
        )

        self.assertEqual(site7_scraper.enabled_machine_names, {SITE7_NEO_IM_MACHINE_NAME})
        self.assertTrue(site7_scraper.include_graph_differences)
        self.assertTrue(site7_scraper.defer_graph_differences)
        self.assertEqual(persistence_service.require_source_difference_values, [True])
        self.assertEqual(store_result.save_summary.web_data_record_count, 1)

    def test_fetch_single_site7_store_uses_full_day_index_before_slot_checks(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.fetch_cancel_event = threading.Event()
        app.result_queue = queue.Queue()
        callback_results: list[set[tuple[str, str]]] = []

        class FakeSite7Scraper:
            def fetch_target_machine_history(
                self,
                *,
                recent_days: int,
                browser_visible: bool,
                progress_callback: object,
                target_store: object,
                cancel_requested: object,
                machine_base_result_callback: object,
                machine_result_callback: object,
                machine_result_filter_callback: object,
                machine_protected_slots_callback: object,
                include_graph_differences: bool,
                defer_graph_differences: bool,
            ) -> MachineHistoryResult:
                first_result = machine_protected_slots_callback(
                    Site7MachineEntry(display_name="マイジャグラーV", machine_name="マイジャグラーV"),
                    ["2026-06-03", "2026-06-02", "2026-06-01"],
                    ["821", "822"],
                    "2026-06-03T23:20:00+09:00",
                )
                second_result = machine_protected_slots_callback(
                    Site7MachineEntry(display_name="ネオアイムジャグラーEX", machine_name="ネオアイムジャグラーEX"),
                    ["2026-06-03", "2026-06-02", "2026-06-01"],
                    ["831"],
                    "2026-06-03T23:20:00+09:00",
                )
                callback_results.extend([first_result, second_result])
                return MachineHistoryResult(
                    store_name="Aパーク春日店",
                    store_url="https://example.com/store",
                    start_date="2026-06-01",
                    end_date="2026-06-03",
                    date_pages=[],
                    datasets=[],
                    skipped_targets=[],
                    skipped_dates=[],
                )

        class FakePersistenceService:
            def __init__(self) -> None:
                self.full_day_calls: list[tuple[str, str, str, str]] = []
                self.slot_calls: list[tuple[str, str, tuple[str, ...], str | datetime | None]] = []

            def resolve_preferred_store_by_name(self, store_name: str) -> None:
                return None

            def find_saved_full_day_dates(
                self,
                store_name: str,
                store_url: str,
                start_date: str,
                end_date: str,
            ) -> SavedFullDayDatesSummary:
                self.full_day_calls.append((store_name, store_url, start_date, end_date))
                return SavedFullDayDatesSummary(saved_dates={"2026-06-01"})

            def find_saved_machine_slots(
                self,
                store_name: str,
                store_url: str,
                start_date: str,
                end_date: str,
                slot_numbers: list[str],
                require_source_difference: bool = True,
                site7_updated_at: str | datetime | None = None,
            ) -> SavedMachineSlotsSummary:
                self.slot_calls.append((start_date, end_date, tuple(slot_numbers), site7_updated_at))
                return SavedMachineSlotsSummary(protected_slots={(start_date, slot_numbers[0])})

            def save_history_result(self, history_result: MachineHistoryResult) -> PersistenceSummary:
                return PersistenceSummary(web_data_saved=True)

        persistence_service = FakePersistenceService()
        app.site7_scraper = FakeSite7Scraper()
        app.persistence_service = persistence_service

        app._fetch_single_site7_store(
            registered_store=RegisteredStore(
                name="Aパーク春日店",
                url="https://example.com/store",
                site7_enabled=True,
                site7_difference_enabled=True,
            ),
            recent_days=3,
            store_index=1,
            total_stores=1,
            retry_delay_seconds=0,
            browser_visible=True,
        )

        self.assertEqual(
            callback_results[0],
            {
                ("2026-06-01", "821"),
                ("2026-06-01", "822"),
                ("2026-06-02", "821"),
            },
        )
        self.assertEqual(callback_results[1], {("2026-06-01", "831"), ("2026-06-02", "831")})
        self.assertEqual(
            persistence_service.full_day_calls,
            [("Aパーク春日店", "https://example.com/store", "2026-06-01", "2026-06-03")],
        )
        self.assertEqual(
            persistence_service.slot_calls,
            [
                ("2026-06-02", "2026-06-03", ("821", "822"), "2026-06-03T23:20:00+09:00"),
                ("2026-06-02", "2026-06-03", ("831",), "2026-06-03T23:20:00+09:00"),
            ],
        )

    def test_run_site7_fetch_many_skips_site7_when_minrepo_covers_previous_business_day(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.fetch_cancel_event = threading.Event()
        app.result_queue = queue.Queue()
        app._refresh_web_data_for_store_result = mock.Mock()
        app._fetch_single_site7_store = mock.Mock()

        dataset = MachineDataset(
            store_name="Aパーク春日店",
            store_url="https://example.com/store",
            target_date="2026-06-05",
            date_url="https://example.com/minrepo/2026-06-05",
            machine_name=SITE7_TARGET_MACHINE_NAME,
            machine_url="https://example.com/minrepo/machine",
            columns=["台番", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率"],
            rows=[["821", "100", "1000", "-", "5", "2", "1/143", "1/200", "1/500"]],
        )
        minrepo_result = StoreFetchResult(
            history_result=MachineHistoryResult(
                store_name="Aパーク春日店",
                store_url="https://example.com/store",
                start_date="2026-06-05",
                end_date="2026-06-05",
                date_pages=[StoreDatePage(target_date="2026-06-05", date_url="https://example.com/minrepo/2026-06-05")],
                datasets=[dataset],
            ),
            save_summary=PersistenceSummary(web_data_saved=True),
            saved_full_day_summary=SavedFullDayDatesSummary(),
        )
        app._fetch_single_store = mock.Mock(return_value=minrepo_result)

        fetch_many_result = app._run_site7_fetch_many(
            target_stores=[
                RegisteredStore(
                    name="Aパーク春日店",
                    url="https://example.com/store",
                    fetch_source=FETCH_SOURCE_BOTH,
                )
            ],
            recent_days=1,
            retry_delay_seconds=0,
            browser_visible=False,
            site7_updated_at_by_store_url={normalize_store_url("https://example.com/store"): datetime(2026, 6, 5, 23, 8)},
            now=datetime(2026, 6, 6, 1, 5, tzinfo=timezone.utc),
        )

        self.assertEqual(fetch_many_result.results, [minrepo_result])
        app._fetch_single_site7_store.assert_not_called()
        app._fetch_single_store.assert_called_once()
        self.assertEqual(app._fetch_single_store.call_args.kwargs["required_target_dates"], {"2026-06-05"})

    def test_run_site7_fetch_many_falls_back_to_site7_when_minrepo_has_no_previous_day(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.fetch_cancel_event = threading.Event()
        app.result_queue = queue.Queue()
        app._refresh_web_data_for_store_result = mock.Mock()
        minrepo_result = StoreFetchResult(
            history_result=MachineHistoryResult(
                store_name="Aパーク春日店",
                store_url="https://example.com/store",
                start_date="2026-06-05",
                end_date="2026-06-05",
                date_pages=[],
                datasets=[],
            ),
            save_summary=None,
            saved_full_day_summary=SavedFullDayDatesSummary(),
        )
        app._fetch_single_store = mock.Mock(return_value=minrepo_result)
        site7_result = StoreFetchResult(
            history_result=MachineHistoryResult(
                store_name="Aパーク春日店",
                store_url="https://example.com/store",
                start_date="2026-06-05",
                end_date="2026-06-05",
                date_pages=[StoreDatePage(target_date="2026-06-05", date_url="https://m.site777.jp/db/D2300.do")],
                datasets=[],
                skipped_dates=["2026-06-05"],
            ),
            save_summary=None,
            saved_full_day_summary=SavedFullDayDatesSummary(),
        )
        app._fetch_single_site7_store = mock.Mock(return_value=site7_result)

        fetch_many_result = app._run_site7_fetch_many(
            target_stores=[
                RegisteredStore(
                    name="Aパーク春日店",
                    url="https://example.com/store",
                    fetch_source=FETCH_SOURCE_BOTH,
                )
            ],
            recent_days=1,
            retry_delay_seconds=0,
            browser_visible=False,
            site7_updated_at_by_store_url={normalize_store_url("https://example.com/store"): datetime(2026, 6, 5, 23, 8)},
            now=datetime(2026, 6, 6, 1, 5, tzinfo=timezone.utc),
        )

        self.assertEqual(fetch_many_result.results, [site7_result])
        app._fetch_single_store.assert_called_once()
        app._fetch_single_site7_store.assert_called_once()

    def test_run_site7_fetch_many_can_skip_minrepo_prefetch_for_neo_im_only(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.fetch_cancel_event = threading.Event()
        app.result_queue = queue.Queue()
        app._refresh_web_data_for_store_result = mock.Mock()
        app._fetch_single_store = mock.Mock()
        site7_result = StoreFetchResult(
            history_result=MachineHistoryResult(
                store_name="Aパーク春日店",
                store_url="https://example.com/store",
                start_date="2026-06-05",
                end_date="2026-06-05",
                date_pages=[StoreDatePage(target_date="2026-06-05", date_url="https://m.site777.jp/db/D2300.do")],
                datasets=[],
            ),
            save_summary=None,
            saved_full_day_summary=SavedFullDayDatesSummary(),
        )
        app._fetch_single_site7_store = mock.Mock(return_value=site7_result)

        fetch_many_result = app._run_site7_fetch_many(
            target_stores=[
                RegisteredStore(
                    name="Aパーク春日店",
                    url="https://example.com/store",
                    fetch_source=FETCH_SOURCE_BOTH,
                )
            ],
            recent_days=1,
            retry_delay_seconds=0,
            browser_visible=False,
            enabled_machine_names={SITE7_NEO_IM_MACHINE_NAME},
            minrepo_prefetch_enabled=False,
            force_site7_difference=True,
            site7_updated_at_by_store_url={normalize_store_url("https://example.com/store"): datetime(2026, 6, 5, 23, 8)},
            now=datetime(2026, 6, 6, 1, 5, tzinfo=timezone.utc),
        )

        self.assertEqual(fetch_many_result.results, [site7_result])
        app._fetch_single_store.assert_not_called()
        app._fetch_single_site7_store.assert_called_once()
        fetch_call_kwargs = app._fetch_single_site7_store.call_args.kwargs
        self.assertEqual(fetch_call_kwargs["enabled_machine_names"], {SITE7_NEO_IM_MACHINE_NAME})
        self.assertTrue(fetch_call_kwargs["force_site7_difference"])

    def test_site7_registered_stores_from_keeps_beam_hikari_daidata_online_store(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        beam_store = RegisteredStore(
            name="ビームヒカリ",
            url="https://example.com/beam",
            site7_enabled=True,
            site7_area="大野城市",
        )
        hinode_store = RegisteredStore(
            name="HINODE大野城店",
            url="https://example.com/hinode",
            site7_enabled=True,
            site7_area="大野城市",
        )

        self.assertEqual(app._site7_registered_stores_from([beam_store, hinode_store]), [hinode_store, beam_store])

    def test_site7_registered_stores_from_keeps_only_beam_hikari_daidata_online_store(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        beam_store = RegisteredStore(
            name="ビームヒカリ",
            url="https://example.com/beam",
            site7_enabled=True,
            site7_area="大野城市",
        )

        self.assertEqual(app._site7_registered_stores_from([beam_store]), [beam_store])

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
            ["821", "379", "2163", "-", "10", "5", "1/144", "1/216", "1/432"],
        )

    def test_site7_parse_machine_history_skips_blank_holiday_rows(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        soup = BeautifulSoup(find_gui_fixture("site7_machine.html"), "html.parser")
        day_container = soup.find(id="ata0")
        self.assertIsNotNone(day_container)
        for row in day_container.find_all("tr")[1:]:
            cells = row.find_all("td")
            for cell in cells[1:7]:
                cell.string = "-"

        history_result = scraper.parse_machine_history_html(
            str(soup),
            store_url="https://example.com/site7",
            page_url="https://example.com/site7/machine",
            recent_days=2,
        )

        self.assertEqual([page.target_date for page in history_result.date_pages], ["2026-04-24"])
        self.assertEqual([dataset.target_date for dataset in history_result.datasets], ["2026-04-24"])

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

    def test_site7_extract_updated_datetime_keeps_source_time(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = '<p id="hall_date">データ更新日時：2026/04/28 23:08</p>'

        updated_at = scraper.extract_updated_datetime(html)

        self.assertEqual(updated_at, datetime(2026, 4, 28, 23, 8))

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
                "difference_value": None,
                "bonus_difference_value": 843,
                "games_count": 5454,
                "payout_rate": None,
                "bb_count": 25,
                "rb_count": 12,
                "combined_ratio_text": "1/147",
                "bb_ratio_text": "1/218",
                "rb_ratio_text": "1/454",
            },
        )

    def test_daidata_machine_dataset_uses_total_start_without_difference(self) -> None:
        html = """
        <html>
          <body>
            <p>データ更新 2026.06.25 00:07</p>
            <select name="hist_num">
              <option value="0" selected>2026/06/24</option>
            </select>
            <table>
              <tr>
                <th>台番号</th>
                <th>スタート回数</th>
                <th>BB回数</th>
                <th>RB回数</th>
                <th>合成確率</th>
                <th>BB確率</th>
                <th>RB確率</th>
                <th>累計スタート</th>
                <th>前日最終スタート</th>
              </tr>
              <tr>
                <td>821</td>
                <td>56</td>
                <td>4</td>
                <td>3</td>
                <td>176.2</td>
                <td>308.5</td>
                <td>411.3</td>
                <td>1,234</td>
                <td>99</td>
              </tr>
            </table>
          </body>
        </html>
        """

        dataset = build_daidata_machine_dataset(
            html,
            store_name="ビームヒカリ",
            store_url=DAIDATA_BEAM_HIKARI_URL,
            machine_name=SITE7_NEO_IM_MACHINE_NAME,
            machine_url=f"{DAIDATA_BEAM_HIKARI_URL}/unit_list?model=neo",
            hist_num=0,
        )

        self.assertEqual(dataset.target_date, "2026-06-24")
        self.assertEqual(dataset.rows[0], ["821", "-", "1234", "-", "4", "3", "1/176.2", "1/308.5", "1/411.3"])
        self.assertEqual(site7_dataset_updated_at(dataset), "2026-06-25T00:07:00+09:00")

    def test_daidata_build_machine_daily_records_is_site7_provisional_source(self) -> None:
        dataset = MachineDataset(
            store_name="ビームヒカリ",
            store_url=DAIDATA_BEAM_HIKARI_URL,
            target_date="2026-06-24",
            date_url=f"{DAIDATA_BEAM_HIKARI_URL}/unit_list?model=neo&hist_num=0",
            machine_name=SITE7_NEO_IM_MACHINE_NAME,
            machine_url=f"{DAIDATA_BEAM_HIKARI_URL}/unit_list?model=neo",
            columns=["台番", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率"],
            rows=[["821", "-", "1234", "-", "4", "3", "1/176.2", "1/308.5", "1/411.3"]],
        )
        set_site7_dataset_updated_at(dataset, "2026-06-25T00:07:00+09:00")
        history_result = MachineHistoryResult(
            store_name="ビームヒカリ",
            store_url=DAIDATA_BEAM_HIKARI_URL,
            start_date="2026-06-24",
            end_date="2026-06-24",
            date_pages=[StoreDatePage(target_date="2026-06-24", date_url=dataset.date_url)],
            datasets=[dataset],
        )

        records = build_machine_daily_records(history_result)

        self.assertEqual(records[0]["data_source"], DATA_SOURCE_SITE7)
        self.assertIsNone(records[0]["difference_value"])
        self.assertEqual(records[0]["games_count"], 1234)
        self.assertEqual(records[0]["site7_fetched_at"], "2026-06-25T00:07:00+09:00")

    def test_daidata_store_and_machine_link_filters_beam_jugglers(self) -> None:
        html = """
        <a href="/100619/unit_list?model=neo&ballPrice=20.00&ps=S">ネオアイムジャグラーEX 20円スロット |</a>
        <a href="/100619/unit_list?model=hokuto&ballPrice=20.00&ps=S">スマスロ北斗の拳 20円スロット |</a>
        """

        entries = DaidataOnlineScraper().extract_juggler_machine_links(
            html,
            DAIDATA_BEAM_HIKARI_URL,
            enabled_machine_names={SITE7_NEO_IM_MACHINE_NAME},
        )

        self.assertTrue(daidata_store_is_beam_hikari("ビームヒカリ", ""))
        self.assertEqual([entry.machine_name for entry in entries], [SITE7_NEO_IM_MACHINE_NAME])

    def test_site7_build_machine_daily_records_skips_blank_rows(self) -> None:
        history_result = MachineHistoryResult(
            store_name="Ａパーク春日店",
            store_url="https://www.d-deltanet.com/pc/HallSelectLink.do?hallcode=example",
            start_date="2026-04-25",
            end_date="2026-04-25",
            date_pages=[
                StoreDatePage(
                    target_date="2026-04-25",
                    date_url="https://www.d-deltanet.com/pc/BonusList.do?model=example#ata0",
                )
            ],
            datasets=[
                MachineDataset(
                    store_name="Ａパーク春日店",
                    store_url="https://www.d-deltanet.com/pc/HallSelectLink.do?hallcode=example",
                    target_date="2026-04-25",
                    date_url="https://www.d-deltanet.com/pc/BonusList.do?model=example#ata0",
                    machine_name=SITE7_TARGET_MACHINE_NAME,
                    machine_url="https://www.d-deltanet.com/pc/BonusList.do?model=example",
                    columns=["台番", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率"],
                    rows=[["821", "-", "-", "-", "-", "-", "-", "-", "-"]],
                )
            ],
        )

        self.assertEqual(build_machine_daily_records(history_result), [])

    def test_site7_build_machine_daily_records_marks_graph_difference_source(self) -> None:
        dataset = MachineDataset(
            store_name="Ａパーク春日店",
            store_url="https://www.d-deltanet.com/pc/HallSelectLink.do?hallcode=example",
            target_date="2026-06-01",
            date_url="https://www.d-deltanet.com/pc/BonusList.do?model=example#ata0",
            machine_name="スマスロ ハナビ",
            machine_url="https://www.d-deltanet.com/pc/BonusList.do?model=example",
            columns=["台番", "差枚", "G数", "BB", "RB"],
            rows=[["718", "-850", "1154", "3", "1"]],
        )
        mark_site7_dataset_graph_difference(dataset, "718")
        history_result = MachineHistoryResult(
            store_name="Ａパーク春日店",
            store_url="https://www.d-deltanet.com/pc/HallSelectLink.do?hallcode=example",
            start_date="2026-06-01",
            end_date="2026-06-01",
            date_pages=[StoreDatePage(target_date="2026-06-01", date_url=dataset.date_url)],
            datasets=[dataset],
        )

        records = build_machine_daily_records(history_result)

        self.assertEqual(records[0]["difference_value"], -850)
        self.assertEqual(records[0]["site7_difference_source"], "graph")

    def test_r2_merge_removes_existing_blank_site7_rows(self) -> None:
        service = HistoryPersistenceService(root_dir=ROOT_DIR, r2_storage=FakeR2JsonStorage())
        existing_records = [
            {
                "target_date": "2026-05-11",
                "slot_number": "821",
                "machine_name": SITE7_TARGET_MACHINE_NAME,
                "data_source": DATA_SOURCE_SITE7,
                "difference_value": None,
                "bonus_difference_value": None,
                "games_count": None,
                "payout_rate": None,
                "bb_count": None,
                "rb_count": None,
                "combined_ratio_text": "-",
                "bb_ratio_text": "-",
                "rb_ratio_text": "-",
            },
            {
                "target_date": "2026-05-12",
                "slot_number": "821",
                "machine_name": SITE7_TARGET_MACHINE_NAME,
                "data_source": DATA_SOURCE_SITE7,
                "difference_value": 100,
                "bonus_difference_value": 80,
                "games_count": 2000,
                "payout_rate": None,
                "bb_count": 8,
                "rb_count": 6,
                "combined_ratio_text": "1/142",
                "bb_ratio_text": "1/250",
                "rb_ratio_text": "1/333",
            },
        ]

        merged_records = service._merge_r2_records(existing_records, [])

        self.assertEqual([record["target_date"] for record in merged_records], ["2026-05-12"])

    def test_web_export_skips_blank_site7_rows(self) -> None:
        record = safe_record(
            {
                "target_date": "2026-05-11",
                "slot_number": "821",
                "machine_name": SITE7_TARGET_MACHINE_NAME,
                "data_source": DATA_SOURCE_SITE7,
                "difference_value": None,
                "bonus_difference_value": None,
                "games_count": None,
                "payout_rate": None,
                "bb_count": None,
                "rb_count": None,
                "combined_ratio_text": "-",
                "bb_ratio_text": "-",
                "rb_ratio_text": "-",
            }
        )

        self.assertIsNone(record)

    def test_web_export_keeps_site7_fetched_at(self) -> None:
        record = safe_record(
            {
                "target_date": "2026-05-12",
                "slot_number": "821",
                "machine_name": SITE7_TARGET_MACHINE_NAME,
                "data_source": DATA_SOURCE_SITE7,
                "difference_value": 100,
                "games_count": 2000,
                "bb_count": 8,
                "rb_count": 6,
                "site7_fetched_at": "2026-05-17T12:34:56+09:00",
                "site7_difference_source": "graph",
            }
        )

        self.assertIsNotNone(record)
        self.assertEqual(record["site7_fetched_at"], "2026-05-17T12:34:56+09:00")
        self.assertEqual(record["site7_difference_source"], "graph")
        self.assertEqual(record["setting_estimate_status"], "provisional")
        self.assertEqual(record["estimated_difference_status"], "provisional")
        self.assertAlmostEqual(record["estimated_grape_denominator"], 6.2269)
        self.assertEqual(record["estimated_grape_status"], "provisional")
        self.assertEqual(record["estimated_grape_source"], "site7")
        self.assertEqual(record["estimated_grape_version"], ESTIMATED_GRAPE_VALUE_VERSION)
        self.assertAlmostEqual(record["setting_estimate_grape_average"], 3.208417771027268)
        self.assertEqual(record["setting_estimate_grape_status"], "provisional")
        self.assertEqual(record["setting_estimate_grape_source"], "site7")
        self.assertEqual(record["setting_estimate_grape_version"], SETTING_ESTIMATE_GRAPE_VALUE_VERSION)

    def test_site7_build_machine_daily_records_uses_source_updated_at(self) -> None:
        dataset = MachineDataset(
            store_name="サイトセブン店",
            store_url="https://example.com/site7",
            target_date="2026-06-05",
            date_url="https://m.site777.jp/db/D2300.do",
            machine_name=SITE7_TARGET_MACHINE_NAME,
            machine_url="https://m.site777.jp/db/D2300.do",
            columns=["台番", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率"],
            rows=[["821", "100", "1000", "-", "5", "2", "1/143", "1/200", "1/500"]],
        )
        set_site7_dataset_updated_at(dataset, datetime(2026, 6, 5, 23, 8))

        records = build_machine_daily_records(
            MachineHistoryResult(
                store_name="サイトセブン店",
                store_url="https://example.com/site7",
                start_date="2026-06-05",
                end_date="2026-06-05",
                date_pages=[],
                datasets=[dataset],
            )
        )

        self.assertEqual(records[0]["site7_fetched_at"], "2026-06-05T23:08:00+09:00")

    def test_site7_dataset_updated_at_survives_rewrite_and_strip(self) -> None:
        dataset = MachineDataset(
            store_name="サイトセブン店",
            store_url="https://example.com/site7",
            target_date="2026-06-05",
            date_url="https://m.site777.jp/db/D2300.do",
            machine_name=SITE7_TARGET_MACHINE_NAME,
            machine_url="https://m.site777.jp/db/D2300.do",
            columns=["台番", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率"],
            rows=[["821", "100", "1000", "-", "5", "2", "1/143", "1/200", "1/500"]],
        )
        set_site7_dataset_updated_at(dataset, "2026-06-05T23:08:00+09:00")
        history_result = MachineHistoryResult(
            store_name="サイトセブン店",
            store_url="https://example.com/site7",
            start_date="2026-06-05",
            end_date="2026-06-05",
            date_pages=[],
            datasets=[dataset],
        )

        rewritten_result = rewrite_history_result_store(history_result, "Aパーク春日店", "https://example.com/store")
        stripped_result = strip_site7_history_result_source_differences(rewritten_result)

        self.assertEqual(site7_dataset_updated_at(stripped_result.datasets[0]), "2026-06-05T23:08:00+09:00")

    def test_web_export_adds_setting_estimate_values(self) -> None:
        record = safe_record(
            {
                "target_date": "2026-05-12",
                "slot_number": "821",
                "machine_name": "マイジャグラーV",
                "games_count": 5454,
                "bb_count": 25,
                "rb_count": 12,
            }
        )

        self.assertIsNotNone(record)
        self.assertAlmostEqual(record["setting_estimate_average"], 2.3920680616849204)
        self.assertEqual(record["setting_estimate_status"], "confirmed")
        self.assertEqual(record["setting_estimate_source"], "minrepo")
        self.assertEqual(record["setting_estimate_version"], SETTING_ESTIMATE_VALUE_VERSION)
        self.assertNotIn("setting_estimate_grape_average", record)
        self.assertEqual(record["estimated_difference_value"], 782)
        self.assertEqual(record["estimated_difference_status"], "confirmed")
        self.assertEqual(record["estimated_difference_source"], "minrepo")
        self.assertEqual(record["estimated_difference_version"], SETTING_ESTIMATE_VALUE_VERSION)

    def test_calculate_estimated_grape_value_for_aim_juggler(self) -> None:
        estimated_grape = calculate_estimated_grape_value(
            "ネオアイムジャグラーEX",
            {
                "games_count": 9264,
                "difference_value": 4595,
                "bb_count": 50,
                "rb_count": 37,
            },
            setting_average=6,
        )

        self.assertIsNotNone(estimated_grape)
        self.assertAlmostEqual(estimated_grape["count"], 1479.476)
        self.assertAlmostEqual(estimated_grape["denominator"], 6.2425)
        self.assertAlmostEqual(estimated_grape["probability"], 0.16019263)

    def test_calculate_estimated_grape_value_for_gogo_juggler(self) -> None:
        estimated_grape = calculate_estimated_grape_value(
            "ゴーゴージャグラー３",
            {
                "games_count": 9283,
                "difference_value": 4096,
                "bb_count": 50,
                "rb_count": 38,
            },
            setting_average=6,
        )

        self.assertIsNotNone(estimated_grape)
        self.assertAlmostEqual(estimated_grape["count"], 1477.134)
        self.assertAlmostEqual(estimated_grape["denominator"], 6.2585)
        self.assertAlmostEqual(estimated_grape["probability"], 0.15978171)

    def test_calculate_estimated_grape_value_for_my_juggler(self) -> None:
        estimated_grape = calculate_estimated_grape_value(
            "マイジャグラーV",
            {
                "games_count": 9000,
                "difference_value": 3000,
                "bb_count": 40,
                "rb_count": 35,
            },
            setting_average=6,
        )

        self.assertIsNotNone(estimated_grape)
        self.assertAlmostEqual(estimated_grape["count"], 1596.882)
        self.assertAlmostEqual(estimated_grape["denominator"], 5.6207)
        self.assertAlmostEqual(estimated_grape["probability"], 0.17791529)

    def test_calculate_estimated_grape_value_for_funky_juggler(self) -> None:
        estimated_grape = calculate_estimated_grape_value(
            "ファンキージャグラー２ＫＴ",
            {
                "games_count": 9000,
                "difference_value": 3000,
                "bb_count": 40,
                "rb_count": 35,
            },
            setting_average=6,
        )

        self.assertIsNotNone(estimated_grape)
        self.assertAlmostEqual(estimated_grape["count"], 1597.283)
        self.assertAlmostEqual(estimated_grape["denominator"], 5.6192)
        self.assertAlmostEqual(estimated_grape["probability"], 0.17795989)

    def test_calculate_estimated_grape_value_for_juggler_girls(self) -> None:
        estimated_grape = calculate_estimated_grape_value(
            "ジャグラーガールズSS",
            {
                "games_count": 9000,
                "difference_value": 3000,
                "bb_count": 40,
                "rb_count": 35,
            },
            setting_average=6,
        )

        self.assertIsNotNone(estimated_grape)
        self.assertAlmostEqual(estimated_grape["count"], 1591.52)
        self.assertAlmostEqual(estimated_grape["denominator"], 5.6396)
        self.assertAlmostEqual(estimated_grape["probability"], 0.17731783)

    def test_calculate_estimated_grape_value_for_mr_juggler(self) -> None:
        estimated_grape = calculate_estimated_grape_value(
            "ミスタージャグラー",
            {
                "games_count": 8474,
                "difference_value": -384,
                "bb_count": 28,
                "rb_count": 33,
            },
            setting_average=4,
        )

        self.assertIsNotNone(estimated_grape)
        self.assertAlmostEqual(estimated_grape["count"], 1312.731)
        self.assertAlmostEqual(estimated_grape["denominator"], 6.4401)
        self.assertAlmostEqual(estimated_grape["probability"], 0.15527769)

    def test_web_export_adds_estimated_grape_values_for_aim_juggler(self) -> None:
        record = safe_record(
            {
                "target_date": "2026-05-12",
                "slot_number": "821",
                "machine_name": "ネオアイムジャグラーEX",
                "difference_value": 4595,
                "games_count": 9264,
                "bb_count": 50,
                "rb_count": 37,
            }
        )

        self.assertIsNotNone(record)
        self.assertAlmostEqual(record["estimated_grape_denominator"], 6.2413)
        self.assertEqual(record["estimated_grape_status"], "confirmed")
        self.assertEqual(record["estimated_grape_source"], "minrepo")
        self.assertEqual(record["estimated_grape_version"], ESTIMATED_GRAPE_VALUE_VERSION)
        self.assertAlmostEqual(record["setting_estimate_grape_average"], 4.47600865437615)
        self.assertEqual(record["setting_estimate_grape_status"], "confirmed")
        self.assertEqual(record["setting_estimate_grape_source"], "minrepo")
        self.assertEqual(record["setting_estimate_grape_version"], SETTING_ESTIMATE_GRAPE_VALUE_VERSION)

    def test_web_export_adds_estimated_grape_values_for_gogo_juggler(self) -> None:
        record = safe_record(
            {
                "target_date": "2026-05-12",
                "slot_number": "821",
                "machine_name": "ゴーゴージャグラー３",
                "difference_value": 4096,
                "games_count": 9283,
                "bb_count": 50,
                "rb_count": 38,
            }
        )

        self.assertIsNotNone(record)
        self.assertAlmostEqual(record["estimated_grape_denominator"], 6.2577)
        self.assertEqual(record["estimated_grape_status"], "confirmed")
        self.assertEqual(record["estimated_grape_source"], "minrepo")
        self.assertEqual(record["estimated_grape_version"], ESTIMATED_GRAPE_VALUE_VERSION)
        self.assertAlmostEqual(record["setting_estimate_grape_average"], 3.8553623185503496)
        self.assertEqual(record["setting_estimate_grape_status"], "confirmed")
        self.assertEqual(record["setting_estimate_grape_source"], "minrepo")
        self.assertEqual(record["setting_estimate_grape_version"], SETTING_ESTIMATE_GRAPE_VALUE_VERSION)

    def test_web_export_adds_estimated_grape_values_for_my_juggler(self) -> None:
        record = safe_record(
            {
                "target_date": "2026-05-12",
                "slot_number": "821",
                "machine_name": "マイジャグラーV",
                "difference_value": 3000,
                "games_count": 9000,
                "bb_count": 40,
                "rb_count": 35,
            }
        )

        self.assertIsNotNone(record)
        self.assertAlmostEqual(record["estimated_grape_denominator"], 5.6204)
        self.assertEqual(record["estimated_grape_status"], "confirmed")
        self.assertEqual(record["estimated_grape_source"], "minrepo")
        self.assertEqual(record["estimated_grape_version"], ESTIMATED_GRAPE_VALUE_VERSION)
        self.assertAlmostEqual(record["setting_estimate_grape_average"], 5.245581474282575)
        self.assertEqual(record["setting_estimate_grape_status"], "confirmed")
        self.assertEqual(record["setting_estimate_grape_source"], "minrepo")
        self.assertEqual(record["setting_estimate_grape_version"], SETTING_ESTIMATE_GRAPE_VALUE_VERSION)

    def test_web_export_adds_estimated_grape_values_for_funky_juggler(self) -> None:
        record = safe_record(
            {
                "target_date": "2026-05-12",
                "slot_number": "821",
                "machine_name": "ファンキージャグラー２ＫＴ",
                "difference_value": 3000,
                "games_count": 9000,
                "bb_count": 40,
                "rb_count": 35,
            }
        )

        self.assertIsNotNone(record)
        self.assertAlmostEqual(record["estimated_grape_denominator"], 5.6212)
        self.assertEqual(record["estimated_grape_status"], "confirmed")
        self.assertEqual(record["estimated_grape_source"], "minrepo")
        self.assertEqual(record["estimated_grape_version"], ESTIMATED_GRAPE_VALUE_VERSION)
        self.assertAlmostEqual(record["setting_estimate_grape_average"], 5.590383780578444)
        self.assertEqual(record["setting_estimate_grape_status"], "confirmed")
        self.assertEqual(record["setting_estimate_grape_source"], "minrepo")
        self.assertEqual(record["setting_estimate_grape_version"], SETTING_ESTIMATE_GRAPE_VALUE_VERSION)

    def test_web_export_adds_estimated_grape_values_for_juggler_girls(self) -> None:
        record = safe_record(
            {
                "target_date": "2026-05-12",
                "slot_number": "1052",
                "machine_name": "ジャグラーガールズSS",
                "difference_value": -221,
                "games_count": 6321,
                "bb_count": 17,
                "rb_count": 33,
            }
        )

        self.assertIsNotNone(record)
        self.assertAlmostEqual(record["estimated_grape_denominator"], 5.9532)
        self.assertEqual(record["estimated_grape_status"], "confirmed")
        self.assertEqual(record["estimated_grape_source"], "minrepo")
        self.assertEqual(record["estimated_grape_version"], ESTIMATED_GRAPE_VALUE_VERSION)
        self.assertAlmostEqual(record["setting_estimate_grape_average"], 4.607028876552125)
        self.assertEqual(record["setting_estimate_grape_status"], "confirmed")
        self.assertEqual(record["setting_estimate_grape_source"], "minrepo")
        self.assertEqual(record["setting_estimate_grape_version"], SETTING_ESTIMATE_GRAPE_VALUE_VERSION)

    def test_web_export_adds_estimated_grape_values_for_mr_juggler(self) -> None:
        record = safe_record(
            {
                "target_date": "2026-05-12",
                "slot_number": "837",
                "machine_name": "ミスタージャグラー",
                "difference_value": -384,
                "games_count": 8474,
                "bb_count": 28,
                "rb_count": 33,
            }
        )

        self.assertIsNotNone(record)
        self.assertAlmostEqual(record["estimated_grape_denominator"], 6.4401)
        self.assertEqual(record["estimated_grape_status"], "confirmed")
        self.assertEqual(record["estimated_grape_source"], "minrepo")
        self.assertEqual(record["estimated_grape_version"], ESTIMATED_GRAPE_VALUE_VERSION)
        self.assertAlmostEqual(record["setting_estimate_grape_average"], 2.9170253352700013)
        self.assertEqual(record["setting_estimate_grape_status"], "confirmed")
        self.assertEqual(record["setting_estimate_grape_source"], "minrepo")
        self.assertEqual(record["setting_estimate_grape_version"], SETTING_ESTIMATE_GRAPE_VALUE_VERSION)

    def test_local_snapshot_export_uses_snapshot_saved_at_for_site7_records(self) -> None:
        with TemporaryDirectory() as temp_dir:
            store_dir = Path(temp_dir) / "Aパーク春日店"
            store_dir.mkdir(parents=True)
            saved_at = "2026-05-17T12:34:56+09:00"
            (store_dir / "snapshot.json").write_text(
                json.dumps(
                    {
                        "saved_at": saved_at,
                        "store": {
                            "store_name": "Aパーク春日店",
                            "store_url": "https://example.com/store/",
                        },
                        "records": [
                            {
                                "target_date": "2026-05-12",
                                "slot_number": "821",
                                "machine_name": SITE7_TARGET_MACHINE_NAME,
                                "data_source": DATA_SOURCE_SITE7,
                                "difference_value": 100,
                                "games_count": 2000,
                                "bb_count": 8,
                                "rb_count": 6,
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            _, records = collect_store_records_from_local_store_dir(store_dir)

            self.assertEqual(records[0]["site7_fetched_at"], saved_at)

    def test_web_export_store_payload_filters_blank_site7_rows(self) -> None:
        store_source = StoreSource(
            store_name="Aパーク春日店",
            store_url="https://min-repo.com/tag/a-%E3%83%91%E3%83%BC%E3%82%AF%E6%98%A5%E6%97%A5%E5%BA%97/",
        )
        blank_record = {
            "target_date": "2026-05-11",
            "slot_number": "821",
            "machine_name": SITE7_TARGET_MACHINE_NAME,
            "data_source": DATA_SOURCE_SITE7,
            "difference_value": None,
            "bonus_difference_value": None,
            "games_count": None,
            "payout_rate": None,
            "bb_count": None,
            "rb_count": None,
            "combined_ratio_text": "-",
            "bb_ratio_text": "-",
            "rb_ratio_text": "-",
        }
        valid_record = {
            "target_date": "2026-05-12",
            "slot_number": "821",
            "machine_name": SITE7_TARGET_MACHINE_NAME,
            "data_source": DATA_SOURCE_SITE7,
            "difference_value": 100,
            "bonus_difference_value": 80,
            "games_count": 2000,
            "payout_rate": None,
            "bb_count": 8,
            "rb_count": 6,
            "combined_ratio_text": "1/142",
            "bb_ratio_text": "1/250",
            "rb_ratio_text": "1/333",
        }

        payload = build_store_payload(store_source, [blank_record, valid_record])
        machine_payloads = payload.get("_machineRecordsByFile", {})
        machine_records = next(iter(machine_payloads.values()))["records"]

        self.assertEqual(payload["summary"]["recordCount"], 1)
        self.assertEqual([record["target_date"] for record in machine_records], ["2026-05-12"])

    def test_web_export_store_payload_adds_grape_values_for_incoming_minrepo_records(self) -> None:
        store_source = StoreSource(
            store_name="Aパーク春日店",
            store_url="https://min-repo.com/tag/a-%E3%83%91%E3%83%BC%E3%82%AF%E6%98%A5%E6%97%A5%E5%BA%97/",
        )
        payload = build_store_payload(
            store_source,
            [
                {
                    "target_date": "2026-06-01",
                    "slot_number": "1095",
                    "machine_name": "マイジャグラーV",
                    "difference_value": 2041,
                    "games_count": 4808,
                    "bb_count": 25,
                    "rb_count": 18,
                }
            ],
        )
        machine_payloads = payload.get("_machineRecordsByFile", {})
        machine_records = next(iter(machine_payloads.values()))["records"]

        self.assertEqual(payload["summary"]["recordCount"], 1)
        self.assertAlmostEqual(machine_records[0]["estimated_grape_denominator"], 5.94)
        self.assertEqual(machine_records[0]["estimated_grape_status"], "confirmed")
        self.assertEqual(machine_records[0]["estimated_grape_source"], "minrepo")

    def test_web_export_store_payload_adds_grape_values_for_site7_graph_records(self) -> None:
        store_source = StoreSource(
            store_name="Aパーク春日店",
            store_url="https://min-repo.com/tag/a-%E3%83%91%E3%83%BC%E3%82%AF%E6%98%A5%E6%97%A5%E5%BA%97/",
        )
        payload = build_store_payload(
            store_source,
            [
                {
                    "target_date": "2026-06-02",
                    "slot_number": "821",
                    "machine_name": SITE7_TARGET_MACHINE_NAME,
                    "data_source": DATA_SOURCE_SITE7,
                    "site7_difference_source": "graph",
                    "difference_value": 100,
                    "games_count": 2000,
                    "bb_count": 8,
                    "rb_count": 6,
                }
            ],
        )
        machine_payloads = payload.get("_machineRecordsByFile", {})
        machine_records = next(iter(machine_payloads.values()))["records"]

        self.assertEqual(payload["summary"]["recordCount"], 1)
        self.assertAlmostEqual(machine_records[0]["estimated_grape_denominator"], 6.2269)
        self.assertEqual(machine_records[0]["estimated_grape_status"], "provisional")
        self.assertEqual(machine_records[0]["estimated_grape_source"], "site7")

    def test_site7_build_machine_daily_records_keeps_site7_source_after_store_rewrite(self) -> None:
        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = find_gui_fixture("site7_machine.html")
        history_result = scraper.parse_machine_history_html(
            html,
            store_url="https://www.d-deltanet.com/pc/HallSelectLink.do?hallcode=example",
            page_url="https://www.d-deltanet.com/pc/BonusList.do?model=example",
            recent_days=1,
        )
        rewritten_result = rewrite_history_result_store(
            history_result,
            store_name="Aパーク春日店",
            store_url="https://min-repo.com/tag/a-%E3%83%91%E3%83%BC%E3%82%AF%E6%98%A5%E6%97%A5%E5%BA%97/",
        )

        records = build_machine_daily_records(rewritten_result)

        self.assertTrue(records)
        self.assertEqual({record["data_source"] for record in records}, {DATA_SOURCE_SITE7})

        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
            snapshot = service._build_local_snapshot(rewritten_result)  # type: ignore[attr-defined]

        self.assertTrue(snapshot["records"])
        self.assertTrue(
            all(record["site7_fetched_at"] == snapshot["saved_at"] for record in snapshot["records"])
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
        self.assertIsNone(payload["bonus_difference_value"])
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
                    "bonus_difference_value": None,
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
                    "bonus_difference_value": None,
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

    def test_save_to_supabase_is_disabled(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))

            with self.assertRaisesRegex(RuntimeError, "無効"):
                service._save_to_supabase({"store": {}, "records": []})  # type: ignore[attr-defined]

    def test_save_history_result_writes_r2_web_data(self) -> None:
        scraper = FixtureScraper()
        history_result = scraper.fetch_machine_history_datasets(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-07 ～ 2026-04-08",
            machine_names=["ネオアイムジャグラーEX"],
        )

        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))

            summary = service.save_history_result(history_result)

            self.assertFalse(summary.has_errors)
            self.assertFalse(summary.supabase_saved)
            self.assertEqual(summary.local_record_count, 80)
            self.assertIsNone(summary.local_file_path)
            self.assertTrue(summary.web_data_saved)
            self.assertIn("index.json", storage.objects)

    def test_minrepo_web_data_save_reads_only_target_machine_files(self) -> None:
        store_name = "テスト店"
        store_url = "https://min-repo.com/tag/test-store/"
        target_machine_name = "ネオアイムジャグラーEX"
        untouched_machine_name = "マイジャグラーV"

        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            seed_r2_store(
                storage,
                store_name=store_name,
                store_url=store_url,
                records=[
                    {
                        "target_date": "2026-06-01",
                        "slot_number": "101",
                        "machine_name": target_machine_name,
                        "data_source": DATA_SOURCE_MINREPO,
                        "difference_value": 10,
                        "games_count": 1000,
                        "payout_rate": 101.0,
                        "bb_count": 4,
                        "rb_count": 3,
                    },
                    {
                        "target_date": "2026-06-01",
                        "slot_number": "201",
                        "machine_name": untouched_machine_name,
                        "data_source": DATA_SOURCE_MINREPO,
                        "difference_value": 20,
                        "games_count": 2000,
                        "payout_rate": 102.0,
                        "bb_count": 8,
                        "rb_count": 5,
                    },
                ],
            )
            store_entry = storage.read_json("index.json")["stores"][0]  # type: ignore[index]
            store_payload = storage.read_json(store_entry["dataFile"])  # type: ignore[index]
            machine_files = {
                machine["machineName"]: machine["dataFile"]
                for machine in store_payload["machines"]  # type: ignore[index]
            }
            target_machine_file = machine_files[target_machine_name]
            untouched_machine_file = machine_files[untouched_machine_name]
            untouched_before = storage.read_json(untouched_machine_file)
            storage.read_keys.clear()

            history_result = MachineHistoryResult(
                store_name=store_name,
                store_url=store_url,
                start_date="2026-06-01",
                end_date="2026-06-01",
                date_pages=[
                    StoreDatePage(
                        target_date="2026-06-01",
                        date_url=f"{store_url}2026-06-01/",
                    )
                ],
                datasets=[
                    MachineDataset(
                        store_name=store_name,
                        store_url=store_url,
                        target_date="2026-06-01",
                        date_url=f"{store_url}2026-06-01/",
                        machine_name=target_machine_name,
                        machine_url=f"{store_url}?kishu=aim",
                        columns=["台番", "差枚", "G数", "BB", "RB", "出率"],
                        rows=[["101", "100", "1200", "5", "4", "103.0"]],
                    )
                ],
            )

            summary = service.save_history_result(history_result, full_day=True)
            save_read_keys = list(storage.read_keys)
            target_after = storage.read_json(target_machine_file)
            untouched_after = storage.read_json(untouched_machine_file)

            self.assertFalse(summary.has_errors)
            self.assertTrue(summary.web_data_saved)
            self.assertIn(target_machine_file, save_read_keys)
            self.assertNotIn(untouched_machine_file, save_read_keys)
            self.assertEqual(untouched_after, untouched_before)
            self.assertEqual(target_after["records"][0]["difference_value"], 100)  # type: ignore[index]

    def test_minrepo_web_data_save_backfills_thin_machine_file_from_full_day_snapshots(self) -> None:
        store_name = "テスト店"
        store_url = "https://min-repo.com/tag/test-store/"
        target_machine_name = "ハナハナホウオウ"

        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            seed_r2_store(
                storage,
                store_name=store_name,
                store_url=store_url,
                records=[
                    {
                        "target_date": "2026-06-06",
                        "slot_number": "1165",
                        "machine_name": target_machine_name,
                        "data_source": DATA_SOURCE_MINREPO,
                        "difference_value": 10,
                        "games_count": 1000,
                        "payout_rate": 101.0,
                    },
                ],
            )
            store_entry = storage.read_json("index.json")["stores"][0]  # type: ignore[index]
            store_payload = storage.read_json(store_entry["dataFile"])  # type: ignore[index]
            machine_file = store_payload["machines"][0]["dataFile"]  # type: ignore[index]
            full_day_index_key = service._r2_full_day_index_key(store_name, store_url)  # type: ignore[attr-defined]
            storage.write_json(
                full_day_index_key,
                {
                    "version": 1,
                    "store": {"store_name": store_name, "store_url": store_url},
                    "full_day_dates": {
                        "2026-05-01": {
                            "saved_at": "2026-05-02T00:00:00+09:00",
                            "snapshot_key": "snapshots/test-store/2026-05-01.json",
                        }
                    },
                },
            )
            storage.write_json(
                "snapshots/test-store/2026-05-01.json",
                {
                    "records": [
                        {
                            "target_date": "2026-05-01",
                            "slot_number": "1165",
                            "machine_name": target_machine_name,
                            "data_source": DATA_SOURCE_MINREPO,
                            "difference_value": 500,
                            "games_count": 3000,
                            "payout_rate": 105.0,
                        },
                        {
                            "target_date": "2026-05-01",
                            "slot_number": "9999",
                            "machine_name": "別機種",
                            "data_source": DATA_SOURCE_MINREPO,
                            "difference_value": 999,
                            "games_count": 9999,
                            "payout_rate": 110.0,
                        },
                        {
                            "target_date": "2026-05-01",
                            "slot_number": "1175",
                            "machine_name": target_machine_name,
                            "data_source": DATA_SOURCE_MINREPO,
                            "difference_value": 888,
                            "games_count": 8888,
                            "payout_rate": 109.0,
                        },
                    ]
                },
            )

            service._save_r2_web_data(  # type: ignore[attr-defined]
                {
                    "store": {"store_name": store_name, "store_url": store_url},
                    "records": [
                        {
                            "target_date": "2026-06-07",
                            "slot_number": "1165",
                            "machine_name": target_machine_name,
                            "data_source": DATA_SOURCE_MINREPO,
                            "difference_value": 700,
                            "games_count": 4000,
                            "payout_rate": 106.0,
                        }
                    ],
                }
            )
            machine_payload = storage.read_json(machine_file)
            records = machine_payload["records"]  # type: ignore[index]
            records_by_date = {
                str(record["target_date"]): record  # type: ignore[index]
                for record in records  # type: ignore[union-attr]
            }

            self.assertEqual(set(records_by_date), {"2026-05-01", "2026-06-06", "2026-06-07"})
            self.assertEqual(records_by_date["2026-05-01"]["difference_value"], 500)
            self.assertNotIn("9999", {str(record["slot_number"]) for record in records})  # type: ignore[union-attr]
            self.assertNotIn("1175", {str(record["slot_number"]) for record in records})  # type: ignore[union-attr]

    def test_minrepo_web_data_save_merges_equivalent_machine_files(self) -> None:
        store_name = "テスト店"
        store_url = "https://min-repo.com/tag/test-store/"
        canonical_name = "スマスロ北斗の拳 転生の章2"
        alias_name = "スマスロ北斗の拳 転生の章"

        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            seed_r2_store(
                storage,
                store_name=store_name,
                store_url=store_url,
                records=[
                    {
                        "target_date": "2026-06-06",
                        "slot_number": "1100",
                        "machine_name": canonical_name,
                        "data_source": DATA_SOURCE_MINREPO,
                        "difference_value": 100,
                        "games_count": 1000,
                        "payout_rate": 103.0,
                    },
                ],
            )
            store_id = build_store_id(store_name, normalize_store_url(store_url))
            alias_file = build_machine_data_file(store_id, alias_name)
            storage.write_json(
                alias_file,
                {
                    "version": 1,
                    "store": {"storeName": store_name, "storeUrl": store_url},
                    "machineName": alias_name,
                    "records": [
                        {
                            "target_date": "2026-05-01",
                            "slot_number": "1100",
                            "machine_name": alias_name,
                            "data_source": DATA_SOURCE_MINREPO,
                            "difference_value": 500,
                            "games_count": 3000,
                            "payout_rate": 105.0,
                        }
                    ],
                },
            )

            service._save_r2_web_data(  # type: ignore[attr-defined]
                {
                    "store": {"store_name": store_name, "store_url": store_url},
                    "records": [
                        {
                            "target_date": "2026-06-07",
                            "slot_number": "1100",
                            "machine_name": canonical_name,
                            "data_source": DATA_SOURCE_MINREPO,
                            "difference_value": 700,
                            "games_count": 4000,
                            "payout_rate": 106.0,
                        }
                    ],
                }
            )

            store_entry = storage.read_json("index.json")["stores"][0]  # type: ignore[index]
            store_payload = storage.read_json(store_entry["dataFile"])  # type: ignore[index]
            machine_file = store_payload["machines"][0]["dataFile"]  # type: ignore[index]
            machine_payload = storage.read_json(machine_file)
            records = machine_payload["records"]  # type: ignore[index]
            records_by_date = {
                str(record["target_date"]): record  # type: ignore[index]
                for record in records  # type: ignore[union-attr]
            }

            self.assertEqual(set(records_by_date), {"2026-05-01", "2026-06-06", "2026-06-07"})
            self.assertEqual(records_by_date["2026-05-01"]["machine_name"], canonical_name)
            self.assertEqual(records_by_date["2026-05-01"]["difference_value"], 500)

    def test_save_history_result_does_not_recreate_missing_r2_index_from_single_store(self) -> None:
        scraper = FixtureScraper()
        history_result = scraper.fetch_machine_history_datasets(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-07 ～ 2026-04-08",
            machine_names=["ネオアイムジャグラーEX"],
        )

        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            storage.delete_object("index.json")

            summary = service.save_history_result(history_result)

            self.assertTrue(summary.has_errors)
            self.assertFalse(summary.web_data_saved)
            self.assertNotIn("index.json", storage.objects)

    def test_save_history_result_local_checkpoint_writes_local_snapshot(self) -> None:
        scraper = FixtureScraper()
        history_result = scraper.fetch_machine_history_datasets(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-07 ～ 2026-04-08",
            machine_names=["ネオアイムジャグラーEX"],
        )

        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))

            summary = service.save_history_result_local_checkpoint(history_result)

            self.assertFalse(summary.has_errors)
            self.assertEqual(summary.local_record_count, 80)
            self.assertIsNotNone(summary.local_file_path)
            self.assertTrue(Path(str(summary.local_file_path)).exists())
            self.assertFalse(summary.web_data_saved)

    def test_delete_local_checkpoint_files_removes_saved_snapshot_only(self) -> None:
        scraper = FixtureScraper()
        history_result = scraper.fetch_machine_history_datasets(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-07 ～ 2026-04-08",
            machine_names=["ネオアイムジャグラーEX"],
        )

        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
            checkpoint_summary = service.save_history_result_local_checkpoint(history_result)
            checkpoint_path = Path(str(checkpoint_summary.local_file_path))
            settings_path = Path(temp_dir) / "local_data" / "gui_settings.json"
            settings_path.write_text("{}", encoding="utf-8")

            delete_summary = service.delete_local_checkpoint_files(
                [str(checkpoint_path), str(settings_path)]
            )

            self.assertFalse(checkpoint_path.exists())
            self.assertTrue(settings_path.exists())
            self.assertEqual(delete_summary.local_record_count, 1)
            self.assertTrue(delete_summary.has_errors)

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
            service, _ = make_r2_service(Path(temp_dir))

            summary = service.save_history_result(history_result, full_day=True)
            saved_dates_summary = service.find_saved_full_day_dates(
                store_name="MJアリーナ箱崎店",
                store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
                start_date="2026-04-07",
                end_date="2026-04-08",
            )

            self.assertFalse(summary.has_errors)
            self.assertEqual(saved_dates_summary.saved_dates, {"2026-04-07"})

    def test_save_history_result_clears_full_day_index_when_site7_remains(self) -> None:
        store_name = "テスト店"
        store_url = "https://min-repo.com/tag/test-store/"
        history_result = MachineHistoryResult(
            store_name=store_name,
            store_url=store_url,
            start_date="2026-04-22",
            end_date="2026-04-22",
            date_pages=[
                StoreDatePage(
                    target_date="2026-04-22",
                    date_url="https://min-repo.com/tag/test-store/2026-04-22/",
                )
            ],
            datasets=[
                MachineDataset(
                    store_name=store_name,
                    store_url=store_url,
                    target_date="2026-04-22",
                    date_url="https://min-repo.com/tag/test-store/2026-04-22/",
                    machine_name="ネオアイムジャグラーEX",
                    machine_url="https://example.com/site7/machine",
                    columns=["台番", "差枚", "G数", "BB", "RB", "出率"],
                    rows=[["1001", "120", "3000", "10", "8", ""]],
                )
            ],
        )

        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            storage.write_json(
                service._r2_full_day_index_key(store_name, store_url),  # type: ignore[attr-defined]
                {
                    "version": 1,
                    "store": {"store_name": store_name, "store_url": store_url},
                    "full_day_dates": {
                        "2026-04-22": {
                            "saved_at": "2026-04-22T12:00:00+09:00",
                            "machine_count": 1,
                            "record_count": 1,
                            "snapshot_key": "dummy.json",
                        }
                    },
                },
            )

            summary = service.save_history_result(history_result)
            saved_dates_summary = service.find_saved_full_day_dates(
                store_name=store_name,
                store_url=store_url,
                start_date="2026-04-22",
                end_date="2026-04-22",
            )

            self.assertFalse(summary.has_errors)
            self.assertEqual(saved_dates_summary.saved_dates, set())

    def test_mark_full_day_saved_can_run_after_partial_saves(self) -> None:
        scraper = FixtureScraper()
        context = scraper.prepare_machine_history_context(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-07 ～ 2026-04-08",
        )
        partial_results: list[MachineHistoryResult] = []
        history_result = scraper.fetch_all_machine_history_for_date_page(
            context=context,
            date_page=context.date_pages[0],
            dataset_callback=partial_results.append,
        )

        with TemporaryDirectory() as temp_dir:
            service, _ = make_r2_service(Path(temp_dir))
            for partial_result in partial_results:
                summary = service.save_history_result(partial_result)
                self.assertFalse(summary.has_errors)

            mark_summary = service.mark_full_day_saved(history_result)
            saved_dates_summary = service.find_saved_full_day_dates(
                store_name="MJアリーナ箱崎店",
                store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
                start_date="2026-04-07",
                end_date="2026-04-08",
            )

            self.assertFalse(mark_summary.has_errors)
            self.assertEqual(saved_dates_summary.saved_dates, {"2026-04-07"})

    def test_mark_full_day_saved_does_not_mark_when_r2_records_are_missing(self) -> None:
        scraper = FixtureScraper()
        context = scraper.prepare_machine_history_context(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-07 ～ 2026-04-08",
        )
        partial_results: list[MachineHistoryResult] = []
        history_result = scraper.fetch_all_machine_history_for_date_page(
            context=context,
            date_page=context.date_pages[0],
            dataset_callback=partial_results.append,
        )

        with TemporaryDirectory() as temp_dir:
            service, _ = make_r2_service(Path(temp_dir))
            summary = service.save_history_result(partial_results[0])
            self.assertFalse(summary.has_errors)

            mark_summary = service.mark_full_day_saved(history_result)
            saved_dates_summary = service.find_saved_full_day_dates(
                store_name="MJアリーナ箱崎店",
                store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
                start_date="2026-04-07",
                end_date="2026-04-08",
            )

            self.assertFalse(mark_summary.has_errors)
            self.assertEqual(saved_dates_summary.saved_dates, set())

    def test_save_history_result_does_not_mark_full_day_index_when_r2_save_fails(self) -> None:
        scraper = FixtureScraper()
        context = scraper.prepare_machine_history_context(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-07 ～ 2026-04-08",
        )
        history_result = scraper.fetch_all_machine_history_for_date_page(
            context=context,
            date_page=context.date_pages[0],
        )

        def fail_to_save(snapshot: dict[str, object]) -> str:
            raise RuntimeError("保存失敗")

        with TemporaryDirectory() as temp_dir:
            service, _ = make_r2_service(Path(temp_dir))
            service._save_r2_snapshot = fail_to_save  # type: ignore[method-assign]

            summary = service.save_history_result(history_result, full_day=True)
            saved_dates_summary = service.find_saved_full_day_dates(
                store_name="MJアリーナ箱崎店",
                store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
                start_date="2026-04-07",
                end_date="2026-04-08",
            )

            self.assertTrue(summary.has_errors)
            self.assertEqual(saved_dates_summary.saved_dates, set())

    def test_find_saved_full_day_dates_uses_r2_index_only(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            storage.write_json(
                service._r2_full_day_index_key("テスト店", "https://example.com/store/"),  # type: ignore[attr-defined]
                {
                    "version": 1,
                    "store": {
                        "store_name": "テスト店",
                        "store_url": "https://example.com/store/",
                    },
                    "full_day_dates": {
                        "2026-04-26": {
                            "saved_at": "2026-04-26T12:00:00+09:00",
                            "machine_count": 2,
                            "record_count": 20,
                            "snapshot_key": "dummy-1.json",
                            "data_source": DATA_SOURCE_MINREPO,
                        },
                        "2026-04-27": {
                            "saved_at": "2026-04-27T12:00:00+09:00",
                            "machine_count": 2,
                            "record_count": 20,
                            "snapshot_key": "dummy-2.json",
                            "data_source": DATA_SOURCE_MINREPO,
                        },
                    },
                },
            )

            saved_dates_summary = service.find_saved_full_day_dates(
                store_name="テスト店",
                store_url="https://example.com/store/",
                start_date="2026-04-26",
                end_date="2026-04-27",
            )

            self.assertFalse(saved_dates_summary.has_errors)
            self.assertEqual(saved_dates_summary.saved_dates, {"2026-04-26", "2026-04-27"})

    def test_find_saved_full_day_dates_rechecks_low_saved_counts(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            storage.write_json(
                service._r2_full_day_index_key("テスト店", "https://example.com/store/"),  # type: ignore[attr-defined]
                {
                    "version": 1,
                    "store": {
                        "store_name": "テスト店",
                        "store_url": "https://example.com/store/",
                    },
                    "full_day_dates": {
                        "2026-05-01": {
                            "saved_at": "2026-05-01T12:00:00+09:00",
                            "machine_count": 41,
                            "record_count": 290,
                            "snapshot_key": "dummy-1.json",
                            "data_source": DATA_SOURCE_MINREPO,
                        },
                        "2026-05-02": {
                            "saved_at": "2026-05-02T12:00:00+09:00",
                            "machine_count": 13,
                            "record_count": 131,
                            "snapshot_key": "dummy-2.json",
                            "data_source": DATA_SOURCE_MINREPO,
                        },
                        "2026-05-03": {
                            "saved_at": "2026-05-03T12:00:00+09:00",
                            "machine_count": 41,
                            "record_count": 290,
                            "snapshot_key": "dummy-3.json",
                            "data_source": DATA_SOURCE_MINREPO,
                        },
                    },
                },
            )

            saved_dates_summary = service.find_saved_full_day_dates(
                store_name="テスト店",
                store_url="https://example.com/store/",
                start_date="2026-05-02",
                end_date="2026-05-02",
            )

            self.assertFalse(saved_dates_summary.has_errors)
            self.assertEqual(saved_dates_summary.saved_dates, set())
            self.assertEqual(saved_dates_summary.incomplete_dates, {"2026-05-02"})

    def test_find_saved_full_day_dates_ignores_high_saved_count_outlier(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            storage.write_json(
                service._r2_full_day_index_key("テスト店", "https://example.com/store/"),  # type: ignore[attr-defined]
                {
                    "version": 1,
                    "store": {
                        "store_name": "テスト店",
                        "store_url": "https://example.com/store/",
                    },
                    "full_day_dates": {
                        "2026-05-01": {
                            "saved_at": "2026-05-01T12:00:00+09:00",
                            "machine_count": 80,
                            "record_count": 580,
                            "snapshot_key": "dummy-1.json",
                            "data_source": DATA_SOURCE_MINREPO,
                        },
                        "2026-05-02": {
                            "saved_at": "2026-05-02T12:00:00+09:00",
                            "machine_count": 41,
                            "record_count": 290,
                            "snapshot_key": "dummy-2.json",
                            "data_source": DATA_SOURCE_MINREPO,
                        },
                        "2026-05-03": {
                            "saved_at": "2026-05-03T12:00:00+09:00",
                            "machine_count": 41,
                            "record_count": 290,
                            "snapshot_key": "dummy-3.json",
                            "data_source": DATA_SOURCE_MINREPO,
                        },
                    },
                },
            )

            saved_dates_summary = service.find_saved_full_day_dates(
                store_name="テスト店",
                store_url="https://example.com/store/",
                start_date="2026-05-02",
                end_date="2026-05-02",
            )

            self.assertFalse(saved_dates_summary.has_errors)
            self.assertEqual(saved_dates_summary.saved_dates, {"2026-05-02"})
            self.assertEqual(saved_dates_summary.incomplete_dates, set())

    def test_find_saved_full_day_dates_keeps_legacy_source_missing_entry(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            storage.write_json(
                service._r2_full_day_index_key("テスト店", "https://example.com/store/"),  # type: ignore[attr-defined]
                {
                    "version": 1,
                    "store": {
                        "store_name": "テスト店",
                        "store_url": "https://example.com/store/",
                    },
                    "full_day_dates": {
                        "2026-06-03": {
                            "saved_at": "2026-06-08T13:51:11+09:00",
                            "machine_count": 31,
                            "record_count": 217,
                            "snapshot_key": "",
                        },
                    },
                },
            )

            saved_dates_summary = service.find_saved_full_day_dates(
                store_name="テスト店",
                store_url="https://example.com/store/",
                start_date="2026-06-03",
                end_date="2026-06-03",
            )

            self.assertFalse(saved_dates_summary.has_errors)
            self.assertEqual(saved_dates_summary.saved_dates, {"2026-06-03"})
            self.assertEqual(saved_dates_summary.incomplete_dates, set())

    def test_clear_full_day_saved_dates_with_site7_removes_only_site7_dates(self) -> None:
        store_name = "テスト店"
        store_url = "https://min-repo.com/tag/test-store/"

        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            seed_r2_store(
                storage,
                store_name=store_name,
                store_url=store_url,
                records=[
                    {
                        "target_date": "2026-04-22",
                        "slot_number": "1001",
                        "machine_name": "ネオアイムジャグラーEX",
                        "data_source": DATA_SOURCE_SITE7,
                        "difference_value": 120,
                        "games_count": 3000,
                        "bb_count": 10,
                        "rb_count": 8,
                    },
                    {
                        "target_date": "2026-04-23",
                        "slot_number": "1001",
                        "machine_name": "ネオアイムジャグラーEX",
                        "data_source": DATA_SOURCE_MINREPO,
                        "difference_value": 240,
                        "games_count": 3200,
                        "payout_rate": 103.1,
                        "bb_count": 12,
                        "rb_count": 9,
                    },
                ],
            )
            storage.write_json(
                service._r2_full_day_index_key(store_name, store_url),  # type: ignore[attr-defined]
                {
                    "version": 1,
                    "store": {"store_name": store_name, "store_url": store_url},
                    "full_day_dates": {
                        "2026-04-22": {
                            "saved_at": "2026-04-22T12:00:00+09:00",
                            "machine_count": 1,
                            "record_count": 1,
                            "snapshot_key": "dummy-1.json",
                            "data_source": DATA_SOURCE_MINREPO,
                        },
                        "2026-04-23": {
                            "saved_at": "2026-04-23T12:00:00+09:00",
                            "machine_count": 1,
                            "record_count": 1,
                            "snapshot_key": "dummy-2.json",
                            "data_source": DATA_SOURCE_MINREPO,
                        },
                    },
                },
            )

            cleanup_summary = service.clear_full_day_saved_dates_with_site7()
            saved_dates_summary = service.find_saved_full_day_dates(
                store_name=store_name,
                store_url=store_url,
                start_date="2026-04-22",
                end_date="2026-04-23",
            )

            self.assertFalse(cleanup_summary.has_errors)
            self.assertEqual(cleanup_summary.updated_store_count, 1)
            self.assertEqual(cleanup_summary.removed_date_count, 1)
            self.assertEqual(saved_dates_summary.saved_dates, {"2026-04-23"})

    def test_full_day_index_counts_each_date_separately(self) -> None:
        scraper = FixtureScraper()
        context = scraper.prepare_machine_history_context(
            store_url="https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
            target_date_input="2026-04-07 ～ 2026-04-08",
        )
        day_results = [
            scraper.fetch_all_machine_history_for_date_page(
                context=context,
                date_page=date_page,
            )
            for date_page in context.date_pages
        ]
        history_result = MachineHistoryResult(
            store_name=context.store_name,
            store_url=context.store_url,
            start_date=context.start_date,
            end_date=context.end_date,
            date_pages=context.date_pages,
            datasets=[
                dataset
                for day_result in day_results
                for dataset in day_result.datasets
            ],
        )

        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            snapshot = service._build_local_snapshot(history_result)  # type: ignore[attr-defined]
            expected_counts = service._full_day_saved_counts_by_date(snapshot)  # type: ignore[attr-defined]
            service._save_r2_web_data(snapshot)  # type: ignore[attr-defined]
            service._mark_full_day_saved_r2(snapshot, "dummy.json")  # type: ignore[attr-defined]

            index_payload = storage.read_json(
                service._r2_full_day_index_key(  # type: ignore[attr-defined]
                    "MJアリーナ箱崎店",
                    "https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/",
                )
            )
            self.assertIsNotNone(index_payload)
            full_day_dates = index_payload["full_day_dates"]  # type: ignore[index]

            for day_result in day_results:
                entry = full_day_dates[day_result.start_date]  # type: ignore[index]
                expected_count = expected_counts[day_result.start_date]
                self.assertEqual(entry["record_count"], expected_count["record_count"])
                self.assertEqual(entry["machine_count"], expected_count["machine_count"])

    def test_save_and_load_registered_stores(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))

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
            self.assertTrue(summary.local_saved)
            self.assertEqual(summary.local_store_count, 2)
            self.assertTrue((Path(temp_dir) / "local_data" / "registered_stores.json").exists())
            self.assertEqual(
                loaded_stores,
                [
                    {
                        "store_name": "MJアリーナ箱崎店",
                        "store_url": "https://example.com/a/",
                        "site7_enabled": True,
                        "site7_difference_enabled": False,
                        "site7_prefecture": "福岡県",
                        "site7_area": "東区",
                        "site7_store_name": "ＭＪアリーナ箱崎店",
                        "site7_hall_id": "",
                        "site7_address": "",
                    },
                    {
                        "store_name": "ABCホール",
                        "store_url": "https://example.com/b/",
                        "site7_enabled": False,
                        "site7_difference_enabled": False,
                        "site7_prefecture": DEFAULT_SITE7_PREFECTURE_NAME,
                        "site7_area": "",
                        "site7_store_name": "ABCホール",
                        "site7_hall_id": "",
                        "site7_address": "",
                    },
                ],
            )

    def test_sync_registered_stores_to_web_data_updates_existing_index_location(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root_dir = Path(temp_dir)
            service, storage = make_r2_service(root_dir)
            store_url = "https://min-repo.com/tag/mj%E5%A4%A9%E7%A5%9Eiii/"
            seed_r2_store(
                storage,
                store_name="GOGOアリーナ天神",
                store_url=store_url,
                records=[],
            )

            summary = service.sync_registered_stores_to_web_data(
                [
                    {
                        "store_name": "GOGOアリーナ天神",
                        "store_url": store_url,
                        "site7_prefecture": "福岡県",
                        "site7_area": "福岡市中央区",
                    }
                ]
            )
            index_payload = storage.read_json("index.json")
            stores = index_payload["stores"] if isinstance(index_payload, dict) else []

            self.assertFalse(summary.has_errors)
            self.assertTrue(summary.web_data_saved)
            self.assertEqual(summary.web_data_store_count, 1)
            self.assertEqual(stores[0]["prefectureName"], "福岡県")
            self.assertEqual(stores[0]["areaName"], "福岡市中央区")
            self.assertTrue(stores[0]["dataFile"])

    def test_sync_registered_stores_uses_existing_store_payload_when_index_entry_is_missing(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root_dir = Path(temp_dir)
            service, storage = make_r2_service(root_dir)
            store_name = "Aパーク春日店"
            store_url = "https://min-repo.com/tag/a-%E3%83%91%E3%83%BC%E3%82%AF%E6%98%A5%E6%97%A5%E5%BA%97/"
            seed_r2_store(
                storage,
                store_name=store_name,
                store_url=store_url,
                records=[
                    {
                        "target_date": "2026-05-01",
                        "machine_name": "ネオアイムジャグラーEX",
                        "slot_number": "1",
                        "games_count": 1000,
                    }
                ],
            )
            store_id = build_store_id(store_name, normalize_store_url(store_url))
            data_file = f"stores/{store_id}.json"
            before_payload = storage.read_json(data_file)
            storage.write_json("index.json", {"version": 1, "stores": []})

            summary = service.sync_registered_stores_to_web_data(
                [{"store_name": store_name, "store_url": store_url, "site7_prefecture": "福岡県"}]
            )
            index_payload = storage.read_json("index.json")
            stores = index_payload["stores"] if isinstance(index_payload, dict) else []

            self.assertFalse(summary.has_errors)
            self.assertEqual(stores[0]["dataFile"], data_file)
            self.assertEqual(stores[0]["recordCount"], 1)
            self.assertEqual(storage.read_json(data_file), before_payload)

    def test_sync_registered_stores_does_not_write_empty_store_payload_for_new_store(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root_dir = Path(temp_dir)
            service, storage = make_r2_service(root_dir)
            store_name = "未取得店"
            store_url = "https://example.com/new-store/"

            summary = service.sync_registered_stores_to_web_data(
                [{"store_name": store_name, "store_url": store_url, "site7_prefecture": "福岡県"}]
            )
            index_payload = storage.read_json("index.json")
            stores = index_payload["stores"] if isinstance(index_payload, dict) else []
            store_id = build_store_id(store_name, normalize_store_url(store_url))

            self.assertFalse(summary.has_errors)
            self.assertEqual(stores[0]["id"], store_id)
            self.assertEqual(stores[0]["dataFile"], "")
            self.assertIsNone(storage.read_json(f"stores/{store_id}.json"))

    def test_sync_registered_stores_to_web_data_stops_after_index_read_error(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root_dir = Path(temp_dir)
            registered_stores_path = root_dir / "local_data" / "registered_stores.json"
            registered_stores_path.parent.mkdir(parents=True, exist_ok=True)
            registered_stores_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "stores": [
                            {
                                "store_name": "GOGOアリーナ天神",
                                "store_url": "https://min-repo.com/tag/mj%E5%A4%A9%E7%A5%9Eiii/",
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            storage = FailingIndexReadStorage()
            service = HistoryPersistenceService(root_dir=root_dir, r2_storage=storage)  # type: ignore[arg-type]

            loaded_stores = service.load_registered_stores()
            summary = service.sync_registered_stores_to_web_data(loaded_stores)

            self.assertEqual([store["store_name"] for store in loaded_stores], ["GOGOアリーナ天神"])
            self.assertTrue(summary.has_errors)
            self.assertFalse(summary.web_data_saved)
            self.assertNotIn("index.json", storage.objects)

    def test_load_registered_stores_merges_static_web_entries_when_local_file_is_partial(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root_dir = Path(temp_dir)
            registered_stores_path = root_dir / "local_data" / "registered_stores.json"
            registered_stores_path.parent.mkdir(parents=True, exist_ok=True)
            registered_stores_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "stores": [
                            {
                                "store_name": "GOGOアリーナ天神",
                                "store_url": "https://min-repo.com/tag/mj%E5%A4%A9%E7%A5%9Eiii/",
                                "site7_enabled": False,
                                "site7_area": "",
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            service, storage = make_r2_service(root_dir)
            storage.write_json(
                "index.json",
                {
                    "version": 1,
                    "stores": [
                        {
                            "storeName": "Aパーク春日店",
                            "storeUrl": "https://min-repo.com/tag/a-%E3%83%91%E3%83%BC%E3%82%AF%E6%98%A5%E6%97%A5%E5%BA%97/",
                        },
                        {
                            "storeName": "GOGOアリーナ天神",
                            "storeUrl": "https://min-repo.com/tag/mj%E5%A4%A9%E7%A5%9Eiii/",
                        },
                    ],
                },
            )

            loaded_stores = service.load_registered_stores()

            self.assertEqual([store["store_name"] for store in loaded_stores], ["GOGOアリーナ天神", "Aパーク春日店"])
            self.assertFalse(loaded_stores[0]["site7_enabled"])
            self.assertEqual(loaded_stores[0]["site7_area"], "")
            self.assertTrue(loaded_stores[1]["site7_enabled"])

    def test_delete_registered_stores_keeps_static_fallback_store_hidden(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root_dir = Path(temp_dir)
            service, storage = make_r2_service(root_dir)
            storage.write_json(
                "index.json",
                {
                    "version": 1,
                    "stores": [
                        {
                            "storeName": "GOGOアリーナ天神",
                            "storeUrl": "https://min-repo.com/tag/mj%E5%A4%A9%E7%A5%9Eiii/",
                        },
                        {
                            "storeName": "Aパーク春日店",
                            "storeUrl": "https://example.com/kasuga/",
                        },
                    ],
                },
            )

            deleted_count = service.delete_registered_stores(["https://example.com/kasuga"])
            loaded_stores = service.load_registered_stores()
            registered_payload = json.loads((root_dir / "local_data" / "registered_stores.json").read_text(encoding="utf-8"))

            self.assertEqual(deleted_count, 1)
            self.assertEqual([store["store_name"] for store in loaded_stores], ["GOGOアリーナ天神"])
            self.assertEqual(registered_payload["excluded_store_urls"], ["https://example.com/kasuga/"])

    def test_load_registered_stores_does_not_fall_back_to_local_snapshots(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root_dir = Path(temp_dir)
            store_dir = root_dir / "local_data" / "テスト店"
            store_dir.mkdir(parents=True, exist_ok=True)
            (store_dir / "sample.json").write_text(
                json.dumps(
                    {
                        "store": {
                            "store_name": "テスト店",
                            "store_url": "https://example.com/test/",
                        },
                        "records": [],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            service, _ = make_r2_service(root_dir)
            loaded_stores = service.load_registered_stores()

            self.assertEqual(loaded_stores, [])

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
                        "site7_difference_enabled": False,
                        "site7_prefecture": "福岡県",
                        "site7_area": "春日市",
                        "site7_store_name": "Ａパーク春日店",
                        "site7_hall_id": "",
                        "site7_address": "福岡県春日市日の出町５－２４",
                    }
                ],
            )

    def test_normalize_registered_stores_keeps_beam_hikari_daidata_online_source(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))

            self.assertEqual(
                service._normalize_registered_stores(  # type: ignore[attr-defined]
                    [
                        {
                            "store_name": "ビームヒカリ",
                            "store_url": "https://example.com/beam",
                            "site7_enabled": True,
                            "site7_area": "大野城市",
                        }
                    ]
                ),
                [
                    {
                        "store_name": "ビームヒカリ",
                        "store_url": "https://example.com/beam/",
                        "site7_enabled": True,
                        "site7_difference_enabled": False,
                        "site7_prefecture": "福岡県",
                        "site7_area": "大野城市",
                        "site7_store_name": "ビームヒカリ",
                        "site7_hall_id": "",
                        "site7_address": "",
                    }
                ],
            )

    def test_normalize_registered_stores_fills_blank_site7_fields_for_known_store(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))

            self.assertEqual(
                service._normalize_registered_stores(  # type: ignore[attr-defined]
                    [
                        {
                            "store_name": "HINODE大野城店",
                            "store_url": "https://example.com/hinode",
                            "site7_enabled": True,
                            "site7_area": "",
                            "site7_store_name": "",
                            "site7_hall_id": "",
                            "site7_address": "",
                        }
                    ]
                ),
                [
                    {
                        "store_name": "HINODE大野城店",
                        "store_url": "https://example.com/hinode/",
                        "site7_enabled": True,
                        "site7_difference_enabled": False,
                        "site7_prefecture": "福岡県",
                        "site7_area": "大野城市",
                        "site7_store_name": "HINODE大野城店",
                        "site7_hall_id": "40101001",
                        "site7_address": "福岡県大野城市瓦田４－１２－５",
                    }
                ],
            )

    def test_normalize_registered_stores_keeps_difference_flag_only_with_site7(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))

            normalized_stores = service._normalize_registered_stores(  # type: ignore[attr-defined]
                [
                    {
                        "store_name": "Aパーク春日店",
                        "store_url": "https://example.com/kasuga",
                        "site7_enabled": True,
                        "site7_difference_enabled": True,
                    },
                    {
                        "store_name": "ABCホール",
                        "store_url": "https://example.com/abc",
                        "site7_enabled": False,
                        "site7_difference_enabled": True,
                    },
                ]
            )

            self.assertTrue(normalized_stores[0]["site7_difference_enabled"])
            self.assertFalse(normalized_stores[1]["site7_difference_enabled"])

    def test_normalize_registered_stores_defaults_difference_flag_from_fetch_order(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))

            normalized_stores = service._normalize_registered_stores(  # type: ignore[attr-defined]
                [
                    {
                        "store_name": "Aパーク春日店",
                        "store_url": "https://example.com/kasuga",
                        "site7_enabled": True,
                        "fetch_order": 1,
                    },
                    {
                        "store_name": "123博多店",
                        "store_url": "https://example.com/hakata",
                        "site7_enabled": True,
                    },
                ]
            )

            self.assertTrue(normalized_stores[0]["site7_difference_enabled"])
            self.assertFalse(normalized_stores[1]["site7_difference_enabled"])

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

            summary = service.save_registered_stores(
                [
                    {"store_name": "GOGOアリーナ天神", "store_url": "https://min-repo.com/tag/mj%e5%a4%a9%e7%a5%9eiii/"},
                    {"store_name": "GOGOアリーナ天神", "store_url": "https://min-repo.com/tag/mj%E5%A4%A9%E7%A5%9Eiii/"},
                ]
            )
            loaded_stores = service.load_registered_stores()

            self.assertFalse(summary.has_errors)
            self.assertEqual(summary.local_store_count, 1)
            self.assertEqual(
                loaded_stores,
                [
                    {
                        "store_name": "GOGOアリーナ天神",
                        "store_url": "https://min-repo.com/tag/mj%E5%A4%A9%E7%A5%9Eiii/",
                        "site7_enabled": True,
                        "site7_difference_enabled": False,
                        "site7_prefecture": "福岡県",
                        "site7_area": "福岡市中央区",
                        "site7_store_name": "ＧＯＧＯアリーナ天神",
                        "site7_hall_id": "",
                        "site7_address": "福岡県福岡市中央区天神２－６－３７",
                    }
                ],
            )

    def test_delete_registered_stores_deduplicates_normalized_url(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
            service.save_registered_stores(
                [
                    {"store_name": "GOGOアリーナ天神", "store_url": "https://min-repo.com/tag/mj%E5%A4%A9%E7%A5%9Eiii/"},
                    {"store_name": "Aパーク春日店", "store_url": "https://example.com/kasuga/"},
                ]
            )

            deleted_count = service.delete_registered_stores(
                [
                    "https://min-repo.com/tag/mj%e5%a4%a9%e7%a5%9eiii/",
                    "https://min-repo.com/tag/mj%E5%A4%A9%E7%A5%9Eiii/",
                ]
            )

            self.assertEqual(deleted_count, 1)
            self.assertEqual(
                [store["store_name"] for store in service.load_registered_stores()],
                ["Aパーク春日店"],
            )

    def test_find_saved_machine_targets_uses_r2_store_data(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            seed_r2_store(
                storage,
                store_name="テスト店",
                store_url="https://example.com/store/",
                records=[
                    {
                        "target_date": "2026-04-07",
                        "slot_number": "1",
                        "machine_name": "ゴーゴージャグラー3",
                        "payout_rate": 101.0,
                    },
                    {
                        "target_date": "2026-04-08",
                        "slot_number": "1",
                        "machine_name": "ゴーゴージャグラー３",
                        "payout_rate": 101.0,
                    },
                ],
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

    def test_find_saved_machine_targets_includes_all_target_machines(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            seed_r2_store(
                storage,
                store_name="テスト店",
                store_url="https://example.com/store/",
                records=[
                    {
                        "target_date": "2026-04-07",
                        "slot_number": "1",
                        "machine_name": "ネオアイムジャグラーEX",
                        "payout_rate": 101.0,
                    },
                    {
                        "target_date": "2026-04-08",
                        "slot_number": "1",
                        "machine_name": "SアイムジャグラーＥＸ",
                        "payout_rate": 101.0,
                    },
                ],
            )

            summary = service.find_saved_machine_targets(
                store_name="テスト店",
                store_url="https://example.com/store/",
                start_date="2026-04-07",
                end_date="2026-04-08",
                machine_names=["ネオアイムジャグラーEX", "SアイムジャグラーＥＸ"],
            )

            self.assertFalse(summary.has_errors)
            self.assertEqual(
                summary.saved_targets,
                {
                    ("2026-04-07", "ネオアイムジャグラーEX"),
                    ("2026-04-08", "SアイムジャグラーＥＸ"),
                },
            )

    def test_find_saved_machine_targets_supabase_alias_uses_r2_json(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            seed_r2_store(
                storage,
                store_name="テスト店",
                store_url="https://example.com/store/",
                records=[
                    {
                        "target_date": "2026-04-24",
                        "slot_number": "737",
                        "machine_name": "ゴーゴージャグラー３",
                        "data_source": DATA_SOURCE_SITE7,
                        "games_count": 1000,
                        "payout_rate": None,
                    },
                    {
                        "target_date": "2026-04-25",
                        "slot_number": "737",
                        "machine_name": "ゴーゴージャグラー３",
                        "data_source": DATA_SOURCE_MINREPO,
                        "payout_rate": 101.2,
                    },
                ],
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

    def test_find_saved_machine_slots_treats_complete_site7_as_protected(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            seed_r2_store(
                storage,
                store_name="テスト店",
                store_url="https://example.com/store/",
                records=[
                    {
                        "target_date": "2026-04-24",
                        "slot_number": "737",
                        "machine_name": "ゴーゴージャグラー３",
                        "data_source": DATA_SOURCE_SITE7,
                        "difference_value": 120,
                        "bonus_difference_value": 120,
                        "site7_difference_source": "graph",
                        "games_count": 1200,
                        "bb_count": 6,
                        "rb_count": 4,
                    },
                ],
            )

            summary = service.find_saved_machine_slots(
                store_name="テスト店",
                store_url="https://example.com/store/",
                start_date="2026-04-24",
                end_date="2026-04-24",
                slot_numbers=["737"],
            )

            self.assertFalse(summary.has_errors)
            self.assertEqual(summary.protected_slots, {("2026-04-24", "737")})
            self.assertEqual(summary.replaceable_slots, set())

    def test_find_saved_machine_slots_replaces_old_complete_site7_update(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            seed_r2_store(
                storage,
                store_name="テスト店",
                store_url="https://example.com/store/",
                records=[
                    {
                        "target_date": "2026-06-07",
                        "slot_number": "737",
                        "machine_name": "ゴーゴージャグラー３",
                        "data_source": DATA_SOURCE_SITE7,
                        "difference_value": 120,
                        "bonus_difference_value": 120,
                        "site7_difference_source": "graph",
                        "games_count": 1200,
                        "bb_count": 6,
                        "rb_count": 4,
                        "site7_fetched_at": "2026-06-07T14:15:00+09:00",
                    },
                    {
                        "target_date": "2026-06-07",
                        "slot_number": "738",
                        "machine_name": "ゴーゴージャグラー３",
                        "data_source": DATA_SOURCE_SITE7,
                        "difference_value": 80,
                        "bonus_difference_value": 80,
                        "site7_difference_source": "graph",
                        "games_count": 900,
                        "bb_count": 5,
                        "rb_count": 2,
                        "site7_fetched_at": "2026-06-07T15:15:00+09:00",
                    },
                    {
                        "target_date": "2026-06-07",
                        "slot_number": "739",
                        "machine_name": "ゴーゴージャグラー３",
                        "data_source": DATA_SOURCE_SITE7,
                        "difference_value": -40,
                        "bonus_difference_value": -40,
                        "site7_difference_source": "graph",
                        "games_count": 700,
                        "bb_count": 3,
                        "rb_count": 2,
                        "site7_fetched_at": "2026-06-07T16:15:00+09:00",
                    },
                ],
            )

            summary = service.find_saved_machine_slots(
                store_name="テスト店",
                store_url="https://example.com/store/",
                start_date="2026-06-07",
                end_date="2026-06-07",
                slot_numbers=["737", "738", "739"],
                site7_updated_at="2026-06-07T15:15:00+09:00",
            )

            self.assertFalse(summary.has_errors)
            self.assertEqual(summary.protected_slots, {("2026-06-07", "738"), ("2026-06-07", "739")})
            self.assertEqual(summary.replaceable_slots, {("2026-06-07", "737")})

    def test_find_saved_machine_slots_protects_past_site7_after_close(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            seed_r2_store(
                storage,
                store_name="テスト店",
                store_url="https://example.com/store/",
                records=[
                    {
                        "target_date": "2026-06-07",
                        "slot_number": "737",
                        "machine_name": "ゴーゴージャグラー３",
                        "data_source": DATA_SOURCE_SITE7,
                        "games_count": 1200,
                        "bb_count": 6,
                        "rb_count": 4,
                        "site7_fetched_at": "2026-06-07T22:59:00+09:00",
                    },
                    {
                        "target_date": "2026-06-07",
                        "slot_number": "738",
                        "machine_name": "ゴーゴージャグラー３",
                        "data_source": DATA_SOURCE_SITE7,
                        "games_count": 900,
                        "bb_count": 5,
                        "rb_count": 2,
                        "site7_fetched_at": "2026-06-07T23:00:00+09:00",
                    },
                    {
                        "target_date": "2026-06-07",
                        "slot_number": "739",
                        "machine_name": "ゴーゴージャグラー３",
                        "data_source": DATA_SOURCE_SITE7,
                        "games_count": 700,
                        "bb_count": 3,
                        "rb_count": 2,
                        "site7_fetched_at": "2026-06-08T12:00:00+09:00",
                    },
                ],
            )

            summary = service.find_saved_machine_slots(
                store_name="テスト店",
                store_url="https://example.com/store/",
                start_date="2026-06-07",
                end_date="2026-06-07",
                slot_numbers=["737", "738", "739"],
                require_source_difference=False,
                site7_updated_at="2026-06-08T23:15:00+09:00",
            )

            self.assertFalse(summary.has_errors)
            self.assertEqual(summary.protected_slots, {("2026-06-07", "738"), ("2026-06-07", "739")})
            self.assertEqual(summary.replaceable_slots, {("2026-06-07", "737")})

    def test_find_saved_machine_slots_treats_formula_site7_difference_as_replaceable(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            seed_r2_store(
                storage,
                store_name="テスト店",
                store_url="https://example.com/store/",
                records=[
                    {
                        "target_date": "2026-04-24",
                        "slot_number": "737",
                        "machine_name": "ゴーゴージャグラー３",
                        "data_source": DATA_SOURCE_SITE7,
                        "difference_value": 120,
                        "bonus_difference_value": 120,
                        "games_count": 1200,
                        "bb_count": 6,
                        "rb_count": 4,
                    },
                ],
            )

            summary = service.find_saved_machine_slots(
                store_name="テスト店",
                store_url="https://example.com/store/",
                start_date="2026-04-24",
                end_date="2026-04-24",
                slot_numbers=["737"],
            )

            self.assertFalse(summary.has_errors)
            self.assertEqual(summary.protected_slots, set())
            self.assertEqual(summary.replaceable_slots, {("2026-04-24", "737")})

    def test_find_saved_machine_slots_can_protect_site7_base_rows_without_difference_requirement(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            seed_r2_store(
                storage,
                store_name="テスト店",
                store_url="https://example.com/store/",
                records=[
                    {
                        "target_date": "2026-04-24",
                        "slot_number": "737",
                        "machine_name": "ゴーゴージャグラー３",
                        "data_source": DATA_SOURCE_SITE7,
                        "difference_value": None,
                        "bonus_difference_value": 120,
                        "games_count": 1200,
                        "bb_count": 6,
                        "rb_count": 4,
                    },
                ],
            )

            summary = service.find_saved_machine_slots(
                store_name="テスト店",
                store_url="https://example.com/store/",
                start_date="2026-04-24",
                end_date="2026-04-24",
                slot_numbers=["737"],
                require_source_difference=False,
            )

            self.assertFalse(summary.has_errors)
            self.assertEqual(summary.protected_slots, {("2026-04-24", "737")})
            self.assertEqual(summary.replaceable_slots, set())

    def test_find_saved_machine_slots_treats_incomplete_site7_as_replaceable(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service, storage = make_r2_service(Path(temp_dir))
            seed_r2_store(
                storage,
                store_name="テスト店",
                store_url="https://example.com/store/",
                records=[
                    {
                        "target_date": "2026-04-24",
                        "slot_number": "737",
                        "machine_name": "ゴーゴージャグラー３",
                        "data_source": DATA_SOURCE_SITE7,
                        "difference_value": None,
                        "games_count": 1200,
                        "bb_count": 6,
                        "rb_count": 4,
                    },
                ],
            )

            summary = service.find_saved_machine_slots(
                store_name="テスト店",
                store_url="https://example.com/store/",
                start_date="2026-04-24",
                end_date="2026-04-24",
                slot_numbers=["737"],
            )

            self.assertFalse(summary.has_errors)
            self.assertEqual(summary.protected_slots, set())
            self.assertEqual(summary.replaceable_slots, {("2026-04-24", "737")})

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

    def test_prepare_site7_history_result_deletes_replaceable_slots_without_source_filter(self) -> None:
        app = MinRepoApp.__new__(MinRepoApp)
        app.result_queue = queue.Queue()
        deleted_calls: list[dict[str, object]] = []

        class FakePersistenceService:
            def resolve_preferred_store_by_name(self, store_name: str) -> None:
                return None

            def find_saved_machine_slots(
                self,
                store_name: str,
                store_url: str,
                start_date: str,
                end_date: str,
                slot_numbers: list[str],
                require_source_difference: bool = True,
                site7_updated_at: str | datetime | None = None,
            ) -> SavedMachineSlotsSummary:
                self.require_source_difference = require_source_difference
                self.site7_updated_at = site7_updated_at
                return SavedMachineSlotsSummary(
                    protected_slots={("2026-04-24", "821")},
                    replaceable_slots={("2026-04-25", "821")},
                )

        persistence_service = FakePersistenceService()
        app.persistence_service = persistence_service

        scraper = Site7Scraper(root_dir=ROOT_DIR)
        html = find_gui_fixture("site7_machine.html")
        history_result = scraper.parse_machine_history_html(
            html,
            store_url="https://example.com/site7",
            page_url="https://example.com/site7/machine",
            recent_days=2,
        )
        for dataset in history_result.datasets:
            set_site7_dataset_updated_at(dataset, "2026-04-25T15:15:00+09:00")

        filtered_result, warning_summary = app._prepare_site7_history_result_for_save(history_result)

        self.assertEqual(warning_summary.messages, [])
        self.assertEqual(deleted_calls, [])
        self.assertEqual(persistence_service.site7_updated_at, "2026-04-25T15:15:00+09:00")
        self.assertEqual([dataset.target_date for dataset in filtered_result.datasets], ["2026-04-24", "2026-04-25"])
        self.assertEqual(
            [dataset.rows[0][0] for dataset in filtered_result.datasets],
            ["822", "821"],
        )

    def test_find_saved_machine_target_sources_local_prefers_minrepo(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
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
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            protected_targets, replaceable_targets = service._find_saved_machine_target_sources_local(  # type: ignore[attr-defined]
                store_name="テスト店",
                store_url="https://example.com/store",
                start_date="2026-04-24",
                end_date="2026-04-25",
                target_machine_names={normalize_text("ゴーゴージャグラー３")},
            )

            self.assertEqual(protected_targets, {("2026-04-25", normalize_text("ゴーゴージャグラー３"))})
            self.assertEqual(replaceable_targets, {("2026-04-24", normalize_text("ゴーゴージャグラー３"))})

    def test_find_saved_machine_slot_sources_local_prefers_minrepo(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
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
                                "payout_rate": 101.2,
                            },
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            protected_slots, replaceable_slots = service._find_saved_machine_slot_sources_local(  # type: ignore[attr-defined]
                store_name="テスト店",
                store_url="https://example.com/store",
                start_date="2026-04-24",
                end_date="2026-04-25",
                target_slot_numbers={"737"},
            )

            self.assertEqual(protected_slots, {("2026-04-25", "737")})
            self.assertEqual(replaceable_slots, {("2026-04-24", "737")})

    def test_find_saved_machine_slot_sources_local_treats_legacy_empty_minrepo_as_replaceable(self) -> None:
        with TemporaryDirectory() as temp_dir:
            service = HistoryPersistenceService(root_dir=Path(temp_dir))
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
                            {
                                "target_date": "2026-04-25",
                                "slot_number": "863",
                                "data_source": DATA_SOURCE_MINREPO,
                                "payout_rate": None,
                            },
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            protected_slots, replaceable_slots = service._find_saved_machine_slot_sources_local(  # type: ignore[attr-defined]
                store_name="テスト店",
                store_url="https://example.com/store",
                start_date="2026-04-25",
                end_date="2026-04-25",
                target_slot_numbers={"863"},
            )

            self.assertEqual(protected_slots, set())
            self.assertEqual(replaceable_slots, {("2026-04-25", "863")})

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

    def test_find_date_pages_does_not_fallback_far_before_end_date(self) -> None:
        scraper = MinRepoScraper()
        soup = BeautifulSoup(
            """
            <html>
              <body>
                <time class="date">2026年6月21日</time>
                <div class="table_wrap">
                  <table>
                    <tr><td><a href="/old">2025/5/18(日)</a></td></tr>
                  </table>
                </div>
              </body>
            </html>
            """,
            "html.parser",
        )

        with self.assertRaisesRegex(ScraperError, "2026-06-18 ～ 2026-06-21"):
            scraper.find_date_pages_in_range(
                soup=soup,
                base_url="https://example.com/tag/store/",
                target_date_input="2026-03-25 ～ 2026-06-21",
            )


if __name__ == "__main__":
    unittest.main()
