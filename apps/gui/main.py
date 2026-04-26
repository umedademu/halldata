from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import queue
import re
import threading
import tkinter as tk
from tkinter import font as tkfont
from tkinter import messagebox, ttk
from typing import Callable, TypeVar
from urllib.parse import urlparse

try:
    import winsound
except ImportError:  # pragma: no cover
    winsound = None

try:
    import pystray
    from PIL import Image, ImageDraw
except ImportError:  # pragma: no cover
    pystray = None
    Image = None
    ImageDraw = None

from data_persistence import (
    HistoryPersistenceService,
    PersistenceSummary,
    RegisteredStoresPersistenceSummary,
    SavedFullDayDatesSummary,
    normalize_store_url,
)
from minrepo_scraper import (
    FetchProgress,
    MachineDataset,
    MachineHistoryResult,
    MinRepoScraper,
    ScraperError,
    normalize_text,
)
from site7_scraper import (
    DEFAULT_SITE7_PREFECTURE_NAME,
    SITE7_MAX_RECENT_DAYS,
    SITE7_TARGET_MACHINE_KEYWORDS,
    Site7FetchCancelled,
    Site7Scraper,
    Site7TargetStore,
    default_site7_store_settings,
    enrich_site7_target_store,
)


DEFAULT_STORE_NAME = "MJアリーナ箱崎店"
DEFAULT_STORE_URL = "https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/"
DEFAULT_RECENT_DAYS = "90"
DEFAULT_RETRY_DELAY_SECONDS = "10"
MAX_FETCH_RETRY_COUNT = 3
DEFAULT_SCHEDULE_HOUR = 2
GUI_SETTINGS_FILE_NAME = "gui_settings.json"
SITE7_BROWSER_MODE_VISIBLE = "visible"
SITE7_BROWSER_MODE_HIDDEN = "hidden"
JST = timezone(timedelta(hours=9))
REGISTERED_STORE_COLUMNS = ("取得対象", "サイトセブン", "店舗名", "URL", "都道府県", "地域", "SS店舗名")
COMPARISON_SUBCOLUMNS = ("機種名", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率")
COMPARISON_DAY_TAIL_OPTIONS = ("全て", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9")
T = TypeVar("T")


def parse_recent_days(value: str) -> int:
    text = value.strip()
    if not re.fullmatch(r"\d+", text):
        raise ScraperError("直近日数は 1 以上の整数で入力してください。")

    recent_days = int(text)
    if recent_days <= 0:
        raise ScraperError("直近日数は 1 以上の整数で入力してください。")

    return recent_days


def build_recent_date_range_input(value: str, today: datetime | None = None) -> str:
    recent_days = parse_recent_days(value)
    today_date = (today or datetime.now(JST)).astimezone(JST).date()
    start_date = today_date - timedelta(days=recent_days - 1)
    return f"{start_date.strftime('%Y-%m-%d')} ～ {today_date.strftime('%Y-%m-%d')}"


def parse_retry_delay_seconds(value: str) -> int:
    text = value.strip()
    if not re.fullmatch(r"\d+", text):
        raise ScraperError("再試行の休止秒数は 0 以上の整数で入力してください。")

    return int(text)


def matches_day_tail(date_text: str, day_tail: str) -> bool:
    if day_tail == "全て":
        return True

    match = re.fullmatch(r"\d{4}-\d{2}-(\d{2})", date_text.strip())
    if match is None:
        return False

    return match.group(1).endswith(day_tail)


def normalize_site7_browser_mode(value: object) -> str:
    text = str(value).strip().lower()
    if text == SITE7_BROWSER_MODE_HIDDEN:
        return SITE7_BROWSER_MODE_HIDDEN
    return SITE7_BROWSER_MODE_VISIBLE


def current_jst_date_text(now: datetime | None = None) -> str:
    return (now or datetime.now(JST)).astimezone(JST).strftime("%Y-%m-%d")


def rewrite_history_result_store(
    history_result: MachineHistoryResult,
    store_name: str,
    store_url: str,
) -> MachineHistoryResult:
    rewritten_datasets = [
        replace(
            dataset,
            store_name=store_name,
            store_url=store_url,
        )
        for dataset in history_result.datasets
    ]
    return replace(
        history_result,
        store_name=store_name,
        store_url=store_url,
        datasets=rewritten_datasets,
    )


def filter_site7_history_result_by_saved_targets(
    history_result: MachineHistoryResult,
    saved_targets: set[tuple[str, str]],
) -> MachineHistoryResult:
    if not saved_targets:
        return history_result

    filtered_datasets: list[MachineDataset] = []
    skipped_targets = list(history_result.skipped_targets)
    skipped_dates = list(history_result.skipped_dates)
    skipped_target_dates: set[str] = set()

    for dataset in history_result.datasets:
        target_key = (dataset.target_date, normalize_text(dataset.machine_name))
        if target_key in saved_targets:
            skipped_targets.append((dataset.target_date, dataset.machine_name))
            skipped_target_dates.add(dataset.target_date)
            continue
        filtered_datasets.append(dataset)

    remaining_dates = {dataset.target_date for dataset in filtered_datasets}
    filtered_date_pages = [date_page for date_page in history_result.date_pages if date_page.target_date in remaining_dates]
    for skipped_date in sorted(skipped_target_dates - remaining_dates):
        if skipped_date not in skipped_dates:
            skipped_dates.append(skipped_date)

    return replace(
        history_result,
        date_pages=filtered_date_pages,
        datasets=filtered_datasets,
        skipped_targets=skipped_targets,
        skipped_dates=skipped_dates,
    )


@dataclass
class RegisteredStore:
    name: str
    url: str
    site7_enabled: bool = False
    site7_prefecture: str = DEFAULT_SITE7_PREFECTURE_NAME
    site7_area: str = ""
    site7_store_name: str = ""

    def resolved_site7_store_name(self) -> str:
        return self.site7_store_name.strip() or self.name.strip()

    def to_site7_target_store(self) -> Site7TargetStore:
        return enrich_site7_target_store(
            Site7TargetStore(
            display_name=self.name.strip() or self.resolved_site7_store_name(),
            site7_hall_name=self.resolved_site7_store_name(),
            prefecture_name=self.site7_prefecture.strip() or DEFAULT_SITE7_PREFECTURE_NAME,
            area_name=self.site7_area.strip(),
            hall_name_aliases=(self.name.strip(),) if self.name.strip() else (),
            )
        )


class FetchCancelled(Exception):
    pass


@dataclass
class StoreFetchResult:
    history_result: MachineHistoryResult
    save_summary: PersistenceSummary | None
    saved_full_day_summary: SavedFullDayDatesSummary


@dataclass
class StoreFetchFailure:
    store: RegisteredStore
    error: Exception


@dataclass
class FetchManyResult:
    results: list[StoreFetchResult]
    failures: list[StoreFetchFailure]
    cancelled: bool = False


@dataclass
class StoreRefreshResult:
    registered_stores: list[RegisteredStore]
    save_summary: RegisteredStoresPersistenceSummary | None = None


@dataclass
class StoreDeleteResult:
    registered_stores: list[RegisteredStore]
    deleted_store_count: int


class MinRepoApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Halldata Prototype")
        self.root.geometry("1320x900")
        self.default_font = tkfont.nametofont("TkDefaultFont")

        self.scraper = MinRepoScraper()
        self.persistence_service = HistoryPersistenceService()
        self.site7_scraper = Site7Scraper(self.persistence_service.root_dir)
        self.result_queue: queue.Queue[tuple[str, object]] = queue.Queue()
        self.current_results: list[MachineDataset] = []
        self.current_history_result: MachineHistoryResult | None = None
        self.comparison_sort_key = "日付"
        self.comparison_sort_descending = False
        self.comparison_slot_numbers: list[str] = []
        self.comparison_rows: list[dict[str, str]] = []
        self.comparison_display_rows: list[dict[str, str]] = []
        self.comparison_selected_date: str | None = None
        self.comparison_focus_mode = False
        self.comparison_header_click_regions: list[tuple[int, int, int, int, str]] = []
        self.comparison_text_cache: dict[tuple[str, int], str] = {}
        self.startup_store_warning: str | None = None
        self.registered_stores: list[RegisteredStore] = self._load_registered_stores_on_startup()
        self.selected_store_urls: set[str] = {
            normalize_store_url(registered_store.url)
            for registered_store in self.registered_stores
        }
        self.is_busy = False
        self.active_operation_kind = ""
        self.fetch_cancel_event = threading.Event()
        self.scheduled_fetch_hour: int | None = self._load_saved_schedule_hour()
        self.site7_browser_mode: str = self._load_saved_site7_browser_mode()
        self.scheduled_last_run_date: str | None = None
        self.scheduled_pending_date: str | None = None
        self.tray_icon: object | None = None
        self.tray_thread: threading.Thread | None = None

        self.target_date_var = tk.StringVar(value=DEFAULT_RECENT_DAYS)
        self.schedule_hour_var = tk.StringVar(value=str(self.scheduled_fetch_hour))
        self.schedule_status_var = tk.StringVar(value=f"毎日 {self.scheduled_fetch_hour} 時に実行")
        self.retry_delay_seconds_var = tk.StringVar(value=DEFAULT_RETRY_DELAY_SECONDS)
        self.status_var = tk.StringVar(value="待機中")
        self.summary_var = tk.StringVar(value="未取得")
        self.fetch_progress_value_var = tk.DoubleVar(value=0.0)
        self.fetch_progress_text_var = tk.StringVar(value="未開始")
        self.skip_comparison_display_var = tk.BooleanVar(value=True)
        self.notify_fetch_complete_var = tk.BooleanVar(value=True)
        self.comparison_day_tail_var = tk.StringVar(value="全て")
        self.register_store_url_var = tk.StringVar()
        self.register_store_site7_enabled_var = tk.BooleanVar(value=False)
        self.register_store_prefecture_var = tk.StringVar(value=DEFAULT_SITE7_PREFECTURE_NAME)
        self.register_store_area_var = tk.StringVar()
        self.register_store_site7_store_name_var = tk.StringVar()
        self.register_store_status_var = tk.StringVar(value="未登録")
        self.site7_browser_mode_var = tk.StringVar(value=self.site7_browser_mode)
        self.site7_status_var = tk.StringVar(
            value="保存済みのログイン情報あり" if self.site7_scraper.has_saved_login_state() else "初回ログインが必要"
        )
        self.fetch_progress_current = 0
        self.fetch_progress_total = 0

        self._build_ui()
        self._reset_fetch_progress()
        self._update_button_states()
        self._refresh_registered_store_table()
        self.root.protocol("WM_DELETE_WINDOW", self._hide_to_resident)
        self._schedule_timer_tick()
        if self.startup_store_warning:
            self.root.after(100, lambda: messagebox.showwarning("登録店舗", self.startup_store_warning))
        self.root.after(250, self._prompt_site7_login_on_startup_if_needed)

    def _build_ui(self) -> None:
        container = ttk.Frame(self.root, padding=16)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)

        notebook = ttk.Notebook(container)
        notebook.grid(row=0, column=0, sticky="nsew")

        self.fetch_tab = ttk.Frame(notebook, padding=12)
        self.fetch_tab.columnconfigure(0, weight=1)
        self.fetch_tab.rowconfigure(2, weight=1)
        notebook.add(self.fetch_tab, text="データ取得")

        register_tab = ttk.Frame(notebook, padding=12)
        register_tab.columnconfigure(0, weight=1)
        register_tab.rowconfigure(1, weight=1)
        notebook.add(register_tab, text="登録店舗")

        self.fetch_form = ttk.LabelFrame(self.fetch_tab, text="取得条件", padding=12)
        self.fetch_form.grid(row=0, column=0, sticky="ew")
        self.fetch_form.columnconfigure(1, weight=1)

        ttk.Label(self.fetch_form, text="直近日数").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        self.target_date_entry = ttk.Entry(self.fetch_form, textvariable=self.target_date_var, width=8)
        self.target_date_entry.grid(row=0, column=1, sticky="w", pady=4)
        ttk.Label(self.fetch_form, text="日（日本時間の今日まで）").grid(row=0, column=1, sticky="w", padx=(72, 0), pady=4)

        ttk.Label(self.fetch_form, text="再試行休止").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        self.retry_delay_entry = ttk.Entry(self.fetch_form, textvariable=self.retry_delay_seconds_var, width=8)
        self.retry_delay_entry.grid(row=1, column=1, sticky="w", pady=4)
        ttk.Label(self.fetch_form, text="秒（取得失敗時は最大3回まで再試行）").grid(
            row=1,
            column=1,
            sticky="w",
            padx=(72, 0),
            pady=4,
        )

        button_row = ttk.Frame(self.fetch_form)
        button_row.grid(row=2, column=1, sticky="w", pady=(8, 0))

        self.fetch_button = ttk.Button(button_row, text="取得", command=self.fetch_data)
        self.fetch_button.grid(row=0, column=0, sticky="w")

        self.cancel_fetch_button = ttk.Button(button_row, text="中止", command=self.cancel_fetch)
        self.cancel_fetch_button.grid(row=0, column=1, sticky="w", padx=(8, 0))

        self.skip_comparison_display_button = ttk.Checkbutton(
            button_row,
            text="取得後に台データ表を表示しない",
            variable=self.skip_comparison_display_var,
            command=self._on_skip_comparison_display_changed,
        )
        self.skip_comparison_display_button.grid(row=0, column=2, sticky="w", padx=(12, 0))

        self.notify_fetch_complete_button = ttk.Checkbutton(
            button_row,
            text="取得完了時に音を鳴らす",
            variable=self.notify_fetch_complete_var,
        )
        self.notify_fetch_complete_button.grid(row=0, column=3, sticky="w", padx=(12, 0))

        schedule_row = ttk.Frame(self.fetch_form)
        schedule_row.grid(row=3, column=1, sticky="w", pady=(8, 0))
        ttk.Label(schedule_row, text="毎日").grid(row=0, column=0, sticky="w")
        self.schedule_hour_entry = ttk.Entry(schedule_row, textvariable=self.schedule_hour_var, width=4)
        self.schedule_hour_entry.grid(row=0, column=1, sticky="w", padx=(6, 4))
        ttk.Label(schedule_row, text="時に実行").grid(row=0, column=2, sticky="w")
        self.apply_schedule_button = ttk.Button(schedule_row, text="設定", command=self.apply_daily_schedule)
        self.apply_schedule_button.grid(row=0, column=3, sticky="w", padx=(8, 0))
        self.clear_schedule_button = ttk.Button(schedule_row, text="解除", command=self.clear_daily_schedule)
        self.clear_schedule_button.grid(row=0, column=4, sticky="w", padx=(8, 0))
        ttk.Label(schedule_row, textvariable=self.schedule_status_var).grid(row=0, column=5, sticky="w", padx=(12, 0))

        site7_row = ttk.LabelFrame(self.fetch_form, text="サイトセブン", padding=12)
        site7_row.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(12, 0))
        site7_row.columnconfigure(0, weight=1)

        ttk.Label(
            site7_row,
            text=(
                "登録店舗タブでサイトセブン列にチェックを入れた店舗の対象ジャグラー機種を取得します。"
                f" 対象語は {'、'.join(SITE7_TARGET_MACHINE_KEYWORDS)} です。"
                f" 直近日数は最大 {SITE7_MAX_RECENT_DAYS} 日まで使えます。"
                " ログイン操作は常に表示で開きます。"
            ),
            wraplength=900,
            justify="left",
        ).grid(row=0, column=0, columnspan=4, sticky="w")

        self.site7_login_button = ttk.Button(
            site7_row,
            text="サイトセブンにログイン",
            command=self.site7_login,
        )
        self.site7_login_button.grid(row=1, column=0, sticky="w", pady=(8, 0))

        self.site7_fetch_button = ttk.Button(
            site7_row,
            text="サイトセブン取得",
            command=self.fetch_site7_data,
        )
        self.site7_fetch_button.grid(row=1, column=1, sticky="w", padx=(8, 0), pady=(8, 0))

        self.site7_cancel_button = ttk.Button(site7_row, text="中止", command=self.cancel_fetch)
        self.site7_cancel_button.grid(row=1, column=2, sticky="w", padx=(8, 0), pady=(8, 0))

        ttk.Label(site7_row, textvariable=self.site7_status_var).grid(row=1, column=3, sticky="w", padx=(12, 0), pady=(8, 0))

        mode_row = ttk.Frame(site7_row)
        mode_row.grid(row=2, column=0, columnspan=4, sticky="w", pady=(8, 0))
        ttk.Label(mode_row, text="取得時のブラウザ").grid(row=0, column=0, sticky="w")
        self.site7_browser_visible_radio = ttk.Radiobutton(
            mode_row,
            text="表示",
            value=SITE7_BROWSER_MODE_VISIBLE,
            variable=self.site7_browser_mode_var,
            command=self._on_site7_browser_mode_changed,
        )
        self.site7_browser_visible_radio.grid(row=0, column=1, sticky="w", padx=(12, 0))
        self.site7_browser_hidden_radio = ttk.Radiobutton(
            mode_row,
            text="非表示",
            value=SITE7_BROWSER_MODE_HIDDEN,
            variable=self.site7_browser_mode_var,
            command=self._on_site7_browser_mode_changed,
        )
        self.site7_browser_hidden_radio.grid(row=0, column=2, sticky="w", padx=(8, 0))
        ttk.Label(mode_row, text="初期値は表示").grid(row=0, column=3, sticky="w", padx=(12, 0))

        self.fetch_info = ttk.Frame(self.fetch_tab, padding=(0, 12, 0, 12))
        self.fetch_info.grid(row=1, column=0, sticky="ew")
        self.fetch_info.columnconfigure(1, weight=1)
        self.fetch_info.columnconfigure(3, weight=1)

        ttk.Label(self.fetch_info, text="状態").grid(row=0, column=0, sticky="w")
        ttk.Label(self.fetch_info, textvariable=self.status_var).grid(row=0, column=1, sticky="w", padx=(8, 24))
        ttk.Label(self.fetch_info, text="概要").grid(row=0, column=2, sticky="w")
        ttk.Label(self.fetch_info, textvariable=self.summary_var).grid(row=0, column=3, sticky="w", padx=(8, 0))

        ttk.Label(self.fetch_info, text="進捗").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.fetch_progress_bar = ttk.Progressbar(
            self.fetch_info,
            variable=self.fetch_progress_value_var,
            maximum=100,
            mode="determinate",
        )
        self.fetch_progress_bar.grid(row=1, column=1, columnspan=2, sticky="ew", padx=(8, 12), pady=(8, 0))
        ttk.Label(self.fetch_info, textvariable=self.fetch_progress_text_var).grid(row=1, column=3, sticky="w", pady=(8, 0))

        self.comparison_frame = ttk.LabelFrame(self.fetch_tab, text="台データ比較", padding=8)
        self.comparison_frame.grid(row=2, column=0, sticky="nsew")
        self.comparison_frame.columnconfigure(1, weight=1)
        self.comparison_frame.rowconfigure(2, weight=1)

        comparison_actions = ttk.Frame(self.comparison_frame)
        comparison_actions.grid(row=0, column=0, columnspan=3, sticky="ew", pady=(0, 8))
        comparison_actions.columnconfigure(0, weight=1)

        ttk.Label(comparison_actions, text="日付末尾").grid(row=0, column=0, sticky="w")

        self.comparison_day_tail_selector = ttk.Combobox(
            comparison_actions,
            textvariable=self.comparison_day_tail_var,
            values=COMPARISON_DAY_TAIL_OPTIONS,
            state="readonly",
            width=5,
        )
        self.comparison_day_tail_selector.grid(row=0, column=1, sticky="w", padx=(8, 0))
        self.comparison_day_tail_selector.bind("<<ComboboxSelected>>", self._on_comparison_day_tail_changed)

        self.comparison_focus_button = ttk.Button(
            comparison_actions,
            text="台データ表を広く表示",
            command=self.toggle_comparison_focus,
        )
        self.comparison_focus_button.grid(row=0, column=2, sticky="e")

        self.comparison_fixed_header_canvas = tk.Canvas(self.comparison_frame, width=1, height=54, highlightthickness=0)
        self.comparison_fixed_header_canvas.grid(row=1, column=0, sticky="nsw")

        self.comparison_header_canvas = tk.Canvas(self.comparison_frame, height=54, highlightthickness=0)
        self.comparison_header_canvas.grid(row=1, column=1, sticky="ew")

        self.comparison_fixed_body_canvas = tk.Canvas(self.comparison_frame, width=1, highlightthickness=0)
        self.comparison_fixed_body_canvas.grid(row=2, column=0, sticky="nsw")

        self.comparison_body_canvas = tk.Canvas(self.comparison_frame, highlightthickness=0)
        self.comparison_body_canvas.grid(row=2, column=1, sticky="nsew")
        self.comparison_fixed_body_canvas.configure(yscrollincrement=self._comparison_body_row_height())
        self.comparison_body_canvas.configure(yscrollincrement=self._comparison_body_row_height())
        self.comparison_body_canvas.configure(yscrollcommand=self._on_comparison_yview_changed)

        y_scroll = ttk.Scrollbar(self.comparison_frame, orient="vertical", command=self._scroll_comparison_y)
        y_scroll.grid(row=2, column=2, sticky="ns")
        self.comparison_y_scrollbar = y_scroll

        x_scroll = ttk.Scrollbar(self.comparison_frame, orient="horizontal", command=self._scroll_comparison_x)
        x_scroll.grid(row=3, column=1, sticky="ew")
        self.comparison_body_canvas.configure(xscrollcommand=x_scroll.set)
        self.comparison_x_scrollbar = x_scroll

        self.comparison_fixed_header_canvas.bind("<Button-1>", self._on_comparison_fixed_header_click)
        self.comparison_header_canvas.bind("<Button-1>", self._on_comparison_header_click)
        self.comparison_fixed_body_canvas.bind("<Button-1>", self._on_comparison_fixed_body_click)
        self.comparison_fixed_body_canvas.bind("<MouseWheel>", self._on_comparison_mousewheel)
        self.comparison_body_canvas.bind("<MouseWheel>", self._on_comparison_mousewheel)

        self._build_register_tab(register_tab)

    def _prompt_site7_login_on_startup_if_needed(self) -> None:
        if self.site7_scraper.has_saved_login_state():
            self.site7_status_var.set("保存済みのログイン情報あり")
            return

        self.site7_status_var.set("初回ログインが必要")
        if not messagebox.askyesno(
            "サイトセブン",
            "サイトセブンは初回ログインが必要です。\n"
            "いまブラウザを開いてログインしますか？\n"
            "ログイン完了後の画面が見えたら、数秒待つと自動で反映します。",
        ):
            return

        self.site7_login()

    def site7_login(self) -> None:
        if self.is_busy:
            return

        messagebox.showinfo(
            "サイトセブン",
            "これからサイトセブンのログイン画面を開きます。\n"
            "ブラウザでログインしたあと、ログイン後の画面が見えるまで進めてください。\n"
            "画面が切り替わったら、数秒待つと自動で反映します。",
        )
        self.status_var.set("サイトセブンのログイン確認中")
        self.site7_status_var.set("ログイン確認中")
        self._start_worker(self._worker_site7_login, operation_kind="site7_login")

    def _worker_site7_login(self) -> None:
        try:
            self.site7_scraper.login_interactively()
            self.result_queue.put(("site7_login_success", None))
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("site7_login_error", exc))

    def _on_site7_browser_mode_changed(self) -> None:
        self.site7_browser_mode = normalize_site7_browser_mode(self.site7_browser_mode_var.get())
        self.site7_browser_mode_var.set(self.site7_browser_mode)
        try:
            self._save_site7_browser_mode(self.site7_browser_mode)
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("設定保存", f"サイトセブンの表示設定保存に失敗しました。\n{exc}")

    def _site7_browser_visible(self) -> bool:
        browser_mode = normalize_site7_browser_mode(self.site7_browser_mode_var.get())
        self.site7_browser_mode = browser_mode
        return browser_mode == SITE7_BROWSER_MODE_VISIBLE

    def apply_daily_schedule(self) -> None:
        try:
            scheduled_hour = self._parse_schedule_hour()
        except ScraperError as exc:
            messagebox.showwarning("入力不正", str(exc))
            return

        self.scheduled_fetch_hour = scheduled_hour
        self.scheduled_last_run_date = None
        self.scheduled_pending_date = None
        try:
            self._save_schedule_hour(scheduled_hour)
        except Exception as exc:  # noqa: BLE001
            messagebox.showwarning("設定保存", f"定期実行の時刻保存に失敗しました。\n{exc}")
        self.schedule_status_var.set(f"毎日 {scheduled_hour} 時に実行")

    def clear_daily_schedule(self) -> None:
        self.scheduled_fetch_hour = None
        self.scheduled_last_run_date = None
        self.scheduled_pending_date = None
        self.schedule_status_var.set("定期実行なし")

    def _parse_schedule_hour(self) -> int:
        text = self.schedule_hour_var.get().strip()
        if not re.fullmatch(r"\d{1,2}", text):
            raise ScraperError("定期実行の時刻は 0 から 23 の整数で入力してください。")

        scheduled_hour = int(text)
        if not 0 <= scheduled_hour <= 23:
            raise ScraperError("定期実行の時刻は 0 から 23 の整数で入力してください。")

        return scheduled_hour

    def _settings_file_path(self) -> Path:
        return self.persistence_service.root_dir / "local_data" / GUI_SETTINGS_FILE_NAME

    def _load_gui_settings(self) -> dict[str, object]:
        settings_path = self._settings_file_path()
        if not settings_path.exists():
            return {}

        try:
            payload = json.loads(settings_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            return {}

        return payload if isinstance(payload, dict) else {}

    def _save_gui_settings(self, **updates: object) -> None:
        settings_path = self._settings_file_path()
        settings_path.parent.mkdir(parents=True, exist_ok=True)
        payload = self._load_gui_settings()
        payload.update(updates)
        settings_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _load_saved_schedule_hour(self) -> int:
        try:
            payload = self._load_gui_settings()
            scheduled_hour = int(payload.get("scheduled_fetch_hour", DEFAULT_SCHEDULE_HOUR))
        except Exception:  # noqa: BLE001
            return DEFAULT_SCHEDULE_HOUR

        if not 0 <= scheduled_hour <= 23:
            return DEFAULT_SCHEDULE_HOUR
        return scheduled_hour

    def _save_schedule_hour(self, scheduled_hour: int) -> None:
        self._save_gui_settings(scheduled_fetch_hour=scheduled_hour)

    def _load_saved_site7_browser_mode(self) -> str:
        try:
            payload = self._load_gui_settings()
        except Exception:  # noqa: BLE001
            return SITE7_BROWSER_MODE_VISIBLE
        return normalize_site7_browser_mode(payload.get("site7_browser_mode", SITE7_BROWSER_MODE_VISIBLE))

    def _save_site7_browser_mode(self, browser_mode: str) -> None:
        self._save_gui_settings(site7_browser_mode=normalize_site7_browser_mode(browser_mode))

    def _schedule_timer_tick(self) -> None:
        self._run_scheduled_fetch_if_due()
        self.root.after(30_000, self._schedule_timer_tick)

    def _run_scheduled_fetch_if_due(self) -> None:
        if self.scheduled_fetch_hour is None:
            return

        now = datetime.now(JST)
        today_text = now.date().isoformat()
        if self.scheduled_pending_date is not None:
            if self.is_busy:
                self.schedule_status_var.set(f"{self.scheduled_pending_date} の定期実行を待機中")
                return
            self.scheduled_last_run_date = self.scheduled_pending_date
            self.scheduled_pending_date = None
            self._start_scheduled_fetch()
            return

        if now.hour != self.scheduled_fetch_hour or self.scheduled_last_run_date == today_text:
            return

        if self.is_busy:
            self.scheduled_pending_date = today_text
            self.schedule_status_var.set(f"本日 {self.scheduled_fetch_hour} 時の定期実行を待機中")
            return

        self.scheduled_last_run_date = today_text
        self._start_scheduled_fetch()

    def _start_scheduled_fetch(self) -> None:
        try:
            target_date_input = self._target_date_input_from_recent_days()
            retry_delay_seconds = self._retry_delay_seconds_input()
        except ScraperError as exc:
            self.schedule_status_var.set("定期実行を開始できません")
            self._show_error(exc)
            return

        self.current_results = []
        self.current_history_result = None
        self.comparison_rows = []
        self.comparison_slot_numbers = []
        self.comparison_display_rows = []
        self.comparison_selected_date = None
        self._clear_comparison_table()
        self._begin_fetch_progress("定期実行: 登録店舗を更新中...")
        self.status_var.set("定期実行中...")
        self.summary_var.set("登録店舗を更新してから取得します")
        self.schedule_status_var.set("定期実行中")
        self.fetch_cancel_event.clear()
        self._start_worker(
            self._worker_scheduled_fetch,
            target_date_input,
            retry_delay_seconds,
            operation_kind="scheduled_fetch",
        )

    def _hide_to_resident(self) -> None:
        if not self._ensure_tray_icon():
            messagebox.showwarning(
                "常駐",
                "常駐アイコンを表示できません。requirements.txt の内容を入れ直してください。",
            )
            return

        self.root.withdraw()
        if not self.is_busy:
            self.status_var.set("常駐中")

    def _ensure_tray_icon(self) -> bool:
        if self.tray_icon is not None:
            return True

        if pystray is None or Image is None or ImageDraw is None:
            return False

        try:
            icon_image = self._create_tray_icon_image()
            self.tray_icon = pystray.Icon(
                "halldata",
                icon_image,
                "Halldata",
                menu=pystray.Menu(
                    pystray.MenuItem("表示", self._on_tray_show, default=True),
                    pystray.MenuItem("終了", self._on_tray_exit),
                ),
            )
            self.tray_thread = threading.Thread(target=self.tray_icon.run, daemon=True)
            self.tray_thread.start()
            return True
        except Exception as exc:  # noqa: BLE001
            self.tray_icon = None
            self.tray_thread = None
            messagebox.showwarning("常駐", f"常駐アイコンを表示できませんでした。\n{exc}")
            return False

    def _create_tray_icon_image(self) -> object:
        image = Image.new("RGBA", (64, 64), (255, 255, 255, 0))
        draw = ImageDraw.Draw(image)
        draw.rectangle((8, 8, 56, 56), fill=(48, 126, 204, 255))
        draw.rectangle((14, 14, 50, 50), outline=(255, 255, 255, 255), width=4)
        draw.text((24, 22), "H", fill=(255, 255, 255, 255))
        return image

    def _on_tray_show(self, *_: object) -> None:
        self.root.after(0, self._show_from_tray)

    def _show_from_tray(self) -> None:
        self.root.deiconify()
        self.root.lift()
        self.root.focus_force()
        if self.status_var.get() == "常駐中":
            self.status_var.set("待機中")

    def _on_tray_exit(self, *_: object) -> None:
        self.root.after(0, self._quit_from_tray)

    def _quit_from_tray(self) -> None:
        if self.tray_icon is not None:
            self.tray_icon.stop()
            self.tray_icon = None
        self.site7_scraper.close_visible_browser()
        self.root.destroy()

    def _build_register_tab(self, register_tab: ttk.Frame) -> None:
        guide = ttk.LabelFrame(register_tab, text="案内", padding=12)
        guide.grid(row=0, column=0, sticky="ew")
        guide.columnconfigure(0, weight=1)

        ttk.Label(
            guide,
            text=(
                "ここでは店舗URLを入れて店舗名を自動取得し、一覧へ登録できます。"
                "取得対象のチェックが入っている店舗を、データ取得タブの取得ボタンで順番に取得します。"
                "登録した店舗一覧は Supabase に保存されます。"
                "一覧で行を選ぶと、選んだ店舗を Supabase から削除できます。"
            ),
            wraplength=900,
            justify="left",
        ).grid(row=0, column=0, sticky="w")

        form = ttk.LabelFrame(register_tab, text="店舗を登録", padding=12)
        form.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        form.columnconfigure(1, weight=1)
        form.rowconfigure(6, weight=1)

        ttk.Label(form, text="店舗URL").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        self.register_store_url_entry = ttk.Entry(form, textvariable=self.register_store_url_var)
        self.register_store_url_entry.grid(row=0, column=1, sticky="ew", pady=4)

        self.register_store_site7_button = ttk.Checkbutton(
            form,
            text="この店舗をサイトセブン取得の対象にする",
            variable=self.register_store_site7_enabled_var,
        )
        self.register_store_site7_button.grid(row=1, column=1, sticky="w", pady=4)

        ttk.Label(form, text="都道府県").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=4)
        self.register_store_prefecture_entry = ttk.Entry(form, textvariable=self.register_store_prefecture_var)
        self.register_store_prefecture_entry.grid(row=2, column=1, sticky="ew", pady=4)

        ttk.Label(form, text="地域").grid(row=3, column=0, sticky="w", padx=(0, 8), pady=4)
        self.register_store_area_entry = ttk.Entry(form, textvariable=self.register_store_area_var)
        self.register_store_area_entry.grid(row=3, column=1, sticky="ew", pady=4)

        ttk.Label(form, text="SS店舗名").grid(row=4, column=0, sticky="w", padx=(0, 8), pady=4)
        self.register_store_site7_store_name_entry = ttk.Entry(form, textvariable=self.register_store_site7_store_name_var)
        self.register_store_site7_store_name_entry.grid(row=4, column=1, sticky="ew", pady=4)

        action_row = ttk.Frame(form)
        action_row.grid(row=5, column=1, sticky="w", pady=(8, 8))
        self.register_store_button = ttk.Button(action_row, text="登録する", command=self.register_store)
        self.register_store_button.grid(row=0, column=0, sticky="w")
        self.update_registered_store_button = ttk.Button(
            action_row,
            text="選択行を更新",
            command=self.update_registered_store,
        )
        self.update_registered_store_button.grid(row=0, column=1, sticky="w", padx=(8, 0))
        self.clear_register_store_form_button = ttk.Button(
            action_row,
            text="入力欄をクリア",
            command=self.clear_register_store_form,
        )
        self.clear_register_store_form_button.grid(row=0, column=2, sticky="w", padx=(8, 0))

        ttk.Label(action_row, textvariable=self.register_store_status_var).grid(row=0, column=3, sticky="w", padx=(12, 0))

        table_frame = ttk.LabelFrame(form, text="登録済み一覧", padding=8)
        table_frame.grid(row=6, column=0, columnspan=2, sticky="nsew", pady=(8, 0))
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(1, weight=1)

        target_action_row = ttk.Frame(table_frame)
        target_action_row.grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))
        self.select_all_stores_button = ttk.Button(
            target_action_row,
            text="全て取得対象にする",
            command=self._select_all_registered_stores,
        )
        self.select_all_stores_button.grid(row=0, column=0, sticky="w")
        self.clear_store_selection_button = ttk.Button(
            target_action_row,
            text="取得対象を全て外す",
            command=self._clear_registered_store_selection,
        )
        self.clear_store_selection_button.grid(row=0, column=1, sticky="w", padx=(8, 0))
        self.refresh_registered_stores_button = ttk.Button(
            target_action_row,
            text="最新に更新",
            command=self.refresh_registered_stores,
        )
        self.refresh_registered_stores_button.grid(row=0, column=2, sticky="w", padx=(8, 0))
        self.delete_registered_stores_button = ttk.Button(
            target_action_row,
            text="選択した店舗を削除",
            command=self.delete_registered_stores,
        )
        self.delete_registered_stores_button.grid(row=0, column=3, sticky="w", padx=(8, 0))

        self.registered_store_tree = ttk.Treeview(
            table_frame,
            columns=REGISTERED_STORE_COLUMNS,
            show="headings",
            selectmode="extended",
        )
        self.registered_store_tree.grid(row=1, column=0, sticky="nsew")

        for column in REGISTERED_STORE_COLUMNS:
            self.registered_store_tree.heading(column, text=column)
            if column in {"取得対象", "サイトセブン"}:
                self.registered_store_tree.column(column, width=80, minwidth=80, anchor="center")
                continue
            self.registered_store_tree.column(
                column,
                width=220 if column == "店舗名" else 180 if column in {"都道府県", "地域", "SS店舗名"} else 520,
                minwidth=120 if column != "URL" else 280,
                anchor="w",
            )
        self.registered_store_tree.bind("<Button-1>", self._on_registered_store_tree_click)
        self.registered_store_tree.bind("<<TreeviewSelect>>", self._on_registered_store_selection_changed)

        y_scroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.registered_store_tree.yview)
        y_scroll.grid(row=1, column=1, sticky="ns")
        self.registered_store_tree.configure(yscrollcommand=y_scroll.set)

        x_scroll = ttk.Scrollbar(table_frame, orient="horizontal", command=self.registered_store_tree.xview)
        x_scroll.grid(row=2, column=0, sticky="ew")
        self.registered_store_tree.configure(xscrollcommand=x_scroll.set)

    def _load_registered_stores_on_startup(self) -> list[RegisteredStore]:
        try:
            return self._load_latest_registered_stores()
        except Exception as exc:  # noqa: BLE001
            self.startup_store_warning = f"登録店舗の読込に失敗したため、初期店舗だけを表示します。\n{exc}"
            return self._default_registered_stores()

    def _default_registered_stores(self) -> list[RegisteredStore]:
        return [self._build_registered_store(DEFAULT_STORE_NAME, DEFAULT_STORE_URL)]

    def _load_latest_registered_stores(self) -> list[RegisteredStore]:
        saved_stores = self.persistence_service.load_registered_stores()
        return [
            self._build_registered_store(
                store_name=store["store_name"],
                store_url=store["store_url"],
                site7_enabled=bool(store.get("site7_enabled", False)),
                site7_prefecture=str(store.get("site7_prefecture", DEFAULT_SITE7_PREFECTURE_NAME)),
                site7_area=str(store.get("site7_area", "")),
                site7_store_name=str(store.get("site7_store_name", "")),
            )
            for store in saved_stores
        ]

    def refresh_registered_stores(self) -> None:
        if self.is_busy:
            return

        self.register_store_status_var.set("登録店舗を更新中...")
        self._start_worker(self._worker_refresh_registered_stores, operation_kind="refresh_stores")

    def _worker_refresh_registered_stores(self) -> None:
        try:
            refresh_result = self._load_and_complete_registered_stores()
            self.result_queue.put(("refresh_registered_stores_success", refresh_result))
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("refresh_registered_stores_error", exc))

    def delete_registered_stores(self) -> None:
        if self.is_busy:
            return

        target_stores = self._selected_registered_store_rows()
        if not target_stores:
            messagebox.showwarning("入力不足", "削除する店舗を一覧から選んでください。")
            return

        if not self._confirm_registered_store_deletion(target_stores):
            return

        self.register_store_status_var.set("登録店舗を削除中...")
        self._start_worker(
            self._worker_delete_registered_stores,
            [registered_store.url for registered_store in target_stores],
            operation_kind="delete_stores",
        )

    def _worker_delete_registered_stores(self, store_urls: list[str]) -> None:
        try:
            deleted_store_count = self.persistence_service.delete_registered_stores(store_urls)
            registered_stores = self._load_latest_registered_stores()
            self.result_queue.put(
                (
                    "delete_registered_stores_success",
                    StoreDeleteResult(
                        registered_stores=registered_stores,
                        deleted_store_count=deleted_store_count,
                    ),
                )
            )
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("delete_registered_stores_error", exc))

    def _worker_scheduled_fetch(self, target_date_input: str, retry_delay_seconds: int) -> None:
        try:
            refresh_result = self._load_and_complete_registered_stores()
            self._raise_if_fetch_cancelled()
            fetch_many_result = self._run_fetch_many(
                refresh_result.registered_stores,
                target_date_input,
                retry_delay_seconds,
            )
            if fetch_many_result.cancelled and not fetch_many_result.results:
                self.result_queue.put(("fetch_cancelled", None))
                return
            self.result_queue.put(("scheduled_fetch_many_success", (refresh_result, fetch_many_result)))
        except FetchCancelled:
            self.result_queue.put(("fetch_cancelled", None))
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("fetch_error", exc))

    def _load_and_complete_registered_stores(self) -> StoreRefreshResult:
        registered_stores = self._load_latest_registered_stores()
        completed_stores: list[RegisteredStore] = []
        save_summary: RegisteredStoresPersistenceSummary | None = None
        changed = False
        messages: list[str] = []

        for registered_store in registered_stores:
            if registered_store.name.strip():
                completed_stores.append(registered_store)
                continue

            try:
                store_name = self.scraper.fetch_store_name(registered_store.url)
            except Exception as exc:  # noqa: BLE001
                messages.append(f"{registered_store.url} の店舗名取得に失敗しました。\n{exc}")
                completed_stores.append(registered_store)
                continue

            completed_stores.append(
                self._build_registered_store(
                    store_name=store_name,
                    store_url=registered_store.url,
                    site7_enabled=registered_store.site7_enabled,
                    site7_prefecture=registered_store.site7_prefecture,
                    site7_area=registered_store.site7_area,
                    site7_store_name=registered_store.site7_store_name,
                )
            )
            changed = True

        if changed:
            save_summary = self._persist_registered_store_list(completed_stores)
        elif messages:
            save_summary = RegisteredStoresPersistenceSummary(messages=list(messages))

        if save_summary is not None and changed:
            save_summary.messages.extend(messages)

        return StoreRefreshResult(registered_stores=completed_stores, save_summary=save_summary)

    def register_store(self) -> None:
        try:
            (
                store_url,
                site7_enabled,
                site7_prefecture,
                site7_area,
                site7_store_name,
            ) = self._validated_register_store_form_input()
        except ScraperError as exc:
            self._show_error(exc)
            return

        normalized_url = normalize_store_url(store_url)
        for registered_store in self.registered_stores:
            if normalize_store_url(registered_store.url) == normalized_url:
                messagebox.showwarning("重複", "同じURLがすでに登録されています。")
                return

        self.register_store_status_var.set("店舗名を取得中...")
        self._start_worker(
            self._worker_register_store,
            store_url,
            site7_enabled,
            site7_prefecture,
            site7_area,
            site7_store_name,
        )

    def update_registered_store(self) -> None:
        target_stores = self._selected_registered_store_rows()
        if len(target_stores) != 1:
            messagebox.showwarning("入力不足", "更新する店舗を一覧から1つだけ選んでください。")
            return

        try:
            (
                store_url,
                site7_enabled,
                site7_prefecture,
                site7_area,
                site7_store_name,
            ) = self._validated_register_store_form_input()
        except ScraperError as exc:
            self._show_error(exc)
            return

        target_store = target_stores[0]
        if normalize_store_url(store_url) == normalize_store_url(target_store.url):
            self._replace_registered_store_entry(
                original_store=target_store,
                store_name=target_store.name,
                store_url=store_url,
                site7_enabled=site7_enabled,
                site7_prefecture=site7_prefecture,
                site7_area=site7_area,
                site7_store_name=site7_store_name,
            )
            return

        self.register_store_status_var.set("更新先URLの店舗名を取得中...")
        self._start_worker(
            self._worker_update_registered_store,
            target_store.url,
            store_url,
            site7_enabled,
            site7_prefecture,
            site7_area,
            site7_store_name,
        )

    def clear_register_store_form(self) -> None:
        self.register_store_url_var.set("")
        self.register_store_site7_enabled_var.set(False)
        self.register_store_prefecture_var.set(DEFAULT_SITE7_PREFECTURE_NAME)
        self.register_store_area_var.set("")
        self.register_store_site7_store_name_var.set("")
        if hasattr(self, "registered_store_tree"):
            selected_items = self.registered_store_tree.selection()
            if selected_items:
                self.registered_store_tree.selection_remove(*selected_items)
        self.register_store_status_var.set("入力欄をクリアしました")
        self._update_button_states()

    def _worker_register_store(
        self,
        store_url: str,
        site7_enabled: bool,
        site7_prefecture: str,
        site7_area: str,
        site7_store_name: str,
    ) -> None:
        try:
            store_name = self.scraper.fetch_store_name(store_url)
            self.result_queue.put(
                (
                    "register_store_success",
                    (
                        store_name,
                        store_url,
                        site7_enabled,
                        site7_prefecture,
                        site7_area,
                        site7_store_name,
                    ),
                )
            )
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("register_store_error", exc))

    def _worker_update_registered_store(
        self,
        original_store_url: str,
        store_url: str,
        site7_enabled: bool,
        site7_prefecture: str,
        site7_area: str,
        site7_store_name: str,
    ) -> None:
        try:
            store_name = self.scraper.fetch_store_name(store_url)
            self.result_queue.put(
                (
                    "update_registered_store_success",
                    (
                        original_store_url,
                        store_name,
                        store_url,
                        site7_enabled,
                        site7_prefecture,
                        site7_area,
                        site7_store_name,
                    ),
                )
            )
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("update_registered_store_error", exc))

    def fetch_site7_data(self) -> None:
        try:
            recent_days = parse_recent_days(self.target_date_var.get())
            retry_delay_seconds = self._retry_delay_seconds_input()
        except ScraperError as exc:
            self._show_error(exc)
            return

        if recent_days > SITE7_MAX_RECENT_DAYS:
            if not messagebox.askyesno(
                "サイトセブン",
                f"サイトセブンは直近 {SITE7_MAX_RECENT_DAYS} 日までです。\n"
                f"{SITE7_MAX_RECENT_DAYS} 日として取得しますか？",
            ):
                return
            recent_days = SITE7_MAX_RECENT_DAYS

        if not self.site7_scraper.has_saved_login_state():
            if messagebox.askyesno(
                "サイトセブン",
                "サイトセブンのログイン情報がまだありません。\n先にログイン画面を開きますか？",
            ):
                self.site7_login()
            return

        try:
            target_stores = self._selected_site7_registered_stores()
        except ScraperError as exc:
            self._show_error(exc)
            return
        if not target_stores:
            messagebox.showwarning("入力不足", "登録店舗タブでサイトセブン列にチェックを入れた店舗を1つ以上用意してください。")
            return

        self.current_results = []
        self.current_history_result = None
        self.comparison_rows = []
        self.comparison_slot_numbers = []
        self.comparison_display_rows = []
        self.comparison_selected_date = None
        self._clear_comparison_table()
        self._begin_fetch_progress("サイトセブンへ接続中...")
        self.status_var.set("サイトセブン取得中...")
        self.summary_var.set(f"{len(target_stores)}店舗の対象ジャグラー機種をサイトセブンから取得中")
        self.fetch_cancel_event.clear()
        browser_visible = self._site7_browser_visible()
        self._start_worker(
            self._worker_fetch_site7,
            target_stores,
            recent_days,
            retry_delay_seconds,
            browser_visible,
            operation_kind="site7_fetch",
        )

    def _worker_fetch_site7(
        self,
        target_stores: list[RegisteredStore],
        recent_days: int,
        retry_delay_seconds: int,
        browser_visible: bool,
    ) -> None:
        try:
            fetch_many_result = self._run_site7_fetch_many(
                target_stores=target_stores,
                recent_days=recent_days,
                retry_delay_seconds=retry_delay_seconds,
                browser_visible=browser_visible,
            )
            if fetch_many_result.cancelled and not fetch_many_result.results:
                self.result_queue.put(("fetch_cancelled", None))
                return
            self.result_queue.put(("fetch_many_success", fetch_many_result))
        except FetchCancelled:
            self.result_queue.put(("fetch_cancelled", None))
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("fetch_error", exc))

    def _run_site7_fetch_many(
        self,
        target_stores: list[RegisteredStore],
        recent_days: int,
        retry_delay_seconds: int,
        browser_visible: bool,
    ) -> FetchManyResult:
        results: list[StoreFetchResult] = []
        failures: list[StoreFetchFailure] = []
        total_stores = len(target_stores)
        cancelled = False

        for store_index, registered_store in enumerate(target_stores, start=1):
            if self.fetch_cancel_event.is_set():
                cancelled = True
                break

            try:
                results.append(
                    self._fetch_single_site7_store(
                        registered_store=registered_store,
                        recent_days=recent_days,
                        store_index=store_index,
                        total_stores=total_stores,
                        retry_delay_seconds=retry_delay_seconds,
                        browser_visible=browser_visible,
                    )
                )
            except FetchCancelled:
                cancelled = True
                break
            except Exception as exc:  # noqa: BLE001
                failures.append(StoreFetchFailure(store=registered_store, error=exc))
                self.result_queue.put(
                    (
                        "fetch_progress",
                        FetchProgress(
                            current_step=1,
                            total_steps=1,
                            message=f"{store_index}/{total_stores} {target_store.display_name} は取得失敗",
                        ),
                    )
                )

        if self.fetch_cancel_event.is_set():
            cancelled = True

        if not results and failures:
            failure_lines = "\n".join(f"{failure.store.name}: {failure.error}" for failure in failures)
            raise ScraperError(f"サイトセブンの対象店舗を取得できませんでした。\n{failure_lines}")

        return FetchManyResult(results=results, failures=failures, cancelled=cancelled)

    def _fetch_single_site7_store(
        self,
        registered_store: RegisteredStore,
        recent_days: int,
        store_index: int,
        total_stores: int,
        retry_delay_seconds: int,
        browser_visible: bool,
    ) -> StoreFetchResult:
        self._raise_if_fetch_cancelled()
        target_store = registered_store.to_site7_target_store()
        store_label = f"{store_index}/{total_stores} {registered_store.name}"

        def run_site7_fetch() -> MachineHistoryResult:
            try:
                return self.site7_scraper.fetch_target_machine_history(
                    recent_days=recent_days,
                    browser_visible=browser_visible,
                    progress_callback=lambda progress: self.result_queue.put(
                        (
                            "fetch_progress",
                            FetchProgress(
                                current_step=progress.current_step,
                                total_steps=progress.total_steps,
                                message=f"{store_label}: {progress.message}",
                            ),
                        )
                    ),
                    target_store=target_store,
                    cancel_requested=self.fetch_cancel_event.is_set,
                )
            except Site7FetchCancelled as exc:
                raise FetchCancelled from exc

        history_result = self._run_with_fetch_retries(
            run_site7_fetch,
            retry_delay_seconds=retry_delay_seconds,
            retry_status_callback=lambda retry_number, max_retries, delay_seconds: self.result_queue.put(
                (
                    "fetch_progress",
                    FetchProgress(
                        current_step=0,
                        total_steps=4,
                        message=(
                            f"{store_label}: サイトセブン取得に失敗しました。"
                            f"{delay_seconds}秒後に再試行します（{retry_number}/{max_retries}）"
                        ),
                    ),
                )
            ),
        )
        self._raise_if_fetch_cancelled()
        history_result = rewrite_history_result_store(
            history_result,
            store_name=registered_store.name,
            store_url=registered_store.url,
        )
        self.result_queue.put(
            (
                "fetch_progress",
                FetchProgress(current_step=3, total_steps=4, message=f"{store_label}: 保存済み日付を確認中"),
            )
        )
        history_result, warning_summary = self._prepare_site7_history_result_for_save(history_result)
        self._raise_if_fetch_cancelled()
        save_summary: PersistenceSummary | None = None
        if history_result.datasets:
            self.result_queue.put(
                (
                    "fetch_progress",
                    FetchProgress(current_step=3, total_steps=4, message=f"{store_label}: 保存中"),
                )
            )
            save_summary = self.persistence_service.save_history_result(history_result)
        return StoreFetchResult(
            history_result=history_result,
            save_summary=save_summary,
            saved_full_day_summary=warning_summary,
        )

    def _prepare_site7_history_result_for_save(
        self,
        history_result: MachineHistoryResult,
    ) -> tuple[MachineHistoryResult, SavedFullDayDatesSummary]:
        warning_messages: list[str] = []
        preferred_store = self.persistence_service.resolve_preferred_store_by_name(history_result.store_name)
        if preferred_store is not None:
            preferred_store_name = str(preferred_store.get("store_name", "")).strip()
            preferred_store_url = str(preferred_store.get("store_url", "")).strip()
            if preferred_store_name and preferred_store_url:
                history_result = rewrite_history_result_store(
                    history_result,
                    store_name=preferred_store_name,
                    store_url=preferred_store_url,
                )

        machine_names = sorted({dataset.machine_name for dataset in history_result.datasets}, key=normalize_text)
        saved_targets_summary = self.persistence_service.find_saved_machine_targets_supabase(
            store_url=history_result.store_url,
            start_date=history_result.start_date,
            end_date=history_result.end_date,
            machine_names=machine_names,
        )
        warning_messages.extend(saved_targets_summary.messages)
        history_result = filter_site7_history_result_by_saved_targets(
            history_result,
            saved_targets=saved_targets_summary.saved_targets,
        )

        replaceable_targets = {
            (dataset.target_date, dataset.machine_name)
            for dataset in history_result.datasets
            if (dataset.target_date, normalize_text(dataset.machine_name)) in saved_targets_summary.replaceable_targets
        }
        if replaceable_targets:
            self.result_queue.put(("fetch_progress", FetchProgress(current_step=3, total_steps=4, message="サイトセブン仮置き分を入れ直す準備中")))
            try:
                self.persistence_service.delete_machine_targets_from_supabase(
                    store_url=history_result.store_url,
                    target_pairs=replaceable_targets,
                    data_source="site7",
                )
            except Exception as exc:  # noqa: BLE001
                warning_messages.append(f"サイトセブン仮置き分の上書き準備に失敗しました。\n{exc}")

        return history_result, SavedFullDayDatesSummary(messages=warning_messages)

    def fetch_data(self) -> None:
        try:
            target_date_input = self._target_date_input_from_recent_days()
            retry_delay_seconds = self._retry_delay_seconds_input()
        except ScraperError as exc:
            self._show_error(exc)
            return

        target_stores = self._selected_registered_stores()
        if not target_stores:
            messagebox.showwarning("入力不足", "登録店舗タブで取得対象にする店舗を1つ以上選んでください。")
            return

        self.current_results = []
        self.current_history_result = None
        self.comparison_rows = []
        self.comparison_slot_numbers = []
        self.comparison_display_rows = []
        self.comparison_selected_date = None
        self._clear_comparison_table()
        self._begin_fetch_progress("対象期間を確認中...")
        self.status_var.set("取得中...")
        self.summary_var.set(f"{len(target_stores)}店舗を期間取得中")
        self.fetch_cancel_event.clear()
        self._start_worker(
            self._worker_fetch_many,
            target_stores,
            target_date_input,
            retry_delay_seconds,
            operation_kind="fetch",
        )

    def cancel_fetch(self) -> None:
        if not self.is_busy or self.active_operation_kind not in {"fetch", "scheduled_fetch", "site7_fetch"}:
            return

        self.fetch_cancel_event.set()
        self.status_var.set("中止中...")
        self.fetch_progress_text_var.set("現在の処理が区切れたら中止します")
        self._update_button_states()

    def _start_worker(self, target: object, *args: object, operation_kind: str = "general") -> None:
        self.is_busy = True
        self.active_operation_kind = operation_kind
        self._update_button_states()

        worker = threading.Thread(target=target, args=args, daemon=True)
        worker.start()
        self.root.after(100, self._poll_queue)

    def _raise_if_fetch_cancelled(self) -> None:
        if self.fetch_cancel_event.is_set():
            raise FetchCancelled

    def _run_with_fetch_retries(
        self,
        action: Callable[[], T],
        retry_delay_seconds: int,
        retry_status_callback: Callable[[int, int, int], None],
    ) -> T:
        for failed_count in range(MAX_FETCH_RETRY_COUNT + 1):
            self._raise_if_fetch_cancelled()
            try:
                return action()
            except FetchCancelled:
                raise
            except Exception:
                if failed_count >= MAX_FETCH_RETRY_COUNT:
                    raise

                retry_number = failed_count + 1
                retry_status_callback(retry_number, MAX_FETCH_RETRY_COUNT, retry_delay_seconds)
                if retry_delay_seconds > 0 and self.fetch_cancel_event.wait(retry_delay_seconds):
                    raise FetchCancelled

        raise ScraperError("取得の再試行に失敗しました。")

    def _worker_fetch_many(
        self,
        target_stores: list[RegisteredStore],
        target_date_input: str,
        retry_delay_seconds: int,
    ) -> None:
        try:
            fetch_many_result = self._run_fetch_many(target_stores, target_date_input, retry_delay_seconds)
            if fetch_many_result.cancelled and not fetch_many_result.results:
                self.result_queue.put(("fetch_cancelled", None))
                return
            self.result_queue.put(("fetch_many_success", fetch_many_result))
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("fetch_error", exc))

    def _run_fetch_many(
        self,
        target_stores: list[RegisteredStore],
        target_date_input: str,
        retry_delay_seconds: int,
    ) -> FetchManyResult:
        results: list[StoreFetchResult] = []
        failures: list[StoreFetchFailure] = []
        total_stores = len(target_stores)
        cancelled = False

        for store_index, registered_store in enumerate(target_stores, start=1):
            if self.fetch_cancel_event.is_set():
                cancelled = True
                break

            try:
                results.append(
                    self._fetch_single_store(
                        registered_store=registered_store,
                        target_date_input=target_date_input,
                        store_index=store_index,
                        total_stores=total_stores,
                        retry_delay_seconds=retry_delay_seconds,
                    )
                )
            except FetchCancelled:
                cancelled = True
                break
            except Exception as exc:  # noqa: BLE001
                failures.append(StoreFetchFailure(store=registered_store, error=exc))
                self.result_queue.put(
                    (
                        "fetch_progress",
                        FetchProgress(
                            current_step=1,
                            total_steps=1,
                            message=f"{store_index}/{total_stores} {registered_store.name} は取得失敗",
                        ),
                    )
                )

        if self.fetch_cancel_event.is_set():
            cancelled = True

        if not results and failures:
            failure_lines = "\n".join(f"{failure.store.name}: {failure.error}" for failure in failures)
            raise ScraperError(f"選択した店舗を取得できませんでした。\n{failure_lines}")

        return FetchManyResult(results=results, failures=failures, cancelled=cancelled)

    def _fetch_single_store(
        self,
        registered_store: RegisteredStore,
        target_date_input: str,
        store_index: int,
        total_stores: int,
        retry_delay_seconds: int,
    ) -> StoreFetchResult:
        self._raise_if_fetch_cancelled()
        store_url = registered_store.url
        store_label = f"{store_index}/{total_stores} {self._registered_store_display_name(registered_store)}"
        context = self._run_with_fetch_retries(
            lambda: self.scraper.prepare_machine_history_context(store_url, target_date_input),
            retry_delay_seconds=retry_delay_seconds,
            retry_status_callback=lambda retry_number, max_retries, delay_seconds: self.result_queue.put(
                (
                    "fetch_progress",
                    FetchProgress(
                        current_step=0,
                        total_steps=1,
                        message=(
                            f"{store_label}: 対象期間の確認に失敗しました。"
                            f"{delay_seconds}秒後に再試行します（{retry_number}/{max_retries}）"
                        ),
                    ),
                )
            ),
        )
        self._raise_if_fetch_cancelled()
        saved_full_day_summary = self.persistence_service.find_saved_full_day_dates(
            store_name=context.store_name,
            store_url=store_url,
            start_date=context.start_date,
            end_date=context.end_date,
        )
        self._raise_if_fetch_cancelled()
        skipped_dates = [
            date_page.target_date
            for date_page in context.date_pages
            if date_page.target_date in saved_full_day_summary.saved_dates
        ]
        pending_date_pages = [
            date_page
            for date_page in context.date_pages
            if date_page.target_date not in saved_full_day_summary.saved_dates
        ]
        total_steps = max(1, len(pending_date_pages) * 2 + 1)
        current_step = 0
        self.result_queue.put(
            (
                "fetch_progress",
                FetchProgress(
                    current_step=current_step,
                    total_steps=total_steps,
                    message=(
                        f"{store_label}: "
                        f"{len(context.date_pages)}日分のうち"
                        f"{len(skipped_dates)}日を日付ごとスキップ"
                    ),
                ),
            )
        )

        def step_callback(message: str) -> None:
            nonlocal current_step, total_steps
            self._raise_if_fetch_cancelled()
            current_step += 1
            total_steps = max(total_steps, current_step)
            self.result_queue.put(
                (
                    "fetch_progress",
                    FetchProgress(
                        current_step=current_step,
                        total_steps=total_steps,
                        message=f"{store_label}: {message}",
                    ),
                )
            )

        datasets: list[MachineDataset] = []
        skipped_targets: list[tuple[str, str]] = []
        save_summary: PersistenceSummary | None = None

        for date_index, date_page in enumerate(pending_date_pages, start=1):
            if self.fetch_cancel_event.is_set():
                if datasets or save_summary is not None:
                    break
                raise FetchCancelled
            day_result = self._run_with_fetch_retries(
                lambda: self.scraper.fetch_all_machine_history_for_date_page(
                    context=context,
                    date_page=date_page,
                    step_callback=step_callback,
                    date_index=date_index,
                    total_dates=len(pending_date_pages),
                ),
                retry_delay_seconds=retry_delay_seconds,
                retry_status_callback=lambda retry_number, max_retries, delay_seconds, target_date=date_page.target_date: self.result_queue.put(
                    (
                        "fetch_progress",
                        FetchProgress(
                            current_step=current_step,
                            total_steps=total_steps,
                            message=(
                                f"{store_label}: {target_date} の取得に失敗しました。"
                                f"{delay_seconds}秒後に再試行します（{retry_number}/{max_retries}）"
                            ),
                        ),
                    )
                ),
            )
            datasets.extend(day_result.datasets)
            skipped_targets.extend(day_result.skipped_targets)

            if day_result.datasets:
                self._raise_if_fetch_cancelled()
                step_callback(f"{date_page.target_date} の保存中")
                day_save_summary = self.persistence_service.save_history_result(day_result, full_day=True)
                save_summary = self._merge_persistence_summary(save_summary, day_save_summary)
            else:
                step_callback(f"{date_page.target_date} は保存対象なし")

        result = MachineHistoryResult(
            store_name=context.store_name,
            store_url=context.store_url,
            start_date=context.start_date,
            end_date=context.end_date,
            date_pages=pending_date_pages,
            datasets=datasets,
            skipped_targets=skipped_targets,
            skipped_dates=skipped_dates,
        )
        return StoreFetchResult(
            history_result=result,
            save_summary=save_summary,
            saved_full_day_summary=saved_full_day_summary,
        )

    def _poll_queue(self) -> None:
        try:
            kind, payload = self.result_queue.get_nowait()
        except queue.Empty:
            if self.is_busy:
                self.root.after(100, self._poll_queue)
            return

        if kind == "fetch_progress":
            if isinstance(payload, FetchProgress):
                self._apply_fetch_progress(payload)
            if self.is_busy:
                self.root.after(100, self._poll_queue)
            return

        operation_kind = self.active_operation_kind
        self.is_busy = False
        self.active_operation_kind = ""
        if operation_kind in {"fetch", "scheduled_fetch", "site7_fetch"}:
            self.fetch_cancel_event.clear()
        self._update_button_states()

        if kind == "site7_login_error":
            self.site7_status_var.set("ログイン未完了")
            self.status_var.set("待機中")
            self._show_error(payload)
            return

        if kind == "site7_login_success":
            self.site7_status_var.set("ログイン情報を保存しました")
            self.status_var.set("待機中")
            messagebox.showinfo("サイトセブン", "サイトセブンのログイン状態を確認して保存しました。次回以降は再ログインを省ける場合があります。")
            return

        if kind == "register_store_error":
            self.register_store_status_var.set("店舗登録に失敗しました")
            self._show_error(payload)
            return

        if kind == "register_store_success":
            if (
                not isinstance(payload, tuple)
                or len(payload) != 6
                or not isinstance(payload[0], str)
                or not isinstance(payload[1], str)
            ):
                messagebox.showerror("エラー", "登録店舗の形式が不正です。")
                return
            (
                store_name,
                store_url,
                site7_enabled,
                site7_prefecture,
                site7_area,
                site7_store_name,
            ) = payload
            self._apply_registered_store(
                store_name,
                store_url,
                bool(site7_enabled),
                str(site7_prefecture),
                str(site7_area),
                str(site7_store_name),
            )
            return

        if kind == "update_registered_store_error":
            self.register_store_status_var.set("店舗更新に失敗しました")
            self._show_error(payload)
            return

        if kind == "update_registered_store_success":
            if (
                not isinstance(payload, tuple)
                or len(payload) != 7
                or not isinstance(payload[0], str)
                or not isinstance(payload[1], str)
                or not isinstance(payload[2], str)
            ):
                messagebox.showerror("エラー", "更新店舗の形式が不正です。")
                return
            (
                original_store_url,
                store_name,
                store_url,
                site7_enabled,
                site7_prefecture,
                site7_area,
                site7_store_name,
            ) = payload
            original_store = next(
                (
                    registered_store
                    for registered_store in self.registered_stores
                    if normalize_store_url(registered_store.url) == normalize_store_url(original_store_url)
                ),
                None,
            )
            if original_store is None:
                messagebox.showerror("エラー", "更新対象の店舗が見つかりませんでした。")
                return
            self._replace_registered_store_entry(
                original_store=original_store,
                store_name=store_name,
                store_url=store_url,
                site7_enabled=bool(site7_enabled),
                site7_prefecture=str(site7_prefecture),
                site7_area=str(site7_area),
                site7_store_name=str(site7_store_name),
            )
            return

        if kind == "refresh_registered_stores_error":
            self.register_store_status_var.set("登録店舗の更新に失敗しました")
            messagebox.showerror("登録店舗", f"登録店舗の更新に失敗しました。\n{payload}")
            return

        if kind == "refresh_registered_stores_success":
            if not isinstance(payload, StoreRefreshResult):
                self.register_store_status_var.set("登録店舗の更新に失敗しました")
                messagebox.showerror("登録店舗", "登録店舗の形式が不正です。")
                return
            self._replace_registered_stores(payload.registered_stores, select_all=False)
            self.register_store_status_var.set(f"{len(payload.registered_stores)}店舗を読み込みました")
            if payload.save_summary is not None and payload.save_summary.has_errors:
                messagebox.showwarning("登録店舗", "\n\n".join(payload.save_summary.messages))
            return

        if kind == "delete_registered_stores_error":
            self.register_store_status_var.set("登録店舗の削除に失敗しました")
            messagebox.showerror("登録店舗", f"登録店舗の削除に失敗しました。\n{payload}")
            return

        if kind == "delete_registered_stores_success":
            if not isinstance(payload, StoreDeleteResult):
                self.register_store_status_var.set("登録店舗の削除に失敗しました")
                messagebox.showerror("登録店舗", "削除結果の形式が不正です。")
                return
            self._replace_registered_stores(payload.registered_stores, select_all=False)
            if payload.deleted_store_count > 0:
                self.register_store_status_var.set(f"{payload.deleted_store_count}店舗を削除しました")
            else:
                self.register_store_status_var.set("削除結果を反映しました")
            return

        if kind == "fetch_error":
            self._finish_fetch_progress(success=False, message="取得失敗")
            self.status_var.set("失敗")
            self.summary_var.set("取得できませんでした")
            if operation_kind == "scheduled_fetch":
                self.schedule_status_var.set("定期実行に失敗しました")
            self._show_error(payload)
            return

        if kind == "fetch_cancelled":
            self._finish_fetch_progress(success=False, message="中止しました")
            self.status_var.set("中止")
            self.summary_var.set("取得を中止しました")
            if operation_kind == "scheduled_fetch":
                self.schedule_status_var.set("定期実行を中止しました")
            return

        if kind == "fetch_many_success":
            if not isinstance(payload, FetchManyResult) or not payload.results:
                self._finish_fetch_progress(success=False, message="取得失敗")
                self.status_var.set("失敗")
                self.summary_var.set("不明な結果")
                messagebox.showerror("エラー", "取得結果の形式が不正です。")
                return
            self._apply_fetch_many_result(payload)
            return

        if kind == "scheduled_fetch_many_success":
            if (
                not isinstance(payload, tuple)
                or len(payload) != 2
                or not isinstance(payload[0], StoreRefreshResult)
                or not isinstance(payload[1], FetchManyResult)
            ):
                self._finish_fetch_progress(success=False, message="取得失敗")
                self.status_var.set("失敗")
                self.summary_var.set("不明な結果")
                self.schedule_status_var.set("定期実行に失敗しました")
                messagebox.showerror("エラー", "定期実行の結果形式が不正です。")
                return
            refresh_result, fetch_many_result = payload
            self._replace_registered_stores(refresh_result.registered_stores, select_all=True, reset_fetch_display=False)
            self._apply_fetch_many_result(fetch_many_result)
            if refresh_result.save_summary is not None and refresh_result.save_summary.has_errors:
                messagebox.showwarning("登録店舗", "\n\n".join(refresh_result.save_summary.messages))
            if fetch_many_result.cancelled:
                self.schedule_status_var.set("定期実行を中止しました")
            else:
                self.schedule_status_var.set(f"定期実行完了: 毎日 {self.scheduled_fetch_hour} 時")
            return

        history_result = payload
        save_summary: PersistenceSummary | None = None
        saved_full_day_summary = SavedFullDayDatesSummary()
        if (
            isinstance(payload, tuple)
            and len(payload) == 3
            and isinstance(payload[0], MachineHistoryResult)
            and (isinstance(payload[1], PersistenceSummary) or payload[1] is None)
            and isinstance(payload[2], SavedFullDayDatesSummary)
        ):
            history_result = payload[0]
            save_summary = payload[1]
            saved_full_day_summary = payload[2]
        if not isinstance(history_result, MachineHistoryResult):
            self._finish_fetch_progress(success=False, message="取得失敗")
            self.status_var.set("失敗")
            self.summary_var.set("不明な結果")
            messagebox.showerror("エラー", "取得結果の形式が不正です。")
            return

        self.current_history_result = history_result
        self.current_results = history_result.datasets
        if self.skip_comparison_display_var.get():
            self._clear_comparison_table()
            self._apply_comparison_focus_mode()
        else:
            self._populate_comparison_table(history_result)
        store_name = history_result.store_name
        self._finish_fetch_progress(
            success=True,
            message="取得完了（保存に注意）" if save_summary is not None and save_summary.has_errors else "取得完了",
        )
        if save_summary is not None and save_summary.has_errors:
            self.status_var.set("完了（保存に注意）")
        elif not history_result.datasets and (history_result.skipped_targets or history_result.skipped_dates):
            self.status_var.set("完了（取得済みをスキップ）")
        else:
            self.status_var.set("完了")
        skipped_count = len(history_result.skipped_targets)
        skipped_date_count = len(history_result.skipped_dates)
        fetched_machine_count = len({dataset.machine_name for dataset in history_result.datasets})
        self.summary_var.set(
            f"{store_name} / {history_result.start_date} ～ {history_result.end_date} / "
            f"{fetched_machine_count}機種 / {len(history_result.date_pages)}日取得 / "
            f"{self._save_status_text(save_summary)}"
            f"{f' / 日付スキップ{skipped_date_count}日' if skipped_date_count else ''}"
            f"{f' / スキップ{skipped_count}件' if skipped_count else ''}"
            f"{' / 表表示省略' if self.skip_comparison_display_var.get() else ''}"
        )
        self._update_button_states()
        self._notify_fetch_complete()
        warning_messages = list(saved_full_day_summary.messages)
        if save_summary is not None and save_summary.has_errors:
            warning_messages.extend(save_summary.messages)
        if warning_messages:
            messagebox.showwarning("自動処理", "\n\n".join(warning_messages))

    def _apply_fetch_many_result(self, fetch_many_result: FetchManyResult) -> None:
        last_store_result = fetch_many_result.results[-1]
        history_result = last_store_result.history_result

        self.current_history_result = history_result
        self.current_results = history_result.datasets
        if self.skip_comparison_display_var.get():
            self._clear_comparison_table()
            self._apply_comparison_focus_mode()
        else:
            self._populate_comparison_table(history_result)

        has_save_errors = any(
            store_result.save_summary is not None and store_result.save_summary.has_errors
            for store_result in fetch_many_result.results
        )
        all_skipped = all(
            not store_result.history_result.datasets
            and (store_result.history_result.skipped_targets or store_result.history_result.skipped_dates)
            for store_result in fetch_many_result.results
        )

        if fetch_many_result.cancelled:
            finish_message = "中止しました（保存に注意）" if has_save_errors else "中止しました"
        else:
            finish_message = "取得完了（保存に注意）" if has_save_errors else "取得完了"

        self._finish_fetch_progress(success=True, message=finish_message)
        if fetch_many_result.cancelled:
            self.status_var.set("中止（一部取得済み）")
        elif fetch_many_result.failures:
            self.status_var.set("完了（一部失敗）")
        elif has_save_errors:
            self.status_var.set("完了（保存に注意）")
        elif all_skipped:
            self.status_var.set("完了（取得済みをスキップ）")
        else:
            self.status_var.set("完了")

        self.summary_var.set(self._fetch_many_summary_text(fetch_many_result))
        self._update_button_states()
        if not fetch_many_result.cancelled:
            self._notify_fetch_complete()

        warning_messages: list[str] = []
        for store_result in fetch_many_result.results:
            warning_messages.extend(store_result.saved_full_day_summary.messages)
            if store_result.save_summary is not None and store_result.save_summary.has_errors:
                warning_messages.extend(store_result.save_summary.messages)
        for failure in fetch_many_result.failures:
            warning_messages.append(f"{failure.store.name} の取得に失敗しました。\n{failure.error}")
        if warning_messages:
            messagebox.showwarning("自動処理", "\n\n".join(warning_messages))

    def _fetch_many_summary_text(self, fetch_many_result: FetchManyResult) -> str:
        if len(fetch_many_result.results) == 1 and not fetch_many_result.failures:
            store_result = fetch_many_result.results[0]
            return self._single_fetch_summary_text(store_result.history_result, store_result.save_summary)

        first_history_result = fetch_many_result.results[0].history_result
        last_history_result = fetch_many_result.results[-1].history_result
        fetched_machine_count = sum(
            len({dataset.machine_name for dataset in store_result.history_result.datasets})
            for store_result in fetch_many_result.results
        )
        fetched_day_count = sum(
            len(store_result.history_result.date_pages)
            for store_result in fetch_many_result.results
        )
        skipped_count = sum(
            len(store_result.history_result.skipped_targets)
            for store_result in fetch_many_result.results
        )
        skipped_date_count = sum(
            len(store_result.history_result.skipped_dates)
            for store_result in fetch_many_result.results
        )
        failed_text = f" / 失敗{len(fetch_many_result.failures)}店舗" if fetch_many_result.failures else ""
        cancelled_text = " / 中止" if fetch_many_result.cancelled else ""
        display_text = (
            " / 表表示省略"
            if self.skip_comparison_display_var.get()
            else f" / 表表示は{last_history_result.store_name}"
        )
        return (
            f"{len(fetch_many_result.results)}店舗完了{failed_text}{cancelled_text} / "
            f"{first_history_result.start_date} ～ {first_history_result.end_date} / "
            f"{fetched_machine_count}機種 / {fetched_day_count}日取得 / "
            f"{self._many_save_status_text(fetch_many_result.results)}"
            f"{f' / 日付スキップ{skipped_date_count}日' if skipped_date_count else ''}"
            f"{f' / スキップ{skipped_count}件' if skipped_count else ''}"
            f"{display_text}"
        )

    def _single_fetch_summary_text(
        self,
        history_result: MachineHistoryResult,
        save_summary: PersistenceSummary | None,
    ) -> str:
        skipped_count = len(history_result.skipped_targets)
        skipped_date_count = len(history_result.skipped_dates)
        fetched_machine_count = len({dataset.machine_name for dataset in history_result.datasets})
        return (
            f"{history_result.store_name} / {history_result.start_date} ～ {history_result.end_date} / "
            f"{fetched_machine_count}機種 / {len(history_result.date_pages)}日取得 / "
            f"{self._save_status_text(save_summary)}"
            f"{f' / 日付スキップ{skipped_date_count}日' if skipped_date_count else ''}"
            f"{f' / スキップ{skipped_count}件' if skipped_count else ''}"
            f"{' / 表表示省略' if self.skip_comparison_display_var.get() else ''}"
        )

    def _many_save_status_text(self, store_results: list[StoreFetchResult]) -> str:
        save_summaries = [store_result.save_summary for store_result in store_results]
        if any(save_summary is not None and save_summary.has_errors for save_summary in save_summaries):
            return "保存に注意"

        if any(
            save_summary is not None
            and (save_summary.local_file_path or save_summary.supabase_saved)
            for save_summary in save_summaries
        ):
            return "保存あり"

        return "保存なし"

    def _populate_comparison_table(self, history_result: MachineHistoryResult) -> None:
        self.comparison_sort_key = "日付"
        self.comparison_sort_descending = False
        self.comparison_slot_numbers = []
        self.comparison_rows = []

        row_map_by_date: dict[str, dict[str, str]] = {
            date_page.target_date: {"日付": date_page.target_date}
            for date_page in history_result.date_pages
        }
        slot_numbers: set[str] = set()

        for result in history_result.datasets:
            source_columns = [column for column in result.columns if not self._is_machine_name_column(column)]
            for row in result.rows:
                values = self._filter_machine_name_values(result.columns, row)
                machine_row = dict(zip(source_columns, values, strict=False))
                slot_number = machine_row.get("台番", "")
                if not slot_number:
                    continue

                slot_numbers.add(slot_number)
                target_row = row_map_by_date.setdefault(result.target_date, {"日付": result.target_date})
                for subcolumn in COMPARISON_SUBCOLUMNS:
                    key = self._comparison_key(slot_number, subcolumn)
                    if subcolumn == "機種名":
                        target_row[key] = result.machine_name
                    elif subcolumn in machine_row:
                        target_row[key] = machine_row[subcolumn]

        self.comparison_slot_numbers = sorted(slot_numbers, key=self._slot_sort_key)
        self.comparison_rows = [row_map_by_date[date_page.target_date] for date_page in history_result.date_pages]
        available_dates = {row.get("日付", "") for row in self.comparison_rows}
        if self.comparison_selected_date not in available_dates:
            self.comparison_selected_date = None
        self._refresh_comparison_table(preserve_scroll=False)

    def _refresh_comparison_table(self, preserve_scroll: bool = True) -> None:
        x_position = self.comparison_body_canvas.canvasx(0) if preserve_scroll else 0.0
        y_position = self.comparison_body_canvas.canvasy(0) if preserve_scroll else 0.0
        self.comparison_display_rows = self._sorted_comparison_rows()
        self._clear_comparison_table(reset_view=not preserve_scroll)
        self._draw_comparison_headers()
        self._draw_comparison_body()
        self._update_comparison_scrollregion()
        self._restore_comparison_view(x_position if preserve_scroll else 0.0, y_position if preserve_scroll else 0.0)

    def _draw_comparison_headers(self) -> None:
        date_width = self._comparison_width("日付")
        total_height = self._comparison_header_total_height()
        subheader_y = self._comparison_header_group_height()

        self._draw_canvas_box(
            self.comparison_fixed_header_canvas,
            0,
            0,
            date_width,
            total_height,
            self._heading_text("日付", self.comparison_sort_key, self.comparison_sort_descending),
            "#eaeaea",
            "center",
        )

        self.comparison_header_click_regions = []
        x_position = 0
        for slot_number in self.comparison_slot_numbers:
            group_width = sum(self._comparison_width(subcolumn) for subcolumn in COMPARISON_SUBCOLUMNS)
            self._draw_canvas_box(
                self.comparison_header_canvas,
                x_position,
                0,
                x_position + group_width,
                subheader_y,
                slot_number,
                "#eaeaea",
                "center",
            )

            subcolumn_x = x_position
            for subcolumn in COMPARISON_SUBCOLUMNS:
                column_width = self._comparison_width(subcolumn)
                sort_key = self._comparison_key(slot_number, subcolumn)
                header_text = self._heading_text(subcolumn, sort_key, self.comparison_sort_descending) if self.comparison_sort_key == sort_key else subcolumn
                self._draw_canvas_box(
                    self.comparison_header_canvas,
                    subcolumn_x,
                    subheader_y,
                    subcolumn_x + column_width,
                    total_height,
                    header_text,
                    "#eaeaea",
                    "center",
                )
                self.comparison_header_click_regions.append(
                    (subcolumn_x, subheader_y, subcolumn_x + column_width, total_height, sort_key)
                )
                subcolumn_x += column_width

            x_position += group_width

    def _draw_comparison_body(self) -> None:
        date_width = self._comparison_width("日付")
        row_height = self._comparison_body_row_height()
        body_width = self._comparison_body_total_width()
        flat_columns = self._comparison_flat_columns()

        for row_index, row in enumerate(self.comparison_display_rows):
            row_date = row.get("日付", "")
            row_background = self._comparison_row_background(row_index, row_date)
            y0 = row_index * row_height
            y1 = y0 + row_height

            self.comparison_fixed_body_canvas.create_rectangle(0, y0, date_width, y1, fill=row_background, outline="")
            self._draw_canvas_text(
                self.comparison_fixed_body_canvas,
                0,
                y0,
                date_width,
                y1,
                row_date,
                "center",
            )

            self.comparison_body_canvas.create_rectangle(0, y0, body_width, y1, fill=row_background, outline="")
            x0 = 0
            for slot_number, subcolumn in flat_columns:
                column_width = self._comparison_width(subcolumn)
                self._draw_canvas_text(
                    self.comparison_body_canvas,
                    x0,
                    y0,
                    x0 + column_width,
                    y1,
                    row.get(self._comparison_key(slot_number, subcolumn), ""),
                    "center" if subcolumn != "機種名" else "w",
                )
                x0 += column_width

        body_height = len(self.comparison_display_rows) * row_height
        self._draw_canvas_grid(self.comparison_fixed_body_canvas, [0, date_width], body_height, row_height)
        self._draw_canvas_grid(
            self.comparison_body_canvas,
            self._comparison_body_boundaries(),
            body_height,
            row_height,
        )

    def _draw_canvas_box(
        self,
        canvas: tk.Canvas,
        x0: int,
        y0: int,
        x1: int,
        y1: int,
        text: str,
        background: str,
        anchor: str,
    ) -> None:
        canvas.create_rectangle(x0, y0, x1, y1, fill=background, outline="#2b2b2b")
        self._draw_canvas_text(canvas, x0, y0, x1, y1, text, anchor)

    def _draw_canvas_text(
        self,
        canvas: tk.Canvas,
        x0: int,
        y0: int,
        x1: int,
        y1: int,
        text: str,
        anchor: str,
    ) -> None:
        max_width = max(0, x1 - x0 - 8)
        display_text = self._clip_comparison_text(text, max_width)
        text_x = x0 + 4 if anchor == "w" else (x0 + x1) / 2
        canvas.create_text(
            text_x,
            (y0 + y1) / 2,
            text=display_text,
            anchor=anchor,
            font=self.default_font,
        )

    def _draw_canvas_grid(
        self,
        canvas: tk.Canvas,
        boundaries: list[int],
        body_height: int,
        row_height: int,
    ) -> None:
        if not boundaries:
            return

        x_start = boundaries[0]
        x_end = boundaries[-1]
        for boundary in boundaries:
            canvas.create_line(boundary, 0, boundary, body_height, fill="#2b2b2b")
        for row_index in range(len(self.comparison_display_rows) + 1):
            y_position = row_index * row_height
            canvas.create_line(x_start, y_position, x_end, y_position, fill="#2b2b2b")

    def _on_comparison_fixed_header_click(self, event: tk.Event[tk.Misc]) -> None:
        if 0 <= event.x <= self._comparison_width("日付") and 0 <= event.y <= self._comparison_header_total_height():
            self._sort_comparison_table("日付")

    def _on_comparison_header_click(self, event: tk.Event[tk.Misc]) -> None:
        x_position = int(self.comparison_header_canvas.canvasx(event.x))
        y_position = int(self.comparison_header_canvas.canvasy(event.y))
        for x0, y0, x1, y1, sort_key in self.comparison_header_click_regions:
            if x0 <= x_position <= x1 and y0 <= y_position <= y1:
                self._sort_comparison_table(sort_key)
                return

    def _on_comparison_fixed_body_click(self, event: tk.Event[tk.Misc]) -> None:
        if not self.comparison_display_rows:
            return
        y_position = self.comparison_fixed_body_canvas.canvasy(event.y)
        row_index = int(y_position // self._comparison_body_row_height())
        if 0 <= row_index < len(self.comparison_display_rows):
            target_date = self.comparison_display_rows[row_index].get("日付", "")
            self._select_comparison_date(target_date)

    def _sort_comparison_table(self, key: str) -> None:
        if self.comparison_sort_key == key:
            self.comparison_sort_descending = not self.comparison_sort_descending
        else:
            self.comparison_sort_key = key
            self.comparison_sort_descending = False
        self._refresh_comparison_table()

    def _on_comparison_day_tail_changed(self, _: tk.Event[tk.Misc]) -> None:
        self._refresh_comparison_table(preserve_scroll=False)

    def _select_comparison_date(self, target_date: str) -> None:
        if not target_date:
            return
        self.comparison_selected_date = target_date
        self._refresh_comparison_table()

    def _sorted_comparison_rows(self) -> list[dict[str, str]]:
        filtered_rows = [
            row
            for row in self.comparison_rows
            if matches_day_tail(row.get("日付", ""), self.comparison_day_tail_var.get())
        ]
        return self._sort_records(
            filtered_rows,
            value_getter=lambda row: row.get(self.comparison_sort_key, ""),
            descending=self.comparison_sort_descending,
        )

    def _clear_comparison_table(self, reset_view: bool = True) -> None:
        self.comparison_fixed_header_canvas.delete("all")
        self.comparison_header_canvas.delete("all")
        self.comparison_fixed_body_canvas.delete("all")
        self.comparison_body_canvas.delete("all")
        self.comparison_header_click_regions = []
        self.comparison_fixed_header_canvas.configure(scrollregion=(0, 0, 0, 0))
        self.comparison_fixed_body_canvas.configure(scrollregion=(0, 0, 0, 0))
        self.comparison_header_canvas.configure(scrollregion=(0, 0, 0, 0))
        self.comparison_body_canvas.configure(scrollregion=(0, 0, 0, 0))
        if reset_view:
            self.comparison_y_scrollbar.set(0, 1)

    def _refresh_registered_store_table(self) -> None:
        self.registered_store_tree.delete(*self.registered_store_tree.get_children())
        for index, registered_store in enumerate(self.registered_stores):
            self.registered_store_tree.insert(
                "",
                "end",
                iid=f"registered_store_{index}",
                values=(
                    self._registered_store_target_marker(registered_store),
                    self._registered_store_site7_marker(registered_store),
                    self._registered_store_display_name(registered_store),
                    registered_store.url,
                    registered_store.site7_prefecture,
                    registered_store.site7_area,
                    registered_store.resolved_site7_store_name(),
                ),
            )
        self._update_button_states()

    def _replace_registered_stores(
        self,
        registered_stores: list[RegisteredStore],
        select_all: bool,
        reset_fetch_display: bool = True,
    ) -> None:
        previous_urls = {
            normalize_store_url(registered_store.url)
            for registered_store in self.registered_stores
        }
        previous_selected_urls = set(self.selected_store_urls)
        next_urls = {
            normalize_store_url(registered_store.url)
            for registered_store in registered_stores
        }

        self.registered_stores = registered_stores
        if select_all:
            self.selected_store_urls = next_urls
        else:
            new_urls = next_urls - previous_urls
            self.selected_store_urls = {
                store_url
                for store_url in next_urls
                if store_url in previous_selected_urls or store_url in new_urls
            }

        self._refresh_registered_store_table()
        if reset_fetch_display:
            self._reset_fetch_display_for_store_change()

    def _registered_store_target_marker(self, registered_store: RegisteredStore) -> str:
        return "☑" if normalize_store_url(registered_store.url) in self.selected_store_urls else "☐"

    def _registered_store_site7_marker(self, registered_store: RegisteredStore) -> str:
        return "☑" if registered_store.site7_enabled else "☐"

    def _registered_store_display_name(self, registered_store: RegisteredStore) -> str:
        return registered_store.name.strip() or "（店舗名未取得）"

    def _load_registered_store_form(self, registered_store: RegisteredStore) -> None:
        self.register_store_url_var.set(registered_store.url)
        self.register_store_site7_enabled_var.set(registered_store.site7_enabled)
        self.register_store_prefecture_var.set(registered_store.site7_prefecture or DEFAULT_SITE7_PREFECTURE_NAME)
        self.register_store_area_var.set(registered_store.site7_area)
        self.register_store_site7_store_name_var.set(registered_store.resolved_site7_store_name())
        self.register_store_status_var.set(f"{self._registered_store_display_name(registered_store)} を編集中")

    def _selected_registered_stores(self) -> list[RegisteredStore]:
        return [
            registered_store
            for registered_store in self.registered_stores
            if normalize_store_url(registered_store.url) in self.selected_store_urls
        ]

    def _selected_registered_store_rows(self) -> list[RegisteredStore]:
        return [
            registered_store
            for item_id in self.registered_store_tree.selection()
            if (registered_store := self._registered_store_from_item_id(item_id)) is not None
        ]

    def _registered_store_from_item_id(self, item_id: str) -> RegisteredStore | None:
        prefix = "registered_store_"
        if not item_id.startswith(prefix):
            return None

        index_text = item_id[len(prefix):]
        if not index_text.isdigit():
            return None

        index = int(index_text)
        if index < 0 or index >= len(self.registered_stores):
            return None

        return self.registered_stores[index]

    def _confirm_registered_store_deletion(self, registered_stores: list[RegisteredStore]) -> bool:
        store_lines = [
            self._registered_store_display_name(registered_store)
            for registered_store in registered_stores[:5]
        ]
        if len(registered_stores) > 5:
            store_lines.append(f"ほか {len(registered_stores) - 5} 店舗")

        return messagebox.askyesno(
            "登録店舗",
            (
                "選択した店舗を Supabase から削除します。\n"
                "保存済みの台データも合わせて削除します。\n"
                "ローカル保存ファイルは削除しません。\n\n"
                + "\n".join(store_lines)
            ),
        )

    def _on_registered_store_selection_changed(self, _: tk.Event[tk.Misc]) -> None:
        selected_rows = self._selected_registered_store_rows()
        if len(selected_rows) == 1:
            self._load_registered_store_form(selected_rows[0])
        self._update_button_states()

    def _on_registered_store_tree_click(self, event: tk.Event[tk.Misc]) -> str | None:
        if self.is_busy:
            return None

        if self.registered_store_tree.identify_region(event.x, event.y) != "cell":
            return None

        column_id = self.registered_store_tree.identify_column(event.x)
        if column_id not in {"#1", "#2"}:
            return None

        item_id = self.registered_store_tree.identify_row(event.y)
        if not item_id:
            return None

        if column_id == "#1":
            self._toggle_registered_store_target(item_id)
        else:
            self._toggle_registered_store_site7(item_id)
        return "break"

    def _toggle_registered_store_target(self, item_id: str) -> None:
        prefix = "registered_store_"
        if not item_id.startswith(prefix):
            return

        index_text = item_id[len(prefix):]
        if not index_text.isdigit():
            return

        index = int(index_text)
        if index < 0 or index >= len(self.registered_stores):
            return

        registered_store = self.registered_stores[index]
        normalized_url = normalize_store_url(registered_store.url)
        if normalized_url in self.selected_store_urls:
            self.selected_store_urls.remove(normalized_url)
        else:
            self.selected_store_urls.add(normalized_url)

        self.registered_store_tree.set(item_id, "取得対象", self._registered_store_target_marker(registered_store))
        self._reset_fetch_display_for_store_change()

    def _toggle_registered_store_site7(self, item_id: str) -> None:
        registered_store = self._registered_store_from_item_id(item_id)
        if registered_store is None:
            return

        registered_store.site7_enabled = not registered_store.site7_enabled
        self.registered_store_tree.set(item_id, "サイトセブン", self._registered_store_site7_marker(registered_store))
        save_summary = self._persist_registered_stores()
        if save_summary.has_errors:
            messagebox.showwarning("登録店舗", "\n\n".join(save_summary.messages))
        self._load_registered_store_form(registered_store)
        self._update_button_states()

    def _select_all_registered_stores(self) -> None:
        self.selected_store_urls = {
            normalize_store_url(registered_store.url)
            for registered_store in self.registered_stores
        }
        self._refresh_registered_store_table()
        self._reset_fetch_display_for_store_change()

    def _clear_registered_store_selection(self) -> None:
        self.selected_store_urls.clear()
        self._refresh_registered_store_table()
        self._reset_fetch_display_for_store_change()

    def _validated_register_store_form_input(self) -> tuple[str, bool, str, str, str]:
        store_url = self.register_store_url_var.get().strip()
        site7_enabled = bool(self.register_store_site7_enabled_var.get())
        site7_prefecture = self.register_store_prefecture_var.get().strip() or DEFAULT_SITE7_PREFECTURE_NAME
        site7_area = self.register_store_area_var.get().strip()
        site7_store_name = self.register_store_site7_store_name_var.get().strip()

        if not store_url:
            raise ScraperError("店舗URLを入力してください。")
        if not self._is_valid_url(store_url):
            raise ScraperError("店舗URLは http:// または https:// から入力してください。")
        if site7_enabled and not site7_area:
            raise ScraperError("サイトセブン取得を使う場合は地域を入力してください。")

        return store_url, site7_enabled, site7_prefecture, site7_area, site7_store_name

    def _build_registered_store(
        self,
        store_name: str,
        store_url: str,
        site7_enabled: bool | None = None,
        site7_prefecture: str = "",
        site7_area: str = "",
        site7_store_name: str = "",
    ) -> RegisteredStore:
        defaults = default_site7_store_settings(store_name)
        resolved_site7_enabled = defaults["site7_enabled"] if site7_enabled is None else bool(site7_enabled)
        resolved_site7_prefecture = site7_prefecture.strip() or str(defaults["site7_prefecture"]).strip() or DEFAULT_SITE7_PREFECTURE_NAME
        resolved_site7_area = site7_area.strip() or str(defaults["site7_area"]).strip()
        resolved_site7_store_name = site7_store_name.strip() or str(defaults["site7_store_name"]).strip() or store_name.strip()
        return RegisteredStore(
            name=store_name,
            url=normalize_store_url(store_url),
            site7_enabled=bool(resolved_site7_enabled),
            site7_prefecture=resolved_site7_prefecture,
            site7_area=resolved_site7_area,
            site7_store_name=resolved_site7_store_name,
        )

    def _apply_registered_store(
        self,
        store_name: str,
        store_url: str,
        site7_enabled: bool = False,
        site7_prefecture: str = DEFAULT_SITE7_PREFECTURE_NAME,
        site7_area: str = "",
        site7_store_name: str = "",
    ) -> None:
        normalized_name = normalize_text(store_name)
        normalized_url = normalize_store_url(store_url)
        for registered_store in self.registered_stores:
            if normalize_text(registered_store.name) == normalized_name or normalize_store_url(registered_store.url) == normalized_url:
                messagebox.showwarning("重複", "同じ店舗名またはURLがすでに登録されています。")
                self.register_store_status_var.set("登録済みの店舗です")
                return

        registered_store = self._build_registered_store(
            store_name=store_name,
            store_url=normalized_url,
            site7_enabled=site7_enabled,
            site7_prefecture=site7_prefecture,
            site7_area=site7_area,
            site7_store_name=site7_store_name,
        )
        self.registered_stores.append(registered_store)
        self.selected_store_urls.add(normalized_url)
        self.clear_register_store_form()
        self._refresh_registered_store_table()
        save_summary = self._persist_registered_stores()
        if save_summary.has_errors:
            self.register_store_status_var.set(f"{store_name} を登録しました（保存に注意）")
            messagebox.showwarning("登録店舗", "\n\n".join(save_summary.messages))
            return

        self.register_store_status_var.set(f"{store_name} を登録しました")

    def _replace_registered_store_entry(
        self,
        original_store: RegisteredStore,
        store_name: str,
        store_url: str,
        site7_enabled: bool,
        site7_prefecture: str,
        site7_area: str,
        site7_store_name: str,
    ) -> None:
        normalized_name = normalize_text(store_name)
        normalized_url = normalize_store_url(store_url)
        for registered_store in self.registered_stores:
            if registered_store is original_store:
                continue
            if normalize_text(registered_store.name) == normalized_name or normalize_store_url(registered_store.url) == normalized_url:
                messagebox.showwarning("重複", "同じ店舗名またはURLがすでに登録されています。")
                self.register_store_status_var.set("登録済みの店舗です")
                return

        updated_store = self._build_registered_store(
            store_name=store_name,
            store_url=store_url,
            site7_enabled=site7_enabled,
            site7_prefecture=site7_prefecture,
            site7_area=site7_area,
            site7_store_name=site7_store_name,
        )
        updated_registered_stores = [
            updated_store if registered_store is original_store else registered_store
            for registered_store in self.registered_stores
        ]
        previously_selected = normalize_store_url(original_store.url) in self.selected_store_urls
        if previously_selected:
            self.selected_store_urls.discard(normalize_store_url(original_store.url))
            self.selected_store_urls.add(normalize_store_url(updated_store.url))
        self.registered_stores = updated_registered_stores
        self._refresh_registered_store_table()
        save_summary = self._persist_registered_stores()
        if save_summary.has_errors:
            self.register_store_status_var.set(f"{store_name} を更新しました（保存に注意）")
            messagebox.showwarning("登録店舗", "\n\n".join(save_summary.messages))
            return

        self.register_store_status_var.set(f"{store_name} を更新しました")

    def _selected_site7_registered_stores(self) -> list[RegisteredStore]:
        target_stores = [registered_store for registered_store in self.registered_stores if registered_store.site7_enabled]
        invalid_stores = [
            registered_store.name
            for registered_store in target_stores
            if not registered_store.site7_area.strip()
        ]
        if invalid_stores:
            raise ScraperError("サイトセブン取得を使う店舗は地域を入力してください。\n" + "\n".join(invalid_stores))
        return target_stores

    def _persist_registered_stores(self) -> RegisteredStoresPersistenceSummary:
        return self._persist_registered_store_list(self.registered_stores)

    def _persist_registered_store_list(self, registered_stores: list[RegisteredStore]) -> RegisteredStoresPersistenceSummary:
        store_payloads = [
            {
                "store_name": registered_store.name,
                "store_url": registered_store.url,
                "site7_enabled": registered_store.site7_enabled,
                "site7_prefecture": registered_store.site7_prefecture,
                "site7_area": registered_store.site7_area,
                "site7_store_name": registered_store.resolved_site7_store_name(),
            }
            for registered_store in registered_stores
        ]
        return self.persistence_service.save_registered_stores(store_payloads)

    def toggle_comparison_focus(self) -> None:
        if self.skip_comparison_display_var.get():
            return
        self.comparison_focus_mode = not self.comparison_focus_mode
        self._apply_comparison_focus_mode()

    def _apply_comparison_focus_mode(self) -> None:
        if self.skip_comparison_display_var.get():
            self.fetch_form.grid()
            self.fetch_info.grid()
            self.comparison_frame.grid_remove()
            self.fetch_tab.rowconfigure(2, weight=0)
            self.comparison_focus_button.configure(text="台データ表を広く表示")
            return

        self.comparison_frame.grid()
        if self.comparison_focus_mode:
            self.fetch_form.grid_remove()
            self.fetch_info.grid_remove()
            self.fetch_tab.rowconfigure(2, weight=1)
            self.comparison_focus_button.configure(text="元に戻す")
        else:
            self.fetch_form.grid()
            self.fetch_info.grid()
            self.fetch_tab.rowconfigure(2, weight=1)
            self.comparison_focus_button.configure(text="台データ表を広く表示")

    def _on_skip_comparison_display_changed(self) -> None:
        if self.skip_comparison_display_var.get():
            self.comparison_focus_mode = False
            self._clear_comparison_table()
        elif self.current_history_result is not None:
            self._populate_comparison_table(self.current_history_result)

        self._apply_comparison_focus_mode()
        self._update_button_states()

    def _reset_fetch_display_for_store_change(self) -> None:
        self.current_results = []
        self.current_history_result = None
        self.comparison_rows = []
        self.comparison_slot_numbers = []
        self.comparison_display_rows = []
        self.comparison_selected_date = None
        self._clear_comparison_table()
        self.summary_var.set("未取得")
        self.status_var.set("待機中")
        self._reset_fetch_progress()
        self._apply_comparison_focus_mode()
        self._update_button_states()

    def _begin_fetch_progress(self, message: str) -> None:
        self.fetch_progress_current = 0
        self.fetch_progress_total = 0
        self.fetch_progress_bar.stop()
        self.fetch_progress_bar.configure(mode="indeterminate", maximum=100)
        self.fetch_progress_value_var.set(0.0)
        self.fetch_progress_text_var.set(message)
        self.fetch_progress_bar.start(12)

    def _apply_fetch_progress(self, progress: FetchProgress) -> None:
        total_steps = max(1, progress.total_steps)
        current_step = min(max(0, progress.current_step), total_steps)
        self.fetch_progress_current = current_step
        self.fetch_progress_total = total_steps
        self.fetch_progress_bar.stop()
        self.fetch_progress_bar.configure(mode="determinate", maximum=100)
        self.fetch_progress_value_var.set(current_step * 100 / total_steps)
        self.fetch_progress_text_var.set(f"{current_step}/{total_steps} {progress.message}")

    def _finish_fetch_progress(self, success: bool, message: str) -> None:
        self.fetch_progress_bar.stop()
        self.fetch_progress_bar.configure(mode="determinate", maximum=100)
        if success:
            total_steps = self.fetch_progress_total or 1
            self.fetch_progress_current = total_steps
            self.fetch_progress_total = total_steps
            self.fetch_progress_value_var.set(100.0)
            self.fetch_progress_text_var.set(f"{total_steps}/{total_steps} {message}")
            return

        self.fetch_progress_current = 0
        self.fetch_progress_total = 0
        self.fetch_progress_value_var.set(0.0)
        self.fetch_progress_text_var.set(message)

    def _reset_fetch_progress(self) -> None:
        self.fetch_progress_bar.stop()
        self.fetch_progress_bar.configure(mode="determinate", maximum=100)
        self.fetch_progress_current = 0
        self.fetch_progress_total = 0
        self.fetch_progress_value_var.set(0.0)
        self.fetch_progress_text_var.set("未開始")

    def _notify_fetch_complete(self) -> None:
        if not self.notify_fetch_complete_var.get():
            return

        if winsound is not None:
            try:
                winsound.MessageBeep(winsound.MB_ICONASTERISK)
                return
            except RuntimeError:
                pass

        try:
            self.root.bell()
        except tk.TclError:
            pass

    def _target_date_input_from_recent_days(self) -> str:
        return build_recent_date_range_input(self.target_date_var.get())

    def _retry_delay_seconds_input(self) -> int:
        return parse_retry_delay_seconds(self.retry_delay_seconds_var.get())

    def _build_table_columns(self, results: list[MachineDataset]) -> list[str]:
        columns = ["機種名"]
        seen_columns = set(columns)

        for result in results:
            for column in result.columns:
                if self._is_machine_name_column(column) or column in seen_columns:
                    continue
                columns.append(column)
                seen_columns.add(column)

        return columns

    def _filter_machine_name_values(self, source_columns: list[str], row: list[str]) -> list[str]:
        values: list[str] = []
        for column, value in zip(source_columns, row, strict=False):
            if self._is_machine_name_column(column):
                continue
            values.append(value)
        return values

    def _is_machine_name_column(self, column: str) -> bool:
        return normalize_text(column) in {"機種", "機種名"}

    def _column_width(self, column: str) -> int:
        if column == "機種名":
            return 320
        if column in {"台番", "差枚", "BB", "RB"}:
            return 90
        if column in {"G数", "出率", "合成", "BB率", "RB率"}:
            return 100
        return 120

    def _comparison_key(self, slot_number: str, subcolumn: str) -> str:
        return f"{slot_number}|{subcolumn}"

    def _comparison_width(self, column: str) -> int:
        widths = {
            "日付": 110,
            "機種名": 120,
            "差枚": 82,
            "G数": 82,
            "出率": 82,
            "BB": 66,
            "RB": 66,
            "合成": 82,
            "BB率": 82,
            "RB率": 82,
        }
        return widths.get(column, 82)

    def _comparison_header_group_height(self) -> int:
        return 26

    def _comparison_header_total_height(self) -> int:
        return 52

    def _comparison_body_row_height(self) -> int:
        return 24

    def _comparison_flat_columns(self) -> list[tuple[str, str]]:
        columns: list[tuple[str, str]] = []
        for slot_number in self.comparison_slot_numbers:
            for subcolumn in COMPARISON_SUBCOLUMNS:
                columns.append((slot_number, subcolumn))
        return columns

    def _comparison_body_total_width(self) -> int:
        return sum(self._comparison_width(subcolumn) for _, subcolumn in self._comparison_flat_columns())

    def _comparison_body_boundaries(self) -> list[int]:
        boundaries = [0]
        current_x = 0
        for _, subcolumn in self._comparison_flat_columns():
            current_x += self._comparison_width(subcolumn)
            boundaries.append(current_x)
        return boundaries

    def _clip_comparison_text(self, text: str, max_width: int) -> str:
        plain_text = str(text)
        cache_key = (plain_text, max_width)
        cached_text = self.comparison_text_cache.get(cache_key)
        if cached_text is not None:
            return cached_text

        if max_width <= 0 or self.default_font.measure(plain_text) <= max_width:
            self.comparison_text_cache[cache_key] = plain_text
            return plain_text

        ellipsis = "..."
        allowed_width = max_width - self.default_font.measure(ellipsis)
        if allowed_width <= 0:
            self.comparison_text_cache[cache_key] = ellipsis
            return ellipsis

        low = 0
        high = len(plain_text)
        while low < high:
            middle = (low + high + 1) // 2
            if self.default_font.measure(plain_text[:middle]) <= allowed_width:
                low = middle
            else:
                high = middle - 1

        clipped_text = plain_text[:low] + ellipsis
        self.comparison_text_cache[cache_key] = clipped_text
        return clipped_text

    def _comparison_row_background(self, row_index: int, row_date: str) -> str:
        if row_date and row_date == self.comparison_selected_date:
            return "#fff2a8"
        return "#ffffff" if row_index % 2 == 0 else "#f7f7f7"

    def _slot_sort_key(self, slot_number: str) -> tuple[int, int | str]:
        normalized = slot_number.replace(",", "").strip()
        if normalized.isdigit():
            return (0, int(normalized))
        return (1, normalize_text(slot_number))

    def _heading_text(self, column: str, current_column: str | None, descending: bool) -> str:
        if column != current_column:
            return column
        return f"{column} {'▼' if descending else '▲'}"

    def _scroll_comparison_x(self, *args: str) -> None:
        self.comparison_header_canvas.xview(*args)
        self.comparison_body_canvas.xview(*args)

    def _scroll_comparison_y(self, *args: str) -> None:
        self.comparison_fixed_body_canvas.yview(*args)
        self.comparison_body_canvas.yview(*args)
        self._update_comparison_y_scrollbar()

    def _on_comparison_mousewheel(self, event: tk.Event[tk.Misc]) -> str:
        if event.delta == 0:
            return "break"
        move = -1 if event.delta > 0 else 1
        self._scroll_comparison_y("scroll", str(move), "units")
        return "break"

    def _update_comparison_scrollregion(self) -> None:
        date_width = self._comparison_width("日付")
        header_height = self._comparison_header_total_height()
        body_height = max(1, len(self.comparison_display_rows) * self._comparison_body_row_height())
        body_width = max(1, self._comparison_body_total_width())

        self.comparison_fixed_header_canvas.configure(width=date_width, height=header_height, scrollregion=(0, 0, date_width, header_height))
        self.comparison_fixed_body_canvas.configure(width=date_width, scrollregion=(0, 0, date_width, body_height))
        self.comparison_header_canvas.configure(height=header_height, scrollregion=(0, 0, body_width, header_height))
        self.comparison_body_canvas.configure(scrollregion=(0, 0, body_width, body_height))
        self._update_comparison_y_scrollbar()

    def _on_comparison_yview_changed(self, first: str, last: str) -> None:
        self.comparison_y_scrollbar.set(float(first), float(last))

    def _update_comparison_y_scrollbar(self) -> None:
        first, last = self.comparison_body_canvas.yview()
        self.comparison_y_scrollbar.set(first, last)

    def _restore_comparison_view(self, x_position: float, y_position: float) -> None:
        body_width = max(1, self._comparison_body_total_width())
        body_height = max(1, len(self.comparison_display_rows) * self._comparison_body_row_height())
        visible_width = max(1, self.comparison_body_canvas.winfo_width())
        visible_height = max(1, self.comparison_body_canvas.winfo_height())

        max_x = max(0, body_width - visible_width)
        max_y = max(0, body_height - visible_height)
        target_x = min(max(0.0, x_position), float(max_x))
        target_y = min(max(0.0, y_position), float(max_y))

        self.comparison_header_canvas.xview_moveto(target_x / body_width if body_width else 0.0)
        self.comparison_body_canvas.xview_moveto(target_x / body_width if body_width else 0.0)
        self.comparison_fixed_body_canvas.yview_moveto(target_y / body_height if body_height else 0.0)
        self.comparison_body_canvas.yview_moveto(target_y / body_height if body_height else 0.0)
        self._update_comparison_y_scrollbar()

    def _save_status_text(self, save_summary: PersistenceSummary | None) -> str:
        if save_summary is None:
            return "保存なし"

        saved_targets: list[str] = []
        if save_summary.local_file_path:
            saved_targets.append("ローカル")
        if save_summary.supabase_saved:
            saved_targets.append("Supabase")

        if not saved_targets:
            return "保存失敗"
        return "保存:" + "+".join(saved_targets)

    def _merge_persistence_summary(
        self,
        current_summary: PersistenceSummary | None,
        day_summary: PersistenceSummary,
    ) -> PersistenceSummary:
        if current_summary is None:
            return PersistenceSummary(
                local_file_path=day_summary.local_file_path,
                local_record_count=day_summary.local_record_count,
                supabase_saved=day_summary.supabase_saved,
                supabase_record_count=day_summary.supabase_record_count,
                messages=list(day_summary.messages),
            )

        if day_summary.local_file_path:
            current_summary.local_file_path = day_summary.local_file_path
        current_summary.local_record_count += day_summary.local_record_count
        current_summary.supabase_saved = current_summary.supabase_saved or day_summary.supabase_saved
        current_summary.supabase_record_count += day_summary.supabase_record_count
        current_summary.messages.extend(day_summary.messages)
        return current_summary

    def _sort_records(
        self,
        records: list[object],
        value_getter: Callable[[object], object],
        descending: bool,
    ) -> list[object]:
        filled_records: list[object] = []
        blank_records: list[object] = []

        for record in records:
            value = value_getter(record)
            if self._is_blank_value(value):
                blank_records.append(record)
            else:
                filled_records.append(record)

        filled_records.sort(key=lambda record: self._sortable_value(value_getter(record)), reverse=descending)
        return filled_records + blank_records

    def _is_blank_value(self, value: object) -> bool:
        return str(value).strip() in {"", "-"}

    def _is_valid_url(self, value: str) -> bool:
        parsed = urlparse(value)
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)

    def _sortable_value(self, value: object) -> tuple[int, float | str]:
        text = str(value).strip()
        if text in {"", "-"}:
            return (2, "")

        if isinstance(value, int):
            return (0, float(value))

        normalized = text.replace(",", "").replace("台", "").replace("%", "")
        if re.fullmatch(r"-?\d+(?:\.\d+)?", normalized):
            return (0, float(normalized))

        ratio_match = re.fullmatch(r"(-?\d+(?:\.\d+)?)/(-?\d+(?:\.\d+)?)", text.replace(",", ""))
        if ratio_match:
            numerator = float(ratio_match.group(1))
            denominator = float(ratio_match.group(2))
            if denominator != 0:
                return (0, numerator / denominator)

        return (1, normalize_text(text))

    def _update_button_states(self) -> None:
        has_comparison_data = self.current_history_result is not None and not self.skip_comparison_display_var.get()
        has_registered_store_row_selection = (
            hasattr(self, "registered_store_tree")
            and bool(self.registered_store_tree.selection())
        )
        has_single_registered_store_row_selection = (
            hasattr(self, "registered_store_tree")
            and len(self.registered_store_tree.selection()) == 1
        )

        self.fetch_button.configure(state="disabled" if self.is_busy else "normal")
        can_cancel_fetch = (
            self.is_busy
            and self.active_operation_kind in {"fetch", "scheduled_fetch", "site7_fetch"}
            and not self.fetch_cancel_event.is_set()
        )
        self.cancel_fetch_button.configure(state="normal" if can_cancel_fetch else "disabled")
        self.target_date_entry.configure(state="disabled" if self.is_busy else "normal")
        self.retry_delay_entry.configure(state="disabled" if self.is_busy else "normal")
        self.schedule_hour_entry.configure(state="disabled" if self.is_busy else "normal")
        self.apply_schedule_button.configure(state="disabled" if self.is_busy else "normal")
        self.clear_schedule_button.configure(state="disabled" if self.is_busy else "normal")
        self.comparison_day_tail_selector.configure(state="readonly" if not self.is_busy and has_comparison_data else "disabled")
        self.comparison_focus_button.configure(state="normal" if not self.is_busy and has_comparison_data else "disabled")
        self.skip_comparison_display_button.configure(state="disabled" if self.is_busy else "normal")
        self.notify_fetch_complete_button.configure(state="disabled" if self.is_busy else "normal")
        self.site7_login_button.configure(state="disabled" if self.is_busy else "normal")
        self.site7_fetch_button.configure(state="disabled" if self.is_busy else "normal")
        can_cancel_site7_fetch = self.is_busy and self.active_operation_kind == "site7_fetch" and not self.fetch_cancel_event.is_set()
        self.site7_cancel_button.configure(state="normal" if can_cancel_site7_fetch else "disabled")
        self.site7_browser_visible_radio.configure(state="disabled" if self.is_busy else "normal")
        self.site7_browser_hidden_radio.configure(state="disabled" if self.is_busy else "normal")
        self.register_store_button.configure(state="disabled" if self.is_busy else "normal")
        self.register_store_url_entry.configure(state="disabled" if self.is_busy else "normal")
        self.register_store_site7_button.configure(state="disabled" if self.is_busy else "normal")
        self.register_store_prefecture_entry.configure(state="disabled" if self.is_busy else "normal")
        self.register_store_area_entry.configure(state="disabled" if self.is_busy else "normal")
        self.register_store_site7_store_name_entry.configure(state="disabled" if self.is_busy else "normal")
        self.update_registered_store_button.configure(
            state="disabled" if self.is_busy or not has_single_registered_store_row_selection else "normal"
        )
        self.clear_register_store_form_button.configure(state="disabled" if self.is_busy else "normal")
        self.select_all_stores_button.configure(state="disabled" if self.is_busy else "normal")
        self.clear_store_selection_button.configure(state="disabled" if self.is_busy else "normal")
        self.refresh_registered_stores_button.configure(state="disabled" if self.is_busy else "normal")
        self.delete_registered_stores_button.configure(
            state="disabled" if self.is_busy or not has_registered_store_row_selection else "normal"
        )

    def _show_error(self, exc: object) -> None:
        if isinstance(exc, ScraperError):
            message = str(exc)
        else:
            message = f"想定外のエラーが発生しました。\n{exc}"
        messagebox.showerror("取得失敗", message)


def main() -> None:
    root = tk.Tk()
    style = ttk.Style()
    if "vista" in style.theme_names():
        style.theme_use("vista")
    MinRepoApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
