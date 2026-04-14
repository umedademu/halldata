from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import queue
import re
import threading
import tkinter as tk
from tkinter import font as tkfont
from tkinter import messagebox, ttk
from typing import Callable
from urllib.parse import urlparse

try:
    import winsound
except ImportError:  # pragma: no cover
    winsound = None

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


DEFAULT_STORE_NAME = "MJアリーナ箱崎店"
DEFAULT_STORE_URL = "https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/"
DEFAULT_RECENT_DAYS = "90"
JST = timezone(timedelta(hours=9))
REGISTERED_STORE_COLUMNS = ("取得対象", "店舗名", "URL")
COMPARISON_SUBCOLUMNS = ("機種名", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率")
COMPARISON_DAY_TAIL_OPTIONS = ("全て", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9")


def build_recent_date_range_input(value: str, today: datetime | None = None) -> str:
    text = value.strip()
    if not re.fullmatch(r"\d+", text):
        raise ScraperError("直近日数は 1 以上の整数で入力してください。")

    recent_days = int(text)
    if recent_days <= 0:
        raise ScraperError("直近日数は 1 以上の整数で入力してください。")

    today_date = (today or datetime.now(JST)).astimezone(JST).date()
    start_date = today_date - timedelta(days=recent_days - 1)
    return f"{start_date.strftime('%Y-%m-%d')} ～ {today_date.strftime('%Y-%m-%d')}"


def matches_day_tail(date_text: str, day_tail: str) -> bool:
    if day_tail == "全て":
        return True

    match = re.fullmatch(r"\d{4}-\d{2}-(\d{2})", date_text.strip())
    if match is None:
        return False

    return match.group(1).endswith(day_tail)


@dataclass
class RegisteredStore:
    name: str
    url: str


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


class MinRepoApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Halldata Prototype")
        self.root.geometry("1320x900")
        self.default_font = tkfont.nametofont("TkDefaultFont")

        self.scraper = MinRepoScraper()
        self.persistence_service = HistoryPersistenceService()
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

        self.target_date_var = tk.StringVar(value=DEFAULT_RECENT_DAYS)
        self.status_var = tk.StringVar(value="待機中")
        self.summary_var = tk.StringVar(value="未取得")
        self.fetch_progress_value_var = tk.DoubleVar(value=0.0)
        self.fetch_progress_text_var = tk.StringVar(value="未開始")
        self.skip_comparison_display_var = tk.BooleanVar(value=True)
        self.notify_fetch_complete_var = tk.BooleanVar(value=True)
        self.comparison_day_tail_var = tk.StringVar(value="全て")
        self.register_store_url_var = tk.StringVar()
        self.register_store_status_var = tk.StringVar(value="未登録")
        self.fetch_progress_current = 0
        self.fetch_progress_total = 0

        self._build_ui()
        self._reset_fetch_progress()
        self._update_button_states()
        self._refresh_registered_store_table()
        if self.startup_store_warning:
            self.root.after(100, lambda: messagebox.showwarning("登録店舗", self.startup_store_warning))

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

        button_row = ttk.Frame(self.fetch_form)
        button_row.grid(row=1, column=1, sticky="w", pady=(8, 0))

        self.fetch_button = ttk.Button(button_row, text="取得", command=self.fetch_data)
        self.fetch_button.grid(row=0, column=0, sticky="w")

        self.skip_comparison_display_button = ttk.Checkbutton(
            button_row,
            text="取得後に台データ表を表示しない",
            variable=self.skip_comparison_display_var,
            command=self._on_skip_comparison_display_changed,
        )
        self.skip_comparison_display_button.grid(row=0, column=1, sticky="w", padx=(12, 0))

        self.notify_fetch_complete_button = ttk.Checkbutton(
            button_row,
            text="取得完了時に音を鳴らす",
            variable=self.notify_fetch_complete_var,
        )
        self.notify_fetch_complete_button.grid(row=0, column=2, sticky="w", padx=(12, 0))

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
            ),
            wraplength=900,
            justify="left",
        ).grid(row=0, column=0, sticky="w")

        form = ttk.LabelFrame(register_tab, text="店舗を登録", padding=12)
        form.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        form.columnconfigure(1, weight=1)
        form.rowconfigure(2, weight=1)

        ttk.Label(form, text="店舗URL").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        self.register_store_url_entry = ttk.Entry(form, textvariable=self.register_store_url_var)
        self.register_store_url_entry.grid(row=0, column=1, sticky="ew", pady=4)

        action_row = ttk.Frame(form)
        action_row.grid(row=1, column=1, sticky="w", pady=(8, 8))
        self.register_store_button = ttk.Button(action_row, text="登録する", command=self.register_store)
        self.register_store_button.grid(row=0, column=0, sticky="w")

        ttk.Label(action_row, textvariable=self.register_store_status_var).grid(row=0, column=1, sticky="w", padx=(12, 0))

        table_frame = ttk.LabelFrame(form, text="登録済み一覧", padding=8)
        table_frame.grid(row=2, column=0, columnspan=2, sticky="nsew", pady=(8, 0))
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

        self.registered_store_tree = ttk.Treeview(table_frame, columns=REGISTERED_STORE_COLUMNS, show="headings")
        self.registered_store_tree.grid(row=1, column=0, sticky="nsew")

        for column in REGISTERED_STORE_COLUMNS:
            self.registered_store_tree.heading(column, text=column)
            if column == "取得対象":
                self.registered_store_tree.column(column, width=80, minwidth=80, anchor="center")
                continue
            self.registered_store_tree.column(
                column,
                width=220 if column == "店舗名" else 760,
                minwidth=120,
                anchor="w",
            )
        self.registered_store_tree.bind("<Button-1>", self._on_registered_store_tree_click)

        y_scroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.registered_store_tree.yview)
        y_scroll.grid(row=1, column=1, sticky="ns")
        self.registered_store_tree.configure(yscrollcommand=y_scroll.set)

        x_scroll = ttk.Scrollbar(table_frame, orient="horizontal", command=self.registered_store_tree.xview)
        x_scroll.grid(row=2, column=0, sticky="ew")
        self.registered_store_tree.configure(xscrollcommand=x_scroll.set)

    def _load_registered_stores_on_startup(self) -> list[RegisteredStore]:
        default_stores = [RegisteredStore(name=DEFAULT_STORE_NAME, url=DEFAULT_STORE_URL)]

        try:
            saved_stores = self.persistence_service.load_registered_stores()
        except Exception as exc:  # noqa: BLE001
            self.startup_store_warning = f"登録店舗の読込に失敗したため、初期店舗だけを表示します。\n{exc}"
            return default_stores

        registered_stores = [
            RegisteredStore(name=store["store_name"], url=store["store_url"])
            for store in saved_stores
        ]
        if not registered_stores:
            return default_stores
        return registered_stores

    def register_store(self) -> None:
        store_url = self.register_store_url_var.get().strip()

        if not store_url:
            messagebox.showwarning("入力不足", "店舗URLを入力してください。")
            return

        if not self._is_valid_url(store_url):
            messagebox.showwarning("入力不正", "店舗URLは http:// または https:// から入力してください。")
            return

        normalized_url = normalize_store_url(store_url)
        for registered_store in self.registered_stores:
            if normalize_store_url(registered_store.url) == normalized_url:
                messagebox.showwarning("重複", "同じURLがすでに登録されています。")
                return

        self.register_store_status_var.set("店舗名を取得中...")
        self._start_worker(self._worker_register_store, store_url)

    def _worker_register_store(self, store_url: str) -> None:
        try:
            store_name = self.scraper.fetch_store_name(store_url)
            self.result_queue.put(("register_store_success", (store_name, store_url)))
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("register_store_error", exc))

    def fetch_data(self) -> None:
        try:
            target_date_input = self._target_date_input_from_recent_days()
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
        self._start_worker(
            self._worker_fetch_many,
            target_stores,
            target_date_input,
        )

    def _start_worker(self, target: object, *args: object) -> None:
        self.is_busy = True
        self._update_button_states()

        worker = threading.Thread(target=target, args=args, daemon=True)
        worker.start()
        self.root.after(100, self._poll_queue)

    def _worker_fetch_many(
        self,
        target_stores: list[RegisteredStore],
        target_date_input: str,
    ) -> None:
        results: list[StoreFetchResult] = []
        failures: list[StoreFetchFailure] = []
        total_stores = len(target_stores)

        for store_index, registered_store in enumerate(target_stores, start=1):
            try:
                results.append(
                    self._fetch_single_store(
                        registered_store=registered_store,
                        target_date_input=target_date_input,
                        store_index=store_index,
                        total_stores=total_stores,
                    )
                )
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

        if not results and failures:
            failure_lines = "\n".join(f"{failure.store.name}: {failure.error}" for failure in failures)
            self.result_queue.put(("fetch_error", ScraperError(f"選択した店舗を取得できませんでした。\n{failure_lines}")))
            return

        self.result_queue.put(("fetch_many_success", FetchManyResult(results=results, failures=failures)))

    def _fetch_single_store(
        self,
        registered_store: RegisteredStore,
        target_date_input: str,
        store_index: int,
        total_stores: int,
    ) -> StoreFetchResult:
        store_url = registered_store.url
        store_label = f"{store_index}/{total_stores} {registered_store.name}"
        context = self.scraper.prepare_machine_history_context(store_url, target_date_input)
        saved_full_day_summary = self.persistence_service.find_saved_full_day_dates(
            store_name=context.store_name,
            store_url=store_url,
            start_date=context.start_date,
            end_date=context.end_date,
        )
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
            day_result = self.scraper.fetch_all_machine_history_for_date_page(
                context=context,
                date_page=date_page,
                step_callback=step_callback,
                date_index=date_index,
                total_dates=len(pending_date_pages),
            )
            datasets.extend(day_result.datasets)
            skipped_targets.extend(day_result.skipped_targets)

            if day_result.datasets:
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

        self.is_busy = False
        self._update_button_states()

        if kind == "register_store_error":
            self.register_store_status_var.set("店舗登録に失敗しました")
            self._show_error(payload)
            return

        if kind == "register_store_success":
            if (
                not isinstance(payload, tuple)
                or len(payload) != 2
                or not isinstance(payload[0], str)
                or not isinstance(payload[1], str)
            ):
                messagebox.showerror("エラー", "登録店舗の形式が不正です。")
                return
            store_name, store_url = payload
            self._apply_registered_store(store_name, store_url)
            return

        if kind == "fetch_error":
            self._finish_fetch_progress(success=False, message="取得失敗")
            self.status_var.set("失敗")
            self.summary_var.set("取得できませんでした")
            self._show_error(payload)
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

        self._finish_fetch_progress(
            success=True,
            message="取得完了（保存に注意）" if has_save_errors else "取得完了",
        )
        if fetch_many_result.failures:
            self.status_var.set("完了（一部失敗）")
        elif has_save_errors:
            self.status_var.set("完了（保存に注意）")
        elif all_skipped:
            self.status_var.set("完了（取得済みをスキップ）")
        else:
            self.status_var.set("完了")

        self.summary_var.set(self._fetch_many_summary_text(fetch_many_result))
        self._update_button_states()
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
        display_text = (
            " / 表表示省略"
            if self.skip_comparison_display_var.get()
            else f" / 表表示は{last_history_result.store_name}"
        )
        return (
            f"{len(fetch_many_result.results)}店舗完了{failed_text} / "
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
                    registered_store.name,
                    registered_store.url,
                ),
            )

    def _registered_store_target_marker(self, registered_store: RegisteredStore) -> str:
        return "☑" if normalize_store_url(registered_store.url) in self.selected_store_urls else "☐"

    def _selected_registered_stores(self) -> list[RegisteredStore]:
        return [
            registered_store
            for registered_store in self.registered_stores
            if normalize_store_url(registered_store.url) in self.selected_store_urls
        ]

    def _on_registered_store_tree_click(self, event: tk.Event[tk.Misc]) -> str | None:
        if self.is_busy:
            return None

        if self.registered_store_tree.identify_region(event.x, event.y) != "cell":
            return None

        if self.registered_store_tree.identify_column(event.x) != "#1":
            return None

        item_id = self.registered_store_tree.identify_row(event.y)
        if not item_id:
            return None

        self._toggle_registered_store_target(item_id)
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

    def _apply_registered_store(self, store_name: str, store_url: str) -> None:
        normalized_name = normalize_text(store_name)
        normalized_url = normalize_store_url(store_url)
        for registered_store in self.registered_stores:
            if normalize_text(registered_store.name) == normalized_name or normalize_store_url(registered_store.url) == normalized_url:
                messagebox.showwarning("重複", "同じ店舗名またはURLがすでに登録されています。")
                self.register_store_status_var.set("登録済みの店舗です")
                return

        self.registered_stores.append(RegisteredStore(name=store_name, url=normalized_url))
        self.selected_store_urls.add(normalized_url)
        self.register_store_url_var.set("")
        self._refresh_registered_store_table()
        save_summary = self._persist_registered_stores()
        if save_summary.has_errors:
            self.register_store_status_var.set(f"{store_name} を登録しました（保存に注意）")
            messagebox.showwarning("登録店舗", "\n\n".join(save_summary.messages))
            return

        self.register_store_status_var.set(f"{store_name} を登録しました")

    def _persist_registered_stores(self) -> RegisteredStoresPersistenceSummary:
        store_payloads = [
            {
                "store_name": registered_store.name,
                "store_url": registered_store.url,
            }
            for registered_store in self.registered_stores
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

        self.fetch_button.configure(state="disabled" if self.is_busy else "normal")
        self.target_date_entry.configure(state="disabled" if self.is_busy else "normal")
        self.comparison_day_tail_selector.configure(state="readonly" if not self.is_busy and has_comparison_data else "disabled")
        self.comparison_focus_button.configure(state="normal" if not self.is_busy and has_comparison_data else "disabled")
        self.skip_comparison_display_button.configure(state="disabled" if self.is_busy else "normal")
        self.notify_fetch_complete_button.configure(state="disabled" if self.is_busy else "normal")
        self.register_store_button.configure(state="disabled" if self.is_busy else "normal")
        self.register_store_url_entry.configure(state="disabled" if self.is_busy else "normal")
        self.select_all_stores_button.configure(state="disabled" if self.is_busy else "normal")
        self.clear_store_selection_button.configure(state="disabled" if self.is_busy else "normal")

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
