from __future__ import annotations

from dataclasses import dataclass
import queue
import re
import threading
import tkinter as tk
from tkinter import font as tkfont
from tkinter import messagebox, ttk
from typing import Callable
from urllib.parse import urlparse

from minrepo_scraper import (
    MachineDataset,
    MachineEntry,
    MachineHistoryResult,
    MachineListResult,
    MinRepoScraper,
    ScraperError,
    normalize_text,
    parse_date_range_input,
)


DEFAULT_STORE_NAME = "MJアリーナ箱崎店"
DEFAULT_STORE_URL = "https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/"
DEFAULT_TARGET_DATE = "2026-04-08 ～ 2026-04-08"
DEFAULT_MACHINE_NAME = "ネオアイムジャグラーEX"
CHECK_ON = "☑"
CHECK_OFF = "☐"
MACHINE_COLUMNS = ("チェック", "機種名", "台数", "平均差枚", "平均G数", "勝率", "出率")
REGISTERED_STORE_COLUMNS = ("店舗名", "URL")
COMPARISON_SUBCOLUMNS = ("機種名", "差枚", "G数", "出率", "BB", "RB", "合成", "BB率", "RB率")


@dataclass
class RegisteredStore:
    name: str
    url: str


class MinRepoApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Halldata Prototype")
        self.root.geometry("1320x900")
        self.default_font = tkfont.nametofont("TkDefaultFont")

        self.scraper = MinRepoScraper()
        self.result_queue: queue.Queue[tuple[str, object]] = queue.Queue()
        self.current_results: list[MachineDataset] = []
        self.current_history_result: MachineHistoryResult | None = None
        self.current_machine_list: MachineListResult | None = None
        self.selected_machine_keys: set[str] = set()
        self.machine_sort_column = "台数"
        self.machine_sort_descending = True
        self.comparison_sort_key = "日付"
        self.comparison_sort_descending = False
        self.comparison_slot_numbers: list[str] = []
        self.comparison_rows: list[dict[str, str]] = []
        self.comparison_display_rows: list[dict[str, str]] = []
        self.comparison_selected_date: str | None = None
        self.comparison_focus_mode = False
        self.comparison_header_click_regions: list[tuple[int, int, int, int, str]] = []
        self.comparison_text_cache: dict[tuple[str, int], str] = {}
        self.registered_stores: list[RegisteredStore] = [
            RegisteredStore(name=DEFAULT_STORE_NAME, url=DEFAULT_STORE_URL)
        ]
        self.is_busy = False

        self.selected_store_var = tk.StringVar(value=DEFAULT_STORE_NAME)
        self.store_url_var = tk.StringVar(value=DEFAULT_STORE_URL)
        self.target_date_var = tk.StringVar(value=DEFAULT_TARGET_DATE)
        self.machine_list_var = tk.StringVar(value="機種一覧: 未読込")
        self.status_var = tk.StringVar(value="待機中")
        self.summary_var = tk.StringVar(value="未取得")
        self.register_store_url_var = tk.StringVar()
        self.register_store_status_var = tk.StringVar(value="未登録")

        self._build_ui()
        self._update_button_states()
        self._refresh_registered_store_table()

    def _build_ui(self) -> None:
        container = ttk.Frame(self.root, padding=16)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)

        notebook = ttk.Notebook(container)
        notebook.grid(row=0, column=0, sticky="nsew")

        self.fetch_tab = ttk.Frame(notebook, padding=12)
        self.fetch_tab.columnconfigure(0, weight=1)
        self.fetch_tab.rowconfigure(1, weight=1)
        self.fetch_tab.rowconfigure(3, weight=2)
        notebook.add(self.fetch_tab, text="データ取得")

        register_tab = ttk.Frame(notebook, padding=12)
        register_tab.columnconfigure(0, weight=1)
        register_tab.rowconfigure(1, weight=1)
        notebook.add(register_tab, text="登録店舗")

        self.fetch_form = ttk.LabelFrame(self.fetch_tab, text="取得条件", padding=12)
        self.fetch_form.grid(row=0, column=0, sticky="ew")
        self.fetch_form.columnconfigure(1, weight=1)

        ttk.Label(self.fetch_form, text="対象店舗").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        self.store_selector = ttk.Combobox(self.fetch_form, textvariable=self.selected_store_var, state="readonly")
        self.store_selector.grid(row=0, column=1, sticky="w", pady=4)
        self.store_selector.bind("<<ComboboxSelected>>", self._on_selected_store_changed)

        ttk.Label(self.fetch_form, text="店舗URL").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        self.store_url_entry = ttk.Entry(self.fetch_form, textvariable=self.store_url_var, state="readonly")
        self.store_url_entry.grid(row=1, column=1, sticky="ew", pady=4)

        ttk.Label(self.fetch_form, text="対象期間").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=4)
        self.target_date_entry = ttk.Entry(self.fetch_form, textvariable=self.target_date_var, width=30)
        self.target_date_entry.grid(row=2, column=1, sticky="w", pady=4)

        button_row = ttk.Frame(self.fetch_form)
        button_row.grid(row=3, column=1, sticky="w", pady=(8, 0))

        self.load_machine_button = ttk.Button(button_row, text="機種一覧を読み込む", command=self.load_machine_list)
        self.load_machine_button.grid(row=0, column=0, sticky="w")

        self.fetch_button = ttk.Button(button_row, text="取得", command=self.fetch_data)
        self.fetch_button.grid(row=0, column=1, sticky="w", padx=(8, 0))

        self.machine_frame = ttk.LabelFrame(self.fetch_tab, text="機種一覧", padding=8)
        self.machine_frame.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        self.machine_frame.columnconfigure(0, weight=1)
        self.machine_frame.rowconfigure(1, weight=1)

        machine_actions = ttk.Frame(self.machine_frame)
        machine_actions.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        machine_actions.columnconfigure(0, weight=1)

        ttk.Label(machine_actions, textvariable=self.machine_list_var).grid(row=0, column=0, sticky="w")

        self.select_all_button = ttk.Button(machine_actions, text="全選択", command=self.select_all_machines)
        self.select_all_button.grid(row=0, column=1, sticky="e", padx=(8, 0))

        self.clear_selection_button = ttk.Button(machine_actions, text="全解除", command=self.clear_machine_selection)
        self.clear_selection_button.grid(row=0, column=2, sticky="e", padx=(8, 0))

        machine_table_frame = ttk.Frame(self.machine_frame)
        machine_table_frame.grid(row=1, column=0, sticky="nsew")
        machine_table_frame.columnconfigure(0, weight=1)
        machine_table_frame.rowconfigure(0, weight=1)

        self.machine_tree = ttk.Treeview(machine_table_frame, columns=MACHINE_COLUMNS, show="headings", selectmode="extended")
        self.machine_tree.grid(row=0, column=0, sticky="nsew")

        machine_y_scroll = ttk.Scrollbar(machine_table_frame, orient="vertical", command=self.machine_tree.yview)
        machine_y_scroll.grid(row=0, column=1, sticky="ns")
        self.machine_tree.configure(yscrollcommand=machine_y_scroll.set)

        machine_x_scroll = ttk.Scrollbar(machine_table_frame, orient="horizontal", command=self.machine_tree.xview)
        machine_x_scroll.grid(row=1, column=0, sticky="ew")
        self.machine_tree.configure(xscrollcommand=machine_x_scroll.set)
        self._configure_machine_tree()

        self.machine_tree.bind("<ButtonRelease-1>", self._on_machine_tree_click)
        self.machine_tree.bind("<Double-1>", self._on_machine_tree_double_click)
        self.machine_tree.bind("<space>", self._on_machine_tree_space)

        self.fetch_info = ttk.Frame(self.fetch_tab, padding=(0, 12, 0, 12))
        self.fetch_info.grid(row=2, column=0, sticky="ew")
        self.fetch_info.columnconfigure(1, weight=1)
        self.fetch_info.columnconfigure(3, weight=1)

        ttk.Label(self.fetch_info, text="状態").grid(row=0, column=0, sticky="w")
        ttk.Label(self.fetch_info, textvariable=self.status_var).grid(row=0, column=1, sticky="w", padx=(8, 24))
        ttk.Label(self.fetch_info, text="概要").grid(row=0, column=2, sticky="w")
        ttk.Label(self.fetch_info, textvariable=self.summary_var).grid(row=0, column=3, sticky="w", padx=(8, 0))

        self.comparison_frame = ttk.LabelFrame(self.fetch_tab, text="台データ比較", padding=8)
        self.comparison_frame.grid(row=3, column=0, sticky="nsew")
        self.comparison_frame.columnconfigure(1, weight=1)
        self.comparison_frame.rowconfigure(2, weight=1)

        comparison_actions = ttk.Frame(self.comparison_frame)
        comparison_actions.grid(row=0, column=0, columnspan=3, sticky="ew", pady=(0, 8))
        comparison_actions.columnconfigure(0, weight=1)

        self.comparison_focus_button = ttk.Button(
            comparison_actions,
            text="台データ表を広く表示",
            command=self.toggle_comparison_focus,
        )
        self.comparison_focus_button.grid(row=0, column=1, sticky="e")

        self.comparison_fixed_header_canvas = tk.Canvas(self.comparison_frame, width=1, height=54, highlightthickness=0)
        self.comparison_fixed_header_canvas.grid(row=1, column=0, sticky="nsw")

        self.comparison_header_canvas = tk.Canvas(self.comparison_frame, height=54, highlightthickness=0)
        self.comparison_header_canvas.grid(row=1, column=1, sticky="ew")

        self.comparison_fixed_body_canvas = tk.Canvas(self.comparison_frame, width=1, highlightthickness=0)
        self.comparison_fixed_body_canvas.grid(row=2, column=0, sticky="nsw")

        self.comparison_body_canvas = tk.Canvas(self.comparison_frame, highlightthickness=0)
        self.comparison_body_canvas.grid(row=2, column=1, sticky="nsew")

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
        self._refresh_store_selector()

    def _configure_machine_tree(self) -> None:
        for column in MACHINE_COLUMNS:
            self.machine_tree.heading(column, text=column, command=lambda current=column: self._sort_machine_table(current))
            self.machine_tree.column(column, width=self._machine_column_width(column), minwidth=80, anchor=self._column_anchor(column))

    def _build_register_tab(self, register_tab: ttk.Frame) -> None:
        guide = ttk.LabelFrame(register_tab, text="案内", padding=12)
        guide.grid(row=0, column=0, sticky="ew")
        guide.columnconfigure(0, weight=1)

        ttk.Label(
            guide,
            text=(
                "ここでは店舗URLを入れて店舗名を自動取得し、仮登録できます。"
                "今はこのアプリ内だけの一覧で、あとで保存先をつなぎ込める形です。"
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
        table_frame.rowconfigure(0, weight=1)

        self.registered_store_tree = ttk.Treeview(table_frame, columns=REGISTERED_STORE_COLUMNS, show="headings")
        self.registered_store_tree.grid(row=0, column=0, sticky="nsew")

        for column in REGISTERED_STORE_COLUMNS:
            self.registered_store_tree.heading(column, text=column)
            self.registered_store_tree.column(
                column,
                width=220 if column == "店舗名" else 760,
                minwidth=120,
                anchor="w",
            )

        y_scroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.registered_store_tree.yview)
        y_scroll.grid(row=0, column=1, sticky="ns")
        self.registered_store_tree.configure(yscrollcommand=y_scroll.set)

        x_scroll = ttk.Scrollbar(table_frame, orient="horizontal", command=self.registered_store_tree.xview)
        x_scroll.grid(row=1, column=0, sticky="ew")
        self.registered_store_tree.configure(xscrollcommand=x_scroll.set)

    def load_machine_list(self) -> None:
        self._clear_machine_list("機種一覧: 読込中")
        self.current_results = []
        self.current_history_result = None
        self.comparison_rows = []
        self.comparison_slot_numbers = []
        self.comparison_display_rows = []
        self.comparison_selected_date = None
        self._clear_comparison_table()
        self.status_var.set("機種一覧取得中...")
        self.summary_var.set("期間の終了日を基準に機種を確認中")
        self._start_worker(self._worker_load_machine_list)

    def register_store(self) -> None:
        store_url = self.register_store_url_var.get().strip()

        if not store_url:
            messagebox.showwarning("入力不足", "店舗URLを入力してください。")
            return

        if not self._is_valid_url(store_url):
            messagebox.showwarning("入力不正", "店舗URLは http:// または https:// から入力してください。")
            return

        normalized_url = store_url.rstrip("/")
        for registered_store in self.registered_stores:
            if registered_store.url.rstrip("/") == normalized_url:
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
        machine_list = self.current_machine_list
        if machine_list is None:
            messagebox.showwarning("機種未選択", "先に機種一覧を読み込んでください。")
            return

        if not self._machine_list_matches_inputs(machine_list):
            return

        machine_names = self._selected_machine_names()
        if not machine_names:
            messagebox.showwarning("機種未選択", "取得したい機種を1つ以上選択してください。")
            return

        self.current_results = []
        self.current_history_result = None
        self.comparison_rows = []
        self.comparison_slot_numbers = []
        self.comparison_display_rows = []
        self.comparison_selected_date = None
        self._clear_comparison_table()
        self.status_var.set("取得中...")
        self.summary_var.set(f"{len(machine_names)}機種を期間取得中")
        self._start_worker(self._worker_fetch, self.store_url_var.get(), self.target_date_var.get(), machine_names)

    def _start_worker(self, target: object, *args: object) -> None:
        self.is_busy = True
        self._update_button_states()

        worker = threading.Thread(target=target, args=args, daemon=True)
        worker.start()
        self.root.after(100, self._poll_queue)

    def _worker_load_machine_list(self) -> None:
        try:
            result = self.scraper.fetch_machine_list(
                store_url=self.store_url_var.get(),
                target_date_input=self.target_date_var.get(),
            )
            self.result_queue.put(("machine_list_success", result))
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("machine_list_error", exc))

    def _worker_fetch(
        self,
        store_url: str,
        target_date_input: str,
        machine_names: list[str],
    ) -> None:
        try:
            result = self.scraper.fetch_machine_history_datasets(
                store_url=store_url,
                target_date_input=target_date_input,
                machine_names=machine_names,
            )
            self.result_queue.put(("fetch_success", result))
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("fetch_error", exc))

    def _poll_queue(self) -> None:
        try:
            kind, payload = self.result_queue.get_nowait()
        except queue.Empty:
            if self.is_busy:
                self.root.after(100, self._poll_queue)
            return

        self.is_busy = False
        self._update_button_states()

        if kind == "machine_list_error":
            self.status_var.set("失敗")
            self.summary_var.set("機種一覧を取得できませんでした")
            self._show_error(payload)
            return

        if kind == "machine_list_success":
            if not isinstance(payload, MachineListResult):
                self.status_var.set("失敗")
                self.summary_var.set("不明な結果")
                messagebox.showerror("エラー", "機種一覧の形式が不正です。")
                return
            self._apply_machine_list(payload)
            return

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
            self.status_var.set("失敗")
            self.summary_var.set("取得できませんでした")
            self._show_error(payload)
            return

        history_result = payload
        if not isinstance(history_result, MachineHistoryResult):
            self.status_var.set("失敗")
            self.summary_var.set("不明な結果")
            messagebox.showerror("エラー", "取得結果の形式が不正です。")
            return

        self.current_history_result = history_result
        self.current_results = history_result.datasets
        self._populate_comparison_table(history_result)
        total_rows = sum(len(result.rows) for result in history_result.datasets)
        store_name = history_result.store_name
        self.status_var.set("完了")
        self.summary_var.set(
            f"{store_name} / {history_result.start_date} ～ {history_result.end_date} / "
            f"{len(self._selected_machine_names())}機種 / {len(history_result.date_pages)}日"
        )

    def _apply_machine_list(self, machine_list: MachineListResult) -> None:
        self.current_machine_list = machine_list
        self.selected_machine_keys = set()
        default_key = normalize_text(DEFAULT_MACHINE_NAME)
        if any(normalize_text(machine.name) == default_key for machine in machine_list.machine_entries):
            self.selected_machine_keys.add(default_key)

        self.machine_sort_column = "台数"
        self.machine_sort_descending = True
        self._refresh_machine_table()
        self._refresh_machine_list_summary()
        self.status_var.set("機種一覧読込完了")
        self.summary_var.set(
            f"{machine_list.store_name} / {machine_list.target_date} / {len(machine_list.machine_entries)}機種"
        )

    def _refresh_machine_table(self) -> None:
        self.machine_tree.delete(*self.machine_tree.get_children())
        self._update_machine_headings()

        machine_list = self.current_machine_list
        if machine_list is None:
            return

        for machine_entry in self._sorted_machine_entries(machine_list.machine_entries):
            machine_key = normalize_text(machine_entry.name)
            self.machine_tree.insert(
                "",
                "end",
                iid=machine_key,
                values=(
                    CHECK_ON if machine_key in self.selected_machine_keys else CHECK_OFF,
                    machine_entry.name,
                    machine_entry.machine_count,
                    machine_entry.average_difference,
                    machine_entry.average_games,
                    machine_entry.win_rate,
                    machine_entry.payout_rate,
                ),
            )

    def _sort_machine_table(self, column: str) -> None:
        if self.current_machine_list is None:
            return

        if self.machine_sort_column == column:
            self.machine_sort_descending = not self.machine_sort_descending
        else:
            self.machine_sort_column = column
            self.machine_sort_descending = False

        self._refresh_machine_table()

    def _update_machine_headings(self) -> None:
        for column in MACHINE_COLUMNS:
            heading_text = self._heading_text(column, self.machine_sort_column, self.machine_sort_descending)
            self.machine_tree.heading(column, text=heading_text, command=lambda current=column: self._sort_machine_table(current))

    def _sorted_machine_entries(self, machine_entries: list[MachineEntry]) -> list[MachineEntry]:
        return self._sort_records(
            machine_entries,
            value_getter=lambda machine_entry: self._machine_value(machine_entry, self.machine_sort_column),
            descending=self.machine_sort_descending,
        )

    def _machine_value(self, machine_entry: MachineEntry, column: str) -> str | int:
        machine_key = normalize_text(machine_entry.name)
        values: dict[str, str | int] = {
            "チェック": 1 if machine_key in self.selected_machine_keys else 0,
            "機種名": machine_entry.name,
            "台数": machine_entry.machine_count,
            "平均差枚": machine_entry.average_difference,
            "平均G数": machine_entry.average_games,
            "勝率": machine_entry.win_rate,
            "出率": machine_entry.payout_rate,
        }
        return values.get(column, "")

    def _on_machine_tree_click(self, event: tk.Event[tk.Misc]) -> str | None:
        if self.machine_tree.identify("region", event.x, event.y) != "cell":
            return None

        item_id = self.machine_tree.identify_row(event.y)
        column_id = self.machine_tree.identify_column(event.x)
        if item_id and column_id == "#1":
            self._toggle_machine_selection(item_id)
            return "break"
        return None

    def _on_machine_tree_double_click(self, event: tk.Event[tk.Misc]) -> str | None:
        if self.machine_tree.identify("region", event.x, event.y) != "cell":
            return None

        item_id = self.machine_tree.identify_row(event.y)
        column_id = self.machine_tree.identify_column(event.x)
        if item_id and column_id == "#1":
            self._toggle_machine_selection(item_id)
            return "break"
        return None

    def _on_machine_tree_space(self, _: tk.Event[tk.Misc]) -> str:
        for item_id in self.machine_tree.selection():
            self._toggle_machine_selection(item_id, refresh=False)
        self._refresh_machine_marks()
        return "break"

    def _toggle_machine_selection(self, item_id: str, refresh: bool = True) -> None:
        if item_id in self.selected_machine_keys:
            self.selected_machine_keys.remove(item_id)
        else:
            self.selected_machine_keys.add(item_id)

        if refresh:
            self._refresh_machine_marks()

    def _refresh_machine_marks(self) -> None:
        if self.machine_sort_column == "チェック":
            self._refresh_machine_table()
            self._refresh_machine_list_summary()
            return

        for item_id in self.machine_tree.get_children():
            values = list(self.machine_tree.item(item_id, "values"))
            if not values:
                continue
            values[0] = CHECK_ON if item_id in self.selected_machine_keys else CHECK_OFF
            self.machine_tree.item(item_id, values=values)
        self._refresh_machine_list_summary()

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
        x_position = self.comparison_body_canvas.xview()[0] if preserve_scroll else 0.0
        y_position = self.comparison_body_canvas.yview()[0] if preserve_scroll else 0.0
        self.comparison_display_rows = self._sorted_comparison_rows()
        self._clear_comparison_table(reset_view=not preserve_scroll)
        self._draw_comparison_headers()
        self._draw_comparison_body()
        self._update_comparison_scrollregion()
        if preserve_scroll:
            self._restore_comparison_view(x_position, y_position)

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

    def _select_comparison_date(self, target_date: str) -> None:
        if not target_date:
            return
        self.comparison_selected_date = target_date
        self._refresh_comparison_table()

    def _sorted_comparison_rows(self) -> list[dict[str, str]]:
        return self._sort_records(
            self.comparison_rows,
            value_getter=lambda row: row.get(self.comparison_sort_key, ""),
            descending=self.comparison_sort_descending,
        )

    def _clear_comparison_table(self, reset_view: bool = True) -> None:
        self.comparison_fixed_header_canvas.delete("all")
        self.comparison_header_canvas.delete("all")
        self.comparison_fixed_body_canvas.delete("all")
        self.comparison_body_canvas.delete("all")
        self.comparison_header_click_regions = []
        if reset_view:
            self.comparison_fixed_body_canvas.yview_moveto(0)
            self.comparison_body_canvas.yview_moveto(0)
            self.comparison_header_canvas.xview_moveto(0)
            self.comparison_body_canvas.xview_moveto(0)
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
                values=(registered_store.name, registered_store.url),
            )

    def _refresh_store_selector(self) -> None:
        store_names = [registered_store.name for registered_store in self.registered_stores]
        self.store_selector.configure(values=store_names)

        selected_store = self._find_registered_store(self.selected_store_var.get())
        if selected_store is None and self.registered_stores:
            selected_store = self.registered_stores[0]
            self.selected_store_var.set(selected_store.name)

        if selected_store is not None:
            self.store_url_var.set(selected_store.url)

    def _find_registered_store(self, store_name: str) -> RegisteredStore | None:
        normalized_name = normalize_text(store_name)
        for registered_store in self.registered_stores:
            if normalize_text(registered_store.name) == normalized_name:
                return registered_store
        return None

    def _on_selected_store_changed(self, _: tk.Event[tk.Misc]) -> None:
        selected_store = self._find_registered_store(self.selected_store_var.get())
        if selected_store is None:
            return

        self.store_url_var.set(selected_store.url)
        self._reset_fetch_display_for_store_change()

    def _apply_registered_store(self, store_name: str, store_url: str) -> None:
        normalized_name = normalize_text(store_name)
        normalized_url = store_url.rstrip("/")
        for registered_store in self.registered_stores:
            if normalize_text(registered_store.name) == normalized_name or registered_store.url.rstrip("/") == normalized_url:
                messagebox.showwarning("重複", "同じ店舗名またはURLがすでに登録されています。")
                self.register_store_status_var.set("登録済みの店舗です")
                return

        self.registered_stores.append(RegisteredStore(name=store_name, url=store_url))
        self.register_store_url_var.set("")
        self.register_store_status_var.set(f"{store_name} を仮登録しました")
        self._refresh_registered_store_table()
        self._refresh_store_selector()

    def _clear_machine_list(self, message: str = "機種一覧: 未読込") -> None:
        self.current_machine_list = None
        self.selected_machine_keys = set()
        self.machine_tree.delete(*self.machine_tree.get_children())
        self.machine_list_var.set(message)
        self._update_machine_headings()
        self._update_button_states()

    def toggle_comparison_focus(self) -> None:
        self.comparison_focus_mode = not self.comparison_focus_mode
        self._apply_comparison_focus_mode()

    def _apply_comparison_focus_mode(self) -> None:
        if self.comparison_focus_mode:
            self.fetch_form.grid_remove()
            self.machine_frame.grid_remove()
            self.fetch_info.grid_remove()
            self.fetch_tab.rowconfigure(1, weight=0)
            self.fetch_tab.rowconfigure(3, weight=1)
            self.comparison_focus_button.configure(text="元に戻す")
        else:
            self.fetch_form.grid()
            self.machine_frame.grid()
            self.fetch_info.grid()
            self.fetch_tab.rowconfigure(1, weight=1)
            self.fetch_tab.rowconfigure(3, weight=2)
            self.comparison_focus_button.configure(text="台データ表を広く表示")

    def _reset_fetch_display_for_store_change(self) -> None:
        self._clear_machine_list("機種一覧: 未読込")
        self.current_results = []
        self.current_history_result = None
        self.comparison_rows = []
        self.comparison_slot_numbers = []
        self.comparison_display_rows = []
        self.comparison_selected_date = None
        self._clear_comparison_table()
        self.summary_var.set("未取得")
        self.status_var.set("待機中")

    def _refresh_machine_list_summary(self) -> None:
        if self.current_machine_list is None:
            self.machine_list_var.set("機種一覧: 未読込")
        else:
            machine_count = len(self.current_machine_list.machine_entries)
            selected_count = len(self._selected_machine_names())
            self.machine_list_var.set(f"機種一覧: {machine_count}件 / 選択: {selected_count}件")
        self._update_button_states()

    def _selected_machine_names(self) -> list[str]:
        if self.current_machine_list is None:
            return []

        machine_names: list[str] = []
        for machine_entry in self.current_machine_list.machine_entries:
            machine_key = normalize_text(machine_entry.name)
            if machine_key in self.selected_machine_keys:
                machine_names.append(machine_entry.name)
        return machine_names

    def _machine_list_matches_inputs(self, machine_list: MachineListResult) -> bool:
        try:
            _, current_target_date = parse_date_range_input(self.target_date_var.get())
            current_target_date_text = current_target_date.strftime("%Y-%m-%d")
        except ScraperError as exc:
            self._show_error(exc)
            return False

        current_store_url = self.store_url_var.get().strip()
        if machine_list.store_url != current_store_url or machine_list.target_date != current_target_date_text:
            messagebox.showwarning("再読込が必要", "対象店舗または期間を変更した場合は、機種一覧をもう一度読み込んでください。")
            return False

        return True

    def select_all_machines(self) -> None:
        machine_list = self.current_machine_list
        if machine_list is None:
            return

        self.selected_machine_keys = {normalize_text(machine.name) for machine in machine_list.machine_entries}
        self._refresh_machine_marks()

    def clear_machine_selection(self) -> None:
        self.selected_machine_keys.clear()
        self._refresh_machine_marks()

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

    def _machine_column_width(self, column: str) -> int:
        widths = {
            "チェック": 80,
            "機種名": 340,
            "台数": 80,
            "平均差枚": 100,
            "平均G数": 100,
            "勝率": 100,
            "出率": 100,
        }
        return widths.get(column, 100)

    def _column_anchor(self, column: str) -> str:
        return "w" if column == "機種名" else "center"

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

    def _update_comparison_y_scrollbar(self) -> None:
        first, last = self.comparison_body_canvas.yview()
        self.comparison_y_scrollbar.set(first, last)

    def _restore_comparison_view(self, x_position: float, y_position: float) -> None:
        self.comparison_header_canvas.xview_moveto(x_position)
        self.comparison_body_canvas.xview_moveto(x_position)
        self.comparison_fixed_body_canvas.yview_moveto(y_position)
        self.comparison_body_canvas.yview_moveto(y_position)
        self._update_comparison_y_scrollbar()

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
        has_machine_list = self.current_machine_list is not None
        has_selection = bool(self._selected_machine_names())

        self.load_machine_button.configure(state="disabled" if self.is_busy else "normal")
        self.fetch_button.configure(state="disabled" if self.is_busy or not has_selection else "normal")
        self.select_all_button.configure(state="disabled" if self.is_busy or not has_machine_list else "normal")
        self.clear_selection_button.configure(state="disabled" if self.is_busy or not has_machine_list else "normal")
        self.target_date_entry.configure(state="disabled" if self.is_busy else "normal")
        self.store_selector.configure(state="disabled" if self.is_busy else "readonly")
        self.register_store_button.configure(state="disabled" if self.is_busy else "normal")
        self.register_store_url_entry.configure(state="disabled" if self.is_busy else "normal")

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
