from __future__ import annotations

import queue
import threading
import tkinter as tk
from tkinter import messagebox, ttk

from minrepo_scraper import (
    MachineDataset,
    MachineListResult,
    MinRepoScraper,
    ScraperError,
    normalize_text,
    parse_date_input,
)


DEFAULT_STORE_NAME = "MJアリーナ箱崎店"
DEFAULT_STORE_URL = "https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/"
DEFAULT_TARGET_DATE = "2026-04-08"
DEFAULT_MACHINE_NAME = "ネオアイムジャグラーEX"


class MinRepoApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Halldata Prototype")
        self.root.geometry("1280x860")

        self.scraper = MinRepoScraper()
        self.result_queue: queue.Queue[tuple[str, object]] = queue.Queue()
        self.current_results: list[MachineDataset] = []
        self.current_machine_list: MachineListResult | None = None
        self.machine_vars: dict[str, tk.BooleanVar] = {}
        self.is_busy = False

        self.store_url_var = tk.StringVar(value=DEFAULT_STORE_URL)
        self.target_date_var = tk.StringVar(value=DEFAULT_TARGET_DATE)
        self.machine_list_var = tk.StringVar(value="機種一覧: 未読込")
        self.status_var = tk.StringVar(value="待機中")
        self.summary_var = tk.StringVar(value="未取得")

        self._build_ui()
        self._update_button_states()

    def _build_ui(self) -> None:
        container = ttk.Frame(self.root, padding=16)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(3, weight=1)

        form = ttk.LabelFrame(container, text="取得条件", padding=12)
        form.grid(row=0, column=0, sticky="ew")
        form.columnconfigure(1, weight=1)

        ttk.Label(form, text="対象店舗").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Label(form, text=DEFAULT_STORE_NAME).grid(row=0, column=1, sticky="w", pady=4)

        ttk.Label(form, text="店舗URL").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        self.store_url_entry = ttk.Entry(form, textvariable=self.store_url_var, state="readonly")
        self.store_url_entry.grid(row=1, column=1, sticky="ew", pady=4)

        ttk.Label(form, text="対象日").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=4)
        self.target_date_entry = ttk.Entry(form, textvariable=self.target_date_var, width=20)
        self.target_date_entry.grid(row=2, column=1, sticky="w", pady=4)

        button_row = ttk.Frame(form)
        button_row.grid(row=3, column=1, sticky="w", pady=(8, 0))

        self.load_machine_button = ttk.Button(button_row, text="機種一覧を読み込む", command=self.load_machine_list)
        self.load_machine_button.grid(row=0, column=0, sticky="w")

        self.fetch_button = ttk.Button(button_row, text="取得", command=self.fetch_data)
        self.fetch_button.grid(row=0, column=1, sticky="w", padx=(8, 0))

        machine_frame = ttk.LabelFrame(container, text="機種選択", padding=8)
        machine_frame.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        machine_frame.columnconfigure(0, weight=1)
        machine_frame.rowconfigure(1, weight=1)

        machine_actions = ttk.Frame(machine_frame)
        machine_actions.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        machine_actions.columnconfigure(0, weight=1)

        ttk.Label(machine_actions, textvariable=self.machine_list_var).grid(row=0, column=0, sticky="w")

        self.select_all_button = ttk.Button(machine_actions, text="全選択", command=self.select_all_machines)
        self.select_all_button.grid(row=0, column=1, sticky="e", padx=(8, 0))

        self.clear_selection_button = ttk.Button(machine_actions, text="全解除", command=self.clear_machine_selection)
        self.clear_selection_button.grid(row=0, column=2, sticky="e", padx=(8, 0))

        self.machine_canvas = tk.Canvas(machine_frame, height=220, highlightthickness=0)
        self.machine_canvas.grid(row=1, column=0, sticky="nsew")

        machine_scroll = ttk.Scrollbar(machine_frame, orient="vertical", command=self.machine_canvas.yview)
        machine_scroll.grid(row=1, column=1, sticky="ns")
        self.machine_canvas.configure(yscrollcommand=machine_scroll.set)

        self.machine_inner = ttk.Frame(self.machine_canvas)
        self.machine_window = self.machine_canvas.create_window((0, 0), window=self.machine_inner, anchor="nw")
        self.machine_inner.bind("<Configure>", self._on_machine_list_configure)
        self.machine_canvas.bind("<Configure>", self._on_machine_canvas_configure)

        info = ttk.Frame(container, padding=(0, 12, 0, 12))
        info.grid(row=2, column=0, sticky="ew")
        info.columnconfigure(1, weight=1)
        info.columnconfigure(3, weight=1)

        ttk.Label(info, text="状態").grid(row=0, column=0, sticky="w")
        ttk.Label(info, textvariable=self.status_var).grid(row=0, column=1, sticky="w", padx=(8, 24))
        ttk.Label(info, text="概要").grid(row=0, column=2, sticky="w")
        ttk.Label(info, textvariable=self.summary_var).grid(row=0, column=3, sticky="w", padx=(8, 0))

        table_frame = ttk.LabelFrame(container, text="台データ", padding=8)
        table_frame.grid(row=3, column=0, sticky="nsew")
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)

        self.tree = ttk.Treeview(table_frame, show="headings")
        self.tree.grid(row=0, column=0, sticky="nsew")

        y_scroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        y_scroll.grid(row=0, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=y_scroll.set)

        x_scroll = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        x_scroll.grid(row=1, column=0, sticky="ew")
        self.tree.configure(xscrollcommand=x_scroll.set)

    def load_machine_list(self) -> None:
        self._clear_machine_list("機種一覧: 読込中")
        self.current_results = []
        self._clear_table()
        self.status_var.set("機種一覧取得中...")
        self.summary_var.set("対象日の機種を確認中")
        self._start_worker(self._worker_load_machine_list)

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
        self._clear_table()
        self.status_var.set("取得中...")
        self.summary_var.set(f"{len(machine_names)}機種を取得中")
        self._start_worker(self._worker_fetch, machine_list, machine_names)

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
        machine_list: MachineListResult,
        machine_names: list[str],
    ) -> None:
        try:
            result = self.scraper.fetch_machine_datasets(
                machine_list=machine_list,
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

        if kind == "fetch_error":
            self.status_var.set("失敗")
            self.summary_var.set("取得できませんでした")
            self._show_error(payload)
            return

        results = payload
        if not isinstance(results, list) or not all(isinstance(result, MachineDataset) for result in results):
            self.status_var.set("失敗")
            self.summary_var.set("不明な結果")
            messagebox.showerror("エラー", "取得結果の形式が不正です。")
            return

        self.current_results = results
        self._populate_table(results)
        total_rows = sum(len(result.rows) for result in results)
        target_date = results[0].target_date if results else self.target_date_var.get().strip()
        store_name = results[0].store_name if results else DEFAULT_STORE_NAME
        self.status_var.set("完了")
        self.summary_var.set(f"{store_name} / {target_date} / {len(results)}機種 / {total_rows}行")

    def _apply_machine_list(self, machine_list: MachineListResult) -> None:
        self.current_machine_list = machine_list
        self._build_machine_checkboxes(machine_list)
        self._refresh_machine_list_summary()
        self.status_var.set("機種一覧読込完了")
        self.summary_var.set(
            f"{machine_list.store_name} / {machine_list.target_date} / {len(machine_list.machine_entries)}機種"
        )

    def _build_machine_checkboxes(self, machine_list: MachineListResult) -> None:
        for child in self.machine_inner.winfo_children():
            child.destroy()

        self.machine_vars = {}
        default_name = normalize_text(DEFAULT_MACHINE_NAME)

        for row_index, machine_entry in enumerate(machine_list.machine_entries):
            machine_key = normalize_text(machine_entry.name)
            variable = tk.BooleanVar(value=machine_key == default_name)
            self.machine_vars[machine_key] = variable

            checkbutton = ttk.Checkbutton(
                self.machine_inner,
                text=machine_entry.name,
                variable=variable,
                command=self._on_machine_selection_changed,
            )
            checkbutton.grid(row=row_index, column=0, sticky="w", padx=4, pady=2)

    def _populate_table(self, results: list[MachineDataset]) -> None:
        columns = self._build_table_columns(results)
        self.tree["columns"] = columns
        for column in columns:
            self.tree.heading(column, text=column)
            self.tree.column(
                column,
                width=self._column_width(column),
                minwidth=80,
                anchor="w" if column == "機種名" else "center",
            )

        for result in results:
            source_columns = [column for column in result.columns if not self._is_machine_name_column(column)]

            for row in result.rows:
                row_map = dict(zip(source_columns, self._filter_machine_name_values(result.columns, row), strict=False))
                values = [result.machine_name if column == "機種名" else row_map.get(column, "") for column in columns]
                self.tree.insert("", "end", values=values)

    def _clear_table(self) -> None:
        self.tree.delete(*self.tree.get_children())
        self.tree["columns"] = ()

    def _clear_machine_list(self, message: str = "機種一覧: 未読込") -> None:
        self.current_machine_list = None
        self.machine_vars = {}
        for child in self.machine_inner.winfo_children():
            child.destroy()
        self.machine_list_var.set(message)
        self._update_button_states()

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
            variable = self.machine_vars.get(machine_key)
            if variable and variable.get():
                machine_names.append(machine_entry.name)
        return machine_names

    def _machine_list_matches_inputs(self, machine_list: MachineListResult) -> bool:
        try:
            current_target_date = parse_date_input(self.target_date_var.get()).strftime("%Y-%m-%d")
        except ScraperError as exc:
            self._show_error(exc)
            return False

        current_store_url = self.store_url_var.get().strip()
        if machine_list.store_url != current_store_url or machine_list.target_date != current_target_date:
            messagebox.showwarning("再読込が必要", "対象日を変更した場合は、機種一覧をもう一度読み込んでください。")
            return False

        return True

    def select_all_machines(self) -> None:
        for variable in self.machine_vars.values():
            variable.set(True)
        self._refresh_machine_list_summary()

    def clear_machine_selection(self) -> None:
        for variable in self.machine_vars.values():
            variable.set(False)
        self._refresh_machine_list_summary()

    def _on_machine_selection_changed(self) -> None:
        self._refresh_machine_list_summary()

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

    def _update_button_states(self) -> None:
        has_machine_list = self.current_machine_list is not None and bool(self.machine_vars)
        has_selection = bool(self._selected_machine_names())

        self.load_machine_button.configure(state="disabled" if self.is_busy else "normal")
        self.fetch_button.configure(state="disabled" if self.is_busy or not has_selection else "normal")
        self.select_all_button.configure(state="disabled" if self.is_busy or not has_machine_list else "normal")
        self.clear_selection_button.configure(state="disabled" if self.is_busy or not has_machine_list else "normal")
        self.target_date_entry.configure(state="disabled" if self.is_busy else "normal")

        checkbox_state = "disabled" if self.is_busy else "normal"
        for child in self.machine_inner.winfo_children():
            child.configure(state=checkbox_state)

    def _on_machine_list_configure(self, _: tk.Event[tk.Misc]) -> None:
        self.machine_canvas.configure(scrollregion=self.machine_canvas.bbox("all"))

    def _on_machine_canvas_configure(self, event: tk.Event[tk.Misc]) -> None:
        self.machine_canvas.itemconfigure(self.machine_window, width=event.width)

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
