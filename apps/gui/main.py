from __future__ import annotations

import queue
import threading
import tkinter as tk
from tkinter import messagebox, ttk

from minrepo_scraper import MachineDataset, MinRepoScraper, ScraperError


DEFAULT_STORE_URL = "https://min-repo.com/tag/mj%E3%82%A2%E3%83%AA%E3%83%BC%E3%83%8A%E7%AE%B1%E5%B4%8E%E5%BA%97/"
DEFAULT_TARGET_DATE = "2026-04-08"
DEFAULT_MACHINE_NAME = "ネオアイムジャグラーEX"


class MinRepoApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Halldata Prototype")
        self.root.geometry("1180x720")

        self.scraper = MinRepoScraper()
        self.result_queue: queue.Queue[tuple[str, object]] = queue.Queue()
        self.current_result: MachineDataset | None = None

        self.store_url_var = tk.StringVar(value=DEFAULT_STORE_URL)
        self.target_date_var = tk.StringVar(value=DEFAULT_TARGET_DATE)
        self.machine_name_var = tk.StringVar(value=DEFAULT_MACHINE_NAME)
        self.status_var = tk.StringVar(value="待機中")
        self.summary_var = tk.StringVar(value="未取得")

        self._build_ui()

    def _build_ui(self) -> None:
        container = ttk.Frame(self.root, padding=16)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(2, weight=1)

        form = ttk.LabelFrame(container, text="取得条件", padding=12)
        form.grid(row=0, column=0, sticky="ew")
        form.columnconfigure(1, weight=1)

        ttk.Label(form, text="店舗URL").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(form, textvariable=self.store_url_var).grid(row=0, column=1, sticky="ew", pady=4)

        ttk.Label(form, text="対象日").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(form, textvariable=self.target_date_var, width=20).grid(row=1, column=1, sticky="w", pady=4)

        ttk.Label(form, text="機種名").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=4)
        ttk.Entry(form, textvariable=self.machine_name_var).grid(row=2, column=1, sticky="ew", pady=4)

        self.fetch_button = ttk.Button(form, text="取得", command=self.fetch_data)
        self.fetch_button.grid(row=3, column=1, sticky="w", pady=(8, 0))

        info = ttk.Frame(container, padding=(0, 12, 0, 12))
        info.grid(row=1, column=0, sticky="ew")
        info.columnconfigure(1, weight=1)
        info.columnconfigure(3, weight=1)

        ttk.Label(info, text="状態").grid(row=0, column=0, sticky="w")
        ttk.Label(info, textvariable=self.status_var).grid(row=0, column=1, sticky="w", padx=(8, 24))
        ttk.Label(info, text="概要").grid(row=0, column=2, sticky="w")
        ttk.Label(info, textvariable=self.summary_var).grid(row=0, column=3, sticky="w", padx=(8, 0))

        table_frame = ttk.LabelFrame(container, text="台データ", padding=8)
        table_frame.grid(row=2, column=0, sticky="nsew")
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

    def fetch_data(self) -> None:
        self.fetch_button.configure(state="disabled")
        self.status_var.set("取得中...")
        self.summary_var.set("処理開始")
        self._clear_table()

        worker = threading.Thread(target=self._worker_fetch, daemon=True)
        worker.start()
        self.root.after(100, self._poll_queue)

    def _worker_fetch(self) -> None:
        try:
            result = self.scraper.fetch_machine_dataset(
                store_url=self.store_url_var.get(),
                target_date_input=self.target_date_var.get(),
                machine_name=self.machine_name_var.get(),
            )
            self.result_queue.put(("success", result))
        except Exception as exc:  # noqa: BLE001
            self.result_queue.put(("error", exc))

    def _poll_queue(self) -> None:
        try:
            kind, payload = self.result_queue.get_nowait()
        except queue.Empty:
            self.root.after(100, self._poll_queue)
            return

        self.fetch_button.configure(state="normal")

        if kind == "error":
            self.status_var.set("失敗")
            self.summary_var.set("取得できませんでした")
            self._show_error(payload)
            return

        result = payload
        if not isinstance(result, MachineDataset):
            self.status_var.set("失敗")
            self.summary_var.set("不明な結果")
            messagebox.showerror("エラー", "取得結果の形式が不正です。")
            return

        self.current_result = result
        self._populate_table(result)
        self.status_var.set("完了")
        self.summary_var.set(
            f"{result.store_name} / {result.target_date} / {result.machine_name} / {len(result.rows)}台"
        )

    def _populate_table(self, result: MachineDataset) -> None:
        self.tree["columns"] = result.columns
        for column in result.columns:
            self.tree.heading(column, text=column)
            width = 110 if column not in {"台番", "差枚"} else 90
            self.tree.column(column, width=width, minwidth=80, anchor="center")

        for row in result.rows:
            self.tree.insert("", "end", values=row)

    def _clear_table(self) -> None:
        self.tree.delete(*self.tree.get_children())
        self.tree["columns"] = ()

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
