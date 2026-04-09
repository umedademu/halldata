"use client";

import { useCallback } from "react";

/**
 * 台データ比較の表をCSVとしてダウンロードするボタン。
 * サーバー側で組み立て済みの文字列二次元配列を受け取る。
 *
 * @param {{ machineName: string, csvRows: string[][] }} props
 */
export function CsvExportButton({ machineName, csvRows }) {
  const handleExport = useCallback(() => {
    const bom = "\uFEFF";
    const csvText =
      bom +
      csvRows
        .map((row) =>
          row
            .map((cell) => {
              const text = String(cell ?? "");
              if (/[,"\n\r]/.test(text)) {
                return `"${text.replace(/"/g, '""')}"`;
              }
              return text;
            })
            .join(","),
        )
        .join("\r\n");

    const blob = new Blob([csvText], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);

    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${machineName}_台データ比較.csv`;
    document.body.appendChild(anchor);
    anchor.click();

    document.body.removeChild(anchor);
    URL.revokeObjectURL(url);
  }, [machineName, csvRows]);

  return (
    <button
      type="button"
      className="csvExportBtn"
      onClick={handleExport}
      title="台データ比較をCSVとしてダウンロード"
    >
      <svg
        width="16"
        height="16"
        viewBox="0 0 16 16"
        fill="none"
        aria-hidden="true"
        style={{ flexShrink: 0 }}
      >
        <path
          d="M8 1v9m0 0L5 7m3 3 3-3M2.5 11v2a1.5 1.5 0 0 0 1.5 1.5h8A1.5 1.5 0 0 0 13.5 13v-2"
          stroke="currentColor"
          strokeWidth="1.6"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
      CSV出力
    </button>
  );
}
