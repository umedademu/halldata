"use client";

import { useCallback } from "react";

/**
 * 台データ比較の表をCSVとしてダウンロードするボタン。
 *
 * @param {{ machineName: string, slotNumbers: number[], dateRows: object[], metrics: { key: string, label: string, render: (v: any) => string }[] }} props
 */
export function CsvExportButton({ machineName, slotNumbers, dateRows, metrics }) {
  const handleExport = useCallback(() => {
    const headerRow1 = ["日付"];
    const headerRow2 = [""];

    for (const slotNumber of slotNumbers) {
      for (let i = 0; i < metrics.length; i++) {
        headerRow1.push(i === 0 ? `${slotNumber}番台` : "");
        headerRow2.push(metrics[i].label);
      }
    }

    const dataRows = dateRows.map((row) => {
      const cells = [row.date];
      for (const slotNumber of slotNumbers) {
        const record = row.recordsBySlot[slotNumber] ?? null;
        for (const metric of metrics) {
          const value = record?.[metric.key];
          cells.push(metric.render(value));
        }
      }
      return cells;
    });

    const allRows = [headerRow1, headerRow2, ...dataRows];

    const bom = "\uFEFF";
    const csvText =
      bom +
      allRows
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
  }, [machineName, slotNumbers, dateRows, metrics]);

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
