"use client";

import { memo, useMemo, useState, useTransition } from "react";

import {
  formatAverageGames,
  formatNarrowInteger,
  formatNarrowPercent,
  formatNarrowSignedNumber,
  formatPercent,
  formatRatio,
  formatShortDate,
  formatSignedNumber,
  valueToneClass,
} from "../lib/format";
import { createEventFilters, matchesEventFilters } from "../lib/event-filters";
import { CsvExportButton } from "./csv-export-button";

const DAY_TAIL_OPTIONS = Array.from({ length: 10 }, (_, value) => value);

const METRICS = [
  {
    key: "difference_value",
    label: "差枚",
    render: formatNarrowSignedNumber,
    csvRender: formatSignedNumber,
    tone: true,
    columnClass: "matrixColumnWide",
  },
  {
    key: "games_count",
    label: "G数",
    render: formatNarrowInteger,
    csvRender: formatAverageGames,
    columnClass: "matrixColumnMedium",
  },
  {
    key: "payout_rate",
    label: "出率",
    render: formatNarrowPercent,
    csvRender: formatPercent,
    tone: true,
    columnClass: "matrixColumnWide",
  },
  {
    key: "bb_count",
    label: "BB",
    render: formatNarrowInteger,
    csvRender: formatAverageGames,
    columnClass: "matrixColumnNarrow",
  },
  {
    key: "rb_count",
    label: "RB",
    render: formatNarrowInteger,
    csvRender: formatAverageGames,
    columnClass: "matrixColumnNarrow",
  },
  { key: "combined_ratio_text", label: "合成", render: formatRatio, columnClass: "matrixColumnWide" },
  { key: "bb_ratio_text", label: "BB率", render: formatRatio, columnClass: "matrixColumnWide" },
  { key: "rb_ratio_text", label: "RB率", render: formatRatio, columnClass: "matrixColumnWide" },
];

function buildCsvRows(slotNumbers, dateRows) {
  const headerRow1 = ["日付"];
  const headerRow2 = [""];

  for (const slotNumber of slotNumbers) {
    for (let i = 0; i < METRICS.length; i++) {
      headerRow1.push(i === 0 ? `${slotNumber}番台` : "");
      headerRow2.push(METRICS[i].label);
    }
  }

  const dataRows = dateRows.map((row) => {
    const cells = [row.date];
    for (const slotNumber of slotNumbers) {
      const record = row.recordsBySlot[slotNumber] ?? null;
      for (const metric of METRICS) {
        const value = record?.[metric.key];
        cells.push((metric.csvRender ?? metric.render)(value));
      }
    }
    return cells;
  });

  return [headerRow1, headerRow2, ...dataRows];
}

const MatrixRow = memo(function MatrixRow({ row, slotNumbers, isHighlighted }) {
  return (
    <tr className={isHighlighted ? "matrixRowHighlighted" : ""}>
      <th className="dateCell">{formatShortDate(row.date)}</th>
      {slotNumbers.flatMap((slotNumber) =>
        METRICS.map((metric) => {
          const record = row.recordsBySlot[slotNumber] ?? null;
          const value = record?.[metric.key];
          const toneClass = metric.tone ? valueToneClass(metric.key, value) : "";
          return (
            <td key={`${row.date}-${slotNumber}-${metric.key}`} className={toneClass}>
              {metric.render(value)}
            </td>
          );
        }),
      )}
    </tr>
  );
});

export function MachineComparison({
  machineName,
  slotNumbers,
  dateRows,
  initialEventFilters,
  initialEventDisplayMode = "filter",
}) {
  const [eventFilters, setEventFilters] = useState(() =>
    createEventFilters(initialEventFilters?.dayTails ?? [], initialEventFilters?.zoro ?? false),
  );
  const [eventDisplayMode, setEventDisplayMode] = useState(initialEventDisplayMode);
  const [isPending, startTransition] = useTransition();

  const visibleRows = useMemo(() => {
    if (eventDisplayMode === "highlight") {
      return dateRows;
    }
    return dateRows.filter((row) => matchesEventFilters(row.date, eventFilters));
  }, [dateRows, eventDisplayMode, eventFilters]);

  const highlightedDateSet = useMemo(() => {
    if (eventDisplayMode !== "highlight" || !eventFilters.isActive) {
      return new Set();
    }

    return new Set(
      dateRows.filter((row) => matchesEventFilters(row.date, eventFilters)).map((row) => row.date),
    );
  }, [dateRows, eventDisplayMode, eventFilters]);

  const csvRows = useMemo(() => buildCsvRows(slotNumbers, visibleRows), [slotNumbers, visibleRows]);

  const updateDisplayMode = (mode) => {
    startTransition(() => {
      setEventDisplayMode(mode);
    });
  };

  const clearFilters = () => {
    startTransition(() => {
      setEventFilters(createEventFilters());
    });
  };

  const toggleDayTail = (dayTail) => {
    startTransition(() => {
      setEventFilters((currentFilters) => {
        const nextDayTails = currentFilters.dayTails.includes(dayTail)
          ? currentFilters.dayTails.filter((value) => value !== dayTail)
          : [...currentFilters.dayTails, dayTail];
        return createEventFilters(nextDayTails, currentFilters.zoro);
      });
    });
  };

  const toggleZoro = () => {
    startTransition(() => {
      setEventFilters((currentFilters) =>
        createEventFilters(currentFilters.dayTails, !currentFilters.zoro),
      );
    });
  };

  const displayCountText = isPending
    ? "切り替え中"
    : `表示 ${visibleRows.length} / ${dateRows.length}`;

  return (
    <>
      <section className="filterPanel">
        <div>
          <p className="sectionLabel">日付の末尾を選ぶ</p>
          <p className="filterLead">
            イベント日を絞り込むか、全日を表示したまま該当日だけを強調できます。
          </p>
        </div>
        <div className="filterPanelStatus">{displayCountText}</div>
        <div className="filterControlGroup">
          <p className="filterControlLabel">表示方法</p>
          <div className="dayFilterRow">
            <button
              type="button"
              onClick={() => updateDisplayMode("filter")}
              className={`dayFilterChip ${eventDisplayMode === "filter" ? "dayFilterChipActive" : ""}`}
              aria-pressed={eventDisplayMode === "filter"}
            >
              絞り込む
            </button>
            <button
              type="button"
              onClick={() => updateDisplayMode("highlight")}
              className={`dayFilterChip ${eventDisplayMode === "highlight" ? "dayFilterChipActive" : ""}`}
              aria-pressed={eventDisplayMode === "highlight"}
            >
              強調する
            </button>
          </div>
        </div>
        <div className="filterControlGroup">
          <p className="filterControlLabel">日付</p>
          <div className="dayFilterRow">
            <button
              type="button"
              onClick={clearFilters}
              className={`dayFilterChip ${eventFilters.isActive ? "" : "dayFilterChipActive"}`}
              aria-pressed={!eventFilters.isActive}
            >
              すべて
            </button>
            {DAY_TAIL_OPTIONS.map((value) => (
              <button
                key={value}
                type="button"
                onClick={() => toggleDayTail(value)}
                className={`dayFilterChip ${
                  eventFilters.dayTails.includes(value) ? "dayFilterChipActive" : ""
                }`}
                aria-pressed={eventFilters.dayTails.includes(value)}
              >
                末尾{value}
              </button>
            ))}
            <button
              type="button"
              onClick={toggleZoro}
              className={`dayFilterChip ${eventFilters.zoro ? "dayFilterChipActive" : ""}`}
              aria-pressed={eventFilters.zoro}
            >
              ゾロ目
            </button>
          </div>
        </div>
      </section>

      <MachineComparisonTable
        machineName={machineName}
        slotNumbers={slotNumbers}
        dateRows={visibleRows}
        highlightedDateSet={highlightedDateSet}
        csvRows={csvRows}
      />
    </>
  );
}

function MachineComparisonTable({ machineName, slotNumbers, dateRows, highlightedDateSet, csvRows }) {
  if (dateRows.length === 0) {
    return (
      <section className="statusPanel">
        <h2>条件に合う日付がありません</h2>
        <p>別の末尾に切り替えるか、すべて表示へ戻してください。</p>
      </section>
    );
  }

  return (
    <section className="tablePanel matrixPanel">
      <div className="tablePanelHeader">
        <div>
          <p className="sectionLabel">台データ比較</p>
          <h2 className="tablePanelTitle">{machineName}</h2>
        </div>
        <CsvExportButton
          machineName={machineName}
          csvRows={csvRows}
        />
      </div>
      <div className="tableScroller matrixScroller">
        <table className="matrixTable">
          <colgroup>
            <col className="matrixDateColumn" />
            {slotNumbers.flatMap((slotNumber) =>
              METRICS.map((metric) => (
                <col key={`${slotNumber}-${metric.key}`} className={metric.columnClass} />
              )),
            )}
          </colgroup>
          <thead>
            <tr>
              <th rowSpan={2} className="dateHeaderCell">
                日付
              </th>
              {slotNumbers.map((slotNumber) => (
                <th key={slotNumber} colSpan={METRICS.length} className="slotHeader">
                  {slotNumber}番台
                </th>
              ))}
            </tr>
            <tr>
              {slotNumbers.flatMap((slotNumber) =>
                METRICS.map((metric) => (
                  <th key={`${slotNumber}-${metric.key}`} className="metricHeader">
                    {metric.label}
                  </th>
                )),
              )}
            </tr>
          </thead>
          <tbody>
            {dateRows.map((row) => (
              <MatrixRow
                key={row.date}
                row={row}
                slotNumbers={slotNumbers}
                isHighlighted={highlightedDateSet.has(row.date)}
              />
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
