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
import {
  calculateSettingEstimate,
  formatSettingEstimateAverage,
  formatSettingEstimateBreakdown,
  getSettingEstimateDefinition,
} from "../lib/setting-estimates";
import { CsvExportButton } from "./csv-export-button";

const DAY_TAIL_OPTIONS = Array.from({ length: 10 }, (_, value) => value);
const DEFAULT_VISIBLE_METRIC_KEYS = [
  "difference_value",
  "games_count",
  "bb_count",
  "rb_count",
  "combined_ratio_text",
  "setting_estimate",
];
const MATRIX_DATE_COLUMN_WIDTH_REM = 4.8;
const MATRIX_SLOT_WIDTH_REM = 16;
const settingEstimateCache = new WeakMap();

function getSettingEstimate(definition, record) {
  if (!record) {
    return null;
  }
  if (!settingEstimateCache.has(record)) {
    settingEstimateCache.set(record, new Map());
  }
  const recordCache = settingEstimateCache.get(record);
  if (recordCache.has(definition.key)) {
    return recordCache.get(definition.key);
  }
  const estimate = calculateSettingEstimate(definition, record);
  recordCache.set(definition.key, estimate);
  return estimate;
}

function createSettingEstimateMetric(definition) {
  const renderSettingEstimate = (_value, record) =>
    formatSettingEstimateAverage(getSettingEstimate(definition, record));
  const titleSettingEstimate = (_value, record) =>
    formatSettingEstimateBreakdown(getSettingEstimate(definition, record));

  return {
    key: "setting_estimate",
    label: "設定",
    render: renderSettingEstimate,
    csvRender: renderSettingEstimate,
    title: titleSettingEstimate,
    columnClass: "matrixColumnNarrow",
  };
}

function getSettingEstimateHighlightClass(definition, record) {
  const estimate = getSettingEstimate(definition, record);
  if (!estimate) {
    return "";
  }
  if (estimate.average >= 5) {
    return "settingEstimateLevel3";
  }
  if (estimate.average >= 4.5) {
    return "settingEstimateLevel2";
  }
  if (estimate.average >= 4) {
    return "settingEstimateLevel1";
  }
  return "";
}

const COMMON_METRICS = [
  {
    key: "difference_value",
    label: "差枚",
    render: formatNarrowSignedNumber,
    csvRender: formatSignedNumber,
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
];

const RATIO_METRICS = [
  { key: "bb_ratio_text", label: "BB率", render: formatRatio, columnClass: "matrixColumnWide" },
  { key: "rb_ratio_text", label: "RB率", render: formatRatio, columnClass: "matrixColumnWide" },
];

function getMetrics(settingEstimateDefinition) {
  if (settingEstimateDefinition) {
    return [
      ...COMMON_METRICS,
      createSettingEstimateMetric(settingEstimateDefinition),
      ...RATIO_METRICS,
    ];
  }
  return [...COMMON_METRICS, ...RATIO_METRICS];
}

function buildCsvRows(slotNumbers, dateRows, metrics) {
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
        cells.push((metric.csvRender ?? metric.render)(value, record));
      }
    }
    return cells;
  });

  return [headerRow1, headerRow2, ...dataRows];
}

const MatrixRow = memo(function MatrixRow({
  row,
  slotNumbers,
  visibleMetrics,
  isHighlighted,
  settingEstimateDefinition,
}) {
  return (
    <tr className={isHighlighted ? "matrixRowHighlighted" : ""}>
      <th className="dateCell">{formatShortDate(row.date)}</th>
      {slotNumbers.flatMap((slotNumber, slotIndex) => {
        const record = row.recordsBySlot[slotNumber] ?? null;
        const settingHighlightClass = settingEstimateDefinition
          ? getSettingEstimateHighlightClass(settingEstimateDefinition, record)
          : "";
        const settingTitle = settingEstimateDefinition
          ? formatSettingEstimateBreakdown(getSettingEstimate(settingEstimateDefinition, record))
          : "";
        const isLastSlot = slotIndex === slotNumbers.length - 1;

        return visibleMetrics.map((metric, metricIndex) => {
          const value = record?.[metric.key];
          const toneClass = metric.tone ? valueToneClass(metric.key, value) : "";
          const boundaryClass =
            !isLastSlot && metricIndex === visibleMetrics.length - 1 ? "slotGroupBoundary" : "";
          const className = [toneClass, settingHighlightClass, boundaryClass]
            .filter(Boolean)
            .join(" ");
          const title = settingTitle || (metric.title ? metric.title(value, record) : "");
          return (
            <td
              key={`${row.date}-${slotNumber}-${metric.key}`}
              className={className || undefined}
              title={title || undefined}
            >
              {metric.render(value, record)}
            </td>
          );
        });
      })}
    </tr>
  );
});

export function MachineComparison({
  machineName,
  slotNumbers,
  dateRows,
  initialEventFilters,
  initialEventDisplayMode = "highlight",
}) {
  const [eventFilters, setEventFilters] = useState(() =>
    createEventFilters(initialEventFilters?.dayTails ?? [], initialEventFilters?.zoro ?? false),
  );
  const [eventDisplayMode, setEventDisplayMode] = useState(initialEventDisplayMode);
  const [visibleMetricKeys, setVisibleMetricKeys] = useState(DEFAULT_VISIBLE_METRIC_KEYS);
  const [isPending, startTransition] = useTransition();
  const settingEstimateDefinition = useMemo(
    () => getSettingEstimateDefinition(machineName),
    [machineName],
  );
  const metrics = useMemo(() => getMetrics(settingEstimateDefinition), [settingEstimateDefinition]);

  const visibleMetrics = useMemo(
    () => metrics.filter((metric) => visibleMetricKeys.includes(metric.key)),
    [metrics, visibleMetricKeys],
  );

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

  const csvRows = useMemo(
    () => buildCsvRows(slotNumbers, visibleRows, visibleMetrics),
    [slotNumbers, visibleRows, visibleMetrics],
  );

  const tableStyle = useMemo(() => {
    const visibleMetricCount = Math.max(visibleMetrics.length, 1);
    const cellFontSize = Math.min(0.96, Math.max(0.64, 1.08 - visibleMetricCount * 0.06));
    const headerFontSize = Math.min(0.88, Math.max(0.62, cellFontSize - 0.04));
    const dateFontSize = Math.min(0.8, cellFontSize);

    return {
      "--matrix-date-column-width": `${MATRIX_DATE_COLUMN_WIDTH_REM}rem`,
      "--matrix-metric-column-width": `${MATRIX_SLOT_WIDTH_REM / visibleMetricCount}rem`,
      "--matrix-table-width": `${MATRIX_DATE_COLUMN_WIDTH_REM + slotNumbers.length * MATRIX_SLOT_WIDTH_REM}rem`,
      "--matrix-cell-font-size": `${cellFontSize}rem`,
      "--matrix-header-font-size": `${headerFontSize}rem`,
      "--matrix-date-font-size": `${dateFontSize}rem`,
    };
  }, [slotNumbers.length, visibleMetrics.length]);

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

  const toggleMetric = (metricKey) => {
    setVisibleMetricKeys((currentKeys) => {
      const currentSet = new Set(currentKeys);
      const currentVisibleCount = metrics.filter((metric) => currentSet.has(metric.key)).length;

      if (currentSet.has(metricKey)) {
        if (currentVisibleCount === 1) {
          return currentKeys;
        }
        currentSet.delete(metricKey);
      } else {
        currentSet.add(metricKey);
      }

      return metrics.filter((metric) => currentSet.has(metric.key)).map((metric) => metric.key);
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
              onClick={() => updateDisplayMode("highlight")}
              className={`dayFilterChip ${eventDisplayMode === "highlight" ? "dayFilterChipActive" : ""}`}
              aria-pressed={eventDisplayMode === "highlight"}
            >
              強調
            </button>
            <button
              type="button"
              onClick={() => updateDisplayMode("filter")}
              className={`dayFilterChip ${eventDisplayMode === "filter" ? "dayFilterChipActive" : ""}`}
              aria-pressed={eventDisplayMode === "filter"}
            >
              絞込
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
        <div className="filterControlGroup">
          <p className="filterControlLabel">表示する列</p>
          <div className="metricToggleRow">
            {metrics.map((metric) => {
              const isChecked = visibleMetricKeys.includes(metric.key);
              const isLastVisible = isChecked && visibleMetrics.length === 1;

              return (
                <label
                  key={metric.key}
                  className={`metricToggleChip ${isChecked ? "metricToggleChipActive" : ""}`}
                >
                  <input
                    type="checkbox"
                    checked={isChecked}
                    disabled={isLastVisible}
                    onChange={() => toggleMetric(metric.key)}
                  />
                  <span>{metric.label}</span>
                </label>
              );
            })}
          </div>
        </div>
      </section>

      <MachineComparisonTable
        machineName={machineName}
        slotNumbers={slotNumbers}
        dateRows={visibleRows}
        visibleMetrics={visibleMetrics}
        highlightedDateSet={highlightedDateSet}
        settingEstimateDefinition={settingEstimateDefinition}
        csvRows={csvRows}
        tableStyle={tableStyle}
      />
    </>
  );
}

function MachineComparisonTable({
  machineName,
  slotNumbers,
  dateRows,
  visibleMetrics,
  highlightedDateSet,
  settingEstimateDefinition,
  csvRows,
  tableStyle,
}) {
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
        <table className="matrixTable" style={tableStyle}>
          <colgroup>
            <col className="matrixDateColumn" />
            {slotNumbers.flatMap((slotNumber) =>
              visibleMetrics.map((metric) => (
                <col key={`${slotNumber}-${metric.key}`} className="matrixMetricColumn" />
              )),
            )}
          </colgroup>
          <thead>
            <tr>
              <th rowSpan={2} className="dateHeaderCell">
                日付
              </th>
              {slotNumbers.map((slotNumber, slotIndex) => (
                <th
                  key={slotNumber}
                  colSpan={visibleMetrics.length}
                  className={`slotHeader ${
                    slotIndex === slotNumbers.length - 1 ? "" : "slotGroupBoundary"
                  }`}
                >
                  {slotNumber}番台
                </th>
              ))}
            </tr>
            <tr>
              {slotNumbers.flatMap((slotNumber, slotIndex) =>
                visibleMetrics.map((metric, metricIndex) => (
                  <th
                    key={`${slotNumber}-${metric.key}`}
                    className={`metricHeader ${
                      slotIndex !== slotNumbers.length - 1 &&
                      metricIndex === visibleMetrics.length - 1
                        ? "slotGroupBoundary"
                        : ""
                    }`}
                  >
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
                visibleMetrics={visibleMetrics}
                isHighlighted={highlightedDateSet.has(row.date)}
                settingEstimateDefinition={settingEstimateDefinition}
              />
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
