"use client";

import { memo, useCallback, useMemo, useState, useTransition } from "react";

import {
  formatAverageGames,
  formatNarrowInteger,
  formatNarrowPercent,
  formatNarrowSignedNumber,
  formatPercent,
  formatRatio,
  formatShortDate,
  formatSignedNumber,
  formatWeekday,
  valueToneClass,
} from "../lib/format";
import { createEventFilters, matchesEventFilters } from "../lib/event-filters";
import {
  calculateGameCountEstimate,
  calculateSettingEstimate,
  formatSettingEstimateAverage,
  formatSettingEstimateBreakdown,
  formatSettingEstimateScore,
  getSettingEstimateScoreRange,
  getSettingEstimateDefinition,
} from "../lib/setting-estimates";
import { CsvExportButton } from "./csv-export-button";

const DAY_TAIL_OPTIONS = Array.from({ length: 10 }, (_, value) => value);
const WEEKDAY_FILTER_OPTIONS = [
  { value: 0, label: "日曜" },
  { value: 1, label: "月曜" },
  { value: 2, label: "火曜" },
  { value: 3, label: "水曜" },
  { value: 4, label: "木曜" },
  { value: 5, label: "金曜" },
  { value: 6, label: "土曜" },
];
const DEFAULT_VISIBLE_METRIC_KEYS = [
  "difference_value",
  "games_count",
  "bb_count",
  "rb_count",
  "combined_ratio_text",
  "setting_estimate",
];
const MATRIX_DATE_COLUMN_WIDTH_REM = 4.8;
const MATRIX_WEEKDAY_COLUMN_WIDTH_REM = 2.4;
const MATRIX_SLOT_WIDTH_REM = 16;
const DEFAULT_GAME_MIN_GAMES = 6000;
const DEFAULT_GAME_MAX_GAMES = 9000;
const DEFAULT_GAME_EXPONENT = 1.5;
const COMPARISON_SCORE_EPSILON = 0.000000001;
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

function isJugglerMachine(machineName) {
  return String(machineName ?? "").normalize("NFKC").includes("ジャグラー");
}

function createDefaultEstimateOptions(slotCount, machineName) {
  if (isJugglerMachine(machineName)) {
    return {
      dataWeight: 100,
      gameEnabled: false,
      gameWeight: 0,
      comparisonEnabled: false,
      comparisonWeight: 0,
      minGames: DEFAULT_GAME_MIN_GAMES,
      maxGames: DEFAULT_GAME_MAX_GAMES,
      gameExponent: DEFAULT_GAME_EXPONENT,
    };
  }

  const isSmallMachine = slotCount <= 8;

  return {
    dataWeight: isSmallMachine ? 20 : 80,
    gameEnabled: true,
    gameWeight: isSmallMachine ? 40 : 20,
    comparisonEnabled: isSmallMachine,
    comparisonWeight: isSmallMachine ? 40 : 0,
    minGames: DEFAULT_GAME_MIN_GAMES,
    maxGames: DEFAULT_GAME_MAX_GAMES,
    gameExponent: DEFAULT_GAME_EXPONENT,
  };
}

function readWeight(value) {
  const weight = Number(value);
  return Number.isFinite(weight) ? Math.max(0, weight) : 0;
}

function formatWeight(value) {
  const rounded = Math.round(value * 10) / 10;
  return Number.isInteger(rounded) ? String(rounded) : rounded.toFixed(1);
}

function isSpecialEventDate(date, eventFilters) {
  return Boolean(eventFilters?.isActive) && matchesEventFilters(date, eventFilters);
}

function buildWeightedAverage(parts) {
  const activeParts = parts.filter(
    (part) => part && Number.isFinite(part.score) && readWeight(part.weight) > 0,
  );
  const totalWeight = activeParts.reduce((sum, part) => sum + readWeight(part.weight), 0);

  if (totalWeight <= 0) {
    return null;
  }

  return {
    average:
      activeParts.reduce((sum, part) => sum + part.score * readWeight(part.weight), 0) /
      totalWeight,
    parts: activeParts,
    totalWeight,
  };
}

function calculateComparisonBaseScore(definition, record, options) {
  const dataEstimate = getSettingEstimate(definition, record);
  const gameEstimate = options.gameEnabled
    ? calculateGameCountEstimate(definition, record, options)
    : null;
  const weighted = buildWeightedAverage([
    dataEstimate
      ? {
          score: dataEstimate.average,
          weight: options.dataWeight,
        }
      : null,
    gameEstimate
      ? {
          score: gameEstimate.average,
          weight: options.gameWeight,
        }
      : null,
  ]);

  return weighted?.average ?? null;
}

function buildComparisonEstimateMap(definition, slotNumbers, dateRows, eventFilters, options) {
  const comparisonEstimateMap = new WeakMap();

  if (!definition || !options.comparisonEnabled || readWeight(options.comparisonWeight) <= 0) {
    return comparisonEstimateMap;
  }

  const { minSetting, maxSetting } = getSettingEstimateScoreRange(definition);

  for (const row of dateRows) {
    if (!isSpecialEventDate(row.date, eventFilters)) {
      continue;
    }

    const candidates = slotNumbers
      .map((slotNumber) => ({
        slotNumber,
        record: row.recordsBySlot[slotNumber] ?? null,
      }))
      .filter((candidate) => candidate.record)
      .map((candidate) => ({
        ...candidate,
        baseScore: calculateComparisonBaseScore(definition, candidate.record, options),
      }))
      .filter((candidate) => Number.isFinite(candidate.baseScore))
      .sort((left, right) => {
        if (Math.abs(right.baseScore - left.baseScore) > COMPARISON_SCORE_EPSILON) {
          return right.baseScore - left.baseScore;
        }
        return String(left.slotNumber).localeCompare(String(right.slotNumber), "ja");
      });

    const total = candidates.length;
    if (total === 0) {
      continue;
    }

    let index = 0;
    while (index < total) {
      let endIndex = index + 1;
      while (
        endIndex < total &&
        Math.abs(candidates[endIndex].baseScore - candidates[index].baseScore) <=
          COMPARISON_SCORE_EPSILON
      ) {
        endIndex += 1;
      }

      const averageIndex = (index + endIndex - 1) / 2;
      const score =
        total === 1
          ? maxSetting
          : maxSetting - ((maxSetting - minSetting) * averageIndex) / (total - 1);

      for (let candidateIndex = index; candidateIndex < endIndex; candidateIndex += 1) {
        comparisonEstimateMap.set(candidates[candidateIndex].record, {
          average: score,
          rank: index + 1,
          total,
          baseScore: candidates[candidateIndex].baseScore,
        });
      }

      index = endIndex;
    }
  }

  return comparisonEstimateMap;
}

function buildCompositeSettingEstimate(definition, record, comparisonEstimateMap, options) {
  if (!definition || !record) {
    return null;
  }

  const dataEstimate = getSettingEstimate(definition, record);
  const gameEstimate = options.gameEnabled
    ? calculateGameCountEstimate(definition, record, options)
    : null;
  const comparisonEstimate = options.comparisonEnabled
    ? comparisonEstimateMap.get(record) ?? null
    : null;
  const weighted = buildWeightedAverage([
    dataEstimate
      ? {
          key: "data",
          label: "データ推測",
          score: dataEstimate.average,
          weight: options.dataWeight,
        }
      : null,
    gameEstimate
      ? {
          key: "games",
          label: "G数推測",
          score: gameEstimate.average,
          weight: options.gameWeight,
          detail: `${Math.round(gameEstimate.games)}G`,
        }
      : null,
    comparisonEstimate
      ? {
          key: "comparison",
          label: "比較推測",
          score: comparisonEstimate.average,
          weight: options.comparisonWeight,
          detail: `特定日内 ${comparisonEstimate.rank}/${comparisonEstimate.total}位`,
        }
      : null,
  ]);

  if (!weighted) {
    return null;
  }

  return {
    average: weighted.average,
    parts: weighted.parts,
    totalWeight: weighted.totalWeight,
    dataEstimate,
    gameEstimate,
    comparisonEstimate,
  };
}

function formatCompositeSettingEstimateBreakdown(estimate) {
  if (!estimate) {
    return "";
  }

  const lines = [`推測設定: ${formatSettingEstimateScore(estimate.average)}`];

  if (estimate.parts.length > 0) {
    lines.push(
      ...estimate.parts.map((part) => {
        const detail = part.detail ? ` / ${part.detail}` : "";
        return `${part.label}: ${formatSettingEstimateScore(part.score)} / 重み${formatWeight(
          readWeight(part.weight),
        )}%${detail}`;
      }),
    );
  }

  if (estimate.totalWeight !== 100) {
    lines.push(`計算重み合計: ${formatWeight(estimate.totalWeight)}%`);
  }

  if (estimate.dataEstimate) {
    const dataBreakdown = formatSettingEstimateBreakdown(estimate.dataEstimate)
      .split("\n")
      .slice(1);
    if (dataBreakdown.length > 0) {
      lines.push("データ推測の割合:", ...dataBreakdown);
    }
  }

  return lines.join("\n");
}

function createSettingEstimateMetric(getCompositeSettingEstimate) {
  const renderSettingEstimate = (_value, record) =>
    formatSettingEstimateAverage(getCompositeSettingEstimate(record));
  const titleSettingEstimate = (_value, record) =>
    formatCompositeSettingEstimateBreakdown(getCompositeSettingEstimate(record));

  return {
    key: "setting_estimate",
    label: "設定",
    render: renderSettingEstimate,
    csvRender: renderSettingEstimate,
    title: titleSettingEstimate,
    columnClass: "matrixColumnNarrow",
  };
}

function getSettingEstimateHighlightClass(estimate) {
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

function getMetrics(settingEstimateDefinition, getCompositeSettingEstimate) {
  if (settingEstimateDefinition) {
    return [
      ...COMMON_METRICS,
      createSettingEstimateMetric(getCompositeSettingEstimate),
      ...RATIO_METRICS,
    ];
  }
  return [...COMMON_METRICS, ...RATIO_METRICS];
}

function buildCsvRows(slotNumbers, dateRows, metrics, specialDateSet) {
  const headerRow1 = ["日付", "曜日", "特定日"];
  const headerRow2 = ["", "", ""];

  for (const slotNumber of slotNumbers) {
    for (let i = 0; i < metrics.length; i++) {
      headerRow1.push(i === 0 ? `${slotNumber}番台` : "");
      headerRow2.push(metrics[i].label);
    }
  }

  const dataRows = dateRows.map((row) => {
    const cells = [row.date, formatWeekday(row.date), specialDateSet.has(row.date) ? "はい" : "いいえ"];
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

function calculateActiveWeightTotal(options) {
  return (
    readWeight(options.dataWeight) +
    (options.gameEnabled ? readWeight(options.gameWeight) : 0) +
    (options.comparisonEnabled ? readWeight(options.comparisonWeight) : 0)
  );
}

function EstimateNumberField({
  label,
  value,
  min,
  max,
  step = 1,
  disabled = false,
  suffix = "",
  onChange,
}) {
  return (
    <label className="estimateField">
      <span>{label}</span>
      <span className="estimateInputWrap">
        <input
          type="number"
          value={value}
          min={min}
          max={max}
          step={step}
          disabled={disabled}
          onChange={(event) => {
            const nextValue = event.target.value;
            onChange(nextValue === "" ? "" : Number(nextValue));
          }}
        />
        {suffix ? <span className="estimateInputSuffix">{suffix}</span> : null}
      </span>
    </label>
  );
}

function SettingEstimateControls({ options, onChange }) {
  const activeWeightTotal = calculateActiveWeightTotal(options);
  const isWeightTotalValid = Math.abs(activeWeightTotal - 100) < 0.001;

  const updateOption = (key, value) => {
    onChange({ [key]: value });
  };

  return (
    <div className="estimateControlGrid">
      <div className="estimateControlHeader">
        <div>
          <p className="filterControlLabel">設定推測の比重</p>
          <p className="estimateHelpText">
            有効な項目だけを使い、合計が100%でない場合は比率として補正します。
          </p>
        </div>
        <p
          className={`estimateWeightTotal ${
            isWeightTotalValid ? "" : "estimateWeightTotalWarn"
          }`}
        >
          合計 {formatWeight(activeWeightTotal)}%
        </p>
      </div>

      <div className="estimateMethodRow">
        <div className="estimateMethodHeader">
          <div>
            <p className="estimateMethodTitle">データ推測</p>
            <p className="estimateHelpText">BBとRBから出す既存の推測です。</p>
          </div>
        </div>
        <div className="estimateFields">
          <EstimateNumberField
            label="重み"
            value={options.dataWeight}
            min={0}
            max={100}
            suffix="%"
            onChange={(value) => updateOption("dataWeight", value)}
          />
        </div>
      </div>

      <div className="estimateMethodRow">
        <div className="estimateMethodHeader">
          <label className={`estimateToggle ${options.gameEnabled ? "estimateToggleActive" : ""}`}>
            <input
              type="checkbox"
              checked={options.gameEnabled}
              onChange={(event) => updateOption("gameEnabled", event.target.checked)}
            />
            <span>G数による推測</span>
          </label>
          <p className="estimateHelpText">最低G数から最大G数まで、指数に合わせて評価します。</p>
        </div>
        <div className="estimateFields">
          <EstimateNumberField
            label="重み"
            value={options.gameWeight}
            min={0}
            max={100}
            disabled={!options.gameEnabled}
            suffix="%"
            onChange={(value) => updateOption("gameWeight", value)}
          />
          <EstimateNumberField
            label="最低G数"
            value={options.minGames}
            min={0}
            disabled={!options.gameEnabled}
            suffix="G"
            onChange={(value) => updateOption("minGames", value)}
          />
          <EstimateNumberField
            label="最大G数"
            value={options.maxGames}
            min={1}
            disabled={!options.gameEnabled}
            suffix="G"
            onChange={(value) => updateOption("maxGames", value)}
          />
          <EstimateNumberField
            label="指数"
            value={options.gameExponent}
            min={0.1}
            step={0.1}
            disabled={!options.gameEnabled}
            onChange={(value) => updateOption("gameExponent", value)}
          />
        </div>
      </div>

      <div className="estimateMethodRow">
        <div className="estimateMethodHeader">
          <label
            className={`estimateToggle ${
              options.comparisonEnabled ? "estimateToggleActive" : ""
            }`}
          >
            <input
              type="checkbox"
              checked={options.comparisonEnabled}
              onChange={(event) => updateOption("comparisonEnabled", event.target.checked)}
            />
            <span>特定日6あり</span>
          </label>
          <p className="estimateHelpText">特定日行だけ、その機種内の相対順位を加えます。</p>
        </div>
        <div className="estimateFields">
          <EstimateNumberField
            label="重み"
            value={options.comparisonWeight}
            min={0}
            max={100}
            disabled={!options.comparisonEnabled}
            suffix="%"
            onChange={(value) => updateOption("comparisonWeight", value)}
          />
        </div>
      </div>
    </div>
  );
}

const MatrixRow = memo(function MatrixRow({
  row,
  slotNumbers,
  visibleMetrics,
  isHighlighted,
  settingEstimateDefinition,
  getCompositeSettingEstimate,
}) {
  return (
    <tr className={isHighlighted ? "matrixRowHighlighted" : ""}>
      <th className="dateCell">{formatShortDate(row.date)}</th>
      <td className="weekdayCell">{formatWeekday(row.date)}</td>
      {slotNumbers.flatMap((slotNumber, slotIndex) => {
        const record = row.recordsBySlot[slotNumber] ?? null;
        const settingEstimate =
          settingEstimateDefinition && getCompositeSettingEstimate
            ? getCompositeSettingEstimate(record)
            : null;
        const settingHighlightClass = getSettingEstimateHighlightClass(settingEstimate);
        const settingTitle = formatCompositeSettingEstimateBreakdown(settingEstimate);
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
  storeId,
  machineName,
  slotNumbers,
  dateRows,
  initialEventFilters,
  initialEventDisplayMode = "highlight",
}) {
  const [eventFilters, setEventFilters] = useState(() =>
    createEventFilters(
      initialEventFilters?.dayTails ?? [],
      initialEventFilters?.zoro ?? false,
      initialEventFilters?.weekdays ?? [],
    ),
  );
  const [eventDisplayMode, setEventDisplayMode] = useState(initialEventDisplayMode);
  const [visibleMetricKeys, setVisibleMetricKeys] = useState(DEFAULT_VISIBLE_METRIC_KEYS);
  const [estimateOptions, setEstimateOptions] = useState(() =>
    createDefaultEstimateOptions(slotNumbers.length, machineName),
  );
  const [isPending, startTransition] = useTransition();
  const settingEstimateDefinition = useMemo(
    () => getSettingEstimateDefinition(machineName),
    [machineName],
  );
  const comparisonEstimateMap = useMemo(
    () =>
      buildComparisonEstimateMap(
        settingEstimateDefinition,
        slotNumbers,
        dateRows,
        eventFilters,
        estimateOptions,
      ),
    [dateRows, estimateOptions, eventFilters, settingEstimateDefinition, slotNumbers],
  );
  const getCompositeSettingEstimate = useCallback(
    (record) =>
      buildCompositeSettingEstimate(
        settingEstimateDefinition,
        record,
        comparisonEstimateMap,
        estimateOptions,
      ),
    [comparisonEstimateMap, estimateOptions, settingEstimateDefinition],
  );
  const metrics = useMemo(
    () => getMetrics(settingEstimateDefinition, getCompositeSettingEstimate),
    [getCompositeSettingEstimate, settingEstimateDefinition],
  );

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

  const specialDateSet = useMemo(() => {
    if (!eventFilters.isActive) {
      return new Set();
    }

    return new Set(
      dateRows.filter((row) => matchesEventFilters(row.date, eventFilters)).map((row) => row.date),
    );
  }, [dateRows, eventFilters]);

  const highlightedDateSet = useMemo(() => {
    if (eventDisplayMode !== "highlight") {
      return new Set();
    }

    return specialDateSet;
  }, [eventDisplayMode, specialDateSet]);

  const csvRows = useMemo(
    () => buildCsvRows(slotNumbers, visibleRows, visibleMetrics, specialDateSet),
    [slotNumbers, specialDateSet, visibleRows, visibleMetrics],
  );

  const tableStyle = useMemo(() => {
    const visibleMetricCount = Math.max(visibleMetrics.length, 1);
    const cellFontSize = Math.min(0.96, Math.max(0.64, 1.08 - visibleMetricCount * 0.06));
    const headerFontSize = Math.min(0.88, Math.max(0.62, cellFontSize - 0.04));
    const dateFontSize = Math.min(0.8, cellFontSize);

    return {
      "--matrix-date-column-width": `${MATRIX_DATE_COLUMN_WIDTH_REM}rem`,
      "--matrix-weekday-column-width": `${MATRIX_WEEKDAY_COLUMN_WIDTH_REM}rem`,
      "--matrix-metric-column-width": `${MATRIX_SLOT_WIDTH_REM / visibleMetricCount}rem`,
      "--matrix-table-width": `${
        MATRIX_DATE_COLUMN_WIDTH_REM +
        MATRIX_WEEKDAY_COLUMN_WIDTH_REM +
        slotNumbers.length * MATRIX_SLOT_WIDTH_REM
      }rem`,
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

  const saveEventFilters = useCallback(
    (nextFilters) => {
      if (!storeId) {
        return;
      }

      fetch(`/api/stores/${encodeURIComponent(storeId)}/event-settings`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dayTails: nextFilters.dayTails,
          zoro: nextFilters.zoro,
          weekdays: nextFilters.weekdays,
        }),
      }).catch(() => {});
    },
    [storeId],
  );

  const clearFilters = () => {
    startTransition(() => {
      const nextFilters = createEventFilters();
      setEventFilters(nextFilters);
      saveEventFilters(nextFilters);
    });
  };

  const toggleDayTail = (dayTail) => {
    startTransition(() => {
      setEventFilters((currentFilters) => {
        const nextDayTails = currentFilters.dayTails.includes(dayTail)
          ? currentFilters.dayTails.filter((value) => value !== dayTail)
          : [...currentFilters.dayTails, dayTail];
        const nextFilters = createEventFilters(
          nextDayTails,
          currentFilters.zoro,
          currentFilters.weekdays,
        );
        saveEventFilters(nextFilters);
        return nextFilters;
      });
    });
  };

  const toggleZoro = () => {
    startTransition(() => {
      setEventFilters((currentFilters) => {
        const nextFilters = createEventFilters(
          currentFilters.dayTails,
          !currentFilters.zoro,
          currentFilters.weekdays,
        );
        saveEventFilters(nextFilters);
        return nextFilters;
      });
    });
  };

  const toggleWeekday = (weekday) => {
    startTransition(() => {
      setEventFilters((currentFilters) => {
        const currentWeekdays = currentFilters.weekdays ?? [];
        const nextWeekdays = currentWeekdays.includes(weekday)
          ? currentWeekdays.filter((value) => value !== weekday)
          : [...currentWeekdays, weekday];
        const nextFilters = createEventFilters(
          currentFilters.dayTails,
          currentFilters.zoro,
          nextWeekdays,
        );
        saveEventFilters(nextFilters);
        return nextFilters;
      });
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

  const updateEstimateOptions = useCallback((changes) => {
    setEstimateOptions((currentOptions) => ({
      ...currentOptions,
      ...changes,
    }));
  }, []);

  const displayCountText = isPending
    ? "切り替え中"
    : `表示 ${visibleRows.length} / ${dateRows.length}`;

  return (
    <>
      <section className="filterPanel">
        <div>
          <p className="sectionLabel">特定日を選ぶ</p>
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
          <p className="filterControlLabel">曜日</p>
          <div className="dayFilterRow">
            {WEEKDAY_FILTER_OPTIONS.map((option) => (
              <button
                key={option.value}
                type="button"
                onClick={() => toggleWeekday(option.value)}
                className={`dayFilterChip ${
                  eventFilters.weekdays?.includes(option.value) ? "dayFilterChipActive" : ""
                }`}
                aria-pressed={eventFilters.weekdays?.includes(option.value) ?? false}
              >
                {option.label}
              </button>
            ))}
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
        {settingEstimateDefinition ? (
          <div className="filterControlGroup">
            <SettingEstimateControls
              options={estimateOptions}
              onChange={updateEstimateOptions}
            />
          </div>
        ) : null}
      </section>

      <MachineComparisonTable
        machineName={machineName}
        slotNumbers={slotNumbers}
        dateRows={visibleRows}
        visibleMetrics={visibleMetrics}
        highlightedDateSet={highlightedDateSet}
        settingEstimateDefinition={settingEstimateDefinition}
        getCompositeSettingEstimate={getCompositeSettingEstimate}
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
  getCompositeSettingEstimate,
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
            <col className="matrixWeekdayColumn" />
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
              <th rowSpan={2} className="weekdayHeaderCell">
                曜日
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
                getCompositeSettingEstimate={getCompositeSettingEstimate}
              />
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
