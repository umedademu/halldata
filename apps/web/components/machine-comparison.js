"use client";

import { memo, useCallback, useEffect, useMemo, useState, useTransition } from "react";

import {
  formatAverageGames,
  formatCompactDate,
  formatNumber,
  formatNarrowInteger,
  formatNarrowDecimal,
  formatNarrowPercent,
  formatNarrowSignedNumber,
  formatDecimal,
  formatPercent,
  formatRatio,
  formatShortDate,
  formatSignedNumber,
  formatWeekday,
  valueToneClass,
} from "../lib/format";
import { createEventFilters, matchesEventFilters } from "../lib/event-filters";
import { calculateHuntScoreDeviationMap } from "../lib/hunt-bookmark";
import { selectDifferenceValue } from "../lib/machine-difference";
import {
  calculateGameCountEstimate,
  calculateSettingEstimate,
  formatSettingEstimateAverage,
  formatSettingEstimateBreakdown,
  formatSettingEstimateScore,
  getSettingEstimateScoreRange,
  getSettingEstimateDefinition,
  getSettingEstimateHighlightClass,
} from "../lib/setting-estimates";
import { CsvExportButton } from "./csv-export-button";

const DAY_TAIL_OPTIONS = Array.from({ length: 10 }, (_, value) => value);
const WEEKDAY_FILTER_OPTIONS = [
  { value: 1, label: "月曜" },
  { value: 2, label: "火曜" },
  { value: 3, label: "水曜" },
  { value: 4, label: "木曜" },
  { value: 5, label: "金曜" },
  { value: 6, label: "土曜" },
  { value: 0, label: "日曜" },
];
const DEFAULT_VISIBLE_METRIC_KEYS = [
  "difference_value",
  "games_count",
  "bb_count",
  "rb_count",
  "combined_ratio_text",
  "setting_estimate",
  "hunt_score",
  "hunt_score_deviation",
];
const DEFAULT_DIFFERENCE_MODE = "bonus";
const MATRIX_DATE_COLUMN_WIDTH_REM = 4.8;
const MATRIX_WEEKDAY_COLUMN_WIDTH_REM = 2.4;
const MATRIX_SLOT_WIDTH_REM = 16;
const DEFAULT_GAME_MIN_GAMES = 6000;
const DEFAULT_GAME_MAX_GAMES = 9000;
const DEFAULT_GAME_EXPONENT = 1.5;
const DEFAULT_COMPARISON_RECENT_DAYS = 14;
const DEFAULT_HUNT_SCORE_HIGHLIGHT_THRESHOLD = 70;
const DEFAULT_HUNT_SCORE_DEVIATION_MIN = 60;
const DEFAULT_HUNT_SCORE_RANK_MIN = 1;
const DEFAULT_HUNT_SCORE_RANK_MAX = 3;
const DEFAULT_HUNT_SCORE_RANK_SCOPE = "selected";
const DEFAULT_HUNT_SCORE_DEVIATION_SCOPE = "selected";
const DEFAULT_HUNT_SCORE_MATCH_MODE = "or";
const HUNT_SCORE_HIGHLIGHT_STORAGE_PREFIX = "machine-hunt-score-highlight:";
const COMPARISON_SCORE_EPSILON = 0.000000001;
const settingEstimateCache = new WeakMap();

function isIsoDateText(value) {
  return typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/u.test(value.trim());
}

function formatCalendarDate(value) {
  const formatted = formatCompactDate(value);
  return formatted === "-" ? formatted : formatted.replaceAll("-", "/");
}

function parseDateText(value) {
  if (!isIsoDateText(value)) {
    return null;
  }

  const [year, month, day] = value.split("-").map((part) => Number(part));
  const date = new Date(year, month - 1, day);
  return Number.isNaN(date.getTime()) ? null : date;
}

function shiftDateText(value, offsetDays) {
  const date = parseDateText(value);
  if (!date) {
    return "";
  }

  date.setDate(date.getDate() + offsetDays);
  const year = String(date.getFullYear());
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function clampDateText(value, minDate, maxDate, fallbackDate) {
  if (!isIsoDateText(value)) {
    return fallbackDate;
  }
  if (minDate && value < minDate) {
    return minDate;
  }
  if (maxDate && value > maxDate) {
    return maxDate;
  }
  return value;
}

function normalizeRecentDaysInput(value) {
  const parsed = Number.parseInt(String(value ?? "").trim(), 10);
  if (!Number.isFinite(parsed)) {
    return DEFAULT_COMPARISON_RECENT_DAYS;
  }
  return Math.max(1, parsed);
}

function parseHuntScoreHighlightThreshold(value, fallbackValue = null) {
  const text = String(value ?? "").trim();
  if (text === "") {
    return fallbackValue;
  }
  const parsed = Number(text);
  if (!Number.isFinite(parsed)) {
    return fallbackValue;
  }
  return Math.min(100, Math.max(0, parsed));
}

function parsePositiveIntegerOption(value, fallbackValue = null) {
  const parsed = Number.parseInt(String(value ?? "").trim(), 10);
  return Number.isInteger(parsed) && parsed >= 1 ? parsed : fallbackValue;
}

function normalizeHuntScoreMatchMode(value) {
  return value === "and" ? "and" : DEFAULT_HUNT_SCORE_MATCH_MODE;
}

function normalizeHuntScoreRankScope(value) {
  if (value === "all" || value === "machine" || value === "selected") {
    return value;
  }
  return DEFAULT_HUNT_SCORE_RANK_SCOPE;
}

function buildHuntScoreRankFilter(rankMinValue, rankMaxValue) {
  const parsedRankMin = parsePositiveIntegerOption(rankMinValue);
  const parsedRankMax = parsePositiveIntegerOption(rankMaxValue);

  if (parsedRankMin === null && parsedRankMax === null) {
    return {
      rankMin: null,
      rankMax: null,
      hasRankFilter: false,
    };
  }

  const rankMin = parsedRankMin ?? DEFAULT_HUNT_SCORE_RANK_MIN;
  const rankMax = parsedRankMax ?? rankMin;

  return {
    rankMin: Math.min(rankMin, rankMax),
    rankMax: Math.max(rankMin, rankMax),
    hasRankFilter: true,
  };
}

function isHuntScoreValueMatched(value, threshold) {
  const score = Number(value);
  return Number.isFinite(score) && Number.isFinite(threshold) && score >= threshold;
}

function buildHuntScoreHighlightKey(date, machineName, slotNumber) {
  return `${String(date ?? "").trim()}\t${String(machineName ?? "").trim()}\t${String(
    slotNumber ?? "",
  ).trim()}`;
}

function createDefaultHuntScoreHighlightOptions(machineNames) {
  const availableMachineNames = normalizeAvailableHuntScoreMachineNames(machineNames);
  return {
    rankMin: DEFAULT_HUNT_SCORE_RANK_MIN,
    rankMax: DEFAULT_HUNT_SCORE_RANK_MAX,
    scoreMin: DEFAULT_HUNT_SCORE_HIGHLIGHT_THRESHOLD,
    deviationMin: DEFAULT_HUNT_SCORE_DEVIATION_MIN,
    matchMode: DEFAULT_HUNT_SCORE_MATCH_MODE,
    rankScope: DEFAULT_HUNT_SCORE_RANK_SCOPE,
    deviationScope: DEFAULT_HUNT_SCORE_DEVIATION_SCOPE,
    selectedMachineNames: availableMachineNames.filter(isJugglerMachine),
  };
}

function normalizeAvailableHuntScoreMachineNames(machineNames) {
  return [
    ...new Set(
      (Array.isArray(machineNames) ? machineNames : [])
        .map((machineName) => String(machineName ?? "").trim())
        .filter(Boolean),
    ),
  ];
}

function normalizeSelectedHuntScoreMachineNames(machineNames, availableMachineNames) {
  const availableMachineNameSet = new Set(availableMachineNames);
  return [
    ...new Set(
      (Array.isArray(machineNames) ? machineNames : [])
        .map((machineName) => String(machineName ?? "").trim())
        .filter((machineName) => availableMachineNameSet.has(machineName)),
    ),
  ];
}

function normalizeHuntScoreRankInputValue(value, fallbackValue) {
  if (String(value ?? "").trim() === "") {
    return "";
  }
  return parsePositiveIntegerOption(value, fallbackValue);
}

function normalizeHuntScoreScoreInputValue(value, fallbackValue) {
  if (String(value ?? "").trim() === "") {
    return "";
  }
  return parseHuntScoreHighlightThreshold(value, fallbackValue);
}

function normalizeHuntScoreDeviationInputValue(value, fallbackValue) {
  if (String(value ?? "").trim() === "") {
    return "";
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallbackValue;
}

function normalizeHuntScoreHighlightOptions(value, availableMachineNames) {
  const defaults = createDefaultHuntScoreHighlightOptions(availableMachineNames);
  if (!value || typeof value !== "object") {
    return defaults;
  }
  const rankScope = normalizeHuntScoreRankScope(value.rankScope);
  const deviationScope = normalizeHuntScoreRankScope(value.deviationScope ?? rankScope);

  return {
    rankMin: Object.hasOwn(value, "rankMin")
      ? normalizeHuntScoreRankInputValue(value.rankMin, defaults.rankMin)
      : defaults.rankMin,
    rankMax: Object.hasOwn(value, "rankMax")
      ? normalizeHuntScoreRankInputValue(value.rankMax, defaults.rankMax)
      : defaults.rankMax,
    scoreMin: Object.hasOwn(value, "scoreMin")
      ? normalizeHuntScoreScoreInputValue(value.scoreMin, defaults.scoreMin)
      : defaults.scoreMin,
    deviationMin: Object.hasOwn(value, "deviationMin")
      ? normalizeHuntScoreDeviationInputValue(value.deviationMin, defaults.deviationMin)
      : defaults.deviationMin,
    matchMode: normalizeHuntScoreMatchMode(value.matchMode),
    rankScope,
    deviationScope,
    selectedMachineNames: normalizeSelectedHuntScoreMachineNames(
      value.selectedMachineNames,
      availableMachineNames,
    ),
  };
}

function readHuntScoreHighlightOptions(storeId, availableMachineNames) {
  const defaults = createDefaultHuntScoreHighlightOptions(availableMachineNames);
  if (typeof window === "undefined") {
    return defaults;
  }

  try {
    const storageKey = `${HUNT_SCORE_HIGHLIGHT_STORAGE_PREFIX}${storeId}`;
    const storedValue = window.localStorage.getItem(storageKey);
    return storedValue
      ? normalizeHuntScoreHighlightOptions(JSON.parse(storedValue), availableMachineNames)
      : defaults;
  } catch {
    return defaults;
  }
}

function saveHuntScoreHighlightOptions(storeId, options) {
  if (typeof window === "undefined") {
    return;
  }

  try {
    const storageKey = `${HUNT_SCORE_HIGHLIGHT_STORAGE_PREFIX}${storeId}`;
    window.localStorage.setItem(storageKey, JSON.stringify(options));
  } catch {
    // 保存できない環境では、画面上の変更だけを有効にします。
  }
}

function parseHuntScoreDeviationThreshold(value) {
  const text = String(value ?? "").trim();
  if (text === "") {
    return null;
  }
  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : null;
}

function matchesHuntScoreHighlightCondition(
  rankValue,
  huntScore,
  deviationValue,
  rankFilter,
  scoreMin,
  deviationMin,
  matchMode,
) {
  const rankMatched =
    rankFilter.hasRankFilter &&
    Number.isInteger(rankValue) &&
    rankValue >= rankFilter.rankMin &&
    rankValue <= rankFilter.rankMax;
  const scoreMatched =
    Number.isFinite(scoreMin) && isHuntScoreValueMatched(huntScore, scoreMin);
  const deviationMatched =
    Number.isFinite(deviationMin) &&
    Number.isFinite(deviationValue) &&
    deviationValue >= deviationMin;
  const conditionMatches = [];

  if (rankFilter.hasRankFilter) {
    conditionMatches.push(rankMatched);
  }
  if (Number.isFinite(scoreMin)) {
    conditionMatches.push(scoreMatched);
  }
  if (Number.isFinite(deviationMin)) {
    conditionMatches.push(deviationMatched);
  }

  if (conditionMatches.length === 0) {
    return false;
  }

  return matchMode === "and"
    ? conditionMatches.every(Boolean)
    : conditionMatches.some(Boolean);
}

function buildHuntScoreDeviationValueMap(highlightDetail, options) {
  const valueMap = new Map();
  const snapshots = Array.isArray(highlightDetail?.snapshots) ? highlightDetail.snapshots : [];
  const normalizedOptions = normalizeHuntScoreHighlightOptions(
    options,
    normalizeAvailableHuntScoreMachineNames(highlightDetail?.availableMachineNames),
  );
  const selectedMachineNameSet = new Set(normalizedOptions.selectedMachineNames);
  const deviationScope = normalizeHuntScoreRankScope(normalizedOptions.deviationScope);

  for (const snapshot of snapshots) {
    const rows = Array.isArray(snapshot.rows) ? snapshot.rows : [];
    const overallDeviationMap = calculateHuntScoreDeviationMap(rows);
    const selectedRows = rows.filter((row) =>
      selectedMachineNameSet.has(String(row.machineName ?? "").trim()),
    );
    const selectedDeviationMap = calculateHuntScoreDeviationMap(selectedRows);
    const rowsByMachineName = new Map();

    for (const row of rows) {
      const machineName = String(row.machineName ?? "").trim();
      if (!machineName) {
        continue;
      }
      if (!rowsByMachineName.has(machineName)) {
        rowsByMachineName.set(machineName, []);
      }
      rowsByMachineName.get(machineName).push(row);
    }

    const machineDeviationMap = new Map();
    for (const machineRows of rowsByMachineName.values()) {
      const deviationMap = calculateHuntScoreDeviationMap(machineRows);
      for (const row of machineRows) {
        if (deviationMap.has(row)) {
          machineDeviationMap.set(row, deviationMap.get(row));
        }
      }
    }

    for (const row of rows) {
      const deviationValue =
        deviationScope === "all"
          ? overallDeviationMap.get(row)
          : deviationScope === "machine"
            ? machineDeviationMap.get(row)
            : selectedDeviationMap.get(row);

      if (Number.isFinite(deviationValue)) {
        valueMap.set(
          buildHuntScoreHighlightKey(snapshot.date, row.machineName, row.slotNumber),
          deviationValue,
        );
      }
    }
  }

  return valueMap;
}

function buildHuntScoreHighlightKeySet(highlightDetail, options, deviationValueMap) {
  const matchKeys = new Set();
  const snapshots = Array.isArray(highlightDetail?.snapshots) ? highlightDetail.snapshots : [];
  const normalizedOptions = normalizeHuntScoreHighlightOptions(
    options,
    normalizeAvailableHuntScoreMachineNames(highlightDetail?.availableMachineNames),
  );
  const selectedMachineNameSet = new Set(normalizedOptions.selectedMachineNames);
  const rankFilter = buildHuntScoreRankFilter(
    normalizedOptions.rankMin,
    normalizedOptions.rankMax,
  );
  const scoreMin = parseHuntScoreHighlightThreshold(normalizedOptions.scoreMin);
  const deviationMin = parseHuntScoreDeviationThreshold(normalizedOptions.deviationMin);
  const rankScope = normalizeHuntScoreRankScope(normalizedOptions.rankScope);
  const matchMode = normalizeHuntScoreMatchMode(normalizedOptions.matchMode);

  if (!rankFilter.hasRankFilter && !Number.isFinite(scoreMin) && !Number.isFinite(deviationMin)) {
    return matchKeys;
  }

  for (const snapshot of snapshots) {
    const machineRankCounts = new Map();
    let selectedRank = 0;

    for (const row of Array.isArray(snapshot.rows) ? snapshot.rows : []) {
      const machineName = String(row.machineName ?? "").trim();
      const slotNumber = String(row.slotNumber ?? "").trim();
      const huntScore = Number(row.huntScore);
      if (!machineName || !slotNumber || !Number.isFinite(huntScore)) {
        continue;
      }

      const machineRank = (machineRankCounts.get(machineName) ?? 0) + 1;
      machineRankCounts.set(machineName, machineRank);

      const isSelectedMachine = selectedMachineNameSet.has(machineName);
      const selectedRankValue = isSelectedMachine ? selectedRank + 1 : null;
      if (isSelectedMachine) {
        selectedRank += 1;
      }

      const rankValue =
        rankScope === "all"
          ? parsePositiveIntegerOption(row.rank)
          : rankScope === "machine"
            ? machineRank
            : selectedRankValue;
      const rowKey = buildHuntScoreHighlightKey(snapshot.date, machineName, slotNumber);
      const deviationValue = deviationValueMap.get(rowKey);

      if (
        matchesHuntScoreHighlightCondition(
          rankValue,
          huntScore,
          deviationValue,
          rankFilter,
          scoreMin,
          deviationMin,
          matchMode,
        )
      ) {
        matchKeys.add(rowKey);
      }
    }
  }

  return matchKeys;
}

function buildDisplayedPeriodLabel(startDate, endDate) {
  if (!startDate || !endDate) {
    return "日付データなし";
  }
  return `${formatCalendarDate(startDate)} ~ ${formatCalendarDate(endDate)}`;
}

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

function createHuntScoreMetric() {
  return {
    key: "hunt_score",
    label: "狙い度",
    render: formatNarrowInteger,
    csvRender: formatNumber,
    columnClass: "matrixColumnMedium",
  };
}

function createHuntScoreDeviationMetric(getHuntScoreDeviationValue) {
  const renderDeviation = (_value, record, context) =>
    formatNarrowDecimal(getHuntScoreDeviationValue(record, context));
  const csvRenderDeviation = (_value, record, context) =>
    formatDecimal(getHuntScoreDeviationValue(record, context));

  return {
    key: "hunt_score_deviation",
    label: "偏差値",
    render: renderDeviation,
    csvRender: csvRenderDeviation,
    columnClass: "matrixColumnMedium",
  };
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

function createDifferenceMetric(differenceMode) {
  return {
    key: "difference_value",
    label: "差枚",
    render: (_value, record) => formatNarrowSignedNumber(selectDifferenceValue(record, differenceMode)),
    csvRender: (_value, record) => formatSignedNumber(selectDifferenceValue(record, differenceMode)),
    columnClass: "matrixColumnWide",
  };
}

function getMetrics(
  settingEstimateDefinition,
  getCompositeSettingEstimate,
  hasHuntScore,
  differenceMode,
  getHuntScoreDeviationValue,
) {
  const metrics = [
    createDifferenceMetric(differenceMode),
    ...COMMON_METRICS.filter((metric) => metric.key !== "difference_value"),
  ];

  if (settingEstimateDefinition) {
    metrics.push(createSettingEstimateMetric(getCompositeSettingEstimate));
  }

  if (hasHuntScore) {
    metrics.push(createHuntScoreMetric());
    metrics.push(createHuntScoreDeviationMetric(getHuntScoreDeviationValue));
  }

  return [...metrics, ...RATIO_METRICS];
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
        cells.push((metric.csvRender ?? metric.render)(value, record, { row, slotNumber }));
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

function HuntScoreHighlightControls({ options, availableMachineNames, onChange }) {
  const selectedMachineNameSet = new Set(options.selectedMachineNames);

  const updateOption = (key, value) => {
    onChange({ ...options, [key]: value });
  };

  const toggleMachine = (machineName) => {
    const nextMachineNameSet = new Set(options.selectedMachineNames);
    if (nextMachineNameSet.has(machineName)) {
      nextMachineNameSet.delete(machineName);
    } else {
      nextMachineNameSet.add(machineName);
    }
    onChange({
      ...options,
      selectedMachineNames: availableMachineNames.filter((name) => nextMachineNameSet.has(name)),
    });
  };

  return (
    <div className="huntHighlightControls">
      <div className="estimateFields">
        <EstimateNumberField
          label="順位の開始"
          value={options.rankMin}
          min={1}
          onChange={(value) => updateOption("rankMin", value)}
        />
        <EstimateNumberField
          label="順位の終了"
          value={options.rankMax}
          min={1}
          onChange={(value) => updateOption("rankMax", value)}
        />
        <EstimateNumberField
          label="狙い度の下限"
          value={options.scoreMin}
          min={0}
          max={100}
          step={0.1}
          suffix="以上"
          onChange={(value) => updateOption("scoreMin", value)}
        />
        <EstimateNumberField
          label="偏差値の下限"
          value={options.deviationMin}
          min={0}
          step={0.1}
          onChange={(value) => updateOption("deviationMin", value)}
        />
      </div>

      <div className="backtestBlock">
        <p className="filterControlLabel">順位の見方</p>
        <div className="metricToggleRow">
          <label
            className={`metricToggleChip ${
              options.rankScope === "selected" ? "metricToggleChipActive" : ""
            }`}
          >
            <input
              type="radio"
              name="machineHuntScoreRankScope"
              value="selected"
              checked={options.rankScope === "selected"}
              onChange={() => updateOption("rankScope", "selected")}
            />
            <span>チェック機種内順位</span>
          </label>
          <label
            className={`metricToggleChip ${
              options.rankScope === "machine" ? "metricToggleChipActive" : ""
            }`}
          >
            <input
              type="radio"
              name="machineHuntScoreRankScope"
              value="machine"
              checked={options.rankScope === "machine"}
              onChange={() => updateOption("rankScope", "machine")}
            />
            <span>機種内順位</span>
          </label>
          <label
            className={`metricToggleChip ${
              options.rankScope === "all" ? "metricToggleChipActive" : ""
            }`}
          >
            <input
              type="radio"
              name="machineHuntScoreRankScope"
              value="all"
              checked={options.rankScope === "all"}
              onChange={() => updateOption("rankScope", "all")}
            />
            <span>全機種順位</span>
          </label>
        </div>
      </div>

      <div className="backtestBlock">
        <p className="filterControlLabel">偏差値の比較対象</p>
        <div className="metricToggleRow">
          <label
            className={`metricToggleChip ${
              options.deviationScope === "selected" ? "metricToggleChipActive" : ""
            }`}
          >
            <input
              type="radio"
              name="machineHuntScoreDeviationScope"
              value="selected"
              checked={options.deviationScope === "selected"}
              onChange={() => updateOption("deviationScope", "selected")}
            />
            <span>チェック機種内</span>
          </label>
          <label
            className={`metricToggleChip ${
              options.deviationScope === "machine" ? "metricToggleChipActive" : ""
            }`}
          >
            <input
              type="radio"
              name="machineHuntScoreDeviationScope"
              value="machine"
              checked={options.deviationScope === "machine"}
              onChange={() => updateOption("deviationScope", "machine")}
            />
            <span>機種内</span>
          </label>
          <label
            className={`metricToggleChip ${
              options.deviationScope === "all" ? "metricToggleChipActive" : ""
            }`}
          >
            <input
              type="radio"
              name="machineHuntScoreDeviationScope"
              value="all"
              checked={options.deviationScope === "all"}
              onChange={() => updateOption("deviationScope", "all")}
            />
            <span>全機種内</span>
          </label>
        </div>
      </div>

      <div className="backtestBlock">
        <p className="filterControlLabel">順位、狙い度、偏差値を複数入れた時の条件</p>
        <div className="metricToggleRow">
          <label
            className={`metricToggleChip ${
              options.matchMode === "or" ? "metricToggleChipActive" : ""
            }`}
          >
            <input
              type="radio"
              name="machineHuntScoreMatchMode"
              value="or"
              checked={options.matchMode === "or"}
              onChange={() => updateOption("matchMode", "or")}
            />
            <span>どれか一致</span>
          </label>
          <label
            className={`metricToggleChip ${
              options.matchMode === "and" ? "metricToggleChipActive" : ""
            }`}
          >
            <input
              type="radio"
              name="machineHuntScoreMatchMode"
              value="and"
              checked={options.matchMode === "and"}
              onChange={() => updateOption("matchMode", "and")}
            />
            <span>すべて一致</span>
          </label>
        </div>
      </div>

      {availableMachineNames.length > 0 ? (
        <div className="backtestBlock">
          <p className="filterControlLabel">順位と偏差値に使う機種</p>
          <div className="metricToggleRow">
            {availableMachineNames.map((machineName) => {
              const checked = selectedMachineNameSet.has(machineName);
              return (
                <label
                  key={machineName}
                  className={`metricToggleChip ${checked ? "metricToggleChipActive" : ""}`}
                >
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={() => toggleMachine(machineName)}
                  />
                  <span>{machineName}</span>
                </label>
              );
            })}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function SettingEstimateControls({
  options,
  onChange,
  hasHuntScore,
  huntScoreHighlightOptions,
  huntScoreHighlightAvailableMachineNames,
  onHuntScoreHighlightOptionsChange,
}) {
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
        <div className="estimateFields estimateFieldsStacked">
          <EstimateNumberField
            label="重み"
            value={options.dataWeight}
            min={0}
            max={100}
            suffix="%"
            onChange={(value) => updateOption("dataWeight", value)}
          />
        </div>
        {hasHuntScore ? (
          <HuntScoreHighlightControls
            options={huntScoreHighlightOptions}
            availableMachineNames={huntScoreHighlightAvailableMachineNames}
            onChange={onHuntScoreHighlightOptionsChange}
          />
        ) : null}
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
  huntScoreHighlightKeySet = new Set(),
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
          const isHuntScoreMetric =
            metric.key === "hunt_score" || metric.key === "hunt_score_deviation";
          const huntScoreHighlightClass =
            isHuntScoreMetric &&
            huntScoreHighlightKeySet.has(
              buildHuntScoreHighlightKey(row.date, record?.machine_name, slotNumber),
            )
              ? "huntScoreHighlighted"
              : "";
          const className = [
            toneClass,
            isHuntScoreMetric ? "" : settingHighlightClass,
            huntScoreHighlightClass,
            boundaryClass,
          ]
            .filter(Boolean)
            .join(" ");
          const title = settingTitle || (metric.title ? metric.title(value, record) : "");
          return (
            <td
              key={`${row.date}-${slotNumber}-${metric.key}`}
              className={className || undefined}
              title={title || undefined}
            >
              {metric.render(value, record, { row, slotNumber })}
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
  huntScoreHighlight,
}) {
  const latestAvailableDate = dateRows[0]?.date ?? "";
  const oldestAvailableDate = dateRows.at(-1)?.date ?? latestAvailableDate;
  const initialRangeStartDate = latestAvailableDate
    ? clampDateText(
        shiftDateText(latestAvailableDate, -(DEFAULT_COMPARISON_RECENT_DAYS - 1)),
        oldestAvailableDate,
        latestAvailableDate,
        oldestAvailableDate,
      )
    : "";
  const [periodMode, setPeriodMode] = useState("recent");
  const [recentDaysInput, setRecentDaysInput] = useState(String(DEFAULT_COMPARISON_RECENT_DAYS));
  const [rangeStartInput, setRangeStartInput] = useState(initialRangeStartDate);
  const [rangeEndInput, setRangeEndInput] = useState(latestAvailableDate);
  const [eventFilters, setEventFilters] = useState(() =>
    createEventFilters(
      initialEventFilters?.dayTails ?? [],
      initialEventFilters?.zoro ?? false,
      initialEventFilters?.weekdays ?? [],
    ),
  );
  const [eventDisplayMode, setEventDisplayMode] = useState(initialEventDisplayMode);
  const [differenceMode, setDifferenceMode] = useState(DEFAULT_DIFFERENCE_MODE);
  const [visibleMetricKeys, setVisibleMetricKeys] = useState(DEFAULT_VISIBLE_METRIC_KEYS);
  const [estimateOptions, setEstimateOptions] = useState(() =>
    createDefaultEstimateOptions(slotNumbers.length, machineName),
  );
  const huntScoreHighlightAvailableMachineNames = useMemo(
    () => normalizeAvailableHuntScoreMachineNames(huntScoreHighlight?.availableMachineNames),
    [huntScoreHighlight],
  );
  const [huntScoreHighlightOptions, setHuntScoreHighlightOptions] = useState(() =>
    createDefaultHuntScoreHighlightOptions(huntScoreHighlightAvailableMachineNames),
  );
  const [huntScoreHighlightOptionsLoadedStoreId, setHuntScoreHighlightOptionsLoadedStoreId] =
    useState("");
  const [, startTransition] = useTransition();
  const recentDays = useMemo(() => normalizeRecentDaysInput(recentDaysInput), [recentDaysInput]);
  const huntScoreDeviationValueMap = useMemo(
    () => buildHuntScoreDeviationValueMap(huntScoreHighlight, huntScoreHighlightOptions),
    [huntScoreHighlight, huntScoreHighlightOptions],
  );
  const huntScoreHighlightKeySet = useMemo(
    () =>
      buildHuntScoreHighlightKeySet(
        huntScoreHighlight,
        huntScoreHighlightOptions,
        huntScoreDeviationValueMap,
      ),
    [huntScoreDeviationValueMap, huntScoreHighlight, huntScoreHighlightOptions],
  );
  const activeDateRange = useMemo(() => {
    if (!latestAvailableDate) {
      return {
        startDate: "",
        endDate: "",
      };
    }

    if (periodMode === "range") {
      const startDate = clampDateText(
        rangeStartInput,
        oldestAvailableDate,
        latestAvailableDate,
        oldestAvailableDate,
      );
      const endDate = clampDateText(
        rangeEndInput,
        oldestAvailableDate,
        latestAvailableDate,
        latestAvailableDate,
      );

      return startDate <= endDate
        ? { startDate, endDate }
        : { startDate: endDate, endDate: startDate };
    }

    return {
      startDate: shiftDateText(latestAvailableDate, -(recentDays - 1)),
      endDate: latestAvailableDate,
    };
  }, [
    latestAvailableDate,
    oldestAvailableDate,
    periodMode,
    rangeEndInput,
    rangeStartInput,
    recentDays,
  ]);
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
  const getHuntScoreDeviationValue = useCallback(
    (record, context) =>
      huntScoreDeviationValueMap.get(
        buildHuntScoreHighlightKey(
          context?.row?.date ?? record?.target_date,
          record?.machine_name,
          context?.slotNumber ?? record?.slot_number,
        ),
      ) ?? null,
    [huntScoreDeviationValueMap],
  );
  const hasHuntScore = useMemo(
    () =>
      dateRows.some((row) =>
        slotNumbers.some((slotNumber) =>
          Number.isFinite(Number(row.recordsBySlot?.[slotNumber]?.hunt_score)),
        ),
      ),
    [dateRows, slotNumbers],
  );
  const metrics = useMemo(
    () =>
      getMetrics(
        settingEstimateDefinition,
        getCompositeSettingEstimate,
        hasHuntScore,
        differenceMode,
        getHuntScoreDeviationValue,
      ),
    [
      differenceMode,
      getCompositeSettingEstimate,
      getHuntScoreDeviationValue,
      hasHuntScore,
      settingEstimateDefinition,
    ],
  );

  useEffect(() => {
    setHuntScoreHighlightOptionsLoadedStoreId("");
    setHuntScoreHighlightOptions(
      readHuntScoreHighlightOptions(storeId, huntScoreHighlightAvailableMachineNames),
    );
    setHuntScoreHighlightOptionsLoadedStoreId(storeId);
  }, [huntScoreHighlightAvailableMachineNames, storeId]);

  useEffect(() => {
    if (huntScoreHighlightOptionsLoadedStoreId !== storeId) {
      return;
    }
    saveHuntScoreHighlightOptions(storeId, huntScoreHighlightOptions);
  }, [huntScoreHighlightOptions, huntScoreHighlightOptionsLoadedStoreId, storeId]);

  const visibleMetrics = useMemo(
    () => metrics.filter((metric) => visibleMetricKeys.includes(metric.key)),
    [metrics, visibleMetricKeys],
  );

  const periodFilteredRows = useMemo(() => {
    if (!activeDateRange.startDate || !activeDateRange.endDate) {
      return dateRows;
    }

    return dateRows.filter((row) => {
      const rowDate = String(row.date ?? "");
      return rowDate >= activeDateRange.startDate && rowDate <= activeDateRange.endDate;
    });
  }, [activeDateRange.endDate, activeDateRange.startDate, dateRows]);

  const visibleRows = useMemo(() => {
    if (eventDisplayMode === "highlight") {
      return periodFilteredRows;
    }
    return periodFilteredRows.filter((row) => matchesEventFilters(row.date, eventFilters));
  }, [eventDisplayMode, eventFilters, periodFilteredRows]);

  const specialDateSet = useMemo(() => {
    if (!eventFilters.isActive) {
      return new Set();
    }

    return new Set(
      periodFilteredRows
        .filter((row) => matchesEventFilters(row.date, eventFilters))
        .map((row) => row.date),
    );
  }, [eventFilters, periodFilteredRows]);

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

  const updatePeriodMode = (mode) => {
    startTransition(() => {
      if (mode === "range" && periodMode !== "range") {
        setRangeStartInput(
          clampDateText(
            activeDateRange.startDate,
            oldestAvailableDate,
            latestAvailableDate,
            oldestAvailableDate,
          ),
        );
        setRangeEndInput(
          clampDateText(
            activeDateRange.endDate,
            oldestAvailableDate,
            latestAvailableDate,
            latestAvailableDate,
          ),
        );
      }
      setPeriodMode(mode);
    });
  };

  const handleRangeStartChange = (value) => {
    const nextStart = clampDateText(
      value,
      oldestAvailableDate,
      latestAvailableDate,
      oldestAvailableDate,
    );
    setRangeStartInput(nextStart);
    if (rangeEndInput && nextStart > rangeEndInput) {
      setRangeEndInput(nextStart);
    }
  };

  const handleRangeEndChange = (value) => {
    const nextEnd = clampDateText(
      value,
      oldestAvailableDate,
      latestAvailableDate,
      latestAvailableDate,
    );
    setRangeEndInput(nextEnd);
    if (rangeStartInput && nextEnd < rangeStartInput) {
      setRangeStartInput(nextEnd);
    }
  };

  const saveEventFilters = useCallback(
    (nextFilters) => {
      return nextFilters;
    },
    [],
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

  return (
    <>
      <section className="filterPanel">
        <div>
          <p className="sectionLabel">表示条件</p>
        </div>
        <div className="filterControlGroup">
          <p className="filterControlLabel">表示期間</p>
          <div className="dayFilterRow">
            <button
              type="button"
              onClick={() => updatePeriodMode("recent")}
              className={`dayFilterChip ${periodMode === "recent" ? "dayFilterChipActive" : ""}`}
              aria-pressed={periodMode === "recent"}
            >
              直近
            </button>
            <button
              type="button"
              onClick={() => updatePeriodMode("range")}
              className={`dayFilterChip ${periodMode === "range" ? "dayFilterChipActive" : ""}`}
              aria-pressed={periodMode === "range"}
            >
              期間指定
            </button>
          </div>
          <div className="periodInputGrid">
            {periodMode === "recent" ? (
              <label className="estimateField">
                <span>直近日数</span>
                <span className="estimateInputWrap">
                  <input
                    type="number"
                    min="1"
                    step="1"
                    inputMode="numeric"
                    value={recentDaysInput}
                    onChange={(event) => setRecentDaysInput(event.target.value)}
                    onBlur={() => setRecentDaysInput(String(recentDays))}
                  />
                  <span className="estimateInputSuffix">日</span>
                </span>
              </label>
            ) : (
              <>
                <label className="estimateField">
                  <span>開始日</span>
                  <span className="estimateInputWrap">
                    <input
                      type="date"
                      min={oldestAvailableDate}
                      max={latestAvailableDate}
                      value={rangeStartInput}
                      onChange={(event) => handleRangeStartChange(event.target.value)}
                    />
                  </span>
                </label>
                <label className="estimateField">
                  <span>終了日</span>
                  <span className="estimateInputWrap">
                    <input
                      type="date"
                      min={oldestAvailableDate}
                      max={latestAvailableDate}
                      value={rangeEndInput}
                      onChange={(event) => handleRangeEndChange(event.target.value)}
                    />
                  </span>
                </label>
              </>
            )}
          </div>
          <p className="filterPanelStatus">
            {buildDisplayedPeriodLabel(activeDateRange.startDate, activeDateRange.endDate)} /{" "}
            {periodFilteredRows.length}日分
          </p>
        </div>
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
          <p className="filterControlLabel">差枚の基準</p>
          <div className="metricToggleRow">
            <label
              className={`metricToggleChip ${
                differenceMode === "bonus" ? "metricToggleChipActive" : ""
              }`}
            >
              <input
                type="radio"
                name="machineDifferenceMode"
                value="bonus"
                checked={differenceMode === "bonus"}
                onChange={() => setDifferenceMode("bonus")}
              />
              <span>ボーナス数基準</span>
            </label>
            <label
              className={`metricToggleChip ${
                differenceMode === "minrepo" ? "metricToggleChipActive" : ""
              }`}
            >
              <input
                type="radio"
                name="machineDifferenceMode"
                value="minrepo"
                checked={differenceMode === "minrepo"}
                onChange={() => setDifferenceMode("minrepo")}
              />
              <span>みんレポ基準</span>
            </label>
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
              hasHuntScore={hasHuntScore}
              huntScoreHighlightOptions={huntScoreHighlightOptions}
              huntScoreHighlightAvailableMachineNames={huntScoreHighlightAvailableMachineNames}
              onHuntScoreHighlightOptionsChange={setHuntScoreHighlightOptions}
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
        huntScoreHighlightKeySet={huntScoreHighlightKeySet}
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
  huntScoreHighlightKeySet,
  csvRows,
  tableStyle,
}) {
  if (dateRows.length === 0) {
    return (
      <section className="statusPanel">
        <h2>条件に合う日付がありません</h2>
        <p>表示期間か特定日条件を見直してください。</p>
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
                huntScoreHighlightKeySet={huntScoreHighlightKeySet}
              />
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
