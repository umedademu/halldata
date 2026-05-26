"use client";

import { memo, useCallback, useEffect, useMemo, useRef, useState, useTransition } from "react";

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
  formatSite7FetchedDateTime,
  formatSite7FetchedTime,
  formatSignedNumber,
  formatWeekday,
  valueToneClass,
} from "../lib/format";
import { createEventFilters, matchesEventFilters } from "../lib/event-filters";
import {
  buildConditionRequirementOptions,
  buildNextGapFilter,
  buildScoreFilter,
  calculateHuntScoreNextGapMap,
  matchesRequiredConditionFilters,
} from "../lib/hunt-bookmark";
import {
  AIM_JUGGLER_MACHINE_NAMES,
  HANABI_MACHINE_NAMES,
  groupHuntMachineOptions,
  hasAimJugglerHuntMachineGroupOption,
  hasHanabiHuntMachineGroupOption,
  resolveHuntMachineGroupName,
} from "../lib/hunt-machine-display";
import {
  DEFAULT_DIFFERENCE_MODE,
  normalizeDifferenceMode,
  selectDifferenceValue,
} from "../lib/machine-difference";
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
const MONTH_DAY_OPTIONS = Array.from({ length: 31 }, (_, index) => index + 1);
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
  "estimated_grape_denominator",
  "setting_estimate",
  "hunt_score",
  "hunt_score_next_gap",
];
const PREVIOUS_DEFAULT_VISIBLE_METRIC_KEYS = [
  "difference_value",
  "games_count",
  "bb_count",
  "rb_count",
  "combined_ratio_text",
  "setting_estimate",
  "hunt_score",
  "hunt_score_next_gap",
];
const ESTIMATED_GRAPE_METRIC_KEY = "estimated_grape_denominator";
const MACHINE_COMPARISON_METRIC_DEFAULTS_VERSION = 3;
const MATRIX_DATE_COLUMN_WIDTH_REM = 4.8;
const MATRIX_WEEKDAY_COLUMN_WIDTH_REM = 2.4;
const MATRIX_SLOT_WIDTH_REM = 16;
const DEFAULT_GAME_MIN_GAMES = 6000;
const DEFAULT_GAME_MAX_GAMES = 9000;
const DEFAULT_GAME_EXPONENT = 1.5;
const DEFAULT_COMPARISON_RECENT_DAYS = 90;
const DEFAULT_HUNT_SCORE_HIGHLIGHT_THRESHOLD = "";
const DEFAULT_HUNT_SCORE_NEXT_GAP_MIN = "";
const DEFAULT_HUNT_SCORE_RANK_MIN = 1;
const DEFAULT_HUNT_SCORE_RANK_MAX = 1;
const DEFAULT_HUNT_SCORE_RANK_SCOPE = "machine";
const DEFAULT_HUNT_SCORE_NEXT_GAP_SCOPE = "machine";
const DEFAULT_HUNT_RANK_REQUIRED = true;
const DEFAULT_HUNT_SCORE_REQUIRED = false;
const DEFAULT_HUNT_NEXT_GAP_REQUIRED = false;
const HUNT_SCORE_HIGHLIGHT_STORAGE_PREFIX = "machine-hunt-score-highlight:v2:";
const MACHINE_COMPARISON_STORAGE_PREFIX = "machine-comparison-options:";
const MACHINE_COMPARISON_PERIOD_STORAGE_KEY = "machine-comparison-period-options";
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

function normalizeRecentDaysInputText(value, fallbackValue = DEFAULT_COMPARISON_RECENT_DAYS) {
  if (value === undefined || value === null) {
    return String(normalizeRecentDaysInput(fallbackValue));
  }
  const text = String(value ?? "").trim();
  if (text === "") {
    return "";
  }
  return String(normalizeRecentDaysInput(text));
}

function normalizePeriodMode(value) {
  return value === "range" ? "range" : "recent";
}

function normalizeDateInputValue(value, minDate, maxDate, fallbackDate) {
  return clampDateText(String(value ?? "").trim(), minDate, maxDate, fallbackDate);
}

function normalizeSavedDateInputValue(value, fallbackDate) {
  const text = String(value ?? "").trim();
  return isIsoDateText(text) ? text : fallbackDate;
}

function normalizeMetricKeys(value, allowedMetricKeys = null, fallbackKeys = DEFAULT_VISIBLE_METRIC_KEYS) {
  const allowedMetricKeySet = allowedMetricKeys ? new Set(allowedMetricKeys) : null;
  const keys = [
    ...new Set(
      (Array.isArray(value) ? value : [])
        .map((key) => String(key ?? "").trim())
        .filter((key) => key && (!allowedMetricKeySet || allowedMetricKeySet.has(key))),
    ),
  ];
  if (keys.length > 0) {
    return keys;
  }
  return fallbackKeys.filter((key) => !allowedMetricKeySet || allowedMetricKeySet.has(key));
}

function metricKeysMatch(leftKeys, rightKeys) {
  if (!Array.isArray(leftKeys) || !Array.isArray(rightKeys) || leftKeys.length !== rightKeys.length) {
    return false;
  }
  return leftKeys.every((key, index) => key === rightKeys[index]);
}

function normalizeStoredNumberInput(value, fallbackValue) {
  if (String(value ?? "").trim() === "") {
    return "";
  }
  const number = Number(value);
  return Number.isFinite(number) ? number : fallbackValue;
}

function isStoredNumberValue(value, expectedValue) {
  return normalizeStoredNumberInput(value, null) === expectedValue;
}

function isStoredEnabledValue(value, expectedValue) {
  if (value === undefined || value === null) {
    return false;
  }
  return normalizeEnabledOption(value, !expectedValue) === expectedValue;
}

function isLegacyDefaultEstimateOptions(source) {
  if (!source || typeof source !== "object") {
    return false;
  }

  const hasDefaultGameRange =
    isStoredNumberValue(source.minGames, DEFAULT_GAME_MIN_GAMES) &&
    isStoredNumberValue(source.maxGames, DEFAULT_GAME_MAX_GAMES) &&
    isStoredNumberValue(source.gameExponent, DEFAULT_GAME_EXPONENT);
  if (!hasDefaultGameRange) {
    return false;
  }

  const isSmallMachineDefault =
    isStoredNumberValue(source.dataWeight, 20) &&
    isStoredEnabledValue(source.gameEnabled, true) &&
    isStoredNumberValue(source.gameWeight, 40) &&
    isStoredEnabledValue(source.comparisonEnabled, true) &&
    isStoredNumberValue(source.comparisonWeight, 40);
  const isStandardMachineDefault =
    isStoredNumberValue(source.dataWeight, 80) &&
    isStoredEnabledValue(source.gameEnabled, true) &&
    isStoredNumberValue(source.gameWeight, 20) &&
    isStoredEnabledValue(source.comparisonEnabled, false) &&
    isStoredNumberValue(source.comparisonWeight, 0);
  return isSmallMachineDefault || isStandardMachineDefault;
}

function normalizeEstimateOptions(value, defaults) {
  const source = value && typeof value === "object" ? value : {};
  if (isLegacyDefaultEstimateOptions(source)) {
    return defaults;
  }
  return {
    dataWeight: normalizeStoredNumberInput(source.dataWeight, defaults.dataWeight),
    gameEnabled: Boolean(source.gameEnabled ?? defaults.gameEnabled),
    gameWeight: normalizeStoredNumberInput(source.gameWeight, defaults.gameWeight),
    comparisonEnabled: Boolean(source.comparisonEnabled ?? defaults.comparisonEnabled),
    comparisonWeight: normalizeStoredNumberInput(source.comparisonWeight, defaults.comparisonWeight),
    minGames: normalizeStoredNumberInput(source.minGames, defaults.minGames),
    maxGames: normalizeStoredNumberInput(source.maxGames, defaults.maxGames),
    gameExponent: normalizeStoredNumberInput(source.gameExponent, defaults.gameExponent),
  };
}

function normalizeStoredEventFilters(value, fallbackFilters) {
  if (!value || typeof value !== "object") {
    return fallbackFilters;
  }
  return createEventFilters(value.dayTails, Boolean(value.zoro), value.weekdays, value.monthDays);
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

function normalizeEnabledOption(value, fallbackValue) {
  if (value === undefined || value === null) {
    return fallbackValue;
  }
  return value === true || value === 1 || value === "1" || value === "true" || value === "on";
}

function buildHuntScoreHighlightKey(date, machineName, slotNumber) {
  return `${String(date ?? "").trim()}\t${String(machineName ?? "").trim()}\t${String(
    slotNumber ?? "",
  ).trim()}`;
}

function normalizeMachineNameText(value) {
  return String(value ?? "").replace(/\s+/gu, "").trim();
}

function chooseDefaultHuntScoreMachineName(machineNames, currentMachineName = "") {
  const availableMachineNames = normalizeAvailableHuntScoreMachineNames(machineNames);
  const normalizedCurrentMachineName = normalizeMachineNameText(currentMachineName);
  return (
    availableMachineNames.find(
      (machineName) => normalizeMachineNameText(machineName) === normalizedCurrentMachineName,
    ) ??
    availableMachineNames[0] ??
    ""
  );
}

function createDefaultHuntScoreHighlightOptions(machineNames, currentMachineName = "") {
  const availableMachineNames = normalizeAvailableHuntScoreMachineNames(machineNames);
  const defaultMachineName = chooseDefaultHuntScoreMachineName(
    availableMachineNames,
    currentMachineName,
  );
  return {
    rankMin: DEFAULT_HUNT_SCORE_RANK_MIN,
    rankMax: DEFAULT_HUNT_SCORE_RANK_MAX,
    scoreMin: DEFAULT_HUNT_SCORE_HIGHLIGHT_THRESHOLD,
    nextGapMin: DEFAULT_HUNT_SCORE_NEXT_GAP_MIN,
    rankRequired: DEFAULT_HUNT_RANK_REQUIRED,
    scoreRequired: DEFAULT_HUNT_SCORE_REQUIRED,
    nextGapRequired: DEFAULT_HUNT_NEXT_GAP_REQUIRED,
    rankScope: DEFAULT_HUNT_SCORE_RANK_SCOPE,
    nextGapScope: DEFAULT_HUNT_SCORE_NEXT_GAP_SCOPE,
    selectedMachineNames: defaultMachineName ? [defaultMachineName] : [],
    combineAimJuggler: false,
    combineHanabi: false,
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

function selectedIncludesAllHuntMachineGroupMembers(selectedMachineNames, availableMachineNames, groupMachineNames) {
  const selectedMachineNameSet = new Set(selectedMachineNames);
  const availableMachineNameSet = new Set(availableMachineNames);
  const availableGroupMachineNames = groupMachineNames.filter((machineName) =>
    availableMachineNameSet.has(machineName),
  );
  return (
    availableGroupMachineNames.length > 0 &&
    availableGroupMachineNames.every((machineName) => selectedMachineNameSet.has(machineName))
  );
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

function normalizeHuntScoreNextGapInputValue(value, fallbackValue) {
  if (String(value ?? "").trim() === "") {
    return "";
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallbackValue;
}

function normalizeHuntScoreHighlightOptions(value, availableMachineNames, currentMachineName = "") {
  const defaults = createDefaultHuntScoreHighlightOptions(availableMachineNames, currentMachineName);
  if (!value || typeof value !== "object") {
    return defaults;
  }
  const rankScope = normalizeHuntScoreRankScope(value.rankScope);
  const nextGapScope = normalizeHuntScoreRankScope(value.nextGapScope ?? defaults.nextGapScope);
  const requirementOptions = buildConditionRequirementOptions(value, {
    rankRequired: defaults.rankRequired,
    scoreRequired: defaults.scoreRequired,
    nextGapRequired: defaults.nextGapRequired,
  });

  const selectedMachineNames = normalizeSelectedHuntScoreMachineNames(
    value.selectedMachineNames,
    availableMachineNames,
  );

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
    nextGapMin: Object.hasOwn(value, "nextGapMin")
      ? normalizeHuntScoreNextGapInputValue(value.nextGapMin, defaults.nextGapMin)
      : defaults.nextGapMin,
    rankRequired: requirementOptions.rankRequired,
    scoreRequired: requirementOptions.scoreRequired,
    nextGapRequired: requirementOptions.nextGapRequired,
    rankScope,
    nextGapScope,
    selectedMachineNames,
    combineAimJuggler:
      hasAimJugglerHuntMachineGroupOption(availableMachineNames) &&
      selectedIncludesAllHuntMachineGroupMembers(
        selectedMachineNames,
        availableMachineNames,
        AIM_JUGGLER_MACHINE_NAMES,
      ) &&
      normalizeEnabledOption(value.combineAimJuggler, defaults.combineAimJuggler),
    combineHanabi:
      hasHanabiHuntMachineGroupOption(availableMachineNames) &&
      selectedIncludesAllHuntMachineGroupMembers(
        selectedMachineNames,
        availableMachineNames,
        HANABI_MACHINE_NAMES,
      ) &&
      normalizeEnabledOption(value.combineHanabi, defaults.combineHanabi),
  };
}

function readHuntScoreHighlightOptions(storeId, availableMachineNames, currentMachineName = "") {
  const defaults = createDefaultHuntScoreHighlightOptions(availableMachineNames, currentMachineName);
  if (typeof window === "undefined") {
    return defaults;
  }

  try {
    const storageKey = `${HUNT_SCORE_HIGHLIGHT_STORAGE_PREFIX}${storeId}`;
    const storedValue = window.localStorage.getItem(storageKey);
    return storedValue
      ? normalizeHuntScoreHighlightOptions(JSON.parse(storedValue), availableMachineNames, currentMachineName)
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

function normalizeHuntScoreOptionSignature(options) {
  return JSON.stringify({
    rankMin: options.rankMin,
    rankMax: options.rankMax,
    scoreMin: options.scoreMin,
    nextGapMin: options.nextGapMin,
    rankRequired: Boolean(options.rankRequired),
    scoreRequired: Boolean(options.scoreRequired),
    nextGapRequired: Boolean(options.nextGapRequired),
    rankScope: normalizeHuntScoreRankScope(options.rankScope),
    nextGapScope: normalizeHuntScoreRankScope(options.nextGapScope),
    selectedMachineNames: [...new Set(options.selectedMachineNames ?? [])].sort(),
    combineAimJuggler: Boolean(options.combineAimJuggler),
    combineHanabi: Boolean(options.combineHanabi),
  });
}

function huntScoreHighlightOptionsEqual(left, right) {
  return normalizeHuntScoreOptionSignature(left) === normalizeHuntScoreOptionSignature(right);
}

function huntScoreHighlightNeedsFullData(options, currentMachineName) {
  if (!options) {
    return false;
  }
  const rankScope = normalizeHuntScoreRankScope(options.rankScope);
  const nextGapScope = normalizeHuntScoreRankScope(options.nextGapScope);
  if (rankScope === "all" || nextGapScope === "all") {
    return true;
  }
  const normalizedCurrentMachineName = normalizeMachineNameText(currentMachineName);
  const hasSelectedOtherMachine = (options.selectedMachineNames ?? []).some(
    (machineName) => normalizeMachineNameText(machineName) !== normalizedCurrentMachineName,
  );
  return (
    (rankScope === "selected" ||
      nextGapScope === "selected") &&
    hasSelectedOtherMachine
  );
}

function readLocalStorageJson(storageKey) {
  if (typeof window === "undefined") {
    return null;
  }

  try {
    const storedValue = window.localStorage.getItem(storageKey);
    if (!storedValue) {
      return null;
    }

    const parsedValue = JSON.parse(storedValue);
    return parsedValue && typeof parsedValue === "object" ? parsedValue : null;
  } catch {
    return null;
  }
}

function normalizeMachineComparisonPeriodOptions(value, defaults) {
  const source = value && typeof value === "object" ? value : {};
  return {
    periodMode: normalizePeriodMode(source.periodMode ?? defaults.periodMode),
    recentDaysInput: normalizeRecentDaysInputText(
      source.recentDaysInput ?? source.recentDays ?? defaults.recentDaysInput,
    ),
    rangeStartInput: normalizeSavedDateInputValue(
      source.rangeStartInput ?? source.startDate ?? defaults.rangeStartInput,
      defaults.rangeStartInput,
    ),
    rangeEndInput: normalizeSavedDateInputValue(
      source.rangeEndInput ?? source.endDate ?? defaults.rangeEndInput,
      defaults.rangeEndInput,
    ),
  };
}

function readMachineComparisonPeriodOptions(defaults) {
  const parsedValue = readLocalStorageJson(MACHINE_COMPARISON_PERIOD_STORAGE_KEY);
  return parsedValue ? normalizeMachineComparisonPeriodOptions(parsedValue, defaults) : null;
}

function normalizeMachineComparisonMetricKeys(source, defaults) {
  const metricKeys = normalizeMetricKeys(source.visibleMetricKeys, null, defaults.visibleMetricKeys);
  const metricDefaultsVersion = Number(source.metricDefaultsVersion ?? 0);
  if (
    metricDefaultsVersion < MACHINE_COMPARISON_METRIC_DEFAULTS_VERSION &&
    metricKeysMatch(metricKeys, PREVIOUS_DEFAULT_VISIBLE_METRIC_KEYS) &&
    defaults.visibleMetricKeys.includes(ESTIMATED_GRAPE_METRIC_KEY)
  ) {
    return defaults.visibleMetricKeys;
  }
  return metricKeys;
}

function saveMachineComparisonPeriodOptions(options) {
  if (typeof window === "undefined") {
    return;
  }

  try {
    window.localStorage.setItem(
      MACHINE_COMPARISON_PERIOD_STORAGE_KEY,
      JSON.stringify({
        version: 1,
        periodMode: normalizePeriodMode(options.periodMode),
        recentDaysInput: String(options.recentDaysInput ?? ""),
        rangeStartInput: String(options.rangeStartInput ?? ""),
        rangeEndInput: String(options.rangeEndInput ?? ""),
      }),
    );
  } catch {
    // 保存できない環境では、画面上の変更だけを有効にします。
  }
}

function readMachineComparisonOptions(storeId, defaults, options = {}) {
  if (typeof window === "undefined") {
    return defaults;
  }

  const storageKey = `${MACHINE_COMPARISON_STORAGE_PREFIX}${storeId}`;
  const parsedValue = readLocalStorageJson(storageKey);
  const source = parsedValue ?? {};
  const periodOptions =
    readMachineComparisonPeriodOptions(defaults) ??
    normalizeMachineComparisonPeriodOptions(source, defaults);

  return {
    ...periodOptions,
    eventFilters: options.preferInitialEventFilters
      ? defaults.eventFilters
      : normalizeStoredEventFilters(source.eventFilters, defaults.eventFilters),
    differenceMode: normalizeDifferenceMode(source.differenceMode),
    visibleMetricKeys: normalizeMachineComparisonMetricKeys(source, defaults),
    estimateOptions: options.preferDefaultEstimateOptions
      ? defaults.estimateOptions
      : normalizeEstimateOptions(source.estimateOptions, defaults.estimateOptions),
    displayControlsOpen: normalizeEnabledOption(source.displayControlsOpen, defaults.displayControlsOpen),
    settingControlsOpen: normalizeEnabledOption(source.settingControlsOpen, defaults.settingControlsOpen),
    huntScoreControlsOpen: normalizeEnabledOption(source.huntScoreControlsOpen, defaults.huntScoreControlsOpen),
  };
}

function saveMachineComparisonOptions(storeId, options) {
  if (typeof window === "undefined") {
    return;
  }

  try {
    saveMachineComparisonPeriodOptions(options);
    const storageKey = `${MACHINE_COMPARISON_STORAGE_PREFIX}${storeId}`;
    const existingValue = options.preserveEstimateOptions
      ? readLocalStorageJson(storageKey)
      : null;
    window.localStorage.setItem(
      storageKey,
      JSON.stringify({
        version: 1,
        metricDefaultsVersion: MACHINE_COMPARISON_METRIC_DEFAULTS_VERSION,
        eventFilters: {
          dayTails: options.eventFilters?.dayTails ?? [],
          zoro: Boolean(options.eventFilters?.zoro),
          weekdays: options.eventFilters?.weekdays ?? [],
          monthDays: options.eventFilters?.monthDays ?? [],
        },
        differenceMode: normalizeDifferenceMode(options.differenceMode),
        visibleMetricKeys: normalizeMetricKeys(options.visibleMetricKeys),
        estimateOptions: existingValue?.estimateOptions ?? options.estimateOptions,
        displayControlsOpen: Boolean(options.displayControlsOpen),
        settingControlsOpen: Boolean(options.settingControlsOpen),
        huntScoreControlsOpen: Boolean(options.huntScoreControlsOpen),
      }),
    );
  } catch {
    // 保存できない環境では、画面上の変更だけを有効にします。
  }
}

function buildHuntScoreNextGapValueMap(highlightDetail, options, useRankFilter = false) {
  const valueMap = new Map();
  const snapshots = Array.isArray(highlightDetail?.snapshots) ? highlightDetail.snapshots : [];
  const normalizedOptions = normalizeHuntScoreHighlightOptions(
    options,
    normalizeAvailableHuntScoreMachineNames(highlightDetail?.availableMachineNames),
  );
  const selectedMachineNameSet = new Set(normalizedOptions.selectedMachineNames);
  const nextGapScope = normalizeHuntScoreRankScope(normalizedOptions.nextGapScope);
  const rankFilter = useRankFilter
    ? buildHuntScoreRankFilter(
        normalizedOptions.rankMin,
        normalizedOptions.rankMax,
      )
    : null;

  for (const snapshot of snapshots) {
    const rows = Array.isArray(snapshot.rows) ? snapshot.rows : [];
    const overallNextGapMap = calculateHuntScoreNextGapMap(rows, rankFilter);
    const selectedRows = rows.filter((row) =>
      selectedMachineNameSet.has(String(row.machineName ?? "").trim()),
    );
    const selectedNextGapMap = calculateHuntScoreNextGapMap(selectedRows, rankFilter);
    const rowsByMachineName = new Map();

    for (const row of rows) {
      const machineName = resolveHuntMachineGroupName(row.machineName, normalizedOptions);
      if (!machineName) {
        continue;
      }
      if (!rowsByMachineName.has(machineName)) {
        rowsByMachineName.set(machineName, []);
      }
      rowsByMachineName.get(machineName).push(row);
    }

    const machineNextGapMap = new Map();
    for (const machineRows of rowsByMachineName.values()) {
      const nextGapMap = calculateHuntScoreNextGapMap(machineRows, rankFilter);
      for (const row of machineRows) {
        if (nextGapMap.has(row)) {
          machineNextGapMap.set(row, nextGapMap.get(row));
        }
      }
    }

    for (const row of rows) {
      const nextGapValue =
        nextGapScope === "all"
          ? overallNextGapMap.get(row)
          : nextGapScope === "machine"
            ? machineNextGapMap.get(row)
            : selectedNextGapMap.get(row);

      if (Number.isFinite(nextGapValue)) {
        valueMap.set(
          buildHuntScoreHighlightKey(snapshot.date, row.machineName, row.slotNumber),
          nextGapValue,
        );
      }
    }
  }

  return valueMap;
}

function buildHuntScoreRankValueMap(rows) {
  const valueMap = new Map();
  const rankedRows = (Array.isArray(rows) ? rows : [])
    .map((row, index) => ({
      row,
      index,
      huntScore: Number(row?.huntScore),
    }))
    .filter((row) => Number.isFinite(row.huntScore))
    .sort((left, right) => {
      const scoreDifference = right.huntScore - left.huntScore;
      if (Math.abs(scoreDifference) > COMPARISON_SCORE_EPSILON) {
        return scoreDifference;
      }
      return left.index - right.index;
    });

  let previousScore = null;
  let previousRank = 0;

  for (let index = 0; index < rankedRows.length; index += 1) {
    const row = rankedRows[index];
    const rank =
      previousScore !== null &&
      Math.abs(row.huntScore - previousScore) <= COMPARISON_SCORE_EPSILON
        ? previousRank
        : index + 1;

    valueMap.set(row.row, rank);
    previousScore = row.huntScore;
    previousRank = rank;
  }

  return valueMap;
}

function buildHuntScoreHighlightKeySet(highlightDetail, options, nextGapValueMap) {
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
  const scoreFilter = buildScoreFilter(normalizedOptions.scoreMin);
  const nextGapFilter = buildNextGapFilter(normalizedOptions.nextGapMin);
  const requirementOptions = buildConditionRequirementOptions(normalizedOptions, {
    rankRequired: DEFAULT_HUNT_RANK_REQUIRED,
    scoreRequired: DEFAULT_HUNT_SCORE_REQUIRED,
    nextGapRequired: DEFAULT_HUNT_NEXT_GAP_REQUIRED,
  });
  const rankScope = normalizeHuntScoreRankScope(normalizedOptions.rankScope);

  if (
    !rankFilter.hasRankFilter &&
    !scoreFilter.hasScoreFilter &&
    !nextGapFilter.hasNextGapFilter
  ) {
    return matchKeys;
  }

  for (const snapshot of snapshots) {
    const rows = Array.isArray(snapshot.rows) ? snapshot.rows : [];
    const overallRankMap = buildHuntScoreRankValueMap(rows);
    const selectedRows = rows.filter((row) =>
      selectedMachineNameSet.has(String(row.machineName ?? "").trim()),
    );
    const selectedRankMap = buildHuntScoreRankValueMap(selectedRows);
    const rowsByMachineName = new Map();

    for (const row of rows) {
      const machineName = resolveHuntMachineGroupName(row.machineName, normalizedOptions);
      if (!machineName) {
        continue;
      }
      if (!rowsByMachineName.has(machineName)) {
        rowsByMachineName.set(machineName, []);
      }
      rowsByMachineName.get(machineName).push(row);
    }

    const machineRankMap = new Map();
    for (const machineRows of rowsByMachineName.values()) {
      const rankMap = buildHuntScoreRankValueMap(machineRows);
      for (const row of machineRows) {
        if (rankMap.has(row)) {
          machineRankMap.set(row, rankMap.get(row));
        }
      }
    }

    for (const row of rows) {
      const machineName = String(row.machineName ?? "").trim();
      const slotNumber = String(row.slotNumber ?? "").trim();
      const huntScore = Number(row.huntScore);
      if (!machineName || !slotNumber || !Number.isFinite(huntScore)) {
        continue;
      }

      const rankValue =
        rankScope === "all"
          ? overallRankMap.get(row) ?? parsePositiveIntegerOption(row.rank)
          : rankScope === "machine"
            ? machineRankMap.get(row)
            : selectedRankMap.get(row);
      const rowKey = buildHuntScoreHighlightKey(snapshot.date, machineName, slotNumber);
      const nextGapValue = nextGapValueMap.get(rowKey);

      if (
        matchesRequiredConditionFilters(
          rankValue,
          huntScore,
          rankFilter,
          scoreFilter,
          requirementOptions,
          false,
          nextGapValue,
          nextGapFilter,
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

function createDefaultEstimateOptions() {
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
  if (!record) {
    return null;
  }

  const resolvedDefinition = definition ?? getSettingEstimateDefinition(record.machine_name);
  if (!resolvedDefinition) {
    return null;
  }

  const dataEstimate = getSettingEstimate(resolvedDefinition, record);
  const gameEstimate = options.gameEnabled
    ? calculateGameCountEstimate(resolvedDefinition, record, options)
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

function createHuntScoreNextGapMetric(getHuntScoreNextGapValue) {
  const renderNextGap = (_value, record, context) =>
    formatNarrowDecimal(getHuntScoreNextGapValue(record, context));
  const csvRenderNextGap = (_value, record, context) =>
    formatDecimal(getHuntScoreNextGapValue(record, context));

  return {
    key: "hunt_score_next_gap",
    label: "次点差",
    render: renderNextGap,
    csvRender: csvRenderNextGap,
    columnClass: "matrixColumnMedium",
  };
}

function formatEstimatedGrapeDenominator(value) {
  const denominator = Number(value);
  if (!Number.isFinite(denominator) || denominator <= 0) {
    return "-";
  }
  return `1/${denominator.toFixed(2)}`;
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

const ESTIMATED_GRAPE_METRIC = {
  key: ESTIMATED_GRAPE_METRIC_KEY,
  label: "推定ブドウ",
  render: formatEstimatedGrapeDenominator,
  csvRender: formatEstimatedGrapeDenominator,
  columnClass: "matrixColumnWide",
};

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
  hasSettingEstimate,
  getCompositeSettingEstimate,
  hasEstimatedGrape,
  hasHuntScore,
  differenceMode,
  getHuntScoreNextGapValue,
) {
  const metrics = [
    createDifferenceMetric(differenceMode),
    ...COMMON_METRICS.filter((metric) => metric.key !== "difference_value"),
  ];

  if (hasEstimatedGrape) {
    metrics.push(ESTIMATED_GRAPE_METRIC);
  }

  if (hasSettingEstimate) {
    metrics.push(createSettingEstimateMetric(getCompositeSettingEstimate));
  }

  if (hasHuntScore) {
    metrics.push(createHuntScoreMetric());
    metrics.push(createHuntScoreNextGapMetric(getHuntScoreNextGapValue));
  }

  return [...metrics, ...RATIO_METRICS];
}

function formatSlotHeaderLabel(slotLabels, slotNumber) {
  return slotLabels?.[slotNumber] ?? `${slotNumber}番台`;
}

function buildCsvRows(slotNumbers, slotLabels, dateRows, metrics, specialDateSet) {
  const headerRow1 = ["日付", "曜日", "特定日"];
  const headerRow2 = ["", "", ""];

  for (const slotNumber of slotNumbers) {
    for (let i = 0; i < metrics.length; i++) {
      headerRow1.push(i === 0 ? formatSlotHeaderLabel(slotLabels, slotNumber) : "");
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

function HuntScoreHighlightControls({
  options,
  availableMachineNames,
  availableMachineSlotCounts = {},
  onChange,
  onApply,
  hasPendingChanges,
  isApplying,
  applyError,
  loadedFullData,
}) {
  const selectedMachineNameSet = new Set(options.selectedMachineNames);
  const hasAimJugglerGroupOption = hasAimJugglerHuntMachineGroupOption(availableMachineNames);
  const hasHanabiGroupOption = hasHanabiHuntMachineGroupOption(availableMachineNames);
  const machineOptionGroups = groupHuntMachineOptions(
    availableMachineNames.map((machineName) => ({
      name: machineName,
      checked: selectedMachineNameSet.has(machineName),
      slotCount: availableMachineSlotCounts?.[machineName] ?? null,
    })),
    {
      combineAimJuggler: options.combineAimJuggler,
      combineHanabi: options.combineHanabi,
    },
  );

  const updateOption = (key, value) => {
    onChange({ ...options, [key]: value });
  };

  const selectMachineCategory = (category) => {
    const targetOptions = machineOptionGroups.find((group) => group.key === category)?.options ?? [];
    const targetMachineNames = targetOptions.flatMap((machine) =>
      machine.combinedRole === "group" ? machine.combinedMemberNames ?? [] : [machine.name],
    );
    if (targetMachineNames.length === 0) {
      return;
    }
    const nextMachineNameSet = new Set(options.selectedMachineNames);
    for (const machineName of targetMachineNames) {
      nextMachineNameSet.add(machineName);
    }
    onChange({
      ...options,
      combineAimJuggler:
        targetOptions.some((machine) => machine.combinedGroupKey === "aimJuggler")
          ? true
          : options.combineAimJuggler,
      combineHanabi:
        targetOptions.some((machine) => machine.combinedGroupKey === "hanabi")
          ? true
          : options.combineHanabi,
      selectedMachineNames: availableMachineNames.filter((name) => nextMachineNameSet.has(name)),
    });
  };

  const clearMachineCategory = (category) => {
    const targetOptions = machineOptionGroups.find((group) => group.key === category)?.options ?? [];
    const targetMachineNames = targetOptions.flatMap((machine) =>
      machine.combinedRole === "group" ? machine.combinedMemberNames ?? [] : [machine.name],
    );
    if (targetMachineNames.length === 0) {
      return;
    }
    const targetMachineNameSet = new Set(targetMachineNames);
    onChange({
      ...options,
      combineAimJuggler:
        targetOptions.some((machine) => machine.combinedGroupKey === "aimJuggler")
          ? false
          : options.combineAimJuggler,
      combineHanabi:
        targetOptions.some((machine) => machine.combinedGroupKey === "hanabi")
          ? false
          : options.combineHanabi,
      selectedMachineNames: options.selectedMachineNames.filter(
        (name) => !targetMachineNameSet.has(name),
      ),
    });
  };

  const selectAllMachines = () => {
    onChange({
      ...options,
      combineAimJuggler: hasAimJugglerGroupOption,
      combineHanabi: hasHanabiGroupOption,
      selectedMachineNames: availableMachineNames,
    });
  };

  const clearAllMachines = () => {
    onChange({
      ...options,
      combineAimJuggler: false,
      combineHanabi: false,
      selectedMachineNames: [],
    });
  };

  const toggleMachine = (machine) => {
    const machineName = machine.name;
    const nextMachineNameSet = new Set(options.selectedMachineNames);
    const nextOptions = { ...options };

    if (machine.combinedRole === "group") {
      const memberNames = (machine.combinedMemberNames ?? []).filter((name) =>
        availableMachineNames.includes(name),
      );
      if (machine.checked) {
        for (const memberName of memberNames) {
          nextMachineNameSet.delete(memberName);
        }
      } else {
        for (const memberName of memberNames) {
          nextMachineNameSet.add(memberName);
        }
      }
      if (machine.combinedGroupKey === "aimJuggler") {
        nextOptions.combineAimJuggler = !machine.checked;
      }
      if (machine.combinedGroupKey === "hanabi") {
        nextOptions.combineHanabi = !machine.checked;
      }
    } else if (machine.combinedRole === "member") {
      const groupActive =
        (machine.combinedGroupKey === "aimJuggler" && options.combineAimJuggler) ||
        (machine.combinedGroupKey === "hanabi" && options.combineHanabi);
      const memberNames =
        machine.combinedGroupKey === "aimJuggler"
          ? AIM_JUGGLER_MACHINE_NAMES
          : machine.combinedGroupKey === "hanabi"
            ? HANABI_MACHINE_NAMES
            : [];
      if (groupActive) {
        for (const memberName of memberNames) {
          nextMachineNameSet.delete(memberName);
        }
        nextMachineNameSet.add(machineName);
      } else if (nextMachineNameSet.has(machineName)) {
        nextMachineNameSet.delete(machineName);
      } else {
        nextMachineNameSet.add(machineName);
      }
      if (machine.combinedGroupKey === "aimJuggler") {
        nextOptions.combineAimJuggler = false;
      }
      if (machine.combinedGroupKey === "hanabi") {
        nextOptions.combineHanabi = false;
      }
    } else if (nextMachineNameSet.has(machineName)) {
      nextMachineNameSet.delete(machineName);
    } else {
      nextMachineNameSet.add(machineName);
    }

    onChange({
      ...nextOptions,
      selectedMachineNames: availableMachineNames.filter((name) => nextMachineNameSet.has(name)),
    });
  };

  return (
    <div className="huntHighlightControls">
      <div className="huntHighlightApplyRow">
        <button
          type="button"
          className="storeReserveButton"
          disabled={!hasPendingChanges || isApplying}
          onClick={onApply}
        >
          {isApplying ? "更新中" : "狙い度条件を反映"}
        </button>
        <p className="filterPanelStatus">
          {hasPendingChanges ? "条件変更はまだ表に反映されていません" : "条件は表に反映済み"}
          {loadedFullData ? " / 全体比較データ読み込み済み" : ""}
        </p>
        {applyError ? <p className="formErrorText">{applyError}</p> : null}
      </div>
      <div className="huntConditionRows">
        <div className="huntConditionRow">
          <p className="huntConditionLabel">順位</p>
          <div className="huntConditionInputs">
            <EstimateNumberField
              label="開始"
              value={options.rankMin}
              min={1}
              onChange={(value) => updateOption("rankMin", value)}
            />
            <EstimateNumberField
              label="終了"
              value={options.rankMax}
              min={1}
              onChange={(value) => updateOption("rankMax", value)}
            />
          </div>
          <label
            className={`metricToggleChip huntConditionRequired ${
              options.rankRequired ? "metricToggleChipActive" : ""
            }`}
          >
            <input
              type="checkbox"
              checked={options.rankRequired}
              onChange={(event) => updateOption("rankRequired", event.target.checked)}
            />
            <span>必須</span>
          </label>
        </div>
        <div className="huntConditionRow">
          <p className="huntConditionLabel">狙い度</p>
          <div className="huntConditionInputs">
            <EstimateNumberField
              label="下限"
              value={options.scoreMin}
              min={0}
              max={100}
              step={0.1}
              suffix="以上"
              onChange={(value) => updateOption("scoreMin", value)}
            />
          </div>
          <label
            className={`metricToggleChip huntConditionRequired ${
              options.scoreRequired ? "metricToggleChipActive" : ""
            }`}
          >
            <input
              type="checkbox"
              checked={options.scoreRequired}
              onChange={(event) => updateOption("scoreRequired", event.target.checked)}
            />
            <span>必須</span>
          </label>
        </div>
        <div className="huntConditionRow">
          <p className="huntConditionLabel">次点差</p>
          <div className="huntConditionInputs">
            <EstimateNumberField
              label="下限"
              value={options.nextGapMin}
              min={0}
              step={0.1}
              onChange={(value) => updateOption("nextGapMin", value)}
            />
          </div>
          <label
            className={`metricToggleChip huntConditionRequired ${
              options.nextGapRequired ? "metricToggleChipActive" : ""
            }`}
          >
            <input
              type="checkbox"
              checked={options.nextGapRequired}
              onChange={(event) => updateOption("nextGapRequired", event.target.checked)}
            />
            <span>必須</span>
          </label>
        </div>
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
            <span>選択機種内順位</span>
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
        <p className="filterControlLabel">次点差の比較対象</p>
        <div className="metricToggleRow">
          <label
            className={`metricToggleChip ${
              options.nextGapScope === "selected" ? "metricToggleChipActive" : ""
            }`}
          >
            <input
              type="radio"
              name="machineHuntScoreNextGapScope"
              value="selected"
              checked={options.nextGapScope === "selected"}
              onChange={() => updateOption("nextGapScope", "selected")}
            />
            <span>選択機種内</span>
          </label>
          <label
            className={`metricToggleChip ${
              options.nextGapScope === "machine" ? "metricToggleChipActive" : ""
            }`}
          >
            <input
              type="radio"
              name="machineHuntScoreNextGapScope"
              value="machine"
              checked={options.nextGapScope === "machine"}
              onChange={() => updateOption("nextGapScope", "machine")}
            />
            <span>機種内</span>
          </label>
          <label
            className={`metricToggleChip ${
              options.nextGapScope === "all" ? "metricToggleChipActive" : ""
            }`}
          >
            <input
              type="radio"
              name="machineHuntScoreNextGapScope"
              value="all"
              checked={options.nextGapScope === "all"}
              onChange={() => updateOption("nextGapScope", "all")}
            />
            <span>選択機種内</span>
          </label>
        </div>
      </div>

      {availableMachineNames.length > 0 ? (
        <div className="backtestBlock">
          <p className="filterControlLabel">順位と次点差に使う機種</p>
          <div className="machineFilterActionRow">
            <button
              type="button"
              className="storeReserveButton storeReserveButtonSecondary machineFilterAction"
              onClick={selectAllMachines}
            >
              全てのチェックをON
            </button>
            <button
              type="button"
              className="storeReserveButton storeReserveButtonSecondary machineFilterAction"
              onClick={clearAllMachines}
            >
              全てのチェックをOFF
            </button>
          </div>
          <div className="machineFilterGroups">
            {machineOptionGroups.map((group) => (
              <div key={group.key} className="machineFilterGroup">
                <p className="machineFilterGroupLabel">{group.label}</p>
                <div className="machineGroupToggleRow">
                  <button
                    type="button"
                    className="storeReserveButton storeReserveButtonSecondary machineFilterAction"
                    onClick={() => selectMachineCategory(group.key)}
                  >
                    {group.label}のみ選択
                  </button>
                  <button
                    type="button"
                    className="storeReserveButton storeReserveButtonSecondary machineFilterAction"
                    onClick={() => clearMachineCategory(group.key)}
                  >
                    {group.label}のみ解除
                  </button>
                </div>
                <div className="metricToggleRow">
                  {group.options.map((machine) => (
                    <label
                      key={machine.name}
                      className={`metricToggleChip ${
                        machine.checked ? "metricToggleChipActive" : ""
                      }`}
                      title={machine.name}
                    >
                      <input
                        type="checkbox"
                        checked={machine.checked}
                        onChange={() => toggleMachine(machine)}
                      />
                      <span>{machine.optionLabel}</span>
                    </label>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function SettingEstimateControls({
  options,
  onChange,
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

function CollapsibleControlGroup({ title, open, onOpenChange, children }) {
  return (
    <div className="collapsibleControlGroup">
      <button
        type="button"
        className="collapsibleControlHeader"
        aria-expanded={open}
        onClick={() => onOpenChange(!open)}
      >
        <span>{title}</span>
        <span className="collapsibleControlStatus">{open ? "閉じる" : "開く"}</span>
      </button>
      {open ? <div className="collapsibleControlBody">{children}</div> : null}
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
  const dateCellClassName = ["dateCell", row.hasSite7Data ? "site7DateCell" : ""]
    .filter(Boolean)
    .join(" ");
  const site7FetchedTime = formatSite7FetchedTime(row.site7FetchedAt);
  const site7FetchedDateTime = formatSite7FetchedDateTime(row.site7FetchedAt);
  const site7Title = row.hasSite7Data
    ? site7FetchedDateTime
      ? `Sセブン暫定データ\n取得: ${site7FetchedDateTime}`
      : "Sセブン暫定データ"
    : undefined;

  return (
    <tr className={isHighlighted ? "matrixRowHighlighted" : ""}>
      <th
        className={dateCellClassName}
        title={site7Title}
      >
        <span className="dateCellStack">
          <span>{formatShortDate(row.date)}</span>
          {row.hasSite7Data ? (
            <span className="site7DateBadge">
              Sセブン{site7FetchedTime ? <span className="site7BadgeTime">{site7FetchedTime}</span> : null}
            </span>
          ) : null}
        </span>
      </th>
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
            metric.key === "hunt_score" ||
            metric.key === "hunt_score_next_gap";
          const huntScoreHighlightClass =
            isHuntScoreMetric &&
            huntScoreHighlightKeySet.has(
              buildHuntScoreHighlightKey(row.date, record?.machine_name, record?.slot_number ?? slotNumber),
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
  storeName = "",
  machineName,
  slotNumbers,
  slotLabels = {},
  dateRows,
  initialEventFilters,
  initialEventFiltersFromSearchParams = false,
  huntScoreHighlight,
  fullHuntScoreHighlightUrl = "",
  initialDifferenceMode = DEFAULT_DIFFERENCE_MODE,
  preferDefaultEstimateOptions = false,
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
  const defaultEventFilters = useMemo(
    () =>
      createEventFilters(
        initialEventFilters?.dayTails ?? [],
        initialEventFilters?.zoro ?? false,
        initialEventFilters?.weekdays ?? [],
        initialEventFilters?.monthDays ?? [],
      ),
    [initialEventFilters],
  );
  const defaultEstimateOptions = useMemo(
    () => createDefaultEstimateOptions(),
    [],
  );
  const defaultComparisonOptions = useMemo(
    () => ({
      periodMode: "recent",
      recentDaysInput: String(DEFAULT_COMPARISON_RECENT_DAYS),
      rangeStartInput: initialRangeStartDate,
      rangeEndInput: latestAvailableDate,
      eventFilters: defaultEventFilters,
      differenceMode: normalizeDifferenceMode(initialDifferenceMode),
      visibleMetricKeys: DEFAULT_VISIBLE_METRIC_KEYS,
      estimateOptions: defaultEstimateOptions,
      displayControlsOpen: true,
      settingControlsOpen: true,
      huntScoreControlsOpen: true,
    }),
    [
      defaultEstimateOptions,
      defaultEventFilters,
      initialDifferenceMode,
      initialRangeStartDate,
      latestAvailableDate,
    ],
  );
  const [periodMode, setPeriodMode] = useState(defaultComparisonOptions.periodMode);
  const [recentDaysInput, setRecentDaysInput] = useState(defaultComparisonOptions.recentDaysInput);
  const [rangeStartInput, setRangeStartInput] = useState(defaultComparisonOptions.rangeStartInput);
  const [rangeEndInput, setRangeEndInput] = useState(defaultComparisonOptions.rangeEndInput);
  const [eventFilters, setEventFilters] = useState(defaultComparisonOptions.eventFilters);
  const [differenceMode, setDifferenceMode] = useState(defaultComparisonOptions.differenceMode);
  const [visibleMetricKeys, setVisibleMetricKeys] = useState(defaultComparisonOptions.visibleMetricKeys);
  const [estimateOptions, setEstimateOptions] = useState(defaultComparisonOptions.estimateOptions);
  const [displayControlsOpen, setDisplayControlsOpen] = useState(
    defaultComparisonOptions.displayControlsOpen,
  );
  const [settingControlsOpen, setSettingControlsOpen] = useState(
    defaultComparisonOptions.settingControlsOpen,
  );
  const [huntScoreControlsOpen, setHuntScoreControlsOpen] = useState(
    defaultComparisonOptions.huntScoreControlsOpen,
  );
  const [machineComparisonOptionsLoadedStoreId, setMachineComparisonOptionsLoadedStoreId] =
    useState("");
  const estimateOptionsTouchedRef = useRef(false);
  const huntScoreHighlightAvailableMachineNames = useMemo(
    () => normalizeAvailableHuntScoreMachineNames(huntScoreHighlight?.availableMachineNames),
    [huntScoreHighlight],
  );
  const [huntScoreHighlightOptions, setHuntScoreHighlightOptions] = useState(() =>
    createDefaultHuntScoreHighlightOptions(huntScoreHighlightAvailableMachineNames, machineName),
  );
  const [appliedHuntScoreHighlightOptions, setAppliedHuntScoreHighlightOptions] = useState(() =>
    createDefaultHuntScoreHighlightOptions(huntScoreHighlightAvailableMachineNames, machineName),
  );
  const [activeHuntScoreHighlight, setActiveHuntScoreHighlight] = useState(huntScoreHighlight);
  const [fullHuntScoreHighlight, setFullHuntScoreHighlight] = useState(null);
  const huntScoreHighlightMachineSlotCounts = useMemo(() => {
    const source = fullHuntScoreHighlight?.machineSlotCounts ?? huntScoreHighlight?.machineSlotCounts;
    return source && typeof source === "object" ? source : {};
  }, [fullHuntScoreHighlight, huntScoreHighlight]);
  const [isHuntScoreHighlightApplying, setIsHuntScoreHighlightApplying] = useState(false);
  const [huntScoreHighlightApplyError, setHuntScoreHighlightApplyError] = useState("");
  const [huntScoreHighlightOptionsLoadedStoreId, setHuntScoreHighlightOptionsLoadedStoreId] =
    useState("");
  const [, startTransition] = useTransition();
  const recentDays = useMemo(() => normalizeRecentDaysInput(recentDaysInput), [recentDaysInput]);
  const huntScoreNextGapValueMap = useMemo(
    () =>
      buildHuntScoreNextGapValueMap(
        activeHuntScoreHighlight,
        appliedHuntScoreHighlightOptions,
      ),
    [activeHuntScoreHighlight, appliedHuntScoreHighlightOptions],
  );
  const huntScoreNextGapConditionValueMap = useMemo(
    () =>
      buildHuntScoreNextGapValueMap(
        activeHuntScoreHighlight,
        appliedHuntScoreHighlightOptions,
        true,
      ),
    [activeHuntScoreHighlight, appliedHuntScoreHighlightOptions],
  );
  const huntScoreHighlightKeySet = useMemo(
    () =>
      buildHuntScoreHighlightKeySet(
        activeHuntScoreHighlight,
        appliedHuntScoreHighlightOptions,
        huntScoreNextGapConditionValueMap,
      ),
    [
      activeHuntScoreHighlight,
      appliedHuntScoreHighlightOptions,
      huntScoreNextGapConditionValueMap,
    ],
  );
  const hasPendingHuntScoreHighlightOptions = useMemo(
    () => !huntScoreHighlightOptionsEqual(huntScoreHighlightOptions, appliedHuntScoreHighlightOptions),
    [appliedHuntScoreHighlightOptions, huntScoreHighlightOptions],
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
  const hasSettingEstimate = useMemo(
    () =>
      Boolean(settingEstimateDefinition) ||
      dateRows.some((row) =>
        slotNumbers.some((slotNumber) =>
          Boolean(getSettingEstimateDefinition(row.recordsBySlot?.[slotNumber]?.machine_name)),
        ),
      ),
    [dateRows, settingEstimateDefinition, slotNumbers],
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
  const getHuntScoreNextGapValue = useCallback(
    (record, context) =>
      huntScoreNextGapValueMap.get(
        buildHuntScoreHighlightKey(
          context?.row?.date ?? record?.target_date,
          record?.machine_name,
          record?.slot_number ?? context?.slotNumber,
        ),
      ) ?? null,
    [huntScoreNextGapValueMap],
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
  const hasEstimatedGrape = useMemo(
    () =>
      dateRows.some((row) =>
        slotNumbers.some((slotNumber) =>
          Number.isFinite(Number(row.recordsBySlot?.[slotNumber]?.estimated_grape_denominator)),
        ),
      ),
    [dateRows, slotNumbers],
  );
  const metrics = useMemo(
    () =>
      getMetrics(
        hasSettingEstimate,
        getCompositeSettingEstimate,
        hasEstimatedGrape,
        hasHuntScore,
        differenceMode,
        getHuntScoreNextGapValue,
      ),
    [
      differenceMode,
      getCompositeSettingEstimate,
      getHuntScoreNextGapValue,
      hasEstimatedGrape,
      hasHuntScore,
      hasSettingEstimate,
      settingEstimateDefinition,
    ],
  );
  const metricKeys = useMemo(() => metrics.map((metric) => metric.key), [metrics]);

  useEffect(() => {
    setMachineComparisonOptionsLoadedStoreId("");
    estimateOptionsTouchedRef.current = false;
    const options = readMachineComparisonOptions(storeId, defaultComparisonOptions, {
      oldestAvailableDate,
      latestAvailableDate,
      preferInitialEventFilters: initialEventFiltersFromSearchParams,
      preferDefaultEstimateOptions,
    });
    setPeriodMode(options.periodMode);
    setRecentDaysInput(options.recentDaysInput);
    setRangeStartInput(options.rangeStartInput);
    setRangeEndInput(options.rangeEndInput);
    setEventFilters(options.eventFilters);
    setDifferenceMode(normalizeDifferenceMode(initialDifferenceMode));
    setVisibleMetricKeys(options.visibleMetricKeys);
    setEstimateOptions(options.estimateOptions);
    setDisplayControlsOpen(options.displayControlsOpen);
    setSettingControlsOpen(options.settingControlsOpen);
    setHuntScoreControlsOpen(options.huntScoreControlsOpen);
    setMachineComparisonOptionsLoadedStoreId(storeId);
  }, [
    defaultComparisonOptions,
    initialEventFiltersFromSearchParams,
    latestAvailableDate,
    oldestAvailableDate,
    preferDefaultEstimateOptions,
    initialDifferenceMode,
    storeId,
  ]);

  useEffect(() => {
    setVisibleMetricKeys((currentKeys) =>
      normalizeMetricKeys(currentKeys, metricKeys, DEFAULT_VISIBLE_METRIC_KEYS),
    );
  }, [metricKeys]);

  useEffect(() => {
    if (machineComparisonOptionsLoadedStoreId !== storeId) {
      return;
    }
    saveMachineComparisonOptions(storeId, {
      periodMode,
      recentDaysInput,
      rangeStartInput,
      rangeEndInput,
      eventFilters,
      differenceMode,
      visibleMetricKeys,
      estimateOptions,
      preserveEstimateOptions: preferDefaultEstimateOptions && !estimateOptionsTouchedRef.current,
      displayControlsOpen,
      settingControlsOpen,
      huntScoreControlsOpen,
    });
  }, [
    displayControlsOpen,
    differenceMode,
    estimateOptions,
    eventFilters,
    huntScoreControlsOpen,
    machineComparisonOptionsLoadedStoreId,
    periodMode,
    rangeEndInput,
    rangeStartInput,
    recentDaysInput,
    settingControlsOpen,
    storeId,
    visibleMetricKeys,
    preferDefaultEstimateOptions,
  ]);

  useEffect(() => {
    setHuntScoreHighlightOptionsLoadedStoreId("");
    setActiveHuntScoreHighlight(huntScoreHighlight);
    setFullHuntScoreHighlight(null);
    setHuntScoreHighlightApplyError("");
    const loadedOptions = readHuntScoreHighlightOptions(
      storeId,
      huntScoreHighlightAvailableMachineNames,
      machineName,
    );
    const machineOnlyDefaults = createDefaultHuntScoreHighlightOptions(
      huntScoreHighlightAvailableMachineNames,
      machineName,
    );
    const machineScopeOptions = {
      ...loadedOptions,
      rankScope: "machine",
      nextGapScope: "machine",
      selectedMachineNames: machineOnlyDefaults.selectedMachineNames,
    };
    setHuntScoreHighlightOptions(machineScopeOptions);
    setAppliedHuntScoreHighlightOptions(machineScopeOptions);
    setHuntScoreHighlightOptionsLoadedStoreId(storeId);
  }, [huntScoreHighlight, huntScoreHighlightAvailableMachineNames, machineName, storeId]);

  useEffect(() => {
    if (huntScoreHighlightOptionsLoadedStoreId !== storeId) {
      return;
    }
    saveHuntScoreHighlightOptions(storeId, appliedHuntScoreHighlightOptions);
  }, [appliedHuntScoreHighlightOptions, huntScoreHighlightOptionsLoadedStoreId, storeId]);

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

  const visibleRows = periodFilteredRows;

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

  const highlightedDateSet = specialDateSet;

  const csvRows = useMemo(
    () => buildCsvRows(slotNumbers, slotLabels, visibleRows, visibleMetrics, specialDateSet),
    [slotLabels, slotNumbers, specialDateSet, visibleRows, visibleMetrics],
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

  const updateDifferenceMode = (value) => {
    const nextDifferenceMode = normalizeDifferenceMode(value);
    setDifferenceMode(nextDifferenceMode);

    if (typeof window === "undefined") {
      return;
    }

    const url = new URL(window.location.href);
    url.searchParams.set("differenceMode", nextDifferenceMode);
    window.location.assign(url.toString());
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
          currentFilters.monthDays,
        );
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
          currentFilters.monthDays,
        );
        return nextFilters;
      });
    });
  };

  const toggleMonthDay = (monthDay) => {
    startTransition(() => {
      setEventFilters((currentFilters) => {
        const currentMonthDays = currentFilters.monthDays ?? [];
        const nextMonthDays = currentMonthDays.includes(monthDay)
          ? currentMonthDays.filter((value) => value !== monthDay)
          : [...currentMonthDays, monthDay];
        return createEventFilters(
          currentFilters.dayTails,
          currentFilters.zoro,
          currentFilters.weekdays,
          nextMonthDays,
        );
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
          currentFilters.monthDays,
        );
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
    estimateOptionsTouchedRef.current = true;
    setEstimateOptions((currentOptions) => ({
      ...currentOptions,
      ...changes,
    }));
  }, []);

  const applyHuntScoreHighlightOptions = useCallback(async () => {
    const needsFullData = huntScoreHighlightNeedsFullData(huntScoreHighlightOptions, machineName);
    setIsHuntScoreHighlightApplying(true);
    setHuntScoreHighlightApplyError("");

    try {
      let nextHighlight = huntScoreHighlight;
      if (needsFullData) {
        if (fullHuntScoreHighlight) {
          nextHighlight = fullHuntScoreHighlight;
        } else {
          if (!fullHuntScoreHighlightUrl) {
            throw new Error("全体比較データの取得先がありません。");
          }
          const fullHighlightUrl = new URL(fullHuntScoreHighlightUrl, window.location.href);
          fullHighlightUrl.searchParams.set("differenceMode", differenceMode);
          const response = await fetch(fullHighlightUrl.toString(), {
            method: "GET",
            headers: {
              accept: "application/json",
            },
          });
          if (!response.ok) {
            throw new Error("全体比較データを読み込めませんでした。");
          }
          nextHighlight = await response.json();
          setFullHuntScoreHighlight(nextHighlight);
        }
      }

      startTransition(() => {
        setActiveHuntScoreHighlight(nextHighlight);
        setAppliedHuntScoreHighlightOptions(huntScoreHighlightOptions);
      });
    } catch (error) {
      setHuntScoreHighlightApplyError(
        error instanceof Error ? error.message : "狙い度条件を反映できませんでした。",
      );
    } finally {
      setIsHuntScoreHighlightApplying(false);
    }
  }, [
    fullHuntScoreHighlight,
    fullHuntScoreHighlightUrl,
    huntScoreHighlight,
    huntScoreHighlightOptions,
    differenceMode,
    machineName,
    startTransition,
  ]);

  return (
    <>
      <section className="filterPanel machineComparisonFilterPanel">
        <CollapsibleControlGroup
          title="表示条件"
          open={displayControlsOpen}
          onOpenChange={setDisplayControlsOpen}
        >
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
          <p className="filterControlLabel">日付</p>
          <div className="dayFilterRow">
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
          <p className="filterControlLabel">毎月日付</p>
          <div className="dayFilterRow">
            {MONTH_DAY_OPTIONS.map((value) => (
              <button
                key={value}
                type="button"
                onClick={() => toggleMonthDay(value)}
                className={`dayFilterChip ${
                  eventFilters.monthDays?.includes(value) ? "dayFilterChipActive" : ""
                }`}
                aria-pressed={eventFilters.monthDays?.includes(value) ?? false}
              >
                {value}日
              </button>
            ))}
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
          <p className="filterControlLabel">差枚・狙い度計算基準</p>
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
                onChange={() => updateDifferenceMode("bonus")}
              />
              <span>設定1基準</span>
            </label>
            <label
              className={`metricToggleChip ${
                differenceMode === "estimated" ? "metricToggleChipActive" : ""
              }`}
            >
              <input
                type="radio"
                name="machineDifferenceMode"
                value="estimated"
                checked={differenceMode === "estimated"}
                onChange={() => updateDifferenceMode("estimated")}
              />
              <span>推定設定基準</span>
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
                onChange={() => updateDifferenceMode("minrepo")}
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
        </CollapsibleControlGroup>
        {hasSettingEstimate ? (
          <CollapsibleControlGroup
            title="設定推測"
            open={settingControlsOpen}
            onOpenChange={setSettingControlsOpen}
          >
            <SettingEstimateControls
              options={estimateOptions}
              onChange={updateEstimateOptions}
            />
          </CollapsibleControlGroup>
        ) : null}
        {hasHuntScore ? (
          <CollapsibleControlGroup
            title="狙い度"
            open={huntScoreControlsOpen}
            onOpenChange={setHuntScoreControlsOpen}
          >
            <HuntScoreHighlightControls
              options={huntScoreHighlightOptions}
              availableMachineNames={huntScoreHighlightAvailableMachineNames}
              availableMachineSlotCounts={huntScoreHighlightMachineSlotCounts}
              onChange={setHuntScoreHighlightOptions}
              onApply={applyHuntScoreHighlightOptions}
              hasPendingChanges={hasPendingHuntScoreHighlightOptions}
              isApplying={isHuntScoreHighlightApplying}
              applyError={huntScoreHighlightApplyError}
              loadedFullData={Boolean(fullHuntScoreHighlight)}
            />
          </CollapsibleControlGroup>
        ) : null}
      </section>

      <MachineComparisonTable
        storeName={storeName}
        machineName={machineName}
        slotNumbers={slotNumbers}
        slotLabels={slotLabels}
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
  storeName,
  machineName,
  slotNumbers,
  slotLabels,
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
          storeName={storeName}
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
                  {formatSlotHeaderLabel(slotLabels, slotNumber)}
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
