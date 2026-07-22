import { listHuntScoreTargetMachineNames } from "./hunt-score";
import {
  buildBoundaryGapFilters,
  buildNextGapFilter,
  buildRankFilter,
  buildScoreFilter,
  buildUpperGapFilter,
  calculateHuntScoreNextGapMap,
  calculateHuntScoreUpperGapMap,
  buildConditionRequirementOptions,
  matchesRequiredConditionFilters,
  buildScopedRankFilters,
  normalizeDateText,
  readFiniteNumber,
  readNextGapForRankScope,
  readUpperGapForRankScope,
} from "./hunt-bookmark";
import {
  DEFAULT_DIFFERENCE_MODE,
  canonicalMachineName,
  normalizeDifferenceMode as normalizeMachineDifferenceMode,
  selectDifferenceValue,
} from "./machine-difference";
import { isHuntJugglerMachine } from "./hunt-machine-display";
import {
  MACHINE_EVALUATION_BACKTEST_MODE_AND,
  MACHINE_EVALUATION_BACKTEST_MODE_COMMON,
  MACHINE_EVALUATION_BACKTEST_MODE_MACHINE,
  MACHINE_EVALUATION_BACKTEST_MODE_OR,
  normalizeBeamHikariNeoSpatialSelectionEnabled,
  normalizeMachineEvaluationBacktestMode,
} from "./machine-evaluation";
import {
  calculateSettingEstimate,
  getSettingEstimateDefinition,
  normalizeSettingEstimateMode,
  readGrapeSettingEstimateObservation,
  SETTING_ESTIMATE_MODE_GRAPE,
} from "./setting-estimates";
import {
  SETTING_DISTRIBUTION_HIDE,
  SETTING_DISTRIBUTION_SHOW,
  normalizeSettingDistribution,
  shouldShowSettingDistribution,
} from "./setting-distribution";

export {
  SETTING_DISTRIBUTION_HIDE,
  SETTING_DISTRIBUTION_SHOW,
  normalizeSettingDistribution,
  shouldShowSettingDistribution,
} from "./setting-distribution";

const DEFAULT_RECENT_DAYS = 90;
const DAILY_SELECTION_MODE_MACHINE_TOP_NEXT_GAP = "machineTopNextGap";
const LOGIC_CONDITION_MODE_SUM = "sum";
const LOGIC_CONDITION_MODE_AND = "and";
const AIM_JUGGLER_GROUP_NAME = "アイムジャグラーEX";
const AIM_JUGGLER_MACHINE_NAMES = ["SアイムジャグラーＥＸ", "ネオアイムジャグラーEX"];
const HANABI_GROUP_NAME = "ハナビ";
const HANABI_MACHINE_NAMES = ["新ハナビ", "スマスロ ハナビ"];
const BACKTEST_BREAKDOWN_DEFINITIONS = [
  { key: "all", title: "全合算" },
  { key: "normal", title: "通常日" },
  { key: "eventTotal", title: "特定日合算" },
  { key: "dayTail", title: "翌営業日が末尾の日" },
  { key: "weekday", title: "翌営業日が指定曜日" },
];
export function calculateBacktestScoreFilterMax(logicConditionMode, logicCount = 1) {
  const normalizedLogicCount = Number.isInteger(logicCount) && logicCount > 0 ? logicCount : 1;
  return normalizeLogicConditionMode(logicConditionMode) === LOGIC_CONDITION_MODE_SUM
    ? normalizedLogicCount * 100
    : 100;
}

function buildMachineEvaluationConditionFilters(options = {}) {
  return {
    machineEvaluationScoreFilter: buildScoreFilter(
      options?.machineEvaluationScoreMin,
      options?.machineEvaluationScoreMax,
      100,
    ),
    machineEvaluationRankFilter: buildRankFilter(
      options?.machineEvaluationRankMin,
      options?.machineEvaluationRankMax,
    ),
    selectedMachineEvaluationRankFilter: buildRankFilter(
      options?.selectedMachineEvaluationRankMin,
      options?.selectedMachineEvaluationRankMax,
    ),
    machineEvaluationNextGapFilter: buildNextGapFilter(
      options?.machineEvaluationNextGapMin,
      options?.machineEvaluationNextGapMax,
    ),
    selectedMachineEvaluationNextGapFilter: buildNextGapFilter(
      options?.selectedMachineEvaluationNextGapMin,
      options?.selectedMachineEvaluationNextGapMax,
    ),
    machineEvaluationUpperGapFilter: buildUpperGapFilter(
      options?.machineEvaluationUpperGapMin,
      options?.machineEvaluationUpperGapMax,
    ),
    selectedMachineEvaluationUpperGapFilter: buildUpperGapFilter(
      options?.selectedMachineEvaluationUpperGapMin,
      options?.selectedMachineEvaluationUpperGapMax,
    ),
  };
}

function readPositiveInteger(value) {
  const parsedValue = Number(value);
  if (!Number.isInteger(parsedValue) || parsedValue < 1) {
    return null;
  }
  return parsedValue;
}

function readNullableFiniteNumber(value) {
  const parsedValue = Number(value);
  return Number.isFinite(parsedValue) ? parsedValue : null;
}

function normalizeMachineNameText(value) {
  return String(value ?? "").normalize("NFKC").replace(/\s+/gu, "").trim();
}

function isAimJugglerMachine(machineName) {
  const normalizedMachineName = normalizeMachineNameText(machineName);
  return AIM_JUGGLER_MACHINE_NAMES.some(
    (candidate) => normalizeMachineNameText(candidate) === normalizedMachineName,
  );
}

function isHanabiMachine(machineName) {
  const normalizedMachineName = normalizeMachineNameText(machineName);
  return HANABI_MACHINE_NAMES.some(
    (candidate) => normalizeMachineNameText(candidate) === normalizedMachineName,
  );
}

function isAimJugglerGroup(machineName) {
  return normalizeMachineNameText(machineName) === normalizeMachineNameText(AIM_JUGGLER_GROUP_NAME);
}

function isHanabiGroup(machineName) {
  return normalizeMachineNameText(machineName) === normalizeMachineNameText(HANABI_GROUP_NAME);
}

function resolveBacktestMachineName(machineName, combineAimJuggler, combineHanabi) {
  const text = String(machineName ?? "").trim();
  if (combineAimJuggler && isAimJugglerMachine(text)) {
    return AIM_JUGGLER_GROUP_NAME;
  }
  if (combineHanabi && isHanabiMachine(text)) {
    return HANABI_GROUP_NAME;
  }
  return text;
}

function expandRequestedMachineName(machineName) {
  if (isAimJugglerGroup(machineName)) {
    return AIM_JUGGLER_MACHINE_NAMES;
  }
  if (isHanabiGroup(machineName)) {
    return HANABI_MACHINE_NAMES;
  }
  return [String(machineName ?? "").trim()];
}

function expandRequestedMachineNamesForCombine(machineNames, combineAimJuggler, combineHanabi) {
  const requestedMachineNames = [
    ...new Set(
      (Array.isArray(machineNames) ? machineNames : [machineNames])
        .flatMap((machineName) => expandRequestedMachineName(machineName))
        .map((machineName) => String(machineName ?? "").trim())
        .filter(Boolean),
    ),
  ];
  const expandedMachineNames = new Set(requestedMachineNames);

  if (combineAimJuggler && requestedMachineNames.some((machineName) => isAimJugglerMachine(machineName))) {
    for (const machineName of AIM_JUGGLER_MACHINE_NAMES) {
      expandedMachineNames.add(machineName);
    }
  }

  if (combineHanabi && requestedMachineNames.some((machineName) => isHanabiMachine(machineName))) {
    for (const machineName of HANABI_MACHINE_NAMES) {
      expandedMachineNames.add(machineName);
    }
  }

  return [...expandedMachineNames];
}

function splitOptionValues(value) {
  if (Array.isArray(value)) {
    return value.flatMap((item) => splitOptionValues(item));
  }
  if (value === null || value === undefined || value === "") {
    return [];
  }
  return String(value)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizeLogicConditionMode(value) {
  return value === LOGIC_CONDITION_MODE_AND ? LOGIC_CONDITION_MODE_AND : LOGIC_CONDITION_MODE_SUM;
}

function normalizeIntegerOptions(value, min, max) {
  const normalizedValues = new Set();
  for (const item of splitOptionValues(value)) {
    const parsedValue = Number(item);
    if (Number.isInteger(parsedValue) && parsedValue >= min && parsedValue <= max) {
      normalizedValues.add(parsedValue);
    }
  }
  return [...normalizedValues].sort((left, right) => left - right);
}

function shiftDateText(dateText, days) {
  const normalizedDate = normalizeDateText(dateText);
  if (!normalizedDate) {
    return null;
  }

  const baseDate = new Date(`${normalizedDate}T00:00:00Z`);
  if (Number.isNaN(baseDate.getTime())) {
    return null;
  }

  baseDate.setUTCDate(baseDate.getUTCDate() + days);
  return baseDate.toISOString().slice(0, 10);
}

function normalizeCombineAimJuggler(value, machineNames = [], machineTouched = false) {
  if ((Array.isArray(machineNames) ? machineNames : [machineNames]).some(isAimJugglerGroup)) {
    return true;
  }
  const values = splitOptionValues(value);
  if (values.length === 0) {
    return !machineTouched;
  }
  return values.includes("1") || values.includes("true") || values.includes("on");
}

function normalizeCombineHanabi(value, machineNames = [], machineTouched = false) {
  if ((Array.isArray(machineNames) ? machineNames : [machineNames]).some(isHanabiGroup)) {
    return true;
  }
  const values = splitOptionValues(value);
  if (values.length === 0) {
    return !machineTouched;
  }
  return values.includes("1") || values.includes("true") || values.includes("on");
}

function normalizeMachineSelectionTouched(value) {
  return value === true || value === "1" || value === "true" || value === "on";
}

function buildMachineOrder(machineOrder = listHuntScoreTargetMachineNames()) {
  const orderedMachineNames = [...new Set(machineOrder.map(canonicalMachineName))];
  return new Map(orderedMachineNames.map((machineName, index) => [machineName, index]));
}

function buildAvailableMachineNames(snapshots, machineOrderNames) {
  const orderedMachineNames = Array.isArray(machineOrderNames)
    ? machineOrderNames.map((machineName) => String(machineName ?? "").trim()).filter(Boolean)
    : [];
  const machineOrder = buildMachineOrder(orderedMachineNames);
  const snapshotMachineNames = [
    ...new Set(
      snapshots.flatMap((snapshot) =>
        snapshot.rows
          .map((row) => String(row.machineName ?? "").trim())
          .filter(Boolean),
      ),
    ),
  ];
  const machineNames = orderedMachineNames.length > 0
    ? [...new Set([...orderedMachineNames, ...snapshotMachineNames])]
    : snapshotMachineNames;

  return machineNames.sort((left, right) => {
    const leftOrder = machineOrder.get(left);
    const rightOrder = machineOrder.get(right);

    if (leftOrder !== undefined || rightOrder !== undefined) {
      return (leftOrder ?? Number.MAX_SAFE_INTEGER) - (rightOrder ?? Number.MAX_SAFE_INTEGER);
    }

    return left.localeCompare(right, "ja");
  });
}

function buildSelectedMachineNames(requestedMachineNames, availableMachineNames, fallbackToAll = true) {
  const availableMachineNameSet = new Set(availableMachineNames);
  const availableMachineNameByCanonicalName = new Map(
    availableMachineNames.map((machineName) => [canonicalMachineName(machineName), machineName]),
  );
  const normalizedMachineNames = [
    ...new Set(
      (Array.isArray(requestedMachineNames) ? requestedMachineNames : [requestedMachineNames])
        .flatMap((value) => expandRequestedMachineName(value))
        .map((value) => String(value ?? "").trim())
        .filter(Boolean),
    ),
  ]
    .map((machineName) =>
      availableMachineNameSet.has(machineName)
        ? machineName
        : availableMachineNameByCanonicalName.get(canonicalMachineName(machineName)),
    )
    .filter(Boolean);

  if (normalizedMachineNames.length > 0) {
    return [...new Set(normalizedMachineNames)];
  }
  return fallbackToAll ? availableMachineNames : [];
}

function normalizeBacktestRankScope(value, fallbackValue = "selected") {
  return value === "machine" || value === "selected" ? value : fallbackValue;
}

function normalizeDailySelectionMode(value) {
  return splitOptionValues(value).includes(DAILY_SELECTION_MODE_MACHINE_TOP_NEXT_GAP)
    ? DAILY_SELECTION_MODE_MACHINE_TOP_NEXT_GAP
    : "";
}

function isMachineTopNextGapSelectionMode(value) {
  return normalizeDailySelectionMode(value) === DAILY_SELECTION_MODE_MACHINE_TOP_NEXT_GAP;
}

function requireActiveConditionFilters(requirementOptions, filters = {}) {
  return {
    rankRequired: filters.rankFilter?.hasRankFilter
      ? true
      : Boolean(requirementOptions.rankRequired),
    machineRankRequired: filters.machineRankFilter?.hasRankFilter
      ? true
      : Boolean(requirementOptions.machineRankRequired),
    selectedRankRequired: filters.selectedRankFilter?.hasRankFilter
      ? true
      : Boolean(requirementOptions.selectedRankRequired),
    scoreRequired: filters.scoreFilter?.hasScoreFilter
      ? true
      : Boolean(requirementOptions.scoreRequired),
    nextGapRequired: filters.nextGapFilter?.hasNextGapFilter
      ? true
      : Boolean(requirementOptions.nextGapRequired),
    upperGapRequired: filters.upperGapFilter?.hasUpperGapFilter
      ? true
      : Boolean(requirementOptions.upperGapRequired),
    machineNextGapRequired: filters.machineNextGapFilter?.hasNextGapFilter
      ? true
      : Boolean(requirementOptions.machineNextGapRequired),
    selectedNextGapRequired: filters.selectedNextGapFilter?.hasNextGapFilter
      ? true
      : Boolean(requirementOptions.selectedNextGapRequired),
    machineUpperGapRequired: filters.machineUpperGapFilter?.hasUpperGapFilter
      ? true
      : Boolean(requirementOptions.machineUpperGapRequired),
    selectedUpperGapRequired: filters.selectedUpperGapFilter?.hasUpperGapFilter
      ? true
      : Boolean(requirementOptions.selectedUpperGapRequired),
    machineEvaluationScoreRequired: filters.machineEvaluationScoreFilter?.hasScoreFilter
      ? true
      : Boolean(requirementOptions.machineEvaluationScoreRequired),
    machineEvaluationRankRequired: filters.machineEvaluationRankFilter?.hasRankFilter
      ? true
      : Boolean(requirementOptions.machineEvaluationRankRequired),
    selectedMachineEvaluationRankRequired:
      filters.selectedMachineEvaluationRankFilter?.hasRankFilter
        ? true
        : Boolean(requirementOptions.selectedMachineEvaluationRankRequired),
    machineEvaluationNextGapRequired: filters.machineEvaluationNextGapFilter?.hasNextGapFilter
      ? true
      : Boolean(requirementOptions.machineEvaluationNextGapRequired),
    selectedMachineEvaluationNextGapRequired:
      filters.selectedMachineEvaluationNextGapFilter?.hasNextGapFilter
        ? true
        : Boolean(requirementOptions.selectedMachineEvaluationNextGapRequired),
    machineEvaluationUpperGapRequired: filters.machineEvaluationUpperGapFilter?.hasUpperGapFilter
      ? true
      : Boolean(requirementOptions.machineEvaluationUpperGapRequired),
    selectedMachineEvaluationUpperGapRequired:
      filters.selectedMachineEvaluationUpperGapFilter?.hasUpperGapFilter
        ? true
        : Boolean(requirementOptions.selectedMachineEvaluationUpperGapRequired),
  };
}

function buildMachineSlotCountLookup(machineSlotCounts) {
  const lookup = new Map();
  if (!machineSlotCounts || typeof machineSlotCounts !== "object") {
    return lookup;
  }

  for (const [machineName, rawSlotCount] of Object.entries(machineSlotCounts)) {
    const normalizedMachineName = String(machineName ?? "").trim();
    const slotCount = Number(rawSlotCount ?? 0);
    if (!normalizedMachineName || !Number.isFinite(slotCount) || slotCount <= 0) {
      continue;
    }
    lookup.set(normalizedMachineName, slotCount);
  }

  return lookup;
}

function readMachineSlotCount(machineSlotCountLookup, machineName) {
  const normalizedMachineName = String(machineName ?? "").trim();
  if (!normalizedMachineName || !(machineSlotCountLookup instanceof Map)) {
    return null;
  }

  const slotCount = Number(machineSlotCountLookup.get(normalizedMachineName) ?? 0);
  return Number.isFinite(slotCount) && slotCount > 0 ? slotCount : null;
}

function addSummarySlotCount(summary, machineSlotCountLookup) {
  return {
    ...summary,
    slotCount: readMachineSlotCount(machineSlotCountLookup, summary.machineName),
  };
}

function calculateMachineSlotCountTotal(machineNames, machineSlotCountLookup) {
  let slotCountTotal = 0;
  let hasSlotCount = false;

  for (const machineName of Array.isArray(machineNames) ? machineNames : []) {
    const slotCount = readMachineSlotCount(machineSlotCountLookup, machineName);
    if (slotCount === null) {
      continue;
    }
    slotCountTotal += slotCount;
    hasSlotCount = true;
  }

  return hasSlotCount ? slotCountTotal : null;
}

function normalizeDifferenceMode(value) {
  return normalizeMachineDifferenceMode(value);
}

function readOptionWithDefault(options, key, fallbackValue) {
  return Object.hasOwn(options ?? {}, key) ? options[key] : fallbackValue;
}

function buildPeriodState(options, latestDate) {
  const periodMode = options?.periodMode === "range" ? "range" : "recent";
  const recentDays = readPositiveInteger(options?.recentDays) ?? DEFAULT_RECENT_DAYS;
  const fallbackStartDate = latestDate ? shiftDateText(latestDate, -(recentDays - 1)) : null;

  if (periodMode === "range") {
    let startDate = normalizeDateText(options?.startDate);
    let endDate = normalizeDateText(options?.endDate);

    if (startDate && !endDate) {
      endDate = startDate;
    } else if (!startDate && endDate) {
      startDate = endDate;
    }

    if (startDate && endDate) {
      return {
        periodMode,
        recentDays,
        startDate: startDate <= endDate ? startDate : endDate,
        endDate: startDate <= endDate ? endDate : startDate,
        usedFallbackRange: false,
      };
    }

    return {
      periodMode,
      recentDays,
      startDate: fallbackStartDate,
      endDate: latestDate ?? null,
      usedFallbackRange: true,
    };
  }

  return {
    periodMode,
    recentDays,
    startDate: fallbackStartDate,
    endDate: latestDate ?? null,
    usedFallbackRange: false,
  };
}

function isSnapshotInPeriod(snapshot, startDate, endDate) {
  const baseDate = String(snapshot?.baseDate ?? "").trim();
  if (!baseDate) {
    return false;
  }
  if (startDate && baseDate < startDate) {
    return false;
  }
  if (endDate && baseDate > endDate) {
    return false;
  }
  return true;
}

function getDateWeekday(dateText) {
  const match = String(dateText).match(/^(\d{4})-(\d{2})-(\d{2})$/u);
  const parsedDate = match
    ? new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]))
    : new Date(dateText);

  if (Number.isNaN(parsedDate.getTime())) {
    return null;
  }

  return parsedDate.getDay();
}

function matchesDayTail(dateText, dayTails) {
  const dayTail = Number(String(dateText).slice(-1));
  return dayTails.includes(dayTail);
}

function matchesWeekday(dateText, weekdays) {
  const weekday = getDateWeekday(dateText);
  return weekday !== null && weekdays.includes(weekday);
}

function matchesMonthDay(dateText, monthDays) {
  const match = String(dateText).match(/^\d{4}-\d{2}-(\d{2})$/u);
  if (!match) {
    return false;
  }
  const monthDay = Number(match[1]);
  return monthDays.includes(monthDay);
}

function matchesZoro(dateText, zoro) {
  if (!zoro) {
    return false;
  }
  const match = String(dateText).match(/^\d{4}-(\d{2})-(\d{2})$/u);
  return Boolean(match) && Number(match[1]) === Number(match[2]);
}

function buildBacktestEventFilters(options) {
  return {
    dayTails: normalizeIntegerOptions(options?.dayTails, 0, 9),
    weekdays: normalizeIntegerOptions(options?.weekdays, 0, 6),
    monthDays: normalizeIntegerOptions(options?.monthDays, 1, 31),
    zoro: splitOptionValues(options?.zoro).some((value) =>
      ["1", "true", "on"].includes(String(value).toLowerCase()),
    ),
  };
}

function getActualDate(row, snapshot) {
  return String(row?.nextBusinessDate ?? snapshot?.nextBusinessDate ?? "").trim();
}

function buildBreakdownRowFilter(breakdownKey, eventFilters) {
  if (breakdownKey === "all") {
    return () => true;
  }

  return ({ actualDate }) => {
    if (!actualDate) {
      return false;
    }

    const isDayTail = matchesDayTail(actualDate, eventFilters.dayTails);
    const isWeekday = matchesWeekday(actualDate, eventFilters.weekdays);
    const isMonthDay = matchesMonthDay(actualDate, eventFilters.monthDays);
    const isZoro = matchesZoro(actualDate, eventFilters.zoro);
    const isEvent = isDayTail || isWeekday || isMonthDay || isZoro;

    if (breakdownKey === "dayTail") {
      return isDayTail;
    }
    if (breakdownKey === "weekday") {
      return isWeekday;
    }
    if (breakdownKey === "eventTotal") {
      return isEvent;
    }

    return !isEvent;
  };
}

function calculateAverage(total, count) {
  if (!Number.isFinite(total) || !Number.isInteger(count) || count <= 0) {
    return null;
  }
  return total / count;
}

function calculatePayoutRate(investedCoinsTotal, differenceTotal) {
  if (!Number.isFinite(investedCoinsTotal) || investedCoinsTotal <= 0) {
    return null;
  }

  return ((investedCoinsTotal + differenceTotal) / investedCoinsTotal) * 100;
}

function formatProbability(gamesTotal, hitCount) {
  if (!Number.isFinite(gamesTotal) || gamesTotal <= 0 || !Number.isFinite(hitCount) || hitCount <= 0) {
    return null;
  }

  const probability = gamesTotal / hitCount;
  const roundedProbability = Math.round(probability * 10) / 10;
  const probabilityText = Number.isInteger(roundedProbability)
    ? String(roundedProbability)
    : roundedProbability.toFixed(1);

  return `1/${probabilityText}`;
}

function resolveActualMetrics(machineName, nextRecord, differenceMode) {
  const gamesCount = readFiniteNumber(nextRecord?.games_count);
  const bbCount = readFiniteNumber(nextRecord?.bb_count);
  const rbCount = readFiniteNumber(nextRecord?.rb_count);
  const standardInvestedCoins = gamesCount > 0 ? gamesCount * 3 : 0;

  if (differenceMode === "bonus" || differenceMode === "estimated") {
    const storedBonusDifferenceValue = selectDifferenceValue(nextRecord, differenceMode, machineName);
    return {
      differenceValue: readFiniteNumber(storedBonusDifferenceValue),
      investedCoins: standardInvestedCoins,
      gamesCount,
      bbCount,
      rbCount,
    };
  }

  return {
    differenceValue: readFiniteNumber(selectDifferenceValue(nextRecord, "minrepo")),
    investedCoins: standardInvestedCoins,
    gamesCount,
    bbCount,
    rbCount,
  };
}

function getSettingEstimateBucketKey(definition) {
  return String(definition?.displayName ?? "").trim();
}

function addAggregateSettingMetrics(summary, machineName, actualMetrics, nextRecord, settingEstimateMode) {
  const definition = getSettingEstimateDefinition(machineName);
  const bucketKey = getSettingEstimateBucketKey(definition);
  if (!definition || !bucketKey) {
    return;
  }

  if (!summary.settingEstimateBuckets.has(bucketKey)) {
    summary.settingEstimateBuckets.set(bucketKey, {
      definition,
      gamesTotal: 0,
      bbTotal: 0,
      rbTotal: 0,
      grapeCountTotal: 0,
      grapeGameTotal: 0,
      hasMissingGrapeObservation: false,
    });
  }

  const bucket = summary.settingEstimateBuckets.get(bucketKey);
  bucket.gamesTotal += actualMetrics.gamesCount;
  bucket.bbTotal += actualMetrics.bbCount;
  bucket.rbTotal += actualMetrics.rbCount;

  if (settingEstimateMode === SETTING_ESTIMATE_MODE_GRAPE) {
    const grapeObservation = readGrapeSettingEstimateObservation(definition, nextRecord);
    if (grapeObservation) {
      bucket.grapeCountTotal += grapeObservation.successCount;
      bucket.grapeGameTotal += grapeObservation.totalCount;
    } else {
      bucket.hasMissingGrapeObservation = true;
    }
  }
}

function addSettingEstimateRateMetrics(summary, machineName, nextRecord, settingEstimateMode) {
  const definition = getSettingEstimateDefinition(machineName);
  if (!definition) {
    return;
  }

  const settingEstimate = calculateSettingEstimate(definition, nextRecord, {
    mode: settingEstimateMode,
  });
  const settingAverage = settingEstimate?.average;
  if (!Number.isFinite(settingAverage)) {
    return;
  }

  summary.settingEstimateSampleCount += 1;
  if (settingAverage >= 3.5) {
    summary.setting35PlusCount += 1;
  }
  if (settingAverage >= 4) {
    summary.setting4PlusCount += 1;
  }
  if (settingAverage >= 4.5) {
    summary.setting45PlusCount += 1;
  }
  if (settingAverage >= 5) {
    summary.setting5PlusCount += 1;
  }
}

function calculateAggregateSettingAverage(summary, settingEstimateMode) {
  const estimates = [...summary.settingEstimateBuckets.values()]
    .map((bucket) => {
      const aggregateRecord = {
        games_count: bucket.gamesTotal,
        bb_count: bucket.bbTotal,
        rb_count: bucket.rbTotal,
      };
      if (
        settingEstimateMode === SETTING_ESTIMATE_MODE_GRAPE &&
        !bucket.hasMissingGrapeObservation &&
        bucket.grapeCountTotal > 0 &&
        bucket.grapeGameTotal > 0
      ) {
        aggregateRecord.estimated_grape_count = bucket.grapeCountTotal;
        aggregateRecord.estimated_grape_probability = bucket.grapeCountTotal / bucket.grapeGameTotal;
        aggregateRecord.estimated_grape_denominator = bucket.grapeGameTotal / bucket.grapeCountTotal;
      }

      const estimate = calculateSettingEstimate(bucket.definition, aggregateRecord, {
        mode: settingEstimateMode,
      });
      return estimate?.average !== undefined
        ? {
            average: estimate.average,
            gamesTotal: bucket.gamesTotal,
          }
        : null;
    })
    .filter((estimate) => estimate && Number.isFinite(estimate.average));

  if (estimates.length === 0) {
    return null;
  }
  if (estimates.length === 1) {
    return estimates[0].average;
  }

  const weightedGamesTotal = estimates.reduce(
    (total, estimate) => total + estimate.gamesTotal,
    0,
  );
  if (!Number.isFinite(weightedGamesTotal) || weightedGamesTotal <= 0) {
    return null;
  }

  return estimates.reduce(
    (total, estimate) => total + estimate.average * estimate.gamesTotal,
    0,
  ) / weightedGamesTotal;
}

function calculateGrapeDenominator(summary) {
  if (
    !Number.isFinite(summary.grapeGameTotal) ||
    !Number.isFinite(summary.grapeCountTotal) ||
    summary.grapeGameTotal <= 0 ||
    summary.grapeCountTotal <= 0
  ) {
    return null;
  }
  return summary.grapeGameTotal / summary.grapeCountTotal;
}

function addGrapeMetrics(summary, machineName, nextRecord) {
  const definition = getSettingEstimateDefinition(machineName);
  const grapeObservation = readGrapeSettingEstimateObservation(definition, nextRecord);
  if (!grapeObservation) {
    return;
  }
  summary.grapeCountTotal += grapeObservation.successCount;
  summary.grapeGameTotal += grapeObservation.totalCount;
}

function buildEmptySummary(machineName = "総計") {
  return {
    machineName,
    huntScoreTotal: 0,
    nextGapTotal: 0,
    nextGapSampleCount: 0,
    upperGapTotal: 0,
    upperGapSampleCount: 0,
    averageHuntScore: null,
    averageNextGap: null,
    averageUpperGap: null,
    actualRowCount: 0,
    winCount: 0,
    winRate: null,
    differenceTotal: 0,
    gamesTotal: 0,
    bbTotal: 0,
    rbTotal: 0,
    grapeCountTotal: 0,
    grapeGameTotal: 0,
    grapeDenominator: null,
    payoutRate: null,
    bbProbability: null,
    rbProbability: null,
    combinedProbability: null,
    averageSetting: null,
    settingEstimateBuckets: new Map(),
    settingEstimateSampleCount: 0,
    setting35PlusCount: 0,
    setting4PlusCount: 0,
    setting45PlusCount: 0,
    setting5PlusCount: 0,
    setting35PlusRate: null,
    setting4PlusRate: null,
    setting45PlusRate: null,
    setting5PlusRate: null,
    investedCoinsTotal: 0,
  };
}

function buildEmptyDailySummary(date, predictionDate) {
  return {
    date,
    predictionDate,
    actualRowCount: 0,
    differenceTotal: 0,
  };
}

function finalizeSummary(summary, settingEstimateMode) {
  const { settingEstimateBuckets, ...publicSummary } = summary;
  return {
    ...publicSummary,
    averageHuntScore: calculateAverage(summary.huntScoreTotal, summary.actualRowCount),
    averageNextGap: calculateAverage(summary.nextGapTotal, summary.nextGapSampleCount),
    averageUpperGap: calculateAverage(summary.upperGapTotal, summary.upperGapSampleCount),
    averageGames: calculateAverage(summary.gamesTotal, summary.actualRowCount),
    winRate: calculateAverage(summary.winCount * 100, summary.actualRowCount),
    payoutRate: calculatePayoutRate(summary.investedCoinsTotal, summary.differenceTotal),
    bbProbability: formatProbability(summary.gamesTotal, summary.bbTotal),
    rbProbability: formatProbability(summary.gamesTotal, summary.rbTotal),
    combinedProbability: formatProbability(summary.gamesTotal, summary.bbTotal + summary.rbTotal),
    grapeDenominator: calculateGrapeDenominator(summary),
    averageSetting: calculateAggregateSettingAverage(summary, settingEstimateMode),
    setting35PlusRate: calculateAverage(summary.setting35PlusCount * 100, summary.settingEstimateSampleCount),
    setting4PlusRate: calculateAverage(summary.setting4PlusCount * 100, summary.settingEstimateSampleCount),
    setting45PlusRate: calculateAverage(summary.setting45PlusCount * 100, summary.settingEstimateSampleCount),
    setting5PlusRate: calculateAverage(summary.setting5PlusCount * 100, summary.settingEstimateSampleCount),
  };
}

function addActualMetricsToSummary(
  summary,
  machineName,
  row,
  actualMetrics,
  nextGapValue,
  upperGapValue,
  settingEstimateMode,
  showSettingDistribution,
  huntScoreValue = row?.huntScore,
) {
  summary.actualRowCount += 1;
  if (actualMetrics.differenceValue > 0) {
    summary.winCount += 1;
  }
  summary.huntScoreTotal += readFiniteNumber(huntScoreValue);
  summary.differenceTotal += actualMetrics.differenceValue;
  summary.gamesTotal += actualMetrics.gamesCount;
  summary.bbTotal += actualMetrics.bbCount;
  summary.rbTotal += actualMetrics.rbCount;
  summary.investedCoinsTotal += actualMetrics.investedCoins;
  if (Number.isFinite(nextGapValue)) {
    summary.nextGapTotal += nextGapValue;
    summary.nextGapSampleCount += 1;
  }
  if (Number.isFinite(upperGapValue)) {
    summary.upperGapTotal += upperGapValue;
    summary.upperGapSampleCount += 1;
  }
  addAggregateSettingMetrics(summary, machineName, actualMetrics, row.nextRecord, settingEstimateMode);
  if (showSettingDistribution) {
    addSettingEstimateRateMetrics(summary, machineName, row.nextRecord, settingEstimateMode);
  }
  addGrapeMetrics(summary, machineName, row.nextRecord);
}

function buildSnapshotGapRows(
  snapshot,
  selectedMachineNameSet,
  combineAimJuggler,
  combineHanabi,
  rankFilters = {},
) {
  const rows = Array.isArray(snapshot?.rows) ? snapshot.rows : [];
  const selectedRows = rows.filter((row) =>
    selectedMachineNameSet.has(String(row.machineName ?? "").trim()),
  );
  const selectedNextGapMap = calculateHuntScoreNextGapMap(
    selectedRows,
    rankFilters.selectedRankFilter,
  );
  const selectedUpperGapMap = calculateHuntScoreUpperGapMap(
    selectedRows,
    rankFilters.selectedRankFilter,
  );
  const machineRowsByName = new Map();

  for (const row of selectedRows) {
    const machineName = resolveBacktestMachineName(
      row.machineName,
      combineAimJuggler,
      combineHanabi,
    );
    if (!machineRowsByName.has(machineName)) {
      machineRowsByName.set(machineName, []);
    }
    machineRowsByName.get(machineName).push(row);
  }

  const machineNextGapMap = new Map();
  const machineUpperGapMap = new Map();
  for (const machineRows of machineRowsByName.values()) {
    const nextGapMap = calculateHuntScoreNextGapMap(
      machineRows,
      rankFilters.machineRankFilter,
    );
    const upperGapMap = calculateHuntScoreUpperGapMap(
      machineRows,
      rankFilters.machineRankFilter,
    );
    for (const row of machineRows) {
      if (nextGapMap.has(row)) {
        machineNextGapMap.set(row, nextGapMap.get(row));
      }
      if (upperGapMap.has(row)) {
        machineUpperGapMap.set(row, upperGapMap.get(row));
      }
    }
  }

  return new Map(
    selectedRows.map((row) => [
      row,
      {
        ...row,
        selectedNextGap: selectedNextGapMap.get(row) ?? null,
        machineNextGap: machineNextGapMap.get(row) ?? null,
        selectedUpperGap: selectedUpperGapMap.get(row) ?? null,
        machineUpperGap: machineUpperGapMap.get(row) ?? null,
      },
    ]),
  );
}

function compareMachineEvaluationConditionRows(left, right) {
  const leftScore = readNullableFiniteNumber(left?.row?.machineEvaluation?.score);
  const rightScore = readNullableFiniteNumber(right?.row?.machineEvaluation?.score);
  const scoreDiff = (rightScore ?? Number.NEGATIVE_INFINITY) -
    (leftScore ?? Number.NEGATIVE_INFINITY);
  if (Math.abs(scoreDiff) > 0.000000001) {
    return scoreDiff;
  }

  return (
    String(left?.machineName ?? "").localeCompare(String(right?.machineName ?? ""), "ja") ||
    String(left?.row?.slotNumber ?? "").localeCompare(String(right?.row?.slotNumber ?? ""), "ja", {
      numeric: true,
    })
  );
}

function buildMachineEvaluationRankContexts(conditionRows) {
  const sortedRows = [...conditionRows].sort(compareMachineEvaluationConditionRows);
  const contextByRow = new Map();

  sortedRows.forEach((entry, index) => {
    const previousEntry = sortedRows[index - 1] ?? null;
    const nextEntry = sortedRows[index + 1] ?? null;
    const score = readNullableFiniteNumber(entry?.row?.machineEvaluation?.score);
    const previousScore = readNullableFiniteNumber(previousEntry?.row?.machineEvaluation?.score);
    const nextScore = readNullableFiniteNumber(nextEntry?.row?.machineEvaluation?.score);

    contextByRow.set(entry.row, {
      rank: index + 1,
      upperGap: score !== null && previousScore !== null ? previousScore - score : null,
      nextGap: score !== null && nextScore !== null ? score - nextScore : null,
    });
  });

  return contextByRow;
}

function buildSnapshotMachineEvaluationRows(
  snapshot,
  selectedMachineNameSet,
  combineAimJuggler,
  combineHanabi,
) {
  const rows = Array.isArray(snapshot?.rows) ? snapshot.rows : [];
  const selectedEntries = [];
  const entriesByMachineName = new Map();

  for (const row of rows) {
    const selectedMachineName = String(row?.machineName ?? "").trim();
    if (!selectedMachineNameSet.has(selectedMachineName) || !row?.machineEvaluation) {
      continue;
    }

    const machineName = resolveBacktestMachineName(
      selectedMachineName,
      combineAimJuggler,
      combineHanabi,
    );
    const entry = { row, machineName };
    selectedEntries.push(entry);
    if (!entriesByMachineName.has(machineName)) {
      entriesByMachineName.set(machineName, []);
    }
    entriesByMachineName.get(machineName).push(entry);
  }

  const selectedContexts = buildMachineEvaluationRankContexts(selectedEntries);
  const machineContexts = new Map();
  for (const machineEntries of entriesByMachineName.values()) {
    const contextByRow = buildMachineEvaluationRankContexts(machineEntries);
    for (const [row, context] of contextByRow.entries()) {
      machineContexts.set(row, context);
    }
  }

  return new Map(
    selectedEntries.map(({ row }) => {
      const machineContext = machineContexts.get(row) ?? {};
      const selectedContext = selectedContexts.get(row) ?? {};
      return [
        row,
        {
          machineEvaluationRank: machineContext.rank ?? null,
          machineEvaluationUpperGapValue: machineContext.upperGap ?? null,
          machineEvaluationNextGapValue: machineContext.nextGap ?? null,
          selectedMachineEvaluationRank: selectedContext.rank ?? null,
          selectedMachineEvaluationUpperGapValue: selectedContext.upperGap ?? null,
          selectedMachineEvaluationNextGapValue: selectedContext.nextGap ?? null,
        },
      ];
    }),
  );
}

function buildSnapshotConditionRowKey(row) {
  return String(row?.rowKey ?? "").trim();
}

function buildSnapshotConditionRows(
  snapshot,
  selectedMachineNameSet,
  combineAimJuggler,
  combineHanabi,
  rankFilters = {},
  selectionMode = "",
) {
  const machineRankCounts = new Map();
  const gapRowsByRow = buildSnapshotGapRows(
    snapshot,
    selectedMachineNameSet,
    combineAimJuggler,
    combineHanabi,
    rankFilters,
  );
  const selectionRowsByRow = isMachineTopNextGapSelectionMode(selectionMode)
    ? buildSnapshotGapRows(
        snapshot,
        selectedMachineNameSet,
        combineAimJuggler,
        combineHanabi,
        { machineRankFilter: buildRankFilter(1, 1) },
      )
    : null;
  const selectedRowSet = selectionRowsByRow
    ? buildMachineTopNextGapSelectionRowSet(
        snapshot,
        selectedMachineNameSet,
        combineAimJuggler,
        combineHanabi,
        selectionRowsByRow,
      )
    : null;
  const rowsByKey = new Map();
  let selectedRank = 0;

  for (const row of Array.isArray(snapshot?.rows) ? snapshot.rows : []) {
    const selectedMachineName = String(row.machineName ?? "").trim();
    if (!selectedMachineNameSet.has(selectedMachineName)) {
      continue;
    }

    const rowKey = buildSnapshotConditionRowKey(row);
    if (!rowKey) {
      continue;
    }

    selectedRank += 1;
    const backtestMachineName = resolveBacktestMachineName(
      selectedMachineName,
      combineAimJuggler,
      combineHanabi,
    );
    const machineRank = (machineRankCounts.get(backtestMachineName) ?? 0) + 1;
    machineRankCounts.set(backtestMachineName, machineRank);

    const gapRow = gapRowsByRow.get(row) ?? row;
    rowsByKey.set(rowKey, {
      row,
      selectedRank,
      machineRank,
      huntScore: row.huntScore,
      machineNextGapValue: readNextGapForRankScope(gapRow, "machine"),
      selectedNextGapValue: readNextGapForRankScope(gapRow, "selected"),
      machineUpperGapValue: readUpperGapForRankScope(gapRow, "machine"),
      selectedUpperGapValue: readUpperGapForRankScope(gapRow, "selected"),
      matchesSelectionMode: !selectedRowSet || selectedRowSet.has(row),
    });
  }

  return rowsByKey;
}

function buildLogicConditionContexts(
  snapshot,
  selectedMachineNameSet,
  combineAimJuggler,
  combineHanabi,
  rankFilters = {},
  selectionMode = "",
) {
  const logicSnapshots = Array.isArray(snapshot?.huntScoreLogicSnapshots)
    ? snapshot.huntScoreLogicSnapshots
    : [];
  if (logicSnapshots.length <= 1) {
    return [];
  }

  return logicSnapshots.map((logicSnapshot) => ({
    key: String(logicSnapshot?.key ?? "").trim(),
    name: String(logicSnapshot?.name ?? "").trim(),
    rowsByKey: buildSnapshotConditionRows(
      {
        baseDate: snapshot.baseDate,
        nextBusinessDate: snapshot.nextBusinessDate,
        rows: Array.isArray(logicSnapshot?.rows) ? logicSnapshot.rows : [],
      },
      selectedMachineNameSet,
      combineAimJuggler,
      combineHanabi,
      rankFilters,
      selectionMode,
    ),
  }));
}

function matchesConditionForRowContext(
  rowContext,
  {
    machineRankFilter,
    selectedRankFilter,
    scoreFilter,
    machineEvaluationScoreFilter,
    machineEvaluationRankFilter,
    selectedMachineEvaluationRankFilter,
    machineNextGapFilter,
    selectedNextGapFilter,
    machineUpperGapFilter,
    selectedUpperGapFilter,
    machineEvaluationNextGapFilter,
    selectedMachineEvaluationNextGapFilter,
    machineEvaluationUpperGapFilter,
    selectedMachineEvaluationUpperGapFilter,
    requirementOptions,
  },
) {
  if (!rowContext?.matchesSelectionMode) {
    return false;
  }

  return matchesRequiredConditionFilters(
    [
      {
        rankValue: rowContext.machineRank,
        rankFilter: machineRankFilter,
        required: requirementOptions.machineRankRequired,
      },
      {
        rankValue: rowContext.selectedRank,
        rankFilter: selectedRankFilter,
        required: requirementOptions.selectedRankRequired,
      },
    ],
    rowContext.huntScore,
    null,
    scoreFilter,
    requirementOptions,
    true,
    null,
    { hasNextGapFilter: false, nextGapMin: null, nextGapMax: null },
    null,
    { hasUpperGapFilter: false, upperGapMin: null, upperGapMax: null },
    [
      {
        value: rowContext.machineUpperGapValue,
        filter: machineUpperGapFilter,
        required: requirementOptions.machineUpperGapRequired,
      },
      {
        value: rowContext.machineNextGapValue,
        filter: machineNextGapFilter,
        required: requirementOptions.machineNextGapRequired,
      },
      {
        value: rowContext.selectedUpperGapValue,
        filter: selectedUpperGapFilter,
        required: requirementOptions.selectedUpperGapRequired,
      },
      {
        value: rowContext.selectedNextGapValue,
        filter: selectedNextGapFilter,
        required: requirementOptions.selectedNextGapRequired,
      },
    ],
  );
}

function matchesAllLogicConditions(row, logicConditionContexts, conditionOptions) {
  const rowKey = buildSnapshotConditionRowKey(row);
  if (!rowKey || logicConditionContexts.length === 0) {
    return false;
  }

  return logicConditionContexts.every((logicContext) => {
    const rowContext = logicContext.rowsByKey.get(rowKey);
    return matchesConditionForRowContext(rowContext, conditionOptions);
  });
}

function hasMachineEvaluationConditionFilters({
  machineEvaluationScoreFilter,
  machineEvaluationRankFilter,
  selectedMachineEvaluationRankFilter,
  machineEvaluationNextGapFilter,
  selectedMachineEvaluationNextGapFilter,
  machineEvaluationUpperGapFilter,
  selectedMachineEvaluationUpperGapFilter,
} = {}) {
  return Boolean(
    machineEvaluationScoreFilter?.hasScoreFilter ||
      machineEvaluationRankFilter?.hasRankFilter ||
      selectedMachineEvaluationRankFilter?.hasRankFilter ||
      machineEvaluationNextGapFilter?.hasNextGapFilter ||
      selectedMachineEvaluationNextGapFilter?.hasNextGapFilter ||
      machineEvaluationUpperGapFilter?.hasUpperGapFilter ||
      selectedMachineEvaluationUpperGapFilter?.hasUpperGapFilter,
  );
}

function resolveMachineEvaluationFilterMatch(row, machineEvaluationContext, conditionOptions) {
  if (!hasMachineEvaluationConditionFilters(conditionOptions)) {
    return true;
  }

  return matchesRequiredConditionFilters(
    [
      {
        rankValue: machineEvaluationContext?.machineEvaluationRank,
        rankFilter: conditionOptions.machineEvaluationRankFilter,
        required: conditionOptions.requirementOptions.machineEvaluationRankRequired,
      },
      {
        rankValue: machineEvaluationContext?.selectedMachineEvaluationRank,
        rankFilter: conditionOptions.selectedMachineEvaluationRankFilter,
        required: conditionOptions.requirementOptions.selectedMachineEvaluationRankRequired,
      },
    ],
    row?.machineEvaluation?.score,
    null,
    conditionOptions.machineEvaluationScoreFilter,
    {
      scoreRequired: conditionOptions.requirementOptions.machineEvaluationScoreRequired,
    },
    false,
    null,
    { hasNextGapFilter: false, nextGapMin: null, nextGapMax: null },
    null,
    { hasUpperGapFilter: false, upperGapMin: null, upperGapMax: null },
    [
      {
        value: machineEvaluationContext?.machineEvaluationUpperGapValue,
        filter: conditionOptions.machineEvaluationUpperGapFilter,
        required: conditionOptions.requirementOptions.machineEvaluationUpperGapRequired,
      },
      {
        value: machineEvaluationContext?.machineEvaluationNextGapValue,
        filter: conditionOptions.machineEvaluationNextGapFilter,
        required: conditionOptions.requirementOptions.machineEvaluationNextGapRequired,
      },
      {
        value: machineEvaluationContext?.selectedMachineEvaluationUpperGapValue,
        filter: conditionOptions.selectedMachineEvaluationUpperGapFilter,
        required: conditionOptions.requirementOptions.selectedMachineEvaluationUpperGapRequired,
      },
      {
        value: machineEvaluationContext?.selectedMachineEvaluationNextGapValue,
        filter: conditionOptions.selectedMachineEvaluationNextGapFilter,
        required: conditionOptions.requirementOptions.selectedMachineEvaluationNextGapRequired,
      },
    ],
  );
}

function resolveMachineEvaluationAdoptionMatch(row) {
  return Boolean(row?.machineEvaluation?.matchesAdoption);
}

function combineBacktestConditionMatches(inputMatchesCondition, adoptionMatchesCondition, mode) {
  const normalizedMode = normalizeMachineEvaluationBacktestMode(mode);
  if (normalizedMode === MACHINE_EVALUATION_BACKTEST_MODE_MACHINE) {
    return adoptionMatchesCondition;
  }
  if (normalizedMode === MACHINE_EVALUATION_BACKTEST_MODE_AND) {
    return inputMatchesCondition && adoptionMatchesCondition;
  }
  if (normalizedMode === MACHINE_EVALUATION_BACKTEST_MODE_OR) {
    return inputMatchesCondition || adoptionMatchesCondition;
  }
  return inputMatchesCondition;
}

function compareSelectionCandidates(left, right) {
  const nextGapDiff = right.nextGapValue - left.nextGapValue;
  if (Math.abs(nextGapDiff) > 0.000000001) {
    return nextGapDiff;
  }

  const scoreDiff = readFiniteNumber(right.huntScore) - readFiniteNumber(left.huntScore);
  if (Math.abs(scoreDiff) > 0.000000001) {
    return scoreDiff;
  }

  const leftRank = readPositiveInteger(left.rank) ?? Number.MAX_SAFE_INTEGER;
  const rightRank = readPositiveInteger(right.rank) ?? Number.MAX_SAFE_INTEGER;
  return (
    leftRank - rightRank ||
    String(left.machineName ?? "").localeCompare(String(right.machineName ?? ""), "ja") ||
    String(left.slotNumber ?? "").localeCompare(String(right.slotNumber ?? ""), "ja", {
      numeric: true,
    })
  );
}

function buildMachineTopNextGapSelectionRowSet(
  snapshot,
  selectedMachineNameSet,
  combineAimJuggler,
  combineHanabi,
  selectionRowsByRow,
) {
  const seenMachineNames = new Set();
  const candidates = [];

  for (const row of Array.isArray(snapshot?.rows) ? snapshot.rows : []) {
    const selectedMachineName = String(row.machineName ?? "").trim();
    if (!selectedMachineNameSet.has(selectedMachineName)) {
      continue;
    }

    const backtestMachineName = resolveBacktestMachineName(
      selectedMachineName,
      combineAimJuggler,
      combineHanabi,
    );
    if (seenMachineNames.has(backtestMachineName)) {
      continue;
    }
    seenMachineNames.add(backtestMachineName);

    const selectionRow = selectionRowsByRow.get(row) ?? row;
    const nextGapValue = readNextGapForRankScope(selectionRow, "machine");
    if (!Number.isFinite(nextGapValue)) {
      continue;
    }

    candidates.push({
      row,
      nextGapValue,
      huntScore: row.huntScore,
      rank: row.rank,
      machineName: backtestMachineName,
      slotNumber: row.slotNumber,
    });
  }

  candidates.sort(compareSelectionCandidates);
  return new Set(candidates[0]?.row ? [candidates[0].row] : []);
}

function buildBacktestAggregationDetail(
  snapshotsInPeriod,
  {
    selectedMachineNames,
    selectedMachineNameSet,
    machineRankFilter,
    selectedRankFilter,
    scoreFilter,
    machineEvaluationScoreFilter,
    machineEvaluationRankFilter,
    selectedMachineEvaluationRankFilter,
    machineEvaluationNextGapFilter,
    selectedMachineEvaluationNextGapFilter,
    machineEvaluationUpperGapFilter,
    selectedMachineEvaluationUpperGapFilter,
    machineNextGapFilter,
    selectedNextGapFilter,
    machineUpperGapFilter,
    selectedUpperGapFilter,
    requirementOptions,
    nextGapRankFilters,
    differenceMode,
    combineAimJuggler,
    combineHanabi,
    machineSlotCountLookup,
    gapRowsCache,
    selectionMode,
    selectionRowsCache,
    settingEstimateMode,
    showSettingDistribution,
    logicConditionMode = LOGIC_CONDITION_MODE_SUM,
    machineEvaluationBacktestMode = MACHINE_EVALUATION_BACKTEST_MODE_COMMON,
    rowFilter = () => true,
  },
) {
  const summariesByMachine = new Map();
  const nonmatchingSummariesByMachine = new Map();
  const dailySummariesByDate = new Map();
  const totalSummary = buildEmptySummary();
  const nonmatchingTotalSummary = buildEmptySummary();
  const actualDates = new Set();
  let actualRowCount = 0;
  const usesLogicAndConditions = logicConditionMode === LOGIC_CONDITION_MODE_AND;
  const usesMachineEvaluationOnly =
    normalizeMachineEvaluationBacktestMode(machineEvaluationBacktestMode) ===
    MACHINE_EVALUATION_BACKTEST_MODE_MACHINE;

  for (const snapshot of snapshotsInPeriod) {
    const machineRankCounts = new Map();
    let gapRowsByRow = gapRowsCache?.get(snapshot);
    if (!gapRowsByRow) {
      gapRowsByRow = buildSnapshotGapRows(
        snapshot,
        selectedMachineNameSet,
        combineAimJuggler,
        combineHanabi,
        nextGapRankFilters,
      );
      gapRowsCache?.set(snapshot, gapRowsByRow);
    }
    const selectionRowsByRow =
      isMachineTopNextGapSelectionMode(selectionMode)
        ? selectionRowsCache?.get(snapshot) ??
          buildSnapshotGapRows(
            snapshot,
            selectedMachineNameSet,
            combineAimJuggler,
            combineHanabi,
            { machineRankFilter: buildRankFilter(1, 1) },
          )
        : null;
    if (selectionRowsByRow && !selectionRowsCache?.has(snapshot)) {
      selectionRowsCache?.set(snapshot, selectionRowsByRow);
    }
    const selectedRowSet = selectionRowsByRow
      ? buildMachineTopNextGapSelectionRowSet(
          snapshot,
          selectedMachineNameSet,
          combineAimJuggler,
          combineHanabi,
          selectionRowsByRow,
        )
      : null;
    const logicConditionContexts = usesLogicAndConditions
      ? buildLogicConditionContexts(
          snapshot,
          selectedMachineNameSet,
          combineAimJuggler,
          combineHanabi,
          nextGapRankFilters,
          selectionMode,
        )
      : [];
    const machineEvaluationRowsByRow = buildSnapshotMachineEvaluationRows(
      snapshot,
      selectedMachineNameSet,
      combineAimJuggler,
      combineHanabi,
    );
    const conditionOptions = {
      machineRankFilter,
      selectedRankFilter,
      scoreFilter,
      machineEvaluationScoreFilter,
      machineEvaluationRankFilter,
      selectedMachineEvaluationRankFilter,
      machineNextGapFilter,
      selectedNextGapFilter,
      machineUpperGapFilter,
      selectedUpperGapFilter,
      machineEvaluationNextGapFilter,
      selectedMachineEvaluationNextGapFilter,
      machineEvaluationUpperGapFilter,
      selectedMachineEvaluationUpperGapFilter,
      requirementOptions,
    };
    let selectedRank = 0;

    for (const row of snapshot.rows) {
      const selectedMachineName = String(row.machineName ?? "").trim();
      if (!selectedMachineNameSet.has(selectedMachineName)) {
        continue;
      }

      selectedRank += 1;
      const backtestMachineName = resolveBacktestMachineName(
        selectedMachineName,
        combineAimJuggler,
        combineHanabi,
      );
      const machineRank = (machineRankCounts.get(backtestMachineName) ?? 0) + 1;
      machineRankCounts.set(backtestMachineName, machineRank);
      const gapRow = gapRowsByRow.get(row) ?? row;
      const machineNextGapValue = readNextGapForRankScope(gapRow, "machine");
      const selectedNextGapValue = readNextGapForRankScope(gapRow, "selected");
      const machineUpperGapValue = readUpperGapForRankScope(gapRow, "machine");
      const selectedUpperGapValue = readUpperGapForRankScope(gapRow, "selected");

      const commonMatchesCondition = usesLogicAndConditions && logicConditionContexts.length > 1
        ? matchesAllLogicConditions(row, logicConditionContexts, conditionOptions)
        : matchesConditionForRowContext(
            {
              row,
              selectedRank,
              machineRank,
              huntScore: row.huntScore,
              machineNextGapValue,
              selectedNextGapValue,
              machineUpperGapValue,
              selectedUpperGapValue,
              matchesSelectionMode: !selectedRowSet || selectedRowSet.has(row),
            },
            conditionOptions,
          );
      const machineEvaluationContext = machineEvaluationRowsByRow.get(row) ?? null;
      const inputMatchesCondition =
        commonMatchesCondition &&
        resolveMachineEvaluationFilterMatch(row, machineEvaluationContext, conditionOptions);
      const matchesCondition = combineBacktestConditionMatches(
        inputMatchesCondition,
        resolveMachineEvaluationAdoptionMatch(row),
        machineEvaluationBacktestMode,
      );

      const actualDate = getActualDate(row, snapshot);
      if (!rowFilter({ snapshot, row, actualDate })) {
        continue;
      }

      if (!row.nextRecord) {
        continue;
      }

      const actualMetrics = resolveActualMetrics(row.machineName, row.nextRecord, differenceMode);
      const targetSummariesByMachine = matchesCondition
        ? summariesByMachine
        : nonmatchingSummariesByMachine;
      const targetTotalSummary = matchesCondition ? totalSummary : nonmatchingTotalSummary;

      if (!targetSummariesByMachine.has(backtestMachineName)) {
        targetSummariesByMachine.set(backtestMachineName, buildEmptySummary(backtestMachineName));
      }

      const summary = targetSummariesByMachine.get(backtestMachineName);
      const summaryHuntScoreValue = usesMachineEvaluationOnly
        ? row.machineEvaluation?.score
        : row.huntScore;
      const summaryNextGapValue = usesMachineEvaluationOnly
        ? (machineEvaluationRowsByRow.get(row)?.machineEvaluationNextGapValue ??
          row.machineEvaluation?.nextGap)
        : machineNextGapValue;
      const summaryUpperGapValue = usesMachineEvaluationOnly
        ? machineEvaluationRowsByRow.get(row)?.machineEvaluationUpperGapValue
        : machineUpperGapValue;
      addActualMetricsToSummary(
        summary,
        row.machineName,
        row,
        actualMetrics,
        summaryNextGapValue,
        summaryUpperGapValue,
        settingEstimateMode,
        showSettingDistribution,
        summaryHuntScoreValue,
      );
      addActualMetricsToSummary(
        targetTotalSummary,
        row.machineName,
        row,
        actualMetrics,
        summaryNextGapValue,
        summaryUpperGapValue,
        settingEstimateMode,
        showSettingDistribution,
        summaryHuntScoreValue,
      );

      if (!matchesCondition) {
        continue;
      }

      actualRowCount += 1;
      if (actualDate) {
        actualDates.add(actualDate);
      }

      if (actualDate) {
        if (!dailySummariesByDate.has(actualDate)) {
          dailySummariesByDate.set(
            actualDate,
            buildEmptyDailySummary(actualDate, snapshot.baseDate),
          );
        }
        const dailySummary = dailySummariesByDate.get(actualDate);
        dailySummary.actualRowCount += 1;
        dailySummary.differenceTotal += actualMetrics.differenceValue;
      }
    }
  }

  const summaryMachineNames = [
    ...new Set(
      selectedMachineNames.map((machineName) =>
        resolveBacktestMachineName(machineName, combineAimJuggler, combineHanabi),
      ),
    ),
  ];
  const machineOrder = new Map(summaryMachineNames.map((machineName, index) => [machineName, index]));
  const summaries = [...summariesByMachine.values()]
    .map((summary) => {
      const nonmatchingSummary = nonmatchingSummariesByMachine.get(summary.machineName);
      return addSummarySlotCount(
        {
          ...finalizeSummary(summary, settingEstimateMode),
          nonmatchingSummary: nonmatchingSummary
            ? finalizeSummary(nonmatchingSummary, settingEstimateMode)
            : null,
        },
        machineSlotCountLookup,
      );
    })
    .sort((left, right) => {
      return (
        (machineOrder.get(left.machineName) ?? Number.MAX_SAFE_INTEGER) -
          (machineOrder.get(right.machineName) ?? Number.MAX_SAFE_INTEGER) ||
        left.machineName.localeCompare(right.machineName, "ja")
      );
    });
  const graphPoints = [...dailySummariesByDate.values()]
    .filter((dailySummary) => dailySummary.actualRowCount > 0)
    .sort((left, right) => left.date.localeCompare(right.date, "ja"));

  return {
    targetDateCount: snapshotsInPeriod.length,
    matchedDateCount: actualDates.size,
    actualRowCount,
    hasMatches: actualRowCount > 0,
    hasActualResults: actualRowCount > 0,
    summaries,
    graphPoints,
    total: {
      ...finalizeSummary(totalSummary, settingEstimateMode),
      nonmatchingSummary: finalizeSummary(nonmatchingTotalSummary, settingEstimateMode),
      slotCount: calculateMachineSlotCountTotal(summaryMachineNames, machineSlotCountLookup),
    },
  };
}

export function buildHuntScoreBacktestDetail(snapshots, options = {}) {
  const rankingDates = Array.isArray(snapshots) ? snapshots.map((snapshot) => snapshot.baseDate) : [];
  const latestDate = rankingDates[0] ?? null;
  const earliestDate = rankingDates.at(-1) ?? null;
  const machineSelectionTouched = normalizeMachineSelectionTouched(options.machineTouched);
  const requestedCombineAimJuggler = normalizeCombineAimJuggler(
    options.combineAimJuggler,
    options.machineNames,
    machineSelectionTouched,
  );
  const requestedCombineHanabi = normalizeCombineHanabi(
    options.combineHanabi,
    options.machineNames,
    machineSelectionTouched,
  );
  const availableMachineNames = buildAvailableMachineNames(
    Array.isArray(snapshots) ? snapshots : [],
    Array.isArray(options.machineOrder) ? options.machineOrder : undefined,
  );
  const hasAimJugglerGroupOption = AIM_JUGGLER_MACHINE_NAMES.some((machineName) =>
    availableMachineNames.includes(machineName),
  );
  const hasHanabiGroupOption = HANABI_MACHINE_NAMES.every((machineName) =>
    availableMachineNames.includes(machineName),
  );
  const combineAimJuggler = hasAimJugglerGroupOption ? requestedCombineAimJuggler : false;
  const combineHanabi = hasHanabiGroupOption ? requestedCombineHanabi : false;
  const machineSlotCountLookup = buildMachineSlotCountLookup(options.machineSlotCounts);
  const selectedMachineNames = buildSelectedMachineNames(
    expandRequestedMachineNamesForCombine(
      options.machineNames,
      requestedCombineAimJuggler,
      requestedCombineHanabi,
    ),
    availableMachineNames,
    !machineSelectionTouched,
  );
  const selectedMachineNameSet = new Set(selectedMachineNames);
  const {
    rankScope,
    rankFilter,
    machineRankFilter,
    selectedRankFilter,
    hasRankFilter,
  } = buildScopedRankFilters(options);
  const {
    machineNextGapFilter,
    selectedNextGapFilter,
    machineUpperGapFilter,
    selectedUpperGapFilter,
  } = buildBoundaryGapFilters(options);
  const baseRequirementOptions = buildConditionRequirementOptions(options);
  const selectionMode = normalizeDailySelectionMode(options.dailySelectionMode);
  const usesMachineTopNextGapSelection = isMachineTopNextGapSelectionMode(selectionMode);
  const scoreDifferenceMode = normalizeDifferenceMode(options.scoreDifferenceMode);
  const differenceMode = normalizeDifferenceMode(options.differenceMode);
  const settingEstimateMode = normalizeSettingEstimateMode(options.settingEstimateMode);
  const settingDistribution = normalizeSettingDistribution(options.settingDistribution);
  const showSettingDistribution = shouldShowSettingDistribution(settingDistribution);
  const machineEvaluationBacktestMode = normalizeMachineEvaluationBacktestMode(options.machineEvaluationMode);
  const beamHikariNeoSpatialSelectionEnabled =
    normalizeBeamHikariNeoSpatialSelectionEnabled(
      options.beamHikariNeoSpatialSelection,
    );
  const showGrapeColumn = selectedMachineNames.some(isHuntJugglerMachine);
  const eventFilters = buildBacktestEventFilters(options);
  const huntScoreLogics = Array.isArray(options.huntScoreLogics) ? options.huntScoreLogics : [];
  const huntScoreLogicKeys = huntScoreLogics.length > 0
    ? huntScoreLogics.map((logic) => String(logic?.key ?? "").trim()).filter(Boolean)
    : splitOptionValues(options.huntScoreLogicKeys);
  const logicConditionMode = normalizeLogicConditionMode(options.logicConditionMode);
  const scoreMaxLimit = calculateBacktestScoreFilterMax(
    logicConditionMode,
    Math.max(1, huntScoreLogicKeys.length),
  );
  const scoreFilter = buildScoreFilter(options.scoreMin, options.scoreMax, scoreMaxLimit);
  const {
    machineEvaluationScoreFilter,
    machineEvaluationRankFilter,
    selectedMachineEvaluationRankFilter,
    machineEvaluationNextGapFilter,
    selectedMachineEvaluationNextGapFilter,
    machineEvaluationUpperGapFilter,
    selectedMachineEvaluationUpperGapFilter,
  } = buildMachineEvaluationConditionFilters(options);
  const requirementOptions = usesMachineTopNextGapSelection
    ? requireActiveConditionFilters(baseRequirementOptions, {
        machineRankFilter,
        selectedRankFilter,
        scoreFilter,
        machineNextGapFilter,
        selectedNextGapFilter,
        machineUpperGapFilter,
        selectedUpperGapFilter,
        machineEvaluationScoreFilter,
        machineEvaluationRankFilter,
        selectedMachineEvaluationRankFilter,
        machineEvaluationNextGapFilter,
        selectedMachineEvaluationNextGapFilter,
        machineEvaluationUpperGapFilter,
        selectedMachineEvaluationUpperGapFilter,
      })
    : baseRequirementOptions;
  const nextGapRankFilters = usesMachineTopNextGapSelection
    ? {}
    : {
        machineRankFilter,
        selectedRankFilter,
      };
  const periodState = buildPeriodState(options, latestDate);
  const snapshotsInPeriod = (Array.isArray(snapshots) ? snapshots : []).filter((snapshot) =>
    isSnapshotInPeriod(snapshot, periodState.startDate, periodState.endDate),
  );
  const gapRowsCache = new Map();
  const selectionRowsCache = new Map();
  const aggregationOptions = {
    selectedMachineNames,
    selectedMachineNameSet,
    machineRankFilter,
    selectedRankFilter,
    scoreFilter,
    machineEvaluationScoreFilter,
    machineEvaluationRankFilter,
    selectedMachineEvaluationRankFilter,
    machineNextGapFilter,
    selectedNextGapFilter,
    machineUpperGapFilter,
    selectedUpperGapFilter,
    machineEvaluationNextGapFilter,
    selectedMachineEvaluationNextGapFilter,
    machineEvaluationUpperGapFilter,
    selectedMachineEvaluationUpperGapFilter,
    requirementOptions,
    nextGapRankFilters,
    differenceMode,
    combineAimJuggler,
    combineHanabi,
    machineSlotCountLookup,
    gapRowsCache,
    selectionMode,
    selectionRowsCache,
    settingEstimateMode,
    showSettingDistribution,
    logicConditionMode,
    machineEvaluationBacktestMode,
  };
  const allAggregation = buildBacktestAggregationDetail(snapshotsInPeriod, aggregationOptions);
  const breakdowns = BACKTEST_BREAKDOWN_DEFINITIONS.map((definition) => ({
    ...definition,
    ...(definition.key === "all"
      ? allAggregation
      : buildBacktestAggregationDetail(snapshotsInPeriod, {
          ...aggregationOptions,
          rowFilter: buildBreakdownRowFilter(definition.key, eventFilters),
        })),
  }));

  return {
    periodMode: periodState.periodMode,
    recentDays: periodState.recentDays,
    startDate: periodState.startDate,
    endDate: periodState.endDate,
    latestDate,
    earliestDate,
    usedFallbackRange: periodState.usedFallbackRange,
    huntScoreLogicKeys,
    huntScoreLogics,
    usesCombinedHuntScoreLogic: huntScoreLogicKeys.length > 1,
    logicConditionMode,
    machineEvaluationBacktestMode,
    beamHikariNeoSpatialSelectionEnabled,
    machineOptions: availableMachineNames.map((machineName) => ({
      name: machineName,
      checked: selectedMachineNameSet.has(machineName),
      slotCount: readMachineSlotCount(machineSlotCountLookup, machineName),
    })),
    selectedMachineNames,
    rankMin: rankFilter.rankMin,
    rankMax: rankFilter.rankMax,
    machineRankMin: machineRankFilter.rankMin,
    machineRankMax: machineRankFilter.rankMax,
    selectedRankMin: selectedRankFilter.rankMin,
    selectedRankMax: selectedRankFilter.rankMax,
    hasRankFilter,
    hasMachineRankFilter: machineRankFilter.hasRankFilter,
    hasSelectedRankFilter: selectedRankFilter.hasRankFilter,
    scoreMin: scoreFilter.scoreMin,
    scoreMax: scoreFilter.scoreMax,
    scoreMaxLimit,
    hasScoreFilter: scoreFilter.hasScoreFilter,
    machineEvaluationScoreMin: machineEvaluationScoreFilter.scoreMin,
    machineEvaluationScoreMax: machineEvaluationScoreFilter.scoreMax,
    hasMachineEvaluationScoreFilter: machineEvaluationScoreFilter.hasScoreFilter,
    machineEvaluationRankMin: machineEvaluationRankFilter.rankMin,
    machineEvaluationRankMax: machineEvaluationRankFilter.rankMax,
    hasMachineEvaluationRankFilter: machineEvaluationRankFilter.hasRankFilter,
    selectedMachineEvaluationRankMin: selectedMachineEvaluationRankFilter.rankMin,
    selectedMachineEvaluationRankMax: selectedMachineEvaluationRankFilter.rankMax,
    hasSelectedMachineEvaluationRankFilter: selectedMachineEvaluationRankFilter.hasRankFilter,
    machineEvaluationNextGapMin: machineEvaluationNextGapFilter.nextGapMin,
    machineEvaluationNextGapMax: machineEvaluationNextGapFilter.nextGapMax,
    hasMachineEvaluationNextGapFilter: machineEvaluationNextGapFilter.hasNextGapFilter,
    selectedMachineEvaluationNextGapMin: selectedMachineEvaluationNextGapFilter.nextGapMin,
    selectedMachineEvaluationNextGapMax: selectedMachineEvaluationNextGapFilter.nextGapMax,
    hasSelectedMachineEvaluationNextGapFilter:
      selectedMachineEvaluationNextGapFilter.hasNextGapFilter,
    machineEvaluationUpperGapMin: machineEvaluationUpperGapFilter.upperGapMin,
    machineEvaluationUpperGapMax: machineEvaluationUpperGapFilter.upperGapMax,
    hasMachineEvaluationUpperGapFilter: machineEvaluationUpperGapFilter.hasUpperGapFilter,
    selectedMachineEvaluationUpperGapMin: selectedMachineEvaluationUpperGapFilter.upperGapMin,
    selectedMachineEvaluationUpperGapMax: selectedMachineEvaluationUpperGapFilter.upperGapMax,
    hasSelectedMachineEvaluationUpperGapFilter:
      selectedMachineEvaluationUpperGapFilter.hasUpperGapFilter,
    machineNextGapMin: machineNextGapFilter.nextGapMin,
    machineNextGapMax: machineNextGapFilter.nextGapMax,
    hasMachineNextGapFilter: machineNextGapFilter.hasNextGapFilter,
    selectedNextGapMin: selectedNextGapFilter.nextGapMin,
    selectedNextGapMax: selectedNextGapFilter.nextGapMax,
    hasSelectedNextGapFilter: selectedNextGapFilter.hasNextGapFilter,
    machineUpperGapMin: machineUpperGapFilter.upperGapMin,
    machineUpperGapMax: machineUpperGapFilter.upperGapMax,
    hasMachineUpperGapFilter: machineUpperGapFilter.hasUpperGapFilter,
    selectedUpperGapMin: selectedUpperGapFilter.upperGapMin,
    selectedUpperGapMax: selectedUpperGapFilter.upperGapMax,
    hasSelectedUpperGapFilter: selectedUpperGapFilter.hasUpperGapFilter,
    rankRequired: requirementOptions.rankRequired,
    machineRankRequired: requirementOptions.machineRankRequired,
    selectedRankRequired: requirementOptions.selectedRankRequired,
    scoreRequired: requirementOptions.scoreRequired,
    machineEvaluationScoreRequired: requirementOptions.machineEvaluationScoreRequired,
    machineEvaluationRankRequired: requirementOptions.machineEvaluationRankRequired,
    selectedMachineEvaluationRankRequired:
      requirementOptions.selectedMachineEvaluationRankRequired,
    machineEvaluationNextGapRequired: requirementOptions.machineEvaluationNextGapRequired,
    selectedMachineEvaluationNextGapRequired:
      requirementOptions.selectedMachineEvaluationNextGapRequired,
    machineEvaluationUpperGapRequired: requirementOptions.machineEvaluationUpperGapRequired,
    selectedMachineEvaluationUpperGapRequired:
      requirementOptions.selectedMachineEvaluationUpperGapRequired,
    machineNextGapRequired: requirementOptions.machineNextGapRequired,
    selectedNextGapRequired: requirementOptions.selectedNextGapRequired,
    machineUpperGapRequired: requirementOptions.machineUpperGapRequired,
    selectedUpperGapRequired: requirementOptions.selectedUpperGapRequired,
    dailySelectionMode: selectionMode,
    rankScope,
    showGraph: "on",
    scoreDifferenceMode,
    differenceMode,
    settingEstimateMode,
    settingDistribution,
    showSettingDistribution,
    showGrapeColumn,
    combineAimJuggler,
    combineHanabi,
    hasAimJugglerGroupOption,
    hasHanabiGroupOption,
    eventFilters,
    breakdowns,
    ...allAggregation,
  };
}
