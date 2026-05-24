import { listHuntScoreTargetMachineNames } from "./hunt-score";
import {
  buildBoundaryGapFilters,
  buildRankFilter,
  buildScoreFilter,
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
import {
  calculateSettingEstimate,
  getSettingEstimateDefinition,
} from "./setting-estimates";

const DEFAULT_RECENT_DAYS = 90;
const DAILY_SELECTION_MODE_MACHINE_TOP_NEXT_GAP = "machineTopNextGap";
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

function readPositiveInteger(value) {
  const parsedValue = Number(value);
  if (!Number.isInteger(parsedValue) || parsedValue < 1) {
    return null;
  }
  return parsedValue;
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
  const normalizedMachineNames = [
    ...new Set(
      (Array.isArray(requestedMachineNames) ? requestedMachineNames : [requestedMachineNames])
        .flatMap((value) => expandRequestedMachineName(value))
        .map((value) => String(value ?? "").trim())
        .filter(Boolean),
    ),
  ]
    .filter((machineName) => availableMachineNameSet.has(machineName));

  if (normalizedMachineNames.length > 0) {
    return normalizedMachineNames;
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

function addAggregateSettingMetrics(summary, machineName, actualMetrics) {
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
    });
  }

  const bucket = summary.settingEstimateBuckets.get(bucketKey);
  bucket.gamesTotal += actualMetrics.gamesCount;
  bucket.bbTotal += actualMetrics.bbCount;
  bucket.rbTotal += actualMetrics.rbCount;
}

function addSettingEstimateRateMetrics(summary, machineName, nextRecord) {
  const definition = getSettingEstimateDefinition(machineName);
  if (!definition) {
    return;
  }

  const settingEstimate = calculateSettingEstimate(definition, nextRecord);
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

function calculateAggregateSettingAverage(summary) {
  const estimates = [...summary.settingEstimateBuckets.values()]
    .map((bucket) => {
      const estimate = calculateSettingEstimate(bucket.definition, {
        games_count: bucket.gamesTotal,
        bb_count: bucket.bbTotal,
        rb_count: bucket.rbTotal,
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

function finalizeSummary(summary) {
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
    averageSetting: calculateAggregateSettingAverage(summary),
    setting35PlusRate: calculateAverage(summary.setting35PlusCount * 100, summary.settingEstimateSampleCount),
    setting4PlusRate: calculateAverage(summary.setting4PlusCount * 100, summary.settingEstimateSampleCount),
    setting45PlusRate: calculateAverage(summary.setting45PlusCount * 100, summary.settingEstimateSampleCount),
    setting5PlusRate: calculateAverage(summary.setting5PlusCount * 100, summary.settingEstimateSampleCount),
  };
}

function addActualMetricsToSummary(summary, machineName, row, actualMetrics, nextGapValue, upperGapValue) {
  summary.actualRowCount += 1;
  if (actualMetrics.differenceValue > 0) {
    summary.winCount += 1;
  }
  summary.huntScoreTotal += readFiniteNumber(row.huntScore);
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
  addAggregateSettingMetrics(summary, machineName, actualMetrics);
  addSettingEstimateRateMetrics(summary, machineName, row.nextRecord);
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

      const matchesCondition =
        (!selectedRowSet || selectedRowSet.has(row)) &&
        matchesRequiredConditionFilters(
          [
            {
              rankValue: machineRank,
              rankFilter: machineRankFilter,
              required: requirementOptions.machineRankRequired,
            },
            {
              rankValue: selectedRank,
              rankFilter: selectedRankFilter,
              required: requirementOptions.selectedRankRequired,
            },
          ],
          row.huntScore,
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
              value: machineUpperGapValue,
              filter: machineUpperGapFilter,
              required: requirementOptions.machineUpperGapRequired,
            },
            {
              value: machineNextGapValue,
              filter: machineNextGapFilter,
              required: requirementOptions.machineNextGapRequired,
            },
            {
              value: selectedUpperGapValue,
              filter: selectedUpperGapFilter,
              required: requirementOptions.selectedUpperGapRequired,
            },
            {
              value: selectedNextGapValue,
              filter: selectedNextGapFilter,
              required: requirementOptions.selectedNextGapRequired,
            },
          ],
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
      addActualMetricsToSummary(
        summary,
        row.machineName,
        row,
        actualMetrics,
        machineNextGapValue,
        machineUpperGapValue,
      );
      addActualMetricsToSummary(
        targetTotalSummary,
        row.machineName,
        row,
        actualMetrics,
        machineNextGapValue,
        machineUpperGapValue,
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
          ...finalizeSummary(summary),
          nonmatchingSummary: nonmatchingSummary ? finalizeSummary(nonmatchingSummary) : null,
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
      ...finalizeSummary(totalSummary),
      nonmatchingSummary: finalizeSummary(nonmatchingTotalSummary),
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
  const scoreFilter = buildScoreFilter(options.scoreMin, options.scoreMax);
  const {
    machineNextGapFilter,
    selectedNextGapFilter,
    machineUpperGapFilter,
    selectedUpperGapFilter,
  } = buildBoundaryGapFilters(options);
  const baseRequirementOptions = buildConditionRequirementOptions(options);
  const selectionMode = normalizeDailySelectionMode(options.dailySelectionMode);
  const usesMachineTopNextGapSelection = isMachineTopNextGapSelectionMode(selectionMode);
  const requirementOptions = usesMachineTopNextGapSelection
    ? requireActiveConditionFilters(baseRequirementOptions, {
        machineRankFilter,
        selectedRankFilter,
        scoreFilter,
        machineNextGapFilter,
        selectedNextGapFilter,
        machineUpperGapFilter,
        selectedUpperGapFilter,
      })
    : baseRequirementOptions;
  const nextGapRankFilters = usesMachineTopNextGapSelection
    ? {}
    : {
        machineRankFilter,
        selectedRankFilter,
      };
  const scoreDifferenceMode = normalizeDifferenceMode(options.scoreDifferenceMode);
  const differenceMode = normalizeDifferenceMode(options.differenceMode);
  const eventFilters = buildBacktestEventFilters(options);
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
    hasScoreFilter: scoreFilter.hasScoreFilter,
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
    machineNextGapRequired: requirementOptions.machineNextGapRequired,
    selectedNextGapRequired: requirementOptions.selectedNextGapRequired,
    machineUpperGapRequired: requirementOptions.machineUpperGapRequired,
    selectedUpperGapRequired: requirementOptions.selectedUpperGapRequired,
    dailySelectionMode: selectionMode,
    rankScope,
    showGraph: "on",
    scoreDifferenceMode,
    differenceMode,
    combineAimJuggler,
    combineHanabi,
    hasAimJugglerGroupOption,
    hasHanabiGroupOption,
    eventFilters,
    breakdowns,
    ...allAggregation,
  };
}
