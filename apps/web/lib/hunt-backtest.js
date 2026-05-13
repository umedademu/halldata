import { listHuntScoreTargetMachineNames } from "./hunt-score";
import {
  buildDeviationFilter,
  buildNextGapFilter,
  buildRankFilter,
  buildScoreFilter,
  calculateHuntScoreDeviationMap,
  calculateHuntScoreNextGapMap,
  buildConditionRequirementOptions,
  matchesRequiredConditionFilters,
  normalizeDateText,
  normalizeRankScope,
  readDeviationForRankScope,
  readFiniteNumber,
  readNextGapForRankScope,
} from "./hunt-bookmark";
import {
  DEFAULT_DIFFERENCE_MODE,
  canonicalMachineName,
  normalizeDifferenceMode as normalizeMachineDifferenceMode,
  selectDifferenceValue,
} from "./machine-difference";

const DEFAULT_RECENT_DAYS = 90;
const DEFAULT_DEVIATION_MIN = 60;
const AIM_JUGGLER_GROUP_NAME = "アイムジャグラーEX";
const AIM_JUGGLER_MACHINE_NAMES = ["SアイムジャグラーＥＸ", "ネオアイムジャグラーEX"];
const HANABI_GROUP_NAME = "ハナビ";
const HANABI_MACHINE_NAMES = ["新ハナビ", "スマスロ ハナビ"];
const BACKTEST_BREAKDOWN_DEFINITIONS = [
  { key: "all", title: "全合算" },
  { key: "dayTail", title: "翌営業日が末尾の日" },
  { key: "weekday", title: "翌営業日が指定曜日" },
  { key: "normal", title: "通常日" },
];

function readNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const parsedValue = Number(value);
  return Number.isFinite(parsedValue) ? parsedValue : null;
}

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

function normalizeCombineAimJuggler(value) {
  const values = splitOptionValues(value);
  if (values.length === 0) {
    return true;
  }
  return values.includes("1") || values.includes("true") || values.includes("on");
}

function normalizeCombineHanabi(value) {
  const values = splitOptionValues(value);
  if (values.length === 0) {
    return true;
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
  const machineOrder = buildMachineOrder(machineOrderNames);
  const machineNames = [
    ...new Set(
      snapshots.flatMap((snapshot) =>
        snapshot.rows
          .map((row) => String(row.machineName ?? "").trim())
          .filter(Boolean),
      ),
    ),
  ];

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

function normalizeShowGraph(value) {
  return value === "off" ? "off" : "on";
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

function buildBacktestEventFilters(options) {
  return {
    dayTails: normalizeIntegerOptions(options?.dayTails, 0, 9),
    weekdays: normalizeIntegerOptions(options?.weekdays, 0, 6),
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

    if (breakdownKey === "dayTail") {
      return isDayTail;
    }
    if (breakdownKey === "weekday") {
      return isWeekday;
    }

    return !isDayTail && !isWeekday;
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

function buildEmptySummary(machineName = "総計") {
  return {
    machineName,
    matchedRowCount: 0,
    huntScoreTotal: 0,
    deviationTotal: 0,
    deviationSampleCount: 0,
    nextGapTotal: 0,
    nextGapSampleCount: 0,
    averageHuntScore: null,
    averageDeviation: null,
    averageNextGap: null,
    actualRowCount: 0,
    differenceTotal: 0,
    gamesTotal: 0,
    bbTotal: 0,
    rbTotal: 0,
    payoutRate: null,
    bbProbability: null,
    rbProbability: null,
    combinedProbability: null,
    averageSetting: null,
    settingSampleCount: 0,
    investedCoinsTotal: 0,
  };
}

function buildEmptyDailySummary(date, predictionDate) {
  return {
    date,
    predictionDate,
    matchedRowCount: 0,
    actualRowCount: 0,
    differenceTotal: 0,
  };
}

function finalizeSummary(summary) {
  return {
    ...summary,
    averageHuntScore: calculateAverage(summary.huntScoreTotal, summary.matchedRowCount),
    averageDeviation: calculateAverage(summary.deviationTotal, summary.deviationSampleCount),
    averageNextGap: calculateAverage(summary.nextGapTotal, summary.nextGapSampleCount),
    payoutRate: calculatePayoutRate(summary.investedCoinsTotal, summary.differenceTotal),
    bbProbability: formatProbability(summary.gamesTotal, summary.bbTotal),
    rbProbability: formatProbability(summary.gamesTotal, summary.rbTotal),
    combinedProbability: formatProbability(summary.gamesTotal, summary.bbTotal + summary.rbTotal),
  };
}

function buildSnapshotDeviationRows(
  snapshot,
  selectedMachineNameSet,
  combineAimJuggler,
  combineHanabi,
  rankFilter,
) {
  const rows = Array.isArray(snapshot?.rows) ? snapshot.rows : [];
  const overallDeviationMap = calculateHuntScoreDeviationMap(rows);
  const overallNextGapMap = calculateHuntScoreNextGapMap(rows, rankFilter);
  const selectedRows = rows.filter((row) =>
    selectedMachineNameSet.has(String(row.machineName ?? "").trim()),
  );
  const selectedDeviationMap = calculateHuntScoreDeviationMap(selectedRows);
  const selectedNextGapMap = calculateHuntScoreNextGapMap(selectedRows, rankFilter);
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

  const machineDeviationMap = new Map();
  const machineNextGapMap = new Map();
  for (const machineRows of machineRowsByName.values()) {
    const deviationMap = calculateHuntScoreDeviationMap(machineRows);
    const nextGapMap = calculateHuntScoreNextGapMap(machineRows, rankFilter);
    for (const row of machineRows) {
      if (deviationMap.has(row)) {
        machineDeviationMap.set(row, deviationMap.get(row));
      }
      if (nextGapMap.has(row)) {
        machineNextGapMap.set(row, nextGapMap.get(row));
      }
    }
  }

  return new Map(
    rows.map((row) => [
      row,
      {
        ...row,
        overallDeviation: overallDeviationMap.get(row) ?? null,
        selectedDeviation: selectedDeviationMap.get(row) ?? null,
        machineDeviation: machineDeviationMap.get(row) ?? null,
        overallNextGap: overallNextGapMap.get(row) ?? null,
        selectedNextGap: selectedNextGapMap.get(row) ?? null,
        machineNextGap: machineNextGapMap.get(row) ?? null,
      },
    ]),
  );
}

function buildBacktestAggregationDetail(
  snapshotsInPeriod,
  {
    selectedMachineNames,
    selectedMachineNameSet,
    rankFilter,
    scoreFilter,
    deviationFilter,
    nextGapFilter,
    requirementOptions,
    rankScope,
    deviationScope,
    nextGapScope,
    differenceMode,
    combineAimJuggler,
    combineHanabi,
    machineSlotCountLookup,
    rowFilter = () => true,
  },
) {
  const summariesByMachine = new Map();
  const dailySummariesByDate = new Map();
  const totalSummary = buildEmptySummary();
  const matchedDates = new Set();
  let matchedRowCount = 0;
  let actualRowCount = 0;

  for (const snapshot of snapshotsInPeriod) {
    const machineRankCounts = new Map();
    const deviationRowsByRow = buildSnapshotDeviationRows(
      snapshot,
      selectedMachineNameSet,
      combineAimJuggler,
      combineHanabi,
      rankFilter,
    );
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
      const rankValue =
        rankScope === "machine" ? machineRank : rankScope === "selected" ? selectedRank : row.rank;
      const deviationRow = deviationRowsByRow.get(row) ?? row;
      const deviationValue = readDeviationForRankScope(deviationRow, deviationScope);
      const nextGapValue = readNextGapForRankScope(deviationRow, nextGapScope);

      if (
        !matchesRequiredConditionFilters(
          rankValue,
          row.huntScore,
          rankFilter,
          scoreFilter,
          requirementOptions,
          deviationValue,
          deviationFilter,
          true,
          nextGapValue,
          nextGapFilter,
        )
      ) {
        continue;
      }

      const actualDate = getActualDate(row, snapshot);
      if (!rowFilter({ snapshot, row, actualDate })) {
        continue;
      }

      matchedRowCount += 1;
      matchedDates.add(actualDate || snapshot.baseDate);

      if (!summariesByMachine.has(backtestMachineName)) {
        summariesByMachine.set(backtestMachineName, buildEmptySummary(backtestMachineName));
      }

      const summary = summariesByMachine.get(backtestMachineName);
      summary.matchedRowCount += 1;
      summary.huntScoreTotal += readFiniteNumber(row.huntScore);
      totalSummary.matchedRowCount += 1;
      totalSummary.huntScoreTotal += readFiniteNumber(row.huntScore);
      if (Number.isFinite(deviationValue)) {
        summary.deviationTotal += deviationValue;
        summary.deviationSampleCount += 1;
        totalSummary.deviationTotal += deviationValue;
        totalSummary.deviationSampleCount += 1;
      }
      if (Number.isFinite(nextGapValue)) {
        summary.nextGapTotal += nextGapValue;
        summary.nextGapSampleCount += 1;
        totalSummary.nextGapTotal += nextGapValue;
        totalSummary.nextGapSampleCount += 1;
      }

      if (actualDate) {
        if (!dailySummariesByDate.has(actualDate)) {
          dailySummariesByDate.set(
            actualDate,
            buildEmptyDailySummary(actualDate, snapshot.baseDate),
          );
        }
        dailySummariesByDate.get(actualDate).matchedRowCount += 1;
      }

      if (!row.nextRecord) {
        continue;
      }

      const actualMetrics = resolveActualMetrics(row.machineName, row.nextRecord, differenceMode);
      const settingAverage = readNumber(row.nextSettingEstimate?.average);

      actualRowCount += 1;
      summary.actualRowCount += 1;
      summary.differenceTotal += actualMetrics.differenceValue;
      summary.gamesTotal += actualMetrics.gamesCount;
      summary.bbTotal += actualMetrics.bbCount;
      summary.rbTotal += actualMetrics.rbCount;
      summary.investedCoinsTotal += actualMetrics.investedCoins;
      totalSummary.actualRowCount += 1;
      totalSummary.differenceTotal += actualMetrics.differenceValue;
      totalSummary.gamesTotal += actualMetrics.gamesCount;
      totalSummary.bbTotal += actualMetrics.bbCount;
      totalSummary.rbTotal += actualMetrics.rbCount;
      totalSummary.investedCoinsTotal += actualMetrics.investedCoins;

      if (actualDate && dailySummariesByDate.has(actualDate)) {
        const dailySummary = dailySummariesByDate.get(actualDate);
        dailySummary.actualRowCount += 1;
        dailySummary.differenceTotal += actualMetrics.differenceValue;
      }

      if (settingAverage !== null) {
        summary.settingSampleCount += 1;
        summary.averageSetting =
          calculateAverage(
            (summary.averageSetting ?? 0) * (summary.settingSampleCount - 1) + settingAverage,
            summary.settingSampleCount,
          );
        totalSummary.settingSampleCount += 1;
        totalSummary.averageSetting =
          calculateAverage(
            (totalSummary.averageSetting ?? 0) * (totalSummary.settingSampleCount - 1) +
              settingAverage,
            totalSummary.settingSampleCount,
          );
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
    .map((summary) => addSummarySlotCount(finalizeSummary(summary), machineSlotCountLookup))
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
    matchedDateCount: matchedDates.size,
    matchedRowCount,
    actualRowCount,
    missingActualRowCount: matchedRowCount - actualRowCount,
    hasMatches: matchedRowCount > 0,
    hasActualResults: actualRowCount > 0,
    summaries,
    graphPoints,
    total: {
      ...finalizeSummary(totalSummary),
      slotCount: calculateMachineSlotCountTotal(summaryMachineNames, machineSlotCountLookup),
    },
  };
}

export function buildHuntScoreBacktestDetail(snapshots, options = {}) {
  const rankingDates = Array.isArray(snapshots) ? snapshots.map((snapshot) => snapshot.baseDate) : [];
  const latestDate = rankingDates[0] ?? null;
  const earliestDate = rankingDates.at(-1) ?? null;
  const requestedCombineAimJuggler = normalizeCombineAimJuggler(options.combineAimJuggler);
  const requestedCombineHanabi = normalizeCombineHanabi(options.combineHanabi);
  const availableMachineNames = buildAvailableMachineNames(
    Array.isArray(snapshots) ? snapshots : [],
    Array.isArray(options.machineOrder) ? options.machineOrder : undefined,
  );
  const hasAimJugglerGroupOption = AIM_JUGGLER_MACHINE_NAMES.every((machineName) =>
    availableMachineNames.includes(machineName),
  );
  const hasHanabiGroupOption = HANABI_MACHINE_NAMES.every((machineName) =>
    availableMachineNames.includes(machineName),
  );
  const combineAimJuggler = hasAimJugglerGroupOption ? requestedCombineAimJuggler : false;
  const combineHanabi = hasHanabiGroupOption ? requestedCombineHanabi : false;
  const machineSlotCountLookup = buildMachineSlotCountLookup(options.machineSlotCounts);
  const machineSelectionTouched = normalizeMachineSelectionTouched(options.machineTouched);
  const selectedMachineNames = buildSelectedMachineNames(
    options.machineNames,
    availableMachineNames,
    !machineSelectionTouched,
  );
  const selectedMachineNameSet = new Set(selectedMachineNames);
  const rankFilter = buildRankFilter(options.rankMin, options.rankMax);
  const scoreFilter = buildScoreFilter(options.scoreMin);
  const deviationFilter = buildDeviationFilter(
    readOptionWithDefault(options, "deviationMin", DEFAULT_DEVIATION_MIN),
  );
  const nextGapFilter = buildNextGapFilter(options.nextGapMin);
  const requirementOptions = buildConditionRequirementOptions(options);
  const rankScope = normalizeRankScope(options.rankScope);
  const deviationScope = normalizeRankScope(options.deviationScope ?? rankScope);
  const nextGapScope = normalizeRankScope(options.nextGapScope ?? "machine");
  const showGraph = normalizeShowGraph(options.showGraph);
  const scoreDifferenceMode = normalizeDifferenceMode(options.scoreDifferenceMode);
  const differenceMode = normalizeDifferenceMode(options.differenceMode);
  const eventFilters = buildBacktestEventFilters(options);
  const periodState = buildPeriodState(options, latestDate);
  const snapshotsInPeriod = (Array.isArray(snapshots) ? snapshots : []).filter((snapshot) =>
    isSnapshotInPeriod(snapshot, periodState.startDate, periodState.endDate),
  );
  const aggregationOptions = {
    selectedMachineNames,
    selectedMachineNameSet,
    rankFilter,
    scoreFilter,
    deviationFilter,
    nextGapFilter,
    requirementOptions,
    rankScope,
    deviationScope,
    nextGapScope,
    differenceMode,
    combineAimJuggler,
    combineHanabi,
    machineSlotCountLookup,
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
    hasRankFilter: rankFilter.hasRankFilter,
    scoreMin: scoreFilter.scoreMin,
    hasScoreFilter: scoreFilter.hasScoreFilter,
    deviationMin: deviationFilter.deviationMin,
    hasDeviationFilter: deviationFilter.hasDeviationFilter,
    nextGapMin: nextGapFilter.nextGapMin,
    hasNextGapFilter: nextGapFilter.hasNextGapFilter,
    rankRequired: requirementOptions.rankRequired,
    scoreRequired: requirementOptions.scoreRequired,
    deviationRequired: requirementOptions.deviationRequired,
    nextGapRequired: requirementOptions.nextGapRequired,
    rankScope,
    deviationScope,
    nextGapScope,
    showGraph,
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
