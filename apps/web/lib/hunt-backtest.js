import { listHuntScoreTargetMachineNames } from "./hunt-score";
import {
  buildRankFilter,
  buildScoreFilter,
  matchesOptionalFilters,
  normalizeDateText,
  normalizeMatchMode,
  normalizeRankScope,
  readFiniteNumber,
} from "./hunt-bookmark";
import { calculateMachineDifferenceMetrics, canonicalMachineName } from "./machine-difference";

const DEFAULT_RECENT_DAYS = 90;
const DEFAULT_DIFFERENCE_MODE = "bonus";
const BACKTEST_BREAKDOWN_DEFINITIONS = [
  { key: "all", title: "全合算" },
  { key: "dayTail", title: "末尾の日のみで絞り込み" },
  { key: "weekday", title: "曜日のみで絞り込み" },
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

function buildMachineOrder() {
  const orderedMachineNames = [...new Set(listHuntScoreTargetMachineNames().map(canonicalMachineName))];
  return new Map(orderedMachineNames.map((machineName, index) => [machineName, index]));
}

function buildAvailableMachineNames(snapshots) {
  const machineOrder = buildMachineOrder();
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

function buildSelectedMachineNames(requestedMachineNames, availableMachineNames) {
  const availableMachineNameSet = new Set(availableMachineNames);
  const normalizedMachineNames = [...new Set((Array.isArray(requestedMachineNames) ? requestedMachineNames : [requestedMachineNames]).map((value) => String(value ?? "").trim()).filter(Boolean))]
    .filter((machineName) => availableMachineNameSet.has(machineName));

  return normalizedMachineNames.length > 0 ? normalizedMachineNames : availableMachineNames;
}

function normalizeShowGraph(value) {
  return value === "off" ? "off" : "on";
}

function normalizeDifferenceMode(value) {
  return value === "minrepo" ? "minrepo" : DEFAULT_DIFFERENCE_MODE;
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

  if (differenceMode === "bonus") {
    const differenceMetrics = calculateMachineDifferenceMetrics(machineName, nextRecord);
    if (differenceMetrics) {
      return {
        differenceValue: readFiniteNumber(differenceMetrics.differenceValue),
        investedCoins: standardInvestedCoins,
        gamesCount,
        bbCount,
        rbCount,
      };
    }
  }

  return {
    differenceValue: readFiniteNumber(nextRecord?.difference_value),
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
    averageHuntScore: null,
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
    payoutRate: calculatePayoutRate(summary.investedCoinsTotal, summary.differenceTotal),
    bbProbability: formatProbability(summary.gamesTotal, summary.bbTotal),
    rbProbability: formatProbability(summary.gamesTotal, summary.rbTotal),
    combinedProbability: formatProbability(summary.gamesTotal, summary.bbTotal + summary.rbTotal),
  };
}

function buildBacktestAggregationDetail(
  snapshotsInPeriod,
  {
    selectedMachineNames,
    selectedMachineNameSet,
    rankFilter,
    scoreFilter,
    matchMode,
    rankScope,
    differenceMode,
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

    for (const row of snapshot.rows) {
      if (!selectedMachineNameSet.has(row.machineName)) {
        continue;
      }

      const machineRank = (machineRankCounts.get(row.machineName) ?? 0) + 1;
      machineRankCounts.set(row.machineName, machineRank);
      const rankValue = rankScope === "machine" ? machineRank : row.rank;

      if (!matchesOptionalFilters(rankValue, row.huntScore, rankFilter, scoreFilter, matchMode)) {
        continue;
      }

      matchedRowCount += 1;
      matchedDates.add(snapshot.baseDate);

      if (!summariesByMachine.has(row.machineName)) {
        summariesByMachine.set(row.machineName, buildEmptySummary(row.machineName));
      }

      const summary = summariesByMachine.get(row.machineName);
      summary.matchedRowCount += 1;
      summary.huntScoreTotal += readFiniteNumber(row.huntScore);
      totalSummary.matchedRowCount += 1;
      totalSummary.huntScoreTotal += readFiniteNumber(row.huntScore);

      const actualDate = String(row.nextBusinessDate ?? snapshot.nextBusinessDate ?? "").trim();
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

  const machineOrder = new Map(selectedMachineNames.map((machineName, index) => [machineName, index]));
  const summaries = [...summariesByMachine.values()]
    .map(finalizeSummary)
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
    total: finalizeSummary(totalSummary),
  };
}

export function buildHuntScoreBacktestDetail(snapshots, options = {}) {
  const rankingDates = Array.isArray(snapshots) ? snapshots.map((snapshot) => snapshot.baseDate) : [];
  const latestDate = rankingDates[0] ?? null;
  const earliestDate = rankingDates.at(-1) ?? null;
  const availableMachineNames = buildAvailableMachineNames(Array.isArray(snapshots) ? snapshots : []);
  const selectedMachineNames = buildSelectedMachineNames(options.machineNames, availableMachineNames);
  const selectedMachineNameSet = new Set(selectedMachineNames);
  const rankFilter = buildRankFilter(options.rankMin, options.rankMax);
  const scoreFilter = buildScoreFilter(options.scoreMin);
  const matchMode = normalizeMatchMode(options.matchMode);
  const rankScope = normalizeRankScope(options.rankScope);
  const showGraph = normalizeShowGraph(options.showGraph);
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
    matchMode,
    rankScope,
    differenceMode,
  };
  const dayTailSnapshots = snapshotsInPeriod.filter((snapshot) =>
    matchesDayTail(snapshot.baseDate, eventFilters.dayTails),
  );
  const weekdaySnapshots = snapshotsInPeriod.filter((snapshot) =>
    matchesWeekday(snapshot.baseDate, eventFilters.weekdays),
  );
  const normalSnapshots = snapshotsInPeriod.filter((snapshot) => {
    return (
      !matchesDayTail(snapshot.baseDate, eventFilters.dayTails) &&
      !matchesWeekday(snapshot.baseDate, eventFilters.weekdays)
    );
  });
  const allAggregation = buildBacktestAggregationDetail(snapshotsInPeriod, aggregationOptions);
  const breakdownAggregations = {
    all: allAggregation,
    dayTail: buildBacktestAggregationDetail(dayTailSnapshots, aggregationOptions),
    weekday: buildBacktestAggregationDetail(weekdaySnapshots, aggregationOptions),
    normal: buildBacktestAggregationDetail(normalSnapshots, aggregationOptions),
  };
  const breakdowns = BACKTEST_BREAKDOWN_DEFINITIONS.map((definition) => ({
    ...definition,
    ...breakdownAggregations[definition.key],
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
    })),
    selectedMachineNames,
    rankMin: rankFilter.rankMin,
    rankMax: rankFilter.rankMax,
    hasRankFilter: rankFilter.hasRankFilter,
    scoreMin: scoreFilter.scoreMin,
    hasScoreFilter: scoreFilter.hasScoreFilter,
    matchMode,
    rankScope,
    showGraph,
    differenceMode,
    eventFilters,
    breakdowns,
    ...allAggregation,
  };
}
