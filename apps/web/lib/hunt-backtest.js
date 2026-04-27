import { listHuntScoreTargetMachineNames } from "./hunt-score";
import { canonicalMachineName } from "./machine-difference";

const DEFAULT_RECENT_DAYS = 90;

function normalizeDateText(value) {
  if (typeof value !== "string") {
    return null;
  }

  const normalized = value.trim();
  if (!normalized) {
    return null;
  }

  const match = normalized.match(/^(\d{4})-(\d{2})-(\d{2})$/u);
  if (!match) {
    return null;
  }

  return `${match[1]}-${match[2]}-${match[3]}`;
}

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

function buildRankFilter(rankMinValue, rankMaxValue) {
  const parsedRankMin = readPositiveInteger(rankMinValue);
  const parsedRankMax = readPositiveInteger(rankMaxValue);

  if (parsedRankMin === null && parsedRankMax === null) {
    return {
      rankMin: null,
      rankMax: null,
      hasRankFilter: false,
    };
  }

  const rankMin = parsedRankMin ?? 1;
  const rankMax = parsedRankMax ?? rankMin;

  return {
    rankMin: Math.min(rankMin, rankMax),
    rankMax: Math.max(rankMin, rankMax),
    hasRankFilter: true,
  };
}

function buildScoreFilter(scoreMinValue) {
  const scoreMin = readNumber(scoreMinValue);

  return {
    scoreMin: scoreMin === null ? null : Math.min(100, Math.max(0, scoreMin)),
    hasScoreFilter: scoreMin !== null,
  };
}

function normalizeMatchMode(value) {
  return value === "or" ? "or" : "and";
}

function normalizeShowGraph(value) {
  return value === "off" ? "off" : "on";
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

function readFiniteNumber(value, fallbackValue = 0) {
  const parsedValue = readNumber(value);
  return parsedValue === null ? fallbackValue : parsedValue;
}

function calculateAverage(total, count) {
  if (!Number.isFinite(total) || !Number.isInteger(count) || count <= 0) {
    return null;
  }
  return total / count;
}

function calculatePayoutRate(gamesTotal, differenceTotal) {
  if (!Number.isFinite(gamesTotal) || gamesTotal <= 0) {
    return null;
  }

  const investedCoins = gamesTotal * 3;
  return ((investedCoins + differenceTotal) / investedCoins) * 100;
}

function buildEmptySummary(machineName = "総計") {
  return {
    machineName,
    matchedRowCount: 0,
    actualRowCount: 0,
    differenceTotal: 0,
    gamesTotal: 0,
    payoutRate: null,
    averageSetting: null,
    settingSampleCount: 0,
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
    payoutRate: calculatePayoutRate(summary.gamesTotal, summary.differenceTotal),
  };
}

function matchesOptionalFilters(row, rankFilter, scoreFilter, matchMode) {
  const rankMatched = rankFilter.hasRankFilter
    ? row.rank >= rankFilter.rankMin && row.rank <= rankFilter.rankMax
    : false;
  const scoreMatched = scoreFilter.hasScoreFilter
    ? readFiniteNumber(row.huntScore, Number.NEGATIVE_INFINITY) >= scoreFilter.scoreMin
    : false;

  if (rankFilter.hasRankFilter && scoreFilter.hasScoreFilter) {
    return matchMode === "or" ? rankMatched || scoreMatched : rankMatched && scoreMatched;
  }
  if (rankFilter.hasRankFilter) {
    return rankMatched;
  }
  if (scoreFilter.hasScoreFilter) {
    return scoreMatched;
  }
  return true;
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
  const showGraph = normalizeShowGraph(options.showGraph);
  const periodState = buildPeriodState(options, latestDate);
  const snapshotsInPeriod = (Array.isArray(snapshots) ? snapshots : []).filter((snapshot) =>
    isSnapshotInPeriod(snapshot, periodState.startDate, periodState.endDate),
  );
  const summariesByMachine = new Map();
  const dailySummariesByDate = new Map();
  const totalSummary = buildEmptySummary();
  const matchedDates = new Set();
  let matchedRowCount = 0;
  let actualRowCount = 0;

  for (const snapshot of snapshotsInPeriod) {
    for (const row of snapshot.rows) {
      if (!selectedMachineNameSet.has(row.machineName)) {
        continue;
      }
      if (!matchesOptionalFilters(row, rankFilter, scoreFilter, matchMode)) {
        continue;
      }

      matchedRowCount += 1;
      matchedDates.add(snapshot.baseDate);

      if (!summariesByMachine.has(row.machineName)) {
        summariesByMachine.set(row.machineName, buildEmptySummary(row.machineName));
      }

      const summary = summariesByMachine.get(row.machineName);
      summary.matchedRowCount += 1;
      totalSummary.matchedRowCount += 1;

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

      const differenceValue = readFiniteNumber(row.nextRecord?.difference_value);
      const gamesCount = readFiniteNumber(row.nextRecord?.games_count);
      const settingAverage = readNumber(row.nextSettingEstimate?.average);

      actualRowCount += 1;
      summary.actualRowCount += 1;
      summary.differenceTotal += differenceValue;
      summary.gamesTotal += gamesCount;
      totalSummary.actualRowCount += 1;
      totalSummary.differenceTotal += differenceValue;
      totalSummary.gamesTotal += gamesCount;

      if (actualDate && dailySummariesByDate.has(actualDate)) {
        const dailySummary = dailySummariesByDate.get(actualDate);
        dailySummary.actualRowCount += 1;
        dailySummary.differenceTotal += differenceValue;
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
    showGraph,
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
