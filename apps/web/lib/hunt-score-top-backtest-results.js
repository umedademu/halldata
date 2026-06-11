export const COMMON_HUNT_SCORE_MACHINE_TOP_BACKTEST_LABEL = "共通狙い度が機種内1位";
export const COMMON_HUNT_SCORE_MACHINE_TOP_BACKTEST_PERIOD_DAYS = 365;

const STORED_COMMON_HUNT_SCORE_MACHINE_TOP_BACKTEST_RESULTS = [];

function normalizeText(value) {
  return String(value ?? "").normalize("NFKC").trim();
}

function normalizeKeyText(value) {
  return normalizeText(value).replace(/\s+/gu, "");
}

function readNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const numberValue = Number(value);
  return Number.isFinite(numberValue) ? numberValue : null;
}

function readPositiveInteger(value) {
  const numberValue = readNumber(value);
  return Number.isInteger(numberValue) && numberValue > 0 ? numberValue : null;
}

function normalizeBacktestResult(entry) {
  const storeId = normalizeText(entry?.storeId);
  const storeName = normalizeText(entry?.storeName);
  const machineName = normalizeText(entry?.machineName);
  if ((!storeId && !storeName) || !machineName) {
    return null;
  }

  const result = {
    storeId,
    storeName,
    storeKey: normalizeKeyText(storeName),
    machineName,
    machineKey: normalizeKeyText(machineName),
    conditionLabel: normalizeText(entry?.conditionLabel) || COMMON_HUNT_SCORE_MACHINE_TOP_BACKTEST_LABEL,
    periodDays:
      readPositiveInteger(entry?.periodDays) ?? COMMON_HUNT_SCORE_MACHINE_TOP_BACKTEST_PERIOD_DAYS,
    savedAt: normalizeText(entry?.savedAt),
    targetStartDate: normalizeText(entry?.targetStartDate),
    targetEndDate: normalizeText(entry?.targetEndDate),
    targetDateCount: readPositiveInteger(entry?.targetDateCount),
    matchedDateCount: readPositiveInteger(entry?.matchedDateCount),
    actualRowCount: readPositiveInteger(entry?.actualRowCount),
    payoutRate: readNumber(entry?.payoutRate),
    averageDifference: readNumber(entry?.averageDifference),
    winRate: readNumber(entry?.winRate),
    averageGames: readNumber(entry?.averageGames),
    averageHuntScore: readNumber(entry?.averageHuntScore),
    averageNextGap: readNumber(entry?.averageNextGap),
    averageUpperGap: readNumber(entry?.averageUpperGap),
    averageSetting: readNumber(entry?.averageSetting),
    bbProbability: normalizeText(entry?.bbProbability),
    rbProbability: normalizeText(entry?.rbProbability),
    combinedProbability: normalizeText(entry?.combinedProbability),
    grapeDenominator: readNumber(entry?.grapeDenominator),
    sourceLabel: normalizeText(entry?.sourceLabel),
  };

  const hasMetric = [
    result.actualRowCount,
    result.payoutRate,
    result.averageDifference,
    result.winRate,
    result.averageGames,
    result.averageHuntScore,
    result.averageNextGap,
    result.averageUpperGap,
    result.averageSetting,
    result.grapeDenominator,
  ].some((value) => Number.isFinite(value));

  return hasMetric ? result : null;
}

const NORMALIZED_COMMON_HUNT_SCORE_MACHINE_TOP_BACKTEST_RESULTS =
  STORED_COMMON_HUNT_SCORE_MACHINE_TOP_BACKTEST_RESULTS
    .map(normalizeBacktestResult)
    .filter(Boolean);

export function listCommonHuntScoreMachineTopBacktestResults() {
  return NORMALIZED_COMMON_HUNT_SCORE_MACHINE_TOP_BACKTEST_RESULTS;
}

export function getCommonHuntScoreMachineTopBacktestResult({
  storeId = "",
  storeName = "",
  machineName = "",
} = {}) {
  const targetStoreId = normalizeText(storeId);
  const targetStoreKey = normalizeKeyText(storeName);
  const targetMachineKey = normalizeKeyText(machineName);
  if (!targetMachineKey || (!targetStoreId && !targetStoreKey)) {
    return null;
  }

  return (
    NORMALIZED_COMMON_HUNT_SCORE_MACHINE_TOP_BACKTEST_RESULTS.find((result) => {
      const storeMatched = result.storeId
        ? result.storeId === targetStoreId
        : result.storeKey === targetStoreKey;
      return storeMatched && result.machineKey === targetMachineKey;
    }) ?? null
  );
}
