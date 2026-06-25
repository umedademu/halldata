import { getCommonHuntScoreMachineTopBacktestResult } from "./hunt-score-top-backtest-results";
import {
  getCommonAndMachineEvaluationTopBacktestResult,
  getMachineEvaluationTopBacktestResult,
} from "./machine-evaluation-top-backtest-results";

const EXPECTATION_METRIC_PAYOUT = "payout";
const EXPECTATION_METRIC_RB = "rb";
const MIN_DISPLAY_EXPECTED_PAYOUT_RATE = 102;
const MAX_DISPLAY_EXPECTED_RB_DENOMINATOR = 310;

const A_PARK_KASUGA_COMMON_HUNT_SCORE_TOP_EXPECTATION_MACHINE_NAMES = new Set([
  "SアイムジャグラーＥＸ",
  "ファンキージャグラー２ＫＴ",
  "ゴーゴージャグラー３",
  "ジャグラーガールズSS",
  "ミスタージャグラー",
  "ネオアイムジャグラーEX",
  "ニューキングハナハナ",
  "マイジャグラーV",
  "新ハナビ",
  "スマスロモンキーターンV",
  "スマスロ北斗の拳 転生の章",
]);

function readFiniteNumber(value) {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "string" && value.trim() === "") {
    return null;
  }
  const parsedValue = Number(value);
  return Number.isFinite(parsedValue) ? parsedValue : null;
}

function readPositiveFiniteNumber(value) {
  const parsedValue = readFiniteNumber(value);
  return parsedValue !== null && parsedValue > 0 ? parsedValue : null;
}

function isDisplayableExpectedPayoutRate(value) {
  const rate = readFiniteNumber(value);
  return rate !== null && rate >= MIN_DISPLAY_EXPECTED_PAYOUT_RATE;
}

function isDisplayableExpectedRbDenominator(value) {
  const denominator = readPositiveFiniteNumber(value);
  return denominator !== null && denominator <= MAX_DISPLAY_EXPECTED_RB_DENOMINATOR;
}

function readProbabilityDenominator(value) {
  const normalizedText = String(value ?? "").normalize("NFKC").replace(/,/g, "").trim();
  if (!normalizedText || normalizedText === "-") {
    return null;
  }

  const ratioMatch = normalizedText.match(/1\s*\/\s*(\d+(?:\.\d+)?)/u);
  if (ratioMatch) {
    return readFiniteNumber(ratioMatch[1]);
  }

  return readFiniteNumber(normalizedText);
}

function isNeoAimMachineName(machineName) {
  const normalizedName = String(machineName ?? "")
    .normalize("NFKC")
    .replace(/\s+/gu, "")
    .toLowerCase();
  return normalizedName.includes("ネオアイムジャグラーex") || normalizedName.includes("ネオアイム");
}

function getExpectedRbMetric(machineName) {
  return isNeoAimMachineName(machineName) ? EXPECTATION_METRIC_RB : EXPECTATION_METRIC_PAYOUT;
}

function isMachineTopRankRow(row) {
  const rankValue = readFiniteNumber(row?.machineRank ?? row?.bookmarkRank ?? row?.rank);
  return rankValue === 1;
}

function isMachineEvaluationTopRank(evaluation) {
  return readFiniteNumber(evaluation?.rank) === 1;
}

function readCommonHuntScoreMachineTopBacktestResult(storeId, storeName, row) {
  if (!isMachineTopRankRow(row)) {
    return null;
  }

  return getCommonHuntScoreMachineTopBacktestResult({
    storeId,
    storeName,
    machineName: row?.machineName,
  });
}

function shouldUseCommonHuntScoreMachineTopExpectation(storeId, storeName, machineName) {
  return (
    (storeId === "store-e0f6c17d91d1" || storeName === "Aパーク春日店") &&
    A_PARK_KASUGA_COMMON_HUNT_SCORE_TOP_EXPECTATION_MACHINE_NAMES.has(machineName)
  );
}

function readCommonHuntScoreMachineTopExpectationBacktestResult(storeId, storeName, row) {
  if (!shouldUseCommonHuntScoreMachineTopExpectation(storeId, storeName, row?.machineName)) {
    return null;
  }

  return readCommonHuntScoreMachineTopBacktestResult(storeId, storeName, row);
}

function readMachineEvaluationTopBacktestResult(storeId, storeName, row) {
  if (!isMachineEvaluationTopRank(row?.machineEvaluation)) {
    return null;
  }

  return getMachineEvaluationTopBacktestResult({
    storeId,
    storeName,
    machineName: row?.machineName,
  });
}

function readCommonAndMachineEvaluationTopBacktestResult(storeId, storeName, row) {
  if (!isMachineTopRankRow(row) || !isMachineEvaluationTopRank(row?.machineEvaluation)) {
    return null;
  }

  return getCommonAndMachineEvaluationTopBacktestResult({
    storeId,
    storeName,
    machineName: row?.machineName,
  });
}

function buildMatchedConditionEntriesForEvaluation(evaluation, fallbackLabel = "") {
  const matchedConditions = Array.isArray(evaluation?.matchedConditions)
    ? evaluation.matchedConditions
    : [];
  const evaluationLabel = String(
    evaluation?.displayLabel ?? evaluation?.logicName ?? fallbackLabel ?? "",
  ).trim();

  return matchedConditions.map((condition) => ({
    ...condition,
    evaluationLabel,
  }));
}

function isWatchExpectationCandidate(candidate) {
  const key = String(candidate?.conditionKey ?? candidate?.key ?? "").toLowerCase();
  const label = String(candidate?.label ?? candidate?.conditionName ?? "");
  return key.includes("-watch-") || label.includes("見送り");
}

function isDisplayableExpectationCandidate(
  candidate,
  metric = EXPECTATION_METRIC_PAYOUT,
) {
  if (!candidate || isWatchExpectationCandidate(candidate)) {
    return false;
  }
  return metric === EXPECTATION_METRIC_RB
    ? isDisplayableExpectedRbDenominator(candidate.rbDenominator)
    : isDisplayableExpectedPayoutRate(candidate.payoutRate);
}

function selectBestExpectationCandidate(candidates, metric = EXPECTATION_METRIC_PAYOUT) {
  const normalizedCandidates = candidates
    .filter(Boolean)
    .map((candidate) => ({
      ...candidate,
      payoutRate: readFiniteNumber(candidate?.payoutRate),
      rbDenominator: readPositiveFiniteNumber(candidate?.rbDenominator),
    }))
    .filter((candidate) => isDisplayableExpectationCandidate(candidate, metric));

  if (metric === EXPECTATION_METRIC_RB) {
    if (normalizedCandidates.length === 0) {
      return null;
    }
    return normalizedCandidates.sort((left, right) => {
      if (left.rbDenominator !== right.rbDenominator) {
        return left.rbDenominator - right.rbDenominator;
      }
      if (right.payoutRate !== left.payoutRate) {
        if (right.payoutRate === null) {
          return -1;
        }
        if (left.payoutRate === null) {
          return 1;
        }
        return right.payoutRate - left.payoutRate;
      }
      return 0;
    })[0];
  }

  if (normalizedCandidates.length === 0) {
    return null;
  }

  return normalizedCandidates.sort((left, right) => {
    if (right.payoutRate !== left.payoutRate) {
      return right.payoutRate - left.payoutRate;
    }
    if (left.rbDenominator !== null && right.rbDenominator !== null) {
      return left.rbDenominator - right.rbDenominator;
    }
    if (left.rbDenominator !== null) {
      return -1;
    }
    if (right.rbDenominator !== null) {
      return 1;
    }
    return 0;
  })[0];
}

function readBestMatchedConditionExpectationCandidate(evaluation, metric = EXPECTATION_METRIC_PAYOUT) {
  return selectBestExpectationCandidate(
    buildMatchedConditionEntriesForEvaluation(evaluation).map((condition) => ({
      conditionKey: condition?.conditionKey,
      label: condition?.conditionName,
      payoutRate: condition?.backtestPayoutRate,
      rbDenominator: condition?.backtestRbDenominator,
    })),
    metric,
  );
}

function buildBacktestResultExpectationCandidate(backtestResult, label) {
  if (!backtestResult) {
    return null;
  }

  return {
    label,
    payoutRate: backtestResult.payoutRate,
    rbDenominator: readProbabilityDenominator(backtestResult.rbProbability),
  };
}

function readMachineEvaluationExpectationDetailForEvaluation(
  storeId,
  storeName,
  row,
  evaluation,
  metric = EXPECTATION_METRIC_PAYOUT,
) {
  if (!evaluation) {
    return null;
  }

  return selectBestExpectationCandidate(
    [readBestMatchedConditionExpectationCandidate(evaluation, metric)],
    metric,
  );
}

function readMachineEvaluationExpectationDetail(
  storeId,
  storeName,
  row,
  metric = EXPECTATION_METRIC_PAYOUT,
) {
  return selectBestExpectationCandidate([
    readMachineEvaluationExpectationDetailForEvaluation(
      storeId,
      storeName,
      row,
      row?.machineEvaluation,
      metric,
    ),
    readMachineEvaluationExpectationDetailForEvaluation(
      storeId,
      storeName,
      row,
      row?.machineEvaluationDaySpecific,
      metric,
    ),
    buildBacktestResultExpectationCandidate(
      readCommonHuntScoreMachineTopExpectationBacktestResult(storeId, storeName, row),
      "Aパーク春日式2.0 機種内1位",
    ),
    buildBacktestResultExpectationCandidate(
      readCommonAndMachineEvaluationTopBacktestResult(storeId, storeName, row),
      "春日式2.0＋機種別点数ともに機種内1位",
    ),
    buildBacktestResultExpectationCandidate(
      readMachineEvaluationTopBacktestResult(storeId, storeName, row),
      "機種別点数 機種内1位",
    ),
  ], metric);
}

export function readExpectedRbDenominatorForHuntRankingRow(storeId, storeName, row) {
  const detail = readMachineEvaluationExpectationDetail(
    storeId,
    storeName,
    row,
    getExpectedRbMetric(row?.machineName),
  );
  return isDisplayableExpectedRbDenominator(detail?.rbDenominator)
    ? readPositiveFiniteNumber(detail?.rbDenominator)
    : null;
}

export function hasExpectedRbForHuntRankingRow(storeId, storeName, row) {
  return readExpectedRbDenominatorForHuntRankingRow(storeId, storeName, row) !== null;
}
