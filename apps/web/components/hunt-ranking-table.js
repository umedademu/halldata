"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import { SortableTableHeader } from "./sortable-table-header";
import {
  formatAverageGames,
  formatDecimal,
  formatMonthDay,
  formatNumber,
  formatPercent,
  formatRatio,
  formatSite7FetchedDateTime,
  formatSite7FetchedTime,
  formatSignedNumber,
} from "../lib/format";
import {
  HUNT_BACKTEST_BOOKMARK_EVENT,
  HUNT_BACKTEST_BOOKMARK_SELECTION_CUSTOM,
  HUNT_BACKTEST_BOOKMARK_SELECTION_NONE,
  buildHuntBacktestBookmarkRowKey,
  buildNextGapFilter,
  buildHuntBacktestBookmarkMatches,
  buildScopedRankFilters,
  buildScoreFilter,
  buildConditionRequirementOptions,
  calculateHuntScoreNextGapMap,
  matchesRequiredConditionFilters,
  readNextGapForRankScope,
  readSavedHuntBacktestBookmarks,
  readSelectedHuntBacktestBookmarkId,
} from "../lib/hunt-bookmark";
import {
  formatSettingEstimateScore,
  getSettingEstimateHighlightClass,
} from "../lib/setting-estimates";
import {
  DEFAULT_DIFFERENCE_MODE,
  normalizeDifferenceMode,
  selectDifferenceValue,
} from "../lib/machine-difference";
import { getHuntMachineShortName } from "../lib/hunt-machine-display";
import {
  getCommonHuntScoreMachineTopBacktestResult,
} from "../lib/hunt-score-top-backtest-results";
import {
  getCommonAndMachineEvaluationTopBacktestResult,
  getMachineEvaluationTopBacktestResult,
} from "../lib/machine-evaluation-top-backtest-results";

const DEFAULT_VISIBLE_RESULT_KEYS = [
  "difference_value",
  "games_count",
  "bb_count",
  "rb_count",
  "combined_ratio_text",
  "estimated_grape_denominator",
  "setting_estimate",
];
const ESTIMATED_GRAPE_RESULT_KEY = "estimated_grape_denominator";
const FIXED_COLUMN_KEYS = {
  common: "common",
  machineEvaluation: "machine_evaluation",
  condition: "condition",
  expectedPayout: "expected_payout",
  expectedRb: "expected_rb",
  nextGap: "next_gap",
  storeName: "store_name",
  machineName: "machine_name",
};
const DEFAULT_VISIBLE_FIXED_COLUMN_KEYS = [
  FIXED_COLUMN_KEYS.common,
  FIXED_COLUMN_KEYS.machineEvaluation,
  FIXED_COLUMN_KEYS.condition,
  FIXED_COLUMN_KEYS.expectedPayout,
  FIXED_COLUMN_KEYS.expectedRb,
  FIXED_COLUMN_KEYS.machineName,
];
const SORT_COLUMN_INDEX = {
  common: 0,
  machineEvaluation: 1,
  daySpecificMachineEvaluation: 2,
  condition: 3,
  expectedPayout: 4,
  expectedRb: 5,
  nextGap: 6,
  storeName: 7,
  machineName: 8,
  slot: 9,
  resultStart: 10,
};
const SETTING_ESTIMATE_RESULT_HIGHLIGHT_START_KEY = "games_count";
const DEFAULT_RANK_SCOPE = "selected";
const DEFAULT_NEXT_GAP_SCOPE = "machine";
const DEFAULT_HIGHLIGHT_RANK_MIN = 1;
const DEFAULT_HIGHLIGHT_RANK_MAX = 3;
const DEFAULT_HIGHLIGHT_SCORE_MIN = 70;
const MACHINE_EVALUATION_HIGHLIGHT_METRIC_PAYOUT = "payout";
const MACHINE_EVALUATION_HIGHLIGHT_METRIC_RB = "rb";
const STORE_NAME_DISPLAY_CHAR_LIMIT = 12;
const MIN_DISPLAY_EXPECTED_PAYOUT_RATE = 102;
const MAX_DISPLAY_EXPECTED_RB_DENOMINATOR = 310;

function formatEstimatedGrapeDenominator(value) {
  const denominator = Number(value);
  if (!Number.isFinite(denominator) || denominator <= 0) {
    return "-";
  }
  return denominator.toFixed(2);
}

const RESULT_COLUMN_DEFINITIONS = [
  {
    key: "difference_value",
    label: "差枚",
    render: (row, differenceMode) =>
      formatSignedNumber(selectDifferenceValue(row.nextRecord, differenceMode, row.machineName)),
    sortValue: (row, differenceMode) =>
      selectDifferenceValue(row.nextRecord, differenceMode, row.machineName),
  },
  {
    key: "games_count",
    label: "G数",
    render: (row) => formatAverageGames(row.nextRecord?.games_count),
    sortValue: (row) => row.nextRecord?.games_count,
  },
  {
    key: "bb_count",
    label: "BB",
    render: (row) => formatAverageGames(row.nextRecord?.bb_count),
    sortValue: (row) => row.nextRecord?.bb_count,
  },
  {
    key: "rb_count",
    label: "RB",
    render: (row) => formatAverageGames(row.nextRecord?.rb_count),
    sortValue: (row) => row.nextRecord?.rb_count,
  },
  {
    key: "combined_ratio_text",
    label: "合成",
    render: (row) => formatRatio(row.nextRecord?.combined_ratio_text),
    sortValue: (row) => row.nextRecord?.combined_ratio_text,
  },
  {
    key: ESTIMATED_GRAPE_RESULT_KEY,
    label: "ブドウ",
    render: (row) => formatEstimatedGrapeDenominator(row.nextRecord?.estimated_grape_denominator),
    sortValue: (row) => row.nextRecord?.estimated_grape_denominator,
  },
  {
    key: "setting_estimate",
    label: "設定",
    render: (row) => formatSettingEstimateScore(row.nextSettingEstimate?.average),
    sortValue: (row) => row.nextSettingEstimate?.average,
  },
  {
    key: "payout_rate",
    label: "出率",
    render: (row) => formatPercent(row.nextRecord?.payout_rate),
    sortValue: (row) => row.nextRecord?.payout_rate,
  },
  {
    key: "bb_ratio_text",
    label: "BB率",
    render: (row) => formatRatio(row.nextRecord?.bb_ratio_text),
    sortValue: (row) => row.nextRecord?.bb_ratio_text,
  },
  {
    key: "rb_ratio_text",
    label: "RB率",
    render: (row) => formatRatio(row.nextRecord?.rb_ratio_text),
    sortValue: (row) => row.nextRecord?.rb_ratio_text,
  },
];

const SETTING_ESTIMATE_RESULT_HIGHLIGHT_KEYS = new Set(
  RESULT_COLUMN_DEFINITIONS.slice(
    RESULT_COLUMN_DEFINITIONS.findIndex(
      (column) => column.key === SETTING_ESTIMATE_RESULT_HIGHLIGHT_START_KEY,
    ),
  ).map((column) => column.key),
);

function getSettingEstimateResultCellClassName(column, settingEstimateCellClassName) {
  return SETTING_ESTIMATE_RESULT_HIGHLIGHT_KEYS.has(column?.key)
    ? settingEstimateCellClassName
    : "";
}

function buildResultColumns(differenceMode, showGrapeColumn) {
  const columnDefinitions = showGrapeColumn
    ? RESULT_COLUMN_DEFINITIONS
    : RESULT_COLUMN_DEFINITIONS.filter((column) => column.key !== ESTIMATED_GRAPE_RESULT_KEY);

  return columnDefinitions.map((column) => ({
    ...column,
    render: (row) => column.render(row, differenceMode),
    sortValue: (row) =>
      typeof column.sortValue === "function"
        ? column.sortValue(row, differenceMode)
        : column.render(row, differenceMode),
  }));
}

function formatRankingDateFlowLabel(predictionDate, actualDate) {
  const scoreDateLabel = predictionDate ? `${formatMonthDay(predictionDate)}狙い度` : "狙い度";
  const actualDateLabel = actualDate ? `${formatMonthDay(actualDate)}実績` : "実績なし";
  return `${scoreDateLabel} → ${actualDateLabel}`;
}

function truncateStoreNameForDisplay(storeName) {
  const text = String(storeName ?? "").trim();
  const chars = Array.from(text);
  if (chars.length <= STORE_NAME_DISPLAY_CHAR_LIMIT) {
    return text;
  }
  return `${chars.slice(0, STORE_NAME_DISPLAY_CHAR_LIMIT).join("")}…`;
}

function StoreNameLinkOrText({ storeId, storeName }) {
  const label = truncateStoreNameForDisplay(storeName) || "-";
  const title = String(storeName ?? "").trim() || undefined;

  return storeId ? (
    <Link
      href={`/stores/${storeId}`}
      className="directoryPrimaryLink crossStoreHuntStoreLink"
      title={title}
    >
      {label}
    </Link>
  ) : (
    <span className="crossStoreHuntStoreLabel" title={title}>{label}</span>
  );
}

function latestSite7FetchedAtFromRows(rows) {
  return (Array.isArray(rows) ? rows : [])
    .map((row) => String(row?.predictionMachineSite7FetchedAt ?? "").trim())
    .filter(Boolean)
    .sort((left, right) => {
      const leftTime = Date.parse(left);
      const rightTime = Date.parse(right);
      if (Number.isFinite(leftTime) && Number.isFinite(rightTime) && leftTime !== rightTime) {
        return rightTime - leftTime;
      }
      return String(right).localeCompare(String(left), "ja");
    })[0] ?? null;
}

function site7BadgeTitle(fetchedAt, fallbackTitle) {
  const fetchedDateTime = formatSite7FetchedDateTime(fetchedAt);
  return fetchedDateTime ? `${fallbackTitle}\n取得: ${fetchedDateTime}` : fallbackTitle;
}

function combineTitleParts(...parts) {
  return parts.map((part) => String(part ?? "").trim()).filter(Boolean).join("\n");
}

function isSite7Record(record) {
  return String(record?.data_source ?? record?.dataSource ?? "").trim().toLowerCase() === "site7";
}

function readSite7FetchedAt(record) {
  return String(record?.site7_fetched_at ?? record?.site7FetchedAt ?? "").trim();
}

function buildSite7RecordTitle(label, record) {
  if (!isSite7Record(record)) {
    return "";
  }

  const fetchedDateTime = formatSite7FetchedDateTime(readSite7FetchedAt(record));
  return fetchedDateTime
    ? `${label}: Sセブン暫定データ\n取得: ${fetchedDateTime}`
    : `${label}: Sセブン暫定データ`;
}

function buildRankingRowSite7Title(row) {
  return combineTitleParts(
    buildSite7RecordTitle("狙い度の日", row?.currentRecord),
    buildSite7RecordTitle("実績の日", row?.nextRecord),
  );
}

function Site7RankingBadge({ fetchedAt, title }) {
  const fetchedTime = formatSite7FetchedTime(fetchedAt);
  return (
    <span className="site7RankingBadge" title={site7BadgeTitle(fetchedAt, title)}>
      Sセブン{fetchedTime ? <span className="site7BadgeTime">{fetchedTime}</span> : null}
    </span>
  );
}

function buildFallbackRankingGroups(rows) {
  const groupsByMachineName = new Map();

  for (const row of Array.isArray(rows) ? rows : []) {
    const machineName = String(row.machineName ?? "").trim();
    if (!machineName) {
      continue;
    }

    if (!groupsByMachineName.has(machineName)) {
      groupsByMachineName.set(machineName, []);
    }
    groupsByMachineName.get(machineName).push(row);
  }

  return [...groupsByMachineName.entries()].map(([machineName, groupRows]) => ({
    machineName,
    totalCount: groupRows.length,
    limit: groupRows.length,
    rows: groupRows,
  }));
}

function readRankingSortNumber(value, fallbackValue = Number.MAX_SAFE_INTEGER) {
  const parsedValue = Number(value);
  return Number.isFinite(parsedValue) ? parsedValue : fallbackValue;
}

function compareRankingRows(left, right) {
  const leftMachineEvaluationOrder = readRankingSortNumber(
    left.machineEvaluationRankingOrder,
    Number.POSITIVE_INFINITY,
  );
  const rightMachineEvaluationOrder = readRankingSortNumber(
    right.machineEvaluationRankingOrder,
    Number.POSITIVE_INFINITY,
  );
  if (
    Number.isFinite(leftMachineEvaluationOrder) ||
    Number.isFinite(rightMachineEvaluationOrder)
  ) {
    const orderDiff = leftMachineEvaluationOrder - rightMachineEvaluationOrder;
    if (orderDiff !== 0) {
      return orderDiff;
    }
  }

  return (
    readRankingSortNumber(right.huntScore, 0) - readRankingSortNumber(left.huntScore, 0) ||
    readRankingSortNumber(left.overallRank ?? left.selectedRank ?? left.rank) -
      readRankingSortNumber(right.overallRank ?? right.selectedRank ?? right.rank) ||
    String(left.machineName ?? "").localeCompare(String(right.machineName ?? ""), "ja") ||
    String(left.slotNumber ?? "").localeCompare(String(right.slotNumber ?? ""), "ja", {
      numeric: true,
    })
  );
}

function getRankingGroupRows(group, includeAllRows = false) {
  if (includeAllRows && Array.isArray(group.allRows)) {
    return group.allRows;
  }
  return Array.isArray(group.rows) ? group.rows : [];
}

function buildSortedRankingRows(rankingGroups, includeAllRows = false) {
  return rankingGroups
    .flatMap((group) => getRankingGroupRows(group, includeAllRows))
    .sort(compareRankingRows)
    .map((row, index) => ({
      ...row,
      selectedRank: index + 1,
    }));
}

function buildOverallRows(rows, overallLimit) {
  return rows.slice(0, overallLimit).map((row, index) => ({
    ...row,
    bookmarkRank: row.rank,
    rank: index + 1,
  }));
}

function compareMachineTopCandidateRows(left, right) {
  const scoreDiff =
    readRankingSortNumber(right.huntScore, Number.NEGATIVE_INFINITY) -
    readRankingSortNumber(left.huntScore, Number.NEGATIVE_INFINITY);
  if (Math.abs(scoreDiff) > 0.000000001) {
    return scoreDiff;
  }

  const leftNextGap = readNextGapForRankScope(left, "machine");
  const rightNextGap = readNextGapForRankScope(right, "machine");
  const nextGapDiff =
    readRankingSortNumber(rightNextGap, Number.NEGATIVE_INFINITY) -
    readRankingSortNumber(leftNextGap, Number.NEGATIVE_INFINITY);
  if (Math.abs(nextGapDiff) > 0.000000001) {
    return nextGapDiff;
  }

  return (
    readRankingSortNumber(left.selectedRank ?? left.overallRank ?? left.rank) -
      readRankingSortNumber(right.selectedRank ?? right.overallRank ?? right.rank) ||
    String(left.machineName ?? "").localeCompare(String(right.machineName ?? ""), "ja") ||
    String(left.slotNumber ?? "").localeCompare(String(right.slotNumber ?? ""), "ja", {
      numeric: true,
    })
  );
}

function buildMachineTopCandidateRows(displayGroups, gapValueByRowKey) {
  return (Array.isArray(displayGroups) ? displayGroups : [])
    .map((group) => {
      const topRow = getRankingGroupRows(group, true)[0] ?? null;
      if (!topRow) {
        return null;
      }

      return decorateRowsWithGapValues([topRow], gapValueByRowKey)[0] ?? null;
    })
    .filter(Boolean)
    .sort(compareMachineTopCandidateRows)
    .map((row, index) => ({
      ...row,
      bookmarkRank: row.rank,
      rank: index + 1,
    }));
}

function buildRankGapRowKey(row) {
  return String(row?.rowKey ?? `${row?.machineName ?? ""}::${row?.slotNumber ?? ""}`).trim();
}

function buildGapValueMaps(displayRows, displayGroups) {
  const overallNextGapMap = calculateHuntScoreNextGapMap(displayRows);
  const selectedNextGapMap = calculateHuntScoreNextGapMap(displayRows);
  const overallNextGapByKey = new Map(
    displayRows.map((row) => [buildRankGapRowKey(row), overallNextGapMap.get(row) ?? null]),
  );
  const selectedNextGapByKey = new Map(
    displayRows.map((row) => [buildRankGapRowKey(row), selectedNextGapMap.get(row) ?? null]),
  );
  const machineNextGapByKey = new Map();

  for (const group of displayGroups) {
    const groupRows = getRankingGroupRows(group, true);
    const nextGapMap = calculateHuntScoreNextGapMap(groupRows);
    for (const row of groupRows) {
      if (nextGapMap.has(row)) {
        machineNextGapByKey.set(buildRankGapRowKey(row), nextGapMap.get(row));
      }
    }
  }

  const valueByRowKey = new Map();
  for (const row of displayRows) {
    const rowKey = buildRankGapRowKey(row);
    valueByRowKey.set(rowKey, {
      overallNextGap: overallNextGapByKey.get(rowKey) ?? null,
      selectedNextGap: selectedNextGapByKey.get(rowKey) ?? null,
      machineNextGap: machineNextGapByKey.get(rowKey) ?? null,
    });
  }

  return valueByRowKey;
}

function decorateRowsWithGapValues(rows, gapValueByRowKey) {
  return rows.map((row) => ({
    ...row,
    ...(gapValueByRowKey.get(buildRankGapRowKey(row)) ?? {}),
  }));
}

function normalizeNextGapScope(value) {
  if (value === "all" || value === "machine" || value === "selected") {
    return value;
  }
  return DEFAULT_NEXT_GAP_SCOPE;
}

function normalizeRankScope(value) {
  if (value === "all" || value === "machine" || value === "selected") {
    return value;
  }
  return DEFAULT_RANK_SCOPE;
}

function readRankForScope(row, rankScope) {
  const normalizedScope = normalizeRankScope(rankScope);
  if (normalizedScope === "machine") {
    return row?.machineRank ?? row?.bookmarkRank ?? row?.rank;
  }
  if (normalizedScope === "all") {
    return row?.overallRank ?? row?.rank;
  }
  return row?.selectedRank ?? null;
}

function formatNextGapForScope(row, nextGapScope) {
  return formatDecimal(readNextGapForRankScope(row, normalizeNextGapScope(nextGapScope)));
}

function getMachineEvaluationPayoutClass(payoutRate) {
  const rate = Number(payoutRate);
  if (!Number.isFinite(rate)) {
    return "";
  }
  if (rate >= 106) {
    return "machineEvaluationPayoutPurple";
  }
  if (rate >= 105) {
    return "machineEvaluationPayoutRed";
  }
  if (rate >= 104) {
    return "machineEvaluationPayoutGreen";
  }
  if (rate >= 103) {
    return "machineEvaluationPayoutYellow";
  }
  if (rate >= 102) {
    return "machineEvaluationPayoutBlue";
  }
  return "";
}

function isDisplayableExpectedPayoutRate(value) {
  const rate = readFiniteNumber(value);
  return rate !== null && rate >= MIN_DISPLAY_EXPECTED_PAYOUT_RATE;
}

function isDisplayableExpectedRbDenominator(value) {
  const denominator = readPositiveFiniteNumber(value);
  return denominator !== null && denominator <= MAX_DISPLAY_EXPECTED_RB_DENOMINATOR;
}

function readDisplayableExpectedRbDenominator(detail) {
  return isDisplayableExpectedRbDenominator(detail?.rbDenominator)
    ? readPositiveFiniteNumber(detail?.rbDenominator)
    : null;
}

function getMachineEvaluationRbClass(rbDenominator) {
  const denominator = Number(rbDenominator);
  if (!Number.isFinite(denominator) || denominator <= 0) {
    return "";
  }
  if (denominator <= 270) {
    return "machineEvaluationPayoutPurple";
  }
  if (denominator <= 280) {
    return "machineEvaluationPayoutRed";
  }
  if (denominator <= 290) {
    return "machineEvaluationPayoutGreen";
  }
  if (denominator <= 300) {
    return "machineEvaluationPayoutYellow";
  }
  if (denominator <= 310) {
    return "machineEvaluationPayoutBlue";
  }
  return "";
}

function isNeoAimMachineName(machineName) {
  const normalizedName = String(machineName ?? "")
    .normalize("NFKC")
    .replace(/\s+/gu, "")
    .toLowerCase();
  return normalizedName.includes("ネオアイムジャグラーex") || normalizedName.includes("ネオアイム");
}

function getMachineEvaluationHighlightMetric(machineName) {
  return isNeoAimMachineName(machineName)
    ? MACHINE_EVALUATION_HIGHLIGHT_METRIC_RB
    : MACHINE_EVALUATION_HIGHLIGHT_METRIC_PAYOUT;
}

function buildMachineEvaluationClassName(highlightClass) {
  return [
    highlightClass ? "machineEvaluationMatchedCell" : "",
    highlightClass,
  ].filter(Boolean).join(" ");
}

function isMachineTopRankRow(row) {
  return readRankingSortNumber(row?.machineRank ?? row?.bookmarkRank ?? row?.rank, null) === 1;
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

function isMachineEvaluationTopRank(evaluation) {
  return readRankingSortNumber(evaluation?.rank, null) === 1;
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

function buildCommonHuntScoreBacktestTitleParts(backtestResult, huntScoreLogicLabel) {
  if (!backtestResult) {
    return [];
  }

  const logicLabel = String(huntScoreLogicLabel || "狙い度").trim();
  const countLabel = Number.isFinite(backtestResult.actualRowCount)
    ? `${formatNumber(backtestResult.actualRowCount)}件`
    : "-";
  const payoutLabel = Number.isFinite(backtestResult.payoutRate)
    ? formatPercent(backtestResult.payoutRate)
    : "-";
  const bbLabel = backtestResult.bbProbability || "-";
  const rbLabel = backtestResult.rbProbability || "-";

  return [
    `${logicLabel} / 狙い度 機種内1位`,
    `件数: ${countLabel} / ${payoutLabel}`,
    `BB率: ${bbLabel} / RB率: ${rbLabel}`,
  ].filter(Boolean);
}

function buildMachineEvaluationTopBacktestTitleParts(backtestResult, evaluation) {
  if (!backtestResult) {
    return [];
  }

  const logicLabel = String(evaluation?.logicName || "機種別ロジック").trim();
  const countLabel = Number.isFinite(backtestResult.actualRowCount)
    ? `${formatNumber(backtestResult.actualRowCount)}件`
    : "-";
  const payoutLabel = Number.isFinite(backtestResult.payoutRate)
    ? formatPercent(backtestResult.payoutRate)
    : "-";
  const bbLabel = backtestResult.bbProbability || "-";
  const rbLabel = backtestResult.rbProbability || "-";

  return [
    `${logicLabel} / 機種別点数 機種内1位`,
    `件数: ${countLabel} / ${payoutLabel}`,
    `BB率: ${bbLabel} / RB率: ${rbLabel}`,
  ].filter(Boolean);
}

function buildCommonAndMachineEvaluationTopBacktestTitleParts(backtestResult, evaluation) {
  if (!backtestResult) {
    return [];
  }

  const logicLabel = String(evaluation?.logicName || "機種別ロジック").trim();
  const countLabel = Number.isFinite(backtestResult.actualRowCount)
    ? `${formatNumber(backtestResult.actualRowCount)}件`
    : "-";
  const payoutLabel = Number.isFinite(backtestResult.payoutRate)
    ? formatPercent(backtestResult.payoutRate)
    : "-";
  const bbLabel = backtestResult.bbProbability || "-";
  const rbLabel = backtestResult.rbProbability || "-";

  return [
    `春日式2.0 + ${logicLabel} / ともに機種内1位`,
    `件数: ${countLabel} / ${payoutLabel}`,
    `BB率: ${bbLabel} / RB率: ${rbLabel}`,
  ].filter(Boolean);
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

function readMatchedConditionEntries(row) {
  return [
    ...buildMatchedConditionEntriesForEvaluation(row?.machineEvaluation, "機種別"),
    ...buildMatchedConditionEntriesForEvaluation(row?.machineEvaluationDaySpecific, "日別"),
  ];
}

function readMatchedConditionCount(row) {
  return readMatchedConditionEntries(row).length;
}

function isSameExpectationCandidate(left, right) {
  if (!left || !right) {
    return false;
  }
  return (
    String(left.label ?? "") === String(right.label ?? "") &&
    readFiniteNumber(left.payoutRate) === readFiniteNumber(right.payoutRate) &&
    readFiniteNumber(left.rbDenominator) === readFiniteNumber(right.rbDenominator)
  );
}

function isWatchExpectationCandidate(candidate) {
  const key = String(candidate?.conditionKey ?? candidate?.key ?? "").toLowerCase();
  const label = String(candidate?.label ?? candidate?.conditionName ?? "");
  return key.includes("-watch-") || label.includes("見送り");
}

function isDisplayableExpectationCandidate(
  candidate,
  metric = MACHINE_EVALUATION_HIGHLIGHT_METRIC_PAYOUT,
) {
  if (!candidate || isWatchExpectationCandidate(candidate)) {
    return false;
  }
  return metric === MACHINE_EVALUATION_HIGHLIGHT_METRIC_RB
    ? isDisplayableExpectedRbDenominator(candidate.rbDenominator)
    : isDisplayableExpectedPayoutRate(candidate.payoutRate);
}

function buildMatchedConditionTitleParts(row, highlightedDetail = null) {
  const matchedConditions = readMatchedConditionEntries(row);
  if (matchedConditions.length === 0) {
    return [];
  }

  return [
    "一致した採用条件:",
    ...matchedConditions.map((condition) => {
      const evaluationLabel = condition.evaluationLabel ? `${condition.evaluationLabel}: ` : "";
      const selectedLabel = condition.isSelected ? " / 選択中" : "";
      const backtestLabel = condition.backtestLabel ? ` / ${condition.backtestLabel}` : "";
      const isHighlighted = isSameExpectationCandidate(
        {
          conditionKey: condition.conditionKey,
          label: condition.conditionName,
          payoutRate: condition.backtestPayoutRate,
          rbDenominator: condition.backtestRbDenominator,
        },
        highlightedDetail,
      );
      const marker = isHighlighted ? "★" : "";
      return `・${marker}${evaluationLabel}${condition.conditionName}${backtestLabel}${selectedLabel}`;
    }),
  ];
}

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

function formatExpectedPayoutRate(value) {
  const rate = readFiniteNumber(value);
  return rate === null ? "-" : `${rate.toFixed(2)}%`;
}

function formatExpectedRbDenominator(value) {
  const denominator = readFiniteNumber(value);
  if (denominator === null || denominator <= 0) {
    return "-";
  }
  return denominator.toFixed(1).replace(/\.0$/u, "");
}

function selectBestExpectationCandidate(
  candidates,
  metric = MACHINE_EVALUATION_HIGHLIGHT_METRIC_PAYOUT,
) {
  const normalizedCandidates = candidates
    .map((candidate) => ({
      ...candidate,
      payoutRate: readFiniteNumber(candidate?.payoutRate),
      rbDenominator: readPositiveFiniteNumber(candidate?.rbDenominator),
    }))
    .filter((candidate) => isDisplayableExpectationCandidate(candidate, metric));

  if (metric === MACHINE_EVALUATION_HIGHLIGHT_METRIC_RB) {
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

function readBestMatchedConditionExpectationCandidateFromEntries(
  matchedConditions,
  metric = MACHINE_EVALUATION_HIGHLIGHT_METRIC_PAYOUT,
) {
  return selectBestExpectationCandidate(
    (Array.isArray(matchedConditions) ? matchedConditions : []).map((condition) => ({
      conditionKey: condition?.conditionKey,
      label: condition?.conditionName,
      payoutRate: condition?.backtestPayoutRate,
      rbDenominator: condition?.backtestRbDenominator,
    })),
    metric,
  );
}

function readBestMatchedConditionExpectationCandidate(
  evaluation,
  metric = MACHINE_EVALUATION_HIGHLIGHT_METRIC_PAYOUT,
) {
  return readBestMatchedConditionExpectationCandidateFromEntries(
    buildMatchedConditionEntriesForEvaluation(evaluation),
    metric,
  );
}

function readMatchedConditionExpectationDetail(
  row,
  metric = MACHINE_EVALUATION_HIGHLIGHT_METRIC_PAYOUT,
) {
  return readBestMatchedConditionExpectationCandidateFromEntries(
    readMatchedConditionEntries(row),
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
  includeStoredTopBacktests = true,
  includeMatchedConditions = true,
  metric = MACHINE_EVALUATION_HIGHLIGHT_METRIC_PAYOUT,
) {
  if (!evaluation) {
    return null;
  }

  const candidates = includeMatchedConditions
    ? [readBestMatchedConditionExpectationCandidate(evaluation, metric)]
    : [];

  if (includeStoredTopBacktests) {
    candidates.push(
      buildBacktestResultExpectationCandidate(
        readCommonAndMachineEvaluationTopBacktestResult(storeId, storeName, row),
        "春日式2.0＋機種別点数ともに機種内1位",
      ),
      buildBacktestResultExpectationCandidate(
        readMachineEvaluationTopBacktestResult(storeId, storeName, row),
        "機種別点数 機種内1位",
      ),
    );
  }

  return selectBestExpectationCandidate(candidates, metric);
}

function readMachineEvaluationExpectationDetail(
  storeId,
  storeName,
  row,
  metric = MACHINE_EVALUATION_HIGHLIGHT_METRIC_PAYOUT,
) {
  return selectBestExpectationCandidate([
    readMachineEvaluationExpectationDetailForEvaluation(
      storeId,
      storeName,
      row,
      row?.machineEvaluation,
      false,
      true,
      metric,
    ),
    readMachineEvaluationExpectationDetailForEvaluation(
      storeId,
      storeName,
      row,
      row?.machineEvaluationDaySpecific,
      false,
      true,
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

function buildMachineEvaluationExpectationTitle(detail, metric = "") {
  if (!detail) {
    return "";
  }

  return combineTitleParts(
    detail.label ? `採用目安: ${detail.label}` : "",
    isDisplayableExpectedPayoutRate(detail.payoutRate)
      ? `${metric === MACHINE_EVALUATION_HIGHLIGHT_METRIC_PAYOUT ? "★" : ""}期待割: ${
          formatExpectedPayoutRate(detail.payoutRate)
        }`
      : "",
    isDisplayableExpectedRbDenominator(detail.rbDenominator)
      ? `${metric === MACHINE_EVALUATION_HIGHLIGHT_METRIC_RB ? "★" : ""}期待RB: ${
          formatExpectedRbDenominator(detail.rbDenominator)
        }`
      : "",
  );
}

function getMachineEvaluationExpectationClassName(expectationDetail, metric) {
  const highlightClass = metric === MACHINE_EVALUATION_HIGHLIGHT_METRIC_RB
    ? getMachineEvaluationRbClass(expectationDetail?.rbDenominator)
    : getMachineEvaluationPayoutClass(expectationDetail?.payoutRate);
  return buildMachineEvaluationClassName(highlightClass);
}

function getMachineEvaluationExpectedPayoutClassName(expectationDetail) {
  return buildMachineEvaluationClassName(
    getMachineEvaluationPayoutClass(expectationDetail?.payoutRate),
  );
}

function getMachineEvaluationExpectedRbClassName(expectationDetail, machineName) {
  if (!isNeoAimMachineName(machineName)) {
    return "";
  }
  return buildMachineEvaluationClassName(
    getMachineEvaluationRbClass(expectationDetail?.rbDenominator),
  );
}

function getMachineEvaluationExpectationCellClassName(storeId, storeName, row) {
  const metric = getMachineEvaluationHighlightMetric(row?.machineName);
  return getMachineEvaluationExpectationClassName(
    readMachineEvaluationExpectationDetail(storeId, storeName, row, metric),
    metric,
  );
}

function readDaySpecificMachineEvaluationColumnLabel(rows) {
  const evaluation = (Array.isArray(rows) ? rows : [])
    .map((row) => row?.machineEvaluationDaySpecific)
    .find(Boolean);
  return String(evaluation?.displayLabel ?? "").trim() || "日別";
}

function MachineEvaluationCell({
  storeId,
  storeName,
  row,
  evaluation,
  extraTitle = "",
  includeStoredTopBacktests = true,
}) {
  if (!evaluation) {
    return <td title={extraTitle || undefined} data-sort-value="">-</td>;
  }

  const topBacktestResult = includeStoredTopBacktests
    ? readMachineEvaluationTopBacktestResult(storeId, storeName, row)
    : null;
  const topBacktestTitleParts = buildMachineEvaluationTopBacktestTitleParts(
    topBacktestResult,
    evaluation,
  );
  const combinedTopBacktestResult = includeStoredTopBacktests
    ? readCommonAndMachineEvaluationTopBacktestResult(
        storeId,
        storeName,
        row,
      )
    : null;
  const combinedTopBacktestTitleParts = buildCommonAndMachineEvaluationTopBacktestTitleParts(
    combinedTopBacktestResult,
    evaluation,
  );
  const expectationDetail = readMachineEvaluationExpectationDetailForEvaluation(
    storeId,
    storeName,
    row,
    evaluation,
    includeStoredTopBacktests,
    true,
    getMachineEvaluationHighlightMetric(row?.machineName),
  );
  const highlightMetric = getMachineEvaluationHighlightMetric(row?.machineName);
  const titleParts = [
    buildMachineEvaluationExpectationTitle(expectationDetail, highlightMetric),
    evaluation.logicName ? `機種別ロジック: ${evaluation.logicName}` : "",
    combinedTopBacktestTitleParts.length > 0 ? combinedTopBacktestTitleParts.join("\n") : "",
    topBacktestTitleParts.length > 0 ? topBacktestTitleParts.join("\n") : "",
    Number.isFinite(evaluation.rank) ? `機種別順位: ${evaluation.rank}` : "",
    Number.isFinite(evaluation.nextGap) ? `次点差: ${formatDecimal(evaluation.nextGap)}` : "",
  ].filter(Boolean);
  const cellClassNames = getMachineEvaluationExpectationClassName(
    expectationDetail,
    highlightMetric,
  );

  return (
    <td
      className={cellClassNames || undefined}
      title={combineTitleParts(titleParts.join("\n"), extraTitle) || undefined}
      data-sort-value={readRankingSortNumber(evaluation.score, "")}
    >
      <span className="machineEvaluationCellValue">{formatNumber(evaluation.score)}</span>
    </td>
  );
}

function MachineEvaluationConditionCell({ row, extraTitle = "" }) {
  const matchedConditionCount = readMatchedConditionCount(row);
  const highlightMetric = getMachineEvaluationHighlightMetric(row?.machineName);
  const expectationDetail = readMatchedConditionExpectationDetail(row, highlightMetric);
  const expectationClassName = getMachineEvaluationExpectationClassName(
    expectationDetail,
    highlightMetric,
  );
  const className = [
    "conditionCountCell",
    expectationClassName,
  ].filter(Boolean).join(" ");
  const matchedConditionTitleParts = buildMatchedConditionTitleParts(row, expectationDetail);
  const expectationTitle = buildMachineEvaluationExpectationTitle(expectationDetail, highlightMetric);
  const title = matchedConditionTitleParts.length > 0
    ? combineTitleParts(expectationTitle, matchedConditionTitleParts.join("\n"))
    : "一致した採用条件はありません";

  return (
    <td
      className={className || undefined}
      title={combineTitleParts(title, extraTitle) || undefined}
      data-sort-value={matchedConditionCount}
    >
      <span className="conditionCountCellValue">{formatNumber(matchedConditionCount)}</span>
    </td>
  );
}

function MachineEvaluationExpectedPayoutCell({ storeId, storeName, row, extraTitle = "" }) {
  const expectationDetail = readMachineEvaluationExpectationDetail(
    storeId,
    storeName,
    row,
    MACHINE_EVALUATION_HIGHLIGHT_METRIC_PAYOUT,
  );
  const className = getMachineEvaluationExpectedPayoutClassName(expectationDetail);
  const title = combineTitleParts(
    buildMachineEvaluationExpectationTitle(
      expectationDetail,
      MACHINE_EVALUATION_HIGHLIGHT_METRIC_PAYOUT,
    ),
    extraTitle,
  );
  return (
    <td
      className={className || undefined}
      title={title || undefined}
      data-sort-value={expectationDetail?.payoutRate ?? ""}
    >
      {formatExpectedPayoutRate(expectationDetail?.payoutRate)}
    </td>
  );
}

function MachineEvaluationExpectedRbCell({ storeId, storeName, row, extraTitle = "" }) {
  const isNeoAim = isNeoAimMachineName(row?.machineName);
  const metric = isNeoAim
    ? MACHINE_EVALUATION_HIGHLIGHT_METRIC_RB
    : MACHINE_EVALUATION_HIGHLIGHT_METRIC_PAYOUT;
  const expectationDetail = readMachineEvaluationExpectationDetail(storeId, storeName, row, metric);
  const rbDenominator = readDisplayableExpectedRbDenominator(expectationDetail);
  const rbExpectationDetail = rbDenominator === null ? null : expectationDetail;
  const className = getMachineEvaluationExpectedRbClassName(rbExpectationDetail, row?.machineName);
  const title = combineTitleParts(
    buildMachineEvaluationExpectationTitle(
      rbExpectationDetail,
      isNeoAim ? MACHINE_EVALUATION_HIGHLIGHT_METRIC_RB : "",
    ),
    extraTitle,
  );
  return (
    <td
      className={className || undefined}
      title={title || undefined}
      data-sort-value={rbDenominator ?? ""}
    >
      {formatExpectedRbDenominator(rbDenominator)}
    </td>
  );
}

function isRankingConditionHighlighted(row, highlightCondition) {
  if (!highlightCondition) {
    return false;
  }

  if (
    !highlightCondition.machineRankFilter.hasRankFilter &&
    !highlightCondition.selectedRankFilter.hasRankFilter &&
    !highlightCondition.scoreFilter.hasScoreFilter &&
    !highlightCondition.nextGapFilter.hasNextGapFilter
  ) {
    return false;
  }

  const nextGapValue = readNextGapForRankScope(
    row,
    normalizeNextGapScope(highlightCondition.nextGapScope),
  );
  return matchesRequiredConditionFilters(
    [
      {
        rankValue: readRankForScope(row, "machine"),
        rankFilter: highlightCondition.machineRankFilter,
        required: highlightCondition.requirementOptions.machineRankRequired,
      },
      {
        rankValue: readRankForScope(row, "selected"),
        rankFilter: highlightCondition.selectedRankFilter,
        required: highlightCondition.requirementOptions.selectedRankRequired,
      },
    ],
    row.huntScore,
    null,
    highlightCondition.scoreFilter,
    highlightCondition.requirementOptions,
    false,
    nextGapValue,
    highlightCondition.nextGapFilter,
  );
}

function getRankingConditionHighlightClass(row, highlightCondition, bookmarkMatchByRowKey = null) {
  if (bookmarkMatchByRowKey) {
    return bookmarkMatchByRowKey.get(buildHuntBacktestBookmarkRowKey(row))
      ? "huntScoreConditionHighlighted"
      : undefined;
  }

  return isRankingConditionHighlighted(row, highlightCondition)
    ? "huntScoreConditionHighlighted"
    : undefined;
}

function buildSelectedRankValueMap(displayGroups) {
  const rows = (Array.isArray(displayGroups) ? displayGroups : [])
    .flatMap((group) => getRankingGroupRows(group, true))
    .sort(compareRankingRows);

  return new Map(
    rows.map((row, index) => [
      buildRankGapRowKey(row),
      row?.selectedRank ?? index + 1,
    ]),
  );
}

function decorateRowsWithSelectedRank(rows, selectedRankValueMap) {
  return rows.map((row) => ({
    ...row,
    selectedRank: selectedRankValueMap.has(buildRankGapRowKey(row))
      ? selectedRankValueMap.get(buildRankGapRowKey(row))
      : null,
  }));
}

const RANKING_TABLE_COLLATOR = new Intl.Collator("ja", {
  numeric: true,
  sensitivity: "base",
});

function readSortableTableNumber(value) {
  const text = String(value ?? "").trim();
  if (!text || text === "-") {
    return null;
  }

  const normalizedText = text.replace(/,/g, "").replace(/%$/u, "");
  const ratioMatch = normalizedText.match(/^1\s*\/\s*([+-]?\d+(?:\.\d+)?)$/u);
  if (ratioMatch) {
    const ratioValue = Number(ratioMatch[1]);
    return Number.isFinite(ratioValue) ? ratioValue : null;
  }

  const parsedValue = Number(normalizedText);
  return Number.isFinite(parsedValue) ? parsedValue : null;
}

function readSortableResultColumnValue(row, column) {
  const type = column.type ?? "number";
  const rawValue = typeof column.sortValue === "function"
    ? column.sortValue(row)
    : column.render(row);

  return {
    missing: false,
    value: type === "text" ? String(rawValue ?? "") : readSortableTableNumber(rawValue),
    type,
  };
}

function readSortableTableValue(
  row,
  columnIndex,
  nextGapScope,
  {
    hasMachineEvaluationColumn = false,
    hasDaySpecificMachineEvaluationColumn = false,
    visibleColumns = [],
    includeStoreColumn = false,
    includeMachineColumn = true,
    storeId = "",
    storeName = "",
  } = {},
) {
  if (columnIndex === SORT_COLUMN_INDEX.common) {
    return { missing: false, value: readSortableTableNumber(row.huntScore), type: "number" };
  }

  if (hasMachineEvaluationColumn && columnIndex === SORT_COLUMN_INDEX.machineEvaluation) {
    return {
      missing: false,
      value: readSortableTableNumber(row.machineEvaluation?.score),
      type: "number",
    };
  }

  if (
    hasDaySpecificMachineEvaluationColumn &&
    columnIndex === SORT_COLUMN_INDEX.daySpecificMachineEvaluation
  ) {
    return {
      missing: false,
      value: readSortableTableNumber(row.machineEvaluationDaySpecific?.score),
      type: "number",
    };
  }

  if (hasMachineEvaluationColumn && columnIndex === SORT_COLUMN_INDEX.condition) {
    return {
      missing: false,
      value: readMatchedConditionCount(row),
      type: "number",
    };
  }

  if (hasMachineEvaluationColumn && columnIndex === SORT_COLUMN_INDEX.expectedPayout) {
    return {
      missing: false,
      value:
        readMachineEvaluationExpectationDetail(
          storeId,
          storeName,
          row,
          MACHINE_EVALUATION_HIGHLIGHT_METRIC_PAYOUT,
        )?.payoutRate ?? null,
      type: "number",
    };
  }

  if (hasMachineEvaluationColumn && columnIndex === SORT_COLUMN_INDEX.expectedRb) {
    const expectedRbMetric = isNeoAimMachineName(row?.machineName)
      ? MACHINE_EVALUATION_HIGHLIGHT_METRIC_RB
      : MACHINE_EVALUATION_HIGHLIGHT_METRIC_PAYOUT;
    const expectationDetail = readMachineEvaluationExpectationDetail(
      storeId,
      storeName,
      row,
      expectedRbMetric,
    );
    return {
      missing: false,
      value: readDisplayableExpectedRbDenominator(expectationDetail),
      type: "number",
    };
  }

  if (columnIndex === SORT_COLUMN_INDEX.nextGap) {
    return {
      missing: false,
      value: readSortableTableNumber(
        readNextGapForRankScope(row, normalizeNextGapScope(nextGapScope)),
      ),
      type: "number",
    };
  }

  if (includeStoreColumn && columnIndex === SORT_COLUMN_INDEX.storeName) {
    return { missing: false, value: String(row.storeName ?? ""), type: "text" };
  }

  if (includeMachineColumn && columnIndex === SORT_COLUMN_INDEX.machineName) {
    return { missing: false, value: String(row.machineName ?? ""), type: "text" };
  }

  if (columnIndex === SORT_COLUMN_INDEX.slot) {
    return { missing: false, value: String(row.slotNumber ?? ""), type: "text" };
  }

  const resultColumn = visibleColumns[columnIndex - SORT_COLUMN_INDEX.resultStart];
  if (resultColumn) {
    return readSortableResultColumnValue(row, resultColumn);
  }

  return { missing: true, value: null, type: "number" };
}

function compareSortableTableRows(
  leftEntry,
  rightEntry,
  sortState,
  nextGapScope,
  options = {},
) {
  const leftValue = readSortableTableValue(
    leftEntry.row,
    sortState.columnIndex,
    nextGapScope,
    options,
  );
  const rightValue = readSortableTableValue(
    rightEntry.row,
    sortState.columnIndex,
    nextGapScope,
    options,
  );
  const leftMissing = leftValue.missing || leftValue.value === null || leftValue.value === "";
  const rightMissing = rightValue.missing || rightValue.value === null || rightValue.value === "";

  if (leftMissing && rightMissing) {
    return leftEntry.originalIndex - rightEntry.originalIndex;
  }
  if (leftMissing) {
    return 1;
  }
  if (rightMissing) {
    return -1;
  }

  const baseResult = sortState.type === "text"
    ? RANKING_TABLE_COLLATOR.compare(leftValue.value, rightValue.value)
    : leftValue.value - rightValue.value;

  if (baseResult === 0) {
    return leftEntry.originalIndex - rightEntry.originalIndex;
  }

  return sortState.direction === "asc" ? baseResult : -baseResult;
}

function HuntScoreCell({
  storeId,
  storeName,
  row,
  highlightCondition,
  bookmarkMatchByRowKey = null,
  extraTitle = "",
  sortable = false,
  huntScoreLogicLabel = "",
}) {
  const backtestResult = readCommonHuntScoreMachineTopBacktestResult(storeId, storeName, row);
  const backtestPayoutClass = getMachineEvaluationPayoutClass(backtestResult?.payoutRate);
  const backtestTitle = buildCommonHuntScoreBacktestTitleParts(
    backtestResult,
    huntScoreLogicLabel,
  ).join("\n");
  const className = [
    getRankingConditionHighlightClass(row, highlightCondition, bookmarkMatchByRowKey),
    backtestPayoutClass ? "huntScoreBacktestMatchedCell" : "",
    backtestPayoutClass,
  ].filter(Boolean).join(" ");
  const sortProps = sortable
    ? { "data-sort-value": readRankingSortNumber(row.huntScore, "") }
    : {};

  return (
    <td
      className={className || undefined}
      title={combineTitleParts(backtestTitle, extraTitle) || undefined}
      {...sortProps}
    >
      <span className="huntScoreBacktestCellValue">{formatNumber(row.huntScore)}</span>
    </td>
  );
}

function OverallRankingTable({
  storeId,
  storeName,
  sectionLabel = "狙い度上位",
  title,
  rows,
  visibleColumns,
  visibleFixedColumnKeys = DEFAULT_VISIBLE_FIXED_COLUMN_KEYS,
  scoreColumnLabel,
  dateFlowLabel,
  nextGapScope,
  highlightCondition,
  bookmarkMatchByRowKey = null,
  sortable = false,
  tableId = "",
  showStoreColumn = false,
  showMachineEvaluation = false,
  huntScoreLogicLabel = "",
}) {
  const hasMachineEvaluationColumn =
    showMachineEvaluation && rows.some((row) => row?.machineEvaluation);
  const hasDaySpecificMachineEvaluationColumn =
    hasMachineEvaluationColumn && rows.some((row) => row?.machineEvaluationDaySpecific);
  const daySpecificMachineEvaluationColumnLabel =
    readDaySpecificMachineEvaluationColumnLabel(rows);
  const visibleFixedColumnSet = useMemo(
    () => new Set(visibleFixedColumnKeys),
    [visibleFixedColumnKeys],
  );
  const showCommonColumn = visibleFixedColumnSet.has(FIXED_COLUMN_KEYS.common);
  const showMachineEvaluationColumn =
    hasMachineEvaluationColumn && visibleFixedColumnSet.has(FIXED_COLUMN_KEYS.machineEvaluation);
  const showDaySpecificMachineEvaluationColumn =
    showMachineEvaluationColumn && hasDaySpecificMachineEvaluationColumn;
  const showConditionColumn =
    hasMachineEvaluationColumn && visibleFixedColumnSet.has(FIXED_COLUMN_KEYS.condition);
  const showExpectedPayoutColumn =
    hasMachineEvaluationColumn && visibleFixedColumnSet.has(FIXED_COLUMN_KEYS.expectedPayout);
  const showExpectedRbColumn =
    hasMachineEvaluationColumn && visibleFixedColumnSet.has(FIXED_COLUMN_KEYS.expectedRb);
  const showNextGapColumn = visibleFixedColumnSet.has(FIXED_COLUMN_KEYS.nextGap);
  const showStoreNameColumn =
    showStoreColumn && visibleFixedColumnSet.has(FIXED_COLUMN_KEYS.storeName);
  const showMachineNameColumn = visibleFixedColumnSet.has(FIXED_COLUMN_KEYS.machineName);
  const [sortState, setSortState] = useState(() =>
    sortable
      ? {
          columnIndex: SORT_COLUMN_INDEX.common,
          direction: "desc",
          type: "number",
        }
      : null,
  );
  const sortedRows = useMemo(() => {
    if (!sortable || !sortState) {
      return rows;
    }

    return rows
      .map((row, originalIndex) => ({ row, originalIndex }))
      .sort((left, right) =>
        compareSortableTableRows(
          left,
          right,
          sortState,
          nextGapScope,
          {
            hasMachineEvaluationColumn,
            hasDaySpecificMachineEvaluationColumn,
            visibleColumns,
            includeStoreColumn: showStoreColumn,
            includeMachineColumn: true,
            storeId,
            storeName,
          },
        ),
      )
      .map((entry) => entry.row);
  }, [
    hasDaySpecificMachineEvaluationColumn,
    hasMachineEvaluationColumn,
    nextGapScope,
    rows,
    showStoreColumn,
    sortState,
    sortable,
    storeId,
    storeName,
    visibleColumns,
  ]);
  const tableProps = tableId ? { id: tableId } : {};
  const handleSort = (columnIndex, type, initialDirection) => {
    if (!sortable) {
      return;
    }

    setSortState((currentState) => {
      const nextDirection =
        currentState?.columnIndex === columnIndex && currentState.direction === "desc"
          ? "asc"
          : currentState?.columnIndex === columnIndex && currentState.direction === "asc"
            ? "desc"
            : initialDirection;

      return {
        columnIndex,
        direction: nextDirection,
        type,
      };
    });
  };
  const HeaderCell = ({
    children,
    columnIndex,
    type = "number",
    initialDirection = "desc",
    className = "",
    title: headerTitle = undefined,
  }) => {
    const activeDirection =
      sortable && sortState?.columnIndex === columnIndex ? sortState.direction : null;

    return sortable ? (
        <SortableTableHeader
          columnIndex={columnIndex}
          type={type}
          initialDirection={initialDirection}
          className={className}
          activeDirection={activeDirection}
          onSort={() => handleSort(columnIndex, type, initialDirection)}
          title={headerTitle}
        >
          {children}
        </SortableTableHeader>
      ) : (
        <th className={className || undefined} title={headerTitle}>{children}</th>
      );
  };
  const scoreColumnIndex = SORT_COLUMN_INDEX.common;
  const machineEvaluationColumnIndex = SORT_COLUMN_INDEX.machineEvaluation;
  const daySpecificMachineEvaluationColumnIndex = SORT_COLUMN_INDEX.daySpecificMachineEvaluation;
  const conditionColumnIndex = SORT_COLUMN_INDEX.condition;
  const expectedPayoutColumnIndex = SORT_COLUMN_INDEX.expectedPayout;
  const expectedRbColumnIndex = SORT_COLUMN_INDEX.expectedRb;
  const nextGapColumnIndex = SORT_COLUMN_INDEX.nextGap;
  const storeColumnIndex = SORT_COLUMN_INDEX.storeName;
  const machineColumnIndex = SORT_COLUMN_INDEX.machineName;
  const slotColumnIndex = SORT_COLUMN_INDEX.slot;
  const resultColumnStartIndex = SORT_COLUMN_INDEX.resultStart;

  return (
    <section className="tablePanel directoryPanel">
      <div className="tablePanelHeader">
        <div>
          <p className="sectionLabel">{sectionLabel}</p>
          <h2 className="tablePanelTitle">
            <span>{title}</span>
            {dateFlowLabel ? <span className="tablePanelDateFlow">{dateFlowLabel}</span> : null}
          </h2>
        </div>
      </div>
      <div className="tableScroller directoryScroller">
        <table className="directoryTable huntCompactTable huntRankingTable" {...tableProps}>
          <thead>
            <tr>
              {showCommonColumn ? (
                <HeaderCell columnIndex={scoreColumnIndex}>{scoreColumnLabel}</HeaderCell>
              ) : null}
              {showMachineEvaluationColumn ? (
                <HeaderCell columnIndex={machineEvaluationColumnIndex} title="機種別評価">
                  機種別
                </HeaderCell>
              ) : null}
              {showDaySpecificMachineEvaluationColumn ? (
                <HeaderCell
                  columnIndex={daySpecificMachineEvaluationColumnIndex}
                  title={`${daySpecificMachineEvaluationColumnLabel}専用評価`}
                >
                  {daySpecificMachineEvaluationColumnLabel}
                </HeaderCell>
              ) : null}
              {showConditionColumn ? (
                <HeaderCell
                  columnIndex={conditionColumnIndex}
                  title="一致した採用条件の件数"
                >
                  条件
                </HeaderCell>
              ) : null}
              {showExpectedPayoutColumn ? (
                <HeaderCell
                  columnIndex={expectedPayoutColumnIndex}
                  title="ホバー表示に使う目安のうち、最も高い機械割"
                >
                  期待割
                </HeaderCell>
              ) : null}
              {showExpectedRbColumn ? (
                <HeaderCell
                  columnIndex={expectedRbColumnIndex}
                  initialDirection="asc"
                  title="期待割に採用した目安のRB分母"
                >
                  期待RB
                </HeaderCell>
              ) : null}
              {showNextGapColumn ? (
                <HeaderCell columnIndex={nextGapColumnIndex}>次点差</HeaderCell>
              ) : null}
              {showStoreNameColumn ? (
                <HeaderCell
                  columnIndex={storeColumnIndex}
                  type="text"
                  initialDirection="asc"
                  className="directoryNameHeader crossStoreHuntStoreHeader"
                >
                  店舗名
                </HeaderCell>
              ) : null}
              {showMachineNameColumn ? (
                <HeaderCell
                  columnIndex={machineColumnIndex}
                  type="text"
                  initialDirection="asc"
                  className="directoryNameHeader"
                >
                  機種名
                </HeaderCell>
              ) : null}
              <HeaderCell columnIndex={slotColumnIndex} type="text" initialDirection="asc">台番</HeaderCell>
              {visibleColumns.map((column, index) => (
                <HeaderCell
                  key={column.key}
                  columnIndex={resultColumnStartIndex + index}
                  type={column.type ?? "number"}
                >
                  {column.label}
                </HeaderCell>
              ))}
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((row) => {
              const settingEstimateCellClassName = getSettingEstimateHighlightClass(
                row.nextSettingEstimate?.average,
              );
              const rowStoreId = String(row.storeId ?? storeId ?? "").trim();
              const rowStoreName = String(row.storeName ?? storeName ?? "").trim();
              const machineHasSite7Data = Boolean(row.predictionMachineHasSite7Data);
              const machineSite7FetchedAt = row.predictionMachineSite7FetchedAt ?? null;
              const machineFullName = String(row.machineName ?? "").trim();
              const machineShortName = getHuntMachineShortName(machineFullName);
              const rowSite7Title = buildRankingRowSite7Title(row);
              const machineTitle = machineHasSite7Data
                ? site7BadgeTitle(
                    machineSite7FetchedAt,
                    `${machineFullName}\nこの機種にSセブン暫定データが含まれます`,
                  )
                : machineFullName;
              const machineCellTitle = combineTitleParts(machineTitle, rowSite7Title);
              const slotExpectedPayoutClassName = getMachineEvaluationExpectationCellClassName(
                rowStoreId,
                rowStoreName,
                row,
              );

              return (
                <tr
                  key={`${row.rowKey ?? row.machineName}-${row.slotNumber}-${title}-${row.rank}`}
                  title={rowSite7Title || undefined}
                >
                  {showCommonColumn ? (
                    <HuntScoreCell
                      storeId={rowStoreId}
                      storeName={rowStoreName}
                      row={row}
                      highlightCondition={highlightCondition}
                      bookmarkMatchByRowKey={bookmarkMatchByRowKey}
                      extraTitle={rowSite7Title}
                      sortable
                      huntScoreLogicLabel={huntScoreLogicLabel}
                    />
                  ) : null}
                  {showMachineEvaluationColumn ? (
                    <MachineEvaluationCell
                      storeId={rowStoreId}
                      storeName={rowStoreName}
                      row={row}
                      evaluation={row.machineEvaluation}
                      extraTitle={rowSite7Title}
                    />
                  ) : null}
                  {showDaySpecificMachineEvaluationColumn ? (
                    <MachineEvaluationCell
                      storeId={rowStoreId}
                      storeName={rowStoreName}
                      row={row}
                      evaluation={row.machineEvaluationDaySpecific}
                      extraTitle={rowSite7Title}
                      includeStoredTopBacktests={false}
                    />
                  ) : null}
                  {showConditionColumn ? (
                    <MachineEvaluationConditionCell
                      row={row}
                      extraTitle={rowSite7Title}
                    />
                  ) : null}
                  {showExpectedPayoutColumn ? (
                    <MachineEvaluationExpectedPayoutCell
                      storeId={rowStoreId}
                      storeName={rowStoreName}
                      row={row}
                      extraTitle={rowSite7Title}
                    />
                  ) : null}
                  {showExpectedRbColumn ? (
                    <MachineEvaluationExpectedRbCell
                      storeId={rowStoreId}
                      storeName={rowStoreName}
                      row={row}
                      extraTitle={rowSite7Title}
                    />
                  ) : null}
                  {showNextGapColumn ? (
                    <td
                      data-sort-value={readRankingSortNumber(
                        readNextGapForRankScope(row, normalizeNextGapScope(nextGapScope)),
                        "",
                      )}
                      title={rowSite7Title || undefined}
                    >
                      {formatNextGapForScope(row, nextGapScope)}
                    </td>
                  ) : null}
                  {showStoreNameColumn ? (
                    <th
                      className="directoryNameCell crossStoreHuntStoreCell"
                      data-sort-value={rowStoreName}
                      title={rowStoreName || undefined}
                    >
                      <StoreNameLinkOrText storeId={rowStoreId} storeName={rowStoreName} />
                    </th>
                  ) : null}
                  {showMachineNameColumn ? (
                    <th
                      className={[
                        "directoryNameCell",
                        machineHasSite7Data ? "site7MachineCell" : "",
                      ].filter(Boolean).join(" ")}
                      title={machineCellTitle || undefined}
                      data-sort-value={row.machineName}
                    >
                      <span className="directoryNameContent">
                        {rowStoreId ? (
                          <Link
                            href={`/stores/${rowStoreId}/machines/${encodeURIComponent(row.machineName)}`}
                            className="directoryPrimaryLink"
                          >
                            {machineShortName}
                          </Link>
                        ) : (
                          <span>{machineShortName}</span>
                        )}
                        {machineHasSite7Data ? (
                          <Site7RankingBadge
                            fetchedAt={machineSite7FetchedAt}
                            title="この機種にSセブン暫定データが含まれます"
                          />
                        ) : null}
                      </span>
                    </th>
                  ) : null}
                  <td
                    className={slotExpectedPayoutClassName || undefined}
                    data-sort-value={row.slotNumber}
                    title={rowSite7Title || undefined}
                  >
                    {row.slotNumber}
                  </td>
                  {visibleColumns.map((column) => (
                    <td
                      key={`${row.machineName}-${row.slotNumber}-${title}-${column.key}`}
                      className={
                        getSettingEstimateResultCellClassName(
                          column,
                          settingEstimateCellClassName,
                        ) || undefined
                      }
                      title={rowSite7Title || undefined}
                    >
                      {column.render(row)}
                    </td>
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function MachineRankingGroupTable({
  group,
  storeId,
  storeName,
  visibleColumns,
  visibleFixedColumnKeys = DEFAULT_VISIBLE_FIXED_COLUMN_KEYS,
  scoreColumnLabel,
  dateFlowLabel,
  nextGapScope,
  highlightCondition,
  bookmarkMatchByRowKey = null,
  showMachineEvaluation = false,
  showStoreColumn = false,
  huntScoreLogicLabel = "",
}) {
  const groupHasSite7Data = group.rows.some((row) => row.predictionMachineHasSite7Data);
  const groupSite7FetchedAt = latestSite7FetchedAtFromRows(group.rows);
  const hasMachineEvaluationColumn =
    showMachineEvaluation && group.rows.some((row) => row?.machineEvaluation);
  const hasDaySpecificMachineEvaluationColumn =
    hasMachineEvaluationColumn && group.rows.some((row) => row?.machineEvaluationDaySpecific);
  const daySpecificMachineEvaluationColumnLabel =
    readDaySpecificMachineEvaluationColumnLabel(group.rows);
  const visibleFixedColumnSet = useMemo(
    () => new Set(visibleFixedColumnKeys),
    [visibleFixedColumnKeys],
  );
  const showCommonColumn = visibleFixedColumnSet.has(FIXED_COLUMN_KEYS.common);
  const showMachineEvaluationColumn =
    hasMachineEvaluationColumn && visibleFixedColumnSet.has(FIXED_COLUMN_KEYS.machineEvaluation);
  const showDaySpecificMachineEvaluationColumn =
    showMachineEvaluationColumn && hasDaySpecificMachineEvaluationColumn;
  const showConditionColumn =
    hasMachineEvaluationColumn && visibleFixedColumnSet.has(FIXED_COLUMN_KEYS.condition);
  const showExpectedPayoutColumn =
    hasMachineEvaluationColumn && visibleFixedColumnSet.has(FIXED_COLUMN_KEYS.expectedPayout);
  const showExpectedRbColumn =
    hasMachineEvaluationColumn && visibleFixedColumnSet.has(FIXED_COLUMN_KEYS.expectedRb);
  const showNextGapColumn = visibleFixedColumnSet.has(FIXED_COLUMN_KEYS.nextGap);
  const showStoreNameColumn =
    showStoreColumn && visibleFixedColumnSet.has(FIXED_COLUMN_KEYS.storeName);
  const [sortState, setSortState] = useState(() => ({
    columnIndex: SORT_COLUMN_INDEX.common,
    direction: "desc",
    type: "number",
  }));
  const sortedRows = useMemo(
    () =>
      group.rows
        .map((row, originalIndex) => ({ row, originalIndex }))
        .sort((left, right) =>
          compareSortableTableRows(
            left,
            right,
            sortState,
            nextGapScope,
            {
              hasMachineEvaluationColumn,
              hasDaySpecificMachineEvaluationColumn,
              visibleColumns,
              includeStoreColumn: showStoreColumn,
              includeMachineColumn: false,
              storeId,
              storeName,
            },
          ),
        )
        .map((entry) => entry.row),
    [
      group.rows,
      hasDaySpecificMachineEvaluationColumn,
      hasMachineEvaluationColumn,
      nextGapScope,
      showStoreColumn,
      sortState,
      storeId,
      storeName,
      visibleColumns,
    ],
  );
  const handleSort = (columnIndex, type, initialDirection) => {
    setSortState((currentState) => {
      const nextDirection =
        currentState?.columnIndex === columnIndex && currentState.direction === "desc"
          ? "asc"
          : currentState?.columnIndex === columnIndex && currentState.direction === "asc"
            ? "desc"
            : initialDirection;

      return {
        columnIndex,
        direction: nextDirection,
        type,
      };
    });
  };
  const HeaderCell = ({
    children,
    columnIndex,
    type = "number",
    initialDirection = "desc",
    className = "",
    title: headerTitle = undefined,
  }) => {
    const activeDirection = sortState?.columnIndex === columnIndex ? sortState.direction : null;

    return (
      <SortableTableHeader
        columnIndex={columnIndex}
        type={type}
        initialDirection={initialDirection}
        className={className}
        activeDirection={activeDirection}
        onSort={() => handleSort(columnIndex, type, initialDirection)}
        title={headerTitle}
      >
        {children}
      </SortableTableHeader>
    );
  };
  const scoreColumnIndex = SORT_COLUMN_INDEX.common;
  const machineEvaluationColumnIndex = SORT_COLUMN_INDEX.machineEvaluation;
  const daySpecificMachineEvaluationColumnIndex = SORT_COLUMN_INDEX.daySpecificMachineEvaluation;
  const conditionColumnIndex = SORT_COLUMN_INDEX.condition;
  const expectedPayoutColumnIndex = SORT_COLUMN_INDEX.expectedPayout;
  const expectedRbColumnIndex = SORT_COLUMN_INDEX.expectedRb;
  const nextGapColumnIndex = SORT_COLUMN_INDEX.nextGap;
  const storeColumnIndex = SORT_COLUMN_INDEX.storeName;
  const slotColumnIndex = SORT_COLUMN_INDEX.slot;
  const resultColumnStartIndex = SORT_COLUMN_INDEX.resultStart;

  return (
    <section className="tablePanel directoryPanel">
      <div className="tablePanelHeader">
        <div>
          <p className="sectionLabel">狙い度上位</p>
          <h2 className="tablePanelTitle">
            <span>
              {group.isCombinedGroup || !storeId ? (
                <span>{group.machineName}</span>
              ) : (
                <Link
                  href={`/stores/${storeId}/machines/${encodeURIComponent(group.machineName)}`}
                  className="directoryPrimaryLink"
                >
                  {group.machineName}
                </Link>
              )}
              {groupHasSite7Data ? (
                <Site7RankingBadge
                  fetchedAt={groupSite7FetchedAt}
                  title="この機種にSセブン暫定データが含まれます"
                />
              ) : null}
              {` 上位${formatNumber(group.rows.length)}台`}
            </span>
            <span className="tablePanelDateFlow">{dateFlowLabel}</span>
          </h2>
        </div>
      </div>
      <div className="tableScroller directoryScroller">
        <table className="directoryTable huntCompactTable huntRankingTable">
          <thead>
            <tr>
              {showCommonColumn ? (
                <HeaderCell columnIndex={scoreColumnIndex}>{scoreColumnLabel}</HeaderCell>
              ) : null}
              {showMachineEvaluationColumn ? (
                <HeaderCell columnIndex={machineEvaluationColumnIndex} title="機種別評価">
                  機種別
                </HeaderCell>
              ) : null}
              {showDaySpecificMachineEvaluationColumn ? (
                <HeaderCell
                  columnIndex={daySpecificMachineEvaluationColumnIndex}
                  title={`${daySpecificMachineEvaluationColumnLabel}専用評価`}
                >
                  {daySpecificMachineEvaluationColumnLabel}
                </HeaderCell>
              ) : null}
              {showConditionColumn ? (
                <HeaderCell
                  columnIndex={conditionColumnIndex}
                  title="一致した採用条件の件数"
                >
                  条件
                </HeaderCell>
              ) : null}
              {showExpectedPayoutColumn ? (
                <HeaderCell
                  columnIndex={expectedPayoutColumnIndex}
                  title="ホバー表示に使う目安のうち、最も高い機械割"
                >
                  期待割
                </HeaderCell>
              ) : null}
              {showExpectedRbColumn ? (
                <HeaderCell
                  columnIndex={expectedRbColumnIndex}
                  initialDirection="asc"
                  title="期待割に採用した目安のRB分母"
                >
                  期待RB
                </HeaderCell>
              ) : null}
              {showNextGapColumn ? (
                <HeaderCell columnIndex={nextGapColumnIndex}>次点差</HeaderCell>
              ) : null}
              {showStoreNameColumn ? (
                <HeaderCell
                  columnIndex={storeColumnIndex}
                  type="text"
                  initialDirection="asc"
                  className="directoryNameHeader crossStoreHuntStoreHeader"
                >
                  店舗名
                </HeaderCell>
              ) : null}
              <HeaderCell columnIndex={slotColumnIndex} type="text" initialDirection="asc">
                台番
              </HeaderCell>
              {visibleColumns.map((column, index) => (
                <HeaderCell
                  key={column.key}
                  columnIndex={resultColumnStartIndex + index}
                  type={column.type ?? "number"}
                >
                  {column.label}
                </HeaderCell>
              ))}
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((row) => {
              const settingEstimateCellClassName = getSettingEstimateHighlightClass(
                row.nextSettingEstimate?.average,
              );
              const rowStoreId = String(row.storeId ?? storeId ?? "").trim();
              const rowStoreName = String(row.storeName ?? storeName ?? "").trim();
              const rowSite7Title = buildRankingRowSite7Title(row);
              const slotExpectedPayoutClassName = getMachineEvaluationExpectationCellClassName(
                rowStoreId,
                rowStoreName,
                row,
              );

              return (
                <tr
                  key={`${row.rowKey ?? row.machineName}-${row.slotNumber}-${row.rank}`}
                  title={rowSite7Title || undefined}
                >
                  {showCommonColumn ? (
                    <HuntScoreCell
                      storeId={rowStoreId}
                      storeName={rowStoreName}
                      row={row}
                      highlightCondition={highlightCondition}
                      bookmarkMatchByRowKey={bookmarkMatchByRowKey}
                      extraTitle={rowSite7Title}
                      sortable
                      huntScoreLogicLabel={huntScoreLogicLabel}
                    />
                  ) : null}
                  {showMachineEvaluationColumn ? (
                    <MachineEvaluationCell
                      storeId={rowStoreId}
                      storeName={rowStoreName}
                      row={row}
                      evaluation={row.machineEvaluation}
                      extraTitle={rowSite7Title}
                    />
                  ) : null}
                  {showDaySpecificMachineEvaluationColumn ? (
                    <MachineEvaluationCell
                      storeId={rowStoreId}
                      storeName={rowStoreName}
                      row={row}
                      evaluation={row.machineEvaluationDaySpecific}
                      extraTitle={rowSite7Title}
                      includeStoredTopBacktests={false}
                    />
                  ) : null}
                  {showConditionColumn ? (
                    <MachineEvaluationConditionCell
                      row={row}
                      extraTitle={rowSite7Title}
                    />
                  ) : null}
                  {showExpectedPayoutColumn ? (
                    <MachineEvaluationExpectedPayoutCell
                      storeId={rowStoreId}
                      storeName={rowStoreName}
                      row={row}
                      extraTitle={rowSite7Title}
                    />
                  ) : null}
                  {showExpectedRbColumn ? (
                    <MachineEvaluationExpectedRbCell
                      storeId={rowStoreId}
                      storeName={rowStoreName}
                      row={row}
                      extraTitle={rowSite7Title}
                    />
                  ) : null}
                  {showNextGapColumn ? (
                    <td title={rowSite7Title || undefined}>
                      {formatNextGapForScope(row, nextGapScope)}
                    </td>
                  ) : null}
                  {showStoreNameColumn ? (
                    <th
                      className="directoryNameCell crossStoreHuntStoreCell"
                      data-sort-value={rowStoreName}
                      title={rowStoreName || undefined}
                    >
                      <StoreNameLinkOrText storeId={rowStoreId} storeName={rowStoreName} />
                    </th>
                  ) : null}
                  <td
                    className={slotExpectedPayoutClassName || undefined}
                    title={rowSite7Title || undefined}
                  >
                    {row.slotNumber}
                  </td>
                  {visibleColumns.map((column) => (
                    <td
                      key={`${row.machineName}-${row.slotNumber}-${column.key}`}
                      className={
                        getSettingEstimateResultCellClassName(
                          column,
                          settingEstimateCellClassName,
                        ) || undefined
                      }
                      title={rowSite7Title || undefined}
                    >
                      {column.render(row)}
                    </td>
                  ))}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export function HuntRankingTable({
  storeId,
  storeName = "",
  rows = [],
  rankingGroups = [],
  overallLimit = 20,
  predictionDate = null,
  actualDate = null,
  highlightOptions = {},
  customHighlightBookmark = null,
  enableConditionHighlight = true,
  initialDifferenceMode = DEFAULT_DIFFERENCE_MODE,
  showMachineTopCandidates = false,
  showStoreColumn = false,
  showMachineEvaluation = false,
  showGrapeColumn = false,
  showOverallRanking = true,
  showMachineGroupTables = true,
  dateFlowLabelOverride = "",
  huntScoreLogicLabel = "",
}) {
  const defaultVisibleFixedColumnKeys = useMemo(
    () =>
      showStoreColumn
        ? [
            FIXED_COLUMN_KEYS.common,
            FIXED_COLUMN_KEYS.storeName,
            FIXED_COLUMN_KEYS.machineEvaluation,
            FIXED_COLUMN_KEYS.condition,
            FIXED_COLUMN_KEYS.expectedPayout,
            FIXED_COLUMN_KEYS.expectedRb,
            FIXED_COLUMN_KEYS.machineName,
          ]
        : DEFAULT_VISIBLE_FIXED_COLUMN_KEYS,
    [showStoreColumn],
  );
  const [visibleResultKeys, setVisibleResultKeys] = useState(DEFAULT_VISIBLE_RESULT_KEYS);
  const [visibleFixedColumnKeys, setVisibleFixedColumnKeys] = useState(
    defaultVisibleFixedColumnKeys,
  );
  const [differenceMode, setDifferenceMode] = useState(() =>
    normalizeDifferenceMode(initialDifferenceMode),
  );
  const [bookmarks, setBookmarks] = useState([]);
  const [bookmarkSelection, setBookmarkSelection] = useState(HUNT_BACKTEST_BOOKMARK_SELECTION_CUSTOM);

  useEffect(() => {
    setDifferenceMode(normalizeDifferenceMode(initialDifferenceMode));
  }, [initialDifferenceMode]);

  useEffect(() => {
    setVisibleFixedColumnKeys((currentKeys) => {
      const currentKeySet = new Set(currentKeys);
      if (showStoreColumn) {
        currentKeySet.add(FIXED_COLUMN_KEYS.storeName);
      } else {
        currentKeySet.delete(FIXED_COLUMN_KEYS.storeName);
      }
      return Object.values(FIXED_COLUMN_KEYS).filter((key) => currentKeySet.has(key));
    });
  }, [showStoreColumn]);

  useEffect(() => {
    if (!showGrapeColumn) {
      return;
    }
    setVisibleResultKeys((currentKeys) =>
      currentKeys.includes(ESTIMATED_GRAPE_RESULT_KEY)
        ? currentKeys
        : [...currentKeys, ESTIMATED_GRAPE_RESULT_KEY],
    );
  }, [showGrapeColumn]);

  useEffect(() => {
    if (!enableConditionHighlight) {
      setBookmarks([]);
      setBookmarkSelection(HUNT_BACKTEST_BOOKMARK_SELECTION_CUSTOM);
      return undefined;
    }

    const syncBookmarks = () => {
      const nextBookmarks = readSavedHuntBacktestBookmarks(storeId);
      const selectedId = readSelectedHuntBacktestBookmarkId(storeId);
      setBookmarks(nextBookmarks);
      setBookmarkSelection(
        selectedId ||
          (nextBookmarks.length > 0
            ? nextBookmarks[0].id
            : HUNT_BACKTEST_BOOKMARK_SELECTION_CUSTOM),
      );
    };

    syncBookmarks();
    window.addEventListener(HUNT_BACKTEST_BOOKMARK_EVENT, syncBookmarks);
    window.addEventListener("storage", syncBookmarks);

    return () => {
      window.removeEventListener(HUNT_BACKTEST_BOOKMARK_EVENT, syncBookmarks);
      window.removeEventListener("storage", syncBookmarks);
    };
  }, [enableConditionHighlight, storeId]);

  const resultColumns = useMemo(
    () => buildResultColumns(differenceMode, showGrapeColumn),
    [differenceMode, showGrapeColumn],
  );
  const visibleColumns = useMemo(
    () => resultColumns.filter((column) => visibleResultKeys.includes(column.key)),
    [resultColumns, visibleResultKeys],
  );
  const scoreColumnLabel = "共通";
  const fixedColumnOptions = useMemo(
    () => [
      {
        key: FIXED_COLUMN_KEYS.common,
        label: scoreColumnLabel,
      },
      ...(showMachineEvaluation
        ? [
            {
              key: FIXED_COLUMN_KEYS.machineEvaluation,
              label: "機種別",
            },
            {
              key: FIXED_COLUMN_KEYS.condition,
              label: "条件",
            },
            {
              key: FIXED_COLUMN_KEYS.expectedPayout,
              label: "期待割",
            },
            {
              key: FIXED_COLUMN_KEYS.expectedRb,
              label: "期待RB",
            },
          ]
        : []),
      {
        key: FIXED_COLUMN_KEYS.nextGap,
        label: "次点差",
      },
      ...(showStoreColumn
        ? [
            {
              key: FIXED_COLUMN_KEYS.storeName,
              label: "店舗名",
            },
          ]
        : []),
      {
        key: FIXED_COLUMN_KEYS.machineName,
        label: "機種名",
      },
    ],
    [scoreColumnLabel, showMachineEvaluation, showStoreColumn],
  );
  const dateFlowLabel = useMemo(
    () => dateFlowLabelOverride || formatRankingDateFlowLabel(predictionDate, actualDate),
    [actualDate, dateFlowLabelOverride, predictionDate],
  );
  const nextGapScope = normalizeNextGapScope(highlightOptions.nextGapScope ?? DEFAULT_NEXT_GAP_SCOPE);
  const highlightCondition = useMemo(
    () => {
      const scopedRankFilters = buildScopedRankFilters({
        ...highlightOptions,
        selectedRankMin:
          highlightOptions.selectedRankMin ??
          highlightOptions.rankMin ??
          String(DEFAULT_HIGHLIGHT_RANK_MIN),
        selectedRankMax:
          highlightOptions.selectedRankMax ??
          highlightOptions.rankMax ??
          String(DEFAULT_HIGHLIGHT_RANK_MAX),
      });

      return {
        machineRankFilter: scopedRankFilters.machineRankFilter,
        selectedRankFilter: scopedRankFilters.selectedRankFilter,
        scoreFilter: buildScoreFilter(
          highlightOptions.scoreMin ?? String(DEFAULT_HIGHLIGHT_SCORE_MIN),
        ),
        nextGapFilter: buildNextGapFilter(highlightOptions.nextGapMin),
        requirementOptions: buildConditionRequirementOptions(highlightOptions, {
          rankRequired: true,
          machineRankRequired: false,
          selectedRankRequired: true,
          scoreRequired: true,
          nextGapRequired: false,
        }),
        rankScope: scopedRankFilters.rankScope,
        nextGapScope,
      };
    },
    [
      nextGapScope,
      highlightOptions.machineRankMax,
      highlightOptions.machineRankMin,
      highlightOptions.nextGapMin,
      highlightOptions.nextGapRequired,
      highlightOptions.nextGapScope,
      highlightOptions.rankMax,
      highlightOptions.rankMin,
      highlightOptions.rankRequired,
      highlightOptions.rankScope,
      highlightOptions.machineRankRequired,
      highlightOptions.selectedRankMax,
      highlightOptions.selectedRankMin,
      highlightOptions.selectedRankRequired,
      highlightOptions.scoreMin,
      highlightOptions.scoreRequired,
    ],
  );
  const displayGroups = useMemo(
    () => (rankingGroups.length > 0 ? rankingGroups : buildFallbackRankingGroups(rows)),
    [rankingGroups, rows],
  );
  const displayRows = useMemo(
    () => buildSortedRankingRows(displayGroups),
    [displayGroups],
  );
  const displayGapRows = useMemo(
    () => buildSortedRankingRows(displayGroups, true),
    [displayGroups],
  );
  const gapValueByRowKey = useMemo(
    () =>
      buildGapValueMaps(
        displayGapRows,
        displayGroups,
      ),
    [displayGroups, displayGapRows],
  );
  const machineTopCandidateGapValueByRowKey = useMemo(
    () =>
      buildGapValueMaps(
        displayGapRows,
        displayGroups,
      ),
    [displayGroups, displayGapRows],
  );
  const selectedRankValueByRowKey = useMemo(
    () => buildSelectedRankValueMap(displayGroups),
    [displayGroups],
  );
  const displayRowsWithGap = useMemo(
    () => decorateRowsWithGapValues(displayRows, gapValueByRowKey),
    [gapValueByRowKey, displayRows],
  );
  const displayGroupsWithGap = useMemo(
    () =>
      displayGroups.map((group) => ({
        ...group,
        rows: decorateRowsWithGapValues(group.rows, gapValueByRowKey),
      })),
    [gapValueByRowKey, displayGroups],
  );
  const selectedOverallRows = useMemo(
    () => buildOverallRows(displayRowsWithGap, overallLimit),
    [displayRowsWithGap, overallLimit],
  );
  const machineTopCandidateRows = useMemo(
    () =>
      showMachineTopCandidates
        ? buildMachineTopCandidateRows(displayGroups, machineTopCandidateGapValueByRowKey)
        : [],
    [displayGroups, machineTopCandidateGapValueByRowKey, showMachineTopCandidates],
  );
  const activeBookmark = useMemo(() => {
    if (!enableConditionHighlight) {
      return null;
    }
    if (bookmarkSelection === HUNT_BACKTEST_BOOKMARK_SELECTION_NONE) {
      return null;
    }
    if (bookmarkSelection === HUNT_BACKTEST_BOOKMARK_SELECTION_CUSTOM) {
      return customHighlightBookmark;
    }
    return (
      bookmarks.find((bookmark) => bookmark.id === bookmarkSelection) ??
      bookmarks[0] ??
      customHighlightBookmark
    );
  }, [bookmarkSelection, bookmarks, customHighlightBookmark, enableConditionHighlight]);
  const bookmarkState = useMemo(
    () => {
      if (!enableConditionHighlight) {
        return { bookmark: null, matchByRowKey: null };
      }

      return buildHuntBacktestBookmarkMatches(
        decorateRowsWithSelectedRank(displayRowsWithGap, selectedRankValueByRowKey),
        activeBookmark,
      );
    },
    [activeBookmark, displayRowsWithGap, enableConditionHighlight, selectedRankValueByRowKey],
  );
  const bookmarkMatchByRowKey =
    enableConditionHighlight && bookmarkState.bookmark ? bookmarkState.matchByRowKey : null;
  const tableHighlightCondition = enableConditionHighlight ? highlightCondition : null;

  const toggleFixedColumn = (columnKey) => {
    setVisibleFixedColumnKeys((currentKeys) => {
      const nextKeys = new Set(currentKeys);
      if (nextKeys.has(columnKey)) {
        nextKeys.delete(columnKey);
      } else {
        nextKeys.add(columnKey);
      }

      return DEFAULT_VISIBLE_FIXED_COLUMN_KEYS
        .filter((key) => nextKeys.has(key))
        .concat(
          Object.values(FIXED_COLUMN_KEYS).filter(
            (key) => !DEFAULT_VISIBLE_FIXED_COLUMN_KEYS.includes(key) && nextKeys.has(key),
          ),
        );
    });
  };

  const toggleResultColumn = (columnKey) => {
    setVisibleResultKeys((currentKeys) => {
      const nextKeys = new Set(currentKeys);
      if (nextKeys.has(columnKey)) {
        nextKeys.delete(columnKey);
      } else {
        nextKeys.add(columnKey);
      }

      return resultColumns.filter((column) => nextKeys.has(column.key)).map((column) => column.key);
    });
  };

  if (displayRows.length === 0) {
    return (
      <section className="statusPanel">
        <h2>表示できる台がありません</h2>
        <p>保存済みの対象データが増えると、ここへ点数順の一覧が表示されます。</p>
      </section>
    );
  }

  return (
    <>
      <section className="filterPanel">
        <div>
          <p className="sectionLabel">コイン持ち基準</p>
          <div className="metricToggleRow">
            <label
              className={`metricToggleChip ${
                differenceMode === "bonus" ? "metricToggleChipActive" : ""
              }`}
            >
              <input
                type="radio"
                name="huntRankingDifferenceMode"
                value="bonus"
                checked={differenceMode === "bonus"}
                onChange={() => setDifferenceMode("bonus")}
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
                name="huntRankingDifferenceMode"
                value="estimated"
                checked={differenceMode === "estimated"}
                onChange={() => setDifferenceMode("estimated")}
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
                name="huntRankingDifferenceMode"
                value="minrepo"
                checked={differenceMode === "minrepo"}
                onChange={() => setDifferenceMode("minrepo")}
              />
              <span>みんレポ基準</span>
            </label>
          </div>
        </div>
        <div>
          <p className="sectionLabel">表示する列</p>
          <p className="filterLead">
            表の列表示だけを切り替えます。
          </p>
        </div>
        <div className="metricToggleRow">
          {fixedColumnOptions.map((column) => {
            const isChecked = visibleFixedColumnKeys.includes(column.key);

            return (
              <label
                key={column.key}
                className={`metricToggleChip ${isChecked ? "metricToggleChipActive" : ""}`}
              >
                <input
                  type="checkbox"
                  checked={isChecked}
                  onChange={() => toggleFixedColumn(column.key)}
                />
                <span>{column.label}</span>
              </label>
            );
          })}
          {resultColumns.map((column) => {
            const isChecked = visibleResultKeys.includes(column.key);

            return (
              <label
                key={column.key}
                className={`metricToggleChip ${isChecked ? "metricToggleChipActive" : ""}`}
              >
                <input
                  type="checkbox"
                  checked={isChecked}
                  onChange={() => toggleResultColumn(column.key)}
                />
                <span>{column.label}</span>
              </label>
            );
          })}
        </div>
      </section>

      {showMachineTopCandidates ? (
        machineTopCandidateRows.length > 0 ? (
          <OverallRankingTable
            storeId={storeId}
            storeName={storeName}
            sectionLabel="各機種1位"
            title={`各機種1位 ${formatNumber(machineTopCandidateRows.length)}台`}
            rows={machineTopCandidateRows}
            visibleColumns={visibleColumns}
            visibleFixedColumnKeys={visibleFixedColumnKeys}
            scoreColumnLabel={scoreColumnLabel}
            dateFlowLabel={dateFlowLabel}
            nextGapScope="machine"
            highlightCondition={tableHighlightCondition}
            bookmarkMatchByRowKey={bookmarkMatchByRowKey}
            sortable
            tableId="machine-top-candidates-ranking"
            showStoreColumn={showStoreColumn}
            showMachineEvaluation={showMachineEvaluation}
            huntScoreLogicLabel={huntScoreLogicLabel}
          />
        ) : (
          <section className="statusPanel">
            <h2>各機種1位はありません</h2>
            <p>2機種以上を選択して各機種の1位台を出せると、ここに表示されます。</p>
          </section>
        )
      ) : null}

      {showOverallRanking ? (
        selectedOverallRows.length > 0 ? (
          <OverallRankingTable
            storeId={storeId}
            storeName={storeName}
            title={`選択機種内ランキング 上位${formatNumber(selectedOverallRows.length)}台`}
            rows={selectedOverallRows}
            visibleColumns={visibleColumns}
            visibleFixedColumnKeys={visibleFixedColumnKeys}
            scoreColumnLabel={scoreColumnLabel}
            dateFlowLabel={dateFlowLabel}
            nextGapScope={nextGapScope}
            highlightCondition={tableHighlightCondition}
            bookmarkMatchByRowKey={bookmarkMatchByRowKey}
            sortable
            showStoreColumn={showStoreColumn}
            showMachineEvaluation={showMachineEvaluation}
            huntScoreLogicLabel={huntScoreLogicLabel}
          />
        ) : (
          <section className="statusPanel">
            <h2>チェック中の機種がありません</h2>
            <p>機種名にチェックを入れると、ここに選択機種内ランキングが表示されます。</p>
          </section>
        )
      ) : null}

      {showMachineGroupTables
        ? displayGroupsWithGap.map((group) => (
            <MachineRankingGroupTable
              key={group.machineName}
              group={group}
              storeId={storeId}
              storeName={storeName}
              visibleColumns={visibleColumns}
              visibleFixedColumnKeys={visibleFixedColumnKeys}
              scoreColumnLabel={scoreColumnLabel}
              dateFlowLabel={dateFlowLabel}
              nextGapScope={nextGapScope}
              highlightCondition={tableHighlightCondition}
              bookmarkMatchByRowKey={bookmarkMatchByRowKey}
              showMachineEvaluation={showMachineEvaluation}
              showStoreColumn={showStoreColumn}
              huntScoreLogicLabel={huntScoreLogicLabel}
            />
          ))
        : null}
    </>
  );
}
