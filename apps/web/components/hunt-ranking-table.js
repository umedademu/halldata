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
const DEFAULT_RANK_SCOPE = "selected";
const DEFAULT_NEXT_GAP_SCOPE = "machine";
const DEFAULT_HIGHLIGHT_RANK_MIN = 1;
const DEFAULT_HIGHLIGHT_RANK_MAX = 3;
const DEFAULT_HIGHLIGHT_SCORE_MIN = 70;

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
  },
  {
    key: "games_count",
    label: "G数",
    render: (row) => formatAverageGames(row.nextRecord?.games_count),
  },
  {
    key: "bb_count",
    label: "BB",
    render: (row) => formatAverageGames(row.nextRecord?.bb_count),
  },
  {
    key: "rb_count",
    label: "RB",
    render: (row) => formatAverageGames(row.nextRecord?.rb_count),
  },
  {
    key: "combined_ratio_text",
    label: "合成",
    render: (row) => formatRatio(row.nextRecord?.combined_ratio_text),
  },
  {
    key: ESTIMATED_GRAPE_RESULT_KEY,
    label: "ブドウ",
    render: (row) => formatEstimatedGrapeDenominator(row.nextRecord?.estimated_grape_denominator),
  },
  {
    key: "setting_estimate",
    label: "設定",
    render: (row) => formatSettingEstimateScore(row.nextSettingEstimate?.average),
  },
  {
    key: "payout_rate",
    label: "出率",
    render: (row) => formatPercent(row.nextRecord?.payout_rate),
  },
  {
    key: "bb_ratio_text",
    label: "BB率",
    render: (row) => formatRatio(row.nextRecord?.bb_ratio_text),
  },
  {
    key: "rb_ratio_text",
    label: "RB率",
    render: (row) => formatRatio(row.nextRecord?.rb_ratio_text),
  },
];

function buildResultColumns(differenceMode, showGrapeColumn) {
  const columnDefinitions = showGrapeColumn
    ? RESULT_COLUMN_DEFINITIONS
    : RESULT_COLUMN_DEFINITIONS.filter((column) => column.key !== ESTIMATED_GRAPE_RESULT_KEY);

  return columnDefinitions.map((column) => ({
    ...column,
    render: (row) => column.render(row, differenceMode),
  }));
}

function formatRankingDateFlowLabel(predictionDate, actualDate) {
  const scoreDateLabel = predictionDate ? `${formatMonthDay(predictionDate)}狙い度` : "狙い度";
  const actualDateLabel = actualDate ? `${formatMonthDay(actualDate)}実績` : "実績なし";
  return `${scoreDateLabel} → ${actualDateLabel}`;
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

function buildMatchedConditionTitleParts(evaluation) {
  const matchedConditions = Array.isArray(evaluation?.matchedConditions)
    ? evaluation.matchedConditions
    : [];
  if (matchedConditions.length === 0) {
    return [];
  }

  return [
    "一致した採用条件:",
    ...matchedConditions.map((condition) => {
      const selectedLabel = condition.isSelected ? " / 選択中" : "";
      const backtestLabel = condition.backtestLabel ? ` / ${condition.backtestLabel}` : "";
      return `・${condition.conditionName}${backtestLabel}${selectedLabel}`;
    }),
  ];
}

function MachineEvaluationCell({ evaluation, extraTitle = "" }) {
  if (!evaluation) {
    return <td title={extraTitle || undefined} data-sort-value="">-</td>;
  }

  const matchedConditionTitleParts = buildMatchedConditionTitleParts(evaluation);
  const titleParts = [
    evaluation.logicName ? `機種別ロジック: ${evaluation.logicName}` : "",
    matchedConditionTitleParts.length > 0
      ? matchedConditionTitleParts.join("\n")
      : evaluation.conditionName
        ? `採用条件: ${evaluation.conditionName}`
        : "",
    matchedConditionTitleParts.length === 0 && evaluation.backtestLabel
      ? `目安: ${evaluation.backtestLabel}`
      : "",
    Number.isFinite(evaluation.rank) ? `機種別順位: ${evaluation.rank}` : "",
    Number.isFinite(evaluation.nextGap) ? `次点差: ${formatDecimal(evaluation.nextGap)}` : "",
  ].filter(Boolean);
  const cellClassNames = [
    evaluation.matchesAnyCondition ? "machineEvaluationMatchedCell" : "",
    getMachineEvaluationPayoutClass(evaluation.bestMatchedBacktestPayoutRate),
  ].filter(Boolean).join(" ");

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

function isRankingConditionHighlighted(row, highlightCondition) {
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

  const parsedValue = Number(text.replace(/,/g, "").replace(/%$/u, ""));
  return Number.isFinite(parsedValue) ? parsedValue : null;
}

function readSortableTableValue(
  row,
  columnIndex,
  nextGapScope,
  hasSubHuntScoreColumn = false,
  hasMachineEvaluationColumn = false,
) {
  if (columnIndex === 0) {
    return { missing: false, value: readSortableTableNumber(row.rank), type: "number" };
  }
  if (columnIndex === 1) {
    return { missing: false, value: readSortableTableNumber(row.huntScore), type: "number" };
  }
  const machineEvaluationColumnIndex = 2 + (hasSubHuntScoreColumn ? 1 : 0);
  const nextGapColumnIndex = 2 + (hasSubHuntScoreColumn ? 1 : 0) + (hasMachineEvaluationColumn ? 1 : 0);
  const machineColumnIndex = nextGapColumnIndex + 1;
  const slotColumnIndex = machineColumnIndex + 1;

  if (hasSubHuntScoreColumn && columnIndex === 2) {
    return { missing: false, value: readSortableTableNumber(row.subHuntScore), type: "number" };
  }
  if (hasMachineEvaluationColumn && columnIndex === machineEvaluationColumnIndex) {
    return {
      missing: false,
      value: readSortableTableNumber(row.machineEvaluation?.score),
      type: "number",
    };
  }
  if (columnIndex === nextGapColumnIndex) {
    return {
      missing: false,
      value: readSortableTableNumber(
        readNextGapForRankScope(row, normalizeNextGapScope(nextGapScope)),
      ),
      type: "number",
    };
  }
  if (columnIndex === machineColumnIndex) {
    return { missing: false, value: String(row.machineName ?? ""), type: "text" };
  }
  if (columnIndex === slotColumnIndex) {
    return { missing: false, value: String(row.slotNumber ?? ""), type: "text" };
  }

  return { missing: true, value: null, type: "number" };
}

function compareSortableTableRows(
  leftEntry,
  rightEntry,
  sortState,
  nextGapScope,
  hasSubHuntScoreColumn = false,
  hasMachineEvaluationColumn = false,
) {
  const leftValue = readSortableTableValue(
    leftEntry.row,
    sortState.columnIndex,
    nextGapScope,
    hasSubHuntScoreColumn,
    hasMachineEvaluationColumn,
  );
  const rightValue = readSortableTableValue(
    rightEntry.row,
    sortState.columnIndex,
    nextGapScope,
    hasSubHuntScoreColumn,
    hasMachineEvaluationColumn,
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

function OverallRankingTable({
  storeId,
  sectionLabel = "狙い度上位",
  rankColumnLabel = "順位",
  title,
  rows,
  visibleColumns,
  scoreColumnLabel,
  dateFlowLabel,
  nextGapScope,
  highlightCondition,
  bookmarkMatchByRowKey = null,
  sortable = false,
  tableId = "",
  subHuntScoreLogic = null,
  showMachineEvaluation = false,
}) {
  const showSubHuntScoreColumn = Boolean(subHuntScoreLogic);
  const hasMachineEvaluationColumn =
    showMachineEvaluation && rows.some((row) => row?.machineEvaluation);
  const subHuntScoreTitle = subHuntScoreLogic?.name
    ? `表示用ロジック: ${subHuntScoreLogic.name}`
    : undefined;
  const [sortState, setSortState] = useState(() =>
    sortable ? { columnIndex: 1, direction: "desc", type: "number" } : null,
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
          showSubHuntScoreColumn,
          hasMachineEvaluationColumn,
        ),
      )
      .map((entry) => entry.row);
  }, [hasMachineEvaluationColumn, nextGapScope, rows, showSubHuntScoreColumn, sortState, sortable]);
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
  const machineEvaluationColumnIndex = 2 + (showSubHuntScoreColumn ? 1 : 0);
  const nextGapColumnIndex = 2 + (showSubHuntScoreColumn ? 1 : 0) + (hasMachineEvaluationColumn ? 1 : 0);
  const machineColumnIndex = nextGapColumnIndex + 1;
  const slotColumnIndex = machineColumnIndex + 1;

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
              <HeaderCell columnIndex={0}>{rankColumnLabel}</HeaderCell>
              <HeaderCell columnIndex={1}>{scoreColumnLabel}</HeaderCell>
              {showSubHuntScoreColumn ? (
                <HeaderCell columnIndex={2} title={subHuntScoreTitle}>表示狙度</HeaderCell>
              ) : null}
              {hasMachineEvaluationColumn ? (
                <HeaderCell columnIndex={machineEvaluationColumnIndex} title="機種別評価">
                  機種別
                </HeaderCell>
              ) : null}
              <HeaderCell columnIndex={nextGapColumnIndex}>次点差</HeaderCell>
              <HeaderCell columnIndex={machineColumnIndex} type="text" initialDirection="asc" className="directoryNameHeader">
                機種名
              </HeaderCell>
              <HeaderCell columnIndex={slotColumnIndex} type="text" initialDirection="asc">台番</HeaderCell>
              {visibleColumns.map((column) => (
                <th key={column.key}>{column.label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((row) => {
              const rowClassName = getSettingEstimateHighlightClass(row.nextSettingEstimate?.average);
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

              return (
                <tr
                  key={`${row.rowKey ?? row.machineName}-${row.slotNumber}-${title}-${row.rank}`}
                  className={rowClassName}
                  title={rowSite7Title || undefined}
                >
                  <td
                    data-sort-value={readRankingSortNumber(row.rank, "")}
                    title={rowSite7Title || undefined}
                  >
                    {row.rank}
                  </td>
                  <td
                    className={getRankingConditionHighlightClass(
                      row,
                      highlightCondition,
                      bookmarkMatchByRowKey,
                    )}
                    data-sort-value={readRankingSortNumber(row.huntScore, "")}
                    title={rowSite7Title || undefined}
                  >
                    {formatNumber(row.huntScore)}
                  </td>
                  {showSubHuntScoreColumn ? (
                    <td
                      title={combineTitleParts(subHuntScoreTitle, rowSite7Title)}
                      data-sort-value={readRankingSortNumber(row.subHuntScore, "")}
                    >
                      {formatNumber(row.subHuntScore)}
                    </td>
                  ) : null}
                  {hasMachineEvaluationColumn ? (
                    <MachineEvaluationCell
                      evaluation={row.machineEvaluation}
                      extraTitle={rowSite7Title}
                    />
                  ) : null}
                  <td
                    data-sort-value={readRankingSortNumber(
                      readNextGapForRankScope(row, normalizeNextGapScope(nextGapScope)),
                      "",
                    )}
                    title={rowSite7Title || undefined}
                  >
                    {formatNextGapForScope(row, nextGapScope)}
                  </td>
                  <th
                    className={`directoryNameCell ${machineHasSite7Data ? "site7MachineCell" : ""}`}
                    title={machineCellTitle || undefined}
                    data-sort-value={row.machineName}
                  >
                    <span className="directoryNameContent">
                      <Link
                        href={`/stores/${storeId}/machines/${encodeURIComponent(row.machineName)}`}
                        className="directoryPrimaryLink"
                      >
                        {machineShortName}
                      </Link>
                      {machineHasSite7Data ? (
                        <Site7RankingBadge
                          fetchedAt={machineSite7FetchedAt}
                          title="この機種にSセブン暫定データが含まれます"
                        />
                      ) : null}
                    </span>
                  </th>
                  <td data-sort-value={row.slotNumber} title={rowSite7Title || undefined}>
                    {row.slotNumber}
                  </td>
                  {visibleColumns.map((column) => (
                    <td
                      key={`${row.machineName}-${row.slotNumber}-${title}-${column.key}`}
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
  rows = [],
  rankingGroups = [],
  overallLimit = 20,
  predictionDate = null,
  actualDate = null,
  highlightOptions = {},
  customHighlightBookmark = null,
  initialDifferenceMode = DEFAULT_DIFFERENCE_MODE,
  showMachineTopCandidates = false,
  subHuntScoreLogic = null,
  showMachineEvaluation = false,
  showGrapeColumn = false,
}) {
  const [visibleResultKeys, setVisibleResultKeys] = useState(DEFAULT_VISIBLE_RESULT_KEYS);
  const [differenceMode, setDifferenceMode] = useState(() =>
    normalizeDifferenceMode(initialDifferenceMode),
  );
  const [bookmarks, setBookmarks] = useState([]);
  const [bookmarkSelection, setBookmarkSelection] = useState(HUNT_BACKTEST_BOOKMARK_SELECTION_CUSTOM);

  useEffect(() => {
    setDifferenceMode(normalizeDifferenceMode(initialDifferenceMode));
  }, [initialDifferenceMode]);

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
  }, [storeId]);

  const resultColumns = useMemo(
    () => buildResultColumns(differenceMode, showGrapeColumn),
    [differenceMode, showGrapeColumn],
  );
  const visibleColumns = useMemo(
    () => resultColumns.filter((column) => visibleResultKeys.includes(column.key)),
    [resultColumns, visibleResultKeys],
  );
  const scoreColumnLabel = "狙い度";
  const dateFlowLabel = useMemo(
    () => formatRankingDateFlowLabel(predictionDate, actualDate),
    [actualDate, predictionDate],
  );
  const showSubHuntScoreColumn = Boolean(subHuntScoreLogic);
  const subHuntScoreTitle = subHuntScoreLogic?.name
    ? `表示用ロジック: ${subHuntScoreLogic.name}`
    : undefined;
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
  }, [bookmarkSelection, bookmarks, customHighlightBookmark]);
  const bookmarkState = useMemo(
    () =>
      buildHuntBacktestBookmarkMatches(
        decorateRowsWithSelectedRank(displayRowsWithGap, selectedRankValueByRowKey),
        activeBookmark,
      ),
    [activeBookmark, displayRowsWithGap, selectedRankValueByRowKey],
  );
  const bookmarkMatchByRowKey = bookmarkState.bookmark ? bookmarkState.matchByRowKey : null;

  const toggleColumn = (columnKey) => {
    setVisibleResultKeys((currentKeys) => {
      const nextKeys = new Set(currentKeys);
      if (nextKeys.has(columnKey)) {
        if (nextKeys.size === 1) {
          return currentKeys;
        }
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
            ここは保存済み実績の表示だけを切り替えます。
          </p>
        </div>
        <div className="metricToggleRow">
          {resultColumns.map((column) => {
            const isChecked = visibleResultKeys.includes(column.key);
            const isLastVisible = isChecked && visibleColumns.length === 1;

            return (
              <label
                key={column.key}
                className={`metricToggleChip ${isChecked ? "metricToggleChipActive" : ""}`}
              >
                <input
                  type="checkbox"
                  checked={isChecked}
                  disabled={isLastVisible}
                  onChange={() => toggleColumn(column.key)}
                />
                <span>{column.label}</span>
              </label>
            );
          })}
        </div>
      </section>

      {selectedOverallRows.length > 0 ? (
        <OverallRankingTable
          storeId={storeId}
          title={`選択機種内ランキング 上位${formatNumber(selectedOverallRows.length)}台`}
          rows={selectedOverallRows}
          visibleColumns={visibleColumns}
          scoreColumnLabel={scoreColumnLabel}
          dateFlowLabel={dateFlowLabel}
          nextGapScope={nextGapScope}
          highlightCondition={highlightCondition}
          bookmarkMatchByRowKey={bookmarkMatchByRowKey}
          subHuntScoreLogic={subHuntScoreLogic}
          showMachineEvaluation={showMachineEvaluation}
        />
      ) : (
        <section className="statusPanel">
          <h2>チェック中の機種がありません</h2>
          <p>機種名にチェックを入れると、ここに選択機種内ランキングが表示されます。</p>
        </section>
      )}

      {showMachineTopCandidates ? (
        machineTopCandidateRows.length > 0 ? (
          <OverallRankingTable
            storeId={storeId}
            sectionLabel="各機種1位"
            rankColumnLabel="順位"
            title={`各機種1位 ${formatNumber(machineTopCandidateRows.length)}台`}
            rows={machineTopCandidateRows}
            visibleColumns={visibleColumns}
            scoreColumnLabel={scoreColumnLabel}
            dateFlowLabel={dateFlowLabel}
            nextGapScope="machine"
            highlightCondition={highlightCondition}
            bookmarkMatchByRowKey={bookmarkMatchByRowKey}
            sortable
            tableId="machine-top-candidates-ranking"
            subHuntScoreLogic={subHuntScoreLogic}
            showMachineEvaluation={showMachineEvaluation}
          />
        ) : (
          <section className="statusPanel">
            <h2>各機種1位はありません</h2>
            <p>2機種以上を選択して各機種の1位台を出せると、ここに表示されます。</p>
          </section>
        )
      ) : null}

      {displayGroupsWithGap.map((group) => {
        const groupHasSite7Data = group.rows.some((row) => row.predictionMachineHasSite7Data);
        const groupSite7FetchedAt = latestSite7FetchedAtFromRows(group.rows);
        const showGroupMachineEvaluation =
          showMachineEvaluation && group.rows.some((row) => row?.machineEvaluation);

        return (
          <section key={group.machineName} className="tablePanel directoryPanel">
            <div className="tablePanelHeader">
              <div>
                <p className="sectionLabel">狙い度上位</p>
                <h2 className="tablePanelTitle">
                  <span>
                    {group.isCombinedGroup ? (
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
                  <th>順位</th>
                  <th>{scoreColumnLabel}</th>
                  {showSubHuntScoreColumn ? (
                    <th title={subHuntScoreTitle}>表示狙度</th>
                  ) : null}
                  {showGroupMachineEvaluation ? <th title="機種別評価">機種別</th> : null}
                  <th>次点差</th>
                  <th>台番</th>
                  {visibleColumns.map((column) => (
                    <th key={column.key}>{column.label}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {group.rows.map((row) => {
                  const rowClassName = getSettingEstimateHighlightClass(row.nextSettingEstimate?.average);
                  const rowSite7Title = buildRankingRowSite7Title(row);

                  return (
                    <tr
                      key={`${row.rowKey ?? row.machineName}-${row.slotNumber}-${row.rank}`}
                      className={rowClassName}
                      title={rowSite7Title || undefined}
                    >
                      <td
                        title={rowSite7Title || undefined}
                      >
                        {row.rank}
                      </td>
                      <td
                        className={getRankingConditionHighlightClass(row, highlightCondition, bookmarkMatchByRowKey)}
                        title={rowSite7Title || undefined}
                      >
                        {formatNumber(row.huntScore)}
                      </td>
                      {showSubHuntScoreColumn ? (
                        <td title={combineTitleParts(subHuntScoreTitle, rowSite7Title)}>
                          {formatNumber(row.subHuntScore)}
                        </td>
                      ) : null}
                      {showGroupMachineEvaluation ? (
                        <MachineEvaluationCell
                          evaluation={row.machineEvaluation}
                          extraTitle={rowSite7Title}
                        />
                      ) : null}
                      <td
                        title={rowSite7Title || undefined}
                      >
                        {formatNextGapForScope(row, nextGapScope)}
                      </td>
                      <td title={rowSite7Title || undefined}>{row.slotNumber}</td>
                      {visibleColumns.map((column) => (
                        <td
                          key={`${row.machineName}-${row.slotNumber}-${column.key}`}
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
      })}
    </>
  );
}
