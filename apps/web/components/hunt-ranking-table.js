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
  buildNextGapFilter,
  buildHuntBacktestBookmarkMatches,
  buildScopedRankFilters,
  buildScoreFilter,
  buildConditionRequirementOptions,
  calculateHuntScoreNextGapMap,
  formatHuntBacktestBookmarkSummary,
  matchesRequiredConditionFilters,
  readNextGapForRankScope,
  readSavedHuntBacktestBookmark,
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
  "setting_estimate",
];
const DEFAULT_RANK_SCOPE = "selected";
const DEFAULT_NEXT_GAP_SCOPE = "machine";
const DEFAULT_HIGHLIGHT_RANK_MIN = 1;
const DEFAULT_HIGHLIGHT_RANK_MAX = 3;
const DEFAULT_HIGHLIGHT_SCORE_MIN = 70;

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

function buildResultColumns(actualDate, differenceMode) {
  const actualDatePrefix = actualDate ? formatMonthDay(actualDate) : "実績";
  return RESULT_COLUMN_DEFINITIONS.map((column) => ({
    ...column,
    label: `${actualDatePrefix}${column.label}`,
    render: (row) => column.render(row, differenceMode),
  }));
}

function formatScoreColumnLabel(predictionDate) {
  return predictionDate ? `${formatMonthDay(predictionDate)}狙い度` : "狙い度";
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

function getRankingConditionHighlightClass(row, highlightCondition) {
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

function readSortableTableValue(row, columnIndex, nextGapScope) {
  if (columnIndex === 0) {
    return { missing: false, value: readSortableTableNumber(row.rank), type: "number" };
  }
  if (columnIndex === 1) {
    return { missing: false, value: readSortableTableNumber(row.huntScore), type: "number" };
  }
  if (columnIndex === 2) {
    return {
      missing: false,
      value: readSortableTableNumber(
        readNextGapForRankScope(row, normalizeNextGapScope(nextGapScope)),
      ),
      type: "number",
    };
  }
  if (columnIndex === 3) {
    return { missing: false, value: String(row.machineName ?? ""), type: "text" };
  }
  if (columnIndex === 4) {
    return { missing: false, value: String(row.slotNumber ?? ""), type: "text" };
  }

  return { missing: true, value: null, type: "number" };
}

function compareSortableTableRows(leftEntry, rightEntry, sortState, nextGapScope) {
  const leftValue = readSortableTableValue(leftEntry.row, sortState.columnIndex, nextGapScope);
  const rightValue = readSortableTableValue(rightEntry.row, sortState.columnIndex, nextGapScope);
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
  nextGapScope,
  highlightCondition,
  sortable = false,
  tableId = "",
}) {
  const [sortState, setSortState] = useState(() =>
    sortable ? { columnIndex: 1, direction: "desc", type: "number" } : null,
  );
  const sortedRows = useMemo(() => {
    if (!sortable || !sortState) {
      return rows;
    }

    return rows
      .map((row, originalIndex) => ({ row, originalIndex }))
      .sort((left, right) => compareSortableTableRows(left, right, sortState, nextGapScope))
      .map((entry) => entry.row);
  }, [nextGapScope, rows, sortState, sortable]);
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
        >
          {children}
        </SortableTableHeader>
      ) : (
        <th className={className || undefined}>{children}</th>
      );
  };

  return (
    <section className="tablePanel directoryPanel">
      <div className="tablePanelHeader">
        <div>
          <p className="sectionLabel">{sectionLabel}</p>
          <h2 className="tablePanelTitle">{title}</h2>
        </div>
      </div>
      <div className="tableScroller directoryScroller">
        <table className="directoryTable huntCompactTable huntRankingTable" {...tableProps}>
          <thead>
            <tr>
              <HeaderCell columnIndex={0}>{rankColumnLabel}</HeaderCell>
              <HeaderCell columnIndex={1}>{scoreColumnLabel}</HeaderCell>
              <HeaderCell columnIndex={2}>次点差</HeaderCell>
              <HeaderCell columnIndex={3} type="text" initialDirection="asc" className="directoryNameHeader">
                機種名
              </HeaderCell>
              <HeaderCell columnIndex={4} type="text" initialDirection="asc">台番</HeaderCell>
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
              const machineTitle = machineHasSite7Data
                ? site7BadgeTitle(
                    machineSite7FetchedAt,
                    `${machineFullName}\nこの機種にSセブン暫定データが含まれます`,
                  )
                : machineFullName;

              return (
                <tr
                  key={`${row.rowKey ?? row.machineName}-${row.slotNumber}-${title}-${row.rank}`}
                  className={rowClassName}
                >
                  <td
                    className={getRankingConditionHighlightClass(row, highlightCondition)}
                    data-sort-value={readRankingSortNumber(row.rank, "")}
                  >
                    {row.rank}
                  </td>
                  <td
                    className={getRankingConditionHighlightClass(row, highlightCondition)}
                    data-sort-value={readRankingSortNumber(row.huntScore, "")}
                  >
                    {formatNumber(row.huntScore)}
                  </td>
                  <td
                    className={getRankingConditionHighlightClass(row, highlightCondition)}
                    data-sort-value={readRankingSortNumber(
                      readNextGapForRankScope(row, normalizeNextGapScope(nextGapScope)),
                      "",
                    )}
                  >
                    {formatNextGapForScope(row, nextGapScope)}
                  </td>
                  <th
                    className={`directoryNameCell ${machineHasSite7Data ? "site7MachineCell" : ""}`}
                    title={machineTitle}
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
                  <td data-sort-value={row.slotNumber}>{row.slotNumber}</td>
                  {visibleColumns.map((column) => (
                    <td key={`${row.machineName}-${row.slotNumber}-${title}-${column.key}`}>
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
  initialDifferenceMode = DEFAULT_DIFFERENCE_MODE,
  showMachineTopCandidates = false,
}) {
  const [visibleResultKeys, setVisibleResultKeys] = useState(DEFAULT_VISIBLE_RESULT_KEYS);
  const [differenceMode, setDifferenceMode] = useState(() =>
    normalizeDifferenceMode(initialDifferenceMode),
  );
  const [bookmark, setBookmark] = useState(null);

  useEffect(() => {
    setDifferenceMode(normalizeDifferenceMode(initialDifferenceMode));
  }, [initialDifferenceMode]);

  useEffect(() => {
    const syncBookmark = () => {
      setBookmark(readSavedHuntBacktestBookmark(storeId));
    };

    syncBookmark();
    window.addEventListener(HUNT_BACKTEST_BOOKMARK_EVENT, syncBookmark);
    window.addEventListener("storage", syncBookmark);

    return () => {
      window.removeEventListener(HUNT_BACKTEST_BOOKMARK_EVENT, syncBookmark);
      window.removeEventListener("storage", syncBookmark);
    };
  }, [storeId]);

  const resultColumns = useMemo(
    () => buildResultColumns(actualDate, differenceMode),
    [actualDate, differenceMode],
  );
  const visibleColumns = useMemo(
    () => resultColumns.filter((column) => visibleResultKeys.includes(column.key)),
    [resultColumns, visibleResultKeys],
  );
  const scoreColumnLabel = useMemo(() => formatScoreColumnLabel(predictionDate), [predictionDate]);
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
  const resultColumnLead = actualDate
    ? `${formatMonthDay(actualDate)}の実績列だけを切り替えられます。`
    : "実績列だけを切り替えられます。";
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
  const bookmarkState = useMemo(
    () =>
      buildHuntBacktestBookmarkMatches(
        decorateRowsWithSelectedRank(displayRowsWithGap, selectedRankValueByRowKey),
        bookmark,
      ),
    [displayRowsWithGap, bookmark, selectedRankValueByRowKey],
  );
  const bookmarkSummary = useMemo(
    () => formatHuntBacktestBookmarkSummary(bookmarkState.bookmark),
    [bookmarkState.bookmark],
  );

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
            {`${resultColumnLead}ここは保存済み実績の表示だけを切り替えます。`}
          </p>
        </div>
        {bookmarkState.bookmark ? (
          <p className="storeReserveNotice storeReserveNotice-info">
            {`保存済み目印条件: ${bookmarkSummary} / 表示中${formatNumber(
              bookmarkState.totalRowCount,
            )}台のうち${formatNumber(bookmarkState.matchedRowCount)}台が一致しています。`}
          </p>
        ) : null}
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
          nextGapScope={nextGapScope}
          highlightCondition={highlightCondition}
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
            nextGapScope="machine"
            highlightCondition={highlightCondition}
            sortable
            tableId="machine-top-candidates-ranking"
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

        return (
          <section key={group.machineName} className="tablePanel directoryPanel">
            <div className="tablePanelHeader">
              <div>
                <p className="sectionLabel">狙い度上位</p>
                <h2 className="tablePanelTitle">
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
                </h2>
              </div>
            </div>
          <div className="tableScroller directoryScroller">
            <table className="directoryTable huntCompactTable huntRankingTable">
              <thead>
                <tr>
                  <th>順位</th>
                  <th>{scoreColumnLabel}</th>
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

                  return (
                    <tr
                      key={`${row.rowKey ?? row.machineName}-${row.slotNumber}-${row.rank}`}
                      className={rowClassName}
                    >
                      <td className={getRankingConditionHighlightClass(row, highlightCondition)}>
                        {row.rank}
                      </td>
                      <td className={getRankingConditionHighlightClass(row, highlightCondition)}>
                        {formatNumber(row.huntScore)}
                      </td>
                      <td className={getRankingConditionHighlightClass(row, highlightCondition)}>
                        {formatNextGapForScope(row, nextGapScope)}
                      </td>
                      <td>{row.slotNumber}</td>
                      {visibleColumns.map((column) => (
                        <td key={`${row.machineName}-${row.slotNumber}-${column.key}`}>
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
