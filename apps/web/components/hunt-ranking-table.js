"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import {
  formatAverageGames,
  formatDecimal,
  formatMonthDay,
  formatNumber,
  formatPercent,
  formatRatio,
  formatSignedNumber,
} from "../lib/format";
import {
  HUNT_BACKTEST_BOOKMARK_EVENT,
  buildDeviationFilter,
  buildNextGapFilter,
  buildHuntBacktestBookmarkMatches,
  buildRankFilter,
  buildScoreFilter,
  buildConditionRequirementOptions,
  calculateHuntScoreDeviationMap,
  calculateHuntScoreNextGapMap,
  formatHuntBacktestBookmarkSummary,
  matchesRequiredConditionFilters,
  readDeviationForRankScope,
  readNextGapForRankScope,
  readSavedHuntBacktestBookmark,
} from "../lib/hunt-bookmark";
import {
  formatSettingEstimateScore,
  getSettingEstimateHighlightClass,
} from "../lib/setting-estimates";
import { selectDifferenceValue } from "../lib/machine-difference";

const DEFAULT_VISIBLE_RESULT_KEYS = [
  "difference_value",
  "games_count",
  "bb_count",
  "rb_count",
  "combined_ratio_text",
  "setting_estimate",
];
const DEFAULT_DIFFERENCE_MODE = "bonus";
const DEFAULT_RANK_SCOPE = "selected";
const DEFAULT_DEVIATION_SCOPE = "selected";
const DEFAULT_NEXT_GAP_SCOPE = "machine";
const DEFAULT_HIGHLIGHT_RANK_MIN = 1;
const DEFAULT_HIGHLIGHT_RANK_MAX = 3;
const DEFAULT_HIGHLIGHT_SCORE_MIN = 70;
const DEFAULT_DEVIATION_MIN = 60;

const RESULT_COLUMN_DEFINITIONS = [
  {
    key: "difference_value",
    label: "差枚",
    render: (row, differenceMode) =>
      formatSignedNumber(selectDifferenceValue(row.nextRecord, differenceMode)),
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

function buildDeviationRowKey(row) {
  return String(row?.rowKey ?? `${row?.machineName ?? ""}::${row?.slotNumber ?? ""}`).trim();
}

function buildDeviationValueMaps(displayRows, allDisplayRows, displayGroups, rankFilter) {
  const overallDeviationMap = calculateHuntScoreDeviationMap(allDisplayRows);
  const overallNextGapMap = calculateHuntScoreNextGapMap(allDisplayRows, rankFilter);
  const selectedDeviationMap = calculateHuntScoreDeviationMap(displayRows);
  const selectedNextGapMap = calculateHuntScoreNextGapMap(displayRows, rankFilter);
  const overallDeviationByKey = new Map(
    allDisplayRows.map((row) => [buildDeviationRowKey(row), overallDeviationMap.get(row) ?? null]),
  );
  const overallNextGapByKey = new Map(
    allDisplayRows.map((row) => [buildDeviationRowKey(row), overallNextGapMap.get(row) ?? null]),
  );
  const selectedDeviationByKey = new Map(
    displayRows.map((row) => [buildDeviationRowKey(row), selectedDeviationMap.get(row) ?? null]),
  );
  const selectedNextGapByKey = new Map(
    displayRows.map((row) => [buildDeviationRowKey(row), selectedNextGapMap.get(row) ?? null]),
  );
  const machineDeviationByKey = new Map();
  const machineNextGapByKey = new Map();

  for (const group of displayGroups) {
    const groupRows = getRankingGroupRows(group, true);
    const deviationMap = calculateHuntScoreDeviationMap(groupRows);
    const nextGapMap = calculateHuntScoreNextGapMap(groupRows, rankFilter);
    for (const row of groupRows) {
      if (deviationMap.has(row)) {
        machineDeviationByKey.set(buildDeviationRowKey(row), deviationMap.get(row));
      }
      if (nextGapMap.has(row)) {
        machineNextGapByKey.set(buildDeviationRowKey(row), nextGapMap.get(row));
      }
    }
  }

  const valueByRowKey = new Map();
  for (const row of allDisplayRows) {
    const rowKey = buildDeviationRowKey(row);
    valueByRowKey.set(rowKey, {
      overallDeviation: overallDeviationByKey.get(rowKey) ?? null,
      selectedDeviation: selectedDeviationByKey.get(rowKey) ?? null,
      machineDeviation: machineDeviationByKey.get(rowKey) ?? null,
      overallNextGap: overallNextGapByKey.get(rowKey) ?? null,
      selectedNextGap: selectedNextGapByKey.get(rowKey) ?? null,
      machineNextGap: machineNextGapByKey.get(rowKey) ?? null,
    });
  }

  return valueByRowKey;
}

function decorateRowsWithDeviation(rows, deviationValueByRowKey) {
  return rows.map((row) => ({
    ...row,
    ...(deviationValueByRowKey.get(buildDeviationRowKey(row)) ?? {}),
  }));
}

function normalizeDeviationScope(value) {
  if (value === "all" || value === "machine" || value === "selected") {
    return value;
  }
  return DEFAULT_DEVIATION_SCOPE;
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

function formatDeviationForScope(row, deviationScope) {
  return formatDecimal(readDeviationForRankScope(row, normalizeDeviationScope(deviationScope)));
}

function formatNextGapForScope(row, nextGapScope) {
  return formatDecimal(readNextGapForRankScope(row, normalizeNextGapScope(nextGapScope)));
}

function isRankingConditionHighlighted(row, highlightCondition) {
  if (
    !highlightCondition.rankFilter.hasRankFilter &&
    !highlightCondition.scoreFilter.hasScoreFilter &&
    !highlightCondition.deviationFilter.hasDeviationFilter &&
    !highlightCondition.nextGapFilter.hasNextGapFilter
  ) {
    return false;
  }

  const deviationValue = readDeviationForRankScope(
    row,
    normalizeDeviationScope(highlightCondition.deviationScope),
  );
  const rankValue = readRankForScope(row, highlightCondition.rankScope);
  const nextGapValue = readNextGapForRankScope(
    row,
    normalizeNextGapScope(highlightCondition.nextGapScope),
  );
  return matchesRequiredConditionFilters(
    rankValue,
    row.huntScore,
    highlightCondition.rankFilter,
    highlightCondition.scoreFilter,
    highlightCondition.requirementOptions,
    deviationValue,
    highlightCondition.deviationFilter,
    false,
    nextGapValue,
    highlightCondition.nextGapFilter,
  );
}

function getRankingConditionHighlightClass(row, highlightCondition) {
  return isRankingConditionHighlighted(row, highlightCondition)
    ? "huntScoreDeviationHighlighted"
    : undefined;
}

function buildSelectedRankValueMap(displayGroups) {
  const rows = (Array.isArray(displayGroups) ? displayGroups : [])
    .flatMap((group) => getRankingGroupRows(group, true))
    .sort(compareRankingRows);

  return new Map(
    rows.map((row, index) => [
      buildDeviationRowKey(row),
      row?.selectedRank ?? index + 1,
    ]),
  );
}

function decorateRowsWithSelectedRank(rows, selectedRankValueMap) {
  return rows.map((row) => ({
    ...row,
    selectedRank: selectedRankValueMap.has(buildDeviationRowKey(row))
      ? selectedRankValueMap.get(buildDeviationRowKey(row))
      : null,
  }));
}

function OverallRankingTable({
  storeId,
  title,
  rows,
  visibleColumns,
  scoreColumnLabel,
  deviationScope,
  nextGapScope,
  highlightCondition,
}) {
  return (
    <section className="tablePanel directoryPanel">
      <div className="tablePanelHeader">
        <div>
          <p className="sectionLabel">狙い度上位</p>
          <h2 className="tablePanelTitle">{title}</h2>
        </div>
      </div>
      <div className="tableScroller directoryScroller">
        <table className="directoryTable">
          <thead>
            <tr>
              <th>順位</th>
              <th>{scoreColumnLabel}</th>
              <th>偏差値</th>
              <th>次点差</th>
              <th className="directoryNameHeader">機種名</th>
              <th>台番</th>
              {visibleColumns.map((column) => (
                <th key={column.key}>{column.label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const rowClassName = getSettingEstimateHighlightClass(row.nextSettingEstimate?.average);

              return (
                <tr
                  key={`${row.rowKey ?? row.machineName}-${row.slotNumber}-${title}-${row.rank}`}
                  className={rowClassName}
                >
                  <td className={getRankingConditionHighlightClass(row, highlightCondition)}>
                    {row.rank}
                  </td>
                  <td className={getRankingConditionHighlightClass(row, highlightCondition)}>
                    {formatNumber(row.huntScore)}
                  </td>
                  <td className={getRankingConditionHighlightClass(row, highlightCondition)}>
                    {formatDeviationForScope(row, deviationScope)}
                  </td>
                  <td className={getRankingConditionHighlightClass(row, highlightCondition)}>
                    {formatNextGapForScope(row, nextGapScope)}
                  </td>
                  <th className="directoryNameCell">
                    <Link
                      href={`/stores/${storeId}/machines/${encodeURIComponent(row.machineName)}`}
                      className="directoryPrimaryLink"
                    >
                      {row.machineName}
                    </Link>
                  </th>
                  <td>{row.slotNumber}</td>
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
  allRankingGroups = [],
  overallLimit = 20,
  predictionDate = null,
  actualDate = null,
  highlightOptions = {},
}) {
  const [visibleResultKeys, setVisibleResultKeys] = useState(DEFAULT_VISIBLE_RESULT_KEYS);
  const [differenceMode, setDifferenceMode] = useState(DEFAULT_DIFFERENCE_MODE);
  const [bookmark, setBookmark] = useState(null);

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
  const deviationScope = normalizeDeviationScope(
    highlightOptions.deviationScope ?? DEFAULT_DEVIATION_SCOPE,
  );
  const nextGapScope = normalizeNextGapScope(highlightOptions.nextGapScope ?? DEFAULT_NEXT_GAP_SCOPE);
  const rankScope = normalizeRankScope(highlightOptions.rankScope ?? DEFAULT_RANK_SCOPE);
  const highlightCondition = useMemo(
    () => ({
      rankFilter: buildRankFilter(
        highlightOptions.rankMin ?? String(DEFAULT_HIGHLIGHT_RANK_MIN),
        highlightOptions.rankMax ?? String(DEFAULT_HIGHLIGHT_RANK_MAX),
      ),
      scoreFilter: buildScoreFilter(
        highlightOptions.scoreMin ?? String(DEFAULT_HIGHLIGHT_SCORE_MIN),
      ),
      deviationFilter: buildDeviationFilter(
        highlightOptions.deviationMin ?? String(DEFAULT_DEVIATION_MIN),
      ),
      nextGapFilter: buildNextGapFilter(highlightOptions.nextGapMin),
      requirementOptions: buildConditionRequirementOptions(highlightOptions, {
        rankRequired: true,
        scoreRequired: true,
        deviationRequired: false,
        nextGapRequired: false,
      }),
      rankScope,
      deviationScope,
      nextGapScope,
    }),
    [
      deviationScope,
      nextGapScope,
      rankScope,
      highlightOptions.deviationMin,
      highlightOptions.deviationRequired,
      highlightOptions.nextGapMin,
      highlightOptions.nextGapRequired,
      highlightOptions.nextGapScope,
      highlightOptions.rankMax,
      highlightOptions.rankMin,
      highlightOptions.rankRequired,
      highlightOptions.rankScope,
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
  const allDisplayGroups = useMemo(
    () => (allRankingGroups.length > 0 ? allRankingGroups : displayGroups),
    [allRankingGroups, displayGroups],
  );
  const displayRows = useMemo(
    () => buildSortedRankingRows(displayGroups),
    [displayGroups],
  );
  const allDisplayRows = useMemo(
    () => buildSortedRankingRows(allDisplayGroups),
    [allDisplayGroups],
  );
  const displayDeviationRows = useMemo(
    () => buildSortedRankingRows(displayGroups, true),
    [displayGroups],
  );
  const allDeviationRows = useMemo(
    () => buildSortedRankingRows(allDisplayGroups, true),
    [allDisplayGroups],
  );
  const deviationValueByRowKey = useMemo(
    () =>
      buildDeviationValueMaps(
        displayDeviationRows,
        allDeviationRows,
        displayGroups,
        highlightCondition.rankFilter,
      ),
    [allDeviationRows, displayGroups, displayDeviationRows, highlightCondition.rankFilter],
  );
  const selectedRankValueByRowKey = useMemo(
    () => buildSelectedRankValueMap(displayGroups),
    [displayGroups],
  );
  const displayRowsWithDeviation = useMemo(
    () => decorateRowsWithDeviation(displayRows, deviationValueByRowKey),
    [deviationValueByRowKey, displayRows],
  );
  const allDisplayRowsWithDeviation = useMemo(
    () =>
      decorateRowsWithSelectedRank(
        decorateRowsWithDeviation(allDisplayRows, deviationValueByRowKey),
        selectedRankValueByRowKey,
      ),
    [allDisplayRows, deviationValueByRowKey, selectedRankValueByRowKey],
  );
  const displayGroupsWithDeviation = useMemo(
    () =>
      displayGroups.map((group) => ({
        ...group,
        rows: decorateRowsWithDeviation(group.rows, deviationValueByRowKey),
      })),
    [deviationValueByRowKey, displayGroups],
  );
  const selectedOverallRows = useMemo(
    () => buildOverallRows(displayRowsWithDeviation, overallLimit),
    [displayRowsWithDeviation, overallLimit],
  );
  const allOverallRows = useMemo(
    () => buildOverallRows(allDisplayRowsWithDeviation, overallLimit),
    [allDisplayRowsWithDeviation, overallLimit],
  );
  const bookmarkState = useMemo(
    () => buildHuntBacktestBookmarkMatches(allDisplayRowsWithDeviation, bookmark),
    [allDisplayRowsWithDeviation, bookmark],
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

  if (allDisplayRows.length === 0) {
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
          <p className="sectionLabel">差枚の基準</p>
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
              <span>ボーナス数基準</span>
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
            {`${resultColumnLead}ここは保存済み実績の表示で、上のバックテスト基準切り替えは反映しません。`}
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
          deviationScope={deviationScope}
          nextGapScope={nextGapScope}
          highlightCondition={highlightCondition}
        />
      ) : (
        <section className="statusPanel">
          <h2>チェック中の機種がありません</h2>
          <p>機種名にチェックを入れると、ここに選択機種内ランキングが表示されます。</p>
        </section>
      )}

      <OverallRankingTable
        storeId={storeId}
        title={`全機種内ランキング 上位${formatNumber(allOverallRows.length)}台`}
        rows={allOverallRows}
        visibleColumns={visibleColumns}
        scoreColumnLabel={scoreColumnLabel}
        deviationScope={deviationScope}
        nextGapScope={nextGapScope}
        highlightCondition={highlightCondition}
      />

      {displayGroupsWithDeviation.map((group) => (
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
                {` 上位${formatNumber(group.rows.length)}台`}
              </h2>
            </div>
          </div>
          <div className="tableScroller directoryScroller">
            <table className="directoryTable">
              <thead>
                <tr>
                  <th>順位</th>
                  <th>{scoreColumnLabel}</th>
                  <th>偏差値</th>
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
                        {formatDeviationForScope(row, deviationScope)}
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
      ))}
    </>
  );
}
