"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import {
  formatAverageGames,
  formatNumber,
  formatPercent,
  formatRatio,
  formatSignedNumber,
} from "../lib/format";
import {
  HUNT_BACKTEST_BOOKMARK_EVENT,
  buildHuntBacktestBookmarkMatches,
  buildHuntBacktestBookmarkRowKey,
  formatHuntBacktestBookmarkSummary,
  readSavedHuntBacktestBookmark,
} from "../lib/hunt-bookmark";
import {
  formatSettingEstimateScore,
  getSettingEstimateHighlightClass,
} from "../lib/setting-estimates";

const DEFAULT_VISIBLE_RESULT_KEYS = [
  "difference_value",
  "games_count",
  "bb_count",
  "rb_count",
  "combined_ratio_text",
  "setting_estimate",
];

const RESULT_COLUMNS = [
  {
    key: "difference_value",
    label: "翌営業日差枚（保存値）",
    render: (row) => formatSignedNumber(row.nextRecord?.difference_value),
  },
  {
    key: "games_count",
    label: "翌営業日G数",
    render: (row) => formatAverageGames(row.nextRecord?.games_count),
  },
  {
    key: "bb_count",
    label: "翌営業日BB",
    render: (row) => formatAverageGames(row.nextRecord?.bb_count),
  },
  {
    key: "rb_count",
    label: "翌営業日RB",
    render: (row) => formatAverageGames(row.nextRecord?.rb_count),
  },
  {
    key: "combined_ratio_text",
    label: "翌営業日合成",
    render: (row) => formatRatio(row.nextRecord?.combined_ratio_text),
  },
  {
    key: "setting_estimate",
    label: "翌営業日設定",
    render: (row) => formatSettingEstimateScore(row.nextSettingEstimate?.average),
  },
  {
    key: "payout_rate",
    label: "翌営業日出率",
    render: (row) => formatPercent(row.nextRecord?.payout_rate),
  },
  {
    key: "bb_ratio_text",
    label: "翌営業日BB率",
    render: (row) => formatRatio(row.nextRecord?.bb_ratio_text),
  },
  {
    key: "rb_ratio_text",
    label: "翌営業日RB率",
    render: (row) => formatRatio(row.nextRecord?.rb_ratio_text),
  },
];

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

function buildSortedRankingRows(rankingGroups) {
  return rankingGroups
    .flatMap((group) => group.rows)
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

function OverallRankingTable({ storeId, title, rows, visibleColumns, bookmarkState }) {
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
              <th>条件</th>
              <th>順位</th>
              <th>狙い度</th>
              <th className="directoryNameHeader">機種名</th>
              <th>台番</th>
              {visibleColumns.map((column) => (
                <th key={column.key}>{column.label}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const rowMatchState = bookmarkState.matchByRowKey.get(
                buildHuntBacktestBookmarkRowKey(row),
              );
              const rowClassName = getSettingEstimateHighlightClass(row.nextSettingEstimate?.average);

              return (
                <tr
                  key={`${row.rowKey ?? row.machineName}-${row.slotNumber}-${title}-${row.rank}`}
                  className={rowClassName}
                >
                  <td className="huntBookmarkConditionCell">
                    {bookmarkState.bookmark && rowMatchState ? (
                      <span className="huntBookmarkConditionMark">★</span>
                    ) : null}
                  </td>
                  <td>{row.rank}</td>
                  <td>{formatNumber(row.huntScore)}</td>
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
}) {
  const [visibleResultKeys, setVisibleResultKeys] = useState(DEFAULT_VISIBLE_RESULT_KEYS);
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

  const visibleColumns = useMemo(
    () => RESULT_COLUMNS.filter((column) => visibleResultKeys.includes(column.key)),
    [visibleResultKeys],
  );
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
  const selectedOverallRows = useMemo(
    () => buildOverallRows(displayRows, overallLimit),
    [displayRows, overallLimit],
  );
  const allOverallRows = useMemo(
    () => buildOverallRows(allDisplayRows, overallLimit),
    [allDisplayRows, overallLimit],
  );
  const bookmarkState = useMemo(
    () => buildHuntBacktestBookmarkMatches(allDisplayRows, bookmark),
    [allDisplayRows, bookmark],
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

      return RESULT_COLUMNS.filter((column) => nextKeys.has(column.key)).map((column) => column.key);
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
          <p className="sectionLabel">表示する列</p>
          <p className="filterLead">
            翌営業日の実績列だけを切り替えられます。ここは保存済み実績の表示で、上のバックテスト基準切り替えは反映しません。
          </p>
        </div>
        {bookmarkState.bookmark ? (
          <p className="storeReserveNotice storeReserveNotice-info">
            {`目印の強調条件を反映中です。${bookmarkSummary} / 表示中${formatNumber(
              bookmarkState.totalRowCount,
            )}台のうち${formatNumber(bookmarkState.matchedRowCount)}台が一致しています。`}
          </p>
        ) : null}
        <div className="metricToggleRow">
          {RESULT_COLUMNS.map((column) => {
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
          bookmarkState={bookmarkState}
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
        bookmarkState={bookmarkState}
      />

      {displayGroups.map((group) => (
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
                  <th>条件</th>
                  <th>順位</th>
                  <th>狙い度</th>
                  <th>台番</th>
                  {visibleColumns.map((column) => (
                    <th key={column.key}>{column.label}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {group.rows.map((row) => {
                  const rowMatchState = bookmarkState.matchByRowKey.get(
                    buildHuntBacktestBookmarkRowKey(row),
                  );
                  const rowClassName = getSettingEstimateHighlightClass(row.nextSettingEstimate?.average);

                  return (
                    <tr
                      key={`${row.rowKey ?? row.machineName}-${row.slotNumber}-${row.rank}`}
                      className={rowClassName}
                    >
                      <td className="huntBookmarkConditionCell">
                        {bookmarkState.bookmark && rowMatchState ? (
                          <span className="huntBookmarkConditionMark">★</span>
                        ) : null}
                      </td>
                      <td>{row.rank}</td>
                      <td>{formatNumber(row.huntScore)}</td>
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
