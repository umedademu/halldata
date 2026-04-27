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
    label: "翌営業日差枚",
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

export function HuntRankingTable({ storeId, rows }) {
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
  const bookmarkState = useMemo(
    () => buildHuntBacktestBookmarkMatches(rows, bookmark),
    [bookmark, rows],
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

  if (rows.length === 0) {
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
          <p className="filterLead">翌営業日の実績列だけを切り替えられます。</p>
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

      <section className="tablePanel directoryPanel">
        <div className="tablePanelHeader">
          <div>
            <p className="sectionLabel">狙い度上位</p>
            <h2 className="tablePanelTitle">高得点上位{formatNumber(rows.length)}台</h2>
          </div>
        </div>
        <div className="tableScroller directoryScroller">
          <table className="directoryTable">
            <thead>
              <tr>
                <th>順位</th>
                <th>狙い度</th>
                <th>機種名</th>
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
                const rowClassName = [
                  getSettingEstimateHighlightClass(row.nextSettingEstimate?.average),
                  bookmarkState.bookmark && rowMatchState ? "huntBookmarkMatchedRow" : "",
                ]
                  .filter(Boolean)
                  .join(" ");

                return (
                  <tr key={`${row.machineName}-${row.slotNumber}-${row.rank}`} className={rowClassName}>
                    <td>{row.rank}</td>
                    <td>{formatNumber(row.huntScore)}</td>
                    <td>
                      <Link
                        href={`/stores/${storeId}/machines/${encodeURIComponent(row.machineName)}`}
                        className="directoryPrimaryLink"
                      >
                        {row.machineName}
                      </Link>
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
    </>
  );
}
