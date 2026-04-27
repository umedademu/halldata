"use client";

import { useEffect, useMemo, useState } from "react";

import {
  HUNT_BACKTEST_BOOKMARK_EVENT,
  areHuntBacktestBookmarksEqual,
  clearSavedHuntBacktestBookmark,
  formatHuntBacktestBookmarkSummary,
  readSavedHuntBacktestBookmark,
  saveHuntBacktestBookmark,
} from "../lib/hunt-bookmark";

export function HuntBacktestBookmarkControl({ storeId, bookmark }) {
  const [savedBookmark, setSavedBookmark] = useState(null);

  useEffect(() => {
    const syncSavedBookmark = () => {
      setSavedBookmark(readSavedHuntBacktestBookmark(storeId));
    };

    syncSavedBookmark();
    window.addEventListener(HUNT_BACKTEST_BOOKMARK_EVENT, syncSavedBookmark);
    window.addEventListener("storage", syncSavedBookmark);

    return () => {
      window.removeEventListener(HUNT_BACKTEST_BOOKMARK_EVENT, syncSavedBookmark);
      window.removeEventListener("storage", syncSavedBookmark);
    };
  }, [storeId]);

  const currentSummary = useMemo(
    () => formatHuntBacktestBookmarkSummary(bookmark),
    [bookmark],
  );
  const savedSummary = useMemo(
    () => formatHuntBacktestBookmarkSummary(savedBookmark),
    [savedBookmark],
  );
  const isCurrentSaved = useMemo(
    () => areHuntBacktestBookmarksEqual(savedBookmark, bookmark),
    [bookmark, savedBookmark],
  );

  const handleSave = () => {
    const nextBookmark = saveHuntBacktestBookmark(storeId, bookmark);
    setSavedBookmark(nextBookmark);
  };

  const handleClear = () => {
    clearSavedHuntBacktestBookmark(storeId);
    setSavedBookmark(null);
  };

  return (
    <section className="filterPanel">
      <div>
        <p className="sectionLabel">一覧への目印</p>
        <p className="filterLead">
          ここで残した目印は、下の狙い度上位で一致行を強め、不一致行を控えめに見せるために使います。
        </p>
      </div>
      <div className="backtestButtonRow">
        <button type="button" className="storeReserveButton" onClick={handleSave}>
          この条件を目印にする
        </button>
        {savedBookmark ? (
          <button
            type="button"
            className="storeReserveButton storeReserveButtonSecondary"
            onClick={handleClear}
          >
            目印を外す
          </button>
        ) : null}
      </div>
      <p
        className={`storeReserveNotice ${
          isCurrentSaved ? "storeReserveNotice-success" : "storeReserveNotice-info"
        }`}
      >
        {isCurrentSaved
          ? `この条件を目印として保存中です。${currentSummary}`
          : savedBookmark
            ? `保存中の目印があります。${savedSummary}`
            : "まだ目印は保存していません。"}
      </p>
      {!isCurrentSaved ? <p className="storeReserveHelp">現在の条件: {currentSummary}</p> : null}
    </section>
  );
}
