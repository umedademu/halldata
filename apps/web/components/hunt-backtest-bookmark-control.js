"use client";

import { useEffect, useMemo, useState } from "react";

import {
  HUNT_BACKTEST_BOOKMARK_EVENT,
  areHuntBacktestBookmarksEqual,
  clearSavedHuntBacktestBookmark,
  formatHuntBacktestBookmarkPeriod,
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
  const currentPeriod = useMemo(
    () => formatHuntBacktestBookmarkPeriod(bookmark),
    [bookmark],
  );
  const savedPeriod = useMemo(
    () => formatHuntBacktestBookmarkPeriod(savedBookmark),
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
      <div className="backtestButtonRow">
        <button type="button" className="storeReserveButton" onClick={handleSave}>
          ★ この条件を保存
        </button>
        {savedBookmark ? (
          <button
            type="button"
            className="storeReserveButton storeReserveButtonSecondary"
            onClick={handleClear}
          >
            保存を解除
          </button>
        ) : null}
      </div>
      <p
        className={`storeReserveNotice ${
          isCurrentSaved ? "storeReserveNotice-success" : "storeReserveNotice-info"
        }`}
      >
        {isCurrentSaved
          ? `この条件を保存中です。${currentSummary}`
          : savedBookmark
            ? `保存中の条件があります。条件は ${savedSummary} です。`
            : "まだ条件は保存していません。"}
      </p>
      {isCurrentSaved && currentPeriod ? (
        <p className="storeReserveHelp">保存時のバックテスト期間: {currentPeriod}</p>
      ) : null}
      {!isCurrentSaved ? <p className="storeReserveHelp">現在の条件: {currentSummary}</p> : null}
      {!isCurrentSaved && savedPeriod ? (
        <p className="storeReserveHelp">保存中のバックテスト期間: {savedPeriod}</p>
      ) : null}
    </section>
  );
}
