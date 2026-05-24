"use client";

import { useEffect, useMemo, useState } from "react";

import {
  HUNT_BACKTEST_BOOKMARK_EVENT,
  HUNT_BACKTEST_BOOKMARK_SELECTION_CUSTOM,
  HUNT_BACKTEST_BOOKMARK_SELECTION_NONE,
  readSavedHuntBacktestBookmarks,
  readSelectedHuntBacktestBookmarkId,
  saveSelectedHuntBacktestBookmarkId,
} from "../lib/hunt-bookmark";

function resolveSelectedValue(storeId, bookmarks) {
  const savedValue = readSelectedHuntBacktestBookmarkId(storeId);
  if (
    savedValue === HUNT_BACKTEST_BOOKMARK_SELECTION_NONE ||
    savedValue === HUNT_BACKTEST_BOOKMARK_SELECTION_CUSTOM ||
    bookmarks.some((bookmark) => bookmark.id === savedValue)
  ) {
    return savedValue;
  }
  return bookmarks[0]?.id ?? HUNT_BACKTEST_BOOKMARK_SELECTION_CUSTOM;
}

export function HuntRankingConditionSelector({ storeId }) {
  const [bookmarks, setBookmarks] = useState([]);
  const [selectedValue, setSelectedValue] = useState(HUNT_BACKTEST_BOOKMARK_SELECTION_CUSTOM);

  useEffect(() => {
    const syncBookmarks = () => {
      const nextBookmarks = readSavedHuntBacktestBookmarks(storeId);
      setBookmarks(nextBookmarks);
      setSelectedValue(resolveSelectedValue(storeId, nextBookmarks));
    };

    syncBookmarks();
    window.addEventListener(HUNT_BACKTEST_BOOKMARK_EVENT, syncBookmarks);
    window.addEventListener("storage", syncBookmarks);

    return () => {
      window.removeEventListener(HUNT_BACKTEST_BOOKMARK_EVENT, syncBookmarks);
      window.removeEventListener("storage", syncBookmarks);
    };
  }, [storeId]);

  const options = useMemo(
    () => [
      { value: HUNT_BACKTEST_BOOKMARK_SELECTION_NONE, label: "強調なし" },
      ...bookmarks.map((bookmark) => ({ value: bookmark.id, label: bookmark.name })),
      { value: HUNT_BACKTEST_BOOKMARK_SELECTION_CUSTOM, label: "カスタム条件" },
    ],
    [bookmarks],
  );

  const handleChange = (event) => {
    const nextValue = event.currentTarget.value;
    setSelectedValue(nextValue);
    saveSelectedHuntBacktestBookmarkId(storeId, nextValue);
  };

  return (
    <div className="savedConditionPanel rankingConditionSelectorPanel">
      <label className="storeReserveField">
        <span>保存条件</span>
        <select
          className="storeReserveInput"
          value={selectedValue}
          onChange={handleChange}
        >
          {options.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      </label>
      {bookmarks.length === 0 ? (
        <p className="storeReserveHelp">
          バックテストページで条件を保存すると、ここから選べます。
        </p>
      ) : null}
    </div>
  );
}
