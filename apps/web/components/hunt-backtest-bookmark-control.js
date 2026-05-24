"use client";

import { useEffect, useMemo, useState } from "react";

import {
  HUNT_BACKTEST_BOOKMARK_EVENT,
  deleteSavedHuntBacktestBookmark,
  readSavedHuntBacktestBookmarks,
  saveHuntBacktestBookmark,
} from "../lib/hunt-bookmark";

function readFormValues(formData, key) {
  return formData
    .getAll(key)
    .map((value) => String(value ?? "").trim())
    .filter(Boolean);
}

function readFormValue(formData, key) {
  return String(formData.get(key) ?? "").trim();
}

function buildBookmarkFromForm(form, storeId, allMachineCount, name) {
  const formData = new FormData(form);
  const periodMode = readFormValue(formData, "periodMode") === "range" ? "range" : "recent";

  return {
    name,
    storeId,
    periodMode,
    recentDays: readFormValue(formData, "recentDays"),
    startDate: periodMode === "range" ? readFormValue(formData, "startDate") : "",
    endDate: periodMode === "range" ? readFormValue(formData, "endDate") : "",
    allMachineCount,
    machineNames: readFormValues(formData, "machine"),
    rankMin: readFormValue(formData, "rankMin"),
    rankMax: readFormValue(formData, "rankMax"),
    rankScope: readFormValue(formData, "rankScope"),
    machineRankMin: readFormValue(formData, "machineRankMin"),
    machineRankMax: readFormValue(formData, "machineRankMax"),
    selectedRankMin: readFormValue(formData, "selectedRankMin"),
    selectedRankMax: readFormValue(formData, "selectedRankMax"),
    scoreMin: readFormValue(formData, "scoreMin"),
    scoreMax: readFormValue(formData, "scoreMax"),
    nextGapScope: readFormValue(formData, "nextGapScope"),
    nextGapMin: readFormValue(formData, "nextGapMin"),
    nextGapMax: readFormValue(formData, "nextGapMax"),
    upperGapMin: readFormValue(formData, "upperGapMin"),
    upperGapMax: readFormValue(formData, "upperGapMax"),
    machineNextGapMin: readFormValue(formData, "machineNextGapMin"),
    machineNextGapMax: readFormValue(formData, "machineNextGapMax"),
    selectedNextGapMin: readFormValue(formData, "selectedNextGapMin"),
    selectedNextGapMax: readFormValue(formData, "selectedNextGapMax"),
    machineUpperGapMin: readFormValue(formData, "machineUpperGapMin"),
    machineUpperGapMax: readFormValue(formData, "machineUpperGapMax"),
    selectedUpperGapMin: readFormValue(formData, "selectedUpperGapMin"),
    selectedUpperGapMax: readFormValue(formData, "selectedUpperGapMax"),
    rankRequired: readFormValues(formData, "rankRequired"),
    machineRankRequired: readFormValues(formData, "machineRankRequired"),
    selectedRankRequired: readFormValues(formData, "selectedRankRequired"),
    scoreRequired: readFormValues(formData, "scoreRequired"),
    nextGapRequired: readFormValues(formData, "nextGapRequired"),
    upperGapRequired: readFormValues(formData, "upperGapRequired"),
    machineNextGapRequired: readFormValues(formData, "machineNextGapRequired"),
    selectedNextGapRequired: readFormValues(formData, "selectedNextGapRequired"),
    machineUpperGapRequired: readFormValues(formData, "machineUpperGapRequired"),
    selectedUpperGapRequired: readFormValues(formData, "selectedUpperGapRequired"),
    scoreDifferenceMode: readFormValue(formData, "scoreDifferenceMode"),
    differenceMode: readFormValue(formData, "differenceMode"),
    dailySelectionMode: readFormValues(formData, "dailySelectionMode"),
    eventDayTails: readFormValues(formData, "backtestDayTail"),
    eventZoro: readFormValues(formData, "backtestZoro"),
    eventWeekdays: readFormValues(formData, "backtestWeekday"),
    eventMonthDays: readFormValues(formData, "backtestMonthDay"),
  };
}

function appendSearchParam(searchParams, key, value) {
  const text = String(value ?? "").trim();
  if (text) {
    searchParams.append(key, text);
  }
}

function appendSearchParams(searchParams, key, values) {
  for (const value of Array.isArray(values) ? values : [values]) {
    appendSearchParam(searchParams, key, value);
  }
}

function buildBookmarkSearchParams(bookmark) {
  const searchParams = new URLSearchParams();

  appendSearchParam(searchParams, "periodMode", bookmark.periodMode);
  if (bookmark.periodMode === "range") {
    appendSearchParam(searchParams, "startDate", bookmark.startDate);
    appendSearchParam(searchParams, "endDate", bookmark.endDate);
  } else {
    appendSearchParam(searchParams, "recentDays", bookmark.recentDays);
  }
  searchParams.set("machineTouched", "1");
  appendSearchParams(searchParams, "machine", bookmark.machineNames);
  if (bookmark.combineAimJuggler) {
    searchParams.append("aimMachineGroup", "1");
  }
  if (bookmark.combineHanabi) {
    searchParams.append("hanabiMachineGroup", "1");
  }
  appendSearchParam(searchParams, "scoreDifferenceMode", bookmark.scoreDifferenceMode);
  appendSearchParam(searchParams, "differenceMode", bookmark.differenceMode);
  appendSearchParam(searchParams, "rankMin", bookmark.rankMin);
  appendSearchParam(searchParams, "rankMax", bookmark.rankMax);
  appendSearchParam(searchParams, "rankScope", bookmark.rankScope);
  appendSearchParam(searchParams, "machineRankMin", bookmark.machineRankMin);
  appendSearchParam(searchParams, "machineRankMax", bookmark.machineRankMax);
  appendSearchParam(searchParams, "selectedRankMin", bookmark.selectedRankMin);
  appendSearchParam(searchParams, "selectedRankMax", bookmark.selectedRankMax);
  appendSearchParam(searchParams, "scoreMin", bookmark.scoreMin);
  appendSearchParam(searchParams, "scoreMax", bookmark.scoreMax);
  appendSearchParam(searchParams, "machineNextGapMin", bookmark.machineNextGapMin);
  appendSearchParam(searchParams, "machineNextGapMax", bookmark.machineNextGapMax);
  appendSearchParam(searchParams, "selectedNextGapMin", bookmark.selectedNextGapMin);
  appendSearchParam(searchParams, "selectedNextGapMax", bookmark.selectedNextGapMax);
  appendSearchParam(searchParams, "machineUpperGapMin", bookmark.machineUpperGapMin);
  appendSearchParam(searchParams, "machineUpperGapMax", bookmark.machineUpperGapMax);
  appendSearchParam(searchParams, "selectedUpperGapMin", bookmark.selectedUpperGapMin);
  appendSearchParam(searchParams, "selectedUpperGapMax", bookmark.selectedUpperGapMax);
  appendSearchParam(searchParams, "machineRankRequired", bookmark.machineRankRequired ? "1" : "0");
  appendSearchParam(searchParams, "selectedRankRequired", bookmark.selectedRankRequired ? "1" : "0");
  appendSearchParam(searchParams, "scoreRequired", bookmark.scoreRequired ? "1" : "0");
  appendSearchParam(searchParams, "machineNextGapRequired", bookmark.machineNextGapRequired ? "1" : "0");
  appendSearchParam(searchParams, "selectedNextGapRequired", bookmark.selectedNextGapRequired ? "1" : "0");
  appendSearchParam(searchParams, "machineUpperGapRequired", bookmark.machineUpperGapRequired ? "1" : "0");
  appendSearchParam(searchParams, "selectedUpperGapRequired", bookmark.selectedUpperGapRequired ? "1" : "0");
  appendSearchParam(searchParams, "dailySelectionMode", bookmark.dailySelectionMode);
  searchParams.set("backtestEventTouched", "1");
  appendSearchParams(searchParams, "backtestDayTail", bookmark.eventDayTails);
  if (bookmark.eventZoro) {
    searchParams.set("backtestZoro", "1");
  }
  appendSearchParams(searchParams, "backtestWeekday", bookmark.eventWeekdays);
  appendSearchParams(searchParams, "backtestMonthDay", bookmark.eventMonthDays);

  return searchParams;
}

export function HuntBacktestBookmarkControl({ storeId, formId, allMachineCount = 0 }) {
  const [bookmarks, setBookmarks] = useState([]);
  const [name, setName] = useState("");
  const [message, setMessage] = useState("");

  const defaultName = useMemo(
    () => `条件${bookmarks.length + 1}`,
    [bookmarks.length],
  );

  useEffect(() => {
    const syncBookmarks = () => {
      setBookmarks(readSavedHuntBacktestBookmarks(storeId));
    };

    syncBookmarks();
    window.addEventListener(HUNT_BACKTEST_BOOKMARK_EVENT, syncBookmarks);
    window.addEventListener("storage", syncBookmarks);

    return () => {
      window.removeEventListener(HUNT_BACKTEST_BOOKMARK_EVENT, syncBookmarks);
      window.removeEventListener("storage", syncBookmarks);
    };
  }, [storeId]);

  const handleSave = () => {
    const form = formId ? document.getElementById(formId) : null;
    if (!(form instanceof HTMLFormElement)) {
      setMessage("保存できる条件入力欄が見つかりません。");
      return;
    }

    const savedBookmark = saveHuntBacktestBookmark(
      storeId,
      buildBookmarkFromForm(form, storeId, allMachineCount, name || defaultName),
    );
    if (!savedBookmark) {
      setMessage("機種を選択してから保存してください。");
      return;
    }

    setName("");
    setMessage(`「${savedBookmark.name}」を保存しました。`);
  };

  const handleLoad = (bookmark) => {
    const searchParams = buildBookmarkSearchParams(bookmark);
    window.location.href = `${window.location.pathname}?${searchParams.toString()}`;
  };

  const handleDelete = (bookmark) => {
    deleteSavedHuntBacktestBookmark(storeId, bookmark.id);
    setMessage(`「${bookmark.name}」を削除しました。`);
  };

  const handleNameKeyDown = (event) => {
    if (event.key !== "Enter") {
      return;
    }
    event.preventDefault();
    handleSave();
  };

  return (
    <section className="savedConditionPanel">
      <div className="savedConditionHeader">
        <p className="filterControlLabel">保存済み条件</p>
        <div className="savedConditionSaveRow">
          <label className="storeReserveField savedConditionNameField">
            <span>条件名</span>
            <input
              type="text"
              className="storeReserveInput"
              placeholder={defaultName}
              value={name}
              onChange={(event) => setName(event.currentTarget.value)}
              onKeyDown={handleNameKeyDown}
            />
          </label>
          <button type="button" className="storeReserveButton" onClick={handleSave}>
            ★ 条件を保存
          </button>
        </div>
      </div>
      {bookmarks.length > 0 ? (
        <div className="savedConditionList">
          {bookmarks.map((bookmark) => (
            <div key={bookmark.id} className="savedConditionItem">
              <button
                type="button"
                className="savedConditionNameButton"
                onClick={() => handleLoad(bookmark)}
              >
                {bookmark.name}
              </button>
              <button
                type="button"
                className="savedConditionDeleteButton"
                aria-label={`${bookmark.name}を削除`}
                title="削除"
                onClick={() => handleDelete(bookmark)}
              >
                ×
              </button>
            </div>
          ))}
        </div>
      ) : null}
      {message ? <p className="storeReserveHelp">{message}</p> : null}
    </section>
  );
}
