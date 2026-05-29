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
    settingEstimateMode: readFormValue(formData, "settingEstimateMode"),
    settingDistribution: readFormValue(formData, "settingDistribution"),
    dailySelectionMode: readFormValues(formData, "dailySelectionMode"),
  };
}

function normalizeFieldValues(value) {
  return (Array.isArray(value) ? value : [value])
    .map((entry) => String(entry ?? "").trim())
    .filter(Boolean);
}

function syncToggleLabel(control) {
  const label = control.closest?.(".metricToggleChip");
  if (label) {
    label.classList.toggle("metricToggleChipActive", Boolean(control.checked));
  }
}

function setFormFieldValues(form, name, values, changedControls) {
  const controls = [...form.elements].filter((control) => control?.name === name);
  if (controls.length === 0) {
    return;
  }

  const normalizedValues = normalizeFieldValues(values);
  const valueSet = new Set(normalizedValues);

  for (const control of controls) {
    if (control instanceof HTMLInputElement && control.type === "hidden") {
      continue;
    }

    if (
      control instanceof HTMLInputElement &&
      (control.type === "checkbox" || control.type === "radio")
    ) {
      const nextChecked = valueSet.has(String(control.value ?? ""));
      if (control.checked !== nextChecked) {
        control.checked = nextChecked;
        changedControls.add(control);
      }
      syncToggleLabel(control);
      continue;
    }

    const nextValue = normalizedValues[0] ?? "";
    if (control.value !== nextValue) {
      control.value = nextValue;
      changedControls.add(control);
    }
  }
}

function dispatchFormChanges(changedControls) {
  for (const control of changedControls) {
    const eventName =
      control instanceof HTMLInputElement &&
      (control.type === "checkbox" || control.type === "radio")
        ? "change"
        : "input";
    control.dispatchEvent(new Event(eventName, { bubbles: true }));
  }
}

function applyBookmarkToForm(form, bookmark) {
  const changedControls = new Set();
  const periodMode = bookmark.periodMode === "range" ? "range" : "recent";
  const singleValueFields = {
    periodMode,
    recentDays: periodMode === "recent" ? bookmark.recentDays : "",
    startDate: periodMode === "range" ? bookmark.startDate : "",
    endDate: periodMode === "range" ? bookmark.endDate : "",
    rankScope: bookmark.rankScope,
    rankMin: bookmark.rankMin,
    rankMax: bookmark.rankMax,
    machineRankMin: bookmark.machineRankMin,
    machineRankMax: bookmark.machineRankMax,
    selectedRankMin: bookmark.selectedRankMin,
    selectedRankMax: bookmark.selectedRankMax,
    scoreMin: bookmark.scoreMin,
    scoreMax: bookmark.scoreMax,
    machineNextGapMin: bookmark.machineNextGapMin,
    machineNextGapMax: bookmark.machineNextGapMax,
    selectedNextGapMin: bookmark.selectedNextGapMin,
    selectedNextGapMax: bookmark.selectedNextGapMax,
    machineUpperGapMin: bookmark.machineUpperGapMin,
    machineUpperGapMax: bookmark.machineUpperGapMax,
    selectedUpperGapMin: bookmark.selectedUpperGapMin,
    selectedUpperGapMax: bookmark.selectedUpperGapMax,
    scoreDifferenceMode: bookmark.scoreDifferenceMode,
    differenceMode: bookmark.differenceMode,
    settingEstimateMode: bookmark.settingEstimateMode,
    settingDistribution: bookmark.settingDistribution,
  };
  const multiValueFields = {
    machine: bookmark.machineNames,
    dailySelectionMode: bookmark.dailySelectionMode ? [bookmark.dailySelectionMode] : [],
    machineRankRequired: bookmark.machineRankRequired ? ["1"] : [],
    selectedRankRequired: bookmark.selectedRankRequired ? ["1"] : [],
    scoreRequired: bookmark.scoreRequired ? ["1"] : [],
    machineNextGapRequired: bookmark.machineNextGapRequired ? ["1"] : [],
    selectedNextGapRequired: bookmark.selectedNextGapRequired ? ["1"] : [],
    machineUpperGapRequired: bookmark.machineUpperGapRequired ? ["1"] : [],
    selectedUpperGapRequired: bookmark.selectedUpperGapRequired ? ["1"] : [],
  };

  for (const [fieldName, fieldValue] of Object.entries(singleValueFields)) {
    setFormFieldValues(form, fieldName, fieldValue, changedControls);
  }
  for (const [fieldName, fieldValue] of Object.entries(multiValueFields)) {
    setFormFieldValues(form, fieldName, fieldValue, changedControls);
  }

  dispatchFormChanges(changedControls);
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
    const form = formId ? document.getElementById(formId) : null;
    if (!(form instanceof HTMLFormElement)) {
      setMessage("条件を反映できる入力欄が見つかりません。");
      return;
    }

    applyBookmarkToForm(form, bookmark);
    setMessage(`「${bookmark.name}」を読み込みました。`);
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
      <div className="savedConditionSaveRow">
        <input
          type="text"
          className="storeReserveInput savedConditionNameInput"
          aria-label="保存する条件名"
          placeholder={defaultName}
          value={name}
          onChange={(event) => setName(event.currentTarget.value)}
          onKeyDown={handleNameKeyDown}
        />
        <button type="button" className="storeReserveButton" onClick={handleSave}>
          ★ 条件を保存
        </button>
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
