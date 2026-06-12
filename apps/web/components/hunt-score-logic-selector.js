"use client";

import { useEffect, useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";

import {
  encodeHuntScoreLogicCookieValue,
  getHuntScoreLogicCookieName,
} from "../lib/hunt-score-logic-selection";

const COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24 * 365;

function findOption(options, logicKey) {
  return options.find((option) => option.key === logicKey) ?? null;
}

export function HuntScoreLogicSelector({ storeId, selectedLogicKey, options, refreshOnSave = true }) {
  const router = useRouter();
  const [isPending, startTransition] = useTransition();
  const safeOptions = useMemo(
    () =>
      (Array.isArray(options) ? options : [])
        .map((option) => ({
          key: String(option?.key ?? "").trim(),
          name: String(option?.name ?? "").trim(),
        }))
        .filter((option) => option.key && option.name),
    [options],
  );
  const initialLogicKey = findOption(safeOptions, selectedLogicKey)?.key ?? safeOptions[0]?.key ?? "";
  const [appliedLogicKey, setAppliedLogicKey] = useState(initialLogicKey);
  const [logicKey, setLogicKey] = useState(initialLogicKey);
  const selectedOption = findOption(safeOptions, appliedLogicKey);
  const isChanged = logicKey && logicKey !== appliedLogicKey;

  useEffect(() => {
    setAppliedLogicKey(initialLogicKey);
    setLogicKey(initialLogicKey);
  }, [initialLogicKey]);

  if (!storeId || safeOptions.length === 0) {
    return null;
  }

  const saveLogicKey = () => {
    if (!logicKey) {
      return;
    }

    const cookieName = getHuntScoreLogicCookieName(storeId);
    document.cookie = `${cookieName}=${encodeHuntScoreLogicCookieValue(
      logicKey,
    )}; Max-Age=${COOKIE_MAX_AGE_SECONDS}; Path=/; SameSite=Lax`;
    setAppliedLogicKey(logicKey);
    if (refreshOnSave) {
      startTransition(() => {
        router.refresh();
      });
    }
  };

  return (
    <div className="huntLogicControl">
      <div className="huntLogicCurrent">
        <p className="sectionLabel">狙い度ロジック</p>
        <p className="dataSourceLabel">現在適用中: {selectedOption?.name ?? "-"}</p>
      </div>
      <label className="huntLogicSelectLabel">
        <span>切替</span>
        <select
          className="huntLogicSelect"
          value={logicKey}
          onChange={(event) => setLogicKey(event.target.value)}
        >
          {safeOptions.map((option) => (
            <option key={option.key} value={option.key}>
              {option.name}
            </option>
          ))}
        </select>
      </label>
      <button
        type="button"
        className="huntLogicSaveButton"
        disabled={!isChanged || isPending}
        onClick={saveLogicKey}
      >
        {isPending ? "保存中" : "保存"}
      </button>
    </div>
  );
}

export function HuntScoreLogicSingleSelect({
  selectedLogicKey,
  options,
  name = "huntScoreLogicKey",
  label = "使用するロジック",
}) {
  const safeOptions = useMemo(
    () =>
      (Array.isArray(options) ? options : [])
        .map((option) => ({
          key: String(option?.key ?? "").trim(),
          name: String(option?.name ?? "").trim(),
        }))
        .filter((option) => option.key && option.name),
    [options],
  );
  const initialLogicKey =
    findOption(safeOptions, selectedLogicKey)?.key ?? safeOptions[0]?.key ?? "";

  if (safeOptions.length === 0) {
    return null;
  }

  return (
    <div className="huntLogicControl">
      <div className="huntLogicCurrent">
        <p className="sectionLabel">{label}</p>
      </div>
      <label className="huntLogicSelectLabel">
        <span>選択</span>
        <select
          name={name}
          className="huntLogicSelect"
          defaultValue={initialLogicKey}
        >
          {safeOptions.map((option) => (
            <option key={option.key} value={option.key}>
              {option.name}
            </option>
          ))}
        </select>
      </label>
    </div>
  );
}

export function HuntScoreLogicMultiSelect({
  selectedLogicKeys,
  options,
  name = "huntScoreLogicKey",
  formId = "",
  label = "使用するロジック",
  selectedLabel = "選択中",
  summaryLabel = "ロジックを選ぶ",
}) {
  const safeOptions = useMemo(
    () =>
      (Array.isArray(options) ? options : [])
        .map((option) => ({
          key: String(option?.key ?? "").trim(),
          name: String(option?.name ?? "").trim(),
        }))
        .filter((option) => option.key && option.name),
    [options],
  );
  const initialLogicKeys = useMemo(() => {
    const selectedKeySet = new Set(
      (Array.isArray(selectedLogicKeys) ? selectedLogicKeys : [selectedLogicKeys])
        .map((key) => String(key ?? "").trim())
        .filter(Boolean),
    );
    const selectedKeys = safeOptions
      .filter((option) => selectedKeySet.has(option.key))
      .map((option) => option.key);
    return selectedKeys.length > 0 ? selectedKeys : safeOptions.slice(0, 1).map((option) => option.key);
  }, [safeOptions, selectedLogicKeys]);
  const [logicKeys, setLogicKeys] = useState(initialLogicKeys);
  const selectedKeySet = new Set(logicKeys);
  const selectedOptions = safeOptions.filter((option) => selectedKeySet.has(option.key));
  const selectedNames = selectedOptions.map((option) => option.name);
  const summaryText =
    selectedOptions.length === 0
      ? "未選択"
      : selectedOptions.length === 1
        ? selectedOptions[0].name
        : `${selectedOptions.length}件選択中`;
  const selectedText = selectedNames.length > 0 ? selectedNames.join("、") : "未選択";

  useEffect(() => {
    setLogicKeys(initialLogicKeys);
  }, [initialLogicKeys]);

  if (safeOptions.length === 0) {
    return null;
  }

  const handleChange = (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) {
      return;
    }

    const changedKey = String(target.value ?? "").trim();
    if (!changedKey) {
      return;
    }

    setLogicKeys((currentKeys) => {
      const nextKeySet = new Set(currentKeys);
      if (target.checked) {
        nextKeySet.add(changedKey);
      } else {
        nextKeySet.delete(changedKey);
      }

      const orderedKeys = safeOptions
        .filter((option) => nextKeySet.has(option.key))
        .map((option) => option.key);
      return orderedKeys.length > 0 ? orderedKeys : currentKeys;
    });
  };

  return (
    <div className="huntLogicControl huntLogicMultiControl">
      <div className="huntLogicCurrent">
        <p className="sectionLabel">{label}</p>
        <p className="dataSourceLabel" title={selectedText}>
          {selectedLabel}: {summaryText}
        </p>
      </div>
      <details className="huntLogicMultiSelect" onChange={handleChange}>
        <summary className="huntLogicMultiSummary">
          <span>{summaryLabel}</span>
          <span className="huntLogicMultiStatus" aria-hidden="true" />
        </summary>
        <div className="huntLogicMultiMenu">
          {safeOptions.map((option) => (
            <label key={option.key} className="huntLogicMultiOption">
              <input
                type="checkbox"
                name={name}
                value={option.key}
                form={formId || undefined}
                checked={selectedKeySet.has(option.key)}
                onChange={() => {}}
              />
              <span>{option.name}</span>
            </label>
          ))}
        </div>
      </details>
    </div>
  );
}
