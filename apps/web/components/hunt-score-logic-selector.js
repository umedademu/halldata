"use client";

import { useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";

import {
  encodeHuntScoreLogicCookieValue,
  getHuntScoreLogicCookieName,
} from "../lib/hunt-score-logic-selection";

const COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24 * 365;

function findOption(options, logicKey) {
  return options.find((option) => option.key === logicKey) ?? null;
}

export function HuntScoreLogicSelector({ storeId, selectedLogicKey, options }) {
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
  const [logicKey, setLogicKey] = useState(initialLogicKey);
  const selectedOption = findOption(safeOptions, initialLogicKey);
  const isChanged = logicKey && logicKey !== initialLogicKey;

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
    startTransition(() => {
      router.refresh();
    });
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
