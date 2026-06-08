"use client";

import { useEffect, useRef } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

const STORAGE_KEY = "cross-store-backtest-form-state";
const MANAGED_PARAM_KEYS = [
  "show",
  "periodMode",
  "recentDays",
  "startDate",
  "endDate",
  "machineTouched",
  "machine",
  "aimMachineGroup",
  "hanabiMachineGroup",
  "logicKey",
  "scoreDifferenceMode",
  "differenceMode",
  "settingEstimateMode",
  "settingDistribution",
  "rankMin",
  "rankMax",
  "machineRankMin",
  "machineRankMax",
  "selectedRankMin",
  "selectedRankMax",
  "scoreMin",
  "scoreMax",
  "upperGapMin",
  "upperGapMax",
  "nextGapMin",
  "nextGapMax",
  "machineUpperGapMin",
  "machineUpperGapMax",
  "machineNextGapMin",
  "machineNextGapMax",
  "selectedUpperGapMin",
  "selectedUpperGapMax",
  "selectedNextGapMin",
  "selectedNextGapMax",
  "rankRequired",
  "machineRankRequired",
  "selectedRankRequired",
  "scoreRequired",
  "upperGapRequired",
  "nextGapRequired",
  "machineUpperGapRequired",
  "machineNextGapRequired",
  "selectedUpperGapRequired",
  "selectedNextGapRequired",
  "rankScope",
  "nextGapScope",
  "prefecture",
  "area",
  "backtestDayTail",
  "backtestZoro",
  "backtestWeekday",
  "backtestMonthDay",
  "minActualRows",
  "minMatchedDateCount",
  "minSlotCount",
  "maxSlotCount",
  "limit",
  "rankingMetric",
];
const MANAGED_PARAM_KEY_SET = new Set(MANAGED_PARAM_KEYS);

function normalizeEntry(key, value) {
  const normalizedKey = String(key ?? "").trim();
  if (!MANAGED_PARAM_KEY_SET.has(normalizedKey)) {
    return null;
  }

  return [normalizedKey, String(value ?? "")];
}

function normalizeStateEntries(entries) {
  const normalizedEntries = [];
  for (const entry of Array.isArray(entries) ? entries : []) {
    const normalizedEntry = Array.isArray(entry) ? normalizeEntry(entry[0], entry[1]) : null;
    if (normalizedEntry) {
      normalizedEntries.push(normalizedEntry);
    }
  }
  return normalizedEntries;
}

function readStateFromSearchParams(searchParams) {
  const entries = [];
  for (const key of MANAGED_PARAM_KEYS) {
    for (const value of searchParams.getAll(key)) {
      entries.push([key, value]);
    }
  }
  return entries;
}

function readStateFromForm(form) {
  const formData = new FormData(form);
  const entries = [];
  for (const key of MANAGED_PARAM_KEYS) {
    for (const value of formData.getAll(key)) {
      entries.push([key, String(value ?? "")]);
    }
  }
  return entries;
}

function hasManagedSearchParams(searchParams) {
  return MANAGED_PARAM_KEYS.some((key) => searchParams.has(key));
}

function buildSearchText(entries) {
  const searchParams = new URLSearchParams();
  for (const [key, value] of normalizeStateEntries(entries)) {
    searchParams.append(key, value);
  }
  return searchParams.toString();
}

function saveState(entries) {
  const normalizedEntries = normalizeStateEntries(entries);
  if (normalizedEntries.length === 0) {
    return;
  }

  try {
    window.localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        version: 1,
        entries: normalizedEntries,
      }),
    );
  } catch {
    // 保存できない環境では、その場の入力だけを有効にします。
  }
}

function readSavedState() {
  try {
    const rawValue = window.localStorage.getItem(STORAGE_KEY);
    if (!rawValue) {
      return [];
    }

    const parsedValue = JSON.parse(rawValue);
    return normalizeStateEntries(parsedValue?.entries);
  } catch {
    return [];
  }
}

export function CrossStoreBacktestFormStateSync({ formId, formStateKey = "" }) {
  const hasSeenManagedParamsRef = useRef(false);
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();

  useEffect(() => {
    if (hasManagedSearchParams(searchParams)) {
      hasSeenManagedParamsRef.current = true;
      saveState(readStateFromSearchParams(searchParams));
      return;
    }

    if (hasSeenManagedParamsRef.current) {
      return;
    }

    const savedSearchText = buildSearchText(readSavedState());
    if (savedSearchText) {
      router.replace(`${pathname}?${savedSearchText}`, { scroll: false });
    }
  }, [formStateKey, pathname, router, searchParams]);

  useEffect(() => {
    if (!formId) {
      return undefined;
    }

    const form = document.getElementById(formId);
    if (!form) {
      return undefined;
    }

    const saveFormState = () => {
      saveState(readStateFromForm(form));
    };

    form.addEventListener("change", saveFormState);
    form.addEventListener("input", saveFormState);
    form.addEventListener("submit", saveFormState);

    return () => {
      form.removeEventListener("change", saveFormState);
      form.removeEventListener("input", saveFormState);
      form.removeEventListener("submit", saveFormState);
    };
  }, [formId, formStateKey]);

  return null;
}
