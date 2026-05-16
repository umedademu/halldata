"use client";

import { useEffect } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

const STORAGE_KEY_PREFIX = "hunt-backtest-form-state:";
const LEGACY_EVENT_STORAGE_KEY_PREFIX = "hunt-backtest-event-filters:";
const MANAGED_PARAM_KEYS = [
  "show",
  "periodMode",
  "recentDays",
  "startDate",
  "endDate",
  "backtestEventTouched",
  "backtestDayTail",
  "backtestWeekday",
  "machineTouched",
  "aimMachineGroup",
  "hanabiMachineGroup",
  "machine",
  "dailySelectionMode",
  "rankMin",
  "rankMax",
  "rankRequired",
  "scoreMin",
  "scoreRequired",
  "nextGapMin",
  "nextGapRequired",
  "scoreDifferenceMode",
  "differenceMode",
  "rankScope",
  "nextGapScope",
  "showGraph",
];
const MANAGED_PARAM_KEY_SET = new Set(MANAGED_PARAM_KEYS);

function storageKeyForStore(storeId) {
  return `${STORAGE_KEY_PREFIX}${storeId}`;
}

function legacyEventStorageKeyForStore(storeId) {
  return `${LEGACY_EVENT_STORAGE_KEY_PREFIX}${storeId}`;
}

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

function normalizeIntegerValues(values, min, max) {
  const normalizedValues = new Set();
  for (const value of Array.isArray(values) ? values : []) {
    const parsedValue = Number(value);
    if (Number.isInteger(parsedValue) && parsedValue >= min && parsedValue <= max) {
      normalizedValues.add(parsedValue);
    }
  }
  return [...normalizedValues].sort((left, right) => left - right);
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

function saveState(storeId, entries) {
  const normalizedEntries = normalizeStateEntries(entries);
  if (!storeId || normalizedEntries.length === 0) {
    return;
  }

  try {
    window.localStorage.setItem(
      storageKeyForStore(storeId),
      JSON.stringify({
        version: 1,
        entries: normalizedEntries,
      }),
    );
  } catch {
    // 保存できない環境では、その場の入力だけを有効にします。
  }
}

function readLegacyEventState(storeId) {
  if (!storeId) {
    return [];
  }

  try {
    const rawValue = window.localStorage.getItem(legacyEventStorageKeyForStore(storeId));
    if (!rawValue) {
      return [];
    }

    const parsedValue = JSON.parse(rawValue);
    const entries = [["backtestEventTouched", "1"]];
    for (const dayTail of normalizeIntegerValues(parsedValue?.dayTails ?? [], 0, 9)) {
      entries.push(["backtestDayTail", String(dayTail)]);
    }
    for (const weekday of normalizeIntegerValues(parsedValue?.weekdays ?? [], 0, 6)) {
      entries.push(["backtestWeekday", String(weekday)]);
    }
    return entries;
  } catch {
    return [];
  }
}

function readSavedState(storeId) {
  if (!storeId) {
    return [];
  }

  try {
    const rawValue = window.localStorage.getItem(storageKeyForStore(storeId));
    if (!rawValue) {
      return readLegacyEventState(storeId);
    }

    const parsedValue = JSON.parse(rawValue);
    const entries = normalizeStateEntries(parsedValue?.entries);
    return entries.length > 0 ? entries : readLegacyEventState(storeId);
  } catch {
    return readLegacyEventState(storeId);
  }
}

function applyStateToSearchParams(searchParams, entries) {
  const nextSearchParams = new URLSearchParams(searchParams.toString());
  for (const key of MANAGED_PARAM_KEYS) {
    nextSearchParams.delete(key);
  }
  for (const [key, value] of entries) {
    nextSearchParams.append(key, value);
  }
  return nextSearchParams;
}

export function HuntBacktestFormStateSync({ storeId, formId, formStateKey = "" }) {
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();

  useEffect(() => {
    if (!storeId) {
      return;
    }

    if (hasManagedSearchParams(searchParams)) {
      saveState(storeId, readStateFromSearchParams(searchParams));
      return;
    }

    const savedEntries = readSavedState(storeId);
    if (savedEntries.length === 0) {
      return;
    }

    const nextSearchParams = applyStateToSearchParams(searchParams, savedEntries);
    const queryText = nextSearchParams.toString();
    router.replace(queryText ? `${pathname}?${queryText}` : pathname, { scroll: false });
  }, [pathname, router, searchParams, storeId]);

  useEffect(() => {
    if (!storeId || !formId) {
      return undefined;
    }

    const form = document.getElementById(formId);
    if (!form) {
      return undefined;
    }

    const saveFormState = () => {
      saveState(storeId, readStateFromForm(form));
    };

    form.addEventListener("change", saveFormState);
    form.addEventListener("input", saveFormState);
    form.addEventListener("submit", saveFormState);

    return () => {
      form.removeEventListener("change", saveFormState);
      form.removeEventListener("input", saveFormState);
      form.removeEventListener("submit", saveFormState);
    };
  }, [formId, formStateKey, storeId]);

  return null;
}
