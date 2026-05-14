"use client";

import { useEffect } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

const STORAGE_KEY_PREFIX = "hunt-ranking-form-state:";
const MANAGED_PARAM_KEYS = [
  "show",
  "machineTouched",
  "date",
  "limit",
  "differenceMode",
  "machine",
  "aimMachineGroup",
  "hanabiMachineGroup",
  "rankMin",
  "rankMax",
  "scoreMin",
  "deviationMin",
  "nextGapMin",
  "rankRequired",
  "scoreRequired",
  "deviationRequired",
  "nextGapRequired",
  "rankScope",
  "deviationScope",
  "nextGapScope",
  "showMachineTopCandidates",
];
const MANAGED_PARAM_KEY_SET = new Set(MANAGED_PARAM_KEYS);

function storageKeyForStore(storeId) {
  return `${STORAGE_KEY_PREFIX}${storeId}`;
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

function readSavedState(storeId) {
  if (!storeId) {
    return [];
  }

  try {
    const rawValue = window.localStorage.getItem(storageKeyForStore(storeId));
    if (!rawValue) {
      return [];
    }

    const parsedValue = JSON.parse(rawValue);
    return normalizeStateEntries(parsedValue?.entries);
  } catch {
    return [];
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

export function HuntRankingFormStateSync({ storeId, formId, formStateKey = "" }) {
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
