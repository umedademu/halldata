"use client";

import { useEffect, useRef } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

const STORAGE_KEY_PREFIX = "hunt-backtest-form-state:";
const MANAGED_PARAM_KEYS = [
  "periodMode",
  "recentDays",
  "startDate",
  "endDate",
  "machineTouched",
  "machine",
  "huntScoreLogicKey",
  "logicConditionMode",
  "machineEvaluationMode",
  "dailySelectionMode",
  "rankMin",
  "rankMax",
  "rankRequired",
  "machineRankMin",
  "machineRankMax",
  "machineRankRequired",
  "selectedRankMin",
  "selectedRankMax",
  "selectedRankRequired",
  "scoreMin",
  "scoreMax",
  "scoreRequired",
  "machineEvaluationScoreMin",
  "machineEvaluationScoreMax",
  "machineEvaluationScoreRequired",
  "machineEvaluationRankMin",
  "machineEvaluationRankMax",
  "machineEvaluationRankRequired",
  "selectedMachineEvaluationRankMin",
  "selectedMachineEvaluationRankMax",
  "selectedMachineEvaluationRankRequired",
  "machineEvaluationUpperGapMin",
  "machineEvaluationUpperGapMax",
  "machineEvaluationUpperGapRequired",
  "machineEvaluationNextGapMin",
  "machineEvaluationNextGapMax",
  "machineEvaluationNextGapRequired",
  "selectedMachineEvaluationUpperGapMin",
  "selectedMachineEvaluationUpperGapMax",
  "selectedMachineEvaluationUpperGapRequired",
  "selectedMachineEvaluationNextGapMin",
  "selectedMachineEvaluationNextGapMax",
  "selectedMachineEvaluationNextGapRequired",
  "upperGapMin",
  "upperGapMax",
  "upperGapRequired",
  "nextGapMin",
  "nextGapMax",
  "nextGapRequired",
  "machineUpperGapMin",
  "machineUpperGapMax",
  "machineUpperGapRequired",
  "machineNextGapMin",
  "machineNextGapMax",
  "machineNextGapRequired",
  "selectedUpperGapMin",
  "selectedUpperGapMax",
  "selectedUpperGapRequired",
  "selectedNextGapMin",
  "selectedNextGapMax",
  "selectedNextGapRequired",
  "scoreDifferenceMode",
  "differenceMode",
  "settingEstimateMode",
  "settingDistribution",
  "rankScope",
  "nextGapScope",
];
const MANAGED_PARAM_KEY_SET = new Set(MANAGED_PARAM_KEYS);
const DEFAULTED_DIFFERENCE_PARAM_KEYS = new Set(["scoreDifferenceMode", "differenceMode"]);

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

function omitDefaultedDifferenceEntries(entries) {
  return normalizeStateEntries(entries).filter(([key]) => !DEFAULTED_DIFFERENCE_PARAM_KEYS.has(key));
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

function applyStateToForm(form, entries) {
  if (!form) {
    return;
  }

  const valuesByKey = new Map();
  for (const [key, value] of normalizeStateEntries(entries)) {
    if (!valuesByKey.has(key)) {
      valuesByKey.set(key, []);
    }
    valuesByKey.get(key).push(value);
  }

  const controls = [...form.elements].filter((control) =>
    MANAGED_PARAM_KEY_SET.has(String(control?.name ?? "")),
  );
  const controlsByName = new Map();
  for (const control of controls) {
    const name = String(control.name ?? "");
    if (!controlsByName.has(name)) {
      controlsByName.set(name, []);
    }
    controlsByName.get(name).push(control);
  }

  for (const [name, namedControls] of controlsByName.entries()) {
    const values = valuesByKey.get(name) ?? [];
    const valueSet = new Set(values);
    for (const control of namedControls) {
      if (control.type === "checkbox" || control.type === "radio") {
        control.checked = valueSet.has(String(control.value ?? ""));
      } else if (control.tagName === "SELECT" && control.multiple) {
        for (const option of control.options) {
          option.selected = valueSet.has(String(option.value ?? ""));
        }
      } else if (values.length > 0) {
        control.value = values[0] ?? "";
      }
    }
  }
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

function saveState(storeId, entries) {
  const normalizedEntries = omitDefaultedDifferenceEntries(entries);
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
    return omitDefaultedDifferenceEntries(parsedValue?.entries);
  } catch {
    return [];
  }
}

export function HuntBacktestFormStateSync({ storeId, formId, formStateKey = "" }) {
  const hasSeenManagedParamsRef = useRef(false);
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();

  useEffect(() => {
    if (!storeId) {
      return;
    }

    if (hasManagedSearchParams(searchParams)) {
      hasSeenManagedParamsRef.current = true;
      const searchEntries = readStateFromSearchParams(searchParams);
      saveState(storeId, searchEntries);
      return;
    }

    if (hasSeenManagedParamsRef.current) {
      return;
    }

    const savedEntries = readSavedState(storeId);
    if (savedEntries.length === 0) {
      return;
    }

    const savedSearchText = buildSearchText(savedEntries);
    if (savedSearchText) {
      router.replace(`${pathname}?${savedSearchText}`, { scroll: false });
      return;
    }

    const form = formId ? document.getElementById(formId) : null;
    applyStateToForm(form, savedEntries);
  }, [formId, formStateKey, pathname, router, searchParams, storeId]);

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
