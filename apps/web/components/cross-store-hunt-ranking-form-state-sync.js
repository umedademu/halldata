"use client";

import { useEffect } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

import {
  MY_HALL_CHANGE_EVENT,
  readSavedMyHallStoreIds,
} from "./store-favorite-button";

const SELECTED_STORE_STORAGE_KEY = "cross-store-hunt-ranking-selected-store-ids";
const CONFIGURED_STORE_STORAGE_KEY =
  "cross-store-hunt-ranking-configured-selected-store-ids";
const SELECTED_MACHINE_STORAGE_KEY = "cross-store-hunt-ranking-selected-machine-names";
const CONFIGURED_MACHINE_STORAGE_KEY =
  "cross-store-hunt-ranking-configured-selected-machine-names";
const STORE_SOURCE_CONFIGURED = "configured";

function normalizeStoreSource(value) {
  return String(value ?? "").trim() === STORE_SOURCE_CONFIGURED
    ? STORE_SOURCE_CONFIGURED
    : "favorites";
}

function getSelectedStoreStorageKey(storeSource) {
  return normalizeStoreSource(storeSource) === STORE_SOURCE_CONFIGURED
    ? CONFIGURED_STORE_STORAGE_KEY
    : SELECTED_STORE_STORAGE_KEY;
}

function getSelectedMachineStorageKey(storeSource) {
  return normalizeStoreSource(storeSource) === STORE_SOURCE_CONFIGURED
    ? CONFIGURED_MACHINE_STORAGE_KEY
    : SELECTED_MACHINE_STORAGE_KEY;
}

function normalizeStoreId(value) {
  return String(value ?? "").trim();
}

function normalizeMachineName(value) {
  return String(value ?? "").trim();
}

function normalizeStoreIds(values) {
  return [
    ...new Set(
      (Array.isArray(values) ? values : [values])
        .map(normalizeStoreId)
        .filter(Boolean),
    ),
  ];
}

function normalizeMachineNames(values) {
  return [
    ...new Set(
      (Array.isArray(values) ? values : [values])
        .map(normalizeMachineName)
        .filter(Boolean),
    ),
  ];
}

function normalizePrefectureName(value) {
  return String(value ?? "")
    .normalize("NFKC")
    .replace(/[都道府県]$/u, "")
    .trim();
}

function buildSearchWithStores(searchParams, favoriteStoreIds, selectedStoreIds) {
  const nextSearchParams = new URLSearchParams(searchParams.toString());
  nextSearchParams.delete("store");
  nextSearchParams.delete("favoriteStore");

  for (const storeId of normalizeStoreIds(favoriteStoreIds)) {
    nextSearchParams.append("favoriteStore", storeId);
  }
  for (const storeId of normalizeStoreIds(selectedStoreIds)) {
    nextSearchParams.append("store", storeId);
  }

  return nextSearchParams.toString();
}

function buildSearchWithMachines(searchParams, selectedMachineNames) {
  const nextSearchParams = new URLSearchParams(searchParams.toString());
  nextSearchParams.delete("machine");
  nextSearchParams.set("machineTouched", "1");

  for (const machineName of normalizeMachineNames(selectedMachineNames)) {
    nextSearchParams.append("machine", machineName);
  }

  return nextSearchParams.toString();
}

function syncStoreClassNames(form) {
  if (!form) {
    return;
  }

  for (const input of form.querySelectorAll('input[data-cross-store-option="1"]')) {
    const chip = input.closest(".metricToggleChip");
    chip?.classList.toggle("metricToggleChipActive", input.checked);
  }
}

function syncMachineClassNames(form) {
  if (!form) {
    return;
  }

  for (const input of form.querySelectorAll('input[data-machine-filter-option="1"]')) {
    const chip = input.closest(".metricToggleChip");
    chip?.classList.toggle("metricToggleChipActive", input.checked);
  }
}

function readSelectedStoreIdsFromForm(form) {
  if (!form) {
    return [];
  }

  return normalizeStoreIds(
    [...form.querySelectorAll('input[data-cross-store-option="1"]')]
      .filter((input) => input.checked)
      .map((input) => input.value),
  );
}

function readSelectedMachineNamesFromForm(form) {
  if (!form) {
    return [];
  }

  return normalizeMachineNames(
    [...form.querySelectorAll('input[data-machine-filter-option="1"]')]
      .filter((input) => input.checked)
      .map((input) => input.value),
  );
}

function readStoreSourceFromForm(form) {
  if (!form) {
    return "favorites";
  }
  const input = form.querySelector('input[name="storeSource"]');
  return normalizeStoreSource(input?.value);
}

function saveSelectedStoreIds(storeIds, storeSource = "favorites") {
  try {
    window.localStorage.setItem(
      getSelectedStoreStorageKey(storeSource),
      JSON.stringify({
        version: 1,
        storeIds: normalizeStoreIds(storeIds),
      }),
    );
  } catch {
    // 保存できない環境では、その場の選択だけを有効にします。
  }
}

function saveSelectedMachineNames(machineNames, storeSource = "favorites") {
  try {
    window.localStorage.setItem(
      getSelectedMachineStorageKey(storeSource),
      JSON.stringify({
        version: 1,
        machineNames: normalizeMachineNames(machineNames),
      }),
    );
  } catch {
    // 保存できない環境では、その場の選択だけを有効にします。
  }
}

function readSavedSelectedStoreIds(storeSource = "favorites") {
  try {
    const rawValue = window.localStorage.getItem(getSelectedStoreStorageKey(storeSource));
    if (!rawValue) {
      return null;
    }

    const parsedValue = JSON.parse(rawValue);
    return normalizeStoreIds(parsedValue?.storeIds);
  } catch {
    return null;
  }
}

function readSavedSelectedMachineNames(storeSource = "favorites") {
  try {
    const rawValue = window.localStorage.getItem(getSelectedMachineStorageKey(storeSource));
    if (!rawValue) {
      return null;
    }

    const parsedValue = JSON.parse(rawValue);
    return normalizeMachineNames(parsedValue?.machineNames);
  } catch {
    return null;
  }
}

function updateStoreInputs(form, { checked, prefecture = "" }) {
  if (!form) {
    return;
  }

  const targetPrefecture = normalizePrefectureName(prefecture);
  for (const input of form.querySelectorAll('input[data-cross-store-option="1"]')) {
    const inputPrefecture = normalizePrefectureName(input.dataset.storePrefecture);
    if (targetPrefecture && inputPrefecture !== targetPrefecture) {
      continue;
    }
    input.checked = checked;
  }

  syncStoreClassNames(form);
  saveSelectedStoreIds(readSelectedStoreIdsFromForm(form), readStoreSourceFromForm(form));
}

export function CrossStoreHuntRankingFormStateSync({ formId, resultActive = false }) {
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();

  useEffect(() => {
    const storeSource = normalizeStoreSource(searchParams.get("storeSource"));
    if (searchParams.has("store")) {
      saveSelectedStoreIds(searchParams.getAll("store"), storeSource);
      return undefined;
    }

    if (resultActive) {
      return undefined;
    }

    if (storeSource === STORE_SOURCE_CONFIGURED) {
      return undefined;
    }

    const syncSearchFromMyHall = () => {
      const myHallStoreIds = readSavedMyHallStoreIds();
      if (myHallStoreIds.length === 0) {
        return;
      }

      const savedStoreIds = readSavedSelectedStoreIds(storeSource);
      const selectedStoreIds =
        savedStoreIds === null
          ? myHallStoreIds
          : myHallStoreIds.filter((storeId) => savedStoreIds.includes(storeId));
      const searchText = buildSearchWithStores(searchParams, myHallStoreIds, selectedStoreIds);
      if (searchText !== searchParams.toString()) {
        router.replace(searchText ? `${pathname}?${searchText}` : pathname, { scroll: false });
      }
    };

    syncSearchFromMyHall();
    window.addEventListener(MY_HALL_CHANGE_EVENT, syncSearchFromMyHall);

    return () => {
      window.removeEventListener(MY_HALL_CHANGE_EVENT, syncSearchFromMyHall);
    };
  }, [pathname, resultActive, router, searchParams]);

  useEffect(() => {
    const storeSource = normalizeStoreSource(searchParams.get("storeSource"));
    const hasMachineSelectionParams =
      searchParams.has("machine") || searchParams.get("machineTouched") === "1";

    if (hasMachineSelectionParams) {
      saveSelectedMachineNames(searchParams.getAll("machine"), storeSource);
      return;
    }

    if (resultActive) {
      return;
    }

    if (
      storeSource !== STORE_SOURCE_CONFIGURED &&
      !searchParams.has("store") &&
      readSavedMyHallStoreIds().length > 0
    ) {
      return;
    }

    const savedMachineNames = readSavedSelectedMachineNames(storeSource);
    if (savedMachineNames === null) {
      return;
    }

    const searchText = buildSearchWithMachines(searchParams, savedMachineNames);
    if (searchText !== searchParams.toString()) {
      router.replace(searchText ? `${pathname}?${searchText}` : pathname, { scroll: false });
    }
  }, [pathname, resultActive, router, searchParams]);

  useEffect(() => {
    if (!formId) {
      return undefined;
    }

    const form = document.getElementById(formId);
    if (!form) {
      return undefined;
    }

    const sync = () => {
      syncStoreClassNames(form);
      syncMachineClassNames(form);
      saveSelectedStoreIds(readSelectedStoreIdsFromForm(form), readStoreSourceFromForm(form));
      saveSelectedMachineNames(
        readSelectedMachineNamesFromForm(form),
        readStoreSourceFromForm(form),
      );
    };
    const syncOnly = () => {
      syncStoreClassNames(form);
      syncMachineClassNames(form);
    };
    const handleClick = (event) => {
      const target = event.target instanceof Element ? event.target : null;
      const button = target?.closest("[data-cross-store-select-action]");
      if (!button || !form.contains(button)) {
        return;
      }

      const action = String(button.dataset.crossStoreSelectAction ?? "").trim();
      if (action !== "check" && action !== "clear") {
        return;
      }

      updateStoreInputs(form, {
        checked: action === "check",
        prefecture: button.dataset.crossStoreSelectPrefecture ?? "",
      });
    };

    syncOnly();
    form.addEventListener("change", sync);
    form.addEventListener("submit", sync);
    form.addEventListener("click", handleClick);
    window.addEventListener(MY_HALL_CHANGE_EVENT, syncOnly);
    window.addEventListener("storage", syncOnly);

    return () => {
      form.removeEventListener("change", sync);
      form.removeEventListener("submit", sync);
      form.removeEventListener("click", handleClick);
      window.removeEventListener(MY_HALL_CHANGE_EVENT, syncOnly);
      window.removeEventListener("storage", syncOnly);
    };
  }, [formId]);

  return null;
}
