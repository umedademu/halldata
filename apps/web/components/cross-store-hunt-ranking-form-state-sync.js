"use client";

import { useEffect } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

import {
  MY_HALL_CHANGE_EVENT,
  readSavedMyHallStoreIds,
} from "./store-favorite-button";

const SELECTED_STORE_STORAGE_KEY = "cross-store-hunt-ranking-selected-store-ids";

function normalizeStoreId(value) {
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

function syncStoreClassNames(form) {
  if (!form) {
    return;
  }

  for (const input of form.querySelectorAll('input[data-cross-store-option="1"]')) {
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

function saveSelectedStoreIds(storeIds) {
  try {
    window.localStorage.setItem(
      SELECTED_STORE_STORAGE_KEY,
      JSON.stringify({
        version: 1,
        storeIds: normalizeStoreIds(storeIds),
      }),
    );
  } catch {
    // 保存できない環境では、その場の選択だけを有効にします。
  }
}

function readSavedSelectedStoreIds() {
  try {
    const rawValue = window.localStorage.getItem(SELECTED_STORE_STORAGE_KEY);
    if (!rawValue) {
      return null;
    }

    const parsedValue = JSON.parse(rawValue);
    return normalizeStoreIds(parsedValue?.storeIds);
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
  saveSelectedStoreIds(readSelectedStoreIdsFromForm(form));
}

export function CrossStoreHuntRankingFormStateSync({ formId }) {
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();

  useEffect(() => {
    if (searchParams.has("store") || searchParams.has("show")) {
      saveSelectedStoreIds(searchParams.getAll("store"));
      return undefined;
    }

    const syncSearchFromMyHall = () => {
      const myHallStoreIds = readSavedMyHallStoreIds();
      if (myHallStoreIds.length === 0) {
        return;
      }

      const savedStoreIds = readSavedSelectedStoreIds();
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
  }, [pathname, router, searchParams]);

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
      saveSelectedStoreIds(readSelectedStoreIdsFromForm(form));
    };
    const syncOnly = () => syncStoreClassNames(form);
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
