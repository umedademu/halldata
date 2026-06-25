"use client";

import { useEffect } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

import {
  MY_HALL_CHANGE_EVENT,
  readSavedMyHallStoreIds,
} from "./store-favorite-button";

function buildSearchWithStores(searchParams, storeIds) {
  const nextSearchParams = new URLSearchParams(searchParams.toString());
  nextSearchParams.delete("store");
  nextSearchParams.delete("favoriteStore");

  for (const storeId of storeIds) {
    nextSearchParams.append("favoriteStore", storeId);
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

export function CrossStoreHuntRankingFormStateSync({ formId }) {
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();

  useEffect(() => {
    if (searchParams.has("store") || searchParams.has("show")) {
      return;
    }

    const myHallStoreIds = readSavedMyHallStoreIds();
    if (myHallStoreIds.length === 0) {
      return;
    }

    const searchText = buildSearchWithStores(searchParams, myHallStoreIds);
    router.replace(searchText ? `${pathname}?${searchText}` : pathname, { scroll: false });
  }, [pathname, router, searchParams]);

  useEffect(() => {
    if (!formId) {
      return undefined;
    }

    const form = document.getElementById(formId);
    if (!form) {
      return undefined;
    }

    const sync = () => syncStoreClassNames(form);
    sync();
    form.addEventListener("change", sync);
    window.addEventListener(MY_HALL_CHANGE_EVENT, sync);
    window.addEventListener("storage", sync);

    return () => {
      form.removeEventListener("change", sync);
      window.removeEventListener(MY_HALL_CHANGE_EVENT, sync);
      window.removeEventListener("storage", sync);
    };
  }, [formId]);

  return null;
}
