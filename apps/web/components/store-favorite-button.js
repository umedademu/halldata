"use client";

import { useEffect, useState } from "react";

export const MY_HALL_STORAGE_KEY = "halldata-my-hall-store-ids";
export const MY_HALL_CHANGE_EVENT = "halldata-my-hall-change";

export function normalizeStoreId(value) {
  return String(value ?? "").trim();
}

export function readSavedMyHallStoreIds() {
  if (typeof window === "undefined") {
    return [];
  }

  try {
    const parsedValue = JSON.parse(window.localStorage.getItem(MY_HALL_STORAGE_KEY) || "[]");
    if (!Array.isArray(parsedValue)) {
      return [];
    }
    return [...new Set(parsedValue.map(normalizeStoreId).filter(Boolean))];
  } catch {
    window.localStorage.removeItem(MY_HALL_STORAGE_KEY);
    return [];
  }
}

export function saveMyHallStoreIds(storeIds) {
  if (typeof window === "undefined") {
    return;
  }

  const normalizedStoreIds = [...new Set(storeIds.map(normalizeStoreId).filter(Boolean))];
  window.localStorage.setItem(MY_HALL_STORAGE_KEY, JSON.stringify(normalizedStoreIds));
  window.dispatchEvent(new CustomEvent(MY_HALL_CHANGE_EVENT));
}

export function StoreFavoriteButton({
  store,
  isFavorite: controlledIsFavorite,
  onToggle,
  className = "",
  compact = false,
}) {
  const storeId = normalizeStoreId(store?.id);
  const storeName = String(store?.storeName ?? "").trim() || "この店舗";
  const isControlled = typeof controlledIsFavorite === "boolean" && typeof onToggle === "function";
  const [myHallStoreIds, setMyHallStoreIds] = useState([]);

  useEffect(() => {
    if (isControlled) {
      return undefined;
    }

    const syncMyHallStoreIds = () => {
      setMyHallStoreIds(readSavedMyHallStoreIds());
    };

    syncMyHallStoreIds();
    window.addEventListener(MY_HALL_CHANGE_EVENT, syncMyHallStoreIds);
    window.addEventListener("storage", syncMyHallStoreIds);

    return () => {
      window.removeEventListener(MY_HALL_CHANGE_EVENT, syncMyHallStoreIds);
      window.removeEventListener("storage", syncMyHallStoreIds);
    };
  }, [isControlled]);

  const isFavorite = isControlled
    ? controlledIsFavorite
    : Boolean(storeId && myHallStoreIds.includes(storeId));
  const label = `${storeName}を${isFavorite ? "マイホールから外す" : "マイホールに追加"}`;
  const buttonClassName = [
    "storeFavoriteButton",
    compact ? "storeFavoriteButtonCompact" : "",
    isFavorite ? "storeFavoriteButtonActive" : "",
    className,
  ]
    .filter(Boolean)
    .join(" ");

  const handleToggle = () => {
    if (!storeId) {
      return;
    }

    if (isControlled) {
      onToggle(store);
      return;
    }

    setMyHallStoreIds((currentStoreIds) => {
      const nextStoreIds = currentStoreIds.includes(storeId)
        ? currentStoreIds.filter((currentStoreId) => currentStoreId !== storeId)
        : [storeId, ...currentStoreIds];
      saveMyHallStoreIds(nextStoreIds);
      return nextStoreIds;
    });
  };

  return (
    <button
      type="button"
      className={buttonClassName}
      onClick={handleToggle}
      aria-label={label}
      aria-pressed={isFavorite}
      title={isFavorite ? "マイホールから外す" : "マイホールに追加"}
      disabled={!storeId}
    >
      <span aria-hidden="true">{isFavorite ? "★" : "☆"}</span>
    </button>
  );
}
