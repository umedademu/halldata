"use client";

import { useEffect, useState } from "react";

import {
  normalizeMyHallClientId,
  normalizeMyHallStoreIds,
} from "../lib/my-hall";

export const MY_HALL_STORAGE_KEY = "halldata-my-hall-store-ids";
export const MY_HALL_UPDATED_AT_STORAGE_KEY = "halldata-my-hall-updated-at";
export const MY_HALL_CLOUD_CLIENT_ID_STORAGE_KEY = "halldata-my-hall-cloud-client-id";
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
    return normalizeMyHallStoreIds(parsedValue);
  } catch {
    window.localStorage.removeItem(MY_HALL_STORAGE_KEY);
    return [];
  }
}

async function writeCloudMyHallStoreIds(storeIds, updatedAt) {
  const response = await fetch("/api/my-hall", {
    method: "PATCH",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify({
      storeIds: normalizeMyHallStoreIds(storeIds),
      updatedAt,
      clientId: readOrCreateMyHallCloudClientId(),
    }),
  });
  if (!response.ok) {
    throw new Error("マイホールを保存できませんでした。");
  }
  return response.json();
}

let pendingCloudSave = null;
let pendingCloudSaveTimer = null;

function queueMyHallCloudSave(storeIds, updatedAt) {
  pendingCloudSave = {
    storeIds: normalizeMyHallStoreIds(storeIds),
    updatedAt,
  };

  if (pendingCloudSaveTimer) {
    window.clearTimeout(pendingCloudSaveTimer);
  }

  pendingCloudSaveTimer = window.setTimeout(() => {
    const payload = pendingCloudSave;
    pendingCloudSave = null;
    pendingCloudSaveTimer = null;
    writeCloudMyHallStoreIds(payload.storeIds, payload.updatedAt).catch((error) => {
      console.warn(error);
    });
  }, 400);
}

function createMyHallCloudClientId() {
  const rawId =
    typeof window.crypto?.randomUUID === "function"
      ? window.crypto.randomUUID()
      : `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  return normalizeMyHallClientId(`client-${rawId}`);
}

function readOrCreateMyHallCloudClientId() {
  if (typeof window === "undefined") {
    return "";
  }

  try {
    const savedClientId = normalizeMyHallClientId(
      window.localStorage.getItem(MY_HALL_CLOUD_CLIENT_ID_STORAGE_KEY),
    );
    if (savedClientId) {
      return savedClientId;
    }

    const nextClientId = createMyHallCloudClientId();
    window.localStorage.setItem(MY_HALL_CLOUD_CLIENT_ID_STORAGE_KEY, nextClientId);
    return nextClientId;
  } catch {
    return createMyHallCloudClientId();
  }
}

export function saveMyHallStoreIds(storeIds, options = {}) {
  if (typeof window === "undefined") {
    return;
  }

  const updatedAt = String(options.updatedAt ?? "").trim() || new Date().toISOString();
  const normalizedStoreIds = normalizeMyHallStoreIds(storeIds);
  try {
    window.localStorage.setItem(MY_HALL_STORAGE_KEY, JSON.stringify(normalizedStoreIds));
    window.localStorage.setItem(MY_HALL_UPDATED_AT_STORAGE_KEY, updatedAt);
    window.dispatchEvent(new CustomEvent(MY_HALL_CHANGE_EVENT));
  } catch {
    // 端末保存が使えない場合は、画面上の更新だけを続けます。
  }
  if (options.syncCloud !== false) {
    queueMyHallCloudSave(normalizedStoreIds, updatedAt);
  }
}

export function syncSavedMyHallStoreIdsToCloud() {
  if (typeof window === "undefined") {
    return;
  }

  if (
    window.localStorage.getItem(MY_HALL_STORAGE_KEY) === null &&
    window.localStorage.getItem(MY_HALL_UPDATED_AT_STORAGE_KEY) === null
  ) {
    return;
  }

  queueMyHallCloudSave(
    readSavedMyHallStoreIds(),
    window.localStorage.getItem(MY_HALL_UPDATED_AT_STORAGE_KEY) || new Date().toISOString(),
  );
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
