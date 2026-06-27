"use client";

import { useEffect, useState } from "react";

import {
  MY_HALL_PROFILES,
  normalizeMyHallProfileId,
  normalizeMyHallStoreIds,
} from "../lib/my-hall";

export const MY_HALL_STORAGE_KEY = "halldata-my-hall-store-ids";
export const MY_HALL_UPDATED_AT_STORAGE_KEY = "halldata-my-hall-updated-at";
export const MY_HALL_PROFILE_STORAGE_KEY = "halldata-my-hall-profile-id";
export const MY_HALL_CHANGE_EVENT = "halldata-my-hall-change";
export const MY_HALL_PROFILE_CHANGE_EVENT = "halldata-my-hall-profile-change";

export function normalizeStoreId(value) {
  return String(value ?? "").trim();
}

function profileStorageKey(profileId, baseKey) {
  const normalizedProfileId = normalizeMyHallProfileId(profileId);
  return normalizedProfileId ? `${baseKey}:${normalizedProfileId}` : baseKey;
}

function hasStoredValue(key) {
  if (typeof window === "undefined") {
    return false;
  }

  try {
    return window.localStorage.getItem(key) !== null;
  } catch {
    return false;
  }
}

function readStoreIdsFromKey(key) {
  if (typeof window === "undefined") {
    return [];
  }

  try {
    const parsedValue = JSON.parse(window.localStorage.getItem(key) || "[]");
    return normalizeMyHallStoreIds(parsedValue);
  } catch {
    window.localStorage.removeItem(key);
    return [];
  }
}

function readStoredText(key) {
  if (typeof window === "undefined") {
    return "";
  }

  try {
    return String(window.localStorage.getItem(key) ?? "");
  } catch {
    return "";
  }
}

function saveStoredText(key, value) {
  if (typeof window === "undefined") {
    return;
  }

  try {
    if (value) {
      window.localStorage.setItem(key, value);
    } else {
      window.localStorage.removeItem(key);
    }
  } catch {
    // 端末保存が使えない場合は、表示中の状態だけで扱います。
  }
}

function readActiveMyHallStorageContext() {
  const profileId = readSavedMyHallProfileId();
  const profileStoreIdsKey = profileStorageKey(profileId, MY_HALL_STORAGE_KEY);
  const profileUpdatedAtKey = profileStorageKey(profileId, MY_HALL_UPDATED_AT_STORAGE_KEY);
  const shouldUseLegacyValue =
    Boolean(profileId) &&
    !hasStoredValue(profileStoreIdsKey) &&
    !hasStoredValue(profileUpdatedAtKey) &&
    hasStoredValue(MY_HALL_STORAGE_KEY);

  return {
    profileId,
    storeIdsKey: shouldUseLegacyValue ? MY_HALL_STORAGE_KEY : profileStoreIdsKey,
    updatedAtKey: shouldUseLegacyValue ? MY_HALL_UPDATED_AT_STORAGE_KEY : profileUpdatedAtKey,
    usesLegacyValue: shouldUseLegacyValue,
  };
}

function readSavedMyHallUpdatedAt() {
  const context = readActiveMyHallStorageContext();
  return readStoredText(context.updatedAtKey);
}

function writeLocalMyHallStoreIds(storeIds, updatedAt) {
  if (typeof window === "undefined") {
    return [];
  }

  const profileId = readSavedMyHallProfileId();
  const storeIdsKey = profileStorageKey(profileId, MY_HALL_STORAGE_KEY);
  const updatedAtKey = profileStorageKey(profileId, MY_HALL_UPDATED_AT_STORAGE_KEY);
  const normalizedStoreIds = normalizeMyHallStoreIds(storeIds);
  const normalizedUpdatedAt = String(updatedAt ?? "").trim() || new Date().toISOString();

  try {
    window.localStorage.setItem(storeIdsKey, JSON.stringify(normalizedStoreIds));
    window.localStorage.setItem(updatedAtKey, normalizedUpdatedAt);
    if (profileId) {
      window.localStorage.removeItem(MY_HALL_STORAGE_KEY);
      window.localStorage.removeItem(MY_HALL_UPDATED_AT_STORAGE_KEY);
    }
    window.dispatchEvent(new CustomEvent(MY_HALL_CHANGE_EVENT));
  } catch {
    // 端末保存が使えない場合は、画面上の更新だけを続けます。
  }

  return normalizedStoreIds;
}

export function readSavedMyHallStoreIds() {
  if (typeof window === "undefined") {
    return [];
  }

  return readStoreIdsFromKey(readActiveMyHallStorageContext().storeIdsKey);
}

function readCloudMyHallApiPath(profileId) {
  const normalizedProfileId = normalizeMyHallProfileId(profileId);
  return normalizedProfileId ? `/api/my-hall/${encodeURIComponent(normalizedProfileId)}` : "";
}

async function readCloudMyHallStoreIds(profileId) {
  const path = readCloudMyHallApiPath(profileId);
  if (!path) {
    return { storeIds: [], updatedAt: "" };
  }

  const response = await fetch(path, { cache: "no-store" });
  if (response.status === 404) {
    return { storeIds: [], updatedAt: "" };
  }
  if (!response.ok) {
    throw new Error("マイホールを読めませんでした。");
  }

  const payload = await response.json();
  return {
    storeIds: normalizeMyHallStoreIds(payload?.storeIds),
    updatedAt: String(payload?.updatedAt ?? "").trim(),
  };
}

async function writeCloudMyHallStoreIds(profileId, storeIds, updatedAt) {
  const path = readCloudMyHallApiPath(profileId);
  if (!path) {
    return null;
  }

  const response = await fetch(path, {
    method: "PATCH",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify({
      storeIds: normalizeMyHallStoreIds(storeIds),
      updatedAt,
    }),
  });
  if (!response.ok) {
    throw new Error("マイホールを保存できませんでした。");
  }
  return response.json();
}

let pendingCloudSave = null;
let pendingCloudSaveTimer = null;
let cloudSyncPromise = null;

function queueMyHallCloudSave(profileId, storeIds, updatedAt) {
  const normalizedProfileId = normalizeMyHallProfileId(profileId);
  if (!normalizedProfileId) {
    return;
  }

  pendingCloudSave = {
    profileId: normalizedProfileId,
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
    writeCloudMyHallStoreIds(payload.profileId, payload.storeIds, payload.updatedAt).catch((error) => {
      console.warn(error);
    });
  }, 400);
}

function parseDateTime(value) {
  const timestamp = Date.parse(String(value ?? ""));
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function storeIdsAreSame(left, right) {
  const leftStoreIds = normalizeMyHallStoreIds(left);
  const rightStoreIds = normalizeMyHallStoreIds(right);
  return (
    leftStoreIds.length === rightStoreIds.length &&
    leftStoreIds.every((storeId, index) => storeId === rightStoreIds[index])
  );
}

export async function syncMyHallStoreIdsWithCloud() {
  if (cloudSyncPromise) {
    return cloudSyncPromise;
  }

  cloudSyncPromise = (async () => {
    const profileId = readSavedMyHallProfileId();
    if (!profileId) {
      return { profileId: "", storeIds: readSavedMyHallStoreIds(), updatedAt: "" };
    }

    const localStoreIds = readSavedMyHallStoreIds();
    const localUpdatedAt = readSavedMyHallUpdatedAt();
    const cloudPayload = await readCloudMyHallStoreIds(profileId);
    const cloudStoreIds = cloudPayload.storeIds;
    const cloudUpdatedAt = cloudPayload.updatedAt;
    const localTime = parseDateTime(localUpdatedAt);
    const cloudTime = parseDateTime(cloudUpdatedAt);
    const hasOldLocalFavorites = localStoreIds.length > 0 && localTime === 0;

    if (hasOldLocalFavorites) {
      const mergedStoreIds = normalizeMyHallStoreIds([...localStoreIds, ...cloudStoreIds]);
      const updatedAt = new Date().toISOString();
      writeLocalMyHallStoreIds(mergedStoreIds, updatedAt);
      await writeCloudMyHallStoreIds(profileId, mergedStoreIds, updatedAt);
      return { profileId, storeIds: mergedStoreIds, updatedAt };
    }

    if (cloudTime > localTime) {
      writeLocalMyHallStoreIds(cloudStoreIds, cloudUpdatedAt || new Date().toISOString());
      return { profileId, storeIds: cloudStoreIds, updatedAt: cloudUpdatedAt };
    }

    if (localStoreIds.length > 0 && (localTime > cloudTime || cloudTime === 0)) {
      const updatedAt = localUpdatedAt || new Date().toISOString();
      await writeCloudMyHallStoreIds(profileId, localStoreIds, updatedAt);
      writeLocalMyHallStoreIds(localStoreIds, updatedAt);
      return { profileId, storeIds: localStoreIds, updatedAt };
    }

    if (localTime === cloudTime && localTime > 0 && !storeIdsAreSame(localStoreIds, cloudStoreIds)) {
      const mergedStoreIds = normalizeMyHallStoreIds([...localStoreIds, ...cloudStoreIds]);
      const updatedAt = new Date().toISOString();
      writeLocalMyHallStoreIds(mergedStoreIds, updatedAt);
      await writeCloudMyHallStoreIds(profileId, mergedStoreIds, updatedAt);
      return { profileId, storeIds: mergedStoreIds, updatedAt };
    }

    return { profileId, storeIds: localStoreIds, updatedAt: localUpdatedAt };
  })();

  try {
    return await cloudSyncPromise;
  } finally {
    cloudSyncPromise = null;
  }
}

export function saveMyHallStoreIds(storeIds, options = {}) {
  if (typeof window === "undefined") {
    return;
  }

  const context = readActiveMyHallStorageContext();
  const updatedAt = String(options.updatedAt ?? "").trim() || new Date().toISOString();
  const normalizedStoreIds = writeLocalMyHallStoreIds(storeIds, updatedAt);
  if (options.syncCloud !== false) {
    queueMyHallCloudSave(context.profileId, normalizedStoreIds, updatedAt);
  }
}

export function readSavedMyHallProfileId() {
  return normalizeMyHallProfileId(readStoredText(MY_HALL_PROFILE_STORAGE_KEY));
}

export function saveMyHallProfileId(profileId) {
  const normalizedProfileId = normalizeMyHallProfileId(profileId);
  saveStoredText(MY_HALL_PROFILE_STORAGE_KEY, normalizedProfileId);
  if (typeof window !== "undefined") {
    window.dispatchEvent(new CustomEvent(MY_HALL_PROFILE_CHANGE_EVENT));
    window.dispatchEvent(new CustomEvent(MY_HALL_CHANGE_EVENT));
  }
}

export function listMyHallProfiles() {
  return MY_HALL_PROFILES;
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
