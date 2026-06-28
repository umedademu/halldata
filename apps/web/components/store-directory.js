"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";

import {
  MY_HALL_CHANGE_EVENT,
  StoreFavoriteButton,
  normalizeStoreId,
  readSavedMyHallStoreIds,
  saveMyHallStoreIds,
} from "./store-favorite-button";
import {
  buildStoreLocationGroups,
  readStoreLocationGroupKey,
} from "../lib/store-location-groups";
import {
  calculateRegionDistanceKm,
  listKnownRegionOptions,
  readRegionPoint,
} from "../lib/store-region-distance";

const REGION_ORDER_MODE_STORAGE_KEY = "halldata-store-region-order-mode";
const HOME_REGION_STORAGE_KEY = "halldata-home-region-key";
const HOME_ADDRESS_REGIONS_STORAGE_KEY = "halldata-home-address-region-keys";
const MY_HALL_ORDER_MODE_STORAGE_KEY = "halldata-my-hall-order-mode";
const REGION_ORDER_NORMAL = "normal";
const REGION_ORDER_NEAR_HOME = "near-home";
const MY_HALL_ORDER_SAVED = "saved";
const MY_HALL_ORDER_NEAR_HOME = "near-home";
const REGION_KEY_SEPARATOR = "||";

function normalizeText(value) {
  return String(value ?? "").trim().toLocaleLowerCase("ja");
}

function normalizeGroupName(value, fallback) {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function buildRegionKey(prefectureName, areaName) {
  return [
    normalizeGroupName(prefectureName, "都道府県未設定"),
    normalizeGroupName(areaName, "地域未設定"),
  ].join(REGION_KEY_SEPARATOR);
}

function parseRegionKey(regionKey) {
  const [prefectureName = "", areaName = ""] = String(regionKey ?? "").split(REGION_KEY_SEPARATOR);
  return { prefectureName, areaName };
}

function formatRegionLabel(prefectureName, areaName) {
  return [prefectureName, areaName].filter(Boolean).join(" / ") || "地域未設定";
}

function readStoredText(key, fallbackValue = "") {
  if (typeof window === "undefined") {
    return fallbackValue;
  }

  try {
    return String(window.localStorage.getItem(key) ?? fallbackValue);
  } catch {
    return fallbackValue;
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
    // 端末保存が使えない環境では、画面上の選択だけを使います。
  }
}

function normalizeRegionKey(value) {
  const regionKey = String(value ?? "").trim();
  if (!regionKey.includes(REGION_KEY_SEPARATOR)) {
    return "";
  }
  return regionKey;
}

function normalizeRegionKeys(values) {
  if (!Array.isArray(values)) {
    return [];
  }
  return [...new Set(values.map(normalizeRegionKey).filter(Boolean))];
}

function readStoredRegionKeys(key) {
  if (typeof window === "undefined") {
    return [];
  }

  try {
    return normalizeRegionKeys(JSON.parse(window.localStorage.getItem(key) || "[]"));
  } catch {
    window.localStorage.removeItem(key);
    return [];
  }
}

function saveStoredRegionKeys(key, regionKeys) {
  if (typeof window === "undefined") {
    return;
  }

  try {
    window.localStorage.setItem(key, JSON.stringify(normalizeRegionKeys(regionKeys)));
  } catch {
    // 端末保存が使えない環境では、画面上の選択だけを使います。
  }
}

function reorderStoreIds(storeIds, sourceStoreId, targetStoreId) {
  const sourceId = normalizeStoreId(sourceStoreId);
  const targetId = normalizeStoreId(targetStoreId);
  if (!sourceId || !targetId || sourceId === targetId) {
    return storeIds;
  }

  const sourceIndex = storeIds.indexOf(sourceId);
  const targetIndex = storeIds.indexOf(targetId);
  if (sourceIndex < 0 || targetIndex < 0) {
    return storeIds;
  }

  const nextStoreIds = [...storeIds];
  const [movedStoreId] = nextStoreIds.splice(sourceIndex, 1);
  nextStoreIds.splice(targetIndex, 0, movedStoreId);
  return nextStoreIds;
}

function compareRegionNames(left, right) {
  const prefectureComparison = left.prefectureName.localeCompare(right.prefectureName, "ja");
  if (prefectureComparison !== 0) {
    return prefectureComparison;
  }
  return left.areaName.localeCompare(right.areaName, "ja");
}

function buildStoreRegions(stores) {
  const regionMap = new Map();

  for (const store of stores) {
    const prefectureName = normalizeGroupName(store.prefectureName, "都道府県未設定");
    const areaName = normalizeGroupName(store.areaName, "地域未設定");
    const regionKey = buildRegionKey(prefectureName, areaName);
    if (!regionMap.has(regionKey)) {
      regionMap.set(regionKey, {
        key: regionKey,
        prefectureName,
        areaName,
        label: formatRegionLabel(prefectureName, areaName),
        point: readRegionPoint(prefectureName, areaName),
        stores: [],
      });
    }
    regionMap.get(regionKey).stores.push(store);
  }

  return [...regionMap.values()]
    .map((region) => ({
      ...region,
      stores: [...region.stores].sort((left, right) =>
        left.storeName.localeCompare(right.storeName, "ja"),
      ),
    }))
    .sort(compareRegionNames);
}

function buildStoreGroups(stores) {
  const prefectureGroups = new Map();

  for (const region of buildStoreRegions(stores)) {
    if (!prefectureGroups.has(region.prefectureName)) {
      prefectureGroups.set(region.prefectureName, []);
    }
    prefectureGroups.get(region.prefectureName).push(region);
  }

  return [...prefectureGroups.entries()]
    .sort(([left], [right]) => left.localeCompare(right, "ja"))
    .map(([prefectureName, areas]) => ({
      prefectureName,
      areas,
      storeCount: areas.reduce((count, area) => count + area.stores.length, 0),
    }));
}

function buildNearbyStoreRegions(stores, homeRegionKey) {
  const homeRegion = parseRegionKey(homeRegionKey);
  const homePoint = readRegionPoint(homeRegion.prefectureName, homeRegion.areaName);

  return buildStoreRegions(stores)
    .map((region) => ({
      ...region,
      distanceKm: calculateRegionDistanceKm(homePoint, region.point),
    }))
    .sort((left, right) => {
      const leftDistance = typeof left.distanceKm === "number" ? left.distanceKm : Number.POSITIVE_INFINITY;
      const rightDistance =
        typeof right.distanceKm === "number" ? right.distanceKm : Number.POSITIVE_INFINITY;
      if (leftDistance !== rightDistance) {
        return leftDistance - rightDistance;
      }
      return compareRegionNames(left, right);
    });
}

function buildHomeRegionOptions(stores) {
  const optionMap = new Map();

  for (const option of listKnownRegionOptions()) {
    const prefectureName = normalizeGroupName(option.prefectureName, "");
    const areaName = normalizeGroupName(option.areaName, "");
    const key = buildRegionKey(prefectureName, areaName);
    optionMap.set(key, {
      key,
      prefectureName,
      areaName,
      label: formatRegionLabel(prefectureName, areaName),
    });
  }

  for (const region of buildStoreRegions(stores)) {
    optionMap.set(region.key, {
      key: region.key,
      prefectureName: region.prefectureName,
      areaName: region.areaName,
      label: region.label,
    });
  }

  return [...optionMap.values()].sort(compareRegionNames);
}

function buildRegionOptionMap(options) {
  return new Map(options.map((option) => [option.key, option]));
}

function readRegionOptionLabel(regionKey, optionMap) {
  const option = optionMap.get(regionKey);
  if (option) {
    return option.label;
  }
  const region = parseRegionKey(regionKey);
  return formatRegionLabel(region.prefectureName, region.areaName);
}

function formatDistance(distanceKm) {
  if (typeof distanceKm !== "number" || !Number.isFinite(distanceKm)) {
    return "";
  }
  if (distanceKm < 10) {
    return `約${distanceKm.toFixed(1)}km`;
  }
  return `約${Math.round(distanceKm).toLocaleString("ja-JP")}km`;
}

function RegionSummaryTitle({ label, distanceKm = null }) {
  return (
    <span className="storeRegionTitle">
      <span>{label}</span>
      {formatDistance(distanceKm) ? (
        <span className="storeRegionDistance">{formatDistance(distanceKm)}</span>
      ) : null}
    </span>
  );
}

function formatNearbyStoreMeta(region) {
  return [region.label, formatDistance(region.distanceKm)].filter(Boolean).join(" ・ ");
}

function StoreListItem({
  store,
  isFavorite,
  onToggle,
  compact = false,
  reorderable = false,
  metaText = "",
}) {
  const className = [
    "storeLinkItem",
    compact ? "storeLinkItemCompact" : "",
    reorderable ? "storeLinkItemDraggable" : "",
    isFavorite ? "storeLinkItemFavorite" : "",
  ]
    .filter(Boolean)
    .join(" ");
  const normalizedMetaText = String(metaText ?? "").trim();
  const linkClassName = [
    "plainStoreLink",
    "storeRowLink",
    normalizedMetaText ? "storeRowLinkWithMeta" : "",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <div className={className}>
      {reorderable ? (
        <span className="storeDragHandle" aria-hidden="true" title="ドラッグで並び替え">
          ≡
        </span>
      ) : null}
      <StoreFavoriteButton store={store} isFavorite={isFavorite} onToggle={onToggle} />
      <Link href={`/stores/${store.id}`} className={linkClassName} draggable={false}>
        {normalizedMetaText ? (
          <>
            <span className="storeRowName">{store.storeName}</span>
            <span className="storeRowMeta">{normalizedMetaText}</span>
          </>
        ) : (
          store.storeName
        )}
      </Link>
    </div>
  );
}

export function StoreDirectory({ completeStores, pendingStores }) {
  const [query, setQuery] = useState("");
  const [regionOrderMode, setRegionOrderMode] = useState(REGION_ORDER_NORMAL);
  const [homeRegionKey, setHomeRegionKey] = useState("");
  const [homeAddressRegionKeys, setHomeAddressRegionKeys] = useState([]);
  const [homeAddressDraftKey, setHomeAddressDraftKey] = useState("");
  const [myHallOrderMode, setMyHallOrderMode] = useState(MY_HALL_ORDER_SAVED);
  const [myHallStoreIds, setMyHallStoreIds] = useState([]);
  const [draggedMyHallStoreId, setDraggedMyHallStoreId] = useState("");
  const [dragOverMyHallStoreId, setDragOverMyHallStoreId] = useState("");
  const draggedMyHallStoreIdRef = useRef("");
  const normalizedQuery = normalizeText(query);
  const storeById = useMemo(
    () => new Map(completeStores.map((store) => [normalizeStoreId(store.id), store])),
    [completeStores],
  );
  const myHallStoreIdSet = useMemo(() => new Set(myHallStoreIds), [myHallStoreIds]);
  const myHallStores = useMemo(
    () => myHallStoreIds.map((storeId) => storeById.get(storeId)).filter(Boolean),
    [myHallStoreIds, storeById],
  );
  const myHallStoreGroups = useMemo(
    () => buildStoreLocationGroups(myHallStores),
    [myHallStores],
  );
  const myHallNearbyRegions = useMemo(
    () => buildNearbyStoreRegions(myHallStores, homeRegionKey),
    [homeRegionKey, myHallStores],
  );
  const myHallNearbyStoreItems = useMemo(
    () =>
      myHallNearbyRegions.flatMap((region) =>
        region.stores.map((store) => ({
          store,
          metaText: formatNearbyStoreMeta(region),
        })),
      ),
    [myHallNearbyRegions],
  );
  const filteredStores = useMemo(() => {
    if (!normalizedQuery) {
      return completeStores;
    }

    return completeStores.filter((store) =>
      normalizeText(store.storeName).includes(normalizedQuery),
    );
  }, [completeStores, normalizedQuery]);
  const favoriteFilteredStores = useMemo(
    () =>
      normalizedQuery
        ? filteredStores.filter((store) => myHallStoreIdSet.has(normalizeStoreId(store.id)))
        : [],
    [filteredStores, myHallStoreIdSet, normalizedQuery],
  );
  const favoriteFilteredStoreGroups = useMemo(
    () => buildStoreLocationGroups(favoriteFilteredStores),
    [favoriteFilteredStores],
  );
  const otherFilteredStores = useMemo(
    () => filteredStores.filter((store) => !myHallStoreIdSet.has(normalizeStoreId(store.id))),
    [filteredStores, myHallStoreIdSet],
  );
  const storeGroups = useMemo(() => buildStoreGroups(otherFilteredStores), [otherFilteredStores]);
  const nearbyStoreRegions = useMemo(
    () => buildNearbyStoreRegions(otherFilteredStores, homeRegionKey),
    [homeRegionKey, otherFilteredStores],
  );
  const homeRegionOptions = useMemo(() => buildHomeRegionOptions(completeStores), [completeStores]);
  const homeRegionOptionMap = useMemo(
    () => buildRegionOptionMap(homeRegionOptions),
    [homeRegionOptions],
  );
  const homeAddressOptions = useMemo(
    () =>
      homeAddressRegionKeys.map((regionKey) => ({
        key: regionKey,
        label: readRegionOptionLabel(regionKey, homeRegionOptionMap),
      })),
    [homeAddressRegionKeys, homeRegionOptionMap],
  );
  const isNearbyRegionOrder = regionOrderMode === REGION_ORDER_NEAR_HOME && Boolean(homeRegionKey);
  const isMyHallNearbyOrder = myHallOrderMode === MY_HALL_ORDER_NEAR_HOME && Boolean(homeRegionKey);
  const shouldOpenMatchedRegions = Boolean(normalizedQuery);
  const directoryTitle = myHallStores.length > 0 ? "その他の店舗" : "保存済み店舗";
  const emptyListText =
    normalizedQuery && favoriteFilteredStores.length > 0
      ? "その他に一致する店舗はありません。"
      : normalizedQuery
        ? "該当する店舗はありません。"
        : "その他の店舗はありません。";

  useEffect(() => {
    const syncMyHallStoreIds = () => {
      setMyHallStoreIds(readSavedMyHallStoreIds());
    };

    const syncRegionOrder = () => {
      const savedRegionOrderMode = readStoredText(
        REGION_ORDER_MODE_STORAGE_KEY,
        REGION_ORDER_NORMAL,
      );
      const savedMyHallOrderMode = readStoredText(
        MY_HALL_ORDER_MODE_STORAGE_KEY,
        MY_HALL_ORDER_SAVED,
      );
      const savedHomeRegionKey = normalizeRegionKey(readStoredText(HOME_REGION_STORAGE_KEY, ""));
      const savedAddressRegionKeys = normalizeRegionKeys([
        ...readStoredRegionKeys(HOME_ADDRESS_REGIONS_STORAGE_KEY),
        savedHomeRegionKey,
      ]);
      setRegionOrderMode(
        savedRegionOrderMode === REGION_ORDER_NEAR_HOME
          ? REGION_ORDER_NEAR_HOME
          : REGION_ORDER_NORMAL,
      );
      setMyHallOrderMode(
        savedMyHallOrderMode === MY_HALL_ORDER_NEAR_HOME
          ? MY_HALL_ORDER_NEAR_HOME
          : MY_HALL_ORDER_SAVED,
      );
      setHomeRegionKey(savedHomeRegionKey);
      setHomeAddressRegionKeys(savedAddressRegionKeys);
      saveStoredRegionKeys(HOME_ADDRESS_REGIONS_STORAGE_KEY, savedAddressRegionKeys);
    };

    syncMyHallStoreIds();
    syncRegionOrder();
    window.addEventListener(MY_HALL_CHANGE_EVENT, syncMyHallStoreIds);
    window.addEventListener("storage", syncMyHallStoreIds);
    window.addEventListener("storage", syncRegionOrder);

    return () => {
      window.removeEventListener(MY_HALL_CHANGE_EVENT, syncMyHallStoreIds);
      window.removeEventListener("storage", syncMyHallStoreIds);
      window.removeEventListener("storage", syncRegionOrder);
    };
  }, []);

  useEffect(() => {
    if (homeAddressDraftKey || homeRegionOptions.length === 0) {
      return;
    }

    const dazaifuRegionKey = buildRegionKey("福岡県", "太宰府市");
    setHomeAddressDraftKey(
      homeRegionOptionMap.has(dazaifuRegionKey) ? dazaifuRegionKey : homeRegionOptions[0].key,
    );
  }, [homeAddressDraftKey, homeRegionOptionMap, homeRegionOptions]);

  const handleRegionOrderModeChange = (event) => {
    const nextMode =
      event.target.value === REGION_ORDER_NEAR_HOME ? REGION_ORDER_NEAR_HOME : REGION_ORDER_NORMAL;
    setRegionOrderMode(nextMode);
    saveStoredText(REGION_ORDER_MODE_STORAGE_KEY, nextMode);
  };

  const handleMyHallOrderModeChange = (event) => {
    const nextMode =
      event.target.value === MY_HALL_ORDER_NEAR_HOME ? MY_HALL_ORDER_NEAR_HOME : MY_HALL_ORDER_SAVED;
    setMyHallOrderMode(nextMode);
    saveStoredText(MY_HALL_ORDER_MODE_STORAGE_KEY, nextMode);
  };

  const setActiveHomeRegion = (regionKey) => {
    const nextHomeRegionKey = normalizeRegionKey(regionKey);
    setHomeRegionKey(nextHomeRegionKey);
    saveStoredText(HOME_REGION_STORAGE_KEY, nextHomeRegionKey);
  };

  const handleHomeRegionChange = (event) => {
    setActiveHomeRegion(event.target.value);
  };

  const handleHomeAddressDraftChange = (event) => {
    setHomeAddressDraftKey(normalizeRegionKey(event.target.value));
  };

  const handleAddHomeAddress = () => {
    if (!homeAddressDraftKey) {
      return;
    }

    const nextAddressRegionKeys = normalizeRegionKeys([
      ...homeAddressRegionKeys,
      homeAddressDraftKey,
    ]);
    setHomeAddressRegionKeys(nextAddressRegionKeys);
    saveStoredRegionKeys(HOME_ADDRESS_REGIONS_STORAGE_KEY, nextAddressRegionKeys);

    if (!homeRegionKey) {
      setHomeRegionKey(homeAddressDraftKey);
      saveStoredText(HOME_REGION_STORAGE_KEY, homeAddressDraftKey);
    }
  };

  const handleRemoveHomeAddress = (regionKey) => {
    const normalizedRegionKey = normalizeRegionKey(regionKey);
    if (!normalizedRegionKey) {
      return;
    }

    const nextAddressRegionKeys = homeAddressRegionKeys.filter(
      (homeAddressRegionKey) => homeAddressRegionKey !== normalizedRegionKey,
    );
    setHomeAddressRegionKeys(nextAddressRegionKeys);
    saveStoredRegionKeys(HOME_ADDRESS_REGIONS_STORAGE_KEY, nextAddressRegionKeys);

    if (homeRegionKey === normalizedRegionKey) {
      const nextHomeRegionKey = nextAddressRegionKeys[0] || "";
      setHomeRegionKey(nextHomeRegionKey);
      saveStoredText(HOME_REGION_STORAGE_KEY, nextHomeRegionKey);
    }
  };

  const handleToggleMyHall = (store) => {
    const storeId = normalizeStoreId(store.id);
    if (!storeId) {
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

  const clearMyHallDragState = () => {
    draggedMyHallStoreIdRef.current = "";
    setDraggedMyHallStoreId("");
    setDragOverMyHallStoreId("");
  };

  const moveMyHallStoreId = (sourceStoreId, targetStoreId) => {
    if (!sourceStoreId || !targetStoreId) {
      return;
    }

    const sourceStore = storeById.get(normalizeStoreId(sourceStoreId));
    const targetStore = storeById.get(normalizeStoreId(targetStoreId));
    if (readStoreLocationGroupKey(sourceStore) !== readStoreLocationGroupKey(targetStore)) {
      return;
    }

    setDragOverMyHallStoreId(targetStoreId);
    setMyHallStoreIds((currentStoreIds) => {
      const nextStoreIds = reorderStoreIds(currentStoreIds, sourceStoreId, targetStoreId);
      if (nextStoreIds === currentStoreIds) {
        return currentStoreIds;
      }

      saveMyHallStoreIds(nextStoreIds);
      return nextStoreIds;
    });
  };

  const handleMyHallPointerDown = (event, store) => {
    if (!(event.target instanceof Element) || !event.target.closest(".storeDragHandle")) {
      return;
    }

    const storeId = normalizeStoreId(store.id);
    if (!storeId) {
      return;
    }

    event.preventDefault();
    draggedMyHallStoreIdRef.current = storeId;
    setDraggedMyHallStoreId(storeId);
    setDragOverMyHallStoreId(storeId);
    event.currentTarget.setPointerCapture?.(event.pointerId);
  };

  const handleMyHallPointerMove = (event) => {
    const sourceStoreId = draggedMyHallStoreIdRef.current;
    if (!sourceStoreId) {
      return;
    }

    event.preventDefault();
    const targetElement = document.elementFromPoint(event.clientX, event.clientY);
    const targetItem = targetElement?.closest?.("[data-my-hall-store-id]");
    const targetStoreId = targetItem?.getAttribute("data-my-hall-store-id") || "";
    moveMyHallStoreId(sourceStoreId, targetStoreId);
  };

  const handleMyHallPointerEnd = (event) => {
    if (!draggedMyHallStoreIdRef.current) {
      return;
    }

    event.preventDefault();
    event.currentTarget.releasePointerCapture?.(event.pointerId);
    clearMyHallDragState();
  };

  return (
    <>
      {completeStores.length > 0 ? (
        <section className="tablePanel myHallPanel">
          <div className="tablePanelHeader myHallHeader">
            <div>
              <p className="sectionLabel">マイホール</p>
              <h2 className="tablePanelTitle">お気に入り店舗</h2>
            </div>
            <span className="myHallCount">{myHallStores.length}店舗</span>
          </div>
          {myHallStores.length > 0 ? (
            <>
              <div className="myHallControlRow">
                <label className="storeRegionControlField">
                  <span>マイホールの並び</span>
                  <select
                    className="storeRegionControlSelect"
                    value={myHallOrderMode}
                    onChange={handleMyHallOrderModeChange}
                  >
                    <option value={MY_HALL_ORDER_SAVED}>保存順</option>
                    <option value={MY_HALL_ORDER_NEAR_HOME}>選択住所から近い順</option>
                  </select>
                </label>
                <label className="storeRegionControlField">
                  <span>使用する住所</span>
                  <select
                    className="storeRegionControlSelect"
                    value={homeRegionKey}
                    onChange={handleHomeRegionChange}
                    disabled={homeAddressOptions.length === 0}
                  >
                    <option value="">未選択</option>
                    {homeAddressOptions.map((option) => (
                      <option key={option.key} value={option.key}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </label>
              </div>
              {isMyHallNearbyOrder ? (
                <ul className="myHallList myHallNearbyList">
                  {myHallNearbyStoreItems.map(({ store, metaText }) => (
                    <li key={store.id} className="myHallItem">
                      <StoreListItem
                        store={store}
                        isFavorite={myHallStoreIdSet.has(normalizeStoreId(store.id))}
                        onToggle={handleToggleMyHall}
                        compact
                        metaText={metaText}
                      />
                    </li>
                  ))}
                </ul>
              ) : (
                <div className="myHallGroupList">
                  {myHallStoreGroups.map((group) => (
                    <section className="myHallGroup" key={group.key}>
                      <div className="storeSubsectionHeader">
                        <p className="storeSubsectionTitle">{group.label}</p>
                        <span>{group.storeCount}店舗</span>
                      </div>
                      <ul className="myHallList">
                        {group.stores.map((store) => {
                          const storeId = normalizeStoreId(store.id);
                          const isReorderable = group.stores.length > 1;
                          const itemClassName = [
                            "myHallItem",
                            isReorderable ? "myHallItemReorderable" : "",
                            draggedMyHallStoreId === storeId ? "myHallItemDragging" : "",
                            dragOverMyHallStoreId === storeId && draggedMyHallStoreId !== storeId
                              ? "myHallItemDragOver"
                              : "",
                          ]
                            .filter(Boolean)
                            .join(" ");

                          return (
                            <li
                              key={store.id}
                              className={itemClassName}
                              data-my-hall-store-id={storeId}
                              onPointerDown={
                                isReorderable
                                  ? (event) => handleMyHallPointerDown(event, store)
                                  : undefined
                              }
                              onPointerMove={isReorderable ? handleMyHallPointerMove : undefined}
                              onPointerUp={isReorderable ? handleMyHallPointerEnd : undefined}
                              onPointerCancel={isReorderable ? handleMyHallPointerEnd : undefined}
                            >
                              <StoreListItem
                                store={store}
                                isFavorite={myHallStoreIdSet.has(normalizeStoreId(store.id))}
                                onToggle={handleToggleMyHall}
                                compact
                                reorderable={isReorderable}
                              />
                            </li>
                          );
                        })}
                      </ul>
                    </section>
                  ))}
                </div>
              )}
            </>
          ) : (
            <p className="myHallEmpty">店舗一覧の星を押すと、ここに店舗を固定できます。</p>
          )}
        </section>
      ) : null}

      {completeStores.length === 0 ? (
        <section className="statusPanel">
          <h2>店舗がまだありません</h2>
          <p>登録待ちURLを更新するか、台データを取得してください。</p>
        </section>
      ) : (
        <section className="tablePanel directoryPanel storeDirectoryPanel">
          <div className="tablePanelHeader storeDirectoryHeader">
            <div>
              <p className="sectionLabel">店舗一覧</p>
              <h2 className="tablePanelTitle">{directoryTitle}</h2>
            </div>
            <span className="storeResultCount">{otherFilteredStores.length}店舗</span>
          </div>
          <div className="storeSearchRow">
            <label className="storeSearchField">
              <span>店舗名検索</span>
              <input
                className="storeSearchInput"
                type="search"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder="店舗名を入力"
              />
            </label>
            {query ? (
              <button className="storeSearchClear" type="button" onClick={() => setQuery("")}>
                消す
              </button>
            ) : null}
          </div>
          <div className="storeRegionControlRow">
            <label className="storeRegionControlField">
              <span>地域の並び</span>
              <select
                className="storeRegionControlSelect"
                value={regionOrderMode}
                onChange={handleRegionOrderModeChange}
              >
                <option value={REGION_ORDER_NORMAL}>通常の地域順</option>
                <option value={REGION_ORDER_NEAR_HOME}>自宅地域から近い順</option>
              </select>
            </label>
            <label className="storeRegionControlField">
              <span>選択中の住所</span>
              <select
                className="storeRegionControlSelect"
                value={homeRegionKey}
                onChange={handleHomeRegionChange}
                disabled={homeAddressOptions.length === 0}
              >
                <option value="">未選択</option>
                {homeAddressOptions.map((option) => (
                  <option key={option.key} value={option.key}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <label className="storeRegionControlField storeRegionAddField">
              <span>追加する住所</span>
              <select
                className="storeRegionControlSelect"
                value={homeAddressDraftKey}
                onChange={handleHomeAddressDraftChange}
              >
                {homeRegionOptions.map((option) => (
                  <option key={option.key} value={option.key}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <button
              className="storeSearchClear homeAddressAddButton"
              type="button"
              onClick={handleAddHomeAddress}
              disabled={!homeAddressDraftKey || homeAddressRegionKeys.includes(homeAddressDraftKey)}
            >
              追加
            </button>
          </div>
          <div className="homeAddressList">
            <span className="homeAddressListLabel">登録住所</span>
            {homeAddressOptions.length > 0 ? (
              homeAddressOptions.map((option) => (
                <span
                  className={[
                    "homeAddressChip",
                    option.key === homeRegionKey ? "homeAddressChipActive" : "",
                  ]
                    .filter(Boolean)
                    .join(" ")}
                  key={option.key}
                >
                  <button type="button" onClick={() => setActiveHomeRegion(option.key)}>
                    {option.label}
                  </button>
                  <button
                    className="homeAddressRemoveButton"
                    type="button"
                    onClick={() => handleRemoveHomeAddress(option.key)}
                    aria-label={`${option.label}を削除`}
                  >
                    削除
                  </button>
                </span>
              ))
            ) : (
              <span className="homeAddressEmpty">住所を追加してください。</span>
            )}
          </div>
          {favoriteFilteredStores.length > 0 ? (
            <div className="storeFavoriteMatches">
              <div className="storeSubsectionHeader">
                <p className="storeSubsectionTitle">マイホールの一致店舗</p>
                <span>{favoriteFilteredStores.length}店舗</span>
              </div>
              <div className="storeFavoriteMatchGroupList">
                {favoriteFilteredStoreGroups.map((group) => (
                  <section className="myHallGroup" key={group.key}>
                    <div className="storeSubsectionHeader">
                      <p className="storeSubsectionTitle">{group.label}</p>
                      <span>{group.storeCount}店舗</span>
                    </div>
                    <ul className="storeLinkList">
                      {group.stores.map((store) => (
                        <li key={store.id}>
                          <StoreListItem
                            store={store}
                            isFavorite={myHallStoreIdSet.has(normalizeStoreId(store.id))}
                            onToggle={handleToggleMyHall}
                          />
                        </li>
                      ))}
                    </ul>
                  </section>
                ))}
              </div>
            </div>
          ) : null}
          {otherFilteredStores.length === 0 ? (
            <div className="emptyListPanel">{emptyListText}</div>
          ) : isNearbyRegionOrder ? (
            <div className="storeRegionList storeNearbyRegionList">
              {nearbyStoreRegions.map((region) => (
                <details
                  className="storeRegionGroup"
                  key={region.key}
                  open={shouldOpenMatchedRegions || undefined}
                >
                  <summary className="storeRegionSummary">
                    <RegionSummaryTitle label={region.label} distanceKm={region.distanceKm} />
                    <span>{region.stores.length}店舗</span>
                  </summary>
                  <ul className="storeLinkList storeRegionStoreList">
                    {region.stores.map((store) => (
                      <li key={store.id}>
                        <StoreListItem
                          store={store}
                          isFavorite={myHallStoreIdSet.has(normalizeStoreId(store.id))}
                          onToggle={handleToggleMyHall}
                        />
                      </li>
                    ))}
                  </ul>
                </details>
              ))}
            </div>
          ) : (
            <div className="storeGroupList">
              {storeGroups.map((prefecture) => (
                <section className="storePrefectureGroup" key={prefecture.prefectureName}>
                  <div className="storePrefectureHeader">
                    <h3>{prefecture.prefectureName}</h3>
                    <span>{prefecture.storeCount}店舗</span>
                  </div>
                  <div className="storeRegionList">
                    {prefecture.areas.map((area) => (
                      <details
                        className="storeRegionGroup"
                        key={`${prefecture.prefectureName}-${area.areaName}`}
                        open={shouldOpenMatchedRegions || undefined}
                      >
                        <summary className="storeRegionSummary">
                          <RegionSummaryTitle label={area.areaName} />
                          <span>{area.stores.length}店舗</span>
                        </summary>
                        <ul className="storeLinkList storeRegionStoreList">
                          {area.stores.map((store) => (
                            <li key={store.id}>
                              <StoreListItem
                                store={store}
                                isFavorite={myHallStoreIdSet.has(normalizeStoreId(store.id))}
                                onToggle={handleToggleMyHall}
                              />
                            </li>
                          ))}
                        </ul>
                      </details>
                    ))}
                  </div>
                </section>
              ))}
            </div>
          )}
        </section>
      )}

      {pendingStores.length > 0 ? (
        <section className="tablePanel directoryPanel">
          <div className="tablePanelHeader">
            <div>
              <p className="sectionLabel">登録待ち</p>
              <h2 className="tablePanelTitle">店舗URL</h2>
            </div>
          </div>
          <div className="tableScroller directoryScroller">
            <table className="directoryTable">
              <thead>
                <tr>
                  <th>URL</th>
                  <th>状態</th>
                </tr>
              </thead>
              <tbody>
                {pendingStores.map((store) => (
                  <tr key={store.id}>
                    <td>{store.storeUrl}</td>
                    <td>店舗名取得待ち</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}
    </>
  );
}
