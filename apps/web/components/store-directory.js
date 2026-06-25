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

function normalizeText(value) {
  return String(value ?? "").trim().toLocaleLowerCase("ja");
}

function normalizeGroupName(value, fallback) {
  const text = String(value ?? "").trim();
  return text || fallback;
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

function buildStoreGroups(stores) {
  const prefectureGroups = new Map();

  for (const store of stores) {
    const prefectureName = normalizeGroupName(store.prefectureName, "都道府県未設定");
    const areaName = normalizeGroupName(store.areaName, "地域未設定");
    if (!prefectureGroups.has(prefectureName)) {
      prefectureGroups.set(prefectureName, new Map());
    }
    const areaGroups = prefectureGroups.get(prefectureName);
    if (!areaGroups.has(areaName)) {
      areaGroups.set(areaName, []);
    }
    areaGroups.get(areaName).push(store);
  }

  return [...prefectureGroups.entries()]
    .sort(([left], [right]) => left.localeCompare(right, "ja"))
    .map(([prefectureName, areaGroups]) => {
      const areas = [...areaGroups.entries()]
        .sort(([left], [right]) => left.localeCompare(right, "ja"))
        .map(([areaName, areaStores]) => ({
          areaName,
          stores: [...areaStores].sort((left, right) => left.storeName.localeCompare(right.storeName, "ja")),
        }));

      return {
        prefectureName,
        areas,
        storeCount: areas.reduce((count, area) => count + area.stores.length, 0),
      };
    });
}

function StoreListItem({ store, isFavorite, onToggle, compact = false, reorderable = false }) {
  const className = [
    "storeLinkItem",
    compact ? "storeLinkItemCompact" : "",
    reorderable ? "storeLinkItemDraggable" : "",
    isFavorite ? "storeLinkItemFavorite" : "",
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
      <Link href={`/stores/${store.id}`} className="plainStoreLink storeRowLink" draggable={false}>
        {store.storeName}
      </Link>
    </div>
  );
}

export function StoreDirectory({ completeStores, pendingStores }) {
  const [query, setQuery] = useState("");
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

    syncMyHallStoreIds();
    window.addEventListener(MY_HALL_CHANGE_EVENT, syncMyHallStoreIds);
    window.addEventListener("storage", syncMyHallStoreIds);

    return () => {
      window.removeEventListener(MY_HALL_CHANGE_EVENT, syncMyHallStoreIds);
      window.removeEventListener("storage", syncMyHallStoreIds);
    };
  }, []);

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
                          <span>{area.areaName}</span>
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
