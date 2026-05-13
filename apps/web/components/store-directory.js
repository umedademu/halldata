"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

const MY_HALL_STORAGE_KEY = "halldata-my-hall-store-ids";
const MY_HALL_CHANGE_EVENT = "halldata-my-hall-change";

function normalizeText(value) {
  return String(value ?? "").trim().toLocaleLowerCase("ja");
}

function normalizeStoreId(value) {
  return String(value ?? "").trim();
}

function normalizeGroupName(value, fallback) {
  const text = String(value ?? "").trim();
  return text || fallback;
}

function readSavedMyHallStoreIds() {
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

function saveMyHallStoreIds(storeIds) {
  if (typeof window === "undefined") {
    return;
  }

  const normalizedStoreIds = [...new Set(storeIds.map(normalizeStoreId).filter(Boolean))];
  window.localStorage.setItem(MY_HALL_STORAGE_KEY, JSON.stringify(normalizedStoreIds));
  window.dispatchEvent(new CustomEvent(MY_HALL_CHANGE_EVENT));
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

function StoreFavoriteButton({ store, isFavorite, onToggle }) {
  return (
    <button
      type="button"
      className={`storeFavoriteButton ${isFavorite ? "storeFavoriteButtonActive" : ""}`}
      onClick={() => onToggle(store)}
      aria-label={`${store.storeName}を${isFavorite ? "マイホールから外す" : "マイホールに追加"}`}
      title={isFavorite ? "マイホールから外す" : "マイホールに追加"}
    >
      <span aria-hidden="true">{isFavorite ? "★" : "☆"}</span>
    </button>
  );
}

function StoreListItem({ store, isFavorite, onToggle, compact = false }) {
  return (
    <div className={`storeLinkItem ${compact ? "storeLinkItemCompact" : ""}`}>
      <StoreFavoriteButton store={store} isFavorite={isFavorite} onToggle={onToggle} />
      <Link href={`/stores/${store.id}`} className="plainStoreLink storeRowLink">
        {store.storeName}
      </Link>
    </div>
  );
}

export function StoreDirectory({ completeStores, pendingStores }) {
  const [query, setQuery] = useState("");
  const [myHallStoreIds, setMyHallStoreIds] = useState([]);
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
  const filteredStores = useMemo(() => {
    if (!normalizedQuery) {
      return completeStores;
    }

    return completeStores.filter((store) =>
      normalizeText(store.storeName).includes(normalizedQuery),
    );
  }, [completeStores, normalizedQuery]);
  const storeGroups = useMemo(() => buildStoreGroups(filteredStores), [filteredStores]);
  const shouldOpenMatchedRegions = Boolean(normalizedQuery);

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
            <ul className="myHallList">
              {myHallStores.map((store) => (
                <li key={store.id}>
                  <StoreListItem
                    store={store}
                    isFavorite={myHallStoreIdSet.has(normalizeStoreId(store.id))}
                    onToggle={handleToggleMyHall}
                    compact
                  />
                </li>
              ))}
            </ul>
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
              <h2 className="tablePanelTitle">保存済み店舗</h2>
            </div>
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
          {filteredStores.length === 0 ? (
            <div className="emptyListPanel">該当する店舗はありません。</div>
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
