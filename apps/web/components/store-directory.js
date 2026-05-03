"use client";

import Link from "next/link";
import { useMemo, useState } from "react";

function normalizeText(value) {
  return String(value ?? "").trim().toLocaleLowerCase("ja");
}

function normalizeGroupName(value, fallback) {
  const text = String(value ?? "").trim();
  return text || fallback;
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

export function StoreDirectory({ completeStores, pendingStores }) {
  const [query, setQuery] = useState("");
  const normalizedQuery = normalizeText(query);
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

  return (
    <>
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
                              <Link href={`/stores/${store.id}`} className="plainStoreLink">
                                {store.storeName}
                              </Link>
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
