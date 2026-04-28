"use client";

import Link from "next/link";
import { useMemo, useState } from "react";

function normalizeText(value) {
  return String(value ?? "").trim().toLocaleLowerCase("ja");
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

  return (
    <>
      {completeStores.length === 0 ? (
        <section className="statusPanel">
          <h2>完全登録済みの店舗がまだありません</h2>
          <p>登録待ちURLを更新するか、台データを取得してください。</p>
        </section>
      ) : (
        <section className="tablePanel directoryPanel storeDirectoryPanel">
          <div className="tablePanelHeader storeDirectoryHeader">
            <div>
              <p className="sectionLabel">店舗一覧</p>
              <h2 className="tablePanelTitle">保存済み店舗</h2>
            </div>
            <p className="directoryCountText">
              {filteredStores.length} / {completeStores.length}店
            </p>
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
            <div className="tableScroller directoryScroller">
              <table className="directoryTable homeStoreTable">
                <thead>
                  <tr>
                    <th className="directoryNameHeader">店舗</th>
                    <th className="directoryActionHeader">操作</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredStores.map((store) => (
                    <tr key={store.id}>
                      <th className="directoryNameCell">
                        <Link href={`/stores/${store.id}`} className="directoryPrimaryLink">
                          {store.storeName}
                        </Link>
                      </th>
                      <td>
                        <Link href={`/stores/${store.id}`} className="tableActionLink">
                          開く
                        </Link>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
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
            <p className="directoryCountText">{pendingStores.length}件</p>
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
