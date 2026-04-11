import Link from "next/link";

import { getStoreSummaries } from "../lib/data";
import { formatCompactDate, formatNumber, formatPeriod } from "../lib/format";

export const dynamic = "force-dynamic";

export default async function StoresPage() {
  try {
    const stores = await getStoreSummaries();
    const latestDate = stores.reduce(
      (currentLatest, store) =>
        !currentLatest || (store.endDate && store.endDate > currentLatest) ? store.endDate : currentLatest,
      null,
    );

    return (
      <main className="pageStack">
        <section className="heroPanel">
          <div className="heroCopy">
            <p className="eyebrow">Supabase Viewer</p>
            <h1 className="pageTitle">店舗一覧</h1>
            <p className="leadText">
              保存済みの店舗データから、機種一覧と台データ比較へ順番に進めます。
            </p>
          </div>
          <div className="heroMeta">
            <div className="metaCard">
              <span className="metaLabel">登録店舗</span>
              <strong className="metaValue">{formatNumber(stores.length)}</strong>
            </div>
            <div className="metaCard">
              <span className="metaLabel">最新日</span>
              <strong className="metaValue">
                {latestDate ? formatCompactDate(latestDate) : "-"}
              </strong>
            </div>
          </div>
        </section>

        {stores.length === 0 ? (
          <section className="statusPanel">
            <h2>保存済みの店舗がまだありません</h2>
            <p>
              先にGUIアプリで台データを取得し、`Supabase` の `stores` と
              `machine_daily_results` に保存してください。
            </p>
          </section>
        ) : (
          <section className="tablePanel directoryPanel">
            <div className="tablePanelHeader">
              <div>
                <p className="sectionLabel">店舗一覧</p>
                <h2 className="tablePanelTitle">保存済み店舗</h2>
              </div>
            </div>
            <div className="tableScroller directoryScroller">
              <table className="directoryTable">
                <thead>
                  <tr>
                    <th className="directoryNameHeader">店舗</th>
                    <th>期間</th>
                    <th>最新日</th>
                    <th>機種数</th>
                    <th>台番数</th>
                    <th>日数</th>
                    <th>記録件数</th>
                    <th>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {stores.map((store) => (
                    <tr key={store.id}>
                      <th className="directoryNameCell">
                        <Link href={`/stores/${store.id}`} className="directoryPrimaryLink">
                          {store.storeName}
                        </Link>
                      </th>
                      <td>{formatPeriod(store.startDate, store.endDate)}</td>
                      <td>{store.endDate ? formatCompactDate(store.endDate) : "-"}</td>
                      <td>{formatNumber(store.machineCount)}</td>
                      <td>{formatNumber(store.slotCount)}</td>
                      <td>{formatNumber(store.dayCount)}</td>
                      <td>{formatNumber(store.recordCount)}</td>
                      <td>
                        <Link href={`/stores/${store.id}`} className="tableActionLink">
                          機種一覧
                        </Link>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        )}
      </main>
    );
  } catch (error) {
    return (
      <main className="pageStack">
        <section className="heroPanel">
          <div className="heroCopy">
            <p className="eyebrow">Supabase Viewer</p>
            <h1 className="pageTitle">店舗一覧</h1>
          </div>
        </section>
        <section className="statusPanel">
          <h2>店舗一覧を読み込めませんでした</h2>
          <p>{error instanceof Error ? error.message : "設定を確認してください。"}</p>
        </section>
      </main>
    );
  }
}
