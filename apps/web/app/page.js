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
          <section className="cardsGrid">
            {stores.map((store) => (
              <Link key={store.id} href={`/stores/${store.id}`} className="linkCard">
                <div className="cardGlow" />
                <div className="cardTop">
                  <span className="cardBadge">Store</span>
                  <span className="cardDate">
                    最新 {store.endDate ? formatCompactDate(store.endDate) : "-"}
                  </span>
                </div>
                <h2 className="cardTitle">{store.storeName}</h2>
                <p className="cardLead">{formatPeriod(store.startDate, store.endDate)}</p>
                <dl className="statsGrid">
                  <div>
                    <dt>機種数</dt>
                    <dd>{formatNumber(store.machineCount)}</dd>
                  </div>
                  <div>
                    <dt>台番数</dt>
                    <dd>{formatNumber(store.slotCount)}</dd>
                  </div>
                  <div>
                    <dt>記録件数</dt>
                    <dd>{formatNumber(store.recordCount)}</dd>
                  </div>
                  <div>
                    <dt>日数</dt>
                    <dd>{formatNumber(store.dayCount)}</dd>
                  </div>
                </dl>
                <span className="cardAction">機種一覧を見る</span>
              </Link>
            ))}
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
