import Link from "next/link";
import { notFound } from "next/navigation";

import { Breadcrumbs } from "../../../components/breadcrumbs";
import { getStoreDetail } from "../../../lib/data";
import {
  formatAverageGames,
  formatCompactDate,
  formatNumber,
  formatPercent,
  formatPeriod,
  formatSignedNumber,
} from "../../../lib/format";

export const dynamic = "force-dynamic";

export default async function StoreDetailPage({ params }) {
  const { storeId } = await params;
  let storeDetail;

  try {
    storeDetail = await getStoreDetail(storeId);
  } catch (error) {
    return (
      <main className="pageStack">
        <Breadcrumbs items={[{ label: "店舗一覧", href: "/" }, { label: "機種一覧" }]} />
        <section className="statusPanel">
          <h2>機種一覧を読み込めませんでした</h2>
          <p>{error instanceof Error ? error.message : "設定を確認してください。"}</p>
        </section>
      </main>
    );
  }

  if (!storeDetail) {
    notFound();
  }

  const { store, summary, machines } = storeDetail;

  return (
    <main className="pageStack">
      <Breadcrumbs
        items={[
          { label: "店舗一覧", href: "/" },
          { label: store.storeName },
        ]}
      />

      <section className="heroPanel">
        <div className="heroCopy">
          <p className="eyebrow">Machine Directory</p>
          <h1 className="pageTitle">{store.storeName}</h1>
          <p className="leadText">
            機種ごとに、最新日の平均値と保存済みの記録期間を見ながら台データページへ進めます。
          </p>
        </div>
        <div className="heroMeta heroMetaWide">
          <div className="metaCard">
            <span className="metaLabel">機種数</span>
            <strong className="metaValue">{formatNumber(summary.machineCount)}</strong>
          </div>
          <div className="metaCard">
            <span className="metaLabel">期間</span>
            <strong className="metaValue">{formatPeriod(summary.startDate, summary.endDate)}</strong>
          </div>
          <div className="metaCard">
            <span className="metaLabel">元ページ</span>
            <a href={store.storeUrl} target="_blank" rel="noreferrer" className="externalLink">
              店舗ページを開く
            </a>
          </div>
        </div>
      </section>

      {machines.length === 0 ? (
        <section className="statusPanel">
          <h2>この店舗には保存済みの機種データがありません</h2>
          <p>GUIアプリ側でこの店舗の台データを取得すると、ここに機種一覧が並びます。</p>
        </section>
      ) : (
        <section className="cardsGrid">
          {machines.map((machine) => (
            <Link
              key={machine.machineName}
              href={`/stores/${store.id}/machines/${encodeURIComponent(machine.machineName)}`}
              className="linkCard machineCard"
            >
              <div className="cardGlow" />
              <div className="cardTop">
                <span className="cardBadge">Machine</span>
                <span className="cardDate">
                  最新 {machine.latestDate ? formatCompactDate(machine.latestDate) : "-"}
                </span>
              </div>
              <h2 className="cardTitle">{machine.machineName}</h2>
              <p className="cardLead">{formatPeriod(machine.startDate, machine.endDate)}</p>
              <dl className="statsGrid">
                <div>
                  <dt>台数</dt>
                  <dd>{formatNumber(machine.slotCount)}</dd>
                </div>
                <div>
                  <dt>日数</dt>
                  <dd>{formatNumber(machine.dayCount)}</dd>
                </div>
                <div>
                  <dt>平均差枚</dt>
                  <dd>{formatSignedNumber(machine.latestAverageDifference)}</dd>
                </div>
                <div>
                  <dt>平均G数</dt>
                  <dd>{formatAverageGames(machine.latestAverageGames)}</dd>
                </div>
                <div>
                  <dt>平均出率</dt>
                  <dd>{formatPercent(machine.latestAveragePayout)}</dd>
                </div>
                <div>
                  <dt>記録件数</dt>
                  <dd>{formatNumber(machine.recordCount)}</dd>
                </div>
              </dl>
              <span className="cardAction">台データを見る</span>
            </Link>
          ))}
        </section>
      )}
    </main>
  );
}
