import Link from "next/link";
import { notFound } from "next/navigation";

import { Breadcrumbs } from "../../../components/breadcrumbs";
import { getStoreDetail } from "../../../lib/data";
import { isHuntScoreTargetStore } from "../../../lib/hunt-score";
import {
  formatAverageGames,
  formatCompactDate,
  formatNumber,
  formatPercent,
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
  const hasHuntScoreRanking = isHuntScoreTargetStore(store.storeName);

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
            各機種ごとの最新記録日を基準に一覧を出し、機種名から台データページへ進めます。
          </p>
          {hasHuntScoreRanking ? (
            <div className="heroLinks">
              <Link href={`/stores/${store.id}/hunt-ranking`} className="inlineAction">
                狙い度ランキングを見る
              </Link>
            </div>
          ) : null}
        </div>
        <div className="heroMeta heroMetaWide">
          <div className="metaCard">
            <span className="metaLabel">保存済み機種数</span>
            <strong className="metaValue">{formatNumber(summary.machineCount)}</strong>
          </div>
          <div className="metaCard">
            <span className="metaLabel">店内の最新日</span>
            <strong className="metaValue">
              {summary.latestDate ? formatCompactDate(summary.latestDate) : "-"}
            </strong>
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
        <section className="tablePanel directoryPanel">
          <div className="tablePanelHeader">
            <div>
              <p className="sectionLabel">機種一覧</p>
              <h2 className="tablePanelTitle">{store.storeName}</h2>
            </div>
          </div>
          <div className="tableScroller directoryScroller">
            <table className="directoryTable">
              <thead>
                <tr>
                  <th className="directoryNameHeader">機種</th>
                  <th>最新日</th>
                  <th>台数</th>
                  <th>平均差枚</th>
                  <th>平均G数</th>
                  <th>平均出率</th>
                </tr>
              </thead>
              <tbody>
                {machines.map((machine) => {
                  const machineHref = `/stores/${store.id}/machines/${encodeURIComponent(machine.machineName)}`;

                  return (
                    <tr key={machine.machineName}>
                      <th className="directoryNameCell">
                        <Link href={machineHref} className="directoryPrimaryLink">
                          {machine.machineName}
                        </Link>
                      </th>
                      <td>{machine.latestDate ? formatCompactDate(machine.latestDate) : "-"}</td>
                      <td>{formatNumber(machine.slotCount)}</td>
                      <td>{formatSignedNumber(machine.latestAverageDifference)}</td>
                      <td>{formatAverageGames(machine.latestAverageGames)}</td>
                      <td>{formatPercent(machine.latestAveragePayout)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </main>
  );
}
