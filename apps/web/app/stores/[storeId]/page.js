import Link from "next/link";
import { notFound } from "next/navigation";

import { Breadcrumbs } from "../../../components/breadcrumbs";
import { getStoreDetail, getStoreIdentity } from "../../../lib/data";
import {
  formatAverageGames,
  formatCompactDate,
  formatNumber,
  formatPercent,
  formatSignedNumber,
} from "../../../lib/format";

export const dynamic = "force-dynamic";

export async function generateMetadata({ params }) {
  const { storeId } = await params;

  try {
    const store = await getStoreIdentity(storeId);
    return {
      title: store ? `${store.storeName}の機種一覧` : "機種一覧",
    };
  } catch {
    return {
      title: "機種一覧",
    };
  }
}

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

  const { store, machines } = storeDetail;

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
          <h1 className="pageTitle pageTitleCompact">{store.storeName}</h1>
          {store.storeUrl ? (
            <a href={store.storeUrl} target="_blank" rel="noreferrer" className="externalLink">
              店舗ページを開く
            </a>
          ) : null}
        </div>
      </section>

      {machines.length === 0 ? (
        <section className="statusPanel">
          <h2>この店舗には保存済みの機種データがありません</h2>
          <p>GUIアプリ側でこの店舗の台データを取得すると、ここに機種一覧が並びます。</p>
        </section>
      ) : (
        <section className="tablePanel directoryPanel">
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
