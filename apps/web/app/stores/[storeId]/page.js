import Link from "next/link";
import { cookies } from "next/headers";
import { notFound } from "next/navigation";

import { Breadcrumbs } from "../../../components/breadcrumbs";
import { HuntScoreLogicSelector } from "../../../components/hunt-score-logic-selector";
import { getStoreDetail, getStoreIdentity } from "../../../lib/data";
import {
  getHuntScoreLogicDetail,
  isHuntScoreTargetStore,
  listHuntScoreLogicOptions,
} from "../../../lib/hunt-score";
import {
  decodeHuntScoreLogicCookieValue,
  getHuntScoreLogicCookieName,
} from "../../../lib/hunt-score-logic-selection";
import {
  formatAverageGames,
  formatCompactDate,
  formatNumber,
  formatPercent,
  formatSignedNumber,
} from "../../../lib/format";

export const dynamic = "force-dynamic";

async function readStoredHuntScoreLogicKey(storeId) {
  const cookieStore = await cookies();
  return decodeHuntScoreLogicCookieValue(
    cookieStore.get(getHuntScoreLogicCookieName(storeId))?.value ?? "",
  );
}

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
  const hasHuntScoreAnalysis = isHuntScoreTargetStore(store.storeName);
  const huntScoreLogic = hasHuntScoreAnalysis
    ? getHuntScoreLogicDetail(await readStoredHuntScoreLogicKey(store.id), store.storeName)
    : null;

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
          <div className="heroLinks simpleHeroLinks">
            {hasHuntScoreAnalysis ? (
              <>
                <Link href={`/stores/${store.id}/hunt-analysis`} className="externalLink">
                  狙い度ランキングを見る
                </Link>
                <Link href={`/stores/${store.id}/hunt-backtest`} className="externalLink">
                  バックテストを見る
                </Link>
              </>
            ) : null}
            {store.storeUrl ? (
              <a href={store.storeUrl} target="_blank" rel="noreferrer" className="externalLink">
                店舗ページを開く
              </a>
            ) : null}
          </div>
          {hasHuntScoreAnalysis ? (
            <HuntScoreLogicSelector
              storeId={store.id}
              selectedLogicKey={huntScoreLogic.key}
              options={listHuntScoreLogicOptions()}
            />
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
                    <tr
                      key={`${machine.machineName}-${machine.isCombinedMachineGroup ? "group" : "machine"}`}
                      className={
                        machine.isCombinedMachineGroup
                          ? "combinedMachineGroupRow"
                          : machine.isCombinedMachineChild
                            ? "combinedMachineChildRow"
                            : undefined
                      }
                    >
                      <th
                        className={`directoryNameCell ${
                          machine.isCombinedMachineChild ? "directoryNameCellIndented" : ""
                        }`}
                      >
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
