import Link from "next/link";
import { cookies } from "next/headers";
import { notFound } from "next/navigation";

import { Breadcrumbs } from "../../../../../components/breadcrumbs";
import { DataSourceLabel } from "../../../../../components/data-source-label";
import { MachineComparison } from "../../../../../components/machine-comparison";
import {
  getMachineDetail,
  getStoreIdentity,
  readRouteSegment,
} from "../../../../../lib/data";
import { parseEventFilters } from "../../../../../lib/event-filters";
import {
  decodeHuntScoreLogicCookieValue,
  getHuntScoreLogicCookieName,
} from "../../../../../lib/hunt-score-logic-selection";
import { getSettingEstimateDefinition } from "../../../../../lib/setting-estimates";

export const dynamic = "force-dynamic";

async function readStoredHuntScoreLogicKey(storeId) {
  const cookieStore = await cookies();
  return decodeHuntScoreLogicCookieValue(
    cookieStore.get(getHuntScoreLogicCookieName(storeId))?.value ?? "",
  );
}

function hasSearchParamValue(searchParams, key) {
  const value = searchParams?.[key];
  return Array.isArray(value) ? value.length > 0 : value !== undefined;
}

export async function generateMetadata({ params }) {
  const resolvedParams = await params;
  const storeId = resolvedParams.storeId;
  const machineName = readRouteSegment(resolvedParams.machineName);

  try {
    const store = await getStoreIdentity(storeId);
    return {
      title: store ? `${machineName}（${store.storeName}）` : machineName || "台データ",
    };
  } catch {
    return {
      title: machineName || "台データ",
    };
  }
}

export default async function MachineDetailPage({ params, searchParams }) {
  const resolvedParams = await params;
  const resolvedSearchParams = await searchParams;
  const storeId = resolvedParams.storeId;
  const machineName = readRouteSegment(resolvedParams.machineName);
  const huntScoreLogicKey = await readStoredHuntScoreLogicKey(storeId);
  const eventFilters = parseEventFilters(resolvedSearchParams);
  const hasEventFilterSearchParams =
    hasSearchParamValue(resolvedSearchParams, "dayTail") ||
    hasSearchParamValue(resolvedSearchParams, "zoro") ||
    hasSearchParamValue(resolvedSearchParams, "weekday");
  let detail;

  try {
    detail = await getMachineDetail(storeId, machineName, huntScoreLogicKey);
  } catch (error) {
    return (
      <main className="pageStack">
        <Breadcrumbs
          items={[
            { label: "店舗一覧", href: "/" },
            { label: "機種一覧", href: `/stores/${storeId}` },
            { label: machineName },
          ]}
        />
        <section className="statusPanel">
          <h2>台データを読み込めませんでした</h2>
          <p>{error instanceof Error ? error.message : "設定を確認してください。"}</p>
        </section>
      </main>
    );
  }

  if (!detail) {
    notFound();
  }

  const displayMachineName = detail.machineName ?? machineName;
  const settingEstimateDefinition = getSettingEstimateDefinition(displayMachineName);
  const initialEventFilters = hasEventFilterSearchParams
    ? eventFilters
    : detail.store.eventFilters;

  return (
    <main className="pageStack">
      <Breadcrumbs
        items={[
          { label: "店舗一覧", href: "/" },
          { label: detail.store.storeName, href: `/stores/${detail.store.id}` },
          { label: displayMachineName },
        ]}
      />

      <section className="heroPanel">
        <div className="heroCopy">
          <h1 className="pageTitle pageTitleCompact">{displayMachineName}</h1>
          <p className="machineStoreName">{detail.store.storeName}</p>
          {detail.huntScoreLogic ? (
            <p className="dataSourceLabel">適用中: {detail.huntScoreLogic.name}</p>
          ) : null}
          <DataSourceLabel source={detail.dataSource} />
          <div className="heroLinks simpleHeroLinks">
            <Link href={`/stores/${detail.store.id}`} className="externalLink">
              機種一覧へ戻る
            </Link>
            {detail.store.storeUrl ? (
              <a href={detail.store.storeUrl} target="_blank" rel="noreferrer" className="externalLink">
                店舗ページを開く
              </a>
            ) : null}
          </div>
        </div>
      </section>

      {settingEstimateDefinition ? (
        <details className="tablePanel specDetailsPanel">
          <summary className="specDetailsSummary">
            {settingEstimateDefinition.displayName} 確率
          </summary>
          <div className="tableScroller directoryScroller">
            <table className="directoryTable neoSpecTable">
              <thead>
                <tr>
                  <th>設定</th>
                  <th>BIG確率</th>
                  <th>REG確率</th>
                  <th>合成確率</th>
                </tr>
              </thead>
              <tbody>
                {settingEstimateDefinition.rateTable.map((row) => (
                  <tr key={row.setting}>
                    <th scope="row">{row.setting}</th>
                    <td>{row.bb}</td>
                    <td>{row.rb}</td>
                    <td>{row.combined}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </details>
      ) : null}

      <MachineComparison
        storeId={detail.store.id}
        machineName={displayMachineName}
        slotNumbers={detail.slotNumbers}
        slotLabels={detail.slotLabels}
        dateRows={detail.dateRows}
        initialEventFilters={initialEventFilters}
        initialEventFiltersFromSearchParams={hasEventFilterSearchParams}
        huntScoreHighlight={detail.huntScoreHighlight}
      />
    </main>
  );
}
