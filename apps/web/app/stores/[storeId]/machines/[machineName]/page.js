import Link from "next/link";
import { notFound } from "next/navigation";

import { Breadcrumbs } from "../../../../../components/breadcrumbs";
import { MachineComparison } from "../../../../../components/machine-comparison";
import {
  getMachineDetail,
  getStoreIdentity,
  readRouteSegment,
} from "../../../../../lib/data";
import { parseEventDisplayMode, parseEventFilters } from "../../../../../lib/event-filters";
import { getSettingEstimateDefinition } from "../../../../../lib/setting-estimates";

export const dynamic = "force-dynamic";

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
  const eventFilters = parseEventFilters(resolvedSearchParams);
  const eventDisplayMode = parseEventDisplayMode(resolvedSearchParams);
  const hasEventFilterSearchParams =
    hasSearchParamValue(resolvedSearchParams, "dayTail") ||
    hasSearchParamValue(resolvedSearchParams, "zoro") ||
    hasSearchParamValue(resolvedSearchParams, "weekday");
  let detail;

  try {
    detail = await getMachineDetail(storeId, machineName);
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

  const settingEstimateDefinition = getSettingEstimateDefinition(machineName);
  const initialEventFilters = hasEventFilterSearchParams
    ? eventFilters
    : detail.store.eventFilters;

  return (
    <main className="pageStack">
      <Breadcrumbs
        items={[
          { label: "店舗一覧", href: "/" },
          { label: detail.store.storeName, href: `/stores/${detail.store.id}` },
          { label: machineName },
        ]}
      />

      <section className="heroPanel">
        <div className="heroCopy">
          <h1 className="pageTitle pageTitleCompact">{machineName}</h1>
          <p className="machineStoreName">{detail.store.storeName}</p>
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
        machineName={machineName}
        slotNumbers={detail.slotNumbers}
        dateRows={detail.dateRows}
        initialEventFilters={initialEventFilters}
        initialEventDisplayMode={eventDisplayMode}
      />
    </main>
  );
}
