import Link from "next/link";
import { cookies } from "next/headers";
import { notFound } from "next/navigation";

import { Breadcrumbs } from "../../../../../../components/breadcrumbs";
import { HuntScoreLogicSelector } from "../../../../../../components/hunt-score-logic-selector";
import { MachineComparison } from "../../../../../../components/machine-comparison";
import { StoreFavoriteButton } from "../../../../../../components/store-favorite-button";
import {
  getMachineDetail,
  getStoreIdentity,
  readRouteSegment,
} from "../../../../../../lib/data";
import { parseEventFilters } from "../../../../../../lib/event-filters";
import { listHuntScoreLogicOptions } from "../../../../../../lib/hunt-score";
import {
  decodeHuntScoreLogicCookieValue,
  getHuntScoreLogicCookieName,
} from "../../../../../../lib/hunt-score-logic-selection";
import { normalizeDifferenceMode } from "../../../../../../lib/machine-difference";
import { normalizeSettingEstimateMode } from "../../../../../../lib/setting-estimates";

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

function readSingleSearchParam(value) {
  return Array.isArray(value) ? value[0] ?? "" : value ?? "";
}

export async function generateMetadata({ params }) {
  const resolvedParams = await params;
  const storeId = resolvedParams.storeId;
  const machineName = readRouteSegment(resolvedParams.machineName);

  try {
    const store = await getStoreIdentity(storeId);
    return {
      title: store ? `${machineName}のロジック検証（${store.storeName}）` : `${machineName}のロジック検証`,
    };
  } catch {
    return {
      title: `${machineName}のロジック検証`,
    };
  }
}

export default async function HuntScoreVerificationPage({ params, searchParams }) {
  const resolvedParams = await params;
  const resolvedSearchParams = await searchParams;
  const storeId = resolvedParams.storeId;
  const machineName = readRouteSegment(resolvedParams.machineName);
  const huntScoreLogicKey = await readStoredHuntScoreLogicKey(storeId);
  const differenceMode = normalizeDifferenceMode(
    readSingleSearchParam(resolvedSearchParams?.differenceMode),
  );
  const displayDifferenceMode = normalizeDifferenceMode(
    readSingleSearchParam(resolvedSearchParams?.displayDifferenceMode),
  );
  const settingEstimateMode = normalizeSettingEstimateMode(
    readSingleSearchParam(resolvedSearchParams?.settingEstimateMode),
  );
  const hasDisplayDifferenceModeSearchParam = hasSearchParamValue(
    resolvedSearchParams,
    "displayDifferenceMode",
  );
  const hasHuntScoreDifferenceModeSearchParam = hasSearchParamValue(
    resolvedSearchParams,
    "differenceMode",
  );
  const hasSettingEstimateModeSearchParam = hasSearchParamValue(
    resolvedSearchParams,
    "settingEstimateMode",
  );
  const eventFilters = parseEventFilters(resolvedSearchParams);
  const hasEventFilterSearchParams =
    hasSearchParamValue(resolvedSearchParams, "dayTail") ||
    hasSearchParamValue(resolvedSearchParams, "zoro") ||
    hasSearchParamValue(resolvedSearchParams, "weekday") ||
    hasSearchParamValue(resolvedSearchParams, "monthDay");
  let detail;

  try {
    detail = await getMachineDetail(
      storeId,
      machineName,
      huntScoreLogicKey,
      differenceMode,
      settingEstimateMode,
    );
  } catch (error) {
    return (
      <main className="pageStack">
        <Breadcrumbs
          items={[
            { label: "店舗一覧", href: "/" },
            { label: "機種一覧", href: `/stores/${storeId}` },
            { label: "ロジック検証" },
          ]}
        />
        <section className="statusPanel">
          <h2>ロジック検証を読み込めませんでした</h2>
          <p>{error instanceof Error ? error.message : "設定を確認してください。"}</p>
        </section>
      </main>
    );
  }

  if (!detail) {
    notFound();
  }

  const displayMachineName = detail.machineName ?? machineName;
  const initialEventFilters = hasEventFilterSearchParams
    ? eventFilters
    : detail.store.eventFilters;

  return (
    <main className="pageStack">
      <Breadcrumbs
        items={[
          { label: "店舗一覧", href: "/" },
          { label: detail.store.storeName, href: `/stores/${detail.store.id}` },
          { label: displayMachineName, href: `/stores/${detail.store.id}/machines/${encodeURIComponent(displayMachineName)}` },
          { label: "ロジック検証" },
        ]}
      />

      <section className="heroPanel">
        <div className="heroCopy">
          <h1 className="pageTitle pageTitleCompact">ロジック検証</h1>
          <div className="storeContextLine">
            <StoreFavoriteButton
              store={{ id: detail.store.id, storeName: detail.store.storeName }}
              compact
            />
            <Link href={`/stores/${detail.store.id}`} className="storeContextLink">
              {detail.store.storeName}
            </Link>
          </div>
          <p className="dataSourceLabel">
            {displayMachineName}
            {detail.huntScoreLogic ? ` / 適用中: ${detail.huntScoreLogic.name}` : ""}
          </p>
          <div className="heroLinks simpleHeroLinks">
            <Link href={`/stores/${detail.store.id}/machines/${encodeURIComponent(displayMachineName)}`} className="externalLink">
              通常表示へ戻る
            </Link>
            <Link href={`/stores/${detail.store.id}`} className="externalLink">
              機種一覧へ戻る
            </Link>
            {detail.store.storeUrl ? (
              <a href={detail.store.storeUrl} target="_blank" rel="noreferrer" className="externalLink">
                店舗ページを開く
              </a>
            ) : null}
          </div>
          {detail.huntScoreLogic ? (
            <HuntScoreLogicSelector
              storeId={detail.store.id}
              selectedLogicKey={detail.huntScoreLogic.key}
              options={listHuntScoreLogicOptions()}
            />
          ) : null}
        </div>
      </section>

      <MachineComparison
        storeId={detail.store.id}
        storeName={detail.store.storeName}
        machineName={displayMachineName}
        slotNumbers={detail.slotNumbers}
        slotLabels={detail.slotLabels}
        dateRows={detail.dateRows}
        initialEventFilters={initialEventFilters}
        initialEventFiltersFromSearchParams={hasEventFilterSearchParams}
        huntScoreHighlight={detail.huntScoreHighlight}
        fullHuntScoreHighlightUrl={`/api/stores/${detail.store.id}/machines/${encodeURIComponent(displayMachineName)}/hunt-score-highlight`}
        initialDifferenceMode={detail.differenceMode}
        initialHuntScoreDifferenceModeFromSearchParams={hasHuntScoreDifferenceModeSearchParam}
        initialDisplayDifferenceMode={displayDifferenceMode}
        initialDisplayDifferenceModeFromSearchParams={hasDisplayDifferenceModeSearchParam}
        initialSettingEstimateMode={detail.settingEstimateMode}
        initialSettingEstimateModeFromSearchParams={hasSettingEstimateModeSearchParam}
        preferDefaultEstimateOptions={Boolean(detail.huntScoreHighlight)}
        verificationMode
        huntScoreWindowDays={detail.huntScoreLogic?.historyWindowDays ?? detail.huntScoreLogic?.windowDays ?? 7}
      />
    </main>
  );
}
