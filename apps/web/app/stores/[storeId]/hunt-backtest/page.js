import Link from "next/link";
import { Fragment } from "react";
import { cookies } from "next/headers";
import { notFound } from "next/navigation";

import { ExpandableTableRowsController } from "../../../../components/expandable-table-rows-controller";
import { HuntBacktestBookmarkControl } from "../../../../components/hunt-backtest-bookmark-control";
import { HuntBacktestFormStateSync } from "../../../../components/hunt-backtest-form-state-sync";
import { Breadcrumbs } from "../../../../components/breadcrumbs";
import { HuntBacktestGraph } from "../../../../components/hunt-backtest-graph";
import {
  AllMachineFilterButtons,
  MachineFilterCategoryButton,
} from "../../../../components/hunt-machine-filter-tools";
import { HuntScoreLogicSelector } from "../../../../components/hunt-score-logic-selector";
import { NativeGetForm } from "../../../../components/native-get-form";
import { SortableTableController } from "../../../../components/sortable-table-controller";
import { SortableTableHeader } from "../../../../components/sortable-table-header";
import { StoreFavoriteButton } from "../../../../components/store-favorite-button";
import {
  getHuntScoreAnalysisPageDetail,
  getHuntScoreInitialPageDetail,
  getStoreIdentity,
} from "../../../../lib/data";
import {
  formatDecimal,
  formatAverageGames,
  formatNumber,
  formatPeriod,
  formatPercent,
  formatSignedNumber,
} from "../../../../lib/format";
import {
  getHuntMachineShortName,
  groupHuntMachineOptions,
} from "../../../../lib/hunt-machine-display";
import {
  decodeHuntScoreLogicCookieValue,
  getHuntScoreLogicCookieName,
} from "../../../../lib/hunt-score-logic-selection";
import { listHuntScoreLogicOptions } from "../../../../lib/hunt-score";
import {
  formatSettingEstimateScore,
  getSettingEstimateHighlightClass,
} from "../../../../lib/setting-estimates";

export const dynamic = "force-dynamic";
const DAY_TAIL_OPTIONS = Array.from({ length: 10 }, (_, index) => index);
const MONTH_DAY_OPTIONS = Array.from({ length: 31 }, (_, index) => index + 1);
const HUNT_BACKTEST_FORM_ID = "hunt-backtest-condition-form";
const WEEKDAY_OPTIONS = [
  { value: 1, label: "月曜" },
  { value: 2, label: "火曜" },
  { value: 3, label: "水曜" },
  { value: 4, label: "木曜" },
  { value: 5, label: "金曜" },
  { value: 6, label: "土曜" },
  { value: 0, label: "日曜" },
];

async function readStoredHuntScoreLogicKey(storeId) {
  const cookieStore = await cookies();
  return decodeHuntScoreLogicCookieValue(
    cookieStore.get(getHuntScoreLogicCookieName(storeId))?.value ?? "",
  );
}

function readSingleSearchParam(value) {
  if (Array.isArray(value)) {
    return typeof value[0] === "string" ? value[0] : "";
  }
  return typeof value === "string" ? value : "";
}

function readMultiSearchParam(value) {
  if (Array.isArray(value)) {
    return value.filter((entry) => typeof entry === "string");
  }
  return typeof value === "string" ? [value] : [];
}

function readProbabilitySortValue(value) {
  const match = /^1\/(\d+(?:\.\d+)?)$/u.exec(String(value ?? "").trim());
  return match ? Number(match[1]) : "";
}

function readSortNumber(value) {
  return Number.isFinite(value) ? value : "";
}

function BacktestMetricCell({ sortValue, children }) {
  return <td data-sort-value={sortValue}>{children}</td>;
}

function hasNonmatchingSummary(summary) {
  return Number(summary?.actualRowCount ?? 0) > 0;
}

function BacktestNonmatchingSummaryRow({ parentKey, summary, label }) {
  if (!hasNonmatchingSummary(summary)) {
    return null;
  }

  return (
    <tr
      className="backtestNonmatchingRow"
      data-expand-detail-row="1"
      data-expand-parent-key={parentKey}
      hidden
    >
      <th className="directoryNameCell" data-sort-value={label}>{label}</th>
      <td data-sort-value="">-</td>
      <BacktestMetricCell sortValue={readSortNumber(summary.averageHuntScore)}>{formatDecimal(summary.averageHuntScore)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.averageUpperGap)}>{formatDecimal(summary.averageUpperGap)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.averageNextGap)}>{formatDecimal(summary.averageNextGap)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={summary.actualRowCount}>{formatNumber(summary.actualRowCount)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.winRate)}>{formatPercent(summary.winRate)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={summary.differenceTotal}>{formatSignedNumber(summary.differenceTotal)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={summary.gamesTotal}>{formatNumber(summary.gamesTotal)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.averageGames)}>{formatAverageGames(summary.averageGames)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={summary.bbTotal}>{formatNumber(summary.bbTotal)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={summary.rbTotal}>{formatNumber(summary.rbTotal)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readProbabilitySortValue(summary.bbProbability)}>{summary.bbProbability ?? "-"}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readProbabilitySortValue(summary.rbProbability)}>{summary.rbProbability ?? "-"}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readProbabilitySortValue(summary.combinedProbability)}>{summary.combinedProbability ?? "-"}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.payoutRate)}>{formatPercent(summary.payoutRate)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.averageSetting)}>{formatSettingEstimateScore(summary.averageSetting)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.setting35PlusRate)}>{formatPercent(summary.setting35PlusRate)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.setting4PlusRate)}>{formatPercent(summary.setting4PlusRate)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.setting45PlusRate)}>{formatPercent(summary.setting45PlusRate)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.setting5PlusRate)}>{formatPercent(summary.setting5PlusRate)}</BacktestMetricCell>
    </tr>
  );
}

function BacktestResultTable({ title, backtest, tableId, storeId }) {
  const totalNonmatchingSummary = backtest.total.nonmatchingSummary;
  const totalRowKey = `${tableId}:total`;

  return (
    <section className="tablePanel directoryPanel">
      <SortableTableController tableId={tableId} />
      <ExpandableTableRowsController tableId={tableId} />
      <div className="tablePanelHeader">
        <div>
          <p className="tablePanelTitle">{title}</p>
        </div>
      </div>
      <div className="tableScroller directoryScroller">
        <table id={tableId} className="directoryTable huntCompactTable huntBacktestResultTable" data-sortable-table="1">
          <thead>
            <tr>
              <SortableTableHeader
                columnIndex={0}
                type="text"
                initialDirection="asc"
                className="directoryNameHeader"
              >
                機種名
              </SortableTableHeader>
              <SortableTableHeader columnIndex={1}>設置台数</SortableTableHeader>
              <SortableTableHeader columnIndex={2}>狙い度</SortableTableHeader>
              <SortableTableHeader columnIndex={3}>上位境界差</SortableTableHeader>
              <SortableTableHeader columnIndex={4}>下位境界差</SortableTableHeader>
              <SortableTableHeader columnIndex={5}>集計台数</SortableTableHeader>
              <SortableTableHeader columnIndex={6}>勝率</SortableTableHeader>
              <SortableTableHeader columnIndex={7}>合計差枚</SortableTableHeader>
              <SortableTableHeader columnIndex={8}>合計G数</SortableTableHeader>
              <SortableTableHeader columnIndex={9}>平均G数</SortableTableHeader>
              <SortableTableHeader columnIndex={10}>BB</SortableTableHeader>
              <SortableTableHeader columnIndex={11}>RB</SortableTableHeader>
              <SortableTableHeader columnIndex={12} initialDirection="asc">
                BB率
              </SortableTableHeader>
              <SortableTableHeader columnIndex={13} initialDirection="asc">
                RB率
              </SortableTableHeader>
              <SortableTableHeader columnIndex={14} initialDirection="asc">
                合成
              </SortableTableHeader>
              <SortableTableHeader columnIndex={15}>機械割</SortableTableHeader>
              <SortableTableHeader columnIndex={16}>平均設定</SortableTableHeader>
              <SortableTableHeader columnIndex={17}>推定3.5+</SortableTableHeader>
              <SortableTableHeader columnIndex={18}>推定4.0+</SortableTableHeader>
              <SortableTableHeader columnIndex={19}>推定4.5+</SortableTableHeader>
              <SortableTableHeader columnIndex={20}>推定5.0+</SortableTableHeader>
            </tr>
          </thead>
          <tbody>
            <tr className="backtestTotalRow" data-sort-fixed="1" data-expand-row-key={totalRowKey}>
              <th className="directoryNameCell" data-sort-value="総計">
                {hasNonmatchingSummary(totalNonmatchingSummary) ? (
                  <span className="backtestExpandIndicator" aria-hidden="true">＋</span>
                ) : null}
                総計
              </th>
              <td data-sort-value={readSortNumber(backtest.total.slotCount)}>{formatNumber(backtest.total.slotCount)}</td>
              <BacktestMetricCell sortValue={readSortNumber(backtest.total.averageHuntScore)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="狙い度" nonmatchingValue={formatDecimal(totalNonmatchingSummary?.averageHuntScore)}>{formatDecimal(backtest.total.averageHuntScore)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readSortNumber(backtest.total.averageUpperGap)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="上位境界差" nonmatchingValue={formatDecimal(totalNonmatchingSummary?.averageUpperGap)}>{formatDecimal(backtest.total.averageUpperGap)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readSortNumber(backtest.total.averageNextGap)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="下位境界差" nonmatchingValue={formatDecimal(totalNonmatchingSummary?.averageNextGap)}>{formatDecimal(backtest.total.averageNextGap)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={backtest.total.actualRowCount} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="集計台数" nonmatchingValue={formatNumber(totalNonmatchingSummary?.actualRowCount)}>{formatNumber(backtest.total.actualRowCount)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readSortNumber(backtest.total.winRate)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="勝率" nonmatchingValue={formatPercent(totalNonmatchingSummary?.winRate)}>{formatPercent(backtest.total.winRate)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={backtest.total.differenceTotal} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="合計差枚" nonmatchingValue={formatSignedNumber(totalNonmatchingSummary?.differenceTotal)}>{formatSignedNumber(backtest.total.differenceTotal)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={backtest.total.gamesTotal} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="合計G数" nonmatchingValue={formatNumber(totalNonmatchingSummary?.gamesTotal)}>{formatNumber(backtest.total.gamesTotal)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readSortNumber(backtest.total.averageGames)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="平均G数" nonmatchingValue={formatAverageGames(totalNonmatchingSummary?.averageGames)}>{formatAverageGames(backtest.total.averageGames)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={backtest.total.bbTotal} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="BB" nonmatchingValue={formatNumber(totalNonmatchingSummary?.bbTotal)}>{formatNumber(backtest.total.bbTotal)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={backtest.total.rbTotal} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="RB" nonmatchingValue={formatNumber(totalNonmatchingSummary?.rbTotal)}>{formatNumber(backtest.total.rbTotal)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readProbabilitySortValue(backtest.total.bbProbability)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="BB率" nonmatchingValue={totalNonmatchingSummary?.bbProbability ?? "-"}>{backtest.total.bbProbability ?? "-"}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readProbabilitySortValue(backtest.total.rbProbability)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="RB率" nonmatchingValue={totalNonmatchingSummary?.rbProbability ?? "-"}>{backtest.total.rbProbability ?? "-"}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readProbabilitySortValue(backtest.total.combinedProbability)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="合成" nonmatchingValue={totalNonmatchingSummary?.combinedProbability ?? "-"}>{backtest.total.combinedProbability ?? "-"}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readSortNumber(backtest.total.payoutRate)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="機械割" nonmatchingValue={formatPercent(totalNonmatchingSummary?.payoutRate)}>{formatPercent(backtest.total.payoutRate)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readSortNumber(backtest.total.averageSetting)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="平均設定" nonmatchingValue={formatSettingEstimateScore(totalNonmatchingSummary?.averageSetting)}>{formatSettingEstimateScore(backtest.total.averageSetting)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readSortNumber(backtest.total.setting35PlusRate)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="推定3.5+" nonmatchingValue={formatPercent(totalNonmatchingSummary?.setting35PlusRate)}>{formatPercent(backtest.total.setting35PlusRate)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readSortNumber(backtest.total.setting4PlusRate)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="推定4.0+" nonmatchingValue={formatPercent(totalNonmatchingSummary?.setting4PlusRate)}>{formatPercent(backtest.total.setting4PlusRate)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readSortNumber(backtest.total.setting45PlusRate)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="推定4.5+" nonmatchingValue={formatPercent(totalNonmatchingSummary?.setting45PlusRate)}>{formatPercent(backtest.total.setting45PlusRate)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readSortNumber(backtest.total.setting5PlusRate)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="推定5.0+" nonmatchingValue={formatPercent(totalNonmatchingSummary?.setting5PlusRate)}>{formatPercent(backtest.total.setting5PlusRate)}</BacktestMetricCell>
            </tr>
            <BacktestNonmatchingSummaryRow
              parentKey={totalRowKey}
              summary={totalNonmatchingSummary}
              label="非該当台 合計"
            />
            {backtest.summaries.map((summary) => {
              const shortMachineName = getHuntMachineShortName(summary.machineName);
              const nonmatchingSummary = summary.nonmatchingSummary;
              const rowKey = `${tableId}:machine:${summary.machineName}`;
              return (
                <Fragment key={summary.machineName}>
                <tr
                  className={getSettingEstimateHighlightClass(summary.averageSetting)}
                  data-expand-row-key={rowKey}
                >
                  <th
                    className="directoryNameCell"
                    data-sort-value={summary.machineName}
                    title={summary.machineName}
                  >
                    {hasNonmatchingSummary(nonmatchingSummary) ? (
                      <span className="backtestExpandIndicator" aria-hidden="true">＋</span>
                    ) : null}
                    <Link
                      href={`/stores/${storeId}/machines/${encodeURIComponent(summary.machineName)}`}
                      className="directoryPrimaryLink"
                    >
                      {shortMachineName}
                    </Link>
                  </th>
                  <td data-sort-value={readSortNumber(summary.slotCount)}>{formatNumber(summary.slotCount)}</td>
                  <BacktestMetricCell sortValue={readSortNumber(summary.averageHuntScore)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="狙い度" nonmatchingValue={formatDecimal(nonmatchingSummary?.averageHuntScore)}>{formatDecimal(summary.averageHuntScore)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readSortNumber(summary.averageUpperGap)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="上位境界差" nonmatchingValue={formatDecimal(nonmatchingSummary?.averageUpperGap)}>{formatDecimal(summary.averageUpperGap)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readSortNumber(summary.averageNextGap)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="下位境界差" nonmatchingValue={formatDecimal(nonmatchingSummary?.averageNextGap)}>{formatDecimal(summary.averageNextGap)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={summary.actualRowCount} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="集計台数" nonmatchingValue={formatNumber(nonmatchingSummary?.actualRowCount)}>{formatNumber(summary.actualRowCount)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readSortNumber(summary.winRate)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="勝率" nonmatchingValue={formatPercent(nonmatchingSummary?.winRate)}>{formatPercent(summary.winRate)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={summary.differenceTotal} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="合計差枚" nonmatchingValue={formatSignedNumber(nonmatchingSummary?.differenceTotal)}>{formatSignedNumber(summary.differenceTotal)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={summary.gamesTotal} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="合計G数" nonmatchingValue={formatNumber(nonmatchingSummary?.gamesTotal)}>{formatNumber(summary.gamesTotal)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readSortNumber(summary.averageGames)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="平均G数" nonmatchingValue={formatAverageGames(nonmatchingSummary?.averageGames)}>{formatAverageGames(summary.averageGames)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={summary.bbTotal} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="BB" nonmatchingValue={formatNumber(nonmatchingSummary?.bbTotal)}>{formatNumber(summary.bbTotal)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={summary.rbTotal} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="RB" nonmatchingValue={formatNumber(nonmatchingSummary?.rbTotal)}>{formatNumber(summary.rbTotal)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readProbabilitySortValue(summary.bbProbability)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="BB率" nonmatchingValue={nonmatchingSummary?.bbProbability ?? "-"}>{summary.bbProbability ?? "-"}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readProbabilitySortValue(summary.rbProbability)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="RB率" nonmatchingValue={nonmatchingSummary?.rbProbability ?? "-"}>{summary.rbProbability ?? "-"}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readProbabilitySortValue(summary.combinedProbability)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="合成" nonmatchingValue={nonmatchingSummary?.combinedProbability ?? "-"}>{summary.combinedProbability ?? "-"}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readSortNumber(summary.payoutRate)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="機械割" nonmatchingValue={formatPercent(nonmatchingSummary?.payoutRate)}>{formatPercent(summary.payoutRate)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readSortNumber(summary.averageSetting)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="平均設定" nonmatchingValue={formatSettingEstimateScore(nonmatchingSummary?.averageSetting)}>{formatSettingEstimateScore(summary.averageSetting)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readSortNumber(summary.setting35PlusRate)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="推定3.5+" nonmatchingValue={formatPercent(nonmatchingSummary?.setting35PlusRate)}>{formatPercent(summary.setting35PlusRate)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readSortNumber(summary.setting4PlusRate)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="推定4.0+" nonmatchingValue={formatPercent(nonmatchingSummary?.setting4PlusRate)}>{formatPercent(summary.setting4PlusRate)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readSortNumber(summary.setting45PlusRate)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="推定4.5+" nonmatchingValue={formatPercent(nonmatchingSummary?.setting45PlusRate)}>{formatPercent(summary.setting45PlusRate)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readSortNumber(summary.setting5PlusRate)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="推定5.0+" nonmatchingValue={formatPercent(nonmatchingSummary?.setting5PlusRate)}>{formatPercent(summary.setting5PlusRate)}</BacktestMetricCell>
                </tr>
                <BacktestNonmatchingSummaryRow
                  parentKey={rowKey}
                  summary={nonmatchingSummary}
                  label="非該当台"
                />
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export async function generateMetadata({ params }) {
  const resolvedParams = await params;
  const storeId = resolvedParams.storeId;

  try {
    const store = await getStoreIdentity(storeId);
    return {
      title: store ? `${store.storeName}のバックテスト` : "バックテスト",
    };
  } catch {
    return {
      title: "バックテスト",
    };
  }
}

export default async function HuntBacktestPage({ params, searchParams }) {
  const resolvedParams = await params;
  const resolvedSearchParams = await searchParams;
  const storeId = resolvedParams.storeId;
  const resultRequested = readSingleSearchParam(resolvedSearchParams?.show) === "1";
  const huntScoreLogicKey = await readStoredHuntScoreLogicKey(storeId);
  const requestedBacktestOptions = {
    periodMode: readSingleSearchParam(resolvedSearchParams?.periodMode),
    recentDays: readSingleSearchParam(resolvedSearchParams?.recentDays),
    startDate: readSingleSearchParam(resolvedSearchParams?.startDate),
    endDate: readSingleSearchParam(resolvedSearchParams?.endDate),
    machineNames: readMultiSearchParam(resolvedSearchParams?.machine),
    machineTouched: readSingleSearchParam(resolvedSearchParams?.machineTouched),
    combineAimJuggler: readMultiSearchParam(resolvedSearchParams?.aimMachineGroup),
    combineHanabi: readMultiSearchParam(resolvedSearchParams?.hanabiMachineGroup),
    scoreDifferenceMode: readSingleSearchParam(resolvedSearchParams?.scoreDifferenceMode),
    differenceMode: readSingleSearchParam(resolvedSearchParams?.differenceMode),
    rankMin: readSingleSearchParam(resolvedSearchParams?.rankMin),
    rankMax: readSingleSearchParam(resolvedSearchParams?.rankMax),
    rankScope: readSingleSearchParam(resolvedSearchParams?.rankScope),
    machineRankMin: readSingleSearchParam(resolvedSearchParams?.machineRankMin),
    machineRankMax: readSingleSearchParam(resolvedSearchParams?.machineRankMax),
    selectedRankMin: readSingleSearchParam(resolvedSearchParams?.selectedRankMin),
    selectedRankMax: readSingleSearchParam(resolvedSearchParams?.selectedRankMax),
    scoreMin: readSingleSearchParam(resolvedSearchParams?.scoreMin),
    scoreMax: readSingleSearchParam(resolvedSearchParams?.scoreMax),
    nextGapScope: readSingleSearchParam(resolvedSearchParams?.nextGapScope),
    nextGapMin: readSingleSearchParam(resolvedSearchParams?.nextGapMin),
    nextGapMax: readSingleSearchParam(resolvedSearchParams?.nextGapMax),
    upperGapMin: readSingleSearchParam(resolvedSearchParams?.upperGapMin),
    upperGapMax: readSingleSearchParam(resolvedSearchParams?.upperGapMax),
    rankRequired: readMultiSearchParam(resolvedSearchParams?.rankRequired),
    machineRankRequired: readMultiSearchParam(resolvedSearchParams?.machineRankRequired),
    selectedRankRequired: readMultiSearchParam(resolvedSearchParams?.selectedRankRequired),
    scoreRequired: readMultiSearchParam(resolvedSearchParams?.scoreRequired),
    nextGapRequired: readMultiSearchParam(resolvedSearchParams?.nextGapRequired),
    upperGapRequired: readMultiSearchParam(resolvedSearchParams?.upperGapRequired),
    dailySelectionMode: readMultiSearchParam(resolvedSearchParams?.dailySelectionMode),
    showGraph: readSingleSearchParam(resolvedSearchParams?.showGraph),
    eventTouched: readSingleSearchParam(resolvedSearchParams?.backtestEventTouched) === "1",
    dayTails: readMultiSearchParam(resolvedSearchParams?.backtestDayTail),
    zoro: readSingleSearchParam(resolvedSearchParams?.backtestZoro) === "1",
    weekdays: readMultiSearchParam(resolvedSearchParams?.backtestWeekday),
    monthDays: readMultiSearchParam(resolvedSearchParams?.backtestMonthDay),
  };

  let detail;

  try {
    detail = resultRequested
      ? await getHuntScoreAnalysisPageDetail(
          storeId,
          "",
          20,
          requestedBacktestOptions,
          huntScoreLogicKey,
        )
      : await getHuntScoreInitialPageDetail(storeId, requestedBacktestOptions, huntScoreLogicKey);
  } catch (error) {
    return (
      <main className="pageStack">
        <Breadcrumbs
          items={[
            { label: "店舗一覧", href: "/" },
            { label: "店舗ページ", href: `/stores/${storeId}` },
            { label: "バックテスト" },
          ]}
        />
        <section className="statusPanel">
          <h2>バックテストを読み込めませんでした</h2>
          <p>{error instanceof Error ? error.message : "設定を確認してください。"}</p>
        </section>
      </main>
    );
  }

  if (!detail) {
    notFound();
  }

  const backtestFallbackNotice = resultRequested && detail.backtest.usedFallbackRange
    ? "期間指定が空欄だったため、直近日数の期間を日付範囲へ仮で入れています。"
    : "";
  const backtestBookmark = {
    startDate: detail.backtest.startDate,
    endDate: detail.backtest.endDate,
    allMachineCount: detail.backtest.machineOptions.length,
    machineNames: detail.backtest.selectedMachineNames,
    rankMin: detail.backtest.rankMin,
    rankMax: detail.backtest.rankMax,
    machineRankMin: detail.backtest.machineRankMin,
    machineRankMax: detail.backtest.machineRankMax,
    selectedRankMin: detail.backtest.selectedRankMin,
    selectedRankMax: detail.backtest.selectedRankMax,
    scoreMin: detail.backtest.scoreMin,
    scoreMax: detail.backtest.scoreMax,
    nextGapMin: detail.backtest.nextGapMin,
    nextGapMax: detail.backtest.nextGapMax,
    upperGapMin: detail.backtest.upperGapMin,
    upperGapMax: detail.backtest.upperGapMax,
    rankRequired: detail.backtest.rankRequired,
    machineRankRequired: detail.backtest.machineRankRequired,
    selectedRankRequired: detail.backtest.selectedRankRequired,
    scoreRequired: detail.backtest.scoreRequired,
    nextGapRequired: detail.backtest.nextGapRequired,
    upperGapRequired: detail.backtest.upperGapRequired,
    rankScope: detail.backtest.rankScope,
    nextGapScope: detail.backtest.nextGapScope,
    scoreDifferenceMode: detail.backtest.scoreDifferenceMode,
    differenceMode: detail.backtest.differenceMode,
    eventDayTails: detail.backtest.eventFilters.dayTails,
    eventZoro: detail.backtest.eventFilters.zoro,
    eventWeekdays: detail.backtest.eventFilters.weekdays,
    eventMonthDays: detail.backtest.eventFilters.monthDays,
    combineAimJuggler: detail.backtest.combineAimJuggler,
    combineHanabi: detail.backtest.combineHanabi,
    dailySelectionMode: detail.backtest.dailySelectionMode,
  };
  const selectedBacktestDayTailSet = new Set(detail.backtest.eventFilters.dayTails);
  const selectedBacktestWeekdaySet = new Set(detail.backtest.eventFilters.weekdays);
  const selectedBacktestMonthDaySet = new Set(detail.backtest.eventFilters.monthDays);
  const machineOptionGroups = groupHuntMachineOptions(detail.backtest.machineOptions);
  const backtestFormStateKey = JSON.stringify({
    periodMode: detail.backtest.periodMode,
    recentDays: detail.backtest.recentDays,
    startDate: detail.backtest.startDate ?? "",
    endDate: detail.backtest.endDate ?? "",
    dayTails: detail.backtest.eventFilters.dayTails,
    zoro: detail.backtest.eventFilters.zoro,
    weekdays: detail.backtest.eventFilters.weekdays,
    monthDays: detail.backtest.eventFilters.monthDays,
    machineNames: detail.backtest.selectedMachineNames,
    combineAimJuggler: detail.backtest.combineAimJuggler,
    combineHanabi: detail.backtest.combineHanabi,
    dailySelectionMode: detail.backtest.dailySelectionMode,
    rankMin: detail.backtest.rankMin ?? "",
    rankMax: detail.backtest.rankMax ?? "",
    machineRankMin: detail.backtest.machineRankMin ?? "",
    machineRankMax: detail.backtest.machineRankMax ?? "",
    selectedRankMin: detail.backtest.selectedRankMin ?? "",
    selectedRankMax: detail.backtest.selectedRankMax ?? "",
    scoreMin: detail.backtest.scoreMin ?? "",
    scoreMax: detail.backtest.scoreMax ?? "",
    nextGapMin: detail.backtest.nextGapMin ?? "",
    nextGapMax: detail.backtest.nextGapMax ?? "",
    upperGapMin: detail.backtest.upperGapMin ?? "",
    upperGapMax: detail.backtest.upperGapMax ?? "",
    rankRequired: detail.backtest.rankRequired,
    machineRankRequired: detail.backtest.machineRankRequired,
    selectedRankRequired: detail.backtest.selectedRankRequired,
    scoreRequired: detail.backtest.scoreRequired,
    nextGapRequired: detail.backtest.nextGapRequired,
    upperGapRequired: detail.backtest.upperGapRequired,
    scoreDifferenceMode: detail.backtest.scoreDifferenceMode,
    differenceMode: detail.backtest.differenceMode,
    rankScope: detail.backtest.rankScope,
    nextGapScope: detail.backtest.nextGapScope,
    showGraph: detail.backtest.showGraph,
  });

  return (
    <main className="pageStack">
      <HuntBacktestFormStateSync
        storeId={detail.store.id}
        formId={HUNT_BACKTEST_FORM_ID}
        formStateKey={backtestFormStateKey}
      />
      <Breadcrumbs
        items={[
          { label: "店舗一覧", href: "/" },
          { label: detail.store.storeName, href: `/stores/${detail.store.id}` },
          { label: "バックテスト" },
        ]}
      />

      <section className="heroPanel">
        <div className="heroCopy">
          <h1 className="pageTitle pageTitleCompact">バックテスト</h1>
          <div className="storeContextLine">
            <StoreFavoriteButton
              store={{ id: detail.store.id, storeName: detail.store.storeName }}
              compact
            />
            <Link href={`/stores/${detail.store.id}`} className="storeContextLink">
              {detail.store.storeName}
            </Link>
          </div>
          <div className="heroLinks simpleHeroLinks">
            <Link href={`/stores/${detail.store.id}`} className="externalLink">
              店舗ページへ戻る
            </Link>
            <Link href={`/stores/${detail.store.id}/hunt-analysis`} className="externalLink">
              狙い度ランキングを見る
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

      {detail.backtest.machineOptions.length > 0 ? (
        <>
          <section className="filterPanel">
            <div>
              <p className="sectionLabel">翌営業日バックテスト</p>
            </div>
            <NativeGetForm
              key={backtestFormStateKey}
              id={HUNT_BACKTEST_FORM_ID}
              action={`/stores/${detail.store.id}/hunt-backtest`}
              className="backtestForm"
            >
              <input type="hidden" name="show" value="1" />
              <input type="hidden" name="backtestEventTouched" value="1" />

              <div className="backtestBlock">
                <p className="filterControlLabel">期間の指定方法</p>
                <div className="metricToggleRow">
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.periodMode === "recent" ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="periodMode"
                      value="recent"
                      defaultChecked={detail.backtest.periodMode === "recent"}
                    />
                    <span>直近日数</span>
                  </label>
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.periodMode === "range" ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="periodMode"
                      value="range"
                      defaultChecked={detail.backtest.periodMode === "range"}
                    />
                    <span>日付範囲</span>
                  </label>
                </div>
              </div>

              <div className="backtestFieldGrid">
                <label className="storeReserveField backtestField">
                  <span>直近日数</span>
                  <input
                    type="number"
                    name="recentDays"
                    min="1"
                    defaultValue={detail.backtest.recentDays}
                    className="storeReserveInput"
                  />
                </label>
                <label className="storeReserveField backtestField">
                  <span>開始日</span>
                  <input
                    type="date"
                    name="startDate"
                    defaultValue={detail.backtest.startDate ?? ""}
                    className="storeReserveInput"
                  />
                </label>
                <label className="storeReserveField backtestField">
                  <span>終了日</span>
                  <input
                    type="date"
                    name="endDate"
                    defaultValue={detail.backtest.endDate ?? ""}
                    className="storeReserveInput"
                  />
                </label>
              </div>

              <div className="backtestBlock">
                <p className="filterControlLabel">特定日（翌営業日の末尾）</p>
                <div className="metricToggleRow">
                  {DAY_TAIL_OPTIONS.map((dayTail) => (
                    <label
                      key={dayTail}
                      className={`metricToggleChip ${
                        selectedBacktestDayTailSet.has(dayTail) ? "metricToggleChipActive" : ""
                      }`}
                    >
                      <input
                        type="checkbox"
                        name="backtestDayTail"
                        value={dayTail}
                        defaultChecked={selectedBacktestDayTailSet.has(dayTail)}
                      />
                      <span>{dayTail}</span>
                    </label>
                  ))}
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.eventFilters.zoro ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="checkbox"
                      name="backtestZoro"
                      value="1"
                      defaultChecked={detail.backtest.eventFilters.zoro}
                    />
                    <span>ゾロ目</span>
                  </label>
                </div>
              </div>

              <div className="backtestBlock">
                <p className="filterControlLabel">特定日（翌営業日の日付）</p>
                <div className="metricToggleRow">
                  {MONTH_DAY_OPTIONS.map((monthDay) => (
                    <label
                      key={monthDay}
                      className={`metricToggleChip ${
                        selectedBacktestMonthDaySet.has(monthDay) ? "metricToggleChipActive" : ""
                      }`}
                    >
                      <input
                        type="checkbox"
                        name="backtestMonthDay"
                        value={monthDay}
                        defaultChecked={selectedBacktestMonthDaySet.has(monthDay)}
                      />
                      <span>{monthDay}日</span>
                    </label>
                  ))}
                </div>
              </div>

              <div className="backtestBlock">
                <p className="filterControlLabel">特定日（翌営業日の曜日）</p>
                <div className="metricToggleRow">
                  {WEEKDAY_OPTIONS.map((weekday) => (
                    <label
                      key={weekday.value}
                      className={`metricToggleChip ${
                        selectedBacktestWeekdaySet.has(weekday.value) ? "metricToggleChipActive" : ""
                      }`}
                    >
                      <input
                        type="checkbox"
                        name="backtestWeekday"
                        value={weekday.value}
                        defaultChecked={selectedBacktestWeekdaySet.has(weekday.value)}
                      />
                      <span>{weekday.label}</span>
                    </label>
                  ))}
                </div>
              </div>

              <div className="backtestBlock">
                <p className="filterControlLabel">機種名</p>
                <input type="hidden" name="machineTouched" value="1" />
                <input type="hidden" name="aimMachineGroup" value="0" />
                <input type="hidden" name="hanabiMachineGroup" value="0" />
                <AllMachineFilterButtons />
                <div className="machineFilterGroups">
                  {machineOptionGroups.map((group) => (
                    <div key={group.key} className="machineFilterGroup">
                      <p className="machineFilterGroupLabel">{group.label}</p>
                      <div className="machineGroupToggleRow">
                        {group.key === "juggler" && detail.backtest.hasAimJugglerGroupOption ? (
                          <label
                            className={`metricToggleChip ${
                              detail.backtest.combineAimJuggler ? "metricToggleChipActive" : ""
                            }`}
                          >
                            <input
                              type="checkbox"
                              name="aimMachineGroup"
                              value="1"
                              defaultChecked={detail.backtest.combineAimJuggler}
                            />
                            <span>アイジャグをまとめる</span>
                          </label>
                        ) : null}
                        {group.key === "hanabi" && detail.backtest.hasHanabiGroupOption ? (
                            <label
                              className={`metricToggleChip ${
                                detail.backtest.combineHanabi ? "metricToggleChipActive" : ""
                              }`}
                            >
                              <input
                                type="checkbox"
                                name="hanabiMachineGroup"
                                value="1"
                                defaultChecked={detail.backtest.combineHanabi}
                              />
                              <span>ハナビをまとめる</span>
                            </label>
                        ) : null}
                        <MachineFilterCategoryButton
                          category={group.key}
                          label={`${group.label}のみ選択`}
                        />
                        <MachineFilterCategoryButton
                          category={group.key}
                          label={`${group.label}のみ解除`}
                          action="clear"
                        />
                      </div>
                      <div className="metricToggleRow">
                        {group.options.map((machine) => (
                          <label
                            key={machine.name}
                            className={`metricToggleChip ${
                              machine.checked ? "metricToggleChipActive" : ""
                            }`}
                            title={machine.name}
                          >
                            <input
                              type="checkbox"
                              name="machine"
                              value={machine.name}
                              defaultChecked={machine.checked}
                              data-machine-filter-option="1"
                              data-machine-category={machine.category}
                            />
                            <span>{machine.optionLabel}</span>
                          </label>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="backtestBlock">
                <p className="filterControlLabel">選抜方法</p>
                <div className="metricToggleRow">
                  <input type="hidden" name="dailySelectionMode" value="" />
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.dailySelectionMode === "machineTopNextGap"
                        ? "metricToggleChipActive"
                        : ""
                    }`}
                  >
                    <input
                      type="checkbox"
                      name="dailySelectionMode"
                      value="machineTopNextGap"
                      defaultChecked={detail.backtest.dailySelectionMode === "machineTopNextGap"}
                    />
                    <span>各機種1位から機種内下位境界差1位を1台選抜</span>
                  </label>
                </div>
                <p className="storeReserveHelp">
                  ONの場合、日ごとに各機種の機種内狙い度1位台を候補にし、その中で機種内下位境界差が最大の1台だけを選びます。入力済みの機種内順位、チェック機種内順位、狙い度、境界差条件は、その1台への追加条件としてすべて満たした場合だけ集計します。
                </p>
              </div>

              <div className="huntConditionRows">
                <div className="huntConditionRow">
                  <p className="huntConditionLabel">機種内順位</p>
                  <div className="huntConditionInputs">
                    <label className="storeReserveField backtestField huntConditionNumberField">
                      <span>開始</span>
                      <input
                        type="number"
                        name="machineRankMin"
                        min="1"
                        defaultValue={detail.backtest.machineRankMin ?? ""}
                        className="storeReserveInput"
                      />
                    </label>
                    <label className="storeReserveField backtestField huntConditionNumberField">
                      <span>終了</span>
                      <input
                        type="number"
                        name="machineRankMax"
                        min="1"
                        defaultValue={detail.backtest.machineRankMax ?? ""}
                        className="storeReserveInput"
                      />
                    </label>
                  </div>
                  <input type="hidden" name="machineRankRequired" value="0" />
                  <label
                    className={`metricToggleChip huntConditionRequired ${
                      detail.backtest.machineRankRequired ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="checkbox"
                      name="machineRankRequired"
                      value="1"
                      defaultChecked={detail.backtest.machineRankRequired}
                    />
                    <span>必須</span>
                  </label>
                </div>
                <div className="huntConditionRow">
                  <p className="huntConditionLabel">チェック機種内順位</p>
                  <div className="huntConditionInputs">
                    <label className="storeReserveField backtestField huntConditionNumberField">
                      <span>開始</span>
                      <input
                        type="number"
                        name="selectedRankMin"
                        min="1"
                        defaultValue={detail.backtest.selectedRankMin ?? ""}
                        className="storeReserveInput"
                      />
                    </label>
                    <label className="storeReserveField backtestField huntConditionNumberField">
                      <span>終了</span>
                      <input
                        type="number"
                        name="selectedRankMax"
                        min="1"
                        defaultValue={detail.backtest.selectedRankMax ?? ""}
                        className="storeReserveInput"
                      />
                    </label>
                  </div>
                  <input type="hidden" name="selectedRankRequired" value="0" />
                  <label
                    className={`metricToggleChip huntConditionRequired ${
                      detail.backtest.selectedRankRequired ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="checkbox"
                      name="selectedRankRequired"
                      value="1"
                      defaultChecked={detail.backtest.selectedRankRequired}
                    />
                    <span>必須</span>
                  </label>
                </div>
                <div className="huntConditionRow">
                  <p className="huntConditionLabel">狙い度</p>
                  <div className="huntConditionInputs">
                    <label className="storeReserveField backtestField huntConditionNumberField">
                      <span>下限</span>
                      <input
                        type="number"
                        name="scoreMin"
                        min="0"
                        max="100"
                        step="0.1"
                        defaultValue={detail.backtest.scoreMin ?? ""}
                        className="storeReserveInput"
                      />
                    </label>
                    <label className="storeReserveField backtestField huntConditionNumberField">
                      <span>上限</span>
                      <input
                        type="number"
                        name="scoreMax"
                        min="0"
                        max="100"
                        step="0.1"
                        defaultValue={detail.backtest.scoreMax ?? ""}
                        className="storeReserveInput"
                      />
                    </label>
                  </div>
                  <input type="hidden" name="scoreRequired" value="0" />
                  <label
                    className={`metricToggleChip huntConditionRequired ${
                      detail.backtest.scoreRequired ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="checkbox"
                      name="scoreRequired"
                      value="1"
                      defaultChecked={detail.backtest.scoreRequired}
                    />
                    <span>必須</span>
                  </label>
                </div>
                <div className="huntConditionRow">
                  <p className="huntConditionLabel">上位境界差</p>
                  <div className="huntConditionInputs">
                    <label className="storeReserveField backtestField huntConditionNumberField">
                      <span>下限</span>
                      <input
                        type="number"
                        name="upperGapMin"
                        min="0"
                        max="100"
                        step="0.1"
                        defaultValue={detail.backtest.upperGapMin ?? ""}
                        className="storeReserveInput"
                      />
                    </label>
                    <label className="storeReserveField backtestField huntConditionNumberField">
                      <span>上限</span>
                      <input
                        type="number"
                        name="upperGapMax"
                        min="0"
                        max="100"
                        step="0.1"
                        defaultValue={detail.backtest.upperGapMax ?? ""}
                        className="storeReserveInput"
                      />
                    </label>
                  </div>
                  <input type="hidden" name="upperGapRequired" value="0" />
                  <label
                    className={`metricToggleChip huntConditionRequired ${
                      detail.backtest.upperGapRequired ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="checkbox"
                      name="upperGapRequired"
                      value="1"
                      defaultChecked={detail.backtest.upperGapRequired}
                    />
                    <span>必須</span>
                  </label>
                </div>
                <div className="huntConditionRow">
                  <p className="huntConditionLabel">下位境界差</p>
                  <div className="huntConditionInputs">
                    <label className="storeReserveField backtestField huntConditionNumberField">
                      <span>下限</span>
                      <input
                        type="number"
                        name="nextGapMin"
                        min="0"
                        max="100"
                        step="0.1"
                        defaultValue={detail.backtest.nextGapMin ?? ""}
                        className="storeReserveInput"
                      />
                    </label>
                    <label className="storeReserveField backtestField huntConditionNumberField">
                      <span>上限</span>
                      <input
                        type="number"
                        name="nextGapMax"
                        min="0"
                        max="100"
                        step="0.1"
                        defaultValue={detail.backtest.nextGapMax ?? ""}
                        className="storeReserveInput"
                      />
                    </label>
                  </div>
                  <input type="hidden" name="nextGapRequired" value="0" />
                  <label
                    className={`metricToggleChip huntConditionRequired ${
                      detail.backtest.nextGapRequired ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="checkbox"
                      name="nextGapRequired"
                      value="1"
                      defaultChecked={detail.backtest.nextGapRequired}
                    />
                    <span>必須</span>
                  </label>
                </div>
              </div>

              <div className="backtestBlock">
                <p className="filterControlLabel">狙い度計算の差枚基準</p>
                <div className="metricToggleRow">
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.scoreDifferenceMode === "bonus"
                        ? "metricToggleChipActive"
                        : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="scoreDifferenceMode"
                      value="bonus"
                      defaultChecked={detail.backtest.scoreDifferenceMode === "bonus"}
                    />
                    <span>設定1基準</span>
                  </label>
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.scoreDifferenceMode === "estimated"
                        ? "metricToggleChipActive"
                        : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="scoreDifferenceMode"
                      value="estimated"
                      defaultChecked={detail.backtest.scoreDifferenceMode === "estimated"}
                    />
                    <span>推定設定基準</span>
                  </label>
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.scoreDifferenceMode === "minrepo"
                        ? "metricToggleChipActive"
                        : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="scoreDifferenceMode"
                      value="minrepo"
                      defaultChecked={detail.backtest.scoreDifferenceMode === "minrepo"}
                    />
                    <span>みんレポ基準</span>
                  </label>
                </div>
              </div>

              <div className="backtestBlock">
                <p className="filterControlLabel">実績差枚の集計基準</p>
                <div className="metricToggleRow">
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.differenceMode === "bonus" ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="differenceMode"
                      value="bonus"
                      defaultChecked={detail.backtest.differenceMode === "bonus"}
                    />
                    <span>設定1基準</span>
                  </label>
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.differenceMode === "estimated"
                        ? "metricToggleChipActive"
                        : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="differenceMode"
                      value="estimated"
                      defaultChecked={detail.backtest.differenceMode === "estimated"}
                    />
                    <span>推定設定基準</span>
                  </label>
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.differenceMode === "minrepo" ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="differenceMode"
                      value="minrepo"
                      defaultChecked={detail.backtest.differenceMode === "minrepo"}
                    />
                    <span>みんレポ基準</span>
                  </label>
                </div>
              </div>

              <div className="backtestBlock">
                <p className="filterControlLabel">境界差の比較対象</p>
                <div className="metricToggleRow">
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.nextGapScope === "selected" ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="nextGapScope"
                      value="selected"
                      defaultChecked={detail.backtest.nextGapScope === "selected"}
                    />
                    <span>チェック機種内</span>
                  </label>
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.nextGapScope === "machine" ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="nextGapScope"
                      value="machine"
                      defaultChecked={detail.backtest.nextGapScope === "machine"}
                    />
                    <span>機種内</span>
                  </label>
                </div>
              </div>

              <div className="backtestBlock">
                <p className="filterControlLabel">グラフ表示</p>
                <div className="metricToggleRow">
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.showGraph === "on" ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="showGraph"
                      value="on"
                      defaultChecked={detail.backtest.showGraph === "on"}
                    />
                    <span>表示する</span>
                  </label>
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.showGraph === "off" ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="showGraph"
                      value="off"
                      defaultChecked={detail.backtest.showGraph === "off"}
                    />
                    <span>表示しない</span>
                  </label>
                </div>
              </div>

              <div className="backtestButtonRow">
                <button type="submit" className="storeReserveButton">
                  バックテストする
                </button>
              </div>
            </NativeGetForm>
            {backtestFallbackNotice ? <p className="storeReserveHelp">{backtestFallbackNotice}</p> : null}
          </section>

          {resultRequested ? (
            detail.rankingDates.length > 0 ? (
              <>
                <section className="cardsGrid summaryStrip">
                  <article className="summaryCard">
                    <p className="metaLabel">狙い度期間</p>
                    <strong className="metaValue">
                      {formatPeriod(detail.backtest.startDate, detail.backtest.endDate)}
                    </strong>
                  </article>
                  <article className="summaryCard">
                    <p className="metaLabel">対象集計日</p>
                    <strong className="metaValue">{formatNumber(detail.backtest.targetDateCount)}日</strong>
                  </article>
                  <article className="summaryCard">
                    <p className="metaLabel">集計台数</p>
                    <strong className="metaValue">{formatNumber(detail.backtest.actualRowCount)}台</strong>
                  </article>
                </section>

                <HuntBacktestBookmarkControl storeId={detail.store.id} bookmark={backtestBookmark} />

                {detail.backtest.showGraph === "on" && detail.backtest.graphPoints.length > 0 ? (
                  <HuntBacktestGraph
                    groups={detail.backtest.breakdowns.map((breakdown) => ({
                      key: breakdown.key,
                      title: breakdown.title,
                      points: breakdown.graphPoints,
                    }))}
                  />
                ) : null}

                {detail.backtest.hasMatches ? (
                  detail.backtest.breakdowns.map((breakdown, index) => (
                    <BacktestResultTable
                      key={breakdown.key}
                      title={breakdown.title}
                      backtest={breakdown}
                      tableId={`store-backtest-${breakdown.key}-${index}`}
                      storeId={detail.store.id}
                    />
                  ))
                ) : (
                  <section className="statusPanel">
                    <h2>集計できる台がありません</h2>
                    <p>期間、機種、順位、狙い度の条件を見直してください。</p>
                  </section>
                )}
              </>
            ) : (
              <section className="statusPanel">
                <h2>バックテストを作れる日付がまだありません</h2>
                <p>対象機種の保存済みデータが増えると、ここで条件ごとの結果を確認できます。</p>
              </section>
            )
          ) : (
            <section className="statusPanel">
              <h2>バックテスト結果はまだ表示していません</h2>
              <p>条件を選んでバックテストすると、対象機種の台データを読み込んで集計します。</p>
            </section>
          )}
        </>
      ) : (
        <section className="statusPanel">
          <h2>バックテストを作れる日付がまだありません</h2>
          <p>対象機種の保存済みデータが増えると、ここで条件ごとの結果を確認できます。</p>
        </section>
      )}
    </main>
  );
}
