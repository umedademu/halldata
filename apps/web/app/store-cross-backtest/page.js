import Link from "next/link";
import { Fragment } from "react";

import { ExpandableTableRowsController } from "../../components/expandable-table-rows-controller";
import {
  AllMachineFilterButtons,
  MachineFilterCategoryButton,
} from "../../components/hunt-machine-filter-tools";
import { NativeGetForm } from "../../components/native-get-form";
import { ResultUrlTools } from "../../components/result-url-tools";
import { SpecialDayFilterSettings } from "../../components/special-day-filter-settings";
import { SortableTableController } from "../../components/sortable-table-controller";
import { SortableTableHeader } from "../../components/sortable-table-header";
import { StoreFavoriteButton } from "../../components/store-favorite-button";
import { getCrossStoreBacktestDetail } from "../../lib/data";
import {
  formatAverageGames,
  formatDecimal,
  formatNumber,
  formatPeriod,
  formatPercent,
  formatSignedNumber,
} from "../../lib/format";
import { groupHuntMachineOptions } from "../../lib/hunt-machine-display";
import {
  formatSettingEstimateScore,
  getSettingEstimateHighlightClass,
  SETTING_ESTIMATE_MODE_OPTIONS,
} from "../../lib/setting-estimates";

export const dynamic = "force-dynamic";
export const metadata = {
  title: "店舗横断バックテスト",
};

const DAY_TAIL_OPTIONS = Array.from({ length: 10 }, (_, index) => index);
const WEEKDAY_OPTIONS = [
  { value: 1, label: "月曜" },
  { value: 2, label: "火曜" },
  { value: 3, label: "水曜" },
  { value: 4, label: "木曜" },
  { value: 5, label: "金曜" },
  { value: 6, label: "土曜" },
  { value: 0, label: "日曜" },
];

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

function readOptionalSearchParam(searchParams, key) {
  return Object.hasOwn(searchParams ?? {}, key)
    ? readSingleSearchParam(searchParams?.[key])
    : undefined;
}

function formatAverageDifference(value) {
  return Number.isFinite(value) ? formatSignedNumber(Math.round(value)) : "-";
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

function ScopedConditionRow({
  label,
  minName,
  maxName,
  requiredName,
  minValue,
  maxValue,
  requiredValue,
  minLabel = "下限",
  maxLabel = "上限",
  inputMin = "0",
  inputMax = "100",
  inputStep = "0.1",
}) {
  return (
    <div className="scopedConditionRow">
      <p className="scopedConditionLabel">{label}</p>
      <label className="scopedConditionField">
        <span>{minLabel}</span>
        <input
          type="number"
          name={minName}
          min={inputMin}
          max={inputMax}
          step={inputStep}
          defaultValue={minValue ?? ""}
          className="storeReserveInput"
        />
      </label>
      <label className="scopedConditionField">
        <span>{maxLabel}</span>
        <input
          type="number"
          name={maxName}
          min={inputMin}
          max={inputMax}
          step={inputStep}
          defaultValue={maxValue ?? ""}
          className="storeReserveInput"
        />
      </label>
      <input type="hidden" name={requiredName} value="0" />
      <label
        className={`metricToggleChip scopedConditionRequired ${
          requiredValue ? "metricToggleChipActive" : ""
        }`}
      >
        <input
          type="checkbox"
          name={requiredName}
          value="1"
          defaultChecked={requiredValue}
        />
        <span>必須</span>
      </label>
    </div>
  );
}

function ScoreDifferenceModeOptions({ value }) {
  return (
    <div className="metricToggleRow commonConditionModeOptions">
      <label
        className={`metricToggleChip ${value === "bonus" ? "metricToggleChipActive" : ""}`}
      >
        <input
          type="radio"
          name="scoreDifferenceMode"
          value="bonus"
          defaultChecked={value === "bonus"}
        />
        <span>設定1基準</span>
      </label>
      <label
        className={`metricToggleChip ${value === "estimated" ? "metricToggleChipActive" : ""}`}
      >
        <input
          type="radio"
          name="scoreDifferenceMode"
          value="estimated"
          defaultChecked={value === "estimated"}
        />
        <span>推定設定基準</span>
      </label>
      <label
        className={`metricToggleChip ${value === "minrepo" ? "metricToggleChipActive" : ""}`}
      >
        <input
          type="radio"
          name="scoreDifferenceMode"
          value="minrepo"
          defaultChecked={value === "minrepo"}
        />
        <span>みんレポ基準</span>
      </label>
    </div>
  );
}

function SettingEstimateModeOptions({ value }) {
  return (
    <div className="metricToggleRow commonConditionModeOptions">
      {SETTING_ESTIMATE_MODE_OPTIONS.map((option) => (
        <label
          key={option.value}
          className={`metricToggleChip ${
            value === option.value ? "metricToggleChipActive" : ""
          }`}
        >
          <input
            type="radio"
            name="settingEstimateMode"
            value={option.value}
            defaultChecked={value === option.value}
          />
          <span>{option.label}</span>
        </label>
      ))}
    </div>
  );
}

function CrossStoreNonmatchingRow({ parentKey, summary }) {
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
      <td data-sort-value="">-</td>
      <th className="directoryNameCell" data-sort-value="非該当台">非該当台</th>
      <td data-sort-value="">-</td>
      <BacktestMetricCell sortValue={readSortNumber(summary.payoutRate)}>{formatPercent(summary.payoutRate)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.averageDifference)}>{formatAverageDifference(summary.averageDifference)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={summary.differenceTotal}>{formatSignedNumber(summary.differenceTotal)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={summary.gamesTotal}>{formatNumber(summary.gamesTotal)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.averageGames)}>{formatAverageGames(summary.averageGames)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.averageSetting)}>{formatSettingEstimateScore(summary.averageSetting)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.setting35PlusRate)}>{formatPercent(summary.setting35PlusRate)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.setting4PlusRate)}>{formatPercent(summary.setting4PlusRate)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.setting45PlusRate)}>{formatPercent(summary.setting45PlusRate)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.setting5PlusRate)}>{formatPercent(summary.setting5PlusRate)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={summary.actualRowCount}>{formatNumber(summary.actualRowCount)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.winRate)}>{formatPercent(summary.winRate)}</BacktestMetricCell>
      <td data-sort-value="">-</td>
      <td data-sort-value="">-</td>
      <td data-sort-value="">-</td>
      <td data-sort-value="">-</td>
      <BacktestMetricCell sortValue={readSortNumber(summary.averageHuntScore)}>{formatDecimal(summary.averageHuntScore)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.averageUpperGap)}>{formatDecimal(summary.averageUpperGap)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.averageNextGap)}>{formatDecimal(summary.averageNextGap)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={summary.bbTotal}>{formatNumber(summary.bbTotal)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={summary.rbTotal}>{formatNumber(summary.rbTotal)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readProbabilitySortValue(summary.bbProbability)}>{summary.bbProbability ?? "-"}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readProbabilitySortValue(summary.rbProbability)}>{summary.rbProbability ?? "-"}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readProbabilitySortValue(summary.combinedProbability)}>{summary.combinedProbability ?? "-"}</BacktestMetricCell>
    </tr>
  );
}

function formatCrossStoreRankingMetricLabel(rankingMetric) {
  return rankingMetric === "differenceTotal" ? "合計差枚" : "平均機械割";
}

function StoreRankingTable({ rows, rankingMetric }) {
  const tableId = "cross-store-backtest-results";
  const rankingMetricLabel = formatCrossStoreRankingMetricLabel(rankingMetric);

  return (
    <section className="tablePanel directoryPanel">
      <SortableTableController tableId={tableId} />
      <ExpandableTableRowsController tableId={tableId} />
      <div className="tablePanelHeader">
        <div>
          <p className="sectionLabel">{rankingMetricLabel}ランキング</p>
          <h2 className="tablePanelTitle">店舗横断バックテスト結果</h2>
        </div>
      </div>
      <div className="tableScroller directoryScroller">
        <table id={tableId} className="directoryTable" data-sortable-table="1">
          <thead>
            <tr>
              <SortableTableHeader columnIndex={0} initialDirection="asc">
                順位
              </SortableTableHeader>
              <SortableTableHeader
                columnIndex={1}
                type="text"
                initialDirection="asc"
                className="directoryNameHeader"
              >
                店舗名
              </SortableTableHeader>
              <SortableTableHeader columnIndex={2} type="text" initialDirection="asc">
                地域
              </SortableTableHeader>
              <SortableTableHeader columnIndex={3}>平均機械割</SortableTableHeader>
              <SortableTableHeader columnIndex={4}>平均差枚</SortableTableHeader>
              <SortableTableHeader columnIndex={5}>合計差枚</SortableTableHeader>
              <SortableTableHeader columnIndex={6}>合計G数</SortableTableHeader>
              <SortableTableHeader columnIndex={7}>平均G数</SortableTableHeader>
              <SortableTableHeader columnIndex={8}>平均設定</SortableTableHeader>
              <SortableTableHeader columnIndex={9}>推定3.5+</SortableTableHeader>
              <SortableTableHeader columnIndex={10}>推定4.0+</SortableTableHeader>
              <SortableTableHeader columnIndex={11}>推定4.5+</SortableTableHeader>
              <SortableTableHeader columnIndex={12}>推定5.0+</SortableTableHeader>
              <SortableTableHeader columnIndex={13}>集計台数</SortableTableHeader>
              <SortableTableHeader columnIndex={14}>勝率</SortableTableHeader>
              <SortableTableHeader columnIndex={15}>対象日数</SortableTableHeader>
              <SortableTableHeader columnIndex={16}>集計日数</SortableTableHeader>
              <SortableTableHeader columnIndex={17}>対象機種</SortableTableHeader>
              <SortableTableHeader columnIndex={18}>設置台数</SortableTableHeader>
              <SortableTableHeader columnIndex={19}>狙い度</SortableTableHeader>
              <SortableTableHeader columnIndex={20}>上位境界差（同一）</SortableTableHeader>
              <SortableTableHeader columnIndex={21}>下位境界差（同一）</SortableTableHeader>
              <SortableTableHeader columnIndex={22}>BB</SortableTableHeader>
              <SortableTableHeader columnIndex={23}>RB</SortableTableHeader>
              <SortableTableHeader columnIndex={24} initialDirection="asc">
                BB率
              </SortableTableHeader>
              <SortableTableHeader columnIndex={25} initialDirection="asc">
                RB率
              </SortableTableHeader>
              <SortableTableHeader columnIndex={26} initialDirection="asc">
                合成
              </SortableTableHeader>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const nonmatchingSummary = row.nonmatchingSummary;
              const rowKey = `${tableId}:store:${row.store.id}`;
              return (
              <Fragment key={row.store.id}>
              <tr
                className={getSettingEstimateHighlightClass(row.averageSetting)}
                data-expand-row-key={rowKey}
              >
                <td data-sort-value={row.rank}>{formatNumber(row.rank)}</td>
                <th className="directoryNameCell" data-sort-value={row.store.storeName}>
                  <span className="storeNameWithFavorite">
                    {hasNonmatchingSummary(nonmatchingSummary) ? (
                      <span className="backtestExpandIndicator" aria-hidden="true">＋</span>
                    ) : null}
                    <StoreFavoriteButton
                      store={{ id: row.store.id, storeName: row.store.storeName }}
                      compact
                    />
                    <Link href={`/stores/${row.store.id}`}>{row.store.storeName}</Link>
                  </span>
                </th>
                <td
                  data-sort-value={
                    [row.store.prefectureName, row.store.areaName].filter(Boolean).join(" / ") ||
                    ""
                  }
                >
                  {[row.store.prefectureName, row.store.areaName].filter(Boolean).join(" / ") ||
                    "-"}
                </td>
                <BacktestMetricCell sortValue={readSortNumber(row.payoutRate)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="平均機械割" nonmatchingValue={formatPercent(nonmatchingSummary?.payoutRate)}>{formatPercent(row.payoutRate)}</BacktestMetricCell>
                <BacktestMetricCell sortValue={readSortNumber(row.averageDifference)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="平均差枚" nonmatchingValue={formatAverageDifference(nonmatchingSummary?.averageDifference)}>{formatAverageDifference(row.averageDifference)}</BacktestMetricCell>
                <BacktestMetricCell sortValue={row.differenceTotal} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="合計差枚" nonmatchingValue={formatSignedNumber(nonmatchingSummary?.differenceTotal)}>{formatSignedNumber(row.differenceTotal)}</BacktestMetricCell>
                <BacktestMetricCell sortValue={row.gamesTotal} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="合計G数" nonmatchingValue={formatNumber(nonmatchingSummary?.gamesTotal)}>{formatNumber(row.gamesTotal)}</BacktestMetricCell>
                <BacktestMetricCell sortValue={readSortNumber(row.averageGames)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="平均G数" nonmatchingValue={formatAverageGames(nonmatchingSummary?.averageGames)}>{formatAverageGames(row.averageGames)}</BacktestMetricCell>
                <BacktestMetricCell sortValue={readSortNumber(row.averageSetting)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="平均設定" nonmatchingValue={formatSettingEstimateScore(nonmatchingSummary?.averageSetting)}>{formatSettingEstimateScore(row.averageSetting)}</BacktestMetricCell>
                <BacktestMetricCell sortValue={readSortNumber(row.setting35PlusRate)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="推定3.5+" nonmatchingValue={formatPercent(nonmatchingSummary?.setting35PlusRate)}>{formatPercent(row.setting35PlusRate)}</BacktestMetricCell>
                <BacktestMetricCell sortValue={readSortNumber(row.setting4PlusRate)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="推定4.0+" nonmatchingValue={formatPercent(nonmatchingSummary?.setting4PlusRate)}>{formatPercent(row.setting4PlusRate)}</BacktestMetricCell>
                <BacktestMetricCell sortValue={readSortNumber(row.setting45PlusRate)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="推定4.5+" nonmatchingValue={formatPercent(nonmatchingSummary?.setting45PlusRate)}>{formatPercent(row.setting45PlusRate)}</BacktestMetricCell>
                <BacktestMetricCell sortValue={readSortNumber(row.setting5PlusRate)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="推定5.0+" nonmatchingValue={formatPercent(nonmatchingSummary?.setting5PlusRate)}>{formatPercent(row.setting5PlusRate)}</BacktestMetricCell>
                <BacktestMetricCell sortValue={row.actualRowCount} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="集計台数" nonmatchingValue={formatNumber(nonmatchingSummary?.actualRowCount)}>{formatNumber(row.actualRowCount)}</BacktestMetricCell>
                <BacktestMetricCell sortValue={readSortNumber(row.winRate)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="勝率" nonmatchingValue={formatPercent(nonmatchingSummary?.winRate)}>{formatPercent(row.winRate)}</BacktestMetricCell>
                <td data-sort-value={row.targetDateCount}>{formatNumber(row.targetDateCount)}</td>
                <td data-sort-value={row.matchedDateCount}>
                  {formatNumber(row.matchedDateCount)}
                </td>
                <td
                  title={row.selectedMachineNames.join("、")}
                  data-sort-value={row.selectedMachineCount}
                >
                  {formatNumber(row.selectedMachineCount)}
                </td>
                <td data-sort-value={row.slotCount}>{formatNumber(row.slotCount)}</td>
                <BacktestMetricCell sortValue={readSortNumber(row.averageHuntScore)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="狙い度" nonmatchingValue={formatDecimal(nonmatchingSummary?.averageHuntScore)}>{formatDecimal(row.averageHuntScore)}</BacktestMetricCell>
                <BacktestMetricCell sortValue={readSortNumber(row.averageUpperGap)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="上位境界差（同一）" nonmatchingValue={formatDecimal(nonmatchingSummary?.averageUpperGap)}>{formatDecimal(row.averageUpperGap)}</BacktestMetricCell>
                <BacktestMetricCell sortValue={readSortNumber(row.averageNextGap)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="下位境界差（同一）" nonmatchingValue={formatDecimal(nonmatchingSummary?.averageNextGap)}>{formatDecimal(row.averageNextGap)}</BacktestMetricCell>
                <BacktestMetricCell sortValue={row.bbTotal} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="BB" nonmatchingValue={formatNumber(nonmatchingSummary?.bbTotal)}>{formatNumber(row.bbTotal)}</BacktestMetricCell>
                <BacktestMetricCell sortValue={row.rbTotal} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="RB" nonmatchingValue={formatNumber(nonmatchingSummary?.rbTotal)}>{formatNumber(row.rbTotal)}</BacktestMetricCell>
                <BacktestMetricCell sortValue={readProbabilitySortValue(row.bbProbability)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="BB率" nonmatchingValue={nonmatchingSummary?.bbProbability ?? "-"}>{row.bbProbability ?? "-"}</BacktestMetricCell>
                <BacktestMetricCell sortValue={readProbabilitySortValue(row.rbProbability)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="RB率" nonmatchingValue={nonmatchingSummary?.rbProbability ?? "-"}>{row.rbProbability ?? "-"}</BacktestMetricCell>
                <BacktestMetricCell sortValue={readProbabilitySortValue(row.combinedProbability)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="合成" nonmatchingValue={nonmatchingSummary?.combinedProbability ?? "-"}>{row.combinedProbability ?? "-"}</BacktestMetricCell>
              </tr>
              <CrossStoreNonmatchingRow parentKey={rowKey} summary={nonmatchingSummary} />
              </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export default async function CrossStoreBacktestPage({ searchParams }) {
  const resolvedSearchParams = await searchParams;
  const resultRequested = readSingleSearchParam(resolvedSearchParams?.show) === "1";
  const detail = await getCrossStoreBacktestDetail({
    resultRequested,
    logicKey: readSingleSearchParam(resolvedSearchParams?.logicKey),
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
    settingEstimateMode: readSingleSearchParam(resolvedSearchParams?.settingEstimateMode),
    rankMin: readOptionalSearchParam(resolvedSearchParams, "rankMin"),
    rankMax: readOptionalSearchParam(resolvedSearchParams, "rankMax"),
    rankScope: readSingleSearchParam(resolvedSearchParams?.rankScope),
    machineRankMin: readOptionalSearchParam(resolvedSearchParams, "machineRankMin"),
    machineRankMax: readOptionalSearchParam(resolvedSearchParams, "machineRankMax"),
    selectedRankMin: readOptionalSearchParam(resolvedSearchParams, "selectedRankMin"),
    selectedRankMax: readOptionalSearchParam(resolvedSearchParams, "selectedRankMax"),
    scoreMin: readOptionalSearchParam(resolvedSearchParams, "scoreMin"),
    scoreMax: readOptionalSearchParam(resolvedSearchParams, "scoreMax"),
    nextGapScope: readSingleSearchParam(resolvedSearchParams?.nextGapScope),
    upperGapMin: readOptionalSearchParam(resolvedSearchParams, "upperGapMin"),
    upperGapMax: readOptionalSearchParam(resolvedSearchParams, "upperGapMax"),
    nextGapMin: readOptionalSearchParam(resolvedSearchParams, "nextGapMin"),
    nextGapMax: readOptionalSearchParam(resolvedSearchParams, "nextGapMax"),
    machineUpperGapMin: readOptionalSearchParam(resolvedSearchParams, "machineUpperGapMin"),
    machineUpperGapMax: readOptionalSearchParam(resolvedSearchParams, "machineUpperGapMax"),
    machineNextGapMin: readOptionalSearchParam(resolvedSearchParams, "machineNextGapMin"),
    machineNextGapMax: readOptionalSearchParam(resolvedSearchParams, "machineNextGapMax"),
    selectedUpperGapMin: readOptionalSearchParam(resolvedSearchParams, "selectedUpperGapMin"),
    selectedUpperGapMax: readOptionalSearchParam(resolvedSearchParams, "selectedUpperGapMax"),
    selectedNextGapMin: readOptionalSearchParam(resolvedSearchParams, "selectedNextGapMin"),
    selectedNextGapMax: readOptionalSearchParam(resolvedSearchParams, "selectedNextGapMax"),
    rankRequired: readMultiSearchParam(resolvedSearchParams?.rankRequired),
    machineRankRequired: readMultiSearchParam(resolvedSearchParams?.machineRankRequired),
    selectedRankRequired: readMultiSearchParam(resolvedSearchParams?.selectedRankRequired),
    scoreRequired: readMultiSearchParam(resolvedSearchParams?.scoreRequired),
    upperGapRequired: readMultiSearchParam(resolvedSearchParams?.upperGapRequired),
    nextGapRequired: readMultiSearchParam(resolvedSearchParams?.nextGapRequired),
    machineUpperGapRequired: readMultiSearchParam(resolvedSearchParams?.machineUpperGapRequired),
    machineNextGapRequired: readMultiSearchParam(resolvedSearchParams?.machineNextGapRequired),
    selectedUpperGapRequired: readMultiSearchParam(resolvedSearchParams?.selectedUpperGapRequired),
    selectedNextGapRequired: readMultiSearchParam(resolvedSearchParams?.selectedNextGapRequired),
    prefectures: readMultiSearchParam(resolvedSearchParams?.prefecture),
    areaKeys: readMultiSearchParam(resolvedSearchParams?.area),
    dayTails: readMultiSearchParam(resolvedSearchParams?.backtestDayTail),
    zoro: readSingleSearchParam(resolvedSearchParams?.backtestZoro) === "1",
    weekdays: readMultiSearchParam(resolvedSearchParams?.backtestWeekday),
    monthDays: readMultiSearchParam(resolvedSearchParams?.backtestMonthDay),
    minActualRows: readSingleSearchParam(resolvedSearchParams?.minActualRows),
    minMatchedDateCount: readSingleSearchParam(resolvedSearchParams?.minMatchedDateCount),
    minSlotCount: readSingleSearchParam(resolvedSearchParams?.minSlotCount),
    maxSlotCount: readSingleSearchParam(resolvedSearchParams?.maxSlotCount),
    limit: readSingleSearchParam(resolvedSearchParams?.limit),
    rankingMetric: readSingleSearchParam(resolvedSearchParams?.rankingMetric),
  });
  const machineOptionGroups = groupHuntMachineOptions(detail.machineOptions, {
    combineAimJuggler: detail.combineAimJuggler,
    combineHanabi: detail.combineHanabi,
  });
  const locationFilterOpen =
    detail.selectedPrefectures.length > 0 || detail.selectedAreaKeys.length > 0;

  return (
    <main className="pageStack">
      <section className="heroPanel">
        <div className="heroCopy">
          <h1 className="pageTitle pageTitleCompact">店舗横断バックテスト</h1>
          <div className="heroLinks simpleHeroLinks">
            <Link href="/" className="externalLink">
              店舗一覧へ戻る
            </Link>
          </div>
        </div>
      </section>

      <section className="filterPanel">
        <NativeGetForm action="/store-cross-backtest" className="backtestForm">
          <input type="hidden" name="show" value="1" />
          <input type="hidden" name="machineTouched" value="1" />

          <details className="collapsibleControlGroup crossBacktestConditionGroup" open>
            <summary className="collapsibleControlHeader crossBacktestConditionSummary">
              <span className="sectionLabel">店舗横断条件</span>
              <span
                className="collapsibleControlStatus crossBacktestConditionStatus"
                aria-hidden="true"
              />
            </summary>
            <div className="collapsibleControlBody crossBacktestConditionBody">
              <div className="backtestFieldGrid">
            <label className="storeReserveField backtestField">
              <span>ロジック</span>
              <select
                name="logicKey"
                defaultValue={detail.huntScoreLogic.key}
                className="storeReserveInput"
              >
                {detail.logicOptions.map((logic) => (
                  <option key={logic.key} value={logic.key}>
                    {logic.name}
                  </option>
                ))}
              </select>
            </label>
            <label className="storeReserveField backtestField">
              <span>最低集計台数</span>
              <input
                type="number"
                name="minActualRows"
                min="0"
                defaultValue={detail.minActualRows}
                className="storeReserveInput"
              />
            </label>
            <label className="storeReserveField backtestField">
              <span>最低集計日数</span>
              <input
                type="number"
                name="minMatchedDateCount"
                min="0"
                defaultValue={detail.minMatchedDateCount}
                className="storeReserveInput"
              />
            </label>
            <label className="storeReserveField backtestField">
              <span>最低設置台数</span>
              <input
                type="number"
                name="minSlotCount"
                min="0"
                defaultValue={detail.minSlotCount ?? ""}
                className="storeReserveInput"
              />
            </label>
            <label className="storeReserveField backtestField">
              <span>最大設置台数</span>
              <input
                type="number"
                name="maxSlotCount"
                min="0"
                defaultValue={detail.maxSlotCount ?? ""}
                className="storeReserveInput"
              />
            </label>
            <label className="storeReserveField backtestField">
              <span>表示件数</span>
              <input
                type="number"
                name="limit"
                min="1"
                max="300"
                defaultValue={detail.limit}
                className="storeReserveInput"
              />
            </label>
          </div>

          <div className="backtestBlock">
            <p className="filterControlLabel">ランキング基準</p>
            <div className="metricToggleRow">
              {[
                { value: "payoutRate", label: "平均機械割" },
                { value: "differenceTotal", label: "合計差枚" },
              ].map((metric) => (
                <label
                  key={metric.value}
                  className={`metricToggleChip ${
                    detail.rankingMetric === metric.value ? "metricToggleChipActive" : ""
                  }`}
                >
                  <input
                    type="radio"
                    name="rankingMetric"
                    value={metric.value}
                    defaultChecked={detail.rankingMetric === metric.value}
                  />
                  <span>{metric.label}</span>
                </label>
              ))}
            </div>
          </div>

          <div className="filterConditionBox">
            <p className="filterConditionBoxTitle">期間指定</p>
            <div className="periodConditionRow">
              <div className="periodModeGroup">
                <p className="filterControlLabel">指定方法</p>
                <div className="metricToggleRow">
                  <label
                    className={`metricToggleChip ${
                      detail.periodMode === "recent" ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="periodMode"
                      value="recent"
                      defaultChecked={detail.periodMode === "recent"}
                    />
                    <span>直近日数</span>
                  </label>
                  <label
                    className={`metricToggleChip ${
                      detail.periodMode === "range" ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="periodMode"
                      value="range"
                      defaultChecked={detail.periodMode === "range"}
                    />
                    <span>日付範囲</span>
                  </label>
                </div>
              </div>
              <div className="periodInputGroup">
                <label className="storeReserveField backtestField">
                  <span>直近日数</span>
                  <input
                    type="number"
                    name="recentDays"
                    min="1"
                    defaultValue={detail.recentDays}
                    className="storeReserveInput"
                  />
                </label>
                <label className="storeReserveField backtestField">
                  <span>開始日</span>
                  <input
                    type="date"
                    name="startDate"
                    defaultValue={detail.startDate ?? ""}
                    className="storeReserveInput"
                  />
                </label>
                <label className="storeReserveField backtestField">
                  <span>終了日</span>
                  <input
                    type="date"
                    name="endDate"
                    defaultValue={detail.endDate ?? ""}
                    className="storeReserveInput"
                  />
                </label>
              </div>
            </div>
          </div>

          <div className="filterConditionBox">
            <p className="filterConditionBoxTitle">特定日指定</p>
            <SpecialDayFilterSettings
              dayTailOptions={DAY_TAIL_OPTIONS}
              weekdayOptions={WEEKDAY_OPTIONS}
              selectedDayTails={detail.eventFilters.dayTails}
              selectedMonthDays={detail.eventFilters.monthDays}
              selectedWeekdays={detail.eventFilters.weekdays}
              zoro={detail.eventFilters.zoro}
            />
          </div>

          <div className="filterConditionBox">
            <p className="filterConditionBoxTitle">機種選択</p>
            <AllMachineFilterButtons />
            <div className="machineFilterGroups">
              {machineOptionGroups.map((group) => (
                <div key={group.key} className="machineFilterGroup">
                  <p className="machineFilterGroupLabel">{group.label}</p>
                  <div className="machineGroupToggleRow">
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
                          data-machine-slot-count={machine.slotCount ?? ""}
                          data-machine-combined-group-key={machine.combinedGroupKey ?? ""}
                          data-machine-combined-role={machine.combinedRole ?? ""}
                        />
                        <span>{machine.optionLabel}</span>
                      </label>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="huntConditionRows">
            <div className="commonConditionPanel">
              <p className="scopedConditionColumnTitle">共通条件</p>
              <div className="commonConditionGrid">
                <ScopedConditionRow
                  label="狙い度"
                  minName="scoreMin"
                  maxName="scoreMax"
                  requiredName="scoreRequired"
                  minValue={detail.scoreMin}
                  maxValue={detail.scoreMax}
                  requiredValue={detail.scoreRequired}
                />
                <div className="commonConditionMode">
                  <p className="commonConditionSubLabel">狙い度計算の差枚基準</p>
                  <ScoreDifferenceModeOptions value={detail.scoreDifferenceMode} />
                </div>
                <div className="commonConditionMode">
                  <p className="commonConditionSubLabel">設定推定基準</p>
                  <SettingEstimateModeOptions value={detail.settingEstimateMode} />
                </div>
              </div>
            </div>
            <div className="scopedConditionColumns">
              <div className="scopedConditionColumn">
                <p className="scopedConditionColumnTitle">同一機種内</p>
                <ScopedConditionRow
                  label="順位"
                  minName="machineRankMin"
                  maxName="machineRankMax"
                  requiredName="machineRankRequired"
                  minValue={detail.machineRankMin}
                  maxValue={detail.machineRankMax}
                  requiredValue={detail.machineRankRequired}
                  minLabel="開始"
                  maxLabel="終了"
                  inputMin="1"
                  inputMax={undefined}
                  inputStep={undefined}
                />
                <ScopedConditionRow
                  label="上位境界差"
                  minName="machineUpperGapMin"
                  maxName="machineUpperGapMax"
                  requiredName="machineUpperGapRequired"
                  minValue={detail.machineUpperGapMin}
                  maxValue={detail.machineUpperGapMax}
                  requiredValue={detail.machineUpperGapRequired}
                />
                <ScopedConditionRow
                  label="下位境界差"
                  minName="machineNextGapMin"
                  maxName="machineNextGapMax"
                  requiredName="machineNextGapRequired"
                  minValue={detail.machineNextGapMin}
                  maxValue={detail.machineNextGapMax}
                  requiredValue={detail.machineNextGapRequired}
                />
              </div>
              <div className="scopedConditionColumn">
                <p className="scopedConditionColumnTitle">選択機種内</p>
                <ScopedConditionRow
                  label="順位"
                  minName="selectedRankMin"
                  maxName="selectedRankMax"
                  requiredName="selectedRankRequired"
                  minValue={detail.selectedRankMin}
                  maxValue={detail.selectedRankMax}
                  requiredValue={detail.selectedRankRequired}
                  minLabel="開始"
                  maxLabel="終了"
                  inputMin="1"
                  inputMax={undefined}
                  inputStep={undefined}
                />
                <ScopedConditionRow
                  label="上位境界差"
                  minName="selectedUpperGapMin"
                  maxName="selectedUpperGapMax"
                  requiredName="selectedUpperGapRequired"
                  minValue={detail.selectedUpperGapMin}
                  maxValue={detail.selectedUpperGapMax}
                  requiredValue={detail.selectedUpperGapRequired}
                />
                <ScopedConditionRow
                  label="下位境界差"
                  minName="selectedNextGapMin"
                  maxName="selectedNextGapMax"
                  requiredName="selectedNextGapRequired"
                  minValue={detail.selectedNextGapMin}
                  maxValue={detail.selectedNextGapMax}
                  requiredValue={detail.selectedNextGapRequired}
                />
              </div>
            </div>
          </div>

          <div className="backtestBlock">
            <p className="filterControlLabel">実績差枚の集計基準</p>
            <div className="metricToggleRow">
              <label
                className={`metricToggleChip ${
                  detail.differenceMode === "bonus" ? "metricToggleChipActive" : ""
                }`}
              >
                <input
                  type="radio"
                  name="differenceMode"
                  value="bonus"
                  defaultChecked={detail.differenceMode === "bonus"}
                />
                <span>設定1基準</span>
              </label>
              <label
                className={`metricToggleChip ${
                  detail.differenceMode === "estimated" ? "metricToggleChipActive" : ""
                }`}
              >
                <input
                  type="radio"
                  name="differenceMode"
                  value="estimated"
                  defaultChecked={detail.differenceMode === "estimated"}
                />
                <span>推定設定基準</span>
              </label>
              <label
                className={`metricToggleChip ${
                  detail.differenceMode === "minrepo" ? "metricToggleChipActive" : ""
                }`}
              >
                <input
                  type="radio"
                  name="differenceMode"
                  value="minrepo"
                  defaultChecked={detail.differenceMode === "minrepo"}
                />
                <span>みんレポ基準</span>
              </label>
            </div>
          </div>

            </div>
          </details>

          <details className="collapsibleControlGroup crossBacktestConditionGroup" open={locationFilterOpen}>
            <summary className="collapsibleControlHeader crossBacktestConditionSummary">
              <span className="sectionLabel">地域条件</span>
              <span
                className="collapsibleControlStatus crossBacktestConditionStatus"
                aria-hidden="true"
              />
            </summary>
            <div className="collapsibleControlBody crossBacktestConditionBody">
              <div className="machineFilterGroups">
                <div className="machineFilterGroup">
                  <p className="machineFilterGroupLabel">都道府県</p>
                  <div className="metricToggleRow">
                    {detail.prefectureOptions.map((prefecture) => (
                      <label
                        key={prefecture.name}
                        className={`metricToggleChip ${
                          prefecture.checked ? "metricToggleChipActive" : ""
                        }`}
                      >
                        <input
                          type="checkbox"
                          name="prefecture"
                          value={prefecture.name}
                          defaultChecked={prefecture.checked}
                        />
                        <span>{prefecture.name}（{formatNumber(prefecture.count)}）</span>
                      </label>
                    ))}
                  </div>
                </div>
                <div className="machineFilterGroup">
                  <p className="machineFilterGroupLabel">地域</p>
                  {detail.areaOptionGroups.map((group) => (
                    <div key={group.prefectureName} className="machineFilterGroup">
                      <p className="machineFilterGroupLabel">{group.prefectureName}</p>
                      <div className="metricToggleRow">
                        {group.options.map((area) => (
                          <label
                            key={area.key}
                            className={`metricToggleChip ${
                              area.checked ? "metricToggleChipActive" : ""
                            }`}
                          >
                            <input
                              type="checkbox"
                              name="area"
                              value={area.key}
                              defaultChecked={area.checked}
                            />
                            <span>{area.areaName}（{formatNumber(area.count)}）</span>
                          </label>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </details>

          <div className="backtestButtonRow crossBacktestActionRow">
            <button type="submit" className="storeReserveButton backtestPrimaryButton">
              店舗横断バックテストを実行
            </button>
          </div>
        </NativeGetForm>
      </section>

      <ResultUrlTools active={resultRequested} />

      {resultRequested ? (
        detail.rows.length > 0 ? (
          <>
            <section className="cardsGrid summaryStrip">
              <article className="summaryCard">
                <p className="metaLabel">狙い度期間</p>
                <strong className="metaValue">
                  {formatPeriod(detail.startDate, detail.endDate)}
                </strong>
              </article>
              <article className="summaryCard">
                <p className="metaLabel">対象店舗</p>
                <strong className="metaValue">{formatNumber(detail.scannedStoreCount)}店</strong>
              </article>
              <article className="summaryCard">
                <p className="metaLabel">ランキング対象</p>
                <strong className="metaValue">{formatNumber(detail.rankedStoreCount)}店</strong>
              </article>
              <article className="summaryCard">
                <p className="metaLabel">表示件数</p>
                <strong className="metaValue">{formatNumber(detail.rows.length)}件</strong>
              </article>
              <article className="summaryCard">
                <p className="metaLabel">ランキング基準</p>
                <strong className="metaValue">
                  {formatCrossStoreRankingMetricLabel(detail.rankingMetric)}
                </strong>
              </article>
              <article className="summaryCard">
                <p className="metaLabel">対象機種</p>
                <strong className="metaValue">{formatNumber(detail.selectedMachineNames.length)}機種</strong>
              </article>
            </section>
            <StoreRankingTable
              rows={detail.rows}
              rankingMetric={detail.rankingMetric}
            />
          </>
        ) : (
          <section className="statusPanel">
            <h2>条件に合う店舗がありません</h2>
            <p>機種、期間、狙い度条件、最低集計台数を見直してください。</p>
          </section>
        )
      ) : (
        <section className="statusPanel">
          <h2>バックテスト結果はまだ表示していません</h2>
          <p>条件を選ぶと、全店舗を同じ条件で集計して選択した基準で並べます。</p>
        </section>
      )}
    </main>
  );
}
