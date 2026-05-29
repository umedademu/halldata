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
import { ResultUrlTools } from "../../../../components/result-url-tools";
import { SpecialDayFilterSettings } from "../../../../components/special-day-filter-settings";
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
  SETTING_ESTIMATE_MODE_OPTIONS,
} from "../../../../lib/setting-estimates";

export const dynamic = "force-dynamic";
const DAY_TAIL_OPTIONS = Array.from({ length: 10 }, (_, index) => index);
const HUNT_BACKTEST_FORM_ID = "hunt-backtest-condition-form";
const SETTING_DISTRIBUTION_OPTIONS = [
  { value: "show", label: "表示" },
  { value: "hide", label: "非表示" },
];
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

function formatBacktestGrapeDenominator(value) {
  return Number.isFinite(value) ? value.toFixed(2) : "-";
}

function readSortNumber(value) {
  return Number.isFinite(value) ? value : "";
}

function BacktestMetricCell({ sortValue, children, title }) {
  return (
    <td data-sort-value={sortValue} title={title || undefined}>
      {children}
    </td>
  );
}

function formatBonusCountTitle(label, value) {
  return `${label}: ${formatNumber(value)}`;
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

function SettingDistributionOptions({ value }) {
  return (
    <div className="metricToggleRow commonConditionModeOptions">
      {SETTING_DISTRIBUTION_OPTIONS.map((option) => (
        <label
          key={option.value}
          className={`metricToggleChip ${
            value === option.value ? "metricToggleChipActive" : ""
          }`}
        >
          <input
            type="radio"
            name="settingDistribution"
            value={option.value}
            defaultChecked={value === option.value}
          />
          <span>{option.label}</span>
        </label>
      ))}
    </div>
  );
}

function BacktestNonmatchingSummaryRow({
  parentKey,
  summary,
  label,
  showGrapeColumn = false,
  showSettingDistribution = true,
}) {
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
      <BacktestMetricCell sortValue={readProbabilitySortValue(summary.bbProbability)} title={formatBonusCountTitle("BB", summary.bbTotal)}>{summary.bbProbability ?? "-"}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readProbabilitySortValue(summary.rbProbability)} title={formatBonusCountTitle("RB", summary.rbTotal)}>{summary.rbProbability ?? "-"}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readProbabilitySortValue(summary.combinedProbability)}>{summary.combinedProbability ?? "-"}</BacktestMetricCell>
      {showGrapeColumn ? (
        <BacktestMetricCell sortValue={readSortNumber(summary.grapeDenominator)}>{formatBacktestGrapeDenominator(summary.grapeDenominator)}</BacktestMetricCell>
      ) : null}
      <BacktestMetricCell sortValue={readSortNumber(summary.payoutRate)}>{formatPercent(summary.payoutRate)}</BacktestMetricCell>
      <BacktestMetricCell sortValue={readSortNumber(summary.averageSetting)}>{formatSettingEstimateScore(summary.averageSetting)}</BacktestMetricCell>
      {showSettingDistribution ? (
        <>
          <BacktestMetricCell sortValue={readSortNumber(summary.setting35PlusRate)}>{formatPercent(summary.setting35PlusRate)}</BacktestMetricCell>
          <BacktestMetricCell sortValue={readSortNumber(summary.setting4PlusRate)}>{formatPercent(summary.setting4PlusRate)}</BacktestMetricCell>
          <BacktestMetricCell sortValue={readSortNumber(summary.setting45PlusRate)}>{formatPercent(summary.setting45PlusRate)}</BacktestMetricCell>
          <BacktestMetricCell sortValue={readSortNumber(summary.setting5PlusRate)}>{formatPercent(summary.setting5PlusRate)}</BacktestMetricCell>
        </>
      ) : null}
    </tr>
  );
}

function BacktestResultTable({
  title,
  backtest,
  tableId,
  storeId,
  showGrapeColumn = false,
  showSettingDistribution = true,
}) {
  const totalNonmatchingSummary = backtest.total.nonmatchingSummary;
  const totalRowKey = `${tableId}:total`;
  const grapeColumnOffset = showGrapeColumn ? 1 : 0;

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
        <table id={tableId} className="directoryTable huntCompactTable backtestResultTable huntBacktestResultTable" data-sortable-table="1">
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
              <SortableTableHeader columnIndex={1}>設置数</SortableTableHeader>
              <SortableTableHeader columnIndex={2}>狙い度</SortableTableHeader>
              <SortableTableHeader columnIndex={3}>上差(同)</SortableTableHeader>
              <SortableTableHeader columnIndex={4}>下差(同)</SortableTableHeader>
              <SortableTableHeader columnIndex={5}>集計数</SortableTableHeader>
              <SortableTableHeader columnIndex={6}>勝率</SortableTableHeader>
              <SortableTableHeader columnIndex={7}>合計差枚</SortableTableHeader>
              <SortableTableHeader columnIndex={8}>合計G数</SortableTableHeader>
              <SortableTableHeader columnIndex={9}>平均G</SortableTableHeader>
              <SortableTableHeader columnIndex={10} initialDirection="asc">
                BB率
              </SortableTableHeader>
              <SortableTableHeader columnIndex={11} initialDirection="asc">
                RB率
              </SortableTableHeader>
              <SortableTableHeader columnIndex={12} initialDirection="asc">
                合成
              </SortableTableHeader>
              {showGrapeColumn ? (
                <SortableTableHeader columnIndex={13} initialDirection="asc">ブドウ</SortableTableHeader>
              ) : null}
              <SortableTableHeader columnIndex={13 + grapeColumnOffset}>機械割</SortableTableHeader>
              <SortableTableHeader columnIndex={14 + grapeColumnOffset}>平均設定</SortableTableHeader>
              {showSettingDistribution ? (
                <>
                  <SortableTableHeader columnIndex={15 + grapeColumnOffset}>推定3.5+</SortableTableHeader>
                  <SortableTableHeader columnIndex={16 + grapeColumnOffset}>推定4.0+</SortableTableHeader>
                  <SortableTableHeader columnIndex={17 + grapeColumnOffset}>推定4.5+</SortableTableHeader>
                  <SortableTableHeader columnIndex={18 + grapeColumnOffset}>推定5.0+</SortableTableHeader>
                </>
              ) : null}
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
              <BacktestMetricCell sortValue={readSortNumber(backtest.total.averageUpperGap)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="上差(同)" nonmatchingValue={formatDecimal(totalNonmatchingSummary?.averageUpperGap)}>{formatDecimal(backtest.total.averageUpperGap)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readSortNumber(backtest.total.averageNextGap)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="下差(同)" nonmatchingValue={formatDecimal(totalNonmatchingSummary?.averageNextGap)}>{formatDecimal(backtest.total.averageNextGap)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={backtest.total.actualRowCount} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="集計台数" nonmatchingValue={formatNumber(totalNonmatchingSummary?.actualRowCount)}>{formatNumber(backtest.total.actualRowCount)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readSortNumber(backtest.total.winRate)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="勝率" nonmatchingValue={formatPercent(totalNonmatchingSummary?.winRate)}>{formatPercent(backtest.total.winRate)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={backtest.total.differenceTotal} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="合計差枚" nonmatchingValue={formatSignedNumber(totalNonmatchingSummary?.differenceTotal)}>{formatSignedNumber(backtest.total.differenceTotal)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={backtest.total.gamesTotal} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="合計G数" nonmatchingValue={formatNumber(totalNonmatchingSummary?.gamesTotal)}>{formatNumber(backtest.total.gamesTotal)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readSortNumber(backtest.total.averageGames)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="平均G" nonmatchingValue={formatAverageGames(totalNonmatchingSummary?.averageGames)}>{formatAverageGames(backtest.total.averageGames)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readProbabilitySortValue(backtest.total.bbProbability)} title={formatBonusCountTitle("BB", backtest.total.bbTotal)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="BB率" nonmatchingValue={totalNonmatchingSummary?.bbProbability ?? "-"}>{backtest.total.bbProbability ?? "-"}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readProbabilitySortValue(backtest.total.rbProbability)} title={formatBonusCountTitle("RB", backtest.total.rbTotal)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="RB率" nonmatchingValue={totalNonmatchingSummary?.rbProbability ?? "-"}>{backtest.total.rbProbability ?? "-"}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readProbabilitySortValue(backtest.total.combinedProbability)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="合成" nonmatchingValue={totalNonmatchingSummary?.combinedProbability ?? "-"}>{backtest.total.combinedProbability ?? "-"}</BacktestMetricCell>
              {showGrapeColumn ? (
                <BacktestMetricCell sortValue={readSortNumber(backtest.total.grapeDenominator)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="ブドウ" nonmatchingValue={formatBacktestGrapeDenominator(totalNonmatchingSummary?.grapeDenominator)}>{formatBacktestGrapeDenominator(backtest.total.grapeDenominator)}</BacktestMetricCell>
              ) : null}
              <BacktestMetricCell sortValue={readSortNumber(backtest.total.payoutRate)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="機械割" nonmatchingValue={formatPercent(totalNonmatchingSummary?.payoutRate)}>{formatPercent(backtest.total.payoutRate)}</BacktestMetricCell>
              <BacktestMetricCell sortValue={readSortNumber(backtest.total.averageSetting)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="平均設定" nonmatchingValue={formatSettingEstimateScore(totalNonmatchingSummary?.averageSetting)}>{formatSettingEstimateScore(backtest.total.averageSetting)}</BacktestMetricCell>
              {showSettingDistribution ? (
                <>
                  <BacktestMetricCell sortValue={readSortNumber(backtest.total.setting35PlusRate)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="推定3.5+" nonmatchingValue={formatPercent(totalNonmatchingSummary?.setting35PlusRate)}>{formatPercent(backtest.total.setting35PlusRate)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readSortNumber(backtest.total.setting4PlusRate)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="推定4.0+" nonmatchingValue={formatPercent(totalNonmatchingSummary?.setting4PlusRate)}>{formatPercent(backtest.total.setting4PlusRate)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readSortNumber(backtest.total.setting45PlusRate)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="推定4.5+" nonmatchingValue={formatPercent(totalNonmatchingSummary?.setting45PlusRate)}>{formatPercent(backtest.total.setting45PlusRate)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readSortNumber(backtest.total.setting5PlusRate)} nonmatchingSummary={totalNonmatchingSummary} nonmatchingLabel="推定5.0+" nonmatchingValue={formatPercent(totalNonmatchingSummary?.setting5PlusRate)}>{formatPercent(backtest.total.setting5PlusRate)}</BacktestMetricCell>
                </>
              ) : null}
            </tr>
            <BacktestNonmatchingSummaryRow
              parentKey={totalRowKey}
              summary={totalNonmatchingSummary}
              label="非該当台 合計"
              showGrapeColumn={showGrapeColumn}
              showSettingDistribution={showSettingDistribution}
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
                  <BacktestMetricCell sortValue={readSortNumber(summary.averageUpperGap)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="上差(同)" nonmatchingValue={formatDecimal(nonmatchingSummary?.averageUpperGap)}>{formatDecimal(summary.averageUpperGap)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readSortNumber(summary.averageNextGap)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="下差(同)" nonmatchingValue={formatDecimal(nonmatchingSummary?.averageNextGap)}>{formatDecimal(summary.averageNextGap)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={summary.actualRowCount} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="集計台数" nonmatchingValue={formatNumber(nonmatchingSummary?.actualRowCount)}>{formatNumber(summary.actualRowCount)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readSortNumber(summary.winRate)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="勝率" nonmatchingValue={formatPercent(nonmatchingSummary?.winRate)}>{formatPercent(summary.winRate)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={summary.differenceTotal} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="合計差枚" nonmatchingValue={formatSignedNumber(nonmatchingSummary?.differenceTotal)}>{formatSignedNumber(summary.differenceTotal)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={summary.gamesTotal} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="合計G数" nonmatchingValue={formatNumber(nonmatchingSummary?.gamesTotal)}>{formatNumber(summary.gamesTotal)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readSortNumber(summary.averageGames)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="平均G" nonmatchingValue={formatAverageGames(nonmatchingSummary?.averageGames)}>{formatAverageGames(summary.averageGames)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readProbabilitySortValue(summary.bbProbability)} title={formatBonusCountTitle("BB", summary.bbTotal)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="BB率" nonmatchingValue={nonmatchingSummary?.bbProbability ?? "-"}>{summary.bbProbability ?? "-"}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readProbabilitySortValue(summary.rbProbability)} title={formatBonusCountTitle("RB", summary.rbTotal)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="RB率" nonmatchingValue={nonmatchingSummary?.rbProbability ?? "-"}>{summary.rbProbability ?? "-"}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readProbabilitySortValue(summary.combinedProbability)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="合成" nonmatchingValue={nonmatchingSummary?.combinedProbability ?? "-"}>{summary.combinedProbability ?? "-"}</BacktestMetricCell>
                  {showGrapeColumn ? (
                    <BacktestMetricCell sortValue={readSortNumber(summary.grapeDenominator)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="ブドウ" nonmatchingValue={formatBacktestGrapeDenominator(nonmatchingSummary?.grapeDenominator)}>{formatBacktestGrapeDenominator(summary.grapeDenominator)}</BacktestMetricCell>
                  ) : null}
                  <BacktestMetricCell sortValue={readSortNumber(summary.payoutRate)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="機械割" nonmatchingValue={formatPercent(nonmatchingSummary?.payoutRate)}>{formatPercent(summary.payoutRate)}</BacktestMetricCell>
                  <BacktestMetricCell sortValue={readSortNumber(summary.averageSetting)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="平均設定" nonmatchingValue={formatSettingEstimateScore(nonmatchingSummary?.averageSetting)}>{formatSettingEstimateScore(summary.averageSetting)}</BacktestMetricCell>
                  {showSettingDistribution ? (
                    <>
                      <BacktestMetricCell sortValue={readSortNumber(summary.setting35PlusRate)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="推定3.5+" nonmatchingValue={formatPercent(nonmatchingSummary?.setting35PlusRate)}>{formatPercent(summary.setting35PlusRate)}</BacktestMetricCell>
                      <BacktestMetricCell sortValue={readSortNumber(summary.setting4PlusRate)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="推定4.0+" nonmatchingValue={formatPercent(nonmatchingSummary?.setting4PlusRate)}>{formatPercent(summary.setting4PlusRate)}</BacktestMetricCell>
                      <BacktestMetricCell sortValue={readSortNumber(summary.setting45PlusRate)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="推定4.5+" nonmatchingValue={formatPercent(nonmatchingSummary?.setting45PlusRate)}>{formatPercent(summary.setting45PlusRate)}</BacktestMetricCell>
                      <BacktestMetricCell sortValue={readSortNumber(summary.setting5PlusRate)} nonmatchingSummary={nonmatchingSummary} nonmatchingLabel="推定5.0+" nonmatchingValue={formatPercent(nonmatchingSummary?.setting5PlusRate)}>{formatPercent(summary.setting5PlusRate)}</BacktestMetricCell>
                    </>
                  ) : null}
                </tr>
                <BacktestNonmatchingSummaryRow
                  parentKey={rowKey}
                  summary={nonmatchingSummary}
                  label="非該当台"
                  showGrapeColumn={showGrapeColumn}
                  showSettingDistribution={showSettingDistribution}
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
    settingEstimateMode: readSingleSearchParam(resolvedSearchParams?.settingEstimateMode),
    settingDistribution: readSingleSearchParam(resolvedSearchParams?.settingDistribution),
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
    machineNextGapMin: readSingleSearchParam(resolvedSearchParams?.machineNextGapMin),
    machineNextGapMax: readSingleSearchParam(resolvedSearchParams?.machineNextGapMax),
    selectedNextGapMin: readSingleSearchParam(resolvedSearchParams?.selectedNextGapMin),
    selectedNextGapMax: readSingleSearchParam(resolvedSearchParams?.selectedNextGapMax),
    machineUpperGapMin: readSingleSearchParam(resolvedSearchParams?.machineUpperGapMin),
    machineUpperGapMax: readSingleSearchParam(resolvedSearchParams?.machineUpperGapMax),
    selectedUpperGapMin: readSingleSearchParam(resolvedSearchParams?.selectedUpperGapMin),
    selectedUpperGapMax: readSingleSearchParam(resolvedSearchParams?.selectedUpperGapMax),
    rankRequired: readMultiSearchParam(resolvedSearchParams?.rankRequired),
    machineRankRequired: readMultiSearchParam(resolvedSearchParams?.machineRankRequired),
    selectedRankRequired: readMultiSearchParam(resolvedSearchParams?.selectedRankRequired),
    scoreRequired: readMultiSearchParam(resolvedSearchParams?.scoreRequired),
    nextGapRequired: readMultiSearchParam(resolvedSearchParams?.nextGapRequired),
    upperGapRequired: readMultiSearchParam(resolvedSearchParams?.upperGapRequired),
    machineNextGapRequired: readMultiSearchParam(resolvedSearchParams?.machineNextGapRequired),
    selectedNextGapRequired: readMultiSearchParam(resolvedSearchParams?.selectedNextGapRequired),
    machineUpperGapRequired: readMultiSearchParam(resolvedSearchParams?.machineUpperGapRequired),
    selectedUpperGapRequired: readMultiSearchParam(resolvedSearchParams?.selectedUpperGapRequired),
    dailySelectionMode: readMultiSearchParam(resolvedSearchParams?.dailySelectionMode),
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
  const machineOptionGroups = groupHuntMachineOptions(detail.backtest.machineOptions, {
    combineAimJuggler: detail.backtest.combineAimJuggler,
    combineHanabi: detail.backtest.combineHanabi,
  });
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
    machineNextGapMin: detail.backtest.machineNextGapMin ?? "",
    machineNextGapMax: detail.backtest.machineNextGapMax ?? "",
    selectedNextGapMin: detail.backtest.selectedNextGapMin ?? "",
    selectedNextGapMax: detail.backtest.selectedNextGapMax ?? "",
    machineUpperGapMin: detail.backtest.machineUpperGapMin ?? "",
    machineUpperGapMax: detail.backtest.machineUpperGapMax ?? "",
    selectedUpperGapMin: detail.backtest.selectedUpperGapMin ?? "",
    selectedUpperGapMax: detail.backtest.selectedUpperGapMax ?? "",
    rankRequired: detail.backtest.rankRequired,
    machineRankRequired: detail.backtest.machineRankRequired,
    selectedRankRequired: detail.backtest.selectedRankRequired,
    scoreRequired: detail.backtest.scoreRequired,
    machineNextGapRequired: detail.backtest.machineNextGapRequired,
    selectedNextGapRequired: detail.backtest.selectedNextGapRequired,
    machineUpperGapRequired: detail.backtest.machineUpperGapRequired,
    selectedUpperGapRequired: detail.backtest.selectedUpperGapRequired,
    scoreDifferenceMode: detail.backtest.scoreDifferenceMode,
    differenceMode: detail.backtest.differenceMode,
    settingEstimateMode: detail.backtest.settingEstimateMode,
    settingDistribution: detail.backtest.settingDistribution,
    rankScope: detail.backtest.rankScope,
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

              <div className="filterConditionBox">
                <p className="filterConditionBoxTitle">期間指定</p>
                <div className="periodConditionRow">
                  <div className="periodModeGroup">
                    <p className="filterControlLabel">指定方法</p>
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
                  <div className="periodInputGroup">
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
                </div>
              </div>

              <div className="filterConditionBox">
                <p className="filterConditionBoxTitle">特定日指定</p>
                <SpecialDayFilterSettings
                  storeId={detail.store.id}
                  dayTailOptions={DAY_TAIL_OPTIONS}
                  weekdayOptions={WEEKDAY_OPTIONS}
                  selectedDayTails={detail.backtest.eventFilters.dayTails}
                  selectedMonthDays={detail.backtest.eventFilters.monthDays}
                  selectedWeekdays={detail.backtest.eventFilters.weekdays}
                  zoro={detail.backtest.eventFilters.zoro}
                  preferInitialValues={requestedBacktestOptions.eventTouched}
                />
              </div>

              <div className="filterConditionBox">
                <p className="filterConditionBoxTitle">機種選択</p>
                <input type="hidden" name="machineTouched" value="1" />
                <AllMachineFilterButtons enableSlotCountSelection />
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
                      minValue={detail.backtest.scoreMin}
                      maxValue={detail.backtest.scoreMax}
                      requiredValue={detail.backtest.scoreRequired}
                    />
                    <div className="commonConditionMode">
                      <p className="commonConditionSubLabel">狙い度計算の差枚基準</p>
                      <ScoreDifferenceModeOptions value={detail.backtest.scoreDifferenceMode} />
                    </div>
                    <div className="commonConditionMode">
                      <p className="commonConditionSubLabel">設定推定基準</p>
                      <SettingEstimateModeOptions value={detail.backtest.settingEstimateMode} />
                    </div>
                    <div className="commonConditionMode">
                      <p className="commonConditionSubLabel">設定分布を表示</p>
                      <SettingDistributionOptions value={detail.backtest.settingDistribution} />
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
                      minValue={detail.backtest.machineRankMin}
                      maxValue={detail.backtest.machineRankMax}
                      requiredValue={detail.backtest.machineRankRequired}
                      minLabel="開始"
                      maxLabel="終了"
                      inputMin="1"
                      inputMax={undefined}
                      inputStep={undefined}
                    />
                    <ScopedConditionRow
                      label="上差(同)"
                      minName="machineUpperGapMin"
                      maxName="machineUpperGapMax"
                      requiredName="machineUpperGapRequired"
                      minValue={detail.backtest.machineUpperGapMin}
                      maxValue={detail.backtest.machineUpperGapMax}
                      requiredValue={detail.backtest.machineUpperGapRequired}
                    />
                    <ScopedConditionRow
                      label="下差(同)"
                      minName="machineNextGapMin"
                      maxName="machineNextGapMax"
                      requiredName="machineNextGapRequired"
                      minValue={detail.backtest.machineNextGapMin}
                      maxValue={detail.backtest.machineNextGapMax}
                      requiredValue={detail.backtest.machineNextGapRequired}
                    />
                  </div>
                  <div className="scopedConditionColumn">
                    <p className="scopedConditionColumnTitle">選択機種内</p>
                    <ScopedConditionRow
                      label="順位"
                      minName="selectedRankMin"
                      maxName="selectedRankMax"
                      requiredName="selectedRankRequired"
                      minValue={detail.backtest.selectedRankMin}
                      maxValue={detail.backtest.selectedRankMax}
                      requiredValue={detail.backtest.selectedRankRequired}
                      minLabel="開始"
                      maxLabel="終了"
                      inputMin="1"
                      inputMax={undefined}
                      inputStep={undefined}
                    />
                    <ScopedConditionRow
                      label="上差(全)"
                      minName="selectedUpperGapMin"
                      maxName="selectedUpperGapMax"
                      requiredName="selectedUpperGapRequired"
                      minValue={detail.backtest.selectedUpperGapMin}
                      maxValue={detail.backtest.selectedUpperGapMax}
                      requiredValue={detail.backtest.selectedUpperGapRequired}
                    />
                    <ScopedConditionRow
                      label="下差(全)"
                      minName="selectedNextGapMin"
                      maxName="selectedNextGapMax"
                      requiredName="selectedNextGapRequired"
                      minValue={detail.backtest.selectedNextGapMin}
                      maxValue={detail.backtest.selectedNextGapMax}
                      requiredValue={detail.backtest.selectedNextGapRequired}
                    />
                  </div>
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
              </div>

              <HuntBacktestBookmarkControl
                storeId={detail.store.id}
                formId={HUNT_BACKTEST_FORM_ID}
                allMachineCount={detail.backtest.machineOptions.length}
              />

              <div className="backtestButtonRow">
                <button type="submit" className="storeReserveButton backtestPrimaryButton">
                  バックテストを実行
                </button>
              </div>
            </NativeGetForm>
            {backtestFallbackNotice ? <p className="storeReserveHelp">{backtestFallbackNotice}</p> : null}
          </section>

          <ResultUrlTools active={resultRequested} />

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

                {detail.backtest.graphPoints.length > 0 ? (
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
                      showGrapeColumn={detail.backtest.showGrapeColumn}
                      showSettingDistribution={detail.backtest.showSettingDistribution}
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
