import Link from "next/link";

import {
  AllMachineFilterButtons,
  MachineFilterCategoryButton,
} from "../../components/hunt-machine-filter-tools";
import { NativeGetForm } from "../../components/native-get-form";
import { SortableTableController } from "../../components/sortable-table-controller";
import { SortableTableHeader } from "../../components/sortable-table-header";
import { getCrossStoreBacktestDetail } from "../../lib/data";
import {
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

function formatCrossStoreRankingMetricLabel(rankingMetric) {
  return rankingMetric === "differenceTotal" ? "合計差枚" : "平均機械割";
}

function StoreRankingTable({ rows, rankingMetric }) {
  const tableId = "cross-store-backtest-results";
  const rankingMetricLabel = formatCrossStoreRankingMetricLabel(rankingMetric);

  return (
    <section className="tablePanel directoryPanel">
      <SortableTableController tableId={tableId} />
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
              <SortableTableHeader columnIndex={7}>設定</SortableTableHeader>
              <SortableTableHeader columnIndex={8}>集計台数</SortableTableHeader>
              <SortableTableHeader columnIndex={9}>対象日数</SortableTableHeader>
              <SortableTableHeader columnIndex={10}>集計日数</SortableTableHeader>
              <SortableTableHeader columnIndex={11}>対象機種</SortableTableHeader>
              <SortableTableHeader columnIndex={12}>設置台数</SortableTableHeader>
              <SortableTableHeader columnIndex={13}>狙い度</SortableTableHeader>
              <SortableTableHeader columnIndex={14}>次点差</SortableTableHeader>
              <SortableTableHeader columnIndex={15}>BB</SortableTableHeader>
              <SortableTableHeader columnIndex={16}>RB</SortableTableHeader>
              <SortableTableHeader columnIndex={17} initialDirection="asc">
                BB率
              </SortableTableHeader>
              <SortableTableHeader columnIndex={18} initialDirection="asc">
                RB率
              </SortableTableHeader>
              <SortableTableHeader columnIndex={19} initialDirection="asc">
                合成
              </SortableTableHeader>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr
                key={row.store.id}
                className={getSettingEstimateHighlightClass(row.averageSetting)}
              >
                <td data-sort-value={row.rank}>{formatNumber(row.rank)}</td>
                <th className="directoryNameCell" data-sort-value={row.store.storeName}>
                  <Link href={`/stores/${row.store.id}`}>{row.store.storeName}</Link>
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
                <td data-sort-value={readSortNumber(row.payoutRate)}>
                  {formatPercent(row.payoutRate)}
                </td>
                <td data-sort-value={readSortNumber(row.averageDifference)}>
                  {formatAverageDifference(row.averageDifference)}
                </td>
                <td data-sort-value={row.differenceTotal}>
                  {formatSignedNumber(row.differenceTotal)}
                </td>
                <td data-sort-value={row.gamesTotal}>{formatNumber(row.gamesTotal)}</td>
                <td data-sort-value={readSortNumber(row.averageSetting)}>
                  {formatSettingEstimateScore(row.averageSetting)}
                </td>
                <td data-sort-value={row.actualRowCount}>{formatNumber(row.actualRowCount)}</td>
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
                <td data-sort-value={readSortNumber(row.averageHuntScore)}>
                  {formatDecimal(row.averageHuntScore)}
                </td>
                <td data-sort-value={readSortNumber(row.averageNextGap)}>
                  {formatDecimal(row.averageNextGap)}
                </td>
                <td data-sort-value={row.bbTotal}>{formatNumber(row.bbTotal)}</td>
                <td data-sort-value={row.rbTotal}>{formatNumber(row.rbTotal)}</td>
                <td data-sort-value={readProbabilitySortValue(row.bbProbability)}>
                  {row.bbProbability ?? "-"}
                </td>
                <td data-sort-value={readProbabilitySortValue(row.rbProbability)}>
                  {row.rbProbability ?? "-"}
                </td>
                <td data-sort-value={readProbabilitySortValue(row.combinedProbability)}>
                  {row.combinedProbability ?? "-"}
                </td>
              </tr>
            ))}
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
    rankMin: readOptionalSearchParam(resolvedSearchParams, "rankMin"),
    rankMax: readOptionalSearchParam(resolvedSearchParams, "rankMax"),
    rankScope: readSingleSearchParam(resolvedSearchParams?.rankScope),
    scoreMin: readOptionalSearchParam(resolvedSearchParams, "scoreMin"),
    nextGapScope: readSingleSearchParam(resolvedSearchParams?.nextGapScope),
    nextGapMin: readOptionalSearchParam(resolvedSearchParams, "nextGapMin"),
    rankRequired: readMultiSearchParam(resolvedSearchParams?.rankRequired),
    scoreRequired: readMultiSearchParam(resolvedSearchParams?.scoreRequired),
    nextGapRequired: readMultiSearchParam(resolvedSearchParams?.nextGapRequired),
    prefectures: readMultiSearchParam(resolvedSearchParams?.prefecture),
    areaKeys: readMultiSearchParam(resolvedSearchParams?.area),
    dayTails: readMultiSearchParam(resolvedSearchParams?.backtestDayTail),
    weekdays: readMultiSearchParam(resolvedSearchParams?.backtestWeekday),
    minActualRows: readSingleSearchParam(resolvedSearchParams?.minActualRows),
    minMatchedDateCount: readSingleSearchParam(resolvedSearchParams?.minMatchedDateCount),
    minSlotCount: readSingleSearchParam(resolvedSearchParams?.minSlotCount),
    maxSlotCount: readSingleSearchParam(resolvedSearchParams?.maxSlotCount),
    limit: readSingleSearchParam(resolvedSearchParams?.limit),
    rankingMetric: readSingleSearchParam(resolvedSearchParams?.rankingMetric),
  });
  const selectedDayTailSet = new Set(detail.eventFilters.dayTails);
  const selectedWeekdaySet = new Set(detail.eventFilters.weekdays);
  const machineOptionGroups = groupHuntMachineOptions(detail.machineOptions);
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
          <input type="hidden" name="aimMachineGroup" value="0" />
          <input type="hidden" name="hanabiMachineGroup" value="0" />

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

          <div className="backtestBlock">
            <p className="filterControlLabel">期間の指定方法</p>
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

          <div className="backtestFieldGrid">
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

          <div className="backtestBlock">
            <p className="filterControlLabel">特定日（翌営業日の末尾）</p>
            <div className="metricToggleRow">
              {DAY_TAIL_OPTIONS.map((dayTail) => (
                <label
                  key={dayTail}
                  className={`metricToggleChip ${
                    selectedDayTailSet.has(dayTail) ? "metricToggleChipActive" : ""
                  }`}
                >
                  <input
                    type="checkbox"
                    name="backtestDayTail"
                    value={dayTail}
                    defaultChecked={selectedDayTailSet.has(dayTail)}
                  />
                  <span>{dayTail}</span>
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
                    selectedWeekdaySet.has(weekday.value) ? "metricToggleChipActive" : ""
                  }`}
                >
                  <input
                    type="checkbox"
                    name="backtestWeekday"
                    value={weekday.value}
                    defaultChecked={selectedWeekdaySet.has(weekday.value)}
                  />
                  <span>{weekday.label}</span>
                </label>
              ))}
            </div>
          </div>

          <div className="backtestBlock">
            <p className="filterControlLabel">機種名</p>
            <AllMachineFilterButtons />
            <div className="machineFilterGroups">
              {machineOptionGroups.map((group) => (
                <div key={group.key} className="machineFilterGroup">
                  <p className="machineFilterGroupLabel">{group.label}</p>
                  <div className="machineGroupToggleRow">
                    {group.key === "juggler" ? (
                      <label
                        className={`metricToggleChip ${
                          detail.combineAimJuggler ? "metricToggleChipActive" : ""
                        }`}
                      >
                        <input
                          type="checkbox"
                          name="aimMachineGroup"
                          value="1"
                          defaultChecked={detail.combineAimJuggler}
                        />
                        <span>アイジャグをまとめる</span>
                      </label>
                    ) : null}
                    {group.key === "hanabi" ? (
                      <label
                        className={`metricToggleChip ${
                          detail.combineHanabi ? "metricToggleChipActive" : ""
                        }`}
                      >
                        <input
                          type="checkbox"
                          name="hanabiMachineGroup"
                          value="1"
                          defaultChecked={detail.combineHanabi}
                        />
                        <span>ハナビをまとめる</span>
                      </label>
                    ) : null}
                    <MachineFilterCategoryButton
                      category={group.key}
                      label={`${group.label}のみ選択`}
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
                        <span>{machine.shortName}</span>
                      </label>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="huntConditionRows">
            <div className="huntConditionRow">
              <p className="huntConditionLabel">順位</p>
              <div className="huntConditionInputs">
                <label className="storeReserveField backtestField huntConditionNumberField">
                  <span>開始</span>
                  <input
                    type="number"
                    name="rankMin"
                    min="1"
                    defaultValue={detail.rankMin ?? ""}
                    className="storeReserveInput"
                  />
                </label>
                <label className="storeReserveField backtestField huntConditionNumberField">
                  <span>終了</span>
                  <input
                    type="number"
                    name="rankMax"
                    min="1"
                    defaultValue={detail.rankMax ?? ""}
                    className="storeReserveInput"
                  />
                </label>
              </div>
              <input type="hidden" name="rankRequired" value="0" />
              <label
                className={`metricToggleChip huntConditionRequired ${
                  detail.rankRequired ? "metricToggleChipActive" : ""
                }`}
              >
                <input
                  type="checkbox"
                  name="rankRequired"
                  value="1"
                  defaultChecked={detail.rankRequired}
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
                    defaultValue={detail.scoreMin ?? ""}
                    className="storeReserveInput"
                  />
                </label>
              </div>
              <input type="hidden" name="scoreRequired" value="0" />
              <label
                className={`metricToggleChip huntConditionRequired ${
                  detail.scoreRequired ? "metricToggleChipActive" : ""
                }`}
              >
                <input
                  type="checkbox"
                  name="scoreRequired"
                  value="1"
                  defaultChecked={detail.scoreRequired}
                />
                <span>必須</span>
              </label>
            </div>
            <div className="huntConditionRow">
              <p className="huntConditionLabel">次点差</p>
              <div className="huntConditionInputs">
                <label className="storeReserveField backtestField huntConditionNumberField">
                  <span>下限</span>
                  <input
                    type="number"
                    name="nextGapMin"
                    min="0"
                    max="100"
                    step="0.1"
                    defaultValue={detail.nextGapMin ?? ""}
                    className="storeReserveInput"
                  />
                </label>
              </div>
              <input type="hidden" name="nextGapRequired" value="0" />
              <label
                className={`metricToggleChip huntConditionRequired ${
                  detail.nextGapRequired ? "metricToggleChipActive" : ""
                }`}
              >
                <input
                  type="checkbox"
                  name="nextGapRequired"
                  value="1"
                  defaultChecked={detail.nextGapRequired}
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
                  detail.scoreDifferenceMode === "bonus" ? "metricToggleChipActive" : ""
                }`}
              >
                <input
                  type="radio"
                  name="scoreDifferenceMode"
                  value="bonus"
                  defaultChecked={detail.scoreDifferenceMode === "bonus"}
                />
                <span>設定1基準</span>
              </label>
              <label
                className={`metricToggleChip ${
                  detail.scoreDifferenceMode === "estimated" ? "metricToggleChipActive" : ""
                }`}
              >
                <input
                  type="radio"
                  name="scoreDifferenceMode"
                  value="estimated"
                  defaultChecked={detail.scoreDifferenceMode === "estimated"}
                />
                <span>推定設定基準</span>
              </label>
              <label
                className={`metricToggleChip ${
                  detail.scoreDifferenceMode === "minrepo" ? "metricToggleChipActive" : ""
                }`}
              >
                <input
                  type="radio"
                  name="scoreDifferenceMode"
                  value="minrepo"
                  defaultChecked={detail.scoreDifferenceMode === "minrepo"}
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

          <div className="backtestBlock">
            <p className="filterControlLabel">順位の見方</p>
            <div className="metricToggleRow">
              {[
                { value: "selected", label: "チェック機種内順位" },
                { value: "machine", label: "機種内順位" },
              ].map((scope) => (
                <label
                  key={scope.value}
                  className={`metricToggleChip ${
                    detail.rankScope === scope.value ? "metricToggleChipActive" : ""
                  }`}
                >
                  <input
                    type="radio"
                    name="rankScope"
                    value={scope.value}
                    defaultChecked={detail.rankScope === scope.value}
                  />
                  <span>{scope.label}</span>
                </label>
              ))}
            </div>
          </div>

          <div className="backtestBlock">
            <p className="filterControlLabel">次点差の比較対象</p>
            <div className="metricToggleRow">
              {[
                { value: "selected", label: "チェック機種内" },
                { value: "machine", label: "機種内" },
              ].map((scope) => (
                <label
                  key={scope.value}
                  className={`metricToggleChip ${
                    detail.nextGapScope === scope.value ? "metricToggleChipActive" : ""
                  }`}
                >
                  <input
                    type="radio"
                    name="nextGapScope"
                    value={scope.value}
                    defaultChecked={detail.nextGapScope === scope.value}
                  />
                  <span>{scope.label}</span>
                </label>
              ))}
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
            <button type="submit" className="storeReserveButton">
              店舗横断バックテストする
            </button>
          </div>
        </NativeGetForm>
      </section>

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
