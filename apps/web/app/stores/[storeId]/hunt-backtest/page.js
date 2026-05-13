import Link from "next/link";
import { cookies } from "next/headers";
import { notFound } from "next/navigation";

import { HuntBacktestBookmarkControl } from "../../../../components/hunt-backtest-bookmark-control";
import { HuntBacktestEventFilterSync } from "../../../../components/hunt-backtest-event-filter-sync";
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
import {
  getHuntScoreAnalysisPageDetail,
  getHuntScoreInitialPageDetail,
  getStoreIdentity,
} from "../../../../lib/data";
import {
  formatDecimal,
  formatNumber,
  formatPeriod,
  formatPercent,
  formatSignedNumber,
} from "../../../../lib/format";
import { groupHuntMachineOptions } from "../../../../lib/hunt-machine-display";
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
const DEFAULT_DEVIATION_MIN = "60";
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

function BacktestResultTable({ title, backtest, tableId, storeId }) {
  return (
    <section className="tablePanel directoryPanel">
      <SortableTableController tableId={tableId} />
      <div className="tablePanelHeader">
        <div>
          <p className="tablePanelTitle">{title}</p>
          <h2 className="sectionLabel">条件一致分の翌営業日結果</h2>
        </div>
      </div>
      <div className="tableScroller directoryScroller">
        <table id={tableId} className="directoryTable" data-sortable-table="1">
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
              <SortableTableHeader columnIndex={2}>条件一致台数</SortableTableHeader>
              <SortableTableHeader columnIndex={3}>狙い度</SortableTableHeader>
              <SortableTableHeader columnIndex={4}>偏差値</SortableTableHeader>
              <SortableTableHeader columnIndex={5}>次点差</SortableTableHeader>
              <SortableTableHeader columnIndex={6}>実績集計台数</SortableTableHeader>
              <SortableTableHeader columnIndex={7}>合計差枚</SortableTableHeader>
              <SortableTableHeader columnIndex={8}>合計G数</SortableTableHeader>
              <SortableTableHeader columnIndex={9}>BB</SortableTableHeader>
              <SortableTableHeader columnIndex={10}>RB</SortableTableHeader>
              <SortableTableHeader columnIndex={11} initialDirection="asc">
                BB率
              </SortableTableHeader>
              <SortableTableHeader columnIndex={12} initialDirection="asc">
                RB率
              </SortableTableHeader>
              <SortableTableHeader columnIndex={13} initialDirection="asc">
                合成
              </SortableTableHeader>
              <SortableTableHeader columnIndex={14}>機械割</SortableTableHeader>
              <SortableTableHeader columnIndex={15}>平均設定</SortableTableHeader>
            </tr>
          </thead>
          <tbody>
            <tr className="backtestTotalRow" data-sort-fixed="1">
              <th className="directoryNameCell" data-sort-value="総計">総計</th>
              <td data-sort-value={readSortNumber(backtest.total.slotCount)}>{formatNumber(backtest.total.slotCount)}</td>
              <td data-sort-value={backtest.total.matchedRowCount}>{formatNumber(backtest.total.matchedRowCount)}</td>
              <td data-sort-value={readSortNumber(backtest.total.averageHuntScore)}>{formatDecimal(backtest.total.averageHuntScore)}</td>
              <td data-sort-value={readSortNumber(backtest.total.averageDeviation)}>{formatDecimal(backtest.total.averageDeviation)}</td>
              <td data-sort-value={readSortNumber(backtest.total.averageNextGap)}>{formatDecimal(backtest.total.averageNextGap)}</td>
              <td data-sort-value={backtest.total.actualRowCount}>{formatNumber(backtest.total.actualRowCount)}</td>
              <td data-sort-value={backtest.total.differenceTotal}>{formatSignedNumber(backtest.total.differenceTotal)}</td>
              <td data-sort-value={backtest.total.gamesTotal}>{formatNumber(backtest.total.gamesTotal)}</td>
              <td data-sort-value={backtest.total.bbTotal}>{formatNumber(backtest.total.bbTotal)}</td>
              <td data-sort-value={backtest.total.rbTotal}>{formatNumber(backtest.total.rbTotal)}</td>
              <td data-sort-value={readProbabilitySortValue(backtest.total.bbProbability)}>{backtest.total.bbProbability ?? "-"}</td>
              <td data-sort-value={readProbabilitySortValue(backtest.total.rbProbability)}>{backtest.total.rbProbability ?? "-"}</td>
              <td data-sort-value={readProbabilitySortValue(backtest.total.combinedProbability)}>{backtest.total.combinedProbability ?? "-"}</td>
              <td data-sort-value={readSortNumber(backtest.total.payoutRate)}>{formatPercent(backtest.total.payoutRate)}</td>
              <td data-sort-value={readSortNumber(backtest.total.averageSetting)}>{formatSettingEstimateScore(backtest.total.averageSetting)}</td>
            </tr>
            {backtest.summaries.map((summary) => (
              <tr
                key={summary.machineName}
                className={getSettingEstimateHighlightClass(summary.averageSetting)}
              >
                <th className="directoryNameCell" data-sort-value={summary.machineName}>
                  <Link
                    href={`/stores/${storeId}/machines/${encodeURIComponent(summary.machineName)}`}
                    className="directoryPrimaryLink"
                  >
                    {summary.machineName}
                  </Link>
                </th>
                <td data-sort-value={readSortNumber(summary.slotCount)}>{formatNumber(summary.slotCount)}</td>
                <td data-sort-value={summary.matchedRowCount}>{formatNumber(summary.matchedRowCount)}</td>
                <td data-sort-value={readSortNumber(summary.averageHuntScore)}>{formatDecimal(summary.averageHuntScore)}</td>
                <td data-sort-value={readSortNumber(summary.averageDeviation)}>{formatDecimal(summary.averageDeviation)}</td>
                <td data-sort-value={readSortNumber(summary.averageNextGap)}>{formatDecimal(summary.averageNextGap)}</td>
                <td data-sort-value={summary.actualRowCount}>{formatNumber(summary.actualRowCount)}</td>
                <td data-sort-value={summary.differenceTotal}>{formatSignedNumber(summary.differenceTotal)}</td>
                <td data-sort-value={summary.gamesTotal}>{formatNumber(summary.gamesTotal)}</td>
                <td data-sort-value={summary.bbTotal}>{formatNumber(summary.bbTotal)}</td>
                <td data-sort-value={summary.rbTotal}>{formatNumber(summary.rbTotal)}</td>
                <td data-sort-value={readProbabilitySortValue(summary.bbProbability)}>{summary.bbProbability ?? "-"}</td>
                <td data-sort-value={readProbabilitySortValue(summary.rbProbability)}>{summary.rbProbability ?? "-"}</td>
                <td data-sort-value={readProbabilitySortValue(summary.combinedProbability)}>{summary.combinedProbability ?? "-"}</td>
                <td data-sort-value={readSortNumber(summary.payoutRate)}>{formatPercent(summary.payoutRate)}</td>
                <td data-sort-value={readSortNumber(summary.averageSetting)}>{formatSettingEstimateScore(summary.averageSetting)}</td>
              </tr>
            ))}
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
  const hasDeviationMinParam = Object.hasOwn(resolvedSearchParams ?? {}, "deviationMin");
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
    scoreMin: readSingleSearchParam(resolvedSearchParams?.scoreMin),
    deviationScope: readSingleSearchParam(resolvedSearchParams?.deviationScope),
    deviationMin: hasDeviationMinParam
      ? readSingleSearchParam(resolvedSearchParams?.deviationMin)
      : DEFAULT_DEVIATION_MIN,
    nextGapScope: readSingleSearchParam(resolvedSearchParams?.nextGapScope),
    nextGapMin: readSingleSearchParam(resolvedSearchParams?.nextGapMin),
    rankRequired: readMultiSearchParam(resolvedSearchParams?.rankRequired),
    scoreRequired: readMultiSearchParam(resolvedSearchParams?.scoreRequired),
    deviationRequired: readMultiSearchParam(resolvedSearchParams?.deviationRequired),
    nextGapRequired: readMultiSearchParam(resolvedSearchParams?.nextGapRequired),
    dailySelectionMode: readMultiSearchParam(resolvedSearchParams?.dailySelectionMode),
    showGraph: readSingleSearchParam(resolvedSearchParams?.showGraph),
    eventTouched: readSingleSearchParam(resolvedSearchParams?.backtestEventTouched) === "1",
    dayTails: readMultiSearchParam(resolvedSearchParams?.backtestDayTail),
    weekdays: readMultiSearchParam(resolvedSearchParams?.backtestWeekday),
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
  const backtestNoActualNotice =
    resultRequested && detail.backtest.missingActualRowCount > 0
      ? "翌営業日の実績が未取得の台は、実績集計台数と差枚合計などから除外しています。"
      : "";
  const backtestBookmark = {
    startDate: detail.backtest.startDate,
    endDate: detail.backtest.endDate,
    allMachineCount: detail.backtest.machineOptions.length,
    machineNames: detail.backtest.selectedMachineNames,
    rankMin: detail.backtest.rankMin,
    rankMax: detail.backtest.rankMax,
    scoreMin: detail.backtest.scoreMin,
    deviationMin: detail.backtest.deviationMin,
    nextGapMin: detail.backtest.nextGapMin,
    rankRequired: detail.backtest.rankRequired,
    scoreRequired: detail.backtest.scoreRequired,
    deviationRequired: detail.backtest.deviationRequired,
    nextGapRequired: detail.backtest.nextGapRequired,
    rankScope: detail.backtest.rankScope,
    deviationScope: detail.backtest.deviationScope,
    nextGapScope: detail.backtest.nextGapScope,
    scoreDifferenceMode: detail.backtest.scoreDifferenceMode,
    differenceMode: detail.backtest.differenceMode,
    combineAimJuggler: detail.backtest.combineAimJuggler,
    combineHanabi: detail.backtest.combineHanabi,
    dailySelectionMode: detail.backtest.dailySelectionMode,
  };
  const selectedBacktestDayTailSet = new Set(detail.backtest.eventFilters.dayTails);
  const selectedBacktestWeekdaySet = new Set(detail.backtest.eventFilters.weekdays);
  const machineOptionGroups = groupHuntMachineOptions(detail.backtest.machineOptions);

  return (
    <main className="pageStack">
      <HuntBacktestEventFilterSync storeId={detail.store.id} />
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
          {detail.huntScoreLogic ? (
            <p className="dataSourceLabel">適用中: {detail.huntScoreLogic.name}</p>
          ) : null}
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
            <NativeGetForm action={`/stores/${detail.store.id}/hunt-backtest`} className="backtestForm">
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
                    <span>各機種1位から機種内次点差1位を1台選抜</span>
                  </label>
                </div>
                <p className="storeReserveHelp">
                  ONの場合、日ごとに各機種の機種内狙い度1位台を候補にし、その中で機種内次点差が最大の1台だけを集計します。
                </p>
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
                        defaultValue={detail.backtest.rankMin ?? ""}
                        className="storeReserveInput"
                      />
                    </label>
                    <label className="storeReserveField backtestField huntConditionNumberField">
                      <span>終了</span>
                      <input
                        type="number"
                        name="rankMax"
                        min="1"
                        defaultValue={detail.backtest.rankMax ?? ""}
                        className="storeReserveInput"
                      />
                    </label>
                  </div>
                  <input type="hidden" name="rankRequired" value="0" />
                  <label
                    className={`metricToggleChip huntConditionRequired ${
                      detail.backtest.rankRequired ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="checkbox"
                      name="rankRequired"
                      value="1"
                      defaultChecked={detail.backtest.rankRequired}
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
                  <p className="huntConditionLabel">偏差値</p>
                  <div className="huntConditionInputs">
                    <label className="storeReserveField backtestField huntConditionNumberField">
                      <span>下限</span>
                      <input
                        type="number"
                        name="deviationMin"
                        min="0"
                        step="0.1"
                        defaultValue={detail.backtest.deviationMin ?? ""}
                        className="storeReserveInput"
                      />
                    </label>
                  </div>
                  <input type="hidden" name="deviationRequired" value="0" />
                  <label
                    className={`metricToggleChip huntConditionRequired ${
                      detail.backtest.deviationRequired ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="checkbox"
                      name="deviationRequired"
                      value="1"
                      defaultChecked={detail.backtest.deviationRequired}
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
                        defaultValue={detail.backtest.nextGapMin ?? ""}
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
                <p className="filterControlLabel">順位の見方</p>
                <div className="metricToggleRow">
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.rankScope === "selected" ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="rankScope"
                      value="selected"
                      defaultChecked={detail.backtest.rankScope === "selected"}
                    />
                    <span>チェック機種内順位</span>
                  </label>
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.rankScope === "machine" ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="rankScope"
                      value="machine"
                      defaultChecked={detail.backtest.rankScope === "machine"}
                    />
                    <span>機種内順位</span>
                  </label>
                </div>
              </div>

              <div className="backtestBlock">
                <p className="filterControlLabel">偏差値の比較対象</p>
                <div className="metricToggleRow">
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.deviationScope === "selected" ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="deviationScope"
                      value="selected"
                      defaultChecked={detail.backtest.deviationScope === "selected"}
                    />
                    <span>チェック機種内</span>
                  </label>
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.deviationScope === "machine" ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="deviationScope"
                      value="machine"
                      defaultChecked={detail.backtest.deviationScope === "machine"}
                    />
                    <span>機種内</span>
                  </label>
                </div>
              </div>

              <div className="backtestBlock">
                <p className="filterControlLabel">次点差の比較対象</p>
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
                    <p className="metaLabel">条件一致台数</p>
                    <strong className="metaValue">{formatNumber(detail.backtest.matchedRowCount)}台</strong>
                  </article>
                  <article className="summaryCard">
                    <p className="metaLabel">実績集計台数</p>
                    <strong className="metaValue">{formatNumber(detail.backtest.actualRowCount)}台</strong>
                  </article>
                  <article className="summaryCard">
                    <p className="metaLabel">実績未取得台数</p>
                    <strong className="metaValue">{formatNumber(detail.backtest.missingActualRowCount)}台</strong>
                  </article>
                </section>

                <HuntBacktestBookmarkControl storeId={detail.store.id} bookmark={backtestBookmark} />

                {backtestNoActualNotice ? (
                  <p className="filterPanelStatus">{backtestNoActualNotice}</p>
                ) : null}

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
                    <h2>条件に合う台がありません</h2>
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
