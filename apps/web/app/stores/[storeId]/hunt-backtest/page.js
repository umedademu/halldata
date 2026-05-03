import Link from "next/link";
import { notFound } from "next/navigation";

import { HuntBacktestBookmarkControl } from "../../../../components/hunt-backtest-bookmark-control";
import { HuntBacktestEventFilterSync } from "../../../../components/hunt-backtest-event-filter-sync";
import { Breadcrumbs } from "../../../../components/breadcrumbs";
import { DataSourceLabel } from "../../../../components/data-source-label";
import { HuntBacktestGraph } from "../../../../components/hunt-backtest-graph";
import { JugglerOnlyButton } from "../../../../components/hunt-machine-filter-tools";
import { NativeGetForm } from "../../../../components/native-get-form";
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

function BacktestResultTable({ title, backtest }) {
  return (
    <section className="tablePanel directoryPanel">
      <div className="tablePanelHeader">
        <div>
          <p className="tablePanelTitle">{title}</p>
          <h2 className="sectionLabel">条件一致分の翌営業日結果</h2>
        </div>
      </div>
      <div className="tableScroller directoryScroller">
        <table className="directoryTable">
          <thead>
            <tr>
              <th className="directoryNameHeader">機種名</th>
              <th>条件一致台数</th>
              <th>狙い度</th>
              <th>偏差値</th>
              <th>実績集計台数</th>
              <th>合計差枚</th>
              <th>合計G数</th>
              <th>BB</th>
              <th>RB</th>
              <th>BB率</th>
              <th>RB率</th>
              <th>合成</th>
              <th>機械割</th>
              <th>平均設定</th>
            </tr>
          </thead>
          <tbody>
            <tr className="backtestTotalRow">
              <th className="directoryNameCell">総計</th>
              <td>{formatNumber(backtest.total.matchedRowCount)}</td>
              <td>{formatDecimal(backtest.total.averageHuntScore)}</td>
              <td>{formatDecimal(backtest.total.averageDeviation)}</td>
              <td>{formatNumber(backtest.total.actualRowCount)}</td>
              <td>{formatSignedNumber(backtest.total.differenceTotal)}</td>
              <td>{formatNumber(backtest.total.gamesTotal)}</td>
              <td>{formatNumber(backtest.total.bbTotal)}</td>
              <td>{formatNumber(backtest.total.rbTotal)}</td>
              <td>{backtest.total.bbProbability ?? "-"}</td>
              <td>{backtest.total.rbProbability ?? "-"}</td>
              <td>{backtest.total.combinedProbability ?? "-"}</td>
              <td>{formatPercent(backtest.total.payoutRate)}</td>
              <td>{formatSettingEstimateScore(backtest.total.averageSetting)}</td>
            </tr>
            {backtest.summaries.map((summary) => (
              <tr
                key={summary.machineName}
                className={getSettingEstimateHighlightClass(summary.averageSetting)}
              >
                <th className="directoryNameCell">{summary.machineName}</th>
                <td>{formatNumber(summary.matchedRowCount)}</td>
                <td>{formatDecimal(summary.averageHuntScore)}</td>
                <td>{formatDecimal(summary.averageDeviation)}</td>
                <td>{formatNumber(summary.actualRowCount)}</td>
                <td>{formatSignedNumber(summary.differenceTotal)}</td>
                <td>{formatNumber(summary.gamesTotal)}</td>
                <td>{formatNumber(summary.bbTotal)}</td>
                <td>{formatNumber(summary.rbTotal)}</td>
                <td>{summary.bbProbability ?? "-"}</td>
                <td>{summary.rbProbability ?? "-"}</td>
                <td>{summary.combinedProbability ?? "-"}</td>
                <td>{formatPercent(summary.payoutRate)}</td>
                <td>{formatSettingEstimateScore(summary.averageSetting)}</td>
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
    differenceMode: readSingleSearchParam(resolvedSearchParams?.differenceMode),
    rankMin: readSingleSearchParam(resolvedSearchParams?.rankMin),
    rankMax: readSingleSearchParam(resolvedSearchParams?.rankMax),
    rankScope: readSingleSearchParam(resolvedSearchParams?.rankScope),
    scoreMin: readSingleSearchParam(resolvedSearchParams?.scoreMin),
    deviationScope: readSingleSearchParam(resolvedSearchParams?.deviationScope),
    deviationMin: hasDeviationMinParam
      ? readSingleSearchParam(resolvedSearchParams?.deviationMin)
      : DEFAULT_DEVIATION_MIN,
    matchMode: readSingleSearchParam(resolvedSearchParams?.matchMode),
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
        )
      : await getHuntScoreInitialPageDetail(storeId, requestedBacktestOptions);
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
    matchMode: detail.backtest.matchMode,
    rankScope: detail.backtest.rankScope,
    deviationScope: detail.backtest.deviationScope,
    combineAimJuggler: detail.backtest.combineAimJuggler,
    combineHanabi: detail.backtest.combineHanabi,
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
          <DataSourceLabel source={detail.dataSource} />
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
                {detail.backtest.hasAimJugglerGroupOption || detail.backtest.hasHanabiGroupOption ? (
                  <div className="machineGroupToggleRow">
                    {detail.backtest.hasAimJugglerGroupOption ? (
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
                        <span>Sアイムとネオアイムをまとめる</span>
                      </label>
                    ) : null}
                    {detail.backtest.hasHanabiGroupOption ? (
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
                        <span>新ハナビとスマハナビをまとめる</span>
                      </label>
                    ) : null}
                  </div>
                ) : null}
                <div className="machineFilterActionRow">
                  <JugglerOnlyButton />
                </div>
                <div className="machineFilterGroups">
                  {machineOptionGroups.map((group) => (
                    <div key={group.key} className="machineFilterGroup">
                      <p className="machineFilterGroupLabel">{group.label}</p>
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

              <div className="backtestFieldGrid">
                <label className="storeReserveField backtestField">
                  <span>順位の開始</span>
                  <input
                    type="number"
                    name="rankMin"
                    min="1"
                    defaultValue={detail.backtest.rankMin ?? ""}
                    className="storeReserveInput"
                  />
                </label>
                <label className="storeReserveField backtestField">
                  <span>順位の終了</span>
                  <input
                    type="number"
                    name="rankMax"
                    min="1"
                    defaultValue={detail.backtest.rankMax ?? ""}
                    className="storeReserveInput"
                  />
                </label>
                <label className="storeReserveField backtestField">
                  <span>狙い度の下限</span>
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
                <label className="storeReserveField backtestField">
                  <span>偏差値の下限</span>
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

              <div className="backtestBlock">
                <p className="filterControlLabel">差枚と機械割の基準</p>
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
                    <span>ボーナス数基準</span>
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
                      detail.backtest.rankScope === "all" ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="rankScope"
                      value="all"
                      defaultChecked={detail.backtest.rankScope === "all"}
                    />
                    <span>全機種順位</span>
                  </label>
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
                      detail.backtest.deviationScope === "all" ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="deviationScope"
                      value="all"
                      defaultChecked={detail.backtest.deviationScope === "all"}
                    />
                    <span>全機種内</span>
                  </label>
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
                <p className="filterControlLabel">順位、狙い度、偏差値を複数入れた時の条件</p>
                <div className="metricToggleRow">
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.matchMode === "and" ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="matchMode"
                      value="and"
                      defaultChecked={detail.backtest.matchMode === "and"}
                    />
                    <span>すべて一致</span>
                  </label>
                  <label
                    className={`metricToggleChip ${
                      detail.backtest.matchMode === "or" ? "metricToggleChipActive" : ""
                    }`}
                  >
                    <input
                      type="radio"
                      name="matchMode"
                      value="or"
                      defaultChecked={detail.backtest.matchMode === "or"}
                    />
                    <span>どれか一致</span>
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
                  <HuntBacktestGraph points={detail.backtest.graphPoints} />
                ) : null}

                {detail.backtest.hasMatches ? (
                  detail.backtest.breakdowns.map((breakdown) => (
                    <BacktestResultTable
                      key={breakdown.key}
                      title={breakdown.title}
                      backtest={breakdown}
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
