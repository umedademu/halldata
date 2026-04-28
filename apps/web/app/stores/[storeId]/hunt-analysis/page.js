import Link from "next/link";
import { notFound } from "next/navigation";

import { HuntBacktestBookmarkControl } from "../../../../components/hunt-backtest-bookmark-control";
import { HuntBacktestEventFilterSync } from "../../../../components/hunt-backtest-event-filter-sync";
import { Breadcrumbs } from "../../../../components/breadcrumbs";
import { HuntBacktestGraph } from "../../../../components/hunt-backtest-graph";
import { HuntRankingLimitSync } from "../../../../components/hunt-ranking-limit-sync";
import { HuntRankingTable } from "../../../../components/hunt-ranking-table";
import { NativeGetForm } from "../../../../components/native-get-form";
import { getHuntScoreAnalysisPageDetail, getStoreIdentity } from "../../../../lib/data";
import {
  formatCompactDate,
  formatDecimal,
  formatNumber,
  formatPeriod,
  formatPercent,
  formatSignedNumber,
} from "../../../../lib/format";
import {
  formatSettingEstimateScore,
  getSettingEstimateHighlightClass,
} from "../../../../lib/setting-estimates";

export const dynamic = "force-dynamic";
const DEFAULT_RANKING_LIMIT = 20;
const DAY_TAIL_OPTIONS = Array.from({ length: 10 }, (_, index) => index);
const WEEKDAY_OPTIONS = [
  { value: 0, label: "日曜" },
  { value: 1, label: "月曜" },
  { value: 2, label: "火曜" },
  { value: 3, label: "水曜" },
  { value: 4, label: "木曜" },
  { value: 5, label: "金曜" },
  { value: 6, label: "土曜" },
];
const BACKTEST_SEARCH_KEYS = [
  "periodMode",
  "recentDays",
  "startDate",
  "endDate",
  "machine",
  "aimMachineGroup",
  "differenceMode",
  "rankMin",
  "rankMax",
  "rankScope",
  "scoreMin",
  "matchMode",
  "showGraph",
  "backtestEventTouched",
  "backtestDayTail",
  "backtestWeekday",
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

function parseRequestedLimit(value) {
  const parsedValue = Number(value);
  if (!Number.isInteger(parsedValue) || parsedValue < 1) {
    return DEFAULT_RANKING_LIMIT;
  }
  return parsedValue;
}

function renderHiddenSearchParams(searchParams, excludedNames) {
  const excludedNameSet = new Set(excludedNames);
  const hiddenInputs = [];

  for (const [name, rawValue] of Object.entries(searchParams ?? {})) {
    if (excludedNameSet.has(name)) {
      continue;
    }

    const values = Array.isArray(rawValue) ? rawValue : [rawValue];
    values.forEach((value, index) => {
      if (typeof value !== "string" || value.length === 0) {
        return;
      }

      hiddenInputs.push(
        <input
          key={`${name}-${index}-${value}`}
          type="hidden"
          name={name}
          value={value}
        />,
      );
    });
  }

  return hiddenInputs;
}

function BacktestResultTable({ title, backtest }) {
  return (
    <section className="tablePanel directoryPanel">
      <div className="tablePanelHeader">
        <div>
          <p className="sectionLabel">{title}</p>
          <h2 className="tablePanelTitle">条件一致分の翌営業日結果</h2>
        </div>
      </div>
      <div className="tableScroller directoryScroller">
        <table className="directoryTable">
          <thead>
            <tr>
              <th className="directoryNameHeader">機種名</th>
              <th>条件一致台数</th>
              <th>狙い度</th>
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
      title: store ? `${store.storeName}の狙い度分析` : "狙い度分析",
    };
  } catch {
    return {
      title: "狙い度分析",
    };
  }
}

export default async function HuntAnalysisPage({ params, searchParams }) {
  const resolvedParams = await params;
  const resolvedSearchParams = await searchParams;
  const storeId = resolvedParams.storeId;
  const requestedDate = readSingleSearchParam(resolvedSearchParams?.date);
  const requestedLimit = parseRequestedLimit(readSingleSearchParam(resolvedSearchParams?.limit));
  const requestedBacktestOptions = {
    periodMode: readSingleSearchParam(resolvedSearchParams?.periodMode),
    recentDays: readSingleSearchParam(resolvedSearchParams?.recentDays),
    startDate: readSingleSearchParam(resolvedSearchParams?.startDate),
    endDate: readSingleSearchParam(resolvedSearchParams?.endDate),
    machineNames: readMultiSearchParam(resolvedSearchParams?.machine),
    combineAimJuggler: readMultiSearchParam(resolvedSearchParams?.aimMachineGroup),
    differenceMode: readSingleSearchParam(resolvedSearchParams?.differenceMode),
    rankMin: readSingleSearchParam(resolvedSearchParams?.rankMin),
    rankMax: readSingleSearchParam(resolvedSearchParams?.rankMax),
    rankScope: readSingleSearchParam(resolvedSearchParams?.rankScope),
    scoreMin: readSingleSearchParam(resolvedSearchParams?.scoreMin),
    matchMode: readSingleSearchParam(resolvedSearchParams?.matchMode),
    showGraph: readSingleSearchParam(resolvedSearchParams?.showGraph),
    eventTouched: readSingleSearchParam(resolvedSearchParams?.backtestEventTouched) === "1",
    dayTails: readMultiSearchParam(resolvedSearchParams?.backtestDayTail),
    weekdays: readMultiSearchParam(resolvedSearchParams?.backtestWeekday),
  };

  let detail;

  try {
    detail = await getHuntScoreAnalysisPageDetail(
      storeId,
      requestedDate,
      requestedLimit,
      requestedBacktestOptions,
    );
  } catch (error) {
    return (
      <main className="pageStack">
        <Breadcrumbs
          items={[
            { label: "店舗一覧", href: "/" },
            { label: "店舗ページ", href: `/stores/${storeId}` },
            { label: "狙い度分析" },
          ]}
        />
        <section className="statusPanel">
          <h2>狙い度分析を読み込めませんでした</h2>
          <p>{error instanceof Error ? error.message : "設定を確認してください。"}</p>
        </section>
      </main>
    );
  }

  if (!detail) {
    notFound();
  }

  const fallbackNotice =
    detail.requestedDate && detail.requestedDate !== detail.selectedDate
      ? "指定した日付は見つからなかったため、最新の集計日を表示しています。"
      : "";
  const backtestFallbackNotice = detail.backtest.usedFallbackRange
    ? "期間指定が空欄だったため、直近日数の期間を日付範囲へ仮で入れています。"
    : "";
  const backtestNoActualNotice =
    detail.backtest.missingActualRowCount > 0
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
    matchMode: detail.backtest.matchMode,
    rankScope: detail.backtest.rankScope,
    combineAimJuggler: detail.backtest.combineAimJuggler,
  };
  const selectedBacktestDayTailSet = new Set(detail.backtest.eventFilters.dayTails);
  const selectedBacktestWeekdaySet = new Set(detail.backtest.eventFilters.weekdays);

  return (
    <main className="pageStack">
      <HuntRankingLimitSync defaultLimit={DEFAULT_RANKING_LIMIT} />
      <HuntBacktestEventFilterSync storeId={detail.store.id} />
      <Breadcrumbs
        items={[
          { label: "店舗一覧", href: "/" },
          { label: detail.store.storeName, href: `/stores/${detail.store.id}` },
          { label: "狙い度分析" },
        ]}
      />

      <section className="heroPanel">
        <div className="heroCopy">
          <h1 className="pageTitle pageTitleCompact">狙い度分析</h1>
          <div className="heroLinks simpleHeroLinks">
            <Link href={`/stores/${detail.store.id}`} className="externalLink">
              店舗ページへ戻る
            </Link>
            {detail.store.storeUrl ? (
              <a href={detail.store.storeUrl} target="_blank" rel="noreferrer" className="externalLink">
                店舗ページを開く
              </a>
            ) : null}
          </div>
        </div>
      </section>

      {detail.rankingDates.length > 0 ? (
        <>
          <section className="filterPanel">
            <div>
              <p className="sectionLabel">翌営業日バックテスト</p>
            </div>
            <NativeGetForm action={`/stores/${detail.store.id}/hunt-analysis`} className="backtestForm">
              {renderHiddenSearchParams(resolvedSearchParams, BACKTEST_SEARCH_KEYS)}
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
                <input type="hidden" name="aimMachineGroup" value="0" />
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
                  <span>SアイムジャグラーEXとネオアイムジャグラーEXをまとめる</span>
                </label>
                <div className="metricToggleRow">
                  {detail.backtest.machineOptions.map((machine) => (
                    <label
                      key={machine.name}
                      className={`metricToggleChip ${
                        machine.checked ? "metricToggleChipActive" : ""
                      }`}
                    >
                      <input
                        type="checkbox"
                        name="machine"
                        value={machine.name}
                        defaultChecked={machine.checked}
                      />
                      <span>{machine.name}</span>
                    </label>
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
                <p className="filterControlLabel">順位と狙い度を両方入れた時の条件</p>
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
                    <span>両方一致</span>
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
                    <span>どちらか一致</span>
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

          <section className="cardsGrid summaryStrip">
            <article className="summaryCard">
              <p className="metaLabel">狙い度期間</p>
              <strong className="metaValue">{formatPeriod(detail.backtest.startDate, detail.backtest.endDate)}</strong>
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

          <section className="filterPanel">
            <div>
              <p className="sectionLabel">集計日を選ぶ</p>
              <p className="filterLead">
                選んだ日の時点で見た次回営業日の狙い度を、高い順の一覧として確認できます。
              </p>
            </div>
            <NativeGetForm action={`/stores/${detail.store.id}/hunt-analysis`} className="storeReserveForm">
              {renderHiddenSearchParams(resolvedSearchParams, ["date", "limit"])}
              <label className="storeReserveField">
                <span>集計日</span>
                <select name="date" defaultValue={detail.selectedDate ?? ""} className="storeReserveInput">
                  {detail.rankingDates.map((date) => (
                    <option key={date} value={date}>
                      {formatCompactDate(date)}
                    </option>
                  ))}
                </select>
              </label>
              <label className="storeReserveField">
                <span>何位まで表示</span>
                <input
                  type="number"
                  name="limit"
                  min="1"
                  max={Math.max(detail.totalCount, 1)}
                  defaultValue={detail.limit}
                  className="storeReserveInput"
                />
              </label>
              <button type="submit" className="storeReserveButton">
                表示する
              </button>
            </NativeGetForm>
            {fallbackNotice ? <p className="storeReserveHelp">{fallbackNotice}</p> : null}
            {!detail.nextBusinessDate ? (
              <p className="filterPanelStatus">最新日のため、翌営業日の実績はまだありません。</p>
            ) : null}
          </section>

          <HuntRankingTable storeId={detail.store.id} rows={detail.rows} />
        </>
      ) : (
        <section className="statusPanel">
          <h2>狙い度分析を作れる日付がまだありません</h2>
          <p>対象機種の保存済みデータが増えると、ここに点数順の一覧が表示されます。</p>
        </section>
      )}
    </main>
  );
}
