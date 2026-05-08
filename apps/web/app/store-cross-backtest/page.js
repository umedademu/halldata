import Link from "next/link";

import { Breadcrumbs } from "../../components/breadcrumbs";
import { DataSourceLabel } from "../../components/data-source-label";
import { JugglerOnlyButton } from "../../components/hunt-machine-filter-tools";
import { NativeGetForm } from "../../components/native-get-form";
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

function formatAverageDifference(value) {
  return Number.isFinite(value) ? formatSignedNumber(Math.round(value)) : "-";
}

function StoreRankingTable({ rows }) {
  return (
    <section className="tablePanel directoryPanel">
      <div className="tablePanelHeader">
        <div>
          <p className="sectionLabel">平均機械割ランキング</p>
          <h2 className="tablePanelTitle">店舗横断バックテスト結果</h2>
        </div>
      </div>
      <div className="tableScroller directoryScroller">
        <table className="directoryTable">
          <thead>
            <tr>
              <th>順位</th>
              <th className="directoryNameHeader">店舗名</th>
              <th>地域</th>
              <th>平均機械割</th>
              <th>平均差枚</th>
              <th>合計差枚</th>
              <th>合計G数</th>
              <th>平均設定</th>
              <th>実績台数</th>
              <th>条件一致台数</th>
              <th>対象日数</th>
              <th>一致日数</th>
              <th>対象機種</th>
              <th>狙い度</th>
              <th>偏差値</th>
              <th>BB</th>
              <th>RB</th>
              <th>合成</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr
                key={row.store.id}
                className={getSettingEstimateHighlightClass(row.averageSetting)}
              >
                <td>{formatNumber(row.rank)}</td>
                <th className="directoryNameCell">
                  <Link href={`/stores/${row.store.id}`}>{row.store.storeName}</Link>
                </th>
                <td>{[row.store.prefectureName, row.store.areaName].filter(Boolean).join(" / ") || "-"}</td>
                <td>{formatPercent(row.payoutRate)}</td>
                <td>{formatAverageDifference(row.averageDifference)}</td>
                <td>{formatSignedNumber(row.differenceTotal)}</td>
                <td>{formatNumber(row.gamesTotal)}</td>
                <td>{formatSettingEstimateScore(row.averageSetting)}</td>
                <td>{formatNumber(row.actualRowCount)}</td>
                <td>{formatNumber(row.matchedRowCount)}</td>
                <td>{formatNumber(row.targetDateCount)}</td>
                <td>{formatNumber(row.matchedDateCount)}</td>
                <td title={row.selectedMachineNames.join("、")}>
                  {formatNumber(row.selectedMachineCount)}
                </td>
                <td>{formatDecimal(row.averageHuntScore)}</td>
                <td>{formatDecimal(row.averageDeviation)}</td>
                <td>{formatNumber(row.bbTotal)}</td>
                <td>{formatNumber(row.rbTotal)}</td>
                <td>{row.combinedProbability ?? "-"}</td>
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
  const hasDeviationMinParam = Object.hasOwn(resolvedSearchParams ?? {}, "deviationMin");
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
    differenceMode: readSingleSearchParam(resolvedSearchParams?.differenceMode),
    rankMin: readSingleSearchParam(resolvedSearchParams?.rankMin),
    rankMax: readSingleSearchParam(resolvedSearchParams?.rankMax),
    rankScope: readSingleSearchParam(resolvedSearchParams?.rankScope),
    scoreMin: readSingleSearchParam(resolvedSearchParams?.scoreMin),
    deviationScope: readSingleSearchParam(resolvedSearchParams?.deviationScope),
    deviationMin: hasDeviationMinParam
      ? readSingleSearchParam(resolvedSearchParams?.deviationMin)
      : DEFAULT_DEVIATION_MIN,
    rankRequired: readMultiSearchParam(resolvedSearchParams?.rankRequired),
    scoreRequired: readMultiSearchParam(resolvedSearchParams?.scoreRequired),
    deviationRequired: readMultiSearchParam(resolvedSearchParams?.deviationRequired),
    dayTails: readMultiSearchParam(resolvedSearchParams?.backtestDayTail),
    weekdays: readMultiSearchParam(resolvedSearchParams?.backtestWeekday),
    minActualRows: readSingleSearchParam(resolvedSearchParams?.minActualRows),
    minMatchedDateCount: readSingleSearchParam(resolvedSearchParams?.minMatchedDateCount),
    limit: readSingleSearchParam(resolvedSearchParams?.limit),
  });
  const selectedDayTailSet = new Set(detail.eventFilters.dayTails);
  const selectedWeekdaySet = new Set(detail.eventFilters.weekdays);
  const machineOptionGroups = groupHuntMachineOptions(detail.machineOptions);

  return (
    <main className="pageStack">
      <Breadcrumbs
        items={[
          { label: "店舗一覧", href: "/" },
          { label: "店舗横断バックテスト" },
        ]}
      />

      <section className="heroPanel">
        <div className="heroCopy">
          <h1 className="pageTitle pageTitleCompact">店舗横断バックテスト</h1>
          <p className="dataSourceLabel">適用中: {detail.huntScoreLogic.name}</p>
          <DataSourceLabel source={detail.dataSource} />
          <div className="heroLinks simpleHeroLinks">
            <Link href="/" className="externalLink">
              店舗一覧へ戻る
            </Link>
          </div>
        </div>
      </section>

      <section className="filterPanel">
        <div>
          <p className="sectionLabel">店舗横断条件</p>
        </div>
        <NativeGetForm action="/store-cross-backtest" className="backtestForm">
          <input type="hidden" name="show" value="1" />
          <input type="hidden" name="machineTouched" value="1" />
          <input type="hidden" name="aimMachineGroup" value="0" />
          <input type="hidden" name="hanabiMachineGroup" value="0" />

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
              <span>最低実績台数</span>
              <input
                type="number"
                name="minActualRows"
                min="0"
                defaultValue={detail.minActualRows}
                className="storeReserveInput"
              />
            </label>
            <label className="storeReserveField backtestField">
              <span>最低一致日数</span>
              <input
                type="number"
                name="minMatchedDateCount"
                min="0"
                defaultValue={detail.minMatchedDateCount}
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
            <div className="machineFilterGroups">
              {machineOptionGroups.map((group) => (
                <div key={group.key} className="machineFilterGroup">
                  <p className="machineFilterGroupLabel">{group.label}</p>
                  {group.key === "juggler" ? (
                    <div className="machineGroupToggleRow">
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
                      <JugglerOnlyButton />
                    </div>
                  ) : null}
                  {group.key === "hanabi" ? (
                    <div className="machineGroupToggleRow">
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
                    </div>
                  ) : null}
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
              <p className="huntConditionLabel">偏差値</p>
              <div className="huntConditionInputs">
                <label className="storeReserveField backtestField huntConditionNumberField">
                  <span>下限</span>
                  <input
                    type="number"
                    name="deviationMin"
                    min="0"
                    step="0.1"
                    defaultValue={detail.deviationMin ?? ""}
                    className="storeReserveInput"
                  />
                </label>
              </div>
              <input type="hidden" name="deviationRequired" value="0" />
              <label
                className={`metricToggleChip huntConditionRequired ${
                  detail.deviationRequired ? "metricToggleChipActive" : ""
                }`}
              >
                <input
                  type="checkbox"
                  name="deviationRequired"
                  value="1"
                  defaultChecked={detail.deviationRequired}
                />
                <span>必須</span>
              </label>
            </div>
          </div>

          <div className="backtestBlock">
            <p className="filterControlLabel">差枚と機械割の基準</p>
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
                <span>ボーナス数基準</span>
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
                { value: "all", label: "全機種順位" },
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
            <p className="filterControlLabel">偏差値の比較対象</p>
            <div className="metricToggleRow">
              {[
                { value: "all", label: "全機種内" },
                { value: "selected", label: "チェック機種内" },
                { value: "machine", label: "機種内" },
              ].map((scope) => (
                <label
                  key={scope.value}
                  className={`metricToggleChip ${
                    detail.deviationScope === scope.value ? "metricToggleChipActive" : ""
                  }`}
                >
                  <input
                    type="radio"
                    name="deviationScope"
                    value={scope.value}
                    defaultChecked={detail.deviationScope === scope.value}
                  />
                  <span>{scope.label}</span>
                </label>
              ))}
            </div>
          </div>

          <div className="backtestButtonRow">
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
                <p className="metaLabel">対象機種</p>
                <strong className="metaValue">{formatNumber(detail.selectedMachineNames.length)}機種</strong>
              </article>
            </section>
            <StoreRankingTable rows={detail.rows} />
          </>
        ) : (
          <section className="statusPanel">
            <h2>条件に合う店舗がありません</h2>
            <p>機種、期間、狙い度条件、最低実績台数を見直してください。</p>
          </section>
        )
      ) : (
        <section className="statusPanel">
          <h2>バックテスト結果はまだ表示していません</h2>
          <p>条件を選ぶと、全店舗を同じ条件で集計して平均機械割順に並べます。</p>
        </section>
      )}
    </main>
  );
}
