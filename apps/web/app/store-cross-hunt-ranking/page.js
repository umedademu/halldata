import Link from "next/link";
import { cookies } from "next/headers";

import { CrossStoreHuntRankingFormStateSync } from "../../components/cross-store-hunt-ranking-form-state-sync";
import {
  AllMachineFilterButtons,
  MachineFilterCategoryButton,
} from "../../components/hunt-machine-filter-tools";
import { HuntRankingTable } from "../../components/hunt-ranking-table";
import { NativeGetForm } from "../../components/native-get-form";
import { ResultUrlTools } from "../../components/result-url-tools";
import {
  getHuntScoreInitialPageDetail,
  getHuntScoreRankingDetail,
  getStoreList,
} from "../../lib/data";
import { formatMonthDay, formatNumber } from "../../lib/format";
import {
  getHuntMachineShortName,
  isHuntJugglerMachine,
} from "../../lib/hunt-machine-display";
import {
  decodeHuntScoreLogicCookieValue,
  getHuntScoreLogicCookieName,
} from "../../lib/hunt-score-logic-selection";
import {
  decodeMachineEvaluationSettingsCookieValue,
  getMachineEvaluationCookieName,
} from "../../lib/machine-evaluation";
import { normalizeDifferenceMode } from "../../lib/machine-difference";
import {
  SETTING_ESTIMATE_MODE_OPTIONS,
  normalizeSettingEstimateMode,
} from "../../lib/setting-estimates";
import { buildStoreLocationGroups } from "../../lib/store-location-groups";

export const dynamic = "force-dynamic";
export const metadata = {
  title: "店舗横断狙い度ランキング",
};

const FORM_ID = "cross-store-hunt-ranking-form";
const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 300;

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

function normalizeStoreIds(values) {
  return [
    ...new Set(
      (Array.isArray(values) ? values : [values])
        .map((value) => String(value ?? "").trim())
        .filter(Boolean),
    ),
  ];
}

function normalizeLimit(value) {
  const parsedValue = Number(value);
  if (!Number.isInteger(parsedValue) || parsedValue < 1) {
    return DEFAULT_LIMIT;
  }
  return Math.min(parsedValue, MAX_LIMIT);
}

function SettingEstimateModeOptions({ value }) {
  const normalizedValue = normalizeSettingEstimateMode(value);
  return (
    <div className="metricToggleRow">
      {SETTING_ESTIMATE_MODE_OPTIONS.map((option) => (
        <label
          key={option.value}
          className={`metricToggleChip ${
            normalizedValue === option.value ? "metricToggleChipActive" : ""
          }`}
        >
          <input
            type="radio"
            name="settingEstimateMode"
            value={option.value}
            defaultChecked={normalizedValue === option.value}
          />
          <span>{option.label}</span>
        </label>
      ))}
    </div>
  );
}

function readSlotCount(detail, machineName) {
  const slotCount = Number(detail?.machineSlotCounts?.[machineName]);
  return Number.isFinite(slotCount) && slotCount > 0 ? slotCount : 0;
}

function buildJugglerMachineOptions(details, requestedMachineNames, machineTouched) {
  const selectedMachineNameSet = new Set(
    requestedMachineNames.map((machineName) => String(machineName ?? "").trim()).filter(Boolean),
  );
  const machineMap = new Map();

  for (const detail of details) {
    for (const machineName of Array.isArray(detail?.availableMachineNames)
      ? detail.availableMachineNames
      : []) {
      const safeMachineName = String(machineName ?? "").trim();
      if (!safeMachineName || !isHuntJugglerMachine(safeMachineName)) {
        continue;
      }
      if (!machineMap.has(safeMachineName)) {
        machineMap.set(safeMachineName, {
          name: safeMachineName,
          slotCount: 0,
          storeCount: 0,
        });
      }

      const option = machineMap.get(safeMachineName);
      option.slotCount += readSlotCount(detail, safeMachineName);
      option.storeCount += 1;
    }
  }

  return [...machineMap.values()]
    .map((machine) => ({
      ...machine,
      checked: machineTouched ? selectedMachineNameSet.has(machine.name) : true,
      shortName: getHuntMachineShortName(machine.name),
    }))
    .sort((left, right) => {
      if (right.slotCount !== left.slotCount) {
        return right.slotCount - left.slotCount;
      }
      return left.shortName.localeCompare(right.shortName, "ja");
    });
}

function compareRankingRows(left, right) {
  return (
    Number(right.huntScore ?? 0) - Number(left.huntScore ?? 0) ||
    String(left.machineName ?? "").localeCompare(String(right.machineName ?? ""), "ja") ||
    String(left.storeName ?? "").localeCompare(String(right.storeName ?? ""), "ja") ||
    String(left.slotNumber ?? "").localeCompare(String(right.slotNumber ?? ""), "ja", {
      numeric: true,
    })
  );
}

function buildCrossStoreRankingGroups(rows, selectedMachineNames, limit) {
  const selectedMachineNameSet = new Set(selectedMachineNames);
  const groupsByMachineName = new Map();

  for (const row of rows) {
    const machineName = String(row.machineName ?? "").trim();
    if (!machineName || !selectedMachineNameSet.has(machineName)) {
      continue;
    }
    if (!groupsByMachineName.has(machineName)) {
      groupsByMachineName.set(machineName, {
        machineName,
        rows: [],
        allRows: [],
        totalCount: 0,
      });
    }
    groupsByMachineName.get(machineName).rows.push(row);
  }

  return [...groupsByMachineName.values()]
    .map((group) => {
      const rankedRows = [...group.rows].sort(compareRankingRows).map((row, index) => ({
        ...row,
        storeLocalRank: row.storeLocalRank ?? row.rank,
        storeLocalMachineRank: row.storeLocalMachineRank ?? row.machineRank ?? row.rank,
        crossStoreMachineRank: index + 1,
        overallRank: index + 1,
        selectedRank: index + 1,
        rank: index + 1,
      }));
      return {
        ...group,
        allRows: rankedRows,
        rows: rankedRows.slice(0, limit),
        totalCount: rankedRows.length,
        limit: Math.min(limit, rankedRows.length),
      };
    })
    .filter((group) => group.rows.length > 0);
}

function buildRowKey(storeId, row) {
  return [
    storeId,
    row?.rowKey,
    row?.machineName,
    row?.slotNumber,
  ].map((part) => String(part ?? "").trim()).filter(Boolean).join("::");
}

function decorateRowsWithStore(detail) {
  const predictionDate = String(detail?.selectedDate ?? "").trim();
  const requestedPredictionDate = String(detail?.requestedDate ?? "").trim();
  const usesFallbackPredictionDate = Boolean(
    predictionDate && requestedPredictionDate && predictionDate !== requestedPredictionDate,
  );

  return (Array.isArray(detail?.rows) ? detail.rows : []).map((row) => ({
    ...row,
    storeId: detail.store.id,
    storeName: detail.store.storeName,
    storeLocalRank: row.rank,
    storeLocalMachineRank: row.machineRank ?? row.rank,
    predictionDate,
    requestedPredictionDate,
    usesFallbackPredictionDate,
    rowKey: buildRowKey(detail.store.id, row),
  }));
}

function buildStoreById(stores) {
  return new Map(stores.map((store) => [String(store.id ?? "").trim(), store]));
}

function readLatestInitialDate(details) {
  return (Array.isArray(details) ? details : [])
    .map((detail) => String(detail?.selectedDate ?? "").trim())
    .filter(Boolean)
    .sort((left, right) => right.localeCompare(left))[0] ?? "";
}

function readStoreHuntScoreLogicKey(cookieStore, storeId) {
  return decodeHuntScoreLogicCookieValue(
    cookieStore.get(getHuntScoreLogicCookieName(storeId))?.value ?? "",
  );
}

function readStoreMachineEvaluationSettings(cookieStore, storeId) {
  return decodeMachineEvaluationSettingsCookieValue(
    cookieStore.get(getMachineEvaluationCookieName(storeId))?.value ?? "",
  );
}

function buildStoreRuntimeSettings(cookieStore, stores) {
  return new Map(
    stores.map((store) => [
      store.id,
      {
        huntScoreLogicKey: readStoreHuntScoreLogicKey(cookieStore, store.id),
        machineEvaluationSettings: readStoreMachineEvaluationSettings(cookieStore, store.id),
      },
    ]),
  );
}

function buildStoreRequestOptions(storeSettings, options = {}) {
  const huntScoreLogicKey = String(storeSettings?.huntScoreLogicKey ?? "").trim();
  return {
    ...options,
    machineEvaluationSettings: storeSettings?.machineEvaluationSettings ?? {},
    ...(huntScoreLogicKey ? { huntScoreLogicKeys: [huntScoreLogicKey] } : {}),
  };
}

function logCrossStoreReadFailure(step, store, selectedDate, error) {
  console.warn("Failed to read cross-store hunt ranking detail.", {
    step,
    storeId: store?.id,
    storeName: store?.storeName,
    selectedDate,
    message: error instanceof Error ? error.message : String(error ?? ""),
  });
}

async function readCrossStoreInitialDetail({
  store,
  storeSettings,
  differenceMode,
  settingEstimateMode,
}) {
  try {
    return await getHuntScoreInitialPageDetail(
      store.id,
      buildStoreRequestOptions(storeSettings, {
        differenceMode,
        settingEstimateMode,
        skipBacktestDetail: true,
      }),
      storeSettings?.huntScoreLogicKey ?? "",
    );
  } catch (error) {
    logCrossStoreReadFailure("initial", store, "", error);
    return null;
  }
}

async function readCrossStoreRankingDetail({
  store,
  selectedDate,
  requestedLimit,
  storeSettings,
  differenceMode,
  settingEstimateMode,
  selectedMachineNames,
}) {
  try {
    const detail = await getHuntScoreRankingDetail(
      store.id,
      selectedDate,
      requestedLimit,
      storeSettings?.huntScoreLogicKey ?? "",
      differenceMode,
      settingEstimateMode,
      buildStoreRequestOptions(storeSettings, {
        machineNames: selectedMachineNames,
        machineTouched: true,
        expectedRbOnly: true,
        fallbackToPreviousDate: true,
      }),
    );
    return { detail, failed: false };
  } catch (error) {
    logCrossStoreReadFailure("ranking", store, selectedDate, error);
    return { detail: null, failed: true };
  }
}

function isUsableCrossStoreRankingDetail(detail, selectedDate) {
  const requestedDate = String(selectedDate ?? "").trim();
  const detailSelectedDate = String(detail?.selectedDate ?? "").trim();

  return Boolean(
    detail &&
      requestedDate &&
      detailSelectedDate &&
      detailSelectedDate <= requestedDate &&
      Array.isArray(detail.rankingDates) &&
      detail.rankingDates.includes(detailSelectedDate),
  );
}

export default async function StoreCrossHuntRankingPage({ searchParams }) {
  const resolvedSearchParams = await searchParams;
  const resultRequested = readSingleSearchParam(resolvedSearchParams?.show) === "1";
  const requestedFavoriteStoreIds = normalizeStoreIds(
    readMultiSearchParam(resolvedSearchParams?.favoriteStore),
  );
  const requestedStoreIds = normalizeStoreIds(readMultiSearchParam(resolvedSearchParams?.store));
  const requestedDate = readSingleSearchParam(resolvedSearchParams?.date);
  const requestedLimit = normalizeLimit(readSingleSearchParam(resolvedSearchParams?.limit));
  const requestedMachineNames = readMultiSearchParam(resolvedSearchParams?.machine);
  const machineTouched = readSingleSearchParam(resolvedSearchParams?.machineTouched) === "1";
  const differenceMode = normalizeDifferenceMode(
    readSingleSearchParam(resolvedSearchParams?.differenceMode),
  );
  const settingEstimateMode = normalizeSettingEstimateMode(
    readSingleSearchParam(resolvedSearchParams?.settingEstimateMode),
  );
  const stores = (await getStoreList()).filter((store) => !store.isPendingRegistration);
  const storeById = buildStoreById(stores);
  const favoriteStoreIds =
    requestedFavoriteStoreIds.length > 0 ? requestedFavoriteStoreIds : requestedStoreIds;
  const favoriteStores = favoriteStoreIds.map((storeId) => storeById.get(storeId)).filter(Boolean);
  const favoriteStoreGroups = buildStoreLocationGroups(favoriteStores);
  const selectedStores = requestedStoreIds.map((storeId) => storeById.get(storeId)).filter(Boolean);
  const cookieStore = await cookies();
  const storeSettingsById = buildStoreRuntimeSettings(cookieStore, selectedStores);
  const initialDetails = (
    await Promise.all(
      selectedStores.map((store) =>
        readCrossStoreInitialDetail({
          store,
          storeSettings: storeSettingsById.get(store.id),
          differenceMode,
          settingEstimateMode,
        }),
      ),
    )
  ).filter(Boolean);
  const machineOptions = buildJugglerMachineOptions(
    initialDetails,
    requestedMachineNames,
    machineTouched,
  );
  const selectedMachineNames = machineOptions
    .filter((machine) => machine.checked)
    .map((machine) => machine.name);
  const selectedDate = String(requestedDate || readLatestInitialDate(initialDetails)).trim();
  const resultEntries =
    resultRequested && selectedDate
      ? await Promise.all(
          selectedStores.map((store) =>
            readCrossStoreRankingDetail({
              store,
              selectedDate,
              requestedLimit,
              storeSettings: storeSettingsById.get(store.id),
              differenceMode,
              settingEstimateMode,
              selectedMachineNames,
            }),
          ),
        )
      : [];
  const resultDetails = resultEntries
    .map((entry) => entry.detail)
    .filter((detail) => isUsableCrossStoreRankingDetail(detail, selectedDate));
  const unreadableStoreCount = resultEntries.filter((entry) => entry.failed).length;
  const noSelectedDateStoreCount = resultRequested
    ? Math.max(selectedStores.length - resultDetails.length - unreadableStoreCount, 0)
    : 0;
  const skippedStoreCount = unreadableStoreCount + noSelectedDateStoreCount;
  const fallbackDateStoreCount = resultDetails.filter(
    (detail) => String(detail?.selectedDate ?? "").trim() !== selectedDate,
  ).length;
  const rankingRows = resultDetails.flatMap(decorateRowsWithStore);
  const rankingGroups = buildCrossStoreRankingGroups(
    rankingRows,
    selectedMachineNames,
    requestedLimit,
  );
  const huntScoreLogicLabel = "各店舗の設定ロジック";
  const dateFlowLabel = selectedDate
    ? `${formatMonthDay(selectedDate)}狙い度 → 各店舗の翌営業日実績`
    : "狙い度 → 各店舗の翌営業日実績";

  return (
    <main className="pageStack">
      <CrossStoreHuntRankingFormStateSync formId={FORM_ID} />
      <section className="heroPanel">
        <div className="heroCopy">
          <h1 className="pageTitle pageTitleCompact">店舗横断狙い度ランキング</h1>
          <p className="leadText">
            マイホール内のジャグラー系から、期待RB付き候補だけを店舗横断で確認します。
          </p>
          <div className="heroLinks simpleHeroLinks">
            <Link href="/" className="externalLink">
              店舗一覧へ戻る
            </Link>
            <Link href="/store-cross-backtest" className="externalLink">
              店舗横断バックテストを見る
            </Link>
          </div>
        </div>
      </section>

      <section className="filterPanel">
        <NativeGetForm action="/store-cross-hunt-ranking" id={FORM_ID} className="backtestForm">
          <input type="hidden" name="show" value="1" />
          <input type="hidden" name="machineTouched" value="1" />
          {favoriteStores.map((store) => (
            <input key={store.id} type="hidden" name="favoriteStore" value={store.id} />
          ))}

          <div className="filterConditionBox rankingConditionBoxWide">
            <p className="filterConditionBoxTitle">対象店舗</p>
            {favoriteStores.length > 0 ? (
              <div className="crossStoreSelectionStack">
                <div className="storeSelectionToolbar">
                  {[
                    { label: "全てのチェックをON", action: "check" },
                    { label: "全てのチェックをOFF", action: "clear" },
                  ].map((button) => (
                    <button
                      key={`${button.prefecture ?? "all"}-${button.action}`}
                      type="button"
                      className="storeReserveButton storeReserveButtonSecondary storeSelectionButton"
                      data-cross-store-select-action={button.action}
                      data-cross-store-select-prefecture={button.prefecture ?? ""}
                    >
                      {button.label}
                    </button>
                  ))}
                </div>
                <div className="machineFilterGroups">
                  {favoriteStoreGroups.map((group) => (
                    <div key={group.key} className="machineFilterGroup">
                      <p className="machineFilterGroupLabel">
                        {group.label}（{formatNumber(group.storeCount)}店）
                      </p>
                      {group.prefectureName ? (
                        <div className="machineGroupToggleRow">
                          {[
                            { label: `${group.label}を全てON`, action: "check" },
                            { label: `${group.label}を全てOFF`, action: "clear" },
                          ].map((button) => (
                            <button
                              key={`${group.key}-${button.action}`}
                              type="button"
                              className="storeReserveButton storeReserveButtonSecondary storeSelectionButton"
                              data-cross-store-select-action={button.action}
                              data-cross-store-select-prefecture={group.prefectureName}
                            >
                              {button.label}
                            </button>
                          ))}
                        </div>
                      ) : null}
                      <div className="metricToggleRow">
                        {group.stores.map((store) => {
                          const checked = requestedStoreIds.includes(store.id);
                          return (
                            <label
                              key={store.id}
                              className={`metricToggleChip ${
                                checked ? "metricToggleChipActive" : ""
                              }`}
                            >
                              <input
                                type="checkbox"
                                name="store"
                                value={store.id}
                                defaultChecked={checked}
                                data-cross-store-option="1"
                                data-store-prefecture={store.prefectureName ?? ""}
                              />
                              <span>{store.storeName}</span>
                            </label>
                          );
                        })}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ) : (
              <p className="filterPanelStatus">
                マイホールを登録すると、ここに対象店舗が表示されます。
              </p>
            )}
          </div>

          <div className="filterConditionBox rankingConditionBox">
            <p className="filterConditionBoxTitle">集計条件</p>
            <div className="rankingDateGrid">
              <label className="storeReserveField">
                <span>狙い度の日</span>
                <input
                  type="date"
                  name="date"
                  defaultValue={selectedDate}
                  className="storeReserveInput"
                />
              </label>
              <label className="storeReserveField">
                <span>表示件数</span>
                <input
                  type="number"
                  name="limit"
                  min="1"
                  max={MAX_LIMIT}
                  defaultValue={requestedLimit}
                  className="storeReserveInput"
                />
              </label>
            </div>
          </div>

          <div className="filterConditionBox rankingConditionBox">
            <p className="filterConditionBoxTitle">差枚基準</p>
            <div className="metricToggleRow">
              {[
                { value: "bonus", label: "設定1基準" },
                { value: "estimated", label: "推定設定基準" },
                { value: "minrepo", label: "みんレポ基準" },
              ].map((option) => (
                <label
                  key={option.value}
                  className={`metricToggleChip ${
                    differenceMode === option.value ? "metricToggleChipActive" : ""
                  }`}
                >
                  <input
                    type="radio"
                    name="differenceMode"
                    value={option.value}
                    defaultChecked={differenceMode === option.value}
                  />
                  <span>{option.label}</span>
                </label>
              ))}
            </div>
          </div>

          <div className="filterConditionBox rankingConditionBox">
            <p className="filterConditionBoxTitle">設定推定基準</p>
            <SettingEstimateModeOptions value={settingEstimateMode} />
          </div>

          <div className="filterConditionBox rankingConditionBoxWide">
            <p className="filterConditionBoxTitle">表示機種</p>
            {machineOptions.length > 0 ? (
              <div className="backtestBlock rankingCustomMachineFilter">
                <AllMachineFilterButtons />
                <div className="machineFilterGroups">
                  <div className="machineFilterGroup">
                    <p className="machineFilterGroupLabel">ジャグ系</p>
                    <div className="machineGroupToggleRow">
                      <MachineFilterCategoryButton category="juggler" label="ジャグ系のみ選択" />
                      <MachineFilterCategoryButton
                        category="juggler"
                        label="ジャグ系のみ解除"
                        action="clear"
                      />
                    </div>
                    <div className="metricToggleRow">
                      {machineOptions.map((machine) => (
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
                            data-machine-category="juggler"
                            data-machine-slot-count={machine.slotCount}
                          />
                          <span>
                            {machine.shortName}（{formatNumber(machine.storeCount)}店/
                            {formatNumber(machine.slotCount)}台）
                          </span>
                        </label>
                      ))}
                    </div>
                  </div>
                </div>
              </div>
            ) : (
              <p className="filterPanelStatus">
                選択店舗に狙い度対象のジャグラー系機種がありません。
              </p>
            )}
          </div>

          <div className="backtestButtonRow">
            <button type="submit" className="storeReserveButton backtestPrimaryButton">
              店舗横断ランキングを表示
            </button>
          </div>
        </NativeGetForm>
      </section>

      <ResultUrlTools active={resultRequested} />

      {resultRequested ? (
        rankingRows.length > 0 ? (
          <>
            <section className="cardsGrid summaryStrip">
              <article className="summaryCard">
                <p className="metaLabel">狙い度の日</p>
                <strong className="metaValue">{selectedDate || "-"}</strong>
              </article>
              <article className="summaryCard">
                <p className="metaLabel">対象店舗</p>
                <strong className="metaValue">{formatNumber(selectedStores.length)}店</strong>
              </article>
              <article className="summaryCard">
                <p className="metaLabel">集計店舗</p>
                <strong className="metaValue">{formatNumber(resultDetails.length)}店</strong>
              </article>
              {skippedStoreCount > 0 ? (
                <article className="summaryCard">
                  <p className="metaLabel">対象外店舗</p>
                  <strong className="metaValue">{formatNumber(skippedStoreCount)}店</strong>
                </article>
              ) : null}
              {fallbackDateStoreCount > 0 ? (
                <article className="summaryCard">
                  <p className="metaLabel">代替日表示</p>
                  <strong className="metaValue">{formatNumber(fallbackDateStoreCount)}店</strong>
                </article>
              ) : null}
              <article className="summaryCard">
                <p className="metaLabel">対象機種</p>
                <strong className="metaValue">{formatNumber(selectedMachineNames.length)}機種</strong>
              </article>
            </section>
            <HuntRankingTable
              storeId=""
              storeName=""
              rows={rankingRows}
              rankingGroups={rankingGroups}
              overallLimit={requestedLimit}
              predictionDate={selectedDate}
              actualDate={null}
              enableConditionHighlight={false}
              initialDifferenceMode={differenceMode}
              showStoreColumn
              showMachineEvaluation={rankingRows.some((row) => row.machineEvaluation)}
              showGrapeColumn
              showOverallRanking={false}
              showMachineGroupTables
              dateFlowLabelOverride={dateFlowLabel}
              huntScoreLogicLabel={huntScoreLogicLabel}
            />
          </>
        ) : (
          <section className="statusPanel">
            <h2>表示できる台がありません</h2>
            <p>期待RBが表示できる候補がありません。対象店舗、日付、機種を見直してください。</p>
            {noSelectedDateStoreCount > 0 ? (
              <p>指定日以前の保存データがない店舗は、横断結果から除外しています。</p>
            ) : null}
            {unreadableStoreCount > 0 ? (
              <p>読み込めなかった店舗は、横断結果から除外しています。</p>
            ) : null}
          </section>
        )
      ) : (
        <section className="statusPanel">
          <h2>ランキングはまだ表示していません</h2>
          <p>マイホールから店舗を選び、ジャグラー系機種を指定して表示します。</p>
        </section>
      )}
    </main>
  );
}
