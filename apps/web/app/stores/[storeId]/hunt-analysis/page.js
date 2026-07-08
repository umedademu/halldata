import Link from "next/link";
import { cookies } from "next/headers";
import { notFound } from "next/navigation";

import { Breadcrumbs } from "../../../../components/breadcrumbs";
import {
  AllMachineFilterButtons,
  MachineFilterCategoryButton,
} from "../../../../components/hunt-machine-filter-tools";
import { HuntRankingFormStateSync } from "../../../../components/hunt-ranking-form-state-sync";
import { HuntRankingTable } from "../../../../components/hunt-ranking-table";
import {
  HuntScoreLogicSingleSelect,
} from "../../../../components/hunt-score-logic-selector";
import { NativeGetForm } from "../../../../components/native-get-form";
import { ResultDisplayStateSync } from "../../../../components/result-display-state-sync";
import { ResultUrlTools } from "../../../../components/result-url-tools";
import { StoreFavoriteButton } from "../../../../components/store-favorite-button";
import { StoreSwitcher } from "../../../../components/store-switcher";
import {
  getHuntScoreInitialPageDetail,
  getHuntScoreRankingDetail,
  getStoreList,
  getStoreIdentity,
} from "../../../../lib/data";
import { formatMonthDay } from "../../../../lib/format";
import {
  decodeHuntScoreLogicCookieValue,
  getHuntScoreLogicCookieName,
} from "../../../../lib/hunt-score-logic-selection";
import { listHuntScoreLogicOptions } from "../../../../lib/hunt-score";
import {
  MACHINE_EVALUATION_DAY_MODE_OPTIONS,
  MACHINE_EVALUATION_RANKING_MODE_OPTIONS,
  applyMachineEvaluationRankingMode,
  decodeMachineEvaluationSettingsCookieValue,
  getMachineEvaluationCookieName,
  normalizeMachineEvaluationDayMode,
  normalizeMachineEvaluationRankingMode,
  shouldShowMachineEvaluationInRanking,
} from "../../../../lib/machine-evaluation";
import {
  expandHuntMachineCombinedGroupSelection,
  groupHuntMachineOptions,
  isHuntJugglerMachine,
  selectionIncludesAimJugglerHuntMachineGroup,
  selectionIncludesHanabiHuntMachineGroup,
} from "../../../../lib/hunt-machine-display";
import { normalizeDifferenceMode } from "../../../../lib/machine-difference";
import {
  buildSavedParamAccess,
  readFormStateEntriesFromCookies,
  getResultDisplayCookieName,
  isResultDisplayCookieEnabled,
} from "../../../../lib/result-display-state";
import { SETTING_ESTIMATE_MODE_OPTIONS, normalizeSettingEstimateMode } from "../../../../lib/setting-estimates";

export const dynamic = "force-dynamic";

const HUNT_RANKING_ALL_MACHINE_LIMIT = 10000;
const AIM_JUGGLER_GROUP_NAME = "アイムジャグラーEX";
const AIM_JUGGLER_MACHINE_NAMES = ["SアイムジャグラーＥＸ", "ネオアイムジャグラーEX"];
const HANABI_GROUP_NAME = "ハナビ";
const HANABI_MACHINE_NAMES = ["新ハナビ", "スマスロ ハナビ"];
const HUNT_RANKING_FORM_ID = "hunt-ranking-condition-form";
const STORE_DAY_STATUS_CLOSED = "closed";
const HUNT_RANKING_CONDITION_PARAM_KEYS = [
  "machineTouched",
  "date",
  "differenceMode",
  "settingEstimateMode",
  "machineEvaluationRankingMode",
  "machineEvaluationDayMode",
  "huntScoreLogicKey",
  "machine",
];

function buildHuntRankingResultDisplayKey(storeId) {
  return `hunt-ranking-${storeId}`;
}

function formatRankingDateOption(date, nextBusinessDate) {
  const scoreDateLabel = formatMonthDay(date);
  const actualDateLabel = nextBusinessDate ? `${formatMonthDay(nextBusinessDate)}実績` : "実績なし";
  return `${scoreDateLabel}狙い度 → ${actualDateLabel}`;
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

function MachineEvaluationRankingModeOptions({ value }) {
  const normalizedValue = normalizeMachineEvaluationRankingMode(value);
  return (
    <div className="metricToggleRow commonConditionModeOptions">
      {MACHINE_EVALUATION_RANKING_MODE_OPTIONS.map((option) => (
        <label
          key={option.value}
          className={`metricToggleChip ${
            normalizedValue === option.value ? "metricToggleChipActive" : ""
          }`}
        >
          <input
            type="radio"
            name="machineEvaluationRankingMode"
            value={option.value}
            defaultChecked={normalizedValue === option.value}
          />
          <span>{option.label}</span>
        </label>
      ))}
    </div>
  );
}

function MachineEvaluationDayModeOptions({ value }) {
  const normalizedValue = normalizeMachineEvaluationDayMode(value);
  return (
    <div className="metricToggleRow commonConditionModeOptions">
      {MACHINE_EVALUATION_DAY_MODE_OPTIONS.map((option) => (
        <label
          key={option.value}
          className={`metricToggleChip ${
            normalizedValue === option.value ? "metricToggleChipActive" : ""
          }`}
        >
          <input
            type="radio"
            name="machineEvaluationDayMode"
            value={option.value}
            defaultChecked={normalizedValue === option.value}
          />
          <span>{option.label}</span>
        </label>
      ))}
    </div>
  );
}

function normalizeMachineNameText(value) {
  return String(value ?? "").normalize("NFKC").replace(/\s+/gu, "").trim();
}

function isAimJugglerMachine(machineName) {
  const normalizedMachineName = normalizeMachineNameText(machineName);
  return AIM_JUGGLER_MACHINE_NAMES.some(
    (candidate) => normalizeMachineNameText(candidate) === normalizedMachineName,
  );
}

function isHanabiMachine(machineName) {
  const normalizedMachineName = normalizeMachineNameText(machineName);
  return HANABI_MACHINE_NAMES.some(
    (candidate) => normalizeMachineNameText(candidate) === normalizedMachineName,
  );
}

function normalizeCombineAimJuggler(values, machineNames = [], machineTouched = false) {
  if (selectionIncludesAimJugglerHuntMachineGroup(machineNames)) {
    return true;
  }
  const safeValues = (Array.isArray(values) ? values : [])
    .map((value) => String(value ?? "").trim())
    .filter(Boolean);
  if (safeValues.length === 0) {
    return !machineTouched;
  }
  return safeValues.includes("1") || safeValues.includes("true") || safeValues.includes("on");
}

function normalizeCombineHanabi(values, machineNames = [], machineTouched = false) {
  if (selectionIncludesHanabiHuntMachineGroup(machineNames)) {
    return true;
  }
  const safeValues = (Array.isArray(values) ? values : [])
    .map((value) => String(value ?? "").trim())
    .filter(Boolean);
  if (safeValues.length === 0) {
    return !machineTouched;
  }
  return safeValues.includes("1") || safeValues.includes("true") || safeValues.includes("on");
}

function readRankingSortNumber(value, fallbackValue = Number.MAX_SAFE_INTEGER) {
  const parsedValue = Number(value);
  return Number.isFinite(parsedValue) ? parsedValue : fallbackValue;
}

function storeDayStatusIsClosed(status) {
  return String(status?.status ?? "").trim().toLowerCase() === STORE_DAY_STATUS_CLOSED;
}

function compareRankingRows(left, right) {
  return (
    readRankingSortNumber(right.huntScore, 0) - readRankingSortNumber(left.huntScore, 0) ||
    readRankingSortNumber(left.overallRank ?? left.selectedRank ?? left.rank) -
      readRankingSortNumber(right.overallRank ?? right.selectedRank ?? right.rank) ||
    String(left.machineName ?? "").localeCompare(String(right.machineName ?? ""), "ja") ||
    String(left.slotNumber ?? "").localeCompare(String(right.slotNumber ?? ""), "ja", {
      numeric: true,
    })
  );
}

function buildRankingRowKey(row) {
  return String(row?.rowKey ?? `${row?.machineName ?? ""}::${row?.slotNumber ?? ""}`).trim();
}

function resolveRankingGroupName(machineName, combineAimJuggler, combineHanabi) {
  if (combineAimJuggler && isAimJugglerMachine(machineName)) {
    return AIM_JUGGLER_GROUP_NAME;
  }
  if (combineHanabi && isHanabiMachine(machineName)) {
    return HANABI_GROUP_NAME;
  }
  return String(machineName ?? "").trim();
}

function buildVisibleRankingGroups(
  rankingGroups,
  selectedMachineNameSet,
  combineAimJuggler,
  combineHanabi,
  displayLimit,
) {
  const groupsByName = new Map();

  for (const group of Array.isArray(rankingGroups) ? rankingGroups : []) {
    const machineName = String(group.machineName ?? "").trim();
    if (!selectedMachineNameSet.has(machineName)) {
      continue;
    }

    const rankingGroupName = resolveRankingGroupName(machineName, combineAimJuggler, combineHanabi);
    if (!groupsByName.has(rankingGroupName)) {
      groupsByName.set(rankingGroupName, {
        machineName: rankingGroupName,
        rows: [],
        totalCount: 0,
        isCombinedGroup:
          (rankingGroupName === AIM_JUGGLER_GROUP_NAME || rankingGroupName === HANABI_GROUP_NAME) &&
          machineName !== rankingGroupName,
      });
    }

    const rankingGroup = groupsByName.get(rankingGroupName);
    const sourceRows = Array.isArray(group.allRows) ? group.allRows : group.rows;
    rankingGroup.totalCount += group.totalCount ?? sourceRows?.length ?? 0;
    rankingGroup.rows.push(
      ...(Array.isArray(sourceRows) ? sourceRows : []).map((row) => ({
        ...row,
        machineName: String(row.machineName ?? machineName).trim(),
      })),
    );
  }

  const groupedRankings = [...groupsByName.values()]
    .map((group) => {
      const rankedAllRows = group.rows
        .sort(compareRankingRows)
        .map((row, index) => ({
          ...row,
          rank: index + 1,
          machineRank: index + 1,
        }));
      const rankedRows = rankedAllRows.slice(0, displayLimit);

      return {
        ...group,
        limit: Math.min(displayLimit, group.totalCount),
        allRows: rankedAllRows,
        rows: rankedRows,
      };
    })
    .filter((group) => group.rows.length > 0);

  const selectedRankByRowKey = new Map(
    groupedRankings
      .flatMap((group) => group.allRows)
      .sort(compareRankingRows)
      .map((row, index) => [buildRankingRowKey(row), index + 1]),
  );

  return groupedRankings.map((group) => ({
    ...group,
    allRows: group.allRows.map((row) => ({
      ...row,
      selectedRank: selectedRankByRowKey.get(buildRankingRowKey(row)) ?? row.selectedRank,
    })),
    rows: group.rows.map((row) => ({
      ...row,
      selectedRank: selectedRankByRowKey.get(buildRankingRowKey(row)) ?? row.selectedRank,
    })),
  }));
}

export async function generateMetadata({ params }) {
  const resolvedParams = await params;
  const storeId = resolvedParams.storeId;

  try {
    const store = await getStoreIdentity(storeId);
    return {
      title: store ? `${store.storeName}の狙い度ランキング` : "狙い度ランキング",
    };
  } catch {
    return {
      title: "狙い度ランキング",
    };
  }
}

export default async function HuntAnalysisPage({ params, searchParams }) {
  const resolvedParams = await params;
  const resolvedSearchParams = await searchParams;
  const storeId = resolvedParams.storeId;
  const cookieStore = await cookies();
  const resultDisplayKey = buildHuntRankingResultDisplayKey(storeId);
  const savedParamAccess = buildSavedParamAccess(
    readFormStateEntriesFromCookies(cookieStore, resultDisplayKey),
    resolvedSearchParams,
  );
  const requestedDate = savedParamAccess.readSingle("date");
  const resultRequested = isResultDisplayCookieEnabled(
    cookieStore.get(getResultDisplayCookieName(resultDisplayKey))?.value,
  );
  const requestedMachineNames = savedParamAccess.readMulti("machine");
  const requestedRankingLogicKey = savedParamAccess.readSingle("huntScoreLogicKey");
  const huntScoreLogicKey = decodeHuntScoreLogicCookieValue(
    cookieStore.get(getHuntScoreLogicCookieName(storeId))?.value ?? "",
  );
  const machineEvaluationSettings = decodeMachineEvaluationSettingsCookieValue(
    cookieStore.get(getMachineEvaluationCookieName(storeId))?.value ?? "",
  );
  const machineEvaluationRankingMode = normalizeMachineEvaluationRankingMode(
    savedParamAccess.readSingle("machineEvaluationRankingMode"),
  );
  const machineEvaluationDayMode = normalizeMachineEvaluationDayMode(
    savedParamAccess.readSingle("machineEvaluationDayMode"),
  );
  const differenceMode = normalizeDifferenceMode(
    savedParamAccess.readSingle("differenceMode"),
  );
  const settingEstimateMode = normalizeSettingEstimateMode(
    savedParamAccess.readSingle("settingEstimateMode"),
  );
  const machineFilterTouched = savedParamAccess.readSingle("machineTouched") === "1";
  const requestedCombineAimJuggler = normalizeCombineAimJuggler(
    savedParamAccess.readMulti("aimMachineGroup"),
    requestedMachineNames,
    machineFilterTouched,
  );
  const requestedCombineHanabi = normalizeCombineHanabi(
    savedParamAccess.readMulti("hanabiMachineGroup"),
    requestedMachineNames,
    machineFilterTouched,
  );
  let detail;
  let storeOptions = [];

  try {
    const detailPromise = resultRequested
      ? getHuntScoreRankingDetail(
          storeId,
          requestedDate,
          HUNT_RANKING_ALL_MACHINE_LIMIT,
          huntScoreLogicKey,
          differenceMode,
          settingEstimateMode,
          {
            machineNames: requestedMachineNames,
            machineTouched: machineFilterTouched,
            huntScoreLogicKeys: requestedRankingLogicKey ? [requestedRankingLogicKey] : [],
            combineAimJuggler: requestedCombineAimJuggler,
            combineHanabi: requestedCombineHanabi,
            machineEvaluationSettings,
            machineEvaluationDayMode,
            requestedDate,
          },
        )
      : getHuntScoreInitialPageDetail(
          storeId,
          {
            differenceMode,
            settingEstimateMode,
            huntScoreLogicKeys: requestedRankingLogicKey ? [requestedRankingLogicKey] : [],
            machineEvaluationSettings,
          },
          huntScoreLogicKey,
        );
    const [nextDetail, stores] = await Promise.all([detailPromise, getStoreList()]);
    detail = nextDetail;
    storeOptions = stores.filter((store) => !store.isPendingRegistration);
  } catch (error) {
    return (
      <main className="pageStack">
        <Breadcrumbs
          items={[
            { label: "店舗一覧", href: "/" },
            { label: "店舗ページ", href: `/stores/${storeId}` },
            { label: "狙い度ランキング" },
          ]}
        />
        <section className="statusPanel">
          <h2>狙い度ランキングを読み込めませんでした</h2>
          <p>{error instanceof Error ? error.message : "設定を確認してください。"}</p>
        </section>
      </main>
    );
  }

  if (!detail) {
    notFound();
  }

  const fallbackNotice =
    resultRequested && detail.requestedDate && detail.requestedDate !== detail.selectedDate
      ? storeDayStatusIsClosed(detail.requestedDateStatus)
        ? "指定した日付は店休日のため、最新の集計日を表示しています。"
        : "指定した日付は見つからなかったため、最新の集計日を表示しています。"
      : "";
  const availableMachineNames =
    Array.isArray(detail.availableMachineNames) && detail.availableMachineNames.length > 0
      ? detail.availableMachineNames
      : detail.rankingGroups.map((group) => group.machineName);
  const rankingDateOptions =
    Array.isArray(detail.rankingDateOptions) && detail.rankingDateOptions.length > 0
      ? detail.rankingDateOptions
      : detail.rankingDates.map((date) => ({
          date,
          nextBusinessDate: date === detail.selectedDate ? detail.nextBusinessDate : null,
        }));
  const availableMachineNameSet = new Set(availableMachineNames);
  const hasAimJugglerGroupOption = AIM_JUGGLER_MACHINE_NAMES.some((machineName) =>
    availableMachineNameSet.has(machineName),
  );
  const hasHanabiGroupOption = HANABI_MACHINE_NAMES.every((machineName) =>
    availableMachineNameSet.has(machineName),
  );
  const combineAimJuggler = hasAimJugglerGroupOption ? requestedCombineAimJuggler : false;
  const combineHanabi = hasHanabiGroupOption ? requestedCombineHanabi : false;
  const requestedMachineNameSet = new Set(
    expandHuntMachineCombinedGroupSelection(requestedMachineNames)
      .map((machineName) => String(machineName ?? "").trim())
      .filter((machineName) => availableMachineNameSet.has(machineName)),
  );
  const selectedMachineNameSet = machineFilterTouched
    ? requestedMachineNameSet
    : new Set(availableMachineNames);
  const showMachineTopCandidates = selectedMachineNameSet.size >= 2;
  const showGrapeColumn = [...selectedMachineNameSet].some(isHuntJugglerMachine);
  const huntScoreLogicLabel =
    detail.huntScoreLogics?.length > 0
      ? detail.huntScoreLogics.map((logic) => logic.name).join(" + ")
      : detail.huntScoreLogic?.name ?? "狙い度";
  const machineOptions = availableMachineNames.map((machineName) => ({
    name: machineName,
    checked: selectedMachineNameSet.has(machineName),
    slotCount: detail.machineSlotCounts?.[machineName] ?? null,
  }));
  const machineOptionGroups = groupHuntMachineOptions(machineOptions, {
    combineAimJuggler,
    combineHanabi,
  });
  const huntScoreLogicOptions = listHuntScoreLogicOptions();
  const visibleRankingGroups = applyMachineEvaluationRankingMode(
    buildVisibleRankingGroups(
      resultRequested ? detail.rankingGroups : [],
      selectedMachineNameSet,
      combineAimJuggler,
      combineHanabi,
      detail.limit,
    ),
    machineEvaluationRankingMode,
    detail.limit,
  );
  const visibleRows = visibleRankingGroups.flatMap((group) => group.rows);
  const rankingFormStateKey = JSON.stringify({
    date: detail.selectedDate ?? "",
    differenceMode: detail.differenceMode,
    settingEstimateMode: detail.settingEstimateMode,
    huntScoreLogicKeys: detail.huntScoreLogicKeys,
    machines: [...selectedMachineNameSet].sort(),
    combineAimJuggler,
    combineHanabi,
    machineEvaluationRankingMode,
    machineEvaluationDayMode,
  });

  return (
    <main className="pageStack">
      <HuntRankingFormStateSync
        storeId={detail.store.id}
        formId={HUNT_RANKING_FORM_ID}
        formStateKey={rankingFormStateKey}
        resultActive={resultRequested}
      />
      <ResultDisplayStateSync
        formId={HUNT_RANKING_FORM_ID}
        stateKey={resultDisplayKey}
        conditionStateKey={resultDisplayKey}
        conditionParamKeys={HUNT_RANKING_CONDITION_PARAM_KEYS}
        active={resultRequested}
      />
      <Breadcrumbs
        items={[
          { label: "店舗一覧", href: "/" },
          { label: detail.store.storeName, href: `/stores/${detail.store.id}` },
          { label: "狙い度ランキング" },
        ]}
      />

      <section className="heroPanel">
        <div className="heroCopy">
          <h1 className="pageTitle pageTitleCompact">狙い度ランキング</h1>
          <div className="storeContextLine">
            <StoreFavoriteButton
              store={{ id: detail.store.id, storeName: detail.store.storeName }}
              compact
            />
            <Link href={`/stores/${detail.store.id}`} className="storeContextLink">
              {detail.store.storeName}
            </Link>
          </div>
          <StoreSwitcher stores={storeOptions} currentStoreId={detail.store.id} />
          {detail.huntScoreLogics?.length > 0 ? (
            <p className="dataSourceLabel">
              使用するロジック: {detail.huntScoreLogics.map((logic) => logic.name).join(" + ")}
            </p>
          ) : detail.huntScoreLogic ? (
            <p className="dataSourceLabel">適用中: {detail.huntScoreLogic.name}</p>
          ) : null}
          <div className="heroLinks simpleHeroLinks">
            <Link href={`/stores/${detail.store.id}`} className="externalLink">
              店舗ページへ戻る
            </Link>
            <Link href={`/stores/${detail.store.id}/hunt-backtest`} className="externalLink">
              バックテストを見る
            </Link>
            {detail.store.storeUrl ? (
              <a href={detail.store.storeUrl} target="_blank" rel="noreferrer" className="externalLink">
                店舗ページを開く
              </a>
            ) : null}
          </div>
        </div>
      </section>

      {availableMachineNames.length > 0 ? (
        <>
          <section className="filterPanel">
            <div>
              <p className="sectionLabel">集計日を選ぶ</p>
              <p className="filterLead">
                選んだ日の狙い度と、その次の営業日の実績を並べて確認できます。
              </p>
            </div>
            <NativeGetForm
              key={rankingFormStateKey}
              id={HUNT_RANKING_FORM_ID}
              action={`/stores/${detail.store.id}/hunt-analysis`}
              className="storeReserveForm"
            >
              <input type="hidden" name="machineTouched" value="1" />
              <div className="filterConditionBox rankingConditionBox">
                <p className="filterConditionBoxTitle">集計条件</p>
                <div className="rankingDateGrid">
                  <label className="storeReserveField">
                    <span>狙い度の日</span>
                    {resultRequested && rankingDateOptions.length > 0 ? (
                      <select name="date" defaultValue={detail.selectedDate ?? ""} className="storeReserveInput">
                        {rankingDateOptions.map((option) => (
                          <option key={option.date} value={option.date}>
                            {formatRankingDateOption(option.date, option.nextBusinessDate)}
                          </option>
                        ))}
                      </select>
                    ) : (
                      <input
                        type="date"
                        name="date"
                        defaultValue={detail.selectedDate ?? ""}
                        className="storeReserveInput"
                      />
                    )}
                  </label>
                </div>
              </div>
              <div className="filterConditionBox rankingConditionBox">
                <p className="filterConditionBoxTitle">差枚基準</p>
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
              <div className="filterConditionBox rankingConditionBoxWide">
                <HuntScoreLogicSingleSelect
                  selectedLogicKey={detail.huntScoreLogicKeys?.[0] ?? detail.huntScoreLogic?.key ?? ""}
                  options={huntScoreLogicOptions}
                />
              </div>
              <div className="filterConditionBox rankingConditionBox">
                <p className="filterConditionBoxTitle">設定推定基準</p>
                <SettingEstimateModeOptions value={detail.settingEstimateMode} />
              </div>
              <div className="filterConditionBox rankingConditionBoxWide">
                <p className="filterConditionBoxTitle">機種別評価</p>
                <MachineEvaluationRankingModeOptions value={machineEvaluationRankingMode} />
              </div>
              <div className="filterConditionBox rankingConditionBox">
                <p className="filterConditionBoxTitle">日別評価</p>
                <MachineEvaluationDayModeOptions value={machineEvaluationDayMode} />
              </div>
              <div className="filterConditionBox rankingConditionBoxWide">
                <p className="filterConditionBoxTitle">表示機種</p>
                {machineOptions.length > 0 ? (
                  <div className="backtestBlock rankingCustomMachineFilter">
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
                ) : null}
              </div>
              <div className="backtestButtonRow">
                <button type="submit" className="storeReserveButton backtestPrimaryButton">
                  表示する
                </button>
              </div>
            </NativeGetForm>
            {fallbackNotice ? <p className="storeReserveHelp">{fallbackNotice}</p> : null}
            {resultRequested && !detail.nextBusinessDate ? (
              <p className="filterPanelStatus">最新日のため、翌営業日の実績はまだありません。</p>
            ) : null}
          </section>

          <ResultUrlTools active={resultRequested} />

          {resultRequested ? (
            detail.rankingDates.length > 0 ? (
              <HuntRankingTable
                storeId={detail.store.id}
                storeName={detail.store.storeName}
                rows={visibleRows}
                rankingGroups={visibleRankingGroups}
                predictionDate={detail.predictionDate}
                actualDate={detail.nextBusinessDate}
                enableConditionHighlight={false}
                initialDifferenceMode={detail.differenceMode}
                showMachineTopCandidates={showMachineTopCandidates}
                showOverallRanking={false}
                machineGroupTitleMode="all"
                showMachineEvaluation={shouldShowMachineEvaluationInRanking(machineEvaluationRankingMode)}
                showGrapeColumn={showGrapeColumn}
                huntScoreLogicLabel={huntScoreLogicLabel}
              />
            ) : (
              <section className="statusPanel">
                <h2>狙い度ランキングを作れる日付がまだありません</h2>
                <p>対象機種の保存済みデータが増えると、ここに点数順の一覧が表示されます。</p>
              </section>
            )
          ) : (
            <section className="statusPanel">
              <h2>狙い度ランキングはまだ表示していません</h2>
              <p>条件を選んで表示すると、対象機種の台データを読み込んで集計します。</p>
            </section>
          )}
        </>
      ) : (
        <section className="statusPanel">
          <h2>狙い度ランキングを作れる日付がまだありません</h2>
          <p>対象機種の保存済みデータが増えると、ここに点数順の一覧が表示されます。</p>
        </section>
      )}
    </main>
  );
}
