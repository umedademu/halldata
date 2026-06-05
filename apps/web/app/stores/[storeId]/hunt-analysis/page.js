import Link from "next/link";
import { cookies } from "next/headers";
import { notFound } from "next/navigation";

import { Breadcrumbs } from "../../../../components/breadcrumbs";
import {
  AllMachineFilterButtons,
  MachineFilterCategoryButton,
} from "../../../../components/hunt-machine-filter-tools";
import { HuntRankingFormStateSync } from "../../../../components/hunt-ranking-form-state-sync";
import { HuntRankingConditionSelector } from "../../../../components/hunt-ranking-condition-selector";
import { HuntRankingTable } from "../../../../components/hunt-ranking-table";
import {
  HuntScoreLogicMultiSelect,
} from "../../../../components/hunt-score-logic-selector";
import { NativeGetForm } from "../../../../components/native-get-form";
import { ResultUrlTools } from "../../../../components/result-url-tools";
import { StoreFavoriteButton } from "../../../../components/store-favorite-button";
import {
  getHuntScoreInitialPageDetail,
  getHuntScoreRankingDetail,
  getStoreIdentity,
} from "../../../../lib/data";
import { formatMonthDay } from "../../../../lib/format";
import {
  decodeHuntScoreLogicCookieValue,
  getHuntScoreLogicCookieName,
} from "../../../../lib/hunt-score-logic-selection";
import { listHuntScoreLogicOptions } from "../../../../lib/hunt-score";
import {
  MACHINE_EVALUATION_RANKING_MODE_OPTIONS,
  applyMachineEvaluationRankingMode,
  decodeMachineEvaluationSettingsCookieValue,
  getMachineEvaluationCookieName,
  normalizeMachineEvaluationRankingMode,
  shouldShowMachineEvaluationInRanking,
} from "../../../../lib/machine-evaluation";
import {
  expandHuntMachineCombinedGroupSelection,
  groupHuntMachineOptions,
  selectionIncludesAimJugglerHuntMachineGroup,
  selectionIncludesHanabiHuntMachineGroup,
} from "../../../../lib/hunt-machine-display";
import { normalizeDifferenceMode } from "../../../../lib/machine-difference";
import { SETTING_ESTIMATE_MODE_OPTIONS, normalizeSettingEstimateMode } from "../../../../lib/setting-estimates";

export const dynamic = "force-dynamic";

const DEFAULT_RANKING_LIMIT = 20;
const DEFAULT_HIGHLIGHT_RANK_MIN = "1";
const DEFAULT_HIGHLIGHT_RANK_MAX = "3";
const DEFAULT_HIGHLIGHT_MACHINE_RANK_MIN = "";
const DEFAULT_HIGHLIGHT_MACHINE_RANK_MAX = "";
const DEFAULT_HIGHLIGHT_SELECTED_RANK_MIN = "1";
const DEFAULT_HIGHLIGHT_SELECTED_RANK_MAX = "3";
const DEFAULT_HIGHLIGHT_SCORE_MIN = "70";
const DEFAULT_HIGHLIGHT_SCORE_MAX = "";
const DEFAULT_HIGHLIGHT_NEXT_GAP_MIN = "";
const DEFAULT_HIGHLIGHT_NEXT_GAP_MAX = "";
const DEFAULT_HIGHLIGHT_UPPER_GAP_MIN = "";
const DEFAULT_HIGHLIGHT_UPPER_GAP_MAX = "";
const DEFAULT_HIGHLIGHT_RANK_SCOPE = "selected";
const DEFAULT_HIGHLIGHT_NEXT_GAP_SCOPE = "machine";
const DEFAULT_HIGHLIGHT_RANK_REQUIRED = true;
const DEFAULT_HIGHLIGHT_MACHINE_RANK_REQUIRED = false;
const DEFAULT_HIGHLIGHT_SELECTED_RANK_REQUIRED = true;
const DEFAULT_HIGHLIGHT_SCORE_REQUIRED = true;
const DEFAULT_HIGHLIGHT_NEXT_GAP_REQUIRED = false;
const DEFAULT_HIGHLIGHT_UPPER_GAP_REQUIRED = false;
const AIM_JUGGLER_GROUP_NAME = "アイムジャグラーEX";
const AIM_JUGGLER_MACHINE_NAMES = ["SアイムジャグラーＥＸ", "ネオアイムジャグラーEX"];
const HANABI_GROUP_NAME = "ハナビ";
const HANABI_MACHINE_NAMES = ["新ハナビ", "スマスロ ハナビ"];
const HUNT_RANKING_FORM_ID = "hunt-ranking-condition-form";

async function readStoredHuntScoreLogicKey(storeId) {
  const cookieStore = await cookies();
  return decodeHuntScoreLogicCookieValue(
    cookieStore.get(getHuntScoreLogicCookieName(storeId))?.value ?? "",
  );
}

async function readStoredMachineEvaluationSettings(storeId) {
  const cookieStore = await cookies();
  return decodeMachineEvaluationSettingsCookieValue(
    cookieStore.get(getMachineEvaluationCookieName(storeId))?.value ?? "",
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

function hasSearchParam(searchParams, key) {
  return Object.hasOwn(searchParams ?? {}, key);
}

function readSearchParamWithDefault(searchParams, key, defaultValue) {
  return hasSearchParam(searchParams, key) ? readSingleSearchParam(searchParams?.[key]) : defaultValue;
}

function readMultiSearchParamWithDefault(searchParams, key, defaultValue) {
  return hasSearchParam(searchParams, key) ? readMultiSearchParam(searchParams?.[key]) : [defaultValue];
}

function normalizeHighlightScope(value, fallbackValue) {
  if (value === "machine" || value === "selected") {
    return value;
  }
  return fallbackValue;
}

function parseRequestedLimit(value) {
  const parsedValue = Number(value);
  if (!Number.isInteger(parsedValue) || parsedValue < 1) {
    return DEFAULT_RANKING_LIMIT;
  }
  return parsedValue;
}

function formatRankingDateOption(date, nextBusinessDate) {
  const scoreDateLabel = formatMonthDay(date);
  const actualDateLabel = nextBusinessDate ? `${formatMonthDay(nextBusinessDate)}実績` : "実績なし";
  return `${scoreDateLabel}狙い度 → ${actualDateLabel}`;
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
  const requestedDate = readSingleSearchParam(resolvedSearchParams?.date);
  const resultRequested = readSingleSearchParam(resolvedSearchParams?.show) === "1";
  const requestedLimit = parseRequestedLimit(readSingleSearchParam(resolvedSearchParams?.limit));
  const requestedMachineNames = readMultiSearchParam(resolvedSearchParams?.machine);
  const requestedRankingLogicKeys = readMultiSearchParam(resolvedSearchParams?.huntScoreLogicKey);
  const requestedSubHuntScoreLogicKey = readSingleSearchParam(
    resolvedSearchParams?.subHuntScoreLogicKey,
  );
  const huntScoreLogicKey = await readStoredHuntScoreLogicKey(storeId);
  const machineEvaluationSettings = await readStoredMachineEvaluationSettings(storeId);
  const machineEvaluationRankingMode = normalizeMachineEvaluationRankingMode(
    readSingleSearchParam(resolvedSearchParams?.machineEvaluationRankingMode),
  );
  const differenceMode = normalizeDifferenceMode(
    readSingleSearchParam(resolvedSearchParams?.differenceMode),
  );
  const settingEstimateMode = normalizeSettingEstimateMode(
    readSingleSearchParam(resolvedSearchParams?.settingEstimateMode),
  );
  const machineFilterTouched = readSingleSearchParam(resolvedSearchParams?.machineTouched) === "1";
  const requestedCombineAimJuggler = normalizeCombineAimJuggler(
    readMultiSearchParam(resolvedSearchParams?.aimMachineGroup),
    requestedMachineNames,
    machineFilterTouched,
  );
  const requestedCombineHanabi = normalizeCombineHanabi(
    readMultiSearchParam(resolvedSearchParams?.hanabiMachineGroup),
    requestedMachineNames,
    machineFilterTouched,
  );
  const hasMachineRankCondition =
    hasSearchParam(resolvedSearchParams, "machineRankMin") ||
    hasSearchParam(resolvedSearchParams, "machineRankMax");
  const hasSelectedRankCondition =
    hasSearchParam(resolvedSearchParams, "selectedRankMin") ||
    hasSearchParam(resolvedSearchParams, "selectedRankMax");
  const hasScopedRankCondition = hasMachineRankCondition || hasSelectedRankCondition;
  const legacyRankScope = normalizeHighlightScope(
    readSingleSearchParam(resolvedSearchParams?.rankScope),
    DEFAULT_HIGHLIGHT_RANK_SCOPE,
  );
  const legacyRankMin = readSearchParamWithDefault(
    resolvedSearchParams,
    "rankMin",
    DEFAULT_HIGHLIGHT_RANK_MIN,
  );
  const legacyRankMax = readSearchParamWithDefault(
    resolvedSearchParams,
    "rankMax",
    DEFAULT_HIGHLIGHT_RANK_MAX,
  );
  const defaultMachineRankMin =
    !hasScopedRankCondition && legacyRankScope === "machine"
      ? legacyRankMin
      : DEFAULT_HIGHLIGHT_MACHINE_RANK_MIN;
  const defaultMachineRankMax =
    !hasScopedRankCondition && legacyRankScope === "machine"
      ? legacyRankMax
      : DEFAULT_HIGHLIGHT_MACHINE_RANK_MAX;
  const defaultSelectedRankMin =
    !hasScopedRankCondition && legacyRankScope === "selected"
      ? legacyRankMin
      : hasScopedRankCondition
        ? ""
        : DEFAULT_HIGHLIGHT_SELECTED_RANK_MIN;
  const defaultSelectedRankMax =
    !hasScopedRankCondition && legacyRankScope === "selected"
      ? legacyRankMax
      : hasScopedRankCondition
        ? ""
        : DEFAULT_HIGHLIGHT_SELECTED_RANK_MAX;
  const legacyNextGapScope = normalizeHighlightScope(
    readSingleSearchParam(resolvedSearchParams?.nextGapScope),
    DEFAULT_HIGHLIGHT_NEXT_GAP_SCOPE,
  );
  const legacyNextGapMin = readSearchParamWithDefault(
    resolvedSearchParams,
    "nextGapMin",
    DEFAULT_HIGHLIGHT_NEXT_GAP_MIN,
  );
  const legacyNextGapMax = readSearchParamWithDefault(
    resolvedSearchParams,
    "nextGapMax",
    DEFAULT_HIGHLIGHT_NEXT_GAP_MAX,
  );
  const rankingHighlightOptions = {
    rankMin: legacyRankMin,
    rankMax: legacyRankMax,
    machineRankMin: readSearchParamWithDefault(
      resolvedSearchParams,
      "machineRankMin",
      defaultMachineRankMin,
    ),
    machineRankMax: readSearchParamWithDefault(
      resolvedSearchParams,
      "machineRankMax",
      defaultMachineRankMax,
    ),
    selectedRankMin: readSearchParamWithDefault(
      resolvedSearchParams,
      "selectedRankMin",
      defaultSelectedRankMin,
    ),
    selectedRankMax: readSearchParamWithDefault(
      resolvedSearchParams,
      "selectedRankMax",
      defaultSelectedRankMax,
    ),
    scoreMin: readSearchParamWithDefault(
      resolvedSearchParams,
      "scoreMin",
      DEFAULT_HIGHLIGHT_SCORE_MIN,
    ),
    scoreMax: readSearchParamWithDefault(
      resolvedSearchParams,
      "scoreMax",
      DEFAULT_HIGHLIGHT_SCORE_MAX,
    ),
    nextGapMin: readSearchParamWithDefault(
      resolvedSearchParams,
      "nextGapMin",
      DEFAULT_HIGHLIGHT_NEXT_GAP_MIN,
    ),
    nextGapMax: readSearchParamWithDefault(
      resolvedSearchParams,
      "nextGapMax",
      DEFAULT_HIGHLIGHT_NEXT_GAP_MAX,
    ),
    machineNextGapMin: readSearchParamWithDefault(
      resolvedSearchParams,
      "machineNextGapMin",
      legacyNextGapScope === "machine" ? legacyNextGapMin : DEFAULT_HIGHLIGHT_NEXT_GAP_MIN,
    ),
    machineNextGapMax: readSearchParamWithDefault(
      resolvedSearchParams,
      "machineNextGapMax",
      legacyNextGapScope === "machine" ? legacyNextGapMax : DEFAULT_HIGHLIGHT_NEXT_GAP_MAX,
    ),
    selectedNextGapMin: readSearchParamWithDefault(
      resolvedSearchParams,
      "selectedNextGapMin",
      legacyNextGapScope === "selected" ? legacyNextGapMin : DEFAULT_HIGHLIGHT_NEXT_GAP_MIN,
    ),
    selectedNextGapMax: readSearchParamWithDefault(
      resolvedSearchParams,
      "selectedNextGapMax",
      legacyNextGapScope === "selected" ? legacyNextGapMax : DEFAULT_HIGHLIGHT_NEXT_GAP_MAX,
    ),
    machineUpperGapMin: readSearchParamWithDefault(
      resolvedSearchParams,
      "machineUpperGapMin",
      DEFAULT_HIGHLIGHT_UPPER_GAP_MIN,
    ),
    machineUpperGapMax: readSearchParamWithDefault(
      resolvedSearchParams,
      "machineUpperGapMax",
      DEFAULT_HIGHLIGHT_UPPER_GAP_MAX,
    ),
    selectedUpperGapMin: readSearchParamWithDefault(
      resolvedSearchParams,
      "selectedUpperGapMin",
      DEFAULT_HIGHLIGHT_UPPER_GAP_MIN,
    ),
    selectedUpperGapMax: readSearchParamWithDefault(
      resolvedSearchParams,
      "selectedUpperGapMax",
      DEFAULT_HIGHLIGHT_UPPER_GAP_MAX,
    ),
    rankRequired: readMultiSearchParamWithDefault(
      resolvedSearchParams,
      "rankRequired",
      DEFAULT_HIGHLIGHT_RANK_REQUIRED ? "1" : "0",
    ),
    machineRankRequired: readMultiSearchParamWithDefault(
      resolvedSearchParams,
      "machineRankRequired",
      !hasScopedRankCondition && legacyRankScope === "machine"
        ? DEFAULT_HIGHLIGHT_RANK_REQUIRED
          ? "1"
          : "0"
        : hasMachineRankCondition && DEFAULT_HIGHLIGHT_MACHINE_RANK_REQUIRED
          ? "1"
          : "0",
    ),
    selectedRankRequired: readMultiSearchParamWithDefault(
      resolvedSearchParams,
      "selectedRankRequired",
      !hasScopedRankCondition && legacyRankScope === "selected"
        ? DEFAULT_HIGHLIGHT_RANK_REQUIRED
          ? "1"
          : "0"
        : hasSelectedRankCondition && DEFAULT_HIGHLIGHT_SELECTED_RANK_REQUIRED
          ? "1"
          : "0",
    ),
    scoreRequired: readMultiSearchParamWithDefault(
      resolvedSearchParams,
      "scoreRequired",
      DEFAULT_HIGHLIGHT_SCORE_REQUIRED ? "1" : "0",
    ),
    nextGapRequired: readMultiSearchParamWithDefault(
      resolvedSearchParams,
      "nextGapRequired",
      DEFAULT_HIGHLIGHT_NEXT_GAP_REQUIRED ? "1" : "0",
    ),
    machineNextGapRequired: readMultiSearchParamWithDefault(
      resolvedSearchParams,
      "machineNextGapRequired",
      DEFAULT_HIGHLIGHT_NEXT_GAP_REQUIRED ? "1" : "0",
    ),
    selectedNextGapRequired: readMultiSearchParamWithDefault(
      resolvedSearchParams,
      "selectedNextGapRequired",
      DEFAULT_HIGHLIGHT_NEXT_GAP_REQUIRED ? "1" : "0",
    ),
    machineUpperGapRequired: readMultiSearchParamWithDefault(
      resolvedSearchParams,
      "machineUpperGapRequired",
      DEFAULT_HIGHLIGHT_UPPER_GAP_REQUIRED ? "1" : "0",
    ),
    selectedUpperGapRequired: readMultiSearchParamWithDefault(
      resolvedSearchParams,
      "selectedUpperGapRequired",
      DEFAULT_HIGHLIGHT_UPPER_GAP_REQUIRED ? "1" : "0",
    ),
    rankScope: normalizeHighlightScope(
      readSingleSearchParam(resolvedSearchParams?.rankScope),
      DEFAULT_HIGHLIGHT_RANK_SCOPE,
    ),
    nextGapScope: legacyNextGapScope,
  };
  const rankRequired = rankingHighlightOptions.rankRequired.some((value) =>
    ["1", "true", "on"].includes(String(value ?? "").trim()),
  );
  const machineRankRequired = rankingHighlightOptions.machineRankRequired.some((value) =>
    ["1", "true", "on"].includes(String(value ?? "").trim()),
  );
  const selectedRankRequired = rankingHighlightOptions.selectedRankRequired.some((value) =>
    ["1", "true", "on"].includes(String(value ?? "").trim()),
  );
  const scoreRequired = rankingHighlightOptions.scoreRequired.some((value) =>
    ["1", "true", "on"].includes(String(value ?? "").trim()),
  );
  const nextGapRequired = rankingHighlightOptions.nextGapRequired.some((value) =>
    ["1", "true", "on"].includes(String(value ?? "").trim()),
  );
  const machineNextGapRequired = rankingHighlightOptions.machineNextGapRequired.some((value) =>
    ["1", "true", "on"].includes(String(value ?? "").trim()),
  );
  const selectedNextGapRequired = rankingHighlightOptions.selectedNextGapRequired.some((value) =>
    ["1", "true", "on"].includes(String(value ?? "").trim()),
  );
  const machineUpperGapRequired = rankingHighlightOptions.machineUpperGapRequired.some((value) =>
    ["1", "true", "on"].includes(String(value ?? "").trim()),
  );
  const selectedUpperGapRequired = rankingHighlightOptions.selectedUpperGapRequired.some((value) =>
    ["1", "true", "on"].includes(String(value ?? "").trim()),
  );
  const normalizedRankingHighlightOptions = {
    ...rankingHighlightOptions,
    rankRequired,
    machineRankRequired,
    selectedRankRequired,
    scoreRequired,
    nextGapRequired,
    machineNextGapRequired,
    selectedNextGapRequired,
    machineUpperGapRequired,
    selectedUpperGapRequired,
  };

  let detail;

  try {
    detail = resultRequested
      ? await getHuntScoreRankingDetail(
          storeId,
          requestedDate,
          requestedLimit,
          huntScoreLogicKey,
          differenceMode,
          settingEstimateMode,
          {
            machineNames: requestedMachineNames,
            machineTouched: machineFilterTouched,
            huntScoreLogicKeys: requestedRankingLogicKeys,
            subHuntScoreLogicKey: requestedSubHuntScoreLogicKey,
            combineAimJuggler: requestedCombineAimJuggler,
            combineHanabi: requestedCombineHanabi,
            machineEvaluationSettings,
            requestedDate,
          },
        )
      : await getHuntScoreInitialPageDetail(
          storeId,
          {
            differenceMode,
            settingEstimateMode,
            huntScoreLogicKeys: requestedRankingLogicKeys,
            subHuntScoreLogicKey: requestedSubHuntScoreLogicKey,
            machineEvaluationSettings,
          },
          huntScoreLogicKey,
        );
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
      ? "指定した日付は見つからなかったため、最新の集計日を表示しています。"
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
  const customHighlightBookmark = {
    storeId: detail.store.id,
    name: "カスタム条件",
    allMachineCount: availableMachineNames.length,
    machineNames: [...selectedMachineNameSet],
    rankMin: rankingHighlightOptions.rankMin,
    rankMax: rankingHighlightOptions.rankMax,
    machineRankMin: rankingHighlightOptions.machineRankMin,
    machineRankMax: rankingHighlightOptions.machineRankMax,
    selectedRankMin: rankingHighlightOptions.selectedRankMin,
    selectedRankMax: rankingHighlightOptions.selectedRankMax,
    scoreMin: rankingHighlightOptions.scoreMin,
    scoreMax: rankingHighlightOptions.scoreMax,
    machineNextGapMin: rankingHighlightOptions.machineNextGapMin,
    machineNextGapMax: rankingHighlightOptions.machineNextGapMax,
    selectedNextGapMin: rankingHighlightOptions.selectedNextGapMin,
    selectedNextGapMax: rankingHighlightOptions.selectedNextGapMax,
    machineUpperGapMin: rankingHighlightOptions.machineUpperGapMin,
    machineUpperGapMax: rankingHighlightOptions.machineUpperGapMax,
    selectedUpperGapMin: rankingHighlightOptions.selectedUpperGapMin,
    selectedUpperGapMax: rankingHighlightOptions.selectedUpperGapMax,
    rankRequired,
    machineRankRequired,
    selectedRankRequired,
    scoreRequired,
    nextGapRequired,
    machineNextGapRequired,
    selectedNextGapRequired,
    machineUpperGapRequired,
    selectedUpperGapRequired,
    combineAimJuggler,
    combineHanabi,
  };
  const rankingFormStateKey = JSON.stringify({
    date: detail.selectedDate ?? "",
    limit: detail.limit,
    differenceMode: detail.differenceMode,
    settingEstimateMode: detail.settingEstimateMode,
    huntScoreLogicKeys: detail.huntScoreLogicKeys,
    subHuntScoreLogicKey: detail.subHuntScoreLogic?.key ?? "",
    machines: [...selectedMachineNameSet].sort(),
    combineAimJuggler,
    combineHanabi,
    rankMin: rankingHighlightOptions.rankMin,
    rankMax: rankingHighlightOptions.rankMax,
    machineRankMin: rankingHighlightOptions.machineRankMin,
    machineRankMax: rankingHighlightOptions.machineRankMax,
    selectedRankMin: rankingHighlightOptions.selectedRankMin,
    selectedRankMax: rankingHighlightOptions.selectedRankMax,
    scoreMin: rankingHighlightOptions.scoreMin,
    scoreMax: rankingHighlightOptions.scoreMax,
    nextGapMin: rankingHighlightOptions.nextGapMin,
    nextGapMax: rankingHighlightOptions.nextGapMax,
    machineNextGapMin: rankingHighlightOptions.machineNextGapMin,
    machineNextGapMax: rankingHighlightOptions.machineNextGapMax,
    selectedNextGapMin: rankingHighlightOptions.selectedNextGapMin,
    selectedNextGapMax: rankingHighlightOptions.selectedNextGapMax,
    machineUpperGapMin: rankingHighlightOptions.machineUpperGapMin,
    machineUpperGapMax: rankingHighlightOptions.machineUpperGapMax,
    selectedUpperGapMin: rankingHighlightOptions.selectedUpperGapMin,
    selectedUpperGapMax: rankingHighlightOptions.selectedUpperGapMax,
    rankRequired,
    machineRankRequired,
    selectedRankRequired,
    scoreRequired,
    nextGapRequired,
    machineNextGapRequired,
    selectedNextGapRequired,
    machineUpperGapRequired,
    selectedUpperGapRequired,
    rankScope: rankingHighlightOptions.rankScope,
    nextGapScope: rankingHighlightOptions.nextGapScope,
    machineEvaluationRankingMode,
  });

  return (
    <main className="pageStack">
      <HuntRankingFormStateSync
        storeId={detail.store.id}
        formId={HUNT_RANKING_FORM_ID}
        formStateKey={rankingFormStateKey}
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
          {detail.huntScoreLogics?.length > 0 ? (
            <p className="dataSourceLabel">
              使用するロジック: {detail.huntScoreLogics.map((logic) => logic.name).join(" + ")}
            </p>
          ) : detail.huntScoreLogic ? (
            <p className="dataSourceLabel">適用中: {detail.huntScoreLogic.name}</p>
          ) : null}
          {detail.subHuntScoreLogic ? (
            <p className="dataSourceLabel">表示用ロジック: {detail.subHuntScoreLogic.name}</p>
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
              <input type="hidden" name="show" value="1" />
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
                  <label className="storeReserveField">
                    <span>各機種何位まで表示</span>
                    <input
                      type="number"
                      name="limit"
                      min="1"
                      max={Math.max(detail.totalCount, detail.limit, 1)}
                      defaultValue={detail.limit}
                      className="storeReserveInput"
                    />
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
                <HuntScoreLogicMultiSelect
                  selectedLogicKeys={detail.huntScoreLogicKeys}
                  options={huntScoreLogicOptions}
                  formId={HUNT_RANKING_FORM_ID}
                />
              </div>
              <div className="filterConditionBox rankingConditionBox">
                <p className="filterConditionBoxTitle">表示用ロジック</p>
                <label className="storeReserveField">
                  <span>追加表示するロジック</span>
                  <select
                    name="subHuntScoreLogicKey"
                    defaultValue={detail.subHuntScoreLogic?.key ?? ""}
                    className="storeReserveInput"
                  >
                    <option value="">表示しない</option>
                    {huntScoreLogicOptions.map((option) => (
                      <option key={option.key} value={option.key}>
                        {option.name}
                      </option>
                    ))}
                  </select>
                </label>
              </div>
              <div className="filterConditionBox rankingConditionBox">
                <p className="filterConditionBoxTitle">設定推定基準</p>
                <SettingEstimateModeOptions value={detail.settingEstimateMode} />
              </div>
              <div className="filterConditionBox rankingConditionBoxWide">
                <p className="filterConditionBoxTitle">機種別評価</p>
                <MachineEvaluationRankingModeOptions value={machineEvaluationRankingMode} />
              </div>
              <div className="filterConditionBox rankingConditionBoxWide">
                <p className="filterConditionBoxTitle">強調条件</p>
                <HuntRankingConditionSelector storeId={detail.store.id} />
                <details className="collapsibleControlGroup crossBacktestConditionGroup">
                  <summary className="collapsibleControlHeader crossBacktestConditionSummary">
                    <span>選択機種・カスタム条件を直接指定</span>
                    <span className="collapsibleControlStatus crossBacktestConditionStatus" />
                  </summary>
                  <div className="collapsibleControlBody">
                    {machineOptions.length > 0 ? (
                      <div className="backtestBlock rankingCustomMachineFilter">
                        <p className="filterControlLabel">選択機種</p>
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
                    <div className="huntConditionRows">
                      <div className="commonConditionPanel">
                        <p className="scopedConditionColumnTitle">共通条件</p>
                        <div className="commonConditionGrid">
                          <ScopedConditionRow
                            label="狙い度"
                            minName="scoreMin"
                            maxName="scoreMax"
                            requiredName="scoreRequired"
                            minValue={rankingHighlightOptions.scoreMin}
                            maxValue={rankingHighlightOptions.scoreMax}
                            requiredValue={scoreRequired}
                            inputMax={undefined}
                          />
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
                            minValue={rankingHighlightOptions.machineRankMin}
                            maxValue={rankingHighlightOptions.machineRankMax}
                            requiredValue={machineRankRequired}
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
                            minValue={rankingHighlightOptions.machineUpperGapMin}
                            maxValue={rankingHighlightOptions.machineUpperGapMax}
                            requiredValue={machineUpperGapRequired}
                          />
                          <ScopedConditionRow
                            label="下差(同)"
                            minName="machineNextGapMin"
                            maxName="machineNextGapMax"
                            requiredName="machineNextGapRequired"
                            minValue={rankingHighlightOptions.machineNextGapMin}
                            maxValue={rankingHighlightOptions.machineNextGapMax}
                            requiredValue={machineNextGapRequired}
                          />
                        </div>
                        <div className="scopedConditionColumn">
                          <p className="scopedConditionColumnTitle">選択機種内</p>
                          <ScopedConditionRow
                            label="順位"
                            minName="selectedRankMin"
                            maxName="selectedRankMax"
                            requiredName="selectedRankRequired"
                            minValue={rankingHighlightOptions.selectedRankMin}
                            maxValue={rankingHighlightOptions.selectedRankMax}
                            requiredValue={selectedRankRequired}
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
                            minValue={rankingHighlightOptions.selectedUpperGapMin}
                            maxValue={rankingHighlightOptions.selectedUpperGapMax}
                            requiredValue={selectedUpperGapRequired}
                          />
                          <ScopedConditionRow
                            label="下差(全)"
                            minName="selectedNextGapMin"
                            maxName="selectedNextGapMax"
                            requiredName="selectedNextGapRequired"
                            minValue={rankingHighlightOptions.selectedNextGapMin}
                            maxValue={rankingHighlightOptions.selectedNextGapMax}
                            requiredValue={selectedNextGapRequired}
                          />
                        </div>
                      </div>
                    </div>
                  </div>
                </details>
              </div>
              <button type="submit" className="storeReserveButton">
                表示する
              </button>
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
                rows={visibleRows}
                rankingGroups={visibleRankingGroups}
                overallLimit={detail.limit}
                predictionDate={detail.predictionDate}
                actualDate={detail.nextBusinessDate}
                highlightOptions={normalizedRankingHighlightOptions}
                customHighlightBookmark={customHighlightBookmark}
                initialDifferenceMode={detail.differenceMode}
                showMachineTopCandidates={showMachineTopCandidates}
                subHuntScoreLogic={detail.subHuntScoreLogic}
                showMachineEvaluation={shouldShowMachineEvaluationInRanking(machineEvaluationRankingMode)}
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
