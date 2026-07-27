import {
  formatSettingEstimateModeLabel,
  normalizeSettingEstimateMode,
} from "./setting-estimates.js";
import {
  MACHINE_EVALUATION_BACKTEST_MODE_AND,
  MACHINE_EVALUATION_BACKTEST_MODE_MACHINE,
  MACHINE_EVALUATION_BACKTEST_MODE_OR,
  MACHINE_EVALUATION_BACKTEST_MODE_OPTIONS,
  normalizeMachineEvaluationBacktestMode,
} from "./machine-evaluation.js";

const HUNT_BACKTEST_BOOKMARK_STORAGE_PREFIX = "hunt-backtest-bookmark:";
const HUNT_BACKTEST_BOOKMARKS_STORAGE_PREFIX = "hunt-backtest-bookmarks:";
const HUNT_BACKTEST_BOOKMARK_SELECTION_STORAGE_PREFIX = "hunt-backtest-bookmark-selection:";
const DAILY_SELECTION_MODE_MACHINE_TOP_NEXT_GAP = "machineTopNextGap";
const LOGIC_CONDITION_MODE_AND = "and";
const LOGIC_CONDITION_MODE_SUM = "sum";
const AIM_JUGGLER_GROUP_NAME = "アイムジャグラーEX";
const AIM_JUGGLER_MACHINE_NAMES = ["SアイムジャグラーＥＸ", "ネオアイムジャグラーEX"];
const HANABI_GROUP_NAME = "ハナビ";
const HANABI_MACHINE_NAMES = ["新ハナビ", "スマスロ ハナビ"];
const SCORE_EPSILON = 0.000000001;

export const HUNT_BACKTEST_BOOKMARK_EVENT = "hunt-backtest-bookmark-change";
export const HUNT_BACKTEST_BOOKMARK_SELECTION_CUSTOM = "__custom";
export const HUNT_BACKTEST_BOOKMARK_SELECTION_NONE = "__none";
const SETTING_DISTRIBUTION_HIDE = "hide";
const SETTING_DISTRIBUTION_SHOW = "show";

function normalizeText(value) {
  return String(value ?? "").trim();
}

function normalizeBookmarkId(value) {
  return normalizeText(value).replace(/[^\w:-]/gu, "");
}

function createBookmarkId() {
  return `condition-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function normalizeBookmarkName(value, fallback = "保存条件") {
  const name = normalizeText(value);
  return name || fallback;
}

function normalizeSettingDistribution(value) {
  return normalizeText(value) === SETTING_DISTRIBUTION_SHOW
    ? SETTING_DISTRIBUTION_SHOW
    : SETTING_DISTRIBUTION_HIDE;
}

function formatMachineEvaluationBacktestModeLabel(value) {
  const normalizedValue = normalizeMachineEvaluationBacktestMode(value);
  return (
    MACHINE_EVALUATION_BACKTEST_MODE_OPTIONS.find((option) => option.value === normalizedValue)
      ?.label ?? "使用しない"
  );
}

function normalizePeriodMode(value, startDate, endDate) {
  if (value === "range") {
    return "range";
  }
  if (value === "recent") {
    return "recent";
  }
  return startDate && endDate ? "range" : "recent";
}

function normalizeTextArray(value) {
  return [
    ...new Set(
      (Array.isArray(value) ? value : [value])
        .map((entry) => normalizeText(entry))
        .filter(Boolean),
    ),
  ];
}

function normalizeNumberTextArray(value, minValue = null, maxValue = null) {
  return normalizeTextArray(value)
    .map((entry) => Number(entry))
    .filter((entry) => Number.isInteger(entry))
    .filter((entry) => minValue === null || entry >= minValue)
    .filter((entry) => maxValue === null || entry <= maxValue)
    .sort((left, right) => left - right)
    .map((entry) => String(entry));
}

function normalizeMachineNameText(value) {
  return normalizeText(value).normalize("NFKC").replace(/\s+/gu, "");
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

function isAimJugglerGroup(machineName) {
  return normalizeMachineNameText(machineName) === normalizeMachineNameText(AIM_JUGGLER_GROUP_NAME);
}

function isHanabiGroup(machineName) {
  return normalizeMachineNameText(machineName) === normalizeMachineNameText(HANABI_GROUP_NAME);
}

function includesBookmarkMachine(machineName, selectedMachineNameSet) {
  return (
    selectedMachineNameSet.has(machineName) ||
    (isAimJugglerMachine(machineName) && selectedMachineNameSet.has(AIM_JUGGLER_GROUP_NAME)) ||
    (isHanabiMachine(machineName) && selectedMachineNameSet.has(HANABI_GROUP_NAME))
  );
}

function normalizeDailySelectionMode(value) {
  const values = Array.isArray(value) ? value.map(normalizeText) : [normalizeText(value)];
  return values.includes(DAILY_SELECTION_MODE_MACHINE_TOP_NEXT_GAP)
    ? DAILY_SELECTION_MODE_MACHINE_TOP_NEXT_GAP
    : "";
}

function normalizeLogicConditionMode(value) {
  return normalizeText(value) === LOGIC_CONDITION_MODE_AND
    ? LOGIC_CONDITION_MODE_AND
    : LOGIC_CONDITION_MODE_SUM;
}

function isMachineTopNextGapSelectionMode(value) {
  return normalizeDailySelectionMode(value) === DAILY_SELECTION_MODE_MACHINE_TOP_NEXT_GAP;
}

function resolveBookmarkRankMachineName(
  machineName,
  combineAimJuggler,
  combineHanabi,
  selectedMachineNameSet,
) {
  if (
    isAimJugglerMachine(machineName) &&
    (combineAimJuggler || selectedMachineNameSet.has(AIM_JUGGLER_GROUP_NAME))
  ) {
    return AIM_JUGGLER_GROUP_NAME;
  }
  if (
    isHanabiMachine(machineName) &&
    (combineHanabi || selectedMachineNameSet.has(HANABI_GROUP_NAME))
  ) {
    return HANABI_GROUP_NAME;
  }
  return machineName;
}

export function normalizeDateText(value) {
  const normalized = normalizeText(value);
  if (!normalized) {
    return null;
  }

  const match = normalized.match(/^(\d{4})-(\d{2})-(\d{2})$/u);
  if (!match) {
    return null;
  }

  return `${match[1]}-${match[2]}-${match[3]}`;
}

function readNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const parsedValue = Number(value);
  return Number.isFinite(parsedValue) ? parsedValue : null;
}

function readPositiveInteger(value) {
  const parsedValue = Number(value);
  if (!Number.isInteger(parsedValue) || parsedValue < 1) {
    return null;
  }
  return parsedValue;
}

export function readFiniteNumber(value, fallbackValue = 0) {
  const parsedValue = readNumber(value);
  return parsedValue === null ? fallbackValue : parsedValue;
}

export function buildRankFilter(rankMinValue, rankMaxValue) {
  const parsedRankMin = readPositiveInteger(rankMinValue);
  const parsedRankMax = readPositiveInteger(rankMaxValue);

  if (parsedRankMin === null && parsedRankMax === null) {
    return {
      rankMin: null,
      rankMax: null,
      hasRankFilter: false,
    };
  }

  const rankMin = parsedRankMin ?? 1;
  const rankMax = parsedRankMax ?? rankMin;

  return {
    rankMin: Math.min(rankMin, rankMax),
    rankMax: Math.max(rankMin, rankMax),
    hasRankFilter: true,
  };
}

function clampRangeValue(value, minLimit = null, maxLimit = null) {
  const parsedValue = readNumber(value);
  if (parsedValue === null) {
    return null;
  }

  const lowerClampedValue = minLimit === null ? parsedValue : Math.max(minLimit, parsedValue);
  return maxLimit === null ? lowerClampedValue : Math.min(maxLimit, lowerClampedValue);
}

function buildNumberRange(minValue, maxValue, minLimit = null, maxLimit = null) {
  const parsedMin = clampRangeValue(minValue, minLimit, maxLimit);
  const parsedMax = clampRangeValue(maxValue, minLimit, maxLimit);

  if (parsedMin === null && parsedMax === null) {
    return {
      min: null,
      max: null,
      hasRangeFilter: false,
    };
  }

  if (parsedMin !== null && parsedMax !== null && parsedMin > parsedMax) {
    return {
      min: parsedMax,
      max: parsedMin,
      hasRangeFilter: true,
    };
  }

  return {
    min: parsedMin,
    max: parsedMax,
    hasRangeFilter: true,
  };
}

export function buildScoreFilter(scoreMinValue, scoreMaxValue = null, maxLimit = 100) {
  const parsedMaxLimit = readNumber(maxLimit);
  const scoreMaxLimit = parsedMaxLimit !== null && parsedMaxLimit >= 100 ? parsedMaxLimit : 100;
  const range = buildNumberRange(scoreMinValue, scoreMaxValue, 0, scoreMaxLimit);

  return {
    scoreMin: range.min,
    scoreMax: range.max,
    hasScoreFilter: range.hasRangeFilter,
  };
}

export function buildNextGapFilter(nextGapMinValue, nextGapMaxValue = null) {
  const range = buildNumberRange(nextGapMinValue, nextGapMaxValue, 0);

  return {
    nextGapMin: range.min,
    nextGapMax: range.max,
    hasNextGapFilter: range.hasRangeFilter,
  };
}

export function buildUpperGapFilter(upperGapMinValue, upperGapMaxValue = null) {
  const effectiveUpperGapMinValue = arguments.length < 2 ? null : upperGapMinValue;
  const effectiveUpperGapMaxValue = arguments.length < 2 ? upperGapMinValue : upperGapMaxValue;
  const range = buildNumberRange(effectiveUpperGapMinValue, effectiveUpperGapMaxValue, 0);

  return {
    upperGapMin: range.min,
    upperGapMax: range.max,
    hasUpperGapFilter: range.hasRangeFilter,
  };
}

function hasProvidedFilterOption(source, key) {
  return (
    source &&
    typeof source === "object" &&
    Object.hasOwn(source, key) &&
    source[key] !== undefined &&
    source[key] !== ""
  );
}

function hasAnyProvidedFilterOption(source, keys) {
  return keys.some((key) => hasProvidedFilterOption(source, key));
}

function readScopedBoundaryValues(source, scope, minKey, maxKey, legacyMinKey, legacyMaxKey) {
  const safeSource = source && typeof source === "object" ? source : {};
  if (hasAnyProvidedFilterOption(safeSource, [minKey, maxKey])) {
    return [safeSource[minKey], safeSource[maxKey]];
  }

  const legacyScope =
    safeSource.nextGapScope === "selected" || safeSource.nextGapScope === "machine"
      ? safeSource.nextGapScope
      : "machine";
  if (legacyScope === scope && hasAnyProvidedFilterOption(safeSource, [legacyMinKey, legacyMaxKey])) {
    return [safeSource[legacyMinKey], safeSource[legacyMaxKey]];
  }

  return [null, null];
}

export function buildBoundaryGapFilters(source = {}) {
  const [machineNextGapMin, machineNextGapMax] = readScopedBoundaryValues(
    source,
    "machine",
    "machineNextGapMin",
    "machineNextGapMax",
    "nextGapMin",
    "nextGapMax",
  );
  const [selectedNextGapMin, selectedNextGapMax] = readScopedBoundaryValues(
    source,
    "selected",
    "selectedNextGapMin",
    "selectedNextGapMax",
    "nextGapMin",
    "nextGapMax",
  );
  const [machineUpperGapMin, machineUpperGapMax] = readScopedBoundaryValues(
    source,
    "machine",
    "machineUpperGapMin",
    "machineUpperGapMax",
    "upperGapMin",
    "upperGapMax",
  );
  const [selectedUpperGapMin, selectedUpperGapMax] = readScopedBoundaryValues(
    source,
    "selected",
    "selectedUpperGapMin",
    "selectedUpperGapMax",
    "upperGapMin",
    "upperGapMax",
  );

  return {
    machineNextGapFilter: buildNextGapFilter(machineNextGapMin, machineNextGapMax),
    selectedNextGapFilter: buildNextGapFilter(selectedNextGapMin, selectedNextGapMax),
    machineUpperGapFilter: buildUpperGapFilter(machineUpperGapMin, machineUpperGapMax),
    selectedUpperGapFilter: buildUpperGapFilter(selectedUpperGapMin, selectedUpperGapMax),
  };
}

export function normalizeMatchMode(value) {
  return value === "or" ? "or" : "and";
}

export function normalizeRequiredOption(value, fallbackValue = false) {
  if (Array.isArray(value)) {
    if (value.length === 0) {
      return fallbackValue;
    }
    return value.some((entry) => normalizeRequiredOption(entry, false));
  }
  if (value === undefined || value === null) {
    return fallbackValue;
  }
  return value === true || value === 1 || value === "1" || value === "true" || value === "on";
}

export function buildConditionRequirementOptions(source = {}, fallback = {}) {
  const safeSource = source && typeof source === "object" ? source : {};
  if (
    !Object.hasOwn(safeSource, "rankRequired") &&
    !Object.hasOwn(safeSource, "machineRankRequired") &&
    !Object.hasOwn(safeSource, "selectedRankRequired") &&
    !Object.hasOwn(safeSource, "scoreRequired") &&
    !Object.hasOwn(safeSource, "nextGapRequired") &&
    !Object.hasOwn(safeSource, "upperGapRequired") &&
    !Object.hasOwn(safeSource, "machineNextGapRequired") &&
    !Object.hasOwn(safeSource, "selectedNextGapRequired") &&
    !Object.hasOwn(safeSource, "machineUpperGapRequired") &&
    !Object.hasOwn(safeSource, "selectedUpperGapRequired") &&
    !Object.hasOwn(safeSource, "machineEvaluationScoreRequired") &&
    !Object.hasOwn(safeSource, "machineEvaluationRankRequired") &&
    !Object.hasOwn(safeSource, "selectedMachineEvaluationRankRequired") &&
    !Object.hasOwn(safeSource, "machineEvaluationNextGapRequired") &&
    !Object.hasOwn(safeSource, "selectedMachineEvaluationNextGapRequired") &&
    !Object.hasOwn(safeSource, "machineEvaluationUpperGapRequired") &&
    !Object.hasOwn(safeSource, "selectedMachineEvaluationUpperGapRequired") &&
    Object.hasOwn(safeSource, "matchMode")
  ) {
    const allRequired = normalizeMatchMode(safeSource.matchMode) === "and";
    return {
      rankRequired: allRequired,
      machineRankRequired: allRequired,
      selectedRankRequired: allRequired,
      scoreRequired: allRequired,
      nextGapRequired: allRequired,
      upperGapRequired: allRequired,
      machineNextGapRequired: allRequired,
      selectedNextGapRequired: allRequired,
      machineUpperGapRequired: allRequired,
      selectedUpperGapRequired: allRequired,
      machineEvaluationScoreRequired: allRequired,
      machineEvaluationRankRequired: allRequired,
      selectedMachineEvaluationRankRequired: allRequired,
      machineEvaluationNextGapRequired: allRequired,
      selectedMachineEvaluationNextGapRequired: allRequired,
      machineEvaluationUpperGapRequired: allRequired,
      selectedMachineEvaluationUpperGapRequired: allRequired,
    };
  }

  const rankRequired = normalizeRequiredOption(
    safeSource.rankRequired,
    Boolean(fallback.rankRequired),
  );
  const nextGapRequired = normalizeRequiredOption(
    safeSource.nextGapRequired,
    Boolean(fallback.nextGapRequired),
  );
  const upperGapRequired = normalizeRequiredOption(
    safeSource.upperGapRequired,
    Boolean(fallback.upperGapRequired),
  );

  return {
    rankRequired,
    machineRankRequired: normalizeRequiredOption(
      safeSource.machineRankRequired,
      Object.hasOwn(fallback, "machineRankRequired")
        ? Boolean(fallback.machineRankRequired)
        : rankRequired,
    ),
    selectedRankRequired: normalizeRequiredOption(
      safeSource.selectedRankRequired,
      Object.hasOwn(fallback, "selectedRankRequired")
        ? Boolean(fallback.selectedRankRequired)
        : rankRequired,
    ),
    scoreRequired: normalizeRequiredOption(safeSource.scoreRequired, Boolean(fallback.scoreRequired)),
    nextGapRequired,
    upperGapRequired,
    machineNextGapRequired: normalizeRequiredOption(
      safeSource.machineNextGapRequired,
      Object.hasOwn(fallback, "machineNextGapRequired")
        ? Boolean(fallback.machineNextGapRequired)
        : nextGapRequired,
    ),
    selectedNextGapRequired: normalizeRequiredOption(
      safeSource.selectedNextGapRequired,
      Object.hasOwn(fallback, "selectedNextGapRequired")
        ? Boolean(fallback.selectedNextGapRequired)
        : nextGapRequired,
    ),
    machineUpperGapRequired: normalizeRequiredOption(
      safeSource.machineUpperGapRequired,
      Object.hasOwn(fallback, "machineUpperGapRequired")
        ? Boolean(fallback.machineUpperGapRequired)
        : upperGapRequired,
    ),
    selectedUpperGapRequired: normalizeRequiredOption(
      safeSource.selectedUpperGapRequired,
      Object.hasOwn(fallback, "selectedUpperGapRequired")
        ? Boolean(fallback.selectedUpperGapRequired)
        : upperGapRequired,
    ),
    machineEvaluationScoreRequired: normalizeRequiredOption(
      safeSource.machineEvaluationScoreRequired,
      Boolean(fallback.machineEvaluationScoreRequired),
    ),
    machineEvaluationRankRequired: normalizeRequiredOption(
      safeSource.machineEvaluationRankRequired,
      Boolean(fallback.machineEvaluationRankRequired),
    ),
    selectedMachineEvaluationRankRequired: normalizeRequiredOption(
      safeSource.selectedMachineEvaluationRankRequired,
      Boolean(fallback.selectedMachineEvaluationRankRequired),
    ),
    machineEvaluationNextGapRequired: normalizeRequiredOption(
      safeSource.machineEvaluationNextGapRequired,
      Boolean(fallback.machineEvaluationNextGapRequired),
    ),
    selectedMachineEvaluationNextGapRequired: normalizeRequiredOption(
      safeSource.selectedMachineEvaluationNextGapRequired,
      Boolean(fallback.selectedMachineEvaluationNextGapRequired),
    ),
    machineEvaluationUpperGapRequired: normalizeRequiredOption(
      safeSource.machineEvaluationUpperGapRequired,
      Boolean(fallback.machineEvaluationUpperGapRequired),
    ),
    selectedMachineEvaluationUpperGapRequired: normalizeRequiredOption(
      safeSource.selectedMachineEvaluationUpperGapRequired,
      Boolean(fallback.selectedMachineEvaluationUpperGapRequired),
    ),
  };
}

function requireActiveConditionFilters(requirementOptions, filters = {}) {
  return {
    rankRequired: filters.rankFilter?.hasRankFilter
      ? true
      : Boolean(requirementOptions.rankRequired),
    machineRankRequired: filters.machineRankFilter?.hasRankFilter
      ? true
      : Boolean(requirementOptions.machineRankRequired),
    selectedRankRequired: filters.selectedRankFilter?.hasRankFilter
      ? true
      : Boolean(requirementOptions.selectedRankRequired),
    scoreRequired: filters.scoreFilter?.hasScoreFilter
      ? true
      : Boolean(requirementOptions.scoreRequired),
    nextGapRequired: filters.nextGapFilter?.hasNextGapFilter
      ? true
      : Boolean(requirementOptions.nextGapRequired),
    upperGapRequired: filters.upperGapFilter?.hasUpperGapFilter
      ? true
      : Boolean(requirementOptions.upperGapRequired),
    machineNextGapRequired: filters.machineNextGapFilter?.hasNextGapFilter
      ? true
      : Boolean(requirementOptions.machineNextGapRequired),
    selectedNextGapRequired: filters.selectedNextGapFilter?.hasNextGapFilter
      ? true
      : Boolean(requirementOptions.selectedNextGapRequired),
    machineUpperGapRequired: filters.machineUpperGapFilter?.hasUpperGapFilter
      ? true
      : Boolean(requirementOptions.machineUpperGapRequired),
    selectedUpperGapRequired: filters.selectedUpperGapFilter?.hasUpperGapFilter
      ? true
      : Boolean(requirementOptions.selectedUpperGapRequired),
    machineEvaluationScoreRequired: filters.machineEvaluationScoreFilter?.hasScoreFilter
      ? true
      : Boolean(requirementOptions.machineEvaluationScoreRequired),
    machineEvaluationRankRequired: filters.machineEvaluationRankFilter?.hasRankFilter
      ? true
      : Boolean(requirementOptions.machineEvaluationRankRequired),
    selectedMachineEvaluationRankRequired:
      filters.selectedMachineEvaluationRankFilter?.hasRankFilter
        ? true
        : Boolean(requirementOptions.selectedMachineEvaluationRankRequired),
    machineEvaluationNextGapRequired: filters.machineEvaluationNextGapFilter?.hasNextGapFilter
      ? true
      : Boolean(requirementOptions.machineEvaluationNextGapRequired),
    selectedMachineEvaluationNextGapRequired:
      filters.selectedMachineEvaluationNextGapFilter?.hasNextGapFilter
        ? true
        : Boolean(requirementOptions.selectedMachineEvaluationNextGapRequired),
    machineEvaluationUpperGapRequired: filters.machineEvaluationUpperGapFilter?.hasUpperGapFilter
      ? true
      : Boolean(requirementOptions.machineEvaluationUpperGapRequired),
    selectedMachineEvaluationUpperGapRequired:
      filters.selectedMachineEvaluationUpperGapFilter?.hasUpperGapFilter
        ? true
        : Boolean(requirementOptions.selectedMachineEvaluationUpperGapRequired),
  };
}

export function normalizeRankScope(value) {
  if (value === "machine" || value === "selected") {
    return value;
  }
  return "selected";
}

export function buildScopedRankFilters(source = {}) {
  const safeSource = source && typeof source === "object" ? source : {};
  const rankScope = normalizeRankScope(safeSource.rankScope);
  const rankFilter = buildRankFilter(safeSource.rankMin, safeSource.rankMax);
  let machineRankFilter = buildRankFilter(safeSource.machineRankMin, safeSource.machineRankMax);
  let selectedRankFilter = buildRankFilter(
    safeSource.selectedRankMin,
    safeSource.selectedRankMax,
  );

  if (
    rankFilter.hasRankFilter &&
    !machineRankFilter.hasRankFilter &&
    !selectedRankFilter.hasRankFilter
  ) {
    if (rankScope === "machine") {
      machineRankFilter = rankFilter;
    } else {
      selectedRankFilter = rankFilter;
    }
  }

  return {
    rankScope,
    rankFilter,
    machineRankFilter,
    selectedRankFilter,
    hasRankFilter: machineRankFilter.hasRankFilter || selectedRankFilter.hasRankFilter,
  };
}

function formatRankScopeLabel(rankScope) {
  if (rankScope === "machine") {
    return "機種内順位";
  }
  if (rankScope === "selected") {
    return "選択機種内順位";
  }
  return "選択機種内順位";
}

function findNextLowerHuntScore(sortedRows, startIndex, currentScore) {
  for (let index = startIndex + 1; index < sortedRows.length; index += 1) {
    const candidateScore = sortedRows[index].score;
    if (currentScore - candidateScore > SCORE_EPSILON) {
      return candidateScore;
    }
  }

  return null;
}

function findPreviousHigherHuntScore(sortedRows, startIndex, currentScore) {
  for (let index = startIndex - 1; index >= 0; index -= 1) {
    const candidateScore = sortedRows[index].score;
    if (candidateScore - currentScore > SCORE_EPSILON) {
      return candidateScore;
    }
  }

  return null;
}

export function calculateHuntScoreNextGapMap(rows, rankFilter = null) {
  const validRows = (Array.isArray(rows) ? rows : [])
    .map((row, index) => ({
      row,
      index,
      score: readNumber(row?.huntScore),
    }))
    .filter((entry) => entry.score !== null)
    .sort((left, right) => right.score - left.score || left.index - right.index);

  if (validRows.length < 2) {
    return new Map();
  }

  if (rankFilter?.hasRankFilter) {
    const rankMin = readPositiveInteger(rankFilter.rankMin) ?? 1;
    const rankMax = readPositiveInteger(rankFilter.rankMax) ?? rankMin;
    const normalizedRankMin = Math.min(rankMin, rankMax);
    const normalizedRankMax = Math.max(rankMin, rankMax);
    const nextLowerScore = findNextLowerHuntScore(
      validRows,
      normalizedRankMax - 1,
      validRows[normalizedRankMax - 1]?.score ?? Number.NEGATIVE_INFINITY,
    );

    if (nextLowerScore === null) {
      return new Map();
    }

    return new Map(
      validRows.map((entry, index) => {
        const rank = index + 1;
        return [
          entry.row,
          rank >= normalizedRankMin && rank <= normalizedRankMax
            ? Math.max(0, entry.score - nextLowerScore)
            : null,
        ];
      }),
    );
  }

  return new Map(
    validRows.map((entry, index) => {
      const nextLowerScore = findNextLowerHuntScore(validRows, index, entry.score);
      return [
        entry.row,
        nextLowerScore !== null ? Math.max(0, entry.score - nextLowerScore) : null,
      ];
    }),
  );
}

export function calculateHuntScoreUpperGapMap(rows, rankFilter = null) {
  const validRows = (Array.isArray(rows) ? rows : [])
    .map((row, index) => ({
      row,
      index,
      score: readNumber(row?.huntScore),
    }))
    .filter((entry) => entry.score !== null)
    .sort((left, right) => right.score - left.score || left.index - right.index);

  if (validRows.length < 2) {
    return new Map();
  }

  if (rankFilter?.hasRankFilter) {
    const rankMin = readPositiveInteger(rankFilter.rankMin) ?? 1;
    const rankMax = readPositiveInteger(rankFilter.rankMax) ?? rankMin;
    const normalizedRankMin = Math.min(rankMin, rankMax);
    const normalizedRankMax = Math.max(rankMin, rankMax);
    const previousHigherScore = findPreviousHigherHuntScore(
      validRows,
      normalizedRankMin - 1,
      validRows[normalizedRankMin - 1]?.score ?? Number.POSITIVE_INFINITY,
    );

    if (previousHigherScore === null) {
      return new Map();
    }

    return new Map(
      validRows.map((entry, index) => {
        const rank = index + 1;
        return [
          entry.row,
          rank >= normalizedRankMin && rank <= normalizedRankMax
            ? Math.max(0, previousHigherScore - entry.score)
            : null,
        ];
      }),
    );
  }

  return new Map(
    validRows.map((entry, index) => {
      const previousHigherScore = findPreviousHigherHuntScore(validRows, index, entry.score);
      return [
        entry.row,
        previousHigherScore !== null ? Math.max(0, previousHigherScore - entry.score) : null,
      ];
    }),
  );
}

export function readNextGapForRankScope(row, rankScope) {
  if (rankScope === "machine") {
    return readNumber(row?.machineNextGap);
  }
  if (rankScope === "selected") {
    return readNumber(row?.selectedNextGap);
  }
  return readNumber(row?.overallNextGap);
}

export function readUpperGapForRankScope(row, rankScope) {
  if (rankScope === "machine") {
    return readNumber(row?.machineUpperGap);
  }
  if (rankScope === "selected") {
    return readNumber(row?.selectedUpperGap);
  }
  return readNumber(row?.overallUpperGap);
}

function compareSelectionCandidates(left, right) {
  const nextGapDiff = right.nextGapValue - left.nextGapValue;
  if (Math.abs(nextGapDiff) > SCORE_EPSILON) {
    return nextGapDiff;
  }

  const scoreDiff = readFiniteNumber(right.huntScore) - readFiniteNumber(left.huntScore);
  if (Math.abs(scoreDiff) > SCORE_EPSILON) {
    return scoreDiff;
  }

  const leftRank = readPositiveInteger(left.rank) ?? Number.MAX_SAFE_INTEGER;
  const rightRank = readPositiveInteger(right.rank) ?? Number.MAX_SAFE_INTEGER;
  return (
    leftRank - rightRank ||
    String(left.machineName ?? "").localeCompare(String(right.machineName ?? ""), "ja") ||
    String(left.slotNumber ?? "").localeCompare(String(right.slotNumber ?? ""), "ja", {
      numeric: true,
    })
  );
}

function buildMachineTopNextGapBookmarkRowSet(rowsByBookmarkMachineName, machineNextGapMap) {
  const candidates = [];

  for (const [machineName, machineRows] of rowsByBookmarkMachineName.entries()) {
    const row = machineRows[0] ?? null;
    if (!row) {
      continue;
    }

    const nextGapValue = machineNextGapMap.get(row) ?? null;
    if (!Number.isFinite(nextGapValue)) {
      continue;
    }

    candidates.push({
      row,
      nextGapValue,
      huntScore: row.huntScore,
      rank: row.rank,
      machineName,
      slotNumber: row.slotNumber,
    });
  }

  candidates.sort(compareSelectionCandidates);
  return new Set(candidates[0]?.row ? [candidates[0].row] : []);
}

function matchesNumberRange(value, minValue, maxValue) {
  const normalizedValue = readNumber(value);
  if (normalizedValue === null) {
    return false;
  }

  return (
    (minValue === null || minValue === undefined || normalizedValue >= minValue) &&
    (maxValue === null || maxValue === undefined || normalizedValue <= maxValue)
  );
}

export function matchesRequiredConditionFilters(
  rankValue,
  huntScore,
  rankFilter,
  scoreFilter,
  requirementOptions = {},
  noFilterResult = true,
  nextGapValue = null,
  nextGapFilter = { hasNextGapFilter: false, nextGapMin: null },
  upperGapValue = null,
  upperGapFilter = { hasUpperGapFilter: false, upperGapMax: null },
  boundaryGapConditions = [],
) {
  const normalizedRequirements = buildConditionRequirementOptions(requirementOptions);
  const conditionEntries = [];
  const rankConditions = Array.isArray(rankValue)
    ? rankValue
    : [
        {
          rankValue,
          rankFilter,
          required: normalizedRequirements.rankRequired,
        },
      ];

  for (const rankCondition of rankConditions) {
    const currentRankFilter = rankCondition?.rankFilter ?? rankFilter;
    if (!currentRankFilter?.hasRankFilter) {
      continue;
    }
    const normalizedRankValue = readPositiveInteger(rankCondition?.rankValue);
    conditionEntries.push({
      matched:
        normalizedRankValue !== null &&
        normalizedRankValue >= currentRankFilter.rankMin &&
        normalizedRankValue <= currentRankFilter.rankMax,
      required:
        typeof rankCondition?.required === "boolean"
          ? rankCondition.required
          : normalizedRequirements.rankRequired,
    });
  }
  if (scoreFilter.hasScoreFilter) {
    conditionEntries.push({
      matched: matchesNumberRange(huntScore, scoreFilter.scoreMin, scoreFilter.scoreMax),
      required: normalizedRequirements.scoreRequired,
    });
  }
  if (nextGapFilter.hasNextGapFilter) {
    conditionEntries.push({
      matched: matchesNumberRange(
        nextGapValue,
        nextGapFilter.nextGapMin,
        nextGapFilter.nextGapMax,
      ),
      required: normalizedRequirements.nextGapRequired,
    });
  }
  if (upperGapFilter.hasUpperGapFilter) {
    conditionEntries.push({
      matched: matchesNumberRange(
        upperGapValue,
        upperGapFilter.upperGapMin,
        upperGapFilter.upperGapMax,
      ),
      required: normalizedRequirements.upperGapRequired,
    });
  }
  for (const boundaryGapCondition of Array.isArray(boundaryGapConditions)
    ? boundaryGapConditions
    : []) {
    const filter = boundaryGapCondition?.filter ?? {};
    const isUpperGapFilter = Boolean(filter.hasUpperGapFilter);
    const isNextGapFilter = Boolean(filter.hasNextGapFilter);
    if (!isUpperGapFilter && !isNextGapFilter) {
      continue;
    }

    conditionEntries.push({
      matched: matchesNumberRange(
        boundaryGapCondition?.value,
        isUpperGapFilter ? filter.upperGapMin : filter.nextGapMin,
        isUpperGapFilter ? filter.upperGapMax : filter.nextGapMax,
      ),
      required: Boolean(boundaryGapCondition?.required),
    });
  }

  if (conditionEntries.length === 0) {
    return noFilterResult;
  }

  const requiredEntries = conditionEntries.filter((entry) => entry.required);
  const optionalEntries = conditionEntries.filter((entry) => !entry.required);
  const requiredMatched =
    requiredEntries.length > 0 && requiredEntries.every((entry) => entry.matched);
  const optionalMatched =
    optionalEntries.length > 0 && optionalEntries.some((entry) => entry.matched);

  if (requiredEntries.length > 0 && optionalEntries.length > 0) {
    return requiredMatched || optionalMatched;
  }
  if (requiredEntries.length > 0) {
    return requiredMatched;
  }
  return optionalMatched;
}

function normalizeMachineNames(machineNames) {
  return [...new Set((Array.isArray(machineNames) ? machineNames : [machineNames]).map(normalizeText).filter(Boolean))];
}

function trimDecimalText(value) {
  const parsedValue = readNumber(value);
  if (parsedValue === null) {
    return "";
  }

  if (Number.isInteger(parsedValue)) {
    return String(parsedValue);
  }

  return parsedValue.toFixed(1).replace(/\.0$/u, "");
}

function formatRangeConditionText(label, minValue, maxValue, required) {
  const minText = trimDecimalText(minValue);
  const maxText = trimDecimalText(maxValue);
  const suffix = required ? "必須" : "";

  if (minText && maxText) {
    return `${label}${minText}〜${maxText}${suffix}`;
  }
  if (minText) {
    return `${label}${minText}以上${suffix}`;
  }
  return `${label}${maxText}以下${suffix}`;
}

function pushRangeConditionSummary(parts, enabled, label, minValue, maxValue, required) {
  if (!enabled) {
    return;
  }
  parts.push(formatRangeConditionText(label, minValue, maxValue, required));
}

export function normalizeHuntBacktestBookmark(bookmark, fallbackStoreId = "") {
  if (!bookmark || typeof bookmark !== "object") {
    return null;
  }

  const storeId = normalizeText(bookmark.storeId) || normalizeText(fallbackStoreId);
  const machineNames = normalizeMachineNames(bookmark.machineNames);
  const huntScoreLogicKeys = normalizeMachineNames(bookmark.huntScoreLogicKeys);
  if (!storeId || machineNames.length === 0) {
    return null;
  }

  const {
    rankScope,
    rankFilter,
    machineRankFilter,
    selectedRankFilter,
    hasRankFilter,
  } = buildScopedRankFilters(bookmark);
  const scoreFilter = buildScoreFilter(bookmark.scoreMin, bookmark.scoreMax);
  const machineEvaluationScoreFilter = buildScoreFilter(
    bookmark.machineEvaluationScoreMin,
    bookmark.machineEvaluationScoreMax,
  );
  const machineEvaluationRankFilter = buildRankFilter(
    bookmark.machineEvaluationRankMin,
    bookmark.machineEvaluationRankMax,
  );
  const selectedMachineEvaluationRankFilter = buildRankFilter(
    bookmark.selectedMachineEvaluationRankMin,
    bookmark.selectedMachineEvaluationRankMax,
  );
  const machineEvaluationNextGapFilter = buildNextGapFilter(
    bookmark.machineEvaluationNextGapMin,
    bookmark.machineEvaluationNextGapMax,
  );
  const selectedMachineEvaluationNextGapFilter = buildNextGapFilter(
    bookmark.selectedMachineEvaluationNextGapMin,
    bookmark.selectedMachineEvaluationNextGapMax,
  );
  const machineEvaluationUpperGapFilter = buildUpperGapFilter(
    bookmark.machineEvaluationUpperGapMin,
    bookmark.machineEvaluationUpperGapMax,
  );
  const selectedMachineEvaluationUpperGapFilter = buildUpperGapFilter(
    bookmark.selectedMachineEvaluationUpperGapMin,
    bookmark.selectedMachineEvaluationUpperGapMax,
  );
  const {
    machineNextGapFilter,
    selectedNextGapFilter,
    machineUpperGapFilter,
    selectedUpperGapFilter,
  } = buildBoundaryGapFilters(bookmark);
  const dailySelectionMode = normalizeDailySelectionMode(bookmark.dailySelectionMode);
  const logicConditionMode = normalizeLogicConditionMode(bookmark.logicConditionMode);
  const usesMachineTopNextGapSelection = isMachineTopNextGapSelectionMode(dailySelectionMode);
  const baseRequirementOptions = buildConditionRequirementOptions(bookmark);
  const requirementOptions = usesMachineTopNextGapSelection
    ? requireActiveConditionFilters(baseRequirementOptions, {
        machineRankFilter,
        selectedRankFilter,
        scoreFilter,
        machineNextGapFilter,
        selectedNextGapFilter,
        machineUpperGapFilter,
        selectedUpperGapFilter,
        machineEvaluationScoreFilter,
        machineEvaluationRankFilter,
        selectedMachineEvaluationRankFilter,
        machineEvaluationNextGapFilter,
        selectedMachineEvaluationNextGapFilter,
        machineEvaluationUpperGapFilter,
        selectedMachineEvaluationUpperGapFilter,
      })
    : baseRequirementOptions;
  const allMachineCount = readPositiveInteger(bookmark.allMachineCount) ?? machineNames.length;
  const combineAimJuggler = Boolean(bookmark.combineAimJuggler) || machineNames.some(isAimJugglerGroup);
  const combineHanabi = Boolean(bookmark.combineHanabi) || machineNames.some(isHanabiGroup);
  const startDate = normalizeDateText(bookmark.startDate);
  const endDate = normalizeDateText(bookmark.endDate);
  const periodMode = normalizePeriodMode(bookmark.periodMode, startDate, endDate);
  const recentDays = readPositiveInteger(bookmark.recentDays) ?? 90;

  return {
    version: 1,
    id: normalizeBookmarkId(bookmark.id) || null,
    name: normalizeBookmarkName(bookmark.name),
    storeId,
    periodMode,
    recentDays,
    startDate: periodMode === "range" ? startDate : null,
    endDate: periodMode === "range" ? endDate : null,
    allMachineCount,
    machineNames,
    huntScoreLogicKeys,
    logicConditionMode,
    machineEvaluationBacktestMode: normalizeMachineEvaluationBacktestMode(
      bookmark.machineEvaluationMode ?? bookmark.machineEvaluationBacktestMode,
    ),
    rankMin: rankFilter.rankMin,
    rankMax: rankFilter.rankMax,
    machineRankMin: machineRankFilter.rankMin,
    machineRankMax: machineRankFilter.rankMax,
    selectedRankMin: selectedRankFilter.rankMin,
    selectedRankMax: selectedRankFilter.rankMax,
    hasRankFilter,
    hasMachineRankFilter: machineRankFilter.hasRankFilter,
    hasSelectedRankFilter: selectedRankFilter.hasRankFilter,
    scoreMin: scoreFilter.scoreMin,
    scoreMax: scoreFilter.scoreMax,
    hasScoreFilter: scoreFilter.hasScoreFilter,
    machineEvaluationScoreMin: machineEvaluationScoreFilter.scoreMin,
    machineEvaluationScoreMax: machineEvaluationScoreFilter.scoreMax,
    hasMachineEvaluationScoreFilter: machineEvaluationScoreFilter.hasScoreFilter,
    machineEvaluationRankMin: machineEvaluationRankFilter.rankMin,
    machineEvaluationRankMax: machineEvaluationRankFilter.rankMax,
    hasMachineEvaluationRankFilter: machineEvaluationRankFilter.hasRankFilter,
    selectedMachineEvaluationRankMin: selectedMachineEvaluationRankFilter.rankMin,
    selectedMachineEvaluationRankMax: selectedMachineEvaluationRankFilter.rankMax,
    hasSelectedMachineEvaluationRankFilter: selectedMachineEvaluationRankFilter.hasRankFilter,
    machineEvaluationNextGapMin: machineEvaluationNextGapFilter.nextGapMin,
    machineEvaluationNextGapMax: machineEvaluationNextGapFilter.nextGapMax,
    hasMachineEvaluationNextGapFilter: machineEvaluationNextGapFilter.hasNextGapFilter,
    selectedMachineEvaluationNextGapMin: selectedMachineEvaluationNextGapFilter.nextGapMin,
    selectedMachineEvaluationNextGapMax: selectedMachineEvaluationNextGapFilter.nextGapMax,
    hasSelectedMachineEvaluationNextGapFilter:
      selectedMachineEvaluationNextGapFilter.hasNextGapFilter,
    machineEvaluationUpperGapMin: machineEvaluationUpperGapFilter.upperGapMin,
    machineEvaluationUpperGapMax: machineEvaluationUpperGapFilter.upperGapMax,
    hasMachineEvaluationUpperGapFilter: machineEvaluationUpperGapFilter.hasUpperGapFilter,
    selectedMachineEvaluationUpperGapMin: selectedMachineEvaluationUpperGapFilter.upperGapMin,
    selectedMachineEvaluationUpperGapMax: selectedMachineEvaluationUpperGapFilter.upperGapMax,
    hasSelectedMachineEvaluationUpperGapFilter:
      selectedMachineEvaluationUpperGapFilter.hasUpperGapFilter,
    machineNextGapMin: machineNextGapFilter.nextGapMin,
    machineNextGapMax: machineNextGapFilter.nextGapMax,
    hasMachineNextGapFilter: machineNextGapFilter.hasNextGapFilter,
    selectedNextGapMin: selectedNextGapFilter.nextGapMin,
    selectedNextGapMax: selectedNextGapFilter.nextGapMax,
    hasSelectedNextGapFilter: selectedNextGapFilter.hasNextGapFilter,
    machineUpperGapMin: machineUpperGapFilter.upperGapMin,
    machineUpperGapMax: machineUpperGapFilter.upperGapMax,
    hasMachineUpperGapFilter: machineUpperGapFilter.hasUpperGapFilter,
    selectedUpperGapMin: selectedUpperGapFilter.upperGapMin,
    selectedUpperGapMax: selectedUpperGapFilter.upperGapMax,
    hasSelectedUpperGapFilter: selectedUpperGapFilter.hasUpperGapFilter,
    rankRequired: requirementOptions.rankRequired,
    machineRankRequired: requirementOptions.machineRankRequired,
    selectedRankRequired: requirementOptions.selectedRankRequired,
    scoreRequired: requirementOptions.scoreRequired,
    machineEvaluationScoreRequired: requirementOptions.machineEvaluationScoreRequired,
    machineEvaluationRankRequired: requirementOptions.machineEvaluationRankRequired,
    selectedMachineEvaluationRankRequired:
      requirementOptions.selectedMachineEvaluationRankRequired,
    machineEvaluationNextGapRequired: requirementOptions.machineEvaluationNextGapRequired,
    selectedMachineEvaluationNextGapRequired:
      requirementOptions.selectedMachineEvaluationNextGapRequired,
    machineEvaluationUpperGapRequired: requirementOptions.machineEvaluationUpperGapRequired,
    selectedMachineEvaluationUpperGapRequired:
      requirementOptions.selectedMachineEvaluationUpperGapRequired,
    machineNextGapRequired: requirementOptions.machineNextGapRequired,
    selectedNextGapRequired: requirementOptions.selectedNextGapRequired,
    machineUpperGapRequired: requirementOptions.machineUpperGapRequired,
    selectedUpperGapRequired: requirementOptions.selectedUpperGapRequired,
    dailySelectionMode,
    rankScope,
    scoreDifferenceMode: normalizeText(bookmark.scoreDifferenceMode) || "minrepo",
    differenceMode: normalizeText(bookmark.differenceMode) || "minrepo",
    settingEstimateMode: normalizeSettingEstimateMode(bookmark.settingEstimateMode),
    settingDistribution: normalizeSettingDistribution(bookmark.settingDistribution),
    eventDayTails: normalizeNumberTextArray(bookmark.eventDayTails, 0, 9),
    eventZoro: normalizeRequiredOption(bookmark.eventZoro, false),
    eventWeekdays: normalizeNumberTextArray(bookmark.eventWeekdays, 0, 6),
    eventMonthDays: normalizeNumberTextArray(bookmark.eventMonthDays, 1, 31),
    combineAimJuggler,
    combineHanabi,
    savedAt: normalizeText(bookmark.savedAt) || null,
  };
}

export function createHuntBacktestBookmark(storeId, bookmark) {
  return normalizeHuntBacktestBookmark(
    {
      ...bookmark,
      storeId,
    },
    storeId,
  );
}

export function areHuntBacktestBookmarksEqual(left, right) {
  const normalizedLeft = normalizeHuntBacktestBookmark(left);
  const normalizedRight = normalizeHuntBacktestBookmark(right);

  if (!normalizedLeft || !normalizedRight) {
    return false;
  }

  return (
    normalizedLeft.storeId === normalizedRight.storeId &&
    normalizedLeft.periodMode === normalizedRight.periodMode &&
    normalizedLeft.recentDays === normalizedRight.recentDays &&
    normalizedLeft.startDate === normalizedRight.startDate &&
    normalizedLeft.endDate === normalizedRight.endDate &&
    normalizedLeft.rankMin === normalizedRight.rankMin &&
    normalizedLeft.rankMax === normalizedRight.rankMax &&
    normalizedLeft.machineRankMin === normalizedRight.machineRankMin &&
    normalizedLeft.machineRankMax === normalizedRight.machineRankMax &&
    normalizedLeft.selectedRankMin === normalizedRight.selectedRankMin &&
    normalizedLeft.selectedRankMax === normalizedRight.selectedRankMax &&
    normalizedLeft.scoreMin === normalizedRight.scoreMin &&
    normalizedLeft.scoreMax === normalizedRight.scoreMax &&
    normalizedLeft.machineEvaluationScoreMin === normalizedRight.machineEvaluationScoreMin &&
    normalizedLeft.machineEvaluationScoreMax === normalizedRight.machineEvaluationScoreMax &&
    normalizedLeft.machineEvaluationRankMin === normalizedRight.machineEvaluationRankMin &&
    normalizedLeft.machineEvaluationRankMax === normalizedRight.machineEvaluationRankMax &&
    normalizedLeft.selectedMachineEvaluationRankMin ===
      normalizedRight.selectedMachineEvaluationRankMin &&
    normalizedLeft.selectedMachineEvaluationRankMax ===
      normalizedRight.selectedMachineEvaluationRankMax &&
    normalizedLeft.machineEvaluationNextGapMin === normalizedRight.machineEvaluationNextGapMin &&
    normalizedLeft.machineEvaluationNextGapMax === normalizedRight.machineEvaluationNextGapMax &&
    normalizedLeft.selectedMachineEvaluationNextGapMin ===
      normalizedRight.selectedMachineEvaluationNextGapMin &&
    normalizedLeft.selectedMachineEvaluationNextGapMax ===
      normalizedRight.selectedMachineEvaluationNextGapMax &&
    normalizedLeft.machineEvaluationUpperGapMin ===
      normalizedRight.machineEvaluationUpperGapMin &&
    normalizedLeft.machineEvaluationUpperGapMax ===
      normalizedRight.machineEvaluationUpperGapMax &&
    normalizedLeft.selectedMachineEvaluationUpperGapMin ===
      normalizedRight.selectedMachineEvaluationUpperGapMin &&
    normalizedLeft.selectedMachineEvaluationUpperGapMax ===
      normalizedRight.selectedMachineEvaluationUpperGapMax &&
    normalizedLeft.machineNextGapMin === normalizedRight.machineNextGapMin &&
    normalizedLeft.machineNextGapMax === normalizedRight.machineNextGapMax &&
    normalizedLeft.selectedNextGapMin === normalizedRight.selectedNextGapMin &&
    normalizedLeft.selectedNextGapMax === normalizedRight.selectedNextGapMax &&
    normalizedLeft.machineUpperGapMin === normalizedRight.machineUpperGapMin &&
    normalizedLeft.machineUpperGapMax === normalizedRight.machineUpperGapMax &&
    normalizedLeft.selectedUpperGapMin === normalizedRight.selectedUpperGapMin &&
    normalizedLeft.selectedUpperGapMax === normalizedRight.selectedUpperGapMax &&
    normalizedLeft.rankRequired === normalizedRight.rankRequired &&
    normalizedLeft.machineRankRequired === normalizedRight.machineRankRequired &&
    normalizedLeft.selectedRankRequired === normalizedRight.selectedRankRequired &&
    normalizedLeft.scoreRequired === normalizedRight.scoreRequired &&
    normalizedLeft.machineEvaluationScoreRequired ===
      normalizedRight.machineEvaluationScoreRequired &&
    normalizedLeft.machineEvaluationRankRequired ===
      normalizedRight.machineEvaluationRankRequired &&
    normalizedLeft.selectedMachineEvaluationRankRequired ===
      normalizedRight.selectedMachineEvaluationRankRequired &&
    normalizedLeft.machineEvaluationNextGapRequired ===
      normalizedRight.machineEvaluationNextGapRequired &&
    normalizedLeft.selectedMachineEvaluationNextGapRequired ===
      normalizedRight.selectedMachineEvaluationNextGapRequired &&
    normalizedLeft.machineEvaluationUpperGapRequired ===
      normalizedRight.machineEvaluationUpperGapRequired &&
    normalizedLeft.selectedMachineEvaluationUpperGapRequired ===
      normalizedRight.selectedMachineEvaluationUpperGapRequired &&
    normalizedLeft.machineNextGapRequired === normalizedRight.machineNextGapRequired &&
    normalizedLeft.selectedNextGapRequired === normalizedRight.selectedNextGapRequired &&
    normalizedLeft.machineUpperGapRequired === normalizedRight.machineUpperGapRequired &&
    normalizedLeft.selectedUpperGapRequired === normalizedRight.selectedUpperGapRequired &&
    normalizedLeft.dailySelectionMode === normalizedRight.dailySelectionMode &&
    normalizedLeft.logicConditionMode === normalizedRight.logicConditionMode &&
    normalizedLeft.machineEvaluationBacktestMode ===
      normalizedRight.machineEvaluationBacktestMode &&
    normalizedLeft.rankScope === normalizedRight.rankScope &&
    normalizedLeft.scoreDifferenceMode === normalizedRight.scoreDifferenceMode &&
    normalizedLeft.differenceMode === normalizedRight.differenceMode &&
    normalizedLeft.settingEstimateMode === normalizedRight.settingEstimateMode &&
    normalizedLeft.settingDistribution === normalizedRight.settingDistribution &&
    normalizedLeft.eventZoro === normalizedRight.eventZoro &&
    normalizedLeft.combineAimJuggler === normalizedRight.combineAimJuggler &&
    normalizedLeft.combineHanabi === normalizedRight.combineHanabi &&
    normalizedLeft.eventDayTails.length === normalizedRight.eventDayTails.length &&
    normalizedLeft.eventDayTails.every((value, index) => value === normalizedRight.eventDayTails[index]) &&
    normalizedLeft.eventWeekdays.length === normalizedRight.eventWeekdays.length &&
    normalizedLeft.eventWeekdays.every((value, index) => value === normalizedRight.eventWeekdays[index]) &&
    normalizedLeft.eventMonthDays.length === normalizedRight.eventMonthDays.length &&
    normalizedLeft.eventMonthDays.every((value, index) => value === normalizedRight.eventMonthDays[index]) &&
    normalizedLeft.huntScoreLogicKeys.length === normalizedRight.huntScoreLogicKeys.length &&
    normalizedLeft.huntScoreLogicKeys.every((logicKey, index) => logicKey === normalizedRight.huntScoreLogicKeys[index]) &&
    normalizedLeft.machineNames.length === normalizedRight.machineNames.length &&
    normalizedLeft.machineNames.every((machineName, index) => machineName === normalizedRight.machineNames[index])
  );
}

function buildMachineSummaryText(bookmark) {
  if (bookmark.machineNames.length >= bookmark.allMachineCount) {
    return `全${bookmark.allMachineCount}機種`;
  }

  if (bookmark.machineNames.length === 1) {
    return bookmark.machineNames[0];
  }

  return `${bookmark.machineNames.length}機種`;
}

export function formatHuntBacktestBookmarkPeriod(bookmark) {
  const normalizedBookmark = normalizeHuntBacktestBookmark(bookmark);
  if (!normalizedBookmark) {
    return "";
  }

  if (normalizedBookmark.periodMode === "recent") {
    return `直近${normalizedBookmark.recentDays}日`;
  }

  if (normalizedBookmark.startDate && normalizedBookmark.endDate) {
    return `${normalizedBookmark.startDate}〜${normalizedBookmark.endDate}`;
  }

  return "";
}

export function formatHuntBacktestBookmarkSummary(bookmark) {
  const normalizedBookmark = normalizeHuntBacktestBookmark(bookmark);
  if (!normalizedBookmark) {
    return "";
  }

  const parts = [buildMachineSummaryText(normalizedBookmark)];
  if (normalizedBookmark.huntScoreLogicKeys.length >= 2) {
    parts.push(
      normalizedBookmark.logicConditionMode === LOGIC_CONDITION_MODE_AND
        ? `ロジック${normalizedBookmark.huntScoreLogicKeys.length}件AND`
        : `ロジック${normalizedBookmark.huntScoreLogicKeys.length}件合算`,
    );
  }
  parts.push(`設定推定: ${formatSettingEstimateModeLabel(normalizedBookmark.settingEstimateMode)}`);
  if (normalizedBookmark.settingDistribution === SETTING_DISTRIBUTION_HIDE) {
    parts.push("設定分布: 非表示");
  }
  parts.push(
    `機種別採用条件: ${formatMachineEvaluationBacktestModeLabel(
      normalizedBookmark.machineEvaluationBacktestMode,
    )}`,
  );

  if (isMachineTopNextGapSelectionMode(normalizedBookmark.dailySelectionMode)) {
    parts.push("各機種1位から機種内下位境界差1位を1台選抜");
  }

  if (normalizedBookmark.hasMachineRankFilter) {
    parts.push(
      `機種内順位${normalizedBookmark.machineRankMin}〜${normalizedBookmark.machineRankMax}${
        normalizedBookmark.machineRankRequired ? "必須" : ""
      }`,
    );
  }

  if (normalizedBookmark.hasSelectedRankFilter) {
    parts.push(
      `選択機種内順位${normalizedBookmark.selectedRankMin}〜${normalizedBookmark.selectedRankMax}${
        normalizedBookmark.selectedRankRequired ? "必須" : ""
      }`,
    );
  }

  if (normalizedBookmark.hasScoreFilter) {
    parts.push(
      formatRangeConditionText(
        "狙い度",
        normalizedBookmark.scoreMin,
        normalizedBookmark.scoreMax,
        normalizedBookmark.scoreRequired,
      ),
    );
  }

  if (normalizedBookmark.hasMachineEvaluationScoreFilter) {
    parts.push(
      formatRangeConditionText(
        "機種別",
        normalizedBookmark.machineEvaluationScoreMin,
        normalizedBookmark.machineEvaluationScoreMax,
        normalizedBookmark.machineEvaluationScoreRequired,
      ),
    );
  }

  if (normalizedBookmark.hasMachineEvaluationRankFilter) {
    parts.push(
      `機種別同一機種内順位${normalizedBookmark.machineEvaluationRankMin}〜${normalizedBookmark.machineEvaluationRankMax}${
        normalizedBookmark.machineEvaluationRankRequired ? "必須" : ""
      }`,
    );
  }

  if (normalizedBookmark.hasSelectedMachineEvaluationRankFilter) {
    parts.push(
      `機種別選択機種内順位${normalizedBookmark.selectedMachineEvaluationRankMin}〜${normalizedBookmark.selectedMachineEvaluationRankMax}${
        normalizedBookmark.selectedMachineEvaluationRankRequired ? "必須" : ""
      }`,
    );
  }

  pushRangeConditionSummary(
    parts,
    normalizedBookmark.hasMachineUpperGapFilter,
    "上差(同)",
    normalizedBookmark.machineUpperGapMin,
    normalizedBookmark.machineUpperGapMax,
    normalizedBookmark.machineUpperGapRequired,
  );
  pushRangeConditionSummary(
    parts,
    normalizedBookmark.hasMachineNextGapFilter,
    "下差(同)",
    normalizedBookmark.machineNextGapMin,
    normalizedBookmark.machineNextGapMax,
    normalizedBookmark.machineNextGapRequired,
  );
  pushRangeConditionSummary(
    parts,
    normalizedBookmark.hasSelectedUpperGapFilter,
    "上差(全)",
    normalizedBookmark.selectedUpperGapMin,
    normalizedBookmark.selectedUpperGapMax,
    normalizedBookmark.selectedUpperGapRequired,
  );
  pushRangeConditionSummary(
    parts,
    normalizedBookmark.hasSelectedNextGapFilter,
    "下差(全)",
    normalizedBookmark.selectedNextGapMin,
    normalizedBookmark.selectedNextGapMax,
    normalizedBookmark.selectedNextGapRequired,
  );
  pushRangeConditionSummary(
    parts,
    normalizedBookmark.hasMachineEvaluationUpperGapFilter,
    "機種別上差(同)",
    normalizedBookmark.machineEvaluationUpperGapMin,
    normalizedBookmark.machineEvaluationUpperGapMax,
    normalizedBookmark.machineEvaluationUpperGapRequired,
  );
  pushRangeConditionSummary(
    parts,
    normalizedBookmark.hasMachineEvaluationNextGapFilter,
    "機種別下差(同)",
    normalizedBookmark.machineEvaluationNextGapMin,
    normalizedBookmark.machineEvaluationNextGapMax,
    normalizedBookmark.machineEvaluationNextGapRequired,
  );
  pushRangeConditionSummary(
    parts,
    normalizedBookmark.hasSelectedMachineEvaluationUpperGapFilter,
    "機種別上差(全)",
    normalizedBookmark.selectedMachineEvaluationUpperGapMin,
    normalizedBookmark.selectedMachineEvaluationUpperGapMax,
    normalizedBookmark.selectedMachineEvaluationUpperGapRequired,
  );
  pushRangeConditionSummary(
    parts,
    normalizedBookmark.hasSelectedMachineEvaluationNextGapFilter,
    "機種別下差(全)",
    normalizedBookmark.selectedMachineEvaluationNextGapMin,
    normalizedBookmark.selectedMachineEvaluationNextGapMax,
    normalizedBookmark.selectedMachineEvaluationNextGapRequired,
  );

  const activeFilterCount = [
    normalizedBookmark.hasMachineRankFilter,
    normalizedBookmark.hasSelectedRankFilter,
    normalizedBookmark.hasScoreFilter,
    normalizedBookmark.hasMachineEvaluationScoreFilter,
    normalizedBookmark.hasMachineEvaluationRankFilter,
    normalizedBookmark.hasSelectedMachineEvaluationRankFilter,
    normalizedBookmark.hasMachineNextGapFilter,
    normalizedBookmark.hasSelectedNextGapFilter,
    normalizedBookmark.hasMachineUpperGapFilter,
    normalizedBookmark.hasSelectedUpperGapFilter,
    normalizedBookmark.hasMachineEvaluationNextGapFilter,
    normalizedBookmark.hasSelectedMachineEvaluationNextGapFilter,
    normalizedBookmark.hasMachineEvaluationUpperGapFilter,
    normalizedBookmark.hasSelectedMachineEvaluationUpperGapFilter,
  ].filter(Boolean).length;

  if (normalizedBookmark.combineAimJuggler) {
    parts.push("アイム統合");
  }

  if (normalizedBookmark.combineHanabi) {
    parts.push("ハナビ統合");
  }

  if (activeFilterCount === 0) {
    parts.push("順位、狙い度、境界差の指定なし");
  }

  return parts.join(" / ");
}

export function getHuntBacktestBookmarkStorageKey(storeId) {
  return `${HUNT_BACKTEST_BOOKMARK_STORAGE_PREFIX}${normalizeText(storeId)}`;
}

export function getHuntBacktestBookmarksStorageKey(storeId) {
  return `${HUNT_BACKTEST_BOOKMARKS_STORAGE_PREFIX}${normalizeText(storeId)}`;
}

export function getHuntBacktestBookmarkSelectionStorageKey(storeId) {
  return `${HUNT_BACKTEST_BOOKMARK_SELECTION_STORAGE_PREFIX}${normalizeText(storeId)}`;
}

function dispatchHuntBacktestBookmarkEvent(storeId) {
  if (typeof window === "undefined") {
    return;
  }

  window.dispatchEvent(
    new CustomEvent(HUNT_BACKTEST_BOOKMARK_EVENT, {
      detail: { storeId: normalizeText(storeId) },
    }),
  );
}

function normalizeSavedBookmarkList(value, storeId) {
  const entries = Array.isArray(value)
    ? value
    : Array.isArray(value?.bookmarks)
      ? value.bookmarks
      : value
        ? [value]
        : [];
  return entries
    .map((entry, index) =>
      normalizeHuntBacktestBookmark(
        {
          ...entry,
          id: normalizeBookmarkId(entry?.id) || `condition-${index + 1}`,
          name: normalizeBookmarkName(entry?.name, `保存条件${index + 1}`),
          storeId: normalizeText(entry?.storeId) || normalizeText(storeId),
        },
        storeId,
      ),
    )
    .filter(Boolean);
}

export function readSelectedHuntBacktestBookmarkId(storeId) {
  if (typeof window === "undefined") {
    return "";
  }

  try {
    return normalizeText(
      window.localStorage.getItem(getHuntBacktestBookmarkSelectionStorageKey(storeId)),
    );
  } catch {
    return "";
  }
}

export function saveSelectedHuntBacktestBookmarkId(storeId, bookmarkId) {
  if (typeof window === "undefined") {
    return;
  }

  const normalizedId = normalizeText(bookmarkId);
  try {
    if (normalizedId) {
      window.localStorage.setItem(
        getHuntBacktestBookmarkSelectionStorageKey(storeId),
        normalizedId,
      );
    } else {
      window.localStorage.removeItem(getHuntBacktestBookmarkSelectionStorageKey(storeId));
    }
  } catch {
    // 保存できない環境では、その場の選択だけを有効にします。
  }
  dispatchHuntBacktestBookmarkEvent(storeId);
}

export function readSavedHuntBacktestBookmarks(storeId) {
  if (typeof window === "undefined") {
    return [];
  }

  const storageKey = getHuntBacktestBookmarksStorageKey(storeId);
  const legacyStorageKey = getHuntBacktestBookmarkStorageKey(storeId);
  const rawValue = window.localStorage.getItem(storageKey);
  if (rawValue) {
    try {
      const normalizedBookmarks = normalizeSavedBookmarkList(JSON.parse(rawValue), storeId);
      if (normalizedBookmarks.length === 0) {
        window.localStorage.removeItem(storageKey);
      }
      return normalizedBookmarks;
    } catch {
      window.localStorage.removeItem(storageKey);
      return [];
    }
  }

  const legacyRawValue = window.localStorage.getItem(legacyStorageKey);
  if (!legacyRawValue) {
    return [];
  }
  try {
    const legacyBookmarks = normalizeSavedBookmarkList(JSON.parse(legacyRawValue), storeId);
    return legacyBookmarks.length > 0
      ? [{ ...legacyBookmarks[0], id: legacyBookmarks[0].id || "condition-legacy" }]
      : [];
  } catch {
    window.localStorage.removeItem(legacyStorageKey);
    return [];
  }
}

export function readSavedHuntBacktestBookmark(storeId) {
  const bookmarks = readSavedHuntBacktestBookmarks(storeId);
  if (bookmarks.length === 0) {
    return null;
  }

  const selectedId = readSelectedHuntBacktestBookmarkId(storeId);
  if (selectedId === HUNT_BACKTEST_BOOKMARK_SELECTION_NONE) {
    return null;
  }
  if (selectedId && selectedId !== HUNT_BACKTEST_BOOKMARK_SELECTION_CUSTOM) {
    return bookmarks.find((bookmark) => bookmark.id === selectedId) ?? bookmarks[0];
  }
  return bookmarks[0];
}

function writeSavedHuntBacktestBookmarks(storeId, bookmarks) {
  if (typeof window === "undefined") {
    return;
  }

  const normalizedBookmarks = normalizeSavedBookmarkList(bookmarks, storeId);
  if (normalizedBookmarks.length === 0) {
    window.localStorage.removeItem(getHuntBacktestBookmarksStorageKey(storeId));
    window.localStorage.removeItem(getHuntBacktestBookmarkStorageKey(storeId));
    return;
  }

  window.localStorage.setItem(
    getHuntBacktestBookmarksStorageKey(storeId),
    JSON.stringify({
      version: 2,
      bookmarks: normalizedBookmarks,
    }),
  );
  window.localStorage.removeItem(getHuntBacktestBookmarkStorageKey(storeId));
}

export function saveHuntBacktestBookmark(storeId, bookmark) {
  if (typeof window === "undefined") {
    return null;
  }

  const existingBookmarks = readSavedHuntBacktestBookmarks(storeId);
  const bookmarkId = normalizeBookmarkId(bookmark?.id) || createBookmarkId();
  const normalizedBookmark = normalizeHuntBacktestBookmark(
    {
      ...bookmark,
      id: bookmarkId,
      name: normalizeBookmarkName(bookmark?.name, `保存条件${existingBookmarks.length + 1}`),
      storeId,
      savedAt: new Date().toISOString(),
    },
    storeId,
  );

  if (!normalizedBookmark) {
    return null;
  }

  writeSavedHuntBacktestBookmarks(
    storeId,
    [
      normalizedBookmark,
      ...existingBookmarks.filter((entry) => entry.id !== normalizedBookmark.id),
    ],
  );
  saveSelectedHuntBacktestBookmarkId(storeId, normalizedBookmark.id);
  dispatchHuntBacktestBookmarkEvent(storeId);
  return normalizedBookmark;
}

export function deleteSavedHuntBacktestBookmark(storeId, bookmarkId) {
  if (typeof window === "undefined") {
    return [];
  }

  const normalizedId = normalizeBookmarkId(bookmarkId);
  const nextBookmarks = readSavedHuntBacktestBookmarks(storeId).filter(
    (bookmark) => bookmark.id !== normalizedId,
  );
  writeSavedHuntBacktestBookmarks(storeId, nextBookmarks);
  const selectedId = readSelectedHuntBacktestBookmarkId(storeId);
  if (selectedId === normalizedId) {
    saveSelectedHuntBacktestBookmarkId(
      storeId,
      nextBookmarks[0]?.id ?? HUNT_BACKTEST_BOOKMARK_SELECTION_CUSTOM,
    );
  } else {
    dispatchHuntBacktestBookmarkEvent(storeId);
  }
  return nextBookmarks;
}

export function clearSavedHuntBacktestBookmark(storeId) {
  if (typeof window === "undefined") {
    return;
  }

  window.localStorage.removeItem(getHuntBacktestBookmarkStorageKey(storeId));
  window.localStorage.removeItem(getHuntBacktestBookmarksStorageKey(storeId));
  window.localStorage.removeItem(getHuntBacktestBookmarkSelectionStorageKey(storeId));
  dispatchHuntBacktestBookmarkEvent(storeId);
}

function compareMachineEvaluationBookmarkRows(left, right) {
  const leftScore = readNumber(left?.machineEvaluation?.score);
  const rightScore = readNumber(right?.machineEvaluation?.score);
  const scoreDiff = (rightScore ?? Number.NEGATIVE_INFINITY) -
    (leftScore ?? Number.NEGATIVE_INFINITY);
  if (Math.abs(scoreDiff) > SCORE_EPSILON) {
    return scoreDiff;
  }

  return (
    normalizeText(left?.machineName).localeCompare(normalizeText(right?.machineName), "ja") ||
    normalizeText(left?.slotNumber).localeCompare(normalizeText(right?.slotNumber), "ja", {
      numeric: true,
    })
  );
}

function calculateMachineEvaluationBookmarkContextMap(rows) {
  const sortedRows = [...(Array.isArray(rows) ? rows : [])]
    .filter((row) => row?.machineEvaluation)
    .sort(compareMachineEvaluationBookmarkRows);
  const contextMap = new Map();

  sortedRows.forEach((row, index) => {
    const previousRow = sortedRows[index - 1] ?? null;
    const nextRow = sortedRows[index + 1] ?? null;
    const score = readNumber(row?.machineEvaluation?.score);
    const previousScore = readNumber(previousRow?.machineEvaluation?.score);
    const nextScore = readNumber(nextRow?.machineEvaluation?.score);
    contextMap.set(row, {
      rank: index + 1,
      upperGap: score !== null && previousScore !== null ? previousScore - score : null,
      nextGap: score !== null && nextScore !== null ? score - nextScore : null,
    });
  });

  return contextMap;
}

function hasMachineEvaluationBookmarkFilters(bookmark) {
  return Boolean(
    bookmark?.hasMachineEvaluationScoreFilter ||
      bookmark?.hasMachineEvaluationRankFilter ||
      bookmark?.hasSelectedMachineEvaluationRankFilter ||
      bookmark?.hasMachineEvaluationNextGapFilter ||
      bookmark?.hasSelectedMachineEvaluationNextGapFilter ||
      bookmark?.hasMachineEvaluationUpperGapFilter ||
      bookmark?.hasSelectedMachineEvaluationUpperGapFilter,
  );
}

function combineBookmarkConditionMatches(inputMatched, adoptionMatched, mode) {
  const normalizedMode = normalizeMachineEvaluationBacktestMode(mode);
  if (normalizedMode === MACHINE_EVALUATION_BACKTEST_MODE_MACHINE) {
    return adoptionMatched;
  }
  if (normalizedMode === MACHINE_EVALUATION_BACKTEST_MODE_AND) {
    return inputMatched && adoptionMatched;
  }
  if (normalizedMode === MACHINE_EVALUATION_BACKTEST_MODE_OR) {
    return inputMatched || adoptionMatched;
  }
  return inputMatched;
}

export function buildHuntBacktestBookmarkRowKey(row) {
  return [
    normalizeText(row?.machineName),
    normalizeText(row?.slotNumber),
    normalizeText(row?.bookmarkRank ?? row?.rank),
  ].join("::");
}

export function buildHuntBacktestBookmarkMatches(rows, bookmark) {
  const normalizedBookmark = normalizeHuntBacktestBookmark(bookmark);
  const safeRows = Array.isArray(rows) ? rows : [];
  const matchByRowKey = new Map();

  if (!normalizedBookmark) {
    return {
      bookmark: null,
      matchedRowCount: 0,
      totalRowCount: safeRows.length,
      matchByRowKey,
    };
  }

  const selectedMachineNameSet = new Set(normalizedBookmark.machineNames);
  const machineRankCounts = new Map();
  const { machineRankFilter, selectedRankFilter } = buildScopedRankFilters(normalizedBookmark);
  const scoreFilter = buildScoreFilter(normalizedBookmark.scoreMin, normalizedBookmark.scoreMax);
  const machineEvaluationScoreFilter = buildScoreFilter(
    normalizedBookmark.machineEvaluationScoreMin,
    normalizedBookmark.machineEvaluationScoreMax,
  );
  const machineEvaluationRankFilter = buildRankFilter(
    normalizedBookmark.machineEvaluationRankMin,
    normalizedBookmark.machineEvaluationRankMax,
  );
  const selectedMachineEvaluationRankFilter = buildRankFilter(
    normalizedBookmark.selectedMachineEvaluationRankMin,
    normalizedBookmark.selectedMachineEvaluationRankMax,
  );
  const machineEvaluationNextGapFilter = buildNextGapFilter(
    normalizedBookmark.machineEvaluationNextGapMin,
    normalizedBookmark.machineEvaluationNextGapMax,
  );
  const selectedMachineEvaluationNextGapFilter = buildNextGapFilter(
    normalizedBookmark.selectedMachineEvaluationNextGapMin,
    normalizedBookmark.selectedMachineEvaluationNextGapMax,
  );
  const machineEvaluationUpperGapFilter = buildUpperGapFilter(
    normalizedBookmark.machineEvaluationUpperGapMin,
    normalizedBookmark.machineEvaluationUpperGapMax,
  );
  const selectedMachineEvaluationUpperGapFilter = buildUpperGapFilter(
    normalizedBookmark.selectedMachineEvaluationUpperGapMin,
    normalizedBookmark.selectedMachineEvaluationUpperGapMax,
  );
  const usesMachineEvaluationFilters = hasMachineEvaluationBookmarkFilters(normalizedBookmark);
  const {
    machineNextGapFilter,
    selectedNextGapFilter,
    machineUpperGapFilter,
    selectedUpperGapFilter,
  } = buildBoundaryGapFilters(normalizedBookmark);
  const usesMachineTopNextGapSelection = isMachineTopNextGapSelectionMode(
    normalizedBookmark.dailySelectionMode,
  );
  const selectionRankFilter = buildRankFilter(1, 1);
  const selectedNextGapRankFilter = usesMachineTopNextGapSelection ? null : selectedRankFilter;
  const machineNextGapRankFilter = usesMachineTopNextGapSelection ? null : machineRankFilter;
  const selectedRows = safeRows.filter((row) =>
    includesBookmarkMachine(normalizeText(row?.machineName), selectedMachineNameSet),
  );
  const selectedNextGapMap = calculateHuntScoreNextGapMap(
    selectedRows,
    selectedNextGapRankFilter,
  );
  const selectedUpperGapMap = calculateHuntScoreUpperGapMap(
    selectedRows,
    selectedNextGapRankFilter,
  );
  const selectedMachineEvaluationContextMap =
    calculateMachineEvaluationBookmarkContextMap(selectedRows);
  const rowsByBookmarkMachineName = new Map();
  let matchedRowCount = 0;
  let selectedRank = 0;

  for (const row of selectedRows) {
    const machineName = normalizeText(row?.machineName);
    const bookmarkMachineName = resolveBookmarkRankMachineName(
      machineName,
      normalizedBookmark.combineAimJuggler,
      normalizedBookmark.combineHanabi,
      selectedMachineNameSet,
    );
    if (!rowsByBookmarkMachineName.has(bookmarkMachineName)) {
      rowsByBookmarkMachineName.set(bookmarkMachineName, []);
    }
    rowsByBookmarkMachineName.get(bookmarkMachineName).push(row);
  }

  const machineNextGapMap = new Map();
  const machineUpperGapMap = new Map();
  const selectionMachineNextGapMap = new Map();
  const machineEvaluationContextMap = new Map();
  for (const machineRows of rowsByBookmarkMachineName.values()) {
    const nextGapMap = calculateHuntScoreNextGapMap(machineRows, machineNextGapRankFilter);
    const upperGapMap = calculateHuntScoreUpperGapMap(machineRows, machineNextGapRankFilter);
    const machineEvaluationMap = calculateMachineEvaluationBookmarkContextMap(machineRows);
    const selectionNextGapMap = usesMachineTopNextGapSelection
      ? calculateHuntScoreNextGapMap(machineRows, selectionRankFilter)
      : null;
    for (const row of machineRows) {
      if (nextGapMap.has(row)) {
        machineNextGapMap.set(row, nextGapMap.get(row));
      }
      if (upperGapMap.has(row)) {
        machineUpperGapMap.set(row, upperGapMap.get(row));
      }
      if (selectionNextGapMap?.has(row)) {
        selectionMachineNextGapMap.set(row, selectionNextGapMap.get(row));
      }
      if (machineEvaluationMap.has(row)) {
        machineEvaluationContextMap.set(row, machineEvaluationMap.get(row));
      }
    }
  }
  const selectedRowSet = usesMachineTopNextGapSelection
    ? buildMachineTopNextGapBookmarkRowSet(rowsByBookmarkMachineName, selectionMachineNextGapMap)
    : null;

  for (const row of safeRows) {
    const machineName = normalizeText(row?.machineName);
    const rowKey = buildHuntBacktestBookmarkRowKey(row);

    if (!includesBookmarkMachine(machineName, selectedMachineNameSet)) {
      matchByRowKey.set(rowKey, false);
      continue;
    }

    if (selectedRowSet && !selectedRowSet.has(row)) {
      matchByRowKey.set(rowKey, false);
      continue;
    }

    selectedRank += 1;
    const bookmarkMachineName = resolveBookmarkRankMachineName(
      machineName,
      normalizedBookmark.combineAimJuggler,
      normalizedBookmark.combineHanabi,
      selectedMachineNameSet,
    );
    const machineRank = (machineRankCounts.get(bookmarkMachineName) ?? 0) + 1;
    machineRankCounts.set(bookmarkMachineName, machineRank);
    const rowOverallRank = readPositiveInteger(row?.overallRank) ?? readPositiveInteger(row?.rank);
    const rowSelectedRank = readPositiveInteger(row?.selectedRank) ?? selectedRank;
    const rowMachineRank = readPositiveInteger(row?.machineRank) ?? machineRank;
    const machineNextGapValue = machineNextGapMap.get(row) ?? null;
    const selectedNextGapValue = selectedNextGapMap.get(row) ?? null;
    const machineUpperGapValue = machineUpperGapMap.get(row) ?? null;
    const selectedUpperGapValue = selectedUpperGapMap.get(row) ?? null;
    const machineEvaluationContext = machineEvaluationContextMap.get(row) ?? {};
    const selectedMachineEvaluationContext = selectedMachineEvaluationContextMap.get(row) ?? {};
    const commonMatched = matchesRequiredConditionFilters(
      [
        {
          rankValue: rowMachineRank,
          rankFilter: machineRankFilter,
          required: normalizedBookmark.machineRankRequired,
        },
        {
          rankValue: rowSelectedRank ?? rowOverallRank,
          rankFilter: selectedRankFilter,
          required: normalizedBookmark.selectedRankRequired,
        },
      ],
      row?.huntScore,
      null,
      scoreFilter,
      {
        machineRankRequired: normalizedBookmark.machineRankRequired,
        selectedRankRequired: normalizedBookmark.selectedRankRequired,
        scoreRequired: normalizedBookmark.scoreRequired,
      },
      true,
      null,
      { hasNextGapFilter: false, nextGapMin: null, nextGapMax: null },
      null,
      { hasUpperGapFilter: false, upperGapMin: null, upperGapMax: null },
      [
        {
          value: machineUpperGapValue,
          filter: machineUpperGapFilter,
          required: normalizedBookmark.machineUpperGapRequired,
        },
        {
          value: machineNextGapValue,
          filter: machineNextGapFilter,
          required: normalizedBookmark.machineNextGapRequired,
        },
        {
          value: selectedUpperGapValue,
          filter: selectedUpperGapFilter,
          required: normalizedBookmark.selectedUpperGapRequired,
        },
        {
          value: selectedNextGapValue,
          filter: selectedNextGapFilter,
          required: normalizedBookmark.selectedNextGapRequired,
        },
      ],
    );
    const machineEvaluationFilterMatched = usesMachineEvaluationFilters
      ? matchesRequiredConditionFilters(
          [
            {
              rankValue: machineEvaluationContext.rank,
              rankFilter: machineEvaluationRankFilter,
              required: normalizedBookmark.machineEvaluationRankRequired,
            },
            {
              rankValue: selectedMachineEvaluationContext.rank,
              rankFilter: selectedMachineEvaluationRankFilter,
              required: normalizedBookmark.selectedMachineEvaluationRankRequired,
            },
          ],
          row?.machineEvaluation?.score,
          null,
          machineEvaluationScoreFilter,
          {
            scoreRequired: normalizedBookmark.machineEvaluationScoreRequired,
          },
          false,
          null,
          { hasNextGapFilter: false, nextGapMin: null, nextGapMax: null },
          null,
          { hasUpperGapFilter: false, upperGapMin: null, upperGapMax: null },
          [
            {
              value: machineEvaluationContext.upperGap,
              filter: machineEvaluationUpperGapFilter,
              required: normalizedBookmark.machineEvaluationUpperGapRequired,
            },
            {
              value: machineEvaluationContext.nextGap,
              filter: machineEvaluationNextGapFilter,
              required: normalizedBookmark.machineEvaluationNextGapRequired,
            },
            {
              value: selectedMachineEvaluationContext.upperGap,
              filter: selectedMachineEvaluationUpperGapFilter,
              required: normalizedBookmark.selectedMachineEvaluationUpperGapRequired,
            },
            {
              value: selectedMachineEvaluationContext.nextGap,
              filter: selectedMachineEvaluationNextGapFilter,
              required: normalizedBookmark.selectedMachineEvaluationNextGapRequired,
            },
          ],
        )
      : true;
    const inputMatched = commonMatched && machineEvaluationFilterMatched;
    const adoptionMatched = Boolean(row?.machineEvaluation?.matchesAdoption);
    const matched = combineBookmarkConditionMatches(
      inputMatched,
      adoptionMatched,
      normalizedBookmark.machineEvaluationBacktestMode,
    );

    if (matched) {
      matchedRowCount += 1;
    }

    matchByRowKey.set(rowKey, matched);
  }

  return {
    bookmark: normalizedBookmark,
    matchedRowCount,
    totalRowCount: safeRows.length,
    matchByRowKey,
  };
}
