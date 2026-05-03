const HUNT_BACKTEST_BOOKMARK_STORAGE_PREFIX = "hunt-backtest-bookmark:";
const AIM_JUGGLER_GROUP_NAME = "アイムジャグラーEX";
const AIM_JUGGLER_MACHINE_NAMES = ["SアイムジャグラーＥＸ", "ネオアイムジャグラーEX"];
const HANABI_GROUP_NAME = "ハナビ";
const HANABI_MACHINE_NAMES = ["新ハナビ", "スマスロ ハナビ"];
const DEVIATION_EPSILON = 0.000000001;

export const HUNT_BACKTEST_BOOKMARK_EVENT = "hunt-backtest-bookmark-change";

function normalizeText(value) {
  return String(value ?? "").trim();
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

export function buildScoreFilter(scoreMinValue) {
  const scoreMin = readNumber(scoreMinValue);

  return {
    scoreMin: scoreMin === null ? null : Math.min(100, Math.max(0, scoreMin)),
    hasScoreFilter: scoreMin !== null,
  };
}

export function buildDeviationFilter(deviationMinValue) {
  const deviationMin = readNumber(deviationMinValue);

  return {
    deviationMin,
    hasDeviationFilter: deviationMin !== null,
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
    !Object.hasOwn(safeSource, "scoreRequired") &&
    !Object.hasOwn(safeSource, "deviationRequired") &&
    Object.hasOwn(safeSource, "matchMode")
  ) {
    const allRequired = normalizeMatchMode(safeSource.matchMode) === "and";
    return {
      rankRequired: allRequired,
      scoreRequired: allRequired,
      deviationRequired: allRequired,
    };
  }

  return {
    rankRequired: normalizeRequiredOption(safeSource.rankRequired, Boolean(fallback.rankRequired)),
    scoreRequired: normalizeRequiredOption(safeSource.scoreRequired, Boolean(fallback.scoreRequired)),
    deviationRequired: normalizeRequiredOption(
      safeSource.deviationRequired,
      Boolean(fallback.deviationRequired),
    ),
  };
}

export function normalizeRankScope(value) {
  if (value === "machine" || value === "selected") {
    return value;
  }
  return "all";
}

function formatRankScopeLabel(rankScope) {
  if (rankScope === "machine") {
    return "機種内順位";
  }
  if (rankScope === "selected") {
    return "チェック機種内順位";
  }
  return "全機種順位";
}

function formatDeviationScopeLabel(deviationScope) {
  if (deviationScope === "machine") {
    return "機種内偏差値";
  }
  if (deviationScope === "selected") {
    return "チェック機種内偏差値";
  }
  return "全機種内偏差値";
}

export function calculateHuntScoreDeviationMap(rows) {
  const validRows = (Array.isArray(rows) ? rows : [])
    .map((row) => ({
      row,
      score: readNumber(row?.huntScore),
    }))
    .filter((entry) => entry.score !== null);

  if (validRows.length < 2) {
    return new Map();
  }

  const average =
    validRows.reduce((sum, entry) => sum + entry.score, 0) / validRows.length;
  const variance =
    validRows.reduce((sum, entry) => sum + (entry.score - average) ** 2, 0) /
    validRows.length;
  const standardDeviation = Math.sqrt(variance);

  if (!Number.isFinite(standardDeviation) || standardDeviation <= DEVIATION_EPSILON) {
    return new Map();
  }

  return new Map(
    validRows.map((entry) => [
      entry.row,
      50 + ((entry.score - average) / standardDeviation) * 10,
    ]),
  );
}

export function readDeviationForRankScope(row, rankScope) {
  if (rankScope === "machine") {
    return readNumber(row?.machineDeviation);
  }
  if (rankScope === "selected") {
    return readNumber(row?.selectedDeviation);
  }
  return readNumber(row?.overallDeviation);
}

export function matchesRequiredConditionFilters(
  rankValue,
  huntScore,
  rankFilter,
  scoreFilter,
  requirementOptions = {},
  deviationValue = null,
  deviationFilter = { hasDeviationFilter: false, deviationMin: null },
  noFilterResult = true,
) {
  const normalizedRequirements = buildConditionRequirementOptions(requirementOptions);
  const normalizedRankValue = readPositiveInteger(rankValue);
  const conditionEntries = [];

  if (rankFilter.hasRankFilter) {
    conditionEntries.push({
      matched:
        normalizedRankValue !== null &&
        normalizedRankValue >= rankFilter.rankMin &&
        normalizedRankValue <= rankFilter.rankMax,
      required: normalizedRequirements.rankRequired,
    });
  }
  if (scoreFilter.hasScoreFilter) {
    conditionEntries.push({
      matched: readFiniteNumber(huntScore, Number.NEGATIVE_INFINITY) >= scoreFilter.scoreMin,
      required: normalizedRequirements.scoreRequired,
    });
  }
  if (deviationFilter.hasDeviationFilter) {
    const normalizedDeviationValue = readNumber(deviationValue);
    conditionEntries.push({
      matched:
        normalizedDeviationValue !== null &&
        normalizedDeviationValue >= deviationFilter.deviationMin,
      required: normalizedRequirements.deviationRequired,
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

export function normalizeHuntBacktestBookmark(bookmark, fallbackStoreId = "") {
  if (!bookmark || typeof bookmark !== "object") {
    return null;
  }

  const storeId = normalizeText(bookmark.storeId) || normalizeText(fallbackStoreId);
  const machineNames = normalizeMachineNames(bookmark.machineNames);
  if (!storeId || machineNames.length === 0) {
    return null;
  }

  const rankFilter = buildRankFilter(bookmark.rankMin, bookmark.rankMax);
  const scoreFilter = buildScoreFilter(bookmark.scoreMin);
  const deviationFilter = buildDeviationFilter(bookmark.deviationMin);
  const rankScope = normalizeRankScope(bookmark.rankScope);
  const requirementOptions = buildConditionRequirementOptions(bookmark);
  const allMachineCount = readPositiveInteger(bookmark.allMachineCount) ?? machineNames.length;
  const combineAimJuggler = Boolean(bookmark.combineAimJuggler) || machineNames.some(isAimJugglerGroup);
  const combineHanabi = Boolean(bookmark.combineHanabi) || machineNames.some(isHanabiGroup);

  return {
    version: 1,
    storeId,
    startDate: normalizeDateText(bookmark.startDate),
    endDate: normalizeDateText(bookmark.endDate),
    allMachineCount,
    machineNames,
    rankMin: rankFilter.rankMin,
    rankMax: rankFilter.rankMax,
    hasRankFilter: rankFilter.hasRankFilter,
    scoreMin: scoreFilter.scoreMin,
    hasScoreFilter: scoreFilter.hasScoreFilter,
    deviationMin: deviationFilter.deviationMin,
    hasDeviationFilter: deviationFilter.hasDeviationFilter,
    rankRequired: requirementOptions.rankRequired,
    scoreRequired: requirementOptions.scoreRequired,
    deviationRequired: requirementOptions.deviationRequired,
    rankScope,
    deviationScope: normalizeRankScope(bookmark.deviationScope ?? rankScope),
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
    normalizedLeft.startDate === normalizedRight.startDate &&
    normalizedLeft.endDate === normalizedRight.endDate &&
    normalizedLeft.rankMin === normalizedRight.rankMin &&
    normalizedLeft.rankMax === normalizedRight.rankMax &&
    normalizedLeft.scoreMin === normalizedRight.scoreMin &&
    normalizedLeft.deviationMin === normalizedRight.deviationMin &&
    normalizedLeft.rankRequired === normalizedRight.rankRequired &&
    normalizedLeft.scoreRequired === normalizedRight.scoreRequired &&
    normalizedLeft.deviationRequired === normalizedRight.deviationRequired &&
    normalizedLeft.rankScope === normalizedRight.rankScope &&
    normalizedLeft.deviationScope === normalizedRight.deviationScope &&
    normalizedLeft.combineAimJuggler === normalizedRight.combineAimJuggler &&
    normalizedLeft.combineHanabi === normalizedRight.combineHanabi &&
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

  parts.push(formatRankScopeLabel(normalizedBookmark.rankScope));
  parts.push(formatDeviationScopeLabel(normalizedBookmark.deviationScope));

  if (normalizedBookmark.hasRankFilter) {
    parts.push(
      `順位${normalizedBookmark.rankMin}〜${normalizedBookmark.rankMax}${
        normalizedBookmark.rankRequired ? "必須" : ""
      }`,
    );
  }

  if (normalizedBookmark.hasScoreFilter) {
    parts.push(
      `狙い度${trimDecimalText(normalizedBookmark.scoreMin)}以上${
        normalizedBookmark.scoreRequired ? "必須" : ""
      }`,
    );
  }

  if (normalizedBookmark.hasDeviationFilter) {
    parts.push(
      `偏差値${trimDecimalText(normalizedBookmark.deviationMin)}以上${
        normalizedBookmark.deviationRequired ? "必須" : ""
      }`,
    );
  }

  const activeFilterCount = [
    normalizedBookmark.hasRankFilter,
    normalizedBookmark.hasScoreFilter,
    normalizedBookmark.hasDeviationFilter,
  ].filter(Boolean).length;

  if (normalizedBookmark.combineAimJuggler) {
    parts.push("アイム統合");
  }

  if (normalizedBookmark.combineHanabi) {
    parts.push("ハナビ統合");
  }

  if (activeFilterCount === 0) {
    parts.push("順位、狙い度、偏差値の指定なし");
  }

  return parts.join(" / ");
}

export function getHuntBacktestBookmarkStorageKey(storeId) {
  return `${HUNT_BACKTEST_BOOKMARK_STORAGE_PREFIX}${normalizeText(storeId)}`;
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

export function readSavedHuntBacktestBookmark(storeId) {
  if (typeof window === "undefined") {
    return null;
  }

  const storageKey = getHuntBacktestBookmarkStorageKey(storeId);
  const rawValue = window.localStorage.getItem(storageKey);
  if (!rawValue) {
    return null;
  }

  try {
    const parsedValue = JSON.parse(rawValue);
    const normalizedBookmark = normalizeHuntBacktestBookmark(parsedValue, storeId);
    if (!normalizedBookmark) {
      window.localStorage.removeItem(storageKey);
      return null;
    }
    return normalizedBookmark;
  } catch {
    window.localStorage.removeItem(storageKey);
    return null;
  }
}

export function saveHuntBacktestBookmark(storeId, bookmark) {
  if (typeof window === "undefined") {
    return null;
  }

  const normalizedBookmark = normalizeHuntBacktestBookmark(
    {
      ...bookmark,
      storeId,
      savedAt: new Date().toISOString(),
    },
    storeId,
  );

  if (!normalizedBookmark) {
    return null;
  }

  window.localStorage.setItem(
    getHuntBacktestBookmarkStorageKey(storeId),
    JSON.stringify(normalizedBookmark),
  );
  dispatchHuntBacktestBookmarkEvent(storeId);
  return normalizedBookmark;
}

export function clearSavedHuntBacktestBookmark(storeId) {
  if (typeof window === "undefined") {
    return;
  }

  window.localStorage.removeItem(getHuntBacktestBookmarkStorageKey(storeId));
  dispatchHuntBacktestBookmarkEvent(storeId);
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
  const rankFilter = buildRankFilter(normalizedBookmark.rankMin, normalizedBookmark.rankMax);
  const scoreFilter = buildScoreFilter(normalizedBookmark.scoreMin);
  const deviationFilter = buildDeviationFilter(normalizedBookmark.deviationMin);
  const overallDeviationMap = calculateHuntScoreDeviationMap(safeRows);
  const selectedRows = safeRows.filter((row) =>
    includesBookmarkMachine(normalizeText(row?.machineName), selectedMachineNameSet),
  );
  const selectedDeviationMap = calculateHuntScoreDeviationMap(selectedRows);
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

  const machineDeviationMap = new Map();
  for (const machineRows of rowsByBookmarkMachineName.values()) {
    const deviationMap = calculateHuntScoreDeviationMap(machineRows);
    for (const row of machineRows) {
      if (deviationMap.has(row)) {
        machineDeviationMap.set(row, deviationMap.get(row));
      }
    }
  }

  for (const row of safeRows) {
    const machineName = normalizeText(row?.machineName);
    const rowKey = buildHuntBacktestBookmarkRowKey(row);

    if (!includesBookmarkMachine(machineName, selectedMachineNameSet)) {
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
    const rankValue =
      normalizedBookmark.rankScope === "machine"
        ? rowMachineRank
        : normalizedBookmark.rankScope === "selected"
          ? rowSelectedRank
          : rowOverallRank;
    const existingDeviationValue = readDeviationForRankScope(row, normalizedBookmark.deviationScope);
    const calculatedDeviationValue =
      normalizedBookmark.deviationScope === "machine"
        ? machineDeviationMap.get(row) ?? null
        : normalizedBookmark.deviationScope === "selected"
          ? selectedDeviationMap.get(row) ?? null
          : overallDeviationMap.get(row) ?? null;
    const deviationValue = existingDeviationValue ?? calculatedDeviationValue;
    const matched = matchesRequiredConditionFilters(
      rankValue,
      row?.huntScore,
      rankFilter,
      scoreFilter,
      {
        rankRequired: normalizedBookmark.rankRequired,
        scoreRequired: normalizedBookmark.scoreRequired,
        deviationRequired: normalizedBookmark.deviationRequired,
      },
      deviationValue,
      deviationFilter,
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
