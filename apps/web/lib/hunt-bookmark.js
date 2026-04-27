const HUNT_BACKTEST_BOOKMARK_STORAGE_PREFIX = "hunt-backtest-bookmark:";

export const HUNT_BACKTEST_BOOKMARK_EVENT = "hunt-backtest-bookmark-change";

function normalizeText(value) {
  return String(value ?? "").trim();
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

export function normalizeMatchMode(value) {
  return value === "or" ? "or" : "and";
}

export function normalizeRankScope(value) {
  return value === "machine" ? "machine" : "all";
}

export function matchesOptionalFilters(rankValue, huntScore, rankFilter, scoreFilter, matchMode) {
  const normalizedRankValue = readPositiveInteger(rankValue);
  const rankMatched = rankFilter.hasRankFilter
    ? normalizedRankValue !== null &&
      normalizedRankValue >= rankFilter.rankMin &&
      normalizedRankValue <= rankFilter.rankMax
    : false;
  const scoreMatched = scoreFilter.hasScoreFilter
    ? readFiniteNumber(huntScore, Number.NEGATIVE_INFINITY) >= scoreFilter.scoreMin
    : false;

  if (rankFilter.hasRankFilter && scoreFilter.hasScoreFilter) {
    return matchMode === "or" ? rankMatched || scoreMatched : rankMatched && scoreMatched;
  }
  if (rankFilter.hasRankFilter) {
    return rankMatched;
  }
  if (scoreFilter.hasScoreFilter) {
    return scoreMatched;
  }
  return true;
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
  const allMachineCount = readPositiveInteger(bookmark.allMachineCount) ?? machineNames.length;

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
    matchMode: normalizeMatchMode(bookmark.matchMode),
    rankScope: normalizeRankScope(bookmark.rankScope),
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
    normalizedLeft.matchMode === normalizedRight.matchMode &&
    normalizedLeft.rankScope === normalizedRight.rankScope &&
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

export function formatHuntBacktestBookmarkSummary(bookmark) {
  const normalizedBookmark = normalizeHuntBacktestBookmark(bookmark);
  if (!normalizedBookmark) {
    return "";
  }

  const parts = [buildMachineSummaryText(normalizedBookmark)];

  if (normalizedBookmark.startDate && normalizedBookmark.endDate) {
    parts.push(`期間${normalizedBookmark.startDate}〜${normalizedBookmark.endDate}`);
  }

  parts.push(normalizedBookmark.rankScope === "machine" ? "機種内順位" : "全機種順位");

  if (normalizedBookmark.hasRankFilter) {
    parts.push(`順位${normalizedBookmark.rankMin}〜${normalizedBookmark.rankMax}`);
  }

  if (normalizedBookmark.hasScoreFilter) {
    parts.push(`狙い度${trimDecimalText(normalizedBookmark.scoreMin)}以上`);
  }

  if (normalizedBookmark.hasRankFilter && normalizedBookmark.hasScoreFilter) {
    parts.push(normalizedBookmark.matchMode === "or" ? "どちらか一致" : "両方一致");
  }

  if (!normalizedBookmark.hasRankFilter && !normalizedBookmark.hasScoreFilter) {
    parts.push("順位と狙い度の指定なし");
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
  return [normalizeText(row?.machineName), normalizeText(row?.slotNumber), normalizeText(row?.rank)].join("::");
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
  let matchedRowCount = 0;

  for (const row of safeRows) {
    const machineName = normalizeText(row?.machineName);
    const rowKey = buildHuntBacktestBookmarkRowKey(row);

    if (!selectedMachineNameSet.has(machineName)) {
      matchByRowKey.set(rowKey, false);
      continue;
    }

    const machineRank = (machineRankCounts.get(machineName) ?? 0) + 1;
    machineRankCounts.set(machineName, machineRank);
    const rankValue =
      normalizedBookmark.rankScope === "machine" ? machineRank : readPositiveInteger(row?.rank);
    const matched = matchesOptionalFilters(
      rankValue,
      row?.huntScore,
      rankFilter,
      scoreFilter,
      normalizedBookmark.matchMode,
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
