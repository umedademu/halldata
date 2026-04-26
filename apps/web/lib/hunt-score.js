import { calculateSettingEstimate, getSettingEstimateDefinition } from "./setting-estimates";

const HUNT_SCORE_EPSILON = 0.000000001;
const HUNT_SCORE_TARGET_STORE_NAMES = ["Aパーク春日店"];
const HUNT_SCORE_TARGET_MACHINE_NAMES = [
  "SアイムジャグラーＥＸ",
  "ネオアイムジャグラーEX",
  "マイジャグラーV",
  "ゴーゴージャグラー３",
  "ファンキージャグラー２ＫＴ",
  "ミスタージャグラー",
  "ジャグラーガールズSS",
];

function normalizeText(value) {
  return String(value ?? "").normalize("NFKC").replace(/\s+/gu, "").trim();
}

function readNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function hasMeaningfulResult(row) {
  return ["difference_value", "games_count", "bb_count", "rb_count"].some((key) =>
    Number.isFinite(readNumber(row?.[key])),
  );
}

function buildRowKey(row) {
  return [
    String(row?.target_date ?? "").trim(),
    normalizeText(row?.machine_name),
    String(row?.slot_number ?? "").trim(),
  ].join("\u0000");
}

function buildCandidateKey(row) {
  return [normalizeText(row?.machine_name), String(row?.slot_number ?? "").trim()].join("\u0000");
}

function getSettingDefinition(settingDefinitionCache, machineName) {
  const cacheKey = normalizeText(machineName);
  let definition = settingDefinitionCache.get(cacheKey);
  if (definition === undefined) {
    definition = getSettingEstimateDefinition(machineName);
    settingDefinitionCache.set(cacheKey, definition ?? null);
  }
  return definition;
}

function getSettingEstimateAverage(settingDefinitionCache, row) {
  const definition = getSettingDefinition(settingDefinitionCache, row?.machine_name);
  const estimate = definition ? calculateSettingEstimate(definition, row) : null;
  return {
    estimate,
    average: estimate?.average ?? 0,
  };
}

function calculateCurrentLosingStreak(businessDates, dateIndex, recordMapByDate) {
  let streak = 0;

  for (let index = dateIndex; index >= 0; index -= 1) {
    const row = recordMapByDate.get(businessDates[index]);
    const differenceValue = readNumber(row?.difference_value);
    if (!Number.isFinite(differenceValue) || differenceValue >= 0) {
      break;
    }
    streak += 1;
  }

  return streak;
}

function calculateWindowMetrics(businessDates, dateIndex, row, recordMapByDate, settingDefinitionCache) {
  const windowDates = businessDates.slice(Math.max(0, dateIndex - 6), dateIndex + 1);
  let lossDays = 0;
  let winAbsTotal = 0;
  let lossAbsTotal = 0;
  let netTotal = 0;
  let maxWin = 0;

  for (const date of windowDates) {
    const windowRow = recordMapByDate.get(date);
    const differenceValue = readNumber(windowRow?.difference_value);

    if (!Number.isFinite(differenceValue)) {
      continue;
    }

    netTotal += differenceValue;

    if (differenceValue < 0) {
      lossDays += 1;
      lossAbsTotal += Math.abs(differenceValue);
      continue;
    }

    if (differenceValue > 0) {
      winAbsTotal += differenceValue;
      maxWin = Math.max(maxWin, differenceValue);
    }
  }

  const todaySetting = getSettingEstimateAverage(settingDefinitionCache, row).average;

  return {
    lossDays,
    streak: calculateCurrentLosingStreak(businessDates, dateIndex, recordMapByDate),
    lossAbsTotal,
    netWeakness: -netTotal,
    compensationRate: lossAbsTotal === 0 ? 999 : winAbsTotal / lossAbsTotal,
    maxWin,
    todayDifference: readNumber(row?.difference_value) ?? 0,
    todaySetting,
  };
}

function assignRankScore(candidates, metricKey, scoreKey, direction) {
  const sortable = candidates
    .map((candidate) => ({
      candidate,
      value: readNumber(candidate?.metrics?.[metricKey]),
    }))
    .filter((entry) => Number.isFinite(entry.value))
    .sort((left, right) => {
      if (Math.abs(left.value - right.value) <= HUNT_SCORE_EPSILON) {
        return left.candidate.rowKey.localeCompare(right.candidate.rowKey, "ja");
      }
      return direction === "high" ? right.value - left.value : left.value - right.value;
    });

  const total = sortable.length;
  if (total === 0) {
    return;
  }

  let index = 0;
  while (index < total) {
    let endIndex = index + 1;
    while (
      endIndex < total &&
      Math.abs(sortable[endIndex].value - sortable[index].value) <= HUNT_SCORE_EPSILON
    ) {
      endIndex += 1;
    }

    const averageIndex = (index + endIndex - 1) / 2;
    const score = total === 1 ? 1 : 1 - averageIndex / (total - 1);

    for (let candidateIndex = index; candidateIndex < endIndex; candidateIndex += 1) {
      sortable[candidateIndex].candidate[scoreKey] = score;
    }

    index = endIndex;
  }
}

function buildBusinessDates(allStoreRows, targetRows) {
  const openDates = new Map();

  for (const row of [...allStoreRows, ...targetRows]) {
    const date = String(row?.target_date ?? "").trim();
    if (!date) {
      continue;
    }

    if (!openDates.has(date)) {
      openDates.set(date, false);
    }

    if (hasMeaningfulResult(row)) {
      openDates.set(date, true);
    }
  }

  return [...openDates.entries()]
    .filter((entry) => entry[1])
    .map((entry) => entry[0])
    .sort((left, right) => left.localeCompare(right));
}

function buildSourceMaps(targetRows, businessDateSet) {
  const rowsByCandidateKey = new Map();
  const rowsByDate = new Map();

  for (const row of targetRows) {
    if (!hasMeaningfulResult(row) || !businessDateSet.has(row?.target_date)) {
      continue;
    }

    const candidateKey = buildCandidateKey(row);
    if (!rowsByCandidateKey.has(candidateKey)) {
      rowsByCandidateKey.set(candidateKey, new Map());
    }
    rowsByCandidateKey.get(candidateKey).set(row.target_date, row);

    if (!rowsByDate.has(row.target_date)) {
      rowsByDate.set(row.target_date, []);
    }
    rowsByDate.get(row.target_date).push(row);
  }

  return {
    rowsByCandidateKey,
    rowsByDate,
  };
}

function buildSnapshotRowsForDate(
  businessDates,
  dateIndex,
  rowsByDate,
  rowsByCandidateKey,
  settingDefinitionCache,
) {
  const baseDate = businessDates[dateIndex];
  const nextBusinessDate = businessDates[dateIndex + 1] ?? null;
  const dateRows = rowsByDate.get(baseDate) ?? [];

  if (dateRows.length === 0) {
    return {
      baseDate,
      nextBusinessDate,
      rows: [],
    };
  }

  const candidates = dateRows.map((row) => {
    const candidateKey = buildCandidateKey(row);
    const recordMapByDate = rowsByCandidateKey.get(candidateKey) ?? new Map();

    return {
      row,
      rowKey: buildRowKey(row),
      candidateKey,
      metrics: calculateWindowMetrics(
        businessDates,
        dateIndex,
        row,
        recordMapByDate,
        settingDefinitionCache,
      ),
      f_lossDays: 0,
      f_streak: 0,
      f_lossAbs: 0,
      f_netWeak: 0,
      f_compLow: 0,
      f_maxWinLow: 0,
      f_todayDiffLow: 0,
      f_todaySetLow: 0,
    };
  });

  assignRankScore(candidates, "lossDays", "f_lossDays", "high");
  assignRankScore(candidates, "streak", "f_streak", "high");
  assignRankScore(candidates, "lossAbsTotal", "f_lossAbs", "high");
  assignRankScore(candidates, "netWeakness", "f_netWeak", "high");
  assignRankScore(candidates, "compensationRate", "f_compLow", "low");
  assignRankScore(candidates, "maxWin", "f_maxWinLow", "low");
  assignRankScore(candidates, "todayDifference", "f_todayDiffLow", "low");
  assignRankScore(candidates, "todaySetting", "f_todaySetLow", "low");

  const rows = candidates
    .map((candidate) => {
      const huntScore = clamp(
        (
          0.3 * candidate.f_lossDays +
          0.2 * candidate.f_streak +
          0.18 * candidate.f_lossAbs +
          0.15 * candidate.f_netWeak +
          0.1 * candidate.f_compLow +
          0.04 * candidate.f_maxWinLow +
          0.02 * candidate.f_todayDiffLow +
          0.01 * candidate.f_todaySetLow
        ) * 100,
        0,
        100,
      );
      const recordMapByDate = rowsByCandidateKey.get(candidate.candidateKey) ?? new Map();
      const nextRecord = nextBusinessDate ? recordMapByDate.get(nextBusinessDate) ?? null : null;
      const nextSetting = nextRecord
        ? getSettingEstimateAverage(settingDefinitionCache, nextRecord).estimate
        : null;

      return {
        baseDate,
        nextBusinessDate,
        rowKey: candidate.rowKey,
        machineName: candidate.row.machine_name,
        slotNumber: candidate.row.slot_number,
        huntScore,
        currentRecord: candidate.row,
        nextRecord,
        nextSettingEstimate: nextSetting,
      };
    })
    .sort((left, right) => {
      if (Math.abs(right.huntScore - left.huntScore) > HUNT_SCORE_EPSILON) {
        return right.huntScore - left.huntScore;
      }
      const machineComparison = left.machineName.localeCompare(right.machineName, "ja");
      if (machineComparison !== 0) {
        return machineComparison;
      }
      return String(left.slotNumber).localeCompare(String(right.slotNumber), "ja");
    })
    .map((row, index) => ({
      ...row,
      rank: index + 1,
    }));

  return {
    baseDate,
    nextBusinessDate,
    rows,
  };
}

export function isHuntScoreTargetStore(storeName) {
  const normalizedStoreName = normalizeText(storeName);
  return HUNT_SCORE_TARGET_STORE_NAMES.some(
    (candidate) => normalizeText(candidate) === normalizedStoreName,
  );
}

export function isHuntScoreTargetMachine(machineName) {
  const normalizedMachineName = normalizeText(machineName);
  return HUNT_SCORE_TARGET_MACHINE_NAMES.some(
    (candidate) => normalizeText(candidate) === normalizedMachineName,
  );
}

export function isHuntScoreSupported(storeName, machineName) {
  return isHuntScoreTargetStore(storeName) && isHuntScoreTargetMachine(machineName);
}

export function listHuntScoreTargetMachineNames() {
  return [...HUNT_SCORE_TARGET_MACHINE_NAMES];
}

export function buildHuntScoreSnapshots(targetRows, allStoreRows = []) {
  if (!Array.isArray(targetRows) || targetRows.length === 0) {
    return [];
  }

  const businessDates = buildBusinessDates(allStoreRows, targetRows);
  if (businessDates.length === 0) {
    return [];
  }

  const businessDateSet = new Set(businessDates);
  const { rowsByCandidateKey, rowsByDate } = buildSourceMaps(targetRows, businessDateSet);
  const settingDefinitionCache = new Map();

  return businessDates
    .map((_, dateIndex) =>
      buildSnapshotRowsForDate(
        businessDates,
        dateIndex,
        rowsByDate,
        rowsByCandidateKey,
        settingDefinitionCache,
      ),
    )
    .filter((snapshot) => snapshot.rows.length > 0)
    .sort((left, right) => right.baseDate.localeCompare(left.baseDate));
}

export function attachHuntScores(targetRows, allStoreRows = []) {
  const snapshots = buildHuntScoreSnapshots(targetRows, allStoreRows);
  const huntScoreByRowKey = new Map();

  for (const snapshot of snapshots) {
    for (const row of snapshot.rows) {
      huntScoreByRowKey.set(row.rowKey, row.huntScore);
    }
  }

  for (const row of targetRows) {
    const huntScore = huntScoreByRowKey.get(buildRowKey(row));
    if (Number.isFinite(huntScore)) {
      row.hunt_score = huntScore;
    }
  }
}
