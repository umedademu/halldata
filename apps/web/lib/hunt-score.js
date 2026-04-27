import { calculateSettingEstimate, getSettingEstimateDefinition } from "./setting-estimates";

const HUNT_SCORE_EPSILON = 0.000000001;
const HUNT_SCORE_WINDOW_DAYS = 7;
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

function calculateCurrentLosingStreak(windowRows) {
  let streak = 0;

  for (let index = windowRows.length - 1; index >= 0; index -= 1) {
    if (windowRows[index].differenceValue >= 0) {
      break;
    }
    streak += 1;
  }

  return streak;
}

function scoreFromMinimums(value, thresholds) {
  for (const threshold of thresholds) {
    if (value >= threshold.minimum) {
      return threshold.score;
    }
  }

  return 0;
}

function scoreFromMaximums(value, thresholds) {
  for (const threshold of thresholds) {
    if (value <= threshold.maximum) {
      return threshold.score;
    }
  }

  return 0;
}

function calculateLossDaysScore(value) {
  return scoreFromMinimums(value, [
    { minimum: 7, score: 25 },
    { minimum: 6, score: 21 },
    { minimum: 5, score: 16 },
    { minimum: 4, score: 10 },
    { minimum: 3, score: 5 },
  ]);
}

function calculateStreakScore(value) {
  return scoreFromMinimums(value, [
    { minimum: 7, score: 18 },
    { minimum: 6, score: 16 },
    { minimum: 5, score: 14 },
    { minimum: 4, score: 11 },
    { minimum: 3, score: 8 },
    { minimum: 2, score: 4 },
  ]);
}

function calculateLossAbsScore(value) {
  return scoreFromMinimums(value, [
    { minimum: 6000, score: 18 },
    { minimum: 5000, score: 15 },
    { minimum: 4000, score: 12 },
    { minimum: 3000, score: 8 },
    { minimum: 2000, score: 4 },
  ]);
}

function calculateNetTotalScore(value) {
  return scoreFromMaximums(value, [
    { maximum: -5000, score: 14 },
    { maximum: -4000, score: 12 },
    { maximum: -3000, score: 9 },
    { maximum: -2000, score: 6 },
    { maximum: -1000, score: 3 },
  ]);
}

function calculateCompensationRateScore(value) {
  return scoreFromMaximums(value, [
    { maximum: 0.2, score: 10 },
    { maximum: 0.35, score: 8 },
    { maximum: 0.5, score: 6 },
    { maximum: 0.7, score: 3 },
    { maximum: 1, score: 1 },
  ]);
}

function calculateMaxWinScore(value) {
  return scoreFromMaximums(value, [
    { maximum: 500, score: 7 },
    { maximum: 1000, score: 5 },
    { maximum: 1500, score: 3 },
    { maximum: 2000, score: 1 },
  ]);
}

function calculateTodayDifferenceScore(value) {
  return scoreFromMaximums(value, [
    { maximum: -2000, score: 5 },
    { maximum: -1000, score: 4 },
    { maximum: -500, score: 3 },
    { maximum: 0, score: 2 },
    { maximum: 1000, score: 1 },
  ]);
}

function calculateTodaySettingScore(value) {
  return scoreFromMaximums(value, [
    { maximum: 2, score: 3 },
    { maximum: 3, score: 2 },
    { maximum: 4, score: 1 },
  ]);
}

function calculateAbsoluteHuntScore(metrics) {
  const totalScore =
    calculateLossDaysScore(metrics.lossDays) +
    calculateStreakScore(metrics.streak) +
    calculateLossAbsScore(metrics.lossAbsTotal) +
    calculateNetTotalScore(metrics.netTotal) +
    calculateCompensationRateScore(metrics.compensationRate) +
    calculateMaxWinScore(metrics.maxWin) +
    calculateTodayDifferenceScore(metrics.todayDifference) +
    calculateTodaySettingScore(metrics.todaySetting);

  return clamp(totalScore, 0, 100);
}

function buildWindowRows(businessDates, dateIndex, recordMapByDate) {
  if (dateIndex < HUNT_SCORE_WINDOW_DAYS - 1) {
    return null;
  }

  const windowDates = businessDates.slice(dateIndex - (HUNT_SCORE_WINDOW_DAYS - 1), dateIndex + 1);
  if (windowDates.length < HUNT_SCORE_WINDOW_DAYS) {
    return null;
  }

  const windowRows = [];

  for (const date of windowDates) {
    const row = recordMapByDate.get(date);
    const differenceValue = readNumber(row?.difference_value);
    if (!row || !Number.isFinite(differenceValue)) {
      return null;
    }

    windowRows.push({
      row,
      differenceValue,
    });
  }

  return windowRows;
}

function calculateWindowMetrics(businessDates, dateIndex, row, recordMapByDate, settingDefinitionCache) {
  const windowRows = buildWindowRows(businessDates, dateIndex, recordMapByDate);
  if (!windowRows) {
    return null;
  }

  let lossDays = 0;
  let winAbsTotal = 0;
  let lossAbsTotal = 0;
  let netTotal = 0;
  let maxWin = 0;

  for (const windowRow of windowRows) {
    const differenceValue = windowRow.differenceValue;
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
    streak: calculateCurrentLosingStreak(windowRows),
    lossAbsTotal,
    netTotal,
    compensationRate: lossAbsTotal === 0 ? 999 : winAbsTotal / lossAbsTotal,
    maxWin,
    todayDifference: readNumber(row?.difference_value) ?? 0,
    todaySetting,
  };
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
    const metrics = calculateWindowMetrics(
      businessDates,
      dateIndex,
      row,
      recordMapByDate,
      settingDefinitionCache,
    );

    return {
      row,
      rowKey: buildRowKey(row),
      candidateKey,
      metrics,
    };
  });

  const rows = candidates
    .filter((candidate) => candidate.metrics)
    .map((candidate) => {
      const huntScore = calculateAbsoluteHuntScore(candidate.metrics);
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
