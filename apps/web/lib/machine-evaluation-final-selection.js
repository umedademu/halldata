const BEAM_HIKARI_STORE_NAME = "ビームヒカリ";
const BEAM_HIKARI_STORE_COMMON_SIMPLE_LOGIC_KEY = "beam-hikari-store-common-v1s";
const BEAM_HIKARI_NEO_FINAL_CONDITION_KEY = "neo-aim-beam-hikari-spatial-integrated";
const BEAM_HIKARI_NEO_FINAL_SELECTOR_KEY = "beam-hikari-neo-spatial-integrated-v1";
const BEAM_HIKARI_NEO_LOGIC_KEYS = new Set([
  "beam-hikari-neo-aim",
  "beam-hikari-neo-aim-normal",
  "beam-hikari-neo-aim-event",
]);
const BEAM_HIKARI_NEO_ROW_LAYOUTS = [
  [
    "871", "872", "873", "875", "876", "877", "878", "880", "881", "882",
    "883", "885", "886", "887", "888", "1000", "1001", "1002", "1003", "1005",
  ],
  [
    "1030", "1028", "1027", "1026", "1025", "1023", "1022", "1021", "1020", "1018",
    "1017", "1016", "1015", "1013", "1012", "1011", "1010", "1008", "1007", "1006",
  ],
];
const BEAM_HIKARI_NEO_MACHINE_COUNT = 40;
const BEAM_HIKARI_NEO_SPATIAL_CANDIDATE_COUNT = 14;
const BEAM_HIKARI_NEO_SPATIAL_SUPPORT_RATIO_MIN = 1.05;
const BEAM_HIKARI_NEO_NORMAL_PLACEMENT_SIZE = 5;
const BEAM_HIKARI_NEO_EVENT_PLACEMENT_SIZE = 8;
const BEAM_HIKARI_NEO_NORMAL_DISTANCE_PENALTIES = new Map([
  [1, 1.42],
  [2, 0.44],
]);
const BEAM_HIKARI_NEO_EVENT_DISTANCE_PENALTIES = new Map([
  [1, 0.42],
  [2, 0.13],
]);
const BEAM_HIKARI_NEO_FINAL_BACKTEST = {
  backtestLabel:
    "後半93日 / 44台 / RBヒット28台（63.6%） / RB1/261.0 / 合算1/132.3 / 平均+570.7枚 / 機械割103.09% / 勝率68.2%",
  backtestPayoutRate: 103.09,
  backtestRbDenominator: 261.0,
};
const EPSILON = 0.000000001;

const BEAM_HIKARI_NEO_SLOT_POSITIONS = new Map(
  BEAM_HIKARI_NEO_ROW_LAYOUTS.flatMap((slots, rowIndex) =>
    slots.map((slotNumber, positionIndex) => [slotNumber, { rowIndex, positionIndex }]),
  ),
);

function normalizeText(value) {
  return String(value ?? "").normalize("NFKC").replace(/\s+/gu, "").trim();
}

function readFiniteNumber(value, fallback = null) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function compareSlotNumbers(left, right) {
  return String(left?.slotNumber ?? "").localeCompare(String(right?.slotNumber ?? ""), "ja", {
    numeric: true,
  });
}

function isBeamHikariStore(storeName) {
  return normalizeText(storeName) === normalizeText(BEAM_HIKARI_STORE_NAME);
}

function isBeamHikariNeoEvaluation(evaluation) {
  return BEAM_HIKARI_NEO_LOGIC_KEYS.has(String(evaluation?.logicKey ?? "").trim());
}

function isBeamHikariEventDate(dateText) {
  const day = Number(String(dateText ?? "").slice(8, 10));
  return [3, 6, 13, 16, 23, 26].includes(day);
}

function resolveBeamHikariNeoEventMode(evaluation, snapshot) {
  if (evaluation?.logicKey === "beam-hikari-neo-aim-event") {
    return true;
  }
  if (evaluation?.logicKey === "beam-hikari-neo-aim-normal") {
    return false;
  }
  return isBeamHikariEventDate(snapshot?.nextBusinessDate);
}

function isBeamHikariNeoHistoryReady(evaluation) {
  return evaluation?.features?.beamHikariNeoHistoryReady === true;
}

function isBeamHikariNeoRbGateCandidate(row, evaluationKey, isEventDate) {
  const evaluation = row?.[evaluationKey];
  const features = evaluation?.features ?? {};
  if (!isBeamHikariNeoHistoryReady(evaluation)) {
    return false;
  }

  if (isEventDate) {
    return (
      readFiniteNumber(features.recentTwoNetTotal, Number.POSITIVE_INFINITY) <= -2000 &&
      (
        readFiniteNumber(features.recentSevenCombinedDenominator, Number.NEGATIVE_INFINITY) >= 168 ||
        readFiniteNumber(features.recentSevenRbDenominator, Number.NEGATIVE_INFINITY) >= 400
      )
    );
  }

  return (
    readFiniteNumber(features.recentTwoAngle, Number.POSITIVE_INFINITY) <= -500 &&
    readFiniteNumber(features.recentFiveNetTotal, Number.POSITIVE_INFINITY) <= -2000 &&
    readFiniteNumber(features.recentSevenRbDenominator, Number.NEGATIVE_INFINITY) >= 400
  );
}

function compareBeamHikariNeoRbGateRows(left, right, evaluationKey, isEventDate) {
  const leftFeatures = left?.[evaluationKey]?.features ?? {};
  const rightFeatures = right?.[evaluationKey]?.features ?? {};
  const comparisons = isEventDate
    ? [
        readFiniteNumber(leftFeatures.recentTwoNetTotal, 0) -
          readFiniteNumber(rightFeatures.recentTwoNetTotal, 0),
        readFiniteNumber(leftFeatures.recentTwoAngle, 0) -
          readFiniteNumber(rightFeatures.recentTwoAngle, 0),
      ]
    : [
        readFiniteNumber(leftFeatures.recentTwoAngle, 0) -
          readFiniteNumber(rightFeatures.recentTwoAngle, 0),
        readFiniteNumber(leftFeatures.recentFiveNetTotal, 0) -
          readFiniteNumber(rightFeatures.recentFiveNetTotal, 0),
        readFiniteNumber(rightFeatures.recentSevenRbDenominator, 0) -
          readFiniteNumber(leftFeatures.recentSevenRbDenominator, 0),
      ];

  for (const difference of comparisons) {
    if (Math.abs(difference) > EPSILON) {
      return difference;
    }
  }

  return (
    readFiniteNumber(left?.[evaluationKey]?.rank, Number.MAX_SAFE_INTEGER) -
      readFiniteNumber(right?.[evaluationKey]?.rank, Number.MAX_SAFE_INTEGER) ||
    compareSlotNumbers(left, right)
  );
}

function buildGenericCommonRankByRow(rows) {
  return new Map(
    [...rows]
      .sort((left, right) => {
        const scoreDifference =
          readFiniteNumber(right?.huntScore, Number.NEGATIVE_INFINITY) -
          readFiniteNumber(left?.huntScore, Number.NEGATIVE_INFINITY);
        return Math.abs(scoreDifference) > EPSILON
          ? scoreDifference
          : readFiniteNumber(left?.rank, Number.MAX_SAFE_INTEGER) -
              readFiniteNumber(right?.rank, Number.MAX_SAFE_INTEGER) || compareSlotNumbers(left, right);
      })
      .map((row, index) => [row, index + 1]),
  );
}

function buildBeamHikariNeoSimpleCommonRankByRow(rows, machineRankByRow) {
  return new Map(
    rows
      .filter((row) => {
        const features = row?.storeCommonEvaluation?.features ?? {};
        return (
          row?.storeCommonEvaluation?.logicKey === BEAM_HIKARI_STORE_COMMON_SIMPLE_LOGIC_KEY &&
          Number.isFinite(readFiniteNumber(features.prev2Bonus))
        );
      })
      .sort(
        (left, right) =>
          readFiniteNumber(left?.storeCommonEvaluation?.features?.prev2Bonus) -
            readFiniteNumber(right?.storeCommonEvaluation?.features?.prev2Bonus) ||
          readFiniteNumber(machineRankByRow.get(left), BEAM_HIKARI_NEO_MACHINE_COUNT) -
            readFiniteNumber(machineRankByRow.get(right), BEAM_HIKARI_NEO_MACHINE_COUNT) ||
          compareSlotNumbers(left, right),
      )
      .map((row, index) => [row, index + 1]),
  );
}

function readBeamHikariNeoCommonRank(row, genericRankByRow, simpleCommonRankByRow) {
  const commonEvaluation = row?.storeCommonEvaluation;
  const commonFeatures = commonEvaluation?.features ?? {};
  if (commonEvaluation?.logicKey === BEAM_HIKARI_STORE_COMMON_SIMPLE_LOGIC_KEY) {
    if (readFiniteNumber(commonFeatures.prev2Diff, 0) >= 0) {
      return null;
    }
    return readFiniteNumber(simpleCommonRankByRow.get(row));
  }

  return readFiniteNumber(genericRankByRow.get(row));
}

function buildBeamHikariNeoOriginalMachineRankByRow(rows, evaluationKey, isEventDate) {
  return new Map(
    [...rows]
      .sort((left, right) => {
        const leftEvaluation = left?.[evaluationKey];
        const rightEvaluation = right?.[evaluationKey];
        const leftFeatures = leftEvaluation?.features ?? {};
        const rightFeatures = rightEvaluation?.features ?? {};
        const scoreRawKey = isEventDate
          ? "beamHikariNeoEventScoreRaw"
          : "beamHikariNeoNormalScoreRaw";
        const boostCountKey = isEventDate
          ? "beamHikariNeoEventBoostCount"
          : "beamHikariNeoNormalBoostCount";
        const dangerCountKey = isEventDate
          ? "beamHikariNeoEventDangerCount"
          : "beamHikariNeoNormalDangerCount";
        return (
          readFiniteNumber(
            rightFeatures[scoreRawKey],
            rightFeatures.beamHikariNeoOriginalScore ?? rightEvaluation?.score ?? 0,
          ) -
            readFiniteNumber(
              leftFeatures[scoreRawKey],
              leftFeatures.beamHikariNeoOriginalScore ?? leftEvaluation?.score ?? 0,
            ) ||
          readFiniteNumber(rightFeatures[boostCountKey], 0) -
            readFiniteNumber(leftFeatures[boostCountKey], 0) ||
          readFiniteNumber(leftFeatures[dangerCountKey], 0) -
            readFiniteNumber(rightFeatures[dangerCountKey], 0) ||
          readFiniteNumber(leftFeatures.recentTwoAngle, Number.POSITIVE_INFINITY) -
            readFiniteNumber(rightFeatures.recentTwoAngle, Number.POSITIVE_INFINITY) ||
          compareSlotNumbers(left, right)
        );
      })
      .map((row, index) => [row, index + 1]),
  );
}

function buildBeamHikariNeoBaseCandidates(rows, evaluationKey, isEventDate) {
  const genericCommonRankByRow = buildGenericCommonRankByRow(rows);
  const originalMachineRankByRow = buildBeamHikariNeoOriginalMachineRankByRow(
    rows,
    evaluationKey,
    isEventDate,
  );
  const simpleCommonRankByRow = buildBeamHikariNeoSimpleCommonRankByRow(
    rows,
    originalMachineRankByRow,
  );
  return rows
    .map((row) => {
      const machineRank = readFiniteNumber(
        originalMachineRankByRow.get(row),
        BEAM_HIKARI_NEO_MACHINE_COUNT,
      );
      const commonRank = readBeamHikariNeoCommonRank(
        row,
        genericCommonRankByRow,
        simpleCommonRankByRow,
      );
      const machineValue = Math.max(
        0,
        (BEAM_HIKARI_NEO_MACHINE_COUNT + 1 - machineRank) / BEAM_HIKARI_NEO_MACHINE_COUNT,
      );
      const commonValue = Number.isFinite(commonRank)
        ? Math.max(
            0,
            (BEAM_HIKARI_NEO_MACHINE_COUNT + 1 - commonRank) / BEAM_HIKARI_NEO_MACHINE_COUNT,
          )
        : 0;
      return {
        row,
        machineRank,
        commonRank,
        machineValue,
        commonValue,
        baseValue: Math.max(machineValue, commonValue) + 0.2 * Math.min(machineValue, commonValue),
        support: 0,
      };
    })
    .sort((left, right) => {
      const baseDifference = right.baseValue - left.baseValue;
      if (Math.abs(baseDifference) > EPSILON) {
        return baseDifference;
      }
      const machineRankDifference = left.machineRank - right.machineRank;
      if (machineRankDifference !== 0) {
        return machineRankDifference;
      }
      const commonRankDifference =
        readFiniteNumber(left.commonRank, Number.MAX_SAFE_INTEGER) -
        readFiniteNumber(right.commonRank, Number.MAX_SAFE_INTEGER);
      return commonRankDifference || compareSlotNumbers(left.row, right.row);
    });
}

function buildCombinations(items, size) {
  const combinations = [];
  const selected = [];

  function visit(startIndex) {
    if (selected.length === size) {
      combinations.push([...selected]);
      return;
    }

    const remainingCount = size - selected.length;
    for (let index = startIndex; index <= items.length - remainingCount; index += 1) {
      selected.push(items[index]);
      visit(index + 1);
      selected.pop();
    }
  }

  visit(0);
  return combinations;
}

function calculateBeamHikariNeoPlacementPenalty(combination, distancePenalties) {
  let penalty = 0;
  for (let leftIndex = 0; leftIndex < combination.length; leftIndex += 1) {
    const leftPosition = BEAM_HIKARI_NEO_SLOT_POSITIONS.get(
      String(combination[leftIndex]?.row?.slotNumber ?? "").trim(),
    );
    if (!leftPosition) {
      continue;
    }
    for (let rightIndex = leftIndex + 1; rightIndex < combination.length; rightIndex += 1) {
      const rightPosition = BEAM_HIKARI_NEO_SLOT_POSITIONS.get(
        String(combination[rightIndex]?.row?.slotNumber ?? "").trim(),
      );
      if (!rightPosition || leftPosition.rowIndex !== rightPosition.rowIndex) {
        continue;
      }
      const distance = Math.abs(leftPosition.positionIndex - rightPosition.positionIndex);
      penalty += distancePenalties.get(distance) ?? 0;
    }
  }
  return penalty;
}

function buildBeamHikariNeoSpatialResult(rows, evaluationKey, isEventDate) {
  const allCandidates = buildBeamHikariNeoBaseCandidates(rows, evaluationKey, isEventDate);
  const candidates = allCandidates.slice(0, BEAM_HIKARI_NEO_SPATIAL_CANDIDATE_COUNT);
  const placementSize = Math.min(
    isEventDate ? BEAM_HIKARI_NEO_EVENT_PLACEMENT_SIZE : BEAM_HIKARI_NEO_NORMAL_PLACEMENT_SIZE,
    candidates.length,
  );
  const distancePenalties = isEventDate
    ? BEAM_HIKARI_NEO_EVENT_DISTANCE_PENALTIES
    : BEAM_HIKARI_NEO_NORMAL_DISTANCE_PENALTIES;
  let totalWeight = 0;

  if (placementSize > 0) {
    for (const combination of buildCombinations(candidates, placementSize)) {
      const placementValue =
        combination.reduce((total, candidate) => total + candidate.baseValue, 0) -
        calculateBeamHikariNeoPlacementPenalty(combination, distancePenalties);
      const weight = Math.exp(placementValue);
      totalWeight += weight;
      for (const candidate of combination) {
        candidate.support += weight;
      }
    }
  }

  for (const candidate of candidates) {
    candidate.support = totalWeight > 0 ? candidate.support / totalWeight : 0;
  }

  const rankedCandidates = [...allCandidates].sort((left, right) => {
    const supportDifference = right.support - left.support;
    if (Math.abs(supportDifference) > EPSILON) {
      return supportDifference;
    }
    const baseDifference = right.baseValue - left.baseValue;
    return Math.abs(baseDifference) > EPSILON
      ? baseDifference
      : compareSlotNumbers(left.row, right.row);
  });
  const topSupport = rankedCandidates[0]?.support ?? 0;
  const secondSupport = rankedCandidates[1]?.support ?? 0;
  const supportRatio = secondSupport > 0 ? topSupport / secondSupport : topSupport > 0 ? Number.POSITIVE_INFINITY : 0;

  return {
    candidates,
    rankedCandidates,
    placementSize,
    supportRatio,
    selectedRow:
      supportRatio + EPSILON >= BEAM_HIKARI_NEO_SPATIAL_SUPPORT_RATIO_MIN
        ? rankedCandidates[0]?.row ?? null
        : null,
  };
}

function buildFinalConditionSummary(method) {
  return {
    conditionKey: BEAM_HIKARI_NEO_FINAL_CONDITION_KEY,
    conditionName: method === "rb-gate" ? "RB条件を最優先" : "全体配置の支持率比1.05以上",
    ...BEAM_HIKARI_NEO_FINAL_BACKTEST,
    isSelected: true,
  };
}

function decorateBeamHikariNeoRows(rows, evaluationKey, selection) {
  const selectedRow = selection.selectedRow;
  const selectedMethod = selection.method;
  const gateCandidateSet = new Set(selection.gateRows ?? []);
  const spatialCandidateSet = new Set(selection.spatialResult?.candidates.map((candidate) => candidate.row) ?? []);
  const spatialCandidateByRow = new Map(
    (selection.spatialResult?.rankedCandidates ?? []).map((candidate, index) => [
      candidate.row,
      { ...candidate, finalRank: index + 1 },
    ]),
  );
  const gateRankByRow = new Map((selection.gateRows ?? []).map((row, index) => [row, index + 1]));

  return rows.map((row) => {
    const evaluation = row?.[evaluationKey];
    if (!isBeamHikariNeoEvaluation(evaluation)) {
      return row;
    }

    const spatialCandidate = spatialCandidateByRow.get(row) ?? null;
    const finalRank = selectedMethod === "rb-gate"
      ? gateRankByRow.get(row) ?? (selection.gateRows?.length ?? 0) + (spatialCandidate?.finalRank ?? BEAM_HIKARI_NEO_MACHINE_COUNT)
      : spatialCandidate?.finalRank ?? BEAM_HIKARI_NEO_MACHINE_COUNT;
    const selected = row === selectedRow;
    const supportRatio = selection.spatialResult?.supportRatio ?? null;
    const status = selected
      ? selectedMethod === "rb-gate"
        ? "RB条件で採用"
        : "配置支持で採用"
      : selectedMethod === "skip"
        ? "支持率差不足で見送り"
        : selectedMethod === "rb-gate"
          ? "RB条件の次点"
          : "配置支持の次点";
    const finalSelection = {
      selectorKey: BEAM_HIKARI_NEO_FINAL_SELECTOR_KEY,
      enabled: true,
      selected,
      method: selectedMethod,
      status,
      isEventDate: selection.isEventDate,
      finalRank,
      rbGateCandidate: gateCandidateSet.has(row),
      spatialCandidate: spatialCandidateSet.has(row),
      placementSize: selection.spatialResult?.placementSize ?? null,
      support: spatialCandidate?.support ?? null,
      supportRatio,
      supportRatioMinimum: BEAM_HIKARI_NEO_SPATIAL_SUPPORT_RATIO_MIN,
      baseValue: spatialCandidate?.baseValue ?? null,
      machineRank: spatialCandidate?.machineRank ?? evaluation.rank ?? null,
      commonRank: spatialCandidate?.commonRank ?? null,
    };
    const matchedConditions = Array.isArray(evaluation.matchedConditions)
      ? evaluation.matchedConditions.filter(
          (condition) => condition?.conditionKey !== BEAM_HIKARI_NEO_FINAL_CONDITION_KEY,
        )
      : [];
    if (selected) {
      matchedConditions.unshift(buildFinalConditionSummary(selectedMethod));
    }

    return {
      ...row,
      [evaluationKey]: {
        ...evaluation,
        matchesAdoption: selected,
        matchedConditions,
        matchesAnyCondition: matchedConditions.length > 0,
        bestMatchedBacktestPayoutRate: selected
          ? BEAM_HIKARI_NEO_FINAL_BACKTEST.backtestPayoutRate
          : evaluation.bestMatchedBacktestPayoutRate ?? null,
        finalSelection,
        features: {
          ...evaluation.features,
          beamHikariNeoFinalSelectionEnabled: true,
          beamHikariNeoFinalSelected: selected,
          beamHikariNeoRbGateCandidate: gateCandidateSet.has(row),
          beamHikariNeoSpatialCandidate: spatialCandidateSet.has(row),
          beamHikariNeoSpatialSupport: spatialCandidate?.support ?? null,
          beamHikariNeoSpatialSupportRatio: supportRatio,
          beamHikariNeoFinalSelectionRank: finalRank,
        },
      },
    };
  });
}

export function applyBeamHikariNeoFinalSelection(snapshot, options = {}, evaluationKey = "machineEvaluation") {
  const rows = Array.isArray(snapshot?.rows) ? snapshot.rows : [];
  if (!isBeamHikariStore(options?.storeName) || rows.length === 0) {
    return snapshot;
  }

  const targetRows = rows.filter((row) => isBeamHikariNeoEvaluation(row?.[evaluationKey]));
  if (targetRows.length === 0) {
    return snapshot;
  }

  const firstEvaluation = targetRows[0]?.[evaluationKey];
  const isEventDate = resolveBeamHikariNeoEventMode(firstEvaluation, snapshot);
  const gateRows = targetRows
    .filter((row) => isBeamHikariNeoRbGateCandidate(row, evaluationKey, isEventDate))
    .sort((left, right) => compareBeamHikariNeoRbGateRows(left, right, evaluationKey, isEventDate));
  let spatialResult = null;
  let selectedRow = gateRows[0] ?? null;
  let method = selectedRow ? "rb-gate" : "skip";

  if (!selectedRow) {
    spatialResult = buildBeamHikariNeoSpatialResult(targetRows, evaluationKey, isEventDate);
    selectedRow = spatialResult.selectedRow;
    method = selectedRow ? "spatial" : "skip";
  }

  return {
    ...snapshot,
    rows: decorateBeamHikariNeoRows(rows, evaluationKey, {
      gateRows,
      isEventDate,
      method,
      selectedRow,
      spatialResult,
    }),
  };
}

const FINAL_SELECTION_HANDLERS = [applyBeamHikariNeoFinalSelection];

export function applyMachineEvaluationFinalSelection(
  snapshot,
  options = {},
  evaluationKey = "machineEvaluation",
) {
  return FINAL_SELECTION_HANDLERS.reduce(
    (currentSnapshot, handler) => handler(currentSnapshot, options, evaluationKey),
    snapshot,
  );
}
