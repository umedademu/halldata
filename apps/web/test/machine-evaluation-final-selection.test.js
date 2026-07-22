import assert from "node:assert/strict";
import test from "node:test";

import {
  applyBeamHikariNeoFinalSelection,
  applyMachineEvaluationFinalSelectionRankingOrder,
} from "../lib/machine-evaluation-final-selection.js";

const LOGIC_KEY = "beam-hikari-neo-aim";

function buildRow(slotNumber, rank, featureOverrides = {}, rowOverrides = {}) {
  return {
    rowKey: `ネオアイムジャグラーEX::${slotNumber}`,
    machineName: "ネオアイムジャグラーEX",
    slotNumber: String(slotNumber),
    huntScore: 100 - rank,
    rank,
    machineEvaluation: {
      logicKey: LOGIC_KEY,
      logicName: "ネオアイムビームヒカリ式（RB優先＋全体配置）",
      score: 100 - rank,
      rank,
      matchesAdoption: false,
      matchedConditions: [],
      features: {
        beamHikariNeoHistoryReady: true,
        recentTwoNetTotal: -500,
        recentTwoAngle: -100,
        recentFiveNetTotal: -500,
        recentSevenCombinedDenominator: 150,
        recentSevenRbDenominator: 350,
        ...featureOverrides,
      },
    },
    ...rowOverrides,
  };
}

function applySelection(rows, nextBusinessDate = "2026-06-16") {
  return applyBeamHikariNeoFinalSelection(
    {
      baseDate: "2026-06-15",
      nextBusinessDate,
      rows,
    },
    {
      storeName: "ビームヒカリ",
      beamHikariNeoSpatialSelectionEnabled: true,
    },
  );
}

test("空間選定が未指定なら計算せず元のスナップショットを返す", () => {
  const snapshot = {
    baseDate: "2026-06-15",
    nextBusinessDate: "2026-06-17",
    rows: [buildRow("871", 1), buildRow("872", 2)],
  };

  const result = applyBeamHikariNeoFinalSelection(snapshot, {
    storeName: "ビームヒカリ",
  });

  assert.equal(result, snapshot);
  assert.equal(result.rows[0].machineEvaluation.finalSelection, undefined);
});

test("イベント日のRB条件は配置支持より優先される", () => {
  const rows = [
    buildRow("871", 1),
    buildRow("872", 2, {
      recentTwoNetTotal: -2500,
      recentTwoAngle: -600,
      recentSevenCombinedDenominator: 170,
    }),
    buildRow("1020", 3, {
      recentTwoNetTotal: -2300,
      recentTwoAngle: -700,
      recentSevenRbDenominator: 450,
    }),
  ];

  const result = applySelection(rows);
  const selectedRows = result.rows.filter((row) => row.machineEvaluation.matchesAdoption);

  assert.equal(selectedRows.length, 1);
  assert.equal(selectedRows[0].slotNumber, "872");
  assert.equal(selectedRows[0].machineEvaluation.finalSelection.method, "rb-gate");
  assert.equal(selectedRows[0].machineEvaluation.finalSelection.rbGateCandidate, true);
});

test("通常日のRB条件は角度、5日差枚、7日RBの全条件を必要とする", () => {
  const rows = [
    buildRow("871", 1, {
      recentTwoAngle: -600,
      recentFiveNetTotal: -2500,
      recentSevenRbDenominator: 450,
    }),
    buildRow("872", 2, {
      recentTwoAngle: -700,
      recentFiveNetTotal: -1500,
      recentSevenRbDenominator: 500,
    }),
  ];

  const result = applySelection(rows, "2026-06-17");
  const selectedRows = result.rows.filter((row) => row.machineEvaluation.matchesAdoption);

  assert.equal(selectedRows.length, 1);
  assert.equal(selectedRows[0].slotNumber, "871");
  assert.equal(selectedRows[0].machineEvaluation.finalSelection.method, "rb-gate");
});

test("RB条件がない日は店舗共通ロジックの種類に依存せず配置支持を計算する", () => {
  const slots = [
    "871", "872", "873", "875", "876", "877", "878",
    "1030", "1028", "1027", "1026", "1025", "1023", "1022",
  ];
  const rows = slots.map((slotNumber, index) => buildRow(slotNumber, index + 1));

  const result = applySelection(rows, "2026-06-17");
  const finalSelections = result.rows.map((row) => row.machineEvaluation.finalSelection);

  assert.equal(finalSelections.every((selection) => selection.enabled), true);
  assert.equal(finalSelections.every((selection) => Number.isFinite(selection.commonRank)), true);
  assert.equal(
    finalSelections.some((selection) => Number.isFinite(selection.support)),
    true,
  );
});

test("配置支持率の1位と2位に差がなければ見送る", () => {
  const rows = Array.from({ length: 14 }, (_, index) =>
    buildRow(`x${index + 1}`, 1, {}, {
      huntScore: 50,
      storeCommonEvaluation: {
        logicKey: "beam-hikari-store-common-v1s",
        features: {
          prev2Diff: -100,
          prev2BonusRankInMachine: 1,
        },
      },
    }),
  );

  const result = applySelection(rows, "2026-06-17");
  const selectedRows = result.rows.filter((row) => row.machineEvaluation.matchesAdoption);

  assert.equal(selectedRows.length, 0);
  assert.equal(result.rows[0].machineEvaluation.finalSelection.method, "skip");
  assert.equal(result.rows[0].machineEvaluation.finalSelection.supportRatio < 1.05, true);
});

test("ビームヒカリ以外の店舗には適用しない", () => {
  const snapshot = {
    baseDate: "2026-06-15",
    nextBusinessDate: "2026-06-16",
    rows: [buildRow("871", 1)],
  };
  const result = applyBeamHikariNeoFinalSelection(snapshot, {
    storeName: "別店舗",
    beamHikariNeoSpatialSelectionEnabled: true,
  });

  assert.equal(result, snapshot);
  assert.equal(result.rows[0].machineEvaluation.finalSelection, undefined);
});

test("空間選定が有効なら最終順位を機種内の並び順へ反映する", () => {
  const otherMachineRow = {
    rowKey: "別機種::1",
    machineName: "別機種",
    slotNumber: "1",
  };
  const first = buildRow("871", 1);
  const second = buildRow("872", 2);
  first.machineEvaluation.finalSelection = {
    enabled: true,
    selectorKey: "beam-hikari-neo-spatial-integrated-v1",
    finalRank: 2,
  };
  second.machineEvaluation.finalSelection = {
    enabled: true,
    selectorKey: "beam-hikari-neo-spatial-integrated-v1",
    finalRank: 1,
  };
  const snapshot = { rows: [first, otherMachineRow, second] };

  const disabledResult = applyMachineEvaluationFinalSelectionRankingOrder(snapshot);
  const enabledResult = applyMachineEvaluationFinalSelectionRankingOrder(snapshot, {
    beamHikariNeoSpatialSelectionEnabled: true,
  });

  assert.equal(disabledResult, snapshot);
  assert.deepEqual(
    enabledResult.rows.map((row) => row.rowKey),
    [second.rowKey, otherMachineRow.rowKey, first.rowKey],
  );
});
