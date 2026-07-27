import assert from "node:assert/strict";
import test from "node:test";

import {
  readEffectiveBonusCounts,
  withEffectiveBonusCounts,
} from "../lib/bonus-counts.js";
import { buildHuntScoreBacktestDetail } from "../lib/hunt-backtest.js";
import { buildHuntScoreSnapshots } from "../lib/hunt-score.js";

test("ATを指定表示枠だけへ加算し、生のBB・RB・ATを分離して保持する", () => {
  const record = withEffectiveBonusCounts({
    games_count: 1000,
    bb_count: 2,
    rb_count: 3,
    combined_ratio_text: "1/200",
    bb_ratio_text: "1/500",
    rb_ratio_text: "1/333",
    at_count: 5,
    at_display_slot: "bb",
    at_source: "daidata_online",
    at_fetched_at: "2026-07-27T12:34:56+09:00",
  });

  assert.equal(record.raw_bb_count, 2);
  assert.equal(record.raw_rb_count, 3);
  assert.equal(record.at_count, 5);
  assert.equal(record.bb_count, 7);
  assert.equal(record.rb_count, 3);
  assert.equal(record.effective_bb_count, 7);
  assert.equal(record.effective_rb_count, 3);
  assert.equal(record.combined_ratio_text, "1/100");
  assert.equal(record.bb_ratio_text, "1/143");
  assert.equal(record.rb_ratio_text, "1/333");
  assert.equal(record.at_source, "daidata_online");
  assert.equal(record.at_fetched_at, "2026-07-27T12:34:56+09:00");
});

test("AT欠損とignore・unknownは加算せず、繰り返し正規化しても二重加算しない", () => {
  const missingAt = withEffectiveBonusCounts({
    games_count: 1000,
    bb_count: 4,
    rb_count: 6,
    combined_ratio_text: "1/100",
    at_display_slot: "bb",
  });
  assert.equal(missingAt.at_count, null);
  assert.equal(missingAt.bb_count, 4);
  assert.equal(missingAt.combined_ratio_text, "1/100");

  for (const atDisplaySlot of ["ignore", "unknown"]) {
    const counts = readEffectiveBonusCounts({
      bb_count: 4,
      rb_count: 6,
      at_count: 20,
      at_display_slot: atDisplaySlot,
    });
    assert.equal(counts.bbCount, 4);
    assert.equal(counts.rbCount, 6);
  }

  const once = withEffectiveBonusCounts({
    bb_count: 2,
    rb_count: 3,
    at_count: 5,
    at_display_slot: "rb",
  });
  const twice = withEffectiveBonusCounts(once);
  assert.equal(twice.raw_bb_count, 2);
  assert.equal(twice.raw_rb_count, 3);
  assert.equal(twice.bb_count, 2);
  assert.equal(twice.rb_count, 8);
});

test("ビームヒカリ1.0Sの直近2日ボーナス合計へATを反映する", () => {
  const machineName = "スマスロ ミリオンゴッド";
  const rows = [
    {
      machine_name: machineName,
      target_date: "2026-07-25",
      slot_number: "537",
      difference_value: -500,
      games_count: 1800,
      bb_count: 2,
      rb_count: 0,
      at_count: 10,
      at_display_slot: "rb",
    },
    {
      machine_name: machineName,
      target_date: "2026-07-26",
      slot_number: "537",
      difference_value: -200,
      games_count: 2200,
      bb_count: 3,
      rb_count: 0,
      at_count: 20,
      at_display_slot: "rb",
    },
    {
      machine_name: machineName,
      target_date: "2026-07-25",
      slot_number: "538",
      difference_value: 100,
      games_count: 1800,
      bb_count: 5,
      rb_count: 0,
      at_count: 30,
      at_display_slot: "ignore",
    },
    {
      machine_name: machineName,
      target_date: "2026-07-26",
      slot_number: "538",
      difference_value: 200,
      games_count: 2200,
      bb_count: 6,
      rb_count: 0,
      at_count: 40,
      at_display_slot: "unknown",
    },
  ];

  const snapshot = buildHuntScoreSnapshots(
    rows,
    rows,
    "ビームヒカリ店",
    "beam-hikari-store-common-v1s",
  ).find((candidate) => candidate.baseDate === "2026-07-26");
  const mappedRow = snapshot?.rows.find((row) => row.slotNumber === "537");
  const ignoredRow = snapshot?.rows.find((row) => row.slotNumber === "538");

  assert.equal(mappedRow?.machineEvaluationMetrics?.recentTwoBonusTotal, 35);
  assert.equal(mappedRow?.currentRecord?.raw_bb_count, 3);
  assert.equal(mappedRow?.currentRecord?.raw_rb_count, 0);
  assert.equal(mappedRow?.currentRecord?.rb_count, 20);
  assert.equal(ignoredRow?.machineEvaluationMetrics?.recentTwoBonusTotal, 11);
});

test("バックテストの翌日実績BB・RBと確率へATを反映する", () => {
  const machineName = "スマスロ ミリオンゴッド";
  const result = buildHuntScoreBacktestDetail(
    [
      {
        baseDate: "2026-07-25",
        nextBusinessDate: "2026-07-26",
        rows: [
          {
            machineName,
            slotNumber: "537",
            huntScore: 80,
            rank: 1,
            nextRecord: {
              target_date: "2026-07-26",
              machine_name: machineName,
              slot_number: "537",
              difference_value: 300,
              games_count: 1000,
              bb_count: 2,
              rb_count: 0,
              at_count: 8,
              at_display_slot: "rb",
            },
          },
        ],
      },
    ],
    {
      machineNames: [machineName],
      machineTouched: true,
      periodMode: "range",
      startDate: "2026-07-25",
      endDate: "2026-07-25",
      rankRequired: false,
      machineRankRequired: false,
      selectedRankRequired: false,
      scoreRequired: false,
      nextGapRequired: false,
      upperGapRequired: false,
      machineNextGapRequired: false,
      selectedNextGapRequired: false,
      machineUpperGapRequired: false,
      selectedUpperGapRequired: false,
    },
  );

  assert.equal(result.total.actualRowCount, 1);
  assert.equal(result.total.bbTotal, 2);
  assert.equal(result.total.rbTotal, 8);
  assert.equal(result.total.bbProbability, "1/500");
  assert.equal(result.total.rbProbability, "1/125");
  assert.equal(result.total.combinedProbability, "1/100");
});
