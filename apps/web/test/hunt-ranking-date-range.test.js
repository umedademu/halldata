import assert from "node:assert/strict";
import test from "node:test";

import {
  buildHuntRankingDateLoadRange,
  buildHuntRankingSnapshotDateRange,
} from "../lib/hunt-ranking-date-range.js";

test("前回選択日が古くても日付候補の読み込みは最新保存日まで含める", () => {
  assert.deepEqual(
    buildHuntRankingDateLoadRange({
      requestedDate: "2026-07-04",
      latestDate: "2026-07-24",
      historyWindowDays: 60,
      nextResultBufferDays: 7,
    }),
    {
      startDate: "2026-05-05",
      endDate: "2026-07-31",
    },
  );
});

test("ランキング計算用の範囲は選択日を基準にした従来範囲を保つ", () => {
  assert.deepEqual(
    buildHuntRankingSnapshotDateRange({
      selectedDate: "2026-07-04",
      historyWindowDays: 60,
      nextResultBufferDays: 7,
    }),
    {
      startDate: "2026-05-05",
      endDate: "2026-07-11",
    },
  );
});

test("指定日がない場合は最新保存日を基準にする", () => {
  assert.deepEqual(
    buildHuntRankingDateLoadRange({
      latestDate: "2026-07-24",
      historyWindowDays: 60,
      nextResultBufferDays: 7,
    }),
    {
      startDate: "2026-05-25",
      endDate: "2026-07-31",
    },
  );
});
