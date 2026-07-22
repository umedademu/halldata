import assert from "node:assert/strict";
import test from "node:test";

import {
  SETTING_DISTRIBUTION_HIDE,
  SETTING_DISTRIBUTION_SHOW,
  normalizeSettingDistribution,
  shouldShowSettingDistribution,
} from "../lib/setting-distribution.js";

test("設定分布は未指定なら非表示になる", () => {
  assert.equal(normalizeSettingDistribution(undefined), SETTING_DISTRIBUTION_HIDE);
  assert.equal(shouldShowSettingDistribution(undefined), false);
});

test("設定分布は表示を明示した場合だけ表示する", () => {
  assert.equal(normalizeSettingDistribution("show"), SETTING_DISTRIBUTION_SHOW);
  assert.equal(shouldShowSettingDistribution("show"), true);
});
