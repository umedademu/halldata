import settingEstimatesPayload from "../config/setting_estimates.json" with { type: "json" };

export const SETTING_ESTIMATE_VALUE_VERSION = 5;
export const SETTING_ESTIMATE_MODE_BONUS = "bonus";
export const SETTING_ESTIMATE_MODE_GRAPE = "grape";
export const DEFAULT_SETTING_ESTIMATE_MODE = SETTING_ESTIMATE_MODE_BONUS;
export const SETTING_ESTIMATE_MODE_OPTIONS = [
  { value: SETTING_ESTIMATE_MODE_BONUS, label: "ボーナス確率のみ" },
  { value: SETTING_ESTIMATE_MODE_GRAPE, label: "ブドウ確率を加味" },
];
const PREVIOUS_SETTING_ESTIMATE_VALUE_VERSION = 4;
const UPDATED_SETTING_ESTIMATE_KEYS = new Set(["neoim-juggler-ex"]);
const GRAPE_SETTING_ESTIMATE_KEYS = new Set(["neoim-juggler-ex"]);
const AIM_REPLAY_DENOMINATOR = 7.30;
const AIM_REPLAY_PAYOUT = 3;
const AIM_GRAPE_PAYOUT = 8;
const AIM_CHERRY_PAYOUT = 2;
const AIM_BB_PAYOUT = 252;
const AIM_RB_PAYOUT = 96;
const AIM_POST_ANNOUNCEMENT_BONUS_RATIO = 0.75;
const MINREPO_ONE_BET_GAME_FACTOR = 1 / 3;
const ONE_BET_GRAPE_DENOMINATOR = 10.3;
const ONE_BET_REPLAY_DENOMINATOR = 7.3;
const ONE_BET_GRAPE_PAYOUT = 15;
const ONE_BET_REPLAY_PAYOUT = 1;

const SETTING_ESTIMATE_DEFINITIONS = Array.isArray(settingEstimatesPayload?.setting_estimates)
  ? settingEstimatesPayload.setting_estimates
  : [];
const RATE_TABLE_EXTRA_COLUMNS = [
  { field: "grapeText", label: "ブドウ確率" },
  { field: "cherryText", label: "チェリー確率" },
  { field: "payoutRateText", label: "機械割" },
];

function parseRateText(value) {
  const denominator = Number(String(value).replace("1/", ""));
  if (!Number.isFinite(denominator) || denominator <= 0) {
    return 0;
  }
  return 1 / denominator;
}

function formatDenominator(value) {
  const rounded = Math.round(value * 100) / 100;
  const text = rounded.toFixed(2).replace(/0$/u, "");
  if (text.endsWith(".")) {
    return `${text}0`;
  }
  return text;
}

function formatRateFromProbability(probability) {
  if (!Number.isFinite(probability) || probability <= 0) {
    return "-";
  }
  return `1/${formatDenominator(1 / probability)}`;
}

function formatProbabilityValue(probability) {
  const percent = probability * 100;
  if (percent >= 1) {
    return `${percent.toFixed(2)}%`;
  }
  if (percent >= 0.01) {
    return `${percent.toFixed(3)}%`;
  }
  if (percent > 0) {
    return `${percent.toFixed(5)}%`;
  }
  return "0%";
}

export function normalizeSettingEstimateMode(value) {
  return value === SETTING_ESTIMATE_MODE_GRAPE
    ? SETTING_ESTIMATE_MODE_GRAPE
    : SETTING_ESTIMATE_MODE_BONUS;
}

export function formatSettingEstimateModeLabel(value) {
  const normalizedMode = normalizeSettingEstimateMode(value);
  return (
    SETTING_ESTIMATE_MODE_OPTIONS.find((option) => option.value === normalizedMode)?.label ??
    SETTING_ESTIMATE_MODE_OPTIONS[0].label
  );
}

function normalizeMachineName(value) {
  return String(value ?? "")
    .normalize("NFKC")
    .replace(/[\s\u3000・･_-]/gu, "")
    .toUpperCase();
}

function buildDefinition(definition) {
  const settingRates = definition.settings.map((row) => {
    const bb = parseRateText(row.bbText);
    const rb = parseRateText(row.rbText);
    const grape = parseRateText(row.grapeText);
    const cherry = parseRateText(row.cherryText);
    return {
      ...row,
      bb,
      rb,
      grape,
      cherry,
      combined: bb + rb,
    };
  });
  const rateTable = settingRates.map((row) => ({
    setting: row.label,
    bb: row.bbText,
    rb: row.rbText,
    combined: formatRateFromProbability(row.combined),
    grapeText: row.grapeText,
    cherryText: row.cherryText,
    payoutRateText: row.payoutRateText,
  }));
  const rateTableExtraColumns = RATE_TABLE_EXTRA_COLUMNS.filter((column) =>
    rateTable.some((row) => row[column.field]),
  );

  return {
    ...definition,
    normalizedMatchNames: [definition.displayName, ...definition.matchNames].map(normalizeMachineName),
    settingRates,
    rateTable,
    rateTableExtraColumns,
  };
}

const settingEstimateDefinitions = SETTING_ESTIMATE_DEFINITIONS.map(buildDefinition);

export function getSettingEstimateDefinition(machineName) {
  const normalizedMachineName = normalizeMachineName(machineName);
  return (
    settingEstimateDefinitions.find((definition) =>
      definition.normalizedMatchNames.some((matchName) => normalizedMachineName.includes(matchName)),
    ) ?? null
  );
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

export function isCurrentSettingEstimateVersion(definition, version) {
  if (version === SETTING_ESTIMATE_VALUE_VERSION) {
    return true;
  }
  return (
    version === PREVIOUS_SETTING_ESTIMATE_VALUE_VERSION &&
    !UPDATED_SETTING_ESTIMATE_KEYS.has(String(definition?.key ?? ""))
  );
}

function readPrecomputedSettingEstimate(definition, record) {
  const average = readNumber(record?.setting_estimate_average);
  const version = readNumber(record?.setting_estimate_version);
  if (!Number.isFinite(average) || !isCurrentSettingEstimateVersion(definition, version)) {
    return null;
  }

  return {
    average,
    probabilities: [],
    precomputed: true,
  };
}

function normalizeGrapeObservation(grapeCount, games, source) {
  if (
    !Number.isFinite(grapeCount) ||
    !Number.isFinite(games) ||
    grapeCount <= 0 ||
    games <= 0
  ) {
    return null;
  }

  const successCount = Math.round(grapeCount);
  const totalCount = Math.round(games);
  if (!isValidCount(successCount, totalCount)) {
    return null;
  }

  return {
    successCount,
    totalCount,
    grapeCount,
    games,
    source,
  };
}

function readStoredGrapeObservation(record) {
  const grapeCount = readNumber(record?.estimated_grape_count);
  if (!Number.isFinite(grapeCount) || grapeCount <= 0) {
    return null;
  }

  const grapeProbability = readNumber(record?.estimated_grape_probability);
  if (Number.isFinite(grapeProbability) && grapeProbability > 0 && grapeProbability < 1) {
    return normalizeGrapeObservation(grapeCount, grapeCount / grapeProbability, "stored");
  }

  const grapeDenominator = readNumber(record?.estimated_grape_denominator);
  if (Number.isFinite(grapeDenominator) && grapeDenominator > 0) {
    return normalizeGrapeObservation(grapeCount, grapeCount * grapeDenominator, "stored");
  }

  return null;
}

function interpolateSettingRate(definition, settingAverage, rateKey) {
  const rows = (definition?.settingRates ?? [])
    .filter((row) => Number.isFinite(row.setting) && Number.isFinite(row[rateKey]) && row[rateKey] > 0)
    .sort((left, right) => left.setting - right.setting);
  if (rows.length === 0 || !Number.isFinite(settingAverage)) {
    return null;
  }
  if (settingAverage <= rows[0].setting) {
    return rows[0][rateKey];
  }
  const lastRow = rows.at(-1);
  if (settingAverage >= lastRow.setting) {
    return lastRow[rateKey];
  }

  for (let index = 1; index < rows.length; index += 1) {
    const upper = rows[index];
    const lower = rows[index - 1];
    if (settingAverage <= upper.setting) {
      const ratio = (settingAverage - lower.setting) / (upper.setting - lower.setting);
      return lower[rateKey] + (upper[rateKey] - lower[rateKey]) * ratio;
    }
  }

  return lastRow[rateKey];
}

function calculateAimGrapeObservation(definition, record) {
  const games = readNumber(record?.games_count);
  const bbCount = readNumber(record?.bb_count);
  const rbCount = readNumber(record?.rb_count);
  const differenceValue = readNumber(record?.difference_value);
  if (
    !Number.isInteger(games) ||
    games <= 0 ||
    !isValidCount(bbCount, games) ||
    !isValidCount(rbCount, games) ||
    !Number.isFinite(differenceValue)
  ) {
    return null;
  }

  const bonusCount = bbCount + rbCount;
  const oneBetEndProbability =
    1 - 1 / ONE_BET_GRAPE_DENOMINATOR - 1 / ONE_BET_REPLAY_DENOMINATOR;
  if (oneBetEndProbability <= 0) {
    return null;
  }

  const provisionalEstimate = calculateBonusSettingEstimate(definition, record, {
    usePrecomputed: true,
  });
  const cherryProbability = interpolateSettingRate(definition, provisionalEstimate?.average, "cherry");
  if (!Number.isFinite(cherryProbability) || cherryProbability <= 0) {
    return null;
  }

  const oneBetGames =
    (bonusCount * AIM_POST_ANNOUNCEMENT_BONUS_RATIO) / oneBetEndProbability;
  const minrepoOneBetGames = oneBetGames * MINREPO_ONE_BET_GAME_FACTOR;
  const normalGames = games - minrepoOneBetGames;
  if (!Number.isFinite(normalGames) || normalGames <= 0) {
    return null;
  }

  const totalInvestment = games * 3;
  const totalBonusPayout = bbCount * AIM_BB_PAYOUT + rbCount * AIM_RB_PAYOUT;
  const totalSmallPayout = differenceValue + totalInvestment - totalBonusPayout;
  const replayPayout = (normalGames / AIM_REPLAY_DENOMINATOR) * AIM_REPLAY_PAYOUT;
  const cherryPayout = normalGames * cherryProbability * AIM_CHERRY_PAYOUT;
  const oneBetGrapePayout =
    (oneBetGames / ONE_BET_GRAPE_DENOMINATOR) * ONE_BET_GRAPE_PAYOUT;
  const oneBetReplayPayout =
    (oneBetGames / ONE_BET_REPLAY_DENOMINATOR) * ONE_BET_REPLAY_PAYOUT;
  const grapePayout =
    totalSmallPayout - replayPayout - cherryPayout - oneBetGrapePayout - oneBetReplayPayout;
  const grapeCount = grapePayout / AIM_GRAPE_PAYOUT;

  return normalizeGrapeObservation(grapeCount, normalGames, "calculated");
}

export function readGrapeSettingEstimateObservation(definition, record) {
  if (!GRAPE_SETTING_ESTIMATE_KEYS.has(String(definition?.key ?? ""))) {
    return null;
  }
  if (!(definition?.settingRates ?? []).some((row) => Number.isFinite(row.grape) && row.grape > 0)) {
    return null;
  }

  return readStoredGrapeObservation(record) ?? calculateAimGrapeObservation(definition, record);
}

export function getSettingEstimateScoreRange(definition) {
  const settings = definition?.settingRates
    ?.map((row) => row.setting)
    .filter((value) => Number.isFinite(value));

  if (!settings || settings.length === 0) {
    return {
      minSetting: 1,
      maxSetting: 6,
    };
  }

  return {
    minSetting: Math.min(...settings),
    maxSetting: Math.max(...settings),
  };
}

export function calculateGameCountEstimate(definition, record, options = {}) {
  const games = readNumber(record?.games_count);
  if (!definition || !Number.isFinite(games) || games < 0) {
    return null;
  }

  const rawMinGames = readNumber(options.minGames) ?? 6000;
  const rawMaxGames = readNumber(options.maxGames) ?? 9000;
  const minGames = Math.max(0, rawMinGames);
  const maxGames = Math.max(minGames + 1, rawMaxGames);
  const exponent = Math.max(0.1, readNumber(options.gameExponent ?? options.exponent) ?? 1.5);
  const { minSetting, maxSetting } = getSettingEstimateScoreRange(definition);
  const progress = clamp((games - minGames) / (maxGames - minGames), 0, 1);
  const curvedProgress = Math.pow(progress, exponent);

  return {
    average: minSetting + (maxSetting - minSetting) * curvedProgress,
    games,
    progress,
    minGames,
    maxGames,
    exponent,
  };
}

function isValidCount(value, base) {
  return Number.isInteger(value) && value >= 0 && value <= base;
}

function calculateLogBinomialProbability(successCount, totalCount, probability) {
  if (
    totalCount < 0 ||
    successCount < 0 ||
    successCount > totalCount ||
    probability < 0 ||
    probability > 1
  ) {
    return Number.NEGATIVE_INFINITY;
  }

  if (totalCount === 0) {
    return successCount === 0 ? 0 : Number.NEGATIVE_INFINITY;
  }

  if (probability === 0) {
    return successCount === 0 ? 0 : Number.NEGATIVE_INFINITY;
  }

  if (probability === 1) {
    return successCount === totalCount ? 0 : Number.NEGATIVE_INFINITY;
  }

  const smallerSide = Math.min(successCount, totalCount - successCount);
  let logCombination = 0;

  for (let count = 1; count <= smallerSide; count += 1) {
    logCombination += Math.log(totalCount - smallerSide + count) - Math.log(count);
  }

  return (
    logCombination +
    successCount * Math.log(probability) +
    (totalCount - successCount) * Math.log(1 - probability)
  );
}

function calculateEstimateFromLogRows(logRows, extra = {}) {
  const maxLogValue = Math.max(...logRows.map((row) => row.logValue));

  if (!Number.isFinite(maxLogValue)) {
    return null;
  }

  const weightedRows = logRows.map((row) => ({
    ...row,
    weight: Math.exp(row.logValue - maxLogValue),
  }));
  const totalWeight = weightedRows.reduce((sum, row) => sum + row.weight, 0);

  if (!Number.isFinite(totalWeight) || totalWeight <= 0) {
    return null;
  }

  const probabilities = weightedRows.map((row) => ({
    setting: row.setting,
    label: row.label,
    probability: row.weight / totalWeight,
  }));
  const average = probabilities.reduce(
    (sum, row) => sum + row.setting * row.probability,
    0,
  );

  return {
    average,
    probabilities,
    ...extra,
  };
}

function calculateBonusSettingEstimate(definition, record, options = {}) {
  if (!definition) {
    return null;
  }

  const precomputedEstimate = options.usePrecomputed === false
    ? null
    : readPrecomputedSettingEstimate(definition, record);
  if (precomputedEstimate) {
    return precomputedEstimate;
  }

  const games = readNumber(record?.games_count);
  const bbCount = readNumber(record?.bb_count);
  const rbCount = readNumber(record?.rb_count);

  if (
    !Number.isInteger(games) ||
    games <= 0 ||
    !isValidCount(bbCount, games) ||
    !isValidCount(rbCount, games)
  ) {
    return null;
  }

  const logRows = definition.settingRates.map((row) => ({
    setting: row.setting,
    label: row.label,
    logValue:
      calculateLogBinomialProbability(bbCount, games, row.bb) +
      calculateLogBinomialProbability(rbCount, games, row.rb),
  }));

  return calculateEstimateFromLogRows(logRows, {
    mode: SETTING_ESTIMATE_MODE_BONUS,
    sourceMode: SETTING_ESTIMATE_MODE_BONUS,
  });
}

function calculateGrapeSettingEstimate(definition, record) {
  const games = readNumber(record?.games_count);
  const bbCount = readNumber(record?.bb_count);
  const rbCount = readNumber(record?.rb_count);
  const grapeObservation = readGrapeSettingEstimateObservation(definition, record);

  if (
    !Number.isInteger(games) ||
    games <= 0 ||
    !isValidCount(bbCount, games) ||
    !isValidCount(rbCount, games) ||
    !grapeObservation
  ) {
    return null;
  }

  const logRows = definition.settingRates.map((row) => ({
    setting: row.setting,
    label: row.label,
    logValue:
      calculateLogBinomialProbability(bbCount, games, row.bb) +
      calculateLogBinomialProbability(rbCount, games, row.rb) +
      calculateLogBinomialProbability(
        grapeObservation.successCount,
        grapeObservation.totalCount,
        row.grape,
      ),
  }));

  return calculateEstimateFromLogRows(logRows, {
    mode: SETTING_ESTIMATE_MODE_GRAPE,
    sourceMode: SETTING_ESTIMATE_MODE_GRAPE,
    grapeObservation,
  });
}

export function calculateSettingEstimate(definition, record, options = {}) {
  const mode = normalizeSettingEstimateMode(
    typeof options === "string" ? options : options?.mode ?? options?.settingEstimateMode,
  );

  if (mode === SETTING_ESTIMATE_MODE_GRAPE) {
    return calculateGrapeSettingEstimate(definition, record) ??
      calculateBonusSettingEstimate(definition, record);
  }

  return calculateBonusSettingEstimate(definition, record);
}

export function formatSettingEstimateScore(value) {
  return Number.isFinite(value) ? value.toFixed(2) : "-";
}

export function getSettingEstimateHighlightClass(value) {
  const average = typeof value === "number" ? value : value?.average;

  if (!Number.isFinite(average)) {
    return "";
  }
  if (average >= 5) {
    return "settingEstimateLevel3";
  }
  if (average >= 4.5) {
    return "settingEstimateLevel2";
  }
  if (average >= 4) {
    return "settingEstimateLevel1";
  }
  if (average >= 3.5) {
    return "settingEstimateLevel0";
  }
  return "";
}

export function formatSettingEstimateAverage(estimate) {
  return estimate ? formatSettingEstimateScore(estimate.average) : "-";
}

export function formatSettingEstimateBreakdown(estimate) {
  if (!estimate) {
    return "";
  }

  return [
    `推測設定: ${formatSettingEstimateAverage(estimate)}`,
    estimate.sourceMode ? `推定基準: ${formatSettingEstimateModeLabel(estimate.sourceMode)}` : null,
    ...estimate.probabilities.map((row) => `${row.label}: ${formatProbabilityValue(row.probability)}`),
  ].filter(Boolean).join("\n");
}
