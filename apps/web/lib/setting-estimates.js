import settingEstimatesPayload from "../config/setting_estimates.json" with { type: "json" };

export const SETTING_ESTIMATE_VALUE_VERSION = 5;
const PREVIOUS_SETTING_ESTIMATE_VALUE_VERSION = 4;
const UPDATED_SETTING_ESTIMATE_KEYS = new Set(["neoim-juggler-ex"]);

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
    return {
      ...row,
      bb,
      rb,
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

export function calculateSettingEstimate(definition, record) {
  if (!definition) {
    return null;
  }

  const precomputedEstimate = readPrecomputedSettingEstimate(definition, record);
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
  };
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
    ...estimate.probabilities.map((row) => `${row.label}: ${formatProbabilityValue(row.probability)}`),
  ].join("\n");
}
