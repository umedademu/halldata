export const NEO_IM_JUGGLER_EX_DISPLAY_NAME = "ネオアイムジャグラーEX";

function parseRateText(value) {
  const denominator = Number(String(value).replace("1/", ""));
  if (!Number.isFinite(denominator) || denominator <= 0) {
    return 0;
  }
  return 1 / denominator;
}

function formatDenominator(value) {
  const rounded = Math.round(value * 100) / 100;
  if (Number.isInteger(rounded)) {
    return rounded.toFixed(1);
  }
  return rounded.toFixed(2);
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

const settingRateSeeds = [
  { setting: 1, label: "設定1", bbText: "1/273.1", rbText: "1/439.8" },
  { setting: 2, label: "設定2", bbText: "1/269.7", rbText: "1/399.6" },
  { setting: 3, label: "設定3", bbText: "1/269.7", rbText: "1/331.0" },
  { setting: 4, label: "設定4", bbText: "1/259.0", rbText: "1/315.1" },
  { setting: 5, label: "設定5", bbText: "1/259.0", rbText: "1/255.0" },
  { setting: 6, label: "設定6", bbText: "1/255.0", rbText: "1/255.0" },
];

export const NEO_IM_JUGGLER_EX_SETTING_RATES = settingRateSeeds.map((row) => {
  const bb = parseRateText(row.bbText);
  const rb = parseRateText(row.rbText);
  return {
    ...row,
    bb,
    rb,
    combined: bb + rb,
  };
});

export const NEO_IM_JUGGLER_EX_RATE_TABLE = NEO_IM_JUGGLER_EX_SETTING_RATES.map((row) => ({
  setting: row.label,
  bb: row.bbText,
  rb: row.rbText,
  combined: formatRateFromProbability(row.combined),
}));

function normalizeMachineName(value) {
  return String(value ?? "")
    .normalize("NFKC")
    .replace(/[\s\u3000・･_-]/gu, "")
    .toUpperCase();
}

export function isNeoImJugglerExName(machineName) {
  return normalizeMachineName(machineName).includes(normalizeMachineName(NEO_IM_JUGGLER_EX_DISPLAY_NAME));
}

function readNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
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

export function calculateNeoImJugglerSettingEstimate(record) {
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

  const logRows = NEO_IM_JUGGLER_EX_SETTING_RATES.map((row) => ({
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

export function formatNeoImJugglerSettingAverage(estimate) {
  return estimate ? estimate.average.toFixed(2) : "-";
}

export function formatNeoImJugglerSettingBreakdown(estimate) {
  if (!estimate) {
    return "";
  }

  return [
    `推測設定: ${formatNeoImJugglerSettingAverage(estimate)}`,
    ...estimate.probabilities.map((row) => `${row.label}: ${formatProbabilityValue(row.probability)}`),
  ].join("\n");
}
