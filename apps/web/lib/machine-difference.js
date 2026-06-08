import differenceRulesPayload from "../config/machine_difference_rules.json" with { type: "json" };
import {
  calculateSettingEstimate,
  getSettingEstimateDefinition,
  isCurrentSettingEstimateVersion,
} from "./setting-estimates.js";

let cachedRules = null;

export const DEFAULT_DIFFERENCE_MODE = "minrepo";
const SITE7_DIFFERENCE_SOURCE_GRAPH = "graph";
const MINREPO_ONE_BET_GAME_FACTOR = 1 / 3;
const ONE_BET_GRAPE_DENOMINATOR = 10.3;
const ONE_BET_REPLAY_DENOMINATOR = 7.3;
const ONE_BET_GRAPE_PAYOUT = 8;
const ONE_BET_REPLAY_PAYOUT = 1;
const ONE_BET_TARGET_MACHINE_RATIOS = [
  { keyword: "アイムジャグラーex", postAnnouncementBonusRatio: 0.75 },
  { keyword: "ゴーゴージャグラー3", postAnnouncementBonusRatio: 1 },
  { keyword: "マイジャグラー", postAnnouncementBonusRatio: 0.75 },
];

export function normalizeDifferenceMode(value) {
  return value === "bonus" || value === "estimated" || value === "minrepo"
    ? value
    : DEFAULT_DIFFERENCE_MODE;
}

function normalizeMachineName(value) {
  return String(value ?? "")
    .replace(/\u3000/gu, " ")
    .trim()
    .replace(/\s+/gu, "")
    .toLowerCase();
}

function readNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const normalized = String(value).trim().replaceAll(",", "");
  if (!/^[-]?\d+(?:\.\d+)?$/u.test(normalized)) {
    return null;
  }

  const parsedValue = Number(normalized);
  return Number.isFinite(parsedValue) ? parsedValue : null;
}

export function loadMachineDifferenceRules() {
  if (cachedRules !== null) {
    return cachedRules;
  }

  try {
    const sourceRules = Array.isArray(differenceRulesPayload?.machine_rules)
      ? differenceRulesPayload.machine_rules
      : [];
    cachedRules = sourceRules
      .filter((rule) => rule && typeof rule === "object")
      .map((rule) => ({
        ...rule,
        normalizedCanonicalName: normalizeMachineName(rule.canonical_name),
        normalizedMachineNames: Array.isArray(rule.machine_names)
          ? rule.machine_names.map(normalizeMachineName).filter(Boolean)
          : [],
        normalizedMatchKeywords: Array.isArray(rule.match_keywords)
          ? rule.match_keywords.map(normalizeMachineName).filter(Boolean)
          : [],
      }));
  } catch {
    cachedRules = [];
  }

  return cachedRules;
}

export function findMachineDifferenceRule(machineName) {
  const normalizedMachineName = normalizeMachineName(machineName);
  if (!normalizedMachineName) {
    return null;
  }

  return (
    loadMachineDifferenceRules().find((rule) => {
      if (
        rule.normalizedCanonicalName &&
        rule.normalizedCanonicalName === normalizedMachineName
      ) {
        return true;
      }

      if (rule.normalizedMachineNames.includes(normalizedMachineName)) {
        return true;
      }

      return rule.normalizedMatchKeywords.some(
        (keyword) => keyword && normalizedMachineName.includes(keyword),
      );
    }) ?? null
  );
}

export function canonicalMachineName(machineName) {
  const rule = findMachineDifferenceRule(machineName);
  if (!rule) {
    return String(machineName ?? "").trim();
  }

  const canonicalName = String(rule.canonical_name ?? "").trim();
  if (canonicalName) {
    return canonicalName;
  }

  const machineNames = Array.isArray(rule.machine_names) ? rule.machine_names : [];
  for (const candidateName of machineNames) {
    const text = String(candidateName ?? "").trim();
    if (text) {
      return text;
    }
  }

  return String(machineName ?? "").trim();
}

export function listEquivalentMachineNames(machineName) {
  const rule = findMachineDifferenceRule(machineName);
  if (!rule) {
    const text = String(machineName ?? "").trim();
    return text ? [text] : [];
  }

  const names = [];
  const seenNames = new Set();
  for (const candidateName of [rule.canonical_name, ...(Array.isArray(rule.machine_names) ? rule.machine_names : [])]) {
    const text = String(candidateName ?? "").trim();
    if (!text || seenNames.has(text)) {
      continue;
    }
    seenNames.add(text);
    names.push(text);
  }

  return names;
}

function readDifferenceNumber(value) {
  return typeof value === "number" && Number.isFinite(value) ? value : readNumber(value);
}

function roundHalfUp(value) {
  if (!Number.isFinite(value)) {
    return null;
  }
  return value >= 0 ? Math.floor(value + 0.5) : Math.ceil(value - 0.5);
}

function readBonusCount(row, bonusLabel) {
  const normalizedLabel = String(bonusLabel ?? "").trim();
  if (!normalizedLabel) {
    return null;
  }
  return (
    readDifferenceNumber(row?.[normalizedLabel]) ??
    readDifferenceNumber(row?.[`${normalizedLabel.toLowerCase()}_count`])
  );
}

function calculateBonusPayoutAndCount(rule, row) {
  const bonusPayouts = rule?.bonus_payouts;
  if (!bonusPayouts || typeof bonusPayouts !== "object") {
    return null;
  }

  let totalPayout = 0;
  let totalCount = 0;
  let hasBonusRule = false;
  for (const [bonusLabel, payoutValue] of Object.entries(bonusPayouts)) {
    const payoutCoins = readDifferenceNumber(payoutValue);
    const hitCount = readBonusCount(row, bonusLabel);
    if (payoutCoins === null || hitCount === null) {
      return null;
    }
    hasBonusRule = true;
    totalPayout += hitCount * payoutCoins;
    totalCount += hitCount;
  }

  return hasBonusRule ? { totalPayout, totalCount } : null;
}

function readOneBetBonusRatio(rule) {
  const candidateTexts = [
    rule?.canonical_name,
    ...(Array.isArray(rule?.machine_names) ? rule.machine_names : []),
    ...(Array.isArray(rule?.match_keywords) ? rule.match_keywords : []),
  ];
  const normalizedTexts = candidateTexts.map((value) =>
    String(value ?? "")
      .normalize("NFKC")
      .replace(/[\s\u3000・･_-]/gu, "")
      .toLowerCase()
  );
  return (
    ONE_BET_TARGET_MACHINE_RATIOS.find((entry) =>
      normalizedTexts.some((text) => text.includes(entry.keyword)),
    )?.postAnnouncementBonusRatio ?? null
  );
}

function calculateOneBetGames(totalBonusCount, postAnnouncementBonusRatio) {
  if (!Number.isFinite(totalBonusCount) || totalBonusCount <= 0) {
    return 0;
  }
  const settleProbability =
    1 - 1 / ONE_BET_GRAPE_DENOMINATOR - 1 / ONE_BET_REPLAY_DENOMINATOR;
  if (settleProbability <= 0) {
    return 0;
  }
  return (totalBonusCount * postAnnouncementBonusRatio) / settleProbability;
}

function calculateCoinHoldDifferenceValue({
  rule,
  gamesCount,
  investmentCoins,
  coinHold,
  totalBonusPayout,
  totalBonusCount,
}) {
  if (
    !Number.isFinite(gamesCount) ||
    !Number.isFinite(investmentCoins) ||
    !Number.isFinite(coinHold) ||
    coinHold <= 0 ||
    !Number.isFinite(totalBonusPayout)
  ) {
    return null;
  }

  const oneBetBonusRatio = readOneBetBonusRatio(rule);
  if (oneBetBonusRatio !== null) {
    const oneBetGames = calculateOneBetGames(totalBonusCount, oneBetBonusRatio);
    const normalGamesCount = gamesCount - oneBetGames * MINREPO_ONE_BET_GAME_FACTOR;
    if (normalGamesCount <= 0) {
      return null;
    }
    const oneBetSmallPayout =
      oneBetGames *
      (ONE_BET_GRAPE_PAYOUT / ONE_BET_GRAPE_DENOMINATOR +
        ONE_BET_REPLAY_PAYOUT / ONE_BET_REPLAY_DENOMINATOR);
    return roundHalfUp(
      totalBonusPayout -
        (normalGamesCount * investmentCoins) / coinHold +
        oneBetSmallPayout -
        oneBetGames,
    );
  }

  return roundHalfUp(totalBonusPayout - (gamesCount * investmentCoins) / coinHold);
}

function readSettingCoinHoldRows(rule) {
  return Object.entries(rule?.setting_coin_holds ?? {})
    .map(([setting, coinHold]) => ({
      setting: readDifferenceNumber(setting),
      coinHold: readDifferenceNumber(coinHold),
    }))
    .filter((row) => row.setting !== null && row.coinHold !== null && row.coinHold > 0)
    .sort((left, right) => left.setting - right.setting);
}

function interpolateSettingCoinHold(rule, settingAverage) {
  const coinHoldRows = readSettingCoinHoldRows(rule);
  if (coinHoldRows.length === 0 || !Number.isFinite(settingAverage)) {
    return null;
  }

  const firstRow = coinHoldRows[0];
  const lastRow = coinHoldRows.at(-1);
  if (settingAverage <= firstRow.setting) {
    return firstRow.coinHold;
  }
  if (settingAverage >= lastRow.setting) {
    return lastRow.coinHold;
  }

  for (let index = 0; index < coinHoldRows.length - 1; index += 1) {
    const leftRow = coinHoldRows[index];
    const rightRow = coinHoldRows[index + 1];
    if (settingAverage < leftRow.setting || settingAverage > rightRow.setting) {
      continue;
    }
    const settingWidth = rightRow.setting - leftRow.setting;
    if (settingWidth <= 0) {
      return leftRow.coinHold;
    }
    const progress = (settingAverage - leftRow.setting) / settingWidth;
    return leftRow.coinHold + (rightRow.coinHold - leftRow.coinHold) * progress;
  }

  return null;
}

export function calculateEstimatedCoinHoldDifferenceValue(row, machineName = "") {
  const targetMachineName = String(
    machineName || row?.machine_name || row?.machineName || "",
  ).trim();
  const rule = findMachineDifferenceRule(targetMachineName);
  const settingDefinition = getSettingEstimateDefinition(targetMachineName);
  const settingEstimate = settingDefinition
    ? calculateSettingEstimate(settingDefinition, row)
    : null;
  const gamesCount = readDifferenceNumber(row?.games_count);
  const precomputedDifferenceValue = readDifferenceNumber(row?.estimated_difference_value);
  const precomputedVersion = readDifferenceNumber(row?.estimated_difference_version);
  if (
    precomputedDifferenceValue !== null &&
    isCurrentSettingEstimateVersion(settingDefinition, precomputedVersion)
  ) {
    return precomputedDifferenceValue;
  }

  const coinHold = interpolateSettingCoinHold(rule, settingEstimate?.average);
  const investmentCoins = readDifferenceNumber(rule?.investment_coins);
  const bonusValues = calculateBonusPayoutAndCount(rule, row);

  if (
    !rule ||
    coinHold === null ||
    gamesCount === null ||
    investmentCoins === null ||
    bonusValues === null
  ) {
    return null;
  }

  return calculateCoinHoldDifferenceValue({
    rule,
    gamesCount,
    investmentCoins,
    coinHold,
    totalBonusPayout: bonusValues.totalPayout,
    totalBonusCount: bonusValues.totalCount,
  });
}

function readSite7GraphDifferenceValue(row) {
  const differenceSource = String(
    row?.site7_difference_source ?? row?.site7DifferenceSource ?? "",
  ).trim().toLowerCase();
  if (differenceSource !== SITE7_DIFFERENCE_SOURCE_GRAPH) {
    return null;
  }
  return readDifferenceNumber(row?.difference_value);
}

export function selectDifferenceValue(row, differenceMode = DEFAULT_DIFFERENCE_MODE, machineName = "") {
  const normalizedDifferenceMode = normalizeDifferenceMode(differenceMode);
  const site7GraphDifferenceValue = readSite7GraphDifferenceValue(row);
  if (site7GraphDifferenceValue !== null) {
    return site7GraphDifferenceValue;
  }

  if (normalizedDifferenceMode === "estimated") {
    const estimatedDifferenceValue = calculateEstimatedCoinHoldDifferenceValue(row, machineName);
    if (estimatedDifferenceValue !== null) {
      return estimatedDifferenceValue;
    }
  }

  if (normalizedDifferenceMode === "bonus") {
    const bonusDifferenceValue = readDifferenceNumber(row?.bonus_difference_value);
    if (bonusDifferenceValue !== null) {
      return bonusDifferenceValue;
    }
  }

  if (normalizedDifferenceMode === "estimated") {
    const bonusDifferenceValue = readDifferenceNumber(row?.bonus_difference_value);
    if (bonusDifferenceValue !== null) {
      return bonusDifferenceValue;
    }
  }

  return readDifferenceNumber(row?.difference_value);
}

export function withCanonicalMachineName(row) {
  const normalizedMachineName = canonicalMachineName(row?.machine_name);
  if (normalizedMachineName === row?.machine_name) {
    return row;
  }
  return {
    ...row,
    machine_name: normalizedMachineName,
  };
}
