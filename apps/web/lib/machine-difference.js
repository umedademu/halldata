import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const currentDirectory = path.dirname(fileURLToPath(import.meta.url));
const rulesPath = path.resolve(currentDirectory, "../../../config/machine_difference_rules.json");

let cachedRules = null;

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

function roundDifferenceValue(value) {
  if (!Number.isFinite(value)) {
    return null;
  }

  const roundedValue = Math.sign(value) * Math.floor(Math.abs(value) + 0.5);
  return Object.is(roundedValue, -0) ? 0 : roundedValue;
}

function resolveBonusCount(row, bonusLabel) {
  const normalizedBonusLabel = String(bonusLabel ?? "").toLowerCase();
  const candidateValues = [
    row?.[bonusLabel],
    row?.[normalizedBonusLabel],
    row?.[`${normalizedBonusLabel}_count`],
  ];

  for (const value of candidateValues) {
    const parsedValue = readNumber(value);
    if (parsedValue !== null) {
      return parsedValue;
    }
  }

  return null;
}

export function loadMachineDifferenceRules() {
  if (cachedRules !== null) {
    return cachedRules;
  }

  try {
    const payload = JSON.parse(fs.readFileSync(rulesPath, "utf8"));
    const sourceRules = Array.isArray(payload?.machine_rules) ? payload.machine_rules : [];
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

export function calculateMachineDifferenceValue(machineName, row) {
  const rule = findMachineDifferenceRule(machineName);
  if (!rule) {
    return null;
  }

  const investmentCoins = readNumber(rule.investment_coins);
  const gamesPerInvestment = readNumber(rule.games_per_investment);
  const gamesCount = readNumber(row?.games_count ?? row?.["G数"]);
  if (
    investmentCoins === null ||
    gamesPerInvestment === null ||
    gamesPerInvestment === 0 ||
    gamesCount === null
  ) {
    return null;
  }

  const bonusPayouts =
    rule.bonus_payouts && typeof rule.bonus_payouts === "object" ? rule.bonus_payouts : {};
  const bonusEntries = Object.entries(bonusPayouts);
  if (bonusEntries.length === 0) {
    return null;
  }

  let totalBonusPayout = 0;
  for (const [bonusLabel, payoutValue] of bonusEntries) {
    const payoutCoins = readNumber(payoutValue);
    const hitCount = resolveBonusCount(row, bonusLabel);
    if (payoutCoins === null || hitCount === null) {
      return null;
    }
    totalBonusPayout += hitCount * payoutCoins;
  }

  return roundDifferenceValue(totalBonusPayout - (gamesCount * investmentCoins) / gamesPerInvestment);
}

export function withCalculatedDifferenceValue(row) {
  if (typeof row?.difference_value === "number" && Number.isFinite(row.difference_value)) {
    return row;
  }

  const calculatedValue = calculateMachineDifferenceValue(row?.machine_name, row);
  if (calculatedValue === null) {
    return row;
  }

  return {
    ...row,
    difference_value: calculatedValue,
  };
}
