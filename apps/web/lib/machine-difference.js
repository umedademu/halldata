import differenceRulesPayload from "../config/machine_difference_rules.json" with { type: "json" };

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

export function selectDifferenceValue(row, differenceMode = "bonus") {
  if (differenceMode === "bonus") {
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
