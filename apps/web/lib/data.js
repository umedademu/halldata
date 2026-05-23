import { cache } from "react";

import { createEventFilters } from "./event-filters";
import { buildHuntScoreBacktestDetail } from "./hunt-backtest";
import { buildConditionRequirementOptions, buildScopedRankFilters } from "./hunt-bookmark";
import {
  buildHuntScoreSnapshots,
  canonicalHuntScoreTargetMachineName,
  getHuntScoreLogicDetail,
  isHuntScoreTargetStore,
  listHuntScoreRankingDateOptions,
  listHuntScoreLogicOptions,
  listHuntScoreSourceMachineNames,
  listHuntScoreSourceMachineNamesForStoreMachines,
  listHuntScoreTargetMachineNames,
  listHuntScoreTargetMachineNamesForStoreMachines,
} from "./hunt-score";
import {
  canonicalMachineName,
  normalizeDifferenceMode,
  listEquivalentMachineNames,
  withCanonicalMachineName,
} from "./machine-difference";

const PAGE_SIZE = 1000;
const DEFAULT_FETCH_CACHE_TTL_MS = 0;
const HUNT_BACKTEST_DEFAULT_EVENT_FILTERS = {
  "Aパーク春日店": {
    dayTails: [0],
    weekdays: [0, 6],
  },
  "A-PARK屋形原": {
    dayTails: [0],
    weekdays: [0, 6],
  },
  "Aパーク屋形原店": {
    dayTails: [0],
    weekdays: [0, 6],
  },
};
const DEFAULT_HUNT_RANKING_LIMIT = 20;
const DEFAULT_HUNT_BACKTEST_RECENT_DAYS = 90;
const DEFAULT_HUNT_RANK_REQUIRED = true;
const DEFAULT_HUNT_SCORE_REQUIRED = true;
const DEFAULT_HUNT_NEXT_GAP_REQUIRED = false;
const DAILY_SELECTION_MODE_MACHINE_TOP_NEXT_GAP = "machineTopNextGap";
const DEFAULT_CROSS_STORE_BACKTEST_LIMIT = 100;
const MAX_CROSS_STORE_BACKTEST_LIMIT = 300;
const DEFAULT_CROSS_STORE_BACKTEST_RECENT_DAYS = 30;
const DEFAULT_CROSS_STORE_MIN_ACTUAL_ROWS = 10;
const DEFAULT_CROSS_STORE_MIN_MATCHED_DATES = 3;
const CROSS_STORE_BACKTEST_CONCURRENCY = 12;
const CROSS_STORE_BACKTEST_WINDOW_BUFFER_DAYS = 18;
const CROSS_STORE_BACKTEST_NEXT_RESULT_BUFFER_DAYS = 7;
const HUNT_SCORE_ACTIVE_MACHINE_WINDOW_DAYS = 7;
const DEFAULT_CROSS_STORE_MACHINE_NAMES = [
  "SアイムジャグラーＥＸ",
  "ネオアイムジャグラーEX",
  "マイジャグラーV",
  "ゴーゴージャグラー３",
  "ファンキージャグラー２ＫＴ",
  "ミスタージャグラー",
  "ジャグラーガールズSS",
  "ハッピージャグラーＶＩＩＩ",
  "ウルトラミラクルジャグラー",
];

function requireActiveConditionFilters(requirementOptions, filters = {}) {
  return {
    rankRequired: filters.hasRankFilter ? true : Boolean(requirementOptions.rankRequired),
    machineRankRequired: filters.hasMachineRankFilter
      ? true
      : Boolean(requirementOptions.machineRankRequired),
    selectedRankRequired: filters.hasSelectedRankFilter
      ? true
      : Boolean(requirementOptions.selectedRankRequired),
    scoreRequired: filters.hasScoreFilter ? true : Boolean(requirementOptions.scoreRequired),
    nextGapRequired: filters.hasNextGapFilter
      ? true
      : Boolean(requirementOptions.nextGapRequired),
    upperGapRequired: filters.hasUpperGapFilter
      ? true
      : Boolean(requirementOptions.upperGapRequired),
  };
}

const UNKNOWN_PREFECTURE_LABEL = "都道府県未設定";
const UNKNOWN_AREA_LABEL = "地域未設定";
const SLOT_KEY_SEPARATOR = "\u0000";
const COMBINED_MACHINE_GROUPS = [
  {
    groupName: "アイムジャグラーEX",
    machineNames: ["ネオアイムジャグラーEX", "SアイムジャグラーＥＸ"],
    optionKey: "combineAimJuggler",
    allowPartial: true,
    slotLabelPrefixes: {
      "ネオアイムジャグラーEX": "ネオアイム",
      "SアイムジャグラーＥＸ": "Sアイム",
    },
  },
  {
    groupName: "ハナビ",
    machineNames: ["新ハナビ", "スマスロ ハナビ"],
    optionKey: "combineHanabi",
    slotLabelPrefixes: {
      "新ハナビ": "新ハナビ",
      "スマスロ ハナビ": "スマスロハナビ",
    },
  },
];

let cachedFileSettingsPromise = null;

function getFetchCacheTtlMs() {
  const value = Number(process.env.HALLDATA_FETCH_CACHE_TTL_MS);
  return Number.isFinite(value) && value >= 0 ? value : DEFAULT_FETCH_CACHE_TTL_MS;
}

function getRowsCache() {
  if (!globalThis.__halldataRowsCache) {
    globalThis.__halldataRowsCache = new Map();
  }
  return globalThis.__halldataRowsCache;
}

function getStoreMachineSummariesCache() {
  if (!globalThis.__halldataStoreMachineSummariesCache) {
    globalThis.__halldataStoreMachineSummariesCache = new Map();
  }
  return globalThis.__halldataStoreMachineSummariesCache;
}

function normalizeMachineNameForGrouping(value) {
  return String(value ?? "").normalize("NFKC").replace(/\s+/gu, "").trim();
}

function findCombinedMachineGroup(machineName) {
  const normalizedMachineName = normalizeMachineNameForGrouping(machineName);
  return (
    COMBINED_MACHINE_GROUPS.find(
      (group) => normalizeMachineNameForGrouping(group.groupName) === normalizedMachineName,
    ) ?? null
  );
}

function findCombinedMachineChildGroup(machineName) {
  const normalizedMachineName = normalizeMachineNameForGrouping(canonicalMachineName(machineName));
  return (
    COMBINED_MACHINE_GROUPS.find((group) =>
      group.machineNames.some(
        (candidateName) =>
          normalizeMachineNameForGrouping(canonicalMachineName(candidateName)) === normalizedMachineName,
      ),
    ) ?? null
  );
}

function combinedMachineGroupContainsName(group, machineName) {
  const normalizedMachineName = normalizeMachineNameForGrouping(canonicalMachineName(machineName));
  if (!group || !normalizedMachineName) {
    return false;
  }
  if (normalizeMachineNameForGrouping(group.groupName) === normalizeMachineNameForGrouping(machineName)) {
    return true;
  }
  return group.machineNames.some(
    (candidateName) =>
      normalizeMachineNameForGrouping(canonicalMachineName(candidateName)) === normalizedMachineName,
  );
}

function expandCombinedMachineNamesForOptions(machineNames, options = {}) {
  const expandedMachineNames = new Set(
    (Array.isArray(machineNames) ? machineNames : [machineNames])
      .map((machineName) => String(machineName ?? "").trim())
      .filter(Boolean),
  );

  for (const group of COMBINED_MACHINE_GROUPS) {
    const optionKey = String(group.optionKey ?? "").trim();
    if (!optionKey || !normalizeEnabledOption(options?.[optionKey], true)) {
      continue;
    }
    if (![...expandedMachineNames].some((machineName) => combinedMachineGroupContainsName(group, machineName))) {
      continue;
    }
    for (const machineName of group.machineNames) {
      expandedMachineNames.add(machineName);
    }
  }

  return [...expandedMachineNames];
}

function buildMachineSummaryByCanonicalName(machines) {
  return new Map(
    (Array.isArray(machines) ? machines : []).map((machine) => [
      normalizeMachineNameForGrouping(canonicalMachineName(machine.machineName)),
      machine,
    ]),
  );
}

function getAvailableCombinedMachineGroup(group, machinesByCanonicalName) {
  const childMachines = group.machineNames
    .map((machineName) =>
      machinesByCanonicalName.get(normalizeMachineNameForGrouping(canonicalMachineName(machineName))),
    )
    .filter(Boolean);

  const requiredChildCount = group.allowPartial === true ? 1 : group.machineNames.length;
  return childMachines.length >= requiredChildCount ? childMachines : null;
}

function filterLatestCombinedMachineChildren(childMachines) {
  const latestDate = (Array.isArray(childMachines) ? childMachines : [])
    .map((machine) => String(machine?.latestDate ?? "").trim())
    .filter(Boolean)
    .sort((left, right) => right.localeCompare(left))[0] ?? "";
  if (!latestDate) {
    return Array.isArray(childMachines) ? childMachines : [];
  }
  return childMachines.filter((machine) => String(machine?.latestDate ?? "").trim() === latestDate);
}

function calculateWeightedMachineAverage(machines, key) {
  let total = 0;
  let weightTotal = 0;

  for (const machine of machines) {
    const value = readNumber(machine?.[key]);
    const weight = Number(machine?.slotCount ?? 0);
    if (!Number.isFinite(value) || !Number.isFinite(weight) || weight <= 0) {
      continue;
    }
    total += value * weight;
    weightTotal += weight;
  }

  return weightTotal > 0 ? total / weightTotal : null;
}

function buildCombinedMachineSummary(group, childMachines) {
  const activeChildMachines = filterLatestCombinedMachineChildren(childMachines);
  const latestDate =
    activeChildMachines.reduce((latest, machine) => {
      const machineLatestDate = String(machine.latestDate ?? "").trim();
      if (!machineLatestDate) {
        return latest;
      }
      return !latest || machineLatestDate > latest ? machineLatestDate : latest;
    }, "") || null;

  return {
    machineName: group.groupName,
    slotCount: activeChildMachines.reduce((sum, machine) => sum + Number(machine.slotCount ?? 0), 0),
    latestDate,
    latestAverageDifference: calculateWeightedMachineAverage(activeChildMachines, "latestAverageDifference"),
    latestAverageGames: calculateWeightedMachineAverage(activeChildMachines, "latestAverageGames"),
    latestAveragePayout: calculateWeightedMachineAverage(activeChildMachines, "latestAveragePayout"),
    dataFile: null,
    isCombinedMachineGroup: true,
    childMachineNames: childMachines.map((machine) => machine.machineName),
  };
}

function withCombinedMachineEntries(machines) {
  const machinesByCanonicalName = buildMachineSummaryByCanonicalName(machines);
  const emittedMachineNames = new Set();
  const emittedGroupNames = new Set();
  const entries = [];

  for (const machine of machines) {
    const childGroup = findCombinedMachineChildGroup(machine.machineName);
    if (!childGroup) {
      entries.push(machine);
      continue;
    }

    const childMachines = getAvailableCombinedMachineGroup(childGroup, machinesByCanonicalName);
    if (!childMachines) {
      entries.push(machine);
      continue;
    }

    if (!emittedGroupNames.has(childGroup.groupName)) {
      entries.push(buildCombinedMachineSummary(childGroup, childMachines));
      emittedGroupNames.add(childGroup.groupName);
      for (const childMachine of childMachines) {
        const canonicalChildName = normalizeMachineNameForGrouping(canonicalMachineName(childMachine.machineName));
        entries.push({
          ...childMachine,
          isCombinedMachineChild: true,
          parentMachineName: childGroup.groupName,
        });
        emittedMachineNames.add(canonicalChildName);
      }
    }

    const canonicalMachine = normalizeMachineNameForGrouping(canonicalMachineName(machine.machineName));
    if (!emittedMachineNames.has(canonicalMachine)) {
      entries.push(machine);
    }
  }

  return entries;
}

function parseCombinedSlotKey(slotKey) {
  const text = String(slotKey ?? "");
  const separatorIndex = text.indexOf(SLOT_KEY_SEPARATOR);
  if (separatorIndex < 0) {
    return {
      machineName: "",
      slotNumber: text,
    };
  }

  return {
    machineName: text.slice(0, separatorIndex),
    slotNumber: text.slice(separatorIndex + SLOT_KEY_SEPARATOR.length),
  };
}

function getCombinedMachineChildOrder(machineName) {
  const childGroup = findCombinedMachineChildGroup(machineName);
  if (!childGroup) {
    return Number.MAX_SAFE_INTEGER;
  }
  const normalizedMachineName = normalizeMachineNameForGrouping(canonicalMachineName(machineName));
  const index = childGroup.machineNames.findIndex(
    (candidateName) =>
      normalizeMachineNameForGrouping(canonicalMachineName(candidateName)) === normalizedMachineName,
  );
  return index >= 0 ? index : Number.MAX_SAFE_INTEGER;
}

function compareSlotKeys(left, right) {
  const leftSlot = parseCombinedSlotKey(left);
  const rightSlot = parseCombinedSlotKey(right);

  if (leftSlot.machineName || rightSlot.machineName) {
    return (
      getCombinedMachineChildOrder(leftSlot.machineName) -
        getCombinedMachineChildOrder(rightSlot.machineName) ||
      leftSlot.machineName.localeCompare(rightSlot.machineName, "ja") ||
      compareSlotNumbers(leftSlot.slotNumber, rightSlot.slotNumber)
    );
  }

  return compareSlotNumbers(left, right);
}

function getCombinedSlotLabel(machineName, slotNumber) {
  const childGroup = findCombinedMachineChildGroup(machineName);
  const canonicalName = canonicalMachineName(machineName);
  const prefix = childGroup?.slotLabelPrefixes?.[canonicalName] ?? canonicalName;
  return `${prefix} ${slotNumber}番台`;
}

function buildFetchCacheKey(tableName, params) {
  return JSON.stringify({
    tableName,
    params: Object.entries(params).sort(([left], [right]) => left.localeCompare(right)),
  });
}

async function readFallbackSettings() {
  if (cachedFileSettingsPromise !== null) {
    return cachedFileSettingsPromise;
  }

  cachedFileSettingsPromise = (async () => {
    const settings = {};

    const [{ default: fs }, pathModule, urlModule] = await Promise.all([
      import("node:fs"),
      import("node:path"),
      import("node:url"),
    ]);

    const currentDirectory = pathModule.dirname(urlModule.fileURLToPath(import.meta.url));
    const envCandidates = [
      pathModule.resolve(currentDirectory, "../../../env.local"),
      pathModule.resolve(currentDirectory, "../../../.env.local"),
      pathModule.resolve(currentDirectory, "../.env.local"),
    ];

    for (const candidate of envCandidates) {
      if (!fs.existsSync(candidate)) {
        continue;
      }

      const lines = fs.readFileSync(candidate, "utf8").split(/\r?\n/u);
      for (const rawLine of lines) {
        let line = rawLine.trim();
        if (!line || line.startsWith("#") || !line.includes("=")) {
          continue;
        }
        if (line.startsWith("export ")) {
          line = line.slice(7).trim();
        }

        const separatorIndex = line.indexOf("=");
        const name = line.slice(0, separatorIndex).trim();
        let value = line.slice(separatorIndex + 1).trim();
        if (
          value.length >= 2 &&
          ((value.startsWith('"') && value.endsWith('"')) ||
            (value.startsWith("'") && value.endsWith("'")))
        ) {
          value = value.slice(1, -1);
        }
        settings[name] = value;
      }
    }

    return settings;
  })();

  return cachedFileSettingsPromise;
}

async function readSetting(name, fallback = "") {
  const fallbackSettings = await readFallbackSettings();
  return process.env[name] || fallbackSettings[name] || fallback;
}

function buildUrlFromHost(host, protocol = "https") {
  const safeHost = String(host ?? "").trim();
  if (!safeHost) {
    return "";
  }
  return `${String(protocol || "https").replace(/:$/u, "")}://${safeHost}`.replace(/\/+$/u, "");
}

async function getRequestBaseUrl() {
  try {
    const { headers } = await import("next/headers");
    const headerList = await headers();
    return buildUrlFromHost(
      headerList.get("x-forwarded-host") || headerList.get("host"),
      headerList.get("x-forwarded-proto") || "https",
    );
  } catch {
    return "";
  }
}

async function getStaticWebDataBaseUrl() {
  const configuredBaseUrl =
    (await readSetting("CLOUDFLARE_R2_PUBLIC_BASE_URL")) ||
    (await readSetting("CLOUDFLARE_R2_PUBLIC_URL")) ||
    (await readSetting("HALLDATA_R2_PUBLIC_BASE_URL"));
  if (configuredBaseUrl) {
    return configuredBaseUrl.replace(/\/+$/u, "");
  }
  throw new Error("R2の公開URLが設定されていません。CLOUDFLARE_R2_PUBLIC_BASE_URL を設定してください。");
}

function normalizeStaticDataPath(relativePath) {
  return String(relativePath ?? "")
    .replace(/\\/gu, "/")
    .replace(/^\/+/u, "")
    .split("/")
    .filter((part) => part && part !== "." && part !== "..")
    .join("/");
}

async function readStaticJsonFromPublicUrl(relativePath) {
  const baseUrl = await getStaticWebDataBaseUrl();
  if (!baseUrl) {
    return null;
  }

  const normalizedPath = normalizeStaticDataPath(relativePath);
  if (!normalizedPath) {
    return null;
  }

  const url = new URL(normalizedPath, `${baseUrl}/`);
  try {
    const response = await fetch(url, { cache: "no-store" });
    if (response.status === 404) {
      return null;
    }
    if (!response.ok) {
      throw new Error(`R2のJsonを読み込めませんでした。(${response.status}) ${normalizedPath}`);
    }
    return response.json();
  } catch (error) {
    if (error instanceof Error) {
      throw error;
    }
    throw new Error(`R2のJsonを読み込めませんでした。${normalizedPath}`);
  }
}

async function readStaticWebDataPayload(relativePath) {
  const normalizedPath = normalizeStaticDataPath(relativePath);
  if (!normalizedPath) {
    return null;
  }

  return readStaticJsonFromPublicUrl(normalizedPath);
}

async function readStaticWebDataIndex() {
  const payload = await readStaticWebDataPayload("index.json");
  if (!payload || !Array.isArray(payload.stores)) {
    return null;
  }
  return {
    stores: payload.stores.filter((store) => store && typeof store === "object"),
  };
}

function staticStoreMatchesId(storeEntry, storeId) {
  const requestedStoreId = String(storeId ?? "").trim();
  if (!requestedStoreId) {
    return false;
  }
  if (String(storeEntry?.id ?? "").trim() === requestedStoreId) {
    return true;
  }
  return (Array.isArray(storeEntry?.legacyIds) ? storeEntry.legacyIds : [])
    .some((legacyId) => String(legacyId ?? "").trim() === requestedStoreId);
}

function readStaticStoreEntryIdentity(storeEntry) {
  return {
    id: String(storeEntry?.id ?? "").trim(),
    storeName: String(storeEntry?.storeName ?? "").trim(),
    storeUrl: String(storeEntry?.storeUrl ?? "").trim(),
    prefectureName: String(
      storeEntry?.prefectureName ?? storeEntry?.site7Prefecture ?? storeEntry?.site7_prefecture ?? "",
    ).trim(),
    areaName: String(storeEntry?.areaName ?? storeEntry?.site7Area ?? storeEntry?.site7_area ?? "").trim(),
  };
}

function hasOwnValue(source, key) {
  return source && typeof source === "object" && Object.hasOwn(source, key);
}

function readStaticEventArray(source, camelKey, snakeKey, fallbackSource = null) {
  if (hasOwnValue(source, camelKey)) {
    return source[camelKey];
  }
  if (hasOwnValue(source, snakeKey)) {
    return source[snakeKey];
  }
  if (fallbackSource) {
    return readStaticEventArray(fallbackSource, camelKey, snakeKey);
  }
  return [];
}

function readStaticEventFlag(source, camelKey, snakeKey, fallbackSource = null) {
  if (hasOwnValue(source, camelKey)) {
    return Boolean(source[camelKey]);
  }
  if (hasOwnValue(source, snakeKey)) {
    return Boolean(source[snakeKey]);
  }
  if (fallbackSource) {
    return readStaticEventFlag(fallbackSource, camelKey, snakeKey);
  }
  return false;
}

function buildStaticEventFields(source, fallbackSource = null) {
  return {
    eventDayTails: normalizeEventDayTails(
      readStaticEventArray(source, "eventDayTails", "event_day_tails", fallbackSource),
    ),
    eventZoro: readStaticEventFlag(source, "eventZoro", "event_zoro", fallbackSource),
    eventWeekdays: normalizeEventWeekdays(
      readStaticEventArray(source, "eventWeekdays", "event_weekdays", fallbackSource),
    ),
    eventMonthDays: normalizeEventMonthDays(
      readStaticEventArray(source, "eventMonthDays", "event_month_days", fallbackSource),
    ),
    eventSourceText: String(
      source?.eventSourceText ??
        source?.event_source_text ??
        fallbackSource?.eventSourceText ??
        fallbackSource?.event_source_text ??
        "",
    ).trim(),
  };
}

function mergeStaticStoreEntryIdentity(payload, storeEntry) {
  if (!payload || typeof payload !== "object") {
    return payload;
  }
  const store = payload.store && typeof payload.store === "object" ? payload.store : {};
  const entry = readStaticStoreEntryIdentity(storeEntry);
  return {
    ...payload,
    store: {
      ...store,
      id: String(store.id ?? "").trim() || entry.id,
      storeName: String(store.storeName ?? "").trim() || entry.storeName,
      storeUrl: String(store.storeUrl ?? "").trim() || entry.storeUrl,
      prefectureName: String(store.prefectureName ?? "").trim() || entry.prefectureName,
      areaName: String(store.areaName ?? "").trim() || entry.areaName,
      ...buildStaticEventFields(store, storeEntry),
    },
  };
}

async function readStaticStoreByEntry(storeEntry) {
  if (!storeEntry) {
    return null;
  }

  if (!storeEntry.dataFile) {
    const entry = readStaticStoreEntryIdentity(storeEntry);
    return {
      version: 1,
      generatedAt: null,
      store: {
        ...entry,
        legacyIds: Array.isArray(storeEntry.legacyIds) ? storeEntry.legacyIds : [],
        ...buildStaticEventFields(storeEntry),
      },
      summary: {
        machineCount: 0,
        latestDate: null,
        recordCount: 0,
      },
      machines: [],
    };
  }

  const payload = await readStaticWebDataPayload(String(storeEntry.dataFile));
  return payload && typeof payload === "object" ? mergeStaticStoreEntryIdentity(payload, storeEntry) : null;
}

async function readStaticStoreById(storeId) {
  const index = await readStaticWebDataIndex();
  const storeEntry = index?.stores.find((entry) => staticStoreMatchesId(entry, storeId));
  if (!index || !storeEntry) {
    return null;
  }

  return readStaticStoreByEntry(storeEntry);
}

function buildQuery(params) {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || value === "") {
      continue;
    }
    query.set(key, String(value));
  }
  return query;
}

function buildMachineNameFilter(machineNames) {
  const uniqueMachineNames = [...new Set((Array.isArray(machineNames) ? machineNames : []).map((value) => String(value ?? "").trim()).filter(Boolean))];

  if (uniqueMachineNames.length === 0) {
    return {};
  }

  if (uniqueMachineNames.length === 1) {
    return {
      machine_name: `eq.${uniqueMachineNames[0]}`,
    };
  }

  return {
    or: `(${uniqueMachineNames.map((name) => `machine_name.eq.${name}`).join(",")})`,
  };
}

function readJsonObject(value) {
  if (!value) {
    return {};
  }

  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
    } catch {
      return {};
    }
  }

  return typeof value === "object" && !Array.isArray(value) ? value : {};
}

function readNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function readPositiveInteger(value, fallbackValue) {
  const number = Number(value);
  return Number.isInteger(number) && number >= 1 ? number : fallbackValue;
}

function hasProvidedOption(options, key) {
  return (
    options &&
    typeof options === "object" &&
    Object.hasOwn(options, key) &&
    options[key] !== undefined
  );
}

function readPositiveIntegerOption(options, key, fallbackValue) {
  if (!hasProvidedOption(options, key)) {
    return fallbackValue;
  }
  return readPositiveInteger(options?.[key], null);
}

function readNumberOption(options, key, fallbackValue) {
  if (!hasProvidedOption(options, key)) {
    return fallbackValue;
  }
  return readNumber(options?.[key]);
}

function readNonNegativeInteger(value, fallbackValue) {
  const number = Number(value);
  return Number.isInteger(number) && number >= 0 ? number : fallbackValue;
}

function readOptionalNonNegativeInteger(value) {
  const text = String(value ?? "").trim();
  if (!text) {
    return null;
  }
  const number = Number(text);
  return Number.isInteger(number) && number >= 0 ? number : null;
}

function normalizeDateInput(value) {
  const text = String(value ?? "").trim();
  return /^\d{4}-\d{2}-\d{2}$/u.test(text) ? text : null;
}

function shiftDateInput(value, days) {
  const dateText = normalizeDateInput(value);
  if (!dateText) {
    return null;
  }

  const date = new Date(
    Number(dateText.slice(0, 4)),
    Number(dateText.slice(5, 7)) - 1,
    Number(dateText.slice(8, 10)),
  );
  date.setDate(date.getDate() + days);
  const year = String(date.getFullYear()).padStart(4, "0");
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function splitOptionValues(value) {
  if (Array.isArray(value)) {
    return value.flatMap((item) => splitOptionValues(item));
  }
  if (value === null || value === undefined || value === "") {
    return [];
  }
  return String(value)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizeEnabledOption(value, fallbackValue = true) {
  const values = splitOptionValues(value);
  if (values.length === 0) {
    return fallbackValue;
  }
  return values.includes("1") || values.includes("true") || values.includes("on");
}

function normalizeDailySelectionMode(value) {
  return splitOptionValues(value).includes(DAILY_SELECTION_MODE_MACHINE_TOP_NEXT_GAP)
    ? DAILY_SELECTION_MODE_MACHINE_TOP_NEXT_GAP
    : "";
}

function normalizeInitialMachineSelection(machineNames, options = {}) {
  const machineTouched =
    options?.machineTouched === true ||
    options?.machineTouched === "1" ||
    options?.machineTouched === "true" ||
    options?.machineTouched === "on";
  if (!machineTouched) {
    return machineNames;
  }

  const machineNameSet = new Set(machineNames);
  return splitOptionValues(options?.machineNames).filter((machineName) => machineNameSet.has(machineName));
}

function detailRecordHasMeaningfulResult(record) {
  return ["difference_value", "bonus_difference_value", "games_count", "bb_count", "rb_count"].some((key) =>
    Number.isFinite(readNumber(record?.[key])),
  );
}

function buildRawRowsFromMachineDailyDetailRows(rows) {
  const expandedRows = [];

  for (const row of rows) {
    const machineName = String(row.machine_name ?? "").trim();
    const targetDate = String(row.target_date ?? "").trim();
    const recordsBySlot = readJsonObject(row.records_by_slot);
    if (!machineName || !targetDate) {
      continue;
    }

    for (const [slotNumber, record] of Object.entries(recordsBySlot)) {
      expandedRows.push({
        machine_name: machineName,
        target_date: targetDate,
        slot_number: String(slotNumber ?? "").trim(),
        difference_value: record?.difference_value ?? null,
        bonus_difference_value: record?.bonus_difference_value ?? null,
        games_count: record?.games_count ?? null,
        payout_rate: record?.payout_rate ?? null,
        bb_count: record?.bb_count ?? null,
        rb_count: record?.rb_count ?? null,
        setting_estimate_average: record?.setting_estimate_average ?? null,
        setting_estimate_status: String(record?.setting_estimate_status ?? "").trim() || null,
        setting_estimate_source: String(record?.setting_estimate_source ?? "").trim() || null,
        setting_estimate_version: record?.setting_estimate_version ?? null,
        estimated_difference_value: record?.estimated_difference_value ?? null,
        estimated_difference_status: String(record?.estimated_difference_status ?? "").trim() || null,
        estimated_difference_source: String(record?.estimated_difference_source ?? "").trim() || null,
        estimated_difference_version: record?.estimated_difference_version ?? null,
        combined_ratio_text: record?.combined_ratio_text ?? null,
        bb_ratio_text: record?.bb_ratio_text ?? null,
        rb_ratio_text: record?.rb_ratio_text ?? null,
      });
    }
  }

  return expandedRows;
}

function dailyDetailRowHasMeaningfulResult(row) {
  const averageDifference = readNumber(row?.average_difference);
  const averageGames = readNumber(row?.average_games);
  if (Number.isFinite(averageDifference) || Number.isFinite(averageGames)) {
    return true;
  }

  const recordsBySlot = readJsonObject(row?.records_by_slot);
  return Object.values(recordsBySlot).some((record) => detailRecordHasMeaningfulResult(record));
}

async function fetchHuntScoreSourceRows(resultsTable, machineDailyDetailsTable, storeId, storeName) {
  const huntScoreMachineNames = [
    ...new Set(
      listHuntScoreSourceMachineNames(storeName).flatMap((name) => listEquivalentMachineNames(name)),
    ),
  ];

  if (huntScoreMachineNames.length === 0) {
    return {
      targetRows: [],
      storeRows: [],
    };
  }

  try {
    const [targetMachineRows, storeDateRows] = await Promise.all([
      fetchAllRows(machineDailyDetailsTable, {
        select: "machine_name,target_date,records_by_slot",
        store_id: `eq.${storeId}`,
        ...buildMachineNameFilter(huntScoreMachineNames),
        order: "target_date.desc,machine_name.asc",
      }),
      fetchAllRows(machineDailyDetailsTable, {
        select: "target_date,average_difference,average_games,records_by_slot",
        store_id: `eq.${storeId}`,
        order: "target_date.desc",
      }),
    ]);

    if (targetMachineRows.length > 0) {
      const targetRows = buildRawRowsFromMachineDailyDetailRows(targetMachineRows).map(
        withCanonicalMachineName,
      );
      const storeRows = [
        ...new Set(
          storeDateRows
            .filter((row) => dailyDetailRowHasMeaningfulResult(row))
            .map((row) => String(row.target_date ?? "").trim())
            .filter(Boolean),
        ),
      ].map((targetDate) => ({
        target_date: targetDate,
        difference_value: 0,
      }));
      return {
        targetRows,
        storeRows,
      };
    }
  } catch (error) {
    if (
      !(error instanceof Error) ||
      (!error.message.includes("(400)") &&
        !error.message.includes("(404)") &&
        !error.message.includes("(500)"))
    ) {
      throw error;
    }
  }

  const [fetchedTargetRows, fetchedStoreRows] = await Promise.all([
    fetchAllRows(resultsTable, {
      select:
        "store_id,machine_name,target_date,slot_number,difference_value,bonus_difference_value,games_count,payout_rate,bb_count,rb_count,combined_ratio_text,bb_ratio_text,rb_ratio_text",
      store_id: `eq.${storeId}`,
      ...buildMachineNameFilter(huntScoreMachineNames),
      order: "target_date.desc,slot_number.asc",
    }),
    fetchAllRows(resultsTable, {
      select: "target_date,difference_value,games_count,bb_count,rb_count",
      store_id: `eq.${storeId}`,
    }),
  ]);

  return {
    targetRows: fetchedTargetRows.map(withCanonicalMachineName),
    storeRows: fetchedStoreRows,
  };
}

function normalizeStoreUrl(value) {
  const text = String(value ?? "").trim();
  if (!text) {
    throw new Error("店舗URLを入力してください。");
  }

  let parsedUrl;
  try {
    parsedUrl = new URL(text);
  } catch {
    throw new Error("店舗URLは http:// または https:// から入力してください。");
  }

  if (!["http:", "https:"].includes(parsedUrl.protocol) || !parsedUrl.hostname) {
    throw new Error("店舗URLは http:// または https:// から入力してください。");
  }

  if (parsedUrl.pathname !== "/") {
    parsedUrl.pathname = parsedUrl.pathname.replace(/\/+$/u, "") + "/";
  }
  parsedUrl.hash = "";
  return parsedUrl.toString();
}

function clearRowsCache() {
  globalThis.__halldataRowsCache?.clear();
  globalThis.__halldataStoreMachineSummariesCache?.clear();
}

function normalizeEventDayTails(value) {
  const sourceValues = Array.isArray(value) ? value : [];
  return [...new Set(sourceValues)]
    .map((item) => Number(item))
    .filter((item) => Number.isInteger(item) && item >= 0 && item <= 9)
    .sort((left, right) => left - right);
}

function normalizeEventWeekdays(value) {
  const sourceValues = Array.isArray(value) ? value : [];
  return [...new Set(sourceValues)]
    .map((item) => Number(item))
    .filter((item) => Number.isInteger(item) && item >= 0 && item <= 6)
    .sort((left, right) => left - right);
}

function normalizeEventMonthDays(value) {
  const sourceValues = Array.isArray(value) ? value : [];
  return [...new Set(sourceValues)]
    .map((item) => Number(item))
    .filter((item) => Number.isInteger(item) && item >= 1 && item <= 31)
    .sort((left, right) => left - right);
}

function buildEventFiltersFromStore(store) {
  return createEventFilters(
    normalizeEventDayTails(store?.event_day_tails),
    Boolean(store?.event_zoro),
    normalizeEventWeekdays(store?.event_weekdays),
    normalizeEventMonthDays(store?.event_month_days),
  );
}

async function fetchStoreEventRows(storesTable, storeId) {
  try {
    return await fetchAllRows(storesTable, {
      select: "id,store_name,store_url,event_day_tails,event_zoro,event_weekdays,event_month_days",
      id: `eq.${storeId}`,
    });
  } catch (error) {
    if (!(error instanceof Error) || !error.message.includes("(400)")) {
      throw error;
    }
    try {
      return await fetchAllRows(storesTable, {
        select: "id,store_name,store_url,event_day_tails,event_zoro,event_weekdays",
        id: `eq.${storeId}`,
      });
    } catch (fallbackError) {
      if (!(fallbackError instanceof Error) || !fallbackError.message.includes("(400)")) {
        throw fallbackError;
      }
      return fetchAllRows(storesTable, {
        select: "id,store_name,store_url,event_day_tails,event_zoro",
        id: `eq.${storeId}`,
      });
    }
  }
}

async function fetchAllRowsUncached(tableName, params) {
  throw new Error("表示用JSONが見つかりません。GUIアプリでWeb表示用データを生成してください。");
}

async function fetchAllRows(tableName, params) {
  const cacheTtlMs = getFetchCacheTtlMs();
  if (cacheTtlMs === 0) {
    return fetchAllRowsUncached(tableName, params);
  }

  const cacheKey = buildFetchCacheKey(tableName, params);
  const rowsCache = getRowsCache();
  const cachedEntry = rowsCache.get(cacheKey);
  const now = Date.now();

  if (cachedEntry?.rows && cachedEntry.expiresAt > now) {
    return cachedEntry.rows;
  }
  if (cachedEntry?.promise && cachedEntry.expiresAt > now) {
    return cachedEntry.promise;
  }

  const promise = fetchAllRowsUncached(tableName, params)
    .then((rows) => {
      rowsCache.set(cacheKey, {
        rows,
        expiresAt: Date.now() + cacheTtlMs,
      });
      return rows;
    })
    .catch((error) => {
      rowsCache.delete(cacheKey);
      throw error;
    });

  rowsCache.set(cacheKey, {
    promise,
    expiresAt: now + cacheTtlMs,
  });

  return promise;
}

function average(values) {
  const numericValues = values.filter((value) => typeof value === "number" && Number.isFinite(value));
  if (numericValues.length === 0) {
    return null;
  }
  const total = numericValues.reduce((sum, value) => sum + value, 0);
  return total / numericValues.length;
}

function compareSlotNumbers(left, right) {
  const leftNumber = Number(left);
  const rightNumber = Number(right);
  const leftIsNumber = Number.isFinite(leftNumber);
  const rightIsNumber = Number.isFinite(rightNumber);

  if (leftIsNumber && rightIsNumber) {
    return leftNumber - rightNumber;
  }

  return String(left).localeCompare(String(right), "ja");
}

function buildMachineLatestSummaries(rows) {
  const buckets = new Map();
  let latestDate = null;

  for (const row of rows) {
    if (latestDate === null || row.target_date > latestDate) {
      latestDate = row.target_date;
    }

    const key = row.machine_name;
    const bucket = buckets.get(key);
    if (!bucket || row.target_date > bucket.latestDate) {
      buckets.set(key, {
        machineName: row.machine_name,
        slots: new Set(),
        rows: [],
        latestDate: row.target_date,
      });
    }

    const currentBucket = buckets.get(key);
    if (row.target_date !== currentBucket.latestDate) {
      continue;
    }

    currentBucket.rows.push(row);
    currentBucket.slots.add(row.slot_number);
  }

  const machines = [...buckets.values()]
    .map((bucket) => {
      const latestRows = bucket.rows;
      return {
        machineName: bucket.machineName,
        slotCount: bucket.slots.size,
        latestDate: bucket.latestDate,
        latestAverageDifference: average(latestRows.map((row) => row.difference_value)),
        latestAverageGames: average(latestRows.map((row) => row.games_count)),
        latestAveragePayout: average(latestRows.map((row) => row.payout_rate)),
      };
    })
    .sort((left, right) => {
      if (left.latestDate !== right.latestDate) {
        return right.latestDate.localeCompare(left.latestDate, "ja");
      }
      if (left.slotCount !== right.slotCount) {
        return right.slotCount - left.slotCount;
      }
      return left.machineName.localeCompare(right.machineName, "ja");
    });

  return {
    latestDate,
    machines,
  };
}

function buildStoreSummary(store, machineSummaries) {
  return {
    id: store.id,
    storeName: store.store_name,
    storeUrl: store.store_url,
    machineCount: machineSummaries.length,
    latestDate:
      machineSummaries.reduce((currentLatestDate, machine) => {
        if (!machine.latestDate) {
          return currentLatestDate;
        }
        if (currentLatestDate === null || machine.latestDate > currentLatestDate) {
          return machine.latestDate;
        }
        return currentLatestDate;
      }, null) ?? null,
  };
}

function buildMachineSummaryResultFromSummaryRows(rows) {
  const machines = rows
    .map((row) => ({
      machineName: String(row.machine_name ?? "").trim(),
      slotCount: Number(row.slot_count ?? 0),
      latestDate: row.latest_date ? String(row.latest_date) : null,
      latestAverageDifference:
        row.average_difference === null || row.average_difference === undefined
          ? null
          : Number(row.average_difference),
      latestAverageGames:
        row.average_games === null || row.average_games === undefined
          ? null
          : Number(row.average_games),
      latestAveragePayout:
        row.average_payout === null || row.average_payout === undefined
          ? null
          : Number(row.average_payout),
    }))
    .filter((machine) => machine.machineName && machine.latestDate)
    .sort((left, right) => {
      if (left.latestDate !== right.latestDate) {
        return right.latestDate.localeCompare(left.latestDate, "ja");
      }
      if (left.slotCount !== right.slotCount) {
        return right.slotCount - left.slotCount;
      }
      return left.machineName.localeCompare(right.machineName, "ja");
    });

  return {
    latestDate:
      machines.reduce((currentLatestDate, machine) => {
        if (!machine.latestDate) {
          return currentLatestDate;
        }
        if (currentLatestDate === null || machine.latestDate > currentLatestDate) {
          return machine.latestDate;
        }
        return currentLatestDate;
      }, null) ?? null,
    machines,
  };
}

function buildMachineDetailFromDailyRows(rows) {
  const slotNumbersSet = new Set();
  const dateRows = [];
  const allDifferenceValues = [];
  const allGamesValues = [];
  const allPayoutValues = [];
  const bestWorstCandidates = [];
  let recordCount = 0;

  const sortedRows = [...rows].sort((left, right) => {
    return String(right.target_date ?? "").localeCompare(String(left.target_date ?? ""), "ja");
  });

  for (const row of sortedRows) {
    const date = String(row.target_date ?? "").trim();
    const machineName = String(row.machine_name ?? "").trim();
    if (!date || !machineName) {
      continue;
    }

    const sourceRecords = readJsonObject(row.records_by_slot);
    const recordsBySlot = {};
    const dailyDifferenceValues = [];

    for (const slotNumber of Object.keys(sourceRecords).sort(compareSlotNumbers)) {
      const sourceRecord = sourceRecords[slotNumber] ?? {};
      const record = withCanonicalMachineName({
        machine_name: machineName,
        target_date: date,
        slot_number: slotNumber,
        difference_value: sourceRecord.difference_value ?? null,
        bonus_difference_value: sourceRecord.bonus_difference_value ?? null,
        games_count: sourceRecord.games_count ?? null,
        payout_rate: sourceRecord.payout_rate ?? null,
        bb_count: sourceRecord.bb_count ?? null,
        rb_count: sourceRecord.rb_count ?? null,
        setting_estimate_average: sourceRecord.setting_estimate_average ?? null,
        setting_estimate_status: String(sourceRecord.setting_estimate_status ?? "").trim() || null,
        setting_estimate_source: String(sourceRecord.setting_estimate_source ?? "").trim() || null,
        setting_estimate_version: sourceRecord.setting_estimate_version ?? null,
        estimated_difference_value: sourceRecord.estimated_difference_value ?? null,
        estimated_difference_status: String(sourceRecord.estimated_difference_status ?? "").trim() || null,
        estimated_difference_source: String(sourceRecord.estimated_difference_source ?? "").trim() || null,
        estimated_difference_version: sourceRecord.estimated_difference_version ?? null,
        combined_ratio_text: sourceRecord.combined_ratio_text ?? null,
        bb_ratio_text: sourceRecord.bb_ratio_text ?? null,
        rb_ratio_text: sourceRecord.rb_ratio_text ?? null,
        data_source: String(sourceRecord.data_source ?? "").trim() || null,
        site7_fetched_at: readSite7FetchedAt(sourceRecord),
      });
      recordsBySlot[slotNumber] = record;
      slotNumbersSet.add(slotNumber);
      recordCount += 1;

      if (typeof record.difference_value === "number" && Number.isFinite(record.difference_value)) {
        allDifferenceValues.push(record.difference_value);
        dailyDifferenceValues.push(record.difference_value);
      }
      if (typeof record.games_count === "number" && Number.isFinite(record.games_count)) {
        allGamesValues.push(record.games_count);
      }
      if (typeof record.payout_rate === "number" && Number.isFinite(record.payout_rate)) {
        allPayoutValues.push(record.payout_rate);
      }
    }

    dateRows.push({
      date,
      recordsBySlot,
      hasSite7Data: Object.values(recordsBySlot).some(isSite7Record),
      site7FetchedAt: latestSite7FetchedAt(Object.values(recordsBySlot)),
    });

    const storedAverageDifference =
      row.average_difference === null || row.average_difference === undefined
        ? null
        : Number(row.average_difference);
    const dailyAverageDifference = Number.isFinite(storedAverageDifference)
      ? storedAverageDifference
      : average(dailyDifferenceValues);
    if (typeof dailyAverageDifference === "number" && Number.isFinite(dailyAverageDifference)) {
      bestWorstCandidates.push({
        date,
        value: dailyAverageDifference,
      });
    }
  }

  const slotNumbers = [...slotNumbersSet].sort(compareSlotNumbers);
  const dates = dateRows.map((row) => row.date);
  bestWorstCandidates.sort((left, right) => right.value - left.value);

  return {
    slotNumbers,
    dateRows,
    summary: {
      slotCount: slotNumbers.length,
      dayCount: dateRows.length,
      recordCount,
      startDate: dates.at(-1) ?? null,
      endDate: dates[0] ?? null,
      averageDifference: average(allDifferenceValues),
      averageGames: average(allGamesValues),
      averagePayout: average(allPayoutValues),
      bestDay: bestWorstCandidates[0] ?? null,
      worstDay: bestWorstCandidates.at(-1) ?? null,
    },
  };
}

function buildMachineDetail(rows) {
  const slots = new Set();
  const slotLabels = {};
  const recordsByDate = new Map();
  const dailyDifferences = new Map();
  const machineNames = [
    ...new Set(
      rows
        .map((row) => String(row.machine_name ?? "").trim())
        .filter(Boolean),
    ),
  ];
  const useMachineSlotLabels = machineNames.length > 1;

  for (const row of rows) {
    const slotKey = useMachineSlotLabels
      ? `${row.machine_name}${SLOT_KEY_SEPARATOR}${row.slot_number}`
      : row.slot_number;
    slots.add(slotKey);
    if (useMachineSlotLabels) {
      slotLabels[slotKey] = getCombinedSlotLabel(row.machine_name, row.slot_number);
    }
    if (!recordsByDate.has(row.target_date)) {
      recordsByDate.set(row.target_date, {});
    }
    recordsByDate.get(row.target_date)[slotKey] = row;

    if (typeof row.difference_value === "number" && Number.isFinite(row.difference_value)) {
      if (!dailyDifferences.has(row.target_date)) {
        dailyDifferences.set(row.target_date, []);
      }
      dailyDifferences.get(row.target_date).push(row.difference_value);
    }
  }

  const slotNumbers = [...slots].sort(compareSlotKeys);
  const dates = [...recordsByDate.keys()].sort((left, right) => right.localeCompare(left));
  const dateRows = dates.map((date) => ({
    date,
    recordsBySlot: recordsByDate.get(date),
    hasSite7Data: Object.values(recordsByDate.get(date) ?? {}).some(isSite7Record),
    site7FetchedAt: latestSite7FetchedAt(Object.values(recordsByDate.get(date) ?? {})),
  }));

  const bestWorstCandidates = [...dailyDifferences.entries()]
    .map(([date, values]) => ({
      date,
      value: average(values),
    }))
    .filter((entry) => typeof entry.value === "number");

  bestWorstCandidates.sort((left, right) => right.value - left.value);

  return {
    slotNumbers,
    slotLabels,
    dateRows,
    summary: {
      slotCount: slotNumbers.length,
      dayCount: dateRows.length,
      recordCount: rows.length,
      startDate: dates.at(-1) ?? null,
      endDate: dates[0] ?? null,
      averageDifference: average(rows.map((row) => row.difference_value)),
      averageGames: average(rows.map((row) => row.games_count)),
      averagePayout: average(rows.map((row) => row.payout_rate)),
      bestDay: bestWorstCandidates[0] ?? null,
      worstDay: bestWorstCandidates.at(-1) ?? null,
    },
  };
}

export const getStoreList = cache(async function getStoreList() {
  const staticIndex = await readStaticWebDataIndex();
  if (staticIndex?.stores.length > 0) {
    return staticIndex.stores
      .map((store) => ({
        id: String(store.id ?? "").trim(),
        storeName: String(store.storeName ?? "").trim(),
        storeUrl: String(store.storeUrl ?? "").trim(),
        prefectureName: String(
          store.prefectureName ?? store.site7Prefecture ?? store.site7_prefecture ?? "",
        ).trim(),
        areaName: String(store.areaName ?? store.site7Area ?? store.site7_area ?? "").trim(),
        dataSource: "json",
        isPendingRegistration: !String(store.storeName ?? "").trim(),
      }))
      .filter((store) => store.id)
      .sort((left, right) => {
        if (left.isPendingRegistration !== right.isPendingRegistration) {
          return left.isPendingRegistration ? 1 : -1;
        }
        const leftLabel = left.isPendingRegistration ? left.storeUrl : left.storeName;
        const rightLabel = right.isPendingRegistration ? right.storeUrl : right.storeName;
        return leftLabel.localeCompare(rightLabel, "ja");
      });
  }

  return [];
});

export const getStoreIdentity = cache(async function getStoreIdentity(storeId) {
  const staticStore = await readStaticStoreById(storeId);
  if (staticStore) {
    const store = readStaticStoreIdentity(staticStore);
    return {
      id: store.id,
      storeName: store.storeName,
      storeUrl: store.storeUrl,
    };
  }

  return null;
});

function readStaticStoreIdentity(staticStore) {
  const store = staticStore?.store && typeof staticStore.store === "object" ? staticStore.store : {};
  return {
    id: String(store.id ?? "").trim(),
    storeName: String(store.storeName ?? "").trim(),
    storeUrl: String(store.storeUrl ?? "").trim(),
    prefectureName: String(
      store.prefectureName ?? store.site7Prefecture ?? store.site7_prefecture ?? "",
    ).trim(),
    areaName: String(store.areaName ?? store.site7Area ?? store.site7_area ?? "").trim(),
    eventFilters: createEventFilters(
      normalizeEventDayTails(store.eventDayTails),
      Boolean(store.eventZoro),
      normalizeEventWeekdays(store.eventWeekdays),
      normalizeEventMonthDays(store.eventMonthDays),
    ),
    eventSourceText: String(store.eventSourceText ?? "").trim(),
  };
}

function readStaticStoreMachineNames(staticStore) {
  return (Array.isArray(staticStore?.machines) ? staticStore.machines : [])
    .map((machine) => String(machine?.machineName ?? "").trim())
    .filter(Boolean);
}

function readStaticStoreRecentMachineNames(staticStore) {
  const machines = (Array.isArray(staticStore?.machines) ? staticStore.machines : [])
    .map((machine) => ({
      machineName: String(machine?.machineName ?? "").trim(),
      latestDate: normalizeDateInput(machine?.latestDate),
    }))
    .filter((machine) => machine.machineName && machine.latestDate);
  const latestDate =
    machines.reduce((currentLatestDate, machine) => {
      if (!machine.latestDate) {
        return currentLatestDate;
      }
      if (currentLatestDate === null || machine.latestDate > currentLatestDate) {
        return machine.latestDate;
      }
      return currentLatestDate;
    }, null) ?? normalizeDateInput(staticStore?.summary?.latestDate);
  const activeStartDate = shiftDateInput(
    latestDate,
    -(HUNT_SCORE_ACTIVE_MACHINE_WINDOW_DAYS - 1),
  );

  if (!latestDate || !activeStartDate) {
    return readStaticStoreMachineNames(staticStore);
  }

  return machines
    .filter((machine) => machine.latestDate >= activeStartDate && machine.latestDate <= latestDate)
    .map((machine) => machine.machineName);
}

function setMachineSlotCount(machineSlotCounts, machineName, rawSlotCount) {
  const normalizedMachineName = String(machineName ?? "").trim();
  const slotCount = Number(rawSlotCount ?? 0);
  if (!normalizedMachineName || !Number.isFinite(slotCount) || slotCount <= 0) {
    return;
  }

  machineSlotCounts[normalizedMachineName] = slotCount;
}

function readMachineSlotCount(machineSlotCounts, machineName) {
  const normalizedMachineName = String(machineName ?? "").trim();
  const slotCount = Number(machineSlotCounts?.[normalizedMachineName] ?? 0);
  return normalizedMachineName && Number.isFinite(slotCount) && slotCount > 0 ? slotCount : null;
}

function readCombinedStaticStoreMachineSlotCount(staticStore, group) {
  const childCanonicalNames = new Set(
    group.machineNames.map((machineName) => canonicalMachineName(machineName)),
  );
  let latestDate = "";
  let total = 0;

  for (const machine of Array.isArray(staticStore?.machines) ? staticStore.machines : []) {
    const canonicalName = canonicalMachineName(machine?.machineName);
    if (!childCanonicalNames.has(canonicalName)) {
      continue;
    }

    const machineLatestDate = normalizeDateInput(machine?.latestDate) ?? "";
    const slotCount = Number(machine?.slotCount ?? 0);
    if (!machineLatestDate || !Number.isFinite(slotCount) || slotCount <= 0) {
      continue;
    }

    if (machineLatestDate > latestDate) {
      latestDate = machineLatestDate;
      total = slotCount;
    } else if (machineLatestDate === latestDate) {
      total += slotCount;
    }
  }

  return total;
}

function buildActiveStaticStoreMachineSlotCountsByCanonicalName(staticStore) {
  const statsByCanonicalName = new Map();

  for (const machine of Array.isArray(staticStore?.machines) ? staticStore.machines : []) {
    const machineName = String(machine?.machineName ?? "").trim();
    const canonicalName = canonicalMachineName(machineName);
    const slotCount = Number(machine?.slotCount ?? 0);
    if (!machineName || !canonicalName || !Number.isFinite(slotCount) || slotCount <= 0) {
      continue;
    }

    const latestDate = normalizeDateInput(machine?.latestDate) ?? "";
    const currentStats = statsByCanonicalName.get(canonicalName);
    if (!currentStats || latestDate > currentStats.latestDate) {
      statsByCanonicalName.set(canonicalName, {
        latestDate,
        slotCount,
      });
      continue;
    }

    if (latestDate === currentStats.latestDate) {
      currentStats.slotCount += slotCount;
    }
  }

  return new Map(
    [...statsByCanonicalName.entries()].map(([canonicalName, stats]) => [
      canonicalName,
      stats.slotCount,
    ]),
  );
}

function buildStaticStoreMachineSlotCounts(staticStore) {
  const store = readStaticStoreIdentity(staticStore);
  const machineSlotCounts = {};
  const activeSlotCountsByCanonicalName =
    buildActiveStaticStoreMachineSlotCountsByCanonicalName(staticStore);

  for (const machine of Array.isArray(staticStore?.machines) ? staticStore.machines : []) {
    const machineName = String(machine?.machineName ?? "").trim();
    const canonicalName = canonicalMachineName(machineName);
    const slotCount = Number(activeSlotCountsByCanonicalName.get(canonicalName) ?? 0);
    if (!machineName || !canonicalName || !Number.isFinite(slotCount) || slotCount <= 0) {
      continue;
    }
    const targetMachineName =
      canonicalHuntScoreTargetMachineName(canonicalName, store.storeName) ?? canonicalName;
    const slotCountNames = new Set([machineName, canonicalName, targetMachineName].filter(Boolean));

    for (const slotCountName of slotCountNames) {
      setMachineSlotCount(machineSlotCounts, slotCountName, slotCount);
    }
  }

  for (const group of COMBINED_MACHINE_GROUPS) {
    setMachineSlotCount(
      machineSlotCounts,
      group.groupName,
      readCombinedStaticStoreMachineSlotCount(staticStore, group),
    );
  }

  return machineSlotCounts;
}

function rawRecordIsInDateRange(record, dateRange) {
  if (!dateRange?.startDate && !dateRange?.endDate) {
    return true;
  }

  const targetDate = normalizeDateInput(record?.target_date);
  if (!targetDate) {
    return false;
  }
  if (dateRange.startDate && targetDate < dateRange.startDate) {
    return false;
  }
  if (dateRange.endDate && targetDate > dateRange.endDate) {
    return false;
  }
  return true;
}

function readStaticStoreRecords(staticStore, dateRange = null) {
  return (Array.isArray(staticStore?.records) ? staticStore.records : [])
    .filter((record) => rawRecordIsInDateRange(record, dateRange))
    .map((record) => ({
      store_id: record.store_id ?? staticStore?.store?.id ?? null,
      machine_name: String(record.machine_name ?? "").trim(),
      target_date: String(record.target_date ?? "").trim(),
      slot_number: String(record.slot_number ?? "").trim(),
      difference_value: readNumber(record.difference_value),
      bonus_difference_value: readNumber(record.bonus_difference_value),
      games_count: readNumber(record.games_count),
      payout_rate: readNumber(record.payout_rate),
      bb_count: readNumber(record.bb_count),
      rb_count: readNumber(record.rb_count),
      setting_estimate_average: readNumber(record.setting_estimate_average),
      setting_estimate_status: String(record.setting_estimate_status ?? "").trim() || null,
      setting_estimate_source: String(record.setting_estimate_source ?? "").trim() || null,
      setting_estimate_version: readNumber(record.setting_estimate_version),
      estimated_difference_value: readNumber(record.estimated_difference_value),
      estimated_difference_status: String(record.estimated_difference_status ?? "").trim() || null,
      estimated_difference_source: String(record.estimated_difference_source ?? "").trim() || null,
      estimated_difference_version: readNumber(record.estimated_difference_version),
      combined_ratio_text: record.combined_ratio_text ?? null,
      bb_ratio_text: record.bb_ratio_text ?? null,
      rb_ratio_text: record.rb_ratio_text ?? null,
      data_source: String(record.data_source ?? "").trim() || null,
      site7_fetched_at: readSite7FetchedAt(record),
    }))
    .filter((record) => record.machine_name && record.target_date && record.slot_number)
    .map(withCanonicalMachineName);
}

function isSite7Record(record) {
  return String(record?.data_source ?? "").trim().toLowerCase() === "site7";
}

function readSite7FetchedAt(record) {
  const fetchedAt = String(record?.site7_fetched_at ?? record?.site7FetchedAt ?? "").trim();
  return fetchedAt || null;
}

function latestSite7FetchedAt(records) {
  return (Array.isArray(records) ? records : [])
    .filter(isSite7Record)
    .map(readSite7FetchedAt)
    .filter(Boolean)
    .sort((left, right) => {
      const leftTime = Date.parse(left);
      const rightTime = Date.parse(right);
      if (Number.isFinite(leftTime) && Number.isFinite(rightTime) && leftTime !== rightTime) {
        return rightTime - leftTime;
      }
      return String(right).localeCompare(String(left), "ja");
    })[0] ?? null;
}

function findStaticMachineEntries(staticStore, machineNames) {
  const machineNameSet = new Set(
    (Array.isArray(machineNames) ? machineNames : [machineNames])
      .flatMap((name) => listEquivalentMachineNames(name))
      .map(canonicalMachineName),
  );

  return (Array.isArray(staticStore?.machines) ? staticStore.machines : [])
    .filter((machine) => machineNameSet.has(canonicalMachineName(machine?.machineName)))
    .map((machine) => ({
      machineName: String(machine.machineName ?? "").trim(),
      dataFile: String(machine.dataFile ?? "").trim(),
    }))
    .filter((machine) => machine.machineName);
}

function buildStaticRecordKey(row) {
  return [
    canonicalMachineName(row?.machine_name),
    String(row?.target_date ?? "").trim(),
    String(row?.slot_number ?? "").trim(),
  ].join("\u0000");
}

async function readStaticMachineRecords(staticStore, machineNames, dateRange = null) {
  const store = readStaticStoreIdentity(staticStore);
  const machineEntries = findStaticMachineEntries(staticStore, machineNames);
  const fallbackMachineNameSet = new Set(
    (Array.isArray(machineNames) ? machineNames : [machineNames])
      .flatMap((name) => listEquivalentMachineNames(name))
      .map(canonicalMachineName),
  );

  const rowGroups = await Promise.all(machineEntries.map(async (machineEntry) => {
    if (!machineEntry.dataFile) {
      return [];
    }
    const payload = await readStaticWebDataPayload(machineEntry.dataFile);
    if (!payload || typeof payload !== "object") {
      return [];
    }

    return readStaticStoreRecords({
      store: staticStore.store,
      records: payload.records,
    }, dateRange);
  }));
  const fallbackRows = readStaticStoreRecords(staticStore, dateRange)
    .filter((row) => fallbackMachineNameSet.has(canonicalMachineName(row.machine_name)))
    .map((row) => ({
      ...row,
      store_id: row.store_id ?? store.id,
    }));
  const rowsByKey = new Map();

  for (const row of [...fallbackRows, ...rowGroups.flat()]) {
    rowsByKey.set(buildStaticRecordKey(row), row);
  }

  return [...rowsByKey.values()];
}

function buildStaticStoreDetail(staticStore) {
  const store = readStaticStoreIdentity(staticStore);
  const machines = (Array.isArray(staticStore?.machines) ? staticStore.machines : [])
    .map((machine) => ({
      machineName: String(machine.machineName ?? "").trim(),
      slotCount: Number(machine.slotCount ?? 0),
      latestDate: machine.latestDate ? String(machine.latestDate) : null,
      latestAverageDifference: readNumber(machine.latestAverageDifference),
      latestAverageGames: readNumber(machine.latestAverageGames),
      latestAveragePayout: readNumber(machine.latestAveragePayout),
      dataFile: String(machine.dataFile ?? "").trim() || null,
    }))
    .filter((machine) => machine.machineName && machine.latestDate);
  const latestDate =
    machines.reduce((currentLatestDate, machine) => {
      if (!machine.latestDate) {
        return currentLatestDate;
      }
      if (currentLatestDate === null || machine.latestDate > currentLatestDate) {
        return machine.latestDate;
      }
      return currentLatestDate;
    }, null) ?? null;

  return {
    dataSource: "json",
    store: {
      id: store.id,
      storeName: store.storeName,
      storeUrl: store.storeUrl,
      prefectureName: store.prefectureName,
      areaName: store.areaName,
    },
    summary: {
      id: store.id,
      storeName: store.storeName,
      storeUrl: store.storeUrl,
      prefectureName: store.prefectureName,
      areaName: store.areaName,
      machineCount: machines.length,
      latestDate,
    },
    machines: withCombinedMachineEntries(machines),
  };
}

function buildInitialBacktestDetail(
  store,
  options = {},
  huntScoreLogicKey = "",
  storeMachineNames = null,
  machineSlotCounts = {},
) {
  const storeName =
    typeof store === "string"
      ? store
      : String(store?.storeName ?? store?.store_name ?? "").trim();
  const machineNames = Array.isArray(storeMachineNames)
    ? listHuntScoreTargetMachineNamesForStoreMachines(storeName, storeMachineNames)
    : listHuntScoreTargetMachineNames(storeName);
  const selectedMachineNames = normalizeInitialMachineSelection(machineNames, options);
  const selectedMachineNameSet = new Set(selectedMachineNames);
  const defaultedOptions = buildBacktestOptionsForStore(store, options);
  const periodMode = defaultedOptions?.periodMode === "range" ? "range" : "recent";
  const {
    rankScope,
    rankFilter,
    machineRankFilter,
    selectedRankFilter,
    hasRankFilter,
  } = buildScopedRankFilters(defaultedOptions);
  const scoreMin = readNumber(defaultedOptions?.scoreMin);
  const nextGapMin = readNumber(defaultedOptions?.nextGapMin);
  const upperGapMax = readNumber(defaultedOptions?.upperGapMax);
  const dailySelectionMode = normalizeDailySelectionMode(defaultedOptions?.dailySelectionMode);
  const usesMachineTopNextGapSelection =
    dailySelectionMode === DAILY_SELECTION_MODE_MACHINE_TOP_NEXT_GAP;
  const nextGapScope =
    defaultedOptions?.nextGapScope === "selected" ||
    defaultedOptions?.nextGapScope === "machine"
      ? defaultedOptions.nextGapScope
      : "machine";
  const baseRequirementOptions = buildConditionRequirementOptions(defaultedOptions, {
    rankRequired: DEFAULT_HUNT_RANK_REQUIRED,
    scoreRequired: DEFAULT_HUNT_SCORE_REQUIRED,
    nextGapRequired: DEFAULT_HUNT_NEXT_GAP_REQUIRED,
    upperGapRequired: DEFAULT_HUNT_NEXT_GAP_REQUIRED,
  });
  const hasScoreFilter = scoreMin !== null;
  const hasNextGapFilter = nextGapMin !== null;
  const hasUpperGapFilter = upperGapMax !== null;
  const requirementOptions = usesMachineTopNextGapSelection
    ? requireActiveConditionFilters(baseRequirementOptions, {
        hasRankFilter,
        hasMachineRankFilter: machineRankFilter.hasRankFilter,
        hasSelectedRankFilter: selectedRankFilter.hasRankFilter,
        hasScoreFilter,
        hasNextGapFilter,
        hasUpperGapFilter,
      })
    : baseRequirementOptions;
  const combineAimJuggler = normalizeEnabledOption(defaultedOptions?.combineAimJuggler, true);
  const combineHanabi = normalizeEnabledOption(defaultedOptions?.combineHanabi, true);

  return {
    periodMode,
    recentDays: readPositiveInteger(defaultedOptions?.recentDays, DEFAULT_HUNT_BACKTEST_RECENT_DAYS),
    huntScoreLogic: getHuntScoreLogicDetail(huntScoreLogicKey, storeName),
    startDate: normalizeDateInput(defaultedOptions?.startDate),
    endDate: normalizeDateInput(defaultedOptions?.endDate),
    latestDate: null,
    earliestDate: null,
    usedFallbackRange: false,
    machineOptions: machineNames.map((machineName) => ({
      name: machineName,
      checked: selectedMachineNameSet.has(machineName),
      slotCount: readMachineSlotCount(machineSlotCounts, machineName),
    })),
    selectedMachineNames,
    rankMin: rankFilter.rankMin,
    rankMax: rankFilter.rankMax,
    machineRankMin: machineRankFilter.rankMin,
    machineRankMax: machineRankFilter.rankMax,
    selectedRankMin: selectedRankFilter.rankMin,
    selectedRankMax: selectedRankFilter.rankMax,
    hasRankFilter,
    hasMachineRankFilter: machineRankFilter.hasRankFilter,
    hasSelectedRankFilter: selectedRankFilter.hasRankFilter,
    scoreMin,
    hasScoreFilter,
    nextGapMin,
    hasNextGapFilter,
    upperGapMax,
    hasUpperGapFilter,
    rankRequired: requirementOptions.rankRequired,
    machineRankRequired: requirementOptions.machineRankRequired,
    selectedRankRequired: requirementOptions.selectedRankRequired,
    scoreRequired: requirementOptions.scoreRequired,
    nextGapRequired: requirementOptions.nextGapRequired,
    upperGapRequired: requirementOptions.upperGapRequired,
    dailySelectionMode,
    rankScope,
    nextGapScope,
    showGraph: defaultedOptions?.showGraph === "off" ? "off" : "on",
    scoreDifferenceMode: normalizeDifferenceMode(defaultedOptions?.scoreDifferenceMode),
    differenceMode: normalizeDifferenceMode(defaultedOptions?.differenceMode),
    combineAimJuggler,
    combineHanabi,
    hasAimJugglerGroupOption:
      machineNames.includes("SアイムジャグラーＥＸ") || machineNames.includes("ネオアイムジャグラーEX"),
    hasHanabiGroupOption: machineNames.includes("新ハナビ") && machineNames.includes("スマスロ ハナビ"),
    eventFilters: {
      dayTails: normalizeEventDayTails(defaultedOptions?.dayTails),
      zoro: Boolean(defaultedOptions?.zoro),
      weekdays: normalizeEventWeekdays(defaultedOptions?.weekdays),
      monthDays: normalizeEventMonthDays(defaultedOptions?.monthDays),
    },
    breakdowns: [],
    targetDateCount: 0,
    matchedDateCount: 0,
    actualRowCount: 0,
    hasMatches: false,
    hasActualResults: false,
    summaries: [],
    graphPoints: [],
    total: {
      slotCount: null,
      averageHuntScore: null,
      actualRowCount: 0,
      differenceTotal: 0,
      gamesTotal: 0,
      averageGames: null,
      bbTotal: 0,
      rbTotal: 0,
      bbProbability: null,
      rbProbability: null,
      combinedProbability: null,
      payoutRate: null,
      averageSetting: null,
    },
  };
}

function buildInitialHuntScoreDetail(staticStore, backtestOptions = {}, huntScoreLogicKey = "") {
  const store = readStaticStoreIdentity(staticStore);
  if (!isHuntScoreTargetStore(store.storeName)) {
    return null;
  }

  const storeDetail = buildStaticStoreDetail(staticStore);
  const storeMachineNames = readStaticStoreRecentMachineNames(staticStore);
  const differenceMode = normalizeDifferenceMode(backtestOptions?.differenceMode);
  const machineNames = listHuntScoreTargetMachineNamesForStoreMachines(
    store.storeName,
    storeMachineNames,
  );
  const machineSlotCounts = buildStaticStoreMachineSlotCounts(staticStore);
  const huntScoreLogic = getHuntScoreLogicDetail(huntScoreLogicKey, store.storeName);
  return {
    dataSource: "json",
    resultRequested: false,
    huntScoreLogic,
    differenceMode,
    store: {
      id: store.id,
      storeName: store.storeName,
      storeUrl: store.storeUrl,
    },
    availableMachineNames: machineNames,
    rankingDates: [],
    rankingDateOptions: [],
    selectedDate: storeDetail.summary.latestDate,
    requestedDate: "",
    limit: DEFAULT_HUNT_RANKING_LIMIT,
    predictionDate: null,
    nextBusinessDate: null,
    machineSlotCounts,
    rows: [],
    rankingGroups: [],
    totalCount: 0,
    hasActualResults: false,
    backtest: buildInitialBacktestDetail(
      store,
      backtestOptions,
      huntScoreLogic.key,
      storeMachineNames,
      machineSlotCounts,
    ),
  };
}

function applySnapshotHuntScores(snapshots) {
  for (const snapshot of Array.isArray(snapshots) ? snapshots : []) {
    for (const row of Array.isArray(snapshot.rows) ? snapshot.rows : []) {
      if (row?.currentRecord && Number.isFinite(row.huntScore)) {
        row.currentRecord.hunt_score = row.huntScore;
      }
    }
  }
}

function snapshotUsesSite7Data(snapshot) {
  return (Array.isArray(snapshot?.rows) ? snapshot.rows : []).some((row) =>
    isSite7Record(row?.currentRecord),
  );
}

function buildSnapshotSite7MachineNameSet(snapshot) {
  return new Set(
    (Array.isArray(snapshot?.rows) ? snapshot.rows : [])
      .filter((row) => isSite7Record(row?.currentRecord))
      .map((row) => String(row?.machineName ?? "").trim())
      .filter(Boolean),
  );
}

function buildSnapshotSite7MachineFetchedAtMap(snapshot) {
  const fetchedAtByMachineName = new Map();
  for (const row of Array.isArray(snapshot?.rows) ? snapshot.rows : []) {
    if (!isSite7Record(row?.currentRecord)) {
      continue;
    }
    const machineName = String(row?.machineName ?? "").trim();
    const fetchedAt = readSite7FetchedAt(row?.currentRecord);
    if (!machineName || !fetchedAt) {
      continue;
    }
    const currentFetchedAt = fetchedAtByMachineName.get(machineName);
    const latestFetchedAt = latestSite7FetchedAt([
      { data_source: "site7", site7_fetched_at: currentFetchedAt },
      { data_source: "site7", site7_fetched_at: fetchedAt },
    ]);
    fetchedAtByMachineName.set(machineName, latestFetchedAt ?? fetchedAt);
  }
  return fetchedAtByMachineName;
}

function decorateRowsWithSite7MachineData(
  rows,
  site7MachineNameSet,
  site7MachineFetchedAtByName = new Map(),
) {
  return (Array.isArray(rows) ? rows : []).map((row) => ({
    ...row,
    predictionMachineHasSite7Data: site7MachineNameSet.has(String(row?.machineName ?? "").trim()),
    predictionMachineSite7FetchedAt: site7MachineFetchedAtByName.get(String(row?.machineName ?? "").trim()) ?? null,
  }));
}

function buildMachineHuntScoreHighlightDetail(
  storeName,
  snapshots,
  storeMachineNames = null,
  machineSlotCounts = {},
) {
  const snapshotMachineNames = [
    ...new Set(
      (Array.isArray(snapshots) ? snapshots : [])
        .flatMap((snapshot) => (Array.isArray(snapshot.rows) ? snapshot.rows : []))
        .map((row) => String(row.machineName ?? "").trim())
        .filter(Boolean),
    ),
  ];
  const sourceMachineNames = Array.isArray(storeMachineNames)
    ? [...new Set([...storeMachineNames, ...snapshotMachineNames])]
    : snapshotMachineNames;
  const availableMachineNames = listHuntScoreTargetMachineNamesForStoreMachines(
    storeName,
    sourceMachineNames,
  );
  return {
    availableMachineNames,
    machineSlotCounts: Object.fromEntries(
      availableMachineNames.map((machineName) => [
        machineName,
        readMachineSlotCount(machineSlotCounts, machineName),
      ]),
    ),
    snapshots: (Array.isArray(snapshots) ? snapshots : []).map((snapshot) => ({
      date: snapshot.baseDate,
      rows: (Array.isArray(snapshot.rows) ? snapshot.rows : []).map((row) => ({
        machineName: String(row.machineName ?? "").trim(),
        slotNumber: String(row.slotNumber ?? "").trim(),
        huntScore: readNumber(row.huntScore),
        rank: readPositiveInteger(row.rank, null),
      })),
    })),
  };
}

function getHuntScoreRecordMachineName(row, storeName) {
  return (
    canonicalHuntScoreTargetMachineName(canonicalMachineName(row?.machine_name), storeName) ??
    canonicalMachineName(row?.machine_name)
  );
}

function buildHuntScoreRecordKey(row, storeName) {
  return [
    String(row?.target_date ?? "").trim(),
    getHuntScoreRecordMachineName(row, storeName),
    String(row?.slot_number ?? "").trim(),
  ].join("\u0000");
}

async function buildStaticHuntScoreSourceRowsForMachineNames(
  staticStore,
  sourceMachineNames,
  dateRange = null,
  options = {},
) {
  const store = readStaticStoreIdentity(staticStore);
  const huntScoreMachineNameSet = new Set(
    (Array.isArray(sourceMachineNames) ? sourceMachineNames : [])
      .flatMap((name) => listEquivalentMachineNames(name))
      .map(canonicalMachineName),
  );
  if (options?.preferMachineRows === true) {
    const targetRows = await readStaticMachineRecords(staticStore, sourceMachineNames, dateRange);
    if (targetRows.length > 0) {
      return {
        targetRows,
        storeRows: targetRows,
      };
    }
  }

  const storeRows = readStaticStoreRecords(staticStore, dateRange);
  if (storeRows.length === 0) {
    const targetRows = await readStaticMachineRecords(staticStore, sourceMachineNames, dateRange);
    return {
      targetRows,
      storeRows: targetRows,
    };
  }

  return {
    targetRows: storeRows.filter((row) =>
      huntScoreMachineNameSet.has(canonicalMachineName(row.machine_name)),
    ),
    storeRows,
  };
}

async function buildStaticHuntScoreSourceRows(staticStore) {
  const store = readStaticStoreIdentity(staticStore);
  return buildStaticHuntScoreSourceRowsForMachineNames(
    staticStore,
    listHuntScoreSourceMachineNamesForStoreMachines(
      store.storeName,
      readStaticStoreRecentMachineNames(staticStore),
    ),
  );
}

async function buildStaticMachineHuntScoreHighlight(
  staticStore,
  huntScoreLogicKey = "",
  differenceMode = undefined,
) {
  const store = readStaticStoreIdentity(staticStore);
  if (!isHuntScoreTargetStore(store.storeName)) {
    return null;
  }

  const { targetRows, storeRows } = await buildStaticHuntScoreSourceRows(staticStore);
  const huntScoreLogic = getHuntScoreLogicDetail(huntScoreLogicKey, store.storeName);
  const snapshots = buildHuntScoreSnapshots(
    targetRows,
    storeRows,
    store.storeName,
    huntScoreLogic.key,
    normalizeDifferenceMode(differenceMode),
  );
  return buildMachineHuntScoreHighlightDetail(
    store.storeName,
    snapshots,
    readStaticStoreRecentMachineNames(staticStore),
    buildStaticStoreMachineSlotCounts(staticStore),
  );
}

async function buildStaticMachineDetail(
  staticStore,
  machineName,
  huntScoreLogicKey = "",
  differenceMode = undefined,
) {
  const store = readStaticStoreIdentity(staticStore);
  const requestedMachineName = canonicalMachineName(machineName);
  const machines = (Array.isArray(staticStore?.machines) ? staticStore.machines : [])
    .map((machine) => ({
      machineName: String(machine.machineName ?? "").trim(),
      latestDate: machine.latestDate ? String(machine.latestDate) : null,
    }))
    .filter((machine) => machine.machineName && machine.latestDate);
  const activeMachineNames = readStaticStoreRecentMachineNames(staticStore);
  const activeHuntScoreMachineNameSet = new Set(
    listHuntScoreTargetMachineNamesForStoreMachines(store.storeName, activeMachineNames)
      .flatMap((name) => [name, canonicalMachineName(name)])
      .filter(Boolean),
  );
  const machinesByCanonicalName = buildMachineSummaryByCanonicalName(machines);
  const requestedCombinedGroup = findCombinedMachineGroup(requestedMachineName);
  const combinedChildMachines = requestedCombinedGroup
    ? getAvailableCombinedMachineGroup(requestedCombinedGroup, machinesByCanonicalName)
    : null;
  const detailMachineName = combinedChildMachines ? requestedCombinedGroup.groupName : requestedMachineName;
  const requestedMachineNames = combinedChildMachines
    ? requestedCombinedGroup.machineNames
    : [requestedMachineName];
  const requestedHuntScoreMachineNames = new Set(
    requestedMachineNames
      .map((name) => canonicalHuntScoreTargetMachineName(name, store.storeName) ?? canonicalMachineName(name))
      .filter(Boolean),
  );
  const requestedHuntScoreMachineName =
    canonicalHuntScoreTargetMachineName(requestedMachineName, store.storeName) ?? requestedMachineName;
  const huntScoreEnabled = combinedChildMachines
    ? requestedMachineNames.some((name) =>
        activeHuntScoreMachineNameSet.has(
          canonicalHuntScoreTargetMachineName(name, store.storeName) ?? canonicalMachineName(name),
        ),
      )
    : activeHuntScoreMachineNameSet.has(canonicalMachineName(requestedHuntScoreMachineName));
  let huntScoreHighlight = null;
  let rows = [];

  if (huntScoreEnabled) {
    const { targetRows, storeRows } = await buildStaticHuntScoreSourceRowsForMachineNames(
      staticStore,
      requestedMachineNames,
    );
    const huntScoreLogic = getHuntScoreLogicDetail(huntScoreLogicKey, store.storeName);
    const snapshots = buildHuntScoreSnapshots(
      targetRows,
      storeRows,
      store.storeName,
      huntScoreLogic.key,
      normalizeDifferenceMode(differenceMode),
    );
    applySnapshotHuntScores(snapshots);
    huntScoreHighlight = buildMachineHuntScoreHighlightDetail(
      store.storeName,
      snapshots,
      activeMachineNames,
      buildStaticStoreMachineSlotCounts(staticStore),
    );
    const targetMachineRows = targetRows.filter((row) =>
      requestedHuntScoreMachineNames.has(getHuntScoreRecordMachineName(row, store.storeName)),
    );
    if (combinedChildMachines) {
      const huntScoreByRecordKey = new Map(
        targetMachineRows
          .map((row) => [buildHuntScoreRecordKey(row, store.storeName), readNumber(row.hunt_score)])
          .filter(([, huntScore]) => Number.isFinite(huntScore)),
      );
      rows = (await readStaticMachineRecords(staticStore, requestedMachineNames)).map((row) => {
        const huntScore = huntScoreByRecordKey.get(buildHuntScoreRecordKey(row, store.storeName));
        return {
          ...row,
          machine_name: getHuntScoreRecordMachineName(row, store.storeName),
          ...(Number.isFinite(huntScore) ? { hunt_score: huntScore } : {}),
        };
      });
    } else {
      rows = targetMachineRows.map((row) => ({
        ...row,
        machine_name: requestedHuntScoreMachineName,
      }));
    }
  } else {
    rows = await readStaticMachineRecords(staticStore, requestedMachineNames);
  }

  if (rows.length === 0) {
    return null;
  }

  const machineDetail = buildMachineDetail(rows);
  return {
    dataSource: "json",
    store: {
      id: store.id,
      storeName: store.storeName,
      storeUrl: store.storeUrl,
      eventFilters: store.eventFilters,
    },
    huntScoreLogic: isHuntScoreTargetStore(store.storeName)
      ? getHuntScoreLogicDetail(huntScoreLogicKey, store.storeName)
      : null,
    differenceMode: normalizeDifferenceMode(differenceMode),
    machineName: detailMachineName,
    slotNumbers: machineDetail.slotNumbers,
    slotLabels: machineDetail.slotLabels,
    dateRows: machineDetail.dateRows,
    summary: machineDetail.summary,
    isCombinedMachineGroup: Boolean(combinedChildMachines),
    childMachineNames: combinedChildMachines?.map((machine) => machine.machineName) ?? [],
    huntScoreHighlight,
  };
}

export async function registerPendingStoreUrl(storeUrl) {
  const normalizedStoreUrl = normalizeStoreUrl(storeUrl);
  const stores = await getStoreList();
  if (stores.some((store) => normalizeStoreUrl(store.storeUrl) === normalizedStoreUrl)) {
    return { status: "exists", storeUrl: normalizedStoreUrl };
  }
  throw new Error("店舗URLの追加はGUIアプリで行ってください。");
}

export const getStoreDetail = cache(async function getStoreDetail(storeId) {
  const staticStore = await readStaticStoreById(storeId);
  if (staticStore) {
    return buildStaticStoreDetail(staticStore);
  }

  return null;
});

export const getMachineDetail = cache(async function getMachineDetail(
  storeId,
  machineName,
  huntScoreLogicKey = "",
  differenceMode = undefined,
) {
  const staticStore = await readStaticStoreById(storeId);
  if (staticStore) {
    return await buildStaticMachineDetail(staticStore, machineName, huntScoreLogicKey, differenceMode);
  }

  return null;
});

export const getMachineHuntScoreHighlight = cache(async function getMachineHuntScoreHighlight(
  storeId,
  huntScoreLogicKey = "",
  differenceMode = undefined,
) {
  const staticStore = await readStaticStoreById(storeId);
  if (staticStore) {
    return await buildStaticMachineHuntScoreHighlight(staticStore, huntScoreLogicKey, differenceMode);
  }

  return null;
});

export const getHuntScoreInitialPageDetail = cache(async function getHuntScoreInitialPageDetail(
  storeId,
  backtestOptions = {},
  huntScoreLogicKey = "",
) {
  const staticStore = await readStaticStoreById(storeId);
  if (staticStore) {
    return buildInitialHuntScoreDetail(staticStore, backtestOptions, huntScoreLogicKey);
  }

  return null;
});

export const getHuntScoreRankingDetail = cache(async function getHuntScoreRankingDetail(
  storeId,
  requestedDate = "",
  requestedLimit = 20,
  huntScoreLogicKey = "",
  differenceMode = undefined,
  machineOptions = {},
) {
  const snapshotDetail = await getHuntScoreSnapshotsForStore(
    storeId,
    huntScoreLogicKey,
    differenceMode,
    {
      ...machineOptions,
      requestedDate,
      targetDateOnly: true,
    },
  );

  if (!snapshotDetail) {
    return null;
  }

  const { store, snapshots } = snapshotDetail;
  const availableMachineNames =
    snapshotDetail.availableMachineNames ?? listHuntScoreTargetMachineNames(store.store_name);
  const rankingMachineNames = snapshotDetail.rankingMachineNames ?? availableMachineNames;
  const rankingDateOptions = snapshotDetail.rankingDateOptions ?? snapshots.map((snapshot) => ({
    date: snapshot.baseDate,
    nextBusinessDate: snapshot.nextBusinessDate ?? null,
    hasSite7Data: snapshotUsesSite7Data(snapshot),
  }));
  const rankingDates = snapshotDetail.rankingDates ?? rankingDateOptions.map((option) => option.date);
  const selectedDate =
    snapshotDetail.selectedDate ?? (rankingDates.includes(requestedDate) ? requestedDate : rankingDates[0] ?? null);
  const snapshot = snapshots.find((entry) => entry.baseDate === selectedDate) ?? null;
  const snapshotRows = decorateRowsWithSite7MachineData(
    snapshot?.rows ?? [],
    buildSnapshotSite7MachineNameSet(snapshot),
    buildSnapshotSite7MachineFetchedAtMap(snapshot),
  );
  const fullRankingGroups = buildSelectedMachineRankingGroups(
    snapshotRows,
    rankingMachineNames,
  );
  const rankingLimit = normalizeRankingLimit(requestedLimit);
  const totalCount = fullRankingGroups.reduce(
    (maxCount, group) => Math.max(maxCount, group.totalCount),
    0,
  );
  const displayLimit = totalCount > 0 ? Math.min(rankingLimit, totalCount) : rankingLimit;
  const rankingGroups = limitRankingGroups(fullRankingGroups, displayLimit);
  const rankingRows = rankingGroups.flatMap((group) => group.rows);

  return {
    dataSource: snapshotDetail.dataSource ?? "json",
    huntScoreLogic: snapshotDetail.huntScoreLogic,
    differenceMode: snapshotDetail.differenceMode,
    store: {
      id: store.id,
      storeName: store.store_name,
      storeUrl: store.store_url,
    },
    availableMachineNames,
    rankingDateOptions,
    rankingDates,
    selectedDate,
    requestedDate,
    limit: displayLimit,
    predictionDate: snapshot?.baseDate ?? null,
    nextBusinessDate: snapshot?.nextBusinessDate ?? null,
    machineSlotCounts: snapshotDetail.machineSlotCounts ?? {},
    predictionHasSite7Data: snapshotUsesSite7Data(snapshot),
    rows: rankingRows,
    rankingGroups,
    totalCount,
    hasActualResults: rankingRows.some((row) => row.nextRecord),
  };
});

async function getHuntScoreSnapshotsForStore(
  storeId,
  huntScoreLogicKey = "",
  differenceMode = undefined,
  machineOptions = {},
) {
  const staticStore = await readStaticStoreById(storeId);
  if (staticStore) {
    const staticIdentity = readStaticStoreIdentity(staticStore);
    if (!isHuntScoreTargetStore(staticIdentity.storeName)) {
      return null;
    }
    const huntScoreLogic = getHuntScoreLogicDetail(huntScoreLogicKey, staticIdentity.storeName);
    const normalizedDifferenceMode = normalizeDifferenceMode(differenceMode);
    const storeMachineNames = readStaticStoreRecentMachineNames(staticStore);
    const availableMachineNames = listHuntScoreTargetMachineNamesForStoreMachines(
      staticIdentity.storeName,
      storeMachineNames,
    );
    const rankingMachineNames = normalizeInitialMachineSelection(availableMachineNames, machineOptions);
    const targetDateOnly = machineOptions?.targetDateOnly === true;
    const requestedDate = String(machineOptions?.requestedDate ?? "").trim();
    const sourceRequestMachineNames = expandCombinedMachineNamesForOptions(
      rankingMachineNames,
      machineOptions,
    );
    const sourceMachineNames = listHuntScoreSourceMachineNamesForStoreMachines(
      staticIdentity.storeName,
      sourceRequestMachineNames,
    );
    const sourceDateRange = targetDateOnly
      ? null
      : buildCrossStoreSourceDateRange(staticStore, sourceMachineNames, machineOptions);
    const targetDateRange = targetDateOnly
      ? null
      : buildBacktestSnapshotDateRange(staticStore, sourceMachineNames, machineOptions);
    const preferMachineRows =
      rankingMachineNames.length > 0 && rankingMachineNames.length < availableMachineNames.length;
    const { targetRows, storeRows } =
      rankingMachineNames.length > 0
        ? await buildStaticHuntScoreSourceRowsForMachineNames(
            staticStore,
            sourceMachineNames,
            sourceDateRange,
            { preferMachineRows },
          )
        : { targetRows: [], storeRows: [] };
    const rankingDateOptions = listHuntScoreRankingDateOptions(targetRows, storeRows);
    const rankingDates = rankingDateOptions.map((option) => option.date);
    const selectedDate = rankingDates.includes(requestedDate)
      ? requestedDate
      : rankingDates[0] ?? null;
    const snapshots = buildHuntScoreSnapshots(
      targetRows,
      storeRows,
      staticIdentity.storeName,
      huntScoreLogic.key,
      normalizedDifferenceMode,
      targetDateOnly ? { targetDate: selectedDate } : { targetDateRange },
    );
    const snapshotMachineNames = [
      ...new Set(
        snapshots
          .flatMap((snapshot) => (Array.isArray(snapshot.rows) ? snapshot.rows : []))
          .map((row) => String(row.machineName ?? "").trim())
          .filter(Boolean),
      ),
    ];
    const detailAvailableMachineNames = listHuntScoreTargetMachineNamesForStoreMachines(
      staticIdentity.storeName,
      [...new Set([...storeMachineNames, ...snapshotMachineNames])],
    );
    return {
      dataSource: "json",
      huntScoreLogic,
      differenceMode: normalizedDifferenceMode,
      availableMachineNames: detailAvailableMachineNames,
      rankingMachineNames,
      rankingDateOptions,
      rankingDates,
      selectedDate,
      machineSlotCounts: buildStaticStoreMachineSlotCounts(staticStore),
      store: {
        id: staticIdentity.id,
        store_name: staticIdentity.storeName,
        store_url: staticIdentity.storeUrl,
        eventFilters: staticIdentity.eventFilters,
      },
      snapshots,
    };
  }

  return null;
}

function normalizeRankingLimit(requestedLimit) {
  return Number.isInteger(requestedLimit) && requestedLimit >= 1 ? requestedLimit : 20;
}

function buildSelectedMachineRankingGroups(rows, selectedMachineNames) {
  const safeSelectedMachineNames = (Array.isArray(selectedMachineNames) ? selectedMachineNames : [])
    .map((value) => String(value ?? "").trim())
    .filter(Boolean);
  const selectedMachineNameSet = new Set(
    safeSelectedMachineNames,
  );

  if (selectedMachineNameSet.size === 0) {
    return [];
  }

  const rowsByMachineName = new Map(safeSelectedMachineNames.map((machineName) => [machineName, []]));
  const selectedRows = (Array.isArray(rows) ? rows : [])
    .filter((row) => selectedMachineNameSet.has(String(row.machineName ?? "").trim()));

  selectedRows.forEach((row, index) => {
    const machineName = String(row.machineName ?? "").trim();
    const machineRows = rowsByMachineName.get(machineName);
    if (!machineRows) {
      return;
    }

    const machineRank = machineRows.length + 1;
    machineRows.push({
      ...row,
      overallRank: row.rank,
      selectedRank: index + 1,
      machineRank,
      rank: machineRank,
    });
  });

  return safeSelectedMachineNames
    .map((machineName) => ({
      machineName,
      totalCount: rowsByMachineName.get(machineName)?.length ?? 0,
      rows: rowsByMachineName.get(machineName) ?? [],
    }))
    .filter((group) => group.totalCount > 0);
}

function limitRankingGroups(rankingGroups, displayLimit) {
  return rankingGroups.map((group) => ({
    ...group,
    limit: Math.min(displayLimit, group.totalCount),
    allRows: group.rows,
    rows: group.rows.slice(0, displayLimit),
  }));
}

function buildBacktestOptionsForStore(store, backtestOptions) {
  const hasRequestedEventFilters =
    backtestOptions?.eventTouched ||
    (Array.isArray(backtestOptions?.dayTails) && backtestOptions.dayTails.length > 0) ||
    Boolean(backtestOptions?.zoro) ||
    (Array.isArray(backtestOptions?.weekdays) && backtestOptions.weekdays.length > 0) ||
    (Array.isArray(backtestOptions?.monthDays) && backtestOptions.monthDays.length > 0);

  if (hasRequestedEventFilters) {
    return backtestOptions;
  }

  const storeEventFilters = store?.eventFilters?.isActive ? store.eventFilters : null;
  const defaultEventFilters =
    storeEventFilters ?? HUNT_BACKTEST_DEFAULT_EVENT_FILTERS[String(store?.store_name ?? store?.storeName ?? "").trim()];
  if (!defaultEventFilters) {
    return backtestOptions;
  }

  return {
    ...backtestOptions,
    dayTails: defaultEventFilters.dayTails,
    zoro: defaultEventFilters.zoro,
    weekdays: defaultEventFilters.weekdays,
    monthDays: defaultEventFilters.monthDays,
  };
}

function normalizeCrossStoreBacktestLimit(value) {
  return Math.min(
    readPositiveInteger(value, DEFAULT_CROSS_STORE_BACKTEST_LIMIT),
    MAX_CROSS_STORE_BACKTEST_LIMIT,
  );
}

function normalizeCrossStoreRankingMetric(value) {
  return value === "differenceTotal" ? "differenceTotal" : "payoutRate";
}

function normalizeCrossStoreRankScope(value, fallbackValue = "selected") {
  return value === "machine" || value === "selected" ? value : fallbackValue;
}

function normalizeCrossStoreInitialMachineSelection(machineNames, options = {}) {
  const machineTouched =
    options?.machineTouched === true ||
    options?.machineTouched === "1" ||
    options?.machineTouched === "true" ||
    options?.machineTouched === "on";
  if (machineTouched) {
    return normalizeInitialMachineSelection(machineNames, options);
  }

  const machineNameSet = new Set(machineNames);
  const defaultMachineNames = DEFAULT_CROSS_STORE_MACHINE_NAMES.filter((machineName) =>
    machineNameSet.has(machineName),
  );
  return defaultMachineNames.length > 0 ? defaultMachineNames : machineNames;
}

function normalizeLocationFilterValues(value, availableValues) {
  const availableValueSet = new Set(availableValues);
  return [...new Set(splitOptionValues(value))]
    .map((entry) => String(entry ?? "").trim())
    .filter((entry) => entry && availableValueSet.has(entry));
}

function buildCrossStoreLocationDetail(storeEntries, options = {}) {
  const locations = (Array.isArray(storeEntries) ? storeEntries : [])
    .map((storeEntry) => {
      const identity = readStaticStoreEntryIdentity(storeEntry);
      const prefectureName = identity.prefectureName || UNKNOWN_PREFECTURE_LABEL;
      const areaName = identity.areaName || UNKNOWN_AREA_LABEL;
      return {
        prefectureName,
        areaName,
        areaKey: `${prefectureName} / ${areaName}`,
      };
    })
    .filter((location) => location.prefectureName);
  const prefectureCounts = new Map();
  const areaCounts = new Map();

  for (const location of locations) {
    prefectureCounts.set(
      location.prefectureName,
      (prefectureCounts.get(location.prefectureName) ?? 0) + 1,
    );
    if (!areaCounts.has(location.areaKey)) {
      areaCounts.set(location.areaKey, {
        key: location.areaKey,
        prefectureName: location.prefectureName,
        areaName: location.areaName,
        count: 0,
      });
    }
    areaCounts.get(location.areaKey).count += 1;
  }

  const prefectureNames = [...prefectureCounts.keys()].sort((left, right) =>
    left.localeCompare(right, "ja"),
  );
  const areaEntries = [...areaCounts.values()].sort(
    (left, right) =>
      left.prefectureName.localeCompare(right.prefectureName, "ja") ||
      left.areaName.localeCompare(right.areaName, "ja"),
  );
  const selectedPrefectures = normalizeLocationFilterValues(
    options?.prefectures,
    prefectureNames,
  );
  const selectedAreaKeys = normalizeLocationFilterValues(
    options?.areaKeys,
    areaEntries.map((entry) => entry.key),
  );
  const selectedPrefectureSet = new Set(selectedPrefectures);
  const selectedAreaKeySet = new Set(selectedAreaKeys);
  const areaGroupsByPrefecture = new Map();

  for (const areaEntry of areaEntries) {
    if (!areaGroupsByPrefecture.has(areaEntry.prefectureName)) {
      areaGroupsByPrefecture.set(areaEntry.prefectureName, {
        prefectureName: areaEntry.prefectureName,
        options: [],
      });
    }
    areaGroupsByPrefecture.get(areaEntry.prefectureName).options.push({
      ...areaEntry,
      checked: selectedAreaKeySet.has(areaEntry.key),
    });
  }

  return {
    selectedPrefectures,
    selectedAreaKeys,
    selectedPrefectureSet,
    selectedAreaKeySet,
    prefectureOptions: prefectureNames.map((prefectureName) => ({
      name: prefectureName,
      count: prefectureCounts.get(prefectureName) ?? 0,
      checked: selectedPrefectureSet.has(prefectureName),
    })),
    areaOptionGroups: [...areaGroupsByPrefecture.values()],
  };
}

function storeEntryMatchesCrossStoreLocation(storeEntry, locationDetail) {
  const identity = readStaticStoreEntryIdentity(storeEntry);
  const prefectureName = identity.prefectureName || UNKNOWN_PREFECTURE_LABEL;
  const areaName = identity.areaName || UNKNOWN_AREA_LABEL;
  const areaKey = `${prefectureName} / ${areaName}`;

  if (
    locationDetail.selectedPrefectureSet.size > 0 &&
    !locationDetail.selectedPrefectureSet.has(prefectureName)
  ) {
    return false;
  }
  if (
    locationDetail.selectedAreaKeySet.size > 0 &&
    !locationDetail.selectedAreaKeySet.has(areaKey)
  ) {
    return false;
  }
  return true;
}

function buildCrossStoreBacktestOptions(options = {}, availableMachineNames = null) {
  const machineNames = Array.isArray(availableMachineNames) && availableMachineNames.length > 0
    ? availableMachineNames
    : DEFAULT_CROSS_STORE_MACHINE_NAMES;
  const selectedMachineNames = normalizeCrossStoreInitialMachineSelection(machineNames, options);
  const selectedMachineNameSet = new Set(selectedMachineNames);
  const hasScopedRankOption =
    hasProvidedOption(options, "machineRankMin") ||
    hasProvidedOption(options, "machineRankMax") ||
    hasProvidedOption(options, "selectedRankMin") ||
    hasProvidedOption(options, "selectedRankMax");
  const defaultRankMin = hasScopedRankOption ? null : 1;
  const defaultRankMax = hasScopedRankOption ? null : 3;
  const scopedRankFilters = buildScopedRankFilters({
    rankMin: readPositiveIntegerOption(options, "rankMin", defaultRankMin),
    rankMax: readPositiveIntegerOption(options, "rankMax", defaultRankMax),
    rankScope: options?.rankScope,
    machineRankMin: readPositiveIntegerOption(options, "machineRankMin", null),
    machineRankMax: readPositiveIntegerOption(options, "machineRankMax", null),
    selectedRankMin: readPositiveIntegerOption(options, "selectedRankMin", null),
    selectedRankMax: readPositiveIntegerOption(options, "selectedRankMax", null),
  });
  const requirementOptions = buildConditionRequirementOptions(options, {
    rankRequired: DEFAULT_HUNT_RANK_REQUIRED,
    machineRankRequired: DEFAULT_HUNT_RANK_REQUIRED,
    selectedRankRequired: DEFAULT_HUNT_RANK_REQUIRED,
    scoreRequired: DEFAULT_HUNT_SCORE_REQUIRED,
    nextGapRequired: DEFAULT_HUNT_NEXT_GAP_REQUIRED,
    upperGapRequired: DEFAULT_HUNT_NEXT_GAP_REQUIRED,
  });
  const nextGapScope = normalizeCrossStoreRankScope(options?.nextGapScope, "machine");

  return {
    periodMode: options?.periodMode === "range" ? "range" : "recent",
    recentDays: readPositiveInteger(options?.recentDays, DEFAULT_CROSS_STORE_BACKTEST_RECENT_DAYS),
    startDate: normalizeDateInput(options?.startDate),
    endDate: normalizeDateInput(options?.endDate),
    machineNames,
    machineOptions: machineNames.map((machineName) => ({
      name: machineName,
      checked: selectedMachineNameSet.has(machineName),
    })),
    selectedMachineNames,
    rankMin: scopedRankFilters.rankFilter.rankMin,
    rankMax: scopedRankFilters.rankFilter.rankMax,
    machineRankMin: scopedRankFilters.machineRankFilter.rankMin,
    machineRankMax: scopedRankFilters.machineRankFilter.rankMax,
    selectedRankMin: scopedRankFilters.selectedRankFilter.rankMin,
    selectedRankMax: scopedRankFilters.selectedRankFilter.rankMax,
    scoreMin: readNumberOption(options, "scoreMin", 70),
    nextGapMin: readNumberOption(options, "nextGapMin", null),
    upperGapMax: readNumberOption(options, "upperGapMax", null),
    rankRequired: requirementOptions.rankRequired,
    machineRankRequired: requirementOptions.machineRankRequired,
    selectedRankRequired: requirementOptions.selectedRankRequired,
    scoreRequired: requirementOptions.scoreRequired,
    nextGapRequired: requirementOptions.nextGapRequired,
    upperGapRequired: requirementOptions.upperGapRequired,
    rankScope: scopedRankFilters.rankScope,
    nextGapScope,
    scoreDifferenceMode: normalizeDifferenceMode(options?.scoreDifferenceMode),
    differenceMode: normalizeDifferenceMode(options?.differenceMode),
    combineAimJuggler: normalizeEnabledOption(options?.combineAimJuggler, true),
    combineHanabi: normalizeEnabledOption(options?.combineHanabi, true),
    eventFilters: {
      dayTails: normalizeEventDayTails(options?.dayTails),
      zoro: Boolean(options?.zoro),
      weekdays: normalizeEventWeekdays(options?.weekdays),
      monthDays: normalizeEventMonthDays(options?.monthDays),
    },
    minActualRows: readNonNegativeInteger(
      options?.minActualRows,
      DEFAULT_CROSS_STORE_MIN_ACTUAL_ROWS,
    ),
    minMatchedDateCount: readNonNegativeInteger(
      options?.minMatchedDateCount,
      DEFAULT_CROSS_STORE_MIN_MATCHED_DATES,
    ),
    minSlotCount: readOptionalNonNegativeInteger(options?.minSlotCount),
    maxSlotCount: readOptionalNonNegativeInteger(options?.maxSlotCount),
    limit: normalizeCrossStoreBacktestLimit(options?.limit),
    rankingMetric: normalizeCrossStoreRankingMetric(options?.rankingMetric),
  };
}

async function mapWithConcurrency(items, concurrency, mapper) {
  const safeItems = Array.isArray(items) ? items : [];
  const results = new Array(safeItems.length);
  let nextIndex = 0;
  const workerCount = Math.max(1, Math.min(concurrency, safeItems.length));

  await Promise.all(
    Array.from({ length: workerCount }, async () => {
      while (nextIndex < safeItems.length) {
        const currentIndex = nextIndex;
        nextIndex += 1;
        results[currentIndex] = await mapper(safeItems[currentIndex], currentIndex);
      }
    }),
  );

  return results;
}

function sortHuntScoreMachineNamesWithDefaults(machineNames) {
  const names = [...new Set((Array.isArray(machineNames) ? machineNames : [])
    .map((machineName) => String(machineName ?? "").trim())
    .filter(Boolean))];
  const nameSet = new Set(names);
  const defaultNames = DEFAULT_CROSS_STORE_MACHINE_NAMES.filter((machineName) =>
    nameSet.has(machineName),
  );
  const otherNames = names
    .filter((machineName) => !DEFAULT_CROSS_STORE_MACHINE_NAMES.includes(machineName))
    .sort((left, right) => left.localeCompare(right, "ja", { numeric: true }));
  return [...defaultNames, ...otherNames];
}

async function listCrossStoreRecentHuntScoreMachineNames(storeEntries) {
  const storeMachineNameLists = await mapWithConcurrency(
    storeEntries,
    CROSS_STORE_BACKTEST_CONCURRENCY,
    async (storeEntry) => {
      try {
        const staticStore = await readStaticStoreByEntry(storeEntry);
        if (!staticStore) {
          return [];
        }
        const store = readStaticStoreIdentity(staticStore);
        if (!isHuntScoreTargetStore(store.storeName)) {
          return [];
        }
        return listHuntScoreTargetMachineNamesForStoreMachines(
          store.storeName,
          readStaticStoreRecentMachineNames(staticStore),
        );
      } catch {
        return [];
      }
    },
  );

  return sortHuntScoreMachineNamesWithDefaults(storeMachineNameLists.flat());
}

function storeEntryHasBacktestData(storeEntry) {
  const identity = readStaticStoreEntryIdentity(storeEntry);
  return Boolean(identity.id && identity.storeName && storeEntry?.dataFile);
}

function calculateStaticStoreSlotCountForMachineNames(staticStore, machineNames, options = {}) {
  const targetMachineNameSet = new Set(
    (Array.isArray(machineNames) ? machineNames : [])
      .flatMap((name) => listEquivalentMachineNames(name))
      .map(canonicalMachineName),
  );
  const activeSlotCountsByCanonicalName =
    buildActiveStaticStoreMachineSlotCountsByCanonicalName(staticStore);
  let total = 0;

  for (const group of COMBINED_MACHINE_GROUPS) {
    const optionKey = String(group.optionKey ?? "").trim();
    if (!optionKey || !normalizeEnabledOption(options?.[optionKey], true)) {
      continue;
    }
    const groupMatches = group.machineNames.some((machineName) =>
      targetMachineNameSet.has(canonicalMachineName(machineName)),
    );
    if (!groupMatches) {
      continue;
    }

    total += readCombinedStaticStoreMachineSlotCount(staticStore, group);
    for (const machineName of group.machineNames) {
      targetMachineNameSet.delete(canonicalMachineName(machineName));
    }
  }

  for (const canonicalName of targetMachineNameSet) {
    const slotCount = Number(activeSlotCountsByCanonicalName.get(canonicalName) ?? 0);
    if (Number.isFinite(slotCount) && slotCount > 0) {
      total += slotCount;
    }
  }

  return total;
}

function slotCountMatchesCrossStoreRange(slotCount, backtestOptions) {
  if (
    backtestOptions.minSlotCount !== null &&
    Number(slotCount ?? 0) < backtestOptions.minSlotCount
  ) {
    return false;
  }
  if (
    backtestOptions.maxSlotCount !== null &&
    Number(slotCount ?? 0) > backtestOptions.maxSlotCount
  ) {
    return false;
  }
  return true;
}

function readStaticStoreLatestDateForMachines(staticStore, machineNames) {
  const machineNameSet = new Set(
    (Array.isArray(machineNames) ? machineNames : [])
      .flatMap((name) => listEquivalentMachineNames(name))
      .map(canonicalMachineName),
  );
  const machineLatestDates = (Array.isArray(staticStore?.machines) ? staticStore.machines : [])
    .filter((machine) => machineNameSet.has(canonicalMachineName(machine?.machineName)))
    .map((machine) => normalizeDateInput(machine?.latestDate))
    .filter(Boolean)
    .sort((left, right) => right.localeCompare(left));
  return (
    machineLatestDates[0] ??
    normalizeDateInput(staticStore?.summary?.latestDate) ??
    null
  );
}

function buildCrossStoreSourceDateRange(staticStore, sourceMachineNames, backtestOptions) {
  const latestDate = readStaticStoreLatestDateForMachines(staticStore, sourceMachineNames);
  if (!latestDate) {
    return null;
  }

  if (backtestOptions.periodMode === "range") {
    let startDate = backtestOptions.startDate;
    let endDate = backtestOptions.endDate;

    if (startDate && !endDate) {
      endDate = startDate;
    } else if (!startDate && endDate) {
      startDate = endDate;
    }

    if (startDate && endDate) {
      return {
        startDate: shiftDateInput(
          startDate <= endDate ? startDate : endDate,
          -CROSS_STORE_BACKTEST_WINDOW_BUFFER_DAYS,
        ),
        endDate: shiftDateInput(
          startDate <= endDate ? endDate : startDate,
          CROSS_STORE_BACKTEST_NEXT_RESULT_BUFFER_DAYS,
        ),
      };
    }
  }

  const fallbackStartDate = shiftDateInput(
    latestDate,
    -(backtestOptions.recentDays + CROSS_STORE_BACKTEST_WINDOW_BUFFER_DAYS),
  );
  return {
    startDate: fallbackStartDate,
    endDate: shiftDateInput(latestDate, CROSS_STORE_BACKTEST_NEXT_RESULT_BUFFER_DAYS),
  };
}

function buildBacktestSnapshotDateRange(staticStore, sourceMachineNames, backtestOptions) {
  const latestDate = readStaticStoreLatestDateForMachines(staticStore, sourceMachineNames);
  if (!latestDate) {
    return null;
  }

  if (backtestOptions.periodMode === "range") {
    let startDate = backtestOptions.startDate;
    let endDate = backtestOptions.endDate;

    if (startDate && !endDate) {
      endDate = startDate;
    } else if (!startDate && endDate) {
      startDate = endDate;
    }

    if (startDate && endDate) {
      return {
        startDate: startDate <= endDate ? startDate : endDate,
        endDate: startDate <= endDate ? endDate : startDate,
      };
    }
  }

  return {
    startDate: shiftDateInput(latestDate, -(backtestOptions.recentDays - 1)),
    endDate: latestDate,
  };
}

function filterRowsByDateRange(rows, dateRange) {
  if (!dateRange?.startDate && !dateRange?.endDate) {
    return rows;
  }

  return (Array.isArray(rows) ? rows : []).filter((row) => {
    const targetDate = normalizeDateInput(row?.target_date);
    if (!targetDate) {
      return false;
    }
    if (dateRange.startDate && targetDate < dateRange.startDate) {
      return false;
    }
    if (dateRange.endDate && targetDate > dateRange.endDate) {
      return false;
    }
    return true;
  });
}

function buildCrossStoreBacktestRow(store, backtest, slotCount) {
  const total = backtest.total ?? {};
  const actualRowCount = Number(backtest.actualRowCount ?? total.actualRowCount ?? 0);
  const differenceTotal = readNumber(total.differenceTotal) ?? 0;
  const averageDifference = actualRowCount > 0 ? differenceTotal / actualRowCount : null;
  const nonmatchingSummary = total.nonmatchingSummary ?? null;
  const nonmatchingActualRowCount = Number(nonmatchingSummary?.actualRowCount ?? 0);
  const nonmatchingDifferenceTotal = readNumber(nonmatchingSummary?.differenceTotal) ?? 0;
  const nonmatchingAverageDifference =
    nonmatchingActualRowCount > 0 ? nonmatchingDifferenceTotal / nonmatchingActualRowCount : null;

  return {
    store: {
      id: store.id,
      storeName: store.storeName,
      storeUrl: store.storeUrl,
      prefectureName: store.prefectureName,
      areaName: store.areaName,
    },
    selectedMachineNames: backtest.selectedMachineNames,
    selectedMachineCount: backtest.selectedMachineNames.length,
    slotCount,
    targetDateCount: backtest.targetDateCount,
    matchedDateCount: backtest.matchedDateCount,
    actualRowCount,
    payoutRate: total.payoutRate,
    averageDifference,
    differenceTotal,
    gamesTotal: total.gamesTotal,
    averageGames: total.averageGames,
    averageSetting: total.averageSetting,
    setting35PlusRate: total.setting35PlusRate,
    setting4PlusRate: total.setting4PlusRate,
    setting45PlusRate: total.setting45PlusRate,
    setting5PlusRate: total.setting5PlusRate,
    winRate: total.winRate,
    averageHuntScore: total.averageHuntScore,
    averageUpperGap: total.averageUpperGap,
    averageNextGap: total.averageNextGap,
    nonmatchingSummary: nonmatchingSummary
      ? {
          ...nonmatchingSummary,
          averageDifference: nonmatchingAverageDifference,
        }
      : null,
    bbTotal: total.bbTotal,
    rbTotal: total.rbTotal,
    bbProbability: total.bbProbability,
    rbProbability: total.rbProbability,
    combinedProbability: total.combinedProbability,
  };
}

function compareCrossStoreBacktestRows(left, right, rankingMetric) {
  if (rankingMetric === "differenceTotal") {
    return (
      Number(right.differenceTotal ?? 0) - Number(left.differenceTotal ?? 0) ||
      Number(right.payoutRate ?? 0) - Number(left.payoutRate ?? 0) ||
      Number(right.actualRowCount ?? 0) - Number(left.actualRowCount ?? 0) ||
      left.store.storeName.localeCompare(right.store.storeName, "ja")
    );
  }

  return (
    Number(right.payoutRate ?? 0) - Number(left.payoutRate ?? 0) ||
    Number(right.actualRowCount ?? 0) - Number(left.actualRowCount ?? 0) ||
    Number(right.differenceTotal ?? 0) - Number(left.differenceTotal ?? 0) ||
    left.store.storeName.localeCompare(right.store.storeName, "ja")
  );
}

async function buildCrossStoreBacktestRowFromEntry(storeEntry, backtestOptions, huntScoreLogic) {
  try {
    const staticStore = await readStaticStoreByEntry(storeEntry);
    if (!staticStore) {
      return null;
    }

    const store = readStaticStoreIdentity(staticStore);
    if (!store.id || !store.storeName || !isHuntScoreTargetStore(store.storeName)) {
      return null;
    }

    const sourceRequestMachineNames = expandCombinedMachineNamesForOptions(
      backtestOptions.selectedMachineNames,
      backtestOptions,
    );
    const slotCount = calculateStaticStoreSlotCountForMachineNames(
      staticStore,
      sourceRequestMachineNames,
      backtestOptions,
    );
    if (slotCount <= 0 || !slotCountMatchesCrossStoreRange(slotCount, backtestOptions)) {
      return null;
    }

    const storeMachineNames = readStaticStoreRecentMachineNames(staticStore);
    const storeHuntScoreMachineNames = listHuntScoreTargetMachineNamesForStoreMachines(
      store.storeName,
      storeMachineNames,
    );
    const sourceMachineNames = listHuntScoreSourceMachineNamesForStoreMachines(
      store.storeName,
      sourceRequestMachineNames,
    );
    const sourceDateRange = buildCrossStoreSourceDateRange(
      staticStore,
      sourceMachineNames,
      backtestOptions,
    );
    const { targetRows, storeRows } = await buildStaticHuntScoreSourceRowsForMachineNames(
      staticStore,
      sourceMachineNames,
      sourceDateRange,
      {
        preferMachineRows:
          backtestOptions.selectedMachineNames.length > 0 &&
          backtestOptions.selectedMachineNames.length < storeHuntScoreMachineNames.length,
      },
    );
    const targetRowsInRange = filterRowsByDateRange(targetRows, sourceDateRange);
    const storeRowsInRange = filterRowsByDateRange(storeRows, sourceDateRange);
    if (targetRowsInRange.length === 0 || storeRowsInRange.length === 0) {
      return null;
    }

    const snapshots = buildHuntScoreSnapshots(
      targetRowsInRange,
      storeRowsInRange,
      store.storeName,
      huntScoreLogic.key,
      backtestOptions.scoreDifferenceMode,
    );
    const backtest = buildHuntScoreBacktestDetail(snapshots, {
      periodMode: backtestOptions.periodMode,
      recentDays: backtestOptions.recentDays,
      startDate: backtestOptions.startDate,
      endDate: backtestOptions.endDate,
      machineNames: backtestOptions.selectedMachineNames,
      machineTouched: true,
      rankMin: backtestOptions.rankMin,
      rankMax: backtestOptions.rankMax,
      machineRankMin: backtestOptions.machineRankMin,
      machineRankMax: backtestOptions.machineRankMax,
      selectedRankMin: backtestOptions.selectedRankMin,
      selectedRankMax: backtestOptions.selectedRankMax,
      scoreMin: backtestOptions.scoreMin,
      nextGapMin: backtestOptions.nextGapMin,
      upperGapMax: backtestOptions.upperGapMax,
      rankRequired: backtestOptions.rankRequired,
      machineRankRequired: backtestOptions.machineRankRequired,
      selectedRankRequired: backtestOptions.selectedRankRequired,
      scoreRequired: backtestOptions.scoreRequired,
      nextGapRequired: backtestOptions.nextGapRequired,
      upperGapRequired: backtestOptions.upperGapRequired,
      rankScope: backtestOptions.rankScope,
      nextGapScope: backtestOptions.nextGapScope,
      scoreDifferenceMode: backtestOptions.scoreDifferenceMode,
      differenceMode: backtestOptions.differenceMode,
      combineAimJuggler: backtestOptions.combineAimJuggler,
      combineHanabi: backtestOptions.combineHanabi,
      dayTails: backtestOptions.eventFilters.dayTails,
      zoro: backtestOptions.eventFilters.zoro,
      weekdays: backtestOptions.eventFilters.weekdays,
      monthDays: backtestOptions.eventFilters.monthDays,
      machineOrder: storeHuntScoreMachineNames,
    });
    const payoutRate = readNumber(backtest.total?.payoutRate);
    if (
      backtest.selectedMachineNames.length === 0 ||
      backtest.actualRowCount < backtestOptions.minActualRows ||
      backtest.matchedDateCount < backtestOptions.minMatchedDateCount ||
      !Number.isFinite(payoutRate)
    ) {
      return null;
    }

    return buildCrossStoreBacktestRow(store, backtest, slotCount);
  } catch {
    return null;
  }
}

export async function getCrossStoreBacktestDetail(options = {}) {
  const index = await readStaticWebDataIndex();
  const allStoreEntries = (index?.stores ?? []).filter(storeEntryHasBacktestData);
  const locationDetail = buildCrossStoreLocationDetail(allStoreEntries, options);
  const storeEntries = allStoreEntries.filter((storeEntry) =>
    storeEntryMatchesCrossStoreLocation(storeEntry, locationDetail),
  );
  const machineNames = await listCrossStoreRecentHuntScoreMachineNames(storeEntries);
  const backtestOptions = buildCrossStoreBacktestOptions(options, machineNames);
  const huntScoreLogic = getHuntScoreLogicDetail(options?.logicKey, "");
  let rows = [];

  if (options?.resultRequested) {
    const evaluatedRows = await mapWithConcurrency(
      storeEntries,
      CROSS_STORE_BACKTEST_CONCURRENCY,
      (storeEntry) => buildCrossStoreBacktestRowFromEntry(storeEntry, backtestOptions, huntScoreLogic),
    );
    rows = evaluatedRows
      .filter(Boolean)
      .sort((left, right) =>
        compareCrossStoreBacktestRows(left, right, backtestOptions.rankingMetric),
      )
      .slice(0, backtestOptions.limit)
      .map((row, index) => ({
        ...row,
        rank: index + 1,
      }));
  }

  return {
    dataSource: "json",
    resultRequested: Boolean(options?.resultRequested),
    huntScoreLogic,
    logicOptions: listHuntScoreLogicOptions(),
    prefectureOptions: locationDetail.prefectureOptions,
    areaOptionGroups: locationDetail.areaOptionGroups,
    selectedPrefectures: locationDetail.selectedPrefectures,
    selectedAreaKeys: locationDetail.selectedAreaKeys,
    periodMode: backtestOptions.periodMode,
    recentDays: backtestOptions.recentDays,
    startDate: backtestOptions.startDate,
    endDate: backtestOptions.endDate,
    machineOptions: backtestOptions.machineOptions,
    selectedMachineNames: backtestOptions.selectedMachineNames,
    rankMin: backtestOptions.rankMin,
    rankMax: backtestOptions.rankMax,
    machineRankMin: backtestOptions.machineRankMin,
    machineRankMax: backtestOptions.machineRankMax,
    selectedRankMin: backtestOptions.selectedRankMin,
    selectedRankMax: backtestOptions.selectedRankMax,
    scoreMin: backtestOptions.scoreMin,
    nextGapMin: backtestOptions.nextGapMin,
    upperGapMax: backtestOptions.upperGapMax,
    rankRequired: backtestOptions.rankRequired,
    machineRankRequired: backtestOptions.machineRankRequired,
    selectedRankRequired: backtestOptions.selectedRankRequired,
    scoreRequired: backtestOptions.scoreRequired,
    nextGapRequired: backtestOptions.nextGapRequired,
    upperGapRequired: backtestOptions.upperGapRequired,
    rankScope: backtestOptions.rankScope,
    nextGapScope: backtestOptions.nextGapScope,
    scoreDifferenceMode: backtestOptions.scoreDifferenceMode,
    differenceMode: backtestOptions.differenceMode,
    combineAimJuggler: backtestOptions.combineAimJuggler,
    combineHanabi: backtestOptions.combineHanabi,
    hasAimJugglerGroupOption: true,
    hasHanabiGroupOption: true,
    eventFilters: backtestOptions.eventFilters,
    minActualRows: backtestOptions.minActualRows,
    minMatchedDateCount: backtestOptions.minMatchedDateCount,
    minSlotCount: backtestOptions.minSlotCount,
    maxSlotCount: backtestOptions.maxSlotCount,
    limit: backtestOptions.limit,
    rankingMetric: backtestOptions.rankingMetric,
    scannedStoreCount: storeEntries.length,
    rankedStoreCount: rows.length,
    rows,
  };
}

export async function getHuntScoreAnalysisPageDetail(
  storeId,
  requestedDate = "",
  requestedLimit = 20,
  backtestOptions = {},
  huntScoreLogicKey = "",
) {
  const normalizedBacktestOptions = buildBacktestOptionsForStore(null, backtestOptions);
  const snapshotDetail = await getHuntScoreSnapshotsForStore(
    storeId,
    huntScoreLogicKey,
    normalizedBacktestOptions.scoreDifferenceMode,
    normalizedBacktestOptions,
  );

  if (!snapshotDetail) {
    return null;
  }

  const { store, snapshots } = snapshotDetail;
  const rankingDateOptions = snapshots.map((snapshot) => ({
    date: snapshot.baseDate,
    nextBusinessDate: snapshot.nextBusinessDate ?? null,
    hasSite7Data: snapshotUsesSite7Data(snapshot),
  }));
  const rankingDates = snapshots.map((snapshot) => snapshot.baseDate);
  const selectedDate = rankingDates.includes(requestedDate) ? requestedDate : rankingDates[0] ?? null;
  const snapshot = snapshots.find((entry) => entry.baseDate === selectedDate) ?? null;
  const snapshotRows = decorateRowsWithSite7MachineData(
    snapshot?.rows ?? [],
    buildSnapshotSite7MachineNameSet(snapshot),
    buildSnapshotSite7MachineFetchedAtMap(snapshot),
  );
  const backtest = {
    ...buildHuntScoreBacktestDetail(snapshots, {
      ...buildBacktestOptionsForStore(store, normalizedBacktestOptions),
      scoreDifferenceMode: normalizedBacktestOptions.scoreDifferenceMode,
      machineSlotCounts: snapshotDetail.machineSlotCounts,
      machineOrder:
        snapshotDetail.availableMachineNames ?? listHuntScoreTargetMachineNames(store.store_name),
    }),
    huntScoreLogic: snapshotDetail.huntScoreLogic,
  };
  const fullRankingGroups = buildSelectedMachineRankingGroups(snapshotRows, backtest.selectedMachineNames);
  const rankingLimit = normalizeRankingLimit(requestedLimit);
  const totalCount = fullRankingGroups.reduce(
    (maxCount, group) => Math.max(maxCount, group.totalCount),
    0,
  );
  const displayLimit = totalCount > 0 ? Math.min(rankingLimit, totalCount) : rankingLimit;
  const rankingGroups = limitRankingGroups(fullRankingGroups, displayLimit);
  const rankingRows = rankingGroups.flatMap((group) => group.rows);

  return {
    dataSource: snapshotDetail.dataSource ?? "json",
    huntScoreLogic: snapshotDetail.huntScoreLogic,
    differenceMode: snapshotDetail.differenceMode,
    store: {
      id: store.id,
      storeName: store.store_name,
      storeUrl: store.store_url,
    },
    rankingDateOptions,
    rankingDates,
    selectedDate,
    requestedDate,
    limit: displayLimit,
    predictionDate: snapshot?.baseDate ?? null,
    nextBusinessDate: snapshot?.nextBusinessDate ?? null,
    machineSlotCounts: snapshotDetail.machineSlotCounts ?? {},
    predictionHasSite7Data: snapshotUsesSite7Data(snapshot),
    rows: rankingRows,
    rankingGroups,
    totalCount,
    hasActualResults: rankingRows.some((row) => row.nextRecord),
    backtest,
  };
}

export async function updateStoreEventSettings(storeId, eventSettings) {
  const dayTails = normalizeEventDayTails(eventSettings?.dayTails);
  const zoro = Boolean(eventSettings?.zoro);
  const weekdays = normalizeEventWeekdays(eventSettings?.weekdays);
  const monthDays = normalizeEventMonthDays(eventSettings?.monthDays);
  return createEventFilters(dayTails, zoro, weekdays, monthDays);
}

export function readRouteSegment(value) {
  if (typeof value !== "string") {
    return "";
  }

  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

export { matchesEventFilters, parseEventFilters } from "./event-filters";
