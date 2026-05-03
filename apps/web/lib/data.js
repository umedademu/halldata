import { cache } from "react";

import { createEventFilters } from "./event-filters";
import { buildHuntScoreBacktestDetail } from "./hunt-backtest";
import {
  attachHuntScores,
  buildHuntScoreSnapshots,
  canonicalHuntScoreTargetMachineName,
  isHuntScoreSupported,
  isHuntScoreTargetStore,
  listHuntScoreSourceMachineNames,
  listHuntScoreTargetMachineNames,
} from "./hunt-score";
import { canonicalMachineName, listEquivalentMachineNames, withCalculatedDifferenceValue } from "./machine-difference";

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

async function readStaticStoreById(storeId) {
  const index = await readStaticWebDataIndex();
  const storeEntry = index?.stores.find((entry) => staticStoreMatchesId(entry, storeId));
  if (!index || !storeEntry?.dataFile) {
    return null;
  }

  const payload = await readStaticWebDataPayload(String(storeEntry.dataFile));
  return payload && typeof payload === "object" ? payload : null;
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

function normalizeDateInput(value) {
  const text = String(value ?? "").trim();
  return /^\d{4}-\d{2}-\d{2}$/u.test(text) ? text : null;
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
  return ["difference_value", "games_count", "bb_count", "rb_count"].some((key) =>
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
        games_count: record?.games_count ?? null,
        payout_rate: record?.payout_rate ?? null,
        bb_count: record?.bb_count ?? null,
        rb_count: record?.rb_count ?? null,
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
        withCalculatedDifferenceValue,
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
        "store_id,machine_name,target_date,slot_number,difference_value,games_count,payout_rate,bb_count,rb_count,combined_ratio_text,bb_ratio_text,rb_ratio_text",
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
    targetRows: fetchedTargetRows.map(withCalculatedDifferenceValue),
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

function buildEventFiltersFromStore(store) {
  return createEventFilters(
    normalizeEventDayTails(store?.event_day_tails),
    Boolean(store?.event_zoro),
    normalizeEventWeekdays(store?.event_weekdays),
  );
}

async function fetchStoreEventRows(storesTable, storeId) {
  try {
    return await fetchAllRows(storesTable, {
      select: "id,store_name,store_url,event_day_tails,event_zoro,event_weekdays",
      id: `eq.${storeId}`,
    });
  } catch (error) {
    if (!(error instanceof Error) || !error.message.includes("(400)")) {
      throw error;
    }
    return fetchAllRows(storesTable, {
      select: "id,store_name,store_url,event_day_tails,event_zoro",
      id: `eq.${storeId}`,
    });
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
      const record = withCalculatedDifferenceValue({
        machine_name: machineName,
        target_date: date,
        slot_number: slotNumber,
        difference_value: sourceRecord.difference_value ?? null,
        games_count: sourceRecord.games_count ?? null,
        payout_rate: sourceRecord.payout_rate ?? null,
        bb_count: sourceRecord.bb_count ?? null,
        rb_count: sourceRecord.rb_count ?? null,
        combined_ratio_text: sourceRecord.combined_ratio_text ?? null,
        bb_ratio_text: sourceRecord.bb_ratio_text ?? null,
        rb_ratio_text: sourceRecord.rb_ratio_text ?? null,
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
  const recordsByDate = new Map();
  const dailyDifferences = new Map();

  for (const row of rows) {
    slots.add(row.slot_number);
    if (!recordsByDate.has(row.target_date)) {
      recordsByDate.set(row.target_date, {});
    }
    recordsByDate.get(row.target_date)[row.slot_number] = row;

    if (typeof row.difference_value === "number" && Number.isFinite(row.difference_value)) {
      if (!dailyDifferences.has(row.target_date)) {
        dailyDifferences.set(row.target_date, []);
      }
      dailyDifferences.get(row.target_date).push(row.difference_value);
    }
  }

  const slotNumbers = [...slots].sort(compareSlotNumbers);
  const dates = [...recordsByDate.keys()].sort((left, right) => right.localeCompare(left));
  const dateRows = dates.map((date) => ({
    date,
    recordsBySlot: recordsByDate.get(date),
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
    eventFilters: createEventFilters(
      normalizeEventDayTails(store.eventDayTails),
      Boolean(store.eventZoro),
      normalizeEventWeekdays(store.eventWeekdays),
    ),
  };
}

function readStaticStoreRecords(staticStore) {
  return (Array.isArray(staticStore?.records) ? staticStore.records : [])
    .map((record) => ({
      store_id: record.store_id ?? staticStore?.store?.id ?? null,
      machine_name: String(record.machine_name ?? "").trim(),
      target_date: String(record.target_date ?? "").trim(),
      slot_number: String(record.slot_number ?? "").trim(),
      difference_value: readNumber(record.difference_value),
      games_count: readNumber(record.games_count),
      payout_rate: readNumber(record.payout_rate),
      bb_count: readNumber(record.bb_count),
      rb_count: readNumber(record.rb_count),
      combined_ratio_text: record.combined_ratio_text ?? null,
      bb_ratio_text: record.bb_ratio_text ?? null,
      rb_ratio_text: record.rb_ratio_text ?? null,
      data_source: record.data_source ?? null,
    }))
    .filter((record) => record.machine_name && record.target_date && record.slot_number)
    .map(withCalculatedDifferenceValue);
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

async function readStaticMachineRecords(staticStore, machineNames) {
  const store = readStaticStoreIdentity(staticStore);
  const machineEntries = findStaticMachineEntries(staticStore, machineNames);

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
    });
  }));
  const rows = rowGroups.flat();

  if (rows.length > 0) {
    return rows;
  }

  const fallbackMachineNameSet = new Set(
    (Array.isArray(machineNames) ? machineNames : [machineNames])
      .flatMap((name) => listEquivalentMachineNames(name))
      .map(canonicalMachineName),
  );
  return readStaticStoreRecords(staticStore)
    .filter((row) => fallbackMachineNameSet.has(canonicalMachineName(row.machine_name)))
    .map((row) => ({
      ...row,
      store_id: row.store_id ?? store.id,
    }));
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
    },
    summary: {
      id: store.id,
      storeName: store.storeName,
      storeUrl: store.storeUrl,
      machineCount: machines.length,
      latestDate,
    },
    machines,
  };
}

function buildInitialBacktestDetail(storeName, options = {}) {
  const machineNames = listHuntScoreTargetMachineNames(storeName);
  const selectedMachineNames = normalizeInitialMachineSelection(machineNames, options);
  const selectedMachineNameSet = new Set(selectedMachineNames);
  const defaultedOptions = buildBacktestOptionsForStore({ store_name: storeName }, options);
  const periodMode = defaultedOptions?.periodMode === "range" ? "range" : "recent";
  const rankMin = readPositiveInteger(defaultedOptions?.rankMin, null);
  const rankMax = readPositiveInteger(defaultedOptions?.rankMax, null);
  const scoreMin = readNumber(defaultedOptions?.scoreMin);
  const combineAimJuggler = normalizeEnabledOption(defaultedOptions?.combineAimJuggler, true);
  const combineHanabi = normalizeEnabledOption(defaultedOptions?.combineHanabi, true);

  return {
    periodMode,
    recentDays: readPositiveInteger(defaultedOptions?.recentDays, DEFAULT_HUNT_BACKTEST_RECENT_DAYS),
    startDate: normalizeDateInput(defaultedOptions?.startDate),
    endDate: normalizeDateInput(defaultedOptions?.endDate),
    latestDate: null,
    earliestDate: null,
    usedFallbackRange: false,
    machineOptions: machineNames.map((machineName) => ({
      name: machineName,
      checked: selectedMachineNameSet.has(machineName),
    })),
    selectedMachineNames,
    rankMin,
    rankMax,
    hasRankFilter: rankMin !== null || rankMax !== null,
    scoreMin,
    hasScoreFilter: scoreMin !== null,
    matchMode: defaultedOptions?.matchMode === "or" ? "or" : "and",
    rankScope:
      defaultedOptions?.rankScope === "machine" || defaultedOptions?.rankScope === "selected"
        ? defaultedOptions.rankScope
        : "all",
    showGraph: defaultedOptions?.showGraph === "off" ? "off" : "on",
    differenceMode: defaultedOptions?.differenceMode === "minrepo" ? "minrepo" : "bonus",
    combineAimJuggler,
    combineHanabi,
    hasAimJugglerGroupOption:
      machineNames.includes("SアイムジャグラーＥＸ") && machineNames.includes("ネオアイムジャグラーEX"),
    hasHanabiGroupOption: machineNames.includes("新ハナビ") && machineNames.includes("スマスロ ハナビ"),
    eventFilters: {
      dayTails: normalizeEventDayTails(defaultedOptions?.dayTails),
      weekdays: normalizeEventWeekdays(defaultedOptions?.weekdays),
    },
    breakdowns: [],
    targetDateCount: 0,
    matchedDateCount: 0,
    matchedRowCount: 0,
    actualRowCount: 0,
    missingActualRowCount: 0,
    hasMatches: false,
    hasActualResults: false,
    summaries: [],
    graphPoints: [],
    total: {
      matchedRowCount: 0,
      averageHuntScore: null,
      actualRowCount: 0,
      differenceTotal: 0,
      gamesTotal: 0,
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

function buildInitialHuntScoreDetail(staticStore, backtestOptions = {}) {
  const store = readStaticStoreIdentity(staticStore);
  if (!isHuntScoreTargetStore(store.storeName)) {
    return null;
  }

  const storeDetail = buildStaticStoreDetail(staticStore);
  const machineNames = listHuntScoreTargetMachineNames(store.storeName);
  return {
    dataSource: "json",
    resultRequested: false,
    store: {
      id: store.id,
      storeName: store.storeName,
      storeUrl: store.storeUrl,
    },
    availableMachineNames: machineNames,
    rankingDates: [],
    selectedDate: storeDetail.summary.latestDate,
    requestedDate: "",
    limit: DEFAULT_HUNT_RANKING_LIMIT,
    predictionDate: null,
    nextBusinessDate: null,
    rows: [],
    rankingGroups: [],
    totalCount: 0,
    hasActualResults: false,
    backtest: buildInitialBacktestDetail(store.storeName, backtestOptions),
  };
}

async function buildStaticHuntScoreSourceRows(staticStore) {
  const store = readStaticStoreIdentity(staticStore);
  const huntScoreMachineNameSet = new Set(
    listHuntScoreSourceMachineNames(store.storeName)
      .flatMap((name) => listEquivalentMachineNames(name))
      .map(canonicalMachineName),
  );
  const storeRows = readStaticStoreRecords(staticStore);
  if (storeRows.length === 0) {
    const targetRows = await readStaticMachineRecords(
      staticStore,
      listHuntScoreSourceMachineNames(store.storeName),
    );
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

async function buildStaticMachineDetail(staticStore, machineName) {
  const store = readStaticStoreIdentity(staticStore);
  const requestedMachineName = canonicalMachineName(machineName);
  const requestedHuntScoreMachineName =
    canonicalHuntScoreTargetMachineName(requestedMachineName, store.storeName) ?? requestedMachineName;
  const huntScoreEnabled = isHuntScoreSupported(store.storeName, requestedHuntScoreMachineName);
  let rows = [];

  if (huntScoreEnabled) {
    const targetRows = await readStaticMachineRecords(staticStore, machineName);
    attachHuntScores(targetRows, targetRows, store.storeName);
    rows = targetRows
      .filter((row) => {
        const rowMachineName =
          canonicalHuntScoreTargetMachineName(canonicalMachineName(row.machine_name), store.storeName) ??
          canonicalMachineName(row.machine_name);
        return rowMachineName === requestedHuntScoreMachineName;
      })
      .map((row) => ({
        ...row,
        machine_name: requestedHuntScoreMachineName,
      }));
  } else {
    rows = await readStaticMachineRecords(staticStore, machineName);
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
    machineName: requestedMachineName,
    slotNumbers: machineDetail.slotNumbers,
    dateRows: machineDetail.dateRows,
    summary: machineDetail.summary,
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

export const getMachineDetail = cache(async function getMachineDetail(storeId, machineName) {
  const staticStore = await readStaticStoreById(storeId);
  if (staticStore) {
    return await buildStaticMachineDetail(staticStore, machineName);
  }

  return null;
});

export const getHuntScoreInitialPageDetail = cache(async function getHuntScoreInitialPageDetail(
  storeId,
  backtestOptions = {},
) {
  const staticStore = await readStaticStoreById(storeId);
  if (staticStore) {
    return buildInitialHuntScoreDetail(staticStore, backtestOptions);
  }

  return null;
});

export const getHuntScoreRankingDetail = cache(async function getHuntScoreRankingDetail(
  storeId,
  requestedDate = "",
  requestedLimit = 20,
) {
  const snapshotDetail = await getHuntScoreSnapshotsForStore(storeId);

  if (!snapshotDetail) {
    return null;
  }

  const { store, snapshots } = snapshotDetail;
  const rankingDates = snapshots.map((snapshot) => snapshot.baseDate);
  const selectedDate = rankingDates.includes(requestedDate) ? requestedDate : rankingDates[0] ?? null;
  const snapshot = snapshots.find((entry) => entry.baseDate === selectedDate) ?? null;
  const fullRankingGroups = buildSelectedMachineRankingGroups(
    snapshot?.rows ?? [],
    listHuntScoreTargetMachineNames(store.store_name),
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
    store: {
      id: store.id,
      storeName: store.store_name,
      storeUrl: store.store_url,
    },
    rankingDates,
    selectedDate,
    requestedDate,
    limit: displayLimit,
    predictionDate: snapshot?.baseDate ?? null,
    nextBusinessDate: snapshot?.nextBusinessDate ?? null,
    rows: rankingRows,
    rankingGroups,
    totalCount,
    hasActualResults: rankingRows.some((row) => row.nextRecord),
  };
});

async function getHuntScoreSnapshotsForStore(storeId) {
  const staticStore = await readStaticStoreById(storeId);
  if (staticStore) {
    const staticIdentity = readStaticStoreIdentity(staticStore);
    if (!isHuntScoreTargetStore(staticIdentity.storeName)) {
      return null;
    }
    const { targetRows, storeRows } = await buildStaticHuntScoreSourceRows(staticStore);
    return {
      dataSource: "json",
      store: {
        id: staticIdentity.id,
        store_name: staticIdentity.storeName,
        store_url: staticIdentity.storeUrl,
      },
      snapshots: buildHuntScoreSnapshots(targetRows, storeRows, staticIdentity.storeName),
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
    rows: group.rows.slice(0, displayLimit),
  }));
}

function buildBacktestOptionsForStore(store, backtestOptions) {
  const hasRequestedEventFilters =
    backtestOptions?.eventTouched ||
    (Array.isArray(backtestOptions?.dayTails) && backtestOptions.dayTails.length > 0) ||
    (Array.isArray(backtestOptions?.weekdays) && backtestOptions.weekdays.length > 0);

  if (hasRequestedEventFilters) {
    return backtestOptions;
  }

  const defaultEventFilters = HUNT_BACKTEST_DEFAULT_EVENT_FILTERS[String(store?.store_name ?? "").trim()];
  if (!defaultEventFilters) {
    return backtestOptions;
  }

  return {
    ...backtestOptions,
    dayTails: defaultEventFilters.dayTails,
    weekdays: defaultEventFilters.weekdays,
  };
}

export async function getHuntScoreAnalysisPageDetail(
  storeId,
  requestedDate = "",
  requestedLimit = 20,
  backtestOptions = {},
) {
  const snapshotDetail = await getHuntScoreSnapshotsForStore(storeId);

  if (!snapshotDetail) {
    return null;
  }

  const { store, snapshots } = snapshotDetail;
  const rankingDates = snapshots.map((snapshot) => snapshot.baseDate);
  const selectedDate = rankingDates.includes(requestedDate) ? requestedDate : rankingDates[0] ?? null;
  const snapshot = snapshots.find((entry) => entry.baseDate === selectedDate) ?? null;
  const backtest = buildHuntScoreBacktestDetail(snapshots, {
    ...buildBacktestOptionsForStore(store, backtestOptions),
    machineOrder: listHuntScoreTargetMachineNames(store.store_name),
  });
  const fullRankingGroups = buildSelectedMachineRankingGroups(snapshot?.rows ?? [], backtest.selectedMachineNames);
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
    store: {
      id: store.id,
      storeName: store.store_name,
      storeUrl: store.store_url,
    },
    rankingDates,
    selectedDate,
    requestedDate,
    limit: displayLimit,
    predictionDate: snapshot?.baseDate ?? null,
    nextBusinessDate: snapshot?.nextBusinessDate ?? null,
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
  return createEventFilters(dayTails, zoro, weekdays);
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

export { matchesEventFilters, parseEventDisplayMode, parseEventFilters } from "./event-filters";
