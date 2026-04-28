import { cache } from "react";

import { createEventFilters } from "./event-filters";
import { buildHuntScoreBacktestDetail } from "./hunt-backtest";
import {
  attachHuntScores,
  buildHuntScoreSnapshots,
  isHuntScoreSupported,
  isHuntScoreTargetStore,
  listHuntScoreTargetMachineNames,
} from "./hunt-score";
import { canonicalMachineName, listEquivalentMachineNames, withCalculatedDifferenceValue } from "./machine-difference";

const PAGE_SIZE = 1000;
const DEFAULT_FETCH_CACHE_TTL_MS = 60 * 1000;
const HUNT_BACKTEST_DEFAULT_EVENT_FILTERS = {
  "Aパーク春日店": {
    dayTails: [0],
    weekdays: [0, 6],
  },
};

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

    if (
      process.env.SUPABASE_URL &&
      (process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SECRET_KEY)
    ) {
      return settings;
    }

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

async function getSupabaseConfig() {
  const supabaseUrl = await readSetting("SUPABASE_URL");
  const supabaseKey =
    (await readSetting("SUPABASE_SERVICE_ROLE_KEY")) || (await readSetting("SUPABASE_SECRET_KEY"));

  if (!supabaseUrl || !supabaseKey) {
    throw new Error(
      "Supabase の接続情報が見つかりません。apps/web の環境変数、またはルートの .env.local を確認してください。",
    );
  }

  return {
    baseUrl: supabaseUrl.replace(/\/+$/u, ""),
    serviceKey: supabaseKey,
    schema: await readSetting("SUPABASE_SCHEMA", "public"),
    storesTable: await readSetting("SUPABASE_STORES_TABLE", "stores"),
    resultsTable: await readSetting("SUPABASE_MACHINE_RESULTS_TABLE", "machine_daily_results"),
    machineSummariesTable: await readSetting(
      "SUPABASE_MACHINE_SUMMARIES_TABLE",
      "store_machine_summaries",
    ),
    machineDailyDetailsTable: await readSetting(
      "SUPABASE_MACHINE_DAILY_DETAILS_TABLE",
      "store_machine_daily_details",
    ),
  };
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

async function fetchHuntScoreSourceRows(resultsTable, machineDailyDetailsTable, storeId) {
  const huntScoreMachineNames = [
    ...new Set(
      listHuntScoreTargetMachineNames().flatMap((name) => listEquivalentMachineNames(name)),
    ),
  ];

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
  const { baseUrl, serviceKey, schema } = await getSupabaseConfig();
  const requestedLimit = Number(params.limit);
  const hasRequestedLimit = Number.isInteger(requestedLimit) && requestedLimit > 0;
  const pageSize = hasRequestedLimit ? Math.min(requestedLimit, PAGE_SIZE) : PAGE_SIZE;
  const headers = {
    apikey: serviceKey,
    Authorization: `Bearer ${serviceKey}`,
    Accept: "application/json",
    "Accept-Profile": schema,
  };

  const rows = [];
  let offset = 0;

  while (true) {
    const query = buildQuery({
      ...params,
      limit: pageSize,
      offset,
    });

    const response = await fetch(
      `${baseUrl}/rest/v1/${encodeURIComponent(tableName)}?${query.toString()}`,
      {
        headers,
        cache: "no-store",
      },
    );

    if (!response.ok) {
      throw new Error(`Supabase からの取得に失敗しました。(${response.status})`);
    }

    const chunk = await response.json();
    rows.push(...chunk);

    if (chunk.length < pageSize || (hasRequestedLimit && rows.length >= requestedLimit)) {
      break;
    }
    offset += pageSize;
  }

  return hasRequestedLimit ? rows.slice(0, requestedLimit) : rows;
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
  const { storesTable } = await getSupabaseConfig();
  const stores = await fetchAllRows(storesTable, {
    select: "id,store_name,store_url",
    order: "store_name.asc",
  });

  return stores
    .map((store) => ({
      id: store.id,
      storeName: store.store_name ?? "",
      storeUrl: store.store_url,
      isPendingRegistration: !String(store.store_name ?? "").trim(),
    }))
    .sort((left, right) => {
      if (left.isPendingRegistration !== right.isPendingRegistration) {
        return left.isPendingRegistration ? 1 : -1;
      }
      const leftLabel = left.isPendingRegistration ? left.storeUrl : left.storeName;
      const rightLabel = right.isPendingRegistration ? right.storeUrl : right.storeName;
      return leftLabel.localeCompare(rightLabel, "ja");
    });
});

export const getStoreIdentity = cache(async function getStoreIdentity(storeId) {
  const { storesTable } = await getSupabaseConfig();
  const stores = await fetchStoreEventRows(storesTable, storeId);
  const store = stores[0];

  if (!store) {
    return null;
  }

  return {
    id: store.id,
    storeName: store.store_name,
    storeUrl: store.store_url,
  };
});

async function readStoreMachineSummariesFromLocalData(storeName) {
  const machineSummariesCache = getStoreMachineSummariesCache();
  const [{ default: fs }, pathModule, urlModule] = await Promise.all([
    import("node:fs"),
    import("node:path"),
    import("node:url"),
  ]);
  const currentDirectory = pathModule.dirname(urlModule.fileURLToPath(import.meta.url));
  const configuredLocalDataDirectory =
    (await readSetting("SUPABASE_LOCAL_SAVE_DIR")) || (await readSetting("LOCAL_SAVE_DIR"));
  const localDataDirectory = configuredLocalDataDirectory
    ? pathModule.isAbsolute(configuredLocalDataDirectory)
      ? configuredLocalDataDirectory
      : pathModule.resolve(currentDirectory, "../../../", configuredLocalDataDirectory)
    : pathModule.resolve(currentDirectory, "../../../local_data");
  const indexPath = pathModule.resolve(localDataDirectory, storeName, "_full_day_index.json");

  if (!fs.existsSync(indexPath)) {
    return null;
  }

  const modifiedAtMs = fs.statSync(indexPath).mtimeMs;
  const cachedEntry = machineSummariesCache.get(indexPath);

  if (cachedEntry?.modifiedAtMs === modifiedAtMs) {
    return cachedEntry.summary;
  }

  try {
    const index = JSON.parse(fs.readFileSync(indexPath, "utf8"));
    const fullDayDates = Object.entries(index?.full_day_dates ?? {}).sort(([left], [right]) =>
      right.localeCompare(left, "ja"),
    );
    const buckets = new Map();

    for (const [targetDate, entry] of fullDayDates) {
      const savedPath = String(entry?.local_file_path ?? "").trim();
      if (!savedPath) {
        continue;
      }

      const snapshotPath = fs.existsSync(savedPath)
        ? savedPath
        : pathModule.resolve(pathModule.dirname(indexPath), pathModule.basename(savedPath));

      if (!fs.existsSync(snapshotPath)) {
        continue;
      }

      const snapshot = JSON.parse(fs.readFileSync(snapshotPath, "utf8"));
      const rows = (Array.isArray(snapshot?.records) ? snapshot.records : []).map(
        withCalculatedDifferenceValue,
      );

      for (const row of rows) {
        const machineName = String(row.machine_name ?? "").trim();
        if (!machineName) {
          continue;
        }

        const existingBucket = buckets.get(machineName);
        if (existingBucket && existingBucket.latestDate !== targetDate) {
          continue;
        }

        if (!existingBucket) {
          buckets.set(machineName, {
            machineName,
            latestDate: targetDate,
            slots: new Set([row.slot_number]),
            rows: [row],
          });
          continue;
        }

        existingBucket.rows.push(row);
        existingBucket.slots.add(row.slot_number);
      }
    }

    if (buckets.size === 0) {
      return null;
    }

    const machineSummaries = [...buckets.values()].map((bucket) => ({
      machineName: bucket.machineName,
      slotCount: bucket.slots.size,
      latestDate: bucket.latestDate,
      latestAverageDifference: average(bucket.rows.map((row) => row.difference_value)),
      latestAverageGames: average(bucket.rows.map((row) => row.games_count)),
      latestAveragePayout: average(bucket.rows.map((row) => row.payout_rate)),
    }));

    const summary = {
      latestDate:
        machineSummaries.reduce((currentLatestDate, machine) => {
          if (!machine?.latestDate) {
            return currentLatestDate;
          }
          if (currentLatestDate === null || machine.latestDate > currentLatestDate) {
            return machine.latestDate;
          }
          return currentLatestDate;
        }, null) ?? null,
      machines: machineSummaries.sort((left, right) => {
        if (left.latestDate !== right.latestDate) {
          return right.latestDate.localeCompare(left.latestDate, "ja");
        }
        if (left.slotCount !== right.slotCount) {
          return right.slotCount - left.slotCount;
        }
        return left.machineName.localeCompare(right.machineName, "ja");
      }),
    };

    machineSummariesCache.set(indexPath, {
      modifiedAtMs,
      summary,
    });

    return summary;
  } catch {
    return null;
  }
}

export async function registerPendingStoreUrl(storeUrl) {
  const normalizedStoreUrl = normalizeStoreUrl(storeUrl);
  const { baseUrl, serviceKey, schema, storesTable } = await getSupabaseConfig();
  const headers = {
    apikey: serviceKey,
    Authorization: `Bearer ${serviceKey}`,
    Accept: "application/json",
    "Content-Type": "application/json",
    "Accept-Profile": schema,
    "Content-Profile": schema,
  };

  const existingQuery = buildQuery({
    select: "id",
    store_url: `eq.${normalizedStoreUrl}`,
    limit: 1,
  });
  const existingResponse = await fetch(
    `${baseUrl}/rest/v1/${encodeURIComponent(storesTable)}?${existingQuery.toString()}`,
    {
      headers,
      cache: "no-store",
    },
  );

  if (!existingResponse.ok) {
    throw new Error(`登録済みURLの確認に失敗しました。(${existingResponse.status})`);
  }

  const existingStores = await existingResponse.json();
  if (existingStores.length > 0) {
    return { status: "exists", storeUrl: normalizedStoreUrl };
  }

  const insertStore = async (payload) => {
    const response = await fetch(
      `${baseUrl}/rest/v1/${encodeURIComponent(storesTable)}?select=id`,
      {
        method: "POST",
        headers: {
          ...headers,
          Prefer: "return=representation",
        },
        body: JSON.stringify([payload]),
        cache: "no-store",
      },
    );
    if (!response.ok) {
      const errorText = await response.text().catch(() => "");
      throw new Error(`店舗URLの登録に失敗しました。(${response.status}) ${errorText}`.trim());
    }
    return response.json();
  };

  const nowText = new Date().toISOString();
  try {
    await insertStore({
      store_url: normalizedStoreUrl,
      updated_at: nowText,
    });
  } catch {
    await insertStore({
      store_name: "",
      store_url: normalizedStoreUrl,
      updated_at: nowText,
    });
  }

  clearRowsCache();
  return { status: "created", storeUrl: normalizedStoreUrl };
}

export const getStoreDetail = cache(async function getStoreDetail(storeId) {
  const { storesTable, resultsTable, machineSummariesTable } = await getSupabaseConfig();
  const stores = await fetchAllRows(storesTable, {
    select: "id,store_name,store_url",
    id: `eq.${storeId}`,
  });

  const store = stores[0];
  if (!store) {
    return null;
  }

  let machineSummaryResult = null;

  try {
    const summaryRows = await fetchAllRows(machineSummariesTable, {
      select:
        "machine_name,latest_date,slot_count,average_difference,average_games,average_payout",
      store_id: `eq.${storeId}`,
      order: "latest_date.desc,slot_count.desc,machine_name.asc",
    });
    if (summaryRows.length > 0) {
      machineSummaryResult = buildMachineSummaryResultFromSummaryRows(summaryRows);
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

  if (!machineSummaryResult) {
    const localMachineSummaryResult = await readStoreMachineSummariesFromLocalData(store.store_name);
    machineSummaryResult = localMachineSummaryResult
      ? localMachineSummaryResult
      : buildMachineLatestSummaries(
          (await fetchAllRows(resultsTable, {
            select:
              "store_id,machine_name,target_date,slot_number,difference_value,games_count,payout_rate,bb_count,rb_count",
            store_id: `eq.${storeId}`,
          })).map(withCalculatedDifferenceValue),
        );
  }

  return {
    store: {
      id: store.id,
      storeName: store.store_name,
      storeUrl: store.store_url,
    },
    summary: buildStoreSummary(store, machineSummaryResult.machines),
    machines: machineSummaryResult.machines,
  };
});

export const getMachineDetail = cache(async function getMachineDetail(storeId, machineName) {
  const { storesTable, resultsTable, machineDailyDetailsTable } = await getSupabaseConfig();
  const stores = await fetchStoreEventRows(storesTable, storeId);
  const store = stores[0];
  if (!store) {
    return null;
  }

  const requestedMachineName = canonicalMachineName(machineName);
  const huntScoreEnabled = isHuntScoreSupported(store.store_name, requestedMachineName);
  let rows;
  let detail = null;

  if (huntScoreEnabled) {
    const { targetRows, storeRows } = await fetchHuntScoreSourceRows(
      resultsTable,
      machineDailyDetailsTable,
      storeId,
    );
    attachHuntScores(targetRows, storeRows);
    rows = targetRows.filter((row) => canonicalMachineName(row.machine_name) === requestedMachineName);
  } else {
    try {
      const dailyDetailRows = await fetchAllRows(machineDailyDetailsTable, {
        select:
          "machine_name,target_date,average_difference,records_by_slot",
        store_id: `eq.${storeId}`,
        ...buildMachineNameFilter(listEquivalentMachineNames(machineName)),
        order: "target_date.desc,machine_name.asc",
      });
      if (dailyDetailRows.length > 0) {
        detail = buildMachineDetailFromDailyRows(dailyDetailRows);
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

    if (!detail) {
      const fetchedRows = await fetchAllRows(resultsTable, {
        select:
          "store_id,machine_name,target_date,slot_number,difference_value,games_count,payout_rate,bb_count,rb_count,combined_ratio_text,bb_ratio_text,rb_ratio_text",
        store_id: `eq.${storeId}`,
        ...buildMachineNameFilter(listEquivalentMachineNames(machineName)),
        order: "target_date.desc,slot_number.asc",
      });
      rows = fetchedRows.map(withCalculatedDifferenceValue);
    }
  }

  if (!detail && rows.length === 0) {
    return null;
  }

  const machineDetail = detail ?? buildMachineDetail(rows);

  return {
    store: {
      id: store.id,
      storeName: store.store_name,
      storeUrl: store.store_url,
      eventFilters: buildEventFiltersFromStore(store),
    },
    machineName: requestedMachineName,
    slotNumbers: machineDetail.slotNumbers,
    dateRows: machineDetail.dateRows,
    summary: machineDetail.summary,
  };
});

export const getHuntScoreRankingDetail = cache(async function getHuntScoreRankingDetail(
  storeId,
  requestedDate = "",
  requestedLimit = 20,
) {
  const detail = await getHuntScoreAnalysisPageDetail(storeId, requestedDate, requestedLimit);

  if (!detail) {
    return null;
  }

  const {
    store,
    rankingDates,
    selectedDate,
    limit,
    predictionDate,
    nextBusinessDate,
    rows,
    totalCount,
    hasActualResults,
  } = detail;

  return {
    store,
    rankingDates,
    selectedDate,
    requestedDate,
    limit,
    predictionDate,
    nextBusinessDate,
    rows,
    totalCount,
    hasActualResults,
  };
});

async function getHuntScoreSnapshotsForStore(storeId) {
  const { storesTable, resultsTable, machineDailyDetailsTable } = await getSupabaseConfig();
  const stores = await fetchStoreEventRows(storesTable, storeId);
  const store = stores[0];

  if (!store || !isHuntScoreTargetStore(store.store_name)) {
    return null;
  }

  const { targetRows, storeRows } = await fetchHuntScoreSourceRows(
    resultsTable,
    machineDailyDetailsTable,
    storeId,
  );

  return {
    store,
    snapshots: buildHuntScoreSnapshots(targetRows, storeRows),
  };
}

function normalizeRankingLimit(requestedLimit) {
  return Number.isInteger(requestedLimit) && requestedLimit >= 1 ? requestedLimit : 20;
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
  const rankingLimit = normalizeRankingLimit(requestedLimit);
  const totalCount = snapshot?.rows.length ?? 0;
  const displayLimit = totalCount > 0 ? Math.min(rankingLimit, totalCount) : rankingLimit;

  return {
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
    rows: snapshot?.rows.slice(0, displayLimit) ?? [],
    totalCount,
    hasActualResults: snapshot?.rows.some((row) => row.nextRecord) ?? false,
    backtest: buildHuntScoreBacktestDetail(snapshots, buildBacktestOptionsForStore(store, backtestOptions)),
  };
}

export async function updateStoreEventSettings(storeId, eventSettings) {
  const dayTails = normalizeEventDayTails(eventSettings?.dayTails);
  const zoro = Boolean(eventSettings?.zoro);
  const weekdays = normalizeEventWeekdays(eventSettings?.weekdays);
  const { baseUrl, serviceKey, schema, storesTable } = await getSupabaseConfig();
  const url = `${baseUrl}/rest/v1/${encodeURIComponent(storesTable)}?id=eq.${encodeURIComponent(storeId)}`;
  const headers = {
    apikey: serviceKey,
    Authorization: `Bearer ${serviceKey}`,
    Accept: "application/json",
    "Content-Type": "application/json",
    "Accept-Profile": schema,
    "Content-Profile": schema,
    Prefer: "return=minimal",
  };
  const updatedAt = new Date().toISOString();
  const patchEventSettings = (payload) =>
    fetch(url, {
      method: "PATCH",
      headers,
      body: JSON.stringify(payload),
      cache: "no-store",
    });
  let response = await patchEventSettings({
    event_day_tails: dayTails,
    event_zoro: zoro,
    event_weekdays: weekdays,
    updated_at: updatedAt,
  });

  if (!response.ok && response.status === 400) {
    response = await patchEventSettings({
      event_day_tails: dayTails,
      event_zoro: zoro,
      updated_at: updatedAt,
    });
  }

  if (!response.ok) {
    throw new Error(`Supabase への特定日保存に失敗しました。(${response.status})`);
  }

  clearRowsCache();
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
