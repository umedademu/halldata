import { cache } from "react";

import { createEventFilters } from "./event-filters";

const PAGE_SIZE = 1000;
const DEFAULT_FETCH_CACHE_TTL_MS = 60 * 1000;

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

function clearRowsCache() {
  globalThis.__halldataRowsCache?.clear();
}

function normalizeEventDayTails(value) {
  const sourceValues = Array.isArray(value) ? value : [];
  return [...new Set(sourceValues)]
    .map((item) => Number(item))
    .filter((item) => Number.isInteger(item) && item >= 0 && item <= 9)
    .sort((left, right) => left - right);
}

function buildEventFiltersFromStore(store) {
  return createEventFilters(normalizeEventDayTails(store?.event_day_tails), Boolean(store?.event_zoro));
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

function buildStoreLatestDaySummary(store, latestDate, rows) {
  const machineNames = new Set();

  for (const row of rows) {
    machineNames.add(row.machine_name);
  }

  return {
    id: store.id,
    storeName: store.store_name,
    storeUrl: store.store_url,
    machineCount: machineNames.size,
    latestDate,
  };
}

function buildLatestDayMachineSummaries(rows) {
  const buckets = new Map();

  for (const row of rows) {
    const key = row.machine_name;
    if (!buckets.has(key)) {
      buckets.set(key, {
        machineName: row.machine_name,
        slots: new Set(),
        rows: [],
        latestDate: row.target_date,
      });
    }

    const bucket = buckets.get(key);
    bucket.rows.push(row);
    bucket.slots.add(row.slot_number);
  }

  return [...buckets.values()]
    .map((bucket) => ({
      machineName: bucket.machineName,
      slotCount: bucket.slots.size,
      latestDate: bucket.latestDate,
      latestAverageDifference: average(bucket.rows.map((row) => row.difference_value)),
      latestAverageGames: average(bucket.rows.map((row) => row.games_count)),
      latestAveragePayout: average(bucket.rows.map((row) => row.payout_rate)),
    }))
    .sort((left, right) => {
      if (left.slotCount !== right.slotCount) {
        return right.slotCount - left.slotCount;
      }
      return left.machineName.localeCompare(right.machineName, "ja");
    });
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
      storeName: store.store_name,
      storeUrl: store.store_url,
    }))
    .sort((left, right) => left.storeName.localeCompare(right.storeName, "ja"));
});

export const getStoreDetail = cache(async function getStoreDetail(storeId) {
  const { storesTable, resultsTable } = await getSupabaseConfig();
  const [stores, latestRows] = await Promise.all([
    fetchAllRows(storesTable, {
      select: "id,store_name,store_url",
      id: `eq.${storeId}`,
    }),
    fetchAllRows(resultsTable, {
      select: "target_date",
      store_id: `eq.${storeId}`,
      order: "target_date.desc",
      limit: 1,
    }),
  ]);

  const store = stores[0];
  if (!store) {
    return null;
  }

  const latestDate = latestRows[0]?.target_date ?? null;
  const rows = latestDate
    ? await fetchAllRows(resultsTable, {
        select:
          "store_id,machine_name,target_date,slot_number,difference_value,games_count,payout_rate",
        store_id: `eq.${storeId}`,
        target_date: `eq.${latestDate}`,
        order: "machine_name.asc,slot_number.asc",
      })
    : [];

  return {
    store: {
      id: store.id,
      storeName: store.store_name,
      storeUrl: store.store_url,
    },
    summary: buildStoreLatestDaySummary(store, latestDate, rows),
    machines: buildLatestDayMachineSummaries(rows),
  };
});

export const getMachineDetail = cache(async function getMachineDetail(storeId, machineName) {
  const { storesTable, resultsTable } = await getSupabaseConfig();
  const [stores, rows] = await Promise.all([
    fetchAllRows(storesTable, {
      select: "id,store_name,store_url,event_day_tails,event_zoro",
      id: `eq.${storeId}`,
    }),
    fetchAllRows(resultsTable, {
      select:
        "store_id,machine_name,target_date,slot_number,difference_value,games_count,payout_rate,bb_count,rb_count,combined_ratio_text,bb_ratio_text,rb_ratio_text",
      store_id: `eq.${storeId}`,
      machine_name: `eq.${machineName}`,
      order: "target_date.desc,slot_number.asc",
    }),
  ]);

  const store = stores[0];
  if (!store || rows.length === 0) {
    return null;
  }

  const detail = buildMachineDetail(rows);

  return {
    store: {
      id: store.id,
      storeName: store.store_name,
      storeUrl: store.store_url,
      eventFilters: buildEventFiltersFromStore(store),
    },
    machineName,
    slotNumbers: detail.slotNumbers,
    dateRows: detail.dateRows,
    summary: detail.summary,
  };
});

export async function updateStoreEventSettings(storeId, eventSettings) {
  const dayTails = normalizeEventDayTails(eventSettings?.dayTails);
  const zoro = Boolean(eventSettings?.zoro);
  const { baseUrl, serviceKey, schema, storesTable } = await getSupabaseConfig();
  const response = await fetch(
    `${baseUrl}/rest/v1/${encodeURIComponent(storesTable)}?id=eq.${encodeURIComponent(storeId)}`,
    {
      method: "PATCH",
      headers: {
        apikey: serviceKey,
        Authorization: `Bearer ${serviceKey}`,
        Accept: "application/json",
        "Content-Type": "application/json",
        "Accept-Profile": schema,
        "Content-Profile": schema,
        Prefer: "return=minimal",
      },
      body: JSON.stringify({
        event_day_tails: dayTails,
        event_zoro: zoro,
        updated_at: new Date().toISOString(),
      }),
      cache: "no-store",
    },
  );

  if (!response.ok) {
    throw new Error(`Supabase への特定日保存に失敗しました。(${response.status})`);
  }

  clearRowsCache();
  return createEventFilters(dayTails, zoro);
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
