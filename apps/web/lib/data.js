import { cache } from "react";

const PAGE_SIZE = 1000;

let cachedFileSettingsPromise = null;

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

async function fetchAllRows(tableName, params) {
  const { baseUrl, serviceKey, schema } = await getSupabaseConfig();
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
      limit: PAGE_SIZE,
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

    if (chunk.length < PAGE_SIZE) {
      break;
    }
    offset += PAGE_SIZE;
  }

  return rows;
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

function buildStoreSummary(store, rows) {
  const machineNames = new Set();
  const slotNumbers = new Set();
  const dates = new Set();

  for (const row of rows) {
    machineNames.add(row.machine_name);
    slotNumbers.add(row.slot_number);
    dates.add(row.target_date);
  }

  const sortedDates = [...dates].sort((left, right) => right.localeCompare(left));

  return {
    id: store.id,
    storeName: store.store_name,
    storeUrl: store.store_url,
    machineCount: machineNames.size,
    slotCount: slotNumbers.size,
    dayCount: dates.size,
    recordCount: rows.length,
    startDate: sortedDates.at(-1) ?? null,
    endDate: sortedDates[0] ?? null,
  };
}

function buildMachineSummaries(rows) {
  const buckets = new Map();

  for (const row of rows) {
    const key = row.machine_name;
    if (!buckets.has(key)) {
      buckets.set(key, {
        machineName: row.machine_name,
        dates: new Set(),
        slots: new Set(),
        rows: [],
        latestDate: null,
      });
    }

    const bucket = buckets.get(key);
    bucket.rows.push(row);
    bucket.dates.add(row.target_date);
    bucket.slots.add(row.slot_number);
    if (!bucket.latestDate || row.target_date > bucket.latestDate) {
      bucket.latestDate = row.target_date;
    }
  }

  return [...buckets.values()]
    .map((bucket) => {
      const dates = [...bucket.dates].sort((left, right) => right.localeCompare(left));
      const latestRows = bucket.rows.filter((row) => row.target_date === bucket.latestDate);

      return {
        machineName: bucket.machineName,
        slotCount: bucket.slots.size,
        dayCount: bucket.dates.size,
        recordCount: bucket.rows.length,
        startDate: dates.at(-1) ?? null,
        endDate: dates[0] ?? null,
        latestDate: bucket.latestDate,
        latestAverageDifference: average(latestRows.map((row) => row.difference_value)),
        latestAverageGames: average(latestRows.map((row) => row.games_count)),
        latestAveragePayout: average(latestRows.map((row) => row.payout_rate)),
      };
    })
    .sort((left, right) => {
      if (left.latestDate !== right.latestDate) {
        return String(right.latestDate).localeCompare(String(left.latestDate), "ja");
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

export const getStoreSummaries = cache(async function getStoreSummaries() {
  const { storesTable, resultsTable } = await getSupabaseConfig();
  const [stores, results] = await Promise.all([
    fetchAllRows(storesTable, {
      select: "id,store_name,store_url",
      order: "store_name.asc",
    }),
    fetchAllRows(resultsTable, {
      select: "store_id,machine_name,target_date,slot_number",
      order: "target_date.desc",
    }),
  ]);

  const groupedResults = new Map();
  for (const row of results) {
    if (!groupedResults.has(row.store_id)) {
      groupedResults.set(row.store_id, []);
    }
    groupedResults.get(row.store_id).push(row);
  }

  return stores
    .map((store) => buildStoreSummary(store, groupedResults.get(store.id) ?? []))
    .sort((left, right) => left.storeName.localeCompare(right.storeName, "ja"));
});

export const getStoreDetail = cache(async function getStoreDetail(storeId) {
  const { storesTable, resultsTable } = await getSupabaseConfig();
  const [stores, rows] = await Promise.all([
    fetchAllRows(storesTable, {
      select: "id,store_name,store_url",
      id: `eq.${storeId}`,
    }),
    fetchAllRows(resultsTable, {
      select:
        "store_id,machine_name,target_date,slot_number,difference_value,games_count,payout_rate",
      store_id: `eq.${storeId}`,
      order: "target_date.desc,slot_number.asc",
    }),
  ]);

  const store = stores[0];
  if (!store) {
    return null;
  }

  return {
    store: {
      id: store.id,
      storeName: store.store_name,
      storeUrl: store.store_url,
    },
    summary: buildStoreSummary(store, rows),
    machines: buildMachineSummaries(rows),
  };
});

export const getMachineDetail = cache(async function getMachineDetail(storeId, machineName) {
  const { storesTable, resultsTable } = await getSupabaseConfig();
  const [stores, rows] = await Promise.all([
    fetchAllRows(storesTable, {
      select: "id,store_name,store_url",
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
    },
    machineName,
    slotNumbers: detail.slotNumbers,
    dateRows: detail.dateRows,
    summary: detail.summary,
  };
});

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

function splitSearchParamValue(value) {
  if (Array.isArray(value)) {
    return value.flatMap((item) => splitSearchParamValue(item));
  }
  if (value === undefined || value === null || value === "") {
    return [];
  }
  return String(value)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function parseDayTailValues(value) {
  const dayTails = new Set();
  for (const item of splitSearchParamValue(value)) {
    const numericValue = Number(item);
    if (Number.isInteger(numericValue) && numericValue >= 0 && numericValue <= 9) {
      dayTails.add(numericValue);
    }
  }
  return [...dayTails].sort((left, right) => left - right);
}

function parseFlagValue(value) {
  return splitSearchParamValue(value).some((item) => item === "1" || item === "true");
}

function isZoromeDate(date) {
  const match = String(date).match(/^\d{4}-(\d{2})-(\d{2})$/u);
  if (!match) {
    return false;
  }
  return Number(match[1]) === Number(match[2]);
}

export function parseEventFilters(searchParams) {
  const dayTails = parseDayTailValues(searchParams?.dayTail);
  const zoro = parseFlagValue(searchParams?.zoro);

  return {
    dayTails,
    zoro,
    isActive: dayTails.length > 0 || zoro,
  };
}

export function matchesEventFilters(date, filters) {
  if (!filters.isActive) {
    return true;
  }

  const dayTail = Number(String(date).slice(-1));
  if (filters.dayTails.includes(dayTail)) {
    return true;
  }

  return filters.zoro && isZoromeDate(date);
}
