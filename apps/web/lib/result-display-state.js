const RESULT_DISPLAY_COOKIE_PREFIX = "halldata-result-display";
const FORM_STATE_COOKIE_PREFIX = "halldata-form-state";
const FORM_STATE_COOKIE_CHUNK_SIZE = 2800;
const FORM_STATE_COOKIE_MAX_CHUNKS = 20;

function normalizeResultDisplayKey(value) {
  return String(value ?? "")
    .trim()
    .replace(/[^A-Za-z0-9_-]/g, "_");
}

export function getResultDisplayCookieName(key) {
  const normalizedKey = normalizeResultDisplayKey(key);
  return `${RESULT_DISPLAY_COOKIE_PREFIX}-${normalizedKey || "default"}`;
}

export function isResultDisplayCookieEnabled(value) {
  return String(value ?? "").trim() === "1";
}

export function getFormStateCookieName(key) {
  const normalizedKey = normalizeResultDisplayKey(key);
  return `${FORM_STATE_COOKIE_PREFIX}-${normalizedKey || "default"}`;
}

export function encodeFormStateEntries(entries) {
  return encodeURIComponent(
    JSON.stringify({
      version: 1,
      entries: (Array.isArray(entries) ? entries : [])
        .filter((entry) => Array.isArray(entry) && entry.length >= 2)
        .map(([key, value]) => [String(key ?? ""), String(value ?? "")]),
    }),
  );
}

export function decodeFormStateEntries(value) {
  try {
    const parsedValue = JSON.parse(decodeURIComponent(String(value ?? "")));
    return (Array.isArray(parsedValue?.entries) ? parsedValue.entries : [])
      .filter((entry) => Array.isArray(entry) && entry.length >= 2)
      .map(([key, entryValue]) => [String(key ?? ""), String(entryValue ?? "")]);
  } catch {
    return [];
  }
}

export function splitFormStateCookieValue(value) {
  const safeValue = String(value ?? "");
  const chunks = [];
  for (let index = 0; index < safeValue.length; index += FORM_STATE_COOKIE_CHUNK_SIZE) {
    chunks.push(safeValue.slice(index, index + FORM_STATE_COOKIE_CHUNK_SIZE));
  }
  return chunks.slice(0, FORM_STATE_COOKIE_MAX_CHUNKS);
}

export function getFormStateCookieCountName(baseName) {
  return `${baseName}-count`;
}

export function getFormStateCookieChunkName(baseName, index) {
  return `${baseName}-${index}`;
}

export function readChunkedFormStateCookie(cookieStore, baseName) {
  const chunkCount = Number(
    cookieStore?.get?.(getFormStateCookieCountName(baseName))?.value ?? "",
  );
  if (!Number.isInteger(chunkCount) || chunkCount < 1) {
    return "";
  }

  const chunks = [];
  for (let index = 0; index < Math.min(chunkCount, FORM_STATE_COOKIE_MAX_CHUNKS); index += 1) {
    const chunk = cookieStore?.get?.(getFormStateCookieChunkName(baseName, index))?.value;
    if (typeof chunk !== "string") {
      return "";
    }
    chunks.push(chunk);
  }
  return chunks.join("");
}

export function readFormStateEntriesFromCookies(cookieStore, key) {
  const baseName = getFormStateCookieName(key);
  return decodeFormStateEntries(readChunkedFormStateCookie(cookieStore, baseName));
}

export function buildSavedParamAccess(entries, searchParams) {
  const savedValuesByKey = new Map();
  for (const [key, value] of Array.isArray(entries) ? entries : []) {
    if (!savedValuesByKey.has(key)) {
      savedValuesByKey.set(key, []);
    }
    savedValuesByKey.get(key).push(value);
  }

  const hasSearchParam = (key) =>
    Boolean(
      searchParams &&
        Object.prototype.hasOwnProperty.call(searchParams, key) &&
        searchParams[key] !== undefined,
    );

  return {
    readSingle(key, fallback = "") {
      if (hasSearchParam(key)) {
        const value = searchParams[key];
        if (Array.isArray(value)) {
          return typeof value[0] === "string" ? value[0] : fallback;
        }
        return typeof value === "string" ? value : fallback;
      }
      return savedValuesByKey.get(key)?.[0] ?? fallback;
    },
    readMulti(key) {
      if (hasSearchParam(key)) {
        const value = searchParams[key];
        if (Array.isArray(value)) {
          return value.filter((entry) => typeof entry === "string");
        }
        return typeof value === "string" ? [value] : [];
      }
      return savedValuesByKey.get(key) ?? [];
    },
  };
}
