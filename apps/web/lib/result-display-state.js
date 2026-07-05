const RESULT_DISPLAY_COOKIE_PREFIX = "halldata-result-display";

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
