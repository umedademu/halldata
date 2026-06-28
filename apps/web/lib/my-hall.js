export const MY_HALL_CLOUD_DATA_VERSION = 1;
export const MY_HALL_CLOUD_INDEX_PATH = "my-hall/index.json";

export function normalizeMyHallClientId(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]/gu, "")
    .slice(0, 80);
}

export function normalizeMyHallStoreId(value) {
  return String(value ?? "").trim();
}

export function normalizeMyHallStoreIds(values) {
  if (!Array.isArray(values)) {
    return [];
  }
  return [...new Set(values.map(normalizeMyHallStoreId).filter(Boolean))];
}

export function buildMyHallClientDataPath(clientId) {
  const normalizedClientId = normalizeMyHallClientId(clientId);
  return normalizedClientId ? `my-hall/clients/${normalizedClientId}.json` : "";
}
