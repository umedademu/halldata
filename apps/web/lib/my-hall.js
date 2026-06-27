export const MY_HALL_CLOUD_DATA_VERSION = 1;

export const MY_HALL_PROFILES = [
  { id: "a", label: "Aさん" },
  { id: "b", label: "Bさん" },
];

const MY_HALL_PROFILE_IDS = new Set(MY_HALL_PROFILES.map((profile) => profile.id));

export function normalizeMyHallProfileId(value) {
  const profileId = String(value ?? "").trim().toLowerCase();
  return MY_HALL_PROFILE_IDS.has(profileId) ? profileId : "";
}

export function getMyHallProfileLabel(profileId) {
  const normalizedProfileId = normalizeMyHallProfileId(profileId);
  return MY_HALL_PROFILES.find((profile) => profile.id === normalizedProfileId)?.label ?? "";
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

export function buildMyHallCloudDataPath(profileId) {
  const normalizedProfileId = normalizeMyHallProfileId(profileId);
  return normalizedProfileId ? `my-hall/${normalizedProfileId}.json` : "";
}
