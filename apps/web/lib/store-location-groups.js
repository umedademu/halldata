export const STORE_LOCATION_GROUP_DEFINITIONS = [
  { key: "fukuoka", label: "福岡の店舗", prefectureName: "福岡" },
  { key: "tokyo", label: "東京の店舗", prefectureName: "東京" },
  { key: "other", label: "その他の店舗", prefectureName: "" },
];

export function normalizeStoreLocationPrefecture(value) {
  return String(value ?? "")
    .normalize("NFKC")
    .replace(/[都道府県]$/u, "")
    .trim();
}

export function readStoreLocationGroupKey(store) {
  const prefectureName = normalizeStoreLocationPrefecture(store?.prefectureName);
  if (prefectureName === "福岡") {
    return "fukuoka";
  }
  if (prefectureName === "東京") {
    return "tokyo";
  }
  return "other";
}

export function buildStoreLocationGroups(stores) {
  const groupMap = new Map(
    STORE_LOCATION_GROUP_DEFINITIONS.map((group) => [
      group.key,
      {
        ...group,
        stores: [],
        storeCount: 0,
      },
    ]),
  );

  for (const store of Array.isArray(stores) ? stores : []) {
    const groupKey = readStoreLocationGroupKey(store);
    groupMap.get(groupKey)?.stores.push(store);
  }

  return STORE_LOCATION_GROUP_DEFINITIONS.map((group) => {
    const locationGroup = groupMap.get(group.key);
    return {
      ...locationGroup,
      storeCount: locationGroup?.stores.length ?? 0,
    };
  }).filter((group) => group.storeCount > 0);
}
