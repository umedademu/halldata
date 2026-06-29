const REGION_POINT_DEFINITIONS = [
  {
    prefectureName: "自宅",
    areaName: "住所1",
    optionLabel: "住所1（太宰府市朱雀）",
    latitude: 33.505066,
    longitude: 130.519638,
  },
  {
    prefectureName: "自宅",
    areaName: "住所2",
    optionLabel: "住所2（葛飾区青戸）",
    latitude: 35.752541,
    longitude: 139.854187,
  },
  { prefectureName: "福岡", areaName: "福岡市", latitude: 33.5902, longitude: 130.4017 },
  { prefectureName: "福岡", areaName: "福岡市中央区", latitude: 33.5892, longitude: 130.3928 },
  { prefectureName: "福岡", areaName: "天神", latitude: 33.5905, longitude: 130.3988 },
  { prefectureName: "福岡", areaName: "福岡市博多区", latitude: 33.5902, longitude: 130.4206 },
  { prefectureName: "福岡", areaName: "博多", latitude: 33.5902, longitude: 130.4206 },
  { prefectureName: "福岡", areaName: "板付", latitude: 33.5648, longitude: 130.4524 },
  { prefectureName: "福岡", areaName: "雑餉隈", latitude: 33.5476, longitude: 130.4625 },
  { prefectureName: "福岡", areaName: "福岡市東区", latitude: 33.6176, longitude: 130.4178 },
  { prefectureName: "福岡", areaName: "箱崎", latitude: 33.6181, longitude: 130.4255 },
  { prefectureName: "福岡", areaName: "香椎", latitude: 33.6594, longitude: 130.4418 },
  { prefectureName: "福岡", areaName: "福岡市南区", latitude: 33.5615, longitude: 130.4262 },
  { prefectureName: "福岡", areaName: "大橋", latitude: 33.5588, longitude: 130.4264 },
  { prefectureName: "福岡", areaName: "井尻", latitude: 33.5522, longitude: 130.4435 },
  { prefectureName: "福岡", areaName: "屋形原", latitude: 33.5319, longitude: 130.4082 },
  { prefectureName: "福岡", areaName: "福岡市城南区", latitude: 33.5757, longitude: 130.3698 },
  { prefectureName: "福岡", areaName: "福岡市早良区", latitude: 33.581, longitude: 130.3483 },
  { prefectureName: "福岡", areaName: "福岡市西区", latitude: 33.5829, longitude: 130.3231 },
  { prefectureName: "福岡", areaName: "小笹", latitude: 33.5698, longitude: 130.3836 },
  { prefectureName: "福岡", areaName: "福岡空港", latitude: 33.5859, longitude: 130.4502 },
  { prefectureName: "福岡", areaName: "春日市", latitude: 33.5327, longitude: 130.4703 },
  { prefectureName: "福岡", areaName: "春日", latitude: 33.5327, longitude: 130.4703 },
  { prefectureName: "福岡", areaName: "大野城市", latitude: 33.5363, longitude: 130.4786 },
  { prefectureName: "福岡", areaName: "大野城", latitude: 33.5363, longitude: 130.4786 },
  { prefectureName: "福岡", areaName: "南ヶ丘", latitude: 33.5128, longitude: 130.4648 },
  { prefectureName: "福岡", areaName: "筑紫野市", latitude: 33.4964, longitude: 130.5156 },
  { prefectureName: "福岡", areaName: "筑紫野", latitude: 33.4964, longitude: 130.5156 },
  { prefectureName: "福岡県", areaName: "太宰府市", latitude: 33.5128, longitude: 130.5239 },
  { prefectureName: "福岡", areaName: "那珂川市", latitude: 33.4992, longitude: 130.422 },
  { prefectureName: "福岡", areaName: "那珂川", latitude: 33.4992, longitude: 130.422 },
  { prefectureName: "福岡", areaName: "糟屋郡", latitude: 33.6108, longitude: 130.4808 },
  { prefectureName: "福岡", areaName: "粕屋町", latitude: 33.6108, longitude: 130.4808 },
  { prefectureName: "福岡", areaName: "須恵町", latitude: 33.5871, longitude: 130.507 },
  { prefectureName: "福岡", areaName: "須恵", latitude: 33.5871, longitude: 130.507 },
  { prefectureName: "福岡", areaName: "志免町", latitude: 33.5914, longitude: 130.4795 },
  { prefectureName: "福岡", areaName: "宇美町", latitude: 33.5687, longitude: 130.5117 },
  { prefectureName: "福岡", areaName: "篠栗町", latitude: 33.6234, longitude: 130.5261 },
  { prefectureName: "福岡", areaName: "新宮町", latitude: 33.7135, longitude: 130.4465 },
  { prefectureName: "福岡", areaName: "古賀市", latitude: 33.733, longitude: 130.467 },
  { prefectureName: "福岡", areaName: "福津市", latitude: 33.766, longitude: 130.492 },
  { prefectureName: "福岡", areaName: "宗像市", latitude: 33.805, longitude: 130.54 },
  { prefectureName: "福岡", areaName: "久留米市", latitude: 33.3193, longitude: 130.5084 },
  { prefectureName: "福岡", areaName: "久留米", latitude: 33.3193, longitude: 130.5084 },
  { prefectureName: "福岡", areaName: "津福", latitude: 33.2979, longitude: 130.5001 },
  { prefectureName: "福岡", areaName: "朝倉市", latitude: 33.4235, longitude: 130.6651 },
  { prefectureName: "福岡", areaName: "朝倉", latitude: 33.4235, longitude: 130.6651 },
  { prefectureName: "福岡", areaName: "小郡市", latitude: 33.3965, longitude: 130.5557 },
  { prefectureName: "福岡", areaName: "三井郡大刀洗町", latitude: 33.3727, longitude: 130.6224 },
  { prefectureName: "福岡", areaName: "大刀洗", latitude: 33.3727, longitude: 130.6224 },
  { prefectureName: "福岡", areaName: "北九州市", latitude: 33.883, longitude: 130.875 },
  { prefectureName: "福岡", areaName: "飯塚市", latitude: 33.646, longitude: 130.691 },
  { prefectureName: "福岡", areaName: "大牟田市", latitude: 33.0303, longitude: 130.4459 },
  { prefectureName: "東京", areaName: "千代田区", latitude: 35.694, longitude: 139.754 },
  { prefectureName: "東京", areaName: "飯田橋", latitude: 35.7014, longitude: 139.7447 },
  { prefectureName: "東京", areaName: "中央区", latitude: 35.6706, longitude: 139.772 },
  { prefectureName: "東京", areaName: "港区", latitude: 35.6581, longitude: 139.7516 },
  { prefectureName: "東京", areaName: "新宿区", latitude: 35.6938, longitude: 139.7034 },
  { prefectureName: "東京", areaName: "文京区", latitude: 35.708, longitude: 139.7523 },
  { prefectureName: "東京", areaName: "台東区", latitude: 35.7126, longitude: 139.78 },
  { prefectureName: "東京", areaName: "上野", latitude: 35.7138, longitude: 139.7773 },
  { prefectureName: "東京", areaName: "浅草", latitude: 35.7148, longitude: 139.7967 },
  { prefectureName: "東京", areaName: "墨田区", latitude: 35.7107, longitude: 139.8015 },
  { prefectureName: "東京", areaName: "江東区", latitude: 35.6728, longitude: 139.8174 },
  { prefectureName: "東京", areaName: "品川区", latitude: 35.6092, longitude: 139.7302 },
  { prefectureName: "東京", areaName: "目黒区", latitude: 35.6415, longitude: 139.6982 },
  { prefectureName: "東京", areaName: "大田区", latitude: 35.5614, longitude: 139.7161 },
  { prefectureName: "東京", areaName: "蒲田", latitude: 35.5625, longitude: 139.716 },
  { prefectureName: "東京", areaName: "世田谷区", latitude: 35.6466, longitude: 139.6532 },
  { prefectureName: "東京", areaName: "渋谷区", latitude: 35.664, longitude: 139.6982 },
  { prefectureName: "東京", areaName: "中野区", latitude: 35.7074, longitude: 139.6638 },
  { prefectureName: "東京", areaName: "杉並区", latitude: 35.6995, longitude: 139.6364 },
  { prefectureName: "東京", areaName: "豊島区", latitude: 35.7263, longitude: 139.7167 },
  { prefectureName: "東京", areaName: "北区", latitude: 35.7529, longitude: 139.7336 },
  { prefectureName: "東京", areaName: "荒川区", latitude: 35.7361, longitude: 139.7833 },
  { prefectureName: "東京", areaName: "南千住", latitude: 35.7334, longitude: 139.7996 },
  { prefectureName: "東京", areaName: "板橋区", latitude: 35.7512, longitude: 139.7093 },
  { prefectureName: "東京", areaName: "東武練馬", latitude: 35.7682, longitude: 139.6612 },
  { prefectureName: "東京", areaName: "練馬区", latitude: 35.7356, longitude: 139.6517 },
  { prefectureName: "東京", areaName: "足立区", latitude: 35.775, longitude: 139.8046 },
  { prefectureName: "東京", areaName: "葛飾区", latitude: 35.7434, longitude: 139.8472 },
  { prefectureName: "東京", areaName: "江戸川区", latitude: 35.7066, longitude: 139.8683 },
  { prefectureName: "東京", areaName: "調布市", latitude: 35.6506, longitude: 139.5407 },
  { prefectureName: "東京", areaName: "仙川", latitude: 35.6622, longitude: 139.5845 },
  { prefectureName: "東京", areaName: "西東京市", latitude: 35.7255, longitude: 139.5383 },
  { prefectureName: "東京", areaName: "ひばりヶ丘", latitude: 35.7517, longitude: 139.5457 },
  { prefectureName: "東京", areaName: "府中市", latitude: 35.6689, longitude: 139.4776 },
  { prefectureName: "東京", areaName: "立川市", latitude: 35.714, longitude: 139.4078 },
  { prefectureName: "東京", areaName: "八王子市", latitude: 35.6663, longitude: 139.316 },
  { prefectureName: "東京", areaName: "武蔵野市", latitude: 35.7178, longitude: 139.5662 },
  { prefectureName: "東京", areaName: "三鷹市", latitude: 35.6835, longitude: 139.5596 },
  { prefectureName: "東京", areaName: "町田市", latitude: 35.5466, longitude: 139.4385 },
  { prefectureName: "東京", areaName: "多摩市", latitude: 35.637, longitude: 139.4463 },
];

const PREFECTURE_POINT_DEFINITIONS = [
  { prefectureName: "北海道", latitude: 43.0642, longitude: 141.3469 },
  { prefectureName: "青森", latitude: 40.8244, longitude: 140.74 },
  { prefectureName: "岩手", latitude: 39.7036, longitude: 141.1527 },
  { prefectureName: "宮城", latitude: 38.2688, longitude: 140.8721 },
  { prefectureName: "秋田", latitude: 39.7186, longitude: 140.1024 },
  { prefectureName: "山形", latitude: 38.2404, longitude: 140.3633 },
  { prefectureName: "福島", latitude: 37.7503, longitude: 140.4676 },
  { prefectureName: "茨城", latitude: 36.3418, longitude: 140.4468 },
  { prefectureName: "栃木", latitude: 36.5658, longitude: 139.8836 },
  { prefectureName: "群馬", latitude: 36.3911, longitude: 139.0608 },
  { prefectureName: "埼玉", latitude: 35.8569, longitude: 139.6489 },
  { prefectureName: "千葉", latitude: 35.6047, longitude: 140.1233 },
  { prefectureName: "東京", latitude: 35.6895, longitude: 139.6917 },
  { prefectureName: "神奈川", latitude: 35.4478, longitude: 139.6425 },
  { prefectureName: "新潟", latitude: 37.9026, longitude: 139.0232 },
  { prefectureName: "富山", latitude: 36.6953, longitude: 137.2113 },
  { prefectureName: "石川", latitude: 36.5947, longitude: 136.6256 },
  { prefectureName: "福井", latitude: 36.0652, longitude: 136.2216 },
  { prefectureName: "山梨", latitude: 35.6642, longitude: 138.5684 },
  { prefectureName: "長野", latitude: 36.6513, longitude: 138.181 },
  { prefectureName: "岐阜", latitude: 35.3912, longitude: 136.7223 },
  { prefectureName: "静岡", latitude: 34.9769, longitude: 138.3831 },
  { prefectureName: "愛知", latitude: 35.1802, longitude: 136.9066 },
  { prefectureName: "三重", latitude: 34.7303, longitude: 136.5086 },
  { prefectureName: "滋賀", latitude: 35.0045, longitude: 135.8686 },
  { prefectureName: "京都", latitude: 35.0212, longitude: 135.7556 },
  { prefectureName: "大阪", latitude: 34.6937, longitude: 135.5023 },
  { prefectureName: "兵庫", latitude: 34.6913, longitude: 135.183 },
  { prefectureName: "奈良", latitude: 34.6851, longitude: 135.8048 },
  { prefectureName: "和歌山", latitude: 34.226, longitude: 135.1675 },
  { prefectureName: "鳥取", latitude: 35.5039, longitude: 134.2383 },
  { prefectureName: "島根", latitude: 35.4723, longitude: 133.0505 },
  { prefectureName: "岡山", latitude: 34.6618, longitude: 133.935 },
  { prefectureName: "広島", latitude: 34.3963, longitude: 132.4594 },
  { prefectureName: "山口", latitude: 34.1859, longitude: 131.4714 },
  { prefectureName: "徳島", latitude: 34.0658, longitude: 134.5593 },
  { prefectureName: "香川", latitude: 34.3401, longitude: 134.0434 },
  { prefectureName: "愛媛", latitude: 33.8416, longitude: 132.7661 },
  { prefectureName: "高知", latitude: 33.5597, longitude: 133.5311 },
  { prefectureName: "福岡", latitude: 33.6064, longitude: 130.4181 },
  { prefectureName: "佐賀", latitude: 33.2494, longitude: 130.2988 },
  { prefectureName: "長崎", latitude: 32.7448, longitude: 129.8737 },
  { prefectureName: "熊本", latitude: 32.7898, longitude: 130.7417 },
  { prefectureName: "大分", latitude: 33.2382, longitude: 131.6126 },
  { prefectureName: "宮崎", latitude: 31.9111, longitude: 131.4239 },
  { prefectureName: "鹿児島", latitude: 31.5602, longitude: 130.5581 },
  { prefectureName: "沖縄", latitude: 26.2124, longitude: 127.6809 },
];

function normalizeLocationText(value) {
  return String(value ?? "")
    .normalize("NFKC")
    .replace(/[ヶ]/gu, "ケ")
    .replace(/[ヵ]/gu, "カ")
    .replace(/\s+/gu, "")
    .trim();
}

export function normalizeRegionPrefecture(value) {
  return normalizeLocationText(value).replace(/[都道府県]$/u, "");
}

export function normalizeRegionArea(value) {
  return normalizeLocationText(value);
}

function normalizePointDefinition(definition) {
  return {
    ...definition,
    normalizedPrefectureName: normalizeRegionPrefecture(definition.prefectureName),
    normalizedAreaName: normalizeRegionArea(definition.areaName),
  };
}

const REGION_POINTS = REGION_POINT_DEFINITIONS.map(normalizePointDefinition);
const PREFECTURE_POINTS = PREFECTURE_POINT_DEFINITIONS.map((definition) => ({
  ...definition,
  normalizedPrefectureName: normalizeRegionPrefecture(definition.prefectureName),
}));

export function listKnownRegionOptions() {
  return REGION_POINTS.map((definition) => ({
    prefectureName: definition.prefectureName,
    areaName: definition.areaName,
    label: definition.optionLabel ?? "",
  }));
}

function pointFromDefinition(definition) {
  if (!definition) {
    return null;
  }
  return {
    latitude: definition.latitude,
    longitude: definition.longitude,
  };
}

function prefectureMatches(definition, prefectureName) {
  return !prefectureName || definition.normalizedPrefectureName === prefectureName;
}

function findAreaPoint(prefectureName, areaName) {
  if (!areaName) {
    return null;
  }

  const exactMatches = REGION_POINTS.filter(
    (definition) =>
      prefectureMatches(definition, prefectureName) &&
      definition.normalizedAreaName === areaName,
  );
  if (exactMatches.length === 1) {
    return pointFromDefinition(exactMatches[0]);
  }

  const partialMatches = REGION_POINTS.filter(
    (definition) =>
      prefectureMatches(definition, prefectureName) &&
      (areaName.includes(definition.normalizedAreaName) ||
        definition.normalizedAreaName.includes(areaName)),
  );
  if (partialMatches.length > 0) {
    const [bestMatch] = partialMatches.sort(
      (left, right) => right.normalizedAreaName.length - left.normalizedAreaName.length,
    );
    return pointFromDefinition(bestMatch);
  }

  return null;
}

function findPrefecturePoint(prefectureName) {
  if (!prefectureName) {
    return null;
  }
  return pointFromDefinition(
    PREFECTURE_POINTS.find((definition) => definition.normalizedPrefectureName === prefectureName),
  );
}

export function readRegionPoint(prefectureName, areaName) {
  const normalizedPrefectureName = normalizeRegionPrefecture(prefectureName);
  const normalizedAreaName = normalizeRegionArea(areaName);
  return (
    findAreaPoint(normalizedPrefectureName, normalizedAreaName) ??
    findPrefecturePoint(normalizedPrefectureName)
  );
}

function toRadians(value) {
  return (value * Math.PI) / 180;
}

export function calculateRegionDistanceKm(startPoint, endPoint) {
  if (!startPoint || !endPoint) {
    return null;
  }

  const earthRadiusKm = 6371;
  const startLatitude = toRadians(startPoint.latitude);
  const endLatitude = toRadians(endPoint.latitude);
  const latitudeDelta = toRadians(endPoint.latitude - startPoint.latitude);
  const longitudeDelta = toRadians(endPoint.longitude - startPoint.longitude);
  const halfChordLength =
    Math.sin(latitudeDelta / 2) ** 2 +
    Math.cos(startLatitude) * Math.cos(endLatitude) * Math.sin(longitudeDelta / 2) ** 2;
  const angularDistance =
    2 * Math.atan2(Math.sqrt(halfChordLength), Math.sqrt(1 - halfChordLength));
  return earthRadiusKm * angularDistance;
}
