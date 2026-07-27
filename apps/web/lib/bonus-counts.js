const AT_DISPLAY_SLOTS = new Set(["bb", "rb", "ignore", "unknown"]);

function hasOwn(record, key) {
  return Boolean(record && Object.prototype.hasOwnProperty.call(record, key));
}

function readFiniteNumber(value) {
  if (
    value === null ||
    value === undefined ||
    (typeof value === "string" && value.trim() === "")
  ) {
    return null;
  }
  if (typeof value !== "number" && typeof value !== "string") {
    return null;
  }
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function readRawCount(record, fieldName) {
  const rawFieldName = `raw_${fieldName}`;
  return readFiniteNumber(hasOwn(record, rawFieldName) ? record?.[rawFieldName] : record?.[fieldName]);
}

function readRawText(record, fieldName) {
  const rawFieldName = `raw_${fieldName}`;
  const value = hasOwn(record, rawFieldName) ? record?.[rawFieldName] : record?.[fieldName];
  const text = String(value ?? "").trim();
  return text || null;
}

function formatRatioText(gamesCount, hitCount) {
  if (
    !Number.isFinite(gamesCount) ||
    gamesCount <= 0 ||
    !Number.isFinite(hitCount) ||
    hitCount <= 0
  ) {
    return null;
  }
  return `1/${Math.round(gamesCount / hitCount)}`;
}

export function normalizeAtDisplaySlot(value) {
  const normalizedValue = String(value ?? "").trim().toLowerCase();
  return AT_DISPLAY_SLOTS.has(normalizedValue) ? normalizedValue : "unknown";
}

export function readEffectiveBonusCounts(record) {
  const rawBbCount = readRawCount(record, "bb_count");
  const rawRbCount = readRawCount(record, "rb_count");
  const atCount = readFiniteNumber(record?.at_count);
  const atDisplaySlot = normalizeAtDisplaySlot(record?.at_display_slot);
  const appliesToBb = atCount !== null && atDisplaySlot === "bb";
  const appliesToRb = atCount !== null && atDisplaySlot === "rb";

  return {
    rawBbCount,
    rawRbCount,
    atCount,
    atDisplaySlot,
    appliesAtCount: appliesToBb || appliesToRb,
    bbCount: appliesToBb ? (rawBbCount ?? 0) + atCount : rawBbCount,
    rbCount: appliesToRb ? (rawRbCount ?? 0) + atCount : rawRbCount,
  };
}

export function readEffectiveBbCount(record) {
  return readEffectiveBonusCounts(record).bbCount;
}

export function readEffectiveRbCount(record) {
  return readEffectiveBonusCounts(record).rbCount;
}

export function withEffectiveBonusCounts(record) {
  if (!record || typeof record !== "object" || Array.isArray(record)) {
    return record;
  }

  const counts = readEffectiveBonusCounts(record);
  const gamesCount = readFiniteNumber(record.games_count);
  const rawCombinedRatioText = readRawText(record, "combined_ratio_text");
  const rawBbRatioText = readRawText(record, "bb_ratio_text");
  const rawRbRatioText = readRawText(record, "rb_ratio_text");
  const combinedCount = (counts.bbCount ?? 0) + (counts.rbCount ?? 0);

  return {
    ...record,
    raw_bb_count: counts.rawBbCount,
    raw_rb_count: counts.rawRbCount,
    raw_combined_ratio_text: rawCombinedRatioText,
    raw_bb_ratio_text: rawBbRatioText,
    raw_rb_ratio_text: rawRbRatioText,
    at_count: counts.atCount,
    at_display_slot: counts.atDisplaySlot,
    at_source: String(record.at_source ?? "").trim() || null,
    at_fetched_at: String(record.at_fetched_at ?? "").trim() || null,
    effective_bb_count: counts.bbCount,
    effective_rb_count: counts.rbCount,
    bb_count: counts.bbCount,
    rb_count: counts.rbCount,
    combined_ratio_text: counts.appliesAtCount
      ? formatRatioText(gamesCount, combinedCount)
      : rawCombinedRatioText,
    bb_ratio_text: counts.appliesAtCount
      ? formatRatioText(gamesCount, counts.bbCount)
      : rawBbRatioText,
    rb_ratio_text: counts.appliesAtCount
      ? formatRatioText(gamesCount, counts.rbCount)
      : rawRbRatioText,
  };
}
