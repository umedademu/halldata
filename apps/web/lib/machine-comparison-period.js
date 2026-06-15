export const MACHINE_COMPARISON_PERIOD_STORAGE_KEY = "machine-comparison-period-options";
export const MACHINE_COMPARISON_PERIOD_COOKIE_NAME = "machine-comparison-period-options";
export const MACHINE_COMPARISON_PERIOD_COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24 * 365;

function normalizeText(value) {
  return String(value ?? "").trim();
}

function pickPeriodOptions(source) {
  if (!source || typeof source !== "object") {
    return null;
  }

  return {
    version: 1,
    periodMode: normalizeText(source.periodMode),
    recentDaysInput: normalizeText(source.recentDaysInput ?? source.recentDays),
    rangeStartInput: normalizeText(source.rangeStartInput ?? source.startDate),
    rangeEndInput: normalizeText(source.rangeEndInput ?? source.endDate),
  };
}

export function encodeMachineComparisonPeriodCookieValue(options) {
  const normalizedOptions = pickPeriodOptions(options);
  if (!normalizedOptions) {
    return "";
  }

  return encodeURIComponent(JSON.stringify(normalizedOptions));
}

export function decodeMachineComparisonPeriodCookieValue(value) {
  const text = normalizeText(value);
  if (!text) {
    return null;
  }

  try {
    const parsedValue = JSON.parse(decodeURIComponent(text));
    return pickPeriodOptions(parsedValue);
  } catch {
    return null;
  }
}
