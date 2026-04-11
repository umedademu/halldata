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

export function createEventFilters(dayTails = [], zoro = false) {
  const normalizedDayTails = [...new Set(dayTails)]
    .filter((value) => Number.isInteger(value) && value >= 0 && value <= 9)
    .sort((left, right) => left - right);

  return {
    dayTails: normalizedDayTails,
    zoro: Boolean(zoro),
    isActive: normalizedDayTails.length > 0 || Boolean(zoro),
  };
}

export function parseEventFilters(searchParams) {
  return createEventFilters(parseDayTailValues(searchParams?.dayTail), parseFlagValue(searchParams?.zoro));
}

export function parseEventDisplayMode(searchParams) {
  const mode = splitSearchParamValue(searchParams?.eventMode)[0];
  return mode === "highlight" ? "highlight" : "filter";
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
