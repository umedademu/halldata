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

function parseWeekdayValues(value) {
  const weekdays = new Set();
  for (const item of splitSearchParamValue(value)) {
    const numericValue = Number(item);
    if (Number.isInteger(numericValue) && numericValue >= 0 && numericValue <= 6) {
      weekdays.add(numericValue);
    }
  }
  return [...weekdays].sort((left, right) => left - right);
}

function parseFlagValue(value) {
  return splitSearchParamValue(value).some((item) => item === "1" || item === "true");
}

function getDateWeekday(date) {
  const match = String(date).match(/^(\d{4})-(\d{2})-(\d{2})$/u);
  const parsedDate = match
    ? new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]))
    : new Date(date);

  if (Number.isNaN(parsedDate.getTime())) {
    return null;
  }

  return parsedDate.getDay();
}

function isZoromeDate(date) {
  const match = String(date).match(/^\d{4}-(\d{2})-(\d{2})$/u);
  if (!match) {
    return false;
  }
  return Number(match[1]) === Number(match[2]);
}

export function createEventFilters(dayTails = [], zoro = false, weekdays = []) {
  const normalizedDayTails = [...new Set(dayTails)]
    .filter((value) => Number.isInteger(value) && value >= 0 && value <= 9)
    .sort((left, right) => left - right);
  const normalizedWeekdays = [...new Set(weekdays)]
    .filter((value) => Number.isInteger(value) && value >= 0 && value <= 6)
    .sort((left, right) => left - right);

  return {
    dayTails: normalizedDayTails,
    zoro: Boolean(zoro),
    weekdays: normalizedWeekdays,
    isActive: normalizedDayTails.length > 0 || Boolean(zoro) || normalizedWeekdays.length > 0,
  };
}

export function parseEventFilters(searchParams) {
  return createEventFilters(
    parseDayTailValues(searchParams?.dayTail),
    parseFlagValue(searchParams?.zoro),
    parseWeekdayValues(searchParams?.weekday),
  );
}

export function parseEventDisplayMode(searchParams) {
  const mode = splitSearchParamValue(searchParams?.eventMode)[0];
  return mode === "filter" ? "filter" : "highlight";
}

export function matchesEventFilters(date, filters) {
  if (!filters.isActive) {
    return true;
  }

  const dayTail = Number(String(date).slice(-1));
  if (filters.dayTails.includes(dayTail)) {
    return true;
  }

  const weekday = getDateWeekday(date);
  if (weekday !== null && filters.weekdays?.includes(weekday)) {
    return true;
  }

  return filters.zoro && isZoromeDate(date);
}
