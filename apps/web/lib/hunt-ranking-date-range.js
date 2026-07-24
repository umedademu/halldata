function normalizeDateInput(value) {
  const text = String(value ?? "").trim();
  return /^\d{4}-\d{2}-\d{2}$/u.test(text) ? text : null;
}

function normalizeDayCount(value) {
  const number = Number(value);
  return Number.isFinite(number) && number >= 0 ? Math.trunc(number) : 0;
}

function shiftDateInput(value, days) {
  const dateText = normalizeDateInput(value);
  if (!dateText) {
    return null;
  }

  const date = new Date(`${dateText}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

export function buildHuntRankingDateLoadRange({
  requestedDate,
  latestDate,
  historyWindowDays,
  nextResultBufferDays,
} = {}) {
  const normalizedRequestedDate = normalizeDateInput(requestedDate);
  const normalizedLatestDate = normalizeDateInput(latestDate);
  const targetDate = normalizedRequestedDate ?? normalizedLatestDate;
  if (!targetDate) {
    return null;
  }

  const firstBaseDate =
    normalizedLatestDate && normalizedLatestDate < targetDate
      ? normalizedLatestDate
      : targetDate;
  const lastBaseDate =
    normalizedLatestDate && normalizedLatestDate > targetDate
      ? normalizedLatestDate
      : targetDate;

  return {
    startDate: shiftDateInput(firstBaseDate, -normalizeDayCount(historyWindowDays)),
    endDate: shiftDateInput(lastBaseDate, normalizeDayCount(nextResultBufferDays)),
  };
}

export function buildHuntRankingSnapshotDateRange({
  selectedDate,
  historyWindowDays,
  nextResultBufferDays,
} = {}) {
  const normalizedSelectedDate = normalizeDateInput(selectedDate);
  if (!normalizedSelectedDate) {
    return null;
  }

  return {
    startDate: shiftDateInput(normalizedSelectedDate, -normalizeDayCount(historyWindowDays)),
    endDate: shiftDateInput(normalizedSelectedDate, normalizeDayCount(nextResultBufferDays)),
  };
}
