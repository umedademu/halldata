const compactDateFormatter = new Intl.DateTimeFormat("ja-JP", {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});

const averageGamesFormatter = new Intl.NumberFormat("ja-JP", {
  maximumFractionDigits: 0,
});

const integerFormatter = new Intl.NumberFormat("ja-JP");

const signedFormatter = new Intl.NumberFormat("ja-JP", {
  signDisplay: "always",
  maximumFractionDigits: 0,
});

const percentFormatter = new Intl.NumberFormat("ja-JP", {
  maximumFractionDigits: 1,
  minimumFractionDigits: 0,
});

function toDate(value) {
  return value ? new Date(`${value}T00:00:00+09:00`) : null;
}

export function formatCompactDate(value) {
  const date = toDate(value);
  return date ? compactDateFormatter.format(date).replaceAll("/", "-") : "-";
}

export function formatPeriod(startDate, endDate) {
  if (!startDate && !endDate) {
    return "期間データなし";
  }
  if (!startDate || !endDate) {
    return formatCompactDate(startDate || endDate);
  }
  return `${formatCompactDate(startDate)} 〜 ${formatCompactDate(endDate)}`;
}

export function formatNumber(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  return integerFormatter.format(Number(value));
}

export function formatAverageGames(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  return averageGamesFormatter.format(Number(value));
}

export function formatSignedNumber(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  return signedFormatter.format(Number(value));
}

export function formatPercent(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  return `${percentFormatter.format(Number(value))}%`;
}

export function formatRatio(value) {
  return value || "-";
}

export function valueToneClass(metricKey, value) {
  if (value === null || value === undefined || value === "") {
    return "";
  }

  const numericValue = Number(value);
  if (!Number.isFinite(numericValue)) {
    return "";
  }

  if (metricKey === "difference_value" || metricKey === "payout_rate") {
    if (numericValue > 0 || (metricKey === "payout_rate" && numericValue > 100)) {
      return "valuePositive";
    }
    if (numericValue < 0 || (metricKey === "payout_rate" && numericValue < 100)) {
      return "valueNegative";
    }
  }

  return "";
}
