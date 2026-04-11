const compactDateFormatter = new Intl.DateTimeFormat("ja-JP", {
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});

const averageGamesFormatter = new Intl.NumberFormat("ja-JP", {
  maximumFractionDigits: 0,
});

const narrowIntegerFormatter = new Intl.NumberFormat("ja-JP", {
  maximumFractionDigits: 0,
  useGrouping: false,
});

const integerFormatter = new Intl.NumberFormat("ja-JP");

const signedFormatter = new Intl.NumberFormat("ja-JP", {
  signDisplay: "always",
  maximumFractionDigits: 0,
});

const narrowSignedFormatter = new Intl.NumberFormat("ja-JP", {
  signDisplay: "always",
  maximumFractionDigits: 0,
  useGrouping: false,
});

const percentFormatter = new Intl.NumberFormat("ja-JP", {
  maximumFractionDigits: 1,
  minimumFractionDigits: 0,
});

const narrowPercentFormatter = new Intl.NumberFormat("ja-JP", {
  maximumFractionDigits: 1,
  minimumFractionDigits: 0,
  useGrouping: false,
});

function normalizeDateText(value) {
  if (typeof value !== "string") {
    return null;
  }

  const normalized = value.trim();
  if (!normalized) {
    return null;
  }

  const match = normalized.match(/^(\d{4})-(\d{2})-(\d{2})$/u);
  if (!match) {
    return null;
  }

  return `${match[1]}-${match[2]}-${match[3]}`;
}

export function formatCompactDate(value) {
  const normalized = normalizeDateText(value);
  if (normalized) {
    return normalized;
  }

  if (!value) {
    return "-";
  }

  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "-" : compactDateFormatter.format(date).replaceAll("/", "-");
}

export function formatShortDate(value) {
  const normalized = normalizeDateText(value);
  if (normalized) {
    return `${normalized.slice(2, 4)}/${normalized.slice(5, 7)}/${normalized.slice(8, 10)}`;
  }

  if (!value) {
    return "-";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }

  const year = String(date.getFullYear()).slice(-2);
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}/${month}/${day}`;
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

export function formatNarrowInteger(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  return narrowIntegerFormatter.format(Number(value));
}

export function formatSignedNumber(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  return signedFormatter.format(Number(value));
}

export function formatNarrowSignedNumber(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  return narrowSignedFormatter.format(Number(value));
}

export function formatPercent(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  return `${percentFormatter.format(Number(value))}%`;
}

export function formatNarrowPercent(value) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  return `${narrowPercentFormatter.format(Number(value))}%`;
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
