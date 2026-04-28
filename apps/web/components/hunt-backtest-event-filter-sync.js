"use client";

import { useEffect } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

const STORAGE_KEY_PREFIX = "hunt-backtest-event-filters:";
const DAY_TAIL_PARAM = "backtestDayTail";
const WEEKDAY_PARAM = "backtestWeekday";
const TOUCHED_PARAM = "backtestEventTouched";

function normalizeIntegerValues(values, min, max) {
  const normalizedValues = new Set();
  for (const value of values) {
    const parsedValue = Number(value);
    if (Number.isInteger(parsedValue) && parsedValue >= min && parsedValue <= max) {
      normalizedValues.add(parsedValue);
    }
  }
  return [...normalizedValues].sort((left, right) => left - right);
}

function readCurrentFilters(searchParams) {
  return {
    dayTails: normalizeIntegerValues(searchParams.getAll(DAY_TAIL_PARAM), 0, 9),
    weekdays: normalizeIntegerValues(searchParams.getAll(WEEKDAY_PARAM), 0, 6),
  };
}

function readSavedFilters(storeId) {
  try {
    const savedText = window.localStorage.getItem(`${STORAGE_KEY_PREFIX}${storeId}`);
    if (!savedText) {
      return null;
    }

    const savedValue = JSON.parse(savedText);
    return {
      dayTails: normalizeIntegerValues(savedValue?.dayTails ?? [], 0, 9),
      weekdays: normalizeIntegerValues(savedValue?.weekdays ?? [], 0, 6),
    };
  } catch {
    return null;
  }
}

function applySavedFilters(searchParams, savedFilters) {
  const nextSearchParams = new URLSearchParams(searchParams.toString());
  nextSearchParams.set(TOUCHED_PARAM, "1");
  nextSearchParams.delete(DAY_TAIL_PARAM);
  nextSearchParams.delete(WEEKDAY_PARAM);

  savedFilters.dayTails.forEach((dayTail) => {
    nextSearchParams.append(DAY_TAIL_PARAM, String(dayTail));
  });
  savedFilters.weekdays.forEach((weekday) => {
    nextSearchParams.append(WEEKDAY_PARAM, String(weekday));
  });

  return nextSearchParams;
}

export function HuntBacktestEventFilterSync({ storeId }) {
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();

  useEffect(() => {
    if (!storeId) {
      return;
    }

    const storageKey = `${STORAGE_KEY_PREFIX}${storeId}`;
    if (searchParams.get(TOUCHED_PARAM) === "1") {
      window.localStorage.setItem(storageKey, JSON.stringify(readCurrentFilters(searchParams)));
      return;
    }

    const savedFilters = readSavedFilters(storeId);
    if (!savedFilters) {
      return;
    }

    const nextSearchParams = applySavedFilters(searchParams, savedFilters);
    const queryText = nextSearchParams.toString();
    router.replace(queryText ? `${pathname}?${queryText}` : pathname, { scroll: false });
  }, [pathname, router, searchParams, storeId]);

  return null;
}
