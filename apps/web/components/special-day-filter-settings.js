"use client";

import { useMemo, useState } from "react";

function normalizeIntegerValues(values, min, max) {
  const normalizedValues = new Set();
  for (const value of Array.isArray(values) ? values : []) {
    const numericValue = Number(value);
    if (Number.isInteger(numericValue) && numericValue >= min && numericValue <= max) {
      normalizedValues.add(numericValue);
    }
  }
  return [...normalizedValues].sort((left, right) => left - right);
}

function weekdayLabelFor(value, weekdayOptions) {
  return (
    (Array.isArray(weekdayOptions) ? weekdayOptions : []).find(
      (weekday) => Number(weekday.value) === Number(value),
    )?.label ?? `${value}`
  );
}

function formatSummary({ dayTails, zoro, weekdays, monthDays, weekdayOptions }) {
  const parts = [];
  const tailLabels = dayTails.map((value) => `${value}`);
  if (zoro) {
    tailLabels.push("ゾロ目");
  }
  if (tailLabels.length > 0) {
    parts.push(`末尾 ${tailLabels.join("・")}`);
  }
  if (weekdays.length > 0) {
    parts.push(`曜日 ${weekdays.map((value) => weekdayLabelFor(value, weekdayOptions)).join("・")}`);
  }
  if (monthDays.length > 0) {
    parts.push(`日付 ${monthDays.map((value) => `${value}日`).join("・")}`);
  }
  return parts.length > 0 ? parts.join(" / ") : "指定なし";
}

export function SpecialDayFilterSettings({
  dayTailOptions,
  weekdayOptions,
  selectedDayTails,
  selectedMonthDays,
  selectedWeekdays,
  zoro,
}) {
  const [dayTails, setDayTails] = useState(() =>
    normalizeIntegerValues(selectedDayTails, 0, 9),
  );
  const [monthDays, setMonthDays] = useState(() =>
    normalizeIntegerValues(selectedMonthDays, 1, 31),
  );
  const [weekdays, setWeekdays] = useState(() =>
    normalizeIntegerValues(selectedWeekdays, 0, 6),
  );
  const [isZoro, setIsZoro] = useState(Boolean(zoro));
  const [monthDayInput, setMonthDayInput] = useState("");
  const [monthDayError, setMonthDayError] = useState("");

  const dayTailSet = useMemo(() => new Set(dayTails), [dayTails]);
  const weekdaySet = useMemo(() => new Set(weekdays), [weekdays]);
  const summary = formatSummary({
    dayTails,
    zoro: isZoro,
    weekdays,
    monthDays,
    weekdayOptions,
  });

  const toggleDayTail = (dayTail) => {
    const numericValue = Number(dayTail);
    setDayTails((currentValues) =>
      currentValues.includes(numericValue)
        ? currentValues.filter((value) => value !== numericValue)
        : [...currentValues, numericValue].sort((left, right) => left - right),
    );
  };

  const toggleWeekday = (weekday) => {
    const numericValue = Number(weekday);
    setWeekdays((currentValues) =>
      currentValues.includes(numericValue)
        ? currentValues.filter((value) => value !== numericValue)
        : [...currentValues, numericValue].sort((left, right) => left - right),
    );
  };

  const registerMonthDay = () => {
    const numericValue = Number(monthDayInput);
    if (!Number.isInteger(numericValue) || numericValue < 1 || numericValue > 31) {
      setMonthDayError("1から31で入力してください");
      return;
    }

    setMonthDays((currentValues) =>
      currentValues.includes(numericValue)
        ? currentValues
        : [...currentValues, numericValue].sort((left, right) => left - right),
    );
    setMonthDayInput("");
    setMonthDayError("");
  };

  const handleMonthDayKeyDown = (event) => {
    if (event.key !== "Enter") {
      return;
    }

    event.preventDefault();
    registerMonthDay();
  };

  const removeMonthDay = (monthDay) => {
    setMonthDays((currentValues) => currentValues.filter((value) => value !== monthDay));
  };

  return (
    <details className="specialDaySettings">
      <summary className="specialDaySummary">
        <span className="storeReserveButton storeReserveButtonSecondary specialDaySummaryButton">
          特定日を設定
        </span>
        <span className="specialDaySummaryText">選択中: {summary}</span>
      </summary>
      <div className="specialDayPanel">
        <div className="specialDayGroup">
          <p className="specialDayGroupTitle">末尾</p>
          <div className="metricToggleRow">
            {(Array.isArray(dayTailOptions) ? dayTailOptions : []).map((dayTail) => (
              <label
                key={dayTail}
                className={`metricToggleChip ${
                  dayTailSet.has(Number(dayTail)) ? "metricToggleChipActive" : ""
                }`}
              >
                <input
                  type="checkbox"
                  name="backtestDayTail"
                  value={dayTail}
                  checked={dayTailSet.has(Number(dayTail))}
                  onChange={() => toggleDayTail(dayTail)}
                />
                <span>{dayTail}</span>
              </label>
            ))}
            <label className={`metricToggleChip ${isZoro ? "metricToggleChipActive" : ""}`}>
              <input
                type="checkbox"
                name="backtestZoro"
                value="1"
                checked={isZoro}
                onChange={(event) => setIsZoro(event.currentTarget.checked)}
              />
              <span>ゾロ目</span>
            </label>
          </div>
        </div>

        <div className="specialDayGroup">
          <p className="specialDayGroupTitle">曜日</p>
          <div className="metricToggleRow">
            {(Array.isArray(weekdayOptions) ? weekdayOptions : []).map((weekday) => (
              <label
                key={weekday.value}
                className={`metricToggleChip ${
                  weekdaySet.has(Number(weekday.value)) ? "metricToggleChipActive" : ""
                }`}
              >
                <input
                  type="checkbox"
                  name="backtestWeekday"
                  value={weekday.value}
                  checked={weekdaySet.has(Number(weekday.value))}
                  onChange={() => toggleWeekday(weekday.value)}
                />
                <span>{weekday.label}</span>
              </label>
            ))}
          </div>
        </div>

        <div className="specialDayGroup">
          <p className="specialDayGroupTitle">日付</p>
          <div className="specialDayDateEntry">
            <label className="specialDayDateField">
              <span>毎月の日付</span>
              <span className="specialDayDateInputWrap">
                <input
                  type="number"
                  min="1"
                  max="31"
                  step="1"
                  inputMode="numeric"
                  placeholder="22"
                  className="specialDayDateInput"
                  value={monthDayInput}
                  onChange={(event) => {
                    setMonthDayInput(event.currentTarget.value);
                    setMonthDayError("");
                  }}
                  onKeyDown={handleMonthDayKeyDown}
                />
                <span>日</span>
              </span>
            </label>
            <button
              type="button"
              className="storeReserveButton storeReserveButtonSecondary machineFilterAction"
              onClick={registerMonthDay}
            >
              登録
            </button>
          </div>
          {monthDayError ? <p className="specialDayError">{monthDayError}</p> : null}
          <div className="specialDayDateChips">
            {monthDays.length > 0 ? (
              monthDays.map((monthDay) => (
                <span key={monthDay} className="specialDayDateChip">
                  <input type="hidden" name="backtestMonthDay" value={monthDay} />
                  <span>{monthDay}日</span>
                  <button
                    type="button"
                    className="specialDayDateRemove"
                    onClick={() => removeMonthDay(monthDay)}
                    aria-label={`${monthDay}日を削除`}
                  >
                    ×
                  </button>
                </span>
              ))
            ) : (
              <span className="specialDayEmpty">登録なし</span>
            )}
          </div>
        </div>
      </div>
    </details>
  );
}
