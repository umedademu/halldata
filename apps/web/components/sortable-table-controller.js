"use client";

import { useEffect } from "react";

function readSortValue(row, columnIndex, type) {
  const cell = row.children[columnIndex];
  const rawValue = cell?.getAttribute("data-sort-value") ?? cell?.textContent ?? "";
  const text = String(rawValue).trim();
  if (!text || text === "-") {
    return { missing: true, value: null };
  }

  if (type === "number") {
    const number = Number(text.replace(/,/g, "").replace(/%$/u, ""));
    return Number.isFinite(number)
      ? { missing: false, value: number }
      : { missing: true, value: null };
  }

  return { missing: false, value: text };
}

function compareRows(left, right, columnIndex, type, direction, collator) {
  const leftValue = readSortValue(left.row, columnIndex, type);
  const rightValue = readSortValue(right.row, columnIndex, type);

  if (leftValue.missing && rightValue.missing) {
    return left.originalIndex - right.originalIndex;
  }
  if (leftValue.missing) {
    return 1;
  }
  if (rightValue.missing) {
    return -1;
  }

  const baseResult =
    type === "number"
      ? leftValue.value - rightValue.value
      : collator.compare(leftValue.value, rightValue.value);
  if (baseResult === 0) {
    return left.originalIndex - right.originalIndex;
  }
  return direction === "asc" ? baseResult : -baseResult;
}

function resetHeaderState(headers) {
  headers.forEach((header) => {
    header.setAttribute("aria-sort", "none");
    header.removeAttribute("data-sort-direction");
    const indicator = header.querySelector(".sortableTableHeaderIndicator");
    if (indicator) {
      indicator.textContent = "";
    }
  });
}

export function SortableTableController({ tableId }) {
  useEffect(() => {
    const table = document.getElementById(tableId);
    if (!table) {
      return undefined;
    }

    const tbody = table.tBodies[0];
    const headers = [...table.querySelectorAll("thead th[data-sort-index]")];
    if (!tbody || headers.length === 0) {
      return undefined;
    }

    [...tbody.rows].forEach((row, index) => {
      row.dataset.sortOriginalIndex = String(index);
    });

    const collator = new Intl.Collator("ja", {
      numeric: true,
      sensitivity: "base",
    });
    const cleanupCallbacks = [];

    headers.forEach((header) => {
      const button = header.querySelector(".sortableTableHeaderButton") ?? header;
      const handleClick = () => {
        const columnIndex = Number(header.dataset.sortIndex);
        if (!Number.isInteger(columnIndex)) {
          return;
        }

        const type = header.dataset.sortType === "text" ? "text" : "number";
        const initialDirection =
          header.dataset.sortInitialDirection === "asc" ? "asc" : "desc";
        const currentColumn = table.dataset.sortColumn;
        const currentDirection = table.dataset.sortDirection;
        const nextDirection =
          currentColumn === String(columnIndex) && currentDirection === "desc"
            ? "asc"
            : currentColumn === String(columnIndex) && currentDirection === "asc"
              ? "desc"
              : initialDirection;

        const fixedRows = [];
        const sortableRows = [];
        [...tbody.rows].forEach((row) => {
          const entry = {
            row,
            originalIndex: Number(row.dataset.sortOriginalIndex ?? "0"),
          };
          if (row.dataset.sortFixed === "1") {
            fixedRows.push(entry);
          } else {
            sortableRows.push(entry);
          }
        });

        sortableRows.sort((left, right) =>
          compareRows(left, right, columnIndex, type, nextDirection, collator),
        );

        [...fixedRows, ...sortableRows].forEach((entry) => {
          tbody.appendChild(entry.row);
        });

        table.dataset.sortColumn = String(columnIndex);
        table.dataset.sortDirection = nextDirection;
        resetHeaderState(headers);
        header.setAttribute(
          "aria-sort",
          nextDirection === "asc" ? "ascending" : "descending",
        );
        header.dataset.sortDirection = nextDirection;
        const indicator = header.querySelector(".sortableTableHeaderIndicator");
        if (indicator) {
          indicator.textContent = nextDirection === "asc" ? "↑" : "↓";
        }
      };

      button.addEventListener("click", handleClick);
      cleanupCallbacks.push(() => button.removeEventListener("click", handleClick));
    });

    return () => {
      cleanupCallbacks.forEach((cleanup) => cleanup());
    };
  }, [tableId]);

  return null;
}
