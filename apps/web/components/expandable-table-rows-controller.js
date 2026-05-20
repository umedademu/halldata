"use client";

import { useEffect } from "react";

function shouldIgnoreRowToggle(target) {
  return Boolean(
    target?.closest?.("a, button, input, label, select, textarea, summary, details"),
  );
}

export function ExpandableTableRowsController({ tableId }) {
  useEffect(() => {
    const table = document.getElementById(tableId);
    const tbody = table?.tBodies?.[0];
    if (!tbody) {
      return undefined;
    }

    const detailRowsByKey = new Map();
    [...tbody.rows].forEach((row) => {
      if (row.dataset.expandDetailRow !== "1") {
        return;
      }

      const key = row.dataset.expandParentKey;
      if (!key) {
        return;
      }

      row.hidden = true;
      detailRowsByKey.set(key, row);
    });

    const cleanupCallbacks = [];
    [...tbody.rows].forEach((row) => {
      const key = row.dataset.expandRowKey;
      const detailRow = key ? detailRowsByKey.get(key) : null;
      if (!key || !detailRow) {
        return;
      }

      row.classList.add("backtestExpandableRow");
      row.tabIndex = 0;
      row.setAttribute("role", "button");
      row.setAttribute("aria-expanded", detailRow.hidden ? "false" : "true");

      const toggleRow = () => {
        const nextHidden = !detailRow.hidden;
        detailRow.hidden = nextHidden;
        row.setAttribute("aria-expanded", nextHidden ? "false" : "true");
        const indicator = row.querySelector(".backtestExpandIndicator");
        if (indicator) {
          indicator.textContent = nextHidden ? "＋" : "－";
        }
      };

      const handleClick = (event) => {
        if (shouldIgnoreRowToggle(event.target)) {
          return;
        }
        toggleRow();
      };

      const handleKeyDown = (event) => {
        if (event.key !== "Enter" && event.key !== " ") {
          return;
        }
        if (shouldIgnoreRowToggle(event.target)) {
          return;
        }
        event.preventDefault();
        toggleRow();
      };

      row.addEventListener("click", handleClick);
      row.addEventListener("keydown", handleKeyDown);
      cleanupCallbacks.push(() => {
        row.removeEventListener("click", handleClick);
        row.removeEventListener("keydown", handleKeyDown);
        row.classList.remove("backtestExpandableRow");
        row.removeAttribute("role");
        row.removeAttribute("tabindex");
        row.removeAttribute("aria-expanded");
      });
    });

    return () => {
      cleanupCallbacks.forEach((cleanup) => cleanup());
    };
  }, [tableId]);

  return null;
}
