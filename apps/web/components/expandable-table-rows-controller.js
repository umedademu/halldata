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

    let disposed = false;
    let syncAnimationFrame = 0;
    const cleanupByRow = new Map();
    const initializedDetailRows = new WeakSet();

    const updateIndicator = (row, hidden) => {
      const indicator = row.querySelector(".backtestExpandIndicator");
      if (indicator) {
        indicator.textContent = hidden ? "＋" : "－";
      }
    };

    const cleanupRow = (row) => {
      const cleanup = cleanupByRow.get(row);
      if (!cleanup) {
        return;
      }
      cleanup();
      cleanupByRow.delete(row);
    };

    const syncRows = () => {
      if (disposed) {
        return;
      }

      const currentTable = document.getElementById(tableId);
      const currentTbody = currentTable?.tBodies?.[0];
      if (!currentTbody) {
        return;
      }

      const detailRowsByKey = new Map();
      [...currentTbody.rows].forEach((row) => {
        if (row.dataset.expandDetailRow !== "1") {
          return;
        }

        const key = row.dataset.expandParentKey;
        if (!key) {
          return;
        }

        if (!initializedDetailRows.has(row)) {
          row.hidden = true;
          initializedDetailRows.add(row);
        }
        detailRowsByKey.set(key, row);
      });

      const activeRows = new Set();
      [...currentTbody.rows].forEach((row) => {
        const key = row.dataset.expandRowKey;
        const detailRow = key ? detailRowsByKey.get(key) : null;
        if (!key || !detailRow) {
          return;
        }

        activeRows.add(row);
        row.classList.add("backtestExpandableRow");
        row.tabIndex = 0;
        row.setAttribute("role", "button");
        row.setAttribute("aria-expanded", detailRow.hidden ? "false" : "true");
        updateIndicator(row, detailRow.hidden);

        if (cleanupByRow.has(row)) {
          return;
        }

        const toggleRow = () => {
          const nextHidden = !detailRow.hidden;
          detailRow.hidden = nextHidden;
          row.setAttribute("aria-expanded", nextHidden ? "false" : "true");
          updateIndicator(row, nextHidden);
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
        cleanupByRow.set(row, () => {
          row.removeEventListener("click", handleClick);
          row.removeEventListener("keydown", handleKeyDown);
          row.classList.remove("backtestExpandableRow");
          row.removeAttribute("role");
          row.removeAttribute("tabindex");
          row.removeAttribute("aria-expanded");
        });
      });

      [...cleanupByRow.keys()].forEach((row) => {
        if (!activeRows.has(row) || !row.isConnected) {
          cleanupRow(row);
        }
      });
    };

    const scheduleSync = () => {
      if (syncAnimationFrame) {
        return;
      }
      syncAnimationFrame = window.requestAnimationFrame(() => {
        syncAnimationFrame = 0;
        syncRows();
      });
    };

    const retryTimeouts = [0, 250, 1000].map((delay) =>
      window.setTimeout(syncRows, delay),
    );
    const observer = new MutationObserver(scheduleSync);
    observer.observe(table, { childList: true, subtree: true });
    syncRows();

    return () => {
      disposed = true;
      if (syncAnimationFrame) {
        window.cancelAnimationFrame(syncAnimationFrame);
      }
      retryTimeouts.forEach((timeoutId) => window.clearTimeout(timeoutId));
      observer.disconnect();
      [...cleanupByRow.keys()].forEach((row) => cleanupRow(row));
    };
  }, [tableId]);

  return <span hidden data-expandable-table-controller={tableId} />;
}
