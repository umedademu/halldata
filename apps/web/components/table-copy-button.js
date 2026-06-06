"use client";

import { useState } from "react";

function copyText(text) {
  if (navigator.clipboard?.writeText) {
    return navigator.clipboard.writeText(text);
  }

  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "");
  textarea.style.position = "fixed";
  textarea.style.opacity = "0";
  document.body.appendChild(textarea);
  textarea.select();
  document.execCommand("copy");
  document.body.removeChild(textarea);
  return Promise.resolve();
}

function isVisibleRow(row) {
  if (!row || row.hidden) {
    return false;
  }

  const style = window.getComputedStyle(row);
  return style.display !== "none" && style.visibility !== "hidden";
}

function readCellText(cell) {
  const clone = cell.cloneNode(true);
  clone
    .querySelectorAll(".sortableTableHeaderIndicator, .backtestExpandIndicator, [aria-hidden='true']")
    .forEach((node) => node.remove());

  return clone.textContent.replace(/\s+/g, " ").trim();
}

function buildTableText(table) {
  const rows = [...table.querySelectorAll("thead tr, tbody tr")]
    .filter(isVisibleRow)
    .map((row) =>
      [...row.querySelectorAll("th, td")]
        .map(readCellText)
        .join("\t"),
    )
    .filter(Boolean);

  return rows.join("\n");
}

export function TableCopyButton({ tableId, label = "表をコピー" }) {
  const [status, setStatus] = useState("");

  const handleCopy = async () => {
    const table = document.getElementById(tableId);
    const tableText = table ? buildTableText(table) : "";

    if (!tableText) {
      setStatus("コピーできませんでした");
      return;
    }

    try {
      await copyText(tableText);
      setStatus("コピー済み");
    } catch {
      setStatus("コピーできませんでした");
    }
  };

  return (
    <div className="tableCopyControl">
      <button
        type="button"
        className="csvExportBtn tableCopyButton"
        onClick={handleCopy}
        title="表の内容をタブ区切りテキストとしてコピー"
      >
        <svg
          width="16"
          height="16"
          viewBox="0 0 16 16"
          fill="none"
          aria-hidden="true"
          style={{ flexShrink: 0 }}
        >
          <path
            d="M5.5 4.5h6A1.5 1.5 0 0 1 13 6v6a1.5 1.5 0 0 1-1.5 1.5h-6A1.5 1.5 0 0 1 4 12V6a1.5 1.5 0 0 1 1.5-1.5Z"
            stroke="currentColor"
            strokeWidth="1.4"
          />
          <path
            d="M3 10H2.5A1.5 1.5 0 0 1 1 8.5v-6A1.5 1.5 0 0 1 2.5 1h6A1.5 1.5 0 0 1 10 2.5V3"
            stroke="currentColor"
            strokeWidth="1.4"
            strokeLinecap="round"
          />
        </svg>
        {label}
      </button>
      {status ? (
        <span className="tableCopyStatus" aria-live="polite">
          {status}
        </span>
      ) : null}
    </div>
  );
}
