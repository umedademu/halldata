"use client";

import { useEffect, useState } from "react";

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

export function ResultUrlTools({ active = false }) {
  const [shareUrl, setShareUrl] = useState("");
  const [status, setStatus] = useState("");

  useEffect(() => {
    if (!active || typeof window === "undefined") {
      return;
    }

    const currentUrl = new URL(window.location.href);
    if (!currentUrl.searchParams.has("show")) {
      return;
    }

    setShareUrl(currentUrl.href);
    window.history.replaceState(
      window.history.state,
      "",
      `${currentUrl.pathname}${currentUrl.hash}`,
    );
  }, [active]);

  if (!active || !shareUrl) {
    return null;
  }

  const handleCopy = async () => {
    try {
      await copyText(shareUrl);
      setStatus("共有URLをコピーしました。");
    } catch {
      setStatus("共有URLをコピーできませんでした。");
    }
  };

  return (
    <section className="filterPanel">
      <div className="backtestButtonRow">
        <button
          type="button"
          className="storeReserveButton storeReserveButtonSecondary"
          onClick={handleCopy}
        >
          共有URLをコピー
        </button>
      </div>
      {status ? <p className="storeReserveHelp">{status}</p> : null}
    </section>
  );
}
