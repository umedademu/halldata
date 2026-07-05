"use client";

import { useEffect } from "react";

export function ResultUrlTools({ active = false }) {
  useEffect(() => {
    if (!active || typeof window === "undefined") {
      return;
    }

    const currentUrl = new URL(window.location.href);
    if (!currentUrl.searchParams.has("show")) {
      return;
    }

    window.history.replaceState(
      window.history.state,
      "",
      `${currentUrl.pathname}${currentUrl.hash}`,
    );
  }, [active]);

  return null;
}
