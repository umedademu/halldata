"use client";

import { useEffect } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

const HUNT_RANKING_LIMIT_STORAGE_KEY = "hunt-ranking-limit";

function parseLimit(value) {
  const parsedValue = Number(value);
  if (!Number.isInteger(parsedValue) || parsedValue < 1) {
    return null;
  }
  return parsedValue;
}

export function HuntRankingLimitSync({ defaultLimit }) {
  const pathname = usePathname();
  const router = useRouter();
  const searchParams = useSearchParams();

  useEffect(() => {
    const currentLimit = parseLimit(searchParams.get("limit"));

    if (currentLimit !== null) {
      window.localStorage.setItem(HUNT_RANKING_LIMIT_STORAGE_KEY, String(currentLimit));
      return;
    }

    const savedLimit = parseLimit(window.localStorage.getItem(HUNT_RANKING_LIMIT_STORAGE_KEY));
    if (savedLimit === null || savedLimit === defaultLimit) {
      return;
    }

    const nextSearchParams = new URLSearchParams(searchParams.toString());
    nextSearchParams.set("limit", String(savedLimit));
    const queryText = nextSearchParams.toString();
    router.replace(queryText ? `${pathname}?${queryText}` : pathname, { scroll: false });
  }, [defaultLimit, pathname, router, searchParams]);

  return null;
}
