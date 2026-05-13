"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const NAV_ITEMS = [
  { href: "/", label: "店舗選択", match: (pathname) => pathname === "/" || pathname.startsWith("/stores/") },
  {
    href: "/store-cross-backtest",
    label: "店舗横断バックテスト",
    match: (pathname) => pathname.startsWith("/store-cross-backtest"),
  },
  { href: "/updates", label: "更新情報", match: (pathname) => pathname.startsWith("/updates") },
];

export function TopNavigation() {
  const pathname = usePathname() || "/";

  return (
    <nav className="topNavigation" aria-label="主要画面">
      {NAV_ITEMS.map((item) => {
        const isActive = item.match(pathname);

        return (
          <Link
            key={item.href}
            href={item.href}
            className={`topNavButton ${isActive ? "topNavButtonActive" : ""}`}
            aria-current={isActive ? "page" : undefined}
          >
            {item.label}
          </Link>
        );
      })}
    </nav>
  );
}
