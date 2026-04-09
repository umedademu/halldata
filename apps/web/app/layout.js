import { Bebas_Neue, Noto_Sans_JP } from "next/font/google";

import "./globals.css";

const displayFont = Bebas_Neue({
  subsets: ["latin"],
  weight: "400",
  variable: "--font-display",
});

const bodyFont = Noto_Sans_JP({
  subsets: ["latin"],
  weight: ["400", "500", "700", "900"],
  variable: "--font-body",
});

export const metadata = {
  title: "Hall Data Board",
  description: "Supabase に保存された店舗別の台データを一覧で確認するサイト",
};

export default function RootLayout({ children }) {
  return (
    <html lang="ja">
      <body className={`${displayFont.variable} ${bodyFont.variable}`}>
        <div className="backgroundVeil" />
        <div className="backgroundBurst backgroundBurstLeft" />
        <div className="backgroundBurst backgroundBurstRight" />
        <div className="appShell">{children}</div>
      </body>
    </html>
  );
}
