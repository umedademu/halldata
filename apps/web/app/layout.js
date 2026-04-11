import "./globals.css";

export const metadata = {
  title: "Hall Data Board",
  description: "Supabase に保存された店舗別の台データを一覧で確認するサイト",
};

export default function RootLayout({ children }) {
  return (
    <html lang="ja">
      <body>
        <div className="backgroundVeil" />
        <div className="appShell">{children}</div>
      </body>
    </html>
  );
}
