import "./globals.css";

import { MyHallCloudWriter } from "../components/my-hall-cloud-writer";
import { TopNavigation } from "../components/top-navigation";

export const metadata = {
  title: {
    default: "Hall Data Board",
    template: "%s | Hall Data Board",
  },
  description: "JSONに保存された店舗別の台データを一覧で確認するサイト",
};

export default function RootLayout({ children }) {
  return (
    <html lang="ja">
      <body>
        <div className="backgroundVeil" />
        <div className="appShell">
          <MyHallCloudWriter />
          <TopNavigation />
          {children}
        </div>
      </body>
    </html>
  );
}
