import Link from "next/link";

import { getStoreList } from "../lib/data";

export const dynamic = "force-dynamic";

export default async function StoresPage() {
  try {
    const stores = await getStoreList();

    return (
      <main className="pageStack">
        <section className="heroPanel">
          <div className="heroCopy">
            <p className="eyebrow">Supabase Viewer</p>
            <h1 className="pageTitle">店舗一覧</h1>
            <p className="leadText">
              保存済みの店舗データから、機種一覧と台データ比較へ順番に進めます。
            </p>
          </div>
        </section>

        {stores.length === 0 ? (
          <section className="statusPanel">
            <h2>保存済みの店舗がまだありません</h2>
            <p>
              先にGUIアプリで台データを取得し、`Supabase` の `stores` と
              `machine_daily_results` に保存してください。
            </p>
          </section>
        ) : (
          <section className="tablePanel directoryPanel">
            <div className="tablePanelHeader">
              <div>
                <p className="sectionLabel">店舗一覧</p>
                <h2 className="tablePanelTitle">保存済み店舗</h2>
              </div>
            </div>
            <div className="tableScroller directoryScroller">
              <table className="directoryTable">
                <thead>
                  <tr>
                    <th className="directoryNameHeader">店舗</th>
                  </tr>
                </thead>
                <tbody>
                  {stores.map((store) => (
                    <tr key={store.id}>
                      <th className="directoryNameCell">
                        <Link href={`/stores/${store.id}`} className="directoryPrimaryLink">
                          {store.storeName}
                        </Link>
                      </th>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        )}
      </main>
    );
  } catch (error) {
    return (
      <main className="pageStack">
        <section className="heroPanel">
          <div className="heroCopy">
            <p className="eyebrow">Supabase Viewer</p>
            <h1 className="pageTitle">店舗一覧</h1>
          </div>
        </section>
        <section className="statusPanel">
          <h2>店舗一覧を読み込めませんでした</h2>
          <p>{error instanceof Error ? error.message : "設定を確認してください。"}</p>
        </section>
      </main>
    );
  }
}
