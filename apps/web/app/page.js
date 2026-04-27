import Link from "next/link";
import { redirect } from "next/navigation";
import { revalidatePath } from "next/cache";

import { getStoreList, registerPendingStoreUrl } from "../lib/data";

export const dynamic = "force-dynamic";
export const metadata = {
  title: "店舗一覧",
};

async function registerStoreReservation(formData) {
  "use server";

  let result;
  try {
    result = await registerPendingStoreUrl(formData.get("storeUrl"));
  } catch (error) {
    const message = error instanceof Error ? error.message : "登録できませんでした。";
    redirect(`/?storeRegistration=error&message=${encodeURIComponent(message)}`);
  }

  revalidatePath("/");
  redirect(`/?storeRegistration=${result.status}`);
}

function buildRegistrationNotice(searchParams) {
  const status = searchParams.storeRegistration;
  if (status === "created") {
    return { kind: "success", text: "店舗URLを登録待ちに追加しました。" };
  }
  if (status === "exists") {
    return { kind: "info", text: "この店舗URLはすでに登録されています。" };
  }
  if (status === "error") {
    return {
      kind: "error",
      text: searchParams.message || "店舗URLを登録できませんでした。",
    };
  }
  return null;
}

export default async function StoresPage({ searchParams }) {
  try {
    const resolvedSearchParams = searchParams ? await searchParams : {};
    const registrationNotice = buildRegistrationNotice(resolvedSearchParams);
    const stores = await getStoreList();
    const completeStores = stores.filter((store) => !store.isPendingRegistration);
    const pendingStores = stores.filter((store) => store.isPendingRegistration);

    return (
      <main className="pageStack">
        <section className="heroPanel">
          <div className="heroCopy">
            <p className="eyebrow">Supabase Viewer</p>
            <h1 className="pageTitle pageTitleCompact">店舗一覧</h1>
            <p className="leadText">
              保存済みの店舗データから、機種一覧と台データ比較へ順番に進めます。
            </p>
          </div>
        </section>

        {pendingStores.length > 0 ? (
          <section className="tablePanel directoryPanel">
            <div className="tablePanelHeader">
              <div>
                <p className="sectionLabel">登録待ち</p>
                <h2 className="tablePanelTitle">店舗URL</h2>
              </div>
            </div>
            <div className="tableScroller directoryScroller">
              <table className="directoryTable">
                <thead>
                  <tr>
                    <th>URL</th>
                    <th>状態</th>
                  </tr>
                </thead>
                <tbody>
                  {pendingStores.map((store) => (
                    <tr key={store.id}>
                      <td>{store.storeUrl}</td>
                      <td>店舗名取得待ち</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        ) : null}

        {completeStores.length === 0 ? (
          <section className="statusPanel">
            <h2>完全登録済みの店舗がまだありません</h2>
            <p>
              先にGUIアプリで登録待ちURLを更新するか、台データを取得してください。
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
                  {completeStores.map((store) => (
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

        <section className="tablePanel storeReservePanel">
          <div className="tablePanelHeader">
            <div>
              <p className="sectionLabel">登録予約</p>
              <h2 className="tablePanelTitle">店舗URLを追加</h2>
            </div>
          </div>
          <form action={registerStoreReservation} className="storeReserveForm">
            <label className="storeReserveField">
              <span>店舗URL</span>
              <input
                className="storeReserveInput"
                name="storeUrl"
                type="url"
                placeholder="https://min-repo.com/tag/..."
                required
              />
            </label>
            <button className="storeReserveButton" type="submit">
              登録待ちに追加
            </button>
          </form>
          <p className="storeReserveHelp">
            店舗名はここでは取得せず、GUIアプリの更新または定期取得で補完します。
          </p>
          {registrationNotice ? (
            <p className={`storeReserveNotice storeReserveNotice-${registrationNotice.kind}`}>
              {registrationNotice.text}
            </p>
          ) : null}
        </section>
      </main>
    );
  } catch (error) {
    return (
      <main className="pageStack">
        <section className="heroPanel">
          <div className="heroCopy">
            <p className="eyebrow">Supabase Viewer</p>
            <h1 className="pageTitle pageTitleCompact">店舗一覧</h1>
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
