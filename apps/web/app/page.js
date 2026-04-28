import { redirect } from "next/navigation";
import { revalidatePath } from "next/cache";

import { StoreDirectory } from "../components/store-directory";
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
      <main className="pageStack homePage">
        <section className="heroPanel homeHero">
          <div className="heroCopy">
            <h1 className="pageTitle pageTitleCompact">店舗を選ぶ</h1>
            <p className="leadText">
              店舗名で絞り込み、必要な店舗の機種一覧へ進めます。
            </p>
          </div>
        </section>

        <StoreDirectory completeStores={completeStores} pendingStores={pendingStores} />

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
            <h1 className="pageTitle pageTitleCompact">店舗を選ぶ</h1>
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
