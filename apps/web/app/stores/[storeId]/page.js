import Link from "next/link";
import { cookies } from "next/headers";
import { notFound } from "next/navigation";

import { Breadcrumbs } from "../../../components/breadcrumbs";
import { HuntScoreLogicSelector } from "../../../components/hunt-score-logic-selector";
import { MachineDirectoryTable } from "../../../components/machine-evaluation-settings";
import { StoreFavoriteButton } from "../../../components/store-favorite-button";
import { getStoreDetail, getStoreIdentity } from "../../../lib/data";
import {
  getHuntScoreLogicDetail,
  isHuntScoreSupported,
  isHuntScoreTargetStore,
  listHuntScoreLogicOptions,
} from "../../../lib/hunt-score";
import {
  decodeHuntScoreLogicCookieValue,
  getHuntScoreLogicCookieName,
} from "../../../lib/hunt-score-logic-selection";
import {
  buildStoreMachineEvaluationSettings,
  decodeMachineEvaluationSettingsCookieValue,
  getMachineEvaluationCookieName,
} from "../../../lib/machine-evaluation";

export const dynamic = "force-dynamic";

async function readStoredHuntScoreLogicKey(storeId) {
  const cookieStore = await cookies();
  return decodeHuntScoreLogicCookieValue(
    cookieStore.get(getHuntScoreLogicCookieName(storeId))?.value ?? "",
  );
}

async function readStoredMachineEvaluationSettings(storeId) {
  const cookieStore = await cookies();
  return decodeMachineEvaluationSettingsCookieValue(
    cookieStore.get(getMachineEvaluationCookieName(storeId))?.value ?? "",
  );
}

function canOpenHuntScoreVerification(machine, storeName) {
  if (machine?.isCombinedMachineGroup) {
    return (Array.isArray(machine.childMachineNames) ? machine.childMachineNames : []).some((machineName) =>
      isHuntScoreSupported(storeName, machineName),
    );
  }
  return isHuntScoreSupported(storeName, machine?.machineName);
}

export async function generateMetadata({ params }) {
  const { storeId } = await params;

  try {
    const store = await getStoreIdentity(storeId);
    return {
      title: store ? `${store.storeName}の機種一覧` : "機種一覧",
    };
  } catch {
    return {
      title: "機種一覧",
    };
  }
}

export default async function StoreDetailPage({ params }) {
  const { storeId } = await params;
  let storeDetail;

  try {
    storeDetail = await getStoreDetail(storeId);
  } catch (error) {
    return (
      <main className="pageStack">
        <Breadcrumbs items={[{ label: "店舗一覧", href: "/" }, { label: "機種一覧" }]} />
        <section className="statusPanel">
          <h2>機種一覧を読み込めませんでした</h2>
          <p>{error instanceof Error ? error.message : "設定を確認してください。"}</p>
        </section>
      </main>
    );
  }

  if (!storeDetail) {
    notFound();
  }

  const { store, machines } = storeDetail;
  const hasHuntScoreAnalysis = isHuntScoreTargetStore(store.storeName);
  const huntScoreLogic = hasHuntScoreAnalysis
    ? getHuntScoreLogicDetail(await readStoredHuntScoreLogicKey(store.id), store.storeName)
    : null;
  const machineEvaluationSettings = hasHuntScoreAnalysis
    ? buildStoreMachineEvaluationSettings(
        store.storeName,
        machines.map((machine) => machine.machineName),
        await readStoredMachineEvaluationSettings(store.id),
      )
    : [];
  const tableMachines = machines.map((machine) => ({
    ...machine,
    canVerify: hasHuntScoreAnalysis && canOpenHuntScoreVerification(machine, store.storeName),
  }));

  return (
    <main className="pageStack">
      <Breadcrumbs
        items={[
          { label: "店舗一覧", href: "/" },
          { label: store.storeName },
        ]}
      />

      <section className="heroPanel">
        <div className="heroCopy">
          <div className="storeTitleLine">
            <h1 className="pageTitle pageTitleCompact">{store.storeName}</h1>
            <StoreFavoriteButton store={{ id: store.id, storeName: store.storeName }} compact />
          </div>
          <div className="heroLinks simpleHeroLinks">
            {hasHuntScoreAnalysis ? (
              <>
                <Link href={`/stores/${store.id}/hunt-analysis`} className="externalLink">
                  狙い度ランキングを見る
                </Link>
                <Link href={`/stores/${store.id}/hunt-backtest`} className="externalLink">
                  バックテストを見る
                </Link>
              </>
            ) : null}
            {store.storeUrl ? (
              <a href={store.storeUrl} target="_blank" rel="noreferrer" className="externalLink">
                店舗ページを開く
              </a>
            ) : null}
          </div>
          {hasHuntScoreAnalysis ? (
            <HuntScoreLogicSelector
              storeId={store.id}
              selectedLogicKey={huntScoreLogic.key}
              options={listHuntScoreLogicOptions()}
            />
          ) : null}
        </div>
      </section>

      {machines.length === 0 ? (
        <section className="statusPanel">
          <h2>この店舗には保存済みの機種データがありません</h2>
          <p>GUIアプリ側でこの店舗の台データを取得すると、ここに機種一覧が並びます。</p>
        </section>
      ) : (
        <MachineDirectoryTable
          storeId={store.id}
          machines={tableMachines}
          machineEvaluationSettings={machineEvaluationSettings}
          showHuntScoreColumns={hasHuntScoreAnalysis}
        />
      )}
    </main>
  );
}
