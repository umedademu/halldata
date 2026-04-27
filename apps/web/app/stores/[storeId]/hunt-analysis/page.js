import Link from "next/link";
import { notFound } from "next/navigation";

import { Breadcrumbs } from "../../../../components/breadcrumbs";
import { HuntRankingTable } from "../../../../components/hunt-ranking-table";
import { getHuntScoreRankingDetail, getStoreIdentity } from "../../../../lib/data";
import { formatCompactDate, formatNumber } from "../../../../lib/format";

export const dynamic = "force-dynamic";

export async function generateMetadata({ params }) {
  const resolvedParams = await params;
  const storeId = resolvedParams.storeId;

  try {
    const store = await getStoreIdentity(storeId);
    return {
      title: store ? `${store.storeName}の狙い度分析` : "狙い度分析",
    };
  } catch {
    return {
      title: "狙い度分析",
    };
  }
}

export default async function HuntAnalysisPage({ params, searchParams }) {
  const resolvedParams = await params;
  const resolvedSearchParams = await searchParams;
  const storeId = resolvedParams.storeId;
  const requestedDate =
    typeof resolvedSearchParams?.date === "string" ? resolvedSearchParams.date : "";

  let detail;

  try {
    detail = await getHuntScoreRankingDetail(storeId, requestedDate);
  } catch (error) {
    return (
      <main className="pageStack">
        <Breadcrumbs
          items={[
            { label: "店舗一覧", href: "/" },
            { label: "店舗ページ", href: `/stores/${storeId}` },
            { label: "狙い度分析" },
          ]}
        />
        <section className="statusPanel">
          <h2>狙い度分析を読み込めませんでした</h2>
          <p>{error instanceof Error ? error.message : "設定を確認してください。"}</p>
        </section>
      </main>
    );
  }

  if (!detail) {
    notFound();
  }

  const selectedDateText = detail.selectedDate ? formatCompactDate(detail.selectedDate) : "-";
  const targetDateText = detail.nextBusinessDate
    ? formatCompactDate(detail.nextBusinessDate)
    : "次回営業日（未取得）";
  const actualStateText = detail.hasActualResults ? "あり" : "なし";
  const fallbackNotice =
    detail.requestedDate && detail.requestedDate !== detail.selectedDate
      ? "指定した日付は見つからなかったため、最新の集計日を表示しています。"
      : "";

  return (
    <main className="pageStack">
      <Breadcrumbs
        items={[
          { label: "店舗一覧", href: "/" },
          { label: detail.store.storeName, href: `/stores/${detail.store.id}` },
          { label: "狙い度分析" },
        ]}
      />

      <section className="heroPanel">
        <div className="heroCopy">
          <p className="eyebrow">Hunt Score Analysis</p>
          <h1 className="pageTitle pageTitleCompact">狙い度分析</h1>
          <p className="leadText">
            集計日に見た次回営業日の狙い度を、固定ルールの絶対評価で分析し、高い順に並べた20台です。
          </p>
          <div className="heroLinks">
            <Link href={`/stores/${detail.store.id}`} className="inlineAction">
              店舗ページへ戻る
            </Link>
            <a href={detail.store.storeUrl} target="_blank" rel="noreferrer" className="inlineAction ghostAction">
              店舗ページを開く
            </a>
          </div>
        </div>
        <div className="heroMeta heroMetaWide">
          <div className="metaCard">
            <span className="metaLabel">集計日</span>
            <strong className="metaValue">{selectedDateText}</strong>
          </div>
          <div className="metaCard">
            <span className="metaLabel">予測対象日</span>
            <strong className="metaValue">{targetDateText}</strong>
          </div>
          <div className="metaCard">
            <span className="metaLabel">翌営業日実績</span>
            <strong className="metaValue">{actualStateText}</strong>
          </div>
          <div className="metaCard">
            <span className="metaLabel">表示件数</span>
            <strong className="metaValue">
              {formatNumber(detail.rows.length)} / {formatNumber(detail.totalCount)}
            </strong>
          </div>
        </div>
      </section>

      {detail.rankingDates.length > 0 ? (
        <section className="filterPanel">
          <div>
            <p className="sectionLabel">集計日を選ぶ</p>
            <p className="filterLead">
              選んだ日の時点で見た次回営業日の狙い度分析の点数順と、翌営業日の実績を表示します。
            </p>
          </div>
          <form method="get" className="storeReserveForm">
            <label className="storeReserveField">
              <span>集計日</span>
              <select name="date" defaultValue={detail.selectedDate ?? ""} className="storeReserveInput">
                {detail.rankingDates.map((date) => (
                  <option key={date} value={date}>
                    {formatCompactDate(date)}
                  </option>
                ))}
              </select>
            </label>
            <button type="submit" className="storeReserveButton">
              表示する
            </button>
          </form>
          {fallbackNotice ? <p className="storeReserveHelp">{fallbackNotice}</p> : null}
          {!detail.nextBusinessDate ? (
            <p className="filterPanelStatus">最新日のため、翌営業日の実績はまだありません。</p>
          ) : null}
        </section>
      ) : (
        <section className="statusPanel">
          <h2>狙い度分析を作れる日付がまだありません</h2>
          <p>対象機種の保存済みデータが増えると、ここに点数順の一覧が表示されます。</p>
        </section>
      )}

      {detail.rankingDates.length > 0 ? <HuntRankingTable rows={detail.rows} /> : null}
    </main>
  );
}
