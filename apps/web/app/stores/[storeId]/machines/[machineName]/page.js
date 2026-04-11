import Link from "next/link";
import { notFound } from "next/navigation";

import { Breadcrumbs } from "../../../../../components/breadcrumbs";
import { MachineComparison } from "../../../../../components/machine-comparison";
import {
  getMachineDetail,
  matchesEventFilters,
  parseEventDisplayMode,
  parseEventFilters,
  readRouteSegment,
} from "../../../../../lib/data";
import {
  formatAverageGames,
  formatCompactDate,
  formatPercent,
  formatPeriod,
  formatSignedNumber,
} from "../../../../../lib/format";

export const dynamic = "force-dynamic";

export default async function MachineDetailPage({ params, searchParams }) {
  const resolvedParams = await params;
  const resolvedSearchParams = await searchParams;
  const storeId = resolvedParams.storeId;
  const machineName = readRouteSegment(resolvedParams.machineName);
  const eventFilters = parseEventFilters(resolvedSearchParams);
  const eventDisplayMode = parseEventDisplayMode(resolvedSearchParams);
  let detail;

  try {
    detail = await getMachineDetail(storeId, machineName);
  } catch (error) {
    return (
      <main className="pageStack">
        <Breadcrumbs
          items={[
            { label: "店舗一覧", href: "/" },
            { label: "機種一覧", href: `/stores/${storeId}` },
            { label: machineName },
          ]}
        />
        <section className="statusPanel">
          <h2>台データを読み込めませんでした</h2>
          <p>{error instanceof Error ? error.message : "設定を確認してください。"}</p>
        </section>
      </main>
    );
  }

  if (!detail) {
    notFound();
  }

  const visibleRows =
    eventDisplayMode === "highlight"
      ? detail.dateRows
      : detail.dateRows.filter((row) => matchesEventFilters(row.date, eventFilters));
  const highlightedDates =
    eventDisplayMode === "highlight" && eventFilters.isActive
      ? detail.dateRows.filter((row) => matchesEventFilters(row.date, eventFilters)).map((row) => row.date)
      : [];
  const machinePath = `/stores/${detail.store.id}/machines/${encodeURIComponent(machineName)}`;
  const buildFilterHref = ({ dayTail = null, zoro = null, mode = eventDisplayMode, clear = false } = {}) => {
    const nextDayTails = clear ? new Set() : new Set(eventFilters.dayTails);
    const nextZoro = clear ? false : zoro === null ? eventFilters.zoro : zoro;

    if (!clear && dayTail !== null) {
      if (nextDayTails.has(dayTail)) {
        nextDayTails.delete(dayTail);
      } else {
        nextDayTails.add(dayTail);
      }
    }

    const query = new URLSearchParams();
    const sortedDayTails = [...nextDayTails].sort((left, right) => left - right);
    if (sortedDayTails.length > 0) {
      query.set("dayTail", sortedDayTails.join(","));
    }
    if (nextZoro) {
      query.set("zoro", "1");
    }
    if (mode === "highlight") {
      query.set("eventMode", "highlight");
    }

    const queryText = query.toString();
    return queryText ? `${machinePath}?${queryText}` : machinePath;
  };

  return (
    <main className="pageStack">
      <Breadcrumbs
        items={[
          { label: "店舗一覧", href: "/" },
          { label: detail.store.storeName, href: `/stores/${detail.store.id}` },
          { label: machineName },
        ]}
      />

      <section className="heroPanel">
        <div className="heroCopy">
          <p className="eyebrow">Machine Timeline</p>
          <h1 className="pageTitle">{machineName}</h1>
          <p className="leadText">
            期間内の日付ごとに、同じ台番の差枚や回転数を横並びで見比べられます。
          </p>
          <div className="heroLinks">
            <Link href={`/stores/${detail.store.id}`} className="inlineAction">
              機種一覧へ戻る
            </Link>
            <a href={detail.store.storeUrl} target="_blank" rel="noreferrer" className="inlineAction ghostAction">
              店舗ページを開く
            </a>
          </div>
        </div>
        <div className="heroMeta heroMetaWide">
          <div className="metaCard">
            <span className="metaLabel">店舗</span>
            <strong className="metaValue">{detail.store.storeName}</strong>
          </div>
          <div className="metaCard">
            <span className="metaLabel">期間</span>
            <strong className="metaValue">{formatPeriod(detail.summary.startDate, detail.summary.endDate)}</strong>
          </div>
          <div className="metaCard">
            <span className="metaLabel">表示日数</span>
            <strong className="metaValue">
              {visibleRows.length} / {detail.summary.dayCount}
            </strong>
          </div>
        </div>
      </section>

      <section className="summaryStrip">
        <article className="summaryCard">
          <span className="summaryLabel">台数</span>
          <strong className="summaryValue">{detail.summary.slotCount}</strong>
        </article>
        <article className="summaryCard">
          <span className="summaryLabel">平均差枚</span>
          <strong className="summaryValue">{formatSignedNumber(detail.summary.averageDifference)}</strong>
        </article>
        <article className="summaryCard">
          <span className="summaryLabel">平均G数</span>
          <strong className="summaryValue">{formatAverageGames(detail.summary.averageGames)}</strong>
        </article>
        <article className="summaryCard">
          <span className="summaryLabel">平均出率</span>
          <strong className="summaryValue">{formatPercent(detail.summary.averagePayout)}</strong>
        </article>
        <article className="summaryCard">
          <span className="summaryLabel">最高日</span>
          <strong className="summaryValue">
            {detail.summary.bestDay
              ? `${formatCompactDate(detail.summary.bestDay.date)} ${formatSignedNumber(detail.summary.bestDay.value)}`
              : "-"}
          </strong>
        </article>
        <article className="summaryCard">
          <span className="summaryLabel">最低日</span>
          <strong className="summaryValue">
            {detail.summary.worstDay
              ? `${formatCompactDate(detail.summary.worstDay.date)} ${formatSignedNumber(detail.summary.worstDay.value)}`
              : "-"}
          </strong>
        </article>
      </section>

      <section className="filterPanel">
        <div>
          <p className="sectionLabel">日付の末尾を選ぶ</p>
          <p className="filterLead">
            イベント日を絞り込むか、全日を表示したまま該当日だけを強調できます。
          </p>
        </div>
        <div className="filterControlGroup">
          <p className="filterControlLabel">表示方法</p>
          <div className="dayFilterRow">
            <Link
              href={buildFilterHref({ mode: "filter" })}
              className={`dayFilterChip ${eventDisplayMode === "filter" ? "dayFilterChipActive" : ""}`}
            >
              絞り込む
            </Link>
            <Link
              href={buildFilterHref({ mode: "highlight" })}
              className={`dayFilterChip ${eventDisplayMode === "highlight" ? "dayFilterChipActive" : ""}`}
            >
              強調する
            </Link>
          </div>
        </div>
        <div className="filterControlGroup">
          <p className="filterControlLabel">日付</p>
          <div className="dayFilterRow">
            <Link
              href={buildFilterHref({ clear: true })}
              className={`dayFilterChip ${eventFilters.isActive ? "" : "dayFilterChipActive"}`}
            >
              すべて
            </Link>
            {Array.from({ length: 10 }, (_, value) => (
              <Link
                key={value}
                href={buildFilterHref({ dayTail: value })}
                className={`dayFilterChip ${eventFilters.dayTails.includes(value) ? "dayFilterChipActive" : ""}`}
              >
                末尾{value}
              </Link>
            ))}
            <Link
              href={buildFilterHref({ zoro: !eventFilters.zoro })}
              className={`dayFilterChip ${eventFilters.zoro ? "dayFilterChipActive" : ""}`}
            >
              ゾロ目
            </Link>
          </div>
        </div>
      </section>

      <MachineComparison
        machineName={machineName}
        slotNumbers={detail.slotNumbers}
        dateRows={visibleRows}
        highlightedDates={highlightedDates}
      />
    </main>
  );
}
