import Link from "next/link";
import { notFound } from "next/navigation";

import { Breadcrumbs } from "../../../../components/breadcrumbs";
import { DataSourceLabel } from "../../../../components/data-source-label";
import { HuntRankingLimitSync } from "../../../../components/hunt-ranking-limit-sync";
import { HuntRankingTable } from "../../../../components/hunt-ranking-table";
import { NativeGetForm } from "../../../../components/native-get-form";
import { getHuntScoreRankingDetail, getStoreIdentity } from "../../../../lib/data";
import { formatCompactDate } from "../../../../lib/format";

export const dynamic = "force-dynamic";

const DEFAULT_RANKING_LIMIT = 20;
const AIM_JUGGLER_GROUP_NAME = "アイムジャグラーEX";
const AIM_JUGGLER_MACHINE_NAMES = ["SアイムジャグラーＥＸ", "ネオアイムジャグラーEX"];

function readSingleSearchParam(value) {
  if (Array.isArray(value)) {
    return typeof value[0] === "string" ? value[0] : "";
  }
  return typeof value === "string" ? value : "";
}

function readMultiSearchParam(value) {
  if (Array.isArray(value)) {
    return value.filter((entry) => typeof entry === "string");
  }
  return typeof value === "string" ? [value] : [];
}

function parseRequestedLimit(value) {
  const parsedValue = Number(value);
  if (!Number.isInteger(parsedValue) || parsedValue < 1) {
    return DEFAULT_RANKING_LIMIT;
  }
  return parsedValue;
}

function normalizeMachineNameText(value) {
  return String(value ?? "").normalize("NFKC").replace(/\s+/gu, "").trim();
}

function isAimJugglerMachine(machineName) {
  const normalizedMachineName = normalizeMachineNameText(machineName);
  return AIM_JUGGLER_MACHINE_NAMES.some(
    (candidate) => normalizeMachineNameText(candidate) === normalizedMachineName,
  );
}

function normalizeCombineAimJuggler(values) {
  const safeValues = (Array.isArray(values) ? values : [])
    .map((value) => String(value ?? "").trim())
    .filter(Boolean);
  if (safeValues.length === 0) {
    return true;
  }
  return safeValues.includes("1") || safeValues.includes("true") || safeValues.includes("on");
}

function readRankingSortNumber(value, fallbackValue = Number.MAX_SAFE_INTEGER) {
  const parsedValue = Number(value);
  return Number.isFinite(parsedValue) ? parsedValue : fallbackValue;
}

function compareRankingRows(left, right) {
  return (
    readRankingSortNumber(right.huntScore, 0) - readRankingSortNumber(left.huntScore, 0) ||
    readRankingSortNumber(left.overallRank ?? left.selectedRank ?? left.rank) -
      readRankingSortNumber(right.overallRank ?? right.selectedRank ?? right.rank) ||
    String(left.machineName ?? "").localeCompare(String(right.machineName ?? ""), "ja") ||
    String(left.slotNumber ?? "").localeCompare(String(right.slotNumber ?? ""), "ja", {
      numeric: true,
    })
  );
}

function resolveRankingGroupName(machineName, combineAimJuggler) {
  if (combineAimJuggler && isAimJugglerMachine(machineName)) {
    return AIM_JUGGLER_GROUP_NAME;
  }
  return String(machineName ?? "").trim();
}

function buildVisibleRankingGroups(rankingGroups, selectedMachineNameSet, combineAimJuggler, displayLimit) {
  const groupsByName = new Map();

  for (const group of Array.isArray(rankingGroups) ? rankingGroups : []) {
    const machineName = String(group.machineName ?? "").trim();
    if (!selectedMachineNameSet.has(machineName)) {
      continue;
    }

    const rankingGroupName = resolveRankingGroupName(machineName, combineAimJuggler);
    if (!groupsByName.has(rankingGroupName)) {
      groupsByName.set(rankingGroupName, {
        machineName: rankingGroupName,
        rows: [],
        totalCount: 0,
        isCombinedGroup: rankingGroupName === AIM_JUGGLER_GROUP_NAME && machineName !== rankingGroupName,
      });
    }

    const rankingGroup = groupsByName.get(rankingGroupName);
    rankingGroup.totalCount += group.totalCount ?? group.rows?.length ?? 0;
    rankingGroup.rows.push(
      ...(Array.isArray(group.rows) ? group.rows : []).map((row) => ({
        ...row,
        machineName: String(row.machineName ?? machineName).trim(),
      })),
    );
  }

  return [...groupsByName.values()]
    .map((group) => {
      const rankedRows = group.rows
        .sort(compareRankingRows)
        .slice(0, displayLimit)
        .map((row, index) => ({
          ...row,
          rank: index + 1,
          machineRank: index + 1,
        }));

      return {
        ...group,
        limit: Math.min(displayLimit, group.totalCount),
        rows: rankedRows,
      };
    })
    .filter((group) => group.rows.length > 0);
}

export async function generateMetadata({ params }) {
  const resolvedParams = await params;
  const storeId = resolvedParams.storeId;

  try {
    const store = await getStoreIdentity(storeId);
    return {
      title: store ? `${store.storeName}の狙い度ランキング` : "狙い度ランキング",
    };
  } catch {
    return {
      title: "狙い度ランキング",
    };
  }
}

export default async function HuntAnalysisPage({ params, searchParams }) {
  const resolvedParams = await params;
  const resolvedSearchParams = await searchParams;
  const storeId = resolvedParams.storeId;
  const requestedDate = readSingleSearchParam(resolvedSearchParams?.date);
  const requestedLimit = parseRequestedLimit(readSingleSearchParam(resolvedSearchParams?.limit));
  const requestedMachineNames = readMultiSearchParam(resolvedSearchParams?.machine);
  const machineFilterTouched = readSingleSearchParam(resolvedSearchParams?.machineTouched) === "1";
  const requestedCombineAimJuggler = normalizeCombineAimJuggler(
    readMultiSearchParam(resolvedSearchParams?.aimMachineGroup),
  );

  let detail;

  try {
    detail = await getHuntScoreRankingDetail(storeId, requestedDate, requestedLimit);
  } catch (error) {
    return (
      <main className="pageStack">
        <Breadcrumbs
          items={[
            { label: "店舗一覧", href: "/" },
            { label: "店舗ページ", href: `/stores/${storeId}` },
            { label: "狙い度ランキング" },
          ]}
        />
        <section className="statusPanel">
          <h2>狙い度ランキングを読み込めませんでした</h2>
          <p>{error instanceof Error ? error.message : "設定を確認してください。"}</p>
        </section>
      </main>
    );
  }

  if (!detail) {
    notFound();
  }

  const fallbackNotice =
    detail.requestedDate && detail.requestedDate !== detail.selectedDate
      ? "指定した日付は見つからなかったため、最新の集計日を表示しています。"
      : "";
  const availableMachineNames = detail.rankingGroups.map((group) => group.machineName);
  const availableMachineNameSet = new Set(availableMachineNames);
  const hasAimJugglerGroupOption = AIM_JUGGLER_MACHINE_NAMES.every((machineName) =>
    availableMachineNameSet.has(machineName),
  );
  const combineAimJuggler = hasAimJugglerGroupOption ? requestedCombineAimJuggler : false;
  const requestedMachineNameSet = new Set(
    requestedMachineNames
      .map((machineName) => String(machineName ?? "").trim())
      .filter((machineName) => availableMachineNameSet.has(machineName)),
  );
  const selectedMachineNameSet = machineFilterTouched
    ? requestedMachineNameSet
    : new Set(availableMachineNames);
  const machineOptions = availableMachineNames.map((machineName) => ({
    name: machineName,
    checked: selectedMachineNameSet.has(machineName),
  }));
  const visibleRankingGroups = buildVisibleRankingGroups(
    detail.rankingGroups,
    selectedMachineNameSet,
    combineAimJuggler,
    detail.limit,
  );
  const visibleRows = visibleRankingGroups.flatMap((group) => group.rows);

  return (
    <main className="pageStack">
      <HuntRankingLimitSync defaultLimit={DEFAULT_RANKING_LIMIT} />
      <Breadcrumbs
        items={[
          { label: "店舗一覧", href: "/" },
          { label: detail.store.storeName, href: `/stores/${detail.store.id}` },
          { label: "狙い度ランキング" },
        ]}
      />

      <section className="heroPanel">
        <div className="heroCopy">
          <h1 className="pageTitle pageTitleCompact">狙い度ランキング</h1>
          <DataSourceLabel source={detail.dataSource} />
          <div className="heroLinks simpleHeroLinks">
            <Link href={`/stores/${detail.store.id}`} className="externalLink">
              店舗ページへ戻る
            </Link>
            <Link href={`/stores/${detail.store.id}/hunt-backtest`} className="externalLink">
              バックテストを見る
            </Link>
            {detail.store.storeUrl ? (
              <a href={detail.store.storeUrl} target="_blank" rel="noreferrer" className="externalLink">
                店舗ページを開く
              </a>
            ) : null}
          </div>
        </div>
      </section>

      {detail.rankingDates.length > 0 ? (
        <>
          <section className="filterPanel">
            <div>
              <p className="sectionLabel">集計日を選ぶ</p>
              <p className="filterLead">
                選んだ日の時点で見た次回営業日の狙い度を、機種ごとの高い順で確認できます。
              </p>
            </div>
            <NativeGetForm action={`/stores/${detail.store.id}/hunt-analysis`} className="storeReserveForm">
              <input type="hidden" name="machineTouched" value="1" />
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
              <label className="storeReserveField">
                <span>各機種何位まで表示</span>
                <input
                  type="number"
                  name="limit"
                  min="1"
                  max={Math.max(detail.totalCount, 1)}
                  defaultValue={detail.limit}
                  className="storeReserveInput"
                />
              </label>
              {machineOptions.length > 0 ? (
                <div className="backtestBlock rankingMachineFilter">
                  <p className="filterControlLabel">機種名</p>
                  {hasAimJugglerGroupOption ? (
                    <>
                      <input type="hidden" name="aimMachineGroup" value="0" />
                      <label
                        className={`metricToggleChip ${
                          combineAimJuggler ? "metricToggleChipActive" : ""
                        }`}
                      >
                        <input
                          type="checkbox"
                          name="aimMachineGroup"
                          value="1"
                          defaultChecked={combineAimJuggler}
                        />
                        <span>SアイムジャグラーEXとネオアイムジャグラーEXをまとめる</span>
                      </label>
                    </>
                  ) : null}
                  <div className="metricToggleRow">
                    {machineOptions.map((machine) => (
                      <label
                        key={machine.name}
                        className={`metricToggleChip ${
                          machine.checked ? "metricToggleChipActive" : ""
                        }`}
                      >
                        <input
                          type="checkbox"
                          name="machine"
                          value={machine.name}
                          defaultChecked={machine.checked}
                        />
                        <span>{machine.name}</span>
                      </label>
                    ))}
                  </div>
                </div>
              ) : null}
              <button type="submit" className="storeReserveButton">
                表示する
              </button>
            </NativeGetForm>
            {fallbackNotice ? <p className="storeReserveHelp">{fallbackNotice}</p> : null}
            {!detail.nextBusinessDate ? (
              <p className="filterPanelStatus">最新日のため、翌営業日の実績はまだありません。</p>
            ) : null}
          </section>

          <HuntRankingTable
            storeId={detail.store.id}
            rows={visibleRows}
            rankingGroups={visibleRankingGroups}
            overallLimit={detail.limit}
          />
        </>
      ) : (
        <section className="statusPanel">
          <h2>狙い度ランキングを作れる日付がまだありません</h2>
          <p>対象機種の保存済みデータが増えると、ここに点数順の一覧が表示されます。</p>
        </section>
      )}
    </main>
  );
}
