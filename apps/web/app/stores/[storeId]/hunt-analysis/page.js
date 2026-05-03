import Link from "next/link";
import { notFound } from "next/navigation";

import { Breadcrumbs } from "../../../../components/breadcrumbs";
import { DataSourceLabel } from "../../../../components/data-source-label";
import { JugglerOnlyButton } from "../../../../components/hunt-machine-filter-tools";
import { HuntRankingLimitSync } from "../../../../components/hunt-ranking-limit-sync";
import { HuntRankingTable } from "../../../../components/hunt-ranking-table";
import { NativeGetForm } from "../../../../components/native-get-form";
import {
  getHuntScoreInitialPageDetail,
  getHuntScoreRankingDetail,
  getStoreIdentity,
} from "../../../../lib/data";
import { formatMonthDay } from "../../../../lib/format";
import { groupHuntMachineOptions } from "../../../../lib/hunt-machine-display";

export const dynamic = "force-dynamic";

const DEFAULT_RANKING_LIMIT = 20;
const AIM_JUGGLER_GROUP_NAME = "アイムジャグラーEX";
const AIM_JUGGLER_MACHINE_NAMES = ["SアイムジャグラーＥＸ", "ネオアイムジャグラーEX"];
const HANABI_GROUP_NAME = "ハナビ";
const HANABI_MACHINE_NAMES = ["新ハナビ", "スマスロ ハナビ"];

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

function formatRankingDateOption(date, nextBusinessDate) {
  const scoreDateLabel = formatMonthDay(date);
  const actualDateLabel = nextBusinessDate ? `${formatMonthDay(nextBusinessDate)}実績` : "実績なし";
  return `${scoreDateLabel}狙い度 → ${actualDateLabel}`;
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

function isHanabiMachine(machineName) {
  const normalizedMachineName = normalizeMachineNameText(machineName);
  return HANABI_MACHINE_NAMES.some(
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

function normalizeCombineHanabi(values) {
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

function resolveRankingGroupName(machineName, combineAimJuggler, combineHanabi) {
  if (combineAimJuggler && isAimJugglerMachine(machineName)) {
    return AIM_JUGGLER_GROUP_NAME;
  }
  if (combineHanabi && isHanabiMachine(machineName)) {
    return HANABI_GROUP_NAME;
  }
  return String(machineName ?? "").trim();
}

function buildVisibleRankingGroups(
  rankingGroups,
  selectedMachineNameSet,
  combineAimJuggler,
  combineHanabi,
  displayLimit,
) {
  const groupsByName = new Map();

  for (const group of Array.isArray(rankingGroups) ? rankingGroups : []) {
    const machineName = String(group.machineName ?? "").trim();
    if (!selectedMachineNameSet.has(machineName)) {
      continue;
    }

    const rankingGroupName = resolveRankingGroupName(machineName, combineAimJuggler, combineHanabi);
    if (!groupsByName.has(rankingGroupName)) {
      groupsByName.set(rankingGroupName, {
        machineName: rankingGroupName,
        rows: [],
        totalCount: 0,
        isCombinedGroup:
          (rankingGroupName === AIM_JUGGLER_GROUP_NAME || rankingGroupName === HANABI_GROUP_NAME) &&
          machineName !== rankingGroupName,
      });
    }

    const rankingGroup = groupsByName.get(rankingGroupName);
    const sourceRows = Array.isArray(group.allRows) ? group.allRows : group.rows;
    rankingGroup.totalCount += group.totalCount ?? sourceRows?.length ?? 0;
    rankingGroup.rows.push(
      ...(Array.isArray(sourceRows) ? sourceRows : []).map((row) => ({
        ...row,
        machineName: String(row.machineName ?? machineName).trim(),
      })),
    );
  }

  return [...groupsByName.values()]
    .map((group) => {
      const rankedAllRows = group.rows
        .sort(compareRankingRows)
        .map((row, index) => ({
          ...row,
          rank: index + 1,
          machineRank: index + 1,
        }));
      const rankedRows = rankedAllRows.slice(0, displayLimit);

      return {
        ...group,
        limit: Math.min(displayLimit, group.totalCount),
        allRows: rankedAllRows,
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
  const resultRequested = readSingleSearchParam(resolvedSearchParams?.show) === "1";
  const requestedLimit = parseRequestedLimit(readSingleSearchParam(resolvedSearchParams?.limit));
  const requestedMachineNames = readMultiSearchParam(resolvedSearchParams?.machine);
  const machineFilterTouched = readSingleSearchParam(resolvedSearchParams?.machineTouched) === "1";
  const requestedCombineAimJuggler = normalizeCombineAimJuggler(
    readMultiSearchParam(resolvedSearchParams?.aimMachineGroup),
  );
  const requestedCombineHanabi = normalizeCombineHanabi(
    readMultiSearchParam(resolvedSearchParams?.hanabiMachineGroup),
  );

  let detail;

  try {
    detail = resultRequested
      ? await getHuntScoreRankingDetail(storeId, requestedDate, requestedLimit)
      : await getHuntScoreInitialPageDetail(storeId);
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
    resultRequested && detail.requestedDate && detail.requestedDate !== detail.selectedDate
      ? "指定した日付は見つからなかったため、最新の集計日を表示しています。"
      : "";
  const availableMachineNames =
    Array.isArray(detail.availableMachineNames) && detail.availableMachineNames.length > 0
      ? detail.availableMachineNames
      : detail.rankingGroups.map((group) => group.machineName);
  const rankingDateOptions =
    Array.isArray(detail.rankingDateOptions) && detail.rankingDateOptions.length > 0
      ? detail.rankingDateOptions
      : detail.rankingDates.map((date) => ({
          date,
          nextBusinessDate: date === detail.selectedDate ? detail.nextBusinessDate : null,
        }));
  const availableMachineNameSet = new Set(availableMachineNames);
  const hasAimJugglerGroupOption = AIM_JUGGLER_MACHINE_NAMES.every((machineName) =>
    availableMachineNameSet.has(machineName),
  );
  const hasHanabiGroupOption = HANABI_MACHINE_NAMES.every((machineName) =>
    availableMachineNameSet.has(machineName),
  );
  const combineAimJuggler = hasAimJugglerGroupOption ? requestedCombineAimJuggler : false;
  const combineHanabi = hasHanabiGroupOption ? requestedCombineHanabi : false;
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
  const machineOptionGroups = groupHuntMachineOptions(machineOptions);
  const visibleRankingGroups = buildVisibleRankingGroups(
    resultRequested ? detail.rankingGroups : [],
    selectedMachineNameSet,
    combineAimJuggler,
    combineHanabi,
    detail.limit,
  );
  const allChoiceRankingGroups = buildVisibleRankingGroups(
    resultRequested ? detail.rankingGroups : [],
    availableMachineNameSet,
    combineAimJuggler,
    combineHanabi,
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

      {availableMachineNames.length > 0 ? (
        <>
          <section className="filterPanel">
            <div>
              <p className="sectionLabel">集計日を選ぶ</p>
              <p className="filterLead">
                選んだ日の狙い度と、その次の営業日の実績を並べて確認できます。
              </p>
            </div>
            <NativeGetForm action={`/stores/${detail.store.id}/hunt-analysis`} className="storeReserveForm">
              <input type="hidden" name="show" value="1" />
              <input type="hidden" name="machineTouched" value="1" />
              <label className="storeReserveField">
                <span>狙い度の日</span>
                {resultRequested && rankingDateOptions.length > 0 ? (
                  <select name="date" defaultValue={detail.selectedDate ?? ""} className="storeReserveInput">
                    {rankingDateOptions.map((option) => (
                      <option key={option.date} value={option.date}>
                        {formatRankingDateOption(option.date, option.nextBusinessDate)}
                      </option>
                    ))}
                  </select>
                ) : (
                  <input
                    type="date"
                    name="date"
                    defaultValue={detail.selectedDate ?? ""}
                    className="storeReserveInput"
                  />
                )}
              </label>
              <label className="storeReserveField">
                <span>各機種何位まで表示</span>
                <input
                  type="number"
                  name="limit"
                  min="1"
                  max={Math.max(detail.totalCount, detail.limit, 1)}
                  defaultValue={detail.limit}
                  className="storeReserveInput"
                />
              </label>
              {machineOptions.length > 0 ? (
                <div className="backtestBlock rankingMachineFilter">
                  <p className="filterControlLabel">機種名</p>
                  {hasAimJugglerGroupOption ? (
                    <input type="hidden" name="aimMachineGroup" value="0" />
                  ) : null}
                  {hasHanabiGroupOption ? (
                    <input type="hidden" name="hanabiMachineGroup" value="0" />
                  ) : null}
                  {hasAimJugglerGroupOption || hasHanabiGroupOption ? (
                    <div className="machineGroupToggleRow">
                      {hasAimJugglerGroupOption ? (
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
                          <span>Sアイムとネオアイムをまとめる</span>
                        </label>
                      ) : null}
                      {hasHanabiGroupOption ? (
                        <label
                          className={`metricToggleChip ${
                            combineHanabi ? "metricToggleChipActive" : ""
                          }`}
                        >
                          <input
                            type="checkbox"
                            name="hanabiMachineGroup"
                            value="1"
                            defaultChecked={combineHanabi}
                          />
                          <span>新ハナビとスマハナビをまとめる</span>
                        </label>
                      ) : null}
                    </div>
                  ) : null}
                  <div className="machineFilterActionRow">
                    <JugglerOnlyButton />
                  </div>
                  <div className="machineFilterGroups">
                    {machineOptionGroups.map((group) => (
                      <div key={group.key} className="machineFilterGroup">
                        <p className="machineFilterGroupLabel">{group.label}</p>
                        <div className="metricToggleRow">
                          {group.options.map((machine) => (
                            <label
                              key={machine.name}
                              className={`metricToggleChip ${
                                machine.checked ? "metricToggleChipActive" : ""
                              }`}
                              title={machine.name}
                            >
                              <input
                                type="checkbox"
                                name="machine"
                                value={machine.name}
                                defaultChecked={machine.checked}
                                data-machine-filter-option="1"
                                data-machine-category={machine.category}
                              />
                              <span>{machine.shortName}</span>
                            </label>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              ) : null}
              <button type="submit" className="storeReserveButton">
                表示する
              </button>
            </NativeGetForm>
            {fallbackNotice ? <p className="storeReserveHelp">{fallbackNotice}</p> : null}
            {resultRequested && !detail.nextBusinessDate ? (
              <p className="filterPanelStatus">最新日のため、翌営業日の実績はまだありません。</p>
            ) : null}
          </section>

          {resultRequested ? (
            detail.rankingDates.length > 0 ? (
              <HuntRankingTable
                storeId={detail.store.id}
                rows={visibleRows}
                rankingGroups={visibleRankingGroups}
                allRankingGroups={allChoiceRankingGroups}
                overallLimit={detail.limit}
                predictionDate={detail.predictionDate}
                actualDate={detail.nextBusinessDate}
              />
            ) : (
              <section className="statusPanel">
                <h2>狙い度ランキングを作れる日付がまだありません</h2>
                <p>対象機種の保存済みデータが増えると、ここに点数順の一覧が表示されます。</p>
              </section>
            )
          ) : (
            <section className="statusPanel">
              <h2>狙い度ランキングはまだ表示していません</h2>
              <p>条件を選んで表示すると、対象機種の台データを読み込んで集計します。</p>
            </section>
          )}
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
