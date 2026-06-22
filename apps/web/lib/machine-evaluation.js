const MACHINE_EVALUATION_COOKIE_PREFIX = "machine-evaluation-";
const MACHINE_EVALUATION_COOKIE_VERSION = 1;

export const MACHINE_EVALUATION_BACKTEST_MODE_COMMON = "common";
export const MACHINE_EVALUATION_BACKTEST_MODE_MACHINE = "machine";
export const MACHINE_EVALUATION_BACKTEST_MODE_AND = "and";
export const MACHINE_EVALUATION_BACKTEST_MODE_OR = "or";

export const MACHINE_EVALUATION_RANKING_MODE_NONE = "none";
export const MACHINE_EVALUATION_RANKING_MODE_SHOW = "show";
export const MACHINE_EVALUATION_RANKING_MODE_PRIORITY = "priority";
export const MACHINE_EVALUATION_RANKING_MODE_FILTER = "filter";
export const MACHINE_EVALUATION_RANKING_MODE_SCORE = "score";

export const MACHINE_EVALUATION_BACKTEST_MODE_OPTIONS = [
  {
    value: MACHINE_EVALUATION_BACKTEST_MODE_COMMON,
    label: "使用しない",
  },
  {
    value: MACHINE_EVALUATION_BACKTEST_MODE_MACHINE,
    label: "機種別採用条件のみ",
  },
  {
    value: MACHINE_EVALUATION_BACKTEST_MODE_AND,
    label: "共通条件 AND 機種別採用条件",
  },
  {
    value: MACHINE_EVALUATION_BACKTEST_MODE_OR,
    label: "共通条件 OR 機種別採用条件",
  },
];

export const MACHINE_EVALUATION_RANKING_MODE_OPTIONS = [
  {
    value: MACHINE_EVALUATION_RANKING_MODE_SHOW,
    label: "機種別評価を表示",
  },
  {
    value: MACHINE_EVALUATION_RANKING_MODE_PRIORITY,
    label: "採用条件一致を優先",
  },
  {
    value: MACHINE_EVALUATION_RANKING_MODE_FILTER,
    label: "採用条件不一致を除外",
  },
  {
    value: MACHINE_EVALUATION_RANKING_MODE_SCORE,
    label: "機種別点数順",
  },
  {
    value: MACHINE_EVALUATION_RANKING_MODE_NONE,
    label: "表示しない",
  },
];

function normalizeText(value) {
  return String(value ?? "").trim();
}

function normalizeMachineNameText(value) {
  return normalizeText(value).normalize("NFKC").replace(/\s+/gu, "");
}

function readNumber(value, fallbackValue = 0) {
  const parsedValue = Number(value);
  return Number.isFinite(parsedValue) ? parsedValue : fallbackValue;
}

function readNullableNumber(value) {
  const parsedValue = Number(value);
  return Number.isFinite(parsedValue) ? parsedValue : null;
}

function clamp(value, minValue, maxValue) {
  return Math.max(minValue, Math.min(maxValue, value));
}

function scale(value, minValue, maxValue, points) {
  if (!Number.isFinite(value) || maxValue <= minValue) {
    return 0;
  }
  return clamp((value - minValue) / (maxValue - minValue), 0, 1) * points;
}

function rateDenominator(games, count) {
  return games > 0 && count > 0 ? games / count : 9999;
}

function netPerThousandGames(netTotal, gamesTotal) {
  return gamesTotal > 0 ? (netTotal / gamesTotal) * 1000 : 0;
}

function scoreAtMost(value, tiers) {
  if (!Number.isFinite(value)) {
    return 0;
  }
  for (const tier of tiers) {
    if (value <= tier.maximum) {
      return tier.points;
    }
  }
  return 0;
}

function scoreAtLeast(value, tiers) {
  if (!Number.isFinite(value)) {
    return 0;
  }
  for (const tier of tiers) {
    if (value >= tier.minimum) {
      return tier.points;
    }
  }
  return 0;
}

function scoreInRange(value, minimum, maximum, points) {
  return Number.isFinite(value) && value >= minimum && value <= maximum ? points : 0;
}

function isAparkKasugaStore(storeName) {
  return normalizeMachineNameText(storeName) === normalizeMachineNameText("Aパーク春日店");
}

function isAparkYakatabaruStore(storeName) {
  const normalizedStoreName = normalizeMachineNameText(storeName);
  return ["A-PARK屋形原", "A-PARK屋形原店", "Aパーク屋形原", "Aパーク屋形原店"].some(
    (candidateName) => normalizedStoreName === normalizeMachineNameText(candidateName),
  );
}

function isGogoArenaTenjinStore(storeName) {
  const normalizedStoreName = normalizeMachineNameText(storeName);
  return ["GOGOアリーナ天神", "GOGOアリーナ天神店", "ＧＯＧＯアリーナ天神", "ＧＯＧＯアリーナ天神店"].some(
    (candidateName) => normalizedStoreName === normalizeMachineNameText(candidateName),
  );
}

function isMjArenaKurumeStore(storeName) {
  const normalizedStoreName = normalizeMachineNameText(storeName);
  return [
    "MJアリーナ久留米店",
    "MJアリーナ久留米",
    "ＭＪアリーナ久留米店",
    "ＭＪアリーナ久留米",
  ].some((candidateName) => normalizedStoreName === normalizeMachineNameText(candidateName));
}

function isBeamHikariStore(storeName) {
  const normalizedStoreName = normalizeMachineNameText(storeName);
  return ["ビームヒカリ店", "ビームヒカリ", "BEAM HIKARI", "BEAMHIKARI", "ＢＥＡＭヒカリ店"].some(
    (candidateName) => normalizedStoreName === normalizeMachineNameText(candidateName),
  );
}

function isHinodeOnojoStore(storeName) {
  const normalizedStoreName = normalizeMachineNameText(storeName);
  return ["HINODE大野城店", "HINODE大野城"].some(
    (candidateName) => normalizedStoreName === normalizeMachineNameText(candidateName),
  );
}

function isSuperDstationChikushinoStore(storeName) {
  const normalizedStoreName = normalizeMachineNameText(storeName);
  return [
    "スーパーDステーション39筑紫野店",
    "スーパーDステーション筑紫野店",
    "スーパーＤステーション３９筑紫野店",
    "スーパーＤステーション筑紫野店",
    "スーパーＤ’ステーション３９筑紫野店",
  ].some((candidateName) => normalizedStoreName === normalizeMachineNameText(candidateName));
}

function isEspaceUenoStore(storeName) {
  const normalizedStoreName = normalizeMachineNameText(storeName);
  return [
    "エスパス日拓上野本館",
    "エスパス日拓上野本館店",
    "エスパス上野本館",
    "エスパス上野本館店",
  ].some((candidateName) => normalizedStoreName === normalizeMachineNameText(candidateName));
}

function isMesseMinamisenjuStore(storeName) {
  const normalizedStoreName = normalizeMachineNameText(storeName);
  return ["メッセ南千住店", "メッセ南千住"].some(
    (candidateName) => normalizedStoreName === normalizeMachineNameText(candidateName),
  );
}

function isKintokiKamataStore(storeName) {
  const normalizedStoreName = normalizeMachineNameText(storeName);
  return ["金時蒲田東口店", "金時蒲田東口"].some(
    (candidateName) => normalizedStoreName === normalizeMachineNameText(candidateName),
  );
}

function readDateDayNumber(dateText) {
  const normalized = normalizeText(dateText);
  const match = normalized.match(/^\d{4}-\d{2}-(\d{2})$/u) ?? normalized.match(/^\d{2}\/\d{2}\/(\d{2})$/u);
  if (!match) {
    return null;
  }
  const dayNumber = Number(match[1]);
  return Number.isFinite(dayNumber) ? dayNumber : null;
}

function addDaysToDateText(dateText, days) {
  const normalized = normalizeText(dateText);
  const match =
    normalized.match(/^(\d{4})-(\d{2})-(\d{2})$/u) ??
    normalized.match(/^(\d{2})\/(\d{2})\/(\d{2})$/u);
  if (!match) {
    return "";
  }
  const year = match[1].length === 2 ? 2000 + Number(match[1]) : Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
    return "";
  }
  const date = new Date(Date.UTC(year, month - 1, day));
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function readRankingTargetDate(snapshot) {
  const nextBusinessDate = normalizeText(snapshot?.nextBusinessDate);
  if (nextBusinessDate) {
    return nextBusinessDate;
  }
  const baseDate = normalizeText(snapshot?.baseDate ?? snapshot?.date);
  return addDaysToDateText(baseDate, 1) || baseDate;
}

function isBeamHikariEventDate(dateText) {
  const dayNumber = readDateDayNumber(dateText);
  if (!Number.isFinite(dayNumber)) {
    return false;
  }
  const dayTail = dayNumber % 10;
  return dayTail === 3 || dayTail === 6;
}

function isAmuseAsakusaStore(storeName) {
  const normalizedStoreName = normalizeMachineNameText(storeName);
  return ["アミューズ浅草店", "アミューズ浅草", "AMUSE浅草店", "AMUSE浅草", "ＡＭＵＳＥ浅草店", "ＡＭＵＳＥ浅草"].some(
    (candidateName) => normalizedStoreName === normalizeMachineNameText(candidateName),
  );
}

function readBacktestPayoutRate(backtestLabel) {
  const match = String(backtestLabel ?? "").match(/(\d+(?:\.\d+)?)%/u);
  if (!match) {
    return null;
  }
  const value = Number(match[1]);
  return Number.isFinite(value) ? value : null;
}

function readBacktestRbDenominator(backtestLabel) {
  const normalizedLabel = String(backtestLabel ?? "").normalize("NFKC");
  const match = normalizedLabel.match(/RB(?:率)?\s*(?:1\s*\/)?\s*(\d+(?:\.\d+)?)/iu);
  if (!match) {
    return null;
  }
  const value = Number(match[1]);
  return Number.isFinite(value) ? value : null;
}

function buildCondition(keySuffix, name, backtestLabel, matcher, logicKeys = []) {
  return {
    keySuffix,
    name,
    backtestLabel,
    backtestPayoutRate: readBacktestPayoutRate(backtestLabel),
    backtestRbDenominator: readBacktestRbDenominator(backtestLabel),
    matcher,
    logicKeys,
  };
}

function buildLogicVariant(logicKey, logicName, defaultConditionSuffix = "main") {
  return {
    key: logicKey,
    name: logicName,
    defaultConditionSuffix,
  };
}

function listDefinitionLogics(definition) {
  if (!definition) {
    return [];
  }
  if (Array.isArray(definition.logics) && definition.logics.length > 0) {
    return definition.logics;
  }
  return [buildLogicVariant(definition.logicKey, definition.logicName, definition.defaultConditionSuffix)];
}

function findLogicDefinition(definition, logicKey) {
  const normalizedLogicKey = normalizeText(logicKey);
  if (!definition || !normalizedLogicKey) {
    return null;
  }
  return listDefinitionLogics(definition).find((logic) => logic.key === normalizedLogicKey) ?? null;
}

function listConditionDefinitions(definition, logicKey = "") {
  const normalizedLogicKey = normalizeText(logicKey);
  return (Array.isArray(definition?.conditions) ? definition.conditions : []).filter((condition) => {
    if (!Array.isArray(condition.logicKeys) || condition.logicKeys.length === 0) {
      return true;
    }
    return condition.logicKeys.includes(normalizedLogicKey);
  });
}

const MACHINE_EVALUATION_DEFINITIONS = [
  {
    machineKey: "aim",
    machineNames: ["SアイムジャグラーＥＸ", "SアイムジャグラーEX"],
    logicKey: "apark-aim",
    logicName: "Sアイム春日式",
    logics: [
      buildLogicVariant("apark-aim", "Sアイム春日式", "main"),
      buildLogicVariant("mj-kurume-aim", "SアイムMJ久留米式", "mj-kurume-main"),
    ],
    profile: "juggler",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "1位＋60点以上＋次点差5点以上＋沈み滞在または角度強",
        "160件 / 103.55% / RB1/306.6",
        {
          rankMax: 1,
          minScore: 60,
          minNextGap: 5,
          anyFlags: ["aimSinkStayStrong", "aimStrongAngle"],
        },
        ["apark-aim"],
      ),
      buildCondition(
        "mj-kurume-main",
        "70点以上",
        "62件 / 104.80% / RB1/280.0",
        {
          minScore: 81,
          requiredFlags: ["kurumeAimHistoryReady"],
        },
        ["mj-kurume-aim"],
      ),
      buildCondition(
        "mj-kurume-high",
        "80点以上",
        "21件 / 104.33% / RB1/259.9",
        {
          minScore: 80,
          requiredFlags: ["kurumeAimHistoryReady"],
        },
        ["mj-kurume-aim"],
      ),
      buildCondition(
        "mj-kurume-boost",
        "1位＋50点以上＋強化2個以上",
        "122件 / 103.83% / RB1/275.7",
        {
          rankMax: 1,
          minScore: 50,
          minBoost: 2,
          requiredFlags: ["kurumeAimHistoryReady"],
        },
        ["mj-kurume-aim"],
      ),
    ],
  },
  {
    machineKey: "gogo",
    machineNames: ["ゴーゴージャグラー３", "ゴーゴージャグラー3", "ゴーゴージャグラー"],
    logicKey: "apark-gogo",
    logicName: "ゴージャグ春日式",
    logics: [
      buildLogicVariant("apark-gogo", "ゴージャグ春日式", "main"),
      buildLogicVariant("mj-kurume-gogo", "ゴージャグMJ久留米式", "mj-kurume-main"),
      buildLogicVariant("beam-hikari-gogo", "ゴージャグビームヒカリ式", "beam-hikari-main"),
      buildLogicVariant("beam-hikari-gogo-normal", "ゴージャグビームヒカリ通常日式", "beam-hikari-normal-main"),
      buildLogicVariant("beam-hikari-gogo-event", "ゴージャグビームヒカリイベント日式", "beam-hikari-event-main"),
    ],
    profile: "juggler",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "1位＋3連敗以上＋前回高内容4〜14日＋3日角度強",
        "132件 / 103.6% / RB1/260.8",
        {
          rankMax: 1,
          requiredFlags: [
            "gogoHistoryReady",
            "gogoLosingStreak3",
            "gogoHighRest4To14",
            "gogoThreeDayAngleStrong",
          ],
        },
        ["apark-gogo"],
      ),
      buildCondition(
        "mj-kurume-main",
        "90点以上＋複合強化＋危険1個以下",
        "61件 / 104.01% / RB1/277.2",
        {
          minScore: 90,
          maxDanger: 0,
          requiredFlags: ["kurumeGogoHistoryReady", "kurumeGogoComposite"],
        },
        ["mj-kurume-gogo"],
      ),
      buildCondition(
        "mj-kurume-boost",
        "90点以上＋強化3個以上＋危険1個以下",
        "73件 / 103.73% / RB1/285.3",
        {
          minScore: 90,
          minBoost: 3,
          maxDanger: 1,
          requiredFlags: ["kurumeGogoHistoryReady"],
        },
        ["mj-kurume-gogo"],
      ),
      buildCondition(
        "mj-kurume-gap5",
        "1位＋次点差5点以上",
        "146件 / 102.50% / RB1/284.4",
        {
          rankMax: 1,
          minNextGap: 5,
          requiredFlags: ["kurumeGogoHistoryReady"],
        },
        ["mj-kurume-gogo"],
      ),
      buildCondition(
        "beam-hikari-main",
        "1位＋75点以上＋次点差8点以上",
        "74件 / 103.08% / RB1/281.9",
        {
          rankMax: 1,
          minScore: 75,
          minNextGap: 8,
          requiredFlags: ["beamHikariGogoMainHistoryReady"],
        },
        ["beam-hikari-gogo"],
      ),
      buildCondition(
        "beam-hikari-score75",
        "75点以上",
        "104件 / 102.96% / RB1/285.7",
        {
          minScore: 75,
          requiredFlags: ["beamHikariGogoMainHistoryReady"],
        },
        ["beam-hikari-gogo"],
      ),
      buildCondition(
        "beam-hikari-rank1-score75-gap12",
        "1位＋75点以上＋次点差12点以上",
        "64件 / 102.38% / RB1/285.8",
        {
          rankMax: 1,
          minScore: 75,
          minNextGap: 12,
          requiredFlags: ["beamHikariGogoMainHistoryReady"],
        },
        ["beam-hikari-gogo"],
      ),
      buildCondition(
        "beam-hikari-score75-rb-weak",
        "75点以上＋3日RB極端悪化",
        "27件 / 102.92% / RB1/275",
        {
          minScore: 75,
          requiredFlags: ["beamHikariGogoMainHistoryReady", "beamHikariGogoRecentThreeRbVeryWeak"],
        },
        ["beam-hikari-gogo"],
      ),
      buildCondition(
        "beam-hikari-score65-sink-nearby",
        "65点以上＋21日沈み帯＋近隣見せ場",
        "101件 / 103.36% / RB1/289.3",
        {
          minScore: 65,
          requiredFlags: [
            "beamHikariGogoMainHistoryReady",
            "beamHikariGogoTwentyOneSinkBand",
            "beamHikariGogoNearbyShow",
          ],
        },
        ["beam-hikari-gogo"],
      ),
      buildCondition(
        "beam-hikari-rank1-gap20",
        "1位＋次点差20点以上",
        "102件 / 102.49% / RB1/284.7",
        {
          rankMax: 1,
          minNextGap: 20,
          requiredFlags: ["beamHikariGogoMainHistoryReady"],
        },
        ["beam-hikari-gogo"],
      ),
      buildCondition(
        "beam-hikari-normal-main",
        "1位＋75点以上＋次点差8点以上",
        "52件 / 104.42% / RB1/287.8",
        {
          rankMax: 1,
          minScore: 75,
          minNextGap: 8,
          requiredFlags: ["beamHikariGogoNormalHistoryReady"],
        },
        ["beam-hikari-gogo-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-score75",
        "75点以上",
        "56件 / 104.27% / RB1/290.1",
        {
          minScore: 75,
          requiredFlags: ["beamHikariGogoNormalHistoryReady"],
        },
        ["beam-hikari-gogo-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-score80",
        "80点以上",
        "40件 / 104.71% / RB1/286.4",
        {
          minScore: 80,
          requiredFlags: ["beamHikariGogoNormalHistoryReady"],
        },
        ["beam-hikari-gogo-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-rank1-score80",
        "1位＋80点以上",
        "39件 / 104.63% / RB1/287.2",
        {
          rankMax: 1,
          minScore: 80,
          requiredFlags: ["beamHikariGogoNormalHistoryReady"],
        },
        ["beam-hikari-gogo-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-sink-games",
        "75点以上＋21日沈み帯＋3日G中間",
        "44件 / 104.74% / RB1/290",
        {
          minScore: 75,
          requiredFlags: [
            "beamHikariGogoNormalHistoryReady",
            "beamHikariGogoTwentyOneSinkBand",
            "beamHikariGogoRecentThreeGamesMiddle",
          ],
        },
        ["beam-hikari-gogo-normal"],
      ),
      buildCondition(
        "beam-hikari-event-main",
        "1位＋70点以上＋次点差8点以上",
        "98件 / 103.31% / RB1/283.1",
        {
          rankMax: 1,
          minScore: 70,
          minNextGap: 8,
          requiredFlags: ["beamHikariGogoEventHistoryReady"],
        },
        ["beam-hikari-gogo-event"],
      ),
      buildCondition(
        "beam-hikari-event-score65-gap8",
        "1位＋65点以上＋次点差8点以上",
        "126件 / 102.90% / RB1/286.4",
        {
          rankMax: 1,
          minScore: 65,
          minNextGap: 8,
          requiredFlags: ["beamHikariGogoEventHistoryReady"],
        },
        ["beam-hikari-gogo-event"],
      ),
      buildCondition(
        "beam-hikari-event-score70",
        "70点以上",
        "169件 / 102.71% / RB1/288.6",
        {
          minScore: 70,
          requiredFlags: ["beamHikariGogoEventHistoryReady"],
        },
        ["beam-hikari-gogo-event"],
      ),
      buildCondition(
        "beam-hikari-event-score75",
        "75点以上",
        "104件 / 102.96% / RB1/285.7",
        {
          minScore: 75,
          requiredFlags: ["beamHikariGogoEventHistoryReady"],
        },
        ["beam-hikari-gogo-event"],
      ),
      buildCondition(
        "beam-hikari-event-rank1-score75-gap8",
        "1位＋75点以上＋次点差8点以上",
        "74件 / 103.08% / RB1/281.9",
        {
          rankMax: 1,
          minScore: 75,
          minNextGap: 8,
          requiredFlags: ["beamHikariGogoEventHistoryReady"],
        },
        ["beam-hikari-gogo-event"],
      ),
      buildCondition(
        "beam-hikari-event-sink-nearby",
        "65点以上＋21日沈み帯＋近隣見せ場",
        "101件 / 103.36% / RB1/289.3",
        {
          minScore: 65,
          requiredFlags: [
            "beamHikariGogoEventHistoryReady",
            "beamHikariGogoTwentyOneSinkBand",
            "beamHikariGogoNearbyShow",
          ],
        },
        ["beam-hikari-gogo-event"],
      ),
    ],
  },
  {
    machineKey: "girls",
    machineNames: ["ジャグラーガールズSS", "ジャグラーガールズ"],
    logicKey: "apark-girls",
    logicName: "ガールズ春日式",
    logics: [
      buildLogicVariant("apark-girls", "ガールズ春日式", "main"),
      buildLogicVariant("mj-kurume-girls", "ガールズMJ久留米式", "mj-kurume-main"),
      buildLogicVariant("beam-hikari-girls", "ガールズビームヒカリ式", "beam-hikari-main"),
      buildLogicVariant("beam-hikari-girls-normal", "ガールズビームヒカリ通常日式", "beam-hikari-normal-main"),
      buildLogicVariant("beam-hikari-girls-event", "ガールズビームヒカリイベント日式", "beam-hikari-event-main"),
    ],
    profile: "juggler",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "1位＋70点以上＋次点差30点以上",
        "50件 / 103.6% / RB1/270.1",
        {
          rankMax: 1,
          minScore: 70,
          minNextGap: 30,
        },
        ["apark-girls"],
      ),
      buildCondition(
        "mj-kurume-main",
        "実戦見送り型（70点以上または1位＋60点以上＋強化3個以上＋危険0）",
        "46件 / 105.36% / RB1/283.4",
        {
          anyOf: [
            {
              minScore: 70,
              maxDanger: 0,
              requiredFlags: ["kurumeGirlsHistoryReady"],
            },
            {
              rankMax: 1,
              minScore: 60,
              minBoost: 3,
              maxDanger: 0,
              requiredFlags: ["kurumeGirlsHistoryReady"],
            },
          ],
        },
        ["mj-kurume-girls"],
      ),
      buildCondition(
        "mj-kurume-score70",
        "70点以上",
        "34件 / 105.30% / RB1/277.4",
        {
          minScore: 70,
          requiredFlags: ["kurumeGirlsHistoryReady"],
        },
        ["mj-kurume-girls"],
      ),
      buildCondition(
        "mj-kurume-score60",
        "60点以上",
        "67件 / 104.35% / RB1/294.5",
        {
          minScore: 60,
          requiredFlags: ["kurumeGirlsHistoryReady"],
        },
        ["mj-kurume-girls"],
      ),
      buildCondition(
        "mj-kurume-gap12",
        "1位＋次点差12点以上",
        "36件 / 103.86% / RB1/291.2",
        {
          rankMax: 1,
          minNextGap: 12,
          requiredFlags: ["kurumeGirlsHistoryReady"],
        },
        ["mj-kurume-girls"],
      ),
      buildCondition(
        "beam-hikari-main",
        "1位＋85点以上＋次点差10点以上",
        "41件 / 104.96% / RB1/270.6",
        {
          rankMax: 1,
          minScore: 85,
          minNextGap: 10,
          requiredFlags: ["beamHikariGirlsMainHistoryReady"],
        },
        ["beam-hikari-girls"],
      ),
      buildCondition(
        "beam-hikari-rank1-score85",
        "1位＋85点以上",
        "49件 / 103.65% / RB1/276.3",
        {
          rankMax: 1,
          minScore: 85,
          requiredFlags: ["beamHikariGirlsMainHistoryReady"],
        },
        ["beam-hikari-girls"],
      ),
      buildCondition(
        "beam-hikari-gap25",
        "1位＋次点差25点以上",
        "86件 / 103.01% / RB1/291.3",
        {
          rankMax: 1,
          minNextGap: 25,
          requiredFlags: ["beamHikariGirlsMainHistoryReady"],
        },
        ["beam-hikari-girls"],
      ),
      buildCondition(
        "beam-hikari-gap30",
        "1位＋次点差30点以上",
        "55件 / 102.51% / RB1/294.3",
        {
          rankMax: 1,
          minNextGap: 30,
          requiredFlags: ["beamHikariGirlsMainHistoryReady"],
        },
        ["beam-hikari-girls"],
      ),
      buildCondition(
        "beam-hikari-normal-main",
        "1位＋85点以上＋次点差10点以上",
        "24件 / 105.64% / RB1/247.1",
        {
          rankMax: 1,
          minScore: 85,
          minNextGap: 10,
          requiredFlags: ["beamHikariGirlsNormalHistoryReady"],
        },
        ["beam-hikari-girls-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-rank1-score85",
        "1位＋85点以上",
        "28件 / 104.71% / RB1/249.1",
        {
          rankMax: 1,
          minScore: 85,
          requiredFlags: ["beamHikariGirlsNormalHistoryReady"],
        },
        ["beam-hikari-girls-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-rank1-score90",
        "1位＋90点以上",
        "9件 / 105.95% / RB1/235.7",
        {
          rankMax: 1,
          minScore: 90,
          requiredFlags: ["beamHikariGirlsNormalHistoryReady"],
        },
        ["beam-hikari-girls-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-rank1-score85-boost4",
        "1位＋85点以上＋強化4個以上",
        "28件 / 104.71% / RB1/249.1",
        {
          rankMax: 1,
          minScore: 85,
          minBoost: 4,
          requiredFlags: ["beamHikariGirlsNormalHistoryReady"],
        },
        ["beam-hikari-girls-normal"],
      ),
      buildCondition(
        "beam-hikari-event-main",
        "1位＋75点以上＋次点差10点以上または強化4個以上",
        "155件 / 101.71% / RB1/307.1",
        {
          anyOf: [
            {
              rankMax: 1,
              minScore: 75,
              minNextGap: 10,
              requiredFlags: ["beamHikariGirlsEventHistoryReady"],
            },
            {
              rankMax: 1,
              minScore: 75,
              minBoost: 4,
              requiredFlags: ["beamHikariGirlsEventHistoryReady"],
            },
          ],
        },
        ["beam-hikari-girls-event"],
      ),
      buildCondition(
        "beam-hikari-event-rank1-score75",
        "1位＋75点以上",
        "156件 / 101.67% / RB1/307.2",
        {
          rankMax: 1,
          minScore: 75,
          requiredFlags: ["beamHikariGirlsEventHistoryReady"],
        },
        ["beam-hikari-girls-event"],
      ),
      buildCondition(
        "beam-hikari-event-rank1-score75-gap10",
        "1位＋75点以上＋次点差10点以上",
        "120件 / 102.25% / RB1/301.1",
        {
          rankMax: 1,
          minScore: 75,
          minNextGap: 10,
          requiredFlags: ["beamHikariGirlsEventHistoryReady"],
        },
        ["beam-hikari-girls-event"],
      ),
      buildCondition(
        "beam-hikari-event-rank1-score75-boost4",
        "1位＋75点以上＋強化4個以上",
        "155件 / 101.71% / RB1/307.1",
        {
          rankMax: 1,
          minScore: 75,
          minBoost: 4,
          requiredFlags: ["beamHikariGirlsEventHistoryReady"],
        },
        ["beam-hikari-girls-event"],
      ),
      buildCondition(
        "beam-hikari-event-rank1-score80",
        "1位＋80点以上",
        "124件 / 101.92% / RB1/301.6",
        {
          rankMax: 1,
          minScore: 80,
          requiredFlags: ["beamHikariGirlsEventHistoryReady"],
        },
        ["beam-hikari-girls-event"],
      ),
      buildCondition(
        "beam-hikari-event-rank1-score85",
        "1位＋85点以上",
        "90件 / 102.51% / RB1/298.2",
        {
          rankMax: 1,
          minScore: 85,
          requiredFlags: ["beamHikariGirlsEventHistoryReady"],
        },
        ["beam-hikari-girls-event"],
      ),
      buildCondition(
        "beam-hikari-event-rank1-score80-gap10",
        "1位＋80点以上＋次点差10点以上",
        "97件 / 102.44% / RB1/294.7",
        {
          rankMax: 1,
          minScore: 80,
          minNextGap: 10,
          requiredFlags: ["beamHikariGirlsEventHistoryReady"],
        },
        ["beam-hikari-girls-event"],
      ),
      buildCondition(
        "beam-hikari-event-safe",
        "1位＋75点以上＋危険0",
        "152件 / 101.75% / RB1/306.5",
        {
          rankMax: 1,
          minScore: 75,
          maxDanger: 0,
          requiredFlags: ["beamHikariGirlsEventHistoryReady"],
        },
        ["beam-hikari-girls-event"],
      ),
    ],
  },
  {
    machineKey: "star-hana",
    machineNames: ["スターハナハナ", "スターハナハナ-30", "スターハナハナ‐30"],
    logicKey: "apark-star-hana",
    logicName: "スターハナ春日式",
    profile: "hana",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "1位＋80点以上＋強化2個以上＋危険0＋前日500〜2500G",
        "48件 / 103.6% / RB1/278.1",
        {
          rankMax: 1,
          minScore: 80,
          minBoost: 2,
          maxDanger: 0,
          requiredFlags: ["starPreviousCut"],
        },
      ),
    ],
  },
  {
    machineKey: "thunder",
    machineNames: ["スマスロ サンダーV", "スマスロサンダーV", "LサンダーV"],
    logicKey: "apark-thunder",
    logicName: "サンダーV春日式",
    profile: "hanabi",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "50点以上＋強角度＋処遇完了回避",
        "18件 / 107.10% / RB1/377.3",
        {
          minScore: 50,
          maxDanger: 1,
          requiredFlags: ["strongAngle", "thunderHistoryReady"],
        },
      ),
    ],
  },
  {
    machineKey: "smart-hanabi",
    machineNames: ["スマスロ ハナビ", "スマスロハナビ"],
    logicKey: "apark-smart-hanabi",
    logicName: "スマスロハナビ春日式",
    profile: "hanabi",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "1位＋次点差10点以上＋危険0",
        "32件 / 105.00% / RB1/315.2",
        {
          rankMax: 1,
          minNextGap: 10,
          maxDanger: 0,
        },
      ),
    ],
  },
  {
    machineKey: "okidoki-duo",
    machineNames: [
      "スマスロ 沖ドキ!DUO アンコール",
      "スマスロ沖ドキ!DUOアンコール",
      "L沖ドキ!DUO アンコール",
    ],
    logicKey: "apark-okidoki-duo",
    logicName: "沖ドキDUO春日式",
    profile: "okidoki",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "上位2位",
        "284件 / 105.67%",
        {
          rankMax: 2,
          requiredFlags: ["okidokiDuoHistoryReady"],
        },
      ),
      buildCondition(
        "top1",
        "1位",
        "142件 / 106.42%",
        {
          rankMax: 1,
          requiredFlags: ["okidokiDuoHistoryReady"],
        },
      ),
      buildCondition(
        "score65",
        "65点以上",
        "114件 / 104.53%",
        {
          minScore: 64,
          requiredFlags: ["okidokiDuoHistoryReady"],
        },
      ),
      buildCondition(
        "score70",
        "70点以上",
        "75件 / 107.79%",
        {
          minScore: 70,
          requiredFlags: ["okidokiDuoHistoryReady"],
        },
      ),
    ],
  },
  {
    machineKey: "monkey",
    machineNames: ["スマスロモンキーターンV", "スマスロ モンキーターンV", "スマスロモンキーターンⅤ"],
    logicKey: "apark-monkey",
    logicName: "モンキー春日式v2",
    logics: [
      buildLogicVariant("apark-monkey", "モンキー春日式v2", "main"),
      buildLogicVariant("beam-hikari-monkey", "モンキービームヒカリ全日式", "beam-hikari-main"),
      buildLogicVariant("beam-hikari-monkey-normal", "モンキービームヒカリ通常日式", "beam-hikari-normal-main"),
      buildLogicVariant("beam-hikari-monkey-event", "モンキービームヒカリイベント日式", "beam-hikari-event-score70"),
    ],
    profile: "smart",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "1位＋次点差10点以上",
        "182件 / 104.34%",
        {
          rankMax: 1,
          minNextGap: 10,
          requiredFlags: ["monkeyHistoryReady"],
        },
        ["apark-monkey"],
      ),
      buildCondition(
        "safe",
        "1位＋危険0",
        "137件 / 105.87%",
        {
          rankMax: 1,
          maxDanger: 0,
          requiredFlags: ["monkeyHistoryReady"],
        },
        ["apark-monkey"],
      ),
      buildCondition(
        "strong",
        "1位＋65点以上＋強化2個以上＋危険0",
        "98件 / 105.75%",
        {
          rankMax: 1,
          minScore: 65,
          minBoost: 2,
          maxDanger: 0,
          requiredFlags: ["monkeyHistoryReady"],
        },
        ["apark-monkey"],
      ),
      buildCondition(
        "beam-hikari-main",
        "1位＋80点以上＋14日未返済",
        "36件 / 109.36% / RB1/458",
        {
          rankMax: 1,
          minScore: 80,
          requiredFlags: ["beamHikariMonkeyMainHistoryReady", "beamHikariMonkeyMainUnpaid14"],
        },
        ["beam-hikari-monkey"],
      ),
      buildCondition(
        "beam-hikari-rank1-score80",
        "1位＋80点以上",
        "43件 / 109.39% / RB1/444.3",
        {
          rankMax: 1,
          minScore: 80,
          requiredFlags: ["beamHikariMonkeyMainHistoryReady"],
        },
        ["beam-hikari-monkey"],
      ),
      buildCondition(
        "beam-hikari-score80",
        "80点以上",
        "62件 / 106.23% / RB1/430.1",
        {
          minScore: 80,
          requiredFlags: ["beamHikariMonkeyMainHistoryReady"],
        },
        ["beam-hikari-monkey"],
      ),
      buildCondition(
        "beam-hikari-strong",
        "1位＋80点以上＋14日未返済＋近隣冷え",
        "32件 / 111.55% / RB1/461.4",
        {
          rankMax: 1,
          minScore: 80,
          requiredFlags: [
            "beamHikariMonkeyMainHistoryReady",
            "beamHikariMonkeyMainUnpaid14",
            "beamHikariMonkeyMainNearbyCold",
          ],
        },
        ["beam-hikari-monkey"],
      ),
      buildCondition(
        "beam-hikari-score85",
        "85点以上",
        "23件 / 107.34% / RB1/425.2",
        {
          minScore: 85,
          requiredFlags: ["beamHikariMonkeyMainHistoryReady"],
        },
        ["beam-hikari-monkey"],
      ),
      buildCondition(
        "beam-hikari-rank1-score85",
        "1位＋85点以上",
        "20件 / 105.79% / RB1/424",
        {
          rankMax: 1,
          minScore: 85,
          requiredFlags: ["beamHikariMonkeyMainHistoryReady"],
        },
        ["beam-hikari-monkey"],
      ),
      buildCondition(
        "beam-hikari-normal-main",
        "1位＋60点以上＋次点差4点以上",
        "178件 / 104.71% / RB1/450.3",
        {
          rankMax: 1,
          minScore: 60,
          minNextGap: 4,
          requiredFlags: ["beamHikariMonkeyNormalHistoryReady"],
        },
        ["beam-hikari-monkey-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-gap6",
        "1位＋60点以上＋次点差6点以上",
        "145件 / 105.40% / RB1/457.2",
        {
          rankMax: 1,
          minScore: 60,
          minNextGap: 6,
          requiredFlags: ["beamHikariMonkeyNormalHistoryReady"],
        },
        ["beam-hikari-monkey-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-rank1-score60",
        "1位＋60点以上",
        "231件 / 104.09% / RB1/453.4",
        {
          rankMax: 1,
          minScore: 60,
          requiredFlags: ["beamHikariMonkeyNormalHistoryReady"],
        },
        ["beam-hikari-monkey-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-rank1-score70",
        "1位＋70点以上",
        "99件 / 107.26% / RB1/449.1",
        {
          rankMax: 1,
          minScore: 70,
          requiredFlags: ["beamHikariMonkeyNormalHistoryReady"],
        },
        ["beam-hikari-monkey-normal"],
      ),
      buildCondition(
        "beam-hikari-event-main",
        "1位＋70点以上",
        "114件 / 101.94% / RB1/431",
        {
          rankMax: 1,
          minScore: 70,
          requiredFlags: ["beamHikariMonkeyEventHistoryReady"],
        },
        ["beam-hikari-monkey-event"],
      ),
      buildCondition(
        "beam-hikari-event-gap4",
        "1位＋70点以上＋次点差4点以上",
        "97件 / 101.04% / RB1/419.6",
        {
          rankMax: 1,
          minScore: 70,
          minNextGap: 4,
          requiredFlags: ["beamHikariMonkeyEventHistoryReady"],
        },
        ["beam-hikari-monkey-event"],
      ),
      buildCondition(
        "beam-hikari-event-score70",
        "70点以上",
        "169件 / 102.47% / RB1/429.2",
        {
          minScore: 70,
          requiredFlags: ["beamHikariMonkeyEventHistoryReady"],
        },
        ["beam-hikari-monkey-event"],
      ),
      buildCondition(
        "beam-hikari-event-score75",
        "1位＋75点以上",
        "60件 / 101.55% / RB1/425.9",
        {
          rankMax: 1,
          minScore: 75,
          requiredFlags: ["beamHikariMonkeyEventHistoryReady"],
        },
        ["beam-hikari-monkey-event"],
      ),
    ],
  },
  {
    machineKey: "hokuto-tensei",
    machineNames: [
      "スマスロ北斗の拳 転生の章",
      "スマスロ北斗の拳 転生の章2",
      "スマスロ北斗の拳転生の章",
      "スマスロ北斗の拳転生の章2",
    ],
    logicKey: "apark-hokuto-tensei",
    logicName: "北斗転生春日式",
    logics: [
      buildLogicVariant("apark-hokuto-tensei", "北斗転生春日式", "main"),
      buildLogicVariant("beam-hikari-hokuto-tensei", "北斗転生ビームヒカリ全日式", "beam-hikari-main"),
      buildLogicVariant("beam-hikari-hokuto-tensei-normal", "北斗転生ビームヒカリ通常日式", "beam-hikari-normal-main"),
      buildLogicVariant("beam-hikari-hokuto-tensei-event", "北斗転生ビームヒカリイベント日式", "beam-hikari-event-main"),
    ],
    profile: "smart",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "50点以上",
        "320件 / 105.0%",
        {
          minScore: 50,
          requiredFlags: ["hokutoHistoryReady"],
        },
        ["apark-hokuto-tensei"],
      ),
      buildCondition(
        "strong",
        "1位＋55点以上＋次点差5点以上＋危険1以下",
        "84件 / 107.1%",
        {
          rankMax: 1,
          minScore: 55,
          minNextGap: 5,
          maxDanger: 1,
          requiredFlags: ["hokutoHistoryReady"],
        },
        ["apark-hokuto-tensei"],
      ),
      buildCondition(
        "top4",
        "上位4台",
        "500件 / 103.9%",
        {
          rankMax: 4,
          requiredFlags: ["hokutoHistoryReady"],
        },
        ["apark-hokuto-tensei"],
      ),
      buildCondition(
        "top2",
        "上位2台",
        "250件 / 106.4%",
        {
          rankMax: 2,
          requiredFlags: ["hokutoHistoryReady"],
        },
        ["apark-hokuto-tensei"],
      ),
      buildCondition(
        "beam-hikari-main",
        "1位＋75点以上",
        "128件 / 104.49% / RB1/490.1",
        {
          rankMax: 1,
          minScore: 75,
          requiredFlags: ["beamHikariHokutoMainHistoryReady"],
        },
        ["beam-hikari-hokuto-tensei"],
      ),
      buildCondition(
        "beam-hikari-strong",
        "1位＋75点以上＋強化2個以上＋危険1以下",
        "121件 / 104.40% / RB1/491.8",
        {
          rankMax: 1,
          minScore: 75,
          minBoost: 2,
          maxDanger: 1,
          requiredFlags: ["beamHikariHokutoMainHistoryReady"],
        },
        ["beam-hikari-hokuto-tensei"],
      ),
      buildCondition(
        "beam-hikari-rank1-safe",
        "1位＋危険1以下",
        "130件 / 104.93% / RB1/487.4",
        {
          rankMax: 1,
          maxDanger: 1,
          requiredFlags: ["beamHikariHokutoMainHistoryReady"],
        },
        ["beam-hikari-hokuto-tensei"],
      ),
      buildCondition(
        "beam-hikari-gap4",
        "1位＋次点差4点以上",
        "90件 / 105.73% / RB1/516.4",
        {
          rankMax: 1,
          minNextGap: 4,
          requiredFlags: ["beamHikariHokutoMainHistoryReady"],
        },
        ["beam-hikari-hokuto-tensei"],
      ),
      buildCondition(
        "beam-hikari-top2",
        "上位2位",
        "262件 / 103.58% / RB1/482",
        {
          rankMax: 2,
          requiredFlags: ["beamHikariHokutoMainHistoryReady"],
        },
        ["beam-hikari-hokuto-tensei"],
      ),
      buildCondition(
        "beam-hikari-score75",
        "75点以上",
        "430件 / 103.12% / RB1/473.3",
        {
          minScore: 75,
          requiredFlags: ["beamHikariHokutoMainHistoryReady"],
        },
        ["beam-hikari-hokuto-tensei"],
      ),
      buildCondition(
        "beam-hikari-normal-main",
        "1位",
        "0件 / - / RB-",
        {
          rankMax: 1,
          requiredFlags: ["beamHikariHokutoNormalHistoryReady"],
        },
        ["beam-hikari-hokuto-tensei-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-top2",
        "上位2位",
        "0件 / - / RB-",
        {
          rankMax: 2,
          requiredFlags: ["beamHikariHokutoNormalHistoryReady"],
        },
        ["beam-hikari-hokuto-tensei-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-strong",
        "1位＋強化2個以上＋危険1以下",
        "0件 / - / RB-",
        {
          rankMax: 1,
          minBoost: 2,
          maxDanger: 1,
          requiredFlags: ["beamHikariHokutoNormalHistoryReady"],
        },
        ["beam-hikari-hokuto-tensei-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-score65",
        "65点以上",
        "0件 / - / RB-",
        {
          minScore: 65,
          requiredFlags: ["beamHikariHokutoNormalHistoryReady"],
        },
        ["beam-hikari-hokuto-tensei-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-rank1-score75",
        "1位＋75点以上",
        "0件 / - / RB-",
        {
          rankMax: 1,
          minScore: 75,
          requiredFlags: ["beamHikariHokutoNormalHistoryReady"],
        },
        ["beam-hikari-hokuto-tensei-normal"],
      ),
      buildCondition(
        "beam-hikari-event-main",
        "85点以上",
        "0件 / - / RB-",
        {
          minScore: 85,
          requiredFlags: ["beamHikariHokutoEventHistoryReady"],
        },
        ["beam-hikari-hokuto-tensei-event"],
      ),
      buildCondition(
        "beam-hikari-event-top4-score80",
        "上位4位＋80点以上",
        "0件 / - / RB-",
        {
          rankMax: 4,
          minScore: 80,
          requiredFlags: ["beamHikariHokutoEventHistoryReady"],
        },
        ["beam-hikari-hokuto-tensei-event"],
      ),
      buildCondition(
        "beam-hikari-event-top4",
        "上位4位",
        "0件 / - / RB-",
        {
          rankMax: 4,
          requiredFlags: ["beamHikariHokutoEventHistoryReady"],
        },
        ["beam-hikari-hokuto-tensei-event"],
      ),
      buildCondition(
        "beam-hikari-event-strong",
        "1位＋強化2個以上＋危険1以下",
        "0件 / - / RB-",
        {
          rankMax: 1,
          minBoost: 2,
          maxDanger: 1,
          requiredFlags: ["beamHikariHokutoEventHistoryReady"],
        },
        ["beam-hikari-hokuto-tensei-event"],
      ),
      buildCondition(
        "beam-hikari-event-score80",
        "80点以上",
        "0件 / - / RB-",
        {
          minScore: 80,
          requiredFlags: ["beamHikariHokutoEventHistoryReady"],
        },
        ["beam-hikari-hokuto-tensei-event"],
      ),
    ],
  },
  {
    machineKey: "hokuto-base",
    machineNames: ["Lスマスロ北斗の拳", "L スマスロ北斗の拳", "スマスロ北斗の拳"],
    logicKey: "beam-hikari-hokuto-base",
    logicName: "L北斗ビームヒカリ全日式",
    logics: [
      buildLogicVariant("beam-hikari-hokuto-base", "L北斗ビームヒカリ全日式", "beam-hikari-main"),
      buildLogicVariant("beam-hikari-hokuto-base-normal", "L北斗ビームヒカリ通常日式", "beam-hikari-normal-top2-score80"),
      buildLogicVariant("beam-hikari-hokuto-base-event", "L北斗ビームヒカリイベント日式", "beam-hikari-event-main"),
    ],
    profile: "smart",
    defaultConditionSuffix: "beam-hikari-main",
    conditions: [
      buildCondition(
        "beam-hikari-main",
        "1位＋70点以上＋次点差5点以上",
        "196件 / 104.49% / RB1/482.3",
        {
          rankMax: 1,
          minScore: 70,
          minNextGap: 5,
          requiredFlags: ["beamHikariHokutoBaseMainHistoryReady"],
        },
        ["beam-hikari-hokuto-base"],
      ),
      buildCondition(
        "beam-hikari-gap5",
        "1位＋次点差5点以上",
        "200件 / 104.45% / RB1/483.4",
        {
          rankMax: 1,
          minNextGap: 5,
          requiredFlags: ["beamHikariHokutoBaseMainHistoryReady"],
        },
        ["beam-hikari-hokuto-base"],
      ),
      buildCondition(
        "beam-hikari-gap5-safe",
        "1位＋次点差5点以上＋危険0",
        "153件 / 105.09% / RB1/484.9",
        {
          rankMax: 1,
          minNextGap: 5,
          maxDanger: 0,
          requiredFlags: ["beamHikariHokutoBaseMainHistoryReady"],
        },
        ["beam-hikari-hokuto-base"],
      ),
      buildCondition(
        "beam-hikari-gap4",
        "1位＋次点差4点以上",
        "232件 / 103.96% / RB1/487.9",
        {
          rankMax: 1,
          minNextGap: 4,
          requiredFlags: ["beamHikariHokutoBaseMainHistoryReady"],
        },
        ["beam-hikari-hokuto-base"],
      ),
      buildCondition(
        "beam-hikari-gap10",
        "1位＋次点差10点以上",
        "102件 / 103.08% / RB1/499.5",
        {
          rankMax: 1,
          minNextGap: 10,
          requiredFlags: ["beamHikariHokutoBaseMainHistoryReady"],
        },
        ["beam-hikari-hokuto-base"],
      ),
      buildCondition(
        "beam-hikari-normal-top2-score80",
        "上位2台＋80点以上",
        "98件 / 102.41% / RB1/491.1",
        {
          rankMax: 2,
          minScore: 80,
          requiredFlags: ["beamHikariHokutoBaseNormalHistoryReady"],
        },
        ["beam-hikari-hokuto-base-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-score80",
        "80点以上",
        "100件 / 102.37% / RB1/490.9",
        {
          minScore: 80,
          requiredFlags: ["beamHikariHokutoBaseNormalHistoryReady"],
        },
        ["beam-hikari-hokuto-base-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-rank1-score80",
        "1位＋80点以上",
        "84件 / 101.91% / RB1/487",
        {
          rankMax: 1,
          minScore: 80,
          requiredFlags: ["beamHikariHokutoBaseNormalHistoryReady"],
        },
        ["beam-hikari-hokuto-base-normal"],
      ),
      buildCondition(
        "beam-hikari-event-main",
        "1位＋75点以上",
        "306件 / 102.83% / RB1/490.6",
        {
          rankMax: 1,
          minScore: 75,
          requiredFlags: ["beamHikariHokutoBaseEventHistoryReady"],
        },
        ["beam-hikari-hokuto-base-event"],
      ),
      buildCondition(
        "beam-hikari-event-rank1-score75-safe",
        "1位＋75点以上＋危険0",
        "240件 / 103.64% / RB1/484.8",
        {
          rankMax: 1,
          minScore: 75,
          maxDanger: 0,
          requiredFlags: ["beamHikariHokutoBaseEventHistoryReady"],
        },
        ["beam-hikari-hokuto-base-event"],
      ),
      buildCondition(
        "beam-hikari-event-rank1-score75-gap3",
        "1位＋75点以上＋次点差3点以上",
        "226件 / 102.71% / RB1/483.7",
        {
          rankMax: 1,
          minScore: 75,
          minNextGap: 3,
          requiredFlags: ["beamHikariHokutoBaseEventHistoryReady"],
        },
        ["beam-hikari-hokuto-base-event"],
      ),
      buildCondition(
        "beam-hikari-event-top2-score80",
        "上位2台＋80点以上",
        "472件 / 102.20% / RB1/483.9",
        {
          rankMax: 2,
          minScore: 80,
          requiredFlags: ["beamHikariHokutoBaseEventHistoryReady"],
        },
        ["beam-hikari-hokuto-base-event"],
      ),
      buildCondition(
        "beam-hikari-event-rank1",
        "1位",
        "334件 / 102.14% / RB1/488.5",
        {
          rankMax: 1,
          requiredFlags: ["beamHikariHokutoBaseEventHistoryReady"],
        },
        ["beam-hikari-hokuto-base-event"],
      ),
      buildCondition(
        "beam-hikari-event-score80",
        "80点以上",
        "705件 / 102.75% / RB1/482.8",
        {
          minScore: 80,
          requiredFlags: ["beamHikariHokutoBaseEventHistoryReady"],
        },
        ["beam-hikari-hokuto-base-event"],
      ),
      buildCondition(
        "beam-hikari-event-top2-score75",
        "上位2台＋75点以上",
        "550件 / 103.86% / RB1/488.6",
        {
          rankMax: 2,
          minScore: 75,
          requiredFlags: ["beamHikariHokutoBaseEventHistoryReady"],
        },
        ["beam-hikari-hokuto-base-event"],
      ),
    ],
  },
  {
    machineKey: "dragon-hana",
    machineNames: [
      "ドラゴンハナハナ～閃光～",
      "ドラゴンハナハナ",
      "ドラゴンハナハナ閃光",
      "ドラゴンハナハナ～閃光～30",
    ],
    logicKey: "apark-dragon-hana",
    logicName: "ドラハナ春日式",
    profile: "hana",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "1位＋65点以上＋弱ボーナス＋次点差12点以上",
        "26件 / 105.65% / RB1/446.6",
        {
          rankMax: 1,
          minScore: 65,
          minNextGap: 12,
          requiredFlags: ["weakBonus"],
        },
      ),
    ],
  },
  {
    machineKey: "new-king-hana",
    machineNames: ["ニューキングハナハナ", "ニューキングハナハナV", "ニューキングハナハナV-30"],
    logicKey: "apark-new-king-hana",
    logicName: "ニューキング春日式",
    profile: "hana",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "1位＋50点以上＋次点差5点以上＋危険なし＋強化3個以上",
        "59件 / 104.57% / RB1/404.8",
        {
          rankMax: 1,
          minScore: 50,
          minNextGap: 5,
          maxDanger: 0,
          minBoost: 3,
          requiredFlags: ["newKingHistoryReady"],
        },
      ),
    ],
  },
  {
    machineKey: "neo-aim",
    machineNames: ["ネオアイムジャグラーEX", "ネオアイムジャグラーＥＸ"],
    logicKey: "apark-neo-aim",
    logicName: "ネオアイム春日式",
    logics: [
      buildLogicVariant("apark-neo-aim", "ネオアイム春日式", "main"),
      buildLogicVariant("apark-yakatabaru-neo-aim", "ネオアイム屋形原式", "apark-yakatabaru-main"),
      buildLogicVariant("mj-kurume-neo-aim", "ネオアイムMJ久留米式", "mj-kurume-main"),
      buildLogicVariant("beam-hikari-neo-aim", "ネオアイムビームヒカリ式", "beam-hikari-main"),
      buildLogicVariant("beam-hikari-neo-aim-normal", "ネオアイムビームヒカリ通常日式", "beam-hikari-normal-main"),
      buildLogicVariant("beam-hikari-neo-aim-event", "ネオアイムビームヒカリイベント日式", "beam-hikari-event-main"),
      buildLogicVariant("amuse-asakusa-neo-aim", "ネオアイムアミューズ浅草式", "amuse-asakusa-main"),
      buildLogicVariant("gogo-tenjin-neo-aim", "ネオアイムGOGO天神式", "gogo-tenjin-main"),
      buildLogicVariant(
        "hinode-onojo-neo-aim",
        "HINODE大野城ネオアイム沈み返済56狙い式v1",
        "hinode-onojo-short-core",
      ),
      buildLogicVariant(
        "chikushino-neo-aim",
        "筑紫野ネオアイム_56推定ローテスコア",
        "chikushino-rank1-gap3",
      ),
      buildLogicVariant(
        "espace-ueno-neo-aim",
        "エスパス上野本館ネオアイム低中稼働RB残り式",
        "espace-ueno-wide310",
      ),
      buildLogicVariant(
        "messe-minamisenju-neo-aim",
        "メッセ南千住_ネオアイムEX_全日共通_未返済沈み滞在ロジック_v1",
        "messe-minamisenju-free-14rb",
      ),
      buildLogicVariant(
        "kintoki-kamata-neo-aim",
        "金時蒲田東口店_ネオアイムEX_全日共通100点ロジック",
        "kintoki-kamata-free-a",
      ),
    ],
    profile: "juggler",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "1位＋70点以上＋3日沈み2日以上",
        "128件 / 105.7% / RB1/271.4",
        {
          rankMax: 1,
          minScore: 70,
          requiredFlags: ["aimShortSinkStay2"],
        },
        ["apark-neo-aim"],
      ),
      buildCondition(
        "apark-kasuga-trigger-middle-miss",
        "中間不発最強型",
        "20件 / 106.06% / RB1/250.6",
        {
          requiredFlags: ["neoAimKasugaMiddleMissTrigger"],
        },
        ["apark-neo-aim"],
      ),
      buildCondition(
        "apark-kasuga-trigger-deep-losing",
        "深連敗＋前日不発",
        "80件 / 106.14% / RB1/269.9",
        {
          requiredFlags: ["neoAimKasugaDeepLosingTrigger"],
        },
        ["apark-neo-aim"],
      ),
      buildCondition(
        "apark-kasuga-trigger-seven-sink",
        "7日超凹み返済",
        "52件 / 106.63% / RB1/264.5",
        {
          requiredFlags: ["neoAimKasugaSevenSinkTrigger"],
        },
        ["apark-neo-aim"],
      ),
      buildCondition(
        "apark-yakatabaru-main",
        "自由実戦主軸",
        "116件 / 103.40% / RB1/265.4 / p56 50.0%",
        {
          requiredFlags: ["yakatabaruNeoHistoryReady", "yakatabaruNeoFreeMain"],
        },
        ["apark-yakatabaru-neo-aim"],
      ),
      buildCondition(
        "apark-yakatabaru-best106",
        "最本命106",
        "30件 / 106.65% / RB1/250.5 / p56 58.1%",
        {
          requiredFlags: ["yakatabaruNeoHistoryReady", "yakatabaruNeoBest106"],
        },
        ["apark-yakatabaru-neo-aim"],
      ),
      buildCondition(
        "apark-yakatabaru-strong105",
        "強105",
        "30件 / 105.99% / RB1/249.0 / p56 57.9%",
        {
          requiredFlags: ["yakatabaruNeoHistoryReady", "yakatabaruNeoStrong105"],
        },
        ["apark-yakatabaru-neo-aim"],
      ),
      buildCondition(
        "apark-yakatabaru-free-strong",
        "自由強",
        "122件 / 103.75% / RB1/265.7 / p56 50.7%",
        {
          requiredFlags: ["yakatabaruNeoHistoryReady", "yakatabaruNeoFreeStrong"],
        },
        ["apark-yakatabaru-neo-aim"],
      ),
      buildCondition(
        "apark-yakatabaru-main104",
        "本命104",
        "40件 / 104.36% / RB1/292.9 / p56 38.9%",
        {
          maxDanger: 0,
          requiredFlags: ["yakatabaruNeoHistoryReady", "yakatabaruNeoPreviousFail"],
        },
        ["apark-yakatabaru-neo-aim"],
      ),
      buildCondition(
        "apark-yakatabaru-rank-gap15",
        "弱め本命103",
        "23件 / 103.24% / RB1/277.8 / p56 45.6%",
        {
          rankMax: 1,
          minNextGap: 15,
          requiredFlags: ["yakatabaruNeoHistoryReady"],
        },
        ["apark-yakatabaru-neo-aim"],
      ),
      buildCondition(
        "apark-yakatabaru-free-balanced",
        "自由強・打てる日多め",
        "226件 / 103.38% / RB1/278.7 / p56 44.7%",
        {
          requiredFlags: ["yakatabaruNeoHistoryReady", "yakatabaruNeoFreeBalanced"],
        },
        ["apark-yakatabaru-neo-aim"],
      ),
      buildCondition(
        "apark-yakatabaru-top",
        "広め102",
        "294件 / 102.66% / RB1/290.0 / p56 40.3%",
        {
          rankMax: 1,
          requiredFlags: ["yakatabaruNeoHistoryReady"],
        },
        ["apark-yakatabaru-neo-aim"],
      ),
      buildCondition(
        "apark-yakatabaru-score90",
        "弱め102",
        "440件 / 102.05% / RB1/299.0 / p56 36.7%",
        {
          minScore: 90,
          requiredFlags: ["yakatabaruNeoHistoryReady"],
        },
        ["apark-yakatabaru-neo-aim"],
      ),
      buildCondition(
        "mj-kurume-main",
        "広め90点",
        "307件 / 102.37% / RB1/296.9 / p56 34.9%",
        {
          minScore: 90,
          requiredFlags: ["kurumeNeoHistoryReady"],
        },
        ["mj-kurume-neo-aim"],
      ),
      buildCondition(
        "mj-kurume-rank1-boost2",
        "弱本命1位強化2",
        "220件 / 102.66% / RB1/286.1 / p56 37.2%",
        {
          rankMax: 1,
          minBoost: 2,
          maxDanger: 0,
          requiredFlags: ["kurumeNeoHistoryReady"],
        },
        ["mj-kurume-neo-aim"],
      ),
      buildCondition(
        "mj-kurume-rank1-gap8-boost2",
        "本命1位差8強化2",
        "93件 / 103.19% / RB1/278.0 / p56 39.9%",
        {
          rankMax: 1,
          minNextGap: 8,
          minBoost: 2,
          maxDanger: 0,
          requiredFlags: ["kurumeNeoHistoryReady"],
        },
        ["mj-kurume-neo-aim"],
      ),
      buildCondition(
        "mj-kurume-nearby-gap8",
        "強条件1位周辺差8",
        "53件 / 104.13% / RB1/270.9 / p56 42.2%",
        {
          rankMax: 1,
          minNextGap: 8,
          maxDanger: 0,
          requiredFlags: ["kurumeNeoHistoryReady", "kurumeNeoNearbyLeftBehind"],
        },
        ["mj-kurume-neo-aim"],
      ),
      buildCondition(
        "mj-kurume-genuine-3g",
        "最本命1位本物3G",
        "29件 / 105.88% / RB1/264.8 / p56 43.7%",
        {
          rankMax: 1,
          maxDanger: 0,
          requiredFlags: ["kurumeNeoHistoryReady", "kurumeNeoGenuine", "kurumeNeoThreeDayHighGames"],
        },
        ["mj-kurume-neo-aim"],
      ),
      buildCondition(
        "mj-kurume-max-sink-losing",
        "MAX_14沈み+連敗5+1位",
        "79件 / 103.25% / RB1/259.2 / p56 45.0%",
        {
          rankMax: 1,
          requiredFlags: ["kurumeNeoHistoryReady", "kurumeNeoStrongSink", "kurumeNeoLosingReturn"],
        },
        ["mj-kurume-neo-aim"],
      ),
      buildCondition(
        "mj-kurume-max-cluster75",
        "MAX_クラスタ未払い+75+危険0",
        "97件 / 104.93% / RB1/274.0 / p56 41.2%",
        {
          minScore: 75,
          maxDanger: 0,
          requiredFlags: ["kurumeNeoHistoryReady", "kurumeNeoClusterUnpaid"],
        },
        ["mj-kurume-neo-aim"],
      ),
      buildCondition(
        "mj-kurume-max-sink-gap15",
        "MAX_14沈み+次点差15",
        "52件 / 104.53% / RB1/266.8 / p56 43.2%",
        {
          rankMax: 1,
          minNextGap: 15,
          requiredFlags: ["kurumeNeoHistoryReady", "kurumeNeoStrongSink"],
        },
        ["mj-kurume-neo-aim"],
      ),
      buildCondition(
        "gogo-tenjin-main",
        "S1/S2本命",
        "57件 / 106.03% / RB1/256.1",
        {
          requiredFlags: ["gogoTenjinNeoHistoryReady"],
          anyFlags: ["gogoTenjinNeoS1", "gogoTenjinNeoS2"],
        },
        ["gogo-tenjin-neo-aim"],
      ),
      buildCondition(
        "gogo-tenjin-wide",
        "S1/S2/S3広め",
        "78件 / 105.98% / RB1/263.9",
        {
          requiredFlags: ["gogoTenjinNeoHistoryReady"],
          anyFlags: ["gogoTenjinNeoS1", "gogoTenjinNeoS2", "gogoTenjinNeoS3"],
        },
        ["gogo-tenjin-neo-aim"],
      ),
      buildCondition(
        "gogo-tenjin-score75-safe",
        "75点以上＋危険0",
        "85件 / 105.15% / RB1/280.1",
        {
          minScore: 75,
          maxDanger: 0,
          requiredFlags: ["gogoTenjinNeoHistoryReady"],
        },
        ["gogo-tenjin-neo-aim"],
      ),
      buildCondition(
        "gogo-tenjin-score70-genuine-unpaid",
        "70点以上＋本物感＋返済未完",
        "41件 / 104.85% / RB1/267.6",
        {
          minScore: 70,
          requiredFlags: [
            "gogoTenjinNeoHistoryReady",
            "gogoTenjinNeoGenuinePrevious",
            "gogoTenjinNeoUnpaid",
          ],
        },
        ["gogo-tenjin-neo-aim"],
      ),
      buildCondition(
        "gogo-tenjin-score75",
        "75点以上",
        "93件 / 104.88% / RB1/282.7",
        {
          minScore: 75,
          requiredFlags: ["gogoTenjinNeoHistoryReady"],
        },
        ["gogo-tenjin-neo-aim"],
      ),
      buildCondition(
        "gogo-tenjin-rank1-gap10",
        "1位＋次点差10以上",
        "62件 / 104.32% / RB1/283.3",
        {
          rankMax: 1,
          minNextGap: 10,
          requiredFlags: ["gogoTenjinNeoHistoryReady"],
        },
        ["gogo-tenjin-neo-aim"],
      ),
      buildCondition(
        "gogo-tenjin-rank1-gap8",
        "1位＋次点差8以上",
        "77件 / 104.08% / RB1/286.5",
        {
          rankMax: 1,
          minNextGap: 8,
          requiredFlags: ["gogoTenjinNeoHistoryReady"],
        },
        ["gogo-tenjin-neo-aim"],
      ),
      buildCondition(
        "gogo-tenjin-score70",
        "70点以上",
        "195件 / 103.97% / RB1/284.0",
        {
          minScore: 70,
          requiredFlags: ["gogoTenjinNeoHistoryReady"],
        },
        ["gogo-tenjin-neo-aim"],
      ),
      buildCondition(
        "gogo-tenjin-rank1",
        "1位",
        "188件 / 103.79% / RB1/288.6",
        {
          rankMax: 1,
          requiredFlags: ["gogoTenjinNeoHistoryReady"],
        },
        ["gogo-tenjin-neo-aim"],
      ),
      buildCondition(
        "gogo-tenjin-top2",
        "上位2台",
        "376件 / 103.36% / RB1/288.0",
        {
          rankMax: 2,
          requiredFlags: ["gogoTenjinNeoHistoryReady"],
        },
        ["gogo-tenjin-neo-aim"],
      ),
      buildCondition(
        "gogo-tenjin-top3",
        "上位3台",
        "564件 / 102.74% / RB1/289.1",
        {
          rankMax: 3,
          requiredFlags: ["gogoTenjinNeoHistoryReady"],
        },
        ["gogo-tenjin-neo-aim"],
      ),
      buildCondition(
        "gogo-tenjin-top5",
        "上位5台",
        "940件 / 102.31% / RB1/293.2",
        {
          rankMax: 5,
          requiredFlags: ["gogoTenjinNeoHistoryReady"],
        },
        ["gogo-tenjin-neo-aim"],
      ),
      buildCondition(
        "hinode-onojo-six-losing-nearby",
        "6連敗+近隣見せ場",
        "31件 / 104.08% / RB1/268.9 / p56 51.5%",
        {
          requiredFlags: ["hinodeNeoHistoryReady", "hinodeNeoSixLosingNearby"],
        },
        ["hinode-onojo-neo-aim"],
      ),
      buildCondition(
        "hinode-onojo-three4000-five5000",
        "3日-4000×5日-5000",
        "39件 / 103.39% / RB1/273.0 / p56 48.7%",
        {
          requiredFlags: ["hinodeNeoHistoryReady", "hinodeNeoThree4000Five5000"],
        },
        ["hinode-onojo-neo-aim"],
      ),
      buildCondition(
        "hinode-onojo-three-sink-five-loss",
        "3日沈み+5連敗",
        "40件 / 104.70% / RB1/282.6 / p56 45.2%",
        {
          requiredFlags: ["hinodeNeoHistoryReady", "hinodeNeoThreeSinkFiveLoss"],
        },
        ["hinode-onojo-neo-aim"],
      ),
      buildCondition(
        "hinode-onojo-five-loss-bonus-weak",
        "5連敗+7合算悪化",
        "62件 / 104.24% / RB1/295.7 / p56 40.2%",
        {
          requiredFlags: ["hinodeNeoHistoryReady", "hinodeNeoFiveLossBonusWeak"],
        },
        ["hinode-onojo-neo-aim"],
      ),
      buildCondition(
        "hinode-onojo-deep-sink-57",
        "深沈み5×7",
        "652件 / 101.54% / RB1/303.8 / p56 35.5%",
        {
          requiredFlags: ["hinodeNeoHistoryReady", "hinodeNeoDeepSink57"],
        },
        ["hinode-onojo-neo-aim"],
      ),
      buildCondition(
        "hinode-onojo-short-core",
        "短期沈み本命",
        "45件 / 103.94% / RB1/271.7 / p56 49.3%",
        {
          rankMax: 1,
          minScore: 75,
          minNextGap: 5,
          minBoost: 3,
          requiredFlags: ["hinodeNeoHistoryReady", "hinodeNeoDiff3Deep"],
        },
        ["hinode-onojo-neo-aim"],
      ),
      buildCondition(
        "hinode-onojo-rank1-score75-boost3-gap5",
        "1位75+強3+差5",
        "83件 / 104.00% / RB1/280.2 / p56 45.4%",
        {
          rankMax: 1,
          minScore: 75,
          minNextGap: 5,
          minBoost: 3,
          requiredFlags: ["hinodeNeoHistoryReady"],
        },
        ["hinode-onojo-neo-aim"],
      ),
      buildCondition(
        "hinode-onojo-score85-boost3",
        "85+強化3",
        "114件 / 103.33% / RB1/289.4 / p56 41.6%",
        {
          minScore: 85,
          minBoost: 3,
          requiredFlags: ["hinodeNeoHistoryReady"],
        },
        ["hinode-onojo-neo-aim"],
      ),
      buildCondition(
        "hinode-onojo-score85",
        "85点以上",
        "174件 / 102.39% / RB1/292.4 / p56 40.0%",
        {
          minScore: 85,
          requiredFlags: ["hinodeNeoHistoryReady"],
        },
        ["hinode-onojo-neo-aim"],
      ),
      buildCondition(
        "hinode-onojo-rank1-score60",
        "1位60+",
        "236件 / 102.18% / RB1/296.4 / p56 38.2%",
        {
          rankMax: 1,
          minScore: 60,
          requiredFlags: ["hinodeNeoHistoryReady"],
        },
        ["hinode-onojo-neo-aim"],
      ),
      buildCondition(
        "hinode-onojo-score80",
        "広め80+",
        "245件 / 101.99% / RB1/295.5 / p56 38.6%",
        {
          minScore: 80,
          requiredFlags: ["hinodeNeoHistoryReady"],
        },
        ["hinode-onojo-neo-aim"],
      ),
      buildCondition(
        "hinode-onojo-watch-close-rank1",
        "見送り：1位僅差",
        "44件 / 100.63% / RB1/325.3 / p56 29.1%",
        {
          rankMax: 1,
          maxNextGap: 2.999,
          requiredFlags: ["hinodeNeoHistoryReady"],
        },
        ["hinode-onojo-neo-aim"],
      ),
      buildCondition(
        "hinode-onojo-watch-low-score-rank1",
        "見送り：低スコア1位",
        "見送り / 100.01% / p56 29.8%",
        {
          rankMax: 1,
          maxScore: 59.999,
          requiredFlags: ["hinodeNeoHistoryReady"],
        },
        ["hinode-onojo-neo-aim"],
      ),
      buildCondition(
        "hinode-onojo-watch-history-short",
        "見送り：履歴不足",
        "履歴7日未満",
        {
          requiredFlags: ["hinodeNeoHistoryShort"],
        },
        ["hinode-onojo-neo-aim"],
      ),
      buildCondition(
        "hinode-onojo-watch-danger2",
        "見送り：危険複数",
        "危険条件2個以上",
        {
          minDanger: 2,
          requiredFlags: ["hinodeNeoHistoryReady"],
        },
        ["hinode-onojo-neo-aim"],
      ),
      buildCondition(
        "chikushino-rank1-gap3",
        "1位＋次点差3",
        "9台 / RB1/278.8 / 合成1/134.0 / 平均56 43.5 / 56が50以上55.6",
        {
          rankMax: 1,
          minNextGap: 3,
          requiredFlags: ["chikushinoNeoHistoryReady"],
        },
        ["chikushino-neo-aim"],
      ),
      buildCondition(
        "chikushino-score85-low0-prev1500",
        "85点＋低内容5日ゼロ＋前日1500G以下",
        "21台 / 105.02% / RB1/267.5 / 合成1/130.5 / 平均56 46.4%",
        {
          minScore: 85,
          requiredFlags: [
            "chikushinoNeoHistoryReady",
            "chikushinoNeoLowContentFiveZero",
            "chikushinoNeoPreviousGames1500",
          ],
        },
        ["chikushino-neo-aim"],
      ),
      buildCondition(
        "chikushino-score85-prev2500-diff1000",
        "85点＋前日2500G以下＋3日差枚-1000以下",
        "20台 / 106.73% / RB1/277.3 / 合成1/126.2 / 平均56 44.0%",
        {
          minScore: 85,
          requiredFlags: [
            "chikushinoNeoHistoryReady",
            "chikushinoNeoPreviousGames2500",
            "chikushinoNeoThreeDiffSink1000",
          ],
        },
        ["chikushino-neo-aim"],
      ),
      buildCondition(
        "chikushino-score80-diff1000",
        "80点＋3日差枚-1000以下",
        "35台 / 104.43% / RB1/291.4 / 合成1/132.6 / 平均56 38.9%",
        {
          minScore: 80,
          requiredFlags: ["chikushinoNeoHistoryReady", "chikushinoNeoThreeDiffSink1000"],
        },
        ["chikushino-neo-aim"],
      ),
      buildCondition(
        "chikushino-rank2",
        "毎日上位2",
        "44台 / 104.84% / RB1/309.4 / 合成1/138.1 / 平均56 34.6%",
        {
          rankMax: 2,
          requiredFlags: ["chikushinoNeoHistoryReady"],
        },
        ["chikushino-neo-aim"],
      ),
      buildCondition(
        "chikushino-score90",
        "90点以上",
        "114台 / 102.51% / RB1/305.4 / 合成1/140.1 / 平均56 34.5%",
        {
          minScore: 90,
          requiredFlags: ["chikushinoNeoHistoryReady"],
        },
        ["chikushino-neo-aim"],
      ),
      buildCondition(
        "chikushino-score85",
        "85点以上",
        "170台 / 103.68% / RB1/308.9 / 合成1/140.7 / 平均56 33.4%",
        {
          minScore: 85,
          requiredFlags: ["chikushinoNeoHistoryReady"],
        },
        ["chikushino-neo-aim"],
      ),
      buildCondition(
        "chikushino-score80",
        "80点以上",
        "240台 / 103.17% / RB1/310.7 / 合成1/141.7 / 平均56 32.8%",
        {
          minScore: 80,
          requiredFlags: ["chikushinoNeoHistoryReady"],
        },
        ["chikushino-neo-aim"],
      ),
      buildCondition(
        "chikushino-score75",
        "75点以上",
        "339台 / 102.37% / RB1/316.8 / 合成1/143.4 / 平均56 31.5%",
        {
          minScore: 75,
          requiredFlags: ["chikushinoNeoHistoryReady"],
        },
        ["chikushino-neo-aim"],
      ),
      buildCondition(
        "chikushino-low5-zero",
        "低内容5日ゼロ",
        "76台 / 104.79% / RB1/307.4 / 合成1/139.6 / 平均56 34.0%",
        {
          requiredFlags: ["chikushinoNeoHistoryReady", "chikushinoNeoLowContentFiveZero"],
        },
        ["chikushino-neo-aim"],
      ),
      buildCondition(
        "chikushino-weak7-zero",
        "弱内容7日ゼロ",
        "54台 / 104.19% / RB1/320.5 / 合成1/144.6 / 平均56 31.2%",
        {
          requiredFlags: ["chikushinoNeoHistoryReady", "chikushinoNeoWeakContentSevenZero"],
        },
        ["chikushino-neo-aim"],
      ),
      buildCondition(
        "chikushino-watch-low-score",
        "見送り：50点未満",
        "230台 / 99.53% / RB1/369.3 / 合成1/155.5 / 平均56 23.2%",
        {
          maxScore: 49.999,
        },
        ["chikushino-neo-aim"],
      ),
      buildCondition(
        "chikushino-watch-long-weak",
        "見送り：危険長期弱",
        "225台 / RB1/359.5 / 合成1/152.3 / 平均56 24.8",
        {
          requiredFlags: ["chikushinoNeoLongWeakContent"],
        },
        ["chikushino-neo-aim"],
      ),
      buildCondition(
        "chikushino-watch-games-over",
        "見送り：直近5日G過多25000超",
        "70台 / 99.16% / RB1/371.8 / 合成1/157.4 / 平均56 22.1%",
        {
          requiredFlags: ["chikushinoNeoOverWorked"],
        },
        ["chikushino-neo-aim"],
      ),
      buildCondition(
        "chikushino-watch-high-danger",
        "見送り：高点数でも危険多め",
        "70点以上でも危険2個以上かつ強化1個以下",
        {
          minScore: 70,
          minDanger: 2,
          maxBoost: 1,
        },
        ["chikushino-neo-aim"],
      ),
      buildCondition(
        "espace-ueno-best270",
        "最本命270/自由A",
        "20台 / 103.03% / RB1/240.2 / 合成1/130.6 / 平均56 54.8%",
        {
          requiredFlags: ["espaceUenoNeoHistoryReady", "espaceUenoNeoFreeABestRb"],
        },
        ["espace-ueno-neo-aim"],
      ),
      buildCondition(
        "espace-ueno-strong280",
        "強280/自由B",
        "35台 / 102.33% / RB1/266.3 / 合成1/135.8 / 平均56 46.2%",
        {
          minScore: 85,
          requiredFlags: ["espaceUenoNeoHistoryReady", "espaceUenoNeoFreeBBalanced"],
        },
        ["espace-ueno-neo-aim"],
      ),
      buildCondition(
        "espace-ueno-main290",
        "本命290",
        "41台 / 101.86% / RB1/270.1 / 合成1/137.3 / 平均56 44.0%",
        {
          minScore: 85,
          requiredFlags: ["espaceUenoNeoHistoryReady", "espaceUenoNeoLowMidGames3", "espaceUenoNeoRb21Strong"],
        },
        ["espace-ueno-neo-aim"],
      ),
      buildCondition(
        "espace-ueno-weak300",
        "弱本命300",
        "122台 / 102.01% / RB1/296.7 / 合成1/140.8 / 平均56 36.0%",
        {
          minScore: 90,
          requiredFlags: ["espaceUenoNeoHistoryReady"],
        },
        ["espace-ueno-neo-aim"],
      ),
      buildCondition(
        "espace-ueno-wide310",
        "広め310",
        "667台 / 101.94% / RB1/307.9 / 合成1/142.7 / 平均56 33.2%",
        {
          minScore: 80,
          requiredFlags: ["espaceUenoNeoHistoryReady"],
        },
        ["espace-ueno-neo-aim"],
      ),
      buildCondition(
        "espace-ueno-free-c-wide",
        "自由C 広め代替",
        "1641台 / 101.58% / RB1/316.1 / 合成1/144.7 / 平均56 31.2%",
        {
          requiredFlags: ["espaceUenoNeoHistoryReady", "espaceUenoNeoGames3Under10000"],
        },
        ["espace-ueno-neo-aim"],
      ),
      buildCondition(
        "espace-ueno-free-d-compromise",
        "自由D 妥協",
        "599台 / 101.89% / RB1/311.6 / 合成1/143.3 / 平均56 32.5%",
        {
          requiredFlags: ["espaceUenoNeoHistoryReady", "espaceUenoNeoCompromise"],
        },
        ["espace-ueno-neo-aim"],
      ),
      buildCondition(
        "espace-ueno-watch-history-short",
        "見送り：履歴不足",
        "履歴7営業日未満 / 点数上限45",
        {
          requiredFlags: ["espaceUenoNeoHistoryShort"],
        },
        ["espace-ueno-neo-aim"],
      ),
      buildCondition(
        "espace-ueno-watch-over-visible",
        "見送り：高稼働見え切り",
        "G3 15000以上",
        {
          requiredFlags: ["espaceUenoNeoHistoryReady", "espaceUenoNeoGames3TooHigh"],
        },
        ["espace-ueno-neo-aim"],
      ),
      buildCondition(
        "espace-ueno-watch-deep-sink",
        "見送り：深沈み",
        "diff7 -5000以下",
        {
          requiredFlags: ["espaceUenoNeoHistoryReady", "espaceUenoNeoDiff7TooDeep"],
        },
        ["espace-ueno-neo-aim"],
      ),
      buildCondition(
        "espace-ueno-watch-previous-big",
        "見送り：前日大出し",
        "前日+2000枚以上",
        {
          requiredFlags: ["espaceUenoNeoHistoryReady", "espaceUenoNeoPreviousBigWin"],
        },
        ["espace-ueno-neo-aim"],
      ),
      buildCondition(
        "espace-ueno-watch-previous-high",
        "見送り：前日高内容",
        "前日高内容",
        {
          requiredFlags: ["espaceUenoNeoHistoryReady", "espaceUenoNeoPreviousHigh"],
        },
        ["espace-ueno-neo-aim"],
      ),
      buildCondition(
        "espace-ueno-watch-overworked",
        "見送り：G3高稼働＋G5過多",
        "G3 15000以上かつG5 25000以上",
        {
          requiredFlags: ["espaceUenoNeoHistoryReady", "espaceUenoNeoHighVisibleAndOverworked"],
        },
        ["espace-ueno-neo-aim"],
      ),
      buildCondition(
        "espace-ueno-watch-thin80",
        "見送り：80未満強化薄",
        "80点未満かつ強化1個以下",
        {
          maxScore: 79.999,
          maxBoost: 1,
          requiredFlags: ["espaceUenoNeoHistoryReady"],
        },
        ["espace-ueno-neo-aim"],
      ),
      buildCondition(
        "espace-ueno-watch-high-risk",
        "見送り：高点数危険",
        "90点以上でも強化0個＋危険2個以上",
        {
          minScore: 90,
          maxBoost: 0,
          minDanger: 2,
          requiredFlags: ["espaceUenoNeoHistoryReady"],
        },
        ["espace-ueno-neo-aim"],
      ),
      buildCondition(
        "messe-minamisenju-free-14rb",
        "浅沈み前日凹み＋14RB低迷",
        "35台 / 102.31% / RB1/297.9 / 合成1/139.0 / 平均56 36.2%",
        {
          requiredFlags: ["messeNeoHistoryReady", "messeNeoFree14Rb"],
        },
        ["messe-minamisenju-neo-aim"],
      ),
      buildCondition(
        "messe-minamisenju-free-5combined",
        "浅沈み前日凹み＋5日合算低迷",
        "82台 / 101.89% / RB1/301.0 / 合成1/140.4 / 平均56 35.9%",
        {
          requiredFlags: ["messeNeoHistoryReady", "messeNeoFree5Combined"],
        },
        ["messe-minamisenju-neo-aim"],
      ),
      buildCondition(
        "messe-minamisenju-bb-tailwind-gap14",
        "BB寄せ前日＋14日空き",
        "16台 / 100.30% / RB1/263.5 / 合成1/137.8 / 平均56 42.3%",
        {
          maxDanger: 0,
          requiredFlags: ["messeNeoHistoryReady", "messeNeoBbTailwindGap14"],
        },
        ["messe-minamisenju-neo-aim"],
      ),
      buildCondition(
        "messe-minamisenju-score85-rank1-gap4",
        "85点＋1位＋次点差4",
        "63台 / 100.42% / RB1/298.5 / 合成1/143.4 / 平均56 36.3%",
        {
          minScore: 85,
          rankMax: 1,
          minNextGap: 4,
          requiredFlags: ["messeNeoHistoryReady"],
        },
        ["messe-minamisenju-neo-aim"],
      ),
      buildCondition(
        "messe-minamisenju-rank1-gap8",
        "1位＋次点差8",
        "41台 / 100.56% / RB1/303.3 / 合成1/143.8 / 平均56 34.9%",
        {
          rankMax: 1,
          minNextGap: 8,
          requiredFlags: ["messeNeoHistoryReady"],
        },
        ["messe-minamisenju-neo-aim"],
      ),
      buildCondition(
        "messe-minamisenju-rank1-gap2",
        "1位＋次点差2",
        "101台 / 99.98% / RB1/309.0 / 合成1/146.1 / 平均56 33.5%",
        {
          rankMax: 1,
          minNextGap: 2,
          requiredFlags: ["messeNeoHistoryReady"],
        },
        ["messe-minamisenju-neo-aim"],
      ),
      buildCondition(
        "messe-minamisenju-score90-boost3-show",
        "90点＋強化3＋見せ場",
        "19台 / 103.64% / RB1/280.8 / 合成1/134.9 / 平均56 41.5%",
        {
          minScore: 90,
          minBoost: 3,
          requiredFlags: ["messeNeoHistoryReady", "messeNeoRecentShow"],
        },
        ["messe-minamisenju-neo-aim"],
      ),
      buildCondition(
        "messe-minamisenju-score90-sink7-show",
        "90点＋沈み7＋見せ場",
        "12台 / 103.61% / RB1/263.4 / 合成1/132.0 / 平均56 50.7%",
        {
          minScore: 90,
          requiredFlags: ["messeNeoHistoryReady", "messeNeoSinkStay7", "messeNeoRecentShow"],
        },
        ["messe-minamisenju-neo-aim"],
      ),
      buildCondition(
        "messe-minamisenju-score85-boost3-sink7-show",
        "85点＋強化3＋沈み7＋見せ場",
        "12台 / 105.04% / RB1/262.3 / 合成1/129.7 / 平均56 51.9%",
        {
          minScore: 85,
          minBoost: 3,
          requiredFlags: ["messeNeoHistoryReady", "messeNeoSinkStay7", "messeNeoRecentShow"],
        },
        ["messe-minamisenju-neo-aim"],
      ),
      buildCondition(
        "messe-minamisenju-score70-boost2-danger1",
        "70点＋強化2＋危険1以下",
        "524台 / 100.50% / RB1/319.3 / 合成1/146.6 / 平均56 31.4%",
        {
          minScore: 70,
          minBoost: 2,
          maxDanger: 1,
          requiredFlags: ["messeNeoHistoryReady"],
        },
        ["messe-minamisenju-neo-aim"],
      ),
      buildCondition(
        "messe-minamisenju-watch-history-short",
        "見送り：履歴不足",
        "履歴5日未満 / 点数上限60",
        {
          requiredFlags: ["messeNeoHistoryShort"],
        },
        ["messe-minamisenju-neo-aim"],
      ),
      buildCondition(
        "messe-minamisenju-watch-high-score-weak",
        "高スコア強化不足",
        "245台 / 100.14% / RB1/335.3 / 合成1/149.9 / 平均56 28.4%",
        {
          minScore: 70,
          maxBoost: 1,
          requiredFlags: ["messeNeoHistoryReady"],
        },
        ["messe-minamisenju-neo-aim"],
      ),
      buildCondition(
        "messe-minamisenju-watch-deep5",
        "深凹みだけ",
        "290台 / 100.24% / RB1/342.3 / 合成1/150.1 / 平均56 26.9%",
        {
          requiredFlags: ["messeNeoHistoryReady", "messeNeoDeepFiveOnly"],
        },
        ["messe-minamisenju-neo-aim"],
      ),
      buildCondition(
        "messe-minamisenju-watch-losing4",
        "4連敗以上",
        "297台 / 100.23% / RB1/334.8 / 合成1/149.4 / 平均56 28.1%",
        {
          requiredFlags: ["messeNeoHistoryReady", "messeNeoLosing4"],
        },
        ["messe-minamisenju-neo-aim"],
      ),
      buildCondition(
        "messe-minamisenju-watch-low-games",
        "低稼働",
        "直近5日9000G未満",
        {
          requiredFlags: ["messeNeoHistoryReady", "messeNeoLowGames"],
        },
        ["messe-minamisenju-neo-aim"],
      ),
      buildCondition(
        "kintoki-kamata-free-a",
        "自由A_上位3前日弱3日内",
        "21日 / 23台 / 総G92,634 / BB347/REG344 / RB1/269.3 / 合算1/134.1 / 平均差枚+340.9 / 102.82% / 勝率39.1% / 平均56 43.1% / 中央56 28.9% / 56>=50 39.1% / 56<30 52.2% / RB<=300 47.8% / RB>400 34.8% / 合成<=130 30.4% / 合成<=140 47.8%",
        {
          rankMax: 3,
          requiredFlags: ["kintokiNeoHistoryReady", "kintokiNeoPreviousWeak", "kintokiNeoShortHighLeft"],
        },
        ["kintoki-kamata-neo-aim"],
      ),
      buildCondition(
        "kintoki-kamata-free-b",
        "自由B_沈み高稼働前日弱",
        "23日 / 27台 / 総G92,999 / BB324/REG337 / RB1/276.0 / 合算1/140.7 / 平均差枚+57.3 / 100.55% / 勝率37.0% / 平均56 41.2% / 中央56 33.9% / 56>=50 37.0% / 56<30 40.7% / RB<=300 51.9% / RB>400 25.9% / 合成<=130 22.2% / 合成<=140 48.1%",
        {
          requiredFlags: ["kintokiNeoHistoryReady", "kintokiNeoSinkHighGames", "kintokiNeoPreviousWeak"],
        },
        ["kintoki-kamata-neo-aim"],
      ),
      buildCondition(
        "kintoki-kamata-free-c",
        "自由C_近隣2台見せ場後沈み",
        "15日 / 21台 / 総G70,640 / BB262/REG257 / RB1/274.9 / 合算1/136.1 / 平均差枚+283.4 / 102.81% / 勝率66.7% / 平均56 39.4% / 中央56 35.6% / 56>=50 23.8% / 56<30 33.3% / RB<=300 47.6% / RB>400 23.8% / 合成<=130 19.0% / 合成<=140 38.1%",
        {
          requiredFlags: ["kintokiNeoHistoryReady", "kintokiNeoNearbyTwoHighSink"],
        },
        ["kintoki-kamata-neo-aim"],
      ),
      buildCondition(
        "kintoki-kamata-free-d",
        "自由D_前日BB寄り出玉",
        "30日 / 40台 / 総G110,103 / BB391/REG381 / BB1/281.6 / RB1/289.0 / 合算1/142.6 / 平均差枚+62.2 / 100.75% / 勝率40.0% / 平均56 36.0% / 中央56 30.4% / 56>=50 20.0% / 56<30 50.0% / RB<=300 40.0% / RB>400 32.5% / 合成<=130 25.0% / 合成<=140 27.5%",
        {
          requiredFlags: ["kintokiNeoHistoryReady", "kintokiNeoBbOutputContinue"],
        },
        ["kintoki-kamata-neo-aim"],
      ),
      buildCondition(
        "kintoki-kamata-free-e",
        "自由E_上位3前日弱",
        "51日 / 69台 / 総G206,775 / BB772/REG690 / BB1/267.8 / RB1/299.7 / 合算1/141.4 / 平均差枚+137.2 / 101.53% / 勝率39.1% / 平均56 34.6% / 中央56 28.9% / 56>=50 21.7% / 56<30 53.6% / RB<=300 39.1% / RB>400 39.1% / 合成<=130 23.2% / 合成<=140 36.2%",
        {
          rankMax: 3,
          requiredFlags: ["kintokiNeoHistoryReady", "kintokiNeoPreviousWeak"],
        },
        ["kintoki-kamata-neo-aim"],
      ),
      buildCondition(
        "kintoki-kamata-rank1",
        "全日1位",
        "103日 / 103台 / 総G349,003 / BB1260/REG1129 / BB1/277.0 / RB1/309.1 / 合算1/146.1 / 平均差枚+25.9 / 100.25% / 勝率39.8% / 平均56 32.9% / 中央56 27.8% / 56>=50 20.4% / 56<30 57.3% / RB<=300 37.9% / RB>400 39.8% / 合成<=130 16.5% / 合成<=140 30.1%",
        {
          rankMax: 1,
          requiredFlags: ["kintokiNeoHistoryReady"],
        },
        ["kintoki-kamata-neo-aim"],
      ),
      buildCondition(
        "kintoki-kamata-top3-prev-weak",
        "上位3＋前日弱",
        "51日 / 69台 / 総G206,775 / BB772/REG690 / BB1/267.8 / RB1/299.7 / 合算1/141.4 / 平均差枚+137.2 / 101.53% / 勝率39.1% / 平均56 34.6% / 中央56 28.9% / 56>=50 21.7% / 56<30 53.6% / RB<=300 39.1% / RB>400 39.1% / 合成<=130 23.2% / 合成<=140 36.2%",
        {
          rankMax: 3,
          requiredFlags: ["kintokiNeoHistoryReady", "kintokiNeoPreviousWeak"],
        },
        ["kintoki-kamata-neo-aim"],
      ),
      buildCondition(
        "kintoki-kamata-previous-bb-output",
        "前日BB寄り出玉",
        "30日 / 40台 / 総G110,103 / BB391/REG381 / BB1/281.6 / RB1/289.0 / 合算1/142.6 / 平均差枚+62.2 / 100.75% / 勝率40.0% / 平均56 36.0% / 中央56 30.4% / 56>=50 20.0% / 56<30 50.0% / RB<=300 40.0% / RB>400 32.5% / 合成<=130 25.0% / 合成<=140 27.5%",
        {
          requiredFlags: ["kintokiNeoHistoryReady", "kintokiNeoBbOutputContinue"],
        },
        ["kintoki-kamata-neo-aim"],
      ),
      buildCondition(
        "kintoki-kamata-sink-high-games-prev-weak",
        "沈み高稼働＋前日弱",
        "23日 / 27台 / 総G92,999 / BB324/REG337 / RB1/276.0 / 合算1/140.7 / 平均差枚+57.3 / 100.55% / 勝率37.0% / 平均56 41.2% / 中央56 33.9% / 56>=50 37.0% / 56<30 40.7% / RB<=300 51.9% / RB>400 25.9% / 合成<=130 22.2% / 合成<=140 48.1%",
        {
          requiredFlags: ["kintokiNeoHistoryReady", "kintokiNeoSinkHighGames", "kintokiNeoPreviousWeak"],
        },
        ["kintoki-kamata-neo-aim"],
      ),
      buildCondition(
        "kintoki-kamata-top3-prev-weak-3d",
        "上位3＋前日弱＋3日内",
        "21日 / 23台 / 総G92,634 / BB347/REG344 / BB1/267.0 / RB1/269.3 / 合算1/134.1 / 平均差枚+340.9 / 102.82% / 勝率39.1% / 平均56 43.1% / 中央56 28.9% / 56>=70 17.4% / 56>=50 39.1% / 56<30 52.2% / RB<=300 47.8% / RB>400 34.8% / 合成<=130 30.4% / 合成<=140 47.8%",
        {
          rankMax: 3,
          requiredFlags: ["kintokiNeoHistoryReady", "kintokiNeoPreviousWeak", "kintokiNeoShortHighLeft"],
        },
        ["kintoki-kamata-neo-aim"],
      ),
      buildCondition(
        "kintoki-kamata-watch-history-short",
        "見送り：履歴不足",
        "同一台番履歴14営業日未満 / 点数上限35",
        {
          requiredFlags: ["kintokiNeoHistoryShort"],
        },
        ["kintoki-kamata-neo-aim"],
      ),
      buildCondition(
        "kintoki-kamata-watch-long-neglect",
        "見送り：長期放置",
        "前回高内容から29日以上、直近14日高内容0回 / 自由条件なし",
        {
          requiredFlags: ["kintokiNeoHistoryReady", "kintokiNeoLongNeglect", "kintokiNeoNoFree"],
        },
        ["kintoki-kamata-neo-aim"],
      ),
      buildCondition(
        "kintoki-kamata-watch-risk-without-free",
        "見送り：危険あり自由なし",
        "45点以上でも危険条件あり、自由度MAX条件なし",
        {
          minScore: 45,
          minDanger: 1,
          requiredFlags: ["kintokiNeoHistoryReady", "kintokiNeoNoFree"],
        },
        ["kintoki-kamata-neo-aim"],
      ),
      buildCondition(
        "kintoki-kamata-watch-low-score",
        "見送り：40点未満自由なし",
        "スコア40点未満、自由度MAX条件なし",
        {
          maxScore: 39.999,
          requiredFlags: ["kintokiNeoHistoryReady", "kintokiNeoNoFree"],
        },
        ["kintoki-kamata-neo-aim"],
      ),
      buildCondition(
        "kintoki-kamata-watch-rank1-small-gap",
        "慎重：1位次点差小",
        "1位でも次点差8点以下、自由度MAX条件なし",
        {
          rankMax: 1,
          maxNextGap: 8,
          requiredFlags: ["kintokiNeoHistoryReady", "kintokiNeoNoFree"],
        },
        ["kintoki-kamata-neo-aim"],
      ),
      buildCondition(
        "beam-hikari-main",
        "70点以上",
        "388件 / 103.33% / RB1/287.6 / p56 32.7%",
        {
          minScore: 70,
          requiredFlags: ["beamHikariNeoHistoryReady"],
        },
        ["beam-hikari-neo-aim", "beam-hikari-neo-aim-normal", "beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "beam-hikari-score60",
        "60点以上",
        "665件 / 102.89% / RB1/286.4 / p56 34.3%",
        {
          minScore: 60,
          requiredFlags: ["beamHikariNeoHistoryReady"],
        },
        ["beam-hikari-neo-aim", "beam-hikari-neo-aim-normal", "beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "beam-hikari-rank1-gap5",
        "1位＋差5",
        "110件 / 103.70% / RB1/276.0 / p56 43.6%",
        {
          rankMax: 1,
          minNextGap: 5,
          requiredFlags: ["beamHikariNeoHistoryReady"],
        },
        ["beam-hikari-neo-aim", "beam-hikari-neo-aim-normal", "beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "beam-hikari-short-rank1-gap5",
        "短期急沈み＋1位＋差5",
        "77件 / 104.63% / RB1/275.8 / p56 48.1%",
        {
          rankMax: 1,
          minNextGap: 5,
          requiredFlags: ["beamHikariNeoHistoryReady", "beamHikariNeoShortSteepSink"],
        },
        ["beam-hikari-neo-aim", "beam-hikari-neo-aim-normal", "beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "beam-hikari-rank1-gap8",
        "1位＋差8",
        "74件 / 105.45% / RB1/279.4 / p56 41.9%",
        {
          rankMax: 1,
          minNextGap: 8,
          requiredFlags: ["beamHikariNeoHistoryReady"],
        },
        ["beam-hikari-neo-aim", "beam-hikari-neo-aim-normal", "beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "beam-hikari-rank1-gap8-boost2",
        "1位＋差8＋強化2",
        "67件 / 105.87% / RB1/274.8 / p56 44.8%",
        {
          rankMax: 1,
          minNextGap: 8,
          minBoost: 2,
          requiredFlags: ["beamHikariNeoHistoryReady"],
        },
        ["beam-hikari-neo-aim", "beam-hikari-neo-aim-normal", "beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "beam-hikari-normal-main",
        "短期急沈み＋1位＋差8",
        "50件 / 106.46% / RB1/274.8 / p56 48.0%",
        {
          rankMax: 1,
          minNextGap: 8,
          requiredFlags: ["beamHikariNeoHistoryReady", "beamHikariNeoShortSteepSink"],
        },
        ["beam-hikari-neo-aim", "beam-hikari-neo-aim-normal", "beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "beam-hikari-event-main",
        "短期急沈み＋1位＋差8",
        "50件 / 106.46% / RB1/274.8 / p56 48.0%",
        {
          rankMax: 1,
          minNextGap: 8,
          requiredFlags: ["beamHikariNeoHistoryReady", "beamHikariNeoShortSteepSink"],
        },
        ["beam-hikari-neo-aim", "beam-hikari-neo-aim-normal", "beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "beam-hikari-rank1-gap5-previous-low-games",
        "1位＋差5＋前日低G",
        "52件 / 104.67% / RB1/264.7 / p56 50.0%",
        {
          rankMax: 1,
          minNextGap: 5,
          requiredFlags: ["beamHikariNeoHistoryReady", "beamHikariNeoPreviousLowGames"],
        },
        ["beam-hikari-neo-aim", "beam-hikari-neo-aim-normal", "beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "beam-hikari-reference-sp",
        "参考SP：差3＋中凹み＋前日G",
        "20件 / 105.67% / RB1/248.0 / p56 55.0%",
        {
          rankMax: 1,
          minNextGap: 3,
          requiredFlags: ["beamHikariNeoHistoryReady", "beamHikariNeoReferenceSp"],
        },
        ["beam-hikari-neo-aim", "beam-hikari-neo-aim-normal", "beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "beam-hikari-free-a",
        "自由A：短期急沈み",
        "372件 / 103.42% / RB1/282.9 / p56 35.8%",
        {
          requiredFlags: ["beamHikariNeoHistoryReady", "beamHikariNeoFreeA"],
        },
        ["beam-hikari-neo-aim", "beam-hikari-neo-aim-normal", "beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "beam-hikari-free-b",
        "自由B：短期＋7日沈み",
        "59件 / 104.87% / RB1/274.3 / p56 44.1%",
        {
          requiredFlags: ["beamHikariNeoHistoryReady", "beamHikariNeoFreeB"],
        },
        ["beam-hikari-neo-aim", "beam-hikari-neo-aim-normal", "beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "beam-hikari-free-c",
        "自由C：最強raw（件数注意）",
        "30件 / 105.50% / RB1/256.9 / p56 50.0%",
        {
          requiredFlags: ["beamHikariNeoHistoryReady", "beamHikariNeoFreeC"],
        },
        ["beam-hikari-neo-aim", "beam-hikari-neo-aim-normal", "beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "beam-hikari-compromise-low-exposure",
        "妥協：2日低稼働不遇",
        "501件 / 102.90% / RB1/293.9 / p56 30.1%",
        {
          requiredFlags: ["beamHikariNeoHistoryReady", "beamHikariNeoCompromiseLowExposure"],
        },
        ["beam-hikari-neo-aim", "beam-hikari-neo-aim-normal", "beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "amuse-asakusa-rank1",
        "1位のみ",
        "196件 / 102.7% / RB1/292.1",
        {
          rankMax: 1,
          requiredFlags: ["amuseAsakusaNeoHistoryReady"],
        },
        ["amuse-asakusa-neo-aim"],
      ),
      buildCondition(
        "amuse-asakusa-gap5",
        "1位＋次点差5点以上",
        "125件 / 102.5% / RB1/290.5",
        {
          rankMax: 1,
          minNextGap: 5,
          requiredFlags: ["amuseAsakusaNeoHistoryReady"],
        },
        ["amuse-asakusa-neo-aim"],
      ),
      buildCondition(
        "amuse-asakusa-main",
        "1位＋70点以上＋次点差10点以上＋強化2個以上＋危険1以下",
        "34件 / 104.8% / RB1/270.4",
        {
          rankMax: 1,
          minScore: 70,
          minNextGap: 10,
          minBoost: 2,
          maxDanger: 1,
          requiredFlags: ["amuseAsakusaNeoHistoryReady"],
        },
        ["amuse-asakusa-neo-aim"],
      ),
      buildCondition(
        "amuse-asakusa-gap15",
        "1位＋70点以上＋次点差15点以上",
        "21件 / 105.9% / RB1/261.4",
        {
          rankMax: 1,
          minScore: 70,
          minNextGap: 15,
          requiredFlags: ["amuseAsakusaNeoHistoryReady"],
        },
        ["amuse-asakusa-neo-aim"],
      ),
      buildCondition(
        "amuse-asakusa-wide",
        "70点以上＋強化2個以上＋危険1以下",
        "76件 / 103.0% / RB1/295.8",
        {
          minScore: 70,
          minBoost: 2,
          maxDanger: 1,
          requiredFlags: ["amuseAsakusaNeoHistoryReady"],
        },
        ["amuse-asakusa-neo-aim"],
      ),
    ],
  },
  {
    machineKey: "houou",
    machineNames: ["ハナハナホウオウ", "ハナハナホウオウ-30", "ハナハナホウオウ～天翔～-30"],
    logicKey: "apark-houou",
    logicName: "ホウオウ春日式",
    profile: "hana",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "1位＋70点以上＋28日未返済＋7日合成弱",
        "39件 / 103.23% / RB1/375.8",
        {
          rankMax: 1,
          minScore: 70,
          requiredFlags: ["hououUnpaid28Weak7"],
        },
      ),
    ],
  },
  {
    machineKey: "funky",
    machineNames: ["ファンキージャグラー２ＫＴ", "ファンキージャグラー２", "ファンキージャグラー2"],
    logicKey: "apark-funky",
    logicName: "ファンキー春日式",
    logics: [
      buildLogicVariant("apark-funky", "ファンキー春日式", "main"),
      buildLogicVariant("apark-yakatabaru-funky", "ファンキー屋形原式", "apark-yakatabaru-main"),
      buildLogicVariant("mj-kurume-funky", "ファンキーMJ久留米式", "mj-kurume-main"),
      buildLogicVariant("beam-hikari-funky", "ファンキービームヒカリ式", "beam-hikari-main-core"),
      buildLogicVariant("beam-hikari-funky-normal", "ファンキービームヒカリ通常日式", "beam-hikari-normal-core"),
      buildLogicVariant("beam-hikari-funky-event", "ファンキービームヒカリイベント日式", "beam-hikari-event-score90"),
    ],
    profile: "juggler",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "1位＋70点以上＋3〜6連敗＋前回高内容8〜14日",
        "42件 / 105.98% / RB1/300.4",
        {
          rankMax: 1,
          minScore: 70,
          requiredFlags: ["funkyHistoryReady", "funkyLosingStreak3To6", "funkyHighRest8To14"],
        },
        ["apark-funky"],
      ),
      buildCondition(
        "wide",
        "1位＋70点以上",
        "94件 / 103.99% / RB1/316.1",
        {
          rankMax: 1,
          minScore: 70,
          requiredFlags: ["funkyHistoryReady"],
        },
        ["apark-funky"],
      ),
      buildCondition(
        "apark-yakatabaru-main",
        "1位＋60点以上＋沈み強",
        "128件 / 102.46% / RB1/327.7",
        {
          rankMax: 1,
          minScore: 60,
          requiredFlags: ["yakatabaruFunkyHistoryReady", "yakatabaruFunkySinkStrong"],
        },
        ["apark-yakatabaru-funky"],
      ),
      buildCondition(
        "apark-yakatabaru-bonus",
        "1位＋60点以上＋沈み強＋ボーナス弱化",
        "92件 / 102.80% / RB1/322.8",
        {
          rankMax: 1,
          minScore: 60,
          requiredFlags: [
            "yakatabaruFunkyHistoryReady",
            "yakatabaruFunkySinkStrong",
            "yakatabaruFunkyBonusWeak",
          ],
        },
        ["apark-yakatabaru-funky"],
      ),
      buildCondition(
        "apark-yakatabaru-gap30",
        "1位＋60点以上＋沈み強＋次点差30点以上",
        "48件 / 103.19% / RB1/325.1",
        {
          rankMax: 1,
          minScore: 60,
          minNextGap: 30,
          requiredFlags: ["yakatabaruFunkyHistoryReady", "yakatabaruFunkySinkStrong"],
        },
        ["apark-yakatabaru-funky"],
      ),
      buildCondition(
        "apark-yakatabaru-score70",
        "70点以上",
        "152件 / 101.94% / RB1/334.6",
        {
          minScore: 70,
          requiredFlags: ["yakatabaruFunkyHistoryReady"],
        },
        ["apark-yakatabaru-funky"],
      ),
      buildCondition(
        "apark-yakatabaru-top1-score60",
        "1位＋60点以上",
        "175件 / 101.48% / RB1/334.7",
        {
          rankMax: 1,
          minScore: 60,
          requiredFlags: ["yakatabaruFunkyHistoryReady"],
        },
        ["apark-yakatabaru-funky"],
      ),
      buildCondition(
        "apark-yakatabaru-boost2",
        "1位＋60点以上＋強化2個以上",
        "155件 / 101.91% / RB1/335.0",
        {
          rankMax: 1,
          minScore: 60,
          minBoost: 2,
          requiredFlags: ["yakatabaruFunkyHistoryReady"],
        },
        ["apark-yakatabaru-funky"],
      ),
      buildCondition(
        "apark-yakatabaru-boost2-safe",
        "1位＋60点以上＋強化2個以上＋危険0",
        "133件 / 101.93% / RB1/335.9",
        {
          rankMax: 1,
          minScore: 60,
          minBoost: 2,
          maxDanger: 0,
          requiredFlags: ["yakatabaruFunkyHistoryReady"],
        },
        ["apark-yakatabaru-funky"],
      ),
      buildCondition(
        "mj-kurume-main",
        "1位＋60点以上",
        "44件 / 104.49% / RB1/337.8",
        {
          rankMax: 1,
          minScore: 60,
          requiredFlags: ["kurumeFunkyHistoryReady"],
        },
        ["mj-kurume-funky"],
      ),
      buildCondition(
        "mj-kurume-strong",
        "1位＋60点以上＋次点差15点以上",
        "32件 / 105.21% / RB1/351.7",
        {
          rankMax: 1,
          minScore: 60,
          minNextGap: 15,
          requiredFlags: ["kurumeFunkyHistoryReady"],
        },
        ["mj-kurume-funky"],
      ),
      buildCondition(
        "mj-kurume-high",
        "1位＋70点以上",
        "28件 / 105.12% / RB1/342.0",
        {
          rankMax: 1,
          minScore: 70,
          requiredFlags: ["kurumeFunkyHistoryReady"],
        },
        ["mj-kurume-funky"],
      ),
      buildCondition(
        "beam-hikari-main-core",
        "全日：最重要強条件日の最上位",
        "43件 / 105.70% / RB1/328.9",
        {
          rankMax: 1,
          requiredFlags: ["beamHikariFunkyHistoryReady", "beamHikariFunkyCore"],
        },
        ["beam-hikari-funky"],
      ),
      buildCondition(
        "beam-hikari-main-top1-score90",
        "全日：1位＋90点以上",
        "115件 / 103.10% / RB1/342.7",
        {
          rankMax: 1,
          minScore: 90,
          requiredFlags: ["beamHikariFunkyHistoryReady"],
        },
        ["beam-hikari-funky"],
      ),
      buildCondition(
        "beam-hikari-main-score90",
        "全日：90点以上",
        "137件 / 103.15% / RB1/334.5",
        {
          minScore: 90,
          requiredFlags: ["beamHikariFunkyHistoryReady"],
        },
        ["beam-hikari-funky"],
      ),
      buildCondition(
        "beam-hikari-main-gap15",
        "全日：1位＋次点差15点以上",
        "127件 / 102.64% / RB1/336.5",
        {
          rankMax: 1,
          minNextGap: 15,
          requiredFlags: ["beamHikariFunkyHistoryReady"],
        },
        ["beam-hikari-funky"],
      ),
      buildCondition(
        "beam-hikari-main-gap12",
        "全日：1位＋次点差12点以上",
        "158件 / 102.62% / RB1/342.7",
        {
          rankMax: 1,
          minNextGap: 12,
          requiredFlags: ["beamHikariFunkyHistoryReady"],
        },
        ["beam-hikari-funky"],
      ),
      buildCondition(
        "beam-hikari-normal-core",
        "最重要通常条件日の最上位",
        "45件 / 105.54% / RB1/324.3",
        {
          rankMax: 1,
          requiredFlags: ["beamHikariFunkyNormalHistoryReady", "beamHikariFunkyNormalCore"],
        },
        ["beam-hikari-funky-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-score90",
        "1位＋90点以上",
        "82件 / 104.34% / RB1/326",
        {
          rankMax: 1,
          minScore: 90,
          requiredFlags: ["beamHikariFunkyNormalHistoryReady"],
        },
        ["beam-hikari-funky-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-score80",
        "1位＋80点以上",
        "166件 / 102.83% / RB1/342.2",
        {
          rankMax: 1,
          minScore: 80,
          requiredFlags: ["beamHikariFunkyNormalHistoryReady"],
        },
        ["beam-hikari-funky-normal"],
      ),
      buildCondition(
        "beam-hikari-event-score90",
        "1位＋90点以上",
        "85件 / 102.76% / RB1/329",
        {
          rankMax: 1,
          minScore: 90,
          requiredFlags: ["beamHikariFunkyEventHistoryReady"],
        },
        ["beam-hikari-funky-event"],
      ),
      buildCondition(
        "beam-hikari-event-gap10",
        "1位＋次点差10点以上",
        "191件 / 102.89% / RB1/341.3",
        {
          rankMax: 1,
          minNextGap: 10,
          requiredFlags: ["beamHikariFunkyEventHistoryReady"],
        },
        ["beam-hikari-funky-event"],
      ),
      buildCondition(
        "beam-hikari-event-score80-gap10-boost",
        "1位＋80点以上＋次点差10点以上＋強化2個以上＋危険1以下",
        "112件 / 103.49% / RB1/337",
        {
          rankMax: 1,
          minScore: 80,
          minNextGap: 10,
          minBoost: 2,
          maxDanger: 1,
          requiredFlags: ["beamHikariFunkyEventHistoryReady"],
        },
        ["beam-hikari-funky-event"],
      ),
    ],
  },
  {
    machineKey: "happy",
    machineNames: [
      "ハッピージャグラーＶＩＩＩ",
      "ハッピージャグラーVIII",
      "ハッピージャグラーＶ",
      "ハッピージャグラーV",
      "ハッピージャグラー",
    ],
    logicKey: "apark-yakatabaru-happy",
    logicName: "ハッピー屋形原式",
    logics: [
      buildLogicVariant("apark-yakatabaru-happy", "ハッピー屋形原式", "apark-yakatabaru-main"),
      buildLogicVariant("beam-hikari-happy", "ハッピービームヒカリ式", "beam-hikari-main"),
      buildLogicVariant("beam-hikari-happy-normal", "ハッピービームヒカリ通常日式", "beam-hikari-normal-main"),
      buildLogicVariant("beam-hikari-happy-event", "ハッピービームヒカリイベント日式", "beam-hikari-event-main"),
    ],
    profile: "juggler",
    defaultConditionSuffix: "apark-yakatabaru-main",
    conditions: [
      buildCondition(
        "beam-hikari-main",
        "1位＋60点以上＋強化1個以上＋危険0",
        "259件 / 101.98% / RB1/329.9",
        {
          rankMax: 1,
          minScore: 60,
          minBoost: 1,
          maxDanger: 0,
          requiredFlags: ["beamHikariHappyMainHistoryReady"],
        },
        ["beam-hikari-happy"],
      ),
      buildCondition(
        "beam-hikari-score80",
        "80点以上",
        "72件 / 100.57% / RB1/319.8",
        {
          minScore: 80,
          requiredFlags: ["beamHikariHappyMainHistoryReady"],
        },
        ["beam-hikari-happy"],
      ),
      buildCondition(
        "beam-hikari-rank1-previous-weak",
        "1位＋前日弱さ強化",
        "250件 / 101.71% / RB1/322.7",
        {
          rankMax: 1,
          requiredFlags: ["beamHikariHappyMainHistoryReady", "beamHikariHappyPreviousWeak"],
        },
        ["beam-hikari-happy"],
      ),
      buildCondition(
        "beam-hikari-rank1-sink",
        "1位＋沈み強化",
        "47件 / 102.22% / RB1/310.2",
        {
          rankMax: 1,
          requiredFlags: ["beamHikariHappyMainHistoryReady", "beamHikariHappyMainSinkBoost"],
        },
        ["beam-hikari-happy"],
      ),
      buildCondition(
        "beam-hikari-normal-main",
        "1位＋70点以上＋次点差25点以上",
        "26件 / 100.17% / RB1/350",
        {
          rankMax: 1,
          minScore: 70,
          minNextGap: 25,
          requiredFlags: ["beamHikariHappyNormalHistoryReady"],
        },
        ["beam-hikari-happy-normal"],
      ),
      buildCondition(
        "beam-hikari-event-main",
        "65点以上＋イベントローテ強化",
        "167件 / 101.34% / RB1/320.9",
        {
          minScore: 65,
          requiredFlags: ["beamHikariHappyEventHistoryReady", "beamHikariHappyEventRotationBoost"],
        },
        ["beam-hikari-happy-event"],
      ),
      buildCondition(
        "beam-hikari-event-unpaid-rank1",
        "1位＋未返済強化",
        "42件 / 101.58% / RB1/310",
        {
          rankMax: 1,
          minScore: 60,
          requiredFlags: ["beamHikariHappyEventHistoryReady", "beamHikariHappyUnpaid"],
        },
        ["beam-hikari-happy-event"],
      ),
      buildCondition(
        "beam-hikari-event-unpaid-rank2",
        "上位2＋未返済強化",
        "66件 / 100.60% / RB1/325.5",
        {
          rankMax: 2,
          minScore: 60,
          requiredFlags: ["beamHikariHappyEventHistoryReady", "beamHikariHappyUnpaid"],
        },
        ["beam-hikari-happy-event"],
      ),
      buildCondition(
        "beam-hikari-event-unpaid-score80",
        "80点以上＋未返済強化",
        "35件 / 101.70% / RB1/310.7",
        {
          minScore: 80,
          requiredFlags: ["beamHikariHappyEventHistoryReady", "beamHikariHappyUnpaid"],
        },
        ["beam-hikari-happy-event"],
      ),
      buildCondition(
        "beam-hikari-event-rotation-safe",
        "1位＋イベントローテ強化＋危険0",
        "84件 / 100.66% / RB1/317.6",
        {
          rankMax: 1,
          maxDanger: 0,
          requiredFlags: ["beamHikariHappyEventHistoryReady", "beamHikariHappyEventRotationBoost"],
        },
        ["beam-hikari-happy-event"],
      ),
      buildCondition(
        "apark-yakatabaru-main",
        "1位＋70点以上＋次点差25点以上",
        "48件 / 106.34% / RB1/285.1",
        {
          rankMax: 1,
          minScore: 70,
          minNextGap: 25,
          requiredFlags: ["yakatabaruHappyHistoryReady"],
        },
        ["apark-yakatabaru-happy"],
      ),
      buildCondition(
        "apark-yakatabaru-strong",
        "1位＋70点以上＋次点差30点以上",
        "38件 / 107.03% / RB1/287.3",
        {
          rankMax: 1,
          minScore: 70,
          minNextGap: 30,
          requiredFlags: ["yakatabaruHappyHistoryReady"],
        },
        ["apark-yakatabaru-happy"],
      ),
      buildCondition(
        "apark-yakatabaru-daily-top",
        "毎日1位",
        "329件 / 101.84% / RB1/313.6",
        {
          rankMax: 1,
          requiredFlags: ["yakatabaruHappyHistoryReady"],
        },
        ["apark-yakatabaru-happy"],
      ),
      buildCondition(
        "apark-yakatabaru-score70",
        "70点以上",
        "127件 / 103.39% / RB1/300.9",
        {
          minScore: 70,
          requiredFlags: ["yakatabaruHappyHistoryReady"],
        },
        ["apark-yakatabaru-happy"],
      ),
      buildCondition(
        "apark-yakatabaru-top1-score70",
        "1位＋70点以上",
        "101件 / 103.90% / RB1/297.7",
        {
          rankMax: 1,
          minScore: 70,
          requiredFlags: ["yakatabaruHappyHistoryReady"],
        },
        ["apark-yakatabaru-happy"],
      ),
      buildCondition(
        "apark-yakatabaru-gap25",
        "1位＋次点差25点以上",
        "81件 / 104.61% / RB1/301.4",
        {
          rankMax: 1,
          minNextGap: 25,
          requiredFlags: ["yakatabaruHappyHistoryReady"],
        },
        ["apark-yakatabaru-happy"],
      ),
      buildCondition(
        "apark-yakatabaru-gap30",
        "1位＋次点差30点以上",
        "59件 / 105.04% / RB1/300.9",
        {
          rankMax: 1,
          minNextGap: 30,
          requiredFlags: ["yakatabaruHappyHistoryReady"],
        },
        ["apark-yakatabaru-happy"],
      ),
      buildCondition(
        "apark-yakatabaru-strong-sink",
        "1位＋強沈み",
        "48件 / 104.38% / RB1/283.3",
        {
          rankMax: 1,
          requiredFlags: ["yakatabaruHappyHistoryReady", "yakatabaruHappyStrongSink"],
        },
        ["apark-yakatabaru-happy"],
      ),
      buildCondition(
        "apark-yakatabaru-unpaid",
        "1位＋返済未完了",
        "133件 / 102.51% / RB1/311.8",
        {
          rankMax: 1,
          requiredFlags: ["yakatabaruHappyHistoryReady", "yakatabaruHappyUnpaid"],
        },
        ["apark-yakatabaru-happy"],
      ),
      buildCondition(
        "apark-yakatabaru-angle",
        "1位＋角度強",
        "204件 / 102.22% / RB1/316.0",
        {
          rankMax: 1,
          requiredFlags: ["yakatabaruHappyHistoryReady", "yakatabaruHappyAngleStrong"],
        },
        ["apark-yakatabaru-happy"],
      ),
      buildCondition(
        "apark-yakatabaru-bonus",
        "1位＋ボナ弱",
        "257件 / 101.98% / RB1/315.3",
        {
          rankMax: 1,
          requiredFlags: ["yakatabaruHappyHistoryReady", "yakatabaruHappyBonusWeak"],
        },
        ["apark-yakatabaru-happy"],
      ),
    ],
  },
  {
    machineKey: "ultra-miracle",
    machineNames: ["ウルトラミラクルジャグラー"],
    logicKey: "apark-yakatabaru-ultra-miracle",
    logicName: "ウルトラ屋形原式",
    logics: [
      buildLogicVariant("beam-hikari-ultra", "ウルトラビームヒカリ式", "beam-hikari-main"),
      buildLogicVariant("beam-hikari-ultra-normal", "ウルトラビームヒカリ通常日式", "beam-hikari-normal-main"),
      buildLogicVariant("beam-hikari-ultra-event", "ウルトラビームヒカリイベント日式", "beam-hikari-event-main"),
      buildLogicVariant("apark-yakatabaru-ultra-miracle", "ウルトラ屋形原式", "apark-yakatabaru-main"),
    ],
    profile: "juggler",
    defaultConditionSuffix: "apark-yakatabaru-main",
    conditions: [
      buildCondition(
        "beam-hikari-main",
        "1位＋70点以上＋強化2＋危険1以下",
        "4件 / 110.35% / RB1/384.8",
        {
          rankMax: 1,
          minScore: 70,
          minBoost: 2,
          maxDanger: 1,
          requiredFlags: ["beamHikariUltraMainHistoryReady"],
        },
        ["beam-hikari-ultra"],
      ),
      buildCondition(
        "beam-hikari-score70-boost2-safe",
        "70点以上＋強化2＋危険1以下",
        "4件 / 110.35% / RB1/384.8",
        {
          minScore: 70,
          minBoost: 2,
          maxDanger: 1,
          requiredFlags: ["beamHikariUltraMainHistoryReady"],
        },
        ["beam-hikari-ultra"],
      ),
      buildCondition(
        "beam-hikari-rank1-boost2",
        "1位＋強化2以上",
        "23件 / 103.29% / RB1/365.5",
        {
          rankMax: 1,
          minBoost: 2,
          requiredFlags: ["beamHikariUltraMainHistoryReady"],
        },
        ["beam-hikari-ultra"],
      ),
      buildCondition(
        "beam-hikari-normal-main",
        "1位＋70点以上＋強化2＋危険1以下",
        "9件 / 106.10% / RB1/355",
        {
          rankMax: 1,
          minScore: 70,
          minBoost: 2,
          maxDanger: 1,
          requiredFlags: ["beamHikariUltraNormalHistoryReady"],
        },
        ["beam-hikari-ultra-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-boost2",
        "1位＋強化2以上",
        "30件 / 103.72% / RB1/390",
        {
          rankMax: 1,
          minBoost: 2,
          requiredFlags: ["beamHikariUltraNormalHistoryReady"],
        },
        ["beam-hikari-ultra-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-rank1-score70",
        "1位＋70点以上",
        "11件 / 105.29% / RB1/357.7",
        {
          rankMax: 1,
          minScore: 70,
          requiredFlags: ["beamHikariUltraNormalHistoryReady"],
        },
        ["beam-hikari-ultra-normal"],
      ),
      buildCondition(
        "beam-hikari-event-main",
        "1位＋70点以上＋強化2＋危険1以下",
        "30件 / 103.10% / RB1/364.4",
        {
          rankMax: 1,
          minScore: 70,
          minBoost: 2,
          maxDanger: 1,
          requiredFlags: ["beamHikariUltraEventHistoryReady"],
        },
        ["beam-hikari-ultra-event"],
      ),
      buildCondition(
        "beam-hikari-event-score70",
        "70点以上",
        "35件 / 103.84% / RB1/371.6",
        {
          minScore: 70,
          requiredFlags: ["beamHikariUltraEventHistoryReady"],
        },
        ["beam-hikari-ultra-event"],
      ),
      buildCondition(
        "beam-hikari-event-rank1-score70",
        "1位＋70点以上",
        "31件 / 103.35% / RB1/368.1",
        {
          rankMax: 1,
          minScore: 70,
          requiredFlags: ["beamHikariUltraEventHistoryReady"],
        },
        ["beam-hikari-ultra-event"],
      ),
      buildCondition(
        "apark-yakatabaru-main",
        "70点以上＋角度強化",
        "33件 / 105.41% / RB1/304.2",
        {
          minScore: 70,
          requiredFlags: ["yakatabaruUltraHistoryReady", "yakatabaruUltraAngleBoost"],
        },
        ["apark-yakatabaru-ultra-miracle"],
      ),
      buildCondition(
        "apark-yakatabaru-strong",
        "70点以上＋角度強化＋次点差20点以上",
        "20件 / 105.88% / RB1/294.4",
        {
          minScore: 70,
          minNextGap: 20,
          requiredFlags: ["yakatabaruUltraHistoryReady", "yakatabaruUltraAngleBoost"],
        },
        ["apark-yakatabaru-ultra-miracle"],
      ),
      buildCondition(
        "apark-yakatabaru-score70",
        "70点以上",
        "39件 / 104.11% / RB1/314.3",
        {
          minScore: 70,
          requiredFlags: ["yakatabaruUltraHistoryReady"],
        },
        ["apark-yakatabaru-ultra-miracle"],
      ),
      buildCondition(
        "apark-yakatabaru-top1-score70",
        "1位＋70点以上",
        "34件 / 105.12% / RB1/306.9",
        {
          rankMax: 1,
          minScore: 70,
          requiredFlags: ["yakatabaruUltraHistoryReady"],
        },
        ["apark-yakatabaru-ultra-miracle"],
      ),
      buildCondition(
        "apark-yakatabaru-top1-score60",
        "1位＋60点以上",
        "85件 / 102.73% / RB1/321.7",
        {
          rankMax: 1,
          minScore: 60,
          requiredFlags: ["yakatabaruUltraHistoryReady"],
        },
        ["apark-yakatabaru-ultra-miracle"],
      ),
      buildCondition(
        "apark-yakatabaru-daily-top",
        "毎日1位",
        "329件 / 101.16% / RB1/343.6",
        {
          rankMax: 1,
          requiredFlags: ["yakatabaruUltraHistoryReady"],
        },
        ["apark-yakatabaru-ultra-miracle"],
      ),
    ],
  },
  {
    machineKey: "my",
    machineNames: ["マイジャグラーV", "マイジャグラーⅤ", "マイジャグラー"],
    logicKey: "apark-my",
    logicName: "マイジャグ春日式",
    logics: [
      buildLogicVariant("apark-my", "マイジャグ春日式", "main"),
      buildLogicVariant("apark-yakatabaru-my", "マイジャグ屋形原式", "apark-yakatabaru-main"),
      buildLogicVariant("mj-kurume-my", "マイジャグMJ久留米式", "mj-kurume-main"),
      buildLogicVariant("beam-hikari-my", "マイジャグビームヒカリ式", "beam-hikari-main"),
      buildLogicVariant("beam-hikari-my-normal", "マイジャグビームヒカリ通常日式", "beam-hikari-normal-core"),
      buildLogicVariant("beam-hikari-my-event", "マイジャグビームヒカリイベント日式", "beam-hikari-event-rank1"),
    ],
    profile: "juggler",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "1位＋3連敗以上＋直近5日-3500枚以下＋次点差3点以上",
        "82件 / 105.22% / RB1/271.3",
        {
          rankMax: 1,
          minNextGap: 3,
          requiredFlags: ["myHistoryReady", "myLosingStreak3", "myRecentFiveLoss3500"],
        },
        ["apark-my"],
      ),
      buildCondition(
        "apark-yakatabaru-main",
        "90点以上＋直近3日G数9000〜16000",
        "109件 / 104.39% / RB1/291.4",
        {
          minScore: 90,
          requiredFlags: ["yakatabaruMyHistoryReady", "yakatabaruMyGamesCore"],
        },
        ["apark-yakatabaru-my"],
      ),
      buildCondition(
        "apark-yakatabaru-strict",
        "90点以上＋直近3日G数9000〜16000＋直近7日高内容0回",
        "84件 / 104.75% / RB1/287.5",
        {
          minScore: 90,
          requiredFlags: ["yakatabaruMyHistoryReady", "yakatabaruMyGamesCore", "yakatabaruMyNoRecentHigh"],
        },
        ["apark-yakatabaru-my"],
      ),
      buildCondition(
        "apark-yakatabaru-score90",
        "90点以上",
        "155件 / 103.45% / RB1/295.0",
        {
          minScore: 90,
          requiredFlags: ["yakatabaruMyHistoryReady"],
        },
        ["apark-yakatabaru-my"],
      ),
      buildCondition(
        "apark-yakatabaru-score80",
        "80点以上",
        "397件 / 103.17% / RB1/295.2",
        {
          minScore: 80,
          requiredFlags: ["yakatabaruMyHistoryReady"],
        },
        ["apark-yakatabaru-my"],
      ),
      buildCondition(
        "apark-yakatabaru-score70",
        "70点以上",
        "676件 / 102.82% / RB1/300.3",
        {
          minScore: 70,
          requiredFlags: ["yakatabaruMyHistoryReady"],
        },
        ["apark-yakatabaru-my"],
      ),
      buildCondition(
        "apark-yakatabaru-rank3-danger0",
        "上位3位以内＋70点以上＋危険0",
        "597件 / 102.81% / RB1/297.9",
        {
          rankMax: 3,
          minScore: 70,
          maxDanger: 0,
          requiredFlags: ["yakatabaruMyHistoryReady"],
        },
        ["apark-yakatabaru-my"],
      ),
      buildCondition(
        "mj-kurume-main",
        "1位＋次点差8点以上＋低稼働・危険条件なし",
        "80件 / 104.38% / RB1/317.7",
        {
          rankMax: 1,
          minNextGap: 8,
          maxDanger: 0,
          requiredFlags: ["kurumeMyHistoryReady", "kurumeMyNoLowUsage"],
        },
        ["mj-kurume-my"],
      ),
      buildCondition(
        "mj-kurume-rank80",
        "1位＋80点以上＋次点差8点以上",
        "48件 / 104.38% / RB1/323.0",
        {
          rankMax: 1,
          minScore: 80,
          minNextGap: 8,
          requiredFlags: ["kurumeMyHistoryReady"],
        },
        ["mj-kurume-my"],
      ),
      buildCondition(
        "mj-kurume-strong",
        "1位＋80点以上＋次点差20点以上",
        "17件 / 105.99% / RB1/295.6",
        {
          rankMax: 1,
          minScore: 80,
          minNextGap: 20,
          requiredFlags: ["kurumeMyHistoryReady"],
        },
        ["mj-kurume-my"],
      ),
      buildCondition(
        "mj-kurume-score80",
        "80点以上",
        "115件 / 102.71% / RB1/329.0",
        {
          minScore: 80,
          requiredFlags: ["kurumeMyHistoryReady"],
        },
        ["mj-kurume-my"],
      ),
      buildCondition(
        "mj-kurume-score65",
        "65点以上",
        "321件 / 102.19% / RB1/326.2",
        {
          minScore: 65,
          requiredFlags: ["kurumeMyHistoryReady"],
        },
        ["mj-kurume-my"],
      ),
      buildCondition(
        "beam-hikari-main",
        "1位＋2連敗＋次点差10点以上",
        "92件 / 104.73% / RB1/292",
        {
          rankMax: 1,
          minNextGap: 10,
          requiredFlags: ["beamHikariMyMainHistoryReady", "beamHikariMyTwoLoss"],
        },
        ["beam-hikari-my"],
      ),
      buildCondition(
        "beam-hikari-rank1-gap10",
        "1位＋次点差10点以上",
        "127件 / 104.32% / RB1/297",
        {
          rankMax: 1,
          minNextGap: 10,
          requiredFlags: ["beamHikariMyMainHistoryReady"],
        },
        ["beam-hikari-my"],
      ),
      buildCondition(
        "beam-hikari-normal-core",
        "1位＋2連敗＋14日沈み強",
        "69件 / 104.55% / RB1/286.4",
        {
          rankMax: 1,
          requiredFlags: [
            "beamHikariMyNormalHistoryReady",
            "beamHikariMyTwoLoss",
            "beamHikariMyNormalFourteenSinkStrong",
          ],
        },
        ["beam-hikari-my-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-core-safe",
        "1位＋2連敗＋14日沈み強＋危険0",
        "43件 / 104.86% / RB1/292.1",
        {
          rankMax: 1,
          maxDanger: 0,
          requiredFlags: [
            "beamHikariMyNormalHistoryReady",
            "beamHikariMyTwoLoss",
            "beamHikariMyNormalFourteenSinkStrong",
          ],
        },
        ["beam-hikari-my-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-gap20",
        "1位＋次点差20点以上",
        "90件 / 104.16% / RB1/294.2",
        {
          rankMax: 1,
          minNextGap: 20,
          requiredFlags: ["beamHikariMyNormalHistoryReady"],
        },
        ["beam-hikari-my-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-score90",
        "90点以上",
        "137件 / 103.32% / RB1/298.1",
        {
          minScore: 90,
          requiredFlags: ["beamHikariMyNormalHistoryReady"],
        },
        ["beam-hikari-my-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-score85",
        "85点以上",
        "199件 / 103.21% / RB1/302.6",
        {
          minScore: 85,
          requiredFlags: ["beamHikariMyNormalHistoryReady"],
        },
        ["beam-hikari-my-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-rank1-two-loss-gap15",
        "1位＋2連敗＋次点差15点以上",
        "97件 / 103.88% / RB1/302",
        {
          rankMax: 1,
          minNextGap: 15,
          requiredFlags: ["beamHikariMyNormalHistoryReady", "beamHikariMyTwoLoss"],
        },
        ["beam-hikari-my-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-rank1-two-loss-weak",
        "1位＋2連敗＋弱内容",
        "184件 / 103% / RB1/301.5",
        {
          rankMax: 1,
          requiredFlags: [
            "beamHikariMyNormalHistoryReady",
            "beamHikariMyTwoLoss",
            "beamHikariMyNormalWeakContent",
          ],
        },
        ["beam-hikari-my-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-score80",
        "80点以上",
        "270件 / 103% / RB1/305.2",
        {
          minScore: 80,
          requiredFlags: ["beamHikariMyNormalHistoryReady"],
        },
        ["beam-hikari-my-normal"],
      ),
      buildCondition(
        "beam-hikari-event-rank1",
        "1位",
        "347件 / 102.22% / RB1/317.8",
        {
          rankMax: 1,
          requiredFlags: ["beamHikariMyEventHistoryReady"],
        },
        ["beam-hikari-my-event"],
      ),
      buildCondition(
        "beam-hikari-event-gap10",
        "1位＋次点差10点以上",
        "124件 / 101.61% / RB1/325.4",
        {
          rankMax: 1,
          minNextGap: 10,
          requiredFlags: ["beamHikariMyEventHistoryReady"],
        },
        ["beam-hikari-my-event"],
      ),
      buildCondition(
        "beam-hikari-event-reuse",
        "1位＋再投入筋あり",
        "183件 / 102.11% / RB1/322.9",
        {
          rankMax: 1,
          requiredFlags: ["beamHikariMyEventHistoryReady", "beamHikariMyEventReuseLine"],
        },
        ["beam-hikari-my-event"],
      ),
      buildCondition(
        "beam-hikari-event-reuse-safe",
        "1位＋再投入筋あり＋危険0",
        "127件 / 101.35% / RB1/324.4",
        {
          rankMax: 1,
          maxDanger: 0,
          requiredFlags: ["beamHikariMyEventHistoryReady", "beamHikariMyEventReuseLine"],
        },
        ["beam-hikari-my-event"],
      ),
      buildCondition(
        "beam-hikari-event-score90",
        "90点以上",
        "83件 / 102.11% / RB1/324.1",
        {
          minScore: 90,
          requiredFlags: ["beamHikariMyEventHistoryReady"],
        },
        ["beam-hikari-my-event"],
      ),
      buildCondition(
        "beam-hikari-event-score85",
        "85点以上",
        "197件 / 102.71% / RB1/310.1",
        {
          minScore: 85,
          requiredFlags: ["beamHikariMyEventHistoryReady"],
        },
        ["beam-hikari-my-event"],
      ),
      buildCondition(
        "beam-hikari-event-top2",
        "上位2台",
        "694件 / 102.17% / RB1/318.1",
        {
          rankMax: 2,
          requiredFlags: ["beamHikariMyEventHistoryReady"],
        },
        ["beam-hikari-my-event"],
      ),
      buildCondition(
        "beam-hikari-event-score70",
        "70点以上",
        "872件 / 102.06% / RB1/318.8",
        {
          minScore: 70,
          requiredFlags: ["beamHikariMyEventHistoryReady"],
        },
        ["beam-hikari-my-event"],
      ),
    ],
  },
  {
    machineKey: "mister",
    machineNames: ["ミスタージャグラー"],
    logicKey: "apark-mister",
    logicName: "ミスター春日式",
    profile: "juggler",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "1位＋60点以上＋次点差10点以上＋危険少",
        "99件 / 104.4% / RB1/293.8",
        {
          rankMax: 1,
          minScore: 60,
          minNextGap: 10,
          maxDanger: 1,
          requiredFlags: ["misterHistoryReady"],
        },
      ),
    ],
  },
  {
    machineKey: "okidoki-black",
    machineNames: ["沖ドキ！BLACK", "沖ドキ!BLACK", "沖ドキ！ＢＬＡＣＫ"],
    logicKey: "apark-okidoki-black",
    logicName: "沖ドキBLACK春日式",
    profile: "okidoki",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "80点以上",
        "60件 / 104.5%",
        {
          minScore: 80,
          maxDanger: 2,
        },
      ),
    ],
  },
  {
    machineKey: "okidoki-gold",
    machineNames: ["沖ドキ！ＧＯＬＤ", "沖ドキ！ＧＯＬＤ-30", "沖ドキ!GOLD", "沖ドキ!GOLD-30"],
    logicKey: "apark-okidoki-gold",
    logicName: "沖ドキGOLD春日式",
    profile: "okidoki",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "1位＋次点差12点以上＋強化2つ以上＋危険なし",
        "105件 / 103.8%",
        {
          rankMax: 1,
          minNextGap: 12,
          minBoost: 2,
          maxDanger: 0,
        },
      ),
    ],
  },
  {
    machineKey: "shin-hanabi",
    machineNames: ["新ハナビ"],
    logicKey: "apark-shin-hanabi",
    logicName: "新ハナビ春日式",
    profile: "hanabi",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "65点以上＋大見せ場3回以上",
        "32件 / 103.75% / RB1/318.9",
        {
          minScore: 65,
          requiredFlags: ["bigShow3"],
        },
      ),
    ],
  },
];

const DEFINITIONS_BY_MACHINE_KEY = new Map(
  MACHINE_EVALUATION_DEFINITIONS.map((definition) => [definition.machineKey, definition]),
);

const DEFINITIONS_BY_LOGIC_KEY = new Map(
  MACHINE_EVALUATION_DEFINITIONS.flatMap((definition) =>
    listDefinitionLogics(definition).map((logic) => [logic.key, definition]),
  ),
);

function buildMachineNameDefinitionEntries() {
  return MACHINE_EVALUATION_DEFINITIONS.flatMap((definition) =>
    definition.machineNames.map((machineName) => [
      normalizeMachineNameText(machineName),
      definition,
    ]),
  );
}

const DEFINITION_BY_MACHINE_NAME = new Map(buildMachineNameDefinitionEntries());

function findMachineDefinition(machineName) {
  return DEFINITION_BY_MACHINE_NAME.get(normalizeMachineNameText(machineName)) ?? null;
}

function buildConditionKey(definition, condition) {
  return `${definition.machineKey}-${condition.keySuffix}`;
}

function findConditionDefinition(definition, conditionKey, logicKey = "") {
  if (!definition) {
    return null;
  }
  const normalizedConditionKey = normalizeText(conditionKey);
  return (
    listConditionDefinitions(definition, logicKey).find(
      (condition) => buildConditionKey(definition, condition) === normalizedConditionKey,
    ) ??
    null
  );
}

function getDefaultSetting(definition, storeName) {
  if (!definition) {
    return {
      logicKey: "",
      conditionKey: "",
    };
  }

  let defaultLogic = null;
  if (isMjArenaKurumeStore(storeName) && definition.machineKey === "aim") {
    defaultLogic = findLogicDefinition(definition, "mj-kurume-aim");
  } else if (isMjArenaKurumeStore(storeName) && definition.machineKey === "gogo") {
    defaultLogic = findLogicDefinition(definition, "mj-kurume-gogo");
  } else if (isMjArenaKurumeStore(storeName) && definition.machineKey === "neo-aim") {
    defaultLogic = findLogicDefinition(definition, "mj-kurume-neo-aim");
  } else if (isMjArenaKurumeStore(storeName) && definition.machineKey === "funky") {
    defaultLogic = findLogicDefinition(definition, "mj-kurume-funky");
  } else if (isMjArenaKurumeStore(storeName) && definition.machineKey === "my") {
    defaultLogic = findLogicDefinition(definition, "mj-kurume-my");
  } else if (isMjArenaKurumeStore(storeName) && definition.machineKey === "girls") {
    defaultLogic = findLogicDefinition(definition, "mj-kurume-girls");
  } else if (isAmuseAsakusaStore(storeName) && definition.machineKey === "neo-aim") {
    defaultLogic = findLogicDefinition(definition, "amuse-asakusa-neo-aim");
  } else if (isGogoArenaTenjinStore(storeName) && definition.machineKey === "neo-aim") {
    defaultLogic = findLogicDefinition(definition, "gogo-tenjin-neo-aim");
  } else if (isHinodeOnojoStore(storeName) && definition.machineKey === "neo-aim") {
    defaultLogic = findLogicDefinition(definition, "hinode-onojo-neo-aim");
  } else if (isSuperDstationChikushinoStore(storeName) && definition.machineKey === "neo-aim") {
    defaultLogic = findLogicDefinition(definition, "chikushino-neo-aim");
  } else if (isEspaceUenoStore(storeName) && definition.machineKey === "neo-aim") {
    defaultLogic = findLogicDefinition(definition, "espace-ueno-neo-aim");
  } else if (isMesseMinamisenjuStore(storeName) && definition.machineKey === "neo-aim") {
    defaultLogic = findLogicDefinition(definition, "messe-minamisenju-neo-aim");
  } else if (isKintokiKamataStore(storeName) && definition.machineKey === "neo-aim") {
    defaultLogic = findLogicDefinition(definition, "kintoki-kamata-neo-aim");
  } else if (isBeamHikariStore(storeName) && definition.machineKey === "neo-aim") {
    defaultLogic = findLogicDefinition(definition, "beam-hikari-neo-aim");
  } else if (isBeamHikariStore(storeName) && definition.machineKey === "funky") {
    defaultLogic = findLogicDefinition(definition, "beam-hikari-funky");
  } else if (isBeamHikariStore(storeName) && definition.machineKey === "gogo") {
    defaultLogic = findLogicDefinition(definition, "beam-hikari-gogo");
  } else if (isBeamHikariStore(storeName) && definition.machineKey === "my") {
    defaultLogic = findLogicDefinition(definition, "beam-hikari-my");
  } else if (isBeamHikariStore(storeName) && definition.machineKey === "girls") {
    defaultLogic = findLogicDefinition(definition, "beam-hikari-girls");
  } else if (isBeamHikariStore(storeName) && definition.machineKey === "happy") {
    defaultLogic = findLogicDefinition(definition, "beam-hikari-happy");
  } else if (isBeamHikariStore(storeName) && definition.machineKey === "ultra-miracle") {
    defaultLogic = findLogicDefinition(definition, "beam-hikari-ultra");
  } else if (isBeamHikariStore(storeName) && definition.machineKey === "monkey") {
    defaultLogic = findLogicDefinition(definition, "beam-hikari-monkey");
  } else if (isBeamHikariStore(storeName) && definition.machineKey === "hokuto-tensei") {
    defaultLogic = findLogicDefinition(definition, "beam-hikari-hokuto-tensei");
  } else if (isBeamHikariStore(storeName) && definition.machineKey === "hokuto-base") {
    defaultLogic = findLogicDefinition(definition, "beam-hikari-hokuto-base");
  } else if (isAparkYakatabaruStore(storeName) && definition.machineKey === "neo-aim") {
    defaultLogic = findLogicDefinition(definition, "apark-yakatabaru-neo-aim");
  } else if (isAparkYakatabaruStore(storeName) && definition.machineKey === "my") {
    defaultLogic = findLogicDefinition(definition, "apark-yakatabaru-my");
  } else if (isAparkYakatabaruStore(storeName) && definition.machineKey === "funky") {
    defaultLogic = findLogicDefinition(definition, "apark-yakatabaru-funky");
  } else if (isAparkYakatabaruStore(storeName) && definition.machineKey === "happy") {
    defaultLogic = findLogicDefinition(definition, "apark-yakatabaru-happy");
  } else if (isAparkYakatabaruStore(storeName) && definition.machineKey === "ultra-miracle") {
    defaultLogic = findLogicDefinition(definition, "apark-yakatabaru-ultra-miracle");
  } else if (isAparkKasugaStore(storeName)) {
    defaultLogic = findLogicDefinition(definition, definition.logicKey);
  }

  if (!defaultLogic) {
    return {
      logicKey: "",
      conditionKey: "",
    };
  }

  const defaultCondition =
    listConditionDefinitions(definition, defaultLogic.key).find(
      (condition) => condition.keySuffix === defaultLogic.defaultConditionSuffix,
    ) ??
    listConditionDefinitions(definition, defaultLogic.key)[0] ??
    null;

  return {
    logicKey: defaultLogic.key,
    conditionKey: defaultCondition ? buildConditionKey(definition, defaultCondition) : "",
  };
}

function normalizeCookieSettings(value) {
  if (!value || typeof value !== "object") {
    return {};
  }
  const machines = value.machines ?? value.m ?? {};
  if (!machines || typeof machines !== "object") {
    return {};
  }

  const normalizedMachines = {};
  for (const [machineKey, rawSetting] of Object.entries(machines)) {
    const definition = DEFINITIONS_BY_MACHINE_KEY.get(normalizeText(machineKey));
    if (!definition) {
      continue;
    }
    const rawValues = Array.isArray(rawSetting)
      ? rawSetting
      : [rawSetting?.logicKey, rawSetting?.conditionKey];
    const logicKey = normalizeText(rawValues[0]);
    const conditionKey = normalizeText(rawValues[1]);
    normalizedMachines[definition.machineKey] = [logicKey, conditionKey];
  }

  return {
    version: MACHINE_EVALUATION_COOKIE_VERSION,
    machines: normalizedMachines,
  };
}

export function getMachineEvaluationCookieName(storeId) {
  return `${MACHINE_EVALUATION_COOKIE_PREFIX}${normalizeText(storeId)}`;
}

export function encodeMachineEvaluationSettingsCookieValue(value) {
  const normalizedValue = normalizeCookieSettings(value);
  return encodeURIComponent(JSON.stringify(normalizedValue));
}

export function decodeMachineEvaluationSettingsCookieValue(value) {
  const text = normalizeText(value);
  if (!text) {
    return {};
  }

  try {
    return normalizeCookieSettings(JSON.parse(decodeURIComponent(text)));
  } catch {
    return {};
  }
}

export function normalizeMachineEvaluationBacktestMode(value) {
  const normalizedValue = normalizeText(value);
  return MACHINE_EVALUATION_BACKTEST_MODE_OPTIONS.some((option) => option.value === normalizedValue)
    ? normalizedValue
    : MACHINE_EVALUATION_BACKTEST_MODE_COMMON;
}

export function normalizeMachineEvaluationRankingMode(value) {
  const normalizedValue = normalizeText(value);
  return MACHINE_EVALUATION_RANKING_MODE_OPTIONS.some((option) => option.value === normalizedValue)
    ? normalizedValue
    : MACHINE_EVALUATION_RANKING_MODE_SHOW;
}

export function shouldShowMachineEvaluationInRanking(value) {
  return normalizeMachineEvaluationRankingMode(value) !== MACHINE_EVALUATION_RANKING_MODE_NONE;
}

function buildLogicOptions(definition) {
  return [
    { key: "", name: "未設定" },
    ...listDefinitionLogics(definition).map((logic) => ({
      key: logic.key,
      name: logic.name,
    })),
  ];
}

function buildConditionOptions(definition, logicKey = "") {
  return [
    {
      key: "",
      name: "未設定",
      backtestLabel: "",
    },
    ...listConditionDefinitions(definition, logicKey).map((condition) => ({
      key: buildConditionKey(definition, condition),
      name: condition.name,
      backtestLabel: condition.backtestLabel,
      backtestPayoutRate: condition.backtestPayoutRate,
      backtestRbDenominator: condition.backtestRbDenominator,
    })),
  ];
}

function normalizeSettingForDefinition(definition, setting) {
  if (!definition) {
    return {
      logicKey: "",
      conditionKey: "",
    };
  }

  const requestedLogicKey = normalizeText(setting?.logicKey);
  const requestedConditionKey = normalizeText(setting?.conditionKey);
  const logicDefinition = findLogicDefinition(definition, requestedLogicKey);
  const logicKey = logicDefinition ? logicDefinition.key : "";
  const defaultCondition =
    logicDefinition
      ? listConditionDefinitions(definition, logicDefinition.key).find(
          (condition) => condition.keySuffix === logicDefinition.defaultConditionSuffix,
        ) ?? null
      : null;
  const conditionKey = findConditionDefinition(definition, requestedConditionKey, logicKey)
    ? requestedConditionKey
    : defaultCondition
      ? buildConditionKey(definition, defaultCondition)
      : "";

  return {
    logicKey,
    conditionKey: logicKey ? conditionKey : "",
  };
}

export function buildStoreMachineEvaluationSettings(storeName, machineNames = [], cookieSettings = {}) {
  const cookieMachines = normalizeCookieSettings(cookieSettings).machines ?? {};
  const safeMachineNames = [
    ...new Set(
      (Array.isArray(machineNames) ? machineNames : [])
        .map((machineName) => normalizeText(machineName))
        .filter(Boolean),
    ),
  ];

  return safeMachineNames.map((machineName) => {
    const definition = findMachineDefinition(machineName);
    const machineKey = definition?.machineKey ?? `custom:${normalizeMachineNameText(machineName)}`;
    const defaultSetting = getDefaultSetting(definition, storeName);
    const rawOverride = definition ? cookieMachines[definition.machineKey] : null;
    const overrideSetting = rawOverride
      ? {
          logicKey: rawOverride[0],
          conditionKey: rawOverride[1],
        }
      : null;
    const defaultNormalizedSetting = normalizeSettingForDefinition(definition, defaultSetting);
    const overrideNormalizedSetting = overrideSetting
      ? normalizeSettingForDefinition(definition, overrideSetting)
      : null;
    const currentSetting = overrideNormalizedSetting?.logicKey
      ? overrideNormalizedSetting
      : defaultNormalizedSetting;
    const condition = findConditionDefinition(definition, currentSetting.conditionKey, currentSetting.logicKey);

    return {
      machineKey,
      machineName,
      hasDefinition: Boolean(definition),
      logicKey: currentSetting.logicKey,
      conditionKey: currentSetting.conditionKey,
      defaultLogicKey: defaultNormalizedSetting.logicKey,
      defaultConditionKey: defaultNormalizedSetting.conditionKey,
      logicOptions: buildLogicOptions(definition),
      conditionOptions: buildConditionOptions(definition, currentSetting.logicKey),
      selectedConditionLabel: condition?.name ?? "",
      selectedBacktestLabel: condition?.backtestLabel ?? "",
    };
  });
}

export function buildMachineEvaluationCookieOverrides(settingRows = []) {
  const machines = {};
  for (const row of Array.isArray(settingRows) ? settingRows : []) {
    const definition = DEFINITIONS_BY_MACHINE_KEY.get(normalizeText(row?.machineKey));
    if (!definition) {
      continue;
    }
    const logicKey = normalizeText(row?.logicKey);
    const conditionKey = normalizeText(row?.conditionKey);
    const defaultLogicKey = normalizeText(row?.defaultLogicKey);
    const defaultConditionKey = normalizeText(row?.defaultConditionKey);
    if (logicKey === defaultLogicKey && conditionKey === defaultConditionKey) {
      continue;
    }
    machines[definition.machineKey] = [logicKey, conditionKey];
  }

  return {
    version: MACHINE_EVALUATION_COOKIE_VERSION,
    machines,
  };
}

function hasAnyConfiguredSetting(settingRows) {
  return (Array.isArray(settingRows) ? settingRows : []).some((row) => row?.logicKey && row?.conditionKey);
}

function buildSettingByMachineKey(settingRows) {
  const settingByMachineKey = new Map();
  for (const row of Array.isArray(settingRows) ? settingRows : []) {
    const definition = DEFINITIONS_BY_MACHINE_KEY.get(normalizeText(row?.machineKey));
    if (!definition) {
      continue;
    }
    const setting = normalizeSettingForDefinition(definition, row);
    if (!setting.logicKey) {
      continue;
    }
    settingByMachineKey.set(definition.machineKey, setting);
  }
  return settingByMachineKey;
}

function buildFeatureState(metrics = {}) {
  const streak = readNumber(metrics.streak);
  const winningStreak = readNumber(metrics.winningStreak);
  const recentTwoNetTotal = readNumber(metrics.recentTwoNetTotal);
  const recentThreeNetTotal = readNumber(metrics.recentThreeNetTotal);
  const recentFiveNetTotal = readNumber(metrics.recentFiveNetTotal);
  const recentSixNetTotal = readNumber(metrics.recentSixNetTotal);
  const recentSevenNetTotal = readNumber(metrics.recentSevenNetTotal);
  const recentTenNetTotal = readNumber(metrics.recentTenNetTotal);
  const recentFourteenNetTotal = readNumber(metrics.recentFourteenNetTotal);
  const recentTwentyOneNetTotal = readNumber(metrics.recentTwentyOneNetTotal);
  const recentTwentyEightNetTotal = readNumber(metrics.recentTwentyEightNetTotal);
  const recentFortyTwoNetTotal = readNumber(metrics.recentFortyTwoNetTotal);
  const recentFiftySixNetTotal = readNumber(metrics.recentFiftySixNetTotal);
  const shortSevenSinkStayDays = readNumber(metrics.shortSevenSinkStayDays);
  const shortThreeSinkStayDays = readNumber(metrics.shortThreeSinkStayDays);
  const recentFourLossDays = readNumber(metrics.recentFourLossDays);
  const recentSevenLossDays = readNumber(metrics.recentSevenLossDays);
  const recentFourteenWinDays = readNumber(metrics.recentFourteenWinDays);
  const lossAbsTotal = readNumber(metrics.lossAbsTotal);
  const previousGames = readNumber(metrics.previousGames);
  const previousDifference = readNumber(metrics.todayDifference);
  const previousRbCount = readNumber(metrics.previousRbCount);
  const previousBonusTotal = readNumber(metrics.previousBonusTotal);
  const recentThreeGamesTotal = readNumber(metrics.recentThreeGamesTotal);
  const recentThreeBonusTotal = readNumber(metrics.recentThreeBonusTotal);
  const recentFiveGamesTotal = readNumber(metrics.recentFiveGamesTotal);
  const recentFiveBonusTotal = readNumber(metrics.recentFiveBonusTotal);
  const recentThreeRbTotal = readNumber(metrics.recentThreeRbTotal);
  const recentFiveRbTotal = readNumber(metrics.recentFiveRbTotal);
  const recentSevenGamesTotal = readNumber(metrics.recentSevenGamesTotal);
  const recentTenGamesTotal = readNumber(metrics.recentTenGamesTotal);
  const recentFourteenGamesTotal = readNumber(metrics.recentFourteenGamesTotal);
  const recentTwentyOneGamesTotal = readNumber(metrics.recentTwentyOneGamesTotal);
  const recentTwentyEightGamesTotal = readNumber(metrics.recentTwentyEightGamesTotal);
  const recentFortyTwoGamesTotal = readNumber(metrics.recentFortyTwoGamesTotal);
  const recentFiftySixGamesTotal = readNumber(metrics.recentFiftySixGamesTotal);
  const recentSevenRbTotal = readNumber(metrics.recentSevenRbTotal);
  const recentFourteenRbTotal = readNumber(metrics.recentFourteenRbTotal);
  const recentTwentyOneRbTotal = readNumber(metrics.recentTwentyOneRbTotal);
  const recentSevenBbTotal = readNumber(metrics.recentSevenBbTotal);
  const recentFourteenBbTotal = readNumber(metrics.recentFourteenBbTotal);
  const daysSinceHistoryHighSettingCandidate = readNullableNumber(
    metrics.daysSinceHistoryHighSettingCandidate,
  );
  const daysSinceHistoryHighSettingEstimate = readNullableNumber(
    metrics.daysSinceHistoryHighSettingEstimate,
  );
  const daysSinceHistoryStrongHighSettingCandidate = readNullableNumber(
    metrics.daysSinceHistoryStrongHighSettingCandidate,
  );
  const recentFiveHighSettingCandidateCount = readNumber(metrics.recentFiveHighSettingCandidateCount);
  const recentSevenHighSettingCandidateCount = readNumber(metrics.recentSevenHighSettingCandidateCount);
  const recentSevenHighSettingEstimateCount = readNumber(metrics.recentSevenHighSettingEstimateCount);
  const recentFiveHighSettingEstimateCount = readNumber(metrics.recentFiveHighSettingEstimateCount);
  const recentThreeHighSettingEstimateCount = readNumber(metrics.recentThreeHighSettingEstimateCount);
  const recentFourteenHighSettingCandidateCount = readNumber(metrics.recentFourteenHighSettingCandidateCount);
  const historyPositiveDays = readNumber(metrics.historyPositiveDays);
  const recentSevenBigShowDays = readNumber(metrics.recentSevenBigShowDays);
  const recentThreeBigShowDays = readNumber(metrics.recentThreeBigShowDays);
  const recentFourteenGoldShowDays = readNumber(metrics.recentFourteenGoldShowDays);
  const previousBigShow = Boolean(metrics.previousBigShow);
  const adjacentHighSettingCandidateCount7 = readNumber(metrics.adjacentHighSettingCandidateCount7);
  const historyNetTotal = readNumber(metrics.historyNetTotal);

  const bestRestDays = [
    daysSinceHistoryHighSettingCandidate,
    daysSinceHistoryHighSettingEstimate,
    daysSinceHistoryStrongHighSettingCandidate,
  ].filter((value) => Number.isFinite(value) && value > 0)[0] ?? null;
  const recentTwoAngle = netPerThousandGames(recentTwoNetTotal, readNumber(metrics.recentTwoGamesTotal));
  const recentThreeAngle = netPerThousandGames(recentThreeNetTotal, recentThreeGamesTotal);
  const recentSevenAngle = netPerThousandGames(recentSevenNetTotal, recentSevenGamesTotal);
  const recentFourteenAngle = netPerThousandGames(recentFourteenNetTotal, recentFourteenGamesTotal);
  const recentTwentyOneAngle = netPerThousandGames(recentTwentyOneNetTotal, recentTwentyOneGamesTotal);
  const recentFiveAngle = netPerThousandGames(recentFiveNetTotal, recentFiveGamesTotal);
  const recentThreeCombinedDenominator = rateDenominator(recentThreeGamesTotal, recentThreeBonusTotal);
  const recentFiveCombinedDenominator = rateDenominator(recentFiveGamesTotal, recentFiveBonusTotal);
  const recentSevenCombinedDenominator = rateDenominator(
    recentSevenGamesTotal,
    recentSevenBbTotal + recentSevenRbTotal,
  );
  const recentFourteenCombinedDenominator = rateDenominator(
    recentFourteenGamesTotal,
    recentFourteenBbTotal + recentFourteenRbTotal,
  );
  const recentThreeRbDenominator = rateDenominator(recentThreeGamesTotal, recentThreeRbTotal);
  const recentFiveRbDenominator = rateDenominator(recentFiveGamesTotal, recentFiveRbTotal);
  const recentSevenRbDenominator = rateDenominator(recentSevenGamesTotal, recentSevenRbTotal);
  const recentFourteenRbDenominator = rateDenominator(recentFourteenGamesTotal, recentFourteenRbTotal);
  const previousCombinedDenominator = rateDenominator(previousGames, previousBonusTotal);
  const previousRbDenominator = rateDenominator(previousGames, previousRbCount);
  const previousSetting = readNullableNumber(metrics.todaySetting);
  const previousHighContentByEstimate = Number.isFinite(previousSetting) && previousSetting >= 4.5 && previousRbCount >= 25;
  const previousHighContentByBonus =
    (previousGames >= 5000 && previousCombinedDenominator <= 145 && previousRbDenominator <= 315) ||
    (previousGames >= 3478 && previousCombinedDenominator <= 436 && previousDifference >= -836);
  const previousStrongHighContent =
    (Number.isFinite(previousSetting) && previousSetting >= 5) ||
    previousHighContentByEstimate ||
    (previousGames >= 4781 && previousCombinedDenominator <= 414 && previousDifference >= -1389);
  const previousHighContent = previousHighContentByEstimate || previousHighContentByBonus;
  const deepSink =
    recentSevenNetTotal <= -2000 ||
    recentFiveNetTotal <= -3500 ||
    recentTenNetTotal <= -4500 ||
    recentFourteenNetTotal <= -4500 ||
    lossAbsTotal >= 7000 ||
    streak >= 3;
  const strongAngle =
    recentThreeAngle <= -150 ||
    recentSevenAngle <= -60 ||
    recentFiveAngle <= -180 ||
    recentThreeNetTotal <= -2500;
  const stayCore =
    (Number.isFinite(bestRestDays) && bestRestDays >= 4 && bestRestDays <= 21) ||
    recentSevenNetTotal <= -2000 ||
    recentFiveNetTotal <= -3500;
  const weakBonus =
    recentThreeCombinedDenominator >= 170 ||
    recentSevenCombinedDenominator >= 170 ||
    recentSevenRbDenominator >= 500 ||
    recentFourteenRbDenominator >= 550 ||
    (previousGames >= 2500 && previousBonusTotal <= 10) ||
    (recentThreeGamesTotal >= 7000 && recentThreeBonusTotal <= 34) ||
    (recentFiveGamesTotal >= 12000 && recentFiveRbTotal <= 18);
  const staleRest =
    (Number.isFinite(bestRestDays) && bestRestDays >= 28) ||
    (recentTwentyEightNetTotal <= -5800 && recentSevenCombinedDenominator >= 190 && recentSevenGamesTotal >= 15000) ||
    (recentFiftySixNetTotal <= -17000);
  const highRest4To14 = Number.isFinite(bestRestDays) && bestRestDays >= 4 && bestRestDays <= 14;
  const highRest8To14 = Number.isFinite(bestRestDays) && bestRestDays >= 8 && bestRestDays <= 14;
  const losingStreak3 = streak >= 3 || recentFourLossDays >= 3;
  const losingStreak3To6 = streak >= 3 && streak <= 6;
  const recentFiveLoss3500 = recentFiveNetTotal <= -3500;
  const shortSinkStay2 = shortSevenSinkStayDays >= 2 || shortThreeSinkStayDays >= 2;
  const shortSinkStay3 = shortSevenSinkStayDays >= 3 || shortThreeSinkStayDays >= 3;
  const historyPositive3 = historyPositiveDays >= 3;
  const bigShow3 = recentSevenBigShowDays >= 3;
  const recentHigh =
    recentFiveHighSettingCandidateCount >= 2 ||
    recentSevenHighSettingCandidateCount >= 3 ||
    recentSevenHighSettingEstimateCount >= 3 ||
    recentFiveHighSettingEstimateCount >= 2 ||
    recentThreeHighSettingEstimateCount >= 1 ||
    recentFourteenHighSettingCandidateCount >= 4;
  const treatmentDone =
    recentTwoNetTotal >= 2500 ||
    recentSevenNetTotal >= 3500 ||
    recentFourteenNetTotal >= 5000 ||
    previousDifference >= 2000 ||
    (previousDifference >= 1000 && previousCombinedDenominator >= 150 && previousRbDenominator >= 360);
  const lowConfidence =
    previousGames < 1200 ||
    recentThreeGamesTotal < 5000 ||
    recentSevenGamesTotal < 12000;

  const boostFlags = [
    deepSink,
    strongAngle,
    stayCore,
    weakBonus,
    staleRest,
    highRest4To14,
    highRest8To14,
    shortSinkStay2,
    historyPositive3,
    adjacentHighSettingCandidateCount7 > 0,
    historyNetTotal <= -8000,
    bigShow3,
    recentSevenLossDays >= 5,
    recentFourteenNetTotal <= -3000,
    recentTwentyOneNetTotal <= -10000,
    recentFiftySixNetTotal <= -17000,
  ];
  const dangerFlags = [
    recentHigh,
    winningStreak >= 2,
    treatmentDone,
    lowConfidence,
    Number.isFinite(bestRestDays) && bestRestDays <= 2,
  ];

  return {
    deepSink,
    strongAngle,
    stayCore,
    weakBonus,
    staleRest,
    highRest4To14,
    highRest8To14,
    losingStreak3,
    losingStreak3To6,
    recentFiveLoss3500,
    shortSinkStay2,
    shortSinkStay3,
    historyPositive3,
    bigShow3,
    previousHighContent,
    previousStrongHighContent,
    previousBigShow,
    recentHigh,
    treatmentDone,
    lowConfidence,
    boostCount: boostFlags.filter(Boolean).length,
    dangerCount: dangerFlags.filter(Boolean).length,
    bestRestDays,
    recentTwoAngle,
    recentThreeAngle,
    recentFiveAngle,
    recentSevenAngle,
    recentFourteenAngle,
    recentTwentyOneAngle,
    recentThreeCombinedDenominator,
    recentFiveCombinedDenominator,
    recentSevenCombinedDenominator,
    recentFourteenCombinedDenominator,
    recentThreeRbDenominator,
    recentFiveRbDenominator,
    recentSevenRbDenominator,
    recentFourteenRbDenominator,
    previousCombinedDenominator,
    previousRbDenominator,
    recentThreeBigShowDays,
    recentSevenBigShowDays,
    recentFourteenGoldShowDays,
  };
}

function calculateRestScore(bestRestDays, profile) {
  if (!Number.isFinite(bestRestDays) || bestRestDays <= 0) {
    return profile === "okidoki" ? 4 : 0;
  }
  if (bestRestDays <= 2) {
    return -10;
  }
  if (bestRestDays <= 7) {
    return 10;
  }
  if (bestRestDays <= 14) {
    return 18;
  }
  if (bestRestDays <= 28) {
    return 14;
  }
  return 10;
}

function buildMachineSpecificFeatureState(definition, metrics, features) {
  const machineKey = definition?.machineKey ?? "";
  const activeLogicKey = definition?.activeLogicKey ?? definition?.logicKey ?? "";
  const historyRowCount = readNumber(metrics.historyRowCount);
  const targetRangeHistoryRowCount = readNumber(metrics.targetRangeHistoryRowCount, historyRowCount);
  const previousDifference = readNumber(metrics.todayDifference);
  const previousGames = readNumber(metrics.previousGames);
  const previousRbCount = readNumber(metrics.previousRbCount);
  const streak = readNumber(metrics.streak);
  const winningStreak = readNumber(metrics.winningStreak);
  const historyLosingStreak = readNumber(metrics.historyLosingStreak);
  const recentTwoNetTotal = readNumber(metrics.recentTwoNetTotal);
  const recentThreeNetTotal = readNumber(metrics.recentThreeNetTotal);
  const recentFiveNetTotal = readNumber(metrics.recentFiveNetTotal);
  const recentSevenNetTotal = readNumber(metrics.recentSevenNetTotal);
  const recentTenNetTotal = readNumber(metrics.recentTenNetTotal);
  const recentFourteenNetTotal = readNumber(metrics.recentFourteenNetTotal);
  const recentTwentyOneNetTotal = readNumber(metrics.recentTwentyOneNetTotal);
  const recentTwentyEightNetTotal = readNumber(metrics.recentTwentyEightNetTotal);
  const recentThirtyNetTotal = readNumber(metrics.recentThirtyNetTotal);
  const recentFiftySixNetTotal = readNumber(metrics.recentFiftySixNetTotal);
  const recentTwoGamesTotal = readNumber(metrics.recentTwoGamesTotal);
  const recentThreeGamesTotal = readNumber(metrics.recentThreeGamesTotal);
  const recentFiveGamesTotal = readNumber(metrics.recentFiveGamesTotal);
  const recentSevenGamesTotal = readNumber(metrics.recentSevenGamesTotal);
  const recentTenGamesTotal = readNumber(metrics.recentTenGamesTotal);
  const recentFourteenGamesTotal = readNumber(metrics.recentFourteenGamesTotal);
  const recentTwentyOneGamesTotal = readNumber(metrics.recentTwentyOneGamesTotal);
  const recentTwentyEightGamesTotal = readNumber(metrics.recentTwentyEightGamesTotal);
  const recentFourteenGoldShowDays = readNumber(metrics.recentFourteenGoldShowDays);
  const recentSevenGoldShowDays = readNumber(metrics.recentSevenGoldShowDays, readNumber(metrics.recentSevenBigShowDays));
  const recentFourteenLossDays = readNumber(metrics.recentFourteenLossDays);
  const recentFourteenWinDays = readNumber(metrics.recentFourteenWinDays);
  const recentSevenLossDays = readNumber(metrics.recentSevenLossDays);
  const recentSevenHighSettingCandidateCount = readNumber(metrics.recentSevenHighSettingCandidateCount);
  const recentFiveMinus2000StayDays = readNumber(metrics.recentFiveMinus2000StayDays);
  const recentSevenMinus1500StayDays = readNumber(metrics.recentSevenMinus1500StayDays);
  const recentTenMinus3000StayDays = readNumber(metrics.recentTenMinus3000StayDays);
  const recentTenMinus2500StayDays = readNumber(metrics.recentTenMinus2500StayDays);
  const recentThirtyMinus2700StayDays = readNumber(metrics.recentThirtyMinus2700StayDays);
  const recentFourteenMinus500StayDays = readNumber(metrics.recentFourteenMinus500StayDays);
  const recentFourteenMinus1500StayDays = readNumber(metrics.recentFourteenMinus1500StayDays);
  const recentFourteenMinus1800StayDays = readNumber(metrics.recentFourteenMinus1800StayDays);
  const recentFourteenMinus2000StayDays = readNumber(metrics.recentFourteenMinus2000StayDays);
  const recentFourteenMinus3000StayDays = readNumber(metrics.recentFourteenMinus3000StayDays);
  const recentFourteenNegativeStayDays = readNumber(metrics.recentFourteenNegativeStayDays);
  const recentThreeMinus1000StayDays = readNumber(metrics.recentThreeMinus1000StayDays);
  const recentFiveMinus1000StayDays = readNumber(metrics.recentFiveMinus1000StayDays);
  const recentFiveMinus1500StayDays = readNumber(metrics.recentFiveMinus1500StayDays);
  const recentTwentyOneMinus1500StayDays = readNumber(metrics.recentTwentyOneMinus1500StayDays);
  const recentTwentyOneMinus2000StayDays = readNumber(metrics.recentTwentyOneMinus2000StayDays);
  const recentTwentyOneMinus3000StayDays = readNumber(metrics.recentTwentyOneMinus3000StayDays);
  const recentTwentyOneMinus5000StayDays = readNumber(metrics.recentTwentyOneMinus5000StayDays);
  const adjacentHighSettingCandidateCount7 = readNumber(metrics.adjacentHighSettingCandidateCount7);
  const adjacentMachineHighContentCount3 = readNumber(metrics.adjacentMachineHighContentCount3);
  const adjacentMachineHighContentCount3Near2 = readNumber(metrics.adjacentMachineHighContentCount3Near2);
  const adjacentMachineHighContentCount7 = readNumber(metrics.adjacentMachineHighContentCount7);
  const adjacentMachineHighContentCount14 = readNumber(metrics.adjacentMachineHighContentCount14);
  const adjacentMachineHighContentCount7Near2 = readNumber(metrics.adjacentMachineHighContentCount7Near2);
  const adjacentMachineHighContentCount14Near2 = readNumber(metrics.adjacentMachineHighContentCount14Near2);
  const otherSameMachineHighContentCount7 = readNumber(metrics.otherSameMachineHighContentCount7);
  const adjacentMachineBigWin1000Count7Near2 = readNumber(metrics.adjacentMachineBigWin1000Count7Near2);
  const adjacentMachineNetTotal3 = readNumber(metrics.adjacentMachineNetTotal3);
  const adjacentMachineNetTotal3Near2 = readNumber(metrics.adjacentMachineNetTotal3Near2);
  const adjacentMachineNetTotal5 = readNumber(metrics.adjacentMachineNetTotal5);
  const adjacentMachineNetTotal5Near2 = readNumber(metrics.adjacentMachineNetTotal5Near2);
  const adjacentMachineNetTotal7 = readNumber(metrics.adjacentMachineNetTotal7);
  const adjacentMachineNetTotal7Near2 = readNumber(metrics.adjacentMachineNetTotal7Near2);
  const adjacentMachineNetTotal14 = readNumber(metrics.adjacentMachineNetTotal14);
  const previousAdjacentMachineHighContentCount = readNumber(metrics.previousAdjacentMachineHighContentCount);
  const previousAdjacentMachineHighContentCountNear2 = readNumber(metrics.previousAdjacentMachineHighContentCountNear2);
  const previousAdjacentMachineGoodContentCount = readNumber(metrics.previousAdjacentMachineGoodContentCount);
  const previousAdjacentMachineBigWin1000Count = readNumber(metrics.previousAdjacentMachineBigWin1000Count);
  const previousAdjacentMachineNetTotal = readNumber(metrics.previousAdjacentMachineNetTotal);
  const previousAdjacentMachineNetTotalNear2 = readNumber(metrics.previousAdjacentMachineNetTotalNear2);
  const previousOtherMachineHighContentCount = readNumber(metrics.previousOtherMachineHighContentCount);
  const sameMachinePreviousNetTotal = readNumber(metrics.sameMachinePreviousNetTotal);
  const recentThreeMachineHighContentCount = readNumber(metrics.recentThreeMachineHighContentCount);
  const recentFiveMachineHighContentCount = readNumber(metrics.recentFiveMachineHighContentCount);
  const recentSevenMachineHighContentCount = readNumber(metrics.recentSevenMachineHighContentCount);
  const recentFourteenMachineHighContentCount = readNumber(metrics.recentFourteenMachineHighContentCount);
  const recentTwentyOneMachineHighContentCount = readNumber(metrics.recentTwentyOneMachineHighContentCount);
  const recentFourteenMachineStrongHighContentCount = readNumber(metrics.recentFourteenMachineStrongHighContentCount);
  const recentThirtyMachineHighContentCount = readNumber(metrics.recentThirtyMachineHighContentCount);
  const recentSevenMachineGoodContentCount = readNumber(metrics.recentSevenMachineGoodContentCount);
  const recentThreeMachineLowContentCount = readNumber(metrics.recentThreeMachineLowContentCount);
  const recentFiveMachineLowContentCount = readNumber(metrics.recentFiveMachineLowContentCount);
  const recentSevenMachineLowContentCount = readNumber(metrics.recentSevenMachineLowContentCount);
  const recentFiveMachineWeakContentCount = readNumber(metrics.recentFiveMachineWeakContentCount);
  const recentSevenMachineWeakContentCount = readNumber(metrics.recentSevenMachineWeakContentCount);
  const recentFourteenMachineGoodContentCount = readNumber(metrics.recentFourteenMachineGoodContentCount);
  const recentTwentyOneMachineGoodContentCount = readNumber(metrics.recentTwentyOneMachineGoodContentCount);
  const daysSinceMachineHighContent = readNullableNumber(metrics.daysSinceMachineHighContent);
  const daysSinceMachineStrongHighContent = readNullableNumber(metrics.daysSinceMachineStrongHighContent);
  const daysSinceMachineBigWin1500 = readNullableNumber(metrics.daysSinceMachineBigWin1500);
  const daysSinceHistoryRbLight = readNullableNumber(metrics.daysSinceHistoryRbLight);
  const recentTwentyEightRbLightCount = readNumber(metrics.recentTwentyEightRbLightCount);
  const previousMachineHighContent = Boolean(metrics.previousMachineHighContent);
  const previousMachineGoodContent = Boolean(metrics.previousMachineGoodContent);
  const previousMachineWeakContent = Boolean(metrics.previousMachineWeakContent);
  const previousMachineStrongHighContent = Boolean(metrics.previousMachineStrongHighContent);
  const previousMachineSettingFivePlusProbability = readNullableNumber(
    metrics.previousMachineSettingFivePlusProbability,
  );
  const machineHighContentStreak = readNumber(metrics.machineHighContentStreak);
  const machineWeakContentStreak = readNumber(metrics.machineWeakContentStreak);
  const recentFiveBigWin1200Count = readNumber(metrics.recentFiveBigWin1200Count);
  const recentFiveBigWin1000Count = readNumber(metrics.recentFiveBigWin1000Count);
  const previousAdjacentMachineWeakContentCount = readNumber(metrics.previousAdjacentMachineWeakContentCount);
  const recentThreeRawDifferenceTotal = readNumber(metrics.recentThreeRawDifferenceTotal);
  const recentFiveRawDifferenceTotal = readNumber(metrics.recentFiveRawDifferenceTotal);
  const recentThreeRawDifferenceCount = readNumber(metrics.recentThreeRawDifferenceCount);
  const recentFiveRawDifferenceCount = readNumber(metrics.recentFiveRawDifferenceCount);
  const previousRawDifferenceValue = readNullableNumber(metrics.previousRawDifferenceValue);
  const rawDifferenceLosingStreak = readNumber(metrics.rawDifferenceLosingStreak);

  if (machineKey === "aim") {
    if (activeLogicKey === "mj-kurume-aim") {
      const kurumeAimHistoryReady = targetRangeHistoryRowCount >= 14;
      const kurumeAimDeepSink = recentTenNetTotal <= -3000 || recentSevenNetTotal <= -2500;
      const kurumeAimSinkStay =
        recentTenMinus3000StayDays >= 1 ||
        recentTenMinus2500StayDays >= 1 ||
        recentTwentyOneMinus1500StayDays >= 6 ||
        (recentTwentyOneMinus1500StayDays >= 3 && recentFiveMinus2000StayDays >= 2);
      const kurumeAimLosing = streak >= 5;
      const kurumeAimPreviousHighFail = previousMachineHighContent && previousDifference <= 0;
      const kurumeAimGenuineBonus = features.recentFiveRbDenominator <= 270;
      const kurumeAimTrustedGames =
        (recentSevenNetTotal <= -2000 || recentTenNetTotal <= -2500) &&
        recentSevenGamesTotal >= 15000;
      const kurumeAimTreatmentDone = recentSevenNetTotal >= 1500 || recentFourteenNetTotal >= 2000;
      const kurumeAimPreviousHighPlus = previousMachineHighContent && previousDifference >= 1200;
      const kurumeAimHighStreak = machineHighContentStreak >= 2;
      const kurumeAimWinStreak = winningStreak >= 3;
      const kurumeAimPreviousBbOnly =
        previousDifference > 800 && (features.previousRbDenominator > 350 || previousRbCount === 0);
      const kurumeAimLowConfidence = recentSevenGamesTotal < 7000 && streak < 5;
      const boostFlags = [
        kurumeAimDeepSink,
        kurumeAimSinkStay,
        kurumeAimLosing,
        kurumeAimPreviousHighFail,
        kurumeAimGenuineBonus,
        kurumeAimTrustedGames,
      ];
      const dangerFlags = [
        kurumeAimTreatmentDone,
        kurumeAimPreviousHighPlus,
        kurumeAimHighStreak,
        kurumeAimWinStreak,
        kurumeAimPreviousBbOnly,
        kurumeAimLowConfidence,
      ];

      return {
        ...features,
        kurumeAimHistoryReady,
        kurumeAimDeepSink,
        kurumeAimSinkStay,
        kurumeAimLosing,
        kurumeAimPreviousHighFail,
        kurumeAimGenuineBonus,
        kurumeAimTrustedGames,
        kurumeAimTreatmentDone,
        kurumeAimPreviousHighPlus,
        kurumeAimHighStreak,
        kurumeAimWinStreak,
        kurumeAimPreviousBbOnly,
        kurumeAimLowConfidence,
        treatmentDone: kurumeAimTreatmentDone,
        lowConfidence: kurumeAimLowConfidence,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    const aimSinkStayStrong =
      recentSevenNetTotal <= -2000 &&
      features.recentSevenCombinedDenominator >= 155 &&
      recentSevenGamesTotal >= 25000;
    const aimStrongAngle =
      features.recentSevenAngle <= -60 &&
      recentSevenGamesTotal >= 25000;
    const aimNearbyLeftBehind =
      (recentSevenNetTotal <= -2000 && adjacentMachineHighContentCount14 > 0) ||
      (recentSevenNetTotal <= -1000 && adjacentMachineNetTotal7 >= 3000);
    const aimUnpaid =
      previousDifference > 0 &&
      recentSevenNetTotal <= -1500 &&
      features.recentSevenCombinedDenominator >= 155;

    return {
      ...features,
      aimSinkStayStrong,
      aimStrongAngle,
      aimNearbyLeftBehind,
      aimUnpaid,
    };
  }

  if (machineKey === "neo-aim") {
    if (activeLogicKey === "kintoki-kamata-neo-aim") {
      const kintokiNeoHistoryReady = historyRowCount >= 14;
      const kintokiNeoHistoryShort = historyRowCount < 14;
      const hasPreviousFivePlus = Number.isFinite(previousMachineSettingFivePlusProbability);
      const kintokiNeoPreviousWeak =
        hasPreviousFivePlus && previousMachineSettingFivePlusProbability < 0.2;
      const kintokiNeoBbOutputContinue =
        previousDifference >= 1500 &&
        hasPreviousFivePlus &&
        previousMachineSettingFivePlusProbability < 0.3;
      const kintokiNeoSinkHighGames = recentSevenNetTotal <= -2500 && recentFiveGamesTotal >= 14000;
      const kintokiNeoShortHighLeft =
        Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent <= 3;
      const kintokiNeoNearbyTwoHighSink =
        recentSevenNetTotal <= -2500 && previousAdjacentMachineHighContentCountNear2 >= 2;
      const kintokiNeoLongNeglect =
        recentFourteenMachineHighContentCount === 0 &&
        ((Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 29) ||
          (!Number.isFinite(daysSinceMachineHighContent) && historyRowCount >= 29));
      const kintokiNeoTreatmentDone = recentFourteenNetTotal > 3000 && recentSevenNetTotal > 1500;
      const kintokiNeoLowGamesHistory = recentFiveGamesTotal < 9000 && recentFourteenGamesTotal < 25000;
      const kintokiNeoPreviousStrongDone =
        previousDifference >= 2500 &&
        hasPreviousFivePlus &&
        previousMachineSettingFivePlusProbability >= 0.5 &&
        recentFourteenNetTotal > 0;
      const kintokiNeoBadCombinedLowGames =
        features.recentThreeCombinedDenominator > 180 && recentSevenGamesTotal < 16000;
      const dangerFlags = [
        kintokiNeoLongNeglect,
        kintokiNeoTreatmentDone,
        kintokiNeoLowGamesHistory,
        kintokiNeoPreviousStrongDone,
        kintokiNeoBadCombinedLowGames,
      ];
      const dangerCount = dangerFlags.filter(Boolean).length;
      const kintokiNeoFreeNonRank =
        (kintokiNeoSinkHighGames && kintokiNeoPreviousWeak) ||
        kintokiNeoNearbyTwoHighSink ||
        kintokiNeoBbOutputContinue;
      const kintokiNeoNoFree = !kintokiNeoFreeNonRank && !kintokiNeoPreviousWeak;
      const boostFlags = [
        kintokiNeoSinkHighGames,
        kintokiNeoPreviousWeak,
        kintokiNeoBbOutputContinue,
        kintokiNeoShortHighLeft,
        kintokiNeoNearbyTwoHighSink,
        dangerCount === 0,
      ];

      return {
        ...features,
        previousMachineSettingFivePlusProbability,
        kintokiNeoHistoryReady,
        kintokiNeoHistoryShort,
        kintokiNeoPreviousWeak,
        kintokiNeoBbOutputContinue,
        kintokiNeoSinkHighGames,
        kintokiNeoShortHighLeft,
        kintokiNeoNearbyTwoHighSink,
        kintokiNeoLongNeglect,
        kintokiNeoTreatmentDone,
        kintokiNeoLowGamesHistory,
        kintokiNeoPreviousStrongDone,
        kintokiNeoBadCombinedLowGames,
        kintokiNeoFreeNonRank,
        kintokiNeoNoFree,
        treatmentDone: kintokiNeoTreatmentDone || kintokiNeoPreviousStrongDone,
        lowConfidence: kintokiNeoHistoryShort || kintokiNeoLowGamesHistory,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount,
      };
    }

    if (activeLogicKey === "messe-minamisenju-neo-aim") {
      const messeNeoHistoryReady = historyRowCount >= 5;
      const messeNeoHistoryShort = historyRowCount < 5;
      const previousPayoutRate = previousGames > 0 ? 100 + (previousDifference / previousGames / 3) * 100 : 100;
      const messeNeoShallowSink = recentFiveNetTotal >= -2500 && recentFiveNetTotal < 0;
      const messeNeoSinkStay7 = recentFiveMinus1000StayDays >= 7;
      const messeNeoSinkStay8 = recentFiveMinus1000StayDays >= 8;
      const messeNeoRb14Weak = features.recentFourteenRbDenominator >= 350;
      const messeNeoCombined14Weak = features.recentFourteenCombinedDenominator >= 155;
      const messeNeoCombined5Weak = features.recentFiveCombinedDenominator >= 155;
      const messeNeoPreviousSink = previousDifference <= -1000;
      const messeNeoPreviousHighFail = previousMachineHighContent && previousDifference < 800;
      const messeNeoPreviousHighAfter = previousMachineHighContent;
      const messeNeoBbTailwind =
        previousDifference >= 1000 &&
        Number.isFinite(previousMachineSettingFivePlusProbability) &&
        previousMachineSettingFivePlusProbability < 0.3;
      const messeNeoInterval14 =
        Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 14;
      const messeNeoInterval16 =
        Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 16;
      const messeNeoRecentShow = recentFiveBigWin1000Count >= 1;
      const messeNeoNoRecentShow = recentFiveBigWin1000Count === 0;
      const messeNeoLowGames = recentFiveGamesTotal < 9000;
      const messeNeoLosing4 = streak >= 4;
      const messeNeoDeepFiveOnly = recentFiveNetTotal <= -3500 && recentFiveMinus1000StayDays < 4;
      const messeNeoBonusWeak =
        features.recentFourteenRbDenominator >= 330 ||
        messeNeoCombined14Weak ||
        features.recentFiveRbDenominator >= 350 ||
        messeNeoCombined5Weak;
      const messeNeoUnrepaid =
        recentFourteenNetTotal <= 0 ||
        messeNeoInterval14 ||
        recentFiveMachineHighContentCount === 0;
      const messeNeoTreatmentDone =
        recentFiveNetTotal >= 4000 && !messeNeoBonusWeak && !messeNeoUnrepaid;
      const messeNeoTooManyShows = recentFiveBigWin1000Count >= 2;
      const messeNeoFree5Combined = messeNeoShallowSink && messeNeoPreviousSink && messeNeoCombined5Weak;
      const messeNeoFree14Rb = messeNeoShallowSink && messeNeoPreviousSink && messeNeoRb14Weak;
      const baseDangerFlags = [
        messeNeoLowGames,
        messeNeoLosing4,
        messeNeoDeepFiveOnly,
        messeNeoTreatmentDone,
        messeNeoTooManyShows,
      ];
      const messeNeoBbTailwindGap14 = messeNeoBbTailwind && messeNeoInterval14 && baseDangerFlags.filter(Boolean).length === 0;
      const boostFlags = [
        messeNeoSinkStay7,
        messeNeoRb14Weak,
        messeNeoCombined14Weak,
        messeNeoPreviousSink && messeNeoShallowSink,
        messeNeoPreviousHighAfter || messeNeoBbTailwind,
        messeNeoInterval14,
      ];
      const dangerFlags = baseDangerFlags;

      return {
        ...features,
        previousMachineSettingFivePlusProbability,
        previousPayoutRate,
        messeNeoHistoryReady,
        messeNeoHistoryShort,
        messeNeoShallowSink,
        messeNeoSinkStay7,
        messeNeoSinkStay8,
        messeNeoRb14Weak,
        messeNeoCombined14Weak,
        messeNeoCombined5Weak,
        messeNeoPreviousSink,
        messeNeoPreviousHighFail,
        messeNeoPreviousHighAfter,
        messeNeoBbTailwind,
        messeNeoInterval14,
        messeNeoInterval16,
        messeNeoRecentShow,
        messeNeoNoRecentShow,
        messeNeoLowGames,
        messeNeoLosing4,
        messeNeoDeepFiveOnly,
        messeNeoBonusWeak,
        messeNeoUnrepaid,
        messeNeoTreatmentDone,
        messeNeoTooManyShows,
        messeNeoFree5Combined,
        messeNeoFree14Rb,
        messeNeoBbTailwindGap14,
        treatmentDone: messeNeoTreatmentDone,
        lowConfidence: messeNeoHistoryShort || messeNeoLowGames,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    if (activeLogicKey === "espace-ueno-neo-aim") {
      const recentTwentyOneRbTotal = readNumber(metrics.recentTwentyOneRbTotal);
      const recentTwentyOneRbDenominator = rateDenominator(recentTwentyOneGamesTotal, recentTwentyOneRbTotal);
      const espaceUenoNeoHistoryReady = historyRowCount >= 7;
      const espaceUenoNeoHistoryShort = historyRowCount < 7;
      const espaceUenoNeoLowMidGames3 = recentThreeGamesTotal >= 3000 && recentThreeGamesTotal <= 12000;
      const espaceUenoNeoGames3TooHigh = recentThreeGamesTotal >= 15000;
      const espaceUenoNeoGames3Under10000 = recentThreeGamesTotal < 10000;
      const espaceUenoNeoGames5TooHigh = recentFiveGamesTotal >= 25000;
      const espaceUenoNeoGames5Compromise = recentFiveGamesTotal >= 10000 && recentFiveGamesTotal <= 18000;
      const espaceUenoNeoGames14Middle = recentFourteenGamesTotal >= 40000 && recentFourteenGamesTotal <= 70000;
      const espaceUenoNeoDiff5Shallow = recentFiveNetTotal >= -1500 && recentFiveNetTotal <= 500;
      const espaceUenoNeoDiff7TooDeep = recentSevenNetTotal <= -5000;
      const espaceUenoNeoRb21Strong =
        recentTwentyOneGamesTotal >= 30000 && recentTwentyOneRbDenominator <= 290;
      const espaceUenoNeoRb14Strong =
        recentFourteenGamesTotal >= 20000 && features.recentFourteenRbDenominator <= 290;
      const espaceUenoNeoRb14Best =
        recentFourteenGamesTotal >= 20000 && features.recentFourteenRbDenominator <= 270;
      const espaceUenoNeoIntervalGood =
        Number.isFinite(daysSinceMachineHighContent) &&
        ((daysSinceMachineHighContent >= 5 && daysSinceMachineHighContent <= 7) ||
          daysSinceMachineHighContent >= 14);
      const espaceUenoNeoPreviousBigWin = previousDifference >= 2000;
      const espaceUenoNeoPreviousHigh = previousMachineHighContent;
      const espaceUenoNeoPreviousHighPlus = previousMachineHighContent && previousDifference > 1500;
      const espaceUenoNeoSameMachinePreviousHighCount =
        previousOtherMachineHighContentCount + (previousMachineHighContent ? 1 : 0);
      const espaceUenoNeoSameMachinePreviousHighMany = espaceUenoNeoSameMachinePreviousHighCount >= 8;
      const espaceUenoNeoDangerZero =
        !espaceUenoNeoPreviousBigWin &&
        !previousMachineHighContent &&
        !espaceUenoNeoGames3TooHigh &&
        !espaceUenoNeoDiff7TooDeep;
      const espaceUenoNeoLowGames = recentThreeGamesTotal < 3000 || recentFiveGamesTotal < 10000;
      const espaceUenoNeoHighVisibleAndOverworked = espaceUenoNeoGames3TooHigh && espaceUenoNeoGames5TooHigh;
      const espaceUenoNeoFreeABestRb =
        espaceUenoNeoLowMidGames3 && espaceUenoNeoGames14Middle && espaceUenoNeoRb14Best;
      const espaceUenoNeoFreeBBalanced =
        espaceUenoNeoLowMidGames3 && espaceUenoNeoGames14Middle && espaceUenoNeoRb21Strong;
      const espaceUenoNeoCompromise = espaceUenoNeoGames5Compromise && espaceUenoNeoDiff5Shallow;
      const boostFlags = [
        espaceUenoNeoLowMidGames3,
        espaceUenoNeoDiff5Shallow,
        espaceUenoNeoRb21Strong,
        espaceUenoNeoRb14Strong,
        espaceUenoNeoIntervalGood,
        espaceUenoNeoDangerZero,
      ];
      const dangerFlags = [
        espaceUenoNeoGames3TooHigh,
        espaceUenoNeoDiff7TooDeep,
        espaceUenoNeoPreviousBigWin,
        previousMachineHighContent,
        espaceUenoNeoHistoryShort,
        espaceUenoNeoLowGames,
      ];

      return {
        ...features,
        previousMachineSettingFivePlusProbability,
        recentTwentyOneRbDenominator,
        espaceUenoNeoHistoryReady,
        espaceUenoNeoHistoryShort,
        espaceUenoNeoLowMidGames3,
        espaceUenoNeoGames3TooHigh,
        espaceUenoNeoGames3Under10000,
        espaceUenoNeoGames5TooHigh,
        espaceUenoNeoGames5Compromise,
        espaceUenoNeoGames14Middle,
        espaceUenoNeoDiff5Shallow,
        espaceUenoNeoDiff7TooDeep,
        espaceUenoNeoRb21Strong,
        espaceUenoNeoRb14Strong,
        espaceUenoNeoRb14Best,
        espaceUenoNeoIntervalGood,
        espaceUenoNeoPreviousBigWin,
        espaceUenoNeoPreviousHigh,
        espaceUenoNeoPreviousHighPlus,
        espaceUenoNeoSameMachinePreviousHighCount,
        espaceUenoNeoSameMachinePreviousHighMany,
        espaceUenoNeoDangerZero,
        espaceUenoNeoLowGames,
        espaceUenoNeoHighVisibleAndOverworked,
        espaceUenoNeoFreeABestRb,
        espaceUenoNeoFreeBBalanced,
        espaceUenoNeoCompromise,
        treatmentDone: espaceUenoNeoPreviousBigWin || espaceUenoNeoPreviousHighPlus,
        lowConfidence:
          espaceUenoNeoHistoryShort ||
          historyRowCount < 14 ||
          recentFourteenGamesTotal < 30000 ||
          espaceUenoNeoLowGames,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    if (activeLogicKey === "chikushino-neo-aim") {
      const chikushinoNeoHistoryReady = historyRowCount >= 5;
      const chikushinoNeoHistoryShort = historyRowCount < 5;
      const chikushinoNeoLowContentFiveZero =
        historyRowCount >= 5 && recentFiveMachineLowContentCount === 0;
      const chikushinoNeoWeakContentSevenZero =
        historyRowCount >= 7 && recentSevenMachineWeakContentCount === 0;
      const chikushinoNeoLowWeakAvoid =
        historyRowCount >= 5 &&
        recentFiveMachineLowContentCount <= 1 &&
        recentFiveMachineWeakContentCount <= 1;
      const chikushinoNeoPreviousGames1500 = previousGames > 0 && previousGames <= 1500;
      const chikushinoNeoPreviousGames2500 = previousGames > 0 && previousGames <= 2500;
      const chikushinoNeoPreviousReset =
        chikushinoNeoPreviousGames2500 ||
        (previousGames >= 1000 && features.previousRbDenominator >= 500);
      const chikushinoNeoIntervalReady =
        Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 3;
      const chikushinoNeoNearbyTailwind = adjacentMachineHighContentCount3Near2 >= 2;
      const chikushinoNeoHasThreeDifference = recentThreeRawDifferenceCount >= 3;
      const chikushinoNeoHasFiveDifference = recentFiveRawDifferenceCount >= 5;
      const chikushinoNeoThreeDiffSink1000 =
        chikushinoNeoHasThreeDifference && recentThreeRawDifferenceTotal <= -1000;
      const chikushinoNeoShortSinkBoost =
        chikushinoNeoThreeDiffSink1000 || rawDifferenceLosingStreak >= 2;
      const chikushinoNeoLongWeakContent =
        machineWeakContentStreak >= 2 ||
        recentSevenMachineLowContentCount >= 5 ||
        recentSevenMachineWeakContentCount >= 5;
      const chikushinoNeoOverWorked = recentThreeGamesTotal > 15000 || recentFiveGamesTotal > 25000;
      const chikushinoNeoImmediateFollow =
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent >= 1 &&
        daysSinceMachineHighContent <= 2;
      const chikushinoNeoTreatmentDone =
        (chikushinoNeoHasThreeDifference && recentThreeRawDifferenceTotal >= 3000) ||
        (chikushinoNeoHasFiveDifference && recentFiveRawDifferenceTotal >= 3000);
      const chikushinoNeoNearbyWeak = previousAdjacentMachineWeakContentCount >= 3;
      const boostFlags = [
        chikushinoNeoShortSinkBoost,
        chikushinoNeoPreviousReset,
        chikushinoNeoIntervalReady,
        chikushinoNeoLowWeakAvoid,
        chikushinoNeoNearbyTailwind,
      ];
      const dangerFlags = [
        chikushinoNeoHistoryShort,
        chikushinoNeoLongWeakContent,
        chikushinoNeoOverWorked,
        chikushinoNeoImmediateFollow,
        chikushinoNeoTreatmentDone,
        chikushinoNeoNearbyWeak,
      ];

      return {
        ...features,
        previousMachineSettingFivePlusProbability,
        chikushinoNeoHistoryReady,
        chikushinoNeoHistoryShort,
        chikushinoNeoLowContentFiveZero,
        chikushinoNeoWeakContentSevenZero,
        chikushinoNeoLowWeakAvoid,
        chikushinoNeoPreviousGames1500,
        chikushinoNeoPreviousGames2500,
        chikushinoNeoPreviousReset,
        chikushinoNeoIntervalReady,
        chikushinoNeoNearbyTailwind,
        chikushinoNeoHasThreeDifference,
        chikushinoNeoHasFiveDifference,
        chikushinoNeoThreeDiffSink1000,
        chikushinoNeoShortSinkBoost,
        chikushinoNeoLongWeakContent,
        chikushinoNeoOverWorked,
        chikushinoNeoImmediateFollow,
        chikushinoNeoTreatmentDone,
        chikushinoNeoNearbyWeak,
        treatmentDone: chikushinoNeoTreatmentDone,
        lowConfidence: chikushinoNeoHistoryShort,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    if (activeLogicKey === "hinode-onojo-neo-aim") {
      const hinodeNeoHistoryReady = historyRowCount >= 7;
      const hinodeNeoHistoryShort = !hinodeNeoHistoryReady;
      const hinodeNeoDeepSink57 = recentFiveNetTotal <= -3000 && recentSevenNetTotal <= -2500;
      const hinodeNeoLosing4 = streak >= 4;
      const hinodeNeoBonusWeak7 = features.recentSevenCombinedDenominator >= 170;
      const hinodeNeoNearbyLeftBehind =
        previousAdjacentMachineHighContentCount > 0 && recentFiveNetTotal <= -3000;
      const hinodeNeoDiff3Deep = recentThreeNetTotal <= -3000;
      const hinodeNeoFiveLossBonusWeak =
        streak >= 5 && features.recentSevenCombinedDenominator >= 170 && recentSevenGamesTotal >= 25000;
      const hinodeNeoThreeSinkFiveLoss =
        recentThreeNetTotal <= -3000 && streak >= 5 && features.recentSevenCombinedDenominator >= 170;
      const hinodeNeoThree4000Five5000 = recentThreeNetTotal <= -4000 && recentFiveNetTotal <= -5000;
      const hinodeNeoSixLosingNearby =
        recentSevenNetTotal <= -2500 && streak >= 6 && previousAdjacentMachineHighContentCount > 0;
      const hinodeNeoTreatmentDoneDiff = recentFiveNetTotal >= 3000 || recentSevenNetTotal >= 3500;
      const hinodeNeoRecentBigWins = recentFiveBigWin1200Count >= 2 && recentFiveNetTotal >= 1000;
      const hinodeNeoLowHistoryGames = recentFiveGamesTotal < 12000;
      const hinodeNeoOverVisible =
        features.recentSevenCombinedDenominator <= 140 && recentSevenNetTotal >= 0;
      const hinodeNeoHighContentDone = recentSevenMachineHighContentCount >= 2 && recentSevenNetTotal >= 0;
      const boostFlags = [
        hinodeNeoDeepSink57,
        hinodeNeoLosing4,
        hinodeNeoBonusWeak7,
        hinodeNeoNearbyLeftBehind,
      ];
      const dangerFlags = [
        hinodeNeoTreatmentDoneDiff,
        hinodeNeoRecentBigWins,
        hinodeNeoLowHistoryGames,
        hinodeNeoOverVisible,
      ];

      return {
        ...features,
        previousMachineSettingFivePlusProbability,
        hinodeNeoHistoryReady,
        hinodeNeoHistoryShort,
        hinodeNeoDeepSink57,
        hinodeNeoLosing4,
        hinodeNeoBonusWeak7,
        hinodeNeoNearbyLeftBehind,
        hinodeNeoDiff3Deep,
        hinodeNeoFiveLossBonusWeak,
        hinodeNeoThreeSinkFiveLoss,
        hinodeNeoThree4000Five5000,
        hinodeNeoSixLosingNearby,
        hinodeNeoTreatmentDoneDiff,
        hinodeNeoRecentBigWins,
        hinodeNeoLowHistoryGames,
        hinodeNeoOverVisible,
        hinodeNeoHighContentDone,
        treatmentDone: hinodeNeoTreatmentDoneDiff || hinodeNeoHighContentDone,
        lowConfidence: hinodeNeoLowHistoryGames || hinodeNeoHistoryShort,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    if (activeLogicKey === "gogo-tenjin-neo-aim") {
      const gogoTenjinNeoHistoryReady = historyRowCount >= 28;
      const recentTwentyEightAngleRaw =
        recentTwentyEightGamesTotal > 0 ? recentTwentyEightNetTotal / recentTwentyEightGamesTotal : 0;
      const gogoTenjinNeoS1 =
        gogoTenjinNeoHistoryReady &&
        recentTwentyEightAngleRaw <= -0.06 &&
        Number.isFinite(daysSinceHistoryRbLight) &&
        daysSinceHistoryRbLight >= 4 &&
        daysSinceHistoryRbLight <= 7;
      const gogoTenjinNeoS2 =
        gogoTenjinNeoHistoryReady &&
        recentTwentyEightNetTotal <= -7000 &&
        recentFourteenNetTotal <= -5000;
      const gogoTenjinNeoS3 =
        gogoTenjinNeoHistoryReady &&
        recentTwentyEightAngleRaw <= -0.06 &&
        recentTwentyEightRbLightCount <= 2;
      const gogoTenjinNeoPreviousRbFail =
        previousGames >= 3000 &&
        features.previousRbDenominator <= 300 &&
        previousDifference < 1000;
      const gogoTenjinNeoPreviousCombinedFail =
        previousGames >= 3000 &&
        features.previousCombinedDenominator <= 140 &&
        previousDifference < 1500;
      const gogoTenjinNeoPreviousHighFail = previousMachineHighContent && previousDifference < 0;
      const gogoTenjinNeoTreatmentDone =
        recentTwentyEightNetTotal >= 4000 ||
        recentTwentyOneNetTotal >= 5000 ||
        recentFourteenNetTotal >= 4000 ||
        previousDifference >= 2500;
      const gogoTenjinNeoLowGames = recentFourteenGamesTotal < 30000 || recentTwentyEightGamesTotal < 70000;
      const gogoTenjinNeoOverused = recentFourteenMachineHighContentCount >= 3;
      const gogoTenjinNeoGenuinePrevious =
        previousGames >= 3000 &&
        features.previousRbDenominator <= 300 &&
        features.previousCombinedDenominator <= 150 &&
        previousDifference < 1500;
      const gogoTenjinNeoUnpaid =
        recentTwentyEightNetTotal <= -3000 &&
        (recentFourteenMachineHighContentCount >= 1 || previousMachineHighContent);
      const gogoTenjinNeoLongNeglect =
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent >= 14 &&
        recentFourteenMachineHighContentCount === 0 &&
        recentTwentyEightNetTotal > -3000;
      const gogoTenjinNeoBbBiasedOutput = previousDifference >= 2000 && features.previousRbDenominator > 350;
      const boostFlags = [
        gogoTenjinNeoS1,
        gogoTenjinNeoS2,
        gogoTenjinNeoS3,
        gogoTenjinNeoPreviousRbFail,
        gogoTenjinNeoPreviousCombinedFail,
        gogoTenjinNeoPreviousHighFail,
        gogoTenjinNeoGenuinePrevious,
        gogoTenjinNeoUnpaid,
      ];
      const dangerFlags = [
        gogoTenjinNeoTreatmentDone,
        gogoTenjinNeoLowGames,
        gogoTenjinNeoOverused,
        gogoTenjinNeoLongNeglect,
        gogoTenjinNeoBbBiasedOutput,
      ];

      return {
        ...features,
        recentTwentyEightAngleRaw,
        daysSinceHistoryRbLight,
        recentTwentyEightRbLightCount,
        gogoTenjinNeoHistoryReady,
        gogoTenjinNeoS1,
        gogoTenjinNeoS2,
        gogoTenjinNeoS3,
        gogoTenjinNeoPreviousRbFail,
        gogoTenjinNeoPreviousCombinedFail,
        gogoTenjinNeoPreviousHighFail,
        gogoTenjinNeoTreatmentDone,
        gogoTenjinNeoLowGames,
        gogoTenjinNeoOverused,
        gogoTenjinNeoGenuinePrevious,
        gogoTenjinNeoUnpaid,
        gogoTenjinNeoLongNeglect,
        gogoTenjinNeoBbBiasedOutput,
        treatmentDone: gogoTenjinNeoTreatmentDone,
        lowConfidence: gogoTenjinNeoLowGames,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    if (activeLogicKey === "amuse-asakusa-neo-aim") {
      const amuseAsakusaNeoHistoryReady = historyRowCount >= 21;
      const recentTenAngle = netPerThousandGames(recentTenNetTotal, recentTenGamesTotal);
      const recentTwentyOneCombinedDenominator = rateDenominator(
        recentTwentyOneGamesTotal,
        readNumber(metrics.recentTwentyOneBonusTotal),
      );
      const amuseAsakusaNeoSinkStayStrong =
        recentFourteenMinus1500StayDays >= 7 || recentTwentyOneMinus2000StayDays >= 5;
      const amuseAsakusaNeoAngleStrong =
        recentTenAngle <= -30 || features.recentTwentyOneAngle <= -20;
      const amuseAsakusaNeoUnpaid =
        (recentTwentyOneNetTotal <= -2000 && recentThreeNetTotal > 0) ||
        (previousMachineHighContent && recentFourteenNetTotal <= 0);
      const amuseAsakusaNeoGenuine =
        (previousMachineHighContent && previousDifference <= 2000) || features.previousRbDenominator <= 250;
      const amuseAsakusaNeoTrustedGames = recentTenGamesTotal >= 15000 && recentFiveGamesTotal >= 6000;
      const amuseAsakusaNeoTreatmentDone =
        recentSevenNetTotal >= 5000 || recentFiveNetTotal >= 4000 || recentTwentyOneNetTotal >= 10000;
      const amuseAsakusaNeoLowGames = recentTenGamesTotal < 8000 || recentFiveGamesTotal < 4000;
      const amuseAsakusaNeoLongNeglect =
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent >= 22 &&
        recentTwentyOneMachineHighContentCount === 0 &&
        recentTwentyOneNetTotal > -2000;
      const amuseAsakusaNeoOutputOnly =
        previousDifference >= 2000 && features.previousRbDenominator > 300;
      const amuseAsakusaNeoNearbyTooHot = adjacentMachineHighContentCount3Near2 >= 3;
      const boostFlags = [
        amuseAsakusaNeoSinkStayStrong,
        amuseAsakusaNeoAngleStrong,
        amuseAsakusaNeoUnpaid,
        amuseAsakusaNeoGenuine,
        amuseAsakusaNeoTrustedGames,
      ];
      const dangerFlags = [
        amuseAsakusaNeoTreatmentDone,
        amuseAsakusaNeoLowGames,
        amuseAsakusaNeoLongNeglect,
        amuseAsakusaNeoOutputOnly,
        amuseAsakusaNeoNearbyTooHot,
      ];

      return {
        ...features,
        recentTenAngle,
        recentTwentyOneCombinedDenominator,
        amuseAsakusaNeoHistoryReady,
        amuseAsakusaNeoSinkStayStrong,
        amuseAsakusaNeoAngleStrong,
        amuseAsakusaNeoUnpaid,
        amuseAsakusaNeoGenuine,
        amuseAsakusaNeoTrustedGames,
        amuseAsakusaNeoTreatmentDone,
        amuseAsakusaNeoLowGames,
        amuseAsakusaNeoLongNeglect,
        amuseAsakusaNeoOutputOnly,
        amuseAsakusaNeoNearbyTooHot,
        treatmentDone: amuseAsakusaNeoTreatmentDone,
        lowConfidence: amuseAsakusaNeoLowGames,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    const buildBeamHikariNeoFeatureState = (logicKey) => {
      const beamHikariNeoHistoryReady = historyRowCount >= 21;
      const recentTwoRbTotal = readNumber(metrics.recentTwoRbTotal);
      const recentTwoBonusTotal = readNumber(metrics.recentTwoBonusTotal);
      const recentTwoCombinedDenominator = rateDenominator(recentTwoGamesTotal, recentTwoBonusTotal);
      const recentTwoRbDenominator = rateDenominator(recentTwoGamesTotal, recentTwoRbTotal);
      const beamHikariNeoShortSteepSink =
        features.recentTwoAngle <= -500 && recentTwoCombinedDenominator >= 225;
      const beamHikariNeoCompromiseLowExposure =
        recentTwoGamesTotal > 0 && recentTwoGamesTotal <= 2500 && recentTwoCombinedDenominator >= 207;
      const beamHikariNeoTwoDayVeryBadCombined = recentTwoCombinedDenominator >= 260;
      const beamHikariNeoSevenDaySink =
        recentSevenNetTotal <= -2500 && recentSevenGamesTotal >= 17000;
      const beamHikariNeoTwentyOneDaySink =
        recentTwentyOneNetTotal <= -3000 &&
        recentThreeNetTotal < 1500 &&
        recentTwentyOneGamesTotal >= 45000;
      const beamHikariNeoLosingFit =
        streak >= 2 && streak <= 4 && recentSevenGamesTotal >= 15000;
      const beamHikariNeoRotationFit =
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent >= 4 &&
        daysSinceMachineHighContent <= 12 &&
        recentFourteenGamesTotal >= 25000;
      const beamHikariNeoRecentHighOnce =
        recentFourteenMachineHighContentCount === 1 &&
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent >= 4 &&
        daysSinceMachineHighContent <= 10;
      const beamHikariNeoNearbyLeftBehind =
        previousAdjacentMachineHighContentCount > 0 &&
        recentSevenNetTotal < 0 &&
        recentSevenGamesTotal >= 12000;
      const beamHikariNeoPreviousLowGames = previousGames > 0 && previousGames <= 2018;
      const beamHikariNeoReferenceSp =
        recentTwoNetTotal >= -1100 &&
        recentTwoNetTotal <= -500 &&
        previousGames >= 950 &&
        previousGames <= 2018;
      const beamHikariNeoMiddleSinkDay3 =
        recentTwoCombinedDenominator >= 194 &&
        recentTwoNetTotal >= -1100 &&
        recentTwoNetTotal <= -500 &&
        daysSinceMachineHighContent === 3;
      const beamHikariNeoLowExposureMiddleSink =
        recentTwoGamesTotal <= 3600 &&
        recentTwoNetTotal >= -1100 &&
        recentTwoNetTotal <= -500 &&
        previousGames <= 2018;
      const beamHikariNeoFreeA = beamHikariNeoShortSteepSink;
      const beamHikariNeoFreeB = beamHikariNeoShortSteepSink && beamHikariNeoSevenDaySink;
      const beamHikariNeoFreeC =
        features.recentTwoAngle <= -500 &&
        beamHikariNeoTwoDayVeryBadCombined &&
        beamHikariNeoSevenDaySink &&
        streak >= 2 &&
        streak <= 4;
      const beamHikariNeoPreviousHighUntreated =
        previousMachineHighContent && previousDifference < 1000 && previousGames >= 3000;
      const beamHikariNeoPreviousStrongHighDone =
        previousMachineStrongHighContent && previousGames >= 4000;
      const beamHikariNeoThreeDayTreatmentDone =
        recentThreeNetTotal >= 2500 && recentThreeGamesTotal >= 7000;
      const beamHikariNeoSevenDayTreatmentDone =
        recentSevenNetTotal >= 4000 && recentSevenGamesTotal >= 15000;
      const beamHikariNeoRecentHighTooMany =
        recentSevenMachineHighContentCount >= 2 && recentSevenNetTotal >= 0;
      const beamHikariNeoLowConfidence = recentSevenGamesTotal < 5000;
      const beamHikariNeoOverExposure =
        recentTwoGamesTotal > 9000 ||
        previousGames > 5345 ||
        recentSevenGamesTotal > 26086 ||
        recentFourteenGamesTotal > 50028;
      const beamHikariNeoLongDeepNeglect =
        recentTwentyOneMachineHighContentCount === 0 &&
        recentTwentyOneGamesTotal >= 50000 &&
        recentTwentyOneNetTotal <= -6000;
      const boostFlags = [
        beamHikariNeoShortSteepSink,
        beamHikariNeoCompromiseLowExposure,
        beamHikariNeoTwoDayVeryBadCombined,
        beamHikariNeoSevenDaySink,
        beamHikariNeoTwentyOneDaySink,
        beamHikariNeoLosingFit,
        beamHikariNeoRotationFit,
        beamHikariNeoRecentHighOnce,
        beamHikariNeoNearbyLeftBehind,
        beamHikariNeoPreviousLowGames,
        beamHikariNeoMiddleSinkDay3,
        beamHikariNeoLowExposureMiddleSink,
      ];
      const dangerFlags = [
        beamHikariNeoPreviousHighUntreated,
        beamHikariNeoPreviousStrongHighDone,
        beamHikariNeoThreeDayTreatmentDone,
        beamHikariNeoSevenDayTreatmentDone,
        beamHikariNeoRecentHighTooMany,
        beamHikariNeoLowConfidence,
        beamHikariNeoOverExposure,
        beamHikariNeoLongDeepNeglect,
      ];

      return {
        ...features,
        recentTwoCombinedDenominator,
        recentTwoRbDenominator,
        beamHikariNeoHistoryReady,
        beamHikariNeoNormalHistoryReady:
          logicKey === "beam-hikari-neo-aim-normal" ? beamHikariNeoHistoryReady : false,
        beamHikariNeoEventHistoryReady:
          logicKey === "beam-hikari-neo-aim-event" ? beamHikariNeoHistoryReady : false,
        beamHikariNeoShortSteepSink,
        beamHikariNeoCompromiseLowExposure,
        beamHikariNeoTwoDayVeryBadCombined,
        beamHikariNeoSevenDaySink,
        beamHikariNeoTwentyOneDaySink,
        beamHikariNeoLosingFit,
        beamHikariNeoRotationFit,
        beamHikariNeoRecentHighOnce,
        beamHikariNeoNearbyLeftBehind,
        beamHikariNeoPreviousLowGames,
        beamHikariNeoReferenceSp,
        beamHikariNeoMiddleSinkDay3,
        beamHikariNeoLowExposureMiddleSink,
        beamHikariNeoFreeA,
        beamHikariNeoFreeB,
        beamHikariNeoFreeC,
        beamHikariNeoPreviousHighUntreated,
        beamHikariNeoPreviousStrongHighDone,
        beamHikariNeoThreeDayTreatmentDone,
        beamHikariNeoSevenDayTreatmentDone,
        beamHikariNeoRecentHighTooMany,
        beamHikariNeoLowConfidence,
        beamHikariNeoOverExposure,
        beamHikariNeoLongDeepNeglect,
        treatmentDone:
          beamHikariNeoThreeDayTreatmentDone ||
          beamHikariNeoSevenDayTreatmentDone ||
          beamHikariNeoPreviousHighUntreated,
        lowConfidence: beamHikariNeoLowConfidence,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    };

    if (activeLogicKey === "beam-hikari-neo-aim") {
      return buildBeamHikariNeoFeatureState(activeLogicKey);
    }

    if (activeLogicKey === "beam-hikari-neo-aim-event" || activeLogicKey === "beam-hikari-neo-aim-normal") {
      return buildBeamHikariNeoFeatureState(activeLogicKey);
    }

    if (activeLogicKey === "apark-yakatabaru-neo-aim") {
      const yakatabaruNeoHistoryReady = targetRangeHistoryRowCount >= 21;
      const yakatabaruNeoDeepSink =
        streak >= 2 && (recentThreeNetTotal <= -1450 || recentFiveNetTotal <= -1780);
      const yakatabaruNeoPreviousFail = previousMachineHighContent && previousDifference < 0;
      const yakatabaruNeoUnpaid =
        recentFourteenNetTotal <= -3000 ||
        recentTwentyOneNetTotal <= -5000 ||
        recentFiveMinus1000StayDays >= 2;
      const yakatabaruNeoWeakBonus =
        features.recentFiveCombinedDenominator >= 160 || features.recentSevenCombinedDenominator >= 162;
      const yakatabaruNeoTrustedGames = recentFiveGamesTotal >= 12000 && recentFiveGamesTotal <= 24000;
      const yakatabaruNeoIntervalGood =
        recentSevenMachineHighContentCount === 0 ||
        (Number.isFinite(daysSinceMachineHighContent) &&
          daysSinceMachineHighContent >= 3 &&
          daysSinceMachineHighContent <= 13);
      const yakatabaruNeoFreeMain = streak >= 4 && features.recentFiveCombinedDenominator >= 165;
      const yakatabaruNeoFreeStrong =
        streak >= 4 && features.recentFiveCombinedDenominator >= 160 && features.recentFiveAngle <= -120;
      const yakatabaruNeoFreeBalanced =
        streak >= 3 && features.recentFiveCombinedDenominator >= 160 && features.recentFiveAngle <= -90;
      const yakatabaruNeoBest106 =
        recentFiveNetTotal <= -1780 && features.recentFiveCombinedDenominator >= 160 && yakatabaruNeoPreviousFail;
      const yakatabaruNeoStrong105 =
        streak >= 3 && features.recentFiveAngle <= -120 && yakatabaruNeoPreviousFail;
      const yakatabaruNeoPreviousSmallShow =
        previousMachineHighContent && previousDifference >= 0 && previousDifference < 500;
      const yakatabaruNeoGoodPreviousSmallShow =
        previousMachineGoodContent && previousDifference >= 0 && previousDifference < 500;
      const yakatabaruNeoTreatmentDone =
        (previousMachineHighContent && previousDifference >= 1000) ||
        (previousMachineGoodContent && previousDifference >= 1000) ||
        previousDifference >= 1500 ||
        recentThreeNetTotal >= 1500 ||
        recentFiveNetTotal >= 2500 ||
        recentTwentyOneMachineGoodContentCount >= 4;
      const yakatabaruNeoPreviousHighOutput = previousMachineHighContent && previousDifference >= 1000;
      const yakatabaruNeoRecentOutput = recentThreeNetTotal >= 1500 || recentFiveNetTotal >= 2500;
      const yakatabaruNeoLowConfidence = targetRangeHistoryRowCount < 21 || recentFiveGamesTotal < 9000;
      const yakatabaruNeoNoLosing = streak === 0;
      const yakatabaruNeoOverused = recentTwentyOneMachineGoodContentCount >= 4;
      const boostFlags = [
        yakatabaruNeoDeepSink,
        yakatabaruNeoPreviousFail,
        yakatabaruNeoUnpaid,
        yakatabaruNeoWeakBonus,
        yakatabaruNeoTrustedGames,
        yakatabaruNeoIntervalGood,
      ];
      const dangerFlags = [
        yakatabaruNeoTreatmentDone,
        yakatabaruNeoPreviousHighOutput,
        yakatabaruNeoRecentOutput,
        yakatabaruNeoLowConfidence,
        yakatabaruNeoNoLosing,
        yakatabaruNeoOverused,
      ];

      return {
        ...features,
        yakatabaruNeoHistoryReady,
        yakatabaruNeoDeepSink,
        yakatabaruNeoPreviousFail,
        yakatabaruNeoUnpaid,
        yakatabaruNeoWeakBonus,
        yakatabaruNeoTrustedGames,
        yakatabaruNeoIntervalGood,
        yakatabaruNeoFreeMain,
        yakatabaruNeoFreeStrong,
        yakatabaruNeoFreeBalanced,
        yakatabaruNeoBest106,
        yakatabaruNeoStrong105,
        yakatabaruNeoPreviousSmallShow,
        yakatabaruNeoGoodPreviousSmallShow,
        yakatabaruNeoTreatmentDone,
        yakatabaruNeoPreviousHighOutput,
        yakatabaruNeoRecentOutput,
        yakatabaruNeoLowConfidence,
        yakatabaruNeoNoLosing,
        yakatabaruNeoOverused,
        treatmentDone: yakatabaruNeoTreatmentDone,
        lowConfidence: yakatabaruNeoLowConfidence,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    if (activeLogicKey === "mj-kurume-neo-aim") {
      const kurumeNeoHistoryReady = targetRangeHistoryRowCount >= 14;
      const kurumeNeoStrongSink =
        recentTwentyEightNetTotal <= -5000 ||
        recentTwentyOneNetTotal <= -3000 ||
        recentFourteenNetTotal <= -4000;
      const kurumeNeoSinkStay =
        recentSevenLossDays >= 7 ||
        recentFourteenLossDays >= 12 ||
        recentFourteenMinus2000StayDays >= 10 ||
        (streak >= 4 && recentFourteenNetTotal <= -1500);
      const kurumeNeoLosingReturn = streak >= 5 && recentFourteenNetTotal <= -1500 && recentFourteenGamesTotal >= 15000;
      const kurumeNeoPreviousHighUnpaid =
        previousGames >= 3000 && previousMachineHighContent && previousDifference < 500;
      const kurumeNeoPreviousStrongHighUnpaid =
        previousGames >= 3000 && previousMachineStrongHighContent && previousDifference < 500;
      const kurumeNeoGenuineBonus =
        previousGames >= 3000 &&
        features.previousRbDenominator <= 300 &&
        features.previousCombinedDenominator <= 140;
      const kurumeNeoRecentGenuine = recentThreeMachineHighContentCount >= 2 || recentSevenMachineHighContentCount >= 3;
      const kurumeNeoGenuine =
        kurumeNeoPreviousHighUnpaid || kurumeNeoPreviousStrongHighUnpaid || kurumeNeoGenuineBonus || kurumeNeoRecentGenuine;
      const kurumeNeoThreeDayHighGames = recentThreeGamesTotal >= 12000;
      const kurumeNeoGamesTrusted =
        recentThreeGamesTotal >= 10500 ||
        (recentFourteenGamesTotal >= 18000 && recentFourteenGamesTotal <= 45000);
      const kurumeNeoNearbyLeftBehind =
        adjacentMachineHighContentCount7 >= 2 && recentSevenMachineHighContentCount <= 1 && recentFourteenNetTotal <= 0;
      const kurumeNeoClusterUnpaid =
        recentFourteenMachineHighContentCount >= 3 &&
        recentFourteenMachineHighContentCount <= 5 &&
        recentFourteenNetTotal <= 0;
      const kurumeNeoRecentHighUnpaid = recentSevenMachineHighContentCount >= 2 && recentFourteenNetTotal <= 0;
      const kurumeNeoPostHighUnpaid =
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent >= 1 &&
        daysSinceMachineHighContent <= 3 &&
        recentFourteenNetTotal <= 0;
      const kurumeNeoRotationReturn =
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent >= 14 &&
        daysSinceMachineHighContent <= 21 &&
        recentTwentyOneNetTotal <= 0;
      const kurumeNeoStrongRotationReturn =
        Number.isFinite(daysSinceMachineStrongHighContent) &&
        daysSinceMachineStrongHighContent >= 28 &&
        recentTwentyEightNetTotal <= 0;
      const kurumeNeoTreatmentDone =
        recentTwentyOneNetTotal >= 5000 || recentFourteenNetTotal >= 4000 || recentSevenNetTotal >= 3000;
      const kurumeNeoPreviousHighOutput =
        previousGames >= 3000 && previousMachineHighContent && previousDifference >= 1500;
      const kurumeNeoOverheated =
        recentTwentyEightNetTotal >= 4000 || (recentFourteenLossDays <= 4 && recentFourteenNetTotal >= 1000);
      const kurumeNeoLowEvidence =
        (recentThreeMachineHighContentCount === 0 && recentFourteenNetTotal > -1000) ||
        (recentFourteenGamesTotal < 8000 && recentFourteenNetTotal > -2000);
      const kurumeNeoBbOnly =
        previousDifference >= 1000 &&
        !previousMachineHighContent &&
        features.previousRbDenominator >= 400;
      const kurumeNeoNearbyLead = previousAdjacentMachineHighContentCount >= 2;
      const boostFlags = [
        kurumeNeoStrongSink,
        kurumeNeoSinkStay,
        kurumeNeoLosingReturn,
        kurumeNeoPreviousHighUnpaid,
        kurumeNeoPreviousStrongHighUnpaid,
        kurumeNeoGenuineBonus,
        kurumeNeoRecentGenuine,
        kurumeNeoGamesTrusted,
        kurumeNeoNearbyLeftBehind,
        kurumeNeoClusterUnpaid,
        kurumeNeoRecentHighUnpaid,
        kurumeNeoPostHighUnpaid,
        kurumeNeoRotationReturn,
        kurumeNeoStrongRotationReturn,
      ];
      const dangerFlags = [
        kurumeNeoTreatmentDone,
        kurumeNeoPreviousHighOutput,
        kurumeNeoOverheated,
        kurumeNeoLowEvidence,
        kurumeNeoBbOnly,
        kurumeNeoNearbyLead,
      ];

      return {
        ...features,
        kurumeNeoHistoryReady,
        kurumeNeoStrongSink,
        kurumeNeoSinkStay,
        kurumeNeoLosingReturn,
        kurumeNeoPreviousHighUnpaid,
        kurumeNeoPreviousStrongHighUnpaid,
        kurumeNeoGenuineBonus,
        kurumeNeoRecentGenuine,
        kurumeNeoGenuine,
        kurumeNeoThreeDayHighGames,
        kurumeNeoGamesTrusted,
        kurumeNeoNearbyLeftBehind,
        kurumeNeoClusterUnpaid,
        kurumeNeoRecentHighUnpaid,
        kurumeNeoPostHighUnpaid,
        kurumeNeoRotationReturn,
        kurumeNeoStrongRotationReturn,
        kurumeNeoTreatmentDone,
        kurumeNeoPreviousHighOutput,
        kurumeNeoOverheated,
        kurumeNeoLowEvidence,
        kurumeNeoBbOnly,
        kurumeNeoNearbyLead,
        treatmentDone: kurumeNeoTreatmentDone,
        lowConfidence: kurumeNeoLowEvidence,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    const aimThreeSinkStayDays = readNumber(metrics.recentThreeMinus1700StayDays);
    const aimShortSinkStay2 = aimThreeSinkStayDays >= 2;
    const aimShortSinkStay3 = aimThreeSinkStayDays >= 3;
    const neoAimKasugaMiddleMissTrigger =
      previousGames >= 4500 &&
      features.previousRbDenominator >= 300 &&
      features.previousRbDenominator <= 360 &&
      features.previousCombinedDenominator >= 155 &&
      previousDifference < 0 &&
      recentSevenGamesTotal >= 35000 &&
      recentSevenLossDays >= 6 &&
      streak >= 4;
    const neoAimKasugaDeepLosingTrigger =
      streak >= 5 &&
      recentThreeNetTotal <= -1500 &&
      previousGames > 0 &&
      features.previousCombinedDenominator >= 155;
    const neoAimKasugaSevenSinkTrigger =
      recentSevenNetTotal <= -4500 &&
      streak >= 3 &&
      recentSevenGamesTotal >= 35000;

    return {
      ...features,
      aimShortSinkStay2,
      aimShortSinkStay3,
      neoAimKasugaMiddleMissTrigger,
      neoAimKasugaDeepLosingTrigger,
      neoAimKasugaSevenSinkTrigger,
    };
  }

  if (machineKey === "girls") {
    if (
      activeLogicKey === "beam-hikari-girls" ||
      activeLogicKey === "beam-hikari-girls-normal" ||
      activeLogicKey === "beam-hikari-girls-event"
    ) {
      const beamHikariGirlsHistoryReady = targetRangeHistoryRowCount >= 7;
      const beamHikariGirlsPreviousShow = previousMachineHighContent || previousMachineGoodContent;
      const beamHikariGirlsShortBonusWeak =
        (features.recentThreeCombinedDenominator >= 180 && features.recentThreeRbDenominator >= 400) ||
        features.recentThreeRbDenominator >= 500;
      const beamHikariGirlsSevenBonusWeak =
        (features.recentSevenCombinedDenominator >= 170 && features.recentSevenRbDenominator >= 350) ||
        features.recentSevenRbDenominator >= 470;
      const beamHikariGirlsFiveBonusWeak =
        features.recentFiveCombinedDenominator >= 170 && features.recentFiveRbDenominator >= 350;
      const beamHikariGirlsShortSink =
        recentThreeNetTotal <= -1050 ||
        features.recentThreeAngle <= -205 ||
        (activeLogicKey === "beam-hikari-girls-event" &&
          (recentFiveNetTotal <= -2100 || features.recentFiveAngle <= -160));
      const beamHikariGirlsMiddleSink =
        recentSevenNetTotal <= -1700 ||
        (activeLogicKey === "beam-hikari-girls-event" && recentSevenNetTotal <= -2500) ||
        features.recentSevenAngle <= -110;
      const beamHikariGirlsRotationGood =
        (Number.isFinite(daysSinceMachineHighContent) &&
          daysSinceMachineHighContent >= 8 &&
          daysSinceMachineHighContent <= 14) ||
        (activeLogicKey === "beam-hikari-girls-event" &&
          Number.isFinite(daysSinceMachineHighContent) &&
          daysSinceMachineHighContent >= 15 &&
          daysSinceMachineHighContent <= 28);
      const beamHikariGirlsStreakGood =
        activeLogicKey === "beam-hikari-girls-event"
          ? streak >= 2 && streak <= 9 && streak !== 5
          : streak >= 2 && streak <= 9;
      const beamHikariGirlsGamesTrusted =
        activeLogicKey === "beam-hikari-girls-event"
          ? recentSevenGamesTotal >= 7000 && recentSevenGamesTotal <= 21000
          : recentSevenGamesTotal >= 4500 && recentSevenGamesTotal <= 18000;
      const beamHikariGirlsNearbyLeftBehind =
        recentSevenNetTotal < 0 &&
        (previousAdjacentMachineHighContentCount > 0 ||
          adjacentMachineHighContentCount7Near2 > 0 ||
          adjacentMachineNetTotal7Near2 > 0);
      const beamHikariGirlsUnpaid =
        recentFourteenNetTotal < 0 &&
        recentTwentyOneNetTotal < 500 &&
        recentSevenMachineHighContentCount <= 1;
      const beamHikariGirlsTreatmentDone =
        beamHikariGirlsPreviousShow ||
        previousDifference >= 1000 ||
        recentThreeNetTotal >= 1200 ||
        recentSevenMachineHighContentCount >= 2;
      const beamHikariGirlsOverVisible =
        (activeLogicKey === "beam-hikari-girls-event" &&
          recentSevenGamesTotal >= 23000 &&
          recentSevenNetTotal > 0) ||
        (activeLogicKey === "beam-hikari-girls-normal" && recentSevenGamesTotal >= 20500);
      const beamHikariGirlsBadStreak = streak === 0 || streak >= 10;
      const beamHikariGirlsMainHistoryReady = beamHikariGirlsHistoryReady;
      const beamHikariGirlsEventHistoryReady = beamHikariGirlsHistoryReady;
      const beamHikariGirlsNormalHistoryReady = beamHikariGirlsHistoryReady;

      if (activeLogicKey === "beam-hikari-girls") {
        const beamHikariGirlsMainSink =
          recentThreeNetTotal <= -1100 || recentFiveNetTotal <= -1400 || recentSevenNetTotal <= -1700;
        const beamHikariGirlsMainAngle =
          features.recentThreeAngle <= -205 || features.recentFiveAngle <= -142;
        const beamHikariGirlsMainBonusWeak =
          (features.recentSevenCombinedDenominator >= 180 && features.recentSevenRbDenominator >= 400) ||
          (features.recentFiveCombinedDenominator >= 180 && features.recentFiveRbDenominator >= 400) ||
          (features.recentThreeCombinedDenominator >= 180 && features.recentThreeRbDenominator >= 400) ||
          features.recentSevenRbDenominator >= 450;
        const beamHikariGirlsMainIntervalGood =
          (Number.isFinite(daysSinceMachineHighContent) &&
            daysSinceMachineHighContent >= 8 &&
            daysSinceMachineHighContent <= 14) ||
          (Number.isFinite(daysSinceMachineHighContent) &&
            daysSinceMachineHighContent >= 15 &&
            daysSinceMachineHighContent <= 28) ||
          recentSevenMachineHighContentCount === 0 ||
          recentSevenMachineGoodContentCount === 0;
        const beamHikariGirlsMainTrusted =
          targetRangeHistoryRowCount >= 14 && recentSevenGamesTotal >= 7400 && recentSevenGamesTotal <= 18000;
        const beamHikariGirlsMainUnpaid =
          recentFourteenNetTotal < 0 &&
          recentTwentyOneNetTotal < 500 &&
          recentFourteenMachineGoodContentCount <= 1;
        const beamHikariGirlsMainTreatmentDone =
          beamHikariGirlsPreviousShow ||
          previousDifference >= 1000 ||
          recentThreeNetTotal >= 1000 ||
          recentFiveNetTotal >= 1500 ||
          recentSevenNetTotal >= 2600 ||
          recentSevenMachineGoodContentCount >= 2 ||
          recentSevenMachineHighContentCount >= 2;
        const beamHikariGirlsMainBoostFlags = [
          beamHikariGirlsMainSink,
          beamHikariGirlsMainAngle,
          beamHikariGirlsMainBonusWeak,
          beamHikariGirlsMainIntervalGood,
          streak >= 2 && streak <= 9,
          beamHikariGirlsMainTrusted,
          beamHikariGirlsMainUnpaid,
          beamHikariGirlsNearbyLeftBehind,
        ];
        const beamHikariGirlsMainDangerFlags = [
          beamHikariGirlsMainTreatmentDone,
          !beamHikariGirlsMainTrusted,
          streak === 0 || streak >= 10,
          previousDifference >= 500,
        ];

        return {
          ...features,
          beamHikariGirlsHistoryReady,
          beamHikariGirlsMainHistoryReady,
          beamHikariGirlsMainSink,
          beamHikariGirlsMainAngle,
          beamHikariGirlsMainBonusWeak,
          beamHikariGirlsMainIntervalGood,
          beamHikariGirlsMainTrusted,
          beamHikariGirlsMainUnpaid,
          treatmentDone: beamHikariGirlsMainTreatmentDone,
          lowConfidence: !beamHikariGirlsHistoryReady || !beamHikariGirlsMainTrusted,
          boostCount: beamHikariGirlsMainBoostFlags.filter(Boolean).length,
          dangerCount: beamHikariGirlsMainDangerFlags.filter(Boolean).length,
        };
      }

      const boostFlags = [
        beamHikariGirlsShortSink,
        beamHikariGirlsShortBonusWeak || beamHikariGirlsSevenBonusWeak || beamHikariGirlsFiveBonusWeak,
        beamHikariGirlsRotationGood,
        beamHikariGirlsStreakGood,
        beamHikariGirlsUnpaid,
        beamHikariGirlsNearbyLeftBehind,
        beamHikariGirlsGamesTrusted,
      ];
      const dangerFlags = [
        beamHikariGirlsTreatmentDone,
        beamHikariGirlsOverVisible,
        beamHikariGirlsBadStreak,
        previousDifference >= 500,
      ];

      return {
        ...features,
        beamHikariGirlsHistoryReady,
        beamHikariGirlsMainHistoryReady,
        beamHikariGirlsEventHistoryReady,
        beamHikariGirlsNormalHistoryReady,
        beamHikariGirlsShortBonusWeak,
        beamHikariGirlsSevenBonusWeak,
        beamHikariGirlsFiveBonusWeak,
        beamHikariGirlsShortSink,
        beamHikariGirlsMiddleSink,
        beamHikariGirlsRotationGood,
        beamHikariGirlsStreakGood,
        beamHikariGirlsGamesTrusted,
        beamHikariGirlsNearbyLeftBehind,
        beamHikariGirlsUnpaid,
        beamHikariGirlsTreatmentDone,
        beamHikariGirlsOverVisible,
        treatmentDone: beamHikariGirlsTreatmentDone,
        lowConfidence: !beamHikariGirlsHistoryReady,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    if (activeLogicKey === "mj-kurume-girls") {
      const kurumeGirlsHistoryReady = historyRowCount >= 14;
      const kurumeGirlsSinkStayStrong =
        recentSevenNetTotal <= -1500 ||
        recentFourteenNetTotal <= -1500 ||
        recentTwentyOneNetTotal <= -1500;
      const kurumeGirlsAngleStrong =
        features.recentSevenAngle <= -70 || features.recentFourteenAngle <= -20;
      const kurumeGirlsTreatmentDone =
        recentFiveNetTotal >= 1500 ||
        recentSevenNetTotal >= 2500 ||
        (previousMachineHighContent && previousDifference >= 2000);
      const kurumeGirlsUnpaid =
        (recentFourteenNetTotal <= 0 || recentTwentyOneNetTotal <= 0) && !kurumeGirlsTreatmentDone;
      const kurumeGirlsGenuineBonus =
        (previousMachineHighContent && previousDifference <= 1000) ||
        (previousMachineStrongHighContent && previousDifference <= 1500) ||
        (recentThreeGamesTotal >= 3000 &&
          features.recentThreeCombinedDenominator <= 170 &&
          features.recentThreeRbDenominator <= 360);
      const kurumeGirlsTrustedGames =
        recentSevenGamesTotal >= 8000 && recentFourteenGamesTotal >= 25000;
      const kurumeGirlsPreviousHighOutput =
        previousMachineHighContent && previousDifference >= 2000;
      const kurumeGirlsLongNeglect =
        Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 15;
      const kurumeGirlsLowConfidence = !kurumeGirlsHistoryReady || recentSevenGamesTotal < 8000;
      const kurumeGirlsOverVisible = recentSevenGamesTotal >= 25000 && recentFiveNetTotal >= 1500;
      const boostFlags = [
        kurumeGirlsSinkStayStrong,
        kurumeGirlsAngleStrong,
        kurumeGirlsUnpaid,
        kurumeGirlsGenuineBonus,
      ];
      const dangerFlags = [
        kurumeGirlsTreatmentDone,
        kurumeGirlsPreviousHighOutput,
        kurumeGirlsLongNeglect,
        kurumeGirlsLowConfidence,
        kurumeGirlsOverVisible,
      ];

      return {
        ...features,
        kurumeGirlsHistoryReady,
        kurumeGirlsSinkStayStrong,
        kurumeGirlsAngleStrong,
        kurumeGirlsUnpaid,
        kurumeGirlsGenuineBonus,
        kurumeGirlsTrustedGames,
        kurumeGirlsTreatmentDone,
        kurumeGirlsPreviousHighOutput,
        kurumeGirlsLongNeglect,
        kurumeGirlsLowConfidence,
        kurumeGirlsOverVisible,
        treatmentDone: kurumeGirlsTreatmentDone,
        lowConfidence: kurumeGirlsLowConfidence,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }
  }

  if (machineKey === "mister") {
    const misterHistoryReady = historyRowCount >= 30;
    const misterTreatmentDone =
      recentSevenNetTotal > 2500 ||
      recentFourteenNetTotal > 3600 ||
      previousDifference > 1500 ||
      (previousMachineHighContent && previousDifference > 500);
    const misterLongNeglect = recentSevenMinus1500StayDays >= 7 || recentThirtyMinus2700StayDays >= 7;
    const misterLowUsage = recentThreeGamesTotal < 6000 || recentSevenGamesTotal < 18000;
    const misterRecentHighDone = recentSevenMachineHighContentCount >= 2;
    const misterPreviousHighPlus = previousMachineHighContent && previousDifference > 0;
    const boostFlags = [
      recentThreeNetTotal <= -1376 || streak >= 3,
      features.recentThreeAngle <= -100 || features.recentSevenAngle <= -45,
      recentFourteenNetTotal <= -1400 || recentTwentyOneNetTotal <= -2450,
      recentThreeGamesTotal >= 12000 || recentSevenGamesTotal >= 30000,
      (Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 16) ||
        (Number.isFinite(daysSinceMachineStrongHighContent) && daysSinceMachineStrongHighContent >= 30),
      adjacentMachineHighContentCount7 > 0 && recentSevenNetTotal < 0,
      features.previousCombinedDenominator <= 135 &&
        features.previousRbDenominator <= 290 &&
        previousDifference <= 1000,
    ];
    const dangerFlags = [
      misterTreatmentDone,
      misterLongNeglect,
      misterLowUsage,
      misterRecentHighDone,
      misterPreviousHighPlus,
    ];

    return {
      ...features,
      misterHistoryReady,
      misterTreatmentDone,
      misterLongNeglect,
      misterLowUsage,
      misterRecentHighDone,
      misterPreviousHighPlus,
      boostCount: boostFlags.filter(Boolean).length,
      dangerCount: dangerFlags.filter(Boolean).length,
    };
  }

  if (machineKey === "gogo") {
    if (
      activeLogicKey === "beam-hikari-gogo" ||
      activeLogicKey === "beam-hikari-gogo-normal" ||
      activeLogicKey === "beam-hikari-gogo-event"
    ) {
      const beamHikariGogoHistoryReady = targetRangeHistoryRowCount >= 7;
      const beamHikariGogoPreviousWeak =
        previousGames >= 1500 &&
        (previousDifference <= -500 ||
          features.previousCombinedDenominator >= 170 ||
          features.previousRbDenominator >= 450);
      const beamHikariGogoBonusWeak =
        features.recentThreeCombinedDenominator >= 180 ||
        features.recentThreeRbDenominator >= 500 ||
        beamHikariGogoPreviousWeak;
      const beamHikariGogoAngleStrong = features.recentThreeAngle <= -200;
      const beamHikariGogoTwentyOneSinkBand =
        recentTwentyOneNetTotal >= -8000 && recentTwentyOneNetTotal <= -4000;
      const beamHikariGogoRecentThreeGamesMiddle =
        recentThreeGamesTotal >= 2000 && recentThreeGamesTotal <= 7000;
      const beamHikariGogoRecentThreeRbVeryWeak = features.recentThreeRbDenominator >= 500;
      const beamHikariGogoNearbyShow =
        adjacentMachineHighContentCount7 > 0 ||
        adjacentMachineHighContentCount7Near2 > 0 ||
        previousAdjacentMachineHighContentCount > 0;
      const beamHikariGogoTreatmentDone =
        previousMachineHighContent ||
        previousDifference >= 1500 ||
        recentSevenNetTotal >= 1500 ||
        recentTwentyOneNetTotal >= 6000;
      const beamHikariGogoLowInfo = recentThreeGamesTotal < 2000;
      const beamHikariGogoLongRest =
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent >= 21 &&
        daysSinceMachineHighContent <= 30;
      const beamHikariGogoMainHistoryReady = beamHikariGogoHistoryReady;
      const beamHikariGogoNormalHistoryReady = beamHikariGogoHistoryReady;
      const beamHikariGogoEventHistoryReady = beamHikariGogoHistoryReady;
      const boostFlags = [
        beamHikariGogoBonusWeak,
        beamHikariGogoAngleStrong,
        beamHikariGogoTwentyOneSinkBand,
        beamHikariGogoRecentThreeGamesMiddle,
        beamHikariGogoNearbyShow && (recentTwentyOneNetTotal < 0 || features.recentThreeAngle <= -200),
        streak >= 2 && streak <= 4,
        activeLogicKey === "beam-hikari-gogo-event" && beamHikariGogoLongRest,
      ];
      const dangerFlags = [
        beamHikariGogoTreatmentDone,
        beamHikariGogoLowInfo,
        recentFiveGamesTotal >= 20000,
        recentSevenGamesTotal >= 26000,
        features.recentThreeAngle >= 100,
        (activeLogicKey === "beam-hikari-gogo" || activeLogicKey === "beam-hikari-gogo-normal") &&
          streak >= 6,
      ];

      return {
        ...features,
        beamHikariGogoHistoryReady,
        beamHikariGogoMainHistoryReady,
        beamHikariGogoNormalHistoryReady,
        beamHikariGogoEventHistoryReady,
        beamHikariGogoPreviousWeak,
        beamHikariGogoBonusWeak,
        beamHikariGogoAngleStrong,
        beamHikariGogoTwentyOneSinkBand,
        beamHikariGogoRecentThreeGamesMiddle,
        beamHikariGogoRecentThreeRbVeryWeak,
        beamHikariGogoNearbyShow,
        beamHikariGogoTreatmentDone,
        beamHikariGogoLowInfo,
        beamHikariGogoLongRest,
        treatmentDone: beamHikariGogoTreatmentDone,
        lowConfidence: beamHikariGogoLowInfo,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    if (activeLogicKey === "mj-kurume-gogo") {
      const kurumeGogoHistoryReady = targetRangeHistoryRowCount >= 14;
      const kurumeGogoPreviousRbGood = previousGames >= 2000 && features.previousRbDenominator <= 255;
      const kurumeGogoComposite =
        recentFourteenNetTotal >= -3000 &&
        recentFourteenNetTotal <= 0 &&
        kurumeGogoPreviousRbGood &&
        recentTwentyOneNetTotal < 3500;
      const kurumeGogoRepay = recentFourteenNetTotal < 0 && recentTwentyOneNetTotal < 0;
      const kurumeGogoSinkStay = recentFourteenNegativeStayDays >= 7;
      const kurumeGogoGentleAngle = features.recentFourteenAngle >= -60 && features.recentFourteenAngle <= 0;
      const kurumeGogoPreviousHighFail = previousMachineHighContent && previousDifference <= 500;
      const kurumeGogoTrustedGames = recentFourteenGamesTotal >= 33000 && recentSevenGamesTotal >= 18000;
      const kurumeGogoNearbyLeftBehind = adjacentMachineNetTotal14 > 0 && recentFourteenNetTotal < 0;
      const kurumeGogoTreatmentDone = recentFourteenNetTotal >= 3000;
      const kurumeGogoAngleTooUp = features.recentFourteenAngle >= 70;
      const kurumeGogoPreviousHighOut = previousMachineHighContent && previousDifference >= 1000;
      const kurumeGogoPreviousLowUsage = previousGames < 800;
      const kurumeGogoRecentLowUsage = recentSevenGamesTotal < 12000;
      const kurumeGogoShowTooMany = recentSevenMachineHighContentCount >= 3;
      const kurumeGogoLongNeglect = streak >= 8 && recentFourteenGamesTotal < 33000;
      const kurumeGogoHistoryShort = targetRangeHistoryRowCount < 14;
      const boostFlags = [
        kurumeGogoRepay,
        kurumeGogoSinkStay,
        kurumeGogoGentleAngle,
        kurumeGogoPreviousHighFail,
        kurumeGogoTrustedGames,
        kurumeGogoNearbyLeftBehind,
        kurumeGogoComposite,
      ];
      const dangerFlags = [
        kurumeGogoTreatmentDone,
        kurumeGogoAngleTooUp,
        kurumeGogoPreviousHighOut,
        kurumeGogoPreviousLowUsage,
        kurumeGogoRecentLowUsage,
        kurumeGogoShowTooMany,
        kurumeGogoLongNeglect,
        kurumeGogoHistoryShort,
      ];

      return {
        ...features,
        kurumeGogoHistoryReady,
        kurumeGogoPreviousRbGood,
        kurumeGogoComposite,
        kurumeGogoRepay,
        kurumeGogoSinkStay,
        kurumeGogoGentleAngle,
        kurumeGogoPreviousHighFail,
        kurumeGogoTrustedGames,
        kurumeGogoNearbyLeftBehind,
        kurumeGogoTreatmentDone,
        kurumeGogoAngleTooUp,
        kurumeGogoPreviousHighOut,
        kurumeGogoPreviousLowUsage,
        kurumeGogoRecentLowUsage,
        kurumeGogoShowTooMany,
        kurumeGogoLongNeglect,
        treatmentDone: kurumeGogoTreatmentDone,
        lowConfidence: kurumeGogoPreviousLowUsage || kurumeGogoRecentLowUsage,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    const gogoHistoryReady = historyRowCount >= 7;
    const gogoLosingStreak3 = streak >= 3;
    const gogoHighRest4To14 =
      Number.isFinite(daysSinceMachineHighContent) &&
      daysSinceMachineHighContent >= 4 &&
      daysSinceMachineHighContent <= 14;
    const gogoThreeDayAngleStrong = features.recentThreeAngle <= -150;

    return {
      ...features,
      gogoHistoryReady,
      gogoLosingStreak3,
      gogoHighRest4To14,
      gogoThreeDayAngleStrong,
    };
  }

  if (machineKey === "star-hana") {
    const starPreviousCut = previousGames >= 500 && previousGames <= 2500;
    const starStrongSinkStay =
      recentFourteenMachineHighContentCount === 0 &&
      recentSevenNetTotal <= -1000 &&
      streak >= 2;
    const starStrongAngle =
      recentFourteenMachineHighContentCount === 0 &&
      features.recentSevenAngle <= -50 &&
      recentSevenGamesTotal >= 12000;
    const starNearbyLeftBehind =
      adjacentMachineHighContentCount7 > 0 &&
      recentSevenNetTotal < 0 &&
      recentFourteenMachineHighContentCount === 0;
    const starUnpaid =
      recentFourteenNetTotal < 0 &&
      previousDifference > 0 &&
      previousDifference < 1800;
    const starTrustedGames = recentSevenGamesTotal >= 12000 && recentFourteenGamesTotal >= 25000;
    const boostFlags = [
      starStrongSinkStay,
      starStrongAngle,
      starUnpaid,
      starNearbyLeftBehind,
      starTrustedGames,
      starPreviousCut,
    ];
    const dangerFlags = [
      recentSevenNetTotal > 1500,
      recentFourteenNetTotal > 5000,
      previousMachineGoodContent && previousDifference > 1200,
      previousMachineHighContent,
      recentFourteenMachineHighContentCount >= 2,
      streak >= 11 || (recentFourteenMachineHighContentCount === 0 && recentFourteenGamesTotal < 20000),
    ];

    return {
      ...features,
      starPreviousCut,
      starStrongSinkStay,
      starStrongAngle,
      starUnpaid,
      starNearbyLeftBehind,
      starTrustedGames,
      boostCount: boostFlags.filter(Boolean).length,
      dangerCount: dangerFlags.filter(Boolean).length,
    };
  }

  if (machineKey === "thunder") {
    const thunderHistoryReady = historyRowCount >= 14;
    const thunderTwoAngle = netPerThousandGames(recentTwoNetTotal, recentTwoGamesTotal);
    const thunderStrongAngle =
      (recentTwoGamesTotal >= 4000 && thunderTwoAngle <= -350) ||
      features.recentThreeAngle <= -300;
    const thunderTreatmentDone =
      previousMachineHighContent ||
      previousDifference >= 1500 ||
      recentThreeMachineHighContentCount >= 1;
    const thunderLongNeglect =
      streak >= 4 ||
      (Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent > 20);
    const thunderLowInfo = recentThreeGamesTotal < 5000 || previousGames < 1000;
    const thunderGamesDanger =
      recentThreeGamesTotal > 14000 || (previousGames > 4000 && previousGames <= 5000);
    const thunderBoostFlags = [
      thunderStrongAngle,
      recentTwoNetTotal <= -1600,
      recentThreeNetTotal <= -1500,
      streak >= 1 && streak <= 3,
      features.recentFiveRbDenominator >= 490,
      previousDifference > 0 && recentFourteenNetTotal < 0,
    ];
    const thunderDangerFlags = [
      thunderTreatmentDone,
      thunderLongNeglect,
      thunderLowInfo,
      thunderGamesDanger,
    ];

    return {
      ...features,
      strongAngle: thunderStrongAngle,
      thunderHistoryReady,
      treatmentDone: thunderTreatmentDone,
      lowConfidence: thunderLowInfo,
      boostCount: thunderBoostFlags.filter(Boolean).length,
      dangerCount: thunderDangerFlags.filter(Boolean).length,
    };
  }

  if (machineKey === "hokuto-tensei") {
    if (activeLogicKey === "beam-hikari-hokuto-tensei") {
      const beamHikariHokutoHistoryReady = targetRangeHistoryRowCount >= 21;
      const sevenSinkStayProxy =
        recentSevenNetTotal <= -5000
          ? recentSevenNetTotal <= -20000
            ? 6
            : recentSevenNetTotal <= -10000
              ? 4
              : 3
          : recentSevenNetTotal < 0
            ? 1
            : 0;
      const fourteenSinkStayProxy =
        recentFourteenNetTotal <= -8000
          ? 6
          : recentFourteenNetTotal <= -5000
            ? 4
            : 0;
      const leftBehindAmount = recentSevenNetTotal - adjacentMachineNetTotal7Near2 / 2;
      const boostFlags = [
        recentTwentyOneNetTotal >= -12000 && recentTwentyOneNetTotal <= -8000,
        sevenSinkStayProxy >= 3 && sevenSinkStayProxy <= 5,
        features.recentFourteenAngle >= -120 && features.recentFourteenAngle < 50,
        streak >= 3 && streak <= 4,
        previousGames >= 3000 && previousGames < 5000,
        leftBehindAmount >= -10000 && leftBehindAmount < -5000,
      ];
      const dangerFlags = [
        previousMachineHighContent,
        previousDifference >= 4000,
        previousGames >= 7000,
        sevenSinkStayProxy >= 6,
        streak >= 5 && streak <= 6,
        streak >= 8,
        recentFourteenGamesTotal < 50000,
        features.recentFourteenAngle >= 180,
      ];

      return {
        ...features,
        beamHikariHokutoMainHistoryReady: beamHikariHokutoHistoryReady,
        beamHikariHokutoSevenSinkStayProxy: sevenSinkStayProxy,
        beamHikariHokutoFourteenSinkStayProxy: fourteenSinkStayProxy,
        beamHikariHokutoLeftBehindAmount: leftBehindAmount,
        treatmentDone: dangerFlags.some(Boolean),
        lowConfidence: !beamHikariHokutoHistoryReady,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    if (
      activeLogicKey === "beam-hikari-hokuto-tensei-normal" ||
      activeLogicKey === "beam-hikari-hokuto-tensei-event"
    ) {
      const beamHikariHokutoHistoryReady = targetRangeHistoryRowCount >= 21;
      const sevenSinkStayProxy =
        recentSevenNetTotal <= -5000
          ? recentSevenNetTotal <= -20000
            ? 6
            : recentSevenNetTotal <= -10000
              ? 4
              : 3
          : recentSevenNetTotal < 0
            ? 1
            : 0;
      const fourteenSinkStayProxy =
        recentFourteenNetTotal <= -8000
          ? 6
          : recentFourteenNetTotal <= -5000
            ? 4
            : 0;
      const leftBehindAmount = recentSevenNetTotal - adjacentMachineNetTotal7Near2 / 2;
      const normalBoostFlags = [
        recentTwentyOneNetTotal >= -12000 && recentTwentyOneNetTotal <= -8000,
        previousGames >= 3000 && previousGames < 5000,
        streak === 3,
        sevenSinkStayProxy >= 3 && sevenSinkStayProxy <= 5,
        features.recentFourteenAngle >= -250 && features.recentFourteenAngle <= 50,
        recentFourteenMachineHighContentCount === 0,
      ];
      const normalDangerFlags = [
        previousMachineHighContent,
        previousGames >= 7000,
        recentTwentyOneNetTotal < -20000,
        streak >= 4 && streak <= 6,
        sevenSinkStayProxy >= 6,
        recentFourteenGamesTotal < 50000,
        features.recentFourteenAngle >= 180,
      ];
      const eventBoostFlags = [
        previousDifference < 0,
        recentTwentyOneNetTotal < -8000,
        recentSevenNetTotal < -10000,
        sevenSinkStayProxy >= 3 && sevenSinkStayProxy <= 5,
        streak === 4,
        leftBehindAmount < -5000,
      ];
      const eventDangerFlags = [
        previousMachineHighContent,
        previousDifference >= 4000,
        previousGames >= 7000,
        sevenSinkStayProxy >= 6,
        streak >= 5 && streak <= 6,
        features.recentFourteenAngle >= 180,
      ];
      const isEventLogic = activeLogicKey === "beam-hikari-hokuto-tensei-event";

      return {
        ...features,
        beamHikariHokutoNormalHistoryReady: beamHikariHokutoHistoryReady,
        beamHikariHokutoEventHistoryReady: beamHikariHokutoHistoryReady,
        beamHikariHokutoSevenSinkStayProxy: sevenSinkStayProxy,
        beamHikariHokutoFourteenSinkStayProxy: fourteenSinkStayProxy,
        beamHikariHokutoLeftBehindAmount: leftBehindAmount,
        treatmentDone: (isEventLogic ? eventDangerFlags : normalDangerFlags).some(Boolean),
        lowConfidence: !beamHikariHokutoHistoryReady,
        boostCount: (isEventLogic ? eventBoostFlags : normalBoostFlags).filter(Boolean).length,
        dangerCount: (isEventLogic ? eventDangerFlags : normalDangerFlags).filter(Boolean).length,
      };
    }

    const hokutoHistoryReady = historyRowCount >= 14;
    const hokutoSevenAngle = features.recentSevenAngle;
    const hokutoRestStrong =
      Number.isFinite(daysSinceMachineHighContent) &&
      daysSinceMachineHighContent >= 7 &&
      daysSinceMachineHighContent <= 10;
    const boostFlags = [
      recentSevenNetTotal <= -4850,
      hokutoSevenAngle <= -125,
      recentFourteenNetTotal <= -5660,
      streak >= 4,
      hokutoRestStrong,
      recentSevenGamesTotal >= 37800,
      previousMachineHighContent && previousDifference < 0,
    ];
    const dangerFlags = [
      recentSevenGamesTotal < 26955,
      recentSevenNetTotal > 5420,
      recentFourteenNetTotal > 10000,
      previousDifference >= 2500,
      previousMachineHighContent && previousDifference >= 2000,
      recentSevenMachineHighContentCount >= 4,
    ];

    return {
      ...features,
      hokutoHistoryReady,
      boostCount: boostFlags.filter(Boolean).length,
      dangerCount: dangerFlags.filter(Boolean).length,
      treatmentDone: dangerFlags.slice(1, 5).some(Boolean),
      lowConfidence: recentSevenGamesTotal < 26955,
    };
  }

  if (machineKey === "hokuto-base") {
    const isBeamHikariHokutoBaseLogic =
      activeLogicKey === "beam-hikari-hokuto-base" ||
      activeLogicKey === "beam-hikari-hokuto-base-normal" ||
      activeLogicKey === "beam-hikari-hokuto-base-event";
    if (isBeamHikariHokutoBaseLogic) {
      const beamHikariHokutoBaseHistoryReady = targetRangeHistoryRowCount >= 14;
      const weakContent5 = features.recentFiveCombinedDenominator > 150;
      const weakContent5Semi = features.recentFiveCombinedDenominator > 130;
      const previousWeakContent = features.previousCombinedDenominator > 162;
      const previousNearbyShow =
        previousAdjacentMachineHighContentCount > 0 || previousAdjacentMachineBigWin1000Count > 0;
      const eventBoostFlags = [
        recentFiveNetTotal <= -2300 || (recentFiveGamesTotal >= 4000 && features.recentFiveAngle <= -350),
        recentFourteenGamesTotal >= 15000 && features.recentFourteenAngle <= -195,
        weakContent5 || previousWeakContent,
        Number.isFinite(daysSinceMachineHighContent) &&
          daysSinceMachineHighContent >= 8 &&
          !previousMachineHighContent,
        (recentFiveGamesTotal >= 4500 && recentFiveGamesTotal <= 9000) || recentFourteenGamesTotal >= 17000,
      ];
      const eventDangerFlags = [
        previousMachineHighContent,
        previousDifference >= 1500 || recentFiveNetTotal > 2100 || recentFourteenNetTotal > 3888,
        features.previousCombinedDenominator <= 73 && previousGames >= 500,
        previousNearbyShow,
        recentFiveGamesTotal < 3000 || recentFourteenGamesTotal < 12000,
      ];
      const normalBoostFlags = [
        streak >= 6 || (recentFiveGamesTotal >= 3000 && features.recentFiveAngle <= -500) || previousDifference <= -2000,
        weakContent5 || previousWeakContent,
        recentFourteenNetTotal <= -3000 && recentFiveNetTotal <= -2300 && streak >= 4,
        recentFourteenMachineHighContentCount >= 4,
        (recentFiveGamesTotal >= 3000 && recentFiveGamesTotal <= 9000) ||
          (recentFourteenGamesTotal >= 12000 && recentFourteenGamesTotal <= 30000),
      ];
      const normalDangerFlags = [
        previousDifference > 0 || recentThreeNetTotal > 1000 || recentFiveNetTotal > 2100,
        features.previousCombinedDenominator <= 90 && previousGames >= 500,
        Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 30,
        recentFiveGamesTotal < 2000 || recentFourteenGamesTotal < 8000,
        previousNearbyShow,
      ];
      const isMainOrEventLogic =
        activeLogicKey === "beam-hikari-hokuto-base" || activeLogicKey === "beam-hikari-hokuto-base-event";
      const boostFlags = isMainOrEventLogic ? eventBoostFlags : normalBoostFlags;
      const dangerFlags = isMainOrEventLogic ? eventDangerFlags : normalDangerFlags;

      return {
        ...features,
        beamHikariHokutoBaseMainHistoryReady: beamHikariHokutoBaseHistoryReady,
        beamHikariHokutoBaseNormalHistoryReady: beamHikariHokutoBaseHistoryReady,
        beamHikariHokutoBaseEventHistoryReady: beamHikariHokutoBaseHistoryReady,
        beamHikariHokutoBasePreviousNearbyShow: previousNearbyShow,
        treatmentDone: dangerFlags.some(Boolean),
        lowConfidence: !beamHikariHokutoBaseHistoryReady,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }
  }

  if (machineKey === "dragon-hana") {
    const dragonStreak = historyLosingStreak;
    const dragonWeakBonus =
      features.recentSevenCombinedDenominator >= 190 ||
      features.recentSevenRbDenominator >= 800;
    const dragonUnpaid = previousDifference > 0 && recentFourteenNetTotal <= -1000;
    const dragonThirtySink = recentThirtyNetTotal <= -3000;
    const dragonIslandLeftBehind = recentSevenNetTotal <= -1000 && sameMachinePreviousNetTotal > 0;
    const dragonAdjacentSemiHigh = recentSevenNetTotal <= -1000 && previousAdjacentMachineGoodContentCount > 0;
    const dragonTreatmentDone =
      recentSevenNetTotal > 2000 ||
      features.recentSevenAngle > 100 ||
      previousDifference > 2500 ||
      previousMachineHighContent ||
      features.recentSevenCombinedDenominator <= 160 ||
      recentSevenGamesTotal >= 30000;
    const dragonLongNeglect =
      Number.isFinite(daysSinceMachineHighContent) &&
      daysSinceMachineHighContent >= 61 &&
      recentFourteenNetTotal > 0;
    const boostFlags = [
      dragonWeakBonus,
      dragonUnpaid,
      dragonThirtySink,
      dragonIslandLeftBehind,
      dragonAdjacentSemiHigh,
      dragonStreak >= 3,
      features.recentSevenAngle <= -40,
    ];
    const dangerFlags = [dragonTreatmentDone, dragonLongNeglect];

    return {
      ...features,
      weakBonus: dragonWeakBonus,
      dragonWeakBonus,
      dragonUnpaid,
      dragonThirtySink,
      dragonIslandLeftBehind,
      dragonAdjacentSemiHigh,
      treatmentDone: dragonTreatmentDone,
      boostCount: boostFlags.filter(Boolean).length,
      dangerCount: dangerFlags.filter(Boolean).length,
    };
  }

  if (machineKey === "new-king-hana") {
    const newKingHistoryReady = historyRowCount >= 21;
    const newKingStrongSink =
      recentTwentyOneNetTotal <= -8000 ||
      recentFourteenNetTotal <= -6000 ||
      (readNumber(metrics.recentThreeNetTotal) <= -3000 && recentThreeGamesTotal >= 6000);
    const newKingStrongAngle =
      features.recentFourteenAngle <= -110 ||
      features.recentSevenAngle <= -170;
    const newKingWeakBonus14 =
      recentFourteenGamesTotal >= 28000 &&
      features.recentFourteenCombinedDenominator >= 185 &&
      features.recentFourteenRbDenominator >= 420;
    const newKingUnpaid =
      recentTwentyOneNetTotal <= -8000 ||
      (recentFourteenNetTotal <= -4000 && readNumber(metrics.recentThreeNetTotal) <= 0);
    const newKingStrongStreak = streak >= 6;
    const newKingTrustedGames = recentSevenGamesTotal >= 17000 && recentThreeGamesTotal >= 6000;
    const boostFlags = [
      newKingStrongSink,
      newKingStrongAngle,
      newKingWeakBonus14,
      newKingUnpaid,
      newKingStrongStreak,
      newKingTrustedGames,
    ];
    const dangerFlags = [
      previousMachineHighContent && previousDifference >= 1500,
      recentTwentyOneNetTotal >= 6000,
      recentFiveNetTotal >= 3000,
      recentSevenGamesTotal < 17000,
      Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent > 30,
      previousDifference >= 1500,
    ];

    return {
      ...features,
      newKingHistoryReady,
      newKingStrongSink,
      newKingStrongAngle,
      newKingWeakBonus14,
      newKingUnpaid,
      newKingStrongStreak,
      newKingTrustedGames,
      boostCount: boostFlags.filter(Boolean).length,
      dangerCount: dangerFlags.filter(Boolean).length,
    };
  }

  if (machineKey === "monkey") {
    if (activeLogicKey === "beam-hikari-monkey") {
      const beamHikariMonkeyMainHistoryReady = targetRangeHistoryRowCount >= 14;
      const beamHikariMonkeyMainUnpaid14 =
        recentFourteenNetTotal >= -6500 && recentFourteenNetTotal <= 1900;
      const beamHikariMonkeyMainNearbyCold = adjacentMachineHighContentCount3 <= 1;
      const recentFiveMaxDifference = readNumber(metrics.recentFiveMaxDifference, previousDifference);
      const beamHikariMonkeyMainTreatmentDone =
        recentFourteenNetTotal > 5800 || recentSevenNetTotal > 4430 || previousDifference > 3000;
      const beamHikariMonkeyMainLowConfidence = recentFiveGamesTotal < 8000 || targetRangeHistoryRowCount < 14;
      const beamHikariMonkeyMainLongLosing = streak >= 8;
      const boostFlags = [
        beamHikariMonkeyMainUnpaid14,
        beamHikariMonkeyMainNearbyCold,
        Number.isFinite(daysSinceMachineHighContent) &&
          daysSinceMachineHighContent >= 3 &&
          daysSinceMachineHighContent <= 14,
        streak === 2 || (streak >= 6 && streak <= 7),
        previousGames >= 2900 && previousGames <= 5800,
        recentFiveGamesTotal >= 8000 && recentFiveGamesTotal <= 23500,
        recentSevenMachineHighContentCount <= 2,
        recentFiveMaxDifference < 5000 && recentSevenNetTotal <= 4430,
      ];
      const dangerFlags = [
        beamHikariMonkeyMainTreatmentDone,
        previousGames > 7000,
        recentSevenMachineHighContentCount >= 4,
        beamHikariMonkeyMainLongLosing,
        beamHikariMonkeyMainLowConfidence,
      ];

      return {
        ...features,
        beamHikariMonkeyMainHistoryReady,
        beamHikariMonkeyMainUnpaid14,
        beamHikariMonkeyMainNearbyCold,
        beamHikariMonkeyMainTreatmentDone,
        beamHikariMonkeyMainLowConfidence,
        treatmentDone: beamHikariMonkeyMainTreatmentDone,
        lowConfidence: beamHikariMonkeyMainLowConfidence,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    if (activeLogicKey === "beam-hikari-monkey-normal" || activeLogicKey === "beam-hikari-monkey-event") {
      const beamHikariMonkeyHistoryReady = targetRangeHistoryRowCount >= 14;
      const beamHikariMonkeyNormalUnpaid14 =
        recentFourteenNetTotal > -6500 && recentFourteenNetTotal <= 1900;
      const beamHikariMonkeyNormalCore14 =
        recentFourteenNetTotal > -2200 && recentFourteenNetTotal <= 1900;
      const beamHikariMonkeyNormalGapOk =
        Number.isFinite(daysSinceMachineHighContent) &&
        ((daysSinceMachineHighContent >= 3 && daysSinceMachineHighContent <= 4) ||
          (daysSinceMachineHighContent >= 8 && daysSinceMachineHighContent <= 14));
      const beamHikariMonkeyNormalDanger =
        recentFourteenNetTotal <= -10000 ||
        recentFourteenNetTotal > 1900 ||
        (recentSevenNetTotal > -7000 && recentSevenNetTotal <= -1750) ||
        recentSevenNetTotal > 4430 ||
        (streak >= 4 && streak <= 5) ||
        streak >= 8 ||
        (Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 15) ||
        (features.recentFiveCombinedDenominator >= 424 && features.recentFiveCombinedDenominator <= 481) ||
        previousDifference > 3000;
      const beamHikariMonkeyEventSink14 =
        recentFourteenNetTotal > -10000 && recentFourteenNetTotal <= -2200;
      const beamHikariMonkeyEventNearbyCold =
        adjacentMachineHighContentCount3 <= 1 ||
        (adjacentMachineNetTotal3 >= -5500 && adjacentMachineNetTotal3 <= -1800);
      const beamHikariMonkeyEventInterval =
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent >= 18 &&
        daysSinceMachineHighContent <= 42;
      const beamHikariMonkeyEventDanger =
        recentFourteenNetTotal > 5800 ||
        recentThreeNetTotal > 3000 ||
        previousDifference > 3000 ||
        previousGames > 7000 ||
        adjacentMachineHighContentCount3 >= 2;
      const normalBoostFlags = [
        beamHikariMonkeyNormalUnpaid14,
        beamHikariMonkeyNormalCore14,
        beamHikariMonkeyNormalGapOk,
        streak === 2,
        previousGames >= 2900 && previousGames <= 5800,
        recentFiveGamesTotal >= 8000 && recentFiveGamesTotal <= 17999,
        recentSevenMachineHighContentCount >= 1 && recentSevenMachineHighContentCount <= 2,
        adjacentMachineHighContentCount3 <= 1,
      ];
      const eventBoostFlags = [
        beamHikariMonkeyEventSink14,
        recentThreeNetTotal <= -3000,
        beamHikariMonkeyEventInterval,
        beamHikariMonkeyEventNearbyCold,
        recentSevenMachineHighContentCount <= 2,
      ];
      const isEventLogic = activeLogicKey === "beam-hikari-monkey-event";

      return {
        ...features,
        beamHikariMonkeyNormalHistoryReady: beamHikariMonkeyHistoryReady,
        beamHikariMonkeyEventHistoryReady: beamHikariMonkeyHistoryReady,
        beamHikariMonkeyNormalUnpaid14,
        beamHikariMonkeyNormalCore14,
        beamHikariMonkeyNormalGapOk,
        beamHikariMonkeyNormalDanger,
        beamHikariMonkeyEventSink14,
        beamHikariMonkeyEventNearbyCold,
        beamHikariMonkeyEventInterval,
        beamHikariMonkeyEventDanger,
        treatmentDone: isEventLogic ? beamHikariMonkeyEventDanger : beamHikariMonkeyNormalDanger,
        lowConfidence: targetRangeHistoryRowCount < 14,
        boostCount: (isEventLogic ? eventBoostFlags : normalBoostFlags).filter(Boolean).length,
        dangerCount: (isEventLogic ? [beamHikariMonkeyEventDanger] : [beamHikariMonkeyNormalDanger]).filter(Boolean).length,
      };
    }

    const monkeyHistoryReady = historyRowCount >= 21;
    const fourteenSinkStayDays = readNumber(metrics.recentFourteenMinus3218StayDays);
    const twentyOneSinkStayDays = readNumber(metrics.recentTwentyOneMinus11333StayDays);
    const monkeyFiveAngleBoost =
      recentFiveGamesTotal >= 19917 &&
      features.recentFiveAngle >= -261 &&
      features.recentFiveAngle <= -101;
    const monkeyMiddleSinkBoost =
      (recentTwentyOneNetTotal <= -11333 && readNumber(metrics.recentTwentyOneGamesTotal) >= 79247) ||
      (recentFourteenNetTotal <= -3218 && recentFourteenGamesTotal >= 51400) ||
      ((fourteenSinkStayDays >= 2 && fourteenSinkStayDays <= 3) || fourteenSinkStayDays >= 7) ||
      twentyOneSinkStayDays >= 2;
    const monkeyPreviousContentBoost =
      (previousMachineHighContent && previousDifference < 1000) ||
      previousMachineStrongHighContent;
    const monkeyRepayBoost =
      previousDifference > 0 &&
      (recentSevenNetTotal <= -3146 ||
        recentFourteenNetTotal <= -2305 ||
        recentTwentyOneNetTotal <= -394);
    const monkeyStreakBoost = streak === 4 || (streak >= 6 && streak <= 8) || streak >= 9;
    const monkeyRealContentBoost = previousGames >= 4852 && features.previousCombinedDenominator <= 411;
    const treatmentDone =
      recentFourteenNetTotal >= 4776 ||
      recentTenNetTotal >= 4195 ||
      recentSevenNetTotal >= 3310;
    const lowConfidence = recentFiveGamesTotal < 16802 || recentTenGamesTotal < 35852;
    const restedAfterHigh =
      Number.isFinite(daysSinceMachineHighContent) &&
      daysSinceMachineHighContent >= 2 &&
      daysSinceMachineHighContent <= 6;
    const previousHighOutputDone = previousMachineHighContent && previousDifference >= 1000;
    const boostFlags = [
      monkeyFiveAngleBoost,
      monkeyMiddleSinkBoost,
      monkeyPreviousContentBoost,
      monkeyRepayBoost,
      monkeyStreakBoost,
      monkeyRealContentBoost,
    ];
    const dangerFlags = [treatmentDone, lowConfidence, restedAfterHigh, previousHighOutputDone];

    return {
      ...features,
      monkeyHistoryReady,
      monkeyFiveAngleBoost,
      monkeyMiddleSinkBoost,
      monkeyPreviousContentBoost,
      monkeyRepayBoost,
      monkeyStreakBoost,
      monkeyRealContentBoost,
      treatmentDone,
      lowConfidence,
      boostCount: boostFlags.filter(Boolean).length,
      dangerCount: dangerFlags.filter(Boolean).length,
    };
  }

  if (machineKey === "okidoki-duo") {
    const okidokiDuoHistoryReady = historyRowCount >= 14;
    const okidokiDuoSinkCore =
      recentThreeNetTotal <= -4300 ||
      readNumber(metrics.recentTwoNetTotal) <= -4500 ||
      (features.recentFiveAngle <= -456 && recentFiveGamesTotal >= 4000);
    const okidokiDuoShortDeepSink =
      recentThreeNetTotal <= -5400 ||
      readNumber(metrics.recentTwoNetTotal) <= -4500 ||
      previousDifference <= -3000;
    const okidokiDuoRotationStrong =
      recentFourteenMachineHighContentCount === 0 ||
      (recentFourteenMachineHighContentCount <= 1 &&
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent >= 5 &&
        daysSinceMachineHighContent <= 21);
    const okidokiDuoNearbyLeftBehind =
      adjacentMachineHighContentCount14Near2 >= 2 ||
      (recentFiveNetTotal < 0 && adjacentMachineNetTotal5Near2 > 0);
    const okidokiDuoUntreated =
      features.recentFourteenCombinedDenominator >= 130 ||
      features.recentFourteenRbDenominator >= 381;
    const boostFlags = [
      okidokiDuoHistoryReady,
      okidokiDuoSinkCore,
      okidokiDuoShortDeepSink,
      okidokiDuoRotationStrong,
      okidokiDuoNearbyLeftBehind,
      okidokiDuoUntreated,
    ];
    const dangerFlags = [
      readNumber(metrics.recentTwoNetTotal) >= 6600,
      recentThreeNetTotal >= 7500,
      previousDifference >= 5000,
      recentThreeMachineHighContentCount >= 2,
      recentFourteenMachineHighContentCount >= 4,
      readNumber(metrics.recentTwoGamesTotal) < 1800,
      recentThreeGamesTotal < 2500,
      Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent > 21 &&
        recentThirtyMachineHighContentCount <= 1,
      features.recentFourteenCombinedDenominator <= 104,
      features.recentFourteenRbDenominator <= 326,
      readNumber(metrics.winningStreak) >= 2,
    ];

    return {
      ...features,
      okidokiDuoHistoryReady,
      okidokiDuoSinkCore,
      okidokiDuoShortDeepSink,
      okidokiDuoRotationStrong,
      okidokiDuoNearbyLeftBehind,
      okidokiDuoUntreated,
      boostCount: boostFlags.filter(Boolean).length,
      dangerCount: dangerFlags.filter(Boolean).length,
    };
  }

  if (machineKey === "okidoki-gold") {
    const longSinkBoost = recentFiftySixNetTotal <= -17086;
    const showShortageBoost = recentFourteenGoldShowDays <= 1;
    const rbWeakBoost = features.recentFourteenRbDenominator >= 535.6;
    const gamesTrustBoost = recentSevenGamesTotal >= 30020;
    const fewWinBoost = recentFourteenWinDays <= 3;
    const boostFlags = [
      longSinkBoost,
      showShortageBoost,
      rbWeakBoost,
      gamesTrustBoost,
      fewWinBoost,
    ];
    const dangerFlags = [
      previousDifference >= 2173,
      previousMachineStrongHighContent,
      machineHighContentStreak >= 2,
      recentFourteenGoldShowDays >= 6,
      recentFourteenMachineHighContentCount >= 6,
      recentSevenGamesTotal <= 23409,
      Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 26,
    ];

    return {
      ...features,
      longSinkBoost,
      showShortageBoost,
      rbWeakBoost,
      gamesTrustBoost,
      fewWinBoost,
      boostCount: boostFlags.filter(Boolean).length,
      dangerCount: dangerFlags.filter(Boolean).length,
    };
  }

  if (machineKey === "smart-hanabi") {
    const dangerFlags = [
      recentSevenMachineHighContentCount >= 4 ||
        recentFourteenMachineHighContentCount >= 4 ||
        previousDifference >= 3000,
      previousGames < 1400 || recentSevenGamesTotal < 19000,
      (streak >= 4 && streak <= 7 && features.recentThreeAngle < 0) ||
        features.recentThreeAngle <= -160,
      features.previousCombinedDenominator >= 218,
      features.previousRbDenominator >= 539,
      previousAdjacentMachineHighContentCount > 0,
    ];
    const boostFlags = [
      previousGames >= 2000 && features.previousCombinedDenominator <= 146 && features.previousRbDenominator <= 333,
      netPerThousandGames(recentTenNetTotal, recentTenGamesTotal) >= 30,
      Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent >= 8 &&
        daysSinceMachineHighContent <= 12,
      recentFourteenMachineHighContentCount === 3,
    ];

    return {
      ...features,
      boostCount: boostFlags.filter(Boolean).length,
      dangerCount: dangerFlags.filter(Boolean).length,
    };
  }

  if (machineKey === "houou") {
    const hououUnpaid28Weak7 =
      recentTwentyEightNetTotal <= -5800 &&
      features.recentSevenCombinedDenominator >= 190 &&
      recentSevenGamesTotal >= 15000;
    const hououSinkStayStrong =
      readNumber(metrics.recentSevenMinus3000StayDays) >= 4 &&
      recentSevenGamesTotal >= 15000;
    const hououSinkStay2 =
      readNumber(metrics.recentSevenMinus2000StayDays) >= 2 ||
      readNumber(metrics.recentFiveMinus500StayDays) >= 5;
    const hououTreatmentDone =
      (previousMachineHighContent && previousDifference >= 1500) ||
      recentThreeNetTotal >= 1500 ||
      recentSevenNetTotal >= 1800 ||
      recentFourteenNetTotal >= 3000;

    return {
      ...features,
      hououUnpaid28Weak7,
      hououSinkStayStrong,
      hououSinkStay2,
      treatmentDone: hououTreatmentDone,
    };
  }

  if (machineKey === "my") {
    if (
      activeLogicKey === "beam-hikari-my" ||
      activeLogicKey === "beam-hikari-my-normal" ||
      activeLogicKey === "beam-hikari-my-event"
    ) {
      const beamHikariMyHistoryReady = historyRowCount >= 21;
      const beamHikariMyTwoLoss = streak === 2;
      const beamHikariMyTwoLossOrMore = streak >= 2;
      const beamHikariMyNormalFourteenSinkStrong =
        recentFourteenNetTotal <= -6000 || features.recentFourteenAngle <= -100;
      const beamHikariMyNormalWeakContent =
        features.recentThreeCombinedDenominator >= 170 ||
        features.recentThreeRbDenominator >= 420 ||
        features.recentSevenCombinedDenominator >= 160 ||
        features.recentSevenRbDenominator >= 400;
      const beamHikariMyNormalTreatmentDone =
        previousMachineHighContent ||
        previousDifference >= 1000 ||
        recentSevenNetTotal >= 3000 ||
        recentFourteenNetTotal >= 3000 ||
        recentSevenMachineHighContentCount >= 2;
      const beamHikariMyNormalHighActivity =
        previousGames > 6000 || recentThreeGamesTotal > 16000;
      const beamHikariMyNormalStrongRecent =
        features.recentThreeCombinedDenominator <= 150 ||
        features.recentThreeRbDenominator <= 330 ||
        recentSevenMachineHighContentCount >= 2;
      const beamHikariMyNormalBoostFlags = [
        beamHikariMyTwoLoss,
        beamHikariMyNormalFourteenSinkStrong,
        beamHikariMyNormalWeakContent,
        recentSevenNetTotal <= -2000,
        Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 3,
      ];
      const beamHikariMyNormalDangerFlags = [
        beamHikariMyNormalTreatmentDone,
        beamHikariMyNormalHighActivity,
        beamHikariMyNormalStrongRecent,
      ];

      if (activeLogicKey === "beam-hikari-my") {
        const beamHikariMyMainWeakContent =
          features.recentThreeCombinedDenominator >= 170 ||
          features.recentThreeRbDenominator >= 420 ||
          features.recentSevenCombinedDenominator >= 160 ||
          features.recentSevenRbDenominator >= 400;
        const beamHikariMyMainSink =
          recentThreeNetTotal <= -1500 ||
          recentSevenNetTotal <= -3000 ||
          recentFourteenNetTotal <= -4000 ||
          features.recentThreeAngle <= -120 ||
          features.recentSevenAngle <= -80;
        const beamHikariMyMainOpen =
          recentSevenMachineHighContentCount <= 1 &&
          Number.isFinite(daysSinceMachineHighContent) &&
          daysSinceMachineHighContent >= 3;
        const beamHikariMyMainTreatmentDone =
          (Number.isFinite(daysSinceMachineHighContent) &&
            daysSinceMachineHighContent >= 1 &&
            daysSinceMachineHighContent <= 2) ||
          previousMachineHighContent ||
          previousDifference >= 2000;
        const beamHikariMyMainTooVisible =
          recentSevenMachineHighContentCount >= 2 ||
          recentSevenNetTotal >= 3000 ||
          recentFourteenNetTotal >= 5000 ||
          previousGames > 6000 ||
          recentThreeGamesTotal > 16000;
        const beamHikariMyMainBoostFlags = [
          beamHikariMyTwoLoss,
          recentFourteenNetTotal <= -4000 && beamHikariMyTwoLossOrMore,
          features.recentThreeAngle <= -120 || features.recentSevenAngle <= -80,
          beamHikariMyMainWeakContent,
          adjacentMachineHighContentCount7 > 0 && beamHikariMyTwoLossOrMore,
          beamHikariMyMainOpen,
          previousGames >= 1000 && previousGames <= 4000,
        ];
        const beamHikariMyMainDangerFlags = [
          beamHikariMyMainTreatmentDone,
          beamHikariMyMainTooVisible,
          streak >= 8 || (Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 36),
          previousGames <= 1000,
        ];

        return {
          ...features,
          beamHikariMyHistoryReady,
          beamHikariMyMainHistoryReady: beamHikariMyHistoryReady,
          beamHikariMyTwoLoss,
          beamHikariMyTwoLossOrMore,
          beamHikariMyMainWeakContent,
          beamHikariMyMainSink,
          beamHikariMyMainOpen,
          treatmentDone: beamHikariMyMainTreatmentDone,
          lowConfidence: !beamHikariMyMainWeakContent && !beamHikariMyMainSink,
          boostCount: beamHikariMyMainBoostFlags.filter(Boolean).length,
          dangerCount: beamHikariMyMainDangerFlags.filter(Boolean).length,
        };
      }

      if (activeLogicKey === "beam-hikari-my-normal") {
        return {
          ...features,
          beamHikariMyHistoryReady,
          beamHikariMyNormalHistoryReady: beamHikariMyHistoryReady,
          beamHikariMyTwoLoss,
          beamHikariMyTwoLossOrMore,
          beamHikariMyNormalFourteenSinkStrong,
          beamHikariMyNormalWeakContent,
          beamHikariMyNormalTreatmentDone,
          beamHikariMyNormalHighActivity,
          beamHikariMyNormalStrongRecent,
          treatmentDone: beamHikariMyNormalTreatmentDone,
          lowConfidence: !beamHikariMyNormalWeakContent,
          boostCount: beamHikariMyNormalBoostFlags.filter(Boolean).length,
          dangerCount: beamHikariMyNormalDangerFlags.filter(Boolean).length,
        };
      }

      const beamHikariMyEventReuseLine =
        beamHikariMyTwoLossOrMore &&
        (recentFourteenMachineHighContentCount >= 3 ||
          recentTwentyOneMachineHighContentCount >= 3 ||
          features.recentFourteenRbDenominator <= 330 ||
          features.recentFourteenCombinedDenominator <= 145);
      const beamHikariMyEventShortSink =
        recentThreeNetTotal <= -2000 || features.recentThreeAngle <= -150;
      const beamHikariMyEventLongSink =
        recentFourteenNetTotal <= -6000 || recentTwentyOneNetTotal <= -6000;
      const beamHikariMyEventNearbyShow =
        beamHikariMyTwoLossOrMore &&
        (adjacentMachineNetTotal3Near2 >= 3000 || adjacentMachineHighContentCount7Near2 > 0);
      const beamHikariMyEventTreatmentDone =
        previousMachineHighContent ||
        previousDifference >= 1000 ||
        previousGames > 6000;
      const beamHikariMyEventLongNeglect =
        Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 36;
      const beamHikariMyEventBoostFlags = [
        beamHikariMyTwoLossOrMore,
        beamHikariMyEventReuseLine,
        beamHikariMyEventShortSink,
        beamHikariMyEventLongSink,
        beamHikariMyEventNearbyShow,
      ];
      const beamHikariMyEventDangerFlags = [
        beamHikariMyEventTreatmentDone,
        beamHikariMyEventLongNeglect,
        features.recentThreeCombinedDenominator <= 145 || features.recentThreeRbDenominator <= 320,
      ];

      return {
        ...features,
        beamHikariMyHistoryReady,
        beamHikariMyEventHistoryReady: beamHikariMyHistoryReady,
        beamHikariMyTwoLoss,
        beamHikariMyTwoLossOrMore,
        beamHikariMyEventReuseLine,
        beamHikariMyEventShortSink,
        beamHikariMyEventLongSink,
        beamHikariMyEventNearbyShow,
        beamHikariMyEventTreatmentDone,
        beamHikariMyEventLongNeglect,
        treatmentDone: beamHikariMyEventTreatmentDone,
        lowConfidence: !beamHikariMyEventReuseLine && !beamHikariMyEventShortSink,
        boostCount: beamHikariMyEventBoostFlags.filter(Boolean).length,
        dangerCount: beamHikariMyEventDangerFlags.filter(Boolean).length,
      };
    }

    if (activeLogicKey === "apark-yakatabaru-my") {
      const yakatabaruMyHistoryReady = historyRowCount >= 21;
      const yakatabaruMyGamesCore = recentThreeGamesTotal >= 9000 && recentThreeGamesTotal <= 16000;
      const yakatabaruMyNoRecentHigh = recentSevenMachineHighContentCount === 0;
      const yakatabaruMyStrongSink =
        recentThreeNetTotal <= -2000 || streak >= 3 || features.recentThreeAngle <= -100;
      const yakatabaruMyWeakBonus =
        features.recentThreeCombinedDenominator >= 170 ||
        (features.recentThreeCombinedDenominator >= 160 && features.recentThreeRbDenominator >= 420);
      const yakatabaruMyUnpaidSink =
        recentSevenNetTotal <= -2500 ||
        (recentFourteenNetTotal <= -3000 && recentSevenMachineHighContentCount <= 1);
      const yakatabaruMyRestOpen =
        recentSevenMachineHighContentCount === 0 &&
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent >= 5;
      const yakatabaruMyTrustedGames = recentThreeGamesTotal >= 9000 && recentSevenGamesTotal >= 25000;
      const yakatabaruMyTreatmentDone =
        previousDifference >= 2000 ||
        (previousMachineHighContent && previousDifference >= 1000) ||
        recentThreeNetTotal >= 3000 ||
        recentFourteenNetTotal >= 7000;
      const yakatabaruMyLowGames = recentThreeGamesTotal < 6000 || recentSevenGamesTotal < 18000;
      const yakatabaruMyRecentTooStrong = features.recentThreeCombinedDenominator <= 135;
      const yakatabaruMyLongNeglect =
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent >= 35 &&
        recentTwentyOneMachineHighContentCount === 0 &&
        !yakatabaruMyStrongSink;
      const boostFlags = [
        yakatabaruMyStrongSink,
        yakatabaruMyWeakBonus,
        yakatabaruMyUnpaidSink,
        yakatabaruMyRestOpen,
        yakatabaruMyTrustedGames,
      ];
      const dangerFlags = [
        yakatabaruMyTreatmentDone,
        yakatabaruMyLowGames,
        yakatabaruMyRecentTooStrong,
        yakatabaruMyLongNeglect,
      ];

      return {
        ...features,
        yakatabaruMyHistoryReady,
        yakatabaruMyGamesCore,
        yakatabaruMyNoRecentHigh,
        yakatabaruMyStrongSink,
        yakatabaruMyWeakBonus,
        yakatabaruMyUnpaidSink,
        yakatabaruMyRestOpen,
        yakatabaruMyTrustedGames,
        yakatabaruMyTreatmentDone,
        yakatabaruMyLowGames,
        yakatabaruMyRecentTooStrong,
        yakatabaruMyLongNeglect,
        treatmentDone: yakatabaruMyTreatmentDone,
        lowConfidence: yakatabaruMyLowGames,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    if (activeLogicKey === "mj-kurume-my") {
      const kurumeMyHistoryReady = historyRowCount >= 14;
      const kurumeMySinkStayStrong =
        recentFourteenMinus3000StayDays >= 3 || recentFiveMinus2000StayDays >= 2;
      const kurumeMyAngleStrong =
        features.recentFourteenAngle <= -80 && recentFourteenGamesTotal >= 25000;
      const kurumeMyUnpaid =
        recentFourteenNetTotal <= -3000 && recentFourteenGamesTotal >= 25000;
      const kurumeMyTrustedGames =
        recentSevenGamesTotal >= 28000 || recentFourteenGamesTotal >= 60000;
      const kurumeMyPreviousHighFail =
        previousMachineHighContent && previousDifference <= 1000 && previousGames >= 3000;
      const kurumeMyNearbyLeftBehind =
        (recentFourteenNetTotal <= -2000 && adjacentMachineHighContentCount7Near2 >= 2) ||
        (recentSevenNetTotal <= -1500 && adjacentMachineNetTotal7 >= 3000);
      const kurumeMyLowUsage =
        recentSevenGamesTotal < 12000 ||
        recentFourteenGamesTotal < 25000 ||
        previousGames < 1000;
      const kurumeMyTreatmentDone =
        recentSevenNetTotal >= 3000 ||
        recentFourteenNetTotal >= 5000 ||
        (recentThreeNetTotal >= 2000 && recentThreeMachineHighContentCount >= 1);
      const kurumeMyHighContentBurst =
        recentThreeMachineHighContentCount >= 2 || recentFiveMachineHighContentCount >= 2;
      const kurumeMyLongNeglect =
        streak >= 5 && kurumeMyLowUsage && recentFourteenNetTotal > -3000;
      const kurumeMyBbLeanOutput =
        previousDifference >= 1500 && features.previousRbDenominator > 320;
      const boostFlags = [
        kurumeMySinkStayStrong,
        kurumeMyAngleStrong,
        kurumeMyUnpaid,
        kurumeMyTrustedGames,
        kurumeMyPreviousHighFail,
        kurumeMyNearbyLeftBehind,
      ];
      const dangerFlags = [
        kurumeMyTreatmentDone,
        kurumeMyLowUsage,
        kurumeMyHighContentBurst,
        kurumeMyLongNeglect,
        kurumeMyBbLeanOutput,
      ];

      return {
        ...features,
        kurumeMyHistoryReady,
        kurumeMySinkStayStrong,
        kurumeMyAngleStrong,
        kurumeMyUnpaid,
        kurumeMyTrustedGames,
        kurumeMyPreviousHighFail,
        kurumeMyNearbyLeftBehind,
        kurumeMyLowUsage,
        kurumeMyHighContentBurst,
        kurumeMyLongNeglect,
        kurumeMyBbLeanOutput,
        kurumeMyNoLowUsage: !kurumeMyLowUsage,
        treatmentDone: kurumeMyTreatmentDone,
        lowConfidence: kurumeMyLowUsage,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    const myHistoryReady = historyRowCount >= 30;
    const myLosingStreak3 = streak >= 3;
    const myRecentFiveLoss3500 = recentFiveNetTotal <= -3500;
    const mySinkStayDays = readNumber(metrics.recentFiveMinus3500StayDays);
    const myAngleStayDays = readNumber(metrics.recentFiveAngleMinus80StayDays);
    const mySinkStayStrong = mySinkStayDays >= 1 || myAngleStayDays >= 3;
    const myStrongAngle =
      (features.recentFiveAngle <= -80 && recentFiveGamesTotal >= 20000) ||
      (features.recentSevenAngle <= -80 && recentSevenGamesTotal >= 28000);
    const myUnpaid =
      (recentFourteenNetTotal < 0 && previousDifference > 0 && previousDifference < 1500) ||
      (recentTwentyOneNetTotal < 0 && previousDifference > 0);
    const myWeakBonusReturn =
      (recentThreeGamesTotal >= 9000 && features.recentThreeCombinedDenominator >= 164) ||
      (recentFiveGamesTotal >= 15000 && features.recentFiveCombinedDenominator >= 159);
    const myTrustedGames =
      (myLosingStreak3 && recentThreeGamesTotal >= 10000) ||
      (recentFiveNetTotal <= -2500 && recentFiveGamesTotal >= 20000);
    const myOutputOnlyStrong =
      previousDifference >= 1500 &&
      features.previousRbDenominator > 300 &&
      features.previousCombinedDenominator > 140;
    const myTreatmentDone =
      previousMachineHighContent ||
      myOutputOnlyStrong ||
      recentThreeNetTotal >= 2800 ||
      recentFiveNetTotal >= 3300 ||
      recentSevenNetTotal >= 3500 ||
      recentFourteenNetTotal >= 6000 ||
      recentSevenMachineHighContentCount >= 2;
    const myLowGamesUncertain = previousGames < 2000 && recentThreeGamesTotal < 10000;
    const myLongNeglect =
      (mySinkStayDays >= 6 || streak >= 6) &&
      [mySinkStayStrong, myStrongAngle, myUnpaid, myWeakBonusReturn, myTrustedGames].filter(Boolean).length < 2;
    const boostFlags = [
      mySinkStayStrong,
      myStrongAngle,
      myUnpaid,
      myWeakBonusReturn,
      myTrustedGames,
    ];
    const dangerFlags = [
      myTreatmentDone,
      myLowGamesUncertain,
      myLongNeglect,
      myOutputOnlyStrong,
      recentSevenMachineHighContentCount >= 2,
    ];

    return {
      ...features,
      myHistoryReady,
      myLosingStreak3,
      myRecentFiveLoss3500,
      mySinkStayStrong,
      myStrongAngle,
      myUnpaid,
      myWeakBonusReturn,
      myTrustedGames,
      myOutputOnlyStrong,
      treatmentDone: myTreatmentDone,
      lowConfidence: myLowGamesUncertain,
      boostCount: boostFlags.filter(Boolean).length,
      dangerCount: dangerFlags.filter(Boolean).length,
    };
  }

  if (machineKey === "funky") {
    if (
      activeLogicKey === "beam-hikari-funky" ||
      activeLogicKey === "beam-hikari-funky-normal" ||
      activeLogicKey === "beam-hikari-funky-event"
    ) {
      const beamHikariFunkyHistoryReady = historyRowCount >= 21;
      const beamHikariFunkyThreeDayGamesCore = recentThreeGamesTotal >= 3000 && recentThreeGamesTotal <= 6000;
      const beamHikariFunkyThreeDayBonusWeak =
        features.recentThreeCombinedDenominator >= 170 || features.recentThreeRbDenominator >= 430;
      const beamHikariFunkyThreeDayRbWeak = features.recentThreeRbDenominator >= 500;
      const beamHikariFunkyRestCore =
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent >= 11 &&
        daysSinceMachineHighContent <= 20;
      const beamHikariFunkyNormalCore =
        streak >= 2 &&
        beamHikariFunkyThreeDayGamesCore &&
        beamHikariFunkyThreeDayRbWeak &&
        beamHikariFunkyRestCore;
      const beamHikariFunkyMediumSink =
        recentSevenNetTotal <= -1500 ||
        recentFourteenNetTotal <= -3000 ||
        recentTwentyOneNetTotal <= -3500 ||
        features.recentSevenAngle <= -70;
      const beamHikariFunkyNearbyLeftBehind =
        previousDifference < 0 &&
        (previousAdjacentMachineHighContentCount >= 1 ||
          previousAdjacentMachineNetTotal >= 1500 ||
          adjacentMachineHighContentCount7Near2 >= 2);
      const beamHikariFunkyTreatmentDone =
        previousDifference >= 1500 ||
        recentThreeNetTotal >= 1500 ||
        recentSevenNetTotal >= 2500 ||
        (previousMachineHighContent && previousDifference >= 0);
      const beamHikariFunkyTooStrong =
        previousMachineHighContent ||
        recentThreeMachineHighContentCount >= 1 ||
        features.recentThreeCombinedDenominator <= 145 ||
        features.recentThreeRbDenominator <= 320;
      const beamHikariFunkyLowConfidence =
        recentThreeGamesTotal < 1500 ||
        (recentThreeGamesTotal < 3000 && streak < 2 && !beamHikariFunkyThreeDayBonusWeak);

      if (activeLogicKey === "beam-hikari-funky") {
        const beamHikariFunkyCore = beamHikariFunkyNormalCore;
        const beamHikariFunkyShortCore = streak >= 2 && beamHikariFunkyThreeDayGamesCore;
        const beamHikariFunkyStrongAngle =
          features.recentThreeAngle <= -100 || features.recentSevenAngle <= -70;
        const boostFlags = [
          beamHikariFunkyCore,
          beamHikariFunkyShortCore,
          beamHikariFunkyMediumSink,
          beamHikariFunkyStrongAngle,
          beamHikariFunkyNearbyLeftBehind,
        ];
        const dangerFlags = [
          beamHikariFunkyTreatmentDone,
          beamHikariFunkyTooStrong,
          beamHikariFunkyLowConfidence,
          recentThreeGamesTotal > 12000,
        ];

        return {
          ...features,
          beamHikariFunkyHistoryReady,
          beamHikariFunkyCore,
          beamHikariFunkyShortCore,
          beamHikariFunkyThreeDayGamesCore,
          beamHikariFunkyThreeDayBonusWeak,
          beamHikariFunkyThreeDayRbWeak,
          beamHikariFunkyMediumSink,
          beamHikariFunkyStrongAngle,
          beamHikariFunkyNearbyLeftBehind,
          beamHikariFunkyTreatmentDone,
          beamHikariFunkyTooStrong,
          beamHikariFunkyLowConfidence,
          treatmentDone: beamHikariFunkyTreatmentDone,
          lowConfidence: beamHikariFunkyLowConfidence,
          boostCount: boostFlags.filter(Boolean).length,
          dangerCount: dangerFlags.filter(Boolean).length,
        };
      }

      if (activeLogicKey === "beam-hikari-funky-event") {
        const beamHikariFunkyEventHistoryReady = beamHikariFunkyHistoryReady;
        const beamHikariFunkyEventRotationLike =
          beamHikariFunkyRestCore ||
          (Number.isFinite(daysSinceMachineHighContent) &&
            ((daysSinceMachineHighContent >= 4 && daysSinceMachineHighContent <= 10) ||
              (daysSinceMachineHighContent >= 21 && daysSinceMachineHighContent <= 35)));
        const beamHikariFunkyEventClearTop =
          beamHikariFunkyThreeDayGamesCore &&
          beamHikariFunkyThreeDayBonusWeak &&
          !beamHikariFunkyTreatmentDone;
        const boostFlags = [
          beamHikariFunkyEventClearTop,
          beamHikariFunkyThreeDayGamesCore,
          beamHikariFunkyThreeDayBonusWeak,
          beamHikariFunkyMediumSink,
          beamHikariFunkyEventRotationLike,
          beamHikariFunkyNearbyLeftBehind,
          streak >= 2,
        ];
        const dangerFlags = [
          beamHikariFunkyTreatmentDone,
          beamHikariFunkyTooStrong,
          beamHikariFunkyLowConfidence,
          recentThreeGamesTotal >= 9000,
        ];

        return {
          ...features,
          beamHikariFunkyHistoryReady,
          beamHikariFunkyEventHistoryReady,
          beamHikariFunkyEventClearTop,
          beamHikariFunkyNormalCore,
          beamHikariFunkyThreeDayGamesCore,
          beamHikariFunkyThreeDayBonusWeak,
          beamHikariFunkyThreeDayRbWeak,
          beamHikariFunkyMediumSink,
          beamHikariFunkyNearbyLeftBehind,
          beamHikariFunkyTreatmentDone,
          beamHikariFunkyTooStrong,
          beamHikariFunkyLowConfidence,
          treatmentDone: beamHikariFunkyTreatmentDone,
          lowConfidence: beamHikariFunkyLowConfidence,
          boostCount: boostFlags.filter(Boolean).length,
          dangerCount: dangerFlags.filter(Boolean).length,
        };
      }

      const beamHikariFunkyNormalHistoryReady = beamHikariFunkyHistoryReady;
      const beamHikariFunkyNormalSteepSink =
        features.recentThreeAngle <= -200 ||
        (features.recentThreeAngle <= -100 && recentThreeGamesTotal >= 3000);
      const boostFlags = [
        beamHikariFunkyNormalCore,
        beamHikariFunkyNormalSteepSink,
        beamHikariFunkyThreeDayGamesCore,
        beamHikariFunkyThreeDayBonusWeak,
        beamHikariFunkyMediumSink,
        beamHikariFunkyRestCore,
        streak >= 2,
      ];
      const dangerFlags = [
        beamHikariFunkyTreatmentDone,
        beamHikariFunkyTooStrong,
        beamHikariFunkyLowConfidence,
        Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent <= 3,
      ];

      return {
        ...features,
        beamHikariFunkyHistoryReady,
        beamHikariFunkyNormalHistoryReady,
        beamHikariFunkyNormalCore,
        beamHikariFunkyNormalSteepSink,
        beamHikariFunkyThreeDayGamesCore,
        beamHikariFunkyThreeDayBonusWeak,
        beamHikariFunkyThreeDayRbWeak,
        beamHikariFunkyMediumSink,
        beamHikariFunkyTreatmentDone,
        beamHikariFunkyTooStrong,
        beamHikariFunkyLowConfidence,
        treatmentDone: beamHikariFunkyTreatmentDone,
        lowConfidence: beamHikariFunkyLowConfidence,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    if (activeLogicKey === "apark-yakatabaru-funky") {
      const yakatabaruFunkyHistoryReady = historyRowCount >= 21;
      const yakatabaruFunkySinkStrong =
        streak >= 3 ||
        (features.recentThreeAngle <= -180 && recentThreeGamesTotal >= 10000);
      const yakatabaruFunkyAngleStrong = features.recentThreeAngle <= -180;
      const yakatabaruFunkyUnpaid =
        (recentSevenNetTotal <= -2500 || recentFourteenNetTotal <= -2780) &&
        recentTwentyOneNetTotal <= 0;
      const yakatabaruFunkyBonusWeak =
        features.recentSevenCombinedDenominator >= 161 && streak >= 2;
      const yakatabaruFunkyNearbyLeftBehind =
        recentThreeNetTotal <= -1300 && otherSameMachineHighContentCount7 >= 6;
      const yakatabaruFunkyTreatmentDone =
        (previousMachineHighContent && previousDifference >= 1500) ||
        previousDifference >= 2200;
      const yakatabaruFunkyLowInfo =
        recentThreeGamesTotal < 9000 || recentSevenGamesTotal < 25000;
      const yakatabaruFunkyRecentHigh =
        Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent <= 2;
      const yakatabaruFunkyRecentTooStrong =
        features.recentThreeAngle >= 146 || recentSevenNetTotal >= 3430;
      const boostFlags = [
        yakatabaruFunkySinkStrong,
        yakatabaruFunkyAngleStrong,
        yakatabaruFunkyUnpaid,
        yakatabaruFunkyBonusWeak,
        yakatabaruFunkyNearbyLeftBehind,
      ];
      const dangerFlags = [
        yakatabaruFunkyTreatmentDone,
        yakatabaruFunkyLowInfo,
        yakatabaruFunkyRecentHigh,
        yakatabaruFunkyRecentTooStrong,
      ];

      return {
        ...features,
        yakatabaruFunkyHistoryReady,
        yakatabaruFunkySinkStrong,
        yakatabaruFunkyAngleStrong,
        yakatabaruFunkyUnpaid,
        yakatabaruFunkyBonusWeak,
        yakatabaruFunkyNearbyLeftBehind,
        yakatabaruFunkyTreatmentDone,
        yakatabaruFunkyLowInfo,
        yakatabaruFunkyRecentHigh,
        yakatabaruFunkyRecentTooStrong,
        treatmentDone: yakatabaruFunkyTreatmentDone,
        lowConfidence: yakatabaruFunkyLowInfo,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    if (activeLogicKey === "mj-kurume-funky") {
      const kurumeFunkyHistoryReady = historyRowCount >= 21;
      const kurumeFunkySinkStrong =
        recentTwentyOneNetTotal <= -4000 ||
        features.recentTwentyOneAngle <= -80;
      const kurumeFunkyAngleStrong =
        features.recentTwentyOneAngle <= -80 ||
        features.recentFourteenAngle <= -80;
      const kurumeFunkyUnpaid =
        recentTwentyOneNetTotal <= -3000 &&
        recentFourteenNetTotal <= -1000;
      const kurumeFunkyBonusReturn =
        features.recentFourteenCombinedDenominator >= 170 &&
        features.recentFourteenRbDenominator >= 400;
      const kurumeFunkyNearbyLeftBehind =
        (adjacentMachineBigWin1000Count7Near2 >= 2 && recentFourteenNetTotal <= -1000) ||
        (adjacentMachineNetTotal14 > 0 && recentTwentyOneNetTotal <= -2000);
      const kurumeFunkyTrustedGames =
        [
          recentTwentyOneGamesTotal >= 42000,
          recentFourteenGamesTotal >= 28000,
          recentSevenGamesTotal >= 14000,
        ].filter(Boolean).length >= 2;
      const kurumeFunkyTreatmentDone =
        recentFourteenNetTotal >= 4000 ||
        recentTwentyOneNetTotal >= 4000;
      const kurumeFunkyRecentTooStrong = recentSevenNetTotal >= 3000;
      const kurumeFunkyLongNeglect = streak >= 6;
      const kurumeFunkyOverused = recentTwentyOneMachineHighContentCount >= 6;
      const boostFlags = [
        kurumeFunkySinkStrong,
        kurumeFunkyAngleStrong,
        kurumeFunkyUnpaid,
        kurumeFunkyBonusReturn,
        kurumeFunkyNearbyLeftBehind,
        kurumeFunkyTrustedGames,
      ];
      const dangerFlags = [
        kurumeFunkyTreatmentDone,
        kurumeFunkyRecentTooStrong,
        kurumeFunkyLongNeglect,
        kurumeFunkyOverused,
      ];

      return {
        ...features,
        kurumeFunkyHistoryReady,
        kurumeFunkySinkStrong,
        kurumeFunkyAngleStrong,
        kurumeFunkyUnpaid,
        kurumeFunkyBonusReturn,
        kurumeFunkyNearbyLeftBehind,
        kurumeFunkyTrustedGames,
        treatmentDone: kurumeFunkyTreatmentDone,
        lowConfidence: !kurumeFunkyTrustedGames,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    const funkyHistoryReady = historyRowCount >= 28;
    const funkyHighRest8To14 =
      Number.isFinite(daysSinceMachineHighContent) &&
      daysSinceMachineHighContent >= 8 &&
      daysSinceMachineHighContent <= 14;
    const funkyLosingStreak3To6 = streak >= 3 && streak <= 6;
    const funkySinkStrong =
      recentFiveNetTotal <= -3000 ||
      recentSevenNetTotal <= -3000 ||
      readNumber(metrics.recentFiveMinus3000StayDays) >= 2 ||
      readNumber(metrics.recentSevenMinus3000StayDays) >= 2;
    const funkyAngleStrong =
      (recentFiveGamesTotal >= 15000 && features.recentFiveAngle <= -100) ||
      (recentSevenGamesTotal >= 22000 &&
        features.recentSevenAngle <= -50 &&
        features.recentSevenAngle < 0);
    const funkyBonusWeak =
      features.recentThreeCombinedDenominator > 160 ||
      features.recentSevenCombinedDenominator > 160;
    const funkyPreviousBroadFail = previousMachineGoodContent && previousDifference <= 0;
    const funkyTreatmentDone =
      previousDifference > 2000 ||
      recentSevenNetTotal > 3000 ||
      recentTwentyEightNetTotal > 7000 ||
      (previousMachineHighContent && previousDifference > 1000);
    const boostFlags = [
      funkySinkStrong,
      funkyAngleStrong,
      funkyLosingStreak3To6,
      funkyBonusWeak,
      funkyHighRest8To14,
      funkyPreviousBroadFail,
    ];
    const dangerFlags = [
      funkyTreatmentDone,
      recentThreeGamesTotal < 9000,
      recentSevenGamesTotal < 18000,
      recentSevenMachineHighContentCount >= 2,
      recentFourteenMachineHighContentCount >= 3,
    ];

    return {
      ...features,
      funkyHistoryReady,
      funkyHighRest8To14,
      funkyLosingStreak3To6,
      funkySinkStrong,
      funkyAngleStrong,
      funkyBonusWeak,
      funkyPreviousBroadFail,
      treatmentDone: funkyTreatmentDone,
      boostCount: boostFlags.filter(Boolean).length,
      dangerCount: dangerFlags.filter(Boolean).length,
    };
  }

  if (
    machineKey === "happy" &&
    (activeLogicKey === "beam-hikari-happy" ||
      activeLogicKey === "beam-hikari-happy-normal" ||
      activeLogicKey === "beam-hikari-happy-event")
  ) {
    const beamHikariHappyHistoryReady = targetRangeHistoryRowCount >= 21;
    const previousCombinedDenominator = features.previousCombinedDenominator;
    const beamHikariHappyPreviousWeak =
      previousGames >= 500 &&
      previousGames <= 3000 &&
      previousDifference < 0 &&
      previousCombinedDenominator > 170;
    const beamHikariHappyNormalLosingSink =
      streak >= 5 || (streak >= 3 && recentSevenLossDays >= 5);
    const beamHikariHappyUnpaid =
      (recentTwentyOneNetTotal <= -8000 && recentTwentyOneGamesTotal >= 48000) ||
      (recentFourteenNetTotal <= -6000 && recentSevenLossDays >= 4);
    const beamHikariHappyUnfinished =
      recentFourteenGoldShowDays === 0 && recentFourteenNetTotal < 0;
    const beamHikariHappyGamesTrust =
      recentSevenGamesTotal >= 8000 &&
      recentSevenGamesTotal <= 20000 &&
      recentFourteenGamesTotal >= 25000 &&
      recentFourteenGamesTotal <= 45000;
    const beamHikariHappyTreatmentDone =
      previousMachineHighContent ||
      previousMachineGoodContent ||
      previousDifference >= 1500 ||
      recentSevenNetTotal > 1500 ||
      recentTwentyOneNetTotal > 4000;
    const beamHikariHappyWatchedTooMuch =
      previousGames >= 3000 && previousCombinedDenominator <= 150;
    const beamHikariHappyLowInfo =
      previousGames < 500 && recentSevenGamesTotal < 14000;
    const beamHikariHappyRecentGoodTooClose =
      recentSevenMachineGoodContentCount >= 2 ||
      (Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent <= 3);
    const beamHikariHappyEventRotationBoost =
      Number.isFinite(daysSinceMachineHighContent) &&
      daysSinceMachineHighContent >= 18 &&
      daysSinceMachineHighContent <= 36 &&
      recentFourteenMachineHighContentCount === 0 &&
      recentFourteenMachineGoodContentCount <= 1;
    const beamHikariHappyEventSink =
      recentFourteenNetTotal <= -6000 ||
      (recentTwentyOneNetTotal <= -5000 && recentSevenLossDays >= 4);
    const beamHikariHappyEventLeftBehind =
      adjacentMachineHighContentCount14Near2 === 0 ||
      adjacentMachineBigWin1000Count7Near2 === 0 ||
      recentTwentyOneNetTotal < 0;
    const beamHikariHappyEventDanger =
      previousDifference >= 1000 ||
      previousMachineHighContent ||
      previousMachineGoodContent ||
      beamHikariHappyWatchedTooMuch ||
      recentSevenNetTotal > 1500 ||
      recentTwentyOneNetTotal > 4000 ||
      beamHikariHappyLowInfo ||
      (Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent <= 6);
    const normalBoostFlags = [
      beamHikariHappyPreviousWeak,
      beamHikariHappyNormalLosingSink,
      beamHikariHappyUnpaid,
      beamHikariHappyUnfinished,
      beamHikariHappyGamesTrust,
    ];
    const normalDangerFlags = [
      beamHikariHappyTreatmentDone,
      beamHikariHappyWatchedTooMuch,
      beamHikariHappyLowInfo,
      beamHikariHappyRecentGoodTooClose,
    ];
    const eventBoostFlags = [
      beamHikariHappyEventRotationBoost,
      beamHikariHappyPreviousWeak,
      beamHikariHappyEventSink,
      beamHikariHappyUnfinished,
      beamHikariHappyEventLeftBehind,
      beamHikariHappyGamesTrust,
    ];
    const eventDangerFlags = [
      beamHikariHappyEventDanger,
      beamHikariHappyWatchedTooMuch,
      beamHikariHappyLowInfo,
    ];
    if (activeLogicKey === "beam-hikari-happy") {
      const beamHikariHappyMainSinkBoost =
        beamHikariHappyUnpaid ||
        (recentTwentyOneNetTotal <= -8000 && recentTwentyOneGamesTotal >= 48000) ||
        (recentFourteenNetTotal <= -6000 && recentSevenLossDays >= 4);
      const mainBoostFlags = [
        beamHikariHappyPreviousWeak,
        beamHikariHappyMainSinkBoost,
        beamHikariHappyUnfinished,
        beamHikariHappyGamesTrust,
        beamHikariHappyNormalLosingSink,
      ];
      const mainDangerFlags = [
        beamHikariHappyTreatmentDone,
        beamHikariHappyWatchedTooMuch,
        beamHikariHappyLowInfo,
        beamHikariHappyRecentGoodTooClose,
      ];

      return {
        ...features,
        beamHikariHappyMainHistoryReady: beamHikariHappyHistoryReady,
        beamHikariHappyPreviousWeak,
        beamHikariHappyMainSinkBoost,
        beamHikariHappyUnpaid,
        beamHikariHappyUnfinished,
        beamHikariHappyGamesTrust,
        beamHikariHappyTreatmentDone,
        beamHikariHappyWatchedTooMuch,
        beamHikariHappyLowInfo,
        beamHikariHappyRecentGoodTooClose,
        treatmentDone: beamHikariHappyTreatmentDone,
        lowConfidence: beamHikariHappyLowInfo,
        boostCount: mainBoostFlags.filter(Boolean).length,
        dangerCount: mainDangerFlags.filter(Boolean).length,
      };
    }
    const isEventLogic = activeLogicKey === "beam-hikari-happy-event";

    return {
      ...features,
      beamHikariHappyMainHistoryReady: beamHikariHappyHistoryReady,
      beamHikariHappyNormalHistoryReady: beamHikariHappyHistoryReady,
      beamHikariHappyEventHistoryReady: beamHikariHappyHistoryReady,
      beamHikariHappyPreviousWeak,
      beamHikariHappyNormalLosingSink,
      beamHikariHappyUnpaid,
      beamHikariHappyUnfinished,
      beamHikariHappyGamesTrust,
      beamHikariHappyTreatmentDone,
      beamHikariHappyWatchedTooMuch,
      beamHikariHappyLowInfo,
      beamHikariHappyRecentGoodTooClose,
      beamHikariHappyEventRotationBoost,
      beamHikariHappyEventPreviousWeak: beamHikariHappyPreviousWeak,
      beamHikariHappyEventSink,
      beamHikariHappyEventLeftBehind,
      beamHikariHappyEventDanger,
      treatmentDone: isEventLogic ? beamHikariHappyEventDanger : beamHikariHappyTreatmentDone,
      lowConfidence: beamHikariHappyLowInfo,
      boostCount: (isEventLogic ? eventBoostFlags : normalBoostFlags).filter(Boolean).length,
      dangerCount: (isEventLogic ? eventDangerFlags : normalDangerFlags).filter(Boolean).length,
    };
  }

  if (machineKey === "happy" && activeLogicKey === "apark-yakatabaru-happy") {
    const yakatabaruHappyHistoryReady = historyRowCount >= 21;
    const yakatabaruHappyStrongSink =
      streak >= 4 && recentSevenNetTotal <= -1500 && recentSevenGamesTotal >= 25000;
    const yakatabaruHappyAngleStrong =
      recentSevenGamesTotal >= 20000 && features.recentSevenAngle <= -70;
    const yakatabaruHappyUnpaid =
      recentTwentyOneNetTotal <= -3000 && recentSevenNetTotal <= 0;
    const yakatabaruHappyBonusWeak =
      features.recentSevenCombinedDenominator >= 153 && recentSevenGamesTotal >= 20000;
    const yakatabaruHappyNearbyShow =
      recentSevenNetTotal <= 0 && otherSameMachineHighContentCount7 >= 7;
    const yakatabaruHappyPreviousHigh = previousMachineHighContent;
    const yakatabaruHappyTreatmentDone =
      recentSevenNetTotal >= 2500 || recentFourteenNetTotal >= 3500;
    const yakatabaruHappyOverused = recentFourteenMachineHighContentCount >= 4;
    const yakatabaruHappyLowGames = recentSevenGamesTotal < 20000;
    const yakatabaruHappyLongNeglect =
      Number.isFinite(daysSinceMachineHighContent) &&
      daysSinceMachineHighContent >= 22 &&
      !yakatabaruHappyStrongSink &&
      !yakatabaruHappyUnpaid &&
      recentSevenNetTotal > -1500;
    const boostFlags = [
      yakatabaruHappyStrongSink,
      yakatabaruHappyAngleStrong,
      yakatabaruHappyUnpaid,
      yakatabaruHappyBonusWeak,
      yakatabaruHappyNearbyShow,
    ];
    const dangerFlags = [
      yakatabaruHappyPreviousHigh,
      yakatabaruHappyTreatmentDone,
      yakatabaruHappyOverused,
      yakatabaruHappyLowGames,
      yakatabaruHappyLongNeglect,
    ];

    return {
      ...features,
      yakatabaruHappyHistoryReady,
      yakatabaruHappyStrongSink,
      yakatabaruHappyAngleStrong,
      yakatabaruHappyUnpaid,
      yakatabaruHappyBonusWeak,
      yakatabaruHappyNearbyShow,
      yakatabaruHappyPreviousHigh,
      yakatabaruHappyTreatmentDone,
      yakatabaruHappyOverused,
      yakatabaruHappyLowGames,
      yakatabaruHappyLongNeglect,
      treatmentDone: yakatabaruHappyTreatmentDone,
      lowConfidence: yakatabaruHappyLowGames,
      boostCount: boostFlags.filter(Boolean).length,
      dangerCount: dangerFlags.filter(Boolean).length,
    };
  }

  if (
    machineKey === "ultra-miracle" &&
    (activeLogicKey === "beam-hikari-ultra" ||
      activeLogicKey === "beam-hikari-ultra-normal" ||
      activeLogicKey === "beam-hikari-ultra-event")
  ) {
    const beamHikariUltraHistoryReady = historyRowCount >= 21;
    const beamHikariUltraMediumSink =
      (recentSevenNetTotal <= -1000 && recentSevenNetTotal > -2900) ||
      (recentFiveNetTotal <= -1300 && recentFiveNetTotal > -2300);
    const beamHikariUltraWeakComposite =
      features.recentThreeCombinedDenominator > 205 ||
      features.recentFiveCombinedDenominator > 185 ||
      features.recentFiveAngle <= -226.9;
    const beamHikariUltraPreviousRegStrong = features.previousRbDenominator <= 280;
    const beamHikariUltraPreviousUnfinished =
      previousMachineHighContent && previousDifference < 1000;
    const beamHikariUltraRecentUseStrong =
      recentFourteenMachineHighContentCount >= 4 || recentTwentyOneMachineHighContentCount >= 5;
    const beamHikariUltraGamesTrust =
      recentTwentyOneGamesTotal >= 63600 || recentFourteenGamesTotal >= 43200;
    const beamHikariUltraNearbyPreviousHigh = previousAdjacentMachineHighContentCount > 0;
    const beamHikariUltraEventRotation =
      (Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 4 && daysSinceMachineHighContent <= 13) ||
      recentFourteenMachineHighContentCount >= 2;
    const beamHikariUltraEventBusinessSink = recentFiveNetTotal <= -1500;
    const beamHikariUltraPreviousSupport =
      beamHikariUltraPreviousUnfinished || previousDifference <= -800;
    const beamHikariUltraTreatmentDone =
      previousDifference >= 900 ||
      previousMachineHighContent && previousDifference >= 1500;
    const beamHikariUltraHighUsage =
      previousGames >= 5500 || recentThreeGamesTotal >= 10700 || recentSevenGamesTotal >= 24300;
    const beamHikariUltraLongNeglect =
      recentTwentyOneMachineHighContentCount === 0 ||
      (Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 14);
    const beamHikariUltraDeepSink = recentSevenNetTotal <= -2900 || recentFiveNetTotal <= -2300;
    const beamHikariUltraNearbyBad = previousAdjacentMachineNetTotal <= -1000;
    const normalBoostFlags = [
      beamHikariUltraRecentUseStrong,
      beamHikariUltraPreviousUnfinished || beamHikariUltraPreviousRegStrong,
      beamHikariUltraMediumSink,
      beamHikariUltraWeakComposite,
      beamHikariUltraGamesTrust,
    ];
    const mainRecentUseStrong =
      recentFourteenMachineHighContentCount >= 3 || recentTwentyOneMachineHighContentCount >= 5;
    const mainBonusWeak =
      beamHikariUltraPreviousUnfinished ||
      beamHikariUltraPreviousRegStrong ||
      features.recentThreeCombinedDenominator > 205 ||
      features.recentFiveCombinedDenominator > 185;
    const mainBoostFlags = [
      beamHikariUltraMediumSink,
      mainBonusWeak,
      mainRecentUseStrong,
      recentTwentyOneGamesTotal >= 63600,
      beamHikariUltraNearbyPreviousHigh,
    ];
    const eventBoostFlags = [
      beamHikariUltraMediumSink,
      beamHikariUltraWeakComposite || beamHikariUltraPreviousRegStrong,
      beamHikariUltraEventRotation,
      beamHikariUltraEventBusinessSink,
      beamHikariUltraNearbyPreviousHigh,
    ];
    const dangerFlags = [
      beamHikariUltraTreatmentDone,
      beamHikariUltraHighUsage,
      beamHikariUltraLongNeglect,
      beamHikariUltraDeepSink || beamHikariUltraNearbyBad,
    ];
    const isEventLogic = activeLogicKey === "beam-hikari-ultra-event";
    const isMainLogic = activeLogicKey === "beam-hikari-ultra";

    return {
      ...features,
      beamHikariUltraMainHistoryReady: beamHikariUltraHistoryReady,
      beamHikariUltraNormalHistoryReady: beamHikariUltraHistoryReady,
      beamHikariUltraEventHistoryReady: beamHikariUltraHistoryReady,
      beamHikariUltraMediumSink,
      beamHikariUltraWeakComposite,
      beamHikariUltraPreviousRegStrong,
      beamHikariUltraPreviousUnfinished,
      beamHikariUltraRecentUseStrong,
      beamHikariUltraGamesTrust,
      beamHikariUltraNearbyPreviousHigh,
      beamHikariUltraEventRotation,
      beamHikariUltraEventBusinessSink,
      beamHikariUltraPreviousSupport,
      beamHikariUltraTreatmentDone,
      beamHikariUltraHighUsage,
      beamHikariUltraLongNeglect,
      beamHikariUltraDeepSink,
      beamHikariUltraNearbyBad,
      treatmentDone: beamHikariUltraTreatmentDone,
      lowConfidence: !beamHikariUltraGamesTrust && !beamHikariUltraWeakComposite,
      boostCount: (isMainLogic ? mainBoostFlags : isEventLogic ? eventBoostFlags : normalBoostFlags).filter(Boolean).length,
      dangerCount: dangerFlags.filter(Boolean).length,
    };
  }

  if (machineKey === "ultra-miracle" && activeLogicKey === "apark-yakatabaru-ultra-miracle") {
    const yakatabaruUltraHistoryReady = historyRowCount >= 21;
    const yakatabaruUltraAngleBoost =
      features.recentThreeAngle <= -225 || features.recentSevenAngle <= -135;
    const yakatabaruUltraAngleStrongest = features.recentThreeAngle <= -225;
    const yakatabaruUltraDeepSink = recentThreeNetTotal <= -1300 || recentFiveNetTotal <= -1600;
    const yakatabaruUltraUnpaid =
      recentSevenMachineHighContentCount === 0 &&
      recentSevenNetTotal <= 0 &&
      Number.isFinite(daysSinceMachineHighContent) &&
      daysSinceMachineHighContent >= 8;
    const yakatabaruUltraTrustedGames =
      recentThreeGamesTotal >= 3000 && recentSevenGamesTotal >= 12000;
    const yakatabaruUltraNearbyLeftBehind =
      recentSevenNetTotal <= 0 &&
      (adjacentMachineHighContentCount3 > 0 || adjacentMachineBigWin1000Count7Near2 >= 2);
    const yakatabaruUltraTreatmentDone =
      previousMachineHighContent ||
      recentThreeNetTotal >= 1500 ||
      recentSevenNetTotal >= 2800 ||
      previousDifference >= 800;
    const yakatabaruUltraLowConfidence =
      recentThreeGamesTotal < 3000 || recentSevenGamesTotal < 12000;
    const yakatabaruUltraLongNeglect =
      Number.isFinite(daysSinceMachineHighContent) &&
      daysSinceMachineHighContent >= 36 &&
      recentTwentyOneMachineHighContentCount === 0 &&
      recentTwentyOneNetTotal < -9000;
    const yakatabaruUltraOverused = previousGames >= 6200;
    const yakatabaruUltraOutputOnly =
      previousDifference >= 1275 &&
      (features.previousCombinedDenominator > 134 || features.previousRbDenominator > 300);
    const boostFlags = [
      yakatabaruUltraAngleBoost,
      yakatabaruUltraAngleStrongest,
      yakatabaruUltraDeepSink,
      yakatabaruUltraUnpaid,
      yakatabaruUltraTrustedGames,
      yakatabaruUltraNearbyLeftBehind,
    ];
    const dangerFlags = [
      yakatabaruUltraTreatmentDone,
      yakatabaruUltraLowConfidence,
      yakatabaruUltraLongNeglect,
      yakatabaruUltraOverused,
      yakatabaruUltraOutputOnly,
    ];

    return {
      ...features,
      yakatabaruUltraHistoryReady,
      yakatabaruUltraAngleBoost,
      yakatabaruUltraAngleStrongest,
      yakatabaruUltraDeepSink,
      yakatabaruUltraUnpaid,
      yakatabaruUltraTrustedGames,
      yakatabaruUltraNearbyLeftBehind,
      yakatabaruUltraTreatmentDone,
      yakatabaruUltraLowConfidence,
      yakatabaruUltraLongNeglect,
      yakatabaruUltraOverused,
      yakatabaruUltraOutputOnly,
      treatmentDone: yakatabaruUltraTreatmentDone,
      lowConfidence: yakatabaruUltraLowConfidence,
      boostCount: boostFlags.filter(Boolean).length,
      dangerCount: dangerFlags.filter(Boolean).length,
    };
  }

  return features;
}

function calculateMachineScore(definition, metrics, features) {
  const profile = definition?.profile ?? "juggler";
  const machineKey = definition?.machineKey ?? "";
  const activeLogicKey = definition?.activeLogicKey ?? definition?.logicKey ?? "";
  const previousDifference = readNumber(metrics.todayDifference);
  const previousGames = readNumber(metrics.previousGames);
  const previousRbCount = readNumber(metrics.previousRbCount);
  const recentTwoNetTotal = readNumber(metrics.recentTwoNetTotal);
  const recentThreeNetTotal = readNumber(metrics.recentThreeNetTotal);
  const recentFiveNetTotal = readNumber(metrics.recentFiveNetTotal);
  const recentSixNetTotal = readNumber(metrics.recentSixNetTotal);
  const recentSevenNetTotal = readNumber(metrics.recentSevenNetTotal);
  const recentTenNetTotal = readNumber(metrics.recentTenNetTotal);
  const recentFourteenNetTotal = readNumber(metrics.recentFourteenNetTotal);
  const recentTwentyOneNetTotal = readNumber(metrics.recentTwentyOneNetTotal);
  const recentTwentyEightNetTotal = readNumber(metrics.recentTwentyEightNetTotal);
  const recentThirtyNetTotal = readNumber(metrics.recentThirtyNetTotal);
  const recentFortyTwoNetTotal = readNumber(metrics.recentFortyTwoNetTotal);
  const recentFiftySixNetTotal = readNumber(metrics.recentFiftySixNetTotal);
  const lossAbsTotal = readNumber(metrics.lossAbsTotal);
  const streak = readNumber(metrics.streak);
  const historyLosingStreak = readNumber(metrics.historyLosingStreak);
  const winningStreak = readNumber(metrics.winningStreak);
  const historyNetTotal = readNumber(metrics.historyNetTotal);
  const historyPositiveDays = readNumber(metrics.historyPositiveDays);
  const historyRowCount = readNumber(metrics.historyRowCount);
  const targetRangeHistoryRowCount = readNumber(metrics.targetRangeHistoryRowCount, historyRowCount);
  const recentTwoGamesTotal = readNumber(metrics.recentTwoGamesTotal);
  const recentThreeGamesTotal = readNumber(metrics.recentThreeGamesTotal);
  const recentFiveGamesTotal = readNumber(metrics.recentFiveGamesTotal);
  const recentSevenGamesTotal = readNumber(metrics.recentSevenGamesTotal);
  const recentTenGamesTotal = readNumber(metrics.recentTenGamesTotal);
  const recentFourteenGamesTotal = readNumber(metrics.recentFourteenGamesTotal);
  const recentTwentyOneGamesTotal = readNumber(metrics.recentTwentyOneGamesTotal);
  const recentTwentyEightGamesTotal = readNumber(metrics.recentTwentyEightGamesTotal);
  const recentFiftySixGamesTotal = readNumber(metrics.recentFiftySixGamesTotal);
  const recentTwoBonusTotal = readNumber(metrics.recentTwoBonusTotal);
  const recentFourteenGoldShowDays = readNumber(metrics.recentFourteenGoldShowDays);
  const recentSevenGoldShowDays = readNumber(metrics.recentSevenGoldShowDays, readNumber(metrics.recentSevenBigShowDays));
  const recentFourteenLossDays = readNumber(metrics.recentFourteenLossDays);
  const recentFourteenWinDays = readNumber(metrics.recentFourteenWinDays);
  const recentSevenLossDays = readNumber(metrics.recentSevenLossDays);
  const recentFiveHighSettingCandidateCount = readNumber(metrics.recentFiveHighSettingCandidateCount);
  const recentSevenHighSettingCandidateCount = readNumber(metrics.recentSevenHighSettingCandidateCount);
  const recentThreeHighSettingEstimateCount = readNumber(metrics.recentThreeHighSettingEstimateCount);
  const recentThreeStrictHighContentDays = readNumber(metrics.recentThreeStrictHighContentDays);
  const recentSevenStrictHighContentDays = readNumber(metrics.recentSevenStrictHighContentDays);
  const recentSevenMinus2000StayDays = readNumber(metrics.recentSevenMinus2000StayDays);
  const recentThreeMinus1000StayDays = readNumber(metrics.recentThreeMinus1000StayDays);
  const recentThreeMinus1700StayDays = readNumber(metrics.recentThreeMinus1700StayDays);
  const recentSevenMinus3000StayDays = readNumber(metrics.recentSevenMinus3000StayDays);
  const recentFiveMinus1000StayDays = readNumber(metrics.recentFiveMinus1000StayDays);
  const recentFiveMinus1500StayDays = readNumber(metrics.recentFiveMinus1500StayDays);
  const recentFiveMinus2000StayDays = readNumber(metrics.recentFiveMinus2000StayDays);
  const recentFiveMinus3000StayDays = readNumber(metrics.recentFiveMinus3000StayDays);
  const recentFiveMinus3500StayDays = readNumber(metrics.recentFiveMinus3500StayDays);
  const recentSevenMinus1500StayDays = readNumber(metrics.recentSevenMinus1500StayDays);
  const recentThirtyMinus2700StayDays = readNumber(metrics.recentThirtyMinus2700StayDays);
  const recentFiveMinus500StayDays = readNumber(metrics.recentFiveMinus500StayDays);
  const recentTenMinus3000StayDays = readNumber(metrics.recentTenMinus3000StayDays);
  const recentTenMinus2500StayDays = readNumber(metrics.recentTenMinus2500StayDays);
  const recentTenMinus5225StayDays = readNumber(metrics.recentTenMinus5225StayDays);
  const recentFourteenMinus500StayDays = readNumber(metrics.recentFourteenMinus500StayDays);
  const recentFourteenMinus1500StayDays = readNumber(metrics.recentFourteenMinus1500StayDays);
  const recentFourteenMinus1800StayDays = readNumber(metrics.recentFourteenMinus1800StayDays);
  const recentFourteenMinus2000StayDays = readNumber(metrics.recentFourteenMinus2000StayDays);
  const recentFourteenMinus3000StayDays = readNumber(metrics.recentFourteenMinus3000StayDays);
  const recentFourteenMinus3218StayDays = readNumber(metrics.recentFourteenMinus3218StayDays);
  const recentFourteenNegativeStayDays = readNumber(metrics.recentFourteenNegativeStayDays);
  const recentTwentyOneMinus1500StayDays = readNumber(metrics.recentTwentyOneMinus1500StayDays);
  const recentTwentyOneMinus2000StayDays = readNumber(metrics.recentTwentyOneMinus2000StayDays);
  const recentTwentyOneMinus3000StayDays = readNumber(metrics.recentTwentyOneMinus3000StayDays);
  const recentTwentyOneMinus5000StayDays = readNumber(metrics.recentTwentyOneMinus5000StayDays);
  const recentTwentyOneMinus11333StayDays = readNumber(metrics.recentTwentyOneMinus11333StayDays);
  const recentFiveAngleMinus80StayDays = readNumber(metrics.recentFiveAngleMinus80StayDays);
  const recentThreeMachineHighContentCount = readNumber(metrics.recentThreeMachineHighContentCount);
  const recentFiveMachineHighContentCount = readNumber(metrics.recentFiveMachineHighContentCount);
  const recentSevenMachineHighContentCount = readNumber(metrics.recentSevenMachineHighContentCount);
  const recentTenMachineHighContentCount = readNumber(metrics.recentTenMachineHighContentCount);
  const recentFourteenMachineHighContentCount = readNumber(metrics.recentFourteenMachineHighContentCount);
  const recentTwentyOneMachineHighContentCount = readNumber(metrics.recentTwentyOneMachineHighContentCount);
  const recentThirtyMachineHighContentCount = readNumber(metrics.recentThirtyMachineHighContentCount);
  const recentFourteenMachineStrongHighContentCount = readNumber(metrics.recentFourteenMachineStrongHighContentCount);
  const recentSevenMachineGoodContentCount = readNumber(metrics.recentSevenMachineGoodContentCount);
  const recentSevenMachineWeakContentCount = readNumber(metrics.recentSevenMachineWeakContentCount);
  const recentThreeMachineLowContentCount = readNumber(metrics.recentThreeMachineLowContentCount);
  const recentFiveMachineLowContentCount = readNumber(metrics.recentFiveMachineLowContentCount);
  const recentSevenMachineLowContentCount = readNumber(metrics.recentSevenMachineLowContentCount);
  const recentFiveMachineWeakContentCount = readNumber(metrics.recentFiveMachineWeakContentCount);
  const recentTwentyOneMachineGoodContentCount = readNumber(metrics.recentTwentyOneMachineGoodContentCount);
  const daysSinceMachineHighContent = readNullableNumber(metrics.daysSinceMachineHighContent);
  const daysSinceMachineStrongHighContent = readNullableNumber(metrics.daysSinceMachineStrongHighContent);
  const daysSinceMachineBigWin1500 = readNullableNumber(metrics.daysSinceMachineBigWin1500);
  const previousMachineHighContent = Boolean(metrics.previousMachineHighContent);
  const previousMachineGoodContent = Boolean(metrics.previousMachineGoodContent);
  const previousMachineWeakContent = Boolean(metrics.previousMachineWeakContent);
  const previousMachineStrongHighContent = Boolean(metrics.previousMachineStrongHighContent);
  const previousMachineSettingFivePlusProbability = readNullableNumber(
    metrics.previousMachineSettingFivePlusProbability,
  );
  const machineHighContentStreak = readNumber(metrics.machineHighContentStreak);
  const machineGoodContentStreak = readNumber(metrics.machineGoodContentStreak);
  const machineWeakContentStreak = readNumber(metrics.machineWeakContentStreak);
  const recentFiveBigWin1200Count = readNumber(metrics.recentFiveBigWin1200Count);
  const recentFiveBigWin1000Count = readNumber(metrics.recentFiveBigWin1000Count);
  const adjacentMachineHighContentCount3 = readNumber(metrics.adjacentMachineHighContentCount3);
  const adjacentMachineHighContentCount3Near2 = readNumber(metrics.adjacentMachineHighContentCount3Near2);
  const adjacentMachineHighContentCount7 = readNumber(metrics.adjacentMachineHighContentCount7);
  const adjacentMachineHighContentCount14 = readNumber(metrics.adjacentMachineHighContentCount14);
  const adjacentMachineHighContentCount7Near2 = readNumber(metrics.adjacentMachineHighContentCount7Near2);
  const adjacentMachineHighContentCount14Near2 = readNumber(metrics.adjacentMachineHighContentCount14Near2);
  const otherSameMachineHighContentCount7 = readNumber(metrics.otherSameMachineHighContentCount7);
  const adjacentMachineBigWin1000Count7Near2 = readNumber(metrics.adjacentMachineBigWin1000Count7Near2);
  const adjacentMachineNetTotal3 = readNumber(metrics.adjacentMachineNetTotal3);
  const adjacentMachineNetTotal3Near2 = readNumber(metrics.adjacentMachineNetTotal3Near2);
  const adjacentMachineNetTotal5 = readNumber(metrics.adjacentMachineNetTotal5);
  const adjacentMachineNetTotal5Near2 = readNumber(metrics.adjacentMachineNetTotal5Near2);
  const adjacentMachineNetTotal7 = readNumber(metrics.adjacentMachineNetTotal7);
  const adjacentMachineNetTotal7Near2 = readNumber(metrics.adjacentMachineNetTotal7Near2);
  const adjacentMachineNetTotal14 = readNumber(metrics.adjacentMachineNetTotal14);
  const adjacentMachineNetTotal14Near2 = readNumber(metrics.adjacentMachineNetTotal14Near2);
  const previousAdjacentMachineHighContentCount = readNumber(metrics.previousAdjacentMachineHighContentCount);
  const previousAdjacentMachineHighContentCountNear2 = readNumber(metrics.previousAdjacentMachineHighContentCountNear2);
  const previousAdjacentMachineGoodContentCount = readNumber(metrics.previousAdjacentMachineGoodContentCount);
  const previousAdjacentMachineWeakContentCount = readNumber(metrics.previousAdjacentMachineWeakContentCount);
  const previousAdjacentMachineBigWin1000Count = readNumber(metrics.previousAdjacentMachineBigWin1000Count);
  const previousAdjacentMachineNetTotal = readNumber(metrics.previousAdjacentMachineNetTotal);
  const previousAdjacentMachineNetTotalNear2 = readNumber(metrics.previousAdjacentMachineNetTotalNear2);
  const previousOtherMachineHighContentCount = readNumber(metrics.previousOtherMachineHighContentCount);
  const sameMachinePreviousNetTotal = readNumber(metrics.sameMachinePreviousNetTotal);
  const previousCombinedDenominator = features.previousCombinedDenominator;
  const previousRbDenominator = features.previousRbDenominator;
  const recentTwoCombinedDenominator = rateDenominator(recentTwoGamesTotal, recentTwoBonusTotal);
  const recentThreeRawDifferenceTotal = readNumber(metrics.recentThreeRawDifferenceTotal);
  const recentFiveRawDifferenceTotal = readNumber(metrics.recentFiveRawDifferenceTotal);
  const recentThreeRawDifferenceCount = readNumber(metrics.recentThreeRawDifferenceCount);
  const recentFiveRawDifferenceCount = readNumber(metrics.recentFiveRawDifferenceCount);
  const previousRawDifferenceValue = readNullableNumber(metrics.previousRawDifferenceValue);
  const rawDifferenceLosingStreak = readNumber(metrics.rawDifferenceLosingStreak);

  if (machineKey === "ultra-miracle" && activeLogicKey === "beam-hikari-ultra") {
    if (historyRowCount < 21) {
      return 0;
    }

    let recentUseScore = 0;
    recentUseScore += scoreAtLeast(recentFourteenMachineHighContentCount, [
      { minimum: 4, points: 22 },
      { minimum: 3, points: 15 },
      { minimum: 2, points: 8 },
      { minimum: 1, points: 4 },
    ]);
    recentUseScore += recentTwentyOneMachineHighContentCount >= 5 ? 4 : 0;
    recentUseScore = Math.min(recentUseScore, 26);

    let previousContentScore = 0;
    previousContentScore += previousMachineHighContent && previousDifference < 1000 ? 14 : 0;
    previousContentScore += previousMachineHighContent && previousDifference >= 1000 && previousDifference < 1500 ? 8 : 0;
    previousContentScore += previousRbDenominator <= 280 ? 8 : 0;
    previousContentScore = Math.min(previousContentScore, 18);

    let sinkScore = 0;
    sinkScore += features.recentThreeCombinedDenominator > 205 ? 10 : 0;
    sinkScore += features.recentFiveCombinedDenominator > 185 ? 7 : 0;
    sinkScore += recentFiveNetTotal <= -1300 && recentFiveNetTotal > -2300 ? 10 : 0;
    sinkScore += recentSevenNetTotal <= -1000 && recentSevenNetTotal > -2900 ? 8 : 0;
    sinkScore += recentThreeNetTotal <= -600 ? 4 : 0;
    sinkScore += features.recentFiveAngle <= -226.9 ? 4 : 0;
    sinkScore = Math.min(sinkScore, 25);

    let contextScore = 0;
    contextScore += recentTwentyOneGamesTotal >= 63600 ? 6 : 0;
    contextScore += recentFourteenGamesTotal >= 43200 ? 3 : 0;
    contextScore += previousAdjacentMachineHighContentCount > 0 ? 4 : 0;
    contextScore = Math.min(contextScore, 12);

    let penalty = 0;
    penalty += previousDifference >= 900 ? 12 : 0;
    penalty += previousDifference >= 1500 ? 10 : 0;
    penalty += previousGames >= 5500 ? 8 : 0;
    penalty += recentThreeGamesTotal >= 10700 ? 10 : 0;
    penalty += recentSevenGamesTotal >= 24300 ? 8 : 0;
    penalty += recentTwentyOneMachineHighContentCount === 0 ? 16 : 0;
    penalty += Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 14 ? 12 : 0;
    penalty += Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 21 ? 8 : 0;
    penalty += recentTwentyOneNetTotal <= -1600 ? 6 : 0;
    penalty += recentSevenNetTotal <= -2900 ? 12 : 0;
    penalty += recentFiveNetTotal <= -2300 ? 5 : 0;
    penalty += previousAdjacentMachineNetTotal <= -1000 ? 6 : 0;

    return Math.round(clamp(45 + recentUseScore + previousContentScore + sinkScore + contextScore - Math.min(penalty, 60), 0, 100));
  }

  if (machineKey === "ultra-miracle" && (activeLogicKey === "beam-hikari-ultra-normal" || activeLogicKey === "beam-hikari-ultra-event")) {
    if (historyRowCount < 21) {
      return 0;
    }

    if (activeLogicKey === "beam-hikari-ultra-event") {
      let intervalScore = 0;
      if (Number.isFinite(daysSinceMachineHighContent)) {
        intervalScore +=
          daysSinceMachineHighContent >= 8 && daysSinceMachineHighContent <= 13
            ? 12
            : daysSinceMachineHighContent >= 4 && daysSinceMachineHighContent <= 7
              ? 6
              : daysSinceMachineHighContent >= 1 && daysSinceMachineHighContent <= 3
                ? 4
                : 0;
      }
      intervalScore += scoreAtLeast(recentFourteenMachineHighContentCount, [
        { minimum: 4, points: 8 },
        { minimum: 3, points: 5 },
        { minimum: 2, points: 3 },
      ]);
      intervalScore = Math.min(intervalScore, 24);

      let sinkScore = 0;
      sinkScore += recentSevenNetTotal <= -1000 && recentSevenNetTotal > -2900 ? 14 : 0;
      sinkScore += recentFiveNetTotal <= -1300 && recentFiveNetTotal > -2300 ? 10 : 0;
      sinkScore += recentFiveNetTotal <= -1500 ? 8 : 0;
      sinkScore += features.recentFiveCombinedDenominator > 185 ? 8 : 0;
      sinkScore += features.recentThreeCombinedDenominator > 205 ? 6 : 0;
      sinkScore += features.recentFiveAngle <= -226.9 ? 4 : 0;
      sinkScore = Math.min(sinkScore, 28);

      let contextScore = 0;
      contextScore += previousAdjacentMachineHighContentCount > 0 ? 8 : 0;
      contextScore += recentTwentyOneGamesTotal >= 63600 ? 5 : 0;
      contextScore += recentFourteenGamesTotal >= 43200 ? 3 : 0;
      contextScore += previousRbDenominator <= 280 ? 5 : 0;
      contextScore = Math.min(contextScore, 15);

      let previousScore = 0;
      previousScore += previousMachineHighContent && previousDifference < 1000 ? 5 : 0;
      previousScore += previousDifference <= -800 ? 4 : 0;
      previousScore = Math.min(previousScore, 10);

      let penalty = 0;
      penalty += previousDifference >= 900 ? 16 : 0;
      penalty += previousDifference >= 1500 ? 8 : 0;
      penalty += previousMachineHighContent && previousDifference >= 1500 ? 12 : 0;
      penalty += previousGames >= 5500 ? 6 : 0;
      penalty += recentThreeGamesTotal >= 10700 ? 8 : 0;
      penalty += recentSevenGamesTotal >= 24300 ? 16 : 0;
      penalty += recentTwentyOneMachineHighContentCount === 0 ? 18 : 0;
      penalty += Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 14 ? 12 : 0;
      penalty += Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 21 ? 8 : 0;
      penalty += recentSevenNetTotal <= -2900 ? 12 : 0;
      penalty += previousAdjacentMachineNetTotal <= -1000 ? 5 : 0;

      return Math.round(clamp(45 + intervalScore + sinkScore + contextScore + previousScore - Math.min(penalty, 60), 0, 100));
    }

    let recentUseScore = 0;
    recentUseScore += scoreAtLeast(recentFourteenMachineHighContentCount, [
      { minimum: 4, points: 20 },
      { minimum: 3, points: 12 },
      { minimum: 2, points: 7 },
      { minimum: 1, points: 3 },
    ]);
    recentUseScore += recentTwentyOneMachineHighContentCount >= 5 ? 5 : 0;
    recentUseScore = Math.min(recentUseScore, 28);

    let previousContentScore = 0;
    previousContentScore += previousMachineHighContent && previousDifference < 1000 ? 18 : 0;
    previousContentScore += previousMachineHighContent && previousDifference >= 1000 && previousDifference < 1500 ? 8 : 0;
    previousContentScore += previousRbDenominator <= 280 ? 8 : 0;
    previousContentScore = Math.min(previousContentScore, 24);

    let sinkScore = 0;
    sinkScore += recentFiveNetTotal <= -1300 && recentFiveNetTotal > -2300 ? 9 : 0;
    sinkScore += recentSevenNetTotal <= -1000 && recentSevenNetTotal > -2900 ? 7 : 0;
    sinkScore += features.recentThreeCombinedDenominator > 205 ? 8 : 0;
    sinkScore += features.recentFiveCombinedDenominator > 185 ? 4 : 0;
    sinkScore += recentThreeNetTotal <= -600 ? 4 : 0;
    sinkScore = Math.min(sinkScore, 22);

    let contextScore = 0;
    contextScore += recentTwentyOneGamesTotal >= 63600 ? 7 : 0;
    contextScore += recentFourteenGamesTotal >= 43200 ? 3 : 0;
    contextScore += previousAdjacentMachineHighContentCount > 0 ? 3 : 0;
    contextScore = Math.min(contextScore, 12);

    let penalty = 0;
    penalty += previousDifference >= 900 ? 12 : 0;
    penalty += previousDifference >= 1500 ? 10 : 0;
    penalty += previousMachineHighContent && previousDifference >= 1500 ? 8 : 0;
    penalty += previousGames >= 5500 ? 8 : 0;
    penalty += recentThreeGamesTotal >= 10700 ? 10 : 0;
    penalty += recentSevenGamesTotal >= 24300 ? 8 : 0;
    penalty += recentTwentyOneMachineHighContentCount === 0 ? 16 : 0;
    penalty += Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 14 ? 12 : 0;
    penalty += Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 21 ? 8 : 0;
    penalty += recentSevenNetTotal <= -2900 ? 12 : 0;
    penalty += recentFiveNetTotal <= -2300 ? 5 : 0;
    penalty += previousAdjacentMachineNetTotal <= -1000 ? 6 : 0;

    return Math.round(clamp(45 + recentUseScore + previousContentScore + sinkScore + contextScore - Math.min(penalty, 60), 0, 100));
  }

  if (machineKey === "aim") {
    if (activeLogicKey === "mj-kurume-aim") {
      let sinkScore = 0;
      sinkScore += scoreAtMost(recentTenNetTotal, [
        { maximum: -3000, points: 22 },
        { maximum: -2500, points: 18 },
        { maximum: -2000, points: 13 },
        { maximum: -1500, points: 8 },
        { maximum: -1000, points: 4 },
      ]);
      sinkScore += scoreAtMost(recentTwentyOneNetTotal, [
        { maximum: -3000, points: 12 },
        { maximum: -2000, points: 9 },
        { maximum: -1000, points: 5 },
      ]);
      sinkScore += scoreAtMost(recentSevenNetTotal, [
        { maximum: -2500, points: 8 },
        { maximum: -2000, points: 5 },
      ]);
      sinkScore += features.recentTwentyOneAngle <= -50 ? 5 : 0;
      sinkScore = Math.min(sinkScore, 45);

      let stayScore = 0;
      stayScore += recentTenMinus3000StayDays >= 1 ? 10 : recentTenMinus2500StayDays >= 1 ? 6 : 0;
      stayScore +=
        recentTwentyOneMinus1500StayDays >= 6
          ? 8
          : recentTwentyOneMinus1500StayDays >= 3
            ? 5
            : 0;
      stayScore += recentFiveMinus2000StayDays >= 2 ? 5 : 0;
      stayScore = Math.min(stayScore, 20);

      const losingScore = scoreAtLeast(streak, [
        { minimum: 7, points: 15 },
        { minimum: 5, points: 10 },
        { minimum: 3, points: 5 },
      ]);

      let bonusScore = 0;
      bonusScore += previousMachineHighContent && previousDifference <= 0 ? 9 : 0;
      bonusScore +=
        features.recentFiveRbDenominator <= 270
          ? 8
          : features.recentFiveRbDenominator <= 300
            ? 4
            : 0;
      bonusScore += recentFiveMachineHighContentCount >= 2 ? 3 : 0;
      bonusScore = Math.min(bonusScore, 15);

      let gamesScore = 0;
      gamesScore +=
        (recentSevenNetTotal <= -2000 || recentTenNetTotal <= -2500) && recentSevenGamesTotal >= 15000
          ? 5
          : recentFiveGamesTotal >= 15000
            ? 2
            : 0;
      gamesScore -= recentSevenGamesTotal < 7000 && streak < 5 ? 5 : 0;

      let penalty = 0;
      penalty += recentSevenNetTotal >= 1500 ? 8 : 0;
      penalty += recentFourteenNetTotal >= 2000 ? 8 : 0;
      penalty += previousMachineHighContent && previousDifference >= 1200 ? 8 : 0;
      penalty += machineHighContentStreak >= 2 ? 10 : 0;
      penalty += winningStreak >= 3 ? 8 : 0;
      penalty += previousDifference > 800 && (previousRbDenominator > 350 || previousRbCount === 0) ? 8 : 0;
      penalty = Math.min(penalty, 25);

      return Math.round(clamp(sinkScore + stayScore + losingScore + bonusScore + gamesScore - penalty, 0, 100));
    }

    let score = 0;
    score += scoreAtMost(recentSevenNetTotal, [
      { maximum: -3000, points: 24 },
      { maximum: -2000, points: 19 },
      { maximum: -1000, points: 11 },
      { maximum: -1, points: 4 },
    ]);
    score += scoreAtMost(features.recentSevenAngle, [
      { maximum: -90, points: 14 },
      { maximum: -60, points: 11 },
      { maximum: -30, points: 7 },
      { maximum: -1, points: 3 },
    ]);
    score += scoreAtLeast(features.recentSevenCombinedDenominator, [
      { minimum: 165, points: 14 },
      { minimum: 160, points: 11 },
      { minimum: 155, points: 7 },
      { minimum: 150, points: 3 },
    ]);
    score += scoreAtLeast(streak, [
      { minimum: 5, points: 12 },
      { minimum: 4, points: 10 },
      { minimum: 3, points: 8 },
      { minimum: 2, points: 5 },
      { minimum: 1, points: 2 },
    ]);

    const aimNearbyFull =
      recentSevenNetTotal <= -2000 &&
      adjacentMachineHighContentCount14 > 0 &&
      adjacentMachineNetTotal7 >= 3000;
    const aimNearbyPartial =
      (recentSevenNetTotal <= -2000 && adjacentMachineHighContentCount14 > 0) ||
      (recentSevenNetTotal <= -1000 && adjacentMachineNetTotal7 >= 3000);
    const aimUnpaid =
      previousDifference > 0 &&
      recentSevenNetTotal <= -1500 &&
      features.recentSevenCombinedDenominator >= 155;
    score += aimNearbyFull ? 18 : aimNearbyPartial ? 12 : aimUnpaid ? 7 : 0;

    score += scoreAtLeast(daysSinceMachineHighContent, [
      { minimum: 35, points: 8 },
      { minimum: 21, points: 6 },
      { minimum: 14, points: 4 },
      { minimum: 7, points: 2 },
    ]);
    score += recentFourteenMachineHighContentCount === 0 ? 4 : 0;
    score -= recentFourteenMachineHighContentCount >= 2 ? 4 : 0;
    score += scoreInRange(recentSevenGamesTotal, 25000, 45000, 8);
    score += recentSevenGamesTotal > 45000 ? 6 : 0;
    score += scoreInRange(recentSevenGamesTotal, 20000, 24999, 4);
    score += recentThreeGamesTotal >= 15000 ? 3 : 0;

    score -= scoreAtLeast(recentSevenNetTotal, [
      { minimum: 5000, points: 18 },
      { minimum: 3000, points: 14 },
      { minimum: 2000, points: 10 },
    ]);
    score -= scoreAtLeast(recentThreeNetTotal, [
      { minimum: 3000, points: 12 },
      { minimum: 2000, points: 7 },
    ]);
    score -= previousDifference >= 1500 ? 6 : 0;
    const previousAimLooseHighContent =
      previousGames >= 5000 &&
      previousCombinedDenominator <= 145 &&
      previousRbDenominator <= 315;
    score -= previousAimLooseHighContent && recentSevenNetTotal > 0 ? 8 : 0;
    score -= recentSevenGamesTotal < 25000 ? 10 : 0;
    score -= previousGames < 1500 ? 5 : 0;

    return Math.round(clamp(score, 0, 100));
  }

  if (machineKey === "neo-aim") {
    if (activeLogicKey === "kintoki-kamata-neo-aim") {
      const previousFivePlus = previousMachineSettingFivePlusProbability;
      const hasPreviousFivePlus = Number.isFinite(previousFivePlus);

      let gamesTrustScore = 0;
      if (recentFiveGamesTotal >= 18000) {
        gamesTrustScore += 8;
      } else if (recentFiveGamesTotal >= 14000) {
        gamesTrustScore += 5;
      } else if (recentFiveGamesTotal >= 9000) {
        gamesTrustScore += 2;
      } else {
        gamesTrustScore -= 3;
      }
      if (recentSevenGamesTotal >= 22000) {
        gamesTrustScore += 4;
      } else if (recentSevenGamesTotal >= 16000) {
        gamesTrustScore += 2;
      }
      gamesTrustScore += previousGames >= 5000 ? 2 : 0;
      gamesTrustScore -= previousGames < 1000 ? 2 : 0;
      gamesTrustScore = clamp(gamesTrustScore, 0, 12);

      let sinkScore = 0;
      if (recentSevenNetTotal <= -2500 && recentFiveGamesTotal >= 14000) {
        sinkScore += 14;
      } else if (recentSevenNetTotal <= -3000) {
        sinkScore += 10;
      } else if (recentSevenNetTotal <= -1500) {
        sinkScore += 6;
      }
      sinkScore -= recentSevenNetTotal > 1500 ? 3 : 0;
      if (recentTenNetTotal <= -4000) {
        sinkScore += 7;
      } else if (recentTenNetTotal <= -2500) {
        sinkScore += 5;
      } else if (recentTenNetTotal > 2000) {
        sinkScore -= 2;
      }
      if (recentFourteenNetTotal >= -2000 && recentFourteenNetTotal <= 0) {
        sinkScore += 4;
      } else if (recentFourteenNetTotal > 3000) {
        sinkScore -= 4;
      }
      if (streak === 2) {
        sinkScore += 5;
      } else if (streak === 1) {
        sinkScore += 2;
      }
      sinkScore -= streak >= 5 ? 3 : 0;
      sinkScore += recentSevenGamesTotal >= 14000 && recentSevenNetTotal / recentSevenGamesTotal <= -0.12 ? 4 : 0;
      sinkScore = clamp(sinkScore, 0, 25);

      let momentumScore = 0;
      if (previousDifference >= 2000) {
        momentumScore += 8;
      } else if (previousDifference >= 1500) {
        momentumScore += 7;
      } else if (previousDifference >= 1000) {
        momentumScore += 3;
      } else if (previousDifference >= 0) {
        momentumScore -= 1;
      }
      if (recentTwoNetTotal >= 2000) {
        momentumScore += 7;
      } else if (recentTwoNetTotal >= 1500) {
        momentumScore += 2;
      }
      if (features.recentThreeCombinedDenominator <= 130) {
        momentumScore += 5;
      } else if (features.recentThreeCombinedDenominator <= 145) {
        momentumScore += 2;
      } else if (features.recentThreeCombinedDenominator > 160) {
        momentumScore -= 3;
      }
      momentumScore = clamp(momentumScore, 0, 18);

      let bonusScore = 0;
      if (hasPreviousFivePlus) {
        if (previousFivePlus >= 0.7) {
          bonusScore += 5;
        } else if (previousFivePlus >= 0.5) {
          bonusScore += 7;
        } else if (previousFivePlus < 0.2) {
          bonusScore += 2;
        }
      }
      if (previousCombinedDenominator <= 130) {
        bonusScore += 4;
      } else if (previousCombinedDenominator <= 145) {
        bonusScore += 2;
      }
      if (previousRbDenominator <= 300) {
        bonusScore += 4;
      } else if (previousRbDenominator <= 350) {
        bonusScore += 2;
      }
      bonusScore -= previousGames >= 3000 && previousRbDenominator > 600 ? 2 : 0;
      if (hasPreviousFivePlus && previousFivePlus >= 0.5) {
        bonusScore += previousDifference < 1000 ? 4 : 2;
      }
      bonusScore += previousDifference >= 1500 && hasPreviousFivePlus && previousFivePlus < 0.3 ? 3 : 0;
      bonusScore = clamp(bonusScore, 0, 20);

      let rotationScore = 0;
      if (Number.isFinite(daysSinceMachineHighContent)) {
        if (daysSinceMachineHighContent === 1) {
          rotationScore += 5;
        } else if (daysSinceMachineHighContent >= 2 && daysSinceMachineHighContent <= 3) {
          rotationScore += 3;
        } else if (daysSinceMachineHighContent >= 15 && daysSinceMachineHighContent <= 28) {
          rotationScore += 5;
        } else if (daysSinceMachineHighContent >= 29) {
          rotationScore -= 5;
        }
      }
      if (recentFourteenMachineHighContentCount === 1) {
        rotationScore += 5;
      } else if (recentFourteenMachineHighContentCount === 2) {
        rotationScore += 2;
      } else if (recentFourteenMachineHighContentCount >= 3) {
        rotationScore -= 2;
      }
      rotationScore += recentTwentyOneMachineHighContentCount === 1 ? 3 : 0;
      rotationScore -=
        recentTwentyOneMachineHighContentCount === 0 &&
        ((Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 21) ||
          (!Number.isFinite(daysSinceMachineHighContent) && historyRowCount >= 21))
          ? 2
          : 0;
      rotationScore = clamp(rotationScore, 0, 15);

      let nearbyScore = 0;
      if (previousAdjacentMachineHighContentCountNear2 >= 1 && recentSevenNetTotal <= -2500) {
        nearbyScore += 7;
      } else if (
        previousAdjacentMachineHighContentCountNear2 >= 1 &&
        recentSevenNetTotal <= 0 &&
        recentFiveGamesTotal >= 14000
      ) {
        nearbyScore += 4;
      }
      nearbyScore += previousAdjacentMachineHighContentCountNear2 >= 2 ? 3 : 0;
      nearbyScore -= previousAdjacentMachineNetTotalNear2 >= 2000 ? 2 : 0;
      nearbyScore = clamp(nearbyScore, 0, 10);

      let dangerPenalty = 0;
      dangerPenalty += recentFiveGamesTotal < 9000 && recentFourteenGamesTotal < 25000 ? 4 : 0;
      dangerPenalty +=
        recentFourteenMachineHighContentCount === 0 &&
        ((Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 29) ||
          (!Number.isFinite(daysSinceMachineHighContent) && historyRowCount >= 29))
          ? 5
          : 0;
      dangerPenalty += recentFourteenNetTotal > 3000 && recentSevenNetTotal > 1500 ? 4 : 0;
      dangerPenalty +=
        previousDifference >= 2500 &&
        hasPreviousFivePlus &&
        previousFivePlus >= 0.5 &&
        recentFourteenNetTotal > 0
          ? 4
          : 0;
      dangerPenalty += features.recentThreeCombinedDenominator > 180 && recentSevenGamesTotal < 16000 ? 3 : 0;

      const score =
        gamesTrustScore + sinkScore + momentumScore + bonusScore + rotationScore + nearbyScore - dangerPenalty;
      return Math.round(clamp(score, 0, historyRowCount < 14 ? 35 : 100));
    }

    if (activeLogicKey === "messe-minamisenju-neo-aim") {
      const previousPayoutRate = previousGames > 0 ? 100 + (previousDifference / previousGames / 3) * 100 : 100;
      const previousBbTailwind =
        previousDifference >= 1000 &&
        Number.isFinite(previousMachineSettingFivePlusProbability) &&
        previousMachineSettingFivePlusProbability < 0.3;
      const bonusWeak =
        features.recentFourteenRbDenominator >= 330 ||
        features.recentFourteenCombinedDenominator >= 155 ||
        features.recentFiveRbDenominator >= 350 ||
        features.recentFiveCombinedDenominator >= 155;
      const unrepaid =
        recentFourteenNetTotal <= 0 ||
        (Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 14) ||
        recentFiveMachineHighContentCount === 0;

      let sinkScore = 0;
      if (recentFiveMinus1000StayDays >= 8) {
        sinkScore += 14;
      } else if (recentFiveMinus1000StayDays >= 7) {
        sinkScore += 10;
      } else if (recentFiveMinus1500StayDays >= 6) {
        sinkScore += 8;
      }
      sinkScore += recentTenNetTotal <= -5000 ? 5 : 0;
      sinkScore += recentFiveNetTotal >= -2500 && recentFiveNetTotal < 0 ? 4 : 0;
      sinkScore += recentFourteenNetTotal <= 0 ? 3 : 0;
      sinkScore = Math.min(sinkScore, 24);

      let bonusScore = 0;
      if (features.recentFourteenRbDenominator >= 350) {
        bonusScore += 9;
      } else if (features.recentFourteenRbDenominator >= 330) {
        bonusScore += 5;
      }
      if (features.recentFourteenCombinedDenominator >= 160) {
        bonusScore += 7;
      } else if (features.recentFourteenCombinedDenominator >= 155) {
        bonusScore += 5;
      }
      bonusScore += features.recentFiveRbDenominator >= 350 ? 4 : 0;
      bonusScore += features.recentFiveCombinedDenominator >= 155 ? 3 : 0;
      bonusScore = Math.min(bonusScore, 18);

      let previousScore = 0;
      previousScore += previousDifference <= -1000 ? 8 : 0;
      previousScore += previousPayoutRate <= 86 ? 5 : 0;
      if (previousPayoutRate >= 108) {
        previousScore += 7;
      } else if (previousPayoutRate >= 106) {
        previousScore += 5;
      }
      if (previousMachineHighContent) {
        previousScore += previousDifference < 800 ? 5 : 3;
      }
      previousScore += previousBbTailwind ? 5 : 0;
      previousScore = Math.min(previousScore, 18);

      let intervalScore = 0;
      if (Number.isFinite(daysSinceMachineHighContent)) {
        if (daysSinceMachineHighContent >= 16) {
          intervalScore += 7;
        } else if (daysSinceMachineHighContent >= 14) {
          intervalScore += 5;
        } else if (daysSinceMachineHighContent >= 11) {
          intervalScore += 3;
        }
      }
      intervalScore += recentFiveMachineHighContentCount === 0 ? 4 : 0;
      intervalScore += recentFiveBigWin1000Count === 0 ? 2 : 0;
      intervalScore = Math.min(intervalScore, 13);

      let gameTrustScore = 0;
      if (recentFiveGamesTotal >= 12000) {
        gameTrustScore += 4;
      } else if (recentFiveGamesTotal >= 10000) {
        gameTrustScore += 2;
      }
      gameTrustScore += historyRowCount >= 14 ? 3 : 0;
      gameTrustScore = Math.min(gameTrustScore, 7);

      let penalty = 0;
      penalty += recentFiveGamesTotal < 9000 ? 8 : 0;
      penalty += streak >= 4 ? 6 : 0;
      penalty += recentFiveNetTotal <= -3500 && recentFiveMinus1000StayDays < 4 ? 8 : 0;
      penalty += recentFiveNetTotal >= 4000 && !bonusWeak && !unrepaid ? 9 : 0;
      penalty += recentFiveBigWin1000Count >= 2 ? 5 : 0;
      penalty +=
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent <= 5 &&
        recentFiveNetTotal >= 2000
          ? 5
          : 0;

      const score = 40 + sinkScore + bonusScore + previousScore + intervalScore + gameTrustScore - penalty;
      return Math.round(clamp(score, 0, historyRowCount < 5 ? 60 : 100));
    }

    if (activeLogicKey === "espace-ueno-neo-aim") {
      const recentTwentyOneRbTotal = readNumber(metrics.recentTwentyOneRbTotal);
      const recentTwentyOneRbDenominator = rateDenominator(recentTwentyOneGamesTotal, recentTwentyOneRbTotal);
      let score = 35;
      let scoreCap = 100;

      if (historyRowCount < 7) {
        scoreCap = 45;
      }

      if (recentThreeGamesTotal < 3000) {
        score += 3;
      } else if (recentThreeGamesTotal <= 6999) {
        score += 10;
      } else if (recentThreeGamesTotal <= 11999) {
        score += 12;
      } else if (recentThreeGamesTotal <= 14999) {
        score += 6;
      } else if (recentThreeGamesTotal <= 17999) {
        score -= 4;
      } else {
        score -= 8;
      }

      if (recentFiveGamesTotal < 10000) {
        score += 2;
      } else if (recentFiveGamesTotal <= 14999) {
        score += 10;
      } else if (recentFiveGamesTotal <= 19999) {
        score += 7;
      } else if (recentFiveGamesTotal <= 24999) {
        score += 3;
      } else if (recentFiveGamesTotal <= 29999) {
        score -= 5;
      } else {
        score -= 9;
      }

      if (recentFourteenGamesTotal >= 40000 && recentFourteenGamesTotal <= 59999) {
        score += 5;
      } else if (recentFourteenGamesTotal >= 60000 && recentFourteenGamesTotal <= 69999) {
        score += 2;
      } else if (recentFourteenGamesTotal >= 80000) {
        score -= 6;
      }

      if (recentFiveNetTotal >= -1500 && recentFiveNetTotal <= 500) {
        score += 10;
      } else if (
        (recentFiveNetTotal >= -2500 && recentFiveNetTotal <= -1501) ||
        (recentFiveNetTotal >= 501 && recentFiveNetTotal <= 1500)
      ) {
        score += 5;
      } else if (recentFiveNetTotal <= -4000) {
        score -= 6;
      } else if (recentFiveNetTotal >= 2500) {
        score -= 5;
      }

      if (recentSevenNetTotal >= -2500 && recentSevenNetTotal <= 0) {
        score += 7;
      } else if (recentSevenNetTotal >= 1 && recentSevenNetTotal <= 1500) {
        score += 3;
      } else if (recentSevenNetTotal <= -5000) {
        score -= 8;
      } else if (recentSevenNetTotal >= 4000) {
        score -= 3;
      }

      if (recentTwentyOneNetTotal >= 8000) {
        score += 3;
      } else if (recentTwentyOneNetTotal >= -7000 && recentTwentyOneNetTotal <= -5000) {
        score += 2;
      } else if (recentTwentyOneNetTotal <= -10000) {
        score -= 4;
      }

      if (recentTwentyOneGamesTotal >= 30000) {
        if (recentTwentyOneRbDenominator <= 270) {
          score += 13;
        } else if (recentTwentyOneRbDenominator <= 290) {
          score += 11;
        } else if (recentTwentyOneRbDenominator <= 300) {
          score += 8;
        } else if (recentTwentyOneRbDenominator <= 315) {
          score += 5;
        } else if (recentTwentyOneRbDenominator >= 400) {
          score -= 3;
        }
      }

      if (recentFourteenGamesTotal >= 20000) {
        if (features.recentFourteenRbDenominator <= 270) {
          score += 10;
        } else if (features.recentFourteenRbDenominator <= 290) {
          score += 8;
        } else if (features.recentFourteenRbDenominator <= 315) {
          score += 4;
        } else if (features.recentFourteenRbDenominator >= 400) {
          score -= 2;
        }
      }

      if (recentFiveGamesTotal >= 10000) {
        if (features.recentFiveCombinedDenominator <= 130) {
          score += 3;
        } else if (features.recentFiveCombinedDenominator >= 180) {
          score += 2;
        }
      }

      if (Number.isFinite(daysSinceMachineHighContent)) {
        if (daysSinceMachineHighContent >= 5 && daysSinceMachineHighContent <= 7) {
          score += 8;
        } else if (daysSinceMachineHighContent >= 14) {
          score += 6;
        } else if (daysSinceMachineHighContent >= 3 && daysSinceMachineHighContent <= 4) {
          score += 3;
        } else if (daysSinceMachineHighContent >= 1 && daysSinceMachineHighContent <= 2) {
          score -= 5;
        }
      }

      if (recentFourteenMachineHighContentCount === 0) {
        score += 5;
      } else if (recentFourteenMachineHighContentCount === 1) {
        score += 2;
      } else if (recentFourteenMachineHighContentCount >= 4) {
        score -= 2;
      }

      if (streak >= 3 && streak <= 6) {
        score += 5;
      } else if (streak >= 7) {
        score += 1;
      }

      if (previousDifference >= 1500 && previousDifference <= 1999) {
        score -= 4;
      } else if (previousDifference >= 2000) {
        score -= 8;
      }
      score -= previousMachineHighContent ? 5 : 0;
      score -= previousMachineGoodContent && previousDifference > 1500 ? 5 : 0;
      score -= previousAdjacentMachineHighContentCount >= 2 ? 4 : 0;
      score -= previousOtherMachineHighContentCount + (previousMachineHighContent ? 1 : 0) >= 8 ? 3 : 0;
      score -= historyRowCount < 14 ? 4 : 0;
      score -= recentFourteenGamesTotal < 30000 ? 4 : 0;

      return Math.round(clamp(score, 0, scoreCap));
    }

    if (activeLogicKey === "chikushino-neo-aim") {
      let score = 40;
      let scoreCap = 100;

      if (historyRowCount >= 7) {
        score += 4;
      } else if (historyRowCount >= 5) {
        score += 2;
      } else if (historyRowCount >= 3) {
        score -= 4;
        scoreCap = Math.min(scoreCap, 49);
      } else {
        score -= 12;
        scoreCap = Math.min(scoreCap, 39);
      }

      score += scoreInRange(recentThreeGamesTotal, 7000, 11000, 10);
      score += scoreInRange(recentThreeGamesTotal, 3000, 6999, 6);
      score += scoreInRange(recentThreeGamesTotal, 11001, 13000, 3);
      score -= recentThreeGamesTotal > 15000 ? 8 : 0;
      score -= recentThreeGamesTotal < 3000 ? 4 : 0;

      score += scoreInRange(recentFiveGamesTotal, 11000, 21000, 8);
      score += scoreInRange(recentFiveGamesTotal, 9000, 10999, 4);
      score -= scoreInRange(recentFiveGamesTotal, 21001, 25000, 5);
      score -= recentFiveGamesTotal > 25000 ? 10 : 0;
      score -= recentFiveGamesTotal < 9000 ? 6 : 0;

      score += historyRowCount >= 3 && recentThreeMachineLowContentCount === 0 ? 10 : 0;
      score -= recentThreeMachineLowContentCount >= 2 ? 4 : 0;
      score += historyRowCount >= 5 && recentFiveMachineLowContentCount === 0 ? 8 : 0;
      score -= recentFiveMachineLowContentCount >= 4 ? 8 : 0;
      score += historyRowCount >= 5 && recentFiveMachineWeakContentCount === 0 ? 7 : 0;
      score -= recentFiveMachineWeakContentCount >= 4 ? 8 : 0;
      score += historyRowCount >= 7 && recentSevenMachineLowContentCount <= 1 ? 6 : 0;
      score -= recentSevenMachineLowContentCount >= 5 ? 6 : 0;

      if (Number.isFinite(daysSinceMachineHighContent)) {
        score += daysSinceMachineHighContent >= 8 ? 12 : 0;
        score += daysSinceMachineHighContent >= 3 && daysSinceMachineHighContent <= 7 ? 8 : 0;
        score -= daysSinceMachineHighContent >= 1 && daysSinceMachineHighContent <= 2 ? 8 : 0;
      }
      score -= machineHighContentStreak >= 2 ? 4 : 0;
      score -= recentSevenMachineHighContentCount >= 3 ? 4 : 0;

      if (previousGames > 0) {
        score += previousGames <= 1500 ? 8 : 0;
        score += previousGames >= 1501 && previousGames <= 2500 ? 6 : 0;
        score += previousGames >= 2501 && previousGames <= 3500 ? 3 : 0;
        score -= previousGames >= 6000 ? 3 : 0;
      }

      score += previousGames >= 1000 && previousRbDenominator >= 500 ? 5 : 0;
      score -= previousGames >= 2500 && previousRbDenominator <= 260 ? 3 : 0;
      score += previousGames >= 1000 && previousCombinedDenominator >= 250 ? 4 : 0;
      score -= previousGames >= 3000 && previousCombinedDenominator <= 130 ? 2 : 0;

      score +=
        adjacentMachineHighContentCount3Near2 >= 3
          ? 10
          : adjacentMachineHighContentCount3Near2 >= 2
            ? 7
            : adjacentMachineHighContentCount3Near2 >= 1
              ? 2
              : 0;
      score += previousAdjacentMachineHighContentCount >= 2 ? 4 : 0;
      score -= previousAdjacentMachineWeakContentCount >= 3 ? 4 : 0;

      if (recentThreeRawDifferenceCount >= 3) {
        score +=
          recentThreeRawDifferenceTotal <= -1500
            ? 8
            : recentThreeRawDifferenceTotal <= -1000
              ? 6
              : recentThreeRawDifferenceTotal <= 0
                ? 3
                : 0;
        score -= recentThreeRawDifferenceTotal >= 3000 ? 8 : recentThreeRawDifferenceTotal >= 1500 ? 4 : 0;
      }
      score += rawDifferenceLosingStreak >= 3 ? 6 : rawDifferenceLosingStreak >= 2 ? 4 : 0;
      score += Number.isFinite(previousRawDifferenceValue) && previousRawDifferenceValue <= -1000 ? 4 : 0;
      score -= Number.isFinite(previousRawDifferenceValue) && previousRawDifferenceValue >= 1000 ? 4 : 0;
      score -= recentFiveRawDifferenceCount >= 5 && recentFiveRawDifferenceTotal >= 3000 ? 7 : 0;

      return Math.round(clamp(score, 0, scoreCap));
    }

    if (activeLogicKey === "hinode-onojo-neo-aim") {
      let score = 0;

      score += scoreAtMost(recentFiveNetTotal, [
        { maximum: -5000, points: 35 },
        { maximum: -4000, points: 32 },
        { maximum: -3000, points: 27 },
        { maximum: -2000, points: 21 },
        { maximum: -1000, points: 13 },
        { maximum: 0, points: 6 },
      ]);
      score += scoreAtMost(recentSevenNetTotal, [
        { maximum: -5000, points: 18 },
        { maximum: -3500, points: 15 },
        { maximum: -2500, points: 12 },
        { maximum: -1500, points: 8 },
        { maximum: 0, points: 3 },
      ]);
      score += scoreAtLeast(streak, [
        { minimum: 7, points: 22 },
        { minimum: 6, points: 20 },
        { minimum: 5, points: 18 },
        { minimum: 4, points: 14 },
        { minimum: 3, points: 10 },
        { minimum: 2, points: 5 },
        { minimum: 1, points: 1 },
      ]);
      score += scoreAtMost(recentThreeNetTotal, [
        { maximum: -4000, points: 12 },
        { maximum: -3000, points: 9 },
        { maximum: -2000, points: 6 },
        { maximum: -1000, points: 3 },
      ]);
      score += scoreAtLeast(features.recentSevenCombinedDenominator, [
        { minimum: 180, points: 10 },
        { minimum: 170, points: 8 },
        { minimum: 160, points: 4 },
      ]);
      score += scoreAtLeast(features.recentFiveCombinedDenominator, [
        { minimum: 180, points: 6 },
        { minimum: 170, points: 4 },
      ]);

      if (recentSevenGamesTotal >= 25000) {
        score += 6;
      } else if (recentFiveGamesTotal >= 18000) {
        score += 4;
      } else if (recentFiveGamesTotal >= 12000) {
        score += 2;
      }

      score +=
        Number.isFinite(previousMachineSettingFivePlusProbability) &&
        previousMachineSettingFivePlusProbability >= 0.7 &&
        previousDifference <= 500 &&
        recentFiveNetTotal <= 0
          ? 5
          : 0;
      score +=
        Number.isFinite(previousMachineSettingFivePlusProbability) &&
        previousMachineSettingFivePlusProbability >= 0.9 &&
        previousDifference <= 1000
          ? 3
          : 0;
      score += previousAdjacentMachineHighContentCount > 0 && recentFiveNetTotal <= -3000 ? 4 : 0;

      score -= scoreAtLeast(recentFiveNetTotal, [
        { minimum: 4500, points: 24 },
        { minimum: 3000, points: 18 },
        { minimum: 2000, points: 10 },
      ]);
      score -= scoreAtLeast(recentSevenNetTotal, [
        { minimum: 5000, points: 18 },
        { minimum: 3500, points: 12 },
        { minimum: 2500, points: 8 },
      ]);
      score -= scoreAtLeast(recentThreeNetTotal, [
        { minimum: 2500, points: 12 },
        { minimum: 1500, points: 8 },
      ]);
      score -= recentFiveBigWin1200Count >= 2 && recentFiveNetTotal >= 1000 ? 8 : 0;
      score -= recentSevenMachineHighContentCount >= 2 && recentSevenNetTotal >= 0 ? 8 : 0;
      score -= recentFiveGamesTotal < 12000 ? 10 : 0;

      const roundedScore = Math.round(clamp(score, 0, 100));
      return historyRowCount < 7 ? Math.min(roundedScore, 40) : roundedScore;
    }

    if (activeLogicKey === "gogo-tenjin-neo-aim") {
      if (historyRowCount < 28) {
        return 0;
      }

      const recentTwentyEightAngleRaw =
        recentTwentyEightGamesTotal > 0 ? recentTwentyEightNetTotal / recentTwentyEightGamesTotal : 0;
      let score = 0;

      score += scoreAtMost(recentTwentyEightNetTotal, [
        { maximum: -7000, points: 36 },
        { maximum: -5000, points: 32 },
        { maximum: -3000, points: 26 },
        { maximum: -1500, points: 18 },
        { maximum: 0, points: 12 },
        { maximum: 3000, points: 5 },
      ]);
      score += scoreAtMost(recentTwentyEightAngleRaw, [
        { maximum: -0.055, points: 18 },
        { maximum: -0.04, points: 15 },
        { maximum: -0.025, points: 12 },
        { maximum: -0.01, points: 8 },
        { maximum: 0, points: 5 },
      ]);
      score += scoreAtMost(recentTwentyOneNetTotal, [
        { maximum: -5000, points: 10 },
        { maximum: -3000, points: 8 },
        { maximum: -1500, points: 5 },
        { maximum: 0, points: 2 },
      ]);
      score += scoreAtMost(recentSevenNetTotal, [
        { maximum: -1500, points: 6 },
        { maximum: -500, points: 4 },
        { maximum: 0, points: 2 },
      ]);

      score += scoreInRange(streak, 5, 7, 7);
      score += scoreInRange(streak, 3, 4, 5);
      score += streak === 2 ? 2 : 0;

      score += scoreInRange(daysSinceMachineHighContent, 4, 6, 5);
      score += scoreInRange(daysSinceMachineHighContent, 7, 10, 3);
      score += Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 25 ? 2 : 0;

      score +=
        recentFourteenMachineHighContentCount === 1
          ? 3
          : recentFourteenMachineHighContentCount === 2
            ? 2
            : recentFourteenMachineHighContentCount >= 3
              ? -3
              : 0;

      score +=
        previousGames >= 3000 && previousRbDenominator <= 300 && previousDifference < 1000
          ? 5
          : 0;
      score +=
        previousGames >= 3000 && previousCombinedDenominator <= 140 && previousDifference < 1500
          ? 3
          : 0;
      score += previousMachineHighContent && previousDifference < 0 ? 6 : 0;

      const treatmentPenalty =
        recentTwentyEightNetTotal >= 7000
          ? 20
          : recentTwentyEightNetTotal >= 4000
            ? 12
            : recentTwentyOneNetTotal >= 5000
              ? 8
              : previousDifference >= 2500
                ? 4
                : 0;
      score -= treatmentPenalty;
      score -= recentFourteenGamesTotal < 30000 ? 4 : 0;

      return Math.round(clamp(score, 0, 100));
    }

    if (activeLogicKey === "amuse-asakusa-neo-aim") {
      if (historyRowCount < 21) {
        return 0;
      }

      const recentTenAngle = Number.isFinite(features.recentTenAngle)
        ? features.recentTenAngle
        : netPerThousandGames(recentTenNetTotal, recentTenGamesTotal);
      const recentTwentyOneCombinedDenominator = rateDenominator(
        recentTwentyOneGamesTotal,
        readNumber(metrics.recentTwentyOneBonusTotal),
      );

      let repayScore = 0;
      repayScore +=
        recentTenNetTotal <= -4000 && recentTenGamesTotal >= 15000
          ? 12
          : recentTenNetTotal <= -2250
            ? 10
            : recentTenNetTotal <= -1000
              ? 8
              : recentTenNetTotal <= 0
                ? 4
                : 0;
      repayScore +=
        recentTwentyOneNetTotal <= -5000 && recentTwentyOneGamesTotal >= 30000
          ? 10
          : recentTwentyOneNetTotal <= -3000
            ? 8
            : recentTwentyOneNetTotal <= -1500
              ? 6
              : recentTwentyOneNetTotal <= 0
                ? 3
                : 0;
      repayScore = Math.min(repayScore, 24);

      const sinkStayScore = Math.max(
        recentFourteenMinus1500StayDays >= 7 ? 12 : 0,
        recentFourteenMinus2000StayDays >= 10 ? 15 : 0,
        recentFourteenMinus500StayDays >= 14 ? 14 : 0,
        recentTwentyOneMinus1500StayDays >= 5 ? 10 : 0,
        recentTwentyOneMinus2000StayDays >= 7 ? 14 : 0,
        recentTwentyOneMinus3000StayDays >= 7 ? 16 : 0,
        recentTwentyOneMinus5000StayDays >= 4 ? 15 : 0,
      );

      const angleScore = Math.min(
        (recentTenAngle <= -70 ? 8 : recentTenAngle <= -30 ? 6 : recentTenAngle <= -10 ? 3 : 0) +
          (features.recentTwentyOneAngle <= -50
            ? 7
            : features.recentTwentyOneAngle <= -20
              ? 5
              : features.recentTwentyOneAngle <= 0
                ? 2
                : 0),
        14,
      );

      const intervalScore = Math.max(
        Number.isFinite(daysSinceMachineHighContent)
          ? daysSinceMachineHighContent >= 21
            ? 8
            : daysSinceMachineHighContent >= 16
              ? 7
              : daysSinceMachineHighContent >= 11
                ? 4
                : daysSinceMachineHighContent === 6
                  ? 3
                  : 0
          : 0,
        recentFourteenMachineHighContentCount === 0 ? 6 : 0,
        recentTwentyOneMachineHighContentCount === 0 ? 8 : 0,
      );

      let genuineScore = 0;
      genuineScore += previousMachineHighContent ? 4 : 0;
      genuineScore += previousMachineHighContent
        ? previousDifference <= 500
          ? 8
          : previousDifference <= 1500
            ? 7
            : previousDifference <= 2000
              ? 6
              : previousDifference <= 2500
                ? 3
                : 0
        : 0;
      genuineScore += previousMachineHighContent && previousRbDenominator <= 250 ? 3 : 0;
      genuineScore += previousMachineHighContent && recentTwentyOneNetTotal <= 0 ? 5 : 0;
      genuineScore += previousMachineHighContent && recentFourteenNetTotal <= 0 ? 3 : 0;
      genuineScore += previousGames >= 4000 && previousRbDenominator <= 250 ? 4 : 0;
      genuineScore += previousGames >= 4000 && previousRbDenominator <= 270 ? 2 : 0;
      genuineScore = Math.min(genuineScore, 16);

      const bonusWeakScore = Math.min(
        (recentTwentyOneCombinedDenominator >= 150 && recentTwentyOneGamesTotal >= 30000 ? 4 : 0) +
          (features.recentFourteenRbDenominator >= 360 && recentFourteenGamesTotal >= 21000 ? 4 : 0) +
          (features.recentSevenCombinedDenominator >= 150 && recentSevenGamesTotal >= 10500 ? 2 : 0),
        8,
      );

      const losingScore = streak === 3 ? 8 : streak === 2 ? 3 : streak >= 5 ? 2 : 0;
      const gamesScore = Math.min(
        (recentFiveGamesTotal >= 6000 && recentFiveGamesTotal <= 19500 ? 5 : 0) +
          (recentFiveGamesTotal >= 19501 && recentFiveGamesTotal <= 24000 ? 2 : 0) +
          (recentTenGamesTotal >= 15000 ? 2 : 0) +
          (previousGames >= 1500 ? 1 : 0) -
          (recentFiveGamesTotal < 4000 ? 3 : 0),
        8,
      );

      const nearbyScore = Math.min(
        (adjacentMachineNetTotal7Near2 >= 3000 && recentSevenNetTotal <= 0 ? 4 : 0) +
          (adjacentMachineHighContentCount3Near2 === 1 ? 2 : 0),
        6,
      );

      let penaltyScore = 0;
      penaltyScore +=
        recentSevenNetTotal >= 7000
          ? 16
          : recentSevenNetTotal >= 5000
            ? 12
            : recentSevenNetTotal >= 3000
              ? 7
              : 0;
      penaltyScore += recentFiveNetTotal >= 5000 ? 10 : recentFiveNetTotal >= 4000 ? 6 : 0;
      penaltyScore +=
        recentTwentyOneNetTotal >= 12000 ? 12 : recentTwentyOneNetTotal >= 10000 ? 8 : 0;
      penaltyScore +=
        previousMachineHighContent && previousDifference >= 3000
          ? 10
          : previousMachineHighContent && previousDifference >= 2500
            ? 6
            : 0;
      penaltyScore += adjacentMachineHighContentCount3Near2 >= 4 ? 5 : adjacentMachineHighContentCount3Near2 >= 3 ? 3 : 0;
      penaltyScore += recentTenGamesTotal < 8000 ? 5 : 0;
      penaltyScore = Math.min(penaltyScore, 24);

      return Math.round(
        clamp(
          repayScore +
            sinkStayScore +
            angleScore +
            intervalScore +
            genuineScore +
            bonusWeakScore +
            losingScore +
            gamesScore +
            nearbyScore -
            penaltyScore,
          0,
          100,
        ),
      );
    }

    if (
      activeLogicKey === "beam-hikari-neo-aim" ||
      activeLogicKey === "beam-hikari-neo-aim-event" ||
      activeLogicKey === "beam-hikari-neo-aim-normal"
    ) {
      if (historyRowCount < 21 || recentTwoGamesTotal <= 0) {
        return 0;
      }

      const recentTwoRbTotal = readNumber(metrics.recentTwoRbTotal);
      const recentTwoRbDenominator = rateDenominator(recentTwoGamesTotal, recentTwoRbTotal);
      let score = 0;

      score += scoreAtMost(features.recentTwoAngle, [
        { maximum: -500, points: 32 },
        { maximum: -380, points: 27 },
        { maximum: -300, points: 21 },
        { maximum: -250, points: 15 },
        { maximum: -200, points: 10 },
      ]);

      score += scoreAtLeast(recentTwoCombinedDenominator, [
        { minimum: 260, points: 24 },
        { minimum: 225, points: 20 },
        { minimum: 207, points: 15 },
        { minimum: 195, points: 10 },
        { minimum: 185, points: 6 },
      ]);

      score += scoreAtMost(recentTwoGamesTotal, [
        { maximum: 1700, points: 14 },
        { maximum: 2500, points: 11 },
        { maximum: 3100, points: 8 },
        { maximum: 3600, points: 5 },
        { maximum: 4100, points: 3 },
      ]);

      score +=
        recentTwoRbDenominator >= 650
          ? 6
          : recentTwoRbDenominator >= 500
            ? 4
            : recentTwoRbDenominator >= 400
              ? 2
              : 0;

      score += recentSevenNetTotal <= -2500 && recentSevenGamesTotal >= 17000 ? 4 : 0;
      score +=
        recentTwentyOneNetTotal <= -3000 &&
        recentThreeNetTotal < 1500 &&
        recentTwentyOneGamesTotal >= 45000
          ? 3
          : 0;
      score += streak >= 2 && streak <= 4 && recentSevenGamesTotal >= 15000 ? 4 : 0;
      score +=
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent >= 4 &&
        daysSinceMachineHighContent <= 12 &&
        recentFourteenGamesTotal >= 25000
          ? 3
          : 0;
      score +=
        recentFourteenMachineHighContentCount === 1 &&
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent >= 4 &&
        daysSinceMachineHighContent <= 10
          ? 3
          : 0;
      score +=
        previousAdjacentMachineHighContentCount > 0 &&
        recentSevenNetTotal < 0 &&
        recentSevenGamesTotal >= 12000
          ? 2
          : 0;

      score -= previousMachineHighContent && previousGames >= 3000 ? 18 : 0;
      score -= previousMachineStrongHighContent && previousGames >= 4000 ? 7 : 0;
      score -= recentThreeNetTotal >= 2500 && recentThreeGamesTotal >= 7000 ? 10 : 0;
      score -= recentSevenNetTotal >= 4000 && recentSevenGamesTotal >= 15000 ? 7 : 0;
      score -= recentSevenMachineHighContentCount >= 2 && recentSevenNetTotal >= 0 ? 5 : 0;
      score -= recentSevenGamesTotal < 5000 ? 4 : 0;
      score -=
        recentTwentyOneMachineHighContentCount === 0 &&
        recentTwentyOneGamesTotal >= 50000 &&
        recentTwentyOneNetTotal <= -6000
          ? 3
          : 0;
      score -= recentTwoNetTotal > 1100 ? 8 : recentTwoNetTotal > 100 ? 4 : 0;
      score -=
        recentTwoGamesTotal > 9000
          ? 8
          : previousGames > 5345
            ? 6
            : recentSevenGamesTotal > 26086
              ? 4
              : recentFourteenGamesTotal > 50028
                ? 3
                : 0;
      score -= features.recentFiveAngle > 110 ? 4 : 0;

      return Math.round(clamp(score, 0, 100));
    }

    if (activeLogicKey === "apark-yakatabaru-neo-aim") {
      let losingScore = scoreAtLeast(streak, [
        { minimum: 4, points: 24 },
        { minimum: 3, points: 22 },
        { minimum: 2, points: 16 },
        { minimum: 1, points: 5 },
      ]);

      let shortSinkScore = 0;
      shortSinkScore += scoreAtMost(recentThreeNetTotal, [
        { maximum: -2200, points: 12 },
        { maximum: -1450, points: 9 },
        { maximum: -900, points: 6 },
        { maximum: -400, points: 3 },
      ]);
      shortSinkScore += scoreAtMost(recentFiveNetTotal, [
        { maximum: -2700, points: 8 },
        { maximum: -1780, points: 6 },
        { maximum: -1110, points: 3 },
      ]);
      shortSinkScore += scoreAtMost(recentSevenNetTotal, [
        { maximum: -3000, points: 5 },
        { maximum: -2000, points: 3 },
      ]);
      shortSinkScore += scoreAtMost(features.recentFiveAngle, [
        { maximum: -120, points: 6 },
        { maximum: -90, points: 4 },
        { maximum: -60, points: 2 },
      ]);
      shortSinkScore = Math.min(shortSinkScore, 24);

      let unpaidScore = 0;
      unpaidScore += scoreAtMost(recentFourteenNetTotal, [
        { maximum: -5000, points: 9 },
        { maximum: -3000, points: 6 },
        { maximum: -1500, points: 3 },
      ]);
      unpaidScore += scoreAtMost(recentTwentyOneNetTotal, [
        { maximum: -7000, points: 8 },
        { maximum: -5000, points: 5 },
        { maximum: -2500, points: 2 },
      ]);
      unpaidScore += scoreAtLeast(recentFiveMinus1000StayDays, [
        { minimum: 3, points: 5 },
        { minimum: 2, points: 4 },
        { minimum: 1, points: 2 },
      ]);
      unpaidScore = Math.min(unpaidScore, 16);

      let previousScore = 0;
      previousScore += previousMachineHighContent && previousDifference < 0 ? 12 : 0;
      previousScore += previousMachineHighContent && previousDifference >= 0 && previousDifference < 500 ? 5 : 0;
      previousScore += previousMachineGoodContent && previousDifference < 500 ? 4 : 0;
      previousScore += previousGames >= 5000 && previousDifference <= -1000 ? 4 : 0;
      previousScore = Math.min(previousScore, 14);

      let weakBonusScore = 0;
      weakBonusScore += scoreAtLeast(features.recentFiveCombinedDenominator, [
        { minimum: 165, points: 7 },
        { minimum: 160, points: 5 },
        { minimum: 156, points: 3 },
      ]);
      weakBonusScore += scoreAtLeast(features.recentSevenCombinedDenominator, [
        { minimum: 162, points: 4 },
        { minimum: 158, points: 2 },
      ]);
      weakBonusScore +=
        recentFiveGamesTotal >= 12000 && recentFiveGamesTotal <= 24000
          ? 4
          : recentFiveGamesTotal >= 9000 && recentFiveGamesTotal < 12000
            ? 1
            : 0;
      weakBonusScore = Math.min(weakBonusScore, 12);

      let rotationScore = 0;
      rotationScore += scoreInRange(daysSinceMachineHighContent, 3, 13, 3);
      rotationScore += scoreInRange(daysSinceMachineHighContent, 14, 28, 1);

      let penaltyScore = 0;
      penaltyScore += previousMachineHighContent && previousDifference >= 1000 ? 18 : 0;
      penaltyScore += previousMachineGoodContent && previousDifference >= 1000 ? 14 : 0;
      penaltyScore += scoreAtLeast(previousDifference, [
        { minimum: 2500, points: 12 },
        { minimum: 1500, points: 8 },
        { minimum: 1000, points: 4 },
      ]);
      penaltyScore += scoreAtLeast(recentThreeNetTotal, [
        { minimum: 2500, points: 10 },
        { minimum: 1500, points: 7 },
        { minimum: 800, points: 4 },
      ]);
      penaltyScore += scoreAtLeast(recentFiveNetTotal, [
        { minimum: 4000, points: 8 },
        { minimum: 2500, points: 5 },
      ]);
      penaltyScore += streak === 0 ? 8 : 0;
      penaltyScore += winningStreak >= 2 ? 5 : 0;
      penaltyScore += scoreAtLeast(recentTwentyOneMachineGoodContentCount, [
        { minimum: 5, points: 7 },
        { minimum: 4, points: 4 },
      ]);
      penaltyScore += recentFiveGamesTotal < 9000 ? 5 : 0;
      penaltyScore += targetRangeHistoryRowCount < 21 ? 20 : 0;

      const rawScore =
        35 +
        losingScore +
        shortSinkScore +
        unpaidScore +
        previousScore +
        weakBonusScore +
        rotationScore -
        penaltyScore;

      return Math.round(clamp(rawScore, 0, 100));
    }

    if (activeLogicKey === "mj-kurume-neo-aim") {
      let sinkScore = 0;
      sinkScore += scoreAtMost(recentTwentyEightNetTotal, [
        { maximum: -5000, points: 16 },
        { maximum: -4000, points: 12 },
        { maximum: -3000, points: 8 },
        { maximum: 0, points: 3 },
      ]);
      sinkScore += scoreAtMost(recentTwentyOneNetTotal, [
        { maximum: -3000, points: 10 },
        { maximum: -2000, points: 7 },
        { maximum: 0, points: 3 },
      ]);
      sinkScore += scoreAtMost(recentFourteenNetTotal, [
        { maximum: -4000, points: 8 },
        { maximum: -2000, points: 6 },
        { maximum: -1000, points: 4 },
        { maximum: 0, points: 2 },
      ]);
      sinkScore += scoreAtMost(recentFiveNetTotal, [
        { maximum: -3000, points: 6 },
        { maximum: -2000, points: 4 },
        { maximum: -1000, points: 2 },
      ]);
      sinkScore = Math.min(sinkScore, 35);

      let stayScore = 0;
      stayScore += recentSevenLossDays >= 7 ? 14 : 0;
      stayScore +=
        recentFourteenLossDays >= 12
          ? 10
          : recentFourteenLossDays >= 11
            ? 7
            : recentFourteenLossDays >= 10
              ? 4
              : 0;
      stayScore += recentFourteenMinus2000StayDays >= 10 ? 4 : 0;
      stayScore += streak >= 4 ? 4 : streak === 3 ? 3 : streak === 2 ? 1 : 0;
      stayScore = Math.min(stayScore, 22);

      let genuineScore = 0;
      genuineScore += previousMachineStrongHighContent && previousGames >= 3000 && previousDifference < 500 ? 14 : 0;
      genuineScore += previousMachineHighContent && previousGames >= 3000 && previousDifference < 0 ? 10 : 0;
      genuineScore += previousMachineHighContent && previousGames >= 3000 && previousDifference < 500 ? 6 : 0;
      genuineScore += previousGames >= 3000 && previousRbDenominator <= 300 && previousCombinedDenominator <= 140 ? 6 : 0;
      genuineScore += recentThreeMachineHighContentCount >= 2 ? 5 : recentThreeMachineHighContentCount >= 1 ? 4 : 0;
      genuineScore += recentSevenMachineHighContentCount >= 3 ? 3 : 0;
      genuineScore = Math.min(genuineScore, 22);

      let gamesScore = 0;
      gamesScore += recentThreeGamesTotal >= 12000 ? 4 : recentThreeGamesTotal >= 10500 ? 2 : 0;
      gamesScore += recentFourteenGamesTotal >= 18000 && recentFourteenGamesTotal <= 45000 ? 3 : 0;
      gamesScore += adjacentMachineHighContentCount7 >= 2 && recentSevenMachineHighContentCount <= 1 ? 3 : 0;
      gamesScore = Math.min(gamesScore, 10);

      let rotationScore = 0;
      rotationScore +=
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent >= 14 &&
        daysSinceMachineHighContent <= 21 &&
        recentTwentyOneNetTotal <= 0
          ? 4
          : 0;
      rotationScore +=
        Number.isFinite(daysSinceMachineStrongHighContent) &&
        daysSinceMachineStrongHighContent >= 28 &&
        recentTwentyEightNetTotal <= 0
          ? 3
          : 0;
      rotationScore += recentFourteenMachineHighContentCount >= 3 && recentFourteenMachineHighContentCount <= 5 ? 2 : 0;
      rotationScore = Math.min(rotationScore, 6);

      let nearbyScore = 0;
      nearbyScore += adjacentMachineHighContentCount7 >= 2 && recentFourteenNetTotal <= 0 ? 5 : 0;
      nearbyScore += previousAdjacentMachineHighContentCount >= 1 && recentSevenNetTotal <= 0 ? 2 : 0;
      nearbyScore = Math.min(nearbyScore, 5);

      let penalty = 0;
      penalty += scoreAtLeast(recentTwentyOneNetTotal, [{ minimum: 5000, points: 18 }]);
      penalty += scoreAtLeast(recentFourteenNetTotal, [{ minimum: 4000, points: 14 }]);
      penalty += scoreAtLeast(recentSevenNetTotal, [{ minimum: 3000, points: 10 }]);
      penalty += previousMachineHighContent && previousGames >= 3000 && previousDifference >= 1500 ? 12 : 0;
      penalty += previousDifference >= 2500 ? 8 : 0;
      penalty +=
        recentTwentyEightNetTotal >= 4000 || (recentFourteenLossDays <= 4 && recentFourteenNetTotal >= 1000)
          ? 8
          : 0;
      penalty += recentThreeMachineHighContentCount === 0 && recentFourteenNetTotal > -1000 ? 8 : 0;
      penalty += recentFourteenGamesTotal < 8000 && recentFourteenNetTotal > -2000 ? 6 : 0;
      penalty += previousDifference >= 1000 && !previousMachineHighContent && previousRbDenominator >= 400 ? 6 : 0;
      penalty += previousAdjacentMachineHighContentCount >= 2 ? 5 : 0;
      penalty = Math.min(penalty, 35);

      const rawScore =
        45 + sinkScore + stayScore + genuineScore + gamesScore + rotationScore + nearbyScore - penalty;
      const cappedScore =
        targetRangeHistoryRowCount < 7 ? Math.min(rawScore, 40) :
        targetRangeHistoryRowCount < 14 ? Math.min(rawScore, 55) :
        rawScore;

      return Math.round(clamp(cappedScore, 0, 100));
    }

    let sinkScore = 0;
    sinkScore += scoreAtMost(recentSevenNetTotal, [
      { maximum: -2000, points: 22 },
      { maximum: -1000, points: 14 },
      { maximum: 0, points: 5 },
    ]);
    sinkScore += scoreAtMost(features.recentSevenAngle, [
      { maximum: -57, points: 12 },
      { maximum: -21, points: 7 },
      { maximum: 0, points: 3 },
    ]);
    const sevenSinkStayScore =
      recentSevenMinus2000StayDays >= 4 ? 9 :
      recentSevenMinus2000StayDays >= 2 ? 7 :
      recentSevenMinus2000StayDays >= 1 ? 4 :
      0;
    const threeSinkStayScore =
      recentThreeMinus1700StayDays >= 3 ? 6 :
      recentThreeMinus1700StayDays >= 2 ? 4 :
      recentThreeMinus1700StayDays >= 1 ? 2 :
      0;
    sinkScore += Math.max(sevenSinkStayScore, threeSinkStayScore);
    sinkScore = Math.min(sinkScore, 45);

    const streakScore = scoreAtLeast(streak, [
      { minimum: 5, points: 15 },
      { minimum: 4, points: 14 },
      { minimum: 3, points: 11 },
      { minimum: 2, points: 7 },
      { minimum: 1, points: 2 },
    ]);

    const restScore = recentSevenMachineHighContentCount === 0 &&
      Number.isFinite(daysSinceMachineHighContent) &&
      daysSinceMachineHighContent >= 8 &&
      daysSinceMachineHighContent <= 28
      ? 10
      : recentSevenMachineHighContentCount === 0
        ? 7
        : recentFourteenMachineHighContentCount <= 1
          ? 4
          : 0;

    const repayScore = Math.max(
      previousMachineGoodContent && previousDifference < 0 ? 15 : 0,
      previousMachineGoodContent && previousDifference < 500 && recentSevenNetTotal < 0 ? 12 : 0,
      previousMachineHighContent && previousDifference < 500 && recentSevenNetTotal < 0 ? 10 : 0,
      previousDifference > 0 && recentSevenNetTotal <= -2000 ? 5 : 0,
    );

    let weakScore = 0;
    weakScore += scoreAtLeast(features.recentSevenCombinedDenominator, [
      { minimum: 159, points: 7 },
      { minimum: 154, points: 5 },
      { minimum: 151, points: 2 },
    ]);
    weakScore += recentSevenMachineWeakContentCount >= 3 ? 5 :
      recentSevenMachineWeakContentCount === 2 ? 4 :
      recentSevenMachineWeakContentCount === 1 ? 2 :
      0;
    weakScore = Math.min(weakScore, 10);

    const gameTrustScore = scoreAtLeast(recentSevenGamesTotal, [
      { minimum: 42300, points: 5 },
      { minimum: 32400, points: 4 },
      { minimum: 20200, points: 2 },
    ]);

    let penalty = 0;
    penalty += recentSevenNetTotal > 2800 ? 12 : recentSevenNetTotal > 1340 ? 6 : 0;
    penalty += recentFourteenNetTotal > 3800 ? 8 : recentFourteenNetTotal > 2026 ? 4 : 0;
    penalty += recentSevenMachineGoodContentCount >= 3 ? 7 : recentSevenMachineGoodContentCount >= 2 ? 3 : 0;
    penalty += previousMachineGoodContent && previousDifference >= 1500 ? 8 : 0;
    penalty += previousMachineHighContent && previousDifference >= 1500 ? 5 : 0;
    penalty += machineGoodContentStreak >= 2 ? 8 : 0;
    penalty += machineHighContentStreak >= 2 ? 5 : 0;
    penalty += previousMachineHighContent && previousDifference >= 500 && previousDifference < 1500 ? 3 : 0;
    penalty += Number.isFinite(daysSinceMachineHighContent) &&
      daysSinceMachineHighContent >= 29 &&
      recentSevenNetTotal > -2000
      ? 4
      : 0;

    return Math.round(clamp(
      sinkScore + streakScore + restScore + repayScore + weakScore + gameTrustScore - penalty,
      0,
      100,
    ));
  }

  if (machineKey === "gogo") {
    if (
      activeLogicKey === "beam-hikari-gogo" ||
      activeLogicKey === "beam-hikari-gogo-normal" ||
      activeLogicKey === "beam-hikari-gogo-event"
    ) {
      if (targetRangeHistoryRowCount < 7) {
        return 0;
      }

      let score = 0;
      const recentThreeAngle = features.recentThreeAngle;
      const recentFiveAngle = features.recentFiveAngle;
      const recentThreeCombined = features.recentThreeCombinedDenominator;
      const recentThreeRb = features.recentThreeRbDenominator;
      const previousWeak =
        previousGames >= 1500 &&
        (previousDifference <= -500 || previousCombinedDenominator >= 170 || previousRbDenominator >= 450);
      const twentyOneSinkBand = recentTwentyOneNetTotal >= -8000 && recentTwentyOneNetTotal <= -4000;
      const recentThreeGamesMiddle = recentThreeGamesTotal >= 2000 && recentThreeGamesTotal <= 7000;
      const nearbyHigh = adjacentMachineHighContentCount7 > 0 || adjacentMachineHighContentCount7Near2 > 0;

      if (activeLogicKey === "beam-hikari-gogo" || activeLogicKey === "beam-hikari-gogo-event") {
        score += scoreAtLeast(recentThreeCombined, [
          { minimum: 200, points: 14 },
          { minimum: 180, points: 12 },
          { minimum: 160, points: 7 },
          { minimum: 150, points: 4 },
        ]);
        score += scoreAtLeast(recentThreeRb, [
          { minimum: 500, points: 5 },
          { minimum: 380, points: 4 },
          { minimum: 320, points: 2 },
        ]);
        score += previousWeak ? 3 : 0;

        score += scoreAtMost(recentThreeAngle, [
          { maximum: -300, points: 13 },
          { maximum: -250, points: 12 },
          { maximum: -200, points: 10 },
          { maximum: -150, points: 7 },
          { maximum: -100, points: 4 },
        ]);
        score += scoreAtMost(recentFiveAngle, [
          { maximum: -250, points: 5 },
          { maximum: -150, points: 3 },
          { maximum: -100, points: 1 },
        ]);

        if (twentyOneSinkBand) {
          score += 12;
        } else if (recentTwentyOneNetTotal < -8000) {
          score += 6;
        } else {
          score += scoreAtMost(recentTwentyOneNetTotal, [
            { maximum: -3000, points: 9 },
            { maximum: -2000, points: 5 },
            { maximum: -1000, points: 2 },
          ]);
        }
        score += scoreAtMost(recentFourteenNetTotal, [
          { maximum: -5000, points: 7 },
          { maximum: -3000, points: 5 },
          { maximum: -1500, points: 3 },
        ]);
        score += scoreAtMost(recentSevenNetTotal, [
          { maximum: -2500, points: 4 },
          { maximum: -1500, points: 2 },
        ]);

        score +=
          streak === 2 ? 7 :
          streak === 3 ? 9 :
          streak === 4 ? 10 :
          streak === 5 ? 5 :
          streak >= 6 ? 2 :
          0;
        if (Number.isFinite(daysSinceMachineHighContent)) {
          score +=
            daysSinceMachineHighContent === 1 ? -8 :
            daysSinceMachineHighContent >= 4 && daysSinceMachineHighContent <= 6 ? 5 :
            daysSinceMachineHighContent >= 7 && daysSinceMachineHighContent <= 13 ? 1 :
            daysSinceMachineHighContent >= 14 && daysSinceMachineHighContent <= 20 ? 2 :
            daysSinceMachineHighContent >= 21 && daysSinceMachineHighContent <= 30 ? 5 :
            0;
        }
        score += recentSevenMachineHighContentCount === 0 ? 2 : 0;

        score += scoreInRange(recentThreeGamesTotal, 2000, 4999, 7);
        score += scoreInRange(recentThreeGamesTotal, 5000, 6999, 4);
        score += scoreInRange(recentThreeGamesTotal, 7000, 8999, 2);
        score -= recentThreeGamesTotal >= 12000 ? 3 : 0;
        score += scoreInRange(recentFiveGamesTotal, 5000, 12999, 5);
        score += scoreInRange(recentFiveGamesTotal, 13000, 15999, 2);
        score -= recentFiveGamesTotal >= 20000 ? 4 : 0;
        score += scoreInRange(recentSevenGamesTotal, 11000, 13999, 6);
        score -= recentSevenGamesTotal >= 26000 ? 5 : 0;

        score += adjacentMachineHighContentCount7Near2 >= 2 || adjacentMachineHighContentCount7 >= 2 ? 2 : 0;
        score += recentTwentyOneNetTotal <= -4000 && nearbyHigh ? 3 : 0;
        score += recentThreeAngle <= -200 && nearbyHigh ? 2 : 0;
        score -= adjacentMachineNetTotal14 >= 3000 && recentTwentyOneNetTotal > -4000 && recentThreeAngle > -200 ? 2 : 0;

        score +=
          targetRangeHistoryRowCount >= 21 ? 5 :
          targetRangeHistoryRowCount >= 14 ? 3 :
          targetRangeHistoryRowCount >= 7 ? 1 :
          0;

        score -= previousDifference >= 1500 ? 12 : 0;
        score -= previousMachineHighContent ? 8 : 0;
        score -= previousMachineHighContent && previousDifference >= 1500 ? 5 : 0;
        score -= recentSevenNetTotal >= 1500 ? 8 : 0;
        score -= recentTwentyOneNetTotal >= 6000 ? 7 : 0;
        score -= recentThreeAngle >= 100 ? 4 : 0;
        score -= streak >= 6 ? 3 : 0;
        score -= recentThreeGamesTotal < 2000 ? 4 : 0;

        return Math.round(clamp(score, 0, 100));
      }

      if (twentyOneSinkBand) {
        score += 18;
      } else if (recentTwentyOneNetTotal < -8000) {
        score += 6;
      } else {
        score += scoreAtMost(recentTwentyOneNetTotal, [
          { maximum: -3000, points: 12 },
          { maximum: -2000, points: 6 },
          { maximum: -1000, points: 2 },
        ]);
      }
      if (recentFourteenNetTotal >= -5000 && recentFourteenNetTotal <= -1500) {
        score += 5;
      } else if (recentFourteenNetTotal <= -5000) {
        score -= 3;
      }
      score += scoreAtMost(recentSevenNetTotal, [
        { maximum: -2500, points: 5 },
        { maximum: -1500, points: 2 },
      ]);
      score += twentyOneSinkBand && recentThreeGamesMiddle ? 10 : 0;
      score += twentyOneSinkBand && recentThreeCombined >= 180 ? 5 : 0;

      score += scoreAtLeast(recentThreeCombined, [
        { minimum: 200, points: 12 },
        { minimum: 180, points: 10 },
        { minimum: 160, points: 6 },
        { minimum: 150, points: 3 },
      ]);
      score += scoreAtLeast(recentThreeRb, [
        { minimum: 500, points: 3 },
        { minimum: 380, points: 2 },
      ]);
      score += previousWeak ? 3 : 0;

      score += scoreAtMost(recentThreeAngle, [
        { maximum: -300, points: 13 },
        { maximum: -250, points: 11 },
        { maximum: -200, points: 8 },
        { maximum: -150, points: 5 },
        { maximum: -100, points: 2 },
      ]);
      score += scoreAtMost(recentFiveAngle, [
        { maximum: -250, points: 3 },
        { maximum: -150, points: 2 },
      ]);

      score +=
        streak === 2 ? 6 :
        streak === 3 ? 8 :
        streak === 4 ? 8 :
        streak === 5 ? 1 :
        streak >= 6 ? -3 :
        0;
      if (Number.isFinite(daysSinceMachineHighContent)) {
        score +=
          daysSinceMachineHighContent === 1 ? -12 :
          daysSinceMachineHighContent === 2 ? -6 :
          daysSinceMachineHighContent >= 4 && daysSinceMachineHighContent <= 6 ? 3 :
          daysSinceMachineHighContent >= 7 && daysSinceMachineHighContent <= 13 ? -1 :
          daysSinceMachineHighContent >= 14 && daysSinceMachineHighContent <= 20 ? -2 :
          daysSinceMachineHighContent >= 21 ? -2 :
          0;
      }
      score += recentSevenMachineHighContentCount === 0 ? 1 : 0;

      score += scoreInRange(recentThreeGamesTotal, 2000, 4999, 8);
      score += scoreInRange(recentThreeGamesTotal, 5000, 6999, 6);
      score += scoreInRange(recentThreeGamesTotal, 7000, 8999, 1);
      score -= recentThreeGamesTotal >= 12000 ? 5 : 0;
      score += scoreInRange(recentFiveGamesTotal, 5000, 12999, 4);
      score -= recentFiveGamesTotal >= 20000 ? 6 : 0;
      score -= recentSevenGamesTotal >= 26000 ? 5 : 0;

      score += (recentTwentyOneNetTotal < 0 || recentFourteenNetTotal < 0) && nearbyHigh ? 3 : 0;
      score -= adjacentMachineNetTotal14 >= 3000 && recentTwentyOneNetTotal > -4000 ? 2 : 0;
      score +=
        targetRangeHistoryRowCount >= 21 ? 5 :
        targetRangeHistoryRowCount >= 14 ? 3 :
        targetRangeHistoryRowCount >= 7 ? 1 :
        0;

      score -= previousDifference >= 1500 ? 12 : 0;
      score -= previousMachineHighContent ? 12 : 0;
      score -= previousMachineHighContent && previousDifference >= 1500 ? 4 : 0;
      score -= recentSevenNetTotal >= 1500 ? 8 : 0;
      score -= recentTwentyOneNetTotal >= 6000 ? 8 : 0;
      score -= recentThreeAngle >= 100 ? 4 : 0;
      score -= recentThreeGamesTotal < 2000 ? 6 : 0;

      return Math.round(clamp(score, 0, 100));
    }

    if (activeLogicKey === "mj-kurume-gogo") {
      let repayScore = 0;
      if (recentFourteenNetTotal >= -3000 && recentFourteenNetTotal <= 0) {
        repayScore += 14;
      } else if (recentFourteenNetTotal >= -6000 && recentFourteenNetTotal <= -3001) {
        repayScore += 8;
      } else if (recentFourteenNetTotal < -6000) {
        repayScore += 3;
      } else if (recentFourteenNetTotal >= 1 && recentFourteenNetTotal <= 1500) {
        repayScore += 1;
      } else if (recentFourteenNetTotal >= 1501 && recentFourteenNetTotal <= 2999) {
        repayScore -= 6;
      } else if (recentFourteenNetTotal >= 3000) {
        repayScore -= 18;
      }
      if (recentTwentyOneNetTotal >= -6000 && recentTwentyOneNetTotal <= -1000) {
        repayScore += 10;
      } else if (recentTwentyOneNetTotal < -6000) {
        repayScore += 5;
      } else if (recentTwentyOneNetTotal >= -999 && recentTwentyOneNetTotal <= -1) {
        repayScore += 6;
      } else if (recentTwentyOneNetTotal >= 1801 && recentTwentyOneNetTotal <= 3499) {
        repayScore -= 6;
      } else if (recentTwentyOneNetTotal >= 3500) {
        repayScore -= 14;
      }
      repayScore += recentSevenNetTotal <= -1500 ? 3 : 0;
      repayScore -= recentSevenNetTotal >= 2200 ? 5 : 0;
      repayScore = clamp(repayScore, -25, 25);

      let bonusScore = 0;
      bonusScore +=
        previousGames >= 2000 && previousRbDenominator <= 255
          ? 18
          : previousGames >= 2000 && previousRbDenominator <= 300
            ? 6
            : 0;
      bonusScore +=
        previousGames >= 2000 && previousCombinedDenominator <= 130
          ? 5
          : previousGames >= 2000 && previousCombinedDenominator <= 145
            ? 2
            : 0;
      if (previousMachineHighContent && previousDifference < 0) {
        bonusScore += 12;
      } else if (previousMachineHighContent && previousDifference <= 500) {
        bonusScore += 9;
      } else if (previousMachineHighContent && previousDifference <= 1000) {
        bonusScore += 2;
      } else if (previousMachineHighContent && previousDifference >= 1500) {
        bonusScore -= 12;
      }
      bonusScore -= previousGames >= 3000 && previousDifference >= 1000 && previousRbDenominator > 330 ? 9 : 0;
      bonusScore = clamp(bonusScore, -15, 25);

      const compositeCore =
        recentFourteenNetTotal >= -3000 &&
        recentFourteenNetTotal <= 0 &&
        previousGames >= 2000 &&
        previousRbDenominator <= 255 &&
        recentTwentyOneNetTotal < 3500;
      let compositeScore = 0;
      compositeScore += compositeCore ? 15 : 0;
      compositeScore += compositeCore && recentTwentyOneNetTotal < 0 ? 3 : 0;
      compositeScore +=
        recentTwentyOneNetTotal >= -6000 &&
        recentTwentyOneNetTotal <= -1000 &&
        recentFourteenNetTotal >= -3000 &&
        recentFourteenNetTotal <= 0 &&
        recentFourteenGamesTotal >= 33000
          ? 4
          : 0;
      compositeScore +=
        recentFourteenNetTotal < 0 && previousMachineHighContent && previousDifference <= 500 ? 8 : 0;
      compositeScore -= recentFourteenNetTotal >= 3000 ? 6 : 0;
      compositeScore = clamp(compositeScore, -10, 18);

      let gamesScore = 0;
      gamesScore += scoreInRange(recentFourteenGamesTotal, 33000, 62000, 6);
      gamesScore +=
        scoreInRange(recentFourteenGamesTotal, 24000, 32999, 3) ||
        scoreInRange(recentFourteenGamesTotal, 62001, 70000, 3);
      gamesScore -= recentFourteenGamesTotal < 18000 ? 7 : 0;
      gamesScore += recentSevenGamesTotal >= 18000 ? 3 : 0;
      gamesScore -= recentSevenGamesTotal < 12000 ? 5 : 0;
      gamesScore += scoreInRange(previousGames, 2000, 4500, 2);
      gamesScore -= previousGames < 800 ? 4 : 0;
      gamesScore = clamp(gamesScore, -12, 10);

      let sinkStayScore = 0;
      sinkStayScore += streak >= 7 ? 6 :
        streak >= 5 ? 8 :
        streak >= 4 ? 7 :
        streak >= 3 ? 5 :
        streak >= 2 ? 2 :
        0;
      sinkStayScore += recentSevenLossDays >= 6 ? 5 : recentSevenLossDays === 5 ? 3 : recentSevenLossDays <= 3 ? -4 : 0;
      sinkStayScore += recentFourteenNegativeStayDays >= 15 ? 5 :
        recentFourteenNegativeStayDays >= 7 ? 4 :
        recentFourteenNegativeStayDays >= 3 ? 2 :
        0;
      sinkStayScore = clamp(sinkStayScore, -8, 12);

      let angleScore = 0;
      if (features.recentFourteenAngle >= -60 && features.recentFourteenAngle <= 0) {
        angleScore += 7;
      } else if (features.recentFourteenAngle >= -120 && features.recentFourteenAngle <= -61) {
        angleScore += 3;
      } else if (features.recentFourteenAngle >= 1 && features.recentFourteenAngle <= 35) {
        angleScore += 1;
      } else if (features.recentFourteenAngle > 35 && features.recentFourteenAngle < 70) {
        angleScore -= 5;
      } else if (features.recentFourteenAngle >= 70) {
        angleScore -= 12;
      }
      angleScore -= features.recentSevenAngle >= 90 ? 4 : 0;
      angleScore = clamp(angleScore, -14, 8);

      let nearbyScore = 0;
      nearbyScore +=
        !Number.isFinite(daysSinceMachineHighContent) &&
        historyRowCount >= 14 &&
        recentTwentyOneNetTotal < 0
          ? 2
          : 0;
      nearbyScore += scoreInRange(daysSinceMachineHighContent, 2, 7, 3);
      nearbyScore += scoreInRange(daysSinceMachineHighContent, 8, 14, 1);
      nearbyScore -= previousMachineHighContent ? 2 : 0;
      nearbyScore += adjacentMachineNetTotal14 > 0 && recentFourteenNetTotal < 0 ? 3 : 0;
      nearbyScore += adjacentMachineNetTotal14 > 0 && recentFourteenNetTotal < 0 && streak >= 2 ? 2 : 0;
      nearbyScore -= recentSevenMachineHighContentCount >= 3 ? 7 : recentSevenMachineHighContentCount >= 2 ? 3 : 0;
      nearbyScore -= previousAdjacentMachineBigWin1000Count > 0 ? 2 : 0;
      nearbyScore = clamp(nearbyScore, -10, 7);

      let dangerPenalty = 0;
      dangerPenalty += targetRangeHistoryRowCount < 7 ? 25 : 0;
      dangerPenalty += previousGames < 500 ? 5 : 0;
      dangerPenalty += recentFourteenNetTotal >= 3500 && features.recentFourteenAngle >= 70 ? 10 : 0;
      dangerPenalty += recentTwentyOneNetTotal >= 6000 ? 7 : 0;
      dangerPenalty +=
        recentSevenGamesTotal < 10000 &&
        !(previousGames >= 2000 && previousRbDenominator <= 300) &&
        !previousMachineHighContent
          ? 5
          : 0;

      return Math.round(clamp(
        25 + repayScore + bonusScore + compositeScore + gamesScore + sinkStayScore + angleScore + nearbyScore - dangerPenalty,
        0,
        100,
      ));
    }

    let sinkScore = 0;
    sinkScore += scoreAtLeast(streak, [
      { minimum: 4, points: 24 },
      { minimum: 3, points: 20 },
      { minimum: 2, points: 6 },
    ]);
    sinkScore += recentThreeNetTotal <= -1500 ? 8 : 0;
    sinkScore += recentFourteenNetTotal <= -3000 ? 8 : 0;
    sinkScore += recentSevenNetTotal <= -3900 ? 6 : 0;
    sinkScore = Math.min(sinkScore, 38);

    let angleScore = 0;
    angleScore += scoreAtMost(features.recentThreeAngle, [
      { maximum: -250, points: 16 },
      { maximum: -150, points: 14 },
      { maximum: -100, points: 8 },
    ]);
    angleScore += features.recentSevenAngle <= -66 ? 4 : 0;
    angleScore = Math.min(angleScore, 18);

    let restScore = 0;
    restScore += scoreInRange(daysSinceMachineHighContent, 4, 14, 16);
    restScore += scoreInRange(daysSinceMachineHighContent, 15, 30, 8);
    restScore += daysSinceMachineHighContent >= 31 ? 4 : 0;
    restScore -= scoreInRange(daysSinceMachineHighContent, 1, 3, 8);
    restScore -= recentSevenMachineHighContentCount >= 2 ? 5 : 0;

    let weakScore = 0;
    weakScore += scoreAtLeast(features.recentThreeCombinedDenominator, [
      { minimum: 170, points: 10 },
      { minimum: 155, points: 6 },
      { minimum: 145, points: 2 },
    ]);
    weakScore += previousDifference <= -1000 ? 5 : 0;
    weakScore = Math.min(weakScore, 13);

    let activityScore = 0;
    activityScore += previousGames >= 4000 ? 1 : 0;
    activityScore += recentThreeGamesTotal >= 9000 ? 1 : 0;
    activityScore += recentFourteenGamesTotal >= 45000 ? 3 : 0;
    activityScore -= previousGames < 1200 || recentThreeGamesTotal < 5000 ? 4 : 0;

    let penalty = 0;
    penalty += recentThreeNetTotal >= 2000 ? 12 : recentThreeNetTotal >= 1000 ? 8 : 0;
    penalty += recentFourteenNetTotal >= 0 ? 8 : 0;
    penalty += previousDifference > 1000 && !previousMachineHighContent ? 10 : 0;
    penalty += previousDifference > 1500 && features.previousRbDenominator > 420 ? 6 : 0;

    return Math.round(clamp(sinkScore + angleScore + restScore + weakScore + activityScore - penalty, 0, 100));
  }

  if (machineKey === "my") {
    if (
      activeLogicKey === "beam-hikari-my" ||
      activeLogicKey === "beam-hikari-my-normal" ||
      activeLogicKey === "beam-hikari-my-event"
    ) {
      if (historyRowCount < 21) {
        return 0;
      }

      let score = 0;

      if (activeLogicKey === "beam-hikari-my") {
        score +=
          streak === 1 ? 2 :
          streak === 2 ? 35 :
          streak >= 3 && streak <= 4 ? 20 :
          streak >= 5 && streak <= 7 ? 18 :
          streak >= 8 ? 14 :
          0;

        if (Number.isFinite(daysSinceMachineHighContent)) {
          score +=
            daysSinceMachineHighContent === 3 ? 14 :
            daysSinceMachineHighContent >= 4 && daysSinceMachineHighContent <= 8 ? 8 :
            daysSinceMachineHighContent >= 9 && daysSinceMachineHighContent <= 13 ? 10 :
            daysSinceMachineHighContent >= 14 && daysSinceMachineHighContent <= 21 ? 5 :
            daysSinceMachineHighContent >= 22 && daysSinceMachineHighContent <= 35 ? 2 :
            0;
        }

        score += scoreAtMost(recentThreeNetTotal, [
          { maximum: -2500, points: 5 },
          { maximum: -1500, points: 4 },
          { maximum: -500, points: 1 },
        ]);
        score += scoreAtMost(recentSevenNetTotal, [
          { maximum: -4500, points: 4 },
          { maximum: -3000, points: 3 },
          { maximum: -2000, points: 1 },
        ]);
        score += scoreAtMost(recentFourteenNetTotal, [
          { maximum: -6000, points: 3 },
          { maximum: -4000, points: 2 },
          { maximum: -1500, points: 1 },
        ]);

        score += scoreAtMost(features.recentThreeAngle, [
          { maximum: -180, points: 4 },
          { maximum: -120, points: 3 },
          { maximum: -80, points: 1 },
        ]);
        score += scoreAtMost(features.recentSevenAngle, [
          { maximum: -120, points: 4 },
          { maximum: -80, points: 2 },
          { maximum: -50, points: 1 },
        ]);

        score += scoreAtLeast(features.recentThreeCombinedDenominator, [
          { minimum: 180, points: 4 },
          { minimum: 170, points: 3 },
          { minimum: 160, points: 1 },
        ]);
        score += scoreAtLeast(features.recentThreeRbDenominator, [
          { minimum: 500, points: 4 },
          { minimum: 420, points: 2 },
        ]);
        score += scoreAtLeast(features.recentSevenCombinedDenominator, [
          { minimum: 165, points: 3 },
          { minimum: 160, points: 1 },
        ]);
        score += scoreAtLeast(features.recentSevenRbDenominator, [
          { minimum: 450, points: 2 },
          { minimum: 400, points: 1 },
        ]);

        score +=
          previousGames > 6000 ? -5 :
          previousGames >= 5000 ? -2 :
          previousGames >= 4000 ? 1 :
          previousGames >= 2000 ? 5 :
          previousGames >= 1000 ? 3 :
          previousGames >= 0 ? 1 :
          0;
        score +=
          recentThreeGamesTotal > 16000 ? -4 :
          recentThreeGamesTotal >= 14000 ? -2 :
          recentThreeGamesTotal >= 8000 && recentThreeGamesTotal <= 12000 ? 1 :
          recentThreeGamesTotal <= 8000 ? 3 :
          0;

        score += recentSevenMachineHighContentCount === 0 ? 3 : recentSevenMachineHighContentCount === 1 ? 1 : -5;
        score += recentFourteenMachineHighContentCount <= 1 ? 2 : recentFourteenMachineHighContentCount >= 3 ? -3 : 0;
        score += recentFourteenNetTotal <= -4000 && streak === 2 ? 10 : 0;
        score += recentFourteenNetTotal <= -4000 && streak >= 3 ? 5 : 0;
        score += recentTwentyOneNetTotal <= -5000 && streak >= 2 ? 3 : 0;
        score += adjacentMachineHighContentCount7 >= 1 && streak >= 2 ? 5 : 0;
        score += adjacentMachineHighContentCount7 >= 2 && streak >= 2 ? 2 : 0;
        score += adjacentMachineNetTotal3 >= 2000 && recentSevenNetTotal <= -2000 ? 2 : 0;

        score -=
          Number.isFinite(daysSinceMachineHighContent) &&
          daysSinceMachineHighContent >= 1 &&
          daysSinceMachineHighContent <= 2
            ? 14
            : 0;
        score -= previousMachineHighContent ? 8 : 0;
        score -=
          previousDifference >= 3000 ? 14 :
          previousDifference >= 2000 ? 12 :
          previousDifference >= 1000 ? 7 :
          previousDifference >= 500 ? 3 :
          0;
        score -=
          features.recentThreeCombinedDenominator <= 145 &&
          features.recentThreeRbDenominator <= 330
            ? 5
            : 0;
        score -= recentSevenNetTotal >= 3000 ? 5 : 0;
        score -= recentFourteenNetTotal >= 5000 ? 4 : 0;
        score -= features.recentThreeAngle >= 80 ? 4 : 0;

        return Math.round(clamp(score, 0, 100));
      }

      if (activeLogicKey === "beam-hikari-my-normal") {
        score +=
          streak === 0 ? -6 :
          streak === 1 ? 4 :
          streak === 2 ? 34 :
          streak >= 3 && streak <= 4 ? 16 :
          streak >= 5 && streak <= 7 ? 12 :
          streak >= 8 ? 10 :
          0;

        if (Number.isFinite(daysSinceMachineHighContent)) {
          score +=
            daysSinceMachineHighContent === 1 ? -18 :
            daysSinceMachineHighContent === 2 ? -12 :
            daysSinceMachineHighContent === 3 ? 14 :
            daysSinceMachineHighContent >= 4 && daysSinceMachineHighContent <= 5 ? 8 :
            daysSinceMachineHighContent >= 6 && daysSinceMachineHighContent <= 8 ? 8 :
            daysSinceMachineHighContent >= 9 && daysSinceMachineHighContent <= 13 ? 8 :
            daysSinceMachineHighContent >= 14 && daysSinceMachineHighContent <= 21 ? 4 :
            daysSinceMachineHighContent >= 36 ? 1 :
            0;
        }

        score += scoreAtMost(features.recentFourteenAngle, [
          { maximum: -100, points: 14 },
          { maximum: -80, points: 10 },
          { maximum: -60, points: 6 },
        ]);
        score += scoreAtMost(features.recentSevenAngle, [
          { maximum: -120, points: 8 },
          { maximum: -100, points: 6 },
          { maximum: -60, points: 3 },
        ]);
        score += scoreInRange(recentFourteenNetTotal, -8000, -6000, 8);
        score += scoreInRange(recentFourteenNetTotal, -5999, -4500, 6);
        score += scoreInRange(recentFourteenNetTotal, -4499, -3000, 2);
        score += scoreAtMost(recentTwentyOneNetTotal, [
          { maximum: -6000, points: 6 },
          { maximum: -3000, points: 4 },
          { maximum: -1500, points: 2 },
        ]);
        score += scoreAtMost(recentSevenNetTotal, [
          { maximum: -4500, points: 5 },
          { maximum: -3000, points: 4 },
          { maximum: -2000, points: 2 },
        ]);

        score += scoreAtLeast(features.recentThreeCombinedDenominator, [
          { minimum: 200, points: 8 },
          { minimum: 180, points: 6 },
          { minimum: 170, points: 4 },
          { minimum: 160, points: 2 },
        ]);
        score += scoreAtLeast(features.recentThreeRbDenominator, [
          { minimum: 500, points: 6 },
          { minimum: 450, points: 4 },
          { minimum: 420, points: 3 },
          { minimum: 400, points: 2 },
        ]);
        score += scoreAtLeast(features.recentSevenCombinedDenominator, [
          { minimum: 180, points: 6 },
          { minimum: 170, points: 5 },
          { minimum: 160, points: 2 },
        ]);
        score += scoreAtLeast(features.recentSevenRbDenominator, [
          { minimum: 450, points: 3 },
          { minimum: 400, points: 2 },
        ]);

        score +=
          previousGames <= 1000 ? 4 :
          previousGames <= 2000 ? 7 :
          previousGames <= 4000 ? 8 :
          previousGames <= 5000 ? 2 :
          previousGames <= 6000 ? -6 :
          -12;
        score +=
          recentThreeGamesTotal <= 8000 ? 4 :
          recentThreeGamesTotal <= 12000 ? 2 :
          recentThreeGamesTotal > 16000 ? -6 :
          0;

        score += recentSevenMachineHighContentCount === 0 ? 5 : recentSevenMachineHighContentCount === 1 ? 2 : -8;
        score += recentFourteenMachineHighContentCount <= 1 ? 2 : recentFourteenMachineHighContentCount >= 3 ? -4 : 0;
        if (streak === 2) {
          score += adjacentMachineHighContentCount7Near2 > 0 ? 3 : 0;
          score += adjacentMachineNetTotal3Near2 >= 3000 ? 3 : 0;
          score += features.recentFourteenAngle <= -100 ? 10 : 0;
          score += recentFourteenNetTotal <= -6000 ? 8 : 0;
          score += previousGames <= 4000 ? 5 : 0;
          score += features.recentFourteenCombinedDenominator >= 160 ? 4 : 0;
        }

        score -= previousMachineHighContent ? 12 : 0;
        score -=
          previousDifference >= 3000 ? 16 :
          previousDifference >= 2000 ? 14 :
          previousDifference >= 1000 ? 10 :
          previousDifference >= 500 ? 4 :
          0;
        score -= recentSevenNetTotal >= 5000 ? 8 : recentSevenNetTotal >= 3000 ? 4 : 0;
        score -= recentFourteenNetTotal >= 5000 ? 8 : recentFourteenNetTotal >= 3000 ? 4 : 0;
        score -=
          features.recentThreeCombinedDenominator <= 140 || features.recentThreeRbDenominator <= 300
            ? 20
            : features.recentThreeCombinedDenominator <= 150 || features.recentThreeRbDenominator <= 330
              ? 14
              : 0;
        score -= recentThreeMachineHighContentCount > 0 ? 6 : 0;
        score -= recentSevenMachineHighContentCount >= 2 ? 5 : 0;

        return Math.round(clamp(score, 0, 100));
      }

      score +=
        streak === 0 ? -6 :
        streak === 1 ? 8 :
        streak === 2 ? 26 :
        streak >= 3 && streak <= 4 ? 22 :
        streak >= 5 && streak <= 7 ? 24 :
        streak >= 8 ? 22 :
        0;

      if (Number.isFinite(daysSinceMachineHighContent)) {
        score +=
          daysSinceMachineHighContent === 1 ? -18 :
          daysSinceMachineHighContent === 2 ? -5 :
          daysSinceMachineHighContent === 3 ? 14 :
          daysSinceMachineHighContent >= 4 && daysSinceMachineHighContent <= 5 ? 12 :
          daysSinceMachineHighContent >= 6 && daysSinceMachineHighContent <= 8 ? 8 :
          daysSinceMachineHighContent >= 9 && daysSinceMachineHighContent <= 13 ? 10 :
          daysSinceMachineHighContent >= 14 && daysSinceMachineHighContent <= 21 ? 6 :
          daysSinceMachineHighContent >= 22 && daysSinceMachineHighContent <= 35 ? 6 :
          daysSinceMachineHighContent >= 36 ? -8 :
          0;
      }

      score += scoreAtMost(recentThreeNetTotal, [
        { maximum: -4500, points: 12 },
        { maximum: -3000, points: 10 },
        { maximum: -2000, points: 5 },
        { maximum: -1000, points: 3 },
      ]);
      score += scoreAtMost(features.recentThreeAngle, [
        { maximum: -200, points: 8 },
        { maximum: -150, points: 6 },
        { maximum: -100, points: 3 },
      ]);
      score += scoreAtMost(recentFourteenNetTotal, [
        { maximum: -8000, points: 12 },
        { maximum: -6000, points: 10 },
        { maximum: -4500, points: 6 },
        { maximum: -3000, points: 3 },
      ]);
      score += scoreAtMost(recentTwentyOneNetTotal, [
        { maximum: -8000, points: 9 },
        { maximum: -6000, points: 10 },
        { maximum: -3000, points: 2 },
      ]);

      score += scoreAtLeast(features.recentThreeCombinedDenominator, [
        { minimum: 200, points: 7 },
        { minimum: 180, points: 5 },
        { minimum: 170, points: 4 },
        { minimum: 160, points: 2 },
      ]);
      score += scoreAtLeast(features.recentThreeRbDenominator, [
        { minimum: 500, points: 5 },
        { minimum: 420, points: 3 },
        { minimum: 360, points: 2 },
      ]);
      if (streak >= 2) {
        score += features.recentFourteenRbDenominator <= 330 ? 8 : 0;
        score += features.recentFourteenCombinedDenominator <= 145 ? 6 : 0;
        score += features.recentSevenCombinedDenominator <= 155 ? 4 : 0;
      }

      score += recentSevenMachineHighContentCount <= 2 ? 3 : recentSevenMachineHighContentCount >= 3 ? -3 : 0;
      score += recentFourteenMachineHighContentCount === 1 ? 3 : 0;
      score += recentFourteenMachineHighContentCount >= 3 ? 4 : 0;
      score += recentFourteenMachineHighContentCount >= 4 ? 7 : 0;
      if (streak >= 2) {
        score += recentFourteenMachineHighContentCount >= 3 ? 6 : 0;
        score += recentTwentyOneMachineHighContentCount >= 3 ? 5 : 0;
      }

      score +=
        previousGames <= 1000 ? 6 :
        previousGames <= 3000 ? 5 :
        previousGames <= 5000 ? 5 :
        previousGames <= 6000 ? -2 :
        -10;
      score += recentThreeGamesTotal >= 8000 && recentThreeGamesTotal <= 12000 ? 3 : 0;
      score += recentThreeGamesTotal > 12000 && recentThreeGamesTotal <= 16000 ? 1 : 0;
      score -= recentThreeGamesTotal > 16000 ? 3 : 0;

      if (streak >= 2) {
        score += adjacentMachineNetTotal3Near2 >= 3000 ? 6 : 0;
        score += adjacentMachineHighContentCount7Near2 > 0 ? 3 : 0;
      }

      score -= previousMachineHighContent ? 18 : 0;
      score -=
        previousDifference >= 3000 ? 16 :
        previousDifference >= 2000 ? 14 :
        previousDifference >= 1000 ? 10 :
        previousDifference >= 500 ? 5 :
        0;
      score -= previousGames > 6000 ? 5 : 0;
      score -= Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 36 ? 5 : 0;

      return Math.round(clamp(score, 0, 100));
    }

    if (activeLogicKey === "apark-yakatabaru-my") {
      if (historyRowCount < 21) {
        return 0;
      }

      let score = 0;
      score += scoreAtLeast(streak, [
        { minimum: 3, points: 25 },
        { minimum: 2, points: 16 },
        { minimum: 1, points: 5 },
      ]);
      score += scoreAtMost(recentThreeNetTotal, [
        { maximum: -3000, points: 22 },
        { maximum: -2000, points: 18 },
        { maximum: -1500, points: 13 },
        { maximum: -1000, points: 8 },
        { maximum: -500, points: 4 },
      ]);
      score += scoreAtLeast(features.recentThreeCombinedDenominator, [
        { minimum: 180, points: 18 },
        { minimum: 170, points: 15 },
        { minimum: 160, points: 10 },
        { minimum: 155, points: 6 },
      ]);
      score += scoreAtLeast(features.recentThreeRbDenominator, [
        { minimum: 500, points: 5 },
        { minimum: 420, points: 3 },
        { minimum: 380, points: 1 },
      ]);
      score += scoreAtMost(recentSevenNetTotal, [
        { maximum: -5000, points: 12 },
        { maximum: -3000, points: 9 },
        { maximum: -1500, points: 5 },
      ]);
      score += scoreAtMost(recentFourteenNetTotal, [
        { maximum: -5000, points: 5 },
        { maximum: -3000, points: 3 },
      ]);
      score +=
        recentSevenMachineHighContentCount === 0
          ? 8
          : recentSevenMachineHighContentCount === 1
            ? 3
            : 0;
      score +=
        recentFourteenMachineHighContentCount === 0
          ? 4
          : recentFourteenMachineHighContentCount === 1
            ? 2
            : 0;
      score += scoreAtLeast(daysSinceMachineHighContent, [
        { minimum: 10, points: 6 },
        { minimum: 6, points: 4 },
        { minimum: 4, points: 2 },
      ]);
      score +=
        recentThreeGamesTotal >= 9000 && recentThreeGamesTotal <= 16000
          ? 5
          : recentThreeGamesTotal >= 6000 && recentThreeGamesTotal < 9000
            ? 2
            : recentThreeGamesTotal < 6000
              ? -10
              : 0;
      score += recentSevenGamesTotal >= 25000 ? 2 : 0;
      score += adjacentMachineHighContentCount3Near2 === 1 ? 2 : 0;
      score += adjacentMachineHighContentCount3 === 2 ? 1 : 0;

      let penalty = 0;
      penalty += previousDifference >= 2000 ? 15 : previousDifference >= 1500 ? 8 : 0;
      penalty += previousMachineHighContent && previousDifference >= 1000 ? 10 : 0;
      penalty += recentThreeNetTotal >= 3000 ? 12 : recentThreeNetTotal >= 2000 ? 7 : 0;
      penalty += recentFourteenNetTotal >= 7000 ? 6 : 0;
      penalty += features.recentThreeCombinedDenominator <= 140 ? 7 : 0;

      return Math.round(clamp(score - penalty, 0, 100));
    }

    if (activeLogicKey === "mj-kurume-my") {
      let score = 20;

      let sinkScore = 0;
      if (recentFourteenGamesTotal >= 25000) {
        const fourteenNetSinkScore = scoreAtMost(recentFourteenNetTotal, [
          { maximum: -5000, points: 14 },
          { maximum: -3000, points: 11 },
          { maximum: -1500, points: 7 },
          { maximum: -500, points: 3 },
        ]);
        const fourteenAngleSinkScore = scoreAtMost(features.recentFourteenAngle, [
          { maximum: -100, points: 14 },
          { maximum: -80, points: 10 },
          { maximum: -50, points: 6 },
        ]);
        sinkScore +=
          Math.max(fourteenNetSinkScore, fourteenAngleSinkScore) +
          Math.min(fourteenNetSinkScore, fourteenAngleSinkScore) * 0.5;
      }
      if (recentSevenGamesTotal >= 12000) {
        sinkScore += scoreAtMost(recentSevenNetTotal, [
          { maximum: -3000, points: 6 },
          { maximum: -2000, points: 4 },
          { maximum: -1000, points: 2 },
        ]);
      }
      score += Math.min(sinkScore, 32);

      let stayScore = 0;
      stayScore +=
        recentFourteenGamesTotal >= 25000 && recentFourteenMinus3000StayDays >= 4
          ? 12
          : recentFourteenGamesTotal >= 25000 && recentFourteenMinus3000StayDays >= 3
            ? 9
            : 0;
      stayScore +=
        recentFourteenGamesTotal >= 25000 && recentFourteenMinus2000StayDays >= 4 ? 6 : 0;
      stayScore +=
        recentFiveGamesTotal >= 8000 && recentFiveMinus2000StayDays >= 3
          ? 8
          : recentFiveGamesTotal >= 8000 && recentFiveMinus2000StayDays >= 2
            ? 6
            : 0;
      stayScore += recentThreeGamesTotal >= 3000 && recentThreeMinus1000StayDays >= 4 ? 5 : 0;
      score += Math.min(stayScore, 18);

      let gamesScore = 0;
      gamesScore += scoreAtLeast(recentFourteenGamesTotal, [
        { minimum: 60000, points: 8 },
        { minimum: 45000, points: 5 },
        { minimum: 35000, points: 2 },
      ]);
      gamesScore += scoreAtLeast(recentSevenGamesTotal, [
        { minimum: 35000, points: 5 },
        { minimum: 28000, points: 3 },
      ]);
      gamesScore += scoreAtLeast(previousGames, [
        { minimum: 7000, points: 4 },
        { minimum: 5000, points: 3 },
        { minimum: 2000, points: 1 },
      ]);
      score += Math.min(gamesScore, 15);

      let restScore = 0;
      restScore += scoreInRange(daysSinceMachineHighContent, 15, 21, 8);
      restScore += scoreInRange(daysSinceMachineHighContent, 11, 14, 5);
      restScore += scoreInRange(daysSinceMachineHighContent, 7, 10, 3);
      restScore += !Number.isFinite(daysSinceMachineHighContent) ? 3 : 0;
      restScore += recentFourteenMachineHighContentCount === 0 ? 4 : 0;
      restScore += recentSevenMachineHighContentCount === 0 ? 3 : recentSevenMachineHighContentCount === 1 ? 1 : 0;
      score += Math.min(restScore, 12);

      let failScore = 0;
      if (previousMachineHighContent) {
        failScore += 2;
        failScore += previousDifference <= 1500 ? 5 : 0;
        failScore += previousDifference <= 1000 ? 8 : 0;
      }
      failScore += scoreAtMost(previousDifference, [
        { maximum: -1500, points: 7 },
        { maximum: -1000, points: 4 },
      ]);
      failScore +=
        previousGames >= 3000 &&
        previousRbDenominator <= 285 &&
        previousCombinedDenominator <= 140 &&
        previousDifference < 1000
          ? 3
          : 0;
      score += Math.min(failScore, 13);

      let nearbyScore = 0;
      nearbyScore += recentFourteenNetTotal <= -2000 && adjacentMachineNetTotal7 >= 2000 ? 7 : 0;
      nearbyScore +=
        recentFourteenNetTotal <= -2000 && adjacentMachineHighContentCount7Near2 >= 2 ? 4 : 0;
      nearbyScore += recentSevenNetTotal <= -1500 && adjacentMachineNetTotal7 >= 3000 ? 3 : 0;
      score += Math.min(nearbyScore, 10);

      let bonusScore = 0;
      bonusScore +=
        recentFourteenNetTotal <= 0 && features.recentFourteenCombinedDenominator >= 170
          ? 5
          : recentFourteenNetTotal <= 0 && features.recentFourteenCombinedDenominator >= 160
            ? 3
            : 0;
      bonusScore +=
        recentFourteenNetTotal <= -1000 && features.recentFourteenRbDenominator <= 320 ? 3 : 0;
      score += Math.min(bonusScore, 8);

      let penalty = 0;
      penalty += recentSevenNetTotal >= 3000 && recentSevenMachineHighContentCount >= 1 ? 15 : 0;
      penalty += recentFourteenNetTotal >= 5000 && recentFourteenGamesTotal >= 25000 ? 12 : 0;
      penalty += recentThreeNetTotal >= 2000 && recentThreeMachineHighContentCount >= 1 ? 10 : 0;
      penalty += recentThreeMachineHighContentCount >= 2 ? 8 : 0;
      penalty += recentFiveMachineHighContentCount >= 2 ? 5 : 0;
      penalty += recentSevenGamesTotal < 12000 ? 10 : 0;
      penalty += recentFourteenGamesTotal < 25000 ? 6 : 0;
      penalty += previousGames < 1000 ? 4 : 0;
      penalty += features.kurumeMyLowUsage ? 6 : 0;
      penalty += streak >= 5 && features.recentFourteenAngle > -80 ? 6 : 0;
      penalty +=
        features.recentFourteenCombinedDenominator >= 180 && recentFourteenGamesTotal < 45000 ? 6 : 0;
      penalty += readNumber(features.boostCount) < 2 ? 6 : 0;
      penalty += features.treatmentDone && !features.kurumeMyUnpaid ? 6 : 0;

      return Math.round(clamp(score - penalty, 0, 100));
    }

    let score = 0;
    score += scoreAtLeast(streak, [
      { minimum: 5, points: 30 },
      { minimum: 4, points: 27 },
      { minimum: 3, points: 25 },
      { minimum: 2, points: 12 },
      { minimum: 1, points: 3 },
    ]);
    score += Math.max(
      scoreAtMost(recentThreeNetTotal, [
        { maximum: -3000, points: 18 },
        { maximum: -2400, points: 14 },
        { maximum: -1600, points: 8 },
      ]),
      scoreAtMost(recentFiveNetTotal, [
        { maximum: -3500, points: 20 },
        { maximum: -2700, points: 16 },
        { maximum: -1800, points: 10 },
        { maximum: 0, points: 4 },
      ]),
      scoreAtMost(recentSevenNetTotal, [
        { maximum: -3700, points: 18 },
        { maximum: -2800, points: 14 },
        { maximum: -1800, points: 8 },
      ]),
    );
    score += Math.max(
      scoreAtMost(features.recentThreeAngle, [
        { maximum: -120, points: 12 },
        { maximum: -80, points: 9 },
        { maximum: -50, points: 5 },
      ]),
      scoreAtMost(features.recentFiveAngle, [
        { maximum: -120, points: 12 },
        { maximum: -80, points: 9 },
        { maximum: -50, points: 5 },
      ]),
      scoreAtMost(features.recentSevenAngle, [
        { maximum: -120, points: 12 },
        { maximum: -80, points: 9 },
        { maximum: -50, points: 5 },
      ]),
    );
    score += Math.max(
      recentThreeGamesTotal >= 9000
        ? scoreAtLeast(features.recentThreeCombinedDenominator, [
            { minimum: 172, points: 13 },
            { minimum: 164, points: 9 },
          ])
        : 0,
      recentFiveGamesTotal >= 15000
        ? scoreAtLeast(features.recentFiveCombinedDenominator, [
            { minimum: 164, points: 11 },
            { minimum: 159, points: 8 },
          ])
        : 0,
      recentSevenGamesTotal >= 21000
        ? scoreAtLeast(features.recentSevenCombinedDenominator, [
            { minimum: 161, points: 10 },
            { minimum: 157, points: 7 },
          ])
        : 0,
    );
    score += scoreInRange(daysSinceMachineHighContent, 7, 9, 10);
    score += scoreInRange(daysSinceMachineHighContent, 10, 29, 7);
    score +=
      daysSinceMachineHighContent >= 30 || !Number.isFinite(daysSinceMachineHighContent) ? 8 : 0;
    score += scoreInRange(daysSinceMachineHighContent, 4, 6, 5);
    score -= scoreInRange(daysSinceMachineHighContent, 1, 3, 6);
    score += scoreInRange(daysSinceMachineBigWin1500, 10, 21, 8);
    score += scoreInRange(daysSinceMachineBigWin1500, 7, 9, 6);
    score += scoreInRange(daysSinceMachineBigWin1500, 4, 6, 3);
    score -= scoreInRange(daysSinceMachineBigWin1500, 1, 2, 12);
    score -= scoreInRange(daysSinceMachineBigWin1500, 3, 3, 5);
    if (
      adjacentMachineHighContentCount7 >= 2 &&
      recentSevenMachineHighContentCount === 0 &&
      recentSevenNetTotal < 0
    ) {
      score += 9;
    }
    if (adjacentMachineNetTotal3 >= 2000 && recentThreeNetTotal < 0) {
      score += 4;
    }
    score +=
      recentFiveMinus3500StayDays >= 6
        ? -5
        : recentFiveMinus3500StayDays >= 3
          ? 5
          : recentFiveMinus3500StayDays >= 1
            ? 8
            : 0;
    score +=
      recentFiveAngleMinus80StayDays >= 6
        ? -4
        : recentFiveAngleMinus80StayDays >= 3
          ? 6
          : recentFiveAngleMinus80StayDays >= 1
            ? 3
            : 0;

    score -= previousMachineHighContent ? 18 : 0;
    score -= previousMachineHighContent && previousDifference < 1000 ? 5 : 0;
    score -=
      previousDifference >= 1500 &&
      features.previousRbDenominator > 300 &&
      features.previousCombinedDenominator > 140
        ? 12
        : 0;
    score -= recentThreeNetTotal >= 2800 ? 14 : 0;
    score -= recentFiveNetTotal >= 3300 ? 16 : 0;
    score -= recentSevenNetTotal >= 3500 ? 14 : 0;
    score -= recentFourteenNetTotal >= 6000 ? 8 : 0;
    score -= recentSevenMachineHighContentCount >= 2 ? 5 : 0;
    score -= previousGames < 2000 && streak < 2 && recentFiveNetTotal > -1800 ? 5 : 0;

    return Math.round(clamp(score, 0, 100));
  }

  if (machineKey === "girls") {
    if (
      activeLogicKey === "beam-hikari-girls" ||
      activeLogicKey === "beam-hikari-girls-normal" ||
      activeLogicKey === "beam-hikari-girls-event"
    ) {
      if (targetRangeHistoryRowCount < 7) {
        return 0;
      }

      let score = 0;

      if (activeLogicKey === "beam-hikari-girls") {
        let sinkScore = 0;
        sinkScore += scoreAtMost(recentThreeNetTotal, [
          { maximum: -1850, points: 10 },
          { maximum: -1500, points: 9 },
          { maximum: -1100, points: 7 },
          { maximum: -800, points: 5 },
          { maximum: -500, points: 3 },
        ]);
        sinkScore += scoreAtMost(recentFiveNetTotal, [
          { maximum: -2460, points: 8 },
          { maximum: -2000, points: 7 },
          { maximum: -1400, points: 5 },
          { maximum: -1000, points: 3 },
        ]);
        sinkScore += scoreAtMost(recentSevenNetTotal, [
          { maximum: -2950, points: 7 },
          { maximum: -2400, points: 6 },
          { maximum: -1700, points: 4 },
          { maximum: -1000, points: 2 },
        ]);
        score += Math.min(sinkScore, 22);

        let angleScore = 0;
        angleScore += scoreAtMost(features.recentThreeAngle, [
          { maximum: -320, points: 8 },
          { maximum: -205, points: 6 },
          { maximum: -135, points: 4 },
        ]);
        angleScore += scoreAtMost(features.recentFiveAngle, [
          { maximum: -209, points: 6 },
          { maximum: -142, points: 4 },
          { maximum: -90, points: 2 },
        ]);
        score += Math.min(angleScore, 14);

        score +=
          streak >= 7 && streak <= 9
            ? 15
            : streak === 6
              ? 13
              : streak === 4
                ? 12
                : streak >= 2 && streak <= 3
                  ? 9
                  : streak === 5
                    ? 5
                    : streak === 1
                      ? 2
                      : streak >= 10
                        ? 2
                        : 0;

        let bonusWeakScore = 0;
        bonusWeakScore +=
          features.recentSevenCombinedDenominator >= 180 && features.recentSevenRbDenominator >= 400 ? 12 : 0;
        bonusWeakScore +=
          features.recentFiveCombinedDenominator >= 180 && features.recentFiveRbDenominator >= 400 ? 8 : 0;
        bonusWeakScore +=
          features.recentThreeCombinedDenominator >= 180 && features.recentThreeRbDenominator >= 400 ? 5 : 0;
        bonusWeakScore += features.recentSevenRbDenominator >= 450 ? 4 : 0;
        score += Math.min(bonusWeakScore, 16);

        let intervalScore = 0;
        intervalScore += scoreInRange(daysSinceMachineHighContent, 8, 14, 10);
        intervalScore += scoreInRange(daysSinceMachineHighContent, 4, 7, 4);
        intervalScore += scoreInRange(daysSinceMachineHighContent, 15, 28, 3);
        intervalScore += recentSevenMachineHighContentCount === 0 ? 3 : 0;
        intervalScore += recentSevenMachineGoodContentCount === 0 ? 2 : 0;
        score += Math.min(intervalScore, 12);

        let gamesScore = 0;
        gamesScore += targetRangeHistoryRowCount >= 21 ? 7 : targetRangeHistoryRowCount >= 14 ? 5 : 3;
        gamesScore += scoreInRange(recentSevenGamesTotal, 7400, 18000, 5);
        gamesScore += scoreInRange(recentFiveGamesTotal, 5000, 15000, 3);
        gamesScore += scoreInRange(previousGames, 800, 5000, 1);
        score += Math.min(gamesScore, 16);

        let nearbyScore = 0;
        nearbyScore += adjacentMachineHighContentCount7 > 0 && recentSevenNetTotal < 0 ? 3 : 0;
        nearbyScore += adjacentMachineHighContentCount7Near2 >= 3 ? 2 : 0;
        nearbyScore += adjacentMachineNetTotal7Near2 <= 0 && recentSevenNetTotal < 0 ? 1 : 0;
        score += Math.min(nearbyScore, 5);

        let dangerScore = 0;
        dangerScore += previousMachineHighContent && previousDifference >= 1700 ? 24 : 0;
        dangerScore += previousMachineHighContent && previousDifference < 1700 ? 20 : 0;
        dangerScore += !previousMachineHighContent && previousMachineGoodContent ? 18 : 0;
        dangerScore += daysSinceMachineHighContent === 1 ? 8 : 0;
        dangerScore += previousDifference >= 1700 ? 12 : previousDifference >= 1000 ? 8 : previousDifference >= 500 ? 4 : 0;
        dangerScore += recentThreeNetTotal >= 2400 ? 10 : recentThreeNetTotal >= 1000 ? 6 : 0;
        dangerScore += recentFiveNetTotal >= 3000 ? 8 : recentFiveNetTotal >= 1500 ? 6 : 0;
        dangerScore += recentSevenNetTotal >= 2600 ? 5 : 0;
        dangerScore += recentSevenMachineGoodContentCount >= 2 ? 10 : 0;
        dangerScore += recentSevenMachineHighContentCount >= 2 ? 8 : 0;
        dangerScore += streak === 0 ? 8 : streak >= 10 ? 5 : 0;
        dangerScore += recentSevenGamesTotal >= 23000 && recentSevenNetTotal > 0 ? 5 : 0;
        dangerScore += previousGames >= 5700 && previousDifference > 0 ? 5 : 0;
        score -= Math.min(dangerScore, 45);

        return Math.round(clamp(score, 0, 100));
      }

      if (activeLogicKey === "beam-hikari-girls-event") {
        let sinkScore = 0;
        sinkScore += scoreAtMost(recentThreeNetTotal, [
          { maximum: -1800, points: 10 },
          { maximum: -1400, points: 8 },
          { maximum: -1000, points: 6 },
          { maximum: -600, points: 3 },
        ]);
        sinkScore += scoreAtMost(recentFiveNetTotal, [
          { maximum: -2500, points: 12 },
          { maximum: -2100, points: 10 },
          { maximum: -1500, points: 7 },
          { maximum: -1000, points: 4 },
        ]);
        sinkScore += scoreAtMost(recentSevenNetTotal, [
          { maximum: -3000, points: 9 },
          { maximum: -2500, points: 8 },
          { maximum: -1800, points: 6 },
          { maximum: -1200, points: 3 },
        ]);
        score += Math.min(sinkScore, 28);

        let angleScore = 0;
        angleScore += scoreAtMost(features.recentThreeAngle, [
          { maximum: -320, points: 7 },
          { maximum: -200, points: 5 },
          { maximum: -130, points: 3 },
        ]);
        angleScore += scoreAtMost(features.recentFiveAngle, [
          { maximum: -230, points: 10 },
          { maximum: -160, points: 7 },
          { maximum: -100, points: 4 },
        ]);
        angleScore += scoreAtMost(features.recentSevenAngle, [
          { maximum: -190, points: 8 },
          { maximum: -120, points: 5 },
          { maximum: -80, points: 3 },
        ]);
        score += Math.min(angleScore, 18);

        let bonusWeakScore = 0;
        bonusWeakScore +=
          features.recentSevenCombinedDenominator >= 180 && features.recentSevenRbDenominator >= 400
            ? 14
            : features.recentSevenCombinedDenominator >= 170 && features.recentSevenRbDenominator >= 350
              ? 9
              : features.recentSevenCombinedDenominator >= 160 && features.recentSevenRbDenominator >= 330
                ? 5
                : 0;
        bonusWeakScore +=
          features.recentFiveCombinedDenominator >= 180 && features.recentFiveRbDenominator >= 400
            ? 12
            : features.recentFiveCombinedDenominator >= 170 && features.recentFiveRbDenominator >= 350
              ? 7
              : 0;
        bonusWeakScore +=
          features.recentThreeCombinedDenominator >= 200 && features.recentThreeRbDenominator >= 500
            ? 6
            : features.recentThreeCombinedDenominator >= 180 && features.recentThreeRbDenominator >= 400
              ? 4
              : 0;
        bonusWeakScore += features.recentSevenRbDenominator >= 470 ? 4 : 0;
        score += Math.min(bonusWeakScore, 20);

        let rotationScore = 0;
        rotationScore += scoreInRange(daysSinceMachineHighContent, 8, 14, 10);
        rotationScore += scoreInRange(daysSinceMachineHighContent, 15, 28, 5);
        rotationScore += scoreInRange(daysSinceMachineHighContent, 4, 7, 3);
        rotationScore += recentSevenMachineHighContentCount === 0 ? 4 : 0;
        rotationScore += recentSevenMachineGoodContentCount === 0 ? 3 : 0;
        score += Math.min(rotationScore, 16);

        score +=
          streak >= 6 && streak <= 9
            ? 10
            : streak >= 2 && streak <= 3
              ? 6
              : streak === 4
                ? 4
                : streak === 5
                  ? 2
                  : streak === 1
                    ? 1
                    : 0;

        let trustScore = 0;
        trustScore += targetRangeHistoryRowCount >= 21 ? 4 : targetRangeHistoryRowCount >= 14 ? 3 : 2;
        trustScore += scoreInRange(recentSevenGamesTotal, 7000, 21000, 3);
        trustScore += scoreInRange(recentFiveGamesTotal, 5000, 16000, 2);
        trustScore += scoreInRange(previousGames, 800, 5500, 1);
        score += Math.min(trustScore, 8);

        let nearbyScore = 0;
        nearbyScore +=
          adjacentMachineHighContentCount7Near2 >= 3 && recentSevenNetTotal < 0
            ? 4
            : adjacentMachineHighContentCount7Near2 >= 1 && recentSevenNetTotal < 0
              ? 2
              : 0;
        score += Math.min(nearbyScore, 5);

        score -= previousMachineHighContent ? 22 : previousMachineGoodContent ? 18 : 0;
        score -= previousDifference >= 1700 ? 13 : previousDifference >= 1000 ? 9 : previousDifference >= 500 ? 5 : 0;
        score -= recentThreeNetTotal >= 2400 ? 9 : recentThreeNetTotal >= 1200 ? 5 : 0;
        score -= recentSevenMachineGoodContentCount >= 2 ? 8 : 0;
        score -= recentSevenMachineHighContentCount >= 2 ? 8 : 0;
        score -= streak === 0 ? 5 : streak >= 10 ? 6 : 0;
        score -= recentSevenGamesTotal >= 23000 && recentSevenNetTotal > 0 ? 5 : 0;

        return Math.round(clamp(score, 0, 100));
      }

      let sinkScore = 0;
      sinkScore += scoreAtMost(recentThreeNetTotal, [
        { maximum: -1850, points: 10 },
        { maximum: -1500, points: 8 },
        { maximum: -1050, points: 6 },
        { maximum: -650, points: 3 },
      ]);
      sinkScore += scoreAtMost(recentFiveNetTotal, [
        { maximum: -2000, points: 6 },
        { maximum: -1400, points: 4 },
        { maximum: -900, points: 2 },
      ]);
      sinkScore += scoreAtMost(recentSevenNetTotal, [
        { maximum: -2800, points: 3 },
        { maximum: -1700, points: 2 },
      ]);
      score += Math.min(sinkScore, 18);

      let angleScore = 0;
      angleScore += scoreAtMost(features.recentThreeAngle, [
        { maximum: -430, points: 12 },
        { maximum: -320, points: 10 },
        { maximum: -205, points: 7 },
        { maximum: -130, points: 4 },
      ]);
      angleScore += scoreAtMost(features.recentFiveAngle, [
        { maximum: -205, points: 6 },
        { maximum: -135, points: 4 },
        { maximum: -90, points: 2 },
      ]);
      angleScore += scoreAtMost(features.recentSevenAngle, [
        { maximum: -160, points: 4 },
        { maximum: -110, points: 2 },
      ]);
      score += Math.min(angleScore, 22);

      let bonusWeakScore = 0;
      bonusWeakScore +=
        features.recentThreeCombinedDenominator >= 200 && features.recentThreeRbDenominator >= 540
          ? 10
          : features.recentThreeCombinedDenominator >= 180 && features.recentThreeRbDenominator >= 400
            ? 7
            : features.recentThreeCombinedDenominator >= 170 && features.recentThreeRbDenominator >= 350
              ? 4
              : 0;
      bonusWeakScore +=
        features.recentSevenCombinedDenominator >= 180 && features.recentSevenRbDenominator >= 400
          ? 9
          : features.recentSevenCombinedDenominator >= 170 && features.recentSevenRbDenominator >= 350
            ? 5
            : 0;
      bonusWeakScore += features.recentFiveCombinedDenominator >= 180 && features.recentFiveRbDenominator >= 400 ? 5 : 0;
      bonusWeakScore += features.recentSevenRbDenominator >= 470 ? 4 : 0;
      score += Math.min(bonusWeakScore, 22);

      score +=
        streak === 4
          ? 16
          : streak >= 6 && streak <= 9
            ? 12
            : streak >= 2 && streak <= 3
              ? 11
              : streak === 1
                ? 4
                : streak === 5 || streak >= 10
                  ? 2
                  : 0;

      let intervalScore = 0;
      intervalScore += scoreInRange(daysSinceMachineHighContent, 8, 14, 8);
      intervalScore += scoreInRange(daysSinceMachineHighContent, 4, 7, 3);
      intervalScore += scoreInRange(daysSinceMachineHighContent, 15, 28, 4);
      intervalScore += recentSevenMachineHighContentCount === 0 ? 2 : 0;
      score += Math.min(intervalScore, 10);

      let gamesScore = 0;
      gamesScore += targetRangeHistoryRowCount >= 21 ? 4 : targetRangeHistoryRowCount >= 14 ? 3 : 2;
      gamesScore += scoreInRange(recentSevenGamesTotal, 4500, 15500, 4);
      gamesScore += scoreInRange(recentSevenGamesTotal, 15501, 18000, 2);
      gamesScore += scoreInRange(previousGames, 500, 5000, 2);
      score += Math.min(gamesScore, 10);

      score += adjacentMachineHighContentCount7Near2 >= 1 && recentSevenNetTotal < 0 ? 1 : 0;

      score -= previousMachineHighContent ? 22 : previousMachineGoodContent ? 18 : 0;
      score -= previousDifference >= 1700 ? 12 : previousDifference >= 1000 ? 8 : previousDifference >= 500 ? 4 : 0;
      score -= recentThreeNetTotal >= 2400 ? 10 : recentThreeNetTotal >= 1000 ? 6 : 0;
      score -= recentSevenNetTotal >= 2600 ? 6 : recentSevenNetTotal >= 1200 ? 4 : 0;
      score -= recentSevenMachineGoodContentCount >= 2 ? 9 : 0;
      score -= recentSevenMachineHighContentCount >= 2 ? 8 : 0;
      score -= streak === 0 ? 8 : streak >= 10 ? 5 : 0;
      score -= recentSevenGamesTotal >= 20500 ? 5 : 0;

      return Math.round(clamp(score, 0, 100));
    }

    if (activeLogicKey === "mj-kurume-girls") {
      if (historyRowCount < 14) {
        return 0;
      }

      let sinkScore = 0;
      sinkScore += scoreAtMost(recentSevenNetTotal, [
        { maximum: -3000, points: 20 },
        { maximum: -1500, points: 17 },
        { maximum: -500, points: 13 },
        { maximum: 0, points: 8 },
        { maximum: 1000, points: 3 },
      ]);
      sinkScore += scoreAtMost(recentFourteenNetTotal, [
        { maximum: -3000, points: 8 },
        { maximum: -1500, points: 6 },
        { maximum: -500, points: 4 },
        { maximum: 0, points: 2 },
      ]);
      sinkScore += scoreAtMost(recentTwentyOneNetTotal, [
        { maximum: -3000, points: 5 },
        { maximum: -1000, points: 4 },
        { maximum: 0, points: 2 },
      ]);
      sinkScore = Math.min(sinkScore, 30);

      let angleScore = 0;
      angleScore += scoreAtMost(features.recentSevenAngle, [
        { maximum: -120, points: 10 },
        { maximum: -70, points: 8 },
        { maximum: -30, points: 5 },
        { maximum: 0, points: 2 },
      ]);
      angleScore += scoreAtMost(features.recentFourteenAngle, [
        { maximum: -56, points: 5 },
        { maximum: -20, points: 4 },
        { maximum: 0, points: 2 },
      ]);
      angleScore = Math.min(angleScore, 15);

      let lossScore = 0;
      lossScore +=
        streak >= 4 ? 10 :
        streak === 3 ? 8 :
        streak === 2 ? 5 :
        streak === 1 ? 2 :
        0;
      lossScore += scoreAtMost(previousDifference, [
        { maximum: -800, points: 4 },
        { maximum: -500, points: 3 },
        { maximum: -1, points: 1 },
      ]);
      const previousPayout = previousGames > 0 ? 100 + (previousDifference / previousGames / 3) * 100 : 100;
      lossScore += previousPayout <= 85 ? 3 : previousPayout <= 93 ? 1 : 0;
      lossScore += previousCombinedDenominator >= 200 ? 3 : previousCombinedDenominator >= 170 ? 1 : 0;
      lossScore = Math.min(lossScore, 14);

      let rotationScore = 0;
      rotationScore += scoreInRange(daysSinceMachineHighContent, 6, 10, 10);
      rotationScore += scoreInRange(daysSinceMachineHighContent, 4, 5, 7);
      rotationScore += scoreInRange(daysSinceMachineHighContent, 2, 3, 5);
      rotationScore += daysSinceMachineHighContent === 1 ? 3 : 0;
      rotationScore += scoreInRange(daysSinceMachineHighContent, 11, 14, 2);
      rotationScore += recentFourteenMachineHighContentCount <= 1 ? 3 : 0;
      rotationScore = Math.min(rotationScore, 13);

      let unpaidScore = 0;
      if (previousMachineHighContent) {
        unpaidScore += previousDifference <= 1000 ? 8 : 0;
        unpaidScore += previousDifference >= 1001 && previousDifference <= 2000 ? 5 : 0;
      }
      unpaidScore += previousMachineStrongHighContent && previousDifference <= 1500 ? 2 : 0;
      unpaidScore = Math.min(unpaidScore, 10);

      let gamesScore = 0;
      gamesScore += recentSevenGamesTotal >= 8000 ? 2 : 0;
      gamesScore += recentFourteenGamesTotal >= 25000 ? 2 : 0;
      gamesScore +=
        recentSevenGamesTotal < 18000 ? 3 :
        recentSevenGamesTotal >= 18000 && recentSevenGamesTotal <= 23000 ? 1 :
        0;
      gamesScore = Math.min(gamesScore, 8);

      const nearbyScore = Math.min(
        5,
        (previousAdjacentMachineNetTotal >= 1700 ? 3 : 0) +
          (adjacentMachineNetTotal7 >= 780 ? 2 : 0),
      );

      const completionScore =
        features.boostCount >= 3 && features.dangerCount === 0
          ? 5
          : features.boostCount >= 2 && features.dangerCount === 0
            ? 2
            : 0;

      let penalty = 0;
      penalty += recentFiveNetTotal >= 2500 ? 20 : recentFiveNetTotal >= 1500 ? 15 : recentFiveNetTotal >= 1000 ? 8 : 0;
      penalty += recentSevenNetTotal >= 2500 ? 10 : recentSevenNetTotal >= 1500 ? 6 : 0;
      penalty += previousMachineHighContent && previousDifference >= 2000 ? 12 : 0;
      penalty += Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 15 ? 8 : 0;
      penalty += recentSevenGamesTotal >= 25000 && recentFiveNetTotal >= 1500 ? 5 : 0;
      penalty += recentSevenGamesTotal < 8000 ? 5 : 0;
      penalty = Math.min(penalty, 35);

      return Math.round(
        clamp(
          sinkScore +
            angleScore +
            lossScore +
            rotationScore +
            unpaidScore +
            gamesScore +
            nearbyScore +
            completionScore -
            penalty,
          0,
          100,
        ),
      );
    }

    let score = 0;
    score += scoreAtLeast(streak, [
      { minimum: 4, points: 30 },
      { minimum: 3, points: 24 },
      { minimum: 2, points: 10 },
      { minimum: 1, points: 3 },
    ]);
    score += scoreAtMost(recentSevenNetTotal, [
      { maximum: -4000, points: 16 },
      { maximum: -3000, points: 14 },
      { maximum: -2000, points: 11 },
      { maximum: -1000, points: 7 },
      { maximum: -1, points: 3 },
    ]);
    score += scoreAtMost(features.recentSevenAngle, [
      { maximum: -100, points: 10 },
      { maximum: -75, points: 9 },
      { maximum: -50, points: 7 },
      { maximum: -25, points: 4 },
    ]);
    const girlsThreeDaySinkScore = Math.max(
      scoreAtMost(features.recentThreeAngle, [
        { maximum: -175, points: 10 },
        { maximum: -100, points: 7 },
      ]),
      recentThreeNetTotal <= -1800 ? 4 : 0,
    );
    score += girlsThreeDaySinkScore;
    score += scoreInRange(daysSinceMachineHighContent, 11, 20, 16);
    score += scoreInRange(daysSinceMachineHighContent, 6, 10, 9);
    score += scoreInRange(daysSinceMachineHighContent, 21, 40, 4);
    score += scoreInRange(daysSinceMachineHighContent, 3, 5, 3);
    score += scoreInRange(daysSinceMachineHighContent, 1, 2, 1);
    score += recentSevenMachineHighContentCount === 0 ? 8 : recentSevenMachineHighContentCount === 1 ? 1 : 0;
    score -= recentSevenMachineHighContentCount >= 2 ? 8 : 0;
    score += scoreInRange(recentSevenGamesTotal, 25000, 35000, 7);
    score += scoreInRange(recentSevenGamesTotal, 35001, 42000, 4);
    score += recentSevenGamesTotal >= 42001 ? 1 : 0;
    score -= recentSevenGamesTotal < 20000 ? 3 : 0;
    score += previousMachineHighContent && previousDifference <= 500 ? 8 : 0;

    score -= previousMachineHighContent && previousDifference >= 2500 ? 16 : 0;
    score -= recentThreeNetTotal >= 2500 ? 14 : 0;
    score -= recentSevenNetTotal >= 3500 ? 12 : recentSevenNetTotal >= 3000 ? 10 : 0;
    score -= recentFourteenNetTotal >= 5000 ? 6 : 0;
    score -= !Number.isFinite(daysSinceMachineHighContent) || daysSinceMachineHighContent >= 41 ? 6 : 0;

    return Math.round(clamp(score, 0, 100));
  }

  if (machineKey === "mister") {
    let shortSinkScore = 0;
    shortSinkScore += scoreAtMost(recentThreeNetTotal, [
      { maximum: -1900, points: 22 },
      { maximum: -1400, points: 18 },
      { maximum: -800, points: 10 },
    ]);
    shortSinkScore += scoreAtMost(recentSevenNetTotal, [
      { maximum: -2400, points: 12 },
      { maximum: -1600, points: 9 },
      { maximum: -800, points: 5 },
    ]);
    shortSinkScore += recentTwentyOneNetTotal <= -2450 ? 7 : 0;
    shortSinkScore += recentFourteenNetTotal <= -1400 ? 5 : 0;
    shortSinkScore += recentTwentyEightNetTotal <= -2700 ? 4 : 0;
    shortSinkScore = Math.min(shortSinkScore, 35);

    const streakScore = scoreAtLeast(streak, [
      { minimum: 4, points: 22 },
      { minimum: 3, points: 18 },
      { minimum: 2, points: 10 },
      { minimum: 1, points: 3 },
    ]);

    let angleScore = 0;
    angleScore += scoreAtMost(features.recentThreeAngle, [
      { maximum: -150, points: 10 },
      { maximum: -100, points: 8 },
      { maximum: -60, points: 4 },
    ]);
    angleScore += scoreAtMost(features.recentSevenAngle, [
      { maximum: -75, points: 5 },
      { maximum: -45, points: 4 },
      { maximum: -25, points: 2 },
    ]);
    angleScore += recentThreeNetTotal <= -1400 && recentThreeGamesTotal >= 15000 ? 3 : 0;
    angleScore -= recentThreeGamesTotal < 5700 ? 4 : 0;
    angleScore = Math.min(angleScore, 15);

    let restScore = 0;
    restScore += scoreAtLeast(daysSinceMachineHighContent, [
      { minimum: 16, points: 6 },
      { minimum: 12, points: 4 },
      { minimum: 9, points: 2 },
    ]);
    restScore += scoreAtLeast(daysSinceMachineStrongHighContent, [
      { minimum: 30, points: 5 },
      { minimum: 22, points: 3 },
      { minimum: 16, points: 1 },
    ]);
    restScore += recentSevenMachineHighContentCount === 0 ? 2 : 0;
    restScore += recentFourteenMachineStrongHighContentCount === 0 ? 1 : 0;
    restScore = Math.min(restScore, 12);

    let unpaidScore = 0;
    unpaidScore += scoreAtMost(recentFourteenNetTotal, [
      { maximum: -2487, points: 5 },
      { maximum: -1400, points: 4 },
    ]);
    unpaidScore += recentTwentyOneNetTotal <= -2450 ? 3 : 0;
    unpaidScore += recentThirtyNetTotal <= -2700 ? 2 : 0;
    unpaidScore += previousDifference > 1000 && recentThirtyNetTotal < 0 ? 3 : 0;
    unpaidScore += adjacentMachineHighContentCount7 > 0 && recentSevenNetTotal < 0 ? 3 : 0;
    unpaidScore += adjacentMachineNetTotal7 > 0 && recentSevenNetTotal < 0 ? 2 : 0;
    unpaidScore = Math.min(unpaidScore, 10);

    let previousContentScore = 0;
    previousContentScore += previousMachineStrongHighContent && previousDifference <= 1000 ? 3 : 0;
    previousContentScore += previousCombinedDenominator <= 135 && previousRbDenominator <= 290 && previousDifference < 0 ? 2 : 0;
    previousContentScore = Math.min(previousContentScore, 6);

    let penalty = 0;
    penalty += recentSevenNetTotal > 2500 ? 14 : 0;
    penalty += recentFourteenNetTotal > 3600 ? 10 : 0;
    penalty += recentTwentyOneNetTotal > 4716 ? 6 : 0;
    penalty += previousMachineHighContent && previousDifference > 500 ? 8 : 0;
    penalty += previousDifference > 1500 ? 5 : 0;
    penalty += recentSevenMachineHighContentCount >= 2 ? 5 : 0;
    penalty += recentSevenMinus1500StayDays >= 7 ? 8 : 0;
    penalty += recentThirtyMinus2700StayDays >= 7 ? 6 : 0;

    return Math.round(clamp(
      shortSinkScore + streakScore + angleScore + restScore + unpaidScore + previousContentScore - penalty,
      0,
      100,
    ));
  }

  if (machineKey === "star-hana") {
    let score = 0;
    score += scoreAtMost(recentSevenNetTotal, [
      { maximum: -2000, points: 18 },
      { maximum: -1000, points: 14 },
      { maximum: -500, points: 8 },
    ]);
    score += scoreAtMost(recentFourteenNetTotal, [
      { maximum: -3000, points: 10 },
      { maximum: -1500, points: 7 },
      { maximum: -500, points: 4 },
    ]);
    score += scoreAtMost(features.recentSevenAngle, [
      { maximum: -80, points: 10 },
      { maximum: -50, points: 8 },
      { maximum: -25, points: 4 },
    ]);
    score += scoreAtMost(features.recentFourteenAngle, [
      { maximum: -50, points: 4 },
      { maximum: -30, points: 3 },
    ]);
    score += recentFourteenMachineHighContentCount === 0 ? 16 : recentFourteenMachineHighContentCount === 1 ? 4 : 0;
    score -= recentFourteenMachineHighContentCount >= 2 ? 10 : 0;
    score += recentSevenMachineHighContentCount === 0 ? 5 : 0;
    score -= recentSevenMachineHighContentCount >= 2 ? 8 : 0;
    score += scoreInRange(daysSinceMachineHighContent, 15, 21, 6);
    score += scoreInRange(daysSinceMachineHighContent, 22, 35, 4);
    score += daysSinceMachineHighContent >= 36 ? 3 : 0;
    score -= daysSinceMachineHighContent <= 7 && Number.isFinite(daysSinceMachineHighContent) ? 5 : 0;
    score += streak === 2 ? 4 : streak === 3 ? 7 : streak === 4 ? 3 : streak >= 5 && streak <= 10 ? 10 : 0;
    score -= streak >= 11 ? 4 : 0;
    score += scoreInRange(recentSevenGamesTotal, 12000, 35000, 5);
    score += recentSevenGamesTotal > 35000 ? 3 : 0;
    score -= recentSevenGamesTotal < 12000 ? 8 : 0;
    score += recentFourteenGamesTotal >= 25000 ? 3 : 0;
    score -= recentFourteenGamesTotal < 25000 ? 4 : 0;
    score += scoreAtLeast(features.recentSevenCombinedDenominator, [
      { minimum: 150, points: 4 },
      { minimum: 145, points: 2 },
    ]);
    score += previousCombinedDenominator >= 250 ? 5 : previousCombinedDenominator >= 180 ? 3 : 0;
    score -= previousCombinedDenominator <= 120 ? 4 : 0;

    score -= recentSevenNetTotal > 1500 ? 12 : 0;
    score -= recentFourteenNetTotal > 5000 ? 12 : 0;
    score -= previousMachineGoodContent && previousDifference > 1200 ? 8 : 0;
    score -= previousMachineHighContent ? 5 : 0;
    score -= previousDifference > 1800 ? 5 : 0;
    score -= features.recentSevenAngle > 25 ? 5 : 0;

    return Math.round(clamp(score, 0, 100));
  }

  if (machineKey === "dragon-hana") {
    const dragonStreak = historyLosingStreak;
    let score = 0;
    if (recentSevenNetTotal <= -3000) {
      score += 12;
    } else if (recentSevenNetTotal <= -2000) {
      score += 18;
    } else if (recentSevenNetTotal <= -1000) {
      score += 16;
    } else if (recentSevenNetTotal <= 0) {
      score += 6;
    }
    if (recentFourteenNetTotal <= -5000) {
      score += 5;
    } else if (recentFourteenNetTotal <= -3000) {
      score += 3;
    } else if (recentFourteenNetTotal <= -1000) {
      score += 2;
    }
    if (previousDifference <= -1500) {
      score += 5;
    } else if (previousDifference <= -800) {
      score += 2;
    } else if (previousDifference <= -300) {
      score += 3;
    }
    if (features.recentSevenAngle <= -180) {
      score += 8;
    } else if (features.recentSevenAngle <= -100) {
      score += 13;
    } else if (features.recentSevenAngle <= -70) {
      score += 10;
    } else if (features.recentSevenAngle <= -40) {
      score += 5;
    }
    if (features.recentFourteenAngle <= -150) {
      score += 2;
    } else if (features.recentFourteenAngle <= -100) {
      score += 5;
    } else if (features.recentFourteenAngle <= -70) {
      score += 3;
    } else if (features.recentFourteenAngle <= -40) {
      score += 1;
    }
    score += scoreAtLeast(features.recentSevenCombinedDenominator, [
      { minimum: 220, points: 12 },
      { minimum: 190, points: 10 },
      { minimum: 175, points: 6 },
      { minimum: 160, points: 2 },
    ]);
    if (features.recentSevenRbDenominator >= 1000) {
      score += 6;
    } else if (features.recentSevenRbDenominator >= 800) {
      score += 8;
    } else if (features.recentSevenRbDenominator >= 600) {
      score += 3;
    }
    score += scoreInRange(recentSevenGamesTotal, 10000, 15000, 5);
    score += scoreInRange(recentSevenGamesTotal, 15001, 20000, 7);
    score += scoreInRange(recentSevenGamesTotal, 20001, 25000, 3);
    score += scoreInRange(recentSevenGamesTotal, 25001, 30000, 1);
    score += scoreInRange(recentFourteenGamesTotal, 20000, 30000, 2);
    score += scoreInRange(recentFourteenGamesTotal, 30001, 40000, 3);
    score +=
      dragonStreak === 2
        ? 1
        : dragonStreak === 3
          ? 4
          : dragonStreak >= 4 && dragonStreak <= 5
            ? 6
            : dragonStreak >= 6 && dragonStreak <= 7
              ? 8
              : dragonStreak >= 8
                ? 2
                : 0;
    score += scoreInRange(daysSinceMachineHighContent, 8, 14, 2);
    score += scoreInRange(daysSinceMachineHighContent, 15, 30, 4);
    score += scoreInRange(daysSinceMachineHighContent, 31, 60, 1);
    score += previousDifference > 0 && recentFourteenNetTotal <= -1000 ? 3 : 0;
    score += recentThirtyNetTotal <= -3000 ? 2 : 0;
    score += recentSevenNetTotal <= -1000 && sameMachinePreviousNetTotal > 0 ? 5 : 0;
    score += recentSevenNetTotal <= -1000 && previousAdjacentMachineGoodContentCount > 0 ? 2 : 0;

    score -= recentSevenNetTotal > 2000 ? 15 : recentSevenNetTotal > 1000 ? 10 : recentSevenNetTotal > 0 ? 6 : 0;
    score -= features.recentSevenAngle > 100 ? 10 : features.recentSevenAngle > 40 ? 6 : features.recentSevenAngle > 0 ? 3 : 0;
    score -= previousDifference > 2500 ? 8 : previousDifference > 1500 ? 5 : previousDifference > 800 ? 2 : 0;
    score -= previousMachineHighContent ? 4 : 0;
    score -= previousMachineHighContent && previousDifference >= 1500 ? 4 : 0;
    score -= previousMachineGoodContent ? 2 : 0;
    score -= features.recentSevenCombinedDenominator <= 160 ? 6 : 0;
    score -= recentSevenGamesTotal >= 30000 ? 4 : 0;
    score -=
      Number.isFinite(daysSinceMachineHighContent) &&
      daysSinceMachineHighContent >= 61 &&
      recentFourteenNetTotal > 0
        ? 3
        : 0;

    return Math.round(clamp(score, 0, 100));
  }

  if (machineKey === "new-king-hana") {
    if (readNumber(metrics.historyRowCount) < 21) {
      return 0;
    }

    let score = 0;
    score += Math.max(
      scoreAtMost(recentTwentyOneNetTotal, [{ maximum: -8000, points: 24 }]),
      scoreAtMost(recentFourteenNetTotal, [
        { maximum: -6000, points: 20 },
        { maximum: -4000, points: 8 },
        { maximum: -2000, points: 4 },
      ]),
      scoreAtMost(recentSevenNetTotal, [
        { maximum: -5000, points: 18 },
        { maximum: -3000, points: 10 },
      ]),
      scoreAtMost(recentFiveNetTotal, [{ maximum: -4000, points: 14 }]),
    );
    score += Math.max(
      recentFourteenGamesTotal >= 28000 && features.recentFourteenAngle <= -110 ? 14 : 0,
      recentFourteenGamesTotal >= 25000 && features.recentFourteenAngle <= -80 ? 10 : 0,
      recentSevenGamesTotal >= 14000 && features.recentSevenAngle <= -170 ? 8 : 0,
      recentSevenGamesTotal >= 14000 && features.recentSevenAngle <= -120 ? 6 : 0,
      recentSevenGamesTotal >= 14000 && features.recentSevenAngle <= -70 ? 3 : 0,
    );
    score += Math.max(
      recentThreeNetTotal <= -3000 && recentThreeGamesTotal >= 6000 ? 13 : 0,
      recentThreeNetTotal <= -2000 && recentThreeGamesTotal >= 6000 ? 6 : 0,
    );
    score += previousDifference <= -1000 && previousGames >= 1000 ? 5 : 0;
    score += scoreAtLeast(streak, [
      { minimum: 7, points: 15 },
      { minimum: 6, points: 12 },
      { minimum: 4, points: 6 },
    ]);
    score += Math.max(
      recentFourteenGamesTotal >= 28000 && features.recentFourteenCombinedDenominator >= 185 && features.recentFourteenRbDenominator >= 420 ? 12 : 0,
      recentSevenGamesTotal >= 14000 && features.recentSevenCombinedDenominator >= 185 && features.recentSevenRbDenominator >= 420 ? 5 : 0,
    );
    score += recentSevenGamesTotal >= 17000 ? 5 : 0;
    score += recentSevenGamesTotal >= 28000 ? 2 : 0;
    score += recentThreeGamesTotal >= 9000 ? 3 : 0;
    score += scoreInRange(daysSinceMachineHighContent, 4, 8, 5);
    score += scoreInRange(daysSinceMachineHighContent, 2, 3, 2);

    score -= daysSinceMachineHighContent > 30 ? 6 : 0;
    score -= previousMachineHighContent ? 8 : 0;
    score -= previousMachineHighContent && previousDifference >= 1500 ? 7 : 0;
    score -= previousMachineHighContent && previousDifference <= 0 ? 3 : 0;
    score -= previousDifference >= 1500 ? 8 : 0;
    score -= recentTwentyOneNetTotal >= 6000 ? 12 : 0;
    score -= recentFourteenNetTotal >= 6000 ? 8 : 0;
    score -= recentFiveNetTotal >= 3000 ? 8 : 0;
    score -= recentThreeNetTotal >= 1000 ? 5 : 0;
    score -= recentSevenGamesTotal < 17000 ? 5 : 0;
    score -= recentThreeGamesTotal < 6000 ? 3 : 0;

    return Math.round(clamp(score, 0, 100));
  }

  if (machineKey === "houou") {
    let score = 0;
    score += recentFiveGamesTotal >= 10000 ? 4 : 0;
    score += recentSevenGamesTotal >= 15000 ? 3 : 0;
    score += recentFourteenGamesTotal >= 30000 ? 3 : 0;
    score += scoreAtMost(recentSevenNetTotal, [
      { maximum: -3700, points: 12 },
      { maximum: -2000, points: 10 },
      { maximum: -1400, points: 7 },
      { maximum: -800, points: 4 },
    ]);
    score += scoreAtMost(recentTwentyOneNetTotal, [
      { maximum: -5900, points: 8 },
      { maximum: -3700, points: 6 },
      { maximum: -1800, points: 3 },
    ]);
    score += scoreAtMost(recentTwentyEightNetTotal, [
      { maximum: -5800, points: 6 },
      { maximum: -4400, points: 4 },
      { maximum: -2200, points: 2 },
    ]);
    score += scoreAtMost(features.recentFiveAngle, [
      { maximum: -240, points: 8 },
      { maximum: -180, points: 6 },
      { maximum: -120, points: 4 },
      { maximum: -80, points: 2 },
    ]);
    score += scoreAtMost(features.recentSevenAngle, [
      { maximum: -180, points: 4 },
      { maximum: -140, points: 3 },
      { maximum: -90, points: 2 },
    ]);
    score += Math.max(
      recentSevenNetTotal <= -3000 && recentSevenMinus3000StayDays >= 4 ? 18 : 0,
      recentSevenNetTotal <= -3000 && recentSevenMinus3000StayDays === 3 ? 14 : 0,
      recentSevenNetTotal <= -2000 && recentSevenMinus2000StayDays >= 3 ? 10 : 0,
      recentSevenNetTotal <= -2000 && recentSevenMinus2000StayDays === 2 ? 7 : 0,
      recentFiveMinus500StayDays >= 5 ? 5 : 0,
    );
    score += previousDifference <= -1200 && previousGames >= 1000 ? 8 : 0;
    score += previousDifference <= -900 && previousGames >= 800 ? 6 : 0;
    score += previousDifference <= -500 && previousGames >= 500 ? 4 : 0;
    score += previousDifference < 0 && previousGames >= 3000 ? 2 : 0;
    score += recentFiveGamesTotal >= 3500
      ? scoreAtLeast(features.recentFiveCombinedDenominator, [
          { minimum: 200, points: 8 },
          { minimum: 180, points: 6 },
          { minimum: 170, points: 3 },
        ])
      : 0;
    score += recentSevenGamesTotal >= 4900
      ? scoreAtLeast(features.recentSevenCombinedDenominator, [
          { minimum: 200, points: 5 },
          { minimum: 190, points: 4 },
          { minimum: 180, points: 2 },
        ])
      : 0;
    score += features.recentThreeRbDenominator >= 600 && features.recentThreeCombinedDenominator >= 180 ? 2 : 0;
    score += recentFourteenMachineHighContentCount === 0 ? 5 : 0;
    score += recentSevenMachineHighContentCount === 0 ? 3 : 0;
    score += scoreInRange(daysSinceMachineHighContent, 8, 28, 3);
    score += daysSinceMachineHighContent >= 28 ? 4 : 0;

    score -= previousMachineHighContent && previousDifference >= 1500 ? 10 : 0;
    score -= previousMachineHighContent && previousDifference >= 1000 ? 6 : 0;
    score -= recentThreeNetTotal >= 1500 ? 7 : 0;
    score -= recentSevenNetTotal >= 1800 ? 8 : 0;
    score -= recentFourteenNetTotal >= 3000 ? 10 : 0;
    score -= recentTwentyOneNetTotal >= 4000 ? 8 : 0;
    score -= recentFiveGamesTotal < 3000 ? 10 : 0;
    score -= recentSevenGamesTotal < 5000 ? 5 : 0;
    score -= recentSevenMachineHighContentCount >= 2 && recentSevenNetTotal > 0 ? 5 : 0;

    return Math.round(clamp(score, 0, 100));
  }

  if (machineKey === "thunder") {
    let score = 0;
    score += scoreAtMost(previousDifference, [
      { maximum: -1500, points: 11 },
      { maximum: -1000, points: 6 },
    ]);
    score += scoreAtMost(recentTwoNetTotal, [
      { maximum: -2100, points: 7 },
      { maximum: -1600, points: 4 },
    ]);
    score += scoreAtMost(recentThreeNetTotal, [
      { maximum: -2200, points: 5 },
      { maximum: -1500, points: 3 },
    ]);
    score += recentTwoGamesTotal >= 4000
      ? scoreAtMost(netPerThousandGames(recentTwoNetTotal, recentTwoGamesTotal), [
          { maximum: -400, points: 16 },
          { maximum: -350, points: 12 },
          { maximum: -300, points: 5 },
        ])
      : 0;
    score += scoreAtMost(features.recentThreeAngle, [
      { maximum: -300, points: 14 },
      { maximum: -220, points: 6 },
    ]);
    score += scoreInRange(previousGames, 2000, 3000, 6);
    score += scoreInRange(previousGames, 1000, 4000, 3);
    score += scoreInRange(recentThreeGamesTotal, 5000, 7000, 8);
    score += scoreInRange(recentThreeGamesTotal, 7001, 9000, 3);
    score += scoreInRange(recentFiveGamesTotal, 10000, 13000, 6);
    score += streak === 3 ? 12 : streak === 2 ? 6 : streak === 1 ? 2 : 0;
    score += scoreAtLeast(recentTwoCombinedDenominator, [
      { minimum: 220, points: 8 },
      { minimum: 190, points: 5 },
    ]);
    score += scoreAtLeast(features.recentFiveRbDenominator, [
      { minimum: 530, points: 5 },
      { minimum: 490, points: 2 },
    ]);
    score += scoreInRange(daysSinceMachineHighContent, 4, 12, 8);
    score += scoreInRange(daysSinceMachineHighContent, 13, 20, 3);
    score += scoreInRange(daysSinceMachineHighContent, 2, 3, 2);
    const thunderRepaymentScore =
      (previousDifference > 0 &&
      previousDifference < 1000 &&
      (recentTenNetTotal < 0 || recentFourteenNetTotal < 0)
        ? 7
        : 0) +
      (previousDifference > 0 && previousDifference < 1500 && recentFourteenNetTotal < 0 ? 4 : 0) +
      (recentTwoNetTotal > 0 && recentFourteenNetTotal < 0 ? 4 : 0);
    score += Math.min(thunderRepaymentScore, 10);

    score -= previousMachineHighContent ? 16 : 0;
    score -= previousDifference >= 2000 ? 22 : previousDifference >= 1500 ? 14 : 0;
    score -= recentThreeMachineHighContentCount >= 1 ? 5 : 0;
    score -= streak >= 6 ? 20 : streak >= 4 ? 10 : 0;
    score -= Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent <= 1 ? 8 : 0;
    score -= Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent > 20 ? 8 : 0;
    score -= recentThreeGamesTotal < 5000 ? 8 : 0;
    score -= recentThreeGamesTotal > 14000 ? 6 : 0;
    score -= previousGames < 1000 ? 5 : 0;
    score -= previousGames > 4000 && previousGames <= 5000 ? 3 : 0;

    return Math.round(clamp(score, 0, 100));
  }

  if (machineKey === "smart-hanabi") {
    let score = 40;
    if (previousGames >= 2000 && previousCombinedDenominator <= 146 && previousRbDenominator <= 333) {
      score += 18;
      score += previousGames >= 4000 && previousCombinedDenominator <= 145 && previousRbDenominator <= 300 ? 4 : 0;
    } else if (previousGames >= 2000 && previousRbDenominator <= 333 && previousCombinedDenominator <= 171) {
      score += 10;
    } else if (previousGames >= 2000 && previousCombinedDenominator <= 146) {
      score += 6;
    }
    score += previousDifference >= 1001 && previousDifference <= 2000 ? 5 : 0;
    score += previousDifference >= 2001 ? 2 : 0;
    score -= previousCombinedDenominator >= 218 ? 12 : 0;
    score -= previousRbDenominator >= 539 ? 6 : 0;
    score += scoreInRange(netPerThousandGames(recentTenNetTotal, recentTenGamesTotal), 30, 85, 10);
    score += scoreInRange(features.recentFourteenAngle, 30, 80, 8);
    score += scoreInRange(features.recentSevenAngle, 30, 100, 5);
    score -= netPerThousandGames(recentTenNetTotal, recentTenGamesTotal) <= -75 ? 8 : 0;
    score -= features.recentThreeAngle <= -160 ? 8 : 0;
    score += scoreInRange(daysSinceMachineHighContent, 0, 2, 5);
    score += scoreInRange(daysSinceMachineHighContent, 8, 12, 6);
    score += scoreInRange(daysSinceMachineHighContent, 13, 17, 2);
    score += recentFourteenMachineHighContentCount === 3 ? 8 : 0;
    score -= recentFourteenMachineHighContentCount === 2 ? 4 : 0;
    score -= recentFourteenMachineHighContentCount >= 4 ? 6 : 0;
    score += recentSevenMachineHighContentCount === 3 ? 4 : 0;
    score -= recentSevenMachineHighContentCount >= 4 ? 8 : 0;
    score += scoreInRange(previousGames, 2000, 2999, 8);
    score += scoreInRange(previousGames, 3000, 6499, 3);
    score -= previousGames < 2000 ? 8 : 0;
    score -= previousGames >= 6500 ? 4 : 0;
    score += scoreInRange(recentSevenGamesTotal, 22000, 30000, 4);
    score -= recentSevenGamesTotal < 19000 ? 5 : 0;
    score -= streak >= 4 && streak <= 7 ? 5 : 0;
    score += streak >= 8 ? 3 : 0;
    score += previousAdjacentMachineHighContentCount === 0 ? 2 : 0;
    score -= previousAdjacentMachineHighContentCount === 1 ? 4 : 0;
    score += previousOtherMachineHighContentCount === 2 ? 3 : 0;
    score -= previousOtherMachineHighContentCount === 3 ? 3 : 0;

    return Math.round(clamp(score, 0, 100));
  }

  if (machineKey === "okidoki-duo") {
    if (readNumber(metrics.historyRowCount) < 14) {
      return 0;
    }

    const sinkScore = Math.max(
      scoreAtMost(recentThreeNetTotal, [
        { maximum: -5400, points: 39 },
        { maximum: -4300, points: 29 },
        { maximum: -3000, points: 20 },
        { maximum: -2100, points: 10 },
      ]),
      scoreAtMost(recentTwoNetTotal, [
        { maximum: -4500, points: 37 },
        { maximum: -3400, points: 25 },
        { maximum: -2300, points: 14 },
      ]),
      scoreAtMost(recentFiveNetTotal, [
        { maximum: -7500, points: 35 },
        { maximum: -5800, points: 27 },
        { maximum: -3850, points: 18 },
        { maximum: -2600, points: 10 },
      ]),
      scoreAtMost(previousDifference, [
        { maximum: -3000, points: 18 },
        { maximum: -2300, points: 11 },
        { maximum: -1425, points: 6 },
      ]),
    );

    const angleScore = Math.max(
      recentFiveGamesTotal >= 4000 && features.recentFiveAngle <= -553 ? 18 : 0,
      recentFiveGamesTotal >= 4000 && features.recentFiveAngle <= -456 ? 14 : 0,
      recentFiveGamesTotal >= 7000 && features.recentFiveAngle <= -323 ? 8 : 0,
      recentFourteenGamesTotal >= 12000 && features.recentFourteenAngle <= -338 ? 7 : 0,
      recentFourteenGamesTotal >= 12000 && features.recentFourteenAngle <= -173 ? 4 : 0,
    );

    let rotationScore = 0;
    rotationScore += recentFourteenMachineHighContentCount === 0 ? 12 : recentFourteenMachineHighContentCount <= 1 ? 8 : 0;
    rotationScore += recentTenMachineHighContentCount <= 1 ? 4 : 0;
    rotationScore += recentFiveMachineHighContentCount === 0 ? 2 : 0;
    rotationScore += Math.max(
      scoreInRange(daysSinceMachineHighContent, 5, 10, 5),
      scoreInRange(daysSinceMachineHighContent, 14, 21, 5),
      scoreInRange(daysSinceMachineHighContent, 3, 5, 2),
    );
    rotationScore -= !Number.isFinite(daysSinceMachineHighContent) || daysSinceMachineHighContent > 21 ? 3 : 0;
    rotationScore -= recentThreeMachineHighContentCount >= 2 ? 12 : 0;
    rotationScore -= recentFourteenMachineHighContentCount >= 4 ? 8 : 0;
    rotationScore = Math.min(rotationScore, 15);

    let streakScore = 0;
    streakScore += streak === 3 ? 7 : streak === 2 ? 5 : streak >= 5 ? 2 : 0;
    streakScore += previousDifference > 0 && winningStreak < 2 ? 1 : 0;
    streakScore -= winningStreak >= 2 ? 7 : 0;
    streakScore = clamp(streakScore, -7, 7);

    let bonusScore = 0;
    bonusScore += features.recentFourteenCombinedDenominator >= 136 ? 4 : features.recentFourteenCombinedDenominator >= 130 ? 2 : 0;
    bonusScore += features.recentSevenCombinedDenominator >= 142 ? 2 : 0;
    bonusScore += features.recentFourteenRbDenominator >= 424 ? 4 : features.recentFourteenRbDenominator >= 381 ? 3 : 0;
    bonusScore += features.recentSevenRbDenominator >= 343 && features.recentSevenRbDenominator <= 384 ? 1 : 0;
    bonusScore -= features.recentFourteenCombinedDenominator <= 104 ? 5 : 0;
    bonusScore -= features.recentSevenCombinedDenominator <= 106 ? 3 : 0;
    bonusScore -= features.recentFourteenRbDenominator <= 326 ? 5 : 0;
    bonusScore -= features.recentSevenRbDenominator <= 300 ? 2 : 0;
    bonusScore = clamp(bonusScore, -9, 9);

    let nearbyScore = 0;
    nearbyScore += adjacentMachineHighContentCount14Near2 >= 2 && recentFourteenMachineHighContentCount === 0 ? 6 : 0;
    nearbyScore += adjacentMachineNetTotal5Near2 > 0 && recentFiveNetTotal < 0 ? 2 : 0;
    nearbyScore += adjacentMachineHighContentCount7Near2 === 0 ? 1 : 0;
    nearbyScore = Math.min(nearbyScore, 7);

    let gamesScore = 0;
    gamesScore += Math.max(
      recentThreeGamesTotal >= 4000 ? 3 : 0,
      recentFiveGamesTotal >= 7000 && recentFiveNetTotal < 0 ? 3 : 0,
    );
    gamesScore -= recentTwoGamesTotal < 1800 ? 6 : 0;
    gamesScore -= recentThreeGamesTotal < 2500 ? 3 : 0;
    gamesScore = clamp(gamesScore, -7, 7);

    let treatmentPenalty = 0;
    treatmentPenalty += recentTwoNetTotal >= 6600 ? 23 : 0;
    treatmentPenalty += recentThreeNetTotal >= 7500 ? 22 : 0;
    treatmentPenalty += previousDifference >= 5000 ? 19 : 0;
    treatmentPenalty += recentFiveNetTotal >= 7100 ? 14 : 0;
    treatmentPenalty += recentThreeNetTotal >= 5550 ? 11 : 0;
    treatmentPenalty += previousMachineHighContent && previousDifference >= 3200 ? 12 : 0;
    treatmentPenalty += previousMachineHighContent && previousDifference >= 750 ? 6 : 0;
    treatmentPenalty += winningStreak >= 2 ? 10 : 0;
    treatmentPenalty = Math.min(treatmentPenalty, 24);

    const hasSinkCore =
      recentThreeNetTotal <= -4300 ||
      recentTwoNetTotal <= -4500 ||
      recentFiveNetTotal <= -5800 ||
      (recentFiveGamesTotal >= 4000 && features.recentFiveAngle <= -456);
    const hasShortDeepSink =
      recentThreeNetTotal <= -5400 ||
      recentTwoNetTotal <= -4500 ||
      previousDifference <= -3000;

    let score =
      sinkScore +
      angleScore +
      rotationScore +
      streakScore +
      bonusScore +
      nearbyScore +
      gamesScore -
      treatmentPenalty;
    if (!hasShortDeepSink) {
      score = Math.min(score, 73);
    }
    if (!hasSinkCore) {
      score = Math.min(score, 64);
    }

    return Math.round(clamp(score, 0, 100));
  }

  if (machineKey === "okidoki-gold") {
    let score = 0;
    score += scoreAtMost(recentFiftySixNetTotal, [
      { maximum: -22250, points: 15 },
      { maximum: -17086, points: 11 },
      { maximum: -13163, points: 7 },
    ]);
    score += scoreAtMost(recentFortyTwoNetTotal, [
      { maximum: -20096, points: 6 },
      { maximum: -14964, points: 4 },
    ]);
    score += scoreAtMost(recentTwentyEightNetTotal, [
      { maximum: -12046, points: 6 },
      { maximum: -7987, points: 3 },
    ]);
    score += scoreAtMost(recentFourteenNetTotal, [
      { maximum: -7714, points: 3 },
      { maximum: -2946, points: 1 },
    ]);
    if (features.recentSevenAngle > -178.3 && features.recentSevenAngle <= -64.3) {
      score += 6;
    } else if (features.recentSevenAngle <= -178.3) {
      score += 3;
    } else if (features.recentSevenAngle > -64.3 && features.recentSevenAngle < 0) {
      score += 2;
    }
    score += streak >= 6 ? 4 : streak >= 3 && streak <= 5 ? 3 : streak === 1 ? 2 : streak === 2 ? 1 : 0;
    score += recentFourteenWinDays >= 2 && recentFourteenWinDays <= 3 ? 4 : recentFourteenWinDays >= 4 && recentFourteenWinDays <= 5 ? 2 : 0;
    score += scoreAtLeast(recentSevenGamesTotal, [
      { minimum: 34996, points: 7 },
      { minimum: 30020, points: 5 },
      { minimum: 25634, points: 3 },
    ]);
    score += scoreAtLeast(recentFourteenGamesTotal, [
      { minimum: 68392, points: 5 },
      { minimum: 59896, points: 3 },
      { minimum: 52980, points: 1 },
    ]);
    score += previousGames >= 5603 ? 3 : previousGames >= 4299 ? 2 : 0;
    score += scoreAtLeast(features.recentFourteenRbDenominator, [
      { minimum: 557.2, points: 8 },
      { minimum: 535.6, points: 6 },
      { minimum: 501.8, points: 3 },
    ]);
    score += scoreInRange(features.recentFourteenCombinedDenominator, 166.2, 180.1, 5);
    score += features.recentFourteenCombinedDenominator >= 180.1 ? 3 : 0;
    score += recentFourteenGoldShowDays === 0 ? 6 : recentFourteenGoldShowDays === 1 ? 3 : 0;
    score += recentFourteenMachineHighContentCount === 0 ? 5 : recentFourteenMachineHighContentCount === 1 ? 3 : 0;
    score += scoreInRange(daysSinceMachineHighContent, 17, 25, 6);
    score += daysSinceMachineHighContent === 3 ? 3 : daysSinceMachineHighContent === 2 ? 2 : daysSinceMachineHighContent === 1 ? -7 : 0;
    score += daysSinceMachineHighContent >= 26 ? -4 : 0;
    score += previousMachineHighContent && previousDifference < 0 ? 6 : 0;

    score -= previousDifference >= 2173 ? 8 : 0;
    score -= previousMachineStrongHighContent ? 6 : 0;
    score -= previousMachineHighContent ? 4 : 0;
    score -= machineHighContentStreak >= 2 ? 10 : 0;
    score -= recentFourteenGoldShowDays >= 6 ? 9 : 0;
    score -= recentFourteenMachineHighContentCount >= 6 ? 10 : 0;
    score -= recentFourteenNetTotal >= 5463 ? 4 : 0;
    score -= recentFiftySixNetTotal >= 10077 ? 3 : 0;
    score -= recentSevenGamesTotal <= 23409 ? 5 : 0;
    score -= previousGames <= 2003 ? 3 : 0;

    return Math.round(clamp(score, 0, 100));
  }

  if (machineKey === "funky") {
    if (
      activeLogicKey === "beam-hikari-funky" ||
      activeLogicKey === "beam-hikari-funky-normal" ||
      activeLogicKey === "beam-hikari-funky-event"
    ) {
      if (historyRowCount < 21) {
        return 0;
      }

      const recentThreeCombinedDenominator = features.recentThreeCombinedDenominator;
      const recentThreeRbDenominator = features.recentThreeRbDenominator;
      const recentFiveAngle = features.recentFiveAngle;
      const recentSevenAngle = features.recentSevenAngle;
      const recentThreeAngle = features.recentThreeAngle;
      let score = 15;

      if (activeLogicKey === "beam-hikari-funky") {
        score += scoreInRange(recentThreeGamesTotal, 3000, 6000, 14);
        score += scoreInRange(recentThreeGamesTotal, 2000, 2999, 7);
        score += scoreInRange(recentThreeGamesTotal, 6001, 7500, 7);
        score += scoreInRange(recentThreeGamesTotal, 7501, 9000, 2);
        score -= recentThreeGamesTotal < 1500 ? 5 : 0;
        score -= recentThreeGamesTotal > 12000 ? 6 : 0;

        score += streak >= 4 ? 15 : streak === 3 ? 14 : streak === 2 ? 12 : streak === 1 ? 3 : -6;

        score += scoreAtMost(recentThreeAngle, [
          { maximum: -300, points: 12 },
          { maximum: -200, points: 10 },
          { maximum: -100, points: 7 },
          { maximum: -30, points: 3 },
        ]);
        score -= recentThreeAngle >= 50 ? 4 : 0;

        let bonusWeakScore = 0;
        bonusWeakScore += scoreAtLeast(recentThreeCombinedDenominator, [
          { minimum: 205, points: 8 },
          { minimum: 185, points: 6 },
          { minimum: 170, points: 4 },
        ]);
        bonusWeakScore += scoreAtLeast(recentThreeRbDenominator, [
          { minimum: 610, points: 8 },
          { minimum: 500, points: 6 },
          { minimum: 430, points: 3 },
        ]);
        bonusWeakScore -= recentThreeCombinedDenominator <= 145 ? 4 : 0;
        bonusWeakScore -= recentThreeRbDenominator <= 320 ? 3 : 0;
        score += clamp(bonusWeakScore, -6, 14);

        let middleScore = 0;
        middleScore += scoreAtMost(recentSevenNetTotal, [
          { maximum: -3500, points: 8 },
          { maximum: -2500, points: 6 },
          { maximum: -1500, points: 4 },
        ]);
        middleScore -= recentSevenNetTotal >= 1500 ? 5 : 0;
        middleScore += scoreAtMost(recentFourteenNetTotal, [
          { maximum: -4500, points: 6 },
          { maximum: -3000, points: 5 },
          { maximum: -1500, points: 3 },
        ]);
        middleScore -= recentFourteenNetTotal >= 2500 ? 5 : 0;
        middleScore += scoreAtMost(recentTwentyOneNetTotal, [
          { maximum: -5500, points: 5 },
          { maximum: -3500, points: 4 },
          { maximum: -1500, points: 2 },
        ]);
        middleScore -= recentTwentyOneNetTotal >= 3000 ? 4 : 0;
        middleScore += recentSevenAngle <= -120 ? 4 : recentSevenAngle <= -70 ? 2 : recentSevenAngle >= 80 ? -3 : 0;
        score += clamp(middleScore, -10, 18);

        if (!Number.isFinite(daysSinceMachineHighContent)) {
          score += 5;
        } else if (daysSinceMachineHighContent <= 3) {
          score -= 14;
        } else if (daysSinceMachineHighContent >= 4 && daysSinceMachineHighContent <= 10) {
          score += 6;
        } else if (daysSinceMachineHighContent >= 11 && daysSinceMachineHighContent <= 20) {
          score += 12;
        } else if (daysSinceMachineHighContent >= 21 && daysSinceMachineHighContent <= 35) {
          score += 8;
        } else if (daysSinceMachineHighContent >= 36) {
          score += 2;
        }
        score -= recentThreeMachineHighContentCount >= 1 ? 10 : recentSevenMachineHighContentCount >= 2 ? 5 : 0;

        if (recentSevenNetTotal < 0) {
          score +=
            adjacentMachineHighContentCount7 >= 2
              ? 6
              : adjacentMachineHighContentCount7 >= 1
                ? 3
                : adjacentMachineHighContentCount7Near2 >= 3
                  ? 2
                  : 0;
        }

        score += previousDifference <= -600 ? 2 : 0;
        score -= previousDifference >= 2500 ? 18 : previousDifference >= 2000 ? 15 : previousDifference >= 1500 ? 10 : 0;
        score -= recentThreeNetTotal >= 1500 ? 8 : 0;
        score -= recentSevenNetTotal >= 2500 ? 6 : 0;
        score -= previousMachineHighContent ? 8 : 0;
        score -=
          recentThreeCombinedDenominator <= 140 || recentThreeRbDenominator <= 300
            ? 10
            : recentThreeCombinedDenominator <= 150 || recentThreeRbDenominator <= 330
              ? 7.5
              : 0;
        score -= daysSinceMachineHighContent >= 36 && recentTwentyOneNetTotal > -1500 ? 5 : 0;

        const mainCore =
          streak >= 2 &&
          recentThreeGamesTotal >= 3000 &&
          recentThreeGamesTotal <= 6000 &&
          recentThreeRbDenominator >= 500 &&
          Number.isFinite(daysSinceMachineHighContent) &&
          daysSinceMachineHighContent >= 11 &&
          daysSinceMachineHighContent <= 20;
        score += mainCore ? 10 : 0;

        return Math.round(clamp(score, 0, 100));
      }

      if (activeLogicKey === "beam-hikari-funky-event") {
        score += scoreInRange(recentThreeGamesTotal, 3000, 6000, 14);
        score += scoreInRange(recentThreeGamesTotal, 2000, 2999, 7);
        score += scoreInRange(recentThreeGamesTotal, 6001, 7500, 7);
        score += scoreInRange(recentThreeGamesTotal, 7501, 9000, 2);
        score -= recentThreeGamesTotal < 1500 ? 5 : 0;
        score -= recentThreeGamesTotal > 12000 ? 6 : 0;

        score += streak >= 4 ? 15 : streak === 3 ? 14 : streak === 2 ? 12 : streak === 1 ? 3 : -6;

        score += scoreAtMost(recentThreeAngle, [
          { maximum: -300, points: 12 },
          { maximum: -200, points: 10 },
          { maximum: -100, points: 7 },
          { maximum: -30, points: 3 },
        ]);
        score -= recentThreeAngle >= 50 ? 4 : 0;

        let bonusWeakScore = 0;
        bonusWeakScore += scoreAtLeast(recentThreeCombinedDenominator, [
          { minimum: 205, points: 8 },
          { minimum: 185, points: 6 },
          { minimum: 170, points: 4 },
        ]);
        bonusWeakScore += scoreAtLeast(recentThreeRbDenominator, [
          { minimum: 610, points: 8 },
          { minimum: 500, points: 6 },
          { minimum: 430, points: 3 },
        ]);
        bonusWeakScore -= recentThreeCombinedDenominator <= 145 ? 4 : 0;
        bonusWeakScore -= recentThreeRbDenominator <= 320 ? 3 : 0;
        score += clamp(bonusWeakScore, -6, 14);

        let middleScore = 0;
        middleScore += scoreAtMost(recentSevenNetTotal, [
          { maximum: -3500, points: 8 },
          { maximum: -2500, points: 6 },
          { maximum: -1500, points: 4 },
        ]);
        middleScore -= recentSevenNetTotal >= 1500 ? 5 : 0;
        middleScore += scoreAtMost(recentFourteenNetTotal, [
          { maximum: -4500, points: 6 },
          { maximum: -3000, points: 5 },
          { maximum: -1500, points: 3 },
        ]);
        middleScore -= recentFourteenNetTotal >= 2500 ? 5 : 0;
        middleScore += scoreAtMost(recentTwentyOneNetTotal, [
          { maximum: -5500, points: 5 },
          { maximum: -3500, points: 4 },
          { maximum: -1500, points: 2 },
        ]);
        middleScore -= recentTwentyOneNetTotal >= 3000 ? 4 : 0;
        middleScore += recentSevenAngle <= -120 ? 4 : recentSevenAngle <= -70 ? 2 : recentSevenAngle >= 80 ? -3 : 0;
        score += clamp(middleScore, -10, 18);

        if (!Number.isFinite(daysSinceMachineHighContent)) {
          score += 5;
        } else if (daysSinceMachineHighContent <= 3) {
          score -= 14;
        } else if (daysSinceMachineHighContent >= 4 && daysSinceMachineHighContent <= 10) {
          score += 6;
        } else if (daysSinceMachineHighContent >= 11 && daysSinceMachineHighContent <= 20) {
          score += 12;
        } else if (daysSinceMachineHighContent >= 21 && daysSinceMachineHighContent <= 35) {
          score += 8;
        } else if (daysSinceMachineHighContent >= 36) {
          score += 2;
        }
        score -= recentThreeMachineHighContentCount >= 1 ? 10 : recentSevenMachineHighContentCount >= 2 ? 5 : 0;

        if (previousDifference < 0) {
          score +=
            previousAdjacentMachineHighContentCount >= 2
              ? 7
              : previousAdjacentMachineNetTotal >= 1500
                ? 4.9
                : previousAdjacentMachineHighContentCount >= 1
                  ? 2.1
                  : 0;
        }

        score += previousDifference <= -600 ? 2 : 0;
        score -= previousDifference >= 2500 ? 18 : previousDifference >= 2000 ? 15 : previousDifference >= 1500 ? 10 : 0;
        score -= recentThreeNetTotal >= 1500 ? 8 : 0;
        score -= recentSevenNetTotal >= 2500 ? 6 : 0;
        score -= previousMachineHighContent ? 8 : 0;
        score -=
          recentThreeCombinedDenominator <= 140 || recentThreeRbDenominator <= 300
            ? 10
            : recentThreeCombinedDenominator <= 150 || recentThreeRbDenominator <= 330
              ? 7.5
              : 0;

        const eventCore =
          streak >= 2 &&
          recentThreeGamesTotal >= 3000 &&
          recentThreeGamesTotal <= 6000 &&
          recentThreeRbDenominator >= 500 &&
          Number.isFinite(daysSinceMachineHighContent) &&
          daysSinceMachineHighContent >= 11 &&
          daysSinceMachineHighContent <= 20;
        score += eventCore ? 10 : 0;

        return Math.round(clamp(score, 0, 100));
      }

      score += scoreAtMost(recentThreeAngle, [
        { maximum: -300, points: 13 },
        { maximum: -200, points: 11 },
        { maximum: -100, points: 8 },
        { maximum: -30, points: 3 },
      ]);
      score -= recentThreeAngle >= 50 ? 5 : 0;

      score += scoreInRange(recentThreeGamesTotal, 3000, 6000, 15);
      score += scoreInRange(recentThreeGamesTotal, 2000, 2999, 7);
      score += scoreInRange(recentThreeGamesTotal, 6001, 7500, 7);
      score += scoreInRange(recentThreeGamesTotal, 7501, 9000, 2);
      score -= recentThreeGamesTotal < 1500 ? 6 : 0;
      score -= recentThreeGamesTotal > 12000 ? 7 : 0;

      score += streak >= 4 ? 16 : streak === 3 ? 15 : streak === 2 ? 13 : streak === 1 ? 3 : -7;

      let bonusWeakScore = 0;
      bonusWeakScore += scoreAtLeast(recentThreeCombinedDenominator, [
        { minimum: 205, points: 8 },
        { minimum: 185, points: 6 },
        { minimum: 170, points: 4 },
      ]);
      bonusWeakScore += scoreAtLeast(recentThreeRbDenominator, [
        { minimum: 610, points: 8 },
        { minimum: 500, points: 6 },
        { minimum: 430, points: 3 },
      ]);
      bonusWeakScore -= recentThreeCombinedDenominator <= 145 ? 4 : 0;
      bonusWeakScore -= recentThreeRbDenominator <= 320 ? 3 : 0;
      score += clamp(bonusWeakScore, -6, 14);

      if (!Number.isFinite(daysSinceMachineHighContent)) {
        score += 4;
      } else if (daysSinceMachineHighContent <= 3) {
        score -= 14;
      } else if (daysSinceMachineHighContent >= 4 && daysSinceMachineHighContent <= 10) {
        score += 2;
      } else if (daysSinceMachineHighContent >= 11 && daysSinceMachineHighContent <= 20) {
        score += 5;
      } else if (daysSinceMachineHighContent >= 21 && daysSinceMachineHighContent <= 35) {
        score -= 1;
      } else if (daysSinceMachineHighContent >= 36) {
        score -= 2;
      }
      score -= recentThreeMachineHighContentCount >= 1 ? 11 : recentSevenMachineHighContentCount >= 2 ? 5 : 0;

      let middleScore = 0;
      middleScore += recentFiveAngle <= -179 ? 9 : recentFiveAngle <= -122 ? 7 : recentFiveAngle <= -78 ? 4 : 0;
      middleScore += scoreAtMost(recentFiveNetTotal, [
        { maximum: -3000, points: 6 },
        { maximum: -2000, points: 5 },
        { maximum: -1000, points: 2 },
      ]);
      middleScore += recentSevenAngle <= -147 ? 5 : recentSevenAngle <= -99 ? 3 : 0;
      middleScore += scoreAtMost(recentSevenNetTotal, [
        { maximum: -3500, points: 7 },
        { maximum: -2500, points: 5 },
        { maximum: -1500, points: 3 },
      ]);
      middleScore -= recentSevenNetTotal >= 1500 ? 5 : 0;
      middleScore -= recentFourteenNetTotal >= 2500 ? 5 : 0;
      score += clamp(middleScore, -12, 12);

      score += previousDifference <= -669 ? 5 : previousDifference <= -345 ? 3 : previousDifference < 0 ? 1 : 0;
      score +=
        previousCombinedDenominator >= 234 || previousRbDenominator >= 617
          ? 4
          : previousCombinedDenominator >= 184 || previousRbDenominator >= 452
            ? 2
            : 0;
      if (previousDifference < 0) {
        score += previousAdjacentMachineHighContentCount >= 2 ? 2 : previousAdjacentMachineNetTotal >= 1500 ? 1 : 0;
      }

      score -= recentThreeNetTotal >= 1500 ? 8 : 0;
      score -= previousDifference >= 2500 ? 18 : previousDifference >= 2000 ? 15 : previousDifference >= 1500 ? 10 : 0;
      score -= previousMachineStrongHighContent ? 18 : previousMachineHighContent ? 14 : 0;
      score -=
        recentThreeCombinedDenominator <= 140 || recentThreeRbDenominator <= 300
          ? 20
          : recentThreeCombinedDenominator <= 150 || recentThreeRbDenominator <= 330
            ? 14
            : 0;
      score -= recentThreeGamesTotal >= 10000 ? 12 : recentThreeGamesTotal >= 8000 ? 8 : recentThreeGamesTotal >= 6000 ? 4 : 0;
      score -= recentSevenMachineHighContentCount >= 3 ? 8 : recentSevenMachineHighContentCount >= 2 ? 5 : 0;

      const normalCore =
        streak >= 2 &&
        recentThreeGamesTotal >= 3000 &&
        recentThreeGamesTotal <= 6000 &&
        recentThreeRbDenominator >= 500 &&
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent >= 11 &&
        daysSinceMachineHighContent <= 20;
      score += normalCore ? 12 : 0;
      score += normalCore && recentSevenNetTotal <= -1500 ? 5 : 0;

      return Math.round(clamp(score, 0, 100));
    }

    if (activeLogicKey === "apark-yakatabaru-funky") {
      if (readNumber(metrics.historyRowCount) < 21) {
        return 0;
      }

      let shortSinkScore = 0;
      shortSinkScore += scoreAtLeast(streak, [
        { minimum: 4, points: 24 },
        { minimum: 3, points: 18 },
        { minimum: 2, points: 10 },
      ]);
      shortSinkScore += scoreAtMost(features.recentThreeAngle, [
        { maximum: -180, points: 12 },
        { maximum: -90, points: 6 },
      ]);
      shortSinkScore += scoreAtMost(recentThreeNetTotal, [
        { maximum: -2350, points: 8 },
        { maximum: -1300, points: 4 },
      ]);
      shortSinkScore += scoreAtMost(recentFiveNetTotal, [
        { maximum: -3100, points: 6 },
        { maximum: -1600, points: 3 },
      ]);
      shortSinkScore += previousDifference <= -1000 ? 4 : 0;
      shortSinkScore = Math.min(shortSinkScore, 34);

      let middleSinkScore = 0;
      middleSinkScore += scoreAtMost(recentSevenNetTotal, [
        { maximum: -2500, points: 8 },
        { maximum: -2050, points: 6 },
        { maximum: 0, points: 3 },
      ]);
      middleSinkScore +=
        recentFourteenNetTotal >= -5300 && recentFourteenNetTotal <= -2780
          ? 8
          : recentFourteenNetTotal < -5300
            ? 5
            : recentFourteenNetTotal <= 0
              ? 3
              : 0;
      middleSinkScore += recentTwentyOneNetTotal <= 0 ? 4 : 0;
      middleSinkScore = Math.min(middleSinkScore, 18);

      let bonusWeakScore = 0;
      bonusWeakScore += scoreAtLeast(features.recentSevenCombinedDenominator, [
        { minimum: 166, points: 10 },
        { minimum: 161, points: 7 },
      ]);
      bonusWeakScore += scoreAtLeast(features.recentFourteenCombinedDenominator, [
        { minimum: 162, points: 6 },
        { minimum: 158, points: 4 },
      ]);
      bonusWeakScore += features.recentSevenCombinedDenominator >= 161 && streak >= 2 ? 4 : 0;
      bonusWeakScore = Math.min(bonusWeakScore, 16);

      let rotationScore = 0;
      if (Number.isFinite(daysSinceMachineHighContent)) {
        rotationScore += scoreInRange(daysSinceMachineHighContent, 5, 6, 9);
        rotationScore += scoreInRange(daysSinceMachineHighContent, 10, 14, 8);
        rotationScore += scoreInRange(daysSinceMachineHighContent, 15, 21, 4);
        rotationScore += scoreInRange(daysSinceMachineHighContent, 3, 4, 3);
      }
      rotationScore +=
        recentTwentyOneMachineHighContentCount >= 1 && recentTwentyOneMachineHighContentCount <= 2
          ? 5
          : recentTwentyOneMachineHighContentCount === 0
            ? 2
            : recentTwentyOneMachineHighContentCount >= 5
              ? 2
              : 0;
      rotationScore += recentFourteenMachineHighContentCount <= 1 ? 3 : 0;
      rotationScore = Math.min(rotationScore, 15);

      const gamesTrustScore = Math.min(
        9,
        (recentThreeGamesTotal >= 10000 ? 3 : 0) +
          (recentSevenGamesTotal >= 26600 ? 3 : 0) +
          (recentFourteenGamesTotal >= 55100 ? 3 : 0),
      );

      const ownSinkForNearby = recentThreeNetTotal <= -1300;
      const nearbyScore = Math.min(
        8,
        (otherSameMachineHighContentCount7 >= 8 ? 5 : 0) +
          (ownSinkForNearby && otherSameMachineHighContentCount7 >= 6 ? 3 : 0) +
          (ownSinkForNearby && adjacentMachineHighContentCount7Near2 >= 4 ? 2 : 0),
      );

      let dangerScore = 0;
      if (Number.isFinite(daysSinceMachineHighContent)) {
        dangerScore += daysSinceMachineHighContent <= 1 ? 10 : daysSinceMachineHighContent <= 2 ? 4 : 0;
      }
      dangerScore += previousMachineHighContent ? 5 : 0;
      dangerScore += previousMachineHighContent && previousDifference >= 1500 ? 8 : 0;
      dangerScore += previousDifference >= 2200 ? 10 : 0;
      dangerScore += features.previousBigShow ? 4 : 0;
      dangerScore += winningStreak >= 3 ? 12 : winningStreak >= 2 ? 8 : 0;
      dangerScore += features.recentThreeAngle >= 146 ? 10 : 0;
      dangerScore += recentThreeNetTotal >= 2400 ? 8 : 0;
      dangerScore += recentSevenNetTotal >= 3430 ? 6 : 0;
      dangerScore += recentFourteenNetTotal >= 5560 ? 4 : 0;
      dangerScore +=
        recentTwentyOneMachineHighContentCount >= 3 && recentTwentyOneMachineHighContentCount <= 4
          ? 4
          : 0;

      return Math.round(
        clamp(
          shortSinkScore +
            middleSinkScore +
            bonusWeakScore +
            rotationScore +
            gamesTrustScore +
            nearbyScore -
            dangerScore,
          0,
          100,
        ),
      );
    }

    if (activeLogicKey === "mj-kurume-funky") {
      if (readNumber(metrics.historyRowCount) < 21) {
        return 0;
      }

      const recentTenBonusTotal = readNumber(metrics.recentTenBonusTotal);
      const recentTenCombinedDenominator = rateDenominator(recentTenGamesTotal, recentTenBonusTotal);
      const recentTwentyOneMachineHighContentCount = readNumber(metrics.recentTwentyOneMachineHighContentCount);

      const sinkScore = Math.min(
        35,
        Math.max(
          recentTwentyOneNetTotal <= -5000 ? 20 : 0,
          recentTwentyOneNetTotal <= -4000 ? 17 : 0,
          recentTwentyOneNetTotal <= -3000 ? 13 : 0,
          recentTwentyOneNetTotal <= -2000 ? 8 : 0,
          recentTwentyOneNetTotal <= -1000 ? 4 : 0,
        ) +
          Math.max(
            recentFourteenNetTotal <= -4000 ? 10 : 0,
            recentFourteenNetTotal <= -3000 ? 7 : 0,
            recentFourteenNetTotal <= -2000 ? 4 : 0,
          ) +
          Math.max(
            recentSevenNetTotal <= -1800 ? 5 : 0,
            recentSevenNetTotal <= -1000 ? 3 : 0,
          ),
      );

      const angleScore = Math.min(
        20,
        Math.max(
          features.recentTwentyOneAngle <= -100 ? 12 : 0,
          features.recentTwentyOneAngle <= -80 ? 10 : 0,
          features.recentTwentyOneAngle <= -60 ? 7 : 0,
          features.recentTwentyOneAngle <= -40 ? 4 : 0,
        ) +
          Math.max(
            features.recentFourteenAngle <= -120 ? 8 : 0,
            features.recentFourteenAngle <= -80 ? 6 : 0,
            features.recentFourteenAngle <= -60 ? 4 : 0,
          ),
      );

      const bonusWeakScore = Math.max(
        features.recentFourteenCombinedDenominator >= 170 &&
          features.recentFourteenRbDenominator >= 400
          ? 10
          : 0,
        recentTenCombinedDenominator >= 180 ? 7 : 0,
        features.recentSevenCombinedDenominator >= 190 ? 5 : 0,
      );

      const streakBaseScore =
        streak === 2 ? 3 :
        streak === 3 ? 5 :
        streak >= 4 && streak <= 5 ? 6 :
        0;
      let restScore = 0;
      if (Number.isFinite(daysSinceMachineHighContent)) {
        restScore += daysSinceMachineHighContent >= 3 && daysSinceMachineHighContent <= 5 ? 2 : 0;
        restScore += daysSinceMachineHighContent >= 15 && daysSinceMachineHighContent <= 21 ? 2 : 0;
        restScore += daysSinceMachineHighContent >= 36 ? 2 : 0;
      }
      const streakRestScore = Math.min(10, Math.max(streakBaseScore, restScore));

      const nearbyScore = Math.min(
        10,
        (adjacentMachineBigWin1000Count7Near2 >= 2 && recentFourteenNetTotal <= -1000 ? 7 : 0) +
          (adjacentMachineNetTotal14 > 0 && recentTwentyOneNetTotal <= -2000 ? 3 : 0),
      );

      const gamesTrustScore = Math.min(
        10,
        (recentTwentyOneGamesTotal >= 42000 ? 4 : 0) +
          (recentFourteenGamesTotal >= 28000 ? 3 : 0) +
          (recentSevenGamesTotal >= 14000 ? 3 : 0),
      );

      const dangerScore =
        (recentFourteenNetTotal >= 4000 ? 10 : 0) +
        (recentTwentyOneNetTotal >= 4000 ? 8 : 0) +
        (recentSevenNetTotal >= 3000 ? 6 : 0) +
        (streak >= 6 ? 5 : 0) +
        (recentTwentyOneMachineHighContentCount >= 6 ? 5 : 0);

      return Math.round(
        clamp(
          sinkScore +
            angleScore +
            bonusWeakScore +
            streakRestScore +
            nearbyScore +
            gamesTrustScore -
            dangerScore,
          0,
          100,
        ),
      );
    }

    if (readNumber(metrics.historyRowCount) < 28) {
      return 0;
    }

    const sinkDepthScore = Math.max(
      recentFiveGamesTotal >= 18000 && recentFiveNetTotal <= -4000 ? 20 : 0,
      recentFiveGamesTotal >= 15000 && recentFiveNetTotal <= -3000 ? 17 : 0,
      recentSevenGamesTotal >= 25000 && recentSevenNetTotal <= -4000 ? 18 : 0,
      recentSevenGamesTotal >= 25000 && recentSevenNetTotal <= -3000 ? 16 : 0,
      recentSevenGamesTotal >= 22000 && recentSevenNetTotal <= -2000 ? 12 : 0,
      recentThreeGamesTotal >= 9000 && recentThreeNetTotal <= -1500 ? 9 : 0,
      recentThreeGamesTotal >= 9000 && recentThreeNetTotal <= -1000 ? 6 : 0,
    );
    const sinkStayScore = Math.max(
      recentFiveMinus3000StayDays >= 2 || recentSevenMinus3000StayDays >= 2 ? 8 : 0,
      recentFiveMinus2000StayDays >= 2 || recentSevenMinus2000StayDays >= 3 ? 6 : 0,
      recentFiveMinus1500StayDays >= 2 || recentSevenMinus1500StayDays >= 2 ? 4 : 0,
    );
    const sinkScore = Math.min(28, sinkDepthScore + sinkStayScore);

    const angleScore = Math.min(
      14,
      Math.max(
        recentFiveGamesTotal >= 15000 && features.recentFiveAngle <= -150 ? 12 : 0,
        recentFiveGamesTotal >= 15000 && features.recentFiveAngle > -150 && features.recentFiveAngle <= -100 ? 10 : 0,
        recentFiveGamesTotal >= 15000 && features.recentFiveAngle > -100 && features.recentFiveAngle <= -50 ? 7 : 0,
        recentSevenGamesTotal >= 22000 && features.recentSevenAngle > -150 && features.recentSevenAngle <= -100 ? 12 : 0,
        recentSevenGamesTotal >= 22000 && features.recentSevenAngle > -100 && features.recentSevenAngle <= -50 ? 10 : 0,
        recentSevenGamesTotal >= 22000 && features.recentSevenAngle > -50 && features.recentSevenAngle < 0 ? 4 : 0,
        recentSevenGamesTotal >= 22000 && features.recentSevenAngle <= -150 ? 10 : 0,
      ),
    );

    const streakScore =
      streak === 0 ? -3 :
      streak === 1 ? 0 :
      streak === 2 ? 8 :
      streak === 3 ? 14 :
      streak === 4 ? 18 :
      streak >= 5 && streak <= 6 ? 22 :
      streak >= 7 && recentSevenGamesTotal >= 25000 ? 12 :
      streak >= 7 ? 5 :
      0;

    let bonusWeakScore = 0;
    bonusWeakScore +=
      recentThreeGamesTotal >= 9000 && features.recentThreeCombinedDenominator > 190 ? 12 :
      recentThreeGamesTotal >= 9000 && features.recentThreeCombinedDenominator > 160 ? 9 :
      0;
    bonusWeakScore +=
      recentSevenGamesTotal >= 21000 && features.recentSevenCombinedDenominator > 190 ? 12 :
      recentSevenGamesTotal >= 21000 && features.recentSevenCombinedDenominator > 160 ? 10 :
      0;
    bonusWeakScore += recentFourteenGamesTotal >= 45000 && features.recentFourteenCombinedDenominator > 160 ? 7 : 0;
    bonusWeakScore += recentThreeGamesTotal >= 9000 && features.recentThreeRbDenominator > 550 ? 2 : 0;
    bonusWeakScore += recentSevenGamesTotal >= 21000 && features.recentSevenRbDenominator > 400 ? 2 : 0;
    bonusWeakScore = Math.min(14, bonusWeakScore);

    let rotationScore = 0;
    if (!Number.isFinite(daysSinceMachineHighContent)) {
      rotationScore += 6;
    } else if (daysSinceMachineHighContent <= 1) {
      rotationScore -= 8;
    } else if (daysSinceMachineHighContent <= 7) {
      rotationScore += 0;
    } else if (daysSinceMachineHighContent <= 10) {
      rotationScore += 9;
    } else if (daysSinceMachineHighContent <= 14) {
      rotationScore += 14;
    } else if (daysSinceMachineHighContent <= 21) {
      rotationScore += 7;
    } else if (daysSinceMachineHighContent <= 42) {
      rotationScore += 5;
    }
    rotationScore -= recentSevenMachineHighContentCount >= 2 ? 8 : 0;
    rotationScore -= recentFourteenMachineHighContentCount >= 3 ? 4 : 0;

    let gamesTrustScore = 0;
    gamesTrustScore +=
      recentSevenGamesTotal >= 25000 && recentSevenGamesTotal <= 40000 ? 4 :
      (recentSevenGamesTotal >= 18000 && recentSevenGamesTotal < 25000) ||
        (recentSevenGamesTotal > 40000 && recentSevenGamesTotal <= 50000) ? 2 :
      0;
    gamesTrustScore +=
      recentFourteenGamesTotal >= 50000 && recentFourteenGamesTotal <= 85000 ? 3 :
      recentFourteenGamesTotal > 85000 ? 2 :
      0;
    gamesTrustScore += previousGames >= 3000 && previousGames <= 8000 ? 1 : 0;
    gamesTrustScore -= previousGames < 1000 ? 8 : 0;
    gamesTrustScore -= recentThreeGamesTotal < 9000 ? 5 : 0;
    gamesTrustScore -= recentSevenGamesTotal < 18000 ? 5 : 0;

    const previousBroadFailScore =
      previousMachineGoodContent && previousDifference <= 0 ? 8 :
      previousMachineGoodContent && previousDifference <= 1000 ? 2 :
      0;

    let dangerScore = 0;
    dangerScore +=
      previousDifference > 3000 ? 15 :
      previousDifference > 2000 ? 10 :
      previousDifference > 1000 ? 5 :
      0;
    dangerScore += recentThreeNetTotal > 3000 ? 8 : 0;
    dangerScore += recentSevenNetTotal > 3000 ? 12 : recentSevenNetTotal > 1000 ? 5 : 0;
    dangerScore += recentFourteenNetTotal > 5000 ? 8 : 0;
    dangerScore += recentTwentyEightNetTotal > 7000 ? 10 : 0;
    dangerScore +=
      recentSevenGamesTotal >= 21000 &&
      features.recentSevenCombinedDenominator <= 138 &&
      recentSevenNetTotal > 0
        ? 7
        : 0;
    dangerScore += previousMachineHighContent && previousDifference > 1000 ? 8 : 0;

    const restoredScore =
      sinkScore +
      angleScore +
      streakScore +
      bonusWeakScore +
      rotationScore +
      gamesTrustScore +
      previousBroadFailScore -
      dangerScore;

    return Math.round(clamp(restoredScore - 2, 0, 100));
  }

  if (machineKey === "happy" && activeLogicKey === "beam-hikari-happy") {
    if (targetRangeHistoryRowCount < 21) {
      return 0;
    }

    let previousWeakScore = 0;
    previousWeakScore +=
      previousGames >= 500 && previousGames <= 2999
        ? 10
        : previousGames >= 300 && previousGames <= 499
          ? 3
          : 0;
    previousWeakScore +=
      previousDifference <= -1200
        ? 3
        : previousDifference <= -500
          ? 8
          : previousDifference < 0
            ? 5
            : 0;
    previousWeakScore +=
      previousCombinedDenominator > 200
        ? 6
        : previousCombinedDenominator > 170
          ? 4
          : 0;
    previousWeakScore += previousRbDenominator > 700 ? 3 : previousRbDenominator > 500 ? 2 : 0;

    let unpaidScore = scoreAtMost(recentTwentyOneNetTotal, [
      { maximum: -8000, points: 19 },
      { maximum: -5000, points: 13 },
      { maximum: -2500, points: 7 },
      { maximum: -1, points: 3 },
    ]);
    if (recentTwentyOneGamesTotal < 48000) {
      unpaidScore = Math.min(unpaidScore, 11);
    }

    const unfinishedScore = Math.min(
      18,
      (recentFourteenGoldShowDays === 0 ? 6 : 0) +
        (recentSevenGoldShowDays === 0 ? 5 : 0) +
        (recentSevenNetTotal <= 1500 ? 4 : 0) +
        (previousDifference < 1000 ? 3 : 0),
    );

    const gamesTrustScore = Math.min(
      19,
      scoreInRange(recentSevenGamesTotal, 8000, 20000, 10) +
        scoreInRange(recentSevenGamesTotal, 20001, 28000, 5) +
        scoreInRange(recentFourteenGamesTotal, 25000, 45000, 9) +
        scoreInRange(recentFourteenGamesTotal, 45001, 60000, 5),
    );

    const losingScore = Math.min(
      9,
      (streak >= 5 ? 6 : streak >= 3 ? 4 : streak >= 2 ? 3 : 0) +
        (recentSevenLossDays >= 6 ? 3 : recentSevenLossDays >= 5 ? 2 : 0) +
        (recentFourteenLossDays >= 11 ? 2 : recentFourteenLossDays >= 10 ? 1 : 0),
    );

    const angleScore = Math.min(
      6,
      scoreAtMost(recentFourteenNetTotal, [
        { maximum: -6000, points: 4 },
        { maximum: -3500, points: 3 },
        { maximum: -1500, points: 2 },
        { maximum: -1, points: 1 },
      ]) +
        (features.recentFourteenAngle <= -180 ? 2 : features.recentFourteenAngle <= -120 ? 1 : 0),
    );

    let intervalScore = 0;
    if (Number.isFinite(daysSinceMachineHighContent)) {
      intervalScore +=
        daysSinceMachineHighContent >= 10 && daysSinceMachineHighContent <= 21
          ? 1.5
          : (daysSinceMachineHighContent >= 7 && daysSinceMachineHighContent < 10) ||
              (daysSinceMachineHighContent > 21 && daysSinceMachineHighContent <= 30)
            ? 1
            : 0;
      intervalScore += daysSinceMachineHighContent >= 21 && daysSinceMachineHighContent <= 60 ? 0.5 : 0;
    }
    intervalScore = Math.min(intervalScore, 2);

    const penalty = Math.min(
      16,
      (previousMachineHighContent || previousMachineGoodContent ? 5 : 0) +
        (previousDifference >= 1500 || (previousGames >= 3000 && previousCombinedDenominator <= 150) ? 5 : 0) +
        (recentTwentyOneNetTotal > 4000 ? 4 : 0) +
        (recentSevenNetTotal > 1500 ? 3 : 0) +
        (recentSevenGamesTotal < 8000 ? 3 : 0),
    );

    return Math.round(
      clamp(
        Math.min(previousWeakScore, 27) +
          unpaidScore +
          unfinishedScore +
          gamesTrustScore +
          losingScore +
          angleScore +
          intervalScore -
          penalty,
        0,
        100,
      ),
    );
  }

  if (machineKey === "happy" && activeLogicKey === "apark-yakatabaru-happy") {
    if (readNumber(metrics.historyRowCount) < 21) {
      return 0;
    }

    let rawScore = 0;
    rawScore += scoreAtLeast(streak, [
      { minimum: 5, points: 28 },
      { minimum: 4, points: 23 },
      { minimum: 3, points: 17 },
      { minimum: 2, points: 8 },
      { minimum: 1, points: 1 },
    ]);
    rawScore += scoreAtMost(recentSevenNetTotal, [
      { maximum: -3000, points: 25 },
      { maximum: -2500, points: 22 },
      { maximum: -2000, points: 18 },
      { maximum: -1500, points: 13 },
      { maximum: -1000, points: 8 },
      { maximum: -1, points: 3 },
    ]);
    rawScore += scoreAtMost(recentTwentyOneNetTotal, [
      { maximum: -5000, points: 12 },
      { maximum: -3000, points: 9 },
      { maximum: -1500, points: 5 },
      { maximum: 0, points: 2 },
    ]);
    if (Number.isFinite(daysSinceMachineHighContent)) {
      if (daysSinceMachineHighContent === 1) {
        rawScore -= 32;
      } else if (daysSinceMachineHighContent >= 2 && daysSinceMachineHighContent <= 4) {
        rawScore += 3;
      } else if (daysSinceMachineHighContent >= 5 && daysSinceMachineHighContent <= 14) {
        rawScore += 12;
      } else if (daysSinceMachineHighContent >= 15 && daysSinceMachineHighContent <= 21) {
        rawScore += 8;
      }
    }
    rawScore += scoreAtLeast(features.recentSevenCombinedDenominator, [
      { minimum: 160, points: 7 },
      { minimum: 153, points: 4 },
      { minimum: 148, points: 2 },
    ]);
    rawScore +=
      recentSevenGamesTotal >= 25000 && recentSevenGamesTotal <= 32000
        ? 9
        : recentSevenGamesTotal >= 20000 && recentSevenGamesTotal <= 24999
          ? 7
          : recentSevenGamesTotal >= 32001 && recentSevenGamesTotal <= 36000
            ? 4
            : recentSevenGamesTotal >= 36001
              ? 1
              : -5;
    rawScore +=
      previousDifference >= 1500
        ? -8
        : previousDifference >= 800
          ? -4
          : previousDifference <= -800
            ? 5
            : previousDifference < 0
              ? 3
              : 0;
    rawScore -= recentSevenNetTotal >= 2500 ? 12 : 0;
    rawScore -= recentFourteenNetTotal >= 3500 ? 10 : 0;
    rawScore -= recentFourteenMachineHighContentCount >= 4 ? 6 : 0;

    return Math.round(clamp((rawScore / 98) * 100, 0, 100));
  }

  if (machineKey === "ultra-miracle" && activeLogicKey === "apark-yakatabaru-ultra-miracle") {
    if (readNumber(metrics.historyRowCount) < 21) {
      return 0;
    }

    let rawScore = 0;
    rawScore += scoreAtLeast(streak, [
      { minimum: 6, points: 45 },
      { minimum: 5, points: 38 },
      { minimum: 4, points: 30 },
      { minimum: 3, points: 22 },
      { minimum: 2, points: 12 },
      { minimum: 1, points: 5 },
    ]);

    let sinkAngleScore = 0;
    sinkAngleScore += scoreAtMost(recentThreeNetTotal, [
      { maximum: -1900, points: 7 },
      { maximum: -1300, points: 5 },
      { maximum: -500, points: 2 },
    ]);
    sinkAngleScore += scoreAtMost(recentFiveNetTotal, [
      { maximum: -2600, points: 5 },
      { maximum: -1600, points: 3 },
    ]);
    sinkAngleScore += scoreAtMost(features.recentThreeAngle, [
      { maximum: -225, points: 6 },
      { maximum: -130, points: 4 },
    ]);
    sinkAngleScore += scoreAtMost(features.recentSevenAngle, [
      { maximum: -135, points: 2 },
      { maximum: -80, points: 1 },
    ]);
    rawScore += Math.min(sinkAngleScore, 20);

    let activityScore = 0;
    activityScore +=
      recentThreeGamesTotal <= 6400 && streak >= 3
        ? 8
        : recentThreeGamesTotal <= 8500 && streak >= 2
          ? 5
          : 0;
    activityScore += previousGames <= 1300 && streak >= 1 ? 4 : 0;
    activityScore += recentSevenGamesTotal <= 21000 ? 3 : recentSevenGamesTotal <= 30000 ? 2 : 0;
    rawScore += Math.min(activityScore, 15);

    let unpaidScore = 0;
    if (Number.isFinite(daysSinceMachineHighContent)) {
      if (daysSinceMachineHighContent >= 22 && daysSinceMachineHighContent <= 35) {
        unpaidScore += 7;
      } else if (daysSinceMachineHighContent >= 15 && daysSinceMachineHighContent <= 21) {
        unpaidScore += 6;
      } else if (daysSinceMachineHighContent >= 8 && daysSinceMachineHighContent <= 14) {
        unpaidScore += 4;
      } else if (daysSinceMachineHighContent >= 36) {
        unpaidScore += 4;
      }
    }
    unpaidScore += recentSevenMachineHighContentCount === 0 && recentSevenNetTotal <= 0 ? 3 : 0;
    unpaidScore +=
      recentFourteenMachineHighContentCount === 0 && recentFourteenNetTotal <= -1500 ? 3 : 0;
    unpaidScore +=
      recentTwentyOneMachineHighContentCount === 0 &&
      recentTwentyOneNetTotal <= -1500 &&
      recentTwentyOneNetTotal >= -9000
        ? 2
        : 0;
    rawScore += Math.min(unpaidScore, 15);

    let nearbyScore = 0;
    nearbyScore += adjacentMachineHighContentCount3 > 0 && recentThreeNetTotal <= 0 ? 2 : 0;
    nearbyScore += adjacentMachineBigWin1000Count7Near2 >= 2 && recentSevenNetTotal <= 0 ? 2 : 0;
    nearbyScore += adjacentMachineNetTotal7Near2 > 0 && recentSevenNetTotal < 0 ? 1 : 0;
    rawScore += Math.min(nearbyScore, 5);

    let treatmentCap = 100;
    const applyTreatmentCap = (capValue) => {
      treatmentCap = Math.min(treatmentCap, capValue);
    };
    if (previousMachineHighContent) {
      applyTreatmentCap(55);
    }
    if (previousMachineHighContent && previousDifference >= 1000) {
      applyTreatmentCap(45);
    }
    if (previousMachineHighContent && previousDifference >= 1800) {
      applyTreatmentCap(35);
    }
    if (previousMachineHighContent && previousDifference < 500) {
      applyTreatmentCap(45);
    }
    if (previousGames >= 3000 && previousCombinedDenominator <= 134) {
      applyTreatmentCap(60);
    }
    if (previousGames >= 3000 && previousCombinedDenominator <= 134 && previousDifference < 500) {
      applyTreatmentCap(50);
    }
    if (previousDifference >= 1275) {
      applyTreatmentCap(48);
    }
    if (previousDifference >= 800) {
      applyTreatmentCap(60);
    }
    if (previousGames >= 6200) {
      applyTreatmentCap(58);
    }
    if (previousGames >= 4900 && previousDifference > 0) {
      applyTreatmentCap(60);
    }
    if (recentThreeMachineHighContentCount >= 1) {
      applyTreatmentCap(60);
    }
    if (recentSevenMachineHighContentCount >= 1 && recentSevenNetTotal > 0) {
      applyTreatmentCap(65);
    }
    if (recentThreeNetTotal >= 1500) {
      applyTreatmentCap(60);
    }
    if (recentSevenNetTotal >= 2800) {
      applyTreatmentCap(55);
    }

    return Math.round(clamp(Math.min(rawScore, treatmentCap), 0, 100));
  }

  if (machineKey === "okidoki-black") {
    let sinkScore = 0;
    sinkScore += scoreAtMost(features.recentThreeAngle, [
      { maximum: -461, points: 8 },
      { maximum: -376, points: 5 },
    ]);
    sinkScore += scoreAtMost(features.recentFiveAngle, [
      { maximum: -293, points: 10 },
      { maximum: -249, points: 7 },
    ]);
    sinkScore += scoreAtMost(features.recentSevenAngle, [
      { maximum: -245, points: 7 },
      { maximum: -216, points: 4 },
    ]);
    sinkScore += scoreAtMost(recentFourteenNetTotal, [{ maximum: -11791, points: 4 }]);
    sinkScore += scoreAtMost(recentTwentyOneNetTotal, [
      { maximum: -17654, points: 6 },
      { maximum: -15108, points: 4 },
    ]);
    sinkScore = Math.min(sinkScore, 30);

    let activityScore = 0;
    activityScore += scoreAtLeast(recentFiveGamesTotal, [
      { minimum: 27220, points: 10 },
      { minimum: 25679, points: 7 },
    ]);
    activityScore += scoreAtLeast(recentSevenGamesTotal, [{ minimum: 35511, points: 5 }]);
    activityScore = Math.min(activityScore, 15);

    let bonusScore = 0;
    bonusScore += scoreAtLeast(features.recentThreeCombinedDenominator, [
      { minimum: 204.1, points: 10 },
      { minimum: 189.2, points: 7 },
    ]);
    bonusScore += scoreAtLeast(features.recentFiveCombinedDenominator, [
      { minimum: 179.2, points: 6 },
      { minimum: 173.9, points: 4 },
    ]);
    bonusScore += scoreAtLeast(features.recentThreeRbDenominator, [
      { minimum: 561.4, points: 2 },
      { minimum: 516.8, points: 1 },
    ]);
    bonusScore = Math.min(bonusScore, 18);

    let streakScore = scoreAtLeast(streak, [
      { minimum: 6, points: 10 },
      { minimum: 5, points: 8 },
      { minimum: 4, points: 5 },
    ]);
    if (streak === 2) {
      streakScore -= 3;
    }
    if (machineHighContentStreak >= 2) {
      streakScore += 12;
    } else if (recentThreeMachineHighContentCount >= 2) {
      streakScore += 5;
    } else if (recentFiveMachineHighContentCount >= 3) {
      streakScore += 3;
    }
    streakScore = clamp(streakScore, -3, 17);

    let comboScore = 0;
    comboScore += recentFiveGamesTotal >= 25679 && features.recentThreeAngle <= -461 ? 10 : 0;
    comboScore += recentFiveGamesTotal >= 25679 && features.recentThreeAngle <= -376 ? 6 : 0;
    comboScore += recentFiveGamesTotal >= 25679 && features.recentThreeCombinedDenominator >= 204.1 ? 10 : 0;
    comboScore += recentFiveGamesTotal >= 25679 && features.recentThreeCombinedDenominator >= 189.2 ? 6 : 0;
    comboScore +=
      recentFiveGamesTotal >= 25679 &&
      streak >= 4 &&
      (features.recentFiveAngle <= -249 || features.recentSevenAngle <= -216)
        ? 5
        : 0;
    comboScore += recentSevenGamesTotal >= 35511 && recentTwentyOneNetTotal <= -15108 ? 5 : 0;
    comboScore = Math.min(comboScore, 20);

    let penalty = 0;
    penalty += recentFiveGamesTotal <= 18667 ? 8 : 0;
    penalty +=
      recentFourteenMachineHighContentCount === 0 ||
      (Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 21)
        ? 12
        : 0;
    penalty += Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 13 && daysSinceMachineHighContent <= 20 ? 5 : 0;
    penalty += previousMachineHighContent && previousDifference >= 0 ? 8 : 0;
    penalty += scoreAtLeast(recentFiveNetTotal, [
      { minimum: 5558, points: 14 },
      { minimum: 4355, points: 8 },
    ]);
    penalty += scoreAtLeast(recentFourteenNetTotal, [
      { minimum: 7746, points: 10 },
      { minimum: 6097, points: 6 },
    ]);
    penalty += recentSevenMachineHighContentCount >= 2 && machineHighContentStreak < 2 ? 4 : 0;
    penalty += recentFiveNetTotal < 0 && recentFiveGamesTotal < 18667 && !features.strongAngle && !features.weakBonus ? 6 : 0;

    return Math.round(clamp(30 + sinkScore + activityScore + bonusScore + streakScore + comboScore - penalty, 0, 100));
  }

  if (machineKey === "shin-hanabi") {
    const previousStrictHighContent =
      previousGames >= 5000 &&
      previousCombinedDenominator <= 145 &&
      previousRbDenominator <= 315;

    let activityScore = 0;
    activityScore += previousGames >= 5000 ? 8 : 0;
    activityScore += previousGames >= 6500 ? 5 : 0;
    activityScore += readNumber(metrics.recentThreeGamesTotal) >= 15000 ? 6 : 0;
    activityScore += readNumber(metrics.recentThreeGamesTotal) >= 18000 ? 3 : 0;
    activityScore += recentSevenGamesTotal >= 30000 ? 5 : 0;
    activityScore = Math.min(activityScore, 24);

    let upScore = 0;
    upScore += readNumber(metrics.recentTwoNetTotal) >= 1000 ? 6 : 0;
    upScore += readNumber(metrics.recentTwoNetTotal) >= 2500 ? 4 : 0;
    upScore += recentThreeNetTotal >= 1500 ? 7 : 0;
    upScore += recentThreeNetTotal >= 3000 ? 5 : 0;
    upScore += recentFiveNetTotal >= 3000 ? 5 : 0;
    upScore += recentFiveNetTotal >= 4000 ? 4 : 0;
    upScore += features.recentThreeAngle >= 150 ? 6 : 0;
    upScore += features.recentSevenAngle >= 100 ? 3 : 0;
    upScore = Math.min(upScore, 35);

    let showScore = 0;
    showScore += features.recentSevenBigShowDays >= 1 ? 5 : 0;
    showScore += features.recentSevenBigShowDays >= 2 ? 5 : 0;
    showScore += features.recentSevenBigShowDays >= 3 ? 6 : 0;
    showScore += features.previousBigShow ? 4 : 0;
    showScore = Math.min(showScore, 20);

    let bonusScore = 0;
    bonusScore += previousStrictHighContent ? 7 : 0;
    bonusScore +=
      previousGames >= 4000 &&
      features.previousCombinedDenominator <= 145 &&
      features.previousRbDenominator <= 315
        ? 5
        : 0;
    bonusScore += recentThreeStrictHighContentDays >= 1 ? 5 : 0;
    bonusScore += recentSevenStrictHighContentDays >= 2 ? 4 : 0;
    bonusScore = Math.min(bonusScore, 16);

    let penalty = 0;
    penalty += previousGames < 2000 ? 8 : 0;
    penalty += readNumber(metrics.recentThreeGamesTotal) < 8000 ? 7 : 0;
    penalty += recentThreeNetTotal <= -2500 && features.recentThreeAngle <= -150 ? 10 : 0;
    penalty += streak >= 3 && readNumber(metrics.recentThreeGamesTotal) < 12000 ? 6 : 0;
    penalty += Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 60 && recentFourteenGamesTotal < 40000 ? 6 : 0;
    penalty += recentThreeNetTotal >= 4000 && recentThreeStrictHighContentDays === 0 && features.previousRbDenominator > 400 ? 5 : 0;
    penalty = Math.min(penalty, 20);

    return Math.round(clamp(activityScore + upScore + showScore + bonusScore - penalty, 0, 100));
  }

  if (machineKey === "hokuto-tensei") {
    if (activeLogicKey === "beam-hikari-hokuto-tensei") {
      if (targetRangeHistoryRowCount < 21) {
        return 0;
      }

      const sevenSinkStayProxy = readNumber(features.beamHikariHokutoSevenSinkStayProxy);
      const fourteenSinkStayProxy = readNumber(features.beamHikariHokutoFourteenSinkStayProxy);
      const leftBehindAmount = readNumber(features.beamHikariHokutoLeftBehindAmount);
      let score = 40;

      if (recentTwentyOneNetTotal < -20000) {
        score += 2;
      } else if (recentTwentyOneNetTotal < -12000) {
        score += 8;
      } else if (recentTwentyOneNetTotal <= -8000) {
        score += 18;
      } else if (recentTwentyOneNetTotal <= -4000) {
        score += 5;
      } else if (recentTwentyOneNetTotal >= 12000) {
        score -= 10;
      } else if (recentTwentyOneNetTotal >= 6000) {
        score -= 5;
      }

      if (recentSevenNetTotal < -10000) {
        score += 8;
      } else if (recentSevenNetTotal >= 0 && recentSevenNetTotal < 3000) {
        score += 7;
      } else if (recentSevenNetTotal >= 3000) {
        score -= 5;
      }

      if (features.recentFourteenAngle < -250) {
        score -= 4;
      } else if (features.recentFourteenAngle < -120) {
        score += 8;
      } else if (features.recentFourteenAngle < 50) {
        score += 10;
      } else if (features.recentFourteenAngle < 180) {
        score += 3;
      } else {
        score -= 10;
      }

      if (sevenSinkStayProxy === 2) {
        score += 2;
      } else if (sevenSinkStayProxy >= 3 && sevenSinkStayProxy <= 5) {
        score += 16;
      } else if (sevenSinkStayProxy >= 6) {
        score -= 12;
      }
      if (fourteenSinkStayProxy >= 4 && fourteenSinkStayProxy <= 5) {
        score += 6;
      } else if (fourteenSinkStayProxy >= 6) {
        score -= 8;
      }

      if (streak === 2) {
        score += 4;
      } else if (streak === 3) {
        score += 12;
      } else if (streak === 4) {
        score += 8;
      } else if (streak >= 5 && streak <= 6) {
        score -= 12;
      } else if (streak >= 8) {
        score -= 4;
      }

      if (!Number.isFinite(daysSinceMachineHighContent)) {
        score += 4;
      } else if (daysSinceMachineHighContent === 1) {
        score -= 14;
      } else if (daysSinceMachineHighContent >= 3 && daysSinceMachineHighContent <= 4) {
        score += 8;
      } else if (daysSinceMachineHighContent >= 5 && daysSinceMachineHighContent <= 6) {
        score -= 6;
      } else if (daysSinceMachineHighContent >= 9 && daysSinceMachineHighContent <= 10) {
        score += 3;
      } else if (daysSinceMachineHighContent > 20) {
        score += 4;
      }

      if (previousGames < 1000) {
        score -= 4;
      } else if (previousGames >= 3000 && previousGames < 5000) {
        score += 9;
      } else if (previousGames >= 5000 && previousGames < 7000) {
        score += 1;
      } else if (previousGames >= 7000 && previousGames < 9000) {
        score -= 8;
      } else if (previousGames >= 9000) {
        score -= 16;
      }

      if (previousDifference >= -5000 && previousDifference < -3000) {
        score += 5;
      } else if (previousDifference >= -3000 && previousDifference < -1500) {
        score -= 4;
      } else if (previousDifference >= -1500 && previousDifference < 0) {
        score += 7;
      } else if (previousDifference >= 0 && previousDifference < 1500) {
        score += 3;
      } else if (previousDifference >= 1500 && previousDifference < 4000) {
        score -= 5;
      } else if (previousDifference >= 4000 && previousDifference < 8000) {
        score -= 7;
      } else if (previousDifference >= 8000) {
        score -= 16;
      }

      score -= previousMachineHighContent ? 12 : 0;
      if (leftBehindAmount < -10000) {
        score += 5;
      } else if (leftBehindAmount < -5000) {
        score += 7;
      } else if (leftBehindAmount >= 9000) {
        score -= 6;
      }
      score += adjacentMachineHighContentCount7Near2 >= 2 && adjacentMachineHighContentCount7Near2 <= 5 ? 3 : 0;
      score -= adjacentMachineHighContentCount7Near2 === 0 ? 3 : 0;
      score += recentFourteenMachineHighContentCount === 0 ? 6 : 0;
      score -= recentFourteenMachineHighContentCount >= 5 ? 8 : 0;
      score -= recentFourteenGamesTotal < 50000 ? 8 : 0;
      score += recentFourteenGamesTotal >= 70000 && recentFourteenGamesTotal <= 90000 ? 3 : 0;

      return Math.round(clamp(score, 0, 100));
    }

    if (
      activeLogicKey === "beam-hikari-hokuto-tensei-normal" ||
      activeLogicKey === "beam-hikari-hokuto-tensei-event"
    ) {
      if (targetRangeHistoryRowCount < 21) {
        return 0;
      }

      const sevenSinkStayProxy = readNumber(features.beamHikariHokutoSevenSinkStayProxy);
      const fourteenSinkStayProxy = readNumber(features.beamHikariHokutoFourteenSinkStayProxy);
      const leftBehindAmount = readNumber(features.beamHikariHokutoLeftBehindAmount);
      let score = 40;

      if (activeLogicKey === "beam-hikari-hokuto-tensei-event") {
        if (previousDifference < -5000) {
          score += 12;
        } else if (previousDifference < -2000) {
          score += 8;
        } else if (previousDifference < 0) {
          score += 4;
        } else if (previousDifference < 2000) {
          score += 2;
        } else if (previousDifference < 5000) {
          score -= 2;
        } else {
          score -= 8;
        }

        if (recentTwentyOneNetTotal < -20000) {
          score += 14;
        } else if (recentTwentyOneNetTotal < -12000) {
          score += 8;
        } else if (recentTwentyOneNetTotal < -8000) {
          score += 7;
        } else if (recentTwentyOneNetTotal < -4000) {
          score += 10;
        } else if (recentTwentyOneNetTotal < 0) {
          score += 4;
        } else if (recentTwentyOneNetTotal < 6000) {
          score -= 7;
        } else if (recentTwentyOneNetTotal >= 12000) {
          score -= 5;
        }

        if (recentSevenNetTotal < -10000) {
          score += 15;
        } else if (recentSevenNetTotal < -5000) {
          score += 6;
        } else if (recentSevenNetTotal < 0) {
          score += 6;
        } else if (recentSevenNetTotal < 3000) {
          score += 5;
        } else if (recentSevenNetTotal < 8000) {
          score -= 4;
        } else {
          score -= 8;
        }

        if (features.recentFourteenAngle >= -250 && features.recentFourteenAngle < -120) {
          score += 7;
        } else if (features.recentFourteenAngle >= -120 && features.recentFourteenAngle < 50) {
          score += 3;
        } else if (features.recentFourteenAngle >= 50 && features.recentFourteenAngle < 180) {
          score += 7;
        } else if (features.recentFourteenAngle >= 180) {
          score -= 8;
        }

        if (sevenSinkStayProxy === 1) {
          score += 2;
        } else if (sevenSinkStayProxy === 2) {
          score -= 10;
        } else if (sevenSinkStayProxy >= 3 && sevenSinkStayProxy <= 5) {
          score += 14;
        } else if (sevenSinkStayProxy >= 6) {
          score -= 10;
        }
        if (fourteenSinkStayProxy >= 4 && fourteenSinkStayProxy <= 5) {
          score += 3;
        } else if (fourteenSinkStayProxy >= 6) {
          score -= 6;
        }

        if (streak === 1) {
          score += 4;
        } else if (streak === 2) {
          score += 6;
        } else if (streak === 3) {
          score += 2;
        } else if (streak === 4) {
          score += 14;
        } else if (streak >= 5 && streak <= 6) {
          score -= 16;
        } else if (streak >= 7) {
          score += 2;
        }

        if (Number.isFinite(daysSinceMachineHighContent)) {
          if (daysSinceMachineHighContent === 1) {
            score -= 12;
          } else if (daysSinceMachineHighContent >= 3 && daysSinceMachineHighContent <= 4) {
            score += 5;
          } else if (daysSinceMachineHighContent >= 5 && daysSinceMachineHighContent <= 6) {
            score += 2;
          } else if (daysSinceMachineHighContent >= 7) {
            score += 6;
          }
        }

        if (previousGames < 1000) {
          score += 3;
        } else if (previousGames < 3000) {
          score += 8;
        } else if (previousGames < 5000) {
          score += 3;
        } else if (previousGames >= 7000) {
          score -= 12;
        }

        if (previousDifference < -5000) {
          score -= 8;
        } else if (previousDifference < -3000) {
          score += 3;
        } else if (previousDifference < -1500) {
          score -= 3;
        } else if (previousDifference < 0) {
          score += 8;
        } else if (previousDifference < 4000) {
          score += 2;
        } else {
          score -= 10;
        }

        score -= previousMachineHighContent ? 10 : 0;
        if (leftBehindAmount < -10000) {
          score += 8;
        } else if (leftBehindAmount < -5000) {
          score += 5;
        } else if (leftBehindAmount >= 9000) {
          score -= 6;
        }
        score += recentFourteenMachineHighContentCount === 0 ? 8 : 0;
        score += recentFourteenMachineHighContentCount >= 2 && recentFourteenMachineHighContentCount <= 3 ? -3 : 0;
        score += recentFourteenMachineHighContentCount >= 4 ? 5 : 0;
        score += recentFourteenGamesTotal < 50000 ? -5 : 0;
        score += recentFourteenGamesTotal >= 70000 && recentFourteenGamesTotal <= 90000 ? 2 : 0;

        return Math.round(clamp(score, 0, 100));
      }

      if (recentTwentyOneNetTotal < -20000) {
        score -= 8;
      } else if (recentTwentyOneNetTotal < -12000) {
        score += 5;
      } else if (recentTwentyOneNetTotal < -8000) {
        score += 18;
      } else if (recentTwentyOneNetTotal < -4000) {
        score -= 4;
      } else if (recentTwentyOneNetTotal >= 0 && recentTwentyOneNetTotal < 6000) {
        score += 5;
      } else if (recentTwentyOneNetTotal < 12000) {
        score -= 5;
      } else {
        score -= 2;
      }

      if (recentSevenNetTotal < -10000) {
        score += 3;
      } else if (recentSevenNetTotal < -5000) {
        score -= 4;
      } else if (recentSevenNetTotal < 0) {
        score -= 3;
      } else if (recentSevenNetTotal < 3000) {
        score += 8;
      } else if (recentSevenNetTotal < 8000) {
        score -= 4;
      } else {
        score += 2;
      }

      if (features.recentFourteenAngle < -250) {
        score -= 2;
      } else if (features.recentFourteenAngle < -120) {
        score += 8;
      } else if (features.recentFourteenAngle < 50) {
        score += 8;
      } else if (features.recentFourteenAngle < 180) {
        score -= 5;
      } else {
        score -= 10;
      }

      if (sevenSinkStayProxy === 1) {
        score -= 2;
      } else if (sevenSinkStayProxy === 2) {
        score += 2;
      } else if (sevenSinkStayProxy >= 3 && sevenSinkStayProxy <= 5) {
        score += 12;
      } else if (sevenSinkStayProxy >= 6) {
        score -= 8;
      }
      if (fourteenSinkStayProxy >= 4 && fourteenSinkStayProxy <= 5) {
        score += 5;
      } else if (fourteenSinkStayProxy >= 6) {
        score -= 8;
      }

      if (streak === 2) {
        score += 2;
      } else if (streak === 3) {
        score += 14;
      } else if (streak === 4) {
        score -= 8;
      } else if (streak >= 5 && streak <= 6) {
        score -= 12;
      } else if (streak >= 7) {
        score += 5;
      }

      if (Number.isFinite(daysSinceMachineHighContent)) {
        if (daysSinceMachineHighContent === 1) {
          score -= 8;
        } else if (daysSinceMachineHighContent >= 3 && daysSinceMachineHighContent <= 4) {
          score += 6;
        } else if (daysSinceMachineHighContent >= 5 && daysSinceMachineHighContent <= 6) {
          score -= 6;
        } else if (daysSinceMachineHighContent >= 7 && daysSinceMachineHighContent <= 10) {
          score += 5;
        }
      }

      if (previousGames < 1000) {
        score -= 5;
      } else if (previousGames < 3000) {
        score -= 4;
      } else if (previousGames < 5000) {
        score += 10;
      } else if (previousGames < 7000) {
        score -= 2;
      } else {
        score -= 8;
      }

      if (previousDifference < -5000) {
        score += 8;
      } else if (previousDifference < -3000) {
        score += 2;
      } else if (previousDifference < -1500) {
        score -= 5;
      } else if (previousDifference < 0) {
        score += 2;
      } else if (previousDifference >= 1500 && previousDifference < 4000) {
        score -= 4;
      } else if (previousDifference >= 4000) {
        score -= 5;
      }

      score -= previousMachineHighContent ? 8 : 0;
      if (leftBehindAmount < -10000) {
        score += 3;
      } else if (leftBehindAmount < -5000) {
        score += 5;
      } else if (leftBehindAmount >= 9000) {
        score -= 6;
      }
      score += adjacentMachineHighContentCount7Near2 >= 2 && adjacentMachineHighContentCount7Near2 <= 5 ? 2 : 0;
      score -= adjacentMachineHighContentCount7Near2 === 0 ? 2 : 0;
      score += recentFourteenMachineHighContentCount === 0 ? 6 : 0;
      score -= recentFourteenMachineHighContentCount >= 2 && recentFourteenMachineHighContentCount <= 3 ? 3 : 0;
      score -= recentFourteenMachineHighContentCount >= 4 ? 4 : 0;
      score -= recentFourteenGamesTotal < 50000 ? 8 : 0;
      score += recentFourteenGamesTotal >= 70000 && recentFourteenGamesTotal <= 90000 ? 3 : 0;

      return Math.round(clamp(score, 0, 100));
    }

    let sinkScore = 0;
    sinkScore += scoreAtMost(recentSevenNetTotal, [
      { maximum: -8650, points: 26 },
      { maximum: -4850, points: 18 },
      { maximum: -2150, points: 8 },
      { maximum: 0, points: 2 },
    ]);
    sinkScore += scoreAtMost(features.recentSevenAngle, [
      { maximum: -220, points: 9 },
      { maximum: -125, points: 5 },
      { maximum: -56, points: 2 },
    ]);
    sinkScore = Math.min(sinkScore, 35);

    let middleScore = 0;
    middleScore += scoreAtMost(recentFourteenNetTotal, [
      { maximum: -11550, points: 12 },
      { maximum: -5660, points: 8 },
      { maximum: -1750, points: 3 },
    ]);
    middleScore += scoreAtMost(recentTwentyOneNetTotal, [
      { maximum: -12860, points: 3 },
      { maximum: -5870, points: 2 },
    ]);
    middleScore = Math.min(middleScore, 15);

    let streakScore = scoreAtLeast(streak, [
      { minimum: 6, points: 14 },
      { minimum: 4, points: 9 },
      { minimum: 3, points: 6 },
      { minimum: 2, points: 3 },
    ]);

    let restScore = 2;
    if (Number.isFinite(daysSinceMachineHighContent)) {
      if (daysSinceMachineHighContent >= 7 && daysSinceMachineHighContent <= 10) {
        restScore = 12;
      } else if (daysSinceMachineHighContent >= 4 && daysSinceMachineHighContent <= 6) {
        restScore = 6;
      } else if (daysSinceMachineHighContent >= 11 && daysSinceMachineHighContent <= 20) {
        restScore = 3;
      } else if (daysSinceMachineHighContent >= 21) {
        restScore = 4;
      }
    }

    let activityScore = scoreAtLeast(recentSevenGamesTotal, [
      { minimum: 52200, points: 8 },
      { minimum: 46900, points: 6 },
      { minimum: 37800, points: 4 },
      { minimum: 32700, points: 2 },
    ]);
    activityScore += scoreAtLeast(previousGames, [
      { minimum: 7650, points: 4 },
      { minimum: 6625, points: 3 },
      { minimum: 5050, points: 1 },
    ]);
    activityScore = Math.min(activityScore, 12);

    const previousSinkScore = scoreAtMost(previousDifference, [
      { maximum: -3080, points: 8 },
      { maximum: -2040, points: 5 },
      { maximum: -1250, points: 3 },
    ]);
    const previousHighMissScore =
      previousMachineHighContent && previousDifference < -1000
        ? 4
        : previousMachineHighContent && previousDifference < 0
          ? 2
          : 0;
    const adjacentScore = adjacentMachineHighContentCount3 === 1 ? 2 : 0;

    let penalty = 0;
    penalty += recentSevenNetTotal > 5420 ? 10 : 0;
    penalty += recentFourteenNetTotal > 10000 ? 8 : 0;
    penalty += previousDifference >= 2500 ? 8 : 0;
    penalty += previousMachineHighContent && previousDifference >= 2000 ? 8 : 0;
    penalty += previousMachineHighContent && previousDifference >= 4000 ? 4 : 0;
    penalty += recentSevenMachineHighContentCount >= 4 ? 12 : 0;
    penalty += recentSevenGamesTotal < 26955 ? 4 : 0;

    return Math.round(
      clamp(
        sinkScore +
          middleScore +
          streakScore +
          restScore +
          activityScore +
          previousSinkScore +
          previousHighMissScore +
          adjacentScore -
          penalty,
        0,
        100,
      ),
    );
  }

  if (machineKey === "hokuto-base") {
    if (
      activeLogicKey === "beam-hikari-hokuto-base" ||
      activeLogicKey === "beam-hikari-hokuto-base-normal" ||
      activeLogicKey === "beam-hikari-hokuto-base-event"
    ) {
      if (targetRangeHistoryRowCount < 14) {
        return 0;
      }

      const previousNearbyShow = Boolean(features.beamHikariHokutoBasePreviousNearbyShow);
      let score = 0;

      if (activeLogicKey === "beam-hikari-hokuto-base" || activeLogicKey === "beam-hikari-hokuto-base-event") {
        if (recentFiveNetTotal <= -2300) {
          score += 16;
        } else if (recentFiveNetTotal <= -1000) {
          score += 10;
        } else if (recentFiveNetTotal <= 0) {
          score += 5;
        }

        if (recentThreeNetTotal <= -1600) {
          score += 7;
        } else if (recentThreeNetTotal <= -700) {
          score += 4;
        }

        if (previousDifference <= -800) {
          score += 5;
        } else if (previousDifference <= -250) {
          score += 2;
        }

        if (recentFourteenNetTotal <= -4000) {
          score += 4;
        } else if (recentFourteenNetTotal <= -1600) {
          score += 2;
        }

        if (recentFiveGamesTotal >= 4000 && features.recentFiveAngle <= -350) {
          score += 12;
        } else if (recentFiveGamesTotal >= 4000 && features.recentFiveAngle <= -150) {
          score += 7;
        }
        score += recentThreeGamesTotal >= 2000 && features.recentThreeAngle <= -470 ? 5 : 0;
        score += recentFourteenGamesTotal >= 15000 && features.recentFourteenAngle <= -195 ? 4 : 0;

        if (streak >= 4) {
          score += 10;
        } else if (streak === 3) {
          score += 7;
        } else if (streak === 2) {
          score += 4;
        }

        if (Number.isFinite(daysSinceMachineHighContent)) {
          if (daysSinceMachineHighContent >= 8 && daysSinceMachineHighContent <= 11) {
            score += 6;
          } else if (daysSinceMachineHighContent >= 20) {
            score += 5;
          } else if (daysSinceMachineHighContent >= 12 && daysSinceMachineHighContent <= 19) {
            score += 2;
          }
        }

        score += recentSevenMachineHighContentCount === 0 ? 4 : 0;
        score += recentFourteenMachineHighContentCount <= 1 ? 2 : 0;
        score -= recentSevenMachineHighContentCount >= 2 ? 4 : 0;

        if (features.recentFiveCombinedDenominator > 119) {
          score += 8;
        } else if (features.recentFiveCombinedDenominator > 103) {
          score += 4;
        }
        if (previousCombinedDenominator > 162) {
          score += 4;
        } else if (previousCombinedDenominator > 116) {
          score += 2;
        }

        if (recentFiveGamesTotal >= 4500 && recentFiveGamesTotal <= 9000) {
          score += 6;
        } else if (recentFiveGamesTotal >= 3000) {
          score += 3;
        }
        score -= recentFourteenGamesTotal < 12000 ? 3 : 0;

        if (recentFourteenNetTotal <= -1655 && recentFiveNetTotal <= -1000 && recentSevenMachineHighContentCount === 0) {
          score += 8;
        } else if (recentFourteenNetTotal <= -1655 && recentFiveNetTotal <= 0) {
          score += 4;
        }

        score += adjacentMachineHighContentCount3Near2 >= 2 && recentFiveNetTotal <= 0 ? 2 : 0;
        score -= previousNearbyShow ? 3 : 0;
        score -= previousMachineHighContent ? 10 : 0;
        if (previousDifference >= 1500) {
          score -= 8;
        } else if (previousDifference >= 357) {
          score -= 5;
        }
        if (recentFiveNetTotal > 2100) {
          score -= 8;
        } else if (recentFiveNetTotal > 0) {
          score -= 3;
        }
        score -= recentFourteenNetTotal > 3888 ? 4 : 0;
        score -= previousCombinedDenominator <= 73 && previousGames >= 500 ? 5 : 0;
        score -= previousGames > 2630 && previousDifference > 0 ? 3 : 0;

        return Math.round(clamp(score, 0, 100));
      }

      if (previousDifference <= -2000) {
        score += 14;
      } else if (previousDifference <= -1500) {
        score += 10;
      } else if (previousDifference <= -1000) {
        score += 7;
      } else if (previousDifference <= -800) {
        score += 4;
      }

      if (recentFiveNetTotal <= -3000) {
        score += 12;
      } else if (recentFiveNetTotal <= -2300) {
        score += 8;
      } else if (recentFiveNetTotal <= -1000) {
        score += 4;
      }

      if (recentThreeNetTotal <= -2200) {
        score += 7;
      } else if (recentThreeNetTotal <= -1600) {
        score += 5;
      } else if (recentThreeNetTotal <= -700) {
        score += 2;
      }

      if (recentFourteenNetTotal <= -6000) {
        score += 6;
      } else if (recentFourteenNetTotal <= -4000) {
        score += 4;
      } else if (recentFourteenNetTotal <= -1600) {
        score += 1;
      }

      if (recentFiveGamesTotal >= 3000 && features.recentFiveAngle <= -500) {
        score += 13;
      } else if (recentFiveGamesTotal >= 3000 && features.recentFiveAngle <= -350) {
        score += 8;
      }
      score += recentThreeGamesTotal >= 1500 && features.recentThreeAngle <= -470 ? 4 : 0;
      score += recentFourteenGamesTotal >= 12000 && features.recentFourteenAngle <= -195 ? 3 : 0;

      if (streak >= 8) {
        score += 12;
      } else if (streak >= 6) {
        score += 10;
      } else if (streak === 5) {
        score += 7;
      } else if (streak === 4) {
        score += 5;
      } else if (streak === 3) {
        score += 2;
      }

      if (Number.isFinite(daysSinceMachineHighContent)) {
        if (daysSinceMachineHighContent >= 8 && daysSinceMachineHighContent <= 14) {
          score += 2;
        } else if (daysSinceMachineHighContent >= 15 && daysSinceMachineHighContent <= 24) {
          score += 1;
        }
        score -= daysSinceMachineHighContent >= 30 ? 4 : 0;
      }

      score += recentFourteenMachineHighContentCount >= 4 ? 7 : 0;
      score += recentFourteenMachineHighContentCount === 3 ? 4 : 0;
      score += recentSevenMachineHighContentCount >= 3 ? 5 : 0;

      const recentFiveWeak = features.recentFiveCombinedDenominator > 119;
      if (features.recentFiveCombinedDenominator > 150) {
        score += 12;
      } else if (features.recentFiveCombinedDenominator > 130) {
        score += 7;
      } else if (recentFiveWeak) {
        score += 4;
      }
      score += !recentFiveWeak && previousCombinedDenominator > 162 ? 3 : 0;

      if (recentFiveGamesTotal >= 3000 && recentFiveGamesTotal <= 9000) {
        score += 6;
      } else if (recentFiveGamesTotal > 9000 && recentFiveGamesTotal <= 18000) {
        score += 4;
      } else if (recentFiveGamesTotal >= 2000) {
        score += 2;
      }
      score -= recentFourteenGamesTotal < 8000 ? 2 : 0;

      if (recentFourteenNetTotal <= -3000 && recentFiveNetTotal <= -2300 && streak >= 4) {
        score += 8;
      } else if (recentFourteenNetTotal <= -1600 && recentFiveNetTotal <= -1000) {
        score += 4;
      }

      score -= previousMachineHighContent ? 8 : 0;
      if (previousDifference > 2000) {
        score -= 10;
      } else if (previousDifference > 1500) {
        score -= 8;
      } else if (previousDifference > 500) {
        score -= 6;
      } else if (previousDifference > 0) {
        score -= 4;
      }
      if (recentThreeNetTotal > 2000) {
        score -= 9;
      } else if (recentThreeNetTotal > 1000) {
        score -= 5;
      }
      if (recentFiveNetTotal > 3000) {
        score -= 8;
      } else if (recentFiveNetTotal > 2100) {
        score -= 6;
      } else if (recentFiveNetTotal > 0) {
        score -= 3;
      }
      score -= recentFourteenNetTotal > 3888 ? 5 : 0;
      score -= previousCombinedDenominator <= 90 && previousGames >= 500 ? 5 : 0;
      score -= previousGames > 2630 && previousDifference > 0 ? 4 : 0;
      score -= previousNearbyShow ? 2 : 0;

      return Math.round(clamp(score, 0, 100));
    }
  }

  if (machineKey === "monkey") {
    if (activeLogicKey === "beam-hikari-monkey") {
      if (targetRangeHistoryRowCount < 14) {
        return 0;
      }

      let score = 0;
      let penalty = 0;
      const recentFiveMaxDifference = readNumber(metrics.recentFiveMaxDifference, previousDifference);

      if (recentFourteenNetTotal <= -10000) {
        score += 8;
      } else if (recentFourteenNetTotal <= -6500) {
        score += 12;
      } else if (recentFourteenNetTotal <= -2200) {
        score += 18;
      } else if (recentFourteenNetTotal <= 1900) {
        score += 20;
      } else if (recentFourteenNetTotal <= 5800) {
        score += 8;
      }

      if (recentSevenNetTotal <= -7000) {
        score += 8;
      } else if (recentSevenNetTotal <= -1750) {
        score += 12;
      } else if (recentSevenNetTotal <= 1070) {
        score += 10;
      } else if (recentSevenNetTotal <= 4430) {
        score += 6;
      }

      if (recentThreeNetTotal <= -3000) {
        score += 6;
      } else if (recentThreeNetTotal <= -1500) {
        score += 5;
      } else if (recentThreeNetTotal <= 300) {
        score += 8;
      } else if (recentThreeNetTotal <= 3000) {
        score += 5;
      }

      if (!Number.isFinite(daysSinceMachineHighContent)) {
        score += 4;
      } else if (daysSinceMachineHighContent === 2) {
        score += 3;
      } else if (daysSinceMachineHighContent >= 3 && daysSinceMachineHighContent <= 4) {
        score += 14;
      } else if (daysSinceMachineHighContent >= 5 && daysSinceMachineHighContent <= 7) {
        score += 8;
      } else if (daysSinceMachineHighContent >= 8 && daysSinceMachineHighContent <= 14) {
        score += 12;
      } else if (daysSinceMachineHighContent >= 15) {
        score += 2;
      }

      if (streak === 0) {
        score += 1;
      } else if (streak === 1) {
        score += 3;
      } else if (streak === 2) {
        score += 10;
      } else if (streak === 3) {
        score += 5;
      } else if (streak >= 6 && streak <= 7) {
        score += 8;
      }

      if (previousGames < 800) {
        score += 1;
      } else if (previousGames < 1600) {
        score += 4;
      } else if (previousGames < 2900) {
        score += 6;
      } else if (previousGames <= 5800) {
        score += 8;
      }

      if (recentFiveGamesTotal >= 8000 && recentFiveGamesTotal < 12000) {
        score += 4;
      } else if (recentFiveGamesTotal >= 12000 && recentFiveGamesTotal <= 23500) {
        score += 6;
      } else if (recentFiveGamesTotal >= 23501 && recentFiveGamesTotal <= 26000) {
        score += 2;
      }

      if (features.previousCombinedDenominator < 358) {
        score += 0;
      } else if (features.previousCombinedDenominator <= 462) {
        score += 5;
      } else if (features.previousCombinedDenominator <= 535) {
        score += 3;
      } else {
        score += 4;
      }

      if (features.recentFiveCombinedDenominator <= 398) {
        score += 5;
      } else if (features.recentFiveCombinedDenominator <= 423) {
        score += 3;
      } else if (features.recentFiveCombinedDenominator <= 481) {
        score += 0;
      } else {
        score += 4;
      }

      if (adjacentMachineHighContentCount3 <= 1) {
        score += 6;
      } else if (adjacentMachineNetTotal3 >= -5500 && adjacentMachineNetTotal3 <= -1800) {
        score += 4;
      } else if (adjacentMachineHighContentCount3 >= 4) {
        score += 2;
      }

      score += recentSevenMachineHighContentCount <= 2 ? 3 : 0;
      score += recentFiveMaxDifference < 5000 && recentSevenNetTotal <= 4430 ? 3 : 0;

      penalty += recentFourteenNetTotal > 5800 ? 8 : 0;
      penalty += recentSevenNetTotal > 4430 ? 6 : 0;
      penalty += previousDifference > 3000 ? 6 : 0;
      penalty += previousGames > 7000 ? 4 : 0;
      penalty += recentSevenMachineHighContentCount >= 4 ? 5 : 0;
      penalty += streak >= 8 ? 5 : 0;
      penalty += recentFiveGamesTotal < 8000 ? 4 : 0;

      return Math.round(clamp(score - penalty, 0, 100));
    }

    if (activeLogicKey === "beam-hikari-monkey-normal" || activeLogicKey === "beam-hikari-monkey-event") {
      if (targetRangeHistoryRowCount < 14) {
        return 0;
      }

      let score = 0;
      let penalty = 0;

      if (activeLogicKey === "beam-hikari-monkey-event") {
        if (recentFourteenNetTotal <= -10000) {
          score += 14;
        } else if (recentFourteenNetTotal <= -6500) {
          score += 20;
        } else if (recentFourteenNetTotal <= -2200) {
          score += 14;
        } else if (recentFourteenNetTotal <= 1900) {
          score += 6;
        } else if (recentFourteenNetTotal <= 5800) {
          score += 3;
        } else {
          penalty += 8;
        }

        if (recentThreeNetTotal <= -3000) {
          score += 12;
        } else if (recentThreeNetTotal <= -1500) {
          score += 2;
        } else if (recentThreeNetTotal <= 300) {
          score += 8;
        } else if (recentThreeNetTotal <= 3000) {
          score += 8;
        } else {
          penalty += 8;
        }

        if (recentSevenNetTotal <= -7000) {
          score += 4;
        } else if (recentSevenNetTotal <= -1750) {
          score += 8;
        } else if (recentSevenNetTotal <= 1070) {
          score += 5;
        } else if (recentSevenNetTotal <= 4430) {
          score += 4;
        } else {
          penalty += 5;
        }

        if (!Number.isFinite(daysSinceMachineHighContent)) {
          score += 4;
        } else if (daysSinceMachineHighContent <= 8) {
          penalty += 6;
        } else if (daysSinceMachineHighContent <= 16) {
          score += 10;
        } else if (daysSinceMachineHighContent <= 30) {
          score += 16;
        } else if (daysSinceMachineHighContent <= 60) {
          score += 10;
        } else {
          score += 2;
        }

        if (streak === 1) {
          score += 3;
        } else if (streak >= 2 && streak <= 3) {
          score += 8;
        } else if (streak >= 4 && streak <= 5) {
          score += 5;
        } else if (streak >= 6 && streak <= 7) {
          score += 10;
        } else if (streak >= 8) {
          score += 4;
          penalty += 2;
        }

        if (previousGames < 800) {
          score += 4;
        } else if (previousGames < 1600) {
          score += 1;
        } else if (previousGames < 2900) {
          score += 6;
        } else if (previousGames <= 5800) {
          score += 5;
        } else if (previousGames <= 7000) {
          score += 1;
          penalty += 4;
        } else {
          penalty += 7;
        }

        if (recentFiveGamesTotal < 8000) {
          score += 2;
        } else if (recentFiveGamesTotal < 12000) {
          score += 3;
        } else if (recentFiveGamesTotal <= 23500) {
          score += 6;
        } else if (recentFiveGamesTotal <= 26000) {
          score += 2;
          penalty += 2;
        } else {
          penalty += 4;
        }

        if (features.recentFiveCombinedDenominator <= 398) {
          score += 5;
        } else if (features.recentFiveCombinedDenominator <= 423) {
          score += 3;
        } else if (features.recentFiveCombinedDenominator <= 481) {
          score += 4;
        } else {
          score += 2;
        }

        if (adjacentMachineHighContentCount3 <= 1) {
          score += 8;
        } else if (adjacentMachineHighContentCount3 <= 3) {
          score += 1;
          penalty += 4;
        } else if (adjacentMachineHighContentCount3 <= 6) {
          score += 3;
        } else {
          penalty += 5;
        }

        if (adjacentMachineNetTotal3 >= -5500 && adjacentMachineNetTotal3 <= -1800) {
          score += 4;
        } else if (adjacentMachineNetTotal3 < -5500) {
          score += 2;
        } else if (adjacentMachineNetTotal3 > 5000) {
          score += 3;
        } else if (adjacentMachineNetTotal3 >= 0) {
          penalty += 3;
        }

        score += recentSevenMachineHighContentCount <= 2 ? 3 : 0;
        score += previousDifference < 5000 && recentSevenNetTotal <= 4430 ? 2 : 0;
        penalty += previousDifference > 3000 ? 8 : 0;

        return Math.round(clamp(score - penalty, 0, 100));
      }

      if (recentFourteenNetTotal <= -10000) {
        penalty += 8;
      } else if (recentFourteenNetTotal <= -6500) {
        score += 8;
      } else if (recentFourteenNetTotal <= -2200) {
        score += 16;
      } else if (recentFourteenNetTotal <= 1900) {
        score += 24;
      } else if (recentFourteenNetTotal <= 5800) {
        score += 2;
        penalty += 6;
      } else {
        score += 4;
        penalty += 4;
      }

      if (recentSevenNetTotal <= -7000) {
        score += 12;
      } else if (recentSevenNetTotal <= -1750) {
        score += 1;
        penalty += 5;
      } else if (recentSevenNetTotal <= 1070) {
        score += 8;
      } else if (recentSevenNetTotal <= 4430) {
        score += 7;
      } else {
        penalty += 6;
      }

      if (recentThreeNetTotal <= -3000) {
        score += 3;
      } else if (recentThreeNetTotal <= -1500) {
        score += 2;
      } else if (recentThreeNetTotal <= 300) {
        score += 8;
      } else if (recentThreeNetTotal <= 3000) {
        score += 5;
      } else {
        score += 2;
      }

      if (!Number.isFinite(daysSinceMachineHighContent)) {
        score += 4;
      } else if (daysSinceMachineHighContent === 1) {
        score += 5;
      } else if (daysSinceMachineHighContent === 2) {
        score += 4;
      } else if (daysSinceMachineHighContent >= 3 && daysSinceMachineHighContent <= 4) {
        score += 14;
      } else if (daysSinceMachineHighContent >= 5 && daysSinceMachineHighContent <= 7) {
        score += 3;
      } else if (daysSinceMachineHighContent >= 8 && daysSinceMachineHighContent <= 14) {
        score += 10;
      } else {
        penalty += 6;
      }

      if (streak === 0) {
        score += 4;
      } else if (streak === 1) {
        score += 3;
      } else if (streak === 2) {
        score += 10;
      } else if (streak === 3) {
        score += 1;
        penalty += 3;
      } else if (streak >= 4 && streak <= 5) {
        penalty += 5;
      } else if (streak >= 6 && streak <= 7) {
        score += 6;
      } else if (streak >= 8) {
        penalty += 6;
      }

      if (previousGames < 800) {
        score += 1;
      } else if (previousGames < 1600) {
        score += 4;
      } else if (previousGames < 2900) {
        score += 5;
      } else if (previousGames <= 5800) {
        score += 8;
      } else if (previousGames <= 7000) {
        score += 3;
      } else {
        score += 2;
        penalty += 2;
      }

      if (recentFiveGamesTotal < 8000) {
        score += 2;
      } else if (recentFiveGamesTotal < 12000) {
        score += 5;
      } else if (recentFiveGamesTotal < 18000) {
        score += 6;
      } else if (recentFiveGamesTotal <= 23500) {
        score += 3;
      } else if (recentFiveGamesTotal <= 26000) {
        score += 2;
      } else {
        score += 4;
      }

      if (features.recentFiveCombinedDenominator <= 398) {
        score += 8;
      } else if (features.recentFiveCombinedDenominator <= 423) {
        score += 4;
      } else if (features.recentFiveCombinedDenominator <= 481) {
        penalty += 4;
      } else {
        score += 8;
      }

      if (recentSevenMachineHighContentCount === 0) {
        score += 2;
      } else if (recentSevenMachineHighContentCount <= 2) {
        score += 6;
      } else if (recentSevenMachineHighContentCount === 3) {
        score += 2;
      } else {
        score += 3;
      }
      penalty += previousDifference > 3000 ? 2 : 0;

      score += adjacentMachineHighContentCount3 <= 1 ? 3 : adjacentMachineHighContentCount3 <= 3 ? 2 : 1;
      score += adjacentMachineNetTotal3 >= -1800 && adjacentMachineNetTotal3 <= 0 ? 1 : 0;

      return Math.round(clamp(score - penalty, 0, 100));
    }

    if (readNumber(metrics.historyRowCount) < 21) {
      return 0;
    }

    let score = 30;
    const fiveAngleBoost =
      recentFiveGamesTotal >= 19917 &&
      features.recentFiveAngle >= -261 &&
      features.recentFiveAngle <= -101;
    const fiveAngleCore =
      recentFiveGamesTotal >= 19917 &&
      features.recentFiveAngle >= -161 &&
      features.recentFiveAngle <= -101;
    const twentyOneDeepSink = recentTwentyOneNetTotal <= -11333 && recentTwentyOneGamesTotal >= 79247;
    const fourteenDeepSink = recentFourteenNetTotal <= -3218 && recentFourteenGamesTotal >= 51400;
    const fourteenSinkStay =
      (recentFourteenMinus3218StayDays >= 2 && recentFourteenMinus3218StayDays <= 3) ||
      recentFourteenMinus3218StayDays >= 7;

    score += fiveAngleBoost ? 18 : 0;
    score += fiveAngleCore ? 6 : 0;
    score += recentSevenGamesTotal >= 28195 && features.recentSevenAngle >= 3 && features.recentSevenAngle <= 48 ? 8 : 0;
    score += recentFourteenGamesTotal >= 57174 && features.recentFourteenAngle >= -100 && features.recentFourteenAngle <= -30 ? 10 : 0;
    score += recentTwentyOneGamesTotal >= 86812 && features.recentTwentyOneAngle >= -164 && features.recentTwentyOneAngle <= -124 ? 6 : 0;
    score += twentyOneDeepSink ? 18 : 0;
    score += !twentyOneDeepSink && recentTwentyOneNetTotal <= -9189 && recentTwentyOneGamesTotal >= 79247 ? 6 : 0;
    score += fourteenDeepSink ? 8 : 0;
    score += fourteenSinkStay ? 5 : 0;
    score += recentTwentyOneMinus11333StayDays >= 2 ? 7 : 0;
    score += previousMachineHighContent && previousDifference < 0 ? 12 : 0;
    score += previousMachineHighContent && previousDifference >= 0 && previousDifference < 1000 ? 22 : 0;
    score += previousMachineStrongHighContent && previousDifference >= 0 && previousDifference < 1000 ? 6 : 0;
    score += previousMachineStrongHighContent && previousDifference < 0 ? 4 : 0;
    if (previousDifference > 0 && recentSevenNetTotal <= -3146) {
      score += 8;
    } else if (previousDifference > 0 && recentFourteenNetTotal <= -2305) {
      score += 6;
    } else if (previousDifference > 0 && recentTwentyOneNetTotal <= -394) {
      score += 4;
    }
    score += streak === 4 ? 7 : 0;
    score += streak >= 6 && streak <= 8 ? 13 : 0;
    score += streak >= 9 ? 5 : 0;
    score += Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 7 && daysSinceMachineHighContent <= 14 ? 3 : 0;
    score += Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 22 ? 5 : 0;
    score += previousGames >= 4852 && previousCombinedDenominator <= 411 ? 5 : 0;

    score -= Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent >= 2 && daysSinceMachineHighContent <= 6 ? 8 : 0;
    score -= previousMachineHighContent && previousDifference >= 1000 ? 14 : 0;
    score -= previousMachineStrongHighContent && previousDifference >= 1000 ? 4 : 0;
    if (recentFourteenNetTotal >= 4776) {
      score -= 16;
    } else if (recentTenNetTotal >= 4195) {
      score -= 8;
    } else if (recentSevenNetTotal >= 3310) {
      score -= 6;
    }
    score -= recentFourteenNetTotal >= 9664 ? 6 : 0;
    score -= recentSevenNetTotal >= 5876 ? 3 : 0;
    if (recentFiveGamesTotal < 16802) {
      score -= 6;
    } else if (recentTenGamesTotal < 35852) {
      score -= 3;
    }

    return Math.round(clamp(score, 0, 100));
  }

  let score = 18;
  score += scale(-recentFiveNetTotal, 0, profile === "okidoki" ? 9000 : 6500, 22);
  score += scale(-recentThreeNetTotal, 0, profile === "okidoki" ? 5500 : 3500, 14);
  score += scale(lossAbsTotal, 0, profile === "okidoki" ? 13000 : 10000, 14);
  score += scale(streak, 0, 4, 10);
  score += calculateRestScore(features.bestRestDays, profile);
  score += features.weakBonus ? (profile === "hana" ? 14 : 10) : 0;
  score += features.strongAngle ? 8 : 0;
  score += features.historyPositive3 && profile === "hanabi" ? 12 : 0;
  score += profile === "okidoki" ? scale(-recentSixNetTotal, 0, 12000, 12) : 0;
  score += profile === "smart" ? scale(-historyNetTotal, 0, 12000, 8) : 0;
  score += profile === "hanabi" ? scale(historyPositiveDays, 0, 5, 8) : 0;
  score -= features.dangerCount * (profile === "okidoki" ? 7 : 9);

  return Math.round(clamp(score, 0, 100));
}

function matchesFlagRequirements(features, requiredFlags = []) {
  return requiredFlags.every((flag) => Boolean(features?.[flag]));
}

function matchesAnyFlag(features, flags = []) {
  if (!Array.isArray(flags) || flags.length === 0) {
    return true;
  }
  return flags.some((flag) => Boolean(features?.[flag]));
}

function matchesCondition(matcher, evaluation) {
  if (!matcher || !evaluation) {
    return false;
  }
  if (Array.isArray(matcher.anyOf) && matcher.anyOf.length > 0) {
    return matcher.anyOf.some((candidateMatcher) => matchesCondition(candidateMatcher, evaluation));
  }
  if (Number.isFinite(matcher.minScore) && evaluation.score < matcher.minScore) {
    return false;
  }
  if (Number.isFinite(matcher.maxScore) && evaluation.score > matcher.maxScore) {
    return false;
  }
  if (Number.isFinite(matcher.rankMax) && (!Number.isFinite(evaluation.rank) || evaluation.rank > matcher.rankMax)) {
    return false;
  }
  if (
    Number.isFinite(matcher.minNextGap) &&
    (!Number.isFinite(evaluation.nextGap) || evaluation.nextGap < matcher.minNextGap)
  ) {
    return false;
  }
  if (
    Number.isFinite(matcher.maxNextGap) &&
    (!Number.isFinite(evaluation.nextGap) || evaluation.nextGap > matcher.maxNextGap)
  ) {
    return false;
  }
  if (Number.isFinite(matcher.minBoost) && evaluation.boostCount < matcher.minBoost) {
    return false;
  }
  if (Number.isFinite(matcher.maxBoost) && evaluation.boostCount > matcher.maxBoost) {
    return false;
  }
  if (Number.isFinite(matcher.minDanger) && evaluation.dangerCount < matcher.minDanger) {
    return false;
  }
  if (Number.isFinite(matcher.maxDanger) && evaluation.dangerCount > matcher.maxDanger) {
    return false;
  }
  if (!matchesFlagRequirements(evaluation.features, matcher.requiredFlags)) {
    return false;
  }
  if (!matchesAnyFlag(evaluation.features, matcher.anyFlags)) {
    return false;
  }
  return true;
}

function buildBeamHikariDateSetting(definition, dateText) {
  const isEventDate = isBeamHikariEventDate(dateText);
  const logicKey =
    definition?.machineKey === "neo-aim"
      ? isEventDate
        ? "beam-hikari-neo-aim-event"
        : "beam-hikari-neo-aim-normal"
      : definition?.machineKey === "funky"
        ? isEventDate
          ? "beam-hikari-funky-event"
          : "beam-hikari-funky-normal"
        : definition?.machineKey === "gogo"
          ? isEventDate
            ? "beam-hikari-gogo-event"
            : "beam-hikari-gogo-normal"
        : definition?.machineKey === "my"
          ? isEventDate
            ? "beam-hikari-my-event"
            : "beam-hikari-my-normal"
          : definition?.machineKey === "girls"
            ? isEventDate
              ? "beam-hikari-girls-event"
              : "beam-hikari-girls-normal"
            : definition?.machineKey === "happy"
              ? isEventDate
                ? "beam-hikari-happy-event"
                : "beam-hikari-happy-normal"
              : definition?.machineKey === "ultra-miracle"
                ? isEventDate
                  ? "beam-hikari-ultra-event"
                  : "beam-hikari-ultra-normal"
                : definition?.machineKey === "monkey"
                  ? isEventDate
                    ? "beam-hikari-monkey-event"
                    : "beam-hikari-monkey-normal"
                  : definition?.machineKey === "hokuto-tensei"
                    ? isEventDate
                      ? "beam-hikari-hokuto-tensei-event"
                      : "beam-hikari-hokuto-tensei-normal"
                    : definition?.machineKey === "hokuto-base"
                      ? isEventDate
                        ? "beam-hikari-hokuto-base-event"
                        : "beam-hikari-hokuto-base-normal"
        : "";
  if (!logicKey) {
    return null;
  }
  const logic = findLogicDefinition(definition, logicKey);
  if (!logic) {
    return null;
  }
  const condition =
    listConditionDefinitions(definition, logic.key).find(
      (candidate) => candidate.keySuffix === logic.defaultConditionSuffix,
    ) ??
    listConditionDefinitions(definition, logic.key)[0] ??
    null;
  return {
    logicKey: logic.key,
    conditionKey: condition ? buildConditionKey(definition, condition) : "",
  };
}

function resolveRankingDateSpecificSetting(definition, setting, options = {}) {
  if (
    !options?.dateSpecificRanking ||
    !isBeamHikariStore(options?.storeName) ||
    ![
      "neo-aim",
      "funky",
      "gogo",
      "my",
      "girls",
      "happy",
      "ultra-miracle",
      "monkey",
      "hokuto-tensei",
      "hokuto-base",
    ].includes(definition?.machineKey)
  ) {
    return setting;
  }
  const targetDate = readRankingTargetDate(options?.snapshot);
  return buildBeamHikariDateSetting(definition, targetDate) ?? setting;
}

function resolveRankingBaseSetting(definition, setting, options = {}) {
  if (
    options?.dateSpecificRanking &&
    isBeamHikariStore(options?.storeName) &&
    [
      "neo-aim",
      "funky",
      "gogo",
      "my",
      "girls",
      "happy",
      "ultra-miracle",
      "monkey",
      "hokuto-tensei",
      "hokuto-base",
    ].includes(definition?.machineKey)
  ) {
    const baseLogicKeyByMachineKey = {
      "neo-aim": "beam-hikari-neo-aim",
      funky: "beam-hikari-funky",
      gogo: "beam-hikari-gogo",
      my: "beam-hikari-my",
      girls: "beam-hikari-girls",
      happy: "beam-hikari-happy",
      "ultra-miracle": "beam-hikari-ultra",
      monkey: "beam-hikari-monkey",
      "hokuto-tensei": "beam-hikari-hokuto-tensei",
      "hokuto-base": "beam-hikari-hokuto-base",
    };
    const logic = findLogicDefinition(definition, baseLogicKeyByMachineKey[definition.machineKey]);
    const condition =
      logic
        ? listConditionDefinitions(definition, logic.key).find(
            (candidate) => candidate.keySuffix === logic.defaultConditionSuffix,
          ) ??
          listConditionDefinitions(definition, logic.key)[0] ??
          null
        : null;
    return logic
      ? {
          logicKey: logic.key,
          conditionKey: condition ? buildConditionKey(definition, condition) : "",
        }
      : setting;
  }

  return resolveRankingDateSpecificSetting(definition, setting, options);
}

function buildEvaluationForRowWithSetting(row, definition, setting) {
  if (!definition || !setting?.logicKey) {
    return null;
  }

  const logic = findLogicDefinition(definition, setting.logicKey);
  if (!logic) {
    return null;
  }

  const runtimeDefinition = {
    ...definition,
    activeLogicKey: logic.key,
    activeLogicName: logic.name,
  };
  const metrics = row?.machineEvaluationMetrics ?? {};
  const features = buildMachineSpecificFeatureState(runtimeDefinition, metrics, buildFeatureState(metrics));
  const condition = findConditionDefinition(definition, setting.conditionKey, logic.key);
  const score = calculateMachineScore(runtimeDefinition, metrics, features);

  return {
    machineKey: definition.machineKey,
    logicKey: logic.key,
    logicName: logic.name,
    conditionKey: condition ? buildConditionKey(definition, condition) : "",
    conditionName: condition?.name ?? "",
    backtestLabel: condition?.backtestLabel ?? "",
    backtestPayoutRate: condition?.backtestPayoutRate ?? null,
    backtestRbDenominator: condition?.backtestRbDenominator ?? null,
    score,
    rank: null,
    nextGap: null,
    boostCount: features.boostCount,
    dangerCount: features.dangerCount,
    matchesAdoption: false,
    features,
  };
}

function buildEvaluationForRow(row, settingByMachineKey, options = {}) {
  const definition = findMachineDefinition(row?.machineName);
  const rawSetting = definition ? settingByMachineKey.get(definition.machineKey) : null;
  const setting = definition ? resolveRankingBaseSetting(definition, rawSetting, options) : null;
  return buildEvaluationForRowWithSetting(row, definition, setting);
}

function buildDaySpecificEvaluationForRow(row, options = {}) {
  if (!options?.dateSpecificRanking || !isBeamHikariStore(options?.storeName)) {
    return null;
  }

  const definition = findMachineDefinition(row?.machineName);
  if (
    ![
      "neo-aim",
      "funky",
      "gogo",
      "my",
      "girls",
      "happy",
      "ultra-miracle",
      "monkey",
      "hokuto-tensei",
      "hokuto-base",
    ].includes(definition?.machineKey)
  ) {
    return null;
  }

  const targetDate = readRankingTargetDate(options?.snapshot);
  const setting = buildBeamHikariDateSetting(definition, targetDate);
  const evaluation = buildEvaluationForRowWithSetting(row, definition, setting);
  if (!evaluation) {
    return null;
  }

  return {
    ...evaluation,
    displayLabel: isBeamHikariEventDate(targetDate) ? "特定日" : "通常日",
  };
}

function buildMatchedConditionSummaries(definition, logicKey, evaluation) {
  if (!definition || !evaluation) {
    return [];
  }

  return listConditionDefinitions(definition, logicKey)
    .filter((condition) => matchesCondition(condition.matcher, evaluation))
    .map((condition) => {
      const conditionKey = buildConditionKey(definition, condition);
      return {
        conditionKey,
        conditionName: condition.name,
        backtestLabel: condition.backtestLabel,
        backtestPayoutRate: condition.backtestPayoutRate ?? null,
        backtestRbDenominator: condition.backtestRbDenominator ?? null,
        isSelected: conditionKey === evaluation.conditionKey,
      };
    });
}

function readBestMatchedBacktestPayoutRate(matchedConditions) {
  const values = (Array.isArray(matchedConditions) ? matchedConditions : [])
    .map((condition) => readNullableNumber(condition?.backtestPayoutRate))
    .filter((value) => value !== null);
  return values.length > 0 ? Math.max(...values) : null;
}

function compareMachineEvaluationRows(left, right, evaluationKey = "machineEvaluation") {
  const leftScore = readNullableNumber(left?.[evaluationKey]?.score);
  const rightScore = readNullableNumber(right?.[evaluationKey]?.score);
  if (leftScore !== null || rightScore !== null) {
    const scoreDiff = (rightScore ?? Number.NEGATIVE_INFINITY) - (leftScore ?? Number.NEGATIVE_INFINITY);
    if (Math.abs(scoreDiff) > 0.000000001) {
      return scoreDiff;
    }
  }

  return (
    normalizeText(left?.machineName).localeCompare(normalizeText(right?.machineName), "ja") ||
    normalizeText(left?.slotNumber).localeCompare(normalizeText(right?.slotNumber), "ja", {
      numeric: true,
    })
  );
}

function attachMachineEvaluationRanks(rows, evaluationKey = "machineEvaluation") {
  const rowsByMachineName = new Map();

  for (const row of rows) {
    if (!row?.[evaluationKey]) {
      continue;
    }
    const machineName = normalizeText(row.machineName);
    if (!rowsByMachineName.has(machineName)) {
      rowsByMachineName.set(machineName, []);
    }
    rowsByMachineName.get(machineName).push(row);
  }

  const contextByRowKey = new Map();
  for (const machineRows of rowsByMachineName.values()) {
    const sortedRows = [...machineRows].sort((left, right) =>
      compareMachineEvaluationRows(left, right, evaluationKey),
    );
    sortedRows.forEach((row, index) => {
      const score = readNullableNumber(row?.[evaluationKey]?.score);
      let firstSameScoreIndex = index;
      let lastSameScoreIndex = index;
      if (score !== null) {
        while (
          firstSameScoreIndex > 0 &&
          Math.abs(
            (readNullableNumber(sortedRows[firstSameScoreIndex - 1]?.[evaluationKey]?.score) ?? Number.NaN) - score,
          ) <= 0.000000001
        ) {
          firstSameScoreIndex -= 1;
        }
        while (
          lastSameScoreIndex + 1 < sortedRows.length &&
          Math.abs(
            (readNullableNumber(sortedRows[lastSameScoreIndex + 1]?.[evaluationKey]?.score) ?? Number.NaN) - score,
          ) <= 0.000000001
        ) {
          lastSameScoreIndex += 1;
        }
      }
      let nextScore = null;
      for (let nextIndex = lastSameScoreIndex + 1; nextIndex < sortedRows.length; nextIndex += 1) {
        const candidateScore = readNullableNumber(sortedRows[nextIndex]?.[evaluationKey]?.score);
        if (candidateScore !== null && (score === null || Math.abs(candidateScore - score) > 0.000000001)) {
          nextScore = candidateScore;
          break;
        }
      }
      contextByRowKey.set(normalizeText(row?.rowKey), {
        rank: score !== null ? firstSameScoreIndex + 1 : index + 1,
        nextGap: score !== null && nextScore !== null ? score - nextScore : null,
      });
    });
  }

  return rows.map((row) => {
    const evaluation = row?.[evaluationKey];
    if (!evaluation) {
      return row;
    }
    const context = contextByRowKey.get(normalizeText(row?.rowKey)) ?? {};
    const updatedEvaluation = {
      ...evaluation,
      rank: context.rank ?? null,
      nextGap: context.nextGap ?? null,
    };
    const definition = DEFINITIONS_BY_MACHINE_KEY.get(updatedEvaluation.machineKey);
    const condition = findConditionDefinition(
      definition,
      updatedEvaluation.conditionKey,
      updatedEvaluation.logicKey,
    );
    const matchedConditions = buildMatchedConditionSummaries(
      definition,
      updatedEvaluation.logicKey,
      updatedEvaluation,
    );
    return {
      ...row,
      [evaluationKey]: {
        ...updatedEvaluation,
        matchesAdoption: matchesCondition(condition?.matcher, updatedEvaluation),
        matchedConditions,
        matchesAnyCondition: matchedConditions.length > 0,
        bestMatchedBacktestPayoutRate: readBestMatchedBacktestPayoutRate(matchedConditions),
      },
    };
  });
}

export function decorateSnapshotsWithMachineEvaluation(snapshots, settingRows = [], options = {}) {
  if (!hasAnyConfiguredSetting(settingRows)) {
    return snapshots;
  }

  const settingByMachineKey = buildSettingByMachineKey(settingRows);
  return (Array.isArray(snapshots) ? snapshots : []).map((snapshot) => {
    const rowsWithEvaluation = (Array.isArray(snapshot?.rows) ? snapshot.rows : []).map((row) => ({
      ...row,
      machineEvaluation: buildEvaluationForRow(row, settingByMachineKey, {
        ...options,
        snapshot,
      }),
      machineEvaluationDaySpecific: buildDaySpecificEvaluationForRow(row, {
        ...options,
        snapshot,
      }),
    }));
    const rowsWithMachineEvaluationRanks = attachMachineEvaluationRanks(
      rowsWithEvaluation,
      "machineEvaluation",
    );

    return {
      ...snapshot,
      rows: attachMachineEvaluationRanks(
        rowsWithMachineEvaluationRanks,
        "machineEvaluationDaySpecific",
      ),
    };
  });
}

function buildRankingRowKey(row) {
  return normalizeText(row?.rowKey ?? `${row?.machineName ?? ""}::${row?.slotNumber ?? ""}`);
}

function compareRankingRowsByOriginalOrder(left, right) {
  return (
    readNumber(left?.rank, Number.MAX_SAFE_INTEGER) -
      readNumber(right?.rank, Number.MAX_SAFE_INTEGER) ||
    normalizeText(left?.machineName).localeCompare(normalizeText(right?.machineName), "ja") ||
    normalizeText(left?.slotNumber).localeCompare(normalizeText(right?.slotNumber), "ja", {
      numeric: true,
    })
  );
}

function compareRankingRowsByMachineEvaluation(left, right) {
  const leftEvaluation = left?.machineEvaluation ?? null;
  const rightEvaluation = right?.machineEvaluation ?? null;
  const scoreDiff =
    readNumber(rightEvaluation?.score, Number.NEGATIVE_INFINITY) -
    readNumber(leftEvaluation?.score, Number.NEGATIVE_INFINITY);
  if (Math.abs(scoreDiff) > 0.000000001) {
    return scoreDiff;
  }
  const matchDiff =
    Number(Boolean(rightEvaluation?.matchesAdoption)) - Number(Boolean(leftEvaluation?.matchesAdoption));
  if (matchDiff !== 0) {
    return matchDiff;
  }

  return compareRankingRowsByOriginalOrder(left, right);
}

function compareRankingRowsByAdoptionPriority(left, right) {
  const matchDiff =
    Number(Boolean(right?.machineEvaluation?.matchesAdoption)) -
    Number(Boolean(left?.machineEvaluation?.matchesAdoption));
  if (matchDiff !== 0) {
    return matchDiff;
  }
  return compareRankingRowsByOriginalOrder(left, right);
}

function remapRankingGroupRows(group, rows, displayLimit) {
  const rankedRows = rows.map((row, index) => ({
    ...row,
    rank: index + 1,
    machineRank: index + 1,
  }));

  return {
    ...group,
    totalCount: rankedRows.length,
    limit: Math.min(displayLimit, rankedRows.length),
    allRows: rankedRows,
    rows: rankedRows.slice(0, displayLimit),
  };
}

function attachMachineEvaluationRankingOrder(rankingGroups, comparator) {
  const orderByRowKey = new Map(
    rankingGroups
      .flatMap((group) => group.allRows)
      .sort(comparator)
      .map((row, index) => [buildRankingRowKey(row), index + 1]),
  );

  return rankingGroups.map((group) => ({
    ...group,
    allRows: group.allRows.map((row) => ({
      ...row,
      selectedRank: orderByRowKey.get(buildRankingRowKey(row)) ?? row.selectedRank,
      machineEvaluationRankingOrder: orderByRowKey.get(buildRankingRowKey(row)) ?? null,
    })),
    rows: group.rows.map((row) => ({
      ...row,
      selectedRank: orderByRowKey.get(buildRankingRowKey(row)) ?? row.selectedRank,
      machineEvaluationRankingOrder: orderByRowKey.get(buildRankingRowKey(row)) ?? null,
    })),
  }));
}

export function applyMachineEvaluationRankingMode(rankingGroups, mode, displayLimit = 20) {
  const normalizedMode = normalizeMachineEvaluationRankingMode(mode);
  if (
    normalizedMode === MACHINE_EVALUATION_RANKING_MODE_NONE ||
    normalizedMode === MACHINE_EVALUATION_RANKING_MODE_SHOW
  ) {
    return rankingGroups;
  }

  const comparator =
    normalizedMode === MACHINE_EVALUATION_RANKING_MODE_SCORE
      ? compareRankingRowsByMachineEvaluation
      : normalizedMode === MACHINE_EVALUATION_RANKING_MODE_PRIORITY
        ? compareRankingRowsByAdoptionPriority
        : compareRankingRowsByOriginalOrder;
  const remappedGroups = (Array.isArray(rankingGroups) ? rankingGroups : [])
    .map((group) => {
      const sourceRows = Array.isArray(group?.allRows) ? group.allRows : group?.rows ?? [];
      if (normalizedMode === MACHINE_EVALUATION_RANKING_MODE_FILTER) {
        return remapRankingGroupRows(
          group,
          sourceRows.filter((row) => row?.machineEvaluation?.matchesAdoption),
          displayLimit,
        );
      }
      if (normalizedMode === MACHINE_EVALUATION_RANKING_MODE_SCORE) {
        return remapRankingGroupRows(
          group,
          [...sourceRows].sort(compareRankingRowsByMachineEvaluation),
          displayLimit,
        );
      }
      return remapRankingGroupRows(
        group,
        [...sourceRows].sort(compareRankingRowsByAdoptionPriority),
        displayLimit,
      );
    })
    .filter((group) => group.rows.length > 0);

  return attachMachineEvaluationRankingOrder(remappedGroups, comparator);
}
