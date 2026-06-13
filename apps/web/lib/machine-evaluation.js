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

function readDateDayNumber(dateText) {
  const normalized = normalizeText(dateText);
  const match = normalized.match(/^\d{4}-\d{2}-(\d{2})$/u) ?? normalized.match(/^\d{2}\/\d{2}\/(\d{2})$/u);
  if (!match) {
    return null;
  }
  const dayNumber = Number(match[1]);
  return Number.isFinite(dayNumber) ? dayNumber : null;
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

function buildCondition(keySuffix, name, backtestLabel, matcher, logicKeys = []) {
  return {
    keySuffix,
    name,
    backtestLabel,
    backtestPayoutRate: readBacktestPayoutRate(backtestLabel),
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
        "beam-hikari-normal-main",
        "1位＋75点以上＋次点差8点以上",
        "39件 / 104.26% / RB1/294.9",
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
        "42件 / 104.32% / RB1/296.7",
        {
          minScore: 75,
          requiredFlags: ["beamHikariGogoNormalHistoryReady"],
        },
        ["beam-hikari-gogo-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-score80",
        "80点以上",
        "31件 / 105.23% / RB1/287.7",
        {
          minScore: 80,
          requiredFlags: ["beamHikariGogoNormalHistoryReady"],
        },
        ["beam-hikari-gogo-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-rank1-score80",
        "1位＋80点以上",
        "30件 / 105.13% / RB1/288.8",
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
        "33件 / 105.52% / RB1/288.3",
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
        "23件 / 106.17% / RB1/257.5",
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
        "30件 / 105.50% / RB1/263.3",
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
        "38件 / 104.76% / RB1/263.4",
        {
          minScore: 70,
          requiredFlags: ["beamHikariGogoEventHistoryReady"],
        },
        ["beam-hikari-gogo-event"],
      ),
      buildCondition(
        "beam-hikari-event-score75",
        "75点以上",
        "20件 / 105.08% / RB1/258.8",
        {
          minScore: 75,
          requiredFlags: ["beamHikariGogoEventHistoryReady"],
        },
        ["beam-hikari-gogo-event"],
      ),
      buildCondition(
        "beam-hikari-event-rank1-score75-gap8",
        "1位＋75点以上＋次点差8点以上",
        "16件 / 106.43% / RB1/249.2",
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
        "20件 / 105.18% / RB1/262.6",
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
      ),
      buildCondition(
        "top4",
        "上位4台",
        "500件 / 103.9%",
        {
          rankMax: 4,
          requiredFlags: ["hokutoHistoryReady"],
        },
      ),
      buildCondition(
        "top2",
        "上位2台",
        "250件 / 106.4%",
        {
          rankMax: 2,
          requiredFlags: ["hokutoHistoryReady"],
        },
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
      buildLogicVariant("beam-hikari-neo-aim-normal", "ネオアイムビームヒカリ通常日式", "beam-hikari-normal-main"),
      buildLogicVariant("beam-hikari-neo-aim-event", "ネオアイムビームヒカリイベント日式", "beam-hikari-event-main"),
      buildLogicVariant("amuse-asakusa-neo-aim", "ネオアイムアミューズ浅草式", "amuse-asakusa-main"),
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
        "apark-yakatabaru-main",
        "1位＋70点以上＋次点差10点以上",
        "44件 / 104.34% / RB1/254.5",
        {
          rankMax: 1,
          minScore: 70,
          minNextGap: 10,
          requiredFlags: ["yakatabaruNeoHistoryReady"],
        },
        ["apark-yakatabaru-neo-aim"],
      ),
      buildCondition(
        "apark-yakatabaru-wide",
        "1位＋70点以上",
        "78件 / 103.53% / RB1/266.1",
        {
          rankMax: 1,
          minScore: 70,
          requiredFlags: ["yakatabaruNeoHistoryReady"],
        },
        ["apark-yakatabaru-neo-aim"],
      ),
      buildCondition(
        "apark-yakatabaru-strong",
        "1位＋80点以上",
        "19件 / 105.30% / RB1/255.6",
        {
          rankMax: 1,
          minScore: 80,
          requiredFlags: ["yakatabaruNeoHistoryReady"],
        },
        ["apark-yakatabaru-neo-aim"],
      ),
      buildCondition(
        "apark-yakatabaru-gap15",
        "1位＋70点以上＋次点差15点以上",
        "30件 / 103.88% / RB1/249.9",
        {
          rankMax: 1,
          minScore: 70,
          minNextGap: 15,
          requiredFlags: ["yakatabaruNeoHistoryReady"],
        },
        ["apark-yakatabaru-neo-aim"],
      ),
      buildCondition(
        "mj-kurume-main",
        "1位＋75点以上",
        "73件 / 105.69% / RB1/260.7",
        {
          rankMax: 1,
          minScore: 75,
          requiredFlags: ["kurumeNeoHistoryReady"],
        },
        ["mj-kurume-neo-aim"],
      ),
      buildCondition(
        "mj-kurume-strong",
        "1位＋75点以上＋次点差8点以上",
        "50件 / 106.35% / RB1/256.3",
        {
          rankMax: 1,
          minScore: 75,
          minNextGap: 8,
          requiredFlags: ["kurumeNeoHistoryReady"],
        },
        ["mj-kurume-neo-aim"],
      ),
      buildCondition(
        "mj-kurume-wide",
        "上位3位以内＋75点以上",
        "85件 / 104.86% / RB1/266.3",
        {
          rankMax: 3,
          minScore: 75,
          requiredFlags: ["kurumeNeoHistoryReady"],
        },
        ["mj-kurume-neo-aim"],
      ),
      buildCondition(
        "beam-hikari-normal-rank1",
        "通常日：1位のみ",
        "147件 / 103.8% / RB1/279.3",
        {
          rankMax: 1,
          requiredFlags: ["beamHikariNeoNormalHistoryReady"],
        },
        ["beam-hikari-neo-aim-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-top2",
        "通常日：上位2台",
        "294件 / 103.6% / RB1/285.7",
        {
          rankMax: 2,
          requiredFlags: ["beamHikariNeoNormalHistoryReady"],
        },
        ["beam-hikari-neo-aim-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-top3",
        "通常日：上位3台",
        "441件 / 103.3% / RB1/287.5",
        {
          rankMax: 3,
          requiredFlags: ["beamHikariNeoNormalHistoryReady"],
        },
        ["beam-hikari-neo-aim-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-score80",
        "通常日：80点以上",
        "534件 / 102.7% / RB1/293.3",
        {
          minScore: 80,
          requiredFlags: ["beamHikariNeoNormalHistoryReady"],
        },
        ["beam-hikari-neo-aim-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-score90",
        "通常日：90点以上",
        "300件 / 103.9% / RB1/286.2",
        {
          minScore: 90,
          requiredFlags: ["beamHikariNeoNormalHistoryReady"],
        },
        ["beam-hikari-neo-aim-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-rank1-score80",
        "通常日：1位＋80点以上",
        "139件 / 103.9% / RB1/277.7",
        {
          rankMax: 1,
          minScore: 80,
          requiredFlags: ["beamHikariNeoNormalHistoryReady"],
        },
        ["beam-hikari-neo-aim-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-rank1-score90",
        "通常日：1位＋90点以上",
        "122件 / 103.9% / RB1/282.4",
        {
          rankMax: 1,
          minScore: 90,
          requiredFlags: ["beamHikariNeoNormalHistoryReady"],
        },
        ["beam-hikari-neo-aim-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-main",
        "通常日：1位＋80点以上＋次点差3点以上＋強化2個以上",
        "91件 / 105.5% / RB1/271.2",
        {
          rankMax: 1,
          minScore: 80,
          minNextGap: 3,
          minBoost: 2,
          requiredFlags: ["beamHikariNeoNormalHistoryReady"],
        },
        ["beam-hikari-neo-aim-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-gap5",
        "通常日：1位＋80点以上＋次点差5点以上",
        "73件 / 105.3% / RB1/274.3",
        {
          rankMax: 1,
          minScore: 80,
          minNextGap: 5,
          requiredFlags: ["beamHikariNeoNormalHistoryReady"],
        },
        ["beam-hikari-neo-aim-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-boost2",
        "通常日：1位＋強化2個以上",
        "143件 / 103.9% / RB1/279.3",
        {
          rankMax: 1,
          minBoost: 2,
          requiredFlags: ["beamHikariNeoNormalHistoryReady"],
        },
        ["beam-hikari-neo-aim-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-safe",
        "通常日：1位＋危険なし",
        "139件 / 103.9% / RB1/276.8",
        {
          rankMax: 1,
          maxDanger: 0,
          requiredFlags: ["beamHikariNeoNormalHistoryReady"],
        },
        ["beam-hikari-neo-aim-normal"],
      ),
      buildCondition(
        "beam-hikari-event-main",
        "イベント日：1位のみ",
        "38件 / 105.0% / RB1/283.2",
        {
          rankMax: 1,
          requiredFlags: ["beamHikariNeoEventHistoryReady"],
        },
        ["beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "beam-hikari-event-top2",
        "イベント日：上位2台",
        "76件 / 103.5% / RB1/288.8",
        {
          rankMax: 2,
          requiredFlags: ["beamHikariNeoEventHistoryReady"],
        },
        ["beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "beam-hikari-event-top3",
        "イベント日：上位3台",
        "114件 / 103.3% / RB1/282.3",
        {
          rankMax: 3,
          requiredFlags: ["beamHikariNeoEventHistoryReady"],
        },
        ["beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "beam-hikari-event-score70",
        "イベント日：70点以上",
        "158件 / 102.7% / RB1/293.3",
        {
          minScore: 70,
          requiredFlags: ["beamHikariNeoEventHistoryReady"],
        },
        ["beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "beam-hikari-event-rank1-score70",
        "イベント日：1位＋70点以上",
        "37件 / 105.1% / RB1/282.1",
        {
          rankMax: 1,
          minScore: 70,
          requiredFlags: ["beamHikariNeoEventHistoryReady"],
        },
        ["beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "beam-hikari-event-rank1-score70-gap1",
        "イベント日：1位＋70点以上＋次点差1点以上",
        "31件 / 103.4% / RB1/292.3",
        {
          rankMax: 1,
          minScore: 70,
          minNextGap: 1,
          requiredFlags: ["beamHikariNeoEventHistoryReady"],
        },
        ["beam-hikari-neo-aim-event"],
      ),
      buildCondition(
        "beam-hikari-event-boost2",
        "イベント日：1位＋強化2個以上",
        "37件 / 105.1% / RB1/282.1",
        {
          rankMax: 1,
          minBoost: 2,
          requiredFlags: ["beamHikariNeoEventHistoryReady"],
        },
        ["beam-hikari-neo-aim-event"],
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
        "beam-hikari-normal-core",
        "最重要通常条件日の最上位",
        "32件 / 105.66% / RB1/326.3",
        {
          rankMax: 1,
          requiredFlags: ["beamHikariFunkyNormalHistoryReady", "beamHikariFunkyNormalCore"],
        },
        ["beam-hikari-funky-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-score90",
        "1位＋90点以上",
        "60件 / 104.08% / RB1/332.0",
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
        "125件 / 102.58% / RB1/349.9",
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
        "20件 / 102.65% / RB1/302.7",
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
        "40件 / 103.99% / RB1/311.7",
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
        "24件 / 104.83% / RB1/297.5",
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
    ],
    profile: "juggler",
    defaultConditionSuffix: "apark-yakatabaru-main",
    conditions: [
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
      buildLogicVariant("apark-yakatabaru-ultra-miracle", "ウルトラ屋形原式", "apark-yakatabaru-main"),
    ],
    profile: "juggler",
    defaultConditionSuffix: "apark-yakatabaru-main",
    conditions: [
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
        "beam-hikari-normal-core",
        "1位＋2連敗＋14日沈み強",
        "33件 / 105.60% / RB1/284.2",
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
        "22件 / 105.38% / RB1/287.4",
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
        "38件 / 104.55% / RB1/291.3",
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
        "67件 / 103.85% / RB1/303.4",
        {
          minScore: 90,
          requiredFlags: ["beamHikariMyNormalHistoryReady"],
        },
        ["beam-hikari-my-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-score85",
        "85点以上",
        "93件 / 103.83% / RB1/302.8",
        {
          minScore: 85,
          requiredFlags: ["beamHikariMyNormalHistoryReady"],
        },
        ["beam-hikari-my-normal"],
      ),
      buildCondition(
        "beam-hikari-normal-rank1-two-loss-gap15",
        "1位＋2連敗＋次点差15点以上",
        "42件 / 103.73% / RB1/305.5",
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
        "85件 / 102.99% / RB1/309.8",
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
        "115件 / 103.79% / RB1/300.7",
        {
          minScore: 80,
          requiredFlags: ["beamHikariMyNormalHistoryReady"],
        },
        ["beam-hikari-my-normal"],
      ),
      buildCondition(
        "beam-hikari-event-rank1",
        "1位",
        "38件 / 104.04% / RB1/301.4",
        {
          rankMax: 1,
          requiredFlags: ["beamHikariMyEventHistoryReady"],
        },
        ["beam-hikari-my-event"],
      ),
      buildCondition(
        "beam-hikari-event-gap10",
        "1位＋次点差10点以上",
        "10件 / 106.84% / RB1/283.3",
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
        "22件 / 105.01% / RB1/295.6",
        {
          rankMax: 1,
          requiredFlags: ["beamHikariMyEventHistoryReady", "beamHikariMyEventReuseLine"],
        },
        ["beam-hikari-my-event"],
      ),
      buildCondition(
        "beam-hikari-event-reuse-safe",
        "1位＋再投入筋あり＋危険0",
        "13件 / 102.96% / RB1/303.1",
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
        "9件 / 104.58% / RB1/299.0",
        {
          minScore: 90,
          requiredFlags: ["beamHikariMyEventHistoryReady"],
        },
        ["beam-hikari-my-event"],
      ),
      buildCondition(
        "beam-hikari-event-score85",
        "85点以上",
        "27件 / 104.84% / RB1/283.3",
        {
          minScore: 85,
          requiredFlags: ["beamHikariMyEventHistoryReady"],
        },
        ["beam-hikari-my-event"],
      ),
      buildCondition(
        "beam-hikari-event-top2",
        "上位2台",
        "76件 / 104.19% / RB1/301.5",
        {
          rankMax: 2,
          requiredFlags: ["beamHikariMyEventHistoryReady"],
        },
        ["beam-hikari-my-event"],
      ),
      buildCondition(
        "beam-hikari-event-score70",
        "70点以上",
        "107件 / 103.17% / RB1/311.3",
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
  } else if (isBeamHikariStore(storeName) && definition.machineKey === "neo-aim") {
    defaultLogic = findLogicDefinition(definition, "beam-hikari-neo-aim-normal");
  } else if (isBeamHikariStore(storeName) && definition.machineKey === "funky") {
    defaultLogic = findLogicDefinition(definition, "beam-hikari-funky-normal");
  } else if (isBeamHikariStore(storeName) && definition.machineKey === "gogo") {
    defaultLogic = findLogicDefinition(definition, "beam-hikari-gogo-normal");
  } else if (isBeamHikariStore(storeName) && definition.machineKey === "my") {
    defaultLogic = findLogicDefinition(definition, "beam-hikari-my-normal");
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
  const recentFourteenGoldShowDays = readNumber(metrics.recentFourteenGoldShowDays);
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
  const previousAdjacentMachineGoodContentCount = readNumber(metrics.previousAdjacentMachineGoodContentCount);
  const previousAdjacentMachineBigWin1000Count = readNumber(metrics.previousAdjacentMachineBigWin1000Count);
  const previousAdjacentMachineNetTotal = readNumber(metrics.previousAdjacentMachineNetTotal);
  const previousOtherMachineHighContentCount = readNumber(metrics.previousOtherMachineHighContentCount);
  const sameMachinePreviousNetTotal = readNumber(metrics.sameMachinePreviousNetTotal);
  const recentThreeMachineHighContentCount = readNumber(metrics.recentThreeMachineHighContentCount);
  const recentFiveMachineHighContentCount = readNumber(metrics.recentFiveMachineHighContentCount);
  const recentSevenMachineHighContentCount = readNumber(metrics.recentSevenMachineHighContentCount);
  const recentFourteenMachineHighContentCount = readNumber(metrics.recentFourteenMachineHighContentCount);
  const recentTwentyOneMachineHighContentCount = readNumber(metrics.recentTwentyOneMachineHighContentCount);
  const recentFourteenMachineStrongHighContentCount = readNumber(metrics.recentFourteenMachineStrongHighContentCount);
  const recentThirtyMachineHighContentCount = readNumber(metrics.recentThirtyMachineHighContentCount);
  const daysSinceMachineHighContent = readNullableNumber(metrics.daysSinceMachineHighContent);
  const daysSinceMachineStrongHighContent = readNullableNumber(metrics.daysSinceMachineStrongHighContent);
  const daysSinceMachineBigWin1500 = readNullableNumber(metrics.daysSinceMachineBigWin1500);
  const previousMachineHighContent = Boolean(metrics.previousMachineHighContent);
  const previousMachineGoodContent = Boolean(metrics.previousMachineGoodContent);
  const previousMachineStrongHighContent = Boolean(metrics.previousMachineStrongHighContent);
  const machineHighContentStreak = readNumber(metrics.machineHighContentStreak);

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

    if (activeLogicKey === "beam-hikari-neo-aim-event" || activeLogicKey === "beam-hikari-neo-aim-normal") {
      const beamHikariNeoHistoryReady = historyRowCount >= 21;
      const recentTwoRbTotal = readNumber(metrics.recentTwoRbTotal);
      const recentTwoBonusTotal = readNumber(metrics.recentTwoBonusTotal);
      const recentTwoCombinedDenominator = rateDenominator(recentTwoGamesTotal, recentTwoBonusTotal);
      const recentTwoRbDenominator = rateDenominator(recentTwoGamesTotal, recentTwoRbTotal);
      const recentTwoBonusWeak = recentTwoCombinedDenominator >= 194 || recentTwoRbDenominator >= 540;
      const recentSevenBonusWeak =
        features.recentSevenCombinedDenominator >= 155 || features.recentSevenRbDenominator >= 350;

      if (activeLogicKey === "beam-hikari-neo-aim-event") {
        const beamHikariNeoEventHistoryReady = beamHikariNeoHistoryReady;
        const beamHikariNeoEventTwoDayUntreated =
          recentTwoGamesTotal >= 1000 && recentTwoGamesTotal <= 3500 && recentTwoBonusWeak;
        const beamHikariNeoEventSevenDayUntreated =
          recentSevenGamesTotal >= 15000 && recentSevenGamesTotal <= 25000 && recentSevenBonusWeak;
        const beamHikariNeoEventNearbyLeftBehind =
          previousDifference < 0 &&
          (previousAdjacentMachineHighContentCount >= 1 || previousAdjacentMachineNetTotal >= 1563);
        const beamHikariNeoEventTwoDaySink =
          features.recentTwoAngle <= -300 && recentTwoGamesTotal <= 6000;
        const beamHikariNeoEventTreatmentDone =
          previousDifference >= 500 || recentTwoNetTotal >= 1000;
        const beamHikariNeoEventTooStrong =
          recentTwoCombinedDenominator <= 150 ||
          recentTwoRbDenominator <= 330 ||
          previousMachineHighContent;
        const beamHikariNeoEventHighActivity = recentTwoGamesTotal >= 8000;
        const boostFlags = [
          beamHikariNeoEventTwoDayUntreated,
          beamHikariNeoEventSevenDayUntreated,
          beamHikariNeoEventNearbyLeftBehind,
          beamHikariNeoEventTwoDaySink,
        ];
        const dangerFlags = [
          beamHikariNeoEventTreatmentDone,
          beamHikariNeoEventTooStrong,
          beamHikariNeoEventHighActivity,
        ];

        return {
          ...features,
          beamHikariNeoHistoryReady,
          beamHikariNeoEventHistoryReady,
          beamHikariNeoEventTwoDayUntreated,
          beamHikariNeoEventSevenDayUntreated,
          beamHikariNeoEventNearbyLeftBehind,
          beamHikariNeoEventTwoDaySink,
          beamHikariNeoEventTreatmentDone,
          beamHikariNeoEventTooStrong,
          beamHikariNeoEventHighActivity,
          treatmentDone: beamHikariNeoEventTreatmentDone,
          lowConfidence: !beamHikariNeoEventTwoDayUntreated && !beamHikariNeoEventSevenDayUntreated,
          boostCount: boostFlags.filter(Boolean).length,
          dangerCount: dangerFlags.filter(Boolean).length,
        };
      }

      const beamHikariNeoNormalHistoryReady = beamHikariNeoHistoryReady;
      const beamHikariNeoNormalSteepSink =
        recentTwoGamesTotal >= 1000 &&
        recentTwoGamesTotal <= 6000 &&
        features.recentTwoAngle <= -500;
      const beamHikariNeoNormalTwoDayBonusWeak = recentTwoBonusWeak;
      const beamHikariNeoNormalLowMiddleGames =
        recentTwoGamesTotal >= 1000 && recentTwoGamesTotal <= 3500;
      const beamHikariNeoNormalMediumUnpaid =
        features.recentFiveAngle <= -122 || recentFiveNetTotal <= -2000;
      const beamHikariNeoNormalRotationReturn =
        daysSinceMachineHighContent === 3 ||
        (Number.isFinite(daysSinceMachineHighContent) &&
          daysSinceMachineHighContent >= 6 &&
          daysSinceMachineHighContent <= 17);
      const beamHikariNeoNormalTreatmentDone =
        previousDifference >= 500 || recentTwoNetTotal >= 1000;
      const beamHikariNeoNormalTooStrong =
        recentTwoCombinedDenominator <= 150 ||
        recentTwoRbDenominator <= 330 ||
        previousMachineHighContent;
      const beamHikariNeoNormalHighActivity = recentTwoGamesTotal >= 8000;
      const beamHikariNeoNormalLongNeglect =
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent >= 18 &&
        recentFourteenMachineHighContentCount === 0;
      const boostFlags = [
        beamHikariNeoNormalSteepSink,
        beamHikariNeoNormalTwoDayBonusWeak,
        beamHikariNeoNormalLowMiddleGames,
        beamHikariNeoNormalMediumUnpaid,
        beamHikariNeoNormalRotationReturn,
      ];
      const dangerFlags = [
        beamHikariNeoNormalTreatmentDone,
        beamHikariNeoNormalTooStrong,
        beamHikariNeoNormalHighActivity,
        beamHikariNeoNormalLongNeglect,
      ];

      return {
        ...features,
        beamHikariNeoHistoryReady,
        beamHikariNeoNormalHistoryReady,
        beamHikariNeoNormalSteepSink,
        beamHikariNeoNormalTwoDayBonusWeak,
        beamHikariNeoNormalLowMiddleGames,
        beamHikariNeoNormalMediumUnpaid,
        beamHikariNeoNormalRotationReturn,
        beamHikariNeoNormalTreatmentDone,
        beamHikariNeoNormalTooStrong,
        beamHikariNeoNormalHighActivity,
        beamHikariNeoNormalLongNeglect,
        treatmentDone: beamHikariNeoNormalTreatmentDone,
        lowConfidence: !beamHikariNeoNormalLowMiddleGames,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    if (activeLogicKey === "apark-yakatabaru-neo-aim") {
      const yakatabaruNeoHistoryReady = historyRowCount >= 21;
      const yakatabaruNeoSinkStrength = features.recentThreeAngle <= -113 || streak >= 3;

      let yakatabaruNeoRotationCoreScore = 0;
      yakatabaruNeoRotationCoreScore += scoreInRange(daysSinceMachineHighContent, 11, 14, 9);
      yakatabaruNeoRotationCoreScore += scoreInRange(daysSinceMachineHighContent, 4, 10, 6);
      yakatabaruNeoRotationCoreScore += scoreInRange(daysSinceMachineHighContent, 3, 3, 3);
      yakatabaruNeoRotationCoreScore += scoreInRange(daysSinceMachineHighContent, 15, 21, 2);
      yakatabaruNeoRotationCoreScore +=
        recentFourteenNetTotal <= -4238 || recentTwentyOneNetTotal <= -5032
          ? 6
          : recentFourteenNetTotal <= -2736 || recentTwentyOneNetTotal <= -3210
            ? 4
            : 0;
      yakatabaruNeoRotationCoreScore += recentFourteenMachineHighContentCount === 1 ? 3 : 0;
      yakatabaruNeoRotationCoreScore += recentTwentyOneMachineHighContentCount === 2 ? 3 : 0;
      yakatabaruNeoRotationCoreScore = Math.min(yakatabaruNeoRotationCoreScore, 17);

      const yakatabaruNeoUnpaid =
        yakatabaruNeoRotationCoreScore >= 10 ||
        (Number.isFinite(daysSinceMachineHighContent) &&
          daysSinceMachineHighContent >= 4 &&
          daysSinceMachineHighContent <= 14 &&
          previousMachineHighContent &&
          (recentFourteenNetTotal <= -2736 || recentTwentyOneNetTotal <= -3210));
      const yakatabaruNeoPreviousFail =
        (previousMachineHighContent && previousDifference < 0) ||
        (previousGames >= 6000 && previousDifference < 0);
      const yakatabaruNeoNearbyLeftBehind =
        (recentThreeNetTotal < 0 && adjacentMachineNetTotal3Near2 > 0) ||
        (recentSevenNetTotal < 0 && adjacentMachineNetTotal7Near2 > 0);
      const yakatabaruNeoTreatmentDone =
        (previousMachineHighContent && previousDifference > 0) ||
        previousDifference >= 1800 ||
        recentThreeNetTotal > 2509 ||
        recentSevenNetTotal > 3704 ||
        (previousDifference > 0 &&
          features.previousCombinedDenominator <= 150 &&
          features.previousRbDenominator >= 350);
      const yakatabaruNeoLowConfidence = previousGames < 500 || recentThreeGamesTotal < 5000;
      const yakatabaruNeoOverheated =
        previousGames >= 7000 ||
        recentThreeGamesTotal > 17024 ||
        recentFourteenMachineHighContentCount >= 4;
      const boostFlags = [
        yakatabaruNeoSinkStrength,
        yakatabaruNeoUnpaid,
        yakatabaruNeoPreviousFail,
        yakatabaruNeoNearbyLeftBehind,
      ];
      const dangerFlags = [yakatabaruNeoTreatmentDone, yakatabaruNeoLowConfidence, yakatabaruNeoOverheated];

      return {
        ...features,
        yakatabaruNeoHistoryReady,
        yakatabaruNeoSinkStrength,
        yakatabaruNeoUnpaid,
        yakatabaruNeoPreviousFail,
        yakatabaruNeoNearbyLeftBehind,
        yakatabaruNeoTreatmentDone,
        yakatabaruNeoLowConfidence,
        yakatabaruNeoOverheated,
        treatmentDone: yakatabaruNeoTreatmentDone,
        lowConfidence: yakatabaruNeoLowConfidence,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    if (activeLogicKey === "mj-kurume-neo-aim") {
      const kurumeNeoHistoryReady = historyRowCount >= 14;
      const recentTwentyOneRbDenominator = rateDenominator(
        readNumber(metrics.recentTwentyOneGamesTotal),
        readNumber(metrics.recentTwentyOneRbTotal),
      );
      const kurumeNeoSinkStayStrong =
        recentFourteenMinus1800StayDays >= 7 ||
        recentTwentyOneMinus2000StayDays >= 7 ||
        recentFiveNetTotal <= -2000;
      const kurumeNeoStrongAngle =
        features.recentTwentyOneAngle <= -50 ||
        features.recentSevenAngle <= -150 ||
        features.recentFourteenAngle <= -50;
      const kurumeNeoGenuine =
        (recentFourteenNetTotal <= 0 && features.recentFourteenRbDenominator <= 300) ||
        (recentTwentyOneNetTotal <= 0 && recentTwentyOneRbDenominator <= 320);
      const kurumeNeoUnpaid =
        (previousDifference >= 1200 && recentTwentyOneNetTotal <= 1000) ||
        (previousDifference >= 800 && recentTwentyOneNetTotal <= 0);
      const kurumeNeoTrustedGames = recentFiveGamesTotal >= 10000 && recentFourteenGamesTotal >= 30000;
      const kurumeNeoTreatmentDone =
        recentFourteenNetTotal >= 3000 ||
        recentSevenNetTotal >= 3000 ||
        recentTwentyOneNetTotal >= 3000;
      const kurumeNeoLowGameSink = recentFiveGamesTotal < 8000 && recentFourteenGamesTotal < 25000;
      const kurumeNeoLongNeglect =
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent > 21 &&
        recentFourteenNetTotal > -1000 &&
        streak < 3;
      const kurumeNeoBbOnly =
        previousDifference >= 1000 &&
        features.previousRbDenominator >= 400 &&
        features.previousCombinedDenominator >= 150;
      const kurumeNeoRecentShowWeak =
        recentSevenNetTotal >= 2000 &&
        features.recentSevenRbDenominator >= 360;
      const boostFlags = [
        kurumeNeoSinkStayStrong,
        kurumeNeoStrongAngle,
        kurumeNeoGenuine,
        kurumeNeoUnpaid,
        kurumeNeoTrustedGames,
      ];
      const dangerFlags = [
        kurumeNeoTreatmentDone,
        kurumeNeoLowGameSink,
        kurumeNeoLongNeglect,
        kurumeNeoBbOnly,
        kurumeNeoRecentShowWeak,
      ];

      return {
        ...features,
        kurumeNeoHistoryReady,
        kurumeNeoSinkStayStrong,
        kurumeNeoStrongAngle,
        kurumeNeoGenuine,
        kurumeNeoUnpaid,
        kurumeNeoTrustedGames,
        kurumeNeoTreatmentDone,
        kurumeNeoLowGameSink,
        kurumeNeoLongNeglect,
        kurumeNeoBbOnly,
        kurumeNeoRecentShowWeak,
        treatmentDone: kurumeNeoTreatmentDone,
        lowConfidence: kurumeNeoLowGameSink,
        boostCount: boostFlags.filter(Boolean).length,
        dangerCount: dangerFlags.filter(Boolean).length,
      };
    }

    const aimThreeSinkStayDays = readNumber(metrics.recentThreeMinus1700StayDays);
    const aimShortSinkStay2 = aimThreeSinkStayDays >= 2;
    const aimShortSinkStay3 = aimThreeSinkStayDays >= 3;

    return {
      ...features,
      aimShortSinkStay2,
      aimShortSinkStay3,
    };
  }

  if (machineKey === "girls") {
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
    if (activeLogicKey === "beam-hikari-gogo-normal" || activeLogicKey === "beam-hikari-gogo-event") {
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
        activeLogicKey === "beam-hikari-gogo-normal" && streak >= 6,
      ];

      return {
        ...features,
        beamHikariGogoHistoryReady,
        beamHikariGogoNormalHistoryReady,
        beamHikariGogoEventHistoryReady,
        beamHikariGogoPreviousWeak,
        beamHikariGogoBonusWeak,
        beamHikariGogoAngleStrong,
        beamHikariGogoTwentyOneSinkBand,
        beamHikariGogoRecentThreeGamesMiddle,
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
    if (activeLogicKey === "beam-hikari-my-normal" || activeLogicKey === "beam-hikari-my-event") {
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
    if (activeLogicKey === "beam-hikari-funky-normal" || activeLogicKey === "beam-hikari-funky-event") {
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
  const daysSinceMachineHighContent = readNullableNumber(metrics.daysSinceMachineHighContent);
  const daysSinceMachineStrongHighContent = readNullableNumber(metrics.daysSinceMachineStrongHighContent);
  const daysSinceMachineBigWin1500 = readNullableNumber(metrics.daysSinceMachineBigWin1500);
  const previousMachineHighContent = Boolean(metrics.previousMachineHighContent);
  const previousMachineGoodContent = Boolean(metrics.previousMachineGoodContent);
  const previousMachineStrongHighContent = Boolean(metrics.previousMachineStrongHighContent);
  const machineHighContentStreak = readNumber(metrics.machineHighContentStreak);
  const machineGoodContentStreak = readNumber(metrics.machineGoodContentStreak);
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
  const previousAdjacentMachineGoodContentCount = readNumber(metrics.previousAdjacentMachineGoodContentCount);
  const previousAdjacentMachineBigWin1000Count = readNumber(metrics.previousAdjacentMachineBigWin1000Count);
  const previousAdjacentMachineNetTotal = readNumber(metrics.previousAdjacentMachineNetTotal);
  const previousOtherMachineHighContentCount = readNumber(metrics.previousOtherMachineHighContentCount);
  const sameMachinePreviousNetTotal = readNumber(metrics.sameMachinePreviousNetTotal);
  const previousCombinedDenominator = features.previousCombinedDenominator;
  const previousRbDenominator = features.previousRbDenominator;
  const recentTwoCombinedDenominator = rateDenominator(recentTwoGamesTotal, recentTwoBonusTotal);

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

    if (activeLogicKey === "beam-hikari-neo-aim-event" || activeLogicKey === "beam-hikari-neo-aim-normal") {
      if (historyRowCount < 21) {
        return 0;
      }

      const recentTwoRbTotal = readNumber(metrics.recentTwoRbTotal);
      const recentTwoRbDenominator = rateDenominator(recentTwoGamesTotal, recentTwoRbTotal);
      let score = 0;

      if (activeLogicKey === "beam-hikari-neo-aim-event") {
        score += scoreInRange(recentTwoGamesTotal, 1000, 2499, 18);
        score += scoreInRange(recentTwoGamesTotal, 2500, 3499, 15.3);
        score += recentTwoGamesTotal <= 999 ? 11.7 : 0;
        score += scoreInRange(recentTwoGamesTotal, 3500, 4999, 8.1);
        score += scoreInRange(recentTwoGamesTotal, 5000, 5999, 3.6);

        score +=
          features.recentTwoAngle <= -500
            ? 9
            : features.recentTwoAngle <= -400
              ? 10
              : features.recentTwoAngle <= -300
                ? 8.5
                : features.recentTwoAngle <= -200
                  ? 5.5
                  : features.recentTwoAngle <= -100
                    ? 3
                    : features.recentTwoAngle <= 0
                      ? 1.5
                      : 0;

        if (recentTwoCombinedDenominator >= 264 || recentTwoRbDenominator >= 869) {
          score += 16.2;
        } else if (recentTwoCombinedDenominator >= 226 || recentTwoRbDenominator >= 673) {
          score += 18;
        } else if (recentTwoCombinedDenominator >= 194 || recentTwoRbDenominator >= 540) {
          score += 13.5;
        } else if (recentTwoCombinedDenominator >= 178 || recentTwoRbDenominator >= 468) {
          score += 8.1;
        } else if (recentTwoCombinedDenominator >= 168 || recentTwoRbDenominator >= 423) {
          score += 4.5;
        }

        score += scoreInRange(recentSevenGamesTotal, 15000, 25000, 14);
        score += scoreInRange(recentSevenGamesTotal, 10000, 14999, 9.8);
        score += scoreInRange(recentSevenGamesTotal, 25001, 30000, 7);
        score += recentSevenGamesTotal < 10000 ? 2.8 : 0;
        score -= recentSevenGamesTotal > 30000 ? 4.2 : 0;

        if (features.recentSevenCombinedDenominator >= 180 || features.recentSevenRbDenominator >= 500) {
          score += 11;
        } else if (features.recentSevenCombinedDenominator >= 165 || features.recentSevenRbDenominator >= 400) {
          score += 8.25;
        } else if (features.recentSevenCombinedDenominator >= 155 || features.recentSevenRbDenominator >= 350) {
          score += 4.4;
        }

        if (!Number.isFinite(daysSinceMachineHighContent)) {
          score += 2.8;
        } else if (daysSinceMachineHighContent === 3) {
          score += 7;
        } else if (daysSinceMachineHighContent >= 4 && daysSinceMachineHighContent <= 9) {
          score += 5.25;
        } else if (daysSinceMachineHighContent >= 18) {
          score += 4.55;
        } else if (daysSinceMachineHighContent >= 10 && daysSinceMachineHighContent <= 17) {
          score += 3.15;
        }

        if (previousDifference < 0) {
          score +=
            previousAdjacentMachineHighContentCount >= 2
              ? 7
              : previousAdjacentMachineNetTotal >= 1563
                ? 4.9
                : previousAdjacentMachineHighContentCount >= 1
                  ? 2.1
                  : 0;
        }

        score += previousDifference <= -669 ? 8 : previousDifference <= -345 ? 5.6 : previousDifference < 0 ? 2.4 : 0;
        score +=
          previousCombinedDenominator >= 234 || previousRbDenominator >= 617
            ? 4
            : previousCombinedDenominator >= 184 || previousRbDenominator >= 452
              ? 2.4
              : 0;

        score -= recentTwoNetTotal >= 1500 ? 8 : recentTwoNetTotal >= 1000 ? 6 : 0;
        score -= previousDifference >= 1500 ? 6.4 : previousDifference >= 500 ? 4.4 : 0;
        score -=
          recentTwoCombinedDenominator <= 140 || recentTwoRbDenominator <= 300
            ? 10
            : recentTwoCombinedDenominator <= 150 || recentTwoRbDenominator <= 330
              ? 7.5
              : 0;
        score -= previousMachineHighContent ? 8 : 0;
        score -= recentSevenMachineHighContentCount >= 2 ? 5 : 0;
        score -= recentTwoGamesTotal >= 10000 ? 6 : recentTwoGamesTotal >= 8000 ? 4.8 : recentTwoGamesTotal >= 6000 ? 2.4 : 0;
        score -= previousGames >= 5350 ? 2.4 : 0;

        return Math.round(clamp(score, 0, 100));
      }

      score += scoreAtMost(features.recentTwoAngle, [
        { maximum: -500, points: 32 },
        { maximum: -400, points: 26 },
        { maximum: -300, points: 20 },
        { maximum: -200, points: 13 },
        { maximum: -100, points: 7 },
        { maximum: 0, points: 3 },
      ]);

      if (recentTwoCombinedDenominator >= 264 || recentTwoRbDenominator >= 869) {
        score += 22;
      } else if (recentTwoCombinedDenominator >= 226 || recentTwoRbDenominator >= 673) {
        score += 18;
      } else if (recentTwoCombinedDenominator >= 194 || recentTwoRbDenominator >= 540) {
        score += 14;
      } else if (recentTwoCombinedDenominator >= 178 || recentTwoRbDenominator >= 468) {
        score += 9;
      } else if (recentTwoCombinedDenominator >= 168 || recentTwoRbDenominator >= 423) {
        score += 5;
      }

      score += scoreInRange(recentTwoGamesTotal, 1000, 2499, 15);
      score += scoreInRange(recentTwoGamesTotal, 2500, 3499, 13);
      score += scoreInRange(recentTwoGamesTotal, 500, 999, 9);
      score += scoreInRange(recentTwoGamesTotal, 3500, 4999, 8);
      score += recentTwoGamesTotal <= 499 ? 5 : 0;
      score += scoreInRange(recentTwoGamesTotal, 5000, 5999, 3);

      score += scoreInRange(streak, 2, 4, 12);
      score += scoreInRange(streak, 5, 7, 8);
      score += streak >= 8 ? 4 : 0;
      score += streak === 1 ? 4 : 0;

      if (!Number.isFinite(daysSinceMachineHighContent)) {
        score += 3;
      } else if (daysSinceMachineHighContent === 3) {
        score += 10;
      } else if (daysSinceMachineHighContent >= 6 && daysSinceMachineHighContent <= 17) {
        score += 7;
      } else if (daysSinceMachineHighContent >= 4 && daysSinceMachineHighContent <= 5) {
        score += 5;
      } else if (daysSinceMachineHighContent >= 18) {
        score += 1;
      }

      const middleSinkScore = Math.min(
        10,
        scoreAtMost(features.recentFiveAngle, [
          { maximum: -179, points: 9 },
          { maximum: -122, points: 7 },
          { maximum: -78, points: 4 },
        ]) +
          scoreAtMost(recentFiveNetTotal, [
            { maximum: -3000, points: 6 },
            { maximum: -2000, points: 5 },
            { maximum: -1000, points: 2 },
          ]) +
          scoreAtMost(features.recentSevenAngle, [
            { maximum: -147, points: 5 },
            { maximum: -99, points: 3 },
          ]),
      );
      score += middleSinkScore;

      score += previousDifference <= -669 ? 5 : previousDifference <= -345 ? 3 : previousDifference < 0 ? 1 : 0;
      score +=
        previousCombinedDenominator >= 234 || previousRbDenominator >= 617
          ? 4
          : previousCombinedDenominator >= 184 || previousRbDenominator >= 452
            ? 2
            : 0;
      if (previousDifference < 0) {
        score += previousAdjacentMachineHighContentCount >= 2 ? 2 : previousAdjacentMachineNetTotal >= 1563 ? 1 : 0;
      }
      score += recentSevenMachineHighContentCount === 0 ? 3 : recentSevenMachineHighContentCount === 1 ? 1 : 0;

      score -= recentTwoNetTotal >= 1500 ? 26 : recentTwoNetTotal >= 1000 ? 19 : 0;
      score -= previousDifference >= 1500 ? 22 : previousDifference >= 500 ? 16 : 0;
      score -=
        recentTwoCombinedDenominator <= 140 || recentTwoRbDenominator <= 300
          ? 20
          : recentTwoCombinedDenominator <= 150 || recentTwoRbDenominator <= 330
            ? 14
            : 0;
      score -= recentTwoGamesTotal >= 10000 ? 12 : recentTwoGamesTotal >= 8000 ? 8 : recentTwoGamesTotal >= 6000 ? 4 : 0;
      score -= previousGames >= 5350 ? 7 : previousGames >= 4500 ? 4 : 0;
      score -= previousMachineStrongHighContent ? 18 : previousMachineHighContent ? 14 : 0;
      score -= recentSevenMachineHighContentCount >= 3 ? 8 : recentSevenMachineHighContentCount >= 2 ? 5 : 0;

      return Math.round(clamp(score, 0, 100));
    }

    if (activeLogicKey === "apark-yakatabaru-neo-aim") {
      if (historyRowCount < 21) {
        return 0;
      }

      let angleScore = 0;
      angleScore += scoreAtMost(features.recentThreeAngle, [
        { maximum: -178, points: 24 },
        { maximum: -113, points: 17 },
        { maximum: -60, points: 10 },
        { maximum: -0.000001, points: 4 },
      ]);
      angleScore += features.recentFiveAngle <= -121 ? 6 : 0;
      angleScore += features.recentSevenAngle <= -97 ? 4 : 0;
      angleScore = Math.min(angleScore, 30);

      let losingScore = scoreAtLeast(streak, [
        { minimum: 5, points: 22 },
        { minimum: 4, points: 17 },
        { minimum: 3, points: 13 },
        { minimum: 2, points: 8 },
        { minimum: 1, points: 1 },
      ]);
      losingScore += previousDifference <= -1200 ? 3 : 0;
      losingScore += previousGames >= 6000 && previousDifference < 0 ? 4 : 0;
      losingScore = Math.min(losingScore, 22);

      let previousFailScore = 0;
      previousFailScore += previousMachineHighContent && previousDifference < 0 ? 14 : 0;
      previousFailScore += previousGames >= 6000 && previousDifference < 0 ? 10 : 0;
      previousFailScore += previousMachineStrongHighContent && previousDifference < 0 ? 4 : 0;
      previousFailScore = Math.min(previousFailScore, 18);

      let rotationScore = 0;
      rotationScore += scoreInRange(daysSinceMachineHighContent, 11, 14, 9);
      rotationScore += scoreInRange(daysSinceMachineHighContent, 4, 10, 6);
      rotationScore += scoreInRange(daysSinceMachineHighContent, 3, 3, 3);
      rotationScore += scoreInRange(daysSinceMachineHighContent, 15, 21, 2);
      rotationScore +=
        recentFourteenNetTotal <= -4238 || recentTwentyOneNetTotal <= -5032
          ? 6
          : recentFourteenNetTotal <= -2736 || recentTwentyOneNetTotal <= -3210
            ? 4
            : 0;
      rotationScore += recentFourteenMachineHighContentCount === 1 ? 3 : 0;
      rotationScore += recentTwentyOneMachineHighContentCount === 2 ? 3 : 0;
      rotationScore = Math.min(rotationScore, 17);

      const gamesScore =
        recentThreeGamesTotal <= 11340
          ? 6
          : recentThreeGamesTotal <= 14125
            ? 4
            : recentThreeGamesTotal <= 17024
              ? 2
              : 0;

      let nearbyScore = 0;
      nearbyScore += recentThreeNetTotal < 0 && adjacentMachineNetTotal3Near2 > 0 ? 6 : 0;
      nearbyScore += recentSevenNetTotal < 0 && adjacentMachineNetTotal7Near2 > 0 ? 4 : 0;
      nearbyScore += recentFourteenNetTotal < 0 && adjacentMachineNetTotal14Near2 > 0 ? 2 : 0;
      nearbyScore = Math.min(nearbyScore, 7);

      return Math.round(
        clamp(angleScore + losingScore + previousFailScore + rotationScore + gamesScore + nearbyScore, 0, 100),
      );
    }

    if (activeLogicKey === "mj-kurume-neo-aim") {
      const recentTwentyOneRbDenominator = rateDenominator(
        recentTwentyOneGamesTotal,
        readNumber(metrics.recentTwentyOneRbTotal),
      );

      let sinkScore = 0;
      sinkScore += scoreAtMost(recentFiveNetTotal, [
        { maximum: -2000, points: 18 },
        { maximum: -1500, points: 12 },
        { maximum: -1000, points: 8 },
        { maximum: -500, points: 4 },
      ]);
      sinkScore += scoreAtMost(recentFourteenNetTotal, [
        { maximum: -2000, points: 14 },
        { maximum: -1000, points: 10 },
        { maximum: 0, points: 6 },
      ]);
      sinkScore += scoreAtMost(recentTwentyOneNetTotal, [
        { maximum: -3000, points: 10 },
        { maximum: -2000, points: 8 },
        { maximum: 0, points: 4 },
      ]);
      sinkScore = Math.min(sinkScore, 35);

      let angleScore = 0;
      angleScore += scoreAtMost(features.recentSevenAngle, [
        { maximum: -150, points: 7 },
        { maximum: -100, points: 5 },
        { maximum: -50, points: 3 },
      ]);
      angleScore += scoreAtMost(features.recentFourteenAngle, [
        { maximum: -50, points: 6 },
        { maximum: 0, points: 3 },
      ]);
      angleScore += scoreAtMost(features.recentTwentyOneAngle, [
        { maximum: -50, points: 6 },
        { maximum: 0, points: 3 },
      ]);
      angleScore = Math.min(angleScore, 15);

      let genuineScore = 0;
      genuineScore +=
        recentFourteenNetTotal <= 0 && features.recentFourteenRbDenominator <= 300
          ? 12
          : recentFourteenNetTotal <= 0 && features.recentFourteenRbDenominator <= 340
            ? 8
            : 0;
      genuineScore +=
        recentTwentyOneNetTotal <= 0 && recentTwentyOneRbDenominator <= 320
          ? 8
          : recentTwentyOneNetTotal <= 0 && recentTwentyOneRbDenominator <= 340
            ? 5
            : 0;
      genuineScore +=
        previousDifference >= 1200 && recentTwentyOneNetTotal <= 1000
          ? 8
          : previousDifference >= 800 && recentTwentyOneNetTotal <= 0
            ? 6
            : 0;
      genuineScore += previousMachineHighContent && previousDifference <= 1500 ? 4 : 0;
      genuineScore = Math.min(genuineScore, 25);

      let gamesScore = 0;
      gamesScore += recentFiveGamesTotal >= 10000 ? 4 : recentFiveGamesTotal >= 8000 ? 2 : 0;
      gamesScore += recentFourteenGamesTotal >= 30000 ? 4 : recentFourteenGamesTotal >= 25000 ? 2 : 0;
      gamesScore += previousGames >= 1500 ? 2 : 0;
      gamesScore = Math.min(gamesScore, 10);

      let rotationScore = 0;
      rotationScore += scoreAtLeast(streak, [
        { minimum: 7, points: 12 },
        { minimum: 5, points: 9 },
        { minimum: 4, points: 6 },
        { minimum: 3, points: 4 },
      ]);
      rotationScore += recentFourteenMachineHighContentCount === 0 ? 4 : 0;
      rotationScore += scoreInRange(daysSinceMachineHighContent, 15, 20, 4);
      rotationScore +=
        Number.isFinite(daysSinceMachineHighContent) &&
        daysSinceMachineHighContent > 21 &&
        recentFourteenNetTotal <= -1000
          ? 2
          : 0;
      rotationScore = Math.min(rotationScore, 15);

      let penalty = 0;
      penalty += scoreAtLeast(recentFourteenNetTotal, [
        { minimum: 4700, points: 14 },
        { minimum: 3000, points: 8 },
        { minimum: 2000, points: 5 },
      ]);
      penalty += scoreAtLeast(recentSevenNetTotal, [
        { minimum: 3000, points: 8 },
        { minimum: 2000, points: 5 },
      ]);
      penalty += scoreAtLeast(recentTwentyOneNetTotal, [
        { minimum: 3000, points: 6 },
        { minimum: 1500, points: 3 },
      ]);
      penalty += features.recentSevenAngle >= 100 ? 5 : 0;
      penalty += recentFiveGamesTotal < 8000 && sinkScore >= 20 ? 5 : 0;
      penalty = Math.min(penalty, 25);

      return Math.round(clamp(sinkScore + angleScore + genuineScore + gamesScore + rotationScore - penalty, 0, 100));
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
    if (activeLogicKey === "beam-hikari-gogo-normal" || activeLogicKey === "beam-hikari-gogo-event") {
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

      if (activeLogicKey === "beam-hikari-gogo-event") {
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
    if (activeLogicKey === "beam-hikari-my-normal" || activeLogicKey === "beam-hikari-my-event") {
      if (historyRowCount < 21) {
        return 0;
      }

      let score = 0;

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
    if (activeLogicKey === "beam-hikari-funky-normal" || activeLogicKey === "beam-hikari-funky-event") {
      if (historyRowCount < 21) {
        return 0;
      }

      const recentThreeCombinedDenominator = features.recentThreeCombinedDenominator;
      const recentThreeRbDenominator = features.recentThreeRbDenominator;
      const recentFiveAngle = features.recentFiveAngle;
      const recentSevenAngle = features.recentSevenAngle;
      const recentThreeAngle = features.recentThreeAngle;
      let score = 15;

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

  if (machineKey === "monkey") {
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
  if (Number.isFinite(matcher.rankMax) && (!Number.isFinite(evaluation.rank) || evaluation.rank > matcher.rankMax)) {
    return false;
  }
  if (
    Number.isFinite(matcher.minNextGap) &&
    (!Number.isFinite(evaluation.nextGap) || evaluation.nextGap < matcher.minNextGap)
  ) {
    return false;
  }
  if (Number.isFinite(matcher.minBoost) && evaluation.boostCount < matcher.minBoost) {
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
    !["neo-aim", "funky", "gogo", "my"].includes(definition?.machineKey)
  ) {
    return setting;
  }
  const targetDate = options?.snapshot?.nextBusinessDate ?? options?.snapshot?.baseDate ?? options?.snapshot?.date ?? "";
  return buildBeamHikariDateSetting(definition, targetDate) ?? setting;
}

function buildEvaluationForRow(row, settingByMachineKey, options = {}) {
  const definition = findMachineDefinition(row?.machineName);
  const rawSetting = definition ? settingByMachineKey.get(definition.machineKey) : null;
  const setting = definition ? resolveRankingDateSpecificSetting(definition, rawSetting, options) : null;
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
    score,
    rank: null,
    nextGap: null,
    boostCount: features.boostCount,
    dangerCount: features.dangerCount,
    matchesAdoption: false,
    features,
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

function compareMachineEvaluationRows(left, right) {
  const leftScore = readNullableNumber(left?.machineEvaluation?.score);
  const rightScore = readNullableNumber(right?.machineEvaluation?.score);
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

function attachMachineEvaluationRanks(rows) {
  const rowsByMachineName = new Map();

  for (const row of rows) {
    if (!row?.machineEvaluation) {
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
    const sortedRows = [...machineRows].sort(compareMachineEvaluationRows);
    sortedRows.forEach((row, index) => {
      const nextRow = sortedRows[index + 1] ?? null;
      const score = readNullableNumber(row?.machineEvaluation?.score);
      const nextScore = readNullableNumber(nextRow?.machineEvaluation?.score);
      contextByRowKey.set(normalizeText(row?.rowKey), {
        rank: index + 1,
        nextGap: score !== null && nextScore !== null ? score - nextScore : null,
      });
    });
  }

  return rows.map((row) => {
    const evaluation = row?.machineEvaluation;
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
      machineEvaluation: {
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
    }));

    return {
      ...snapshot,
      rows: attachMachineEvaluationRanks(rowsWithEvaluation),
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
