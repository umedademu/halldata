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
    label: "共通条件のみ",
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

function buildCondition(keySuffix, name, backtestLabel, matcher) {
  return {
    keySuffix,
    name,
    backtestLabel,
    matcher,
  };
}

const MACHINE_EVALUATION_DEFINITIONS = [
  {
    machineKey: "aim",
    machineNames: ["SアイムジャグラーＥＸ", "SアイムジャグラーEX"],
    logicKey: "apark-aim",
    logicName: "Sアイム春日式",
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
      ),
    ],
  },
  {
    machineKey: "gogo",
    machineNames: ["ゴーゴージャグラー３", "ゴーゴージャグラー3", "ゴーゴージャグラー"],
    logicKey: "apark-gogo",
    logicName: "ゴージャグ春日式",
    profile: "juggler",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "1位＋3連敗以上＋前回高内容4〜14日＋3日角度強",
        "130件 / 103.66% / RB1/261.6",
        {
          rankMax: 1,
          minScore: 58,
          maxDanger: 1,
          requiredFlags: ["losingStreak3", "highRest4To14"],
          anyFlags: ["strongAngle", "deepSink"],
        },
      ),
    ],
  },
  {
    machineKey: "girls",
    machineNames: ["ジャグラーガールズSS", "ジャグラーガールズ"],
    logicKey: "apark-girls",
    logicName: "ガールズ春日式",
    profile: "juggler",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "1位＋70点以上＋次点差30点以上",
        "47件 / 103.30% / RB1/276.6",
        {
          rankMax: 1,
          minScore: 70,
          minNextGap: 30,
          maxDanger: 1,
        },
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
        "53件 / 104.59% / RB1/269.4",
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
        "20件 / 106.50% / RB1/383.2",
        {
          minScore: 50,
          maxDanger: 1,
          requiredFlags: ["strongAngle"],
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
        "32件 / 105.03% / RB1/314.2",
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
        "284件 / 105.73%",
        {
          rankMax: 2,
          requiredFlags: ["okidokiDuoHistoryReady"],
        },
      ),
      buildCondition(
        "top1",
        "1位",
        "142件 / 106.54%",
        {
          rankMax: 1,
          requiredFlags: ["okidokiDuoHistoryReady"],
        },
      ),
      buildCondition(
        "score65",
        "65点以上",
        "109件 / 104.71%",
        {
          minScore: 65,
          requiredFlags: ["okidokiDuoHistoryReady"],
        },
      ),
      buildCondition(
        "score70",
        "70点以上",
        "75件 / 107.74%",
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
        "181件 / 104.26%",
        {
          rankMax: 1,
          minNextGap: 10,
          requiredFlags: ["monkeyHistoryReady"],
        },
      ),
      buildCondition(
        "safe",
        "1位＋危険0",
        "138件 / 105.63%",
        {
          rankMax: 1,
          maxDanger: 0,
          requiredFlags: ["monkeyHistoryReady"],
        },
      ),
      buildCondition(
        "strong",
        "1位＋65点以上＋強化2個以上＋危険0",
        "99件 / 105.49%",
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
        "302件 / 105.45%",
        {
          minScore: 50,
        },
      ),
      buildCondition(
        "top4",
        "上位4台",
        "516件 / 104.58%",
        {
          rankMax: 4,
        },
      ),
      buildCondition(
        "top2",
        "上位2台",
        "258件 / 105.59%",
        {
          rankMax: 2,
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
        "26件 / 105.47% / RB1/440.3",
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
    profile: "juggler",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "1位＋70点以上＋3日沈み2日以上",
        "132件 / 105.61% / RB1/271.3",
        {
          rankMax: 1,
          minScore: 70,
          requiredFlags: ["aimShortSinkStay2"],
        },
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
        "41件 / 103.20% / RB1/366.8",
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
      ),
    ],
  },
  {
    machineKey: "my",
    machineNames: ["マイジャグラーV", "マイジャグラーⅤ", "マイジャグラー"],
    logicKey: "apark-my",
    logicName: "マイジャグ春日式",
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
        "94件 / 104.73% / RB1/288.6",
        {
          rankMax: 1,
          minScore: 60,
          minNextGap: 10,
          maxDanger: 1,
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
        "51件 / 105.00%",
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
        "105件 / 104.54%",
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
        "35件 / 104.11% / RB1/311.4",
        {
          minScore: 65,
          maxDanger: 2,
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
  MACHINE_EVALUATION_DEFINITIONS.map((definition) => [definition.logicKey, definition]),
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

function findConditionDefinition(definition, conditionKey) {
  if (!definition) {
    return null;
  }
  const normalizedConditionKey = normalizeText(conditionKey);
  return (
    definition.conditions.find((condition) => buildConditionKey(definition, condition) === normalizedConditionKey) ??
    null
  );
}

function getDefaultSetting(definition, storeName) {
  if (!definition || !isAparkKasugaStore(storeName)) {
    return {
      logicKey: "",
      conditionKey: "",
    };
  }

  const defaultCondition =
    definition.conditions.find((condition) => condition.keySuffix === definition.defaultConditionSuffix) ??
    definition.conditions[0] ??
    null;

  return {
    logicKey: definition.logicKey,
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
    ...(definition
      ? [
          {
            key: definition.logicKey,
            name: definition.logicName,
          },
        ]
      : []),
  ];
}

function buildConditionOptions(definition) {
  return [
    {
      key: "",
      name: "未設定",
      backtestLabel: "",
    },
    ...(definition
      ? definition.conditions.map((condition) => ({
          key: buildConditionKey(definition, condition),
          name: condition.name,
          backtestLabel: condition.backtestLabel,
        }))
      : []),
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
  const logicKey = requestedLogicKey === definition.logicKey ? requestedLogicKey : "";
  const conditionKey = findConditionDefinition(definition, requestedConditionKey)
    ? requestedConditionKey
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
    const currentSetting = normalizeSettingForDefinition(
      definition,
      overrideSetting ?? defaultSetting,
    );
    const defaultNormalizedSetting = normalizeSettingForDefinition(definition, defaultSetting);
    const condition = findConditionDefinition(definition, currentSetting.conditionKey);

    return {
      machineKey,
      machineName,
      hasDefinition: Boolean(definition),
      logicKey: currentSetting.logicKey,
      conditionKey: currentSetting.conditionKey,
      defaultLogicKey: defaultNormalizedSetting.logicKey,
      defaultConditionKey: defaultNormalizedSetting.conditionKey,
      logicOptions: buildLogicOptions(definition),
      conditionOptions: buildConditionOptions(definition),
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
  const historyRowCount = readNumber(metrics.historyRowCount);
  const previousDifference = readNumber(metrics.todayDifference);
  const previousGames = readNumber(metrics.previousGames);
  const streak = readNumber(metrics.streak);
  const recentThreeNetTotal = readNumber(metrics.recentThreeNetTotal);
  const recentFiveNetTotal = readNumber(metrics.recentFiveNetTotal);
  const recentSevenNetTotal = readNumber(metrics.recentSevenNetTotal);
  const recentTenNetTotal = readNumber(metrics.recentTenNetTotal);
  const recentFourteenNetTotal = readNumber(metrics.recentFourteenNetTotal);
  const recentTwentyOneNetTotal = readNumber(metrics.recentTwentyOneNetTotal);
  const recentTwentyEightNetTotal = readNumber(metrics.recentTwentyEightNetTotal);
  const recentFiftySixNetTotal = readNumber(metrics.recentFiftySixNetTotal);
  const recentThreeGamesTotal = readNumber(metrics.recentThreeGamesTotal);
  const recentFiveGamesTotal = readNumber(metrics.recentFiveGamesTotal);
  const recentSevenGamesTotal = readNumber(metrics.recentSevenGamesTotal);
  const recentTenGamesTotal = readNumber(metrics.recentTenGamesTotal);
  const recentFourteenGamesTotal = readNumber(metrics.recentFourteenGamesTotal);
  const recentFourteenGoldShowDays = readNumber(metrics.recentFourteenGoldShowDays);
  const recentFourteenWinDays = readNumber(metrics.recentFourteenWinDays);
  const recentFourteenHighSettingCandidateCount = readNumber(metrics.recentFourteenHighSettingCandidateCount);
  const recentSevenHighSettingCandidateCount = readNumber(metrics.recentSevenHighSettingCandidateCount);
  const highSettingCandidateStreak = readNumber(metrics.highSettingCandidateStreak);
  const adjacentHighSettingCandidateCount7 = readNumber(metrics.adjacentHighSettingCandidateCount7);
  const adjacentMachineHighContentCount7 = readNumber(metrics.adjacentMachineHighContentCount7);
  const adjacentMachineHighContentCount14 = readNumber(metrics.adjacentMachineHighContentCount14);
  const adjacentMachineNetTotal3 = readNumber(metrics.adjacentMachineNetTotal3);
  const adjacentMachineNetTotal7 = readNumber(metrics.adjacentMachineNetTotal7);
  const recentThreeMachineHighContentCount = readNumber(metrics.recentThreeMachineHighContentCount);
  const recentSevenMachineHighContentCount = readNumber(metrics.recentSevenMachineHighContentCount);
  const recentFourteenMachineHighContentCount = readNumber(metrics.recentFourteenMachineHighContentCount);
  const recentThirtyMachineHighContentCount = readNumber(metrics.recentThirtyMachineHighContentCount);
  const daysSinceMachineHighContent = readNullableNumber(metrics.daysSinceMachineHighContent);
  const daysSinceMachineBigWin1500 = readNullableNumber(metrics.daysSinceMachineBigWin1500);
  const previousMachineHighContent = Boolean(metrics.previousMachineHighContent);
  const previousMachineGoodContent = Boolean(metrics.previousMachineGoodContent);
  const previousMachineStrongHighContent = Boolean(metrics.previousMachineStrongHighContent);

  if (machineKey === "aim") {
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
    const aimThreeSinkStayDays = readNumber(metrics.recentThreeMinus1700StayDays);
    const aimShortSinkStay2 = aimThreeSinkStayDays >= 2;
    const aimShortSinkStay3 = aimThreeSinkStayDays >= 3;

    return {
      ...features,
      aimShortSinkStay2,
      aimShortSinkStay3,
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
    const boostFlags = [starStrongSinkStay, starStrongAngle, starPreviousCut, starNearbyLeftBehind];
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
      starNearbyLeftBehind,
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
      adjacentMachineHighContentCount14 >= 2 ||
      (recentFiveNetTotal < 0 && adjacentMachineHighContentCount14 > 0);
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
      features.previousStrongHighContent,
      highSettingCandidateStreak >= 2,
      recentFourteenGoldShowDays >= 6,
      recentFourteenHighSettingCandidateCount >= 6,
      recentSevenGamesTotal <= 23409,
      Number.isFinite(features.bestRestDays) && features.bestRestDays >= 26,
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
      features.previousCombinedDenominator >= 218,
      features.previousRbDenominator >= 539,
      previousGames < 2000,
      recentSevenHighSettingCandidateCount >= 4,
      adjacentHighSettingCandidateCount7 === 1,
    ];
    const boostFlags = [
      previousGames >= 2000 && features.previousCombinedDenominator <= 146 && features.previousRbDenominator <= 333,
      netPerThousandGames(recentTenNetTotal, recentTenGamesTotal) >= 30,
      Number.isFinite(features.bestRestDays) && features.bestRestDays >= 8 && features.bestRestDays <= 12,
      recentFourteenHighSettingCandidateCount === 3,
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

  return features;
}

function calculateMachineScore(definition, metrics, features) {
  const profile = definition?.profile ?? "juggler";
  const machineKey = definition?.machineKey ?? "";
  const previousDifference = readNumber(metrics.todayDifference);
  const previousGames = readNumber(metrics.previousGames);
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
  const lossAbsTotal = readNumber(metrics.lossAbsTotal);
  const streak = readNumber(metrics.streak);
  const winningStreak = readNumber(metrics.winningStreak);
  const historyNetTotal = readNumber(metrics.historyNetTotal);
  const historyPositiveDays = readNumber(metrics.historyPositiveDays);
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
  const recentFiveHighSettingCandidateCount = readNumber(metrics.recentFiveHighSettingCandidateCount);
  const recentSevenHighSettingCandidateCount = readNumber(metrics.recentSevenHighSettingCandidateCount);
  const recentFourteenHighSettingCandidateCount = readNumber(metrics.recentFourteenHighSettingCandidateCount);
  const highSettingCandidateStreak = readNumber(metrics.highSettingCandidateStreak);
  const recentThreeHighSettingEstimateCount = readNumber(metrics.recentThreeHighSettingEstimateCount);
  const recentThreeStrictHighContentDays = readNumber(metrics.recentThreeStrictHighContentDays);
  const recentSevenStrictHighContentDays = readNumber(metrics.recentSevenStrictHighContentDays);
  const recentSevenMinus2000StayDays = readNumber(metrics.recentSevenMinus2000StayDays);
  const recentThreeMinus1700StayDays = readNumber(metrics.recentThreeMinus1700StayDays);
  const recentSevenMinus3000StayDays = readNumber(metrics.recentSevenMinus3000StayDays);
  const recentFiveMinus1500StayDays = readNumber(metrics.recentFiveMinus1500StayDays);
  const recentFiveMinus2000StayDays = readNumber(metrics.recentFiveMinus2000StayDays);
  const recentFiveMinus3000StayDays = readNumber(metrics.recentFiveMinus3000StayDays);
  const recentFiveMinus3500StayDays = readNumber(metrics.recentFiveMinus3500StayDays);
  const recentSevenMinus1500StayDays = readNumber(metrics.recentSevenMinus1500StayDays);
  const recentFiveMinus500StayDays = readNumber(metrics.recentFiveMinus500StayDays);
  const recentTenMinus5225StayDays = readNumber(metrics.recentTenMinus5225StayDays);
  const recentFourteenMinus3218StayDays = readNumber(metrics.recentFourteenMinus3218StayDays);
  const recentTwentyOneMinus11333StayDays = readNumber(metrics.recentTwentyOneMinus11333StayDays);
  const recentFiveAngleMinus80StayDays = readNumber(metrics.recentFiveAngleMinus80StayDays);
  const recentThreeMachineHighContentCount = readNumber(metrics.recentThreeMachineHighContentCount);
  const recentFiveMachineHighContentCount = readNumber(metrics.recentFiveMachineHighContentCount);
  const recentSevenMachineHighContentCount = readNumber(metrics.recentSevenMachineHighContentCount);
  const recentTenMachineHighContentCount = readNumber(metrics.recentTenMachineHighContentCount);
  const recentFourteenMachineHighContentCount = readNumber(metrics.recentFourteenMachineHighContentCount);
  const recentThirtyMachineHighContentCount = readNumber(metrics.recentThirtyMachineHighContentCount);
  const recentSevenMachineGoodContentCount = readNumber(metrics.recentSevenMachineGoodContentCount);
  const recentSevenMachineWeakContentCount = readNumber(metrics.recentSevenMachineWeakContentCount);
  const daysSinceMachineHighContent = readNullableNumber(metrics.daysSinceMachineHighContent);
  const daysSinceMachineBigWin1500 = readNullableNumber(metrics.daysSinceMachineBigWin1500);
  const previousMachineHighContent = Boolean(metrics.previousMachineHighContent);
  const previousMachineGoodContent = Boolean(metrics.previousMachineGoodContent);
  const previousMachineStrongHighContent = Boolean(metrics.previousMachineStrongHighContent);
  const machineHighContentStreak = readNumber(metrics.machineHighContentStreak);
  const machineGoodContentStreak = readNumber(metrics.machineGoodContentStreak);
  const adjacentMachineHighContentCount7 = readNumber(metrics.adjacentMachineHighContentCount7);
  const adjacentMachineHighContentCount14 = readNumber(metrics.adjacentMachineHighContentCount14);
  const adjacentMachineNetTotal3 = readNumber(metrics.adjacentMachineNetTotal3);
  const adjacentMachineNetTotal7 = readNumber(metrics.adjacentMachineNetTotal7);
  const previousCombinedDenominator = features.previousCombinedDenominator;
  const previousRbDenominator = features.previousRbDenominator;
  const recentTwoCombinedDenominator = rateDenominator(recentTwoGamesTotal, recentTwoBonusTotal);

  if (machineKey === "aim") {
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
    restScore += scoreInRange(features.bestRestDays, 4, 14, 16);
    restScore += scoreInRange(features.bestRestDays, 15, 30, 8);
    restScore += features.bestRestDays >= 31 ? 4 : 0;
    restScore -= scoreInRange(features.bestRestDays, 1, 3, 8);
    restScore -= recentSevenHighSettingCandidateCount >= 2 ? 5 : 0;

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
    penalty += previousDifference > 1000 && !features.previousHighContent ? 10 : 0;
    penalty += previousDifference > 1500 && features.previousRbDenominator > 420 ? 6 : 0;

    return Math.round(clamp(sinkScore + angleScore + restScore + weakScore + activityScore - penalty, 0, 100));
  }

  if (machineKey === "my") {
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
    let score = 0;
    score += scoreAtLeast(streak, [
      { minimum: 4, points: 28 },
      { minimum: 3, points: 22 },
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
    score += scoreAtMost(features.recentThreeAngle, [
      { maximum: -175, points: 10 },
      { maximum: -100, points: 7 },
    ]);
    score += recentThreeNetTotal <= -1800 ? 4 : 0;
    score += scoreInRange(features.bestRestDays, 11, 20, 14);
    score += scoreInRange(features.bestRestDays, 6, 10, 9);
    score += scoreInRange(features.bestRestDays, 21, 40, 4);
    score += scoreInRange(features.bestRestDays, 3, 5, 3);
    score += scoreInRange(features.bestRestDays, 1, 2, 1);
    score += recentSevenHighSettingCandidateCount === 0 ? 6 : recentSevenHighSettingCandidateCount === 1 ? 1 : 0;
    score -= recentSevenHighSettingCandidateCount >= 2 ? 8 : 0;
    score += scoreInRange(recentSevenGamesTotal, 25000, 35000, 7);
    score += scoreInRange(recentSevenGamesTotal, 35001, 42000, 4);
    score += recentSevenGamesTotal >= 42001 ? 1 : 0;
    score -= recentSevenGamesTotal < 20000 ? 3 : 0;
    score += features.previousHighContent && previousDifference <= 500 ? 8 : 0;

    score -= features.previousHighContent && previousDifference >= 2500 ? 16 : 0;
    score -= recentThreeNetTotal >= 2500 ? 14 : 0;
    score -= recentSevenNetTotal >= 3500 ? 12 : recentSevenNetTotal >= 3000 ? 10 : 0;
    score -= recentFourteenNetTotal >= 5000 ? 6 : 0;
    score -= features.bestRestDays >= 41 ? 6 : 0;

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
    restScore += scoreAtLeast(features.bestRestDays, [
      { minimum: 30, points: 5 },
      { minimum: 22, points: 3 },
      { minimum: 16, points: 6 },
      { minimum: 12, points: 4 },
      { minimum: 9, points: 2 },
    ]);
    restScore += recentSevenHighSettingCandidateCount === 0 ? 2 : 0;
    restScore += recentFourteenHighSettingCandidateCount === 0 ? 1 : 0;
    restScore = Math.min(restScore, 12);

    let repayScore = 0;
    repayScore += scoreAtMost(recentFourteenNetTotal, [
      { maximum: -2487, points: 5 },
      { maximum: -1400, points: 4 },
    ]);
    repayScore += recentTwentyOneNetTotal <= -2450 ? 3 : 0;
    repayScore += recentTwentyEightNetTotal <= -2700 ? 2 : 0;
    repayScore += previousDifference > 1000 && recentTwentyEightNetTotal < 0 ? 3 : 0;
    repayScore += features.previousStrongHighContent && previousDifference <= 1000 ? 3 : 0;
    repayScore += previousCombinedDenominator <= 135 && previousRbDenominator <= 290 && previousDifference < 0 ? 2 : 0;
    repayScore = Math.min(repayScore, 16);

    let penalty = 0;
    penalty += recentSevenNetTotal > 2500 ? 14 : 0;
    penalty += recentFourteenNetTotal > 3600 ? 10 : 0;
    penalty += recentTwentyOneNetTotal > 4716 ? 6 : 0;
    penalty += features.previousHighContent && previousDifference > 500 ? 8 : 0;
    penalty += previousDifference > 1500 ? 5 : 0;
    penalty += recentSevenHighSettingCandidateCount >= 2 ? 5 : 0;
    penalty += features.bestRestDays >= 45 && recentSevenNetTotal > -1000 ? 6 : 0;

    return Math.round(clamp(shortSinkScore + streakScore + angleScore + restScore + repayScore - penalty, 0, 100));
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
    score += streak === 2 ? 1 : streak === 3 ? 4 : streak >= 4 && streak <= 5 ? 6 : streak >= 6 && streak <= 7 ? 8 : streak >= 8 ? 2 : 0;
    score += scoreInRange(features.bestRestDays, 8, 14, 2);
    score += scoreInRange(features.bestRestDays, 15, 30, 4);
    score += scoreInRange(features.bestRestDays, 31, 60, 1);

    score -= recentSevenNetTotal > 2000 ? 15 : recentSevenNetTotal > 1000 ? 10 : recentSevenNetTotal > 0 ? 6 : 0;
    score -= features.recentSevenAngle > 80 ? 8 : features.recentSevenAngle > 30 ? 5 : 0;
    score -= previousDifference > 2500 ? 8 : previousDifference > 1500 ? 5 : 0;
    score -= features.previousHighContent ? 4 : 0;
    score -= features.recentSevenCombinedDenominator <= 160 ? 6 : 0;
    score -= recentSevenGamesTotal >= 30000 ? 3 : 0;

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
    score += scoreInRange(features.bestRestDays, 4, 12, 8);
    score += scoreInRange(features.bestRestDays, 13, 20, 3);
    score += scoreInRange(features.bestRestDays, 2, 3, 2);
    score += previousDifference > 0 && previousDifference < 1000 && recentFourteenNetTotal < 0 ? 7 : 0;
    score += previousDifference > 0 && previousDifference < 1500 && recentFourteenNetTotal < 0 ? 4 : 0;

    score -= features.previousHighContent ? 16 : 0;
    score -= previousDifference >= 2000 ? 22 : previousDifference >= 1500 ? 14 : 0;
    score -= recentThreeHighSettingEstimateCount >= 1 ? 5 : 0;
    score -= streak >= 6 ? 20 : streak >= 4 ? 10 : 0;
    score -= features.bestRestDays <= 1 ? 8 : 0;
    score -= features.bestRestDays > 20 ? 8 : 0;
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
    score += scoreInRange(features.bestRestDays, 0, 2, 5);
    score += scoreInRange(features.bestRestDays, 8, 12, 6);
    score += scoreInRange(features.bestRestDays, 13, 17, 2);
    score += recentFourteenHighSettingCandidateCount === 3 ? 8 : 0;
    score -= recentFourteenHighSettingCandidateCount === 2 ? 4 : 0;
    score -= recentFourteenHighSettingCandidateCount >= 4 ? 6 : 0;
    score += recentSevenHighSettingCandidateCount === 3 ? 4 : 0;
    score -= recentSevenHighSettingCandidateCount >= 4 ? 8 : 0;
    score += scoreInRange(previousGames, 2000, 2999, 8);
    score += scoreInRange(previousGames, 3000, 6499, 3);
    score -= previousGames < 2000 ? 8 : 0;
    score -= previousGames >= 6500 ? 4 : 0;
    score += scoreInRange(recentSevenGamesTotal, 22000, 30000, 4);
    score -= recentSevenGamesTotal < 19000 ? 5 : 0;
    score -= streak >= 4 && streak <= 7 ? 5 : 0;
    score += streak >= 8 ? 3 : 0;
    score += readNumber(metrics.adjacentHighSettingCandidateCount7) === 0 ? 2 : 0;
    score -= readNumber(metrics.adjacentHighSettingCandidateCount7) === 1 ? 4 : 0;

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
    rotationScore += scoreInRange(daysSinceMachineHighContent, 5, 10, 5);
    rotationScore += scoreInRange(daysSinceMachineHighContent, 14, 21, 5);
    rotationScore += scoreInRange(daysSinceMachineHighContent, 3, 5, 2);
    rotationScore -= Number.isFinite(daysSinceMachineHighContent) && daysSinceMachineHighContent > 21 ? 3 : 0;
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
    nearbyScore += adjacentMachineHighContentCount14 >= 2 && recentFourteenMachineHighContentCount === 0 ? 6 : 0;
    nearbyScore += adjacentMachineHighContentCount14 > 0 && recentFiveNetTotal < 0 ? 2 : 0;
    nearbyScore += adjacentMachineHighContentCount14 === 0 ? 1 : 0;
    nearbyScore = Math.min(nearbyScore, 7);

    let gamesScore = 0;
    gamesScore += recentThreeGamesTotal >= 4000 ? 3 : 0;
    gamesScore += recentFiveGamesTotal >= 7000 && recentFiveNetTotal < 0 ? 3 : 0;
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
    score += recentFourteenHighSettingCandidateCount === 0 ? 5 : recentFourteenHighSettingCandidateCount === 1 ? 3 : 0;
    score += scoreInRange(features.bestRestDays, 17, 25, 6);
    score += features.bestRestDays === 3 ? 3 : features.bestRestDays === 2 ? 2 : features.bestRestDays === 1 ? -7 : 0;
    score += features.bestRestDays >= 26 ? -4 : 0;
    score += features.previousHighContent && previousDifference < 0 ? 6 : 0;

    score -= previousDifference >= 2173 ? 8 : 0;
    score -= features.previousStrongHighContent ? 6 : 0;
    score -= features.previousHighContent ? 4 : 0;
    score -= highSettingCandidateStreak >= 2 ? 10 : 0;
    score -= recentFourteenGoldShowDays >= 6 ? 9 : 0;
    score -= recentFourteenHighSettingCandidateCount >= 6 ? 10 : 0;
    score -= recentFourteenNetTotal >= 5463 ? 4 : 0;
    score -= recentFiftySixNetTotal >= 10077 ? 3 : 0;
    score -= recentSevenGamesTotal <= 23409 ? 5 : 0;
    score -= previousGames <= 2003 ? 3 : 0;

    return Math.round(clamp(score, 0, 100));
  }

  if (machineKey === "funky") {
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
    if (highSettingCandidateStreak >= 2) {
      streakScore += 12;
    } else if (recentThreeHighSettingEstimateCount >= 2) {
      streakScore += 5;
    } else if (recentFiveHighSettingCandidateCount >= 3) {
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
    penalty += recentSevenHighSettingCandidateCount === 0 && !Number.isFinite(features.bestRestDays) ? 12 : 0;
    penalty += Number.isFinite(features.bestRestDays) && features.bestRestDays >= 21 ? 12 : 0;
    penalty += Number.isFinite(features.bestRestDays) && features.bestRestDays >= 13 && features.bestRestDays <= 20 ? 5 : 0;
    penalty += features.previousHighContent && previousDifference >= 0 ? 8 : 0;
    penalty += scoreAtLeast(recentFiveNetTotal, [
      { minimum: 5558, points: 14 },
      { minimum: 4355, points: 8 },
    ]);
    penalty += scoreAtLeast(recentFourteenNetTotal, [
      { minimum: 7746, points: 10 },
      { minimum: 6097, points: 6 },
    ]);
    penalty += recentSevenHighSettingCandidateCount >= 2 && highSettingCandidateStreak < 2 ? 4 : 0;
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
    activityScore += recentSevenGamesTotal >= 30000 ? 3 : 0;
    activityScore += recentSevenGamesTotal >= 35000 ? 2 : 0;
    activityScore = Math.min(activityScore, 25);

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
    showScore += features.recentSevenBigShowDays >= 3 ? 7 : 0;
    showScore += features.previousBigShow ? 5 : 0;
    showScore = Math.min(showScore, 22);

    let bonusScore = 0;
    bonusScore += previousStrictHighContent ? 9 : 0;
    bonusScore +=
      !previousStrictHighContent &&
      previousGames >= 4000 &&
      features.previousCombinedDenominator <= 145 &&
      features.previousRbDenominator <= 315
        ? 6
        : 0;
    bonusScore += recentThreeStrictHighContentDays >= 1 ? 5 : 0;
    bonusScore += recentSevenStrictHighContentDays >= 2 ? 4 : 0;
    bonusScore = Math.min(bonusScore, 18);

    let penalty = 0;
    penalty += previousGames < 2000 ? 8 : 0;
    penalty += readNumber(metrics.recentThreeGamesTotal) < 8000 ? 7 : 0;
    penalty += recentThreeNetTotal <= -2500 && features.recentThreeAngle <= -150 ? 10 : 0;
    penalty += streak >= 3 && readNumber(metrics.recentThreeGamesTotal) < 12000 ? 6 : 0;
    penalty += Number.isFinite(features.bestRestDays) && features.bestRestDays >= 60 && recentFourteenGamesTotal < 40000 ? 6 : 0;
    penalty += recentThreeNetTotal >= 4000 && recentThreeHighSettingEstimateCount === 0 && features.previousRbDenominator > 400 ? 5 : 0;
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
    if (Number.isFinite(features.bestRestDays)) {
      if (features.bestRestDays >= 7 && features.bestRestDays <= 10) {
        restScore = 12;
      } else if (features.bestRestDays >= 4 && features.bestRestDays <= 6) {
        restScore = 6;
      } else if (features.bestRestDays >= 11 && features.bestRestDays <= 20) {
        restScore = 3;
      } else if (features.bestRestDays >= 21) {
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
      features.previousHighContent && previousDifference < -1000
        ? 4
        : features.previousHighContent && previousDifference < 0
          ? 2
          : 0;
    const adjacentScore = readNumber(metrics.adjacentHighSettingCandidateCount7) > 0 ? 2 : 0;

    let penalty = 0;
    penalty += recentSevenNetTotal > 5420 ? 10 : 0;
    penalty += recentFourteenNetTotal > 10000 ? 8 : 0;
    penalty += previousDifference >= 2500 ? 8 : 0;
    penalty += features.previousHighContent && previousDifference >= 2000 ? 8 : 0;
    penalty += features.previousHighContent && previousDifference >= 4000 ? 4 : 0;
    penalty += recentSevenHighSettingCandidateCount >= 4 ? 12 : 0;
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

function buildEvaluationForRow(row, settingByMachineKey) {
  const definition = findMachineDefinition(row?.machineName);
  const setting = definition ? settingByMachineKey.get(definition.machineKey) : null;
  if (!definition || !setting?.logicKey) {
    return null;
  }

  const metrics = row?.machineEvaluationMetrics ?? {};
  const features = buildMachineSpecificFeatureState(definition, metrics, buildFeatureState(metrics));
  const condition = findConditionDefinition(definition, setting.conditionKey);
  const score = calculateMachineScore(definition, metrics, features);

  return {
    machineKey: definition.machineKey,
    logicKey: definition.logicKey,
    logicName: definition.logicName,
    conditionKey: condition ? buildConditionKey(definition, condition) : "",
    conditionName: condition?.name ?? "",
    backtestLabel: condition?.backtestLabel ?? "",
    score,
    rank: null,
    nextGap: null,
    boostCount: features.boostCount,
    dangerCount: features.dangerCount,
    matchesAdoption: false,
    features,
  };
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
    const condition = findConditionDefinition(definition, updatedEvaluation.conditionKey);
    return {
      ...row,
      machineEvaluation: {
        ...updatedEvaluation,
        matchesAdoption: matchesCondition(condition?.matcher, updatedEvaluation),
      },
    };
  });
}

export function decorateSnapshotsWithMachineEvaluation(snapshots, settingRows = []) {
  if (!hasAnyConfiguredSetting(settingRows)) {
    return snapshots;
  }

  const settingByMachineKey = buildSettingByMachineKey(settingRows);
  return (Array.isArray(snapshots) ? snapshots : []).map((snapshot) => {
    const rowsWithEvaluation = (Array.isArray(snapshot?.rows) ? snapshot.rows : []).map((row) => ({
      ...row,
      machineEvaluation: buildEvaluationForRow(row, settingByMachineKey),
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
