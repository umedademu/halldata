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
  const tier = [...tiers].sort((left, right) => left.maximum - right.maximum).find((item) => value <= item.maximum);
  return tier?.points ?? 0;
}

function scoreAtLeast(value, tiers) {
  if (!Number.isFinite(value)) {
    return 0;
  }
  const tier = [...tiers].sort((left, right) => right.minimum - left.minimum).find((item) => value >= item.minimum);
  return tier?.points ?? 0;
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
          maxDanger: 1,
          anyFlags: ["stayCore", "strongAngle", "deepSink"],
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
        "1位＋80点以上＋強化2個以上＋危険0",
        "53件 / 104.59% / RB1/269.4",
        {
          rankMax: 1,
          minScore: 80,
          minBoost: 2,
          maxDanger: 0,
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
          minScore: 50,
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
        "65点以上＋沈み核あり＋危険回避",
        "284件 / 105.73%",
        {
          minScore: 65,
          maxDanger: 1,
          anyFlags: ["deepSink", "stayCore", "strongAngle"],
        },
      ),
    ],
  },
  {
    machineKey: "monkey",
    machineNames: ["スマスロモンキーターンV", "スマスロ モンキーターンV", "スマスロモンキーターンⅤ"],
    logicKey: "apark-monkey",
    logicName: "モンキー春日式",
    profile: "smart",
    defaultConditionSuffix: "main",
    conditions: [
      buildCondition(
        "main",
        "1位＋65点以上＋強化2個以上＋危険0",
        "157件 / 105.59%",
        {
          rankMax: 1,
          minScore: 65,
          minBoost: 2,
          maxDanger: 0,
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
        "1位＋65点以上＋弱ボーナス＋次点差12点以上＋危険なし",
        "26件 / 105.47% / RB1/440.3",
        {
          rankMax: 1,
          minScore: 65,
          minNextGap: 12,
          maxDanger: 0,
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
        "1位＋70点以上＋短期沈み滞在2日以上",
        "132件 / 105.61% / RB1/271.3",
        {
          rankMax: 1,
          minScore: 70,
          requiredFlags: ["shortSinkStay2"],
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
          maxDanger: 1,
          requiredFlags: ["staleRest", "weakBonus"],
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
        "42件 / 105.98% / RB1/280.5",
        {
          rankMax: 1,
          minScore: 70,
          requiredFlags: ["losingStreak3To6", "highRest8To14"],
        },
      ),
      buildCondition(
        "wide",
        "1位＋70点以上",
        "107件 / 103.79% / RB1/314.4",
        {
          rankMax: 1,
          minScore: 70,
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
          minScore: 58,
          minNextGap: 3,
          maxDanger: 1,
          requiredFlags: ["losingStreak3", "recentFiveLoss3500"],
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
          minScore: 55,
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

function calculateMachineScore(definition, metrics, features) {
  const profile = definition?.profile ?? "juggler";
  const machineKey = definition?.machineKey ?? "";
  const previousDifference = readNumber(metrics.todayDifference);
  const previousGames = readNumber(metrics.previousGames);
  const recentThreeNetTotal = readNumber(metrics.recentThreeNetTotal);
  const recentFiveNetTotal = readNumber(metrics.recentFiveNetTotal);
  const recentSixNetTotal = readNumber(metrics.recentSixNetTotal);
  const recentSevenNetTotal = readNumber(metrics.recentSevenNetTotal);
  const recentTenNetTotal = readNumber(metrics.recentTenNetTotal);
  const recentFourteenNetTotal = readNumber(metrics.recentFourteenNetTotal);
  const recentTwentyOneNetTotal = readNumber(metrics.recentTwentyOneNetTotal);
  const lossAbsTotal = readNumber(metrics.lossAbsTotal);
  const streak = readNumber(metrics.streak);
  const historyNetTotal = readNumber(metrics.historyNetTotal);
  const historyPositiveDays = readNumber(metrics.historyPositiveDays);
  const recentFiveGamesTotal = readNumber(metrics.recentFiveGamesTotal);
  const recentSevenGamesTotal = readNumber(metrics.recentSevenGamesTotal);
  const recentTenGamesTotal = readNumber(metrics.recentTenGamesTotal);
  const recentFourteenGamesTotal = readNumber(metrics.recentFourteenGamesTotal);
  const recentFiveHighSettingCandidateCount = readNumber(metrics.recentFiveHighSettingCandidateCount);
  const recentSevenHighSettingCandidateCount = readNumber(metrics.recentSevenHighSettingCandidateCount);
  const highSettingCandidateStreak = readNumber(metrics.highSettingCandidateStreak);
  const recentThreeHighSettingEstimateCount = readNumber(metrics.recentThreeHighSettingEstimateCount);

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
    bonusScore += features.previousHighContent ? 9 : 0;
    bonusScore +=
      !features.previousHighContent &&
      previousGames >= 4000 &&
      features.previousCombinedDenominator <= 145 &&
      features.previousRbDenominator <= 315
        ? 6
        : 0;
    bonusScore += recentThreeHighSettingEstimateCount >= 1 ? 5 : 0;
    bonusScore += recentSevenHighSettingCandidateCount >= 2 ? 4 : 0;
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
    let score = 30;
    score += scoreInRange(features.recentFiveAngle, -205, -75, 30);
    score += recentFiveGamesTotal >= 19617 && features.recentFiveAngle < -205 ? 4 : 0;
    score += recentFiveNetTotal <= -3180 && recentFiveGamesTotal >= 19617 ? 6 : 0;
    score += recentSevenNetTotal <= -4485 && recentSevenGamesTotal >= 27704 ? 8 : 0;
    score += recentTenNetTotal <= -5225 && recentTenGamesTotal >= 39745 ? 8 : 0;
    score += features.shortSinkStay3 ? 6 : features.shortSinkStay2 ? 3 : 0;
    score += features.previousHighContent && previousDifference >= 0 && previousDifference < 927 ? 10 : 0;
    score += features.previousStrongHighContent && previousDifference <= 0 ? 6 : 0;
    score += previousGames >= 4816 && features.previousCombinedDenominator <= 434 ? 3 : 0;
    score += previousDifference > 0 && recentFourteenNetTotal < 0 ? 8 : 0;
    score += previousDifference >= 927 && recentFourteenNetTotal < 0 ? 4 : 0;
    score += streak >= 3 && streak <= 5 ? 6 : 0;
    score += streak >= 6 && streak <= 8 ? 14 : 0;
    score += streak >= 9 ? 5 : 0;
    score += Number.isFinite(features.bestRestDays) && features.bestRestDays >= 7 && features.bestRestDays <= 10 ? 4 : 0;
    score += recentTenGamesTotal >= 39745 ? 3 : 0;
    score += previousGames >= 3488 ? 2 : 0;

    score -= Number.isFinite(features.bestRestDays) && features.bestRestDays >= 2 && features.bestRestDays <= 6 ? 12 : 0;
    score -= recentFourteenNetTotal >= 4807 ? 18 : 0;
    score -= recentFourteenNetTotal < 4807 && recentSevenNetTotal >= 3350 ? 12 : 0;
    score -= recentFourteenNetTotal >= 9847 ? 8 : 0;
    score -= features.previousHighContent && previousDifference >= 927 ? 14 : 0;
    score -= !features.previousHighContent && previousDifference >= 1520 ? 6 : 0;
    score -= recentTenGamesTotal < 34862 ? 8 : 0;

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
  const features = buildFeatureState(metrics);
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
    readNumber(left?.rank, Number.MAX_SAFE_INTEGER) -
      readNumber(right?.rank, Number.MAX_SAFE_INTEGER) ||
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
