const HUNT_MACHINE_CATEGORY_ORDER = ["juggler", "hana", "hanabi", "at", "other"];
const HUNT_MACHINE_CATEGORY_LABELS = {
  juggler: "ジャグ系",
  hana: "ハナ系",
  hanabi: "ハナビ系",
  at: "AT系",
  other: "その他",
};

const HUNT_MACHINE_DISPLAY_DEFINITIONS = [
  {
    shortName: "Sアイム",
    category: "juggler",
    names: ["SアイムジャグラーＥＸ", "SアイムジャグラーEX"],
  },
  {
    shortName: "ネオアイム",
    category: "juggler",
    names: ["ネオアイムジャグラーEX", "ネオアイムジャグラーＥＸ"],
  },
  {
    shortName: "アイム",
    category: "juggler",
    names: ["アイムジャグラーEX", "アイムジャグラーＥＸ"],
  },
  {
    shortName: "マイジャグ",
    category: "juggler",
    names: ["マイジャグラーV", "マイジャグラーⅤ", "マイジャグラー"],
  },
  {
    shortName: "ゴージャグ",
    category: "juggler",
    names: ["ゴーゴージャグラー３", "ゴーゴージャグラー3", "ゴーゴージャグラー"],
  },
  {
    shortName: "ファンキー",
    category: "juggler",
    names: [
      "ファンキージャグラー２ＫＴ",
      "ファンキージャグラー2KT",
      "ファンキージャグラー２",
      "ファンキージャグラー2",
      "ファンキージャグラー",
    ],
  },
  {
    shortName: "ミスター",
    category: "juggler",
    names: ["ミスタージャグラー"],
  },
  {
    shortName: "ガールズ",
    category: "juggler",
    names: ["ジャグラーガールズSS", "ジャグラーガールズ"],
  },
  {
    shortName: "ハッピー",
    category: "juggler",
    names: [
      "ハッピージャグラーＶＩＩＩ",
      "ハッピージャグラーVIII",
      "ハッピージャグラーＶ",
      "ハッピージャグラーV",
      "ハッピージャグラー",
    ],
  },
  {
    shortName: "ウルミラ",
    category: "juggler",
    names: ["ウルトラミラクルジャグラー"],
  },
  {
    shortName: "ハナホウオウ",
    category: "hana",
    names: [
      "ハナハナホウオウ",
      "ハナハナホウオウ-30",
      "ハナハナホウオウ‐30",
      "ハナハナホウオウ～天翔～-30",
      "ハナハナホウオウ～天翔～‐30",
    ],
  },
  {
    shortName: "ドラハナ",
    category: "hana",
    names: [
      "ドラゴンハナハナ～閃光～",
      "ドラゴンハナハナ",
      "ドラゴンハナハナ閃光",
      "ドラゴンハナハナ閃光30",
      "ドラゴンハナハナ～閃光～30",
      "ドラゴンハナハナ～閃光～-30",
      "ドラゴンハナハナ～閃光～‐30",
    ],
  },
  {
    shortName: "キンハナ",
    category: "hana",
    names: ["キングハナハナ", "キングハナハナ-30", "キングハナハナ‐30"],
  },
  {
    shortName: "ニューキン",
    category: "hana",
    names: [
      "ニューキングハナハナ",
      "ニューキングハナハナV",
      "ニューキングハナハナV-30",
      "ニューキングハナハナV‐30",
    ],
  },
  {
    shortName: "スタハナ",
    category: "hana",
    names: ["スターハナハナ", "スターハナハナ-30", "スターハナハナ‐30"],
  },
  {
    shortName: "新ハナビ",
    category: "hanabi",
    names: ["新ハナビ"],
  },
  {
    shortName: "スマハナビ",
    category: "hanabi",
    names: ["スマスロ ハナビ", "スマスロハナビ"],
  },
  {
    shortName: "ハナビ",
    category: "hanabi",
    names: ["ハナビ"],
  },
  {
    shortName: "北斗転生",
    category: "at",
    names: ["スマスロ北斗の拳 転生の章", "スマスロ北斗の拳 転生の章2"],
  },
  {
    shortName: "ゴッド",
    category: "at",
    names: ["スマスロ ミリオンゴッド", "スマスロ ミリオンゴッド-神々の軌跡-"],
  },
  {
    shortName: "東京喰種",
    category: "at",
    names: ["L東京喰種"],
  },
  {
    shortName: "モンキーV",
    category: "at",
    names: ["スマスロモンキーターンV", "スマスロ モンキーターンV"],
  },
  {
    shortName: "カバネリ",
    category: "at",
    names: ["スマスロ 甲鉄城のカバネリ 海門決戦"],
  },
  {
    shortName: "北斗",
    category: "at",
    names: ["Lスマスロ北斗の拳", "スマスロ北斗の拳"],
  },
  {
    shortName: "炎炎2",
    category: "at",
    names: ["Lパチスロ炎炎ノ消防隊2", "Lパチスロ炎炎ノ消防隊２"],
  },
];

const AIM_JUGGLER_GROUP_NAME = "アイムジャグラーEX";
const AIM_JUGGLER_MACHINE_NAMES = ["SアイムジャグラーＥＸ", "ネオアイムジャグラーEX"];
const HANABI_GROUP_NAME = "ハナビ";
const HANABI_MACHINE_NAMES = ["新ハナビ", "スマスロ ハナビ"];

function normalizeHuntMachineName(value) {
  return String(value ?? "").normalize("NFKC").replace(/\s+/gu, "").trim();
}

function findHuntMachineDisplayDefinition(machineName) {
  const normalizedMachineName = normalizeHuntMachineName(machineName);
  return (
    HUNT_MACHINE_DISPLAY_DEFINITIONS.find((definition) =>
      definition.names.some(
        (candidateName) => normalizeHuntMachineName(candidateName) === normalizedMachineName,
      ),
    ) ?? null
  );
}

function isHuntMachineInGroup(machineName, groupMachineNames) {
  return groupMachineNames.some(
    (candidateName) => normalizeHuntMachineName(candidateName) === normalizeHuntMachineName(machineName),
  );
}

export function getHuntMachineShortName(machineName) {
  return findHuntMachineDisplayDefinition(machineName)?.shortName ?? String(machineName ?? "").trim();
}

export function getHuntMachineCategory(machineName) {
  const definition = findHuntMachineDisplayDefinition(machineName);
  if (definition) {
    return definition.category;
  }

  const normalizedMachineName = normalizeHuntMachineName(machineName);
  if (normalizedMachineName.includes("ジャグラー")) {
    return "juggler";
  }
  if (normalizedMachineName.includes("ハナビ")) {
    return "hanabi";
  }
  if (normalizedMachineName.includes("ハナハナ")) {
    return "hana";
  }
  if (
    normalizedMachineName.includes("スマスロ") ||
    normalizedMachineName.startsWith("L")
  ) {
    return "at";
  }
  return "other";
}

export function isHuntJugglerMachine(machineName) {
  return getHuntMachineCategory(machineName) === "juggler";
}

export function hasAimJugglerHuntMachineGroupOption(availableMachineNames) {
  return AIM_JUGGLER_MACHINE_NAMES.every((machineName) =>
    (Array.isArray(availableMachineNames) ? availableMachineNames : []).some((availableMachineName) =>
      isHuntMachineInGroup(availableMachineName, [machineName]),
    ),
  );
}

export function hasHanabiHuntMachineGroupOption(availableMachineNames) {
  return HANABI_MACHINE_NAMES.every((machineName) =>
    (Array.isArray(availableMachineNames) ? availableMachineNames : []).some((availableMachineName) =>
      isHuntMachineInGroup(availableMachineName, [machineName]),
    ),
  );
}

export function resolveHuntMachineGroupName(machineName, options = {}) {
  const text = String(machineName ?? "").trim();
  if (options.combineAimJuggler && isHuntMachineInGroup(text, AIM_JUGGLER_MACHINE_NAMES)) {
    return AIM_JUGGLER_GROUP_NAME;
  }
  if (options.combineHanabi && isHuntMachineInGroup(text, HANABI_MACHINE_NAMES)) {
    return HANABI_GROUP_NAME;
  }
  return text;
}

export function groupHuntMachineOptions(machineOptions) {
  const groupsByKey = new Map(
    HUNT_MACHINE_CATEGORY_ORDER.map((categoryKey) => [
      categoryKey,
      {
        key: categoryKey,
        label: HUNT_MACHINE_CATEGORY_LABELS[categoryKey],
        options: [],
      },
    ]),
  );

  for (const machine of Array.isArray(machineOptions) ? machineOptions : []) {
    const categoryKey = getHuntMachineCategory(machine.name);
    const safeCategoryKey = groupsByKey.has(categoryKey) ? categoryKey : "other";
    groupsByKey.get(safeCategoryKey).options.push({
      ...machine,
      shortName: getHuntMachineShortName(machine.name),
      category: safeCategoryKey,
    });
  }

  return [...groupsByKey.values()].filter((group) => group.options.length > 0);
}
