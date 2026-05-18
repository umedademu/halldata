const HUNT_MACHINE_CATEGORY_ORDER = [
  "juggler",
  "hana",
  "hanabi",
  "okidoki",
  "large",
  "medium",
  "small",
  "other",
];
const HUNT_MACHINE_CATEGORY_LABELS = {
  juggler: "ジャグ系",
  hana: "ハナ系",
  hanabi: "ハナビ系",
  okidoki: "沖ドキ系",
  large: "10台以上",
  medium: "3~9台",
  small: "2台以下",
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
    category: "other",
    names: ["スマスロ北斗の拳 転生の章", "スマスロ北斗の拳 転生の章2"],
  },
  {
    shortName: "ゴッド",
    category: "other",
    names: ["スマスロ ミリオンゴッド", "スマスロ ミリオンゴッド-神々の軌跡-"],
  },
  {
    shortName: "東京喰種",
    category: "other",
    names: ["L東京喰種"],
  },
  {
    shortName: "モンキーV",
    category: "other",
    names: ["スマスロモンキーターンV", "スマスロ モンキーターンV"],
  },
  {
    shortName: "カバネリ",
    category: "other",
    names: ["スマスロ 甲鉄城のカバネリ 海門決戦"],
  },
  {
    shortName: "北斗",
    category: "other",
    names: ["Lスマスロ北斗の拳", "スマスロ北斗の拳"],
  },
  {
    shortName: "炎炎2",
    category: "other",
    names: ["Lパチスロ炎炎ノ消防隊2", "Lパチスロ炎炎ノ消防隊２"],
  },
  {
    shortName: "沖ドキGOLD",
    category: "okidoki",
    names: ["沖ドキ！ＧＯＬＤ", "沖ドキ！ＧＯＬＤ-30", "沖ドキ!GOLD", "沖ドキ!GOLD-30"],
  },
  {
    shortName: "沖ドキDUOアン",
    category: "okidoki",
    names: ["スマスロ 沖ドキ!DUO アンコール", "スマスロ沖ドキ!DUOアンコール", "L沖ドキ!DUO アンコール"],
  },
  {
    shortName: "沖ドキBLACK",
    category: "okidoki",
    names: ["沖ドキ！BLACK", "沖ドキ!BLACK"],
  },
  {
    shortName: "沖ドキ豪25",
    category: "okidoki",
    names: ["沖ドキ!ゴージャス 25Φ", "沖ドキ!ゴージャス", "沖ドキ！ゴージャス"],
  },
  {
    shortName: "沖ドキ豪30",
    category: "okidoki",
    names: ["沖ドキ!ゴージャス 30Φ"],
  },
  {
    shortName: "沖ドキDUO",
    category: "okidoki",
    names: ["沖ドキ！DUO", "沖ドキ！DUO-30", "沖ドキ!DUO", "沖ドキ!DUO-30"],
  },
  {
    shortName: "沖ドキ2",
    category: "okidoki",
    names: ["沖ドキ！２-30", "沖ドキ!2-30", "沖ドキ！2-30"],
  },
  {
    shortName: "からくり",
    category: "other",
    names: ["Lパチスロからくりサーカス", "L パチスロからくりサーカス"],
  },
  {
    shortName: "かぐや",
    category: "other",
    names: ["Lパチスロ かぐや様は告らせたい", "Lパチスロかぐや様は告らせたい"],
  },
  {
    shortName: "ヴヴヴ2",
    category: "other",
    names: ["Lパチスロ革命機ヴァルヴレイヴ2", "Lパチスロ革命機ヴァルヴレイヴ２"],
  },
  {
    shortName: "ヴヴヴ",
    category: "other",
    names: ["Lパチスロ革命機ヴァルヴレイヴ"],
  },
  {
    shortName: "炎炎",
    category: "other",
    names: ["Lパチスロ炎炎ノ消防隊", "L パチスロ炎炎ノ消防隊", "L炎炎ノ消防隊"],
  },
  {
    shortName: "ダンベル",
    category: "other",
    names: ["Lパチスロ ダンベル何キロ持てる？", "Lパチスロダンベル何キロ持てる？"],
  },
  {
    shortName: "ガンダムSEED",
    category: "other",
    names: ["Lパチスロ 機動戦士ガンダムSEED", "Lパチスロ機動戦士ガンダムSEED"],
  },
  {
    shortName: "ユニコーン",
    category: "other",
    names: [
      "Lパチスロ 機動戦士ガンダムユニコーン 覚醒DRIVE",
      "機動戦士ガンダムユニコーン 覚醒DRIVE",
      "Lパチスロ機動戦士ガンダムユニコーン覚醒DRIVE",
      "L機動戦士ガンダムユニコーン覚醒",
    ],
  },
  {
    shortName: "ありふれ",
    category: "other",
    names: ["Lパチスロ ありふれた職業で世界最強", "Lパチスロありふれた職業で世界最強"],
  },
  {
    shortName: "シンエヴァ",
    category: "other",
    names: ["Lパチスロ シン・エヴァンゲリオン", "Lパチスロシン・エヴァンゲリオン"],
  },
  {
    shortName: "シンフォ",
    category: "other",
    names: ["Lパチスロ戦姫絶唱シンフォギア 正義の歌"],
  },
  {
    shortName: "ガルパン",
    category: "other",
    names: ["Lパチスロガールズ＆パンツァー 最終章", "Lパチスロガールズ&パンツァー 最終章"],
  },
  {
    shortName: "うみねこ",
    category: "other",
    names: ["Lパチスロうみねこのなく頃に2", "Lパチスロうみねこのなく頃に２"],
  },
  {
    shortName: "閃乱カグラ",
    category: "other",
    names: ["Lパチスロ閃乱カグラ2 SHINOVI MASTER", "Lパチスロ閃乱カグラ２ SHINOVI MASTER"],
  },
  {
    shortName: "ベルセルク",
    category: "other",
    names: ["Lパチスロ ベルセルク無双", "Lパチスロ　ベルセルク無双"],
  },
  {
    shortName: "うる星",
    category: "other",
    names: ["Lパチスロうる星やつら"],
  },
  {
    shortName: "マクロスF4",
    category: "other",
    names: ["Lパチスロ マクロスフロンティア4", "Lパチスロマクロスフロンティア4"],
  },
  {
    shortName: "花の慶次",
    category: "other",
    names: ["Lパチスロ花の慶次～佐渡攻めの章～"],
  },
  {
    shortName: "攻殻",
    category: "other",
    names: ["スマスロ 攻殻機動隊", "スマスロ攻殻機動隊"],
  },
  {
    shortName: "バイオRE3",
    category: "other",
    names: [
      "スマスロ バイオハザードRE:3",
      "スマスロ バイオハザード RE:3",
      "スマスロバイオハザードRE:3",
      "スマスロバイオハザード RE:3",
      "スマスロ バイオハザードＲＥ：３",
      "スマスロバイオハザードＲＥ：３",
    ],
  },
  {
    shortName: "吉宗",
    category: "other",
    names: ["真打 吉宗", "真打吉宗"],
  },
  {
    shortName: "クレアBT",
    category: "other",
    names: [
      "クレアの秘宝伝～はじまりの扉と太陽の石～ボーナストリガーver.",
      "クレアの秘宝伝ボーナストリガーVER.A2",
      "⑳LB/クレアの秘宝伝ボーナストリガーVER.A2",
    ],
  },
  {
    shortName: "SHAKE",
    category: "other",
    names: ["SHAKE BONUS TRIGGER", "LB SHAKE BONUS TRIGGER"],
  },
  {
    shortName: "ニューパルSP4",
    category: "other",
    names: ["ニューパルサーSP4 with 太鼓の達人", "ニューパルサーＳＰ４ with 太鼓の達人"],
  },
  {
    shortName: "ニューパルBT",
    category: "other",
    names: ["スマスロニューパルサーBT", "スマスロ ニューパルサーBT"],
  },
  {
    shortName: "ディスクUR",
    category: "other",
    names: ["A-SLOT+ ディスクアップ ULTRAREMIX", "A-SLOT+ディスクアップ ULTRAREMIX"],
  },
  {
    shortName: "クランキー",
    category: "other",
    names: ["クランキークレスト"],
  },
  {
    shortName: "サンダーV",
    category: "other",
    names: ["スマスロ サンダーV", "LサンダーV", "スマスロサンダーV"],
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

export function getHuntMachineOptionLabel(machineName, slotCount) {
  const shortName = getHuntMachineShortName(machineName);
  const safeSlotCount = Number(slotCount);
  return Number.isFinite(safeSlotCount) && safeSlotCount > 0
    ? `${shortName}(${safeSlotCount})`
    : shortName;
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
  if (normalizedMachineName.includes("沖ドキ")) {
    return "okidoki";
  }
  if (
    normalizedMachineName.includes("スマスロ") ||
    normalizedMachineName.startsWith("L")
  ) {
    return "other";
  }
  return "other";
}

export function isHuntJugglerMachine(machineName) {
  return getHuntMachineCategory(machineName) === "juggler";
}

export function hasAimJugglerHuntMachineGroupOption(availableMachineNames) {
  return AIM_JUGGLER_MACHINE_NAMES.some((machineName) =>
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
    const slotCount = Number(machine.slotCount);
    const machineCategoryKey = getHuntMachineCategory(machine.name);
    let categoryKey = machineCategoryKey;
    if (machineCategoryKey === "other" && Number.isFinite(slotCount) && slotCount > 0) {
      if (slotCount >= 10) {
        categoryKey = "large";
      } else if (slotCount >= 3) {
        categoryKey = "medium";
      } else {
        categoryKey = "small";
      }
    }
    const safeCategoryKey = groupsByKey.has(categoryKey) ? categoryKey : "other";
    groupsByKey.get(safeCategoryKey).options.push({
      ...machine,
      shortName: getHuntMachineShortName(machine.name),
      optionLabel: getHuntMachineOptionLabel(machine.name, machine.slotCount),
      category: safeCategoryKey,
    });
  }

  return [...groupsByKey.values()].filter((group) => group.options.length > 0);
}
