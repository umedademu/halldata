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
    names: [
      "スマスロ 甲鉄城のカバネリ 海門決戦",
      "パチスロ　甲鉄城のカバネリ",
      "パチスロ 甲鉄城のカバネリ",
    ],
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
  {
    shortName: "異世界BT",
    category: "other",
    names: [
      "A-SLOT+異世界かるてっとBT",
      "A-SLOT+ 異世界かるてっとBT",
      "A-SLOT+異世界かるてっとＢＴ",
    ],
  },
  {
    shortName: "ToLOVEる",
    category: "other",
    names: ["L ToLOVEるダークネス", "LToLOVEるダークネス"],
  },
  {
    shortName: "アズレン",
    category: "other",
    names: ["Lアズールレーン THE ANIMATION", "L アズールレーン THE ANIMATION"],
  },
  {
    shortName: "虚構推理",
    category: "other",
    names: ["L虚構推理", "L 虚構推理"],
  },
  {
    shortName: "いざ番長",
    category: "other",
    names: ["いざ！番長", "いざ番長"],
  },
  {
    shortName: "アレックス",
    category: "other",
    names: ["アレックス ブライト", "アレックスブライト"],
  },
  {
    shortName: "ゴッドイーター",
    category: "other",
    names: ["スマスロ ゴッドイーター リザレクション", "スマスロゴッドイーターリザレクション"],
  },
  {
    shortName: "ゴブスレII",
    category: "other",
    names: ["スマスロ ゴブリンスレイヤーⅡ", "スマスロゴブリンスレイヤーⅡ", "スマスロ ゴブリンスレイヤーII"],
  },
  {
    shortName: "マギレコ",
    category: "other",
    names: [
      "スマスロ マギアレコード 魔法少女まどか☆マギカ外伝",
      "スマスロマギアレコード魔法少女まどか☆マギカ外伝",
    ],
  },
  {
    shortName: "モンハンR",
    category: "other",
    names: ["スマスロ モンスターハンターライズ", "スマスロモンスターハンターライズ"],
  },
  {
    shortName: "東リベ",
    category: "other",
    names: ["スマスロ 東京リベンジャーズ", "スマスロ東京リベンジャーズ"],
  },
  {
    shortName: "ヨルムン",
    category: "other",
    names: ["スマスロヨルムンガンド", "スマスロ ヨルムンガンド"],
  },
  {
    shortName: "化物語",
    category: "other",
    names: ["スマスロ化物語", "スマスロ 化物語"],
  },
  {
    shortName: "新鬼武者3",
    category: "other",
    names: ["スマスロ新鬼武者3", "スマスロ 新鬼武者3", "スマスロ新鬼武者３"],
  },
  {
    shortName: "秘宝伝",
    category: "other",
    names: ["スマスロ秘宝伝", "スマスロ 秘宝伝"],
  },
  {
    shortName: "鉄拳6",
    category: "other",
    names: ["スマスロ鉄拳6", "スマスロ 鉄拳6", "スマスロ鉄拳６"],
  },
  {
    shortName: "バーサス",
    category: "other",
    names: ["バーサスリヴァイズ", "バーサス リヴァイズ"],
  },
];

export const AIM_JUGGLER_GROUP_NAME = "アイムジャグラーEX";
export const AIM_JUGGLER_MACHINE_NAMES = ["SアイムジャグラーＥＸ", "ネオアイムジャグラーEX"];
export const HANABI_GROUP_NAME = "ハナビ";
export const HANABI_MACHINE_NAMES = ["新ハナビ", "スマスロ ハナビ"];
const COMBINED_HUNT_MACHINE_GROUPS = [
  {
    key: "aimJuggler",
    groupName: AIM_JUGGLER_GROUP_NAME,
    machineNames: AIM_JUGGLER_MACHINE_NAMES,
    shortName: "アイム系",
    category: "juggler",
    optionKey: "combineAimJuggler",
    allowPartial: true,
  },
  {
    key: "hanabi",
    groupName: HANABI_GROUP_NAME,
    machineNames: HANABI_MACHINE_NAMES,
    shortName: "ハナビ系",
    category: "hanabi",
    optionKey: "combineHanabi",
    allowPartial: false,
  },
];

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

function isSameHuntMachineName(left, right) {
  return normalizeHuntMachineName(left) === normalizeHuntMachineName(right);
}

function findCombinedHuntMachineGroup(machineName) {
  return (
    COMBINED_HUNT_MACHINE_GROUPS.find(
      (group) =>
        isSameHuntMachineName(machineName, group.groupName) ||
        isHuntMachineInGroup(machineName, group.machineNames),
    ) ?? null
  );
}

function hasCombinedHuntMachineGroupOption(group, availableMachineNames) {
  const matchingCount = group.machineNames.filter((machineName) =>
    (Array.isArray(availableMachineNames) ? availableMachineNames : []).some((availableMachineName) =>
      isHuntMachineInGroup(availableMachineName, [machineName]),
    ),
  ).length;
  return matchingCount >= (group.allowPartial ? 1 : group.machineNames.length);
}

function buildCombinedHuntMachineOption(group, machineOptions, options = {}) {
  const memberOptions = (Array.isArray(machineOptions) ? machineOptions : []).filter((machine) =>
    isHuntMachineInGroup(machine?.name, group.machineNames),
  );
  const directGroupChecked = (Array.isArray(machineOptions) ? machineOptions : []).some(
    (machine) => isSameHuntMachineName(machine?.name, group.groupName) && Boolean(machine?.checked),
  );
  if (!hasCombinedHuntMachineGroupOption(group, memberOptions.map((machine) => machine.name))) {
    return null;
  }

  const optionEnabled = Boolean(options?.[group.optionKey]);
  const allAvailableMembersChecked =
    memberOptions.length > 0 && memberOptions.every((machine) => Boolean(machine?.checked));
  const slotCount = memberOptions.reduce((sum, machine) => {
    const value = Number(machine?.slotCount);
    return Number.isFinite(value) && value > 0 ? sum + value : sum;
  }, 0);
  const checked = optionEnabled && (allAvailableMembersChecked || directGroupChecked);

  return {
    name: group.groupName,
    checked,
    slotCount: slotCount > 0 ? slotCount : null,
    shortName: group.shortName,
    optionLabel:
      slotCount > 0
        ? `${group.shortName}(${slotCount})`
        : group.shortName,
    category: group.category,
    combinedGroupKey: group.key,
    combinedRole: "group",
    combinedMemberNames: group.machineNames,
  };
}

function buildFallbackHuntMachineShortName(machineName) {
  const originalText = String(machineName ?? "").trim();
  if (!originalText) {
    return "";
  }

  const cleanedText = originalText
    .normalize("NFKC")
    .replace(/^L\s*/u, "")
    .replace(/^スマスロ\s*/u, "")
    .replace(/^スマスロ/u, "")
    .replace(/^パチスロ\s*/u, "")
    .replace(/^A-SLOT\+\s*/u, "")
    .replace(/～.*?～/gu, "")
    .replace(/ THE ANIMATION/gu, "")
    .replace(/\s+/gu, "")
    .trim();
  if (cleanedText.length <= 8) {
    return cleanedText || originalText;
  }
  return `${cleanedText.slice(0, 8)}…`;
}

export function getHuntMachineShortName(machineName) {
  return findHuntMachineDisplayDefinition(machineName)?.shortName ?? buildFallbackHuntMachineShortName(machineName);
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
  return hasCombinedHuntMachineGroupOption(COMBINED_HUNT_MACHINE_GROUPS[0], availableMachineNames);
}

export function hasHanabiHuntMachineGroupOption(availableMachineNames) {
  return hasCombinedHuntMachineGroupOption(COMBINED_HUNT_MACHINE_GROUPS[1], availableMachineNames);
}

export function isAimJugglerHuntMachineGroupName(machineName) {
  return isSameHuntMachineName(machineName, AIM_JUGGLER_GROUP_NAME);
}

export function isHanabiHuntMachineGroupName(machineName) {
  return isSameHuntMachineName(machineName, HANABI_GROUP_NAME);
}

export function selectionIncludesAimJugglerHuntMachineGroup(machineNames) {
  return (Array.isArray(machineNames) ? machineNames : [machineNames]).some(isAimJugglerHuntMachineGroupName);
}

export function selectionIncludesHanabiHuntMachineGroup(machineNames) {
  return (Array.isArray(machineNames) ? machineNames : [machineNames]).some(isHanabiHuntMachineGroupName);
}

export function expandHuntMachineCombinedGroupSelection(machineNames) {
  return [
    ...new Set(
      (Array.isArray(machineNames) ? machineNames : [machineNames])
        .flatMap((machineName) => {
          const group = COMBINED_HUNT_MACHINE_GROUPS.find((entry) =>
            isSameHuntMachineName(machineName, entry.groupName),
          );
          return group ? group.machineNames : [String(machineName ?? "").trim()];
        })
        .filter(Boolean),
    ),
  ];
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

function readHuntMachineOptionSlotCount(machine) {
  const slotCount = Number(machine?.slotCount);
  return Number.isFinite(slotCount) && slotCount > 0 ? slotCount : 0;
}

function readAimJugglerOptionOrder(machine) {
  const machineName = machine?.name;
  if (isSameHuntMachineName(machineName, AIM_JUGGLER_GROUP_NAME)) {
    return 0;
  }
  if (isHuntMachineInGroup(machineName, ["ネオアイムジャグラーEX", "ネオアイムジャグラーＥＸ"])) {
    return 1;
  }
  if (isHuntMachineInGroup(machineName, ["SアイムジャグラーＥＸ", "SアイムジャグラーEX"])) {
    return 2;
  }
  return null;
}

function sortHuntMachineOptionsBySlotCount(options) {
  return [...(Array.isArray(options) ? options : [])].sort((left, right) => {
    const leftAimOrder = readAimJugglerOptionOrder(left);
    const rightAimOrder = readAimJugglerOptionOrder(right);
    if (leftAimOrder !== null || rightAimOrder !== null) {
      if (leftAimOrder === null) {
        return 1;
      }
      if (rightAimOrder === null) {
        return -1;
      }
      return leftAimOrder - rightAimOrder;
    }

    const slotDifference =
      readHuntMachineOptionSlotCount(right) - readHuntMachineOptionSlotCount(left);
    if (slotDifference !== 0) {
      return slotDifference;
    }

    const leftLabel = String(left?.optionLabel ?? left?.shortName ?? left?.name ?? "");
    const rightLabel = String(right?.optionLabel ?? right?.shortName ?? right?.name ?? "");
    return leftLabel.localeCompare(rightLabel, "ja");
  });
}

export function groupHuntMachineOptions(machineOptions, options = {}) {
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
  const combinedGroupOptions = COMBINED_HUNT_MACHINE_GROUPS
    .map((group) => buildCombinedHuntMachineOption(group, machineOptions, options))
    .filter(Boolean);
  const activeCombinedGroupKeys = new Set(
    combinedGroupOptions
      .filter((groupOption) => groupOption.checked)
      .map((groupOption) => groupOption.combinedGroupKey),
  );

  for (const groupOption of combinedGroupOptions) {
    groupsByKey.get(groupOption.category).options.push(groupOption);
  }

  for (const machine of Array.isArray(machineOptions) ? machineOptions : []) {
    const combinedGroup = findCombinedHuntMachineGroup(machine.name);
    if (combinedGroup && isSameHuntMachineName(machine.name, combinedGroup.groupName)) {
      continue;
    }
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
      checked:
        combinedGroup && activeCombinedGroupKeys.has(combinedGroup.key)
          ? false
          : Boolean(machine.checked),
      shortName: getHuntMachineShortName(machine.name),
      optionLabel: getHuntMachineOptionLabel(machine.name, machine.slotCount),
      category: safeCategoryKey,
      combinedGroupKey: combinedGroup?.key ?? "",
      combinedRole: combinedGroup ? "member" : "",
    });
  }

  return [...groupsByKey.values()]
    .map((group) => ({
      ...group,
      options: sortHuntMachineOptionsBySlotCount(group.options),
    }))
    .filter((group) => group.options.length > 0);
}
