import { calculateSettingEstimate, getSettingEstimateDefinition } from "./setting-estimates";
import {
  DEFAULT_DIFFERENCE_MODE,
  normalizeDifferenceMode,
  selectDifferenceValue,
} from "./machine-difference";

const HUNT_SCORE_EPSILON = 0.000000001;
const DEFAULT_HUNT_SCORE_WINDOW_DAYS = 7;
const TAMAYA_ZASSHONOKUMA_HISTORY_WINDOW_DAYS = 120;
const DEFAULT_HUNT_SCORE_LOGIC_KEY = "apark";

const OTHER_TARGET_MACHINES = [
  {
    name: "スマスロ北斗の拳 転生の章",
    aliases: ["スマスロ北斗の拳 転生の章2", "スマスロ北斗の拳転生の章", "スマスロ北斗の拳転生の章2"],
  },
  {
    name: "スマスロ ミリオンゴッド",
    aliases: [
      "スマスロ ミリオンゴッド-神々の軌跡-",
      "スマスロミリオンゴッド",
      "スマスロミリオンゴッド-神々の軌跡-",
    ],
  },
  { name: "L東京喰種", aliases: ["L 東京喰種", "東京喰種"] },
  {
    name: "スマスロモンキーターンV",
    aliases: ["スマスロ モンキーターンV", "スマスロモンキーターンⅤ", "スマスロ モンキーターンⅤ"],
  },
  {
    name: "スマスロ 甲鉄城のカバネリ 海門決戦",
    aliases: ["スマスロ甲鉄城のカバネリ海門決戦"],
  },
  { name: "Lスマスロ北斗の拳", aliases: ["L スマスロ北斗の拳", "スマスロ北斗の拳"] },
  {
    name: "Lパチスロ炎炎ノ消防隊2",
    aliases: ["Lパチスロ炎炎ノ消防隊２", "L パチスロ炎炎ノ消防隊2", "L炎炎ノ消防隊2"],
  },
  { name: "Lパチスロからくりサーカス", aliases: ["L パチスロからくりサーカス"] },
  { name: "Lパチスロ かぐや様は告らせたい", aliases: ["Lパチスロかぐや様は告らせたい"] },
  { name: "Lパチスロ革命機ヴァルヴレイヴ2", aliases: ["Lパチスロ革命機ヴァルヴレイヴ２"] },
  { name: "Lパチスロ革命機ヴァルヴレイヴ", aliases: [] },
  { name: "Lパチスロ炎炎ノ消防隊", aliases: ["L パチスロ炎炎ノ消防隊", "L炎炎ノ消防隊"] },
  { name: "Lパチスロ ダンベル何キロ持てる？", aliases: ["Lパチスロダンベル何キロ持てる？"] },
  { name: "Lパチスロ 機動戦士ガンダムSEED", aliases: ["Lパチスロ機動戦士ガンダムSEED"] },
  {
    name: "Lパチスロ 機動戦士ガンダムユニコーン 覚醒DRIVE",
    aliases: [
      "機動戦士ガンダムユニコーン 覚醒DRIVE",
      "Lパチスロ機動戦士ガンダムユニコーン覚醒DRIVE",
      "L機動戦士ガンダムユニコーン覚醒",
    ],
  },
  { name: "Lパチスロ ありふれた職業で世界最強", aliases: ["Lパチスロありふれた職業で世界最強"] },
  { name: "Lパチスロ シン・エヴァンゲリオン", aliases: ["Lパチスロシン・エヴァンゲリオン"] },
  { name: "Lパチスロ戦姫絶唱シンフォギア 正義の歌", aliases: [] },
  { name: "Lパチスロガールズ＆パンツァー 最終章", aliases: ["Lパチスロガールズ&パンツァー 最終章"] },
  { name: "Lパチスロうみねこのなく頃に2", aliases: ["Lパチスロうみねこのなく頃に２"] },
  { name: "Lパチスロ閃乱カグラ2 SHINOVI MASTER", aliases: ["Lパチスロ閃乱カグラ２ SHINOVI MASTER"] },
  { name: "Lパチスロ ベルセルク無双", aliases: ["Lパチスロ　ベルセルク無双"] },
  { name: "Lパチスロうる星やつら", aliases: [] },
  { name: "Lパチスロ マクロスフロンティア4", aliases: ["Lパチスロマクロスフロンティア4"] },
  { name: "Lパチスロ花の慶次～佐渡攻めの章～", aliases: [] },
  { name: "スマスロ 攻殻機動隊", aliases: ["スマスロ攻殻機動隊"] },
  { name: "真打 吉宗", aliases: ["真打吉宗"] },
  {
    name: "クレアの秘宝伝～はじまりの扉と太陽の石～ボーナストリガーver.",
    aliases: [
      "クレアの秘宝伝ボーナストリガーVER.A2",
      "⑳LB/クレアの秘宝伝ボーナストリガーVER.A2",
    ],
  },
  { name: "SHAKE BONUS TRIGGER", aliases: ["LB SHAKE BONUS TRIGGER"] },
  { name: "ニューパルサーSP4 with 太鼓の達人", aliases: ["ニューパルサーＳＰ４ with 太鼓の達人"] },
  { name: "スマスロニューパルサーBT", aliases: ["スマスロ ニューパルサーBT"] },
  { name: "A-SLOT+ ディスクアップ ULTRAREMIX", aliases: ["A-SLOT+ディスクアップ ULTRAREMIX"] },
  { name: "クランキークレスト", aliases: [] },
];

const OKIDOKI_TARGET_MACHINES = [
  { name: "沖ドキ！ＧＯＬＤ", aliases: ["沖ドキ！ＧＯＬＤ-30", "沖ドキ!GOLD", "沖ドキ!GOLD-30"] },
  {
    name: "スマスロ 沖ドキ!DUO アンコール",
    aliases: ["スマスロ沖ドキ!DUOアンコール", "L沖ドキ!DUO アンコール"],
  },
  { name: "沖ドキ！BLACK", aliases: ["沖ドキ!BLACK"] },
  { name: "沖ドキ!ゴージャス 25Φ", aliases: ["沖ドキ!ゴージャス", "沖ドキ！ゴージャス"] },
  { name: "沖ドキ!ゴージャス 30Φ", aliases: [] },
  { name: "沖ドキ！DUO", aliases: ["沖ドキ！DUO-30", "沖ドキ!DUO", "沖ドキ!DUO-30"] },
  { name: "沖ドキ！２-30", aliases: ["沖ドキ!2-30", "沖ドキ！2-30"] },
];

const APARK_KASUGA_TARGET_MACHINES = [
  { name: "SアイムジャグラーＥＸ", aliases: ["SアイムジャグラーEX"] },
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
  { name: "マイジャグラーV", aliases: ["マイジャグラーⅤ", "マイジャグラー"] },
  { name: "ゴーゴージャグラー３", aliases: ["ゴーゴージャグラー3", "ゴーゴージャグラー"] },
  {
    name: "ファンキージャグラー２ＫＴ",
    aliases: ["ファンキージャグラー２", "ファンキージャグラー2", "ファンキージャグラー"],
  },
  { name: "ミスタージャグラー", aliases: [] },
  { name: "ジャグラーガールズSS", aliases: ["ジャグラーガールズ"] },
  {
    name: "ハッピージャグラーＶＩＩＩ",
    aliases: ["ハッピージャグラーVIII", "ハッピージャグラーＶ", "ハッピージャグラーV", "ハッピージャグラー"],
  },
  { name: "ウルトラミラクルジャグラー", aliases: [] },
  {
    name: "ハナハナホウオウ",
    aliases: [
      "ハナハナホウオウ-30",
      "ハナハナホウオウ‐30",
      "ハナハナホウオウ～天翔～-30",
      "ハナハナホウオウ～天翔～‐30",
    ],
  },
  {
    name: "ドラゴンハナハナ～閃光～",
    aliases: [
      "ドラゴンハナハナ",
      "ドラゴンハナハナ閃光",
      "ドラゴンハナハナ閃光30",
      "ドラゴンハナハナ～閃光～30",
      "ドラゴンハナハナ～閃光～-30",
      "ドラゴンハナハナ～閃光～‐30",
    ],
  },
  { name: "キングハナハナ", aliases: ["キングハナハナ-30", "キングハナハナ‐30"] },
  {
    name: "ニューキングハナハナ",
    aliases: ["ニューキングハナハナV", "ニューキングハナハナV-30", "ニューキングハナハナV‐30"],
  },
  { name: "スターハナハナ", aliases: ["スターハナハナ-30", "スターハナハナ‐30"] },
  { name: "新ハナビ", aliases: [] },
  { name: "スマスロ ハナビ", aliases: ["スマスロハナビ"] },
  ...OKIDOKI_TARGET_MACHINES,
  ...OTHER_TARGET_MACHINES,
];

const GOGO_ARENA_TENJIN_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
  { name: "マイジャグラーV", aliases: ["マイジャグラーⅤ", "マイジャグラー"] },
  { name: "ゴーゴージャグラー３", aliases: ["ゴーゴージャグラー3", "ゴーゴージャグラー"] },
  {
    name: "ファンキージャグラー２ＫＴ",
    aliases: ["ファンキージャグラー２", "ファンキージャグラー2", "ファンキージャグラー"],
  },
  { name: "ミスタージャグラー", aliases: [] },
  { name: "ジャグラーガールズSS", aliases: ["ジャグラーガールズ"] },
  {
    name: "ハッピージャグラーＶＩＩＩ",
    aliases: ["ハッピージャグラーVIII", "ハッピージャグラーＶ", "ハッピージャグラーV", "ハッピージャグラー"],
  },
  { name: "ウルトラミラクルジャグラー", aliases: [] },
  ...OKIDOKI_TARGET_MACHINES,
  ...OTHER_TARGET_MACHINES,
];

const TAMAYA_ZASSHONOKUMA_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const TAMAYA_OHASHI_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const HAKATA_123_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
  { name: "マイジャグラーV", aliases: ["マイジャグラーⅤ", "マイジャグラー"] },
  { name: "ゴーゴージャグラー３", aliases: ["ゴーゴージャグラー3", "ゴーゴージャグラー"] },
];

const BOOM_TENJIN_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
  { name: "マイジャグラーV", aliases: ["マイジャグラーⅤ", "マイジャグラー"] },
];

const BEAM_HIKARI_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
  { name: "マイジャグラーV", aliases: ["マイジャグラーⅤ", "マイジャグラー"] },
];

const HINODE_ONOJO_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
  { name: "マイジャグラーV", aliases: ["マイジャグラーⅤ", "マイジャグラー"] },
];

const APARK_YAKATABARU_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
  { name: "マイジャグラーV", aliases: ["マイジャグラーⅤ", "マイジャグラー"] },
  {
    name: "ファンキージャグラー２ＫＴ",
    aliases: ["ファンキージャグラー２", "ファンキージャグラー2", "ファンキージャグラー"],
  },
  {
    name: "ハッピージャグラーＶＩＩＩ",
    aliases: ["ハッピージャグラーVIII", "ハッピージャグラーＶ", "ハッピージャグラーV", "ハッピージャグラー"],
  },
  { name: "ウルトラミラクルジャグラー", aliases: [] },
  {
    name: "ハナハナホウオウ",
    aliases: [
      "ハナハナホウオウ-30",
      "ハナハナホウオウ‐30",
      "ハナハナホウオウ～天翔～-30",
      "ハナハナホウオウ～天翔～‐30",
    ],
  },
  { name: "キングハナハナ", aliases: ["キングハナハナ-30", "キングハナハナ‐30"] },
  { name: "新ハナビ", aliases: [] },
  { name: "スマスロ ハナビ", aliases: ["スマスロハナビ"] },
  ...OKIDOKI_TARGET_MACHINES,
  ...OTHER_TARGET_MACHINES,
];

const GOGO_ARENA_TENJIN_REFERENCE_EVENT_DAYS = new Set([5, 10, 15, 20, 25, 30]);

const GOGO_ARENA_TENJIN_MACHINE_SCORES = {
  "ネオアイムジャグラーEX": 16,
  "マイジャグラーV": -8,
  "ゴーゴージャグラー3": 8,
  "ファンキージャグラー2KT": -4,
  "ミスタージャグラー": 5,
  "ジャグラーガールズSS": 6,
  "ハッピージャグラーVIII": 1,
  "ウルトラミラクルジャグラー": 4,
};

const GOGO_ARENA_TENJIN_SLOT_SCORES = {
  "ネオアイムジャグラーEX": {
    161: 10,
    147: 9,
    155: 8,
    166: 8,
    174: 7,
    143: 7,
    157: 7,
    163: 7,
    146: 5,
    149: 5,
    165: 5,
    173: 5,
    176: 5,
    122: -3,
    164: -4,
    172: -4,
    150: -3,
  },
  "ゴーゴージャグラー3": {
    98: 8,
    87: 6,
    84: 4,
    91: 4,
    93: -7,
    88: -5,
    96: -3,
  },
  "ジャグラーガールズSS": {
    57: 5,
    58: 4,
    52: 3,
    54: 3,
    61: 3,
    53: -3,
    55: -2,
    56: -2,
  },
  "ウルトラミラクルジャグラー": {
    71: 4,
    68: 3,
    70: 2,
    69: -3,
    67: -3,
  },
  "ミスタージャグラー": {
    42: 4,
    51: 3,
    46: 3,
    44: -3,
    48: -3,
    43: -2,
  },
  "ハッピージャグラーVIII": {
    64: 4,
    123: 3,
    124: 2,
    122: -4,
    63: -3,
    66: -3,
  },
  "ファンキージャグラー2KT": {
    74: 3,
    78: 2,
    73: 1,
    77: 1,
    79: 1,
    76: -4,
    81: -4,
    72: -2,
  },
  "マイジャグラーV": {
    121: 4,
    107: 3,
    109: 3,
    104: 2,
    106: 2,
    129: 2,
    113: -10,
    114: -10,
    131: -8,
    108: -5,
    115: -5,
    116: -5,
    119: -5,
    120: -5,
    138: -5,
    112: -3,
    128: -3,
    130: -3,
    133: -3,
    140: -3,
  },
};

const TAMAYA_ZASSHONOKUMA_SLOT_SCORES = {
  69: 5,
  71: -2,
  73: 3,
  74: -5,
  77: 4,
  78: -2,
  79: -2,
  97: 6,
  98: -2,
  99: 3,
  100: -4,
  101: -2,
  102: -4,
  103: 3,
  104: 4,
  109: 3,
};

const BOOM_TENJIN_MYJUGGLER_STRONG_DAYS = new Set([3, 8, 13, 18, 23, 26, 28]);
const BOOM_TENJIN_NEO_STRONG_DAYS = new Set([3, 8, 26, 28]);
const BOOM_TENJIN_MYJUGGLER_PREFERRED_SLOTS = new Set(["632", "644", "648", "651", "656", "659", "839"]);
const BOOM_TENJIN_MYJUGGLER_SECONDARY_SLOTS = new Set([
  "633",
  "638",
  "641",
  "643",
  "647",
  "649",
  "650",
  "655",
  "657",
  "817",
  "835",
]);
const BOOM_TENJIN_MYJUGGLER_WEAK_SLOTS = new Set(["814", "815", "833", "836"]);
const BOOM_TENJIN_NEO_BEST_SLOTS = new Set(["795"]);
const BOOM_TENJIN_NEO_SECONDARY_SLOTS = new Set(["796", "797"]);
const BOOM_TENJIN_NEO_WEAK_SLOTS = new Set(["793", "801"]);

const BEAM_HIKARI_A_NEO_STRONG_SLOTS = new Set(["873", "875", "1008", "1021", "1022"]);
const BEAM_HIKARI_A_NEO_SECONDARY_SLOTS = new Set([
  "872",
  "876",
  "878",
  "880",
  "881",
  "1006",
  "1011",
  "1015",
  "1017",
  "1023",
  "1026",
  "1028",
]);
const BEAM_HIKARI_A_NEO_WEAK_SLOTS = new Set(["877", "883", "1016"]);
const BEAM_HIKARI_A_MYJUGGLER_STRONG_SLOTS = new Set(["1087", "1108", "1121"]);
const BEAM_HIKARI_A_MYJUGGLER_SECONDARY_SLOTS = new Set([
  "1101",
  "1102",
  "1103",
  "1105",
  "1106",
  "1111",
  "1112",
  "1120",
  "1123",
]);
const BEAM_HIKARI_A_MYJUGGLER_WEAK_SLOTS = new Set(["1086", "1100", "1113", "1118", "1122"]);

const BEAM_HIKARI_B_NEO_A_SLOTS = new Set(["873", "875", "1008", "1021", "1022"]);
const BEAM_HIKARI_B_NEO_B_SLOTS = new Set([
  "872",
  "876",
  "878",
  "880",
  "881",
  "1006",
  "1011",
  "1015",
  "1017",
  "1023",
  "1026",
  "1028",
]);
const BEAM_HIKARI_B_NEO_D_SLOTS = new Set(["877", "1016", "1020"]);
const BEAM_HIKARI_B_MYJUGGLER_A_SLOTS = new Set(["1108"]);
const BEAM_HIKARI_B_MYJUGGLER_B_SLOTS = new Set(["1087", "1102", "1121"]);
const BEAM_HIKARI_B_MYJUGGLER_D_SLOTS = new Set(["1100", "1101", "1113", "1118", "1122"]);

const HUNT_SCORE_LOGIC_DEFINITIONS = [
  {
    key: "apark",
    name: "Aパーク春日式",
    windowDays: 7,
    scoreCalculator: calculateAparkKasugaHuntScore,
  },
  {
    key: "gogo",
    name: "GOGO式",
    windowDays: 7,
    scoreCalculator: calculateGogoArenaTenjinHuntScore,
  },
  {
    key: "tamaya-zasshonokuma",
    name: "玉屋雑餉隈式",
    windowDays: 7,
    historyWindowDays: TAMAYA_ZASSHONOKUMA_HISTORY_WINDOW_DAYS,
    scoreCalculator: calculateTamayaZasshonokumaHuntScore,
  },
  {
    key: "tamaya-ohashi",
    name: "玉屋555大橋店式",
    windowDays: 7,
    scoreCalculator: calculateTamayaOhashiHuntScore,
  },
  {
    key: "123-hakata",
    name: "123博多式",
    windowDays: 7,
    scoreCalculator: calculate123HakataHuntScore,
  },
  {
    key: "boom-tenjin",
    name: "BOOM天神式",
    windowDays: 7,
    scoreCalculator: calculateBoomTenjinHuntScore,
  },
  {
    key: "beam-hikari-a",
    name: "ビームヒカリ式A",
    windowDays: 7,
    scoreCalculator: calculateBeamHikariAHuntScore,
  },
  {
    key: "beam-hikari-b",
    name: "ビームヒカリ式B",
    windowDays: 7,
    scoreCalculator: calculateBeamHikariBHuntScore,
  },
  {
    key: "hinode-onojo",
    name: "HINODE大野城式",
    windowDays: 7,
    scoreCalculator: calculateHinodeOnojoHuntScore,
  },
  {
    key: "hinode-onojo-a",
    name: "HINODE大野城式A",
    windowDays: 7,
    scoreCalculator: calculateHinodeOnojoAHuntScore,
  },
  {
    key: "hinode-onojo-b",
    name: "HINODE大野城式B",
    windowDays: 7,
    scoreCalculator: calculateHinodeOnojoBHuntScore,
  },
  {
    key: "hinode-onojo-c",
    name: "HINODE大野城式C",
    windowDays: 7,
    scoreCalculator: calculateHinodeOnojoCHuntScore,
  },
  {
    key: "hinode-onojo-d",
    name: "HINODE大野城式D",
    windowDays: 7,
    scoreCalculator: calculateHinodeOnojoDHuntScore,
  },
];

const DEFAULT_HUNT_SCORE_STORE_CONFIG = {
  key: "default",
  storeNames: [],
  targetMachines: APARK_KASUGA_TARGET_MACHINES,
  defaultLogicKey: DEFAULT_HUNT_SCORE_LOGIC_KEY,
};

const HUNT_SCORE_STORE_CONFIGS = [
  {
    key: "apark-kasuga",
    storeNames: ["Aパーク春日店"],
    targetMachines: APARK_KASUGA_TARGET_MACHINES,
    defaultLogicKey: "apark",
  },
  {
    key: "apark-yakatabaru",
    storeNames: ["A-PARK屋形原", "A-PARK屋形原店", "Aパーク屋形原", "Aパーク屋形原店"],
    targetMachines: APARK_YAKATABARU_TARGET_MACHINES,
    defaultLogicKey: "apark",
  },
  {
    key: "gogo-arena-tenjin",
    storeNames: ["GOGOアリーナ天神", "ＧＯＧＯアリーナ天神"],
    targetMachines: GOGO_ARENA_TENJIN_TARGET_MACHINES,
    defaultLogicKey: "gogo",
  },
  {
    key: "tamaya-zasshonokuma",
    storeNames: ["玉屋409雑餉隈", "玉屋雑餉隈", "玉屋雑餉隈店"],
    targetMachines: TAMAYA_ZASSHONOKUMA_TARGET_MACHINES,
    defaultLogicKey: "tamaya-zasshonokuma",
  },
  {
    key: "hinode-onojo",
    storeNames: ["HINODE大野城店", "HINODE大野城"],
    targetMachines: HINODE_ONOJO_TARGET_MACHINES,
    defaultLogicKey: "hinode-onojo",
  },
  {
    key: "tamaya-ohashi",
    storeNames: ["玉屋555大橋店"],
    targetMachines: TAMAYA_OHASHI_TARGET_MACHINES,
    defaultLogicKey: "tamaya-ohashi",
  },
  {
    key: "123-hakata",
    storeNames: ["123博多店"],
    targetMachines: HAKATA_123_TARGET_MACHINES,
    defaultLogicKey: "123-hakata",
  },
  {
    key: "boom-tenjin",
    storeNames: ["BOOM天神店", "BOOM天神", "ＢＯＯＭ天神店", "ＢＯＯＭ天神"],
    targetMachines: BOOM_TENJIN_TARGET_MACHINES,
    defaultLogicKey: "boom-tenjin",
  },
  {
    key: "beam-hikari",
    storeNames: ["ビームヒカリ店", "ビームヒカリ", "BEAM HIKARI", "BEAMHIKARI", "ＢＥＡＭヒカリ店"],
    targetMachines: BEAM_HIKARI_TARGET_MACHINES,
    defaultLogicKey: "beam-hikari-a",
  },
  {
    key: "mj-itazuke",
    storeNames: ["MJアリーナ板付店"],
    targetMachines: APARK_KASUGA_TARGET_MACHINES,
    defaultLogicKey: "apark",
  },
  {
    key: "mzas-ozasa",
    storeNames: ["エムザス小笹店"],
    targetMachines: APARK_KASUGA_TARGET_MACHINES,
    defaultLogicKey: "apark",
  },
  {
    key: "plaza3",
    storeNames: ["プラザ3"],
    targetMachines: APARK_KASUGA_TARGET_MACHINES,
    defaultLogicKey: "apark",
  },
];

function normalizeText(value) {
  return String(value ?? "").normalize("NFKC").replace(/\s+/gu, "").trim();
}

function listHuntScoreTargetMachineNameCandidates(targetMachine) {
  return [
    targetMachine.name,
    ...(Array.isArray(targetMachine.aliases) ? targetMachine.aliases : []),
  ];
}

export function findHuntScoreStoreConfig(storeName) {
  const normalizedStoreName = normalizeText(storeName);
  if (!normalizedStoreName) {
    return null;
  }

  return (
    HUNT_SCORE_STORE_CONFIGS.find((config) =>
      config.storeNames.some((candidate) => normalizeText(candidate) === normalizedStoreName),
    ) ?? null
  );
}

function resolveHuntScoreStoreConfig(storeName = "") {
  return findHuntScoreStoreConfig(storeName) ?? DEFAULT_HUNT_SCORE_STORE_CONFIG;
}

function listKnownHuntScoreStoreConfigs(storeName = "") {
  const primaryConfig = resolveHuntScoreStoreConfig(storeName);
  const configs = [primaryConfig, DEFAULT_HUNT_SCORE_STORE_CONFIG, ...HUNT_SCORE_STORE_CONFIGS];
  const seenKeys = new Set();
  return configs.filter((config) => {
    if (!config?.key || seenKeys.has(config.key)) {
      return false;
    }
    seenKeys.add(config.key);
    return true;
  });
}

function listTargetMachinesFromConfigs(configs) {
  const seenNames = new Set();
  return (Array.isArray(configs) ? configs : [])
    .flatMap((config) => (Array.isArray(config?.targetMachines) ? config.targetMachines : []))
    .filter((targetMachine) => {
      const normalizedName = normalizeText(targetMachine?.name);
      if (!normalizedName || seenNames.has(normalizedName)) {
        return false;
      }
      seenNames.add(normalizedName);
      return true;
    });
}

function findHuntScoreLogicDefinition(logicKey) {
  const normalizedLogicKey = String(logicKey ?? "").trim();
  if (!normalizedLogicKey) {
    return null;
  }
  return (
    HUNT_SCORE_LOGIC_DEFINITIONS.find((definition) => definition.key === normalizedLogicKey) ?? null
  );
}

export function listHuntScoreLogicOptions() {
  return HUNT_SCORE_LOGIC_DEFINITIONS.map((definition) => ({
    key: definition.key,
    name: definition.name,
  }));
}

export function getDefaultHuntScoreLogicKey(storeName = "") {
  const config = resolveHuntScoreStoreConfig(storeName);
  return findHuntScoreLogicDefinition(config?.defaultLogicKey)?.key ?? DEFAULT_HUNT_SCORE_LOGIC_KEY;
}

export function normalizeHuntScoreLogicKey(logicKey = "", storeName = "") {
  return findHuntScoreLogicDefinition(logicKey)?.key ?? getDefaultHuntScoreLogicKey(storeName);
}

export function getHuntScoreLogicDetail(logicKey = "", storeName = "") {
  const normalizedLogicKey = normalizeHuntScoreLogicKey(logicKey, storeName);
  const definition =
    findHuntScoreLogicDefinition(normalizedLogicKey) ??
    findHuntScoreLogicDefinition(DEFAULT_HUNT_SCORE_LOGIC_KEY);
  return {
    key: definition.key,
    name: definition.name,
  };
}

function buildRuntimeHuntScoreConfig(config, logicKey = "", differenceMode = DEFAULT_DIFFERENCE_MODE) {
  const logicDefinition =
    findHuntScoreLogicDefinition(normalizeHuntScoreLogicKey(logicKey, config?.storeNames?.[0] ?? "")) ??
    findHuntScoreLogicDefinition(DEFAULT_HUNT_SCORE_LOGIC_KEY);
  return {
    ...config,
    logicKey: logicDefinition.key,
    logicName: logicDefinition.name,
    windowDays: logicDefinition.windowDays ?? DEFAULT_HUNT_SCORE_WINDOW_DAYS,
    historyWindowDays:
      logicDefinition.historyWindowDays ?? logicDefinition.windowDays ?? DEFAULT_HUNT_SCORE_WINDOW_DAYS,
    differenceMode: normalizeDifferenceMode(differenceMode),
    scoreCalculator: logicDefinition.scoreCalculator,
  };
}

function listSearchConfigs(storeName) {
  return [resolveHuntScoreStoreConfig(storeName)];
}

function findTargetMachine(config, machineName) {
  const normalizedMachineName = normalizeText(machineName);
  if (!config || !normalizedMachineName) {
    return null;
  }

  return (
    config.targetMachines.find((candidate) =>
      listHuntScoreTargetMachineNameCandidates(candidate).some(
        (candidateName) => normalizeText(candidateName) === normalizedMachineName,
      ),
    ) ?? null
  );
}

function findKnownTargetMachine(storeName, machineName) {
  const normalizedMachineName = normalizeText(machineName);
  if (!normalizedMachineName) {
    return null;
  }

  return (
    listTargetMachinesFromConfigs(listKnownHuntScoreStoreConfigs(storeName)).find((candidate) =>
      listHuntScoreTargetMachineNameCandidates(candidate).some(
        (candidateName) => normalizeText(candidateName) === normalizedMachineName,
      ),
    ) ?? null
  );
}

function buildEffectiveHuntScoreStoreConfig(storeName, machineNames) {
  const primaryConfig = resolveHuntScoreStoreConfig(storeName);
  const availableMachineNames = (Array.isArray(machineNames) ? machineNames : [])
    .map((machineName) => String(machineName ?? "").trim())
    .filter(Boolean);
  if (availableMachineNames.length === 0) {
    return primaryConfig;
  }

  const availableMachineNameSet = new Set(availableMachineNames.map(normalizeText));
  const targetMachines = listTargetMachinesFromConfigs(listKnownHuntScoreStoreConfigs(storeName)).filter(
    (targetMachine) =>
      listHuntScoreTargetMachineNameCandidates(targetMachine).some((candidateName) =>
        availableMachineNameSet.has(normalizeText(candidateName)),
      ),
  );

  return {
    ...primaryConfig,
    targetMachines,
  };
}

export function canonicalHuntScoreTargetMachineName(machineName, storeName = "") {
  return findKnownTargetMachine(storeName, machineName)?.name ?? null;
}

function normalizeHuntScoreMachineName(machineName, config) {
  return findTargetMachine(config, machineName)?.name ?? normalizeText(machineName);
}

function readNumber(value) {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function hasMeaningfulResult(row) {
  return ["difference_value", "games_count", "bb_count", "rb_count"].some((key) =>
    Number.isFinite(readNumber(row?.[key])),
  );
}

function readHuntScoreDifferenceValue(row, differenceMode = DEFAULT_DIFFERENCE_MODE, machineName = "") {
  return selectDifferenceValue(row, differenceMode, machineName || row?.machine_name) ?? 0;
}

function buildRowKey(row, config) {
  return [
    String(row?.target_date ?? "").trim(),
    normalizeHuntScoreMachineName(row?.machine_name, config),
    String(row?.slot_number ?? "").trim(),
  ].join("\u0000");
}

function buildCandidateKey(row, config) {
  return [
    normalizeHuntScoreMachineName(row?.machine_name, config),
    String(row?.slot_number ?? "").trim(),
  ].join("\u0000");
}

function getSettingDefinition(settingDefinitionCache, machineName) {
  const cacheKey = normalizeText(machineName);
  let definition = settingDefinitionCache.get(cacheKey);
  if (definition === undefined) {
    definition = getSettingEstimateDefinition(machineName);
    settingDefinitionCache.set(cacheKey, definition ?? null);
  }
  return definition;
}

function getSettingEstimateAverage(settingDefinitionCache, row, config) {
  const definition = getSettingDefinition(
    settingDefinitionCache,
    normalizeHuntScoreMachineName(row?.machine_name, config),
  );
  const estimate = definition ? calculateSettingEstimate(definition, row) : null;
  return {
    estimate,
    average: estimate?.average ?? null,
  };
}

function calculateCurrentLosingStreak(windowRows) {
  let streak = 0;

  for (let index = windowRows.length - 1; index >= 0; index -= 1) {
    if (windowRows[index].differenceValue >= 0) {
      break;
    }
    streak += 1;
  }

  return streak;
}

function calculateCurrentWinningStreak(windowRows) {
  let streak = 0;

  for (let index = windowRows.length - 1; index >= 0; index -= 1) {
    if (windowRows[index].differenceValue <= 0) {
      break;
    }
    streak += 1;
  }

  return streak;
}

function calculateCurrentHighSettingStreak(windowRows) {
  let streak = 0;

  for (let index = windowRows.length - 1; index >= 0; index -= 1) {
    const settingAverage = windowRows[index].settingAverage;
    if (!Number.isFinite(settingAverage) || settingAverage < 4) {
      break;
    }
    streak += 1;
  }

  return streak;
}

function sumDifferenceValues(rows) {
  return rows.reduce((total, row) => total + (readNumber(row?.differenceValue) ?? 0), 0);
}

function calculateSettingAverageFromWindowRows(rows) {
  const settings = rows
    .map((row) => row?.settingAverage)
    .filter((settingAverage) => Number.isFinite(settingAverage));
  if (settings.length === 0) {
    return null;
  }
  return settings.reduce((total, settingAverage) => total + settingAverage, 0) / settings.length;
}

function readDateDay(dateText) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/u.exec(String(dateText ?? "").trim());
  if (!match) {
    return null;
  }
  return Number(match[3]);
}

function isGogoArenaTenjinReferenceEventDate(dateText) {
  const day = readDateDay(dateText);
  return Number.isFinite(day) && GOGO_ARENA_TENJIN_REFERENCE_EVENT_DAYS.has(day);
}

function calculatePreviousReferenceEventMetrics(
  businessDates,
  dateIndex,
  recordMapByDate,
  settingDefinitionCache,
  config,
) {
  const settings = [];

  for (let index = dateIndex; index >= 0; index -= 1) {
    const date = businessDates[index];
    if (!isGogoArenaTenjinReferenceEventDate(date)) {
      continue;
    }

    const eventRow = recordMapByDate.get(date);
    if (!eventRow) {
      continue;
    }

    const settingAverage = getSettingEstimateAverage(settingDefinitionCache, eventRow, config).average;
    if (!Number.isFinite(settingAverage)) {
      continue;
    }
    settings.push(settingAverage);
    if (settings.length >= 3) {
      break;
    }
  }

  return {
    previousReferenceEventSetting: settings[0] ?? null,
    referenceEventHighSettingCount: settings.filter((setting) => setting >= 4).length,
    referenceEventSampleCount: settings.length,
  };
}

function scoreFromMinimums(value, thresholds) {
  for (const threshold of thresholds) {
    if (value >= threshold.minimum) {
      return threshold.score;
    }
  }

  return 0;
}

function scoreFromMaximums(value, thresholds) {
  for (const threshold of thresholds) {
    if (value <= threshold.maximum) {
      return threshold.score;
    }
  }

  return 0;
}

function calculateLossDaysScore(value) {
  return scoreFromMinimums(value, [
    { minimum: 7, score: 25 },
    { minimum: 6, score: 21 },
    { minimum: 5, score: 16 },
    { minimum: 4, score: 10 },
    { minimum: 3, score: 5 },
  ]);
}

function calculateStreakScore(value) {
  return scoreFromMinimums(value, [
    { minimum: 7, score: 18 },
    { minimum: 6, score: 16 },
    { minimum: 5, score: 14 },
    { minimum: 4, score: 11 },
    { minimum: 3, score: 8 },
    { minimum: 2, score: 4 },
  ]);
}

function calculateLossAbsScore(value) {
  return scoreFromMinimums(value, [
    { minimum: 6000, score: 18 },
    { minimum: 5000, score: 15 },
    { minimum: 4000, score: 12 },
    { minimum: 3000, score: 8 },
    { minimum: 2000, score: 4 },
  ]);
}

function calculateNetTotalScore(value) {
  return scoreFromMaximums(value, [
    { maximum: -5000, score: 14 },
    { maximum: -4000, score: 12 },
    { maximum: -3000, score: 9 },
    { maximum: -2000, score: 6 },
    { maximum: -1000, score: 3 },
  ]);
}

function calculateCompensationRateScore(value) {
  return scoreFromMaximums(value, [
    { maximum: 0.2, score: 10 },
    { maximum: 0.35, score: 8 },
    { maximum: 0.5, score: 6 },
    { maximum: 0.7, score: 3 },
    { maximum: 1, score: 1 },
  ]);
}

function calculateMaxWinScore(value) {
  return scoreFromMaximums(value, [
    { maximum: 500, score: 7 },
    { maximum: 1000, score: 5 },
    { maximum: 1500, score: 3 },
    { maximum: 2000, score: 1 },
  ]);
}

function calculateTodayDifferenceScore(value) {
  return scoreFromMaximums(value, [
    { maximum: -2000, score: 5 },
    { maximum: -1000, score: 4 },
    { maximum: -500, score: 3 },
    { maximum: 0, score: 2 },
    { maximum: 1000, score: 1 },
  ]);
}

function calculateTodaySettingScore(value) {
  if (!Number.isFinite(value)) {
    return 0;
  }
  return scoreFromMaximums(value, [
    { maximum: 2, score: 3 },
    { maximum: 3, score: 2 },
    { maximum: 4, score: 1 },
  ]);
}

function calculateAparkKasugaHuntScore(metrics) {
  const totalScore =
    calculateLossDaysScore(metrics.lossDays) +
    calculateStreakScore(metrics.streak) +
    calculateLossAbsScore(metrics.lossAbsTotal) +
    calculateNetTotalScore(metrics.netTotal) +
    calculateCompensationRateScore(metrics.compensationRate) +
    calculateMaxWinScore(metrics.maxWin) +
    calculateTodayDifferenceScore(metrics.todayDifference) +
    calculateTodaySettingScore(metrics.todaySetting);

  return clamp(totalScore, 0, 100);
}

function calculateGogoNetDipScore(value) {
  if (value <= -5000) {
    return 34;
  }
  if (value <= -4000) {
    return 30;
  }
  if (value <= -3000) {
    return 26;
  }
  if (value <= -2000) {
    return 21;
  }
  if (value <= -1500) {
    return 17;
  }
  if (value <= -1000) {
    return 10;
  }
  if (value <= -500) {
    return 5;
  }
  if (value >= 4000) {
    return -18;
  }
  if (value >= 3000) {
    return -14;
  }
  if (value >= 2000) {
    return -10;
  }
  if (value >= 1000) {
    return -5;
  }
  return 0;
}

function calculateGogoShortDipScore(value) {
  if (value <= -2500) {
    return 10;
  }
  if (value <= -2000) {
    return 8;
  }
  if (value <= -1500) {
    return 6;
  }
  if (value <= -1000) {
    return 4;
  }
  if (value <= -500) {
    return 2;
  }
  if (value >= 2500) {
    return -6;
  }
  if (value >= 1500) {
    return -4;
  }
  if (value >= 1000) {
    return -2;
  }
  return 0;
}

function calculateGogoLossDaysScore(value, netTotal) {
  if (value >= 6) {
    return 6;
  }
  if (value >= 5) {
    return 4;
  }
  if (value >= 4) {
    return 2;
  }
  if (value <= 1 && netTotal > 0) {
    return -3;
  }
  return 0;
}

function calculateGogoMachineScore(machineName) {
  return GOGO_ARENA_TENJIN_MACHINE_SCORES[normalizeText(machineName)] ?? 0;
}

function calculateGogoSlotScore(machineName, slotNumber) {
  const machineScores = GOGO_ARENA_TENJIN_SLOT_SCORES[normalizeText(machineName)];
  if (!machineScores) {
    return 0;
  }
  return machineScores[String(slotNumber ?? "").trim()] ?? 0;
}

function calculateGogoReferenceEventScore(metrics) {
  const previousSetting = metrics.previousReferenceEventSetting;
  if (!Number.isFinite(previousSetting)) {
    return 0;
  }
  if (previousSetting >= 5) {
    return 13;
  }
  if (previousSetting >= 4.5) {
    return 11;
  }
  if (previousSetting >= 4) {
    return 9;
  }
  if (previousSetting >= 3.5) {
    return 4;
  }
  if (previousSetting < 3 && metrics.referenceEventSampleCount > 0) {
    return -2;
  }
  return 0;
}

function calculateGogoReferenceEventHistoryScore(metrics) {
  if (metrics.referenceEventHighSettingCount >= 2) {
    return 6;
  }
  if (metrics.referenceEventHighSettingCount === 1) {
    return 2;
  }
  if (metrics.referenceEventSampleCount >= 3) {
    return -4;
  }
  return 0;
}

function calculateGogoReferenceEventDipComboScore(metrics) {
  const previousSetting = metrics.previousReferenceEventSetting;
  if (previousSetting >= 4 && metrics.netTotal <= -3000) {
    return 10;
  }
  if (previousSetting >= 4 && metrics.netTotal <= -1500) {
    return 7;
  }
  if (metrics.referenceEventHighSettingCount >= 2 && metrics.netTotal <= -1500) {
    return 4;
  }
  return 0;
}

function calculateGogoGameTrustScore(value) {
  if (value >= 2500) {
    return 3;
  }
  if (value >= 1500) {
    return 1;
  }
  if (value < 800) {
    return -4;
  }
  if (value < 1200) {
    return -2;
  }
  return 0;
}

function calculateGogoRecentHighSettingScore(value) {
  if (value >= 3) {
    return 3;
  }
  if (value >= 2) {
    return 2;
  }
  return 0;
}

function calculateGogoArenaTenjinHuntScore(metrics) {
  const totalScore =
    20 +
    calculateGogoNetDipScore(metrics.netTotal) +
    calculateGogoShortDipScore(metrics.recentThreeNetTotal) +
    calculateGogoLossDaysScore(metrics.lossDays, metrics.netTotal) +
    calculateGogoMachineScore(metrics.machineName) +
    calculateGogoSlotScore(metrics.machineName, metrics.slotNumber) +
    calculateGogoReferenceEventScore(metrics) +
    calculateGogoReferenceEventHistoryScore(metrics) +
    calculateGogoReferenceEventDipComboScore(metrics) +
    calculateGogoGameTrustScore(metrics.averageGames) +
    calculateGogoRecentHighSettingScore(metrics.highSettingCount);

  return clamp(totalScore, 0, 100);
}

function calculateTamayaRecentNetScore(metrics) {
  const value = metrics.recentThreeNetTotal;
  if (!Number.isFinite(value)) {
    return 0;
  }

  if (value <= -4500) {
    return 30;
  }
  if (value <= -3500) {
    return 25;
  }
  if (value <= -2800) {
    return 21;
  }
  if (value <= -2200) {
    return 17;
  }
  if (value <= -1600) {
    return 12;
  }
  if (value <= -1000) {
    return 7;
  }
  if (value >= 4000) {
    return -20;
  }
  if (value >= 3000) {
    return -16;
  }
  if (value >= 2200) {
    return -12;
  }
  if (value >= 1500) {
    return -8;
  }
  if (value >= 800) {
    return -4;
  }
  return 0;
}

function calculateTamayaRecentBonusScore(metrics) {
  const value = metrics.recentThreeBonusTotal;
  if (!Number.isFinite(value)) {
    return 0;
  }

  if (value <= 8) {
    return 16;
  }
  if (value <= 11) {
    return 13;
  }
  if (value <= 14) {
    return 10;
  }
  if (value <= 18) {
    return 6;
  }
  if (value >= 45) {
    return -14;
  }
  if (value >= 38) {
    return -10;
  }
  if (value >= 32) {
    return -7;
  }
  if (value >= 26) {
    return -4;
  }
  return 0;
}

function calculateTamayaRecentGamesScore(metrics) {
  const value = metrics.recentThreeGamesTotal;
  if (!Number.isFinite(value)) {
    return 0;
  }

  if (value <= 1800) {
    return 11;
  }
  if (value <= 2600) {
    return 9;
  }
  if (value <= 3600) {
    return 6;
  }
  if (value <= 4800) {
    return 3;
  }
  if (value >= 15000) {
    return -10;
  }
  if (value >= 12000) {
    return -7;
  }
  if (value >= 9500) {
    return -4;
  }
  return 0;
}

function calculateTamayaSevenDayNetScore(metrics) {
  const value = metrics.netTotal;
  if (!Number.isFinite(value)) {
    return 0;
  }

  if (value <= -7000) {
    return 10;
  }
  if (value <= -5000) {
    return 8;
  }
  if (value <= -3500) {
    return 6;
  }
  if (value <= -2000) {
    return 3;
  }
  if (value >= 7000) {
    return -8;
  }
  if (value >= 5000) {
    return -6;
  }
  if (value >= 3000) {
    return -3;
  }
  return 0;
}

function calculateTamayaPreviousDayShapeScore(metrics) {
  const recentThreeNetTotal = metrics.recentThreeNetTotal;
  const todayDifference = metrics.todayDifference;
  if (!Number.isFinite(recentThreeNetTotal) || !Number.isFinite(todayDifference)) {
    return 0;
  }

  if (recentThreeNetTotal <= -2200) {
    if (todayDifference > -1000) {
      return 8;
    }
    if (todayDifference <= -2500) {
      return 1;
    }
    return 4;
  }
  if (todayDifference >= 1500) {
    return -6;
  }
  if (todayDifference >= 800) {
    return -3;
  }
  return 0;
}

function calculateTamayaHighSettingScore(metrics) {
  let score = 0;

  if (metrics.highSettingCount === 0) {
    score += 10;
  } else if (metrics.highSettingCount >= 3) {
    score -= 14;
  } else if (metrics.highSettingCount >= 2) {
    score -= 8;
  } else {
    score -= 2;
  }

  if (metrics.recentThreeHighSettingCount === 0) {
    score += 4;
  } else if (metrics.recentThreeHighSettingCount >= 2) {
    score -= 14;
  } else {
    score -= 8;
  }

  if (Number.isFinite(metrics.todaySetting)) {
    if (metrics.todaySetting >= 4) {
      score -= 10;
    } else if (metrics.todaySetting >= 3.5) {
      score -= 4;
    } else if (metrics.todaySetting <= 3) {
      score += 2;
    }
  }

  return score;
}

function readSlotLastDigit(slotNumber) {
  const match = /(\d)$/u.exec(String(slotNumber ?? "").trim());
  return match ? Number(match[1]) : null;
}

function calculateTamayaSlotScore(slotNumber) {
  const normalizedSlotNumber = String(slotNumber ?? "").trim();
  const slotNumberValue = Number(normalizedSlotNumber);
  const fixedScore = TAMAYA_ZASSHONOKUMA_SLOT_SCORES[normalizedSlotNumber] ?? 0;
  const cornerScore = [69, 79, 97, 109].includes(slotNumberValue) ? 2 : 0;
  const lastDigit = readSlotLastDigit(slotNumber);
  const tailScore = [3, 7, 9].includes(lastDigit) ? 1 : [0, 1, 2].includes(lastDigit) ? -1 : 0;
  return fixedScore + cornerScore + tailScore;
}

function calculateTamayaLongTermSettingScore(metrics) {
  const sampleCount = metrics.historySettingSampleCount;
  const rate = metrics.historyHighSettingRate;
  if (!Number.isFinite(sampleCount) || sampleCount < 30 || !Number.isFinite(rate)) {
    return 0;
  }

  if (rate >= 0.21) {
    return 8;
  }
  if (rate >= 0.185) {
    return 6;
  }
  if (rate >= 0.165) {
    return 4;
  }
  if (rate <= 0.085) {
    return -8;
  }
  if (rate <= 0.11) {
    return -6;
  }
  if (rate <= 0.125) {
    return -3;
  }
  return 0;
}

function calculateTamayaZasshonokumaHuntScore(metrics) {
  const totalScore =
    40 +
    calculateTamayaRecentNetScore(metrics) +
    calculateTamayaRecentBonusScore(metrics) +
    calculateTamayaRecentGamesScore(metrics) +
    calculateTamayaSevenDayNetScore(metrics) +
    calculateTamayaPreviousDayShapeScore(metrics) +
    calculateTamayaHighSettingScore(metrics) +
    calculateTamayaLongTermSettingScore(metrics) +
    calculateTamayaSlotScore(metrics.slotNumber);

  return clamp(totalScore, 0, 100);
}

function calculateTamayaOhashiCandidatePointScore(pointCount) {
  if (pointCount >= 3) {
    return 100;
  }
  if (pointCount >= 2) {
    return 90;
  }
  if (pointCount >= 1) {
    return 70;
  }
  return 0;
}

function calculateTamayaOhashiSupportScore(metrics) {
  let score = 0;

  if (metrics.recentFiveNetTotal >= 3000) {
    score += 22;
  } else if (metrics.recentThreeNetTotal >= 3000) {
    score += 20;
  } else if (metrics.recentFiveNetTotal >= 1500) {
    score += 10;
  }

  if (metrics.recentThreeBonusTotal >= 120) {
    score += 20;
  } else if (metrics.recentThreeBonusTotal >= 100) {
    score += 10;
  }

  if (metrics.recentFiveRbTotal >= 75) {
    score += 18;
  } else if (metrics.recentFiveRbTotal >= 60) {
    score += 8;
  }

  return Math.min(score, 55);
}

function calculateTamayaOhashiHuntScore(metrics) {
  let pointCount = 0;

  if (metrics.recentTwoGamesTotal >= 14000) {
    pointCount += 1;
  }
  if (metrics.recentTwoSettingAverage >= 4) {
    pointCount += 1;
  }
  if (metrics.recentFiveSettingAverage >= 4) {
    pointCount += 1;
  }

  let score = Math.max(
    calculateTamayaOhashiCandidatePointScore(pointCount),
    calculateTamayaOhashiSupportScore(metrics),
  );

  if (metrics.netTotal <= -3000) {
    score = Math.min(score, 20);
  } else if (metrics.todayDifference <= -1000) {
    score = Math.min(score, 45);
  }

  if (metrics.highSettingStreak >= 2) {
    score = Math.min(score, 55);
  }

  return clamp(score, 0, 100);
}

function calculate123HakataTailScore(metrics, context = {}) {
  const targetDay = readDateDay(context.nextBusinessDate ?? context.baseDate);
  const slotTail = readSlotLastDigit(metrics.slotNumber);
  if (!Number.isFinite(targetDay) || !Number.isFinite(slotTail)) {
    return 0;
  }

  const targetTail = targetDay % 10;
  if (slotTail !== targetTail) {
    return targetTail === 3 ? 4 : 0;
  }

  let score = 48;
  if ([3, 5, 9].includes(slotTail)) {
    score += 7;
  } else if ([1, 2, 6, 7].includes(slotTail)) {
    score += 5;
  } else if ([4, 8].includes(slotTail)) {
    score += 2;
  }
  return score;
}

function is123HakataTailMatched(metrics, context = {}) {
  const targetDay = readDateDay(context.nextBusinessDate ?? context.baseDate);
  const slotTail = readSlotLastDigit(metrics.slotNumber);
  return Number.isFinite(targetDay) && Number.isFinite(slotTail) && slotTail === targetDay % 10;
}

function calculate123HakataMachineScore(machineName) {
  const normalized = normalizeText(machineName);
  if (normalized === normalizeText("ネオアイムジャグラーEX")) {
    return 10;
  }
  if (normalized === normalizeText("マイジャグラーV")) {
    return 3;
  }
  return 0;
}

function calculate123HakataRecentDipScore(metrics) {
  let score = 0;

  if (metrics.recentThreeNetTotal <= -3000) {
    score += 8;
  } else if (metrics.recentThreeNetTotal <= -2000) {
    score += 6;
  } else if (metrics.recentThreeNetTotal <= -1000) {
    score += 3;
  } else if (metrics.recentThreeNetTotal >= 5000) {
    score -= 6;
  } else if (metrics.recentThreeNetTotal >= 3000) {
    score -= 4;
  }

  if (metrics.recentTwoNetTotal <= -2000) {
    score += 5;
  } else if (metrics.recentTwoNetTotal <= -1000) {
    score += 3;
  } else if (metrics.recentTwoNetTotal >= 3000) {
    score -= 4;
  }

  if (metrics.todayDifference <= -1000) {
    score += 3;
  } else if (metrics.todayDifference >= 3000) {
    score -= 4;
  } else if (metrics.todayDifference >= 1000) {
    score -= 2;
  }

  return score;
}

function calculate123HakataActivityScore(metrics) {
  const recentThreeAverageGames = metrics.recentThreeGamesTotal / 3;
  if (recentThreeAverageGames <= 3000) {
    return 6;
  }
  if (recentThreeAverageGames <= 4000) {
    return 3;
  }
  if (recentThreeAverageGames >= 7000) {
    return -6;
  }
  if (recentThreeAverageGames >= 6000) {
    return -3;
  }
  return 0;
}

function calculate123HakataPreviousHighScore(metrics, tailMatched) {
  const previousHighCandidate = metrics.todaySetting >= 4.5 && metrics.previousRbCount >= 25;
  if (!previousHighCandidate) {
    return 0;
  }
  if (metrics.todayDifference < 0) {
    return 8;
  }
  if (metrics.todayDifference > 0) {
    return tailMatched ? 2 : -8;
  }
  return 0;
}

function calculate123HakataNoInputScore(metrics) {
  if (metrics.highSettingCandidateCount <= 0) {
    return 5;
  }
  if (metrics.highSettingCandidateCount === 1) {
    return 2;
  }
  if (metrics.highSettingCandidateCount === 2) {
    return -3;
  }
  return -6;
}

function calculate123HakataHuntScore(metrics, context = {}) {
  const tailMatched = is123HakataTailMatched(metrics, context);
  const totalScore =
    calculate123HakataTailScore(metrics, context) +
    calculate123HakataMachineScore(metrics.machineName) +
    calculate123HakataRecentDipScore(metrics) +
    calculate123HakataActivityScore(metrics) +
    calculate123HakataPreviousHighScore(metrics, tailMatched) +
    calculate123HakataNoInputScore(metrics);

  return clamp(totalScore, 0, 100);
}

function readBoomTenjinTargetDay(context = {}) {
  return readDateDay(context.nextBusinessDate ?? context.baseDate);
}

function isBoomTenjinMyJuggler(machineName) {
  return normalizeText(machineName) === normalizeText("マイジャグラーV");
}

function isBoomTenjinNeo(machineName) {
  return normalizeText(machineName) === normalizeText("ネオアイムジャグラーEX");
}

function calculateBoomTenjinDatePoint(machineName, context, highMode = false) {
  const targetDay = readBoomTenjinTargetDay(context);
  if (!Number.isFinite(targetDay)) {
    return 0;
  }
  if (isBoomTenjinMyJuggler(machineName)) {
    return BOOM_TENJIN_MYJUGGLER_STRONG_DAYS.has(targetDay) ? (highMode ? 10 : 12) : -10;
  }
  if (isBoomTenjinNeo(machineName)) {
    return BOOM_TENJIN_NEO_STRONG_DAYS.has(targetDay) ? (highMode ? 12 : 8) : -12;
  }
  return 0;
}

function calculateBoomTenjinSlotPoint(machineName, slotNumber, highMode = false) {
  const normalizedSlot = String(slotNumber ?? "").trim();
  if (isBoomTenjinMyJuggler(machineName)) {
    if (BOOM_TENJIN_MYJUGGLER_PREFERRED_SLOTS.has(normalizedSlot)) {
      return highMode ? 7 : 9;
    }
    if (BOOM_TENJIN_MYJUGGLER_SECONDARY_SLOTS.has(normalizedSlot)) {
      return highMode ? 4 : 5;
    }
    if (BOOM_TENJIN_MYJUGGLER_WEAK_SLOTS.has(normalizedSlot)) {
      return highMode ? -10 : -12;
    }
    return 0;
  }
  if (isBoomTenjinNeo(machineName)) {
    if (BOOM_TENJIN_NEO_BEST_SLOTS.has(normalizedSlot)) {
      return highMode ? 10 : 6;
    }
    if (BOOM_TENJIN_NEO_SECONDARY_SLOTS.has(normalizedSlot)) {
      return highMode ? 6 : 4;
    }
    if (BOOM_TENJIN_NEO_WEAK_SLOTS.has(normalizedSlot)) {
      return -12;
    }
  }
  return 0;
}

function calculateBoomTenjinMyJugglerHistoryPoint(metrics, highMode = false) {
  let point = 3;

  if (metrics.netTotal <= -4500) {
    point += 3;
  } else if (metrics.netTotal <= -1500) {
    point += 6;
  } else if (metrics.netTotal >= 3000) {
    point -= 5;
  }

  const recentThreeAverageGames = metrics.recentThreeGamesTotal / 3;
  if (recentThreeAverageGames >= 1000 && recentThreeAverageGames <= 1500) {
    point += 6;
  } else if (recentThreeAverageGames < 1000) {
    point += 2;
  } else if (recentThreeAverageGames <= 2500) {
    point += 3;
  } else if (recentThreeAverageGames >= 7000) {
    point -= 7;
  } else if (recentThreeAverageGames >= 5000) {
    point -= 4;
  }

  if (metrics.recentThreeRbTotal <= 10) {
    point += highMode ? 5 : 4;
  } else if (metrics.recentThreeRbTotal >= 25) {
    point -= 5;
  }

  if (metrics.daysSinceHighSettingCandidate >= 4 && metrics.daysSinceHighSettingCandidate <= 7) {
    point += highMode ? 8 : 6;
  } else if (metrics.daysSinceHighSettingCandidate >= 2 && metrics.daysSinceHighSettingCandidate <= 3) {
    point -= 8;
  } else if (metrics.daysSinceHighSettingCandidate === 1) {
    point -= 2;
  } else if (!Number.isFinite(metrics.daysSinceHighSettingCandidate)) {
    point += 2;
  }

  if (metrics.highSettingCandidateCount >= 2) {
    point -= highMode ? 7 : 8;
  } else if (metrics.highSettingCandidateCount === 0) {
    point += 3;
  }

  return point;
}

function calculateBoomTenjinNeoHistoryPoint(metrics, highMode = false) {
  let point = -3;

  if (metrics.daysSinceHighSettingCandidate === 1) {
    point -= 12;
  } else if (metrics.daysSinceHighSettingCandidate >= 4 && metrics.daysSinceHighSettingCandidate <= 7) {
    point += highMode ? 8 : 5;
  } else if (metrics.daysSinceHighSettingCandidate >= 2 && metrics.daysSinceHighSettingCandidate <= 3) {
    point -= 6;
  } else if (!Number.isFinite(metrics.daysSinceHighSettingCandidate)) {
    point += 2;
  }

  if (metrics.previousRbCount >= 4 && metrics.previousRbCount <= 8) {
    point += 5;
  } else if (metrics.previousRbCount <= 3) {
    point -= 3;
  } else if (metrics.previousRbCount >= 15) {
    point -= 4;
  }

  if (metrics.recentTwoNetTotal >= 1500 && metrics.recentTwoNetTotal <= 3000) {
    point += 6;
  } else if (metrics.recentTwoNetTotal > 3000) {
    point += 2;
  } else if (metrics.recentTwoNetTotal <= -3000) {
    point -= 3;
  }

  if (metrics.recentThreeNetTotal >= -3000 && metrics.recentThreeNetTotal <= -2000) {
    point += 6;
  } else if (metrics.recentThreeNetTotal < -3000) {
    point += 3;
  } else if (metrics.recentThreeNetTotal >= 2000) {
    point -= 3;
  }

  if (metrics.highSettingCandidateCount >= 2) {
    point -= 8;
  } else if (metrics.highSettingCandidateCount === 0) {
    point += 2;
  }

  if (metrics.todaySetting >= 5) {
    point -= 8;
  }

  return point;
}

function calculateBoomTenjinScoreFromRaw(rawScore) {
  return 100 / (1 + Math.exp(-(rawScore - 50) / 12));
}

function calculateBoomTenjinHuntScore(metrics, context = {}) {
  const machineName = metrics.machineName;
  if (!isBoomTenjinMyJuggler(machineName) && !isBoomTenjinNeo(machineName)) {
    return 0;
  }

  const rawHigh =
    50 +
    calculateBoomTenjinDatePoint(machineName, context, true) +
    calculateBoomTenjinSlotPoint(machineName, metrics.slotNumber, true) +
    (isBoomTenjinMyJuggler(machineName)
      ? calculateBoomTenjinMyJugglerHistoryPoint(metrics, true)
      : calculateBoomTenjinNeoHistoryPoint(metrics, true));

  const rawFive =
    50 +
    calculateBoomTenjinDatePoint(machineName, context, false) +
    calculateBoomTenjinSlotPoint(machineName, metrics.slotNumber, false) +
    (isBoomTenjinMyJuggler(machineName)
      ? calculateBoomTenjinMyJugglerHistoryPoint(metrics, false)
      : calculateBoomTenjinNeoHistoryPoint(metrics, false));

  const weightedRaw = isBoomTenjinMyJuggler(machineName)
    ? rawHigh * 0.25 + rawFive * 0.75
    : rawHigh * 0.75 + rawFive * 0.25;

  return clamp(calculateBoomTenjinScoreFromRaw(weightedRaw), 0, 100);
}

function isBeamHikariMyJuggler(machineName) {
  return normalizeText(machineName) === normalizeText("マイジャグラーV");
}

function isBeamHikariNeo(machineName) {
  return normalizeText(machineName) === normalizeText("ネオアイムジャグラーEX");
}

function isBeamHikariTargetMachine(machineName) {
  return isBeamHikariMyJuggler(machineName) || isBeamHikariNeo(machineName);
}

function isBeamHikariPreviousHighCandidate(metrics) {
  return metrics.todaySetting >= 4.5 && metrics.previousRbCount >= 25;
}

function calculateBeamHikariASlotScore(metrics) {
  const slotNumber = String(metrics.slotNumber ?? "").trim();
  if (isBeamHikariNeo(metrics.machineName)) {
    if (BEAM_HIKARI_A_NEO_STRONG_SLOTS.has(slotNumber)) {
      return 14;
    }
    if (BEAM_HIKARI_A_NEO_SECONDARY_SLOTS.has(slotNumber)) {
      return 8;
    }
    if (BEAM_HIKARI_A_NEO_WEAK_SLOTS.has(slotNumber)) {
      return -6;
    }
    return 0;
  }
  if (isBeamHikariMyJuggler(metrics.machineName)) {
    if (BEAM_HIKARI_A_MYJUGGLER_STRONG_SLOTS.has(slotNumber)) {
      return 14;
    }
    if (BEAM_HIKARI_A_MYJUGGLER_SECONDARY_SLOTS.has(slotNumber)) {
      return 8;
    }
    if (BEAM_HIKARI_A_MYJUGGLER_WEAK_SLOTS.has(slotNumber)) {
      return -6;
    }
  }
  return 0;
}

function calculateBeamHikariADipScore(metrics) {
  let score = 0;

  if (metrics.todayDifference <= -2000) {
    score += 10;
  } else if (metrics.todayDifference <= -1000) {
    score += 8;
  } else if (metrics.todayDifference <= -500) {
    score += 5;
  } else if (metrics.todayDifference < 0) {
    score += 2;
  }

  if (metrics.recentTwoNetTotal <= -2000) {
    score += 7;
  } else if (metrics.recentTwoNetTotal <= -1500) {
    score += 5;
  } else if (metrics.recentTwoNetTotal <= -500) {
    score += 2;
  }

  if (metrics.recentThreeNetTotal <= -3000) {
    score += 4;
  } else if (metrics.recentThreeNetTotal <= -2000) {
    score += 3;
  }

  if (metrics.netTotal <= -5000) {
    score += 3;
  } else if (metrics.netTotal <= -3000) {
    score += 2;
  }

  return Math.min(score, 22);
}

function calculateBeamHikariALowActivityScore(metrics) {
  let score = 0;

  if (metrics.previousGames <= 1500) {
    score += 10;
  } else if (metrics.previousGames <= 2000) {
    score += 8;
  } else if (metrics.previousGames <= 3000) {
    score += 6;
  } else if (metrics.previousGames <= 4000) {
    score += 3;
  }

  if (metrics.recentTwoGamesTotal <= 5000) {
    score += 8;
  } else if (metrics.recentTwoGamesTotal <= 6000) {
    score += 6;
  } else if (metrics.recentTwoGamesTotal <= 8000) {
    score += 3;
  }

  if (metrics.recentThreeGamesTotal <= 9000) {
    score += 4;
  }
  if (metrics.gamesTotal <= 21000) {
    score += 2;
  }

  return Math.min(score, 24);
}

function calculateBeamHikariARbShortageScore(metrics) {
  let score = 0;

  if (metrics.previousRbCount <= 5) {
    score += 7;
  } else if (metrics.previousRbCount <= 10) {
    score += 4;
  } else if (metrics.previousRbCount <= 15) {
    score += 2;
  }

  if (metrics.recentTwoRbTotal <= 15) {
    score += 7;
  } else if (metrics.recentTwoRbTotal <= 20) {
    score += 5;
  } else if (metrics.recentTwoRbTotal <= 30) {
    score += 2;
  }

  if (metrics.rbTotal <= 60) {
    score += 4;
  } else if (metrics.rbTotal <= 70) {
    score += 2;
  }

  if (metrics.previousBonusTotal <= 15) {
    score += 3;
  } else if (metrics.previousBonusTotal <= 25) {
    score += 1;
  }

  return Math.min(score, 18);
}

function hasBeamHikariAdjacentPreviousHighCandidate(metrics, context = {}) {
  const slotNumber = Number(String(metrics.slotNumber ?? "").trim());
  if (!Number.isFinite(slotNumber) || !Array.isArray(context.metricsList)) {
    return false;
  }

  return context.metricsList.some((otherMetrics) => {
    if (!otherMetrics || otherMetrics === metrics || otherMetrics.machineName !== metrics.machineName) {
      return false;
    }
    const otherSlotNumber = Number(String(otherMetrics.slotNumber ?? "").trim());
    return (
      Number.isFinite(otherSlotNumber) &&
      Math.abs(otherSlotNumber - slotNumber) === 1 &&
      isBeamHikariPreviousHighCandidate(otherMetrics)
    );
  });
}

function calculateBeamHikariARotationScore(metrics, context = {}) {
  let score = 0;

  if (metrics.highSettingCandidateCount === 0) {
    score += 7;
  } else if (metrics.highSettingCandidateCount === 1) {
    score += 4;
  }

  if (metrics.threeDaysAgoHighSettingCandidate) {
    score += 5;
  }
  if (metrics.fourDaysAgoHighSettingCandidate) {
    score += 4;
  }
  if (hasBeamHikariAdjacentPreviousHighCandidate(metrics, context)) {
    score += 3;
  }

  return Math.min(score, 20);
}

function calculateBeamHikariAPenaltyScore(metrics) {
  let score = 0;

  if (metrics.todaySetting >= 5) {
    score += 25;
  } else if (metrics.todaySetting >= 4.5 || isBeamHikariPreviousHighCandidate(metrics)) {
    score += 20;
  }
  if (metrics.twoDaysAgoHighSettingCandidate) {
    score += 12;
  }
  if (metrics.highSettingCandidateCount >= 2) {
    score += 8;
  }

  if (metrics.todayDifference >= 2000) {
    score += 10;
  } else if (metrics.todayDifference >= 1000) {
    score += 6;
  } else if (metrics.todayDifference > 0) {
    score += 2;
  }

  if (metrics.recentTwoNetTotal >= 2500) {
    score += 8;
  } else if (metrics.recentTwoNetTotal >= 1500) {
    score += 5;
  }

  if (metrics.recentThreeNetTotal >= 3000) {
    score += 5;
  } else if (metrics.recentThreeNetTotal >= 2000) {
    score += 3;
  }

  if (metrics.netTotal >= 5000) {
    score += 4;
  } else if (metrics.netTotal >= 3000) {
    score += 2;
  }

  if (metrics.previousGames >= 8000) {
    score += 8;
  } else if (metrics.previousGames >= 7000) {
    score += 6;
  } else if (metrics.previousGames >= 6000) {
    score += 3;
  }
  if (metrics.recentTwoGamesTotal >= 14000) {
    score += 4;
  }
  if (metrics.recentThreeGamesTotal >= 21000) {
    score += 3;
  }
  if (metrics.gamesTotal >= 42000) {
    score += 2;
  }

  if (metrics.previousRbCount >= 30) {
    score += 5;
  } else if (metrics.previousRbCount >= 25) {
    score += 3;
  }
  if (metrics.recentTwoRbTotal >= 50) {
    score += 3;
  }
  if (metrics.rbTotal >= 140) {
    score += 2;
  }
  if (metrics.previousBonusTotal >= 60) {
    score += 3;
  }

  return score;
}

function calculateBeamHikariAHuntScore(metrics, context = {}) {
  if (!isBeamHikariTargetMachine(metrics.machineName)) {
    return 0;
  }

  const totalScore =
    calculateBeamHikariASlotScore(metrics) +
    calculateBeamHikariADipScore(metrics) +
    calculateBeamHikariALowActivityScore(metrics) +
    calculateBeamHikariARbShortageScore(metrics) +
    calculateBeamHikariARotationScore(metrics, context) -
    calculateBeamHikariAPenaltyScore(metrics);

  return clamp(totalScore, 0, 100);
}

function calculateBeamHikariBSlotPoint(metrics) {
  const slotNumber = String(metrics.slotNumber ?? "").trim();
  if (isBeamHikariNeo(metrics.machineName)) {
    if (BEAM_HIKARI_B_NEO_A_SLOTS.has(slotNumber)) {
      return 3.1;
    }
    if (BEAM_HIKARI_B_NEO_B_SLOTS.has(slotNumber)) {
      return 1.5;
    }
    if (BEAM_HIKARI_B_NEO_D_SLOTS.has(slotNumber)) {
      return -3.2;
    }
    return -0.7;
  }
  if (isBeamHikariMyJuggler(metrics.machineName)) {
    if (BEAM_HIKARI_B_MYJUGGLER_A_SLOTS.has(slotNumber)) {
      return 3.1;
    }
    if (BEAM_HIKARI_B_MYJUGGLER_B_SLOTS.has(slotNumber)) {
      return 1.5;
    }
    if (BEAM_HIKARI_B_MYJUGGLER_D_SLOTS.has(slotNumber)) {
      return -3.2;
    }
    return -0.7;
  }
  return 0;
}

function calculateBeamHikariBDifferencePoint(metrics) {
  let point = 0;

  if (metrics.recentTwoNetTotal <= -3000) {
    point += 7.5;
  } else if (metrics.recentTwoNetTotal <= -2000) {
    point += 4;
  } else if (metrics.recentTwoNetTotal <= -1000) {
    point += 2.8;
  } else if (metrics.recentTwoNetTotal <= 1000) {
    point += 0.1;
  } else if (metrics.recentTwoNetTotal <= 2000) {
    point -= 8.3;
  } else {
    point -= 7.2;
  }

  if (metrics.recentThreeNetTotal <= -3000) {
    point += 4.8;
  } else if (metrics.recentThreeNetTotal <= -2000) {
    point += 3.3;
  } else if (metrics.recentThreeNetTotal <= -1000) {
    point += 0.5;
  } else if (metrics.recentThreeNetTotal <= 1000) {
    point += 0.3;
  } else if (metrics.recentThreeNetTotal <= 2000) {
    point -= 2.4;
  } else {
    point -= 3.6;
  }

  if (metrics.todayDifference <= -1500) {
    point += 3.6;
  } else if (metrics.todayDifference <= -1000) {
    point += 2.4;
  } else if (metrics.todayDifference < 0) {
    point += 1.7;
  } else if (metrics.todayDifference <= 1000) {
    point -= 3.8;
  } else {
    point -= 8.8;
  }

  return point;
}

function calculateBeamHikariBActivityPoint(metrics) {
  if (metrics.recentTwoGamesTotal < 4000) {
    return 4.8;
  }
  if (metrics.recentTwoGamesTotal < 6000) {
    return 2.5;
  }
  if (metrics.recentTwoGamesTotal < 10000) {
    return -3;
  }
  return -5.3;
}

function calculateBeamHikariBRbPoint(metrics) {
  let point = 0;

  if (metrics.recentTwoRbTotal <= 15) {
    point += 3.6;
  } else if (metrics.recentTwoRbTotal <= 30) {
    point -= 3.5;
  } else if (metrics.recentTwoRbTotal <= 45) {
    point -= 5.6;
  } else {
    point -= 8;
  }

  if (metrics.recentThreeRbTotal <= 25) {
    point += 2.8;
  } else if (metrics.recentThreeRbTotal <= 45) {
    point -= 1.9;
  } else if (metrics.recentThreeRbTotal <= 65) {
    point -= 3.8;
  } else {
    point -= 1;
  }

  if (metrics.previousRbCount <= 10) {
    point += 1.9;
  } else if (metrics.previousRbCount <= 20) {
    point -= 3.7;
  } else if (metrics.previousRbCount <= 25) {
    point -= 5.9;
  } else if (metrics.previousRbCount <= 30) {
    point -= 6.8;
  } else {
    point -= 6.5;
  }

  return point;
}

function calculateBeamHikariBDaysSinceHighPoint(daysSinceHigh) {
  if (!Number.isFinite(daysSinceHigh)) {
    return 1.7;
  }
  if (daysSinceHigh >= 8) {
    return 0.3;
  }
  if (daysSinceHigh >= 4) {
    return 1.5;
  }
  if (daysSinceHigh === 3) {
    return 2.2;
  }
  if (daysSinceHigh >= 1) {
    return -7.3;
  }
  return 0;
}

function calculateBeamHikariBDaysSinceFivePoint(daysSinceFive) {
  if (!Number.isFinite(daysSinceFive)) {
    return 0.9;
  }
  if (daysSinceFive >= 8) {
    return 0;
  }
  if (daysSinceFive >= 4) {
    return 1.6;
  }
  if (daysSinceFive === 3) {
    return 2;
  }
  if (daysSinceFive === 2) {
    return -4.3;
  }
  if (daysSinceFive === 1) {
    return -6.4;
  }
  return 0;
}

function calculateBeamHikariBRecentHighCountPoint(metrics) {
  if (metrics.recentFiveHighSettingCandidateCount === 0) {
    return 0.5;
  }
  if (metrics.recentFiveHighSettingCandidateCount === 1) {
    return -0.5;
  }
  return -4.1;
}

function calculateBeamHikariBHuntScore(metrics) {
  if (!isBeamHikariTargetMachine(metrics.machineName)) {
    return 0;
  }

  const machinePoint = isBeamHikariNeo(metrics.machineName) ? 0.6 : -1;
  const score =
    50 +
    machinePoint +
    calculateBeamHikariBSlotPoint(metrics) +
    calculateBeamHikariBDifferencePoint(metrics) +
    calculateBeamHikariBActivityPoint(metrics) +
    calculateBeamHikariBRbPoint(metrics) +
    calculateBeamHikariBDaysSinceHighPoint(metrics.daysSinceHighSettingCandidate) +
    calculateBeamHikariBDaysSinceFivePoint(metrics.daysSinceSettingFive) +
    calculateBeamHikariBRecentHighCountPoint(metrics);

  return clamp(score, 0, 100);
}

function calculateHinodeOnojoHuntScore(metrics) {
  const lossDays = metrics.lossDays;
  const netTotal = metrics.netTotal;
  if (!Number.isFinite(lossDays) || !Number.isFinite(netTotal)) {
    return 0;
  }

  if (netTotal >= 0) {
    return 0;
  }

  if (lossDays === 7) {
    if (netTotal <= -9000) {
      return 100;
    }
    if (netTotal <= -7000) {
      return 97;
    }
    if (netTotal <= -5500) {
      return 94;
    }
    if (netTotal <= -4000) {
      return 91;
    }
    if (netTotal <= -3000) {
      return 78;
    }
    return 68;
  }

  if (lossDays === 6) {
    if (netTotal <= -13000) {
      return 100;
    }
    if (netTotal <= -11000) {
      return 96;
    }
    if (netTotal <= -9500) {
      return 93;
    }
    if (netTotal <= -8500) {
      return 90;
    }
    if (netTotal <= -7000) {
      return 62;
    }
    if (netTotal <= -5500) {
      return 50;
    }
    return 35;
  }

  if (lossDays === 5) {
    if (netTotal <= -9000) {
      return 48;
    }
    if (netTotal <= -6500) {
      return 38;
    }
    if (netTotal <= -4000) {
      return 28;
    }
    return 18;
  }

  if (lossDays === 4) {
    return netTotal <= -6500 ? 24 : 12;
  }

  return 0;
}

function calculateHinodeOnojoAHuntScore(metrics) {
  const streak = metrics.streak;
  const winningStreak = metrics.winningStreak;
  const recentThreeNetTotal = metrics.recentThreeNetTotal;
  const netTotal = metrics.netTotal;
  const todayDifference = metrics.todayDifference;

  if (
    !Number.isFinite(streak) ||
    !Number.isFinite(winningStreak) ||
    !Number.isFinite(recentThreeNetTotal) ||
    !Number.isFinite(netTotal) ||
    !Number.isFinite(todayDifference)
  ) {
    return 0;
  }

  if (streak >= 4 && recentThreeNetTotal <= -3500 && netTotal <= -5000) {
    return 100;
  }
  if (streak >= 3 && recentThreeNetTotal <= -3500 && netTotal <= -5000) {
    return 95;
  }
  if (streak >= 4 && netTotal <= -5000) {
    return 90;
  }
  if (recentThreeNetTotal <= -4000) {
    return 88;
  }
  if (metrics.lossDays === 7) {
    return 80;
  }
  if (streak >= 4) {
    return 70;
  }
  if (winningStreak >= 3) {
    return 20;
  }
  if (netTotal >= 3000) {
    return 30;
  }
  if (todayDifference <= -1000) {
    return 55;
  }
  return 0;
}

function calculateHinodeOnojoBHuntScore(metrics) {
  const recentThreeNetTotal = metrics.recentThreeNetTotal;
  const recentFourNetTotal = metrics.recentFourNetTotal;
  const recentFiveNetTotal = metrics.recentFiveNetTotal;
  const recentSixNetTotal = metrics.recentSixNetTotal;
  const netTotal = metrics.netTotal;
  const recentFourPositiveCount = metrics.recentFourPositiveCount;

  if (
    !Number.isFinite(recentThreeNetTotal) ||
    !Number.isFinite(recentFourNetTotal) ||
    !Number.isFinite(recentFiveNetTotal) ||
    !Number.isFinite(recentSixNetTotal) ||
    !Number.isFinite(netTotal) ||
    !Number.isFinite(recentFourPositiveCount)
  ) {
    return 0;
  }

  if (recentThreeNetTotal <= -3500 && netTotal <= -5000) {
    return 100;
  }
  if (recentSixNetTotal >= 4000 || recentFourPositiveCount >= 3 || recentFourNetTotal >= 3000) {
    return 20;
  }
  if (recentFourNetTotal <= -4500 || recentSixNetTotal <= -5500) {
    return 95;
  }
  if (recentFiveNetTotal <= -4500) {
    return 90;
  }
  if (recentFourNetTotal <= -4000) {
    return 82;
  }
  if (recentFiveNetTotal <= -4000) {
    return 78;
  }
  return 0;
}

function calculateHinodeOnojoCHuntScore(metrics) {
  const recentThreeNetTotal = metrics.recentThreeNetTotal;
  const recentFourNetTotal = metrics.recentFourNetTotal;
  const recentSixNetTotal = metrics.recentSixNetTotal;
  const recentFourLossDays = metrics.recentFourLossDays;

  if (
    !Number.isFinite(recentThreeNetTotal) ||
    !Number.isFinite(recentFourNetTotal) ||
    !Number.isFinite(recentSixNetTotal) ||
    !Number.isFinite(recentFourLossDays)
  ) {
    return 0;
  }

  if (recentFourLossDays === 4 && recentFourNetTotal <= -5500) {
    return 100;
  }
  if (recentFourLossDays === 4 && recentFourNetTotal <= -5000) {
    return 95;
  }
  if (recentFourLossDays === 4 && recentFourNetTotal <= -4500) {
    return 90;
  }
  if (recentFourNetTotal >= 2500 || recentFourLossDays <= 2) {
    return 20;
  }
  if (recentFourNetTotal > 0) {
    return 30;
  }
  if (recentThreeNetTotal <= -4000) {
    return 88;
  }
  if (recentSixNetTotal <= -5500) {
    return 86;
  }
  return 0;
}

function calculateHinodeOnojoDHuntScore(metrics) {
  let score = 0;

  score += scoreFromMinimums(metrics.lossDays, [
    { minimum: 7, score: 18 },
    { minimum: 6, score: 15 },
    { minimum: 5, score: 11 },
    { minimum: 4, score: 7 },
    { minimum: 3, score: 4 },
  ]);
  score += scoreFromMinimums(metrics.streak, [
    { minimum: 7, score: 14 },
    { minimum: 6, score: 12 },
    { minimum: 5, score: 10 },
    { minimum: 4, score: 8 },
    { minimum: 3, score: 6 },
    { minimum: 2, score: 3 },
  ]);
  score += scoreFromMinimums(metrics.lossAbsTotal, [
    { minimum: 9000, score: 14 },
    { minimum: 7500, score: 12 },
    { minimum: 6000, score: 10 },
    { minimum: 4500, score: 7 },
    { minimum: 3000, score: 4 },
    { minimum: 2000, score: 2 },
  ]);
  score += scoreFromMaximums(metrics.netTotal, [
    { maximum: -9000, score: 14 },
    { maximum: -7500, score: 12 },
    { maximum: -6000, score: 10 },
    { maximum: -4500, score: 8 },
    { maximum: -3500, score: 6 },
    { maximum: -2500, score: 4 },
    { maximum: -1500, score: 2 },
  ]);
  score += scoreFromMaximums(metrics.recentThreeNetTotal, [
    { maximum: -4000, score: 10 },
    { maximum: -3500, score: 9 },
    { maximum: -2800, score: 7 },
    { maximum: -2200, score: 5 },
    { maximum: -1500, score: 3 },
    { maximum: -800, score: 1 },
  ]);
  score += Math.max(
    scoreFromMaximums(metrics.recentFourNetTotal, [
      { maximum: -5500, score: 10 },
      { maximum: -4500, score: 8 },
      { maximum: -3500, score: 5 },
      { maximum: -2500, score: 3 },
    ]),
    scoreFromMaximums(metrics.recentFiveNetTotal, [
      { maximum: -5500, score: 9 },
      { maximum: -4500, score: 7 },
      { maximum: -3500, score: 5 },
      { maximum: -2500, score: 3 },
    ]),
    scoreFromMaximums(metrics.recentSixNetTotal, [
      { maximum: -6500, score: 8 },
      { maximum: -5500, score: 7 },
      { maximum: -4500, score: 5 },
      { maximum: -3500, score: 3 },
    ]),
  );
  score += scoreFromMaximums(metrics.compensationRate, [
    { maximum: 0.2, score: 6 },
    { maximum: 0.35, score: 5 },
    { maximum: 0.5, score: 4 },
    { maximum: 0.75, score: 2 },
  ]);
  score += scoreFromMaximums(metrics.maxWin, [
    { maximum: 500, score: 5 },
    { maximum: 1000, score: 4 },
    { maximum: 1500, score: 2 },
  ]);
  score += scoreFromMaximums(metrics.todayDifference, [
    { maximum: -2000, score: 5 },
    { maximum: -1000, score: 4 },
    { maximum: -500, score: 3 },
    { maximum: -1, score: 2 },
  ]);

  if (metrics.netTotal >= 5000) {
    score -= 16;
  } else if (metrics.netTotal >= 3000) {
    score -= 10;
  } else if (metrics.netTotal >= 1500) {
    score -= 5;
  }

  if (metrics.winningStreak >= 3) {
    score -= 12;
  }
  if (metrics.recentFourPositiveCount >= 3) {
    score -= 8;
  }
  if (metrics.recentFourNetTotal >= 3000) {
    score -= 8;
  }
  if (metrics.lossDays <= 2) {
    score -= 6;
  }

  return clamp(score, 0, 100);
}

function buildWindowRows(businessDates, dateIndex, recordMapByDate, windowDays, config) {
  if (dateIndex < windowDays - 1) {
    return null;
  }

  const windowDates = businessDates.slice(dateIndex - (windowDays - 1), dateIndex + 1);
  if (windowDates.length < windowDays) {
    return null;
  }

  const windowRows = [];

  for (const date of windowDates) {
    const row = recordMapByDate.get(date);
    if (!row || !hasMeaningfulResult(row)) {
      return null;
    }

    windowRows.push({
      row,
      differenceValue: readHuntScoreDifferenceValue(
        row,
        config.differenceMode,
        normalizeHuntScoreMachineName(row?.machine_name, config),
      ),
    });
  }

  return windowRows;
}

function buildAvailableWindowRows(businessDates, dateIndex, recordMapByDate, windowDays, config) {
  const normalizedWindowDays = Math.max(1, Number(windowDays) || DEFAULT_HUNT_SCORE_WINDOW_DAYS);
  const startIndex = Math.max(0, dateIndex - (normalizedWindowDays - 1));
  const windowDates = businessDates.slice(startIndex, dateIndex + 1);
  const windowRows = [];

  for (const date of windowDates) {
    const row = recordMapByDate.get(date);
    if (!row || !hasMeaningfulResult(row)) {
      continue;
    }

    windowRows.push({
      row,
      differenceValue: readHuntScoreDifferenceValue(
        row,
        config.differenceMode,
        normalizeHuntScoreMachineName(row?.machine_name, config),
      ),
    });
  }

  return windowRows;
}

function calculateWindowMetrics(businessDates, dateIndex, row, recordMapByDate, settingDefinitionCache, config) {
  const windowRows = buildWindowRows(
    businessDates,
    dateIndex,
    recordMapByDate,
    config.windowDays ?? DEFAULT_HUNT_SCORE_WINDOW_DAYS,
    config,
  );
  if (!windowRows) {
    return null;
  }

  let lossDays = 0;
  let winAbsTotal = 0;
  let lossAbsTotal = 0;
  let netTotal = 0;
  let maxWin = 0;
  let gamesTotal = 0;
  let bbTotal = 0;
  let rbTotal = 0;
  let highSettingCount = 0;
  let highSettingCandidateCount = 0;
  const metricWindowRows = [];
  let historySettingSampleCount = 0;
  let historyHighSettingCount = 0;
  let historyNetTotal = 0;

  for (const windowRow of windowRows) {
    const differenceValue = windowRow.differenceValue;
    const games = readNumber(windowRow.row?.games_count) ?? 0;
    const bbCount = readNumber(windowRow.row?.bb_count) ?? 0;
    const rbCount = readNumber(windowRow.row?.rb_count) ?? 0;
    const settingAverage = getSettingEstimateAverage(settingDefinitionCache, windowRow.row, config).average;
    netTotal += differenceValue;
    gamesTotal += games;
    bbTotal += bbCount;
    rbTotal += rbCount;
    if (Number.isFinite(settingAverage) && settingAverage >= 4) {
      highSettingCount += 1;
    }
    if (Number.isFinite(settingAverage) && settingAverage >= 4.5 && rbCount >= 25) {
      highSettingCandidateCount += 1;
    }

    if (differenceValue < 0) {
      lossDays += 1;
      lossAbsTotal += Math.abs(differenceValue);
    } else if (differenceValue > 0) {
      winAbsTotal += differenceValue;
      maxWin = Math.max(maxWin, differenceValue);
    }

    metricWindowRows.push({
      ...windowRow,
      games,
      bbCount,
      rbCount,
      settingAverage,
    });
  }

  const historyWindowRows = buildAvailableWindowRows(
    businessDates,
    dateIndex,
    recordMapByDate,
    config.historyWindowDays ?? config.windowDays ?? DEFAULT_HUNT_SCORE_WINDOW_DAYS,
    config,
  );
  for (const historyWindowRow of historyWindowRows) {
    const settingAverage = getSettingEstimateAverage(
      settingDefinitionCache,
      historyWindowRow.row,
      config,
    ).average;
    historyNetTotal += historyWindowRow.differenceValue;
    if (Number.isFinite(settingAverage)) {
      historySettingSampleCount += 1;
      if (settingAverage >= 4) {
        historyHighSettingCount += 1;
      }
    }
  }

  const todaySetting = getSettingEstimateAverage(settingDefinitionCache, row, config).average;
  const previousWindowRow = metricWindowRows.at(-2) ?? null;
  const recentTwoRows = metricWindowRows.slice(-2);
  const recentThreeRows = metricWindowRows.slice(-3);
  const recentFourRows = metricWindowRows.slice(-4);
  const recentFiveRows = metricWindowRows.slice(-5);
  const recentSixRows = metricWindowRows.slice(-6);
  const recentTwoNetTotal = sumDifferenceValues(recentTwoRows);
  const recentThreeNetTotal = sumDifferenceValues(recentThreeRows);
  const recentFourNetTotal = sumDifferenceValues(recentFourRows);
  const recentFiveNetTotal = sumDifferenceValues(recentFiveRows);
  const recentSixNetTotal = sumDifferenceValues(recentSixRows);
  const recentFourLossDays = recentFourRows.filter((windowRow) => windowRow.differenceValue < 0).length;
  const recentFourPositiveCount = recentFourRows.filter((windowRow) => windowRow.differenceValue > 0).length;
  const recentTwoGamesTotal = recentTwoRows.reduce((total, windowRow) => total + windowRow.games, 0);
  const recentThreeGamesTotal = recentThreeRows.reduce((total, windowRow) => total + windowRow.games, 0);
  const recentThreeBonusTotal = recentThreeRows.reduce(
    (total, windowRow) => total + windowRow.bbCount + windowRow.rbCount,
    0,
  );
  const recentTwoRbTotal = recentTwoRows.reduce((total, windowRow) => total + windowRow.rbCount, 0);
  const recentThreeRbTotal = recentThreeRows.reduce((total, windowRow) => total + windowRow.rbCount, 0);
  const recentFiveRbTotal = recentFiveRows.reduce((total, windowRow) => total + windowRow.rbCount, 0);
  const recentTwoSettingAverage = calculateSettingAverageFromWindowRows(recentTwoRows);
  const recentFiveSettingAverage = calculateSettingAverageFromWindowRows(recentFiveRows);
  const recentThreeHighSettingCount = recentThreeRows.filter(
    (windowRow) => Number.isFinite(windowRow.settingAverage) && windowRow.settingAverage >= 4,
  ).length;
  const isHighSettingCandidateWindowRow = (windowRow) =>
    Boolean(
      windowRow &&
        Number.isFinite(windowRow.settingAverage) &&
        windowRow.settingAverage >= 4.5 &&
        windowRow.rbCount >= 25,
    );
  const twoDaysAgoHighSettingCandidate = isHighSettingCandidateWindowRow(metricWindowRows.at(-2));
  const threeDaysAgoHighSettingCandidate = isHighSettingCandidateWindowRow(metricWindowRows.at(-3));
  const fourDaysAgoHighSettingCandidate = isHighSettingCandidateWindowRow(metricWindowRows.at(-4));
  const recentFiveHighSettingCandidateCount = recentFiveRows.filter(isHighSettingCandidateWindowRow).length;
  const daysSinceHighSettingCandidate = (() => {
    for (let offset = 1; offset <= metricWindowRows.length; offset += 1) {
      const windowRow = metricWindowRows.at(-offset);
      if (isHighSettingCandidateWindowRow(windowRow)) {
        return offset;
      }
    }
    return null;
  })();
  const daysSinceSettingFive = (() => {
    for (let offset = 1; offset <= metricWindowRows.length; offset += 1) {
      const windowRow = metricWindowRows.at(-offset);
      if (windowRow && Number.isFinite(windowRow.settingAverage) && windowRow.settingAverage >= 5) {
        return offset;
      }
    }
    return null;
  })();
  const previousReferenceEventMetrics = calculatePreviousReferenceEventMetrics(
    businessDates,
    dateIndex,
    recordMapByDate,
    settingDefinitionCache,
    config,
  );

  return {
    machineName: normalizeHuntScoreMachineName(row?.machine_name, config),
    slotNumber: String(row?.slot_number ?? "").trim(),
    lossDays,
    streak: calculateCurrentLosingStreak(metricWindowRows),
    winningStreak: calculateCurrentWinningStreak(metricWindowRows),
    lossAbsTotal,
    netTotal,
    recentTwoNetTotal,
    recentThreeNetTotal,
    recentFourNetTotal,
    recentFiveNetTotal,
    recentSixNetTotal,
    recentFourLossDays,
    recentFourPositiveCount,
    compensationRate: lossAbsTotal === 0 ? 999 : winAbsTotal / lossAbsTotal,
    maxWin,
    todayDifference: readHuntScoreDifferenceValue(
      row,
      config.differenceMode,
      normalizeHuntScoreMachineName(row?.machine_name, config),
    ),
    previousDifference: previousWindowRow?.differenceValue ?? 0,
    previousGames: readNumber(row?.games_count) ?? 0,
    previousBbCount: readNumber(row?.bb_count) ?? 0,
    previousRbCount: readNumber(row?.rb_count) ?? 0,
    previousBonusTotal: (readNumber(row?.bb_count) ?? 0) + (readNumber(row?.rb_count) ?? 0),
    todaySetting,
    highSettingCount,
    highSettingCandidateCount,
    recentFiveHighSettingCandidateCount,
    twoDaysAgoHighSettingCandidate,
    threeDaysAgoHighSettingCandidate,
    fourDaysAgoHighSettingCandidate,
    daysSinceHighSettingCandidate,
    daysSinceSettingFive,
    highSettingStreak: calculateCurrentHighSettingStreak(metricWindowRows),
    recentThreeHighSettingCount,
    gamesTotal,
    averageGames: metricWindowRows.length > 0 ? gamesTotal / metricWindowRows.length : 0,
    recentTwoGamesTotal,
    recentThreeGamesTotal,
    recentThreeBonusTotal,
    recentTwoRbTotal,
    recentThreeRbTotal,
    recentFiveRbTotal,
    recentTwoSettingAverage,
    recentFiveSettingAverage,
    historySettingSampleCount,
    historyHighSettingCount,
    historyHighSettingRate:
      historySettingSampleCount > 0 ? historyHighSettingCount / historySettingSampleCount : null,
    historyNetTotal,
    bbTotal,
    rbTotal,
    bbRate: gamesTotal > 0 ? bbTotal / gamesTotal : 0,
    rbRate: gamesTotal > 0 ? rbTotal / gamesTotal : 0,
    ...previousReferenceEventMetrics,
  };
}

function buildBusinessDates(allStoreRows, targetRows) {
  const openDates = new Map();

  for (const row of [...allStoreRows, ...targetRows]) {
    const date = String(row?.target_date ?? "").trim();
    if (!date) {
      continue;
    }

    if (!openDates.has(date)) {
      openDates.set(date, false);
    }

    if (hasMeaningfulResult(row)) {
      openDates.set(date, true);
    }
  }

  return [...openDates.entries()]
    .filter((entry) => entry[1])
    .map((entry) => entry[0])
    .sort((left, right) => left.localeCompare(right));
}

function buildSourceMaps(targetRows, businessDateSet, config) {
  const rowsByCandidateKey = new Map();
  const rowsByDate = new Map();

  for (const row of targetRows) {
    if (
      !hasMeaningfulResult(row) ||
      !businessDateSet.has(row?.target_date) ||
      !findTargetMachine(config, row?.machine_name)
    ) {
      continue;
    }

    const candidateKey = buildCandidateKey(row, config);
    if (!rowsByCandidateKey.has(candidateKey)) {
      rowsByCandidateKey.set(candidateKey, new Map());
    }
    rowsByCandidateKey.get(candidateKey).set(row.target_date, row);

    if (!rowsByDate.has(row.target_date)) {
      rowsByDate.set(row.target_date, []);
    }
    rowsByDate.get(row.target_date).push(row);
  }

  return {
    rowsByCandidateKey,
    rowsByDate,
  };
}

function roundHuntScore(value) {
  return Number.isFinite(value) ? Math.round(clamp(value, 0, 100)) : null;
}

function buildSnapshotRowsForDate(
  businessDates,
  dateIndex,
  rowsByDate,
  rowsByCandidateKey,
  settingDefinitionCache,
  config,
) {
  const baseDate = businessDates[dateIndex];
  const nextBusinessDate = businessDates[dateIndex + 1] ?? null;
  const dateRows = rowsByDate.get(baseDate) ?? [];

  if (dateRows.length === 0) {
    return {
      baseDate,
      nextBusinessDate,
      rows: [],
    };
  }

  const candidates = dateRows.map((row) => {
    const candidateKey = buildCandidateKey(row, config);
    const recordMapByDate = rowsByCandidateKey.get(candidateKey) ?? new Map();
    const metrics = calculateWindowMetrics(
      businessDates,
      dateIndex,
      row,
      recordMapByDate,
      settingDefinitionCache,
      config,
    );

    return {
      row,
      rowKey: buildRowKey(row, config),
      candidateKey,
      metrics,
    };
  });
  const validCandidates = candidates.filter((candidate) => candidate.metrics);
  const context = {
    baseDate,
    nextBusinessDate,
    metricsList: validCandidates.map((candidate) => candidate.metrics),
  };

  const rows = validCandidates
    .map((candidate) => {
      const huntScore = roundHuntScore(config.scoreCalculator(candidate.metrics, context));
      if (!Number.isFinite(huntScore)) {
        return null;
      }

      const recordMapByDate = rowsByCandidateKey.get(candidate.candidateKey) ?? new Map();
      const nextRecord = nextBusinessDate ? recordMapByDate.get(nextBusinessDate) ?? null : null;
      const nextSetting = nextRecord
        ? getSettingEstimateAverage(settingDefinitionCache, nextRecord, config).estimate
        : null;

      return {
        baseDate,
        nextBusinessDate,
        rowKey: candidate.rowKey,
        machineName: normalizeHuntScoreMachineName(candidate.row.machine_name, config),
        slotNumber: candidate.row.slot_number,
        huntScore,
        currentRecord: candidate.row,
        nextRecord,
        nextSettingEstimate: nextSetting,
      };
    })
    .filter(Boolean)
    .sort((left, right) => {
      if (Math.abs(right.huntScore - left.huntScore) > HUNT_SCORE_EPSILON) {
        return right.huntScore - left.huntScore;
      }
      const machineComparison = left.machineName.localeCompare(right.machineName, "ja");
      if (machineComparison !== 0) {
        return machineComparison;
      }
      return String(left.slotNumber).localeCompare(String(right.slotNumber), "ja");
    })
    .map((row, index) => ({
      ...row,
      rank: index + 1,
    }));

  return {
    baseDate,
    nextBusinessDate,
    rows,
  };
}

export function isHuntScoreTargetStore(storeName) {
  return Boolean(normalizeText(storeName));
}

export function isHuntScoreTargetMachine(machineName, storeName = "") {
  return canonicalHuntScoreTargetMachineName(machineName, storeName) !== null;
}

export function isHuntScoreSupported(storeName, machineName) {
  return isHuntScoreTargetStore(storeName) && isHuntScoreTargetMachine(machineName, storeName);
}

export function listHuntScoreTargetMachineNames(storeName = "") {
  return listSearchConfigs(storeName).flatMap((config) =>
    config.targetMachines.map((targetMachine) => targetMachine.name),
  );
}

export function listHuntScoreTargetMachineNamesForStoreMachines(storeName = "", machineNames = null) {
  if (!Array.isArray(machineNames)) {
    return listHuntScoreTargetMachineNames(storeName);
  }
  if (machineNames.length === 0) {
    return [];
  }
  return buildEffectiveHuntScoreStoreConfig(storeName, machineNames).targetMachines.map(
    (targetMachine) => targetMachine.name,
  );
}

export function listAllHuntScoreTargetMachineNames() {
  return [
    ...new Set(
      [DEFAULT_HUNT_SCORE_STORE_CONFIG, ...HUNT_SCORE_STORE_CONFIGS]
        .flatMap((config) => config.targetMachines)
        .map((targetMachine) => targetMachine.name),
    ),
  ];
}

export function listHuntScoreSourceMachineNames(storeName = "") {
  return [
    ...new Set(
      listSearchConfigs(storeName)
        .flatMap((config) => config.targetMachines)
        .flatMap(listHuntScoreTargetMachineNameCandidates)
        .map((machineName) => String(machineName ?? "").trim())
        .filter(Boolean),
    ),
  ];
}

export function listHuntScoreSourceMachineNamesForStoreMachines(storeName = "", machineNames = null) {
  if (!Array.isArray(machineNames)) {
    return listHuntScoreSourceMachineNames(storeName);
  }
  if (machineNames.length === 0) {
    return [];
  }
  return [
    ...new Set(
      buildEffectiveHuntScoreStoreConfig(storeName, machineNames).targetMachines
        .flatMap(listHuntScoreTargetMachineNameCandidates)
        .map((machineName) => String(machineName ?? "").trim())
        .filter(Boolean),
    ),
  ];
}

export function buildHuntScoreSnapshots(
  targetRows,
  allStoreRows = [],
  storeName = "",
  logicKey = "",
  differenceMode = DEFAULT_DIFFERENCE_MODE,
) {
  const storeConfig = buildEffectiveHuntScoreStoreConfig(
    storeName,
    (Array.isArray(targetRows) ? targetRows : []).map((row) => row?.machine_name),
  );
  if (!storeConfig || !Array.isArray(targetRows) || targetRows.length === 0) {
    return [];
  }
  const config = buildRuntimeHuntScoreConfig(storeConfig, logicKey, differenceMode);

  const businessDates = buildBusinessDates(allStoreRows, targetRows);
  if (businessDates.length === 0) {
    return [];
  }

  const businessDateSet = new Set(businessDates);
  const { rowsByCandidateKey, rowsByDate } = buildSourceMaps(targetRows, businessDateSet, config);
  const settingDefinitionCache = new Map();

  return businessDates
    .map((_, dateIndex) =>
      buildSnapshotRowsForDate(
        businessDates,
        dateIndex,
        rowsByDate,
        rowsByCandidateKey,
        settingDefinitionCache,
        config,
      ),
    )
    .filter((snapshot) => snapshot.rows.length > 0)
    .sort((left, right) => right.baseDate.localeCompare(left.baseDate));
}

export function attachHuntScores(
  targetRows,
  allStoreRows = [],
  storeName = "",
  logicKey = "",
  differenceMode = DEFAULT_DIFFERENCE_MODE,
) {
  const storeConfig = resolveHuntScoreStoreConfig(storeName);
  if (!storeConfig) {
    return;
  }
  const config = buildRuntimeHuntScoreConfig(storeConfig, logicKey, differenceMode);

  const snapshots = buildHuntScoreSnapshots(
    targetRows,
    allStoreRows,
    storeName,
    logicKey,
    differenceMode,
  );
  const huntScoreByRowKey = new Map();

  for (const snapshot of snapshots) {
    for (const row of snapshot.rows) {
      huntScoreByRowKey.set(row.rowKey, row.huntScore);
    }
  }

  for (const row of targetRows) {
    const huntScore = huntScoreByRowKey.get(buildRowKey(row, config));
    if (Number.isFinite(huntScore)) {
      row.hunt_score = huntScore;
    }
  }
}
