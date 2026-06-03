import {
  calculateSettingEstimate,
  getSettingEstimateDefinition,
  normalizeSettingEstimateMode,
} from "./setting-estimates";
import {
  DEFAULT_DIFFERENCE_MODE,
  normalizeDifferenceMode,
  selectDifferenceValue,
} from "./machine-difference";

const HUNT_SCORE_EPSILON = 0.000000001;
const DEFAULT_HUNT_SCORE_WINDOW_DAYS = 7;
const NET_LOSS_SCORE_TARGET_BY_WINDOW_DAYS = {
  3: 15000,
  5: 17500,
  7: 20000,
};
const HIGH_GAMES_SCORE_TARGET_BY_WINDOW_DAYS = {
  7: 70000,
};
const LOW_GAMES_SCORE_TARGET_BY_WINDOW_DAYS = {
  7: 70000,
};
const TAMAYA_ZASSHONOKUMA_HISTORY_WINDOW_DAYS = 30;
const MILLION_TOBU_NERIMA_R30_WINDOW_DAYS = 30;
const AMUSE_ASAKUSA_R30_WINDOW_DAYS = 30;
const DEFAULT_HUNT_SCORE_LOGIC_KEY = "apark";
const APARK_KASUGA_KAI_RAW_MIN = -32;
const APARK_KASUGA_KAI_RAW_MAX = 138;
const APARK_KASUGA_SIX_DAY_SCALE = 6 / 7;

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
  {
    name: "スマスロ バイオハザードRE:3",
    aliases: [
      "スマスロ バイオハザード RE:3",
      "スマスロバイオハザードRE:3",
      "スマスロバイオハザード RE:3",
      "スマスロ バイオハザードＲＥ：３",
      "スマスロバイオハザードＲＥ：３",
    ],
  },
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
  { name: "スマスロ サンダーV", aliases: ["LサンダーV", "スマスロサンダーV"] },
  {
    name: "A-SLOT+異世界かるてっとBT",
    aliases: ["A-SLOT+ 異世界かるてっとBT", "A-SLOT+異世界かるてっとＢＴ"],
  },
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

const BEAM_HIKARI_TARGET_MACHINES = APARK_KASUGA_TARGET_MACHINES;
const MJ_ARENA_IJIRI_TARGET_MACHINES = APARK_KASUGA_TARGET_MACHINES;

const MJ_ARENA_AIRPORT_TARGET_MACHINES = [
  { name: "ゴーゴージャグラー３", aliases: ["ゴーゴージャグラー3", "ゴーゴージャグラー"] },
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
  {
    name: "ファンキージャグラー２ＫＴ",
    aliases: ["ファンキージャグラー２", "ファンキージャグラー2", "ファンキージャグラー"],
  },
  { name: "マイジャグラーV", aliases: ["マイジャグラーⅤ", "マイジャグラー"] },
];

const SLOT_MARUMITSU_OHASHI_TARGET_MACHINES = [
  { name: "ゴーゴージャグラー３", aliases: ["ゴーゴージャグラー3", "ゴーゴージャグラー"] },
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
  {
    name: "ファンキージャグラー２ＫＴ",
    aliases: ["ファンキージャグラー２", "ファンキージャグラー2", "ファンキージャグラー"],
  },
  { name: "マイジャグラーV", aliases: ["マイジャグラーⅤ", "マイジャグラー"] },
  { name: "ミスタージャグラー", aliases: [] },
];

const WONDERLAND_MINAMIGAOKA_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
  { name: "マイジャグラーV", aliases: ["マイジャグラーⅤ", "マイジャグラー"] },
];

const WONDERLAND_SUE_TARGET_MACHINES = [
  { name: "ゴーゴージャグラー３", aliases: ["ゴーゴージャグラー3", "ゴーゴージャグラー"] },
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
  {
    name: "ハッピージャグラーＶＩＩＩ",
    aliases: ["ハッピージャグラーVIII", "ハッピージャグラーＶ", "ハッピージャグラーV", "ハッピージャグラー"],
  },
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

const MILLION_TOBU_NERIMA_TARGET_MACHINES = [
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
];

const AMUSE_ASAKUSA_TARGET_MACHINES = [
  { name: "マイジャグラーV", aliases: ["マイジャグラーⅤ", "マイジャグラー"] },
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
  { name: "ゴーゴージャグラー３", aliases: ["ゴーゴージャグラー3", "ゴーゴージャグラー"] },
  { name: "ジャグラーガールズSS", aliases: ["ジャグラーガールズ"] },
  { name: "ウルトラミラクルジャグラー", aliases: [] },
  { name: "ミスタージャグラー", aliases: [] },
];

const HUNT_SCORE_LOGIC_DEFINITIONS = [
  {
    key: "loss-streak-7",
    name: "7日連敗数",
    windowDays: 7,
    scoreCalculator: calculateLosingStreakOnlyHuntScore,
  },
  {
    key: "loss-days-7",
    name: "7日負け数",
    windowDays: 7,
    scoreCalculator: calculateLossDaysOnlyHuntScore,
  },
  {
    key: "loss-days-5",
    name: "5日負け数",
    windowDays: 5,
    scoreCalculator: calculateLossDaysOnlyHuntScore,
  },
  {
    key: "loss-days-3",
    name: "3日負け数",
    windowDays: 3,
    scoreCalculator: calculateLossDaysOnlyHuntScore,
  },
  {
    key: "net-loss-7",
    name: "7日差枚凹み",
    windowDays: 7,
    scoreCalculator: calculateNetLossOnlyHuntScore,
  },
  {
    key: "net-loss-5",
    name: "5日差枚凹み",
    windowDays: 5,
    scoreCalculator: calculateNetLossOnlyHuntScore,
  },
  {
    key: "net-loss-3",
    name: "3日差枚凹み",
    windowDays: 3,
    scoreCalculator: calculateNetLossOnlyHuntScore,
  },
  {
    key: "low-games-7",
    name: "7日高G数",
    windowDays: 7,
    scoreCalculator: calculateHighGamesOnlyHuntScore,
  },
  {
    key: "low-games-reverse-7",
    name: "7日低G数",
    windowDays: 7,
    scoreCalculator: calculateLowGamesOnlyHuntScore,
  },
  {
    key: "apark",
    name: "Aパーク春日式",
    windowDays: 7,
    scoreCalculator: calculateAparkKasugaHuntScore,
  },
  {
    key: "apark-kai",
    name: "Aパーク春日式v2.0",
    windowDays: 7,
    scoreCalculator: calculateAparkKasugaKaiHuntScore,
  },
  {
    key: "apark-kai-6",
    name: "Aパーク春日式v2.0 6日版",
    windowDays: 6,
    scoreCalculator: calculateAparkKasugaKaiSixDayHuntScore,
  },
  {
    key: "million-tobu-nerima",
    name: "ミリオン東武練馬式",
    windowDays: 7,
    historyWindowDays: MILLION_TOBU_NERIMA_R30_WINDOW_DAYS,
    scoreCalculator: calculateMillionTobuNerimaHuntScore,
  },
  {
    key: "amuse-asakusa",
    name: "アミューズ浅草式",
    windowDays: 7,
    historyWindowDays: AMUSE_ASAKUSA_R30_WINDOW_DAYS,
    scoreCalculator: calculateAmuseAsakusaHuntScore,
  },
  {
    key: "apark-yakatabaru-a",
    name: "Aパーク屋形原式A",
    windowDays: 7,
    historyWindowDays: 14,
    scoreCalculator: calculateAparkYakatabaruAHuntScore,
  },
  {
    key: "apark-yakatabaru-b",
    name: "Aパーク屋形原式B",
    windowDays: 7,
    historyWindowDays: 14,
    scoreCalculator: calculateAparkYakatabaruBHuntScore,
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
    key: "123-hakata-a",
    name: "123博多式A",
    windowDays: 7,
    historyWindowDays: 60,
    scoreCalculator: calculate123HakataAHuntScore,
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
    historyWindowDays: 90,
    scoreCalculator: calculateBeamHikariAHuntScore,
  },
  {
    key: "beam-hikari-b",
    name: "ビームヒカリ式B",
    windowDays: 7,
    historyWindowDays: 90,
    scoreCalculator: calculateBeamHikariBHuntScore,
  },
  {
    key: "mj-arena-ijiri-a",
    name: "MJアリーナ井尻式A",
    windowDays: 7,
    historyWindowDays: 90,
    scoreCalculator: calculateMjArenaIjiriAHuntScore,
  },
  {
    key: "mj-arena-ijiri-b",
    name: "MJアリーナ井尻式B",
    windowDays: 7,
    historyWindowDays: 90,
    scoreCalculator: calculateMjArenaIjiriBHuntScore,
  },
  {
    key: "mj-arena-airport-a",
    name: "MJアリーナ空港式A",
    windowDays: 7,
    historyWindowDays: 90,
    scoreCalculator: calculateMjArenaAirportAHuntScore,
  },
  {
    key: "mj-arena-airport-b",
    name: "MJアリーナ空港式B",
    windowDays: 7,
    historyWindowDays: 90,
    scoreCalculator: calculateMjArenaAirportBHuntScore,
  },
  {
    key: "slot-marumitsu-ohashi-a",
    name: "スロットまるみつ大橋式A",
    windowDays: 7,
    historyWindowDays: 90,
    scoreCalculator: calculateSlotMarumitsuOhashiAHuntScore,
  },
  {
    key: "slot-marumitsu-ohashi-b",
    name: "スロットまるみつ大橋式B",
    windowDays: 7,
    historyWindowDays: 90,
    scoreCalculator: calculateSlotMarumitsuOhashiBHuntScore,
  },
  {
    key: "wonderland-minamigaoka-a",
    name: "ワンダーランド南ヶ丘式A",
    windowDays: 7,
    historyWindowDays: 90,
    scoreCalculator: calculateWonderlandMinamigaokaAHuntScore,
  },
  {
    key: "wonderland-minamigaoka-b",
    name: "ワンダーランド南ヶ丘式B",
    windowDays: 7,
    historyWindowDays: 90,
    scoreCalculator: calculateWonderlandMinamigaokaBHuntScore,
  },
  {
    key: "wonderland-sue-a",
    name: "ワンダーランド須恵式A",
    windowDays: 7,
    historyWindowDays: 90,
    scoreCalculator: calculateWonderlandSueAHuntScore,
  },
  {
    key: "wonderland-sue-b",
    name: "ワンダーランド須恵式B",
    windowDays: 7,
    historyWindowDays: 90,
    scoreCalculator: calculateWonderlandSueBHuntScore,
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
    key: "million-tobu-nerima",
    storeNames: [
      "ミリオン東武練馬店スロット館",
      "ミリオン東武練馬スロット館",
      "ミリオン東武練馬店",
      "ミリオン東武練馬",
    ],
    targetMachines: MILLION_TOBU_NERIMA_TARGET_MACHINES,
    defaultLogicKey: "million-tobu-nerima",
  },
  {
    key: "amuse-asakusa",
    storeNames: [
      "アミューズ浅草店",
      "アミューズ浅草",
      "AMUSE浅草店",
      "AMUSE浅草",
      "ＡＭＵＳＥ浅草店",
      "ＡＭＵＳＥ浅草",
    ],
    targetMachines: AMUSE_ASAKUSA_TARGET_MACHINES,
    defaultLogicKey: "amuse-asakusa",
  },
  {
    key: "apark-yakatabaru",
    storeNames: ["A-PARK屋形原", "A-PARK屋形原店", "Aパーク屋形原", "Aパーク屋形原店"],
    targetMachines: APARK_YAKATABARU_TARGET_MACHINES,
    defaultLogicKey: "apark-yakatabaru-a",
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
    key: "mj-arena-ijiri",
    storeNames: ["MJアリーナ井尻店", "MJアリーナ井尻", "ＭＪアリーナ井尻店", "ＭＪアリーナ井尻"],
    targetMachines: MJ_ARENA_IJIRI_TARGET_MACHINES,
    defaultLogicKey: "mj-arena-ijiri-a",
  },
  {
    key: "mj-arena-airport",
    storeNames: ["MJアリーナ空港店", "MJアリーナ空港", "ＭＪアリーナ空港店", "ＭＪアリーナ空港"],
    targetMachines: MJ_ARENA_AIRPORT_TARGET_MACHINES,
    defaultLogicKey: "mj-arena-airport-a",
  },
  {
    key: "slot-marumitsu-ohashi",
    storeNames: ["スロットまるみつ大橋店", "スロットまるみつ大橋", "まるみつ大橋店", "まるみつ大橋"],
    targetMachines: SLOT_MARUMITSU_OHASHI_TARGET_MACHINES,
    defaultLogicKey: "slot-marumitsu-ohashi-a",
  },
  {
    key: "wonderland-minamigaoka",
    storeNames: [
      "ワンダーランド南ヶ丘店",
      "ワンダーランド南ヶ丘",
      "ワンダーランド南が丘店",
      "ワンダーランド南が丘",
      "ワンダーランド南ケ丘店",
      "ワンダーランド南ケ丘",
    ],
    targetMachines: WONDERLAND_MINAMIGAOKA_TARGET_MACHINES,
    defaultLogicKey: "wonderland-minamigaoka-b",
  },
  {
    key: "wonderland-sue",
    storeNames: ["ワンダーランド須恵店", "ワンダーランド須恵"],
    targetMachines: WONDERLAND_SUE_TARGET_MACHINES,
    defaultLogicKey: "wonderland-sue-a",
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
    windowDays: definition.windowDays ?? DEFAULT_HUNT_SCORE_WINDOW_DAYS,
    historyWindowDays:
      definition.historyWindowDays ?? definition.windowDays ?? DEFAULT_HUNT_SCORE_WINDOW_DAYS,
  };
}

export function getHuntScoreLogicDetails(logicKeys = [], storeName = "") {
  const normalizedKeys = [
    ...new Set(
      (Array.isArray(logicKeys) ? logicKeys : [logicKeys])
        .map((logicKey) => String(logicKey ?? "").trim())
        .filter(Boolean)
        .map((logicKey) => normalizeHuntScoreLogicKey(logicKey, storeName)),
    ),
  ];

  const fallbackKey = getDefaultHuntScoreLogicKey(storeName);
  const keys = normalizedKeys.length > 0 ? normalizedKeys : [fallbackKey];
  return keys.map((logicKey) => getHuntScoreLogicDetail(logicKey, storeName));
}

function buildRuntimeHuntScoreConfig(
  config,
  logicKey = "",
  differenceMode = DEFAULT_DIFFERENCE_MODE,
  settingEstimateMode = undefined,
) {
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
    settingEstimateMode: normalizeSettingEstimateMode(settingEstimateMode),
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
  const knownTargetMachines = listTargetMachinesFromConfigs(listKnownHuntScoreStoreConfigs(storeName));
  const knownAvailableTargetMachines = knownTargetMachines.filter(
    (targetMachine) =>
      listHuntScoreTargetMachineNameCandidates(targetMachine).some((candidateName) =>
        availableMachineNameSet.has(normalizeText(candidateName)),
      ),
  );
  const knownTargetCandidateSet = new Set(
    knownAvailableTargetMachines
      .flatMap(listHuntScoreTargetMachineNameCandidates)
      .map(normalizeText)
      .filter(Boolean),
  );
  const dynamicTargetMachines = availableMachineNames
    .filter((machineName) => !knownTargetCandidateSet.has(normalizeText(machineName)))
    .map((machineName) => ({
      name: machineName,
      aliases: [],
    }));
  const targetMachines = listTargetMachinesFromConfigs([
    { targetMachines: [...knownAvailableTargetMachines, ...dynamicTargetMachines] },
  ]);

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
  const estimate = definition
    ? calculateSettingEstimate(definition, row, { mode: config?.settingEstimateMode })
    : null;
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

function calculateCurrentHighSettingEstimateStreak(windowRows) {
  let streak = 0;

  for (let index = windowRows.length - 1; index >= 0; index -= 1) {
    const settingAverage = windowRows[index].settingAverage;
    if (!Number.isFinite(settingAverage) || settingAverage < 4.5) {
      break;
    }
    streak += 1;
  }

  return streak;
}

function calculateCurrentHighSettingCandidateStreak(windowRows) {
  let streak = 0;

  for (let index = windowRows.length - 1; index >= 0; index -= 1) {
    const windowRow = windowRows[index];
    const settingAverage = windowRow?.settingAverage;
    if (!Number.isFinite(settingAverage) || settingAverage < 4.5 || windowRow.rbCount < 25) {
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

function readDateWeekday(dateText) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/u.exec(String(dateText ?? "").trim());
  const date = match
    ? new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]))
    : new Date(String(dateText ?? ""));
  const weekday = date.getDay();
  return Number.isFinite(weekday) ? weekday : null;
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

function calculateCountOnlyHuntScore(count, context = {}) {
  const windowDays = Math.max(1, Number(context.windowDays) || DEFAULT_HUNT_SCORE_WINDOW_DAYS);
  const normalizedCount = Math.max(0, Number(count) || 0);
  return (normalizedCount / windowDays) * 100;
}

function calculateLosingStreakOnlyHuntScore(metrics, context = {}) {
  return calculateCountOnlyHuntScore(metrics.streak, context);
}

function calculateLossDaysOnlyHuntScore(metrics, context = {}) {
  return calculateCountOnlyHuntScore(metrics.lossDays, context);
}

function calculateNetLossOnlyHuntScore(metrics, context = {}) {
  const windowDays = Math.max(1, Number(context.windowDays) || DEFAULT_HUNT_SCORE_WINDOW_DAYS);
  const targetLoss = NET_LOSS_SCORE_TARGET_BY_WINDOW_DAYS[windowDays] || NET_LOSS_SCORE_TARGET_BY_WINDOW_DAYS[7];
  const netTotal = Number(metrics.netTotal);
  if (!Number.isFinite(netTotal)) {
    return 0;
  }
  return clamp((-netTotal / targetLoss) * 100, 0, 100);
}

function calculateHighGamesOnlyHuntScore(metrics, context = {}) {
  const windowDays = Math.max(1, Number(context.windowDays) || DEFAULT_HUNT_SCORE_WINDOW_DAYS);
  const targetGames =
    HIGH_GAMES_SCORE_TARGET_BY_WINDOW_DAYS[windowDays] || HIGH_GAMES_SCORE_TARGET_BY_WINDOW_DAYS[7];
  const gamesTotal = Number(metrics.gamesTotal);
  if (!Number.isFinite(gamesTotal)) {
    return 0;
  }
  return clamp((gamesTotal / targetGames) * 100, 0, 100);
}

function calculateLowGamesOnlyHuntScore(metrics, context = {}) {
  const windowDays = Math.max(1, Number(context.windowDays) || DEFAULT_HUNT_SCORE_WINDOW_DAYS);
  const targetGames =
    LOW_GAMES_SCORE_TARGET_BY_WINDOW_DAYS[windowDays] || LOW_GAMES_SCORE_TARGET_BY_WINDOW_DAYS[7];
  const gamesTotal = Number(metrics.gamesTotal);
  if (!Number.isFinite(gamesTotal)) {
    return 0;
  }
  return clamp(((targetGames - gamesTotal) / targetGames) * 100, 0, 100);
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

function scaleAparkKasugaSixDayThreshold(value) {
  return Math.round(value * APARK_KASUGA_SIX_DAY_SCALE);
}

function calculateLossDaysSixDayScore(value) {
  return scoreFromMinimums(value, [
    { minimum: 6, score: 25 },
    { minimum: 5, score: 21 },
    { minimum: 4, score: 16 },
    { minimum: 3, score: 10 },
    { minimum: 2, score: 5 },
  ]);
}

function calculateStreakSixDayScore(value) {
  return scoreFromMinimums(value, [
    { minimum: 6, score: 18 },
    { minimum: 5, score: 16 },
    { minimum: 4, score: 14 },
    { minimum: 3, score: 11 },
    { minimum: 2, score: 4 },
  ]);
}

function calculateLossAbsSixDayScore(value) {
  return scoreFromMinimums(value, [
    { minimum: scaleAparkKasugaSixDayThreshold(6000), score: 18 },
    { minimum: scaleAparkKasugaSixDayThreshold(5000), score: 15 },
    { minimum: scaleAparkKasugaSixDayThreshold(4000), score: 12 },
    { minimum: scaleAparkKasugaSixDayThreshold(3000), score: 8 },
    { minimum: scaleAparkKasugaSixDayThreshold(2000), score: 4 },
  ]);
}

function calculateNetTotalSixDayScore(value) {
  return scoreFromMaximums(value, [
    { maximum: scaleAparkKasugaSixDayThreshold(-5000), score: 14 },
    { maximum: scaleAparkKasugaSixDayThreshold(-4000), score: 12 },
    { maximum: scaleAparkKasugaSixDayThreshold(-3000), score: 9 },
    { maximum: scaleAparkKasugaSixDayThreshold(-2000), score: 6 },
    { maximum: scaleAparkKasugaSixDayThreshold(-1000), score: 3 },
  ]);
}

function calculateAparkKasugaSixDayHuntScore(metrics) {
  const totalScore =
    calculateLossDaysSixDayScore(metrics.lossDays) +
    calculateStreakSixDayScore(metrics.streak) +
    calculateLossAbsSixDayScore(metrics.lossAbsTotal) +
    calculateNetTotalSixDayScore(metrics.netTotal) +
    calculateCompensationRateScore(metrics.compensationRate) +
    calculateMaxWinScore(metrics.maxWin) +
    calculateTodayDifferenceScore(metrics.todayDifference) +
    calculateTodaySettingScore(metrics.todaySetting);

  return clamp(totalScore, 0, 100);
}

function readMachineActiveSlotCount(metrics, context = {}) {
  const machineName = String(metrics?.machineName ?? "").trim();
  if (!machineName) {
    return 0;
  }

  if (context.machineActiveSlotCountByName instanceof Map) {
    const count = context.machineActiveSlotCountByName.get(machineName);
    if (Number.isFinite(count)) {
      return count;
    }
  }

  return (Array.isArray(context.metricsList) ? context.metricsList : []).filter(
    (candidateMetrics) => String(candidateMetrics?.machineName ?? "").trim() === machineName,
  ).length;
}

function readMachineHighSettingCandidateRate30(metrics, context = {}) {
  const machineName = String(metrics?.machineName ?? "").trim();
  if (!machineName || !(context.machineHighSettingCandidateRateByName instanceof Map)) {
    return null;
  }

  const rate = context.machineHighSettingCandidateRateByName.get(machineName);
  return Number.isFinite(rate) ? rate : null;
}

function calculateRbSettingEquivalentFromRates(settingRates, games, rbCount) {
  const normalizedGames = Number(games);
  const normalizedRbCount = Number(rbCount);
  if (!Number.isFinite(normalizedGames) || normalizedGames <= 0 || !Number.isFinite(normalizedRbCount)) {
    return null;
  }

  const rates = (Array.isArray(settingRates) ? settingRates : [])
    .map((row) => ({
      setting: Number(row?.setting),
      rb: Number(row?.rb),
    }))
    .filter((row) => Number.isFinite(row.setting) && Number.isFinite(row.rb) && row.rb > 0)
    .sort((left, right) => left.setting - right.setting);

  if (rates.length === 0) {
    return null;
  }

  const probability = normalizedRbCount > 0 ? normalizedRbCount / normalizedGames : 0;
  if (probability <= rates[0].rb) {
    return rates[0].setting;
  }

  const lastRate = rates.at(-1);
  if (probability >= lastRate.rb) {
    return lastRate.setting;
  }

  for (let index = 1; index < rates.length; index += 1) {
    const lower = rates[index - 1];
    const upper = rates[index];
    if (probability > upper.rb) {
      continue;
    }

    if (Math.abs(upper.rb - lower.rb) <= Number.EPSILON) {
      return upper.setting;
    }

    const ratio = (probability - lower.rb) / (upper.rb - lower.rb);
    return lower.setting + ratio * (upper.setting - lower.setting);
  }

  return lastRate.setting;
}

function calculateRbSettingEquivalentForTotals(machineName, games, rbCount, settingDefinitionCache) {
  const definition = getSettingDefinition(settingDefinitionCache, machineName);
  return calculateRbSettingEquivalentFromRates(definition?.settingRates, games, rbCount);
}

function calculateRbSettingEquivalentForRow(row, settingDefinitionCache, config) {
  const machineName = normalizeHuntScoreMachineName(row?.machine_name, config);
  return calculateRbSettingEquivalentForTotals(
    machineName,
    readNumber(row?.games_count) ?? 0,
    readNumber(row?.rb_count) ?? 0,
    settingDefinitionCache,
  );
}

function isStandardHighSettingCandidateRow(row, settingDefinitionCache, config) {
  if (!row) {
    return false;
  }

  const settingAverage = getSettingEstimateAverage(settingDefinitionCache, row, config).average;
  const rbCount = readNumber(row?.rb_count) ?? 0;
  return Number.isFinite(settingAverage) && settingAverage >= 4.5 && rbCount >= 25;
}

function isAmuseAsakusaNormalizedHighSettingRow(row, settingDefinitionCache, config) {
  if (!row) {
    return false;
  }

  const games = readNumber(row?.games_count) ?? 0;
  if (games < 4500) {
    return false;
  }

  const settingAverage = getSettingEstimateAverage(settingDefinitionCache, row, config).average;
  if (!Number.isFinite(settingAverage) || settingAverage < 4.5) {
    return false;
  }

  const rbSettingEquivalent = calculateRbSettingEquivalentForRow(row, settingDefinitionCache, config);
  return Number.isFinite(rbSettingEquivalent) && rbSettingEquivalent >= 4;
}

function normalizeDaysSinceHighSettingEstimateOffset(offset) {
  return Number.isFinite(offset) ? Math.max(0, offset - 1) : 99;
}

function calculateAparkKasugaKaiHuntScore(metrics, context = {}) {
  let rawScore = calculateAparkKasugaHuntScore(metrics);

  if (
    metrics.lossDays >= 5 &&
    metrics.lossDays <= 6 &&
    metrics.streak >= 3 &&
    metrics.netTotal <= -3000
  ) {
    rawScore += 18;
  }
  if (metrics.streak >= 4) {
    rawScore += 8;
  }
  if (metrics.netTotal <= -5000) {
    rawScore += 8;
  }
  if (metrics.compensationRate <= 0.35) {
    rawScore += 4;
  }
  if (metrics.lossDays <= 4) {
    rawScore -= 10;
  }
  if (metrics.streak <= 1) {
    rawScore -= 6;
  }
  if (metrics.netTotal > -2000) {
    rawScore -= 6;
  }
  const activeSlotCount = readMachineActiveSlotCount(metrics, context);
  if (activeSlotCount > 0 && activeSlotCount <= 4) {
    rawScore -= 10;
  }

  return ((rawScore - APARK_KASUGA_KAI_RAW_MIN) /
    (APARK_KASUGA_KAI_RAW_MAX - APARK_KASUGA_KAI_RAW_MIN)) * 100;
}

function calculateAparkKasugaKaiSixDayHuntScore(metrics, context = {}) {
  let rawScore = calculateAparkKasugaSixDayHuntScore(metrics);

  if (
    metrics.lossDays >= 4 &&
    metrics.lossDays <= 5 &&
    metrics.streak >= 3 &&
    metrics.netTotal <= scaleAparkKasugaSixDayThreshold(-3000)
  ) {
    rawScore += 18;
  }
  if (metrics.streak >= 4) {
    rawScore += 8;
  }
  if (metrics.netTotal <= scaleAparkKasugaSixDayThreshold(-5000)) {
    rawScore += 8;
  }
  if (metrics.compensationRate <= 0.35) {
    rawScore += 4;
  }
  if (metrics.lossDays <= 3) {
    rawScore -= 10;
  }
  if (metrics.streak <= 1) {
    rawScore -= 6;
  }
  if (metrics.netTotal > scaleAparkKasugaSixDayThreshold(-2000)) {
    rawScore -= 6;
  }
  const activeSlotCount = readMachineActiveSlotCount(metrics, context);
  if (activeSlotCount > 0 && activeSlotCount <= 4) {
    rawScore -= 10;
  }

  return ((rawScore - APARK_KASUGA_KAI_RAW_MIN) /
    (APARK_KASUGA_KAI_RAW_MAX - APARK_KASUGA_KAI_RAW_MIN)) * 100;
}

function calculateMillionTobuNerimaHuntScore(metrics, context = {}) {
  let rawScore =
    calculateLossDaysScore(metrics.lossDays) +
    calculateStreakScore(metrics.streak) +
    calculateLossAbsScore(metrics.lossAbsTotal) +
    calculateNetTotalScore(metrics.netTotal) +
    calculateCompensationRateScore(metrics.compensationRate) +
    calculateMaxWinScore(metrics.maxWin) +
    calculateTodayDifferenceScore(metrics.todayDifference) +
    calculateTodaySettingScore(metrics.todaySetting);

  const machineCount = readMachineActiveSlotCount(metrics, context);
  const machineHighSettingCandidateRate30 = readMachineHighSettingCandidateRate30(metrics, context);
  const rbDenominator = metrics.rbTotal > 0 && metrics.gamesTotal > 0 ? metrics.gamesTotal / metrics.rbTotal : 9999;
  const daysSinceH45 = normalizeDaysSinceHighSettingEstimateOffset(metrics.daysSinceHistoryHighSettingEstimate);
  const todayRb = readNumber(metrics.previousRbCount) ?? 0;

  if (
    metrics.lossDays >= 5 &&
    metrics.lossDays <= 6 &&
    metrics.streak >= 3 &&
    metrics.netTotal <= -3000
  ) {
    rawScore += 18;
  }
  if (metrics.streak >= 4) {
    rawScore += 8;
  }
  if (metrics.netTotal <= -5000) {
    rawScore += 8;
  }
  if (metrics.compensationRate <= 0.35) {
    rawScore += 4;
  }

  if (metrics.lossDays <= 4) {
    rawScore -= 10;
  }
  if (metrics.streak <= 1) {
    rawScore -= 6;
  }
  if (metrics.netTotal > -2000) {
    rawScore -= 6;
  }
  if (machineCount >= 1 && machineCount <= 4) {
    rawScore -= 10;
  }
  if (daysSinceH45 === 1) {
    rawScore -= 18;
  }
  if (rbDenominator >= 420) {
    rawScore -= 12;
  }
  if (todayRb >= 25 && metrics.todayDifference > 1500) {
    rawScore -= 8;
  }

  const normalizedScore = Math.round(clamp(((rawScore + 32) / 170) * 100, 0, 100));
  if (Number.isFinite(machineHighSettingCandidateRate30) && machineHighSettingCandidateRate30 < 0.1) {
    return 0;
  }

  return normalizedScore;
}

function calculateAmuseAsakusaLargeMachineScore(metrics) {
  let rawScore = 0;
  const daysSinceHNorm = Number.isFinite(metrics.amuseAsakusaDaysSinceHNorm)
    ? metrics.amuseAsakusaDaysSinceHNorm
    : 99;
  const hNormCount30 = Number.isFinite(metrics.amuseAsakusaHNormCount30)
    ? metrics.amuseAsakusaHNormCount30
    : 0;

  rawScore += scoreFromMaximums(metrics.gamesTotal, [
    { maximum: 25000, score: 40 },
    { maximum: 30000, score: 32 },
    { maximum: 35000, score: 24 },
    { maximum: 40000, score: 12 },
  ]);
  if (metrics.gamesTotal > 48000) {
    rawScore -= 12;
  }

  rawScore += scoreFromMaximums(metrics.recentThreeGamesTotal, [
    { maximum: 9000, score: 7 },
    { maximum: 12000, score: 4 },
  ]);
  if (metrics.recentThreeGamesTotal > 21000) {
    rawScore -= 6;
  }

  rawScore += scoreFromMinimums(metrics.todaySetting, [
    { minimum: 5, score: 12 },
    { minimum: 4.5, score: 8 },
    { minimum: 4, score: 4 },
  ]);

  if (hNormCount30 <= 1) {
    rawScore += 5;
  }
  if (daysSinceHNorm === 1) {
    rawScore -= 4;
  }

  return rawScore;
}

function calculateAmuseAsakusaMainMachineScore(metrics) {
  let rawScore = 0;

  rawScore += scoreFromMaximums(metrics.gamesTotal, [
    { maximum: 22000, score: 40 },
    { maximum: 26000, score: 32 },
    { maximum: 30000, score: 24 },
    { maximum: 35000, score: 16 },
    { maximum: 40000, score: 8 },
  ]);
  if (metrics.gamesTotal > 48000) {
    rawScore -= 8;
  }

  rawScore += scoreFromMaximums(metrics.recentThreeGamesTotal, [
    { maximum: 6000, score: 10 },
    { maximum: 9000, score: 7 },
  ]);
  if (metrics.recentThreeGamesTotal > 21000) {
    rawScore -= 5;
  }

  if (metrics.streak >= 2 && metrics.streak <= 3) {
    rawScore += 8;
  } else if (metrics.streak >= 4 && metrics.streak <= 5) {
    rawScore += 4;
  } else if (metrics.streak >= 6) {
    rawScore -= 5;
  }

  return rawScore;
}

function calculateAmuseAsakusaSpotMachineScore(metrics) {
  let rawScore = 0;
  const daysSinceHNorm = Number.isFinite(metrics.amuseAsakusaDaysSinceHNorm)
    ? metrics.amuseAsakusaDaysSinceHNorm
    : 99;
  const hNormCount30 = Number.isFinite(metrics.amuseAsakusaHNormCount30)
    ? metrics.amuseAsakusaHNormCount30
    : 0;

  rawScore += scoreFromMaximums(metrics.gamesTotal, [
    { maximum: 25000, score: 20 },
    { maximum: 30000, score: 15 },
    { maximum: 35000, score: 10 },
    { maximum: 40000, score: 5 },
  ]);
  if (metrics.gamesTotal > 48000) {
    rawScore -= 5;
  }

  rawScore += scoreFromMaximums(metrics.recentThreeGamesTotal, [
    { maximum: 6000, score: 8 },
    { maximum: 9000, score: 5 },
  ]);
  if (metrics.recentThreeGamesTotal > 21000) {
    rawScore -= 5;
  }

  if (daysSinceHNorm >= 2 && daysSinceHNorm <= 3) {
    rawScore += 10;
  } else if (daysSinceHNorm >= 4 && daysSinceHNorm <= 7) {
    rawScore += 5;
  } else if (daysSinceHNorm >= 0 && daysSinceHNorm <= 1) {
    rawScore -= 5;
  }

  if (metrics.streak >= 2 && metrics.streak <= 3) {
    rawScore += 8;
  } else if (metrics.streak >= 4 && metrics.streak <= 5) {
    rawScore += 4;
  }

  if (metrics.lossDays >= 5) {
    rawScore += 5;
  }
  if (metrics.netTotal >= -5000 && metrics.netTotal <= -1000) {
    rawScore += 5;
  }
  if (hNormCount30 >= 3) {
    rawScore += 3;
  }

  return rawScore;
}

function calculateAmuseAsakusaSmallMachineScore(metrics) {
  let rawScore = 0;
  const rbSettingEquivalent7 = Number.isFinite(metrics.amuseAsakusaRbSetting7)
    ? metrics.amuseAsakusaRbSetting7
    : null;
  const daysSinceHNorm = Number.isFinite(metrics.amuseAsakusaDaysSinceHNorm)
    ? metrics.amuseAsakusaDaysSinceHNorm
    : 99;
  const hNormCount30 = Number.isFinite(metrics.amuseAsakusaHNormCount30)
    ? metrics.amuseAsakusaHNormCount30
    : 0;

  rawScore += scoreFromMinimums(metrics.windowSettingAverage, [
    { minimum: 3.8, score: 20 },
    { minimum: 3.5, score: 14 },
    { minimum: 3.2, score: 8 },
  ]);

  rawScore += scoreFromMinimums(rbSettingEquivalent7, [
    { minimum: 4.5, score: 14 },
    { minimum: 3.8, score: 10 },
  ]);
  if (Number.isFinite(rbSettingEquivalent7) && rbSettingEquivalent7 <= 1.5) {
    rawScore -= 8;
  }

  rawScore += scoreFromMinimums(metrics.todaySetting, [
    { minimum: 4.5, score: 10 },
    { minimum: 4, score: 6 },
  ]);

  if (metrics.gamesTotal >= 36000) {
    rawScore += 8;
  }
  if (metrics.gamesTotal <= 25000) {
    rawScore -= 4;
  }

  if (daysSinceHNorm >= 2 && daysSinceHNorm <= 3) {
    rawScore += 10;
  } else if (daysSinceHNorm >= 0 && daysSinceHNorm <= 1) {
    rawScore -= 4;
  }

  if (hNormCount30 >= 2) {
    rawScore += 5;
  }

  return rawScore;
}

function applyAmuseAsakusaDensityGate(score, typeR30, machineCount) {
  if (Number.isFinite(machineCount) && machineCount >= 20) {
    return score;
  }
  if (!Number.isFinite(typeR30)) {
    return score;
  }
  if (typeR30 >= 0.5) {
    return score >= 64 ? score : 0;
  }
  if (typeR30 >= 0.25) {
    return score >= 80 ? score : 0;
  }
  if (typeR30 >= 0.1) {
    return score >= 96 ? score : 0;
  }
  return 0;
}

function calculateAmuseAsakusaHuntScore(metrics, context = {}) {
  let rawScore = 50 - metrics.netTotal / 200;

  if (metrics.todayDifference <= 0) {
    if (metrics.todaySetting >= 5) {
      rawScore += 7;
    } else if (metrics.todaySetting >= 4.5) {
      rawScore += 5;
    }
  }

  if (metrics.todaySetting >= 4.5 && metrics.todayDifference >= 1000) {
    rawScore -= 3;
  }

  if (metrics.adjacentHighSettingCandidateCount7 >= 3) {
    rawScore += 3;
  } else if (metrics.adjacentHighSettingCandidateCount7 === 0) {
    rawScore -= 3;
  }

  if (metrics.historyThirtyHighSettingCandidateCount <= 1) {
    rawScore -= 2;
  } else if (
    metrics.historyThirtyHighSettingCandidateCount >= 3 &&
    metrics.historyThirtyHighSettingCandidateCount <= 5
  ) {
    rawScore += 2;
  }

  if (metrics.previousGames > 7000) {
    rawScore -= 2;
  }

  if (metrics.windowRowCount < (context.windowDays ?? DEFAULT_HUNT_SCORE_WINDOW_DAYS)) {
    rawScore -= 20;
  }

  return rawScore;
}

function isAparkYakatabaruTargetMachine(machineName) {
  const normalizedMachineName = normalizeText(machineName);
  return APARK_YAKATABARU_TARGET_MACHINES.some(
    (targetMachine) =>
      normalizeText(targetMachine.name) === normalizedMachineName ||
      (targetMachine.aliases ?? []).some((alias) => normalizeText(alias) === normalizedMachineName),
  );
}

function isAparkYakatabaruReferenceEventDate(context = {}) {
  const targetDate = context.nextBusinessDate ?? context.baseDate;
  const day = readDateDay(targetDate);
  const weekday = readDateWeekday(targetDate);
  return (Number.isFinite(day) && day % 10 === 0) || weekday === 0 || weekday === 6;
}

function calculateAparkYakatabaruTailScore(metrics, mode = "a") {
  const tail = readSlotLastDigit(metrics.slotNumber);
  if (!Number.isFinite(tail)) {
    return 0;
  }

  if (mode === "b") {
    if ([3, 5].includes(tail)) {
      return 3;
    }
    if ([0, 2].includes(tail)) {
      return 1;
    }
    if (tail === 6) {
      return -2;
    }
    return 0;
  }

  if (tail === 5) {
    return 2;
  }
  if (tail === 3) {
    return 1;
  }
  return 0;
}

function calculateAparkYakatabaruBaseScoreA(metrics) {
  const d1 = metrics.todayDifference;
  const d2 = metrics.recentTwoNetTotal;
  const d3 = metrics.recentThreeNetTotal;
  const d5 = metrics.recentFiveNetTotal;
  const r2 = metrics.recentTwoRbTotal / 2;
  const g3 = metrics.recentThreeGamesTotal / 3;

  let score = 50;
  score += clamp(-d2 / 120, 0, 25);
  score += clamp(-d3 / 250, 0, 16);
  score += clamp(-d5 / 650, 0, 10);
  score += clamp(-d1 / 160, 0, 8);

  score -= clamp(d1 / 120, 0, 20);
  score -= clamp((d2 - 1000) / 160, 0, 18);
  score -= clamp((d3 - 2000) / 300, 0, 12);

  score += clamp((13 - r2) * 2, 0, 12);
  score -= clamp((r2 - 20) * 2, 0, 12);

  score += clamp((4800 - g3) / 350, 0, 9);
  score -= clamp((g3 - 6200) / 300, 0, 8);

  return score;
}

function calculateAparkYakatabaruRotationScoreA(metrics) {
  if (!hasBeamHikariSettingMetrics(metrics)) {
    return 0;
  }

  let score = 0;
  const daysSince = metrics.daysSinceHistoryStrongHighSettingCandidate;
  if (Number.isFinite(daysSince)) {
    if (daysSince >= 1 && daysSince <= 2) {
      score -= 12;
    } else if (daysSince >= 6 && daysSince <= 12) {
      score += 7;
    } else if (daysSince >= 13 && daysSince <= 28) {
      score += 3;
    }
  }

  if (metrics.strongHighSettingCandidateCount === 0) {
    score += 3;
  } else if (metrics.strongHighSettingCandidateCount >= 2) {
    score -= 6;
  }

  const previousStrongCandidate =
    metrics.todaySetting >= 5 || (metrics.todaySetting >= 4.5 && metrics.previousRbCount >= 25);
  if (metrics.todaySetting >= 5) {
    score -= 12;
  }
  if (previousStrongCandidate && metrics.todayDifference >= 1000) {
    score -= 14;
  }
  if (metrics.todaySetting >= 4.5 && metrics.todayDifference <= 0) {
    score += 10;
  }

  return score;
}

function calculateAparkYakatabaruAHuntScore(metrics, context = {}) {
  if (!isAparkYakatabaruTargetMachine(metrics.machineName)) {
    return 0;
  }

  const d2Adjustment = clamp(1 - Math.abs(metrics.recentTwoNetTotal + 2500) / 2500, 0, 1);
  const score =
    Math.min(
      calculateAparkYakatabaruBaseScoreA(metrics) +
        calculateAparkYakatabaruRotationScoreA(metrics) +
        (isAparkYakatabaruReferenceEventDate(context) ? 3 : 0) +
        calculateAparkYakatabaruTailScore(metrics, "a"),
      99,
    ) + d2Adjustment;

  return clamp(score, 0, 100);
}

function calculateAparkYakatabaruDifferencePointB(metrics) {
  let point = 0;
  const d1 = metrics.todayDifference;
  const d2 = metrics.recentTwoNetTotal;
  const d3 = metrics.recentThreeNetTotal;
  const d7 = metrics.netTotal;

  if (d2 <= -3000) {
    point += 28;
  } else if (d2 <= -2000) {
    point += 22;
  } else if (d2 <= -1000) {
    point += 15;
  } else if (d2 <= -500) {
    point += 8;
  } else if (d2 >= 3000) {
    point -= 22;
  } else if (d2 >= 2000) {
    point -= 18;
  } else if (d2 >= 1000) {
    point -= 12;
  } else if (d2 >= 0) {
    point -= 6;
  }

  if (d1 <= -2000) {
    point += 10;
  } else if (d1 <= -1000) {
    point += 7;
  } else if (d1 <= -500) {
    point += 4;
  } else if (d1 >= 3000) {
    point -= 14;
  } else if (d1 >= 2000) {
    point -= 10;
  } else if (d1 >= 1000) {
    point -= 7;
  } else if (d1 >= 0) {
    point -= 3;
  }

  if (d3 <= -3000) {
    point += 12;
  } else if (d3 <= -2000) {
    point += 9;
  } else if (d3 <= -1000) {
    point += 5;
  } else if (d3 >= 3000) {
    point -= 10;
  } else if (d3 >= 2000) {
    point -= 7;
  } else if (d3 >= 1000) {
    point -= 4;
  }

  point -= clamp(d7, 0, 6000) / 500;
  point += clamp(-d7, 0, 5000) / 500;

  if (metrics.streak >= 2) {
    point += 5;
  }
  if (metrics.streak >= 3) {
    point += 5;
  }
  if (metrics.winningStreak >= 2) {
    point -= 6;
  }

  return point;
}

function calculateAparkYakatabaruActivityPointB(metrics) {
  let point = 0;
  const recentTwoAverageGames = metrics.recentTwoGamesTotal / 2;

  if (metrics.recentTwoRbTotal <= 10) {
    point += 12;
  } else if (metrics.recentTwoRbTotal <= 15) {
    point += 9;
  } else if (metrics.recentTwoRbTotal <= 20) {
    point += 5;
  } else if (metrics.recentTwoRbTotal >= 55) {
    point -= 6;
  } else if (metrics.recentTwoRbTotal >= 45) {
    point -= 3;
  }

  if (metrics.recentThreeRbTotal <= 15) {
    point += 6;
  } else if (metrics.recentThreeRbTotal <= 20) {
    point += 4;
  }

  if (recentTwoAverageGames <= 2000) {
    point += 10;
  } else if (recentTwoAverageGames <= 3000) {
    point += 7;
  } else if (recentTwoAverageGames <= 4000) {
    point += 3;
  } else if (recentTwoAverageGames >= 8000) {
    point -= 5;
  } else if (recentTwoAverageGames >= 7000) {
    point -= 3;
  }

  if (metrics.averageGames < 4500) {
    point += 5;
  }
  if (metrics.averageGames >= 5500) {
    point -= 6;
  }

  return point;
}

function calculateAparkYakatabaruSettingPointB(metrics) {
  if (!hasBeamHikariSettingMetrics(metrics)) {
    return 0;
  }

  let point = 0;
  const recentTwoSettingAverage = metrics.recentTwoSettingAverage;
  if (Number.isFinite(recentTwoSettingAverage)) {
    if (recentTwoSettingAverage <= 2) {
      point += 10;
    } else if (recentTwoSettingAverage <= 2.5) {
      point += 6;
    } else if (recentTwoSettingAverage <= 3) {
      point += 2;
    } else if (recentTwoSettingAverage >= 4.5) {
      point -= 18;
    } else if (recentTwoSettingAverage >= 4) {
      point -= 12;
    } else if (recentTwoSettingAverage >= 3.5) {
      point -= 5;
    }
  }

  if (metrics.highSettingEstimateCount === 0) {
    point += 8;
  } else if (metrics.highSettingEstimateCount === 1) {
    point += 1;
  } else if (metrics.highSettingEstimateCount >= 2) {
    point -= 8;
  }
  if (metrics.settingFiveCount === 0) {
    point += 4;
  } else if (metrics.settingFiveCount >= 1) {
    point -= 4;
  }

  const daysSince = metrics.daysSinceHistoryHighSettingEstimate;
  if (Number.isFinite(daysSince)) {
    if (daysSince >= 1 && daysSince <= 2) {
      point -= 10;
    } else if (daysSince >= 8 && daysSince <= 14) {
      point += 7;
    } else if (daysSince >= 5 && daysSince <= 7) {
      point += 4;
    } else if (daysSince >= 15) {
      point += 4;
    }
  }

  if (metrics.todaySetting >= 4.5 && metrics.todayDifference < 0) {
    point += 8;
  }
  if (metrics.todaySetting >= 4.5 && metrics.todayDifference > 0) {
    point -= 16;
  }
  if (metrics.todaySetting >= 5 && metrics.todayDifference < 0) {
    point += 2;
  }
  if (metrics.todaySetting >= 5 && metrics.todayDifference > 0) {
    point -= 8;
  }
  if (metrics.highSettingEstimateStreak >= 2) {
    point -= 10;
  }

  return point;
}

function calculateAparkYakatabaruBHuntScore(metrics, context = {}) {
  if (!isAparkYakatabaruTargetMachine(metrics.machineName)) {
    return 0;
  }

  const point =
    calculateAparkYakatabaruDifferencePointB(metrics) +
    calculateAparkYakatabaruActivityPointB(metrics) +
    calculateAparkYakatabaruSettingPointB(metrics) +
    (isAparkYakatabaruReferenceEventDate(context) ? 4 : 0) +
    calculateAparkYakatabaruTailScore(metrics, "b");

  return clamp(50 + 0.65 * point, 0, 100);
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

function calculate123HakataAShortDipScore(metrics) {
  let score = 0;
  const recentThreeAverageGames = metrics.recentThreeGamesTotal / 3;

  if (metrics.recentTwoNetTotal <= -2000 && metrics.recentThreeRbTotal <= 25) {
    score += 18;
  }
  if (
    metrics.recentThreeNetTotal <= -2000 &&
    metrics.recentThreeRbTotal <= 25 &&
    recentThreeAverageGames <= 3000
  ) {
    score += 15;
  }
  if (metrics.recentFourteenNetTotal <= -4000 && metrics.recentThreeRbTotal <= 25) {
    score += 4;
  }

  return Math.min(score, 37);
}

function calculate123HakataALowActivityScore(metrics) {
  let score = 0;
  const recentThreeAverageGames = metrics.recentThreeGamesTotal / 3;
  const recentFiveAverageGames = metrics.recentFiveGamesTotal / 5;
  const recentSevenAverageGames = metrics.averageGames;

  if (recentThreeAverageGames <= 3000) {
    score += 14;
  } else if (recentThreeAverageGames <= 4000) {
    score += 8;
  } else if (recentThreeAverageGames >= 7000) {
    score -= 10;
  } else if (recentThreeAverageGames >= 6000) {
    score -= 6;
  }

  if (recentFiveAverageGames <= 3000) {
    score += 8;
  } else if (recentFiveAverageGames <= 4000) {
    score += 4;
  } else if (recentFiveAverageGames >= 7000) {
    score -= 6;
  } else if (recentFiveAverageGames >= 6000) {
    score -= 4;
  }

  if (recentSevenAverageGames <= 3000) {
    score += 4;
  } else if (recentSevenAverageGames <= 4000) {
    score += 2;
  } else if (recentSevenAverageGames >= 7000) {
    score -= 4;
  } else if (recentSevenAverageGames >= 6000) {
    score -= 2;
  }

  return Math.min(score, 26);
}

function calculate123HakataAShiftedTailScore(metrics, context = {}) {
  const targetDay = readDateDay(context.nextBusinessDate ?? context.baseDate);
  const slotTail = readSlotLastDigit(metrics.slotNumber);
  if (!Number.isFinite(targetDay) || !Number.isFinite(slotTail)) {
    return 0;
  }

  const shiftedTail = (slotTail - (targetDay % 10) + 10) % 10;
  if (shiftedTail === 6) {
    return 12;
  }
  if (shiftedTail === 5 || shiftedTail === 9) {
    return 8;
  }
  if (shiftedTail === 2 || shiftedTail === 4) {
    return 4;
  }
  if (shiftedTail === 8) {
    return -6;
  }
  return 0;
}

function calculate123HakataARecentInputScore(metrics) {
  let score = 0;
  const historyFortyFiveRate =
    metrics.historyFortyFiveSettingSampleCount > 0
      ? metrics.historyFortyFiveHighSettingCandidateCount / metrics.historyFortyFiveSettingSampleCount
      : null;

  if (metrics.historySixtyHighSettingCandidateCount >= 9) {
    score += 7;
  } else if (metrics.historySixtyHighSettingCandidateCount >= 8) {
    score += 5;
  } else if (metrics.historySixtyHighSettingCandidateCount >= 7) {
    score += 4;
  } else if (metrics.historySixtyHighSettingCandidateCount <= 1) {
    score -= 3;
  }

  if (metrics.historyFortyFiveHighSettingCandidateCount >= 7) {
    score += 4;
  } else if (metrics.historyFortyFiveHighSettingCandidateCount >= 6) {
    score += 3;
  }

  if (Number.isFinite(historyFortyFiveRate) && historyFortyFiveRate >= 0.12) {
    score += 3;
  } else if (Number.isFinite(historyFortyFiveRate) && historyFortyFiveRate >= 0.1) {
    score += 2;
  }

  if (metrics.historyTwentyOneHighSettingCandidateCount >= 4 || metrics.historyThirtyHighSettingCandidateCount >= 4) {
    score += 2;
  }

  return clamp(score, -3, 13);
}

function calculate123HakataAPreviousHighScore(metrics) {
  const previousHighCandidate = metrics.todaySetting >= 4.5 && metrics.previousRbCount >= 25;
  if (metrics.todaySetting >= 5 && metrics.todayDifference < 0) {
    return 6;
  }
  if (previousHighCandidate && metrics.todayDifference < 0) {
    return 5;
  }
  if (previousHighCandidate && metrics.todayDifference > 0) {
    return -5;
  }
  return 0;
}

function calculate123HakataAMidTermDipScore(metrics) {
  const value = metrics.recentFourteenNetTotal;
  if (value <= -6000) {
    return 6;
  }
  if (value <= -4000) {
    return 5;
  }
  if (value <= -2000) {
    return 3;
  }
  if (value >= 8000) {
    return -5;
  }
  if (value >= 5000) {
    return -3;
  }
  return 0;
}

function calculate123HakataAHuntScore(metrics, context = {}) {
  if (is123HakataTailMatched(metrics, context)) {
    return null;
  }

  const totalScore =
    calculate123HakataAShortDipScore(metrics) +
    calculate123HakataALowActivityScore(metrics) +
    calculate123HakataAShiftedTailScore(metrics, context) +
    calculate123HakataARecentInputScore(metrics) +
    calculate123HakataAPreviousHighScore(metrics) +
    calculate123HakataAMidTermDipScore(metrics);

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
  return Boolean(normalizeText(machineName));
}

function hasBeamHikariSettingMetrics(metrics) {
  return Number.isFinite(metrics.settingSampleCount) && metrics.settingSampleCount > 0;
}

function calculateBeamHikariHistoryScore(metrics) {
  if (metrics.historyRowCount < 14) {
    return 0;
  }

  if (metrics.historySettingSampleCount >= 14 && Number.isFinite(metrics.historyHighSettingEstimateRate)) {
    if (metrics.historyHighSettingEstimateRate >= 0.24) {
      return 12;
    }
    if (metrics.historyHighSettingEstimateRate >= 0.18) {
      return 8;
    }
    if (metrics.historyHighSettingEstimateRate >= 0.12) {
      return 4;
    }
    if (metrics.historyHighSettingEstimateRate <= 0.04) {
      return -6;
    }
    return 0;
  }

  const averageNet = metrics.historyNetTotal / metrics.historyRowCount;
  const positiveRate = metrics.historyPositiveDays / metrics.historyRowCount;
  if (averageNet >= 300 || positiveRate >= 0.6) {
    return 10;
  }
  if (averageNet >= 150 || positiveRate >= 0.54) {
    return 6;
  }
  if (averageNet >= 50 || positiveRate >= 0.5) {
    return 3;
  }
  if (averageNet <= -300 || positiveRate <= 0.35) {
    return -8;
  }
  if (averageNet <= -150 || positiveRate <= 0.42) {
    return -5;
  }
  return 0;
}

function calculateBeamHikariADipScore(metrics) {
  let score = 0;

  if (metrics.recentTwoNetTotal <= -3000) {
    score += 16;
  } else if (metrics.recentTwoNetTotal <= -2000) {
    score += 12;
  } else if (metrics.recentTwoNetTotal <= -1000) {
    score += 7;
  } else if (metrics.recentTwoNetTotal < 0) {
    score += 3;
  }

  if (metrics.recentThreeNetTotal <= -4000) {
    score += 8;
  } else if (metrics.recentThreeNetTotal <= -2500) {
    score += 6;
  } else if (metrics.recentThreeNetTotal <= -1500) {
    score += 3;
  }

  if (metrics.netTotal <= -7000) {
    score += 6;
  } else if (metrics.netTotal <= -5000) {
    score += 4;
  } else if (metrics.netTotal <= -3000) {
    score += 2;
  }

  if (metrics.todayDifference <= -2000) {
    score += 6;
  } else if (metrics.todayDifference <= -1000) {
    score += 4;
  } else if (metrics.todayDifference <= -500) {
    score += 2;
  }

  return Math.min(score, 32);
}

function calculateBeamHikariALowActivityScore(metrics) {
  let score = 0;

  if (metrics.recentTwoGamesTotal < 4000) {
    score += 12;
  } else if (metrics.recentTwoGamesTotal <= 6000) {
    score += 8;
  } else if (metrics.recentTwoGamesTotal <= 8000) {
    score += 4;
  }

  if (metrics.previousGames <= 3000) {
    score += 5;
  } else if (metrics.previousGames <= 5000) {
    score += 2;
  }

  if (metrics.recentThreeGamesTotal <= 9000) {
    score += 2;
  }

  return Math.min(score, 20);
}

function calculateBeamHikariASettingCycleScore(metrics) {
  if (!hasBeamHikariSettingMetrics(metrics)) {
    return 0;
  }

  let score = 0;
  if (!Number.isFinite(metrics.daysSinceHighSettingEstimate)) {
    score += 5;
  } else if (metrics.daysSinceHighSettingEstimate === 1) {
    score -= 18;
  } else if (metrics.daysSinceHighSettingEstimate === 2) {
    score -= 12;
  } else if (metrics.daysSinceHighSettingEstimate === 3) {
    score += 6;
  } else if (metrics.daysSinceHighSettingEstimate <= 7) {
    score += 5;
  }

  if (metrics.recentFiveHighSettingEstimateCount === 0) {
    score += 4;
  } else if (metrics.recentFiveHighSettingEstimateCount >= 2) {
    score -= 8;
  }

  return score;
}

function calculateBeamHikariAPenaltyScore(metrics) {
  let score = 0;

  if (hasBeamHikariSettingMetrics(metrics)) {
    if (metrics.todaySetting >= 5) {
      score += 20;
    } else if (metrics.todaySetting >= 4.5) {
      score += 16;
    }
    if (metrics.twoDaysAgoHighSettingEstimate) {
      score += 8;
    }
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

  return score;
}

function calculateBeamHikariAHuntScore(metrics) {
  if (!isBeamHikariTargetMachine(metrics.machineName)) {
    return 0;
  }

  const totalScore =
    30 +
    calculateBeamHikariHistoryScore(metrics) +
    calculateBeamHikariADipScore(metrics) +
    calculateBeamHikariALowActivityScore(metrics) +
    calculateBeamHikariASettingCycleScore(metrics) -
    calculateBeamHikariAPenaltyScore(metrics);

  return clamp(totalScore, 0, 100);
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

function calculateBeamHikariBHistoryPoint(metrics) {
  if (metrics.historyRowCount < 14) {
    return 0;
  }
  return calculateBeamHikariHistoryScore(metrics) * 0.35;
}

function calculateBeamHikariBSettingPoint(metrics) {
  if (!hasBeamHikariSettingMetrics(metrics)) {
    return 0;
  }

  const daysSinceHigh = metrics.daysSinceHighSettingEstimate;
  const daysSinceFive = metrics.daysSinceSettingFive;
  let point = 0;

  if (!Number.isFinite(daysSinceHigh)) {
    return 1.7;
  }
  if (daysSinceHigh >= 8) {
    point += 0.3;
  } else if (daysSinceHigh >= 4) {
    point += 1.5;
  } else if (daysSinceHigh === 3) {
    point += 2.2;
  } else if (daysSinceHigh >= 1) {
    point -= 7.3;
  }

  if (!Number.isFinite(daysSinceFive)) {
    point += 0.9;
  } else if (daysSinceFive >= 4 && daysSinceFive <= 7) {
    point += 1.6;
  } else if (daysSinceFive === 3) {
    point += 2;
  } else if (daysSinceFive === 2) {
    point -= 4.3;
  } else if (daysSinceFive === 1) {
    point -= 6.4;
  }

  if (metrics.recentFiveHighSettingEstimateCount === 0) {
    point += 0.5;
  } else if (metrics.recentFiveHighSettingEstimateCount === 1) {
    point -= 0.5;
  } else {
    point -= 4.1;
  }

  return point;
}

function calculateBeamHikariBHuntScore(metrics) {
  if (!isBeamHikariTargetMachine(metrics.machineName)) {
    return 0;
  }

  const score =
    50 +
    calculateBeamHikariBHistoryPoint(metrics) +
    calculateBeamHikariBDifferencePoint(metrics) +
    calculateBeamHikariBActivityPoint(metrics) +
    calculateBeamHikariBSettingPoint(metrics) -
    calculateBeamHikariAPenaltyScore(metrics) * 0.35;

  return clamp(score, 0, 100);
}

function isMjArenaIjiriTargetMachine(machineName) {
  return Boolean(normalizeText(machineName));
}

function readMjArenaIjiriTargetDate(context = {}) {
  return context.nextBusinessDate ?? context.baseDate;
}

function calculateMjArenaIjiriDateScore(context = {}) {
  const targetDate = readMjArenaIjiriTargetDate(context);
  const day = readDateDay(targetDate);
  const weekday = readDateWeekday(targetDate);
  let score = 0;

  if ([5, 15, 25].includes(day)) {
    score += 10;
  } else if ([1, 31].includes(day)) {
    score += 8;
  }

  if ([0, 2, 6].includes(weekday)) {
    score += 2;
  } else if ([4, 5].includes(weekday)) {
    score -= 2;
  }

  return clamp(score, -4, 12);
}

function calculateMjArenaIjiriTailScore(slotNumber) {
  const tail = readSlotLastDigit(slotNumber);
  if (tail === 1) {
    return 2;
  }
  if (tail === 9) {
    return 1.5;
  }
  if (tail === 0) {
    return 1;
  }
  if ([3, 7].includes(tail)) {
    return 0.5;
  }
  if (tail === 6) {
    return -1.5;
  }
  if (tail === 5) {
    return -0.5;
  }
  return 0;
}

function calculateMjArenaIjiriHistoryScore(metrics) {
  if (metrics.historyRowCount < 14) {
    return 0;
  }

  if (metrics.historySettingSampleCount >= 14 && Number.isFinite(metrics.historyHighSettingEstimateRate)) {
    if (metrics.historyHighSettingEstimateRate >= 0.2) {
      return 8;
    }
    if (metrics.historyHighSettingEstimateRate >= 0.14) {
      return 5;
    }
    if (metrics.historyHighSettingEstimateRate >= 0.1) {
      return 2.5;
    }
    if (metrics.historyHighSettingEstimateRate <= 0.03) {
      return -5;
    }
    return 0;
  }

  const averageNet = metrics.historyNetTotal / metrics.historyRowCount;
  const positiveRate = metrics.historyPositiveDays / metrics.historyRowCount;
  if (averageNet >= 250 || positiveRate >= 0.58) {
    return 6;
  }
  if (averageNet >= 100 || positiveRate >= 0.52) {
    return 3;
  }
  if (averageNet <= -250 || positiveRate <= 0.36) {
    return -5;
  }
  if (averageNet <= -100 || positiveRate <= 0.42) {
    return -2.5;
  }
  return 0;
}

function calculateMjArenaIjiriDipScoreA(metrics) {
  let score = 0;

  score += scoreFromMaximums(metrics.netTotal, [
    { maximum: -5000, score: 22 },
    { maximum: -4000, score: 18 },
    { maximum: -3000, score: 15 },
    { maximum: -2000, score: 11 },
    { maximum: -1000, score: 7 },
    { maximum: -500, score: 4 },
  ]);
  score += scoreFromMaximums(metrics.recentFourNetTotal, [
    { maximum: -3000, score: 12 },
    { maximum: -2000, score: 9 },
    { maximum: -1000, score: 6 },
    { maximum: -500, score: 3 },
  ]);
  score += scoreFromMaximums(metrics.recentThreeNetTotal, [
    { maximum: -2000, score: 8 },
    { maximum: -1500, score: 6 },
    { maximum: -1000, score: 4 },
  ]);
  score += scoreFromMaximums(metrics.recentTwoNetTotal, [
    { maximum: -2000, score: 6 },
    { maximum: -1000, score: 4 },
    { maximum: -500, score: 2 },
  ]);
  score += scoreFromMaximums(metrics.todayDifference, [
    { maximum: -1500, score: 4 },
    { maximum: -1000, score: 2 },
  ]);

  score += scoreFromMinimums(metrics.netTotal, [
    { minimum: 5000, score: -16 },
    { minimum: 3000, score: -12 },
    { minimum: 1500, score: -8 },
    { minimum: 500, score: -4 },
  ]);
  score += scoreFromMinimums(metrics.recentFourNetTotal, [
    { minimum: 3000, score: -10 },
    { minimum: 1500, score: -6 },
    { minimum: 500, score: -3 },
  ]);
  score += scoreFromMinimums(metrics.recentThreeNetTotal, [
    { minimum: 3000, score: -8 },
    { minimum: 1500, score: -6 },
    { minimum: 500, score: -3 },
  ]);
  score += scoreFromMinimums(metrics.recentTwoNetTotal, [
    { minimum: 2000, score: -6 },
    { minimum: 1000, score: -4 },
    { minimum: 500, score: -2 },
  ]);
  score += scoreFromMinimums(metrics.todayDifference, [
    { minimum: 2000, score: -5 },
    { minimum: 1000, score: -3 },
  ]);

  return clamp(score, -30, 45);
}

function calculateMjArenaIjiriDipScoreB(metrics) {
  let score = 0;

  if (metrics.netTotal <= -5000) {
    score += 25;
  } else if (metrics.netTotal <= -3000) {
    score += 18;
  } else if (metrics.netTotal <= -2000) {
    score += 13;
  } else if (metrics.netTotal <= -1000) {
    score += 9;
  } else if (metrics.netTotal < 0) {
    score += 4;
  } else if (metrics.netTotal >= 5000) {
    score -= 12;
  } else if (metrics.netTotal >= 3000) {
    score -= 9;
  } else if (metrics.netTotal >= 2000) {
    score -= 6;
  }

  if (metrics.recentThreeNetTotal <= -3000) {
    score += 12;
  } else if (metrics.recentThreeNetTotal <= -2000) {
    score += 9;
  } else if (metrics.recentThreeNetTotal <= -1000) {
    score += 5;
  } else if (metrics.recentThreeNetTotal >= 5000) {
    score -= 12;
  } else if (metrics.recentThreeNetTotal >= 3000) {
    score -= 10;
  } else if (metrics.recentThreeNetTotal >= 2000) {
    score -= 6;
  }

  if (metrics.todayDifference <= -2000) {
    score += 0;
  } else if (metrics.todayDifference <= -1000) {
    score += 8;
  } else if (metrics.todayDifference < 0) {
    score += 3;
  } else if (metrics.todayDifference >= 3000) {
    score -= 10;
  } else if (metrics.todayDifference >= 1000) {
    score -= 7;
  }

  return clamp(score, -20, 35);
}

function calculateMjArenaIjiriActivityScore(metrics) {
  let score = 0;
  const recentThreeAverageGames = metrics.recentThreeGamesTotal / 3;
  const recentTwoAverageGames = metrics.recentTwoGamesTotal / 2;
  const sevenAverageGames = metrics.averageGames;

  if (sevenAverageGames < 1000) {
    score += 8;
  } else if (sevenAverageGames < 2000) {
    score += 5;
  } else if (sevenAverageGames < 3000) {
    score += 2;
  } else if (sevenAverageGames >= 6000) {
    score -= 8;
  } else if (sevenAverageGames >= 5000) {
    score -= 5;
  }

  if (recentTwoAverageGames < 1000) {
    score += 4;
  } else if (recentTwoAverageGames < 2000) {
    score += 2;
  } else if (recentTwoAverageGames < 3000) {
    score += 1;
  } else if (recentTwoAverageGames >= 5000) {
    score -= 3;
  }

  if (recentThreeAverageGames <= 1500) {
    score += 6;
  } else if (recentThreeAverageGames <= 2500) {
    score += 3;
  } else if (recentThreeAverageGames >= 5500) {
    score -= 5;
  } else if (recentThreeAverageGames >= 4500) {
    score -= 2;
  }

  return clamp(score, -12, 15);
}

function calculateMjArenaIjiriSettingCycleScoreA(metrics) {
  if (!hasBeamHikariSettingMetrics(metrics)) {
    return 0;
  }

  let score = 0;
  if (metrics.highSettingEstimateCount === 0) {
    score += 6;
  } else if (metrics.highSettingEstimateCount >= 2) {
    score -= 9;
  }
  if (metrics.settingFiveCount === 0) {
    score += 3;
  } else if (metrics.settingFiveCount >= 1) {
    score -= 4;
  }
  if (metrics.recentThreeHighSettingEstimateCount >= 1) {
    score -= 2;
  }
  if (metrics.recentThreeSettingFiveCount >= 1) {
    score -= 2;
  }

  if (metrics.todaySetting >= 4.5) {
    if (metrics.todayDifference <= 0) {
      score += 6;
    } else {
      score -= 4;
    }
  }
  if (metrics.todaySetting >= 5) {
    if (metrics.todayDifference <= 0) {
      score += 2;
    } else {
      score -= 2;
    }
  }
  if (metrics.todaySetting >= 4.5 && metrics.twoDaysAgoHighSettingEstimate) {
    score -= 10;
  }
  if (metrics.todaySetting >= 5 && metrics.twoDaysAgoSettingFive) {
    score -= 7;
  }

  return clamp(score, -15, 25);
}

function calculateMjArenaIjiriSettingCycleScoreB(metrics) {
  if (!hasBeamHikariSettingMetrics(metrics)) {
    return 0;
  }

  let score = 0;
  if (metrics.highSettingEstimateCount === 0) {
    score += 8;
  } else if (metrics.highSettingEstimateCount >= 2) {
    score -= 10;
  }
  if (metrics.settingFiveCount === 0) {
    score += 5;
  } else {
    score -= 3;
  }
  if (metrics.recentFifteenHighSettingEstimateCount === 0) {
    score += 4;
  }
  if (Number.isFinite(metrics.daysSinceHistoryHighSettingEstimate)) {
    if (metrics.daysSinceHistoryHighSettingEstimate >= 15) {
      score += 7;
    } else if (metrics.daysSinceHistoryHighSettingEstimate >= 8) {
      score += 3;
    }
  }
  if (metrics.recentThreeHighSettingEstimateCount >= 1) {
    score -= 4;
  }
  if (metrics.todaySetting >= 4.5) {
    if (metrics.todayDifference < 0) {
      score += 12;
    } else if (metrics.todayDifference >= 1000) {
      score -= 6;
    }
  }
  if (metrics.todaySetting >= 5) {
    if (metrics.todayDifference < 0) {
      score += 4;
    } else if (metrics.todayDifference >= 1000) {
      score -= 3;
    }
  }

  return clamp(score, -15, 25);
}

function calculateMjArenaIjiriAHuntScore(metrics) {
  if (!isMjArenaIjiriTargetMachine(metrics.machineName)) {
    return 0;
  }

  const score =
    28 +
    calculateMjArenaIjiriDipScoreA(metrics) +
    calculateMjArenaIjiriActivityScore(metrics) +
    calculateMjArenaIjiriSettingCycleScoreA(metrics) +
    calculateMjArenaIjiriHistoryScore(metrics) +
    calculateMjArenaIjiriTailScore(metrics.slotNumber);

  return clamp(score, 0, 100);
}

function calculateMjArenaIjiriBHuntScore(metrics, context = {}) {
  if (!isMjArenaIjiriTargetMachine(metrics.machineName)) {
    return 0;
  }

  const score =
    25 +
    calculateMjArenaIjiriDateScore(context) +
    calculateMjArenaIjiriDipScoreB(metrics) +
    calculateMjArenaIjiriActivityScore(metrics) +
    calculateMjArenaIjiriSettingCycleScoreB(metrics) +
    calculateMjArenaIjiriHistoryScore(metrics) +
    calculateMjArenaIjiriTailScore(metrics.slotNumber);

  return clamp(score, 0, 100);
}

function isWonderlandMinamigaokaMyJuggler(machineName) {
  return normalizeText(machineName) === normalizeText("マイジャグラーV");
}

function isWonderlandMinamigaokaNeo(machineName) {
  return normalizeText(machineName) === normalizeText("ネオアイムジャグラーEX");
}

function isWonderlandMinamigaokaTargetMachine(machineName) {
  return isWonderlandMinamigaokaMyJuggler(machineName) || isWonderlandMinamigaokaNeo(machineName);
}

function readSlotNumberValue(slotNumber) {
  const match = String(slotNumber ?? "").match(/\d+/u);
  return match ? Number(match[0]) : null;
}

function readWonderlandMinamigaokaTargetDate(context = {}) {
  return context.nextBusinessDate ?? context.baseDate;
}

function calculateWonderlandMinamigaokaDateScoreA(machineName, context = {}) {
  const targetDate = readWonderlandMinamigaokaTargetDate(context);
  const day = readDateDay(targetDate);
  const weekday = readDateWeekday(targetDate);
  let score = 0;

  if (isWonderlandMinamigaokaMyJuggler(machineName)) {
    if (day === 21) {
      score += 18;
    } else if (day === 11) {
      score += 16;
    } else if (day === 31) {
      score += 10;
    } else if (day === 1) {
      score += 6;
    } else if (day === 7) {
      score -= 8;
    } else if (day === 17) {
      score -= 14;
    } else if (day === 27) {
      score -= 12;
    }

    if (weekday === 6) {
      score += 6;
    } else if ([2, 3].includes(weekday)) {
      score += 3;
    } else if (weekday === 4) {
      score -= 5;
    } else if (weekday === 5) {
      score -= 4;
    }
  } else if (isWonderlandMinamigaokaNeo(machineName)) {
    if (day === 21) {
      score += 18;
    } else if (day === 31) {
      score += 12;
    } else if ([1, 11].includes(day)) {
      score += 6;
    } else if (day === 7) {
      score -= 7;
    } else if ([17, 27].includes(day)) {
      score -= 8;
    }

    if (weekday === 6) {
      score += 6;
    } else if (weekday === 2) {
      score += 4;
    } else if (weekday === 0) {
      score += 2;
    } else if ([4, 5].includes(weekday)) {
      score -= 6;
    }
  }

  return clamp(score, -20, 24);
}

function calculateWonderlandMinamigaokaDateScoreB(context = {}) {
  const targetDate = readWonderlandMinamigaokaTargetDate(context);
  const day = readDateDay(targetDate);
  const weekday = readDateWeekday(targetDate);
  let score = 0;

  if (day === 21) {
    score += 12;
  } else if ([11, 31].includes(day)) {
    score += 8;
  } else if (day === 1) {
    score += 5;
  } else if ([7, 17, 27].includes(day)) {
    score -= 8;
  }

  if (weekday === 6) {
    score += 10;
  } else if (weekday === 2) {
    score += 6;
  } else if ([0, 3].includes(weekday)) {
    score += 2;
  } else if (weekday === 4) {
    score -= 7;
  } else if (weekday === 5) {
    score -= 6;
  }

  return clamp(score, -14, 20);
}

function getWonderlandMinamigaokaSlotGroup(machineName, slotNumber) {
  const slot = readSlotNumberValue(slotNumber);
  if (!Number.isFinite(slot)) {
    return "normal";
  }

  if (isWonderlandMinamigaokaMyJuggler(machineName)) {
    if ([1208, 1225, 1238].includes(slot)) {
      return "top";
    }
    if ([1206, 1223, 1224, 1230, 1235, 1239, 1203].includes(slot)) {
      return "good";
    }
    if ([1207, 1227, 1229].includes(slot)) {
      return "weak";
    }
    if ([1231, 1237].includes(slot)) {
      return "slightlyWeak";
    }
  }

  if (isWonderlandMinamigaokaNeo(machineName)) {
    if (slot === 1305) {
      return "top";
    }
    if ([1311, 1316, 1317, 1323, 1327, 1331, 1335, 1310, 1330, 1308].includes(slot)) {
      return "good";
    }
    if ([1318, 1322].includes(slot)) {
      return "weak";
    }
    if ([1222, 1302, 1307, 1319, 1320, 1328].includes(slot)) {
      return "slightlyWeak";
    }
  }

  return "normal";
}

function isWonderlandMinamigaokaStrongSlot(machineName, slotNumber) {
  const group = getWonderlandMinamigaokaSlotGroup(machineName, slotNumber);
  return group === "top" || group === "good";
}

function calculateWonderlandMinamigaokaSlotScoreA(metrics) {
  const group = getWonderlandMinamigaokaSlotGroup(metrics.machineName, metrics.slotNumber);
  if (group === "top") {
    return isWonderlandMinamigaokaNeo(metrics.machineName) ? 10 : 8;
  }
  if (group === "good") {
    return isWonderlandMinamigaokaMyJuggler(metrics.machineName) ? 5 : 4;
  }
  if (group === "weak") {
    return isWonderlandMinamigaokaNeo(metrics.machineName) ? -8 : -6;
  }
  if (group === "slightlyWeak") {
    return -4;
  }
  return 0;
}

function calculateWonderlandMinamigaokaSlotScoreB(metrics) {
  const tail = readSlotLastDigit(metrics.slotNumber);
  let score = isWonderlandMinamigaokaMyJuggler(metrics.machineName) ? 5 : 0;
  const group = getWonderlandMinamigaokaSlotGroup(metrics.machineName, metrics.slotNumber);

  if (group === "top") {
    score += 10;
  } else if (group === "good") {
    score += isWonderlandMinamigaokaMyJuggler(metrics.machineName) ? 6 : 4;
  } else if (group === "weak") {
    score -= isWonderlandMinamigaokaNeo(metrics.machineName) ? 8 : 6;
  } else if (group === "slightlyWeak") {
    score -= 5;
  }

  if (isWonderlandMinamigaokaMyJuggler(metrics.machineName)) {
    if ([8, 5, 4].includes(tail)) {
      score += 4;
    } else if (tail === 7) {
      score -= 4;
    }
  } else if (isWonderlandMinamigaokaNeo(metrics.machineName)) {
    if (tail === 5) {
      score += 3;
    } else if ([2, 8].includes(tail)) {
      score -= 3;
    }
  }

  return clamp(score, -12, 20);
}

function calculateWonderlandMinamigaokaDipScoreA(metrics) {
  const score =
    clamp(-metrics.netTotal / 240, -18, 18) +
    clamp(-metrics.todayDifference / 180, -10, 10) +
    clamp(-metrics.recentThreeNetTotal / 380, -6, 6);

  return Number.isFinite(score) ? score : 0;
}

function calculateWonderlandMinamigaokaDipScoreB(metrics) {
  let score = 0;

  if (metrics.netTotal <= -3000) {
    score += 20;
  } else if (metrics.netTotal <= -1000) {
    score += 10;
  } else if (metrics.netTotal >= 3000) {
    score -= 20;
  } else if (metrics.netTotal >= 1000) {
    score -= 10;
  }

  if (metrics.recentThreeNetTotal <= -3000) {
    score += 8;
  } else if (metrics.recentThreeNetTotal <= -1000) {
    score += 5;
  } else if (metrics.recentThreeNetTotal >= 3000) {
    score -= 8;
  } else if (metrics.recentThreeNetTotal >= 1000) {
    score -= 4;
  }

  if (metrics.todayDifference <= -2000) {
    score += 5;
  } else if (metrics.todayDifference <= -1000) {
    score += 3;
  } else if (metrics.todayDifference >= 2000) {
    score -= 8;
  } else if (metrics.todayDifference >= 1000) {
    score -= 4;
  }

  if (isWonderlandMinamigaokaStrongSlot(metrics.machineName, metrics.slotNumber)) {
    if (metrics.netTotal <= -3000) {
      score += 8;
    } else if (metrics.netTotal <= -1000) {
      score += 4;
    }
  }

  return clamp(score, -32, 36);
}

function calculateWonderlandMinamigaokaRecentBehaviorScoreA(metrics, context = {}) {
  const previousRbDenominator =
    metrics.previousRbCount > 0 && metrics.previousGames > 0 ? metrics.previousGames / metrics.previousRbCount : null;
  const recentThreeRbDenominator =
    metrics.recentThreeRbTotal > 0 && metrics.recentThreeGamesTotal > 0
      ? metrics.recentThreeGamesTotal / metrics.recentThreeRbTotal
      : null;
  let score = 0;

  if (isWonderlandMinamigaokaMyJuggler(metrics.machineName)) {
    if (Number.isFinite(previousRbDenominator) && previousRbDenominator <= 270) {
      score += 6;
    } else if (Number.isFinite(previousRbDenominator) && previousRbDenominator <= 320) {
      score += 3;
    }
    if (metrics.rbTotal <= 45) {
      score += 5;
    }
    if (metrics.averageGames < 3000) {
      score += 5;
    }
    if (countWonderlandMinamigaokaNeighborHighSettings(metrics, context, 2) >= 2) {
      score += 4;
    }
  } else if (isWonderlandMinamigaokaNeo(metrics.machineName)) {
    if (Number.isFinite(previousRbDenominator) && previousRbDenominator <= 270) {
      score += 5;
    } else if (Number.isFinite(previousRbDenominator) && previousRbDenominator <= 320) {
      score += 3;
    }
    if (metrics.rbTotal <= 45) {
      score += 4;
    }
    if (metrics.averageGames < 3000) {
      score += 2;
    }
    if (metrics.previousGames >= 6000) {
      score += 3;
    }
    if (metrics.todaySetting >= 4.5 && metrics.previousRbCount >= 30) {
      score += 3;
    }
    if (countWonderlandMinamigaokaNeighborHighSettings(metrics, context, 1) >= 1) {
      score -= 4;
    }
  }

  if (Number.isFinite(recentThreeRbDenominator) && recentThreeRbDenominator <= 280) {
    score += 2;
  }

  return clamp(score, -8, 18);
}

function calculateWonderlandMinamigaokaRecentBehaviorScoreB(metrics, context = {}) {
  const targetDate = readWonderlandMinamigaokaTargetDate(context);
  const weekday = readDateWeekday(targetDate);
  const recentThreeRbDenominator =
    metrics.recentThreeRbTotal > 0 && metrics.recentThreeGamesTotal > 0
      ? metrics.recentThreeGamesTotal / metrics.recentThreeRbTotal
      : null;
  let score = 0;

  if (metrics.netTotal <= -3000 && Number.isFinite(recentThreeRbDenominator) && recentThreeRbDenominator <= 280) {
    score += 6;
  }
  if (weekday === 6 && metrics.netTotal <= -3000) {
    score += 4;
  }
  if (metrics.previousGames >= 8000) {
    score += 6;
  } else if (metrics.previousGames >= 6000) {
    score += 3;
  } else if (metrics.previousGames < 1000) {
    score -= 6;
  }
  if (metrics.recentThreeGamesTotal >= 15000) {
    score += 3;
  }
  if (metrics.previousRbCount >= 25) {
    score += 3;
  }
  if (Number.isFinite(recentThreeRbDenominator) && recentThreeRbDenominator <= 280) {
    score += 5;
  } else if (Number.isFinite(recentThreeRbDenominator) && recentThreeRbDenominator >= 400) {
    score -= 4;
  }
  if (metrics.recentTwoRbTotal <= 5) {
    score -= 5;
  }

  return clamp(score, -12, 20);
}

function calculateWonderlandMinamigaokaRotationScoreA(metrics) {
  if (!hasBeamHikariSettingMetrics(metrics)) {
    return 0;
  }

  let score = 0;
  const daysSince = metrics.daysSinceHistoryHighSettingEstimate;

  if (isWonderlandMinamigaokaMyJuggler(metrics.machineName)) {
    if (metrics.highSettingEstimateCount === 0) {
      score += 8;
    } else if (metrics.highSettingEstimateCount >= 2) {
      score -= 18;
    }
    if (Number.isFinite(daysSince)) {
      if (daysSince >= 8 && daysSince <= 14) {
        score += 7;
      } else if (daysSince >= 15) {
        score += 4;
      } else if (daysSince >= 5 && daysSince <= 7) {
        score -= 12;
      } else if (daysSince >= 2 && daysSince <= 4) {
        score -= 3;
      }
    } else {
      score += 4;
    }
    if (metrics.todaySetting >= 4.5) {
      score -= 2;
    }
  } else if (isWonderlandMinamigaokaNeo(metrics.machineName)) {
    if (metrics.highSettingEstimateCount === 0) {
      score += 5;
    } else if (metrics.highSettingEstimateCount >= 2) {
      score -= 10;
    }
    if (Number.isFinite(daysSince)) {
      if (daysSince >= 8 && daysSince <= 14) {
        score += 3;
      } else if (daysSince >= 15) {
        score += 2;
      } else if (daysSince >= 5 && daysSince <= 7) {
        score -= 8;
      } else if (daysSince >= 2 && daysSince <= 4) {
        score -= 2;
      }
    } else {
      score += 2;
    }
    if (metrics.todaySetting >= 4.5) {
      score += 5;
    }
  }

  if (metrics.highSettingStreak >= 2) {
    score -= 18;
  }

  return clamp(score, -24, 18);
}

function calculateWonderlandMinamigaokaRotationScoreB(metrics) {
  if (!hasBeamHikariSettingMetrics(metrics)) {
    return 0;
  }

  let score = 0;
  const daysSince = metrics.daysSinceHistoryHighSettingEstimate;

  if (metrics.todaySetting >= 4.5) {
    score += 7;
  }
  if (metrics.todaySetting >= 5) {
    score += 5;
  }
  if (metrics.todaySetting >= 4.5 && metrics.todayDifference < 0) {
    score += 7;
  }
  if (metrics.highSettingStreak >= 2) {
    score -= 30;
  }
  if (metrics.highSettingEstimateCount === 0) {
    score += 4;
  } else if (metrics.highSettingEstimateCount === 1) {
    score -= 2;
  } else if (metrics.highSettingEstimateCount >= 2) {
    score -= 18;
  }
  if (Number.isFinite(daysSince)) {
    if (daysSince >= 8 && daysSince <= 14) {
      score += 5;
    } else if (daysSince >= 3 && daysSince <= 7) {
      score -= 8;
    }
  }

  return clamp(score, -32, 25);
}

function countWonderlandMinamigaokaNeighborHighSettings(metrics, context = {}, distance = 1) {
  const slot = readSlotNumberValue(metrics.slotNumber);
  if (!Number.isFinite(slot) || !Array.isArray(context.metricsList)) {
    return 0;
  }

  return context.metricsList.filter((otherMetrics) => {
    if (!otherMetrics || otherMetrics === metrics) {
      return false;
    }
    if (otherMetrics.machineName !== metrics.machineName || otherMetrics.todaySetting < 4.5) {
      return false;
    }
    const otherSlot = readSlotNumberValue(otherMetrics.slotNumber);
    return Number.isFinite(otherSlot) && Math.abs(otherSlot - slot) <= distance;
  }).length;
}

function calculateWonderlandMinamigaokaAHuntScore(metrics, context = {}) {
  if (!isWonderlandMinamigaokaTargetMachine(metrics.machineName)) {
    return 0;
  }

  const score =
    40 +
    calculateWonderlandMinamigaokaDipScoreA(metrics) +
    calculateWonderlandMinamigaokaDateScoreA(metrics.machineName, context) * 0.8 +
    calculateWonderlandMinamigaokaRotationScoreA(metrics) * 0.8 +
    calculateWonderlandMinamigaokaRecentBehaviorScoreA(metrics, context) * 0.8 +
    calculateWonderlandMinamigaokaSlotScoreA(metrics) * 0.8;

  return clamp(score, 0, 100);
}

function calculateWonderlandMinamigaokaBHuntScore(metrics, context = {}) {
  if (!isWonderlandMinamigaokaTargetMachine(metrics.machineName)) {
    return 0;
  }

  const score =
    40 +
    calculateWonderlandMinamigaokaDateScoreB(context) +
    calculateWonderlandMinamigaokaSlotScoreB(metrics) +
    calculateWonderlandMinamigaokaDipScoreB(metrics) +
    calculateWonderlandMinamigaokaRecentBehaviorScoreB(metrics, context) +
    calculateWonderlandMinamigaokaRotationScoreB(metrics);

  return clamp(score, 0, 100);
}

function isWonderlandSueTargetMachine(machineName) {
  const normalizedMachineName = normalizeText(machineName);
  return WONDERLAND_SUE_TARGET_MACHINES.some(
    (targetMachine) =>
      normalizeText(targetMachine.name) === normalizedMachineName ||
      (targetMachine.aliases ?? []).some((alias) => normalizeText(alias) === normalizedMachineName),
  );
}

function isWonderlandSueHighCandidate(metrics) {
  return Boolean(metrics && metrics.todaySetting >= 4.5 && metrics.previousRbCount >= 25);
}

function calculateWonderlandSueDipScoreA(metrics) {
  const d7 = clamp((-metrics.netTotal - 1000) / 6000, 0, 1);
  const d3 = clamp((-metrics.recentThreeNetTotal - 1000) / 4000, 0, 1);
  const p7 = clamp((metrics.netTotal - 3000) / 5000, 0, 1);
  const rb = clamp((metrics.previousRbCount - 25) / 15, 0, 1);
  const previousHighUnderwhelmed = isWonderlandSueHighCandidate(metrics)
    ? clamp((1000 - metrics.todayDifference) / 2000, 0, 1)
    : 0;
  const games = clamp((metrics.previousGames - 7000) / 2000, 0, 1);

  const rawScore = 20 + 60 * d7 + 30 * d3 + 9 * rb + 3 * previousHighUnderwhelmed + 2 * games - 10 * p7;
  return clamp((rawScore / 124) * 100, 0, 100);
}

function countWonderlandSueNeighborHighCandidates(metrics, context = {}) {
  const slot = readSlotNumberValue(metrics.slotNumber);
  if (!Number.isFinite(slot) || !Array.isArray(context.metricsList)) {
    return 0;
  }

  return context.metricsList.filter((otherMetrics) => {
    if (!otherMetrics || otherMetrics === metrics || otherMetrics.machineName !== metrics.machineName) {
      return false;
    }
    const otherSlot = readSlotNumberValue(otherMetrics.slotNumber);
    return Number.isFinite(otherSlot) && Math.abs(otherSlot - slot) === 1 && isWonderlandSueHighCandidate(otherMetrics);
  }).length;
}

function calculateWonderlandSueDipScoreB(metrics) {
  let score = 50;
  score += 45 * clamp(-metrics.netTotal / 9000, 0, 1);
  score += 20 * clamp(-metrics.recentThreeNetTotal / 6000, 0, 1);
  score += 12 * clamp(-metrics.recentTwoNetTotal / 3000, 0, 1);
  score -= 5 * clamp(metrics.netTotal / 6000, 0, 1);
  score -= 8 * clamp(metrics.recentThreeNetTotal / 3000, 0, 1);
  score -= 3 * clamp((3500 - metrics.previousGames) / 2500, 0, 1);

  if (metrics.todayDifference <= -1500) {
    score += 6;
  } else if (metrics.todayDifference <= -1000) {
    score += 4;
  } else if (metrics.todayDifference >= 1000) {
    score -= 4;
  }

  return score;
}

function calculateWonderlandSuePreviousHighScoreB(metrics, context = {}) {
  if (!hasBeamHikariSettingMetrics(metrics)) {
    return 0;
  }

  let score = 0;
  const highCandidate = isWonderlandSueHighCandidate(metrics);
  const settingFive = metrics.todaySetting >= 5;
  const twoDaysAgoHigh = metrics.twoDaysAgoHighSettingCandidate;
  const threeDaysAgoHigh = metrics.threeDaysAgoHighSettingCandidate;

  if (highCandidate && metrics.todayDifference <= 0) {
    score += 12;
  } else if (highCandidate) {
    score += 5;
  }
  if (highCandidate && !twoDaysAgoHigh) {
    score += 8;
  }
  if (highCandidate && twoDaysAgoHigh && !threeDaysAgoHigh) {
    score += 5;
  }
  if (settingFive) {
    score += 2;
  }
  if (highCandidate && twoDaysAgoHigh && threeDaysAgoHigh) {
    score -= 10;
  }

  if (countWonderlandSueNeighborHighCandidates(metrics, context) > 0 && metrics.todayDifference <= -1000) {
    score += 4;
  }

  return score;
}

function calculateWonderlandSueHistoryScoreB(metrics) {
  if (!hasBeamHikariSettingMetrics(metrics)) {
    return 0;
  }

  let score = 0;
  if (metrics.historyThirtyHighSettingCandidateCount >= 5) {
    score += 3;
  } else if (metrics.historyThirtyHighSettingCandidateCount >= 4) {
    score += 2;
  } else if (metrics.highSettingCandidateCount >= 2) {
    score += 1;
  }

  if (metrics.historyThirtySettingFiveCount >= 4) {
    score += 6;
  } else if (metrics.historyThirtySettingFiveCount >= 3) {
    score += 3;
  }

  if (metrics.historyFortyFiveHighSettingCandidateCount === 0) {
    score -= 18;
  } else if (metrics.historyThirtyHighSettingCandidateCount === 0) {
    score -= 9;
  }

  return score;
}

function calculateWonderlandSueBehaviorScoreB(metrics) {
  let score = 0;

  if (metrics.previousRbCount >= 40) {
    score += 24;
  } else if (metrics.previousRbCount >= 35) {
    score += 18;
  } else if (metrics.previousRbCount >= 30) {
    score += 12;
  } else if (metrics.previousRbCount >= 25) {
    score += 6;
  }

  if (metrics.previousGames >= 8000) {
    score += 10;
  } else if (metrics.previousGames >= 7000) {
    score += 5;
  } else if (metrics.previousGames <= 2000) {
    score -= 8;
  } else if (metrics.previousGames <= 3000) {
    score -= 4;
  }

  if (metrics.todayDifference >= 1000 && !isWonderlandSueHighCandidate(metrics)) {
    score -= 4;
  }

  return score;
}

function calculateWonderlandSueAHuntScore(metrics) {
  if (!isWonderlandSueTargetMachine(metrics.machineName)) {
    return 0;
  }

  return calculateWonderlandSueDipScoreA(metrics);
}

function calculateWonderlandSueBHuntScore(metrics, context = {}) {
  if (!isWonderlandSueTargetMachine(metrics.machineName)) {
    return 0;
  }

  const score =
    calculateWonderlandSueDipScoreB(metrics) +
    calculateWonderlandSuePreviousHighScoreB(metrics, context) +
    calculateWonderlandSueBehaviorScoreB(metrics) +
    calculateWonderlandSueHistoryScoreB(metrics);

  return clamp(score, 0, 100);
}

function isMjArenaAirportTargetMachine(machineName) {
  const normalizedMachineName = normalizeText(machineName);
  return MJ_ARENA_AIRPORT_TARGET_MACHINES.some(
    (targetMachine) =>
      normalizeText(targetMachine.name) === normalizedMachineName ||
      (targetMachine.aliases ?? []).some((alias) => normalizeText(alias) === normalizedMachineName),
  );
}

function isMjArenaAirportHighCandidate(metrics) {
  return Boolean(metrics && metrics.todaySetting >= 4.5 && metrics.previousRbCount >= 25);
}

function calculateMjArenaAirportDifferenceScoreA(metrics) {
  let score = 0;
  const d7 = metrics.netTotal;
  const d3 = metrics.recentThreeNetTotal;
  const d1 = metrics.todayDifference;

  if (d7 <= -5000) {
    score += 8;
  } else if (d7 <= -3000) {
    score += 18;
  } else if (d7 <= -2000) {
    score += 16;
  } else if (d7 <= -1000) {
    score += 10;
  } else if (d7 < 0) {
    score += 4;
  } else if (d7 > 5000) {
    score -= 15;
  } else if (d7 > 3000) {
    score -= 12;
  } else if (d7 > 1000) {
    score -= 8;
  }

  if (d3 <= -3000) {
    score += 6;
  } else if (d3 <= -2000) {
    score += 10;
  } else if (d3 <= -1000) {
    score += 11;
  } else if (d3 < 0) {
    score += 7;
  } else if (d3 > 2000) {
    score -= 10;
  } else if (d3 > 1000) {
    score -= 6;
  }

  if (d1 <= -1500) {
    score += 6;
  } else if (d1 <= -500) {
    score += 5;
  } else if (d1 < 0) {
    score += 2;
  } else if (d1 > 1000) {
    score -= 6;
  } else if (d1 > 500) {
    score -= 4;
  }

  return score;
}

function calculateMjArenaAirportActivityScoreA(metrics) {
  let score = 0;

  if (metrics.rbTotal <= 15) {
    score += 14;
  } else if (metrics.rbTotal <= 20) {
    score += 10;
  } else if (metrics.rbTotal <= 30) {
    score += 6;
  } else if (metrics.rbTotal <= 50) {
    score += 2;
  } else if (metrics.rbTotal > 60) {
    score -= 4;
  }

  if (metrics.bbTotal <= 20) {
    score += 4;
  } else if (metrics.bbTotal <= 25) {
    score += 12;
  } else if (metrics.bbTotal <= 50) {
    score += 8;
  } else if (metrics.bbTotal <= 60) {
    score += 4;
  } else if (metrics.bbTotal > 80) {
    score -= 8;
  }

  if (metrics.averageGames <= 1000) {
    score += 10;
  } else if (metrics.averageGames <= 1500) {
    score += 8;
  } else if (metrics.averageGames <= 2000) {
    score += 6;
  } else if (metrics.averageGames <= 2500) {
    score += 2;
  } else if (metrics.averageGames >= 3500) {
    score -= 3;
  }

  return score;
}

function calculateMjArenaAirportPreviousHighScoreA(metrics) {
  if (!hasBeamHikariSettingMetrics(metrics)) {
    return 0;
  }

  if (metrics.highSettingEstimateStreak >= 2) {
    return -12;
  }

  const d1 = metrics.todayDifference;
  if (metrics.todaySetting >= 5) {
    if (d1 < 500) {
      return 18;
    }
    if (d1 < 1000) {
      return 6;
    }
    return 4;
  }

  if (metrics.todaySetting >= 4.5) {
    if (d1 < 500) {
      return 7;
    }
    if (d1 < 1000) {
      return 3;
    }
    return -5;
  }

  return 0;
}

function calculateMjArenaAirportRotationScoreA(metrics) {
  if (!hasBeamHikariSettingMetrics(metrics)) {
    return 0;
  }

  let score = 0;
  if (metrics.highSettingEstimateCount === 0) {
    score += 6;
  } else if (metrics.highSettingEstimateCount >= 2) {
    score -= 5;
  }
  if (metrics.settingFiveCount === 0) {
    score += 3;
  } else if (metrics.settingFiveCount >= 2) {
    score -= 5;
  }

  const daysSince = metrics.daysSinceHistoryHighSettingEstimate;
  if (Number.isFinite(daysSince)) {
    if (daysSince === 1) {
      score += 1;
    } else if (daysSince >= 2 && daysSince <= 7) {
      score -= 6;
    } else if (daysSince >= 8 && daysSince <= 14) {
      score += 2;
    } else if (daysSince >= 15 && daysSince <= 30) {
      score += 5;
    }
  } else {
    score += 5;
  }

  return score;
}

function calculateMjArenaAirportAHuntScore(metrics) {
  if (!isMjArenaAirportTargetMachine(metrics.machineName)) {
    return 0;
  }

  let score =
    20 +
    calculateMjArenaAirportDifferenceScoreA(metrics) +
    calculateMjArenaAirportActivityScoreA(metrics) +
    calculateMjArenaAirportPreviousHighScoreA(metrics) +
    calculateMjArenaAirportRotationScoreA(metrics);

  if (metrics.previousGames <= 100) {
    score -= 10;
  }
  if (!hasBeamHikariSettingMetrics(metrics) || metrics.todaySetting < 4.5) {
    score = Math.min(score, 85);
  }

  return clamp(score, 0, 100);
}

function calculateMjArenaAirportDateScoreB(context = {}) {
  const targetDate = context.nextBusinessDate ?? context.baseDate;
  const day = readDateDay(targetDate);
  const weekday = readDateWeekday(targetDate);
  let score = 0;

  if ([5, 15, 25].includes(day)) {
    score += 10;
  } else if ([0, 3, 6].includes(weekday)) {
    score += 4;
  } else if ([1, 5].includes(weekday)) {
    score -= 3;
  }

  return score;
}

function calculateMjArenaAirportDifferenceScoreB(metrics) {
  let score = 0;
  const s7 = metrics.netTotal;
  const s3 = metrics.recentThreeNetTotal;
  const s1 = metrics.todayDifference;

  if (s7 <= -2000) {
    score += 18;
  } else if (s7 <= -1000) {
    score += 13;
  } else if (s7 <= -500) {
    score += 8;
  } else if (s7 <= 0) {
    score += 3;
  } else if (s7 >= 2000) {
    score -= 14;
  } else if (s7 >= 1000) {
    score -= 9;
  } else if (s7 >= 500) {
    score -= 4;
  }

  if (s3 <= -1000) {
    score += 9;
  } else if (s3 <= -500) {
    score += 7;
  } else if (s3 <= 0) {
    score += 3;
  } else if (s3 >= 2000) {
    score -= 8;
  } else if (s3 >= 1000) {
    score -= 5;
  }

  if (s1 <= -2000) {
    score += 4;
  } else if (s1 <= -1000) {
    score += 3;
  } else if (s1 <= -500) {
    score += 2;
  } else if (s1 >= 2000) {
    score -= 5;
  } else if (s1 >= 1000) {
    score -= 3;
  }

  return score;
}

function calculateMjArenaAirportActivityScoreB(metrics) {
  let score = 0;
  const rbAverage = metrics.rbTotal / 7;

  if (metrics.averageGames < 1000) {
    score += 10;
  } else if (metrics.averageGames < 2000) {
    score += 8;
  } else if (metrics.averageGames < 3000) {
    score += 3;
  } else if (metrics.averageGames >= 5000) {
    score -= 4;
  }

  if (rbAverage < 5) {
    score += 10;
  } else if (rbAverage < 10) {
    score += 5;
  } else if (rbAverage >= 15) {
    score -= 3;
  }

  return score;
}

function calculateMjArenaAirportRotationScoreB(metrics) {
  if (!hasBeamHikariSettingMetrics(metrics)) {
    return 0;
  }

  let score = 0;
  if (metrics.highSettingCandidateCount === 0) {
    score += 8;
  } else if (metrics.highSettingCandidateCount === 1) {
    score -= 6;
  } else if (metrics.highSettingCandidateCount >= 2) {
    score -= 10;
  }

  if (metrics.recentFifteenHighSettingEstimateCount === 0) {
    score += 5;
  } else if (metrics.recentFifteenHighSettingEstimateCount >= 3) {
    score -= 5;
  }

  const daysSinceHighCandidate = metrics.daysSinceHistoryHighSettingCandidate;
  if (!Number.isFinite(daysSinceHighCandidate) || daysSinceHighCandidate >= 15) {
    score += 4;
  } else if (daysSinceHighCandidate >= 2 && daysSinceHighCandidate <= 7) {
    score -= 4;
  }

  const daysSinceSettingFive = metrics.daysSinceHistorySettingFive;
  if (!Number.isFinite(daysSinceSettingFive) || daysSinceSettingFive >= 15) {
    score += 2;
  }

  return score;
}

function calculateMjArenaAirportPreviousHighScoreB(metrics) {
  if (!hasBeamHikariSettingMetrics(metrics)) {
    return 0;
  }

  let score = 0;
  const d1 = metrics.todayDifference;
  const settingFive = metrics.todaySetting >= 5;
  const highCandidate = isMjArenaAirportHighCandidate(metrics);
  const highEstimate = metrics.todaySetting >= 4.5;

  if (settingFive && d1 < 0) {
    score += 12;
  } else if (highCandidate && d1 < 0) {
    score += 10;
  } else if (highEstimate && d1 < 0) {
    score += 5;
  }

  if (settingFive && d1 >= 0) {
    score += 3;
  }
  if (highEstimate && d1 >= 1000) {
    score -= 5;
  }
  if (metrics.highSettingEstimateStreak >= 2 || metrics.highSettingCandidateStreak >= 2) {
    score -= 10;
  }

  return score;
}

function calculateMjArenaAirportBHuntScore(metrics, context = {}) {
  if (!isMjArenaAirportTargetMachine(metrics.machineName)) {
    return 0;
  }

  const score =
    calculateMjArenaAirportDateScoreB(context) +
    calculateMjArenaAirportDifferenceScoreB(metrics) +
    calculateMjArenaAirportActivityScoreB(metrics) +
    calculateMjArenaAirportRotationScoreB(metrics) +
    calculateMjArenaAirportPreviousHighScoreB(metrics);

  return clamp(score, 0, 100);
}

function isSlotMarumitsuOhashiTargetMachine(machineName) {
  const normalizedMachineName = normalizeText(machineName);
  return SLOT_MARUMITSU_OHASHI_TARGET_MACHINES.some(
    (targetMachine) =>
      normalizeText(targetMachine.name) === normalizedMachineName ||
      (targetMachine.aliases ?? []).some((alias) => normalizeText(alias) === normalizedMachineName),
  );
}

function isSlotMarumitsuOhashiHighCandidate(metrics) {
  return Boolean(metrics && metrics.todaySetting >= 4.5 && metrics.previousRbCount >= 25);
}

function countSlotMarumitsuOhashiNeighborHighCandidates(metrics, context = {}) {
  const slot = readSlotNumberValue(metrics.slotNumber);
  if (!Number.isFinite(slot) || !Array.isArray(context.metricsList)) {
    return 0;
  }

  return context.metricsList.filter((otherMetrics) => {
    if (!otherMetrics || otherMetrics === metrics || otherMetrics.machineName !== metrics.machineName) {
      return false;
    }
    const otherSlot = readSlotNumberValue(otherMetrics.slotNumber);
    return (
      Number.isFinite(otherSlot) &&
      Math.abs(otherSlot - slot) === 1 &&
      isSlotMarumitsuOhashiHighCandidate(otherMetrics)
    );
  }).length;
}

function calculateSlotMarumitsuOhashiDifferenceScoreA(metrics) {
  let score = 20;

  if (metrics.recentThreeNetTotal <= -1000) {
    score += 18;
  }
  if (metrics.recentThreeNetTotal <= -2500) {
    score += 8;
  }
  if (metrics.netTotal <= -5000) {
    score += 6;
  }
  if (metrics.recentTwoNetTotal <= -725) {
    score += 10;
  }
  if (metrics.recentTwoNetTotal <= -1500) {
    score += 4;
  }
  if (metrics.recentTwoNetTotal <= -2500) {
    score += 4;
  }
  if (metrics.todayDifference <= 0) {
    score += 4;
  }
  if (metrics.todayDifference <= -1000) {
    score += 7;
  }

  if (metrics.todayDifference >= 500) {
    score -= 18;
  }
  if (metrics.todayDifference >= 1000) {
    score -= 4;
  }
  if (metrics.recentTwoNetTotal >= 2000) {
    score -= 8;
  }
  if (metrics.recentThreeNetTotal >= 3000) {
    score -= 8;
  }
  if (metrics.netTotal >= 5000) {
    score -= 6;
  }

  return score;
}

function calculateSlotMarumitsuOhashiActivityScoreA(metrics) {
  let score = 0;
  const recentTwoAverageGames = metrics.recentTwoGamesTotal / 2;
  const recentThreeAverageGames = metrics.recentThreeGamesTotal / 3;
  const recentThreeBbTotal = metrics.recentThreeBonusTotal - metrics.recentThreeRbTotal;

  if (recentThreeBbTotal <= 38) {
    score += 10;
  }
  if (recentThreeBbTotal <= 30) {
    score += 2;
  }
  if (metrics.recentThreeRbTotal <= 30) {
    score += 6;
  }
  if (recentTwoAverageGames <= 3000) {
    score += 5;
  }
  if (recentThreeAverageGames <= 4000) {
    score += 3;
  }

  if (recentThreeBbTotal >= 55) {
    score -= 6;
  }
  if (metrics.rbTotal >= 110) {
    score -= 4;
  }
  if (metrics.previousGames >= 7000) {
    score -= 4;
  }

  return score;
}

function calculateSlotMarumitsuOhashiSettingScoreA(metrics, context = {}) {
  if (!hasBeamHikariSettingMetrics(metrics)) {
    return 0;
  }

  let score = 0;
  const previousHigh = isSlotMarumitsuOhashiHighCandidate(metrics);
  const daysSince = metrics.daysSinceHistoryHighSettingCandidate;

  if (metrics.highSettingCandidateCount === 0) {
    score += 5;
    if (metrics.historyThirtyHighSettingCandidateCount >= 3) {
      score += 5;
    }
  }
  if (Number.isFinite(daysSince) && daysSince >= 4 && daysSince <= 14) {
    score += 5;
  }
  if (!previousHigh && countSlotMarumitsuOhashiNeighborHighCandidates(metrics, context) > 0) {
    score += 6;
  }
  if (previousHigh) {
    score -= 18;
    if (metrics.todayDifference < 0) {
      score += 28;
    }
  }
  if (metrics.highSettingCandidateStreak >= 2) {
    score -= 20;
  }
  if (Number.isFinite(daysSince) && [1, 2].includes(daysSince)) {
    score -= 10;
  }
  if (metrics.highSettingCandidateCount >= 2) {
    score -= 8;
  }
  if (metrics.recentFourteenHighSettingCandidateCount >= 3) {
    score -= 5;
  }
  if (Number.isFinite(metrics.windowSettingAverage) && metrics.windowSettingAverage >= 3.2) {
    score -= 4;
  }

  return score;
}

function calculateSlotMarumitsuOhashiAHuntScore(metrics, context = {}) {
  if (!isSlotMarumitsuOhashiTargetMachine(metrics.machineName)) {
    return 0;
  }

  const score =
    calculateSlotMarumitsuOhashiDifferenceScoreA(metrics) +
    calculateSlotMarumitsuOhashiActivityScoreA(metrics) +
    calculateSlotMarumitsuOhashiSettingScoreA(metrics, context);

  return clamp(score, 0, 100);
}

function calculateSlotMarumitsuOhashiDifferenceScoreB(metrics) {
  let score = 50;

  if (metrics.recentThreeNetTotal <= -4000) {
    score += 30;
  } else if (metrics.recentThreeNetTotal <= -3000) {
    score += 25;
  } else if (metrics.recentThreeNetTotal <= -2000) {
    score += 18;
  } else if (metrics.recentThreeNetTotal <= -1000) {
    score += 10;
  } else if (metrics.recentThreeNetTotal >= 2000) {
    score -= 12;
  }

  if (metrics.netTotal <= -5000) {
    score += 10;
  } else if (metrics.netTotal <= -3000) {
    score += 6;
  } else if (metrics.netTotal >= 3000) {
    score -= 8;
  }

  if (metrics.todayDifference >= -2000 && metrics.todayDifference <= -500) {
    score += 8;
  }
  if (metrics.todayDifference >= 1000) {
    score -= 10;
  }

  return score;
}

function calculateSlotMarumitsuOhashiSettingScoreB(metrics) {
  if (!hasBeamHikariSettingMetrics(metrics)) {
    return 0;
  }

  let score = 0;
  if (isSlotMarumitsuOhashiHighCandidate(metrics)) {
    score -= 20;
  }
  if (metrics.highSettingCandidateCount === 0) {
    score += 5;
  } else if (metrics.highSettingCandidateCount >= 2) {
    score -= 10;
  }
  return score;
}

function calculateSlotMarumitsuOhashiActivityScoreB(metrics) {
  let score = 0;
  if (metrics.recentThreeRbTotal <= 20) {
    score += 4;
  } else if (metrics.recentThreeRbTotal >= 60) {
    score -= 4;
  }
  if (metrics.recentThreeBonusTotal <= 60) {
    score += 3;
  } else if (metrics.recentThreeBonusTotal >= 150) {
    score -= 5;
  }
  return score;
}

function calculateSlotMarumitsuOhashiBHuntScore(metrics) {
  if (!isSlotMarumitsuOhashiTargetMachine(metrics.machineName)) {
    return 0;
  }

  const score =
    calculateSlotMarumitsuOhashiDifferenceScoreB(metrics) +
    calculateSlotMarumitsuOhashiSettingScoreB(metrics) +
    calculateSlotMarumitsuOhashiActivityScoreB(metrics);

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

function countAdjacentHighSettingCandidates(
  businessDates,
  dateIndex,
  row,
  rowsByDate,
  settingDefinitionCache,
  config,
  windowDays,
) {
  if (!(rowsByDate instanceof Map)) {
    return 0;
  }

  const slotNumber = Number(row?.slot_number);
  if (!Number.isFinite(slotNumber)) {
    return 0;
  }

  const normalizedWindowDays = Math.max(1, Number(windowDays) || DEFAULT_HUNT_SCORE_WINDOW_DAYS);
  const startIndex = Math.max(0, dateIndex - (normalizedWindowDays - 1));
  const windowDates = businessDates.slice(startIndex, dateIndex + 1);
  let count = 0;

  for (const date of windowDates) {
    for (const dateRow of rowsByDate.get(date) ?? []) {
      const dateRowSlotNumber = Number(dateRow?.slot_number);
      if (
        Number.isFinite(dateRowSlotNumber) &&
        Math.abs(dateRowSlotNumber - slotNumber) === 1 &&
        isStandardHighSettingCandidateRow(dateRow, settingDefinitionCache, config)
      ) {
        count += 1;
      }
    }
  }

  return count;
}

function calculateWindowMetrics(
  businessDates,
  dateIndex,
  row,
  recordMapByDate,
  settingDefinitionCache,
  config,
  rowsByDate = null,
) {
  const windowDays = config.windowDays ?? DEFAULT_HUNT_SCORE_WINDOW_DAYS;
  const useAvailableRows = config?.logicKey === "amuse-asakusa";
  const windowRows = useAvailableRows
    ? buildAvailableWindowRows(businessDates, dateIndex, recordMapByDate, windowDays, config)
    : buildWindowRows(businessDates, dateIndex, recordMapByDate, windowDays, config);
  if (!windowRows || windowRows.length === 0) {
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
  let settingSampleCount = 0;
  let lowSettingCount = 0;
  let highSettingCount = 0;
  let highSettingEstimateCount = 0;
  let highSettingCandidateCount = 0;
  let settingFiveCount = 0;
  let strongHighSettingCandidateCount = 0;
  const metricWindowRows = [];
  let historyRowCount = 0;
  let historySettingSampleCount = 0;
  let historyHighSettingCount = 0;
  let historyHighSettingEstimateCount = 0;
  let historyNetTotal = 0;
  let historyPositiveDays = 0;

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
    if (Number.isFinite(settingAverage)) {
      settingSampleCount += 1;
      if (settingAverage <= 3) {
        lowSettingCount += 1;
      }
      if (settingAverage >= 4) {
        highSettingCount += 1;
      }
      if (settingAverage >= 4.5) {
        highSettingEstimateCount += 1;
      }
      if (settingAverage >= 5) {
        settingFiveCount += 1;
      }
    }
    if (Number.isFinite(settingAverage) && settingAverage >= 4.5 && rbCount >= 25) {
      highSettingCandidateCount += 1;
    }
    if (Number.isFinite(settingAverage) && (settingAverage >= 5 || (settingAverage >= 4.5 && rbCount >= 25))) {
      strongHighSettingCandidateCount += 1;
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
    historyRowCount += 1;
    const settingAverage = getSettingEstimateAverage(
      settingDefinitionCache,
      historyWindowRow.row,
      config,
    ).average;
    historyNetTotal += historyWindowRow.differenceValue;
    if (historyWindowRow.differenceValue > 0) {
      historyPositiveDays += 1;
    }
    if (Number.isFinite(settingAverage)) {
      historySettingSampleCount += 1;
      if (settingAverage >= 4) {
        historyHighSettingCount += 1;
      }
      if (settingAverage >= 4.5) {
        historyHighSettingEstimateCount += 1;
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
  const recentFourteenRows = historyWindowRows.slice(-14);
  const recentTwoNetTotal = sumDifferenceValues(recentTwoRows);
  const recentThreeNetTotal = sumDifferenceValues(recentThreeRows);
  const recentFourNetTotal = sumDifferenceValues(recentFourRows);
  const recentFiveNetTotal = sumDifferenceValues(recentFiveRows);
  const recentSixNetTotal = sumDifferenceValues(recentSixRows);
  const recentFourteenNetTotal = sumDifferenceValues(recentFourteenRows);
  const recentFourLossDays = recentFourRows.filter((windowRow) => windowRow.differenceValue < 0).length;
  const recentFourPositiveCount = recentFourRows.filter((windowRow) => windowRow.differenceValue > 0).length;
  const recentTwoGamesTotal = recentTwoRows.reduce((total, windowRow) => total + windowRow.games, 0);
  const recentThreeGamesTotal = recentThreeRows.reduce((total, windowRow) => total + windowRow.games, 0);
  const recentFiveGamesTotal = recentFiveRows.reduce((total, windowRow) => total + windowRow.games, 0);
  const recentThreeBonusTotal = recentThreeRows.reduce(
    (total, windowRow) => total + windowRow.bbCount + windowRow.rbCount,
    0,
  );
  const recentTwoRbTotal = recentTwoRows.reduce((total, windowRow) => total + windowRow.rbCount, 0);
  const recentThreeRbTotal = recentThreeRows.reduce((total, windowRow) => total + windowRow.rbCount, 0);
  const recentFiveRbTotal = recentFiveRows.reduce((total, windowRow) => total + windowRow.rbCount, 0);
  const recentTwoSettingAverage = calculateSettingAverageFromWindowRows(recentTwoRows);
  const recentFiveSettingAverage = calculateSettingAverageFromWindowRows(recentFiveRows);
  const windowSettingAverage = calculateSettingAverageFromWindowRows(metricWindowRows);
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
  const isHighSettingEstimateWindowRow = (windowRow) =>
    Boolean(windowRow && Number.isFinite(windowRow.settingAverage) && windowRow.settingAverage >= 4.5);
  const isStrongHighSettingCandidateWindowRow = (windowRow) =>
    Boolean(
      windowRow &&
        Number.isFinite(windowRow.settingAverage) &&
        (windowRow.settingAverage >= 5 || (windowRow.settingAverage >= 4.5 && windowRow.rbCount >= 25)),
    );
  const isSettingFiveWindowRow = (windowRow) =>
    Boolean(windowRow && Number.isFinite(windowRow.settingAverage) && windowRow.settingAverage >= 5);
  const isHistoryHighSettingEstimateWindowRow = (historyWindowRow) => {
    if (!historyWindowRow) {
      return false;
    }
    const settingAverage = getSettingEstimateAverage(settingDefinitionCache, historyWindowRow.row, config).average;
    return Number.isFinite(settingAverage) && settingAverage >= 4.5;
  };
  const isHistoryStrongHighSettingCandidateWindowRow = (historyWindowRow) => {
    if (!historyWindowRow) {
      return false;
    }
    const settingAverage = getSettingEstimateAverage(settingDefinitionCache, historyWindowRow.row, config).average;
    const rbCount = readNumber(historyWindowRow.row?.rb_count) ?? 0;
    return Number.isFinite(settingAverage) && (settingAverage >= 5 || (settingAverage >= 4.5 && rbCount >= 25));
  };
  const isHistoryHighSettingCandidateWindowRow = (historyWindowRow) => {
    if (!historyWindowRow) {
      return false;
    }
    const settingAverage = getSettingEstimateAverage(settingDefinitionCache, historyWindowRow.row, config).average;
    const rbCount = readNumber(historyWindowRow.row?.rb_count) ?? 0;
    return Number.isFinite(settingAverage) && settingAverage >= 4.5 && rbCount >= 25;
  };
  const isHistorySettingFiveWindowRow = (historyWindowRow) => {
    if (!historyWindowRow) {
      return false;
    }
    const settingAverage = getSettingEstimateAverage(settingDefinitionCache, historyWindowRow.row, config).average;
    return Number.isFinite(settingAverage) && settingAverage >= 5;
  };
  const twoDaysAgoHighSettingCandidate = isHighSettingCandidateWindowRow(metricWindowRows.at(-2));
  const threeDaysAgoHighSettingCandidate = isHighSettingCandidateWindowRow(metricWindowRows.at(-3));
  const fourDaysAgoHighSettingCandidate = isHighSettingCandidateWindowRow(metricWindowRows.at(-4));
  const recentFiveHighSettingCandidateCount = recentFiveRows.filter(isHighSettingCandidateWindowRow).length;
  const twoDaysAgoHighSettingEstimate = isHighSettingEstimateWindowRow(metricWindowRows.at(-2));
  const twoDaysAgoSettingFive = isSettingFiveWindowRow(metricWindowRows.at(-2));
  const recentThreeHighSettingEstimateCount = recentThreeRows.filter(isHighSettingEstimateWindowRow).length;
  const recentThreeSettingFiveCount = recentThreeRows.filter(isSettingFiveWindowRow).length;
  const recentFiveHighSettingEstimateCount = recentFiveRows.filter(isHighSettingEstimateWindowRow).length;
  const recentFifteenHighSettingEstimateCount = historyWindowRows
    .slice(-15)
    .filter(isHistoryHighSettingEstimateWindowRow).length;
  const recentFourteenHighSettingCandidateCount = historyWindowRows
    .slice(-14)
    .filter(isHistoryHighSettingCandidateWindowRow).length;
  const historyThirtyRows = historyWindowRows.slice(-30);
  const historyFortyFiveRows = historyWindowRows.slice(-45);
  const historySixtyRows = historyWindowRows.slice(-60);
  const historyTwentyOneRows = historyWindowRows.slice(-21);
  const isHistoryAmuseAsakusaHNormWindowRow = (historyWindowRow) =>
    isAmuseAsakusaNormalizedHighSettingRow(historyWindowRow?.row, settingDefinitionCache, config);
  const historyThirtyAmuseAsakusaHNormCount =
    historyThirtyRows.filter(isHistoryAmuseAsakusaHNormWindowRow).length;
  const amuseAsakusaRbSetting7 = calculateRbSettingEquivalentForTotals(
    normalizeHuntScoreMachineName(row?.machine_name, config),
    gamesTotal,
    rbTotal,
    settingDefinitionCache,
  );
  const historyFortyFiveSettingSampleCount = historyFortyFiveRows.filter((historyWindowRow) => {
    const settingAverage = getSettingEstimateAverage(settingDefinitionCache, historyWindowRow.row, config).average;
    return Number.isFinite(settingAverage);
  }).length;
  const historyTwentyOneHighSettingCandidateCount =
    historyTwentyOneRows.filter(isHistoryHighSettingCandidateWindowRow).length;
  const historyThirtyHighSettingCandidateCount = historyThirtyRows.filter(isHistoryHighSettingCandidateWindowRow).length;
  const historyFortyFiveHighSettingCandidateCount =
    historyFortyFiveRows.filter(isHistoryHighSettingCandidateWindowRow).length;
  const historySixtyHighSettingCandidateCount =
    historySixtyRows.filter(isHistoryHighSettingCandidateWindowRow).length;
  const historyThirtySettingFiveCount = historyThirtyRows.filter(isHistorySettingFiveWindowRow).length;
  const daysSinceHighSettingCandidate = (() => {
    for (let offset = 1; offset <= metricWindowRows.length; offset += 1) {
      const windowRow = metricWindowRows.at(-offset);
      if (isHighSettingCandidateWindowRow(windowRow)) {
        return offset;
      }
    }
    return null;
  })();
  const daysSinceHighSettingEstimate = (() => {
    for (let offset = 1; offset <= metricWindowRows.length; offset += 1) {
      const windowRow = metricWindowRows.at(-offset);
      if (isHighSettingEstimateWindowRow(windowRow)) {
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
  const daysSinceHistoryHighSettingEstimate = (() => {
    for (let offset = 1; offset <= historyWindowRows.length; offset += 1) {
      const historyWindowRow = historyWindowRows.at(-offset);
      if (isHistoryHighSettingEstimateWindowRow(historyWindowRow)) {
        return offset;
      }
    }
    return null;
  })();
  const daysSinceHistoryHighSettingCandidate = (() => {
    for (let offset = 1; offset <= historyWindowRows.length; offset += 1) {
      const historyWindowRow = historyWindowRows.at(-offset);
      if (isHistoryHighSettingCandidateWindowRow(historyWindowRow)) {
        return offset;
      }
    }
    return null;
  })();
  const daysSinceHistoryAmuseAsakusaHNorm = (() => {
    for (let offset = 1; offset <= historyWindowRows.length; offset += 1) {
      const historyWindowRow = historyWindowRows.at(-offset);
      if (isHistoryAmuseAsakusaHNormWindowRow(historyWindowRow)) {
        return offset;
      }
    }
    return null;
  })();
  const daysSinceHistoryStrongHighSettingCandidate = (() => {
    for (let offset = 1; offset <= historyWindowRows.length; offset += 1) {
      const historyWindowRow = historyWindowRows.at(-offset);
      if (isHistoryStrongHighSettingCandidateWindowRow(historyWindowRow)) {
        return offset;
      }
    }
    return null;
  })();
  const daysSinceHistorySettingFive = (() => {
    for (let offset = 1; offset <= historyWindowRows.length; offset += 1) {
      const historyWindowRow = historyWindowRows.at(-offset);
      if (isHistorySettingFiveWindowRow(historyWindowRow)) {
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
  const adjacentHighSettingCandidateCount7 = countAdjacentHighSettingCandidates(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    settingDefinitionCache,
    config,
    windowDays,
  );

  return {
    machineName: normalizeHuntScoreMachineName(row?.machine_name, config),
    slotNumber: String(row?.slot_number ?? "").trim(),
    windowRowCount: metricWindowRows.length,
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
    recentFourteenNetTotal,
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
    settingSampleCount,
    lowSettingCount,
    highSettingCount,
    highSettingEstimateCount,
    highSettingCandidateCount,
    settingFiveCount,
    strongHighSettingCandidateCount,
    recentFiveHighSettingCandidateCount,
    recentFiveHighSettingEstimateCount,
    recentFifteenHighSettingEstimateCount,
    recentFourteenHighSettingCandidateCount,
    twoDaysAgoHighSettingCandidate,
    twoDaysAgoHighSettingEstimate,
    twoDaysAgoSettingFive,
    threeDaysAgoHighSettingCandidate,
    fourDaysAgoHighSettingCandidate,
    daysSinceHighSettingCandidate,
    daysSinceHighSettingEstimate,
    daysSinceSettingFive,
    daysSinceHistoryHighSettingEstimate,
    daysSinceHistoryHighSettingCandidate,
    amuseAsakusaDaysSinceHNorm: normalizeDaysSinceHighSettingEstimateOffset(
      daysSinceHistoryAmuseAsakusaHNorm,
    ),
    daysSinceHistoryStrongHighSettingCandidate,
    daysSinceHistorySettingFive,
    highSettingStreak: calculateCurrentHighSettingStreak(metricWindowRows),
    highSettingEstimateStreak: calculateCurrentHighSettingEstimateStreak(metricWindowRows),
    highSettingCandidateStreak: calculateCurrentHighSettingCandidateStreak(metricWindowRows),
    recentThreeHighSettingCount,
    recentThreeHighSettingEstimateCount,
    recentThreeSettingFiveCount,
    gamesTotal,
    averageGames: metricWindowRows.length > 0 ? gamesTotal / metricWindowRows.length : 0,
    recentTwoGamesTotal,
    recentThreeGamesTotal,
    recentFiveGamesTotal,
    recentThreeBonusTotal,
    recentTwoRbTotal,
    recentThreeRbTotal,
    recentFiveRbTotal,
    recentTwoSettingAverage,
    recentFiveSettingAverage,
    windowSettingAverage,
    historyRowCount,
    historySettingSampleCount,
    historyHighSettingCount,
    historyHighSettingRate:
      historySettingSampleCount > 0 ? historyHighSettingCount / historySettingSampleCount : null,
    historyHighSettingEstimateCount,
    historyHighSettingEstimateRate:
      historySettingSampleCount > 0 ? historyHighSettingEstimateCount / historySettingSampleCount : null,
    historyThirtyHighSettingCandidateCount,
    amuseAsakusaHNormCount30: historyThirtyAmuseAsakusaHNormCount,
    historyFortyFiveHighSettingCandidateCount,
    historySixtyHighSettingCandidateCount,
    historyTwentyOneHighSettingCandidateCount,
    historyFortyFiveSettingSampleCount,
    historyThirtySettingFiveCount,
    adjacentHighSettingCandidateCount7,
    historyNetTotal,
    historyPositiveDays,
    bbTotal,
    rbTotal,
    amuseAsakusaRbSetting7,
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

function buildMachineHighSettingCandidateRateMap(
  businessDates,
  dateIndex,
  rowsByDate,
  settingDefinitionCache,
  config,
  windowDays = MILLION_TOBU_NERIMA_R30_WINDOW_DAYS,
) {
  const normalizedWindowDays = Math.max(1, Number(windowDays) || MILLION_TOBU_NERIMA_R30_WINDOW_DAYS);
  const startIndex = Math.max(0, dateIndex - (normalizedWindowDays - 1));
  const windowDates = businessDates.slice(startIndex, dateIndex + 1);
  if (windowDates.length < normalizedWindowDays) {
    return new Map();
  }

  const machineNames = new Set();
  const highSettingCandidateDateCounts = new Map();
  const useAmuseAsakusaHNorm = config?.logicKey === "amuse-asakusa";

  for (const date of windowDates) {
    const highSettingCandidateMachineNames = new Set();

    for (const row of rowsByDate.get(date) ?? []) {
      if (!hasMeaningfulResult(row)) {
        continue;
      }

      const machineName = normalizeHuntScoreMachineName(row?.machine_name, config);
      if (!machineName) {
        continue;
      }

      machineNames.add(machineName);
      const settingAverage = getSettingEstimateAverage(settingDefinitionCache, row, config).average;
      const rbCount = readNumber(row?.rb_count) ?? 0;
      const isHighSettingCandidate = useAmuseAsakusaHNorm
        ? isAmuseAsakusaNormalizedHighSettingRow(row, settingDefinitionCache, config)
        : Number.isFinite(settingAverage) && settingAverage >= 4.5 && rbCount >= 25;
      if (isHighSettingCandidate) {
        highSettingCandidateMachineNames.add(machineName);
      }
    }

    for (const machineName of highSettingCandidateMachineNames) {
      highSettingCandidateDateCounts.set(
        machineName,
        (highSettingCandidateDateCounts.get(machineName) ?? 0) + 1,
      );
    }
  }

  return new Map(
    [...machineNames].map((machineName) => [
      machineName,
      (highSettingCandidateDateCounts.get(machineName) ?? 0) / windowDates.length,
    ]),
  );
}

function buildMachineActiveSlotCountMap(dateRows, config) {
  const countByName = new Map();

  for (const row of dateRows) {
    if (!hasMeaningfulResult(row)) {
      continue;
    }

    const machineName = normalizeHuntScoreMachineName(row?.machine_name, config);
    if (!machineName) {
      continue;
    }

    countByName.set(machineName, (countByName.get(machineName) ?? 0) + 1);
  }

  return countByName;
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
      rowsByDate,
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
    windowDays: config.windowDays ?? DEFAULT_HUNT_SCORE_WINDOW_DAYS,
    metricsList: validCandidates.map((candidate) => candidate.metrics),
    machineActiveSlotCountByName: buildMachineActiveSlotCountMap(dateRows, config),
    machineHighSettingCandidateRateByName: buildMachineHighSettingCandidateRateMap(
      businessDates,
      dateIndex,
      rowsByDate,
      settingDefinitionCache,
      config,
    ),
  };

  const rows = validCandidates
    .map((candidate) => {
      const rawHuntScore = config.scoreCalculator(candidate.metrics, context);
      const huntScore = roundHuntScore(rawHuntScore);
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
        huntScoreSortValue:
          config.logicKey === "amuse-asakusa" && Number.isFinite(rawHuntScore)
            ? rawHuntScore
            : huntScore,
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
      if (Math.abs(right.huntScoreSortValue - left.huntScoreSortValue) > HUNT_SCORE_EPSILON) {
        return right.huntScoreSortValue - left.huntScoreSortValue;
      }
      const machineComparison = left.machineName.localeCompare(right.machineName, "ja");
      if (machineComparison !== 0) {
        return machineComparison;
      }
      return String(left.slotNumber).localeCompare(String(right.slotNumber), "ja");
    })
    .map(({ huntScoreSortValue, ...row }, index) => ({
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
  options = {},
) {
  const storeConfig = buildEffectiveHuntScoreStoreConfig(
    storeName,
    (Array.isArray(targetRows) ? targetRows : []).map((row) => row?.machine_name),
  );
  if (!storeConfig || !Array.isArray(targetRows) || targetRows.length === 0) {
    return [];
  }
  const config = buildRuntimeHuntScoreConfig(
    storeConfig,
    logicKey,
    differenceMode,
    options?.settingEstimateMode,
  );

  const businessDates = buildBusinessDates(allStoreRows, targetRows);
  if (businessDates.length === 0) {
    return [];
  }

  const businessDateSet = new Set(businessDates);
  const { rowsByCandidateKey, rowsByDate } = buildSourceMaps(targetRows, businessDateSet, config);
  const settingDefinitionCache = new Map();
  const targetDate = String(options?.targetDate ?? "").trim();
  const targetDateRange = options?.targetDateRange ?? null;
  const targetStartDate = String(targetDateRange?.startDate ?? "").trim();
  const targetEndDate = String(targetDateRange?.endDate ?? "").trim();
  const dateIndexes = targetDate
    ? [businessDates.indexOf(targetDate)].filter((dateIndex) => dateIndex >= 0)
    : businessDates
        .map((date, dateIndex) => ({ date, dateIndex }))
        .filter(({ date }) => {
          if (targetStartDate && date < targetStartDate) {
            return false;
          }
          if (targetEndDate && date > targetEndDate) {
            return false;
          }
          return true;
        })
        .map(({ dateIndex }) => dateIndex);

  return dateIndexes
    .map((dateIndex) =>
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

function buildSnapshotDateMap(snapshots) {
  return new Map(
    (Array.isArray(snapshots) ? snapshots : [])
      .map((snapshot) => [String(snapshot?.baseDate ?? "").trim(), snapshot])
      .filter(([baseDate]) => Boolean(baseDate)),
  );
}

function buildSnapshotRowMap(snapshot) {
  return new Map(
    (Array.isArray(snapshot?.rows) ? snapshot.rows : [])
      .map((row) => [String(row?.rowKey ?? "").trim(), row])
      .filter(([rowKey]) => Boolean(rowKey)),
  );
}

function sortCombinedHuntScoreRows(rows) {
  return rows
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
}

export function buildCombinedHuntScoreSnapshots(
  targetRows,
  allStoreRows = [],
  storeName = "",
  logicKeys = [],
  differenceMode = DEFAULT_DIFFERENCE_MODE,
  options = {},
) {
  const logicDetails = getHuntScoreLogicDetails(logicKeys, storeName);
  if (logicDetails.length <= 1) {
    return buildHuntScoreSnapshots(
      targetRows,
      allStoreRows,
      storeName,
      logicDetails[0]?.key ?? "",
      differenceMode,
      options,
    );
  }

  const snapshotGroups = logicDetails.map((logic) =>
    buildHuntScoreSnapshots(
      targetRows,
      allStoreRows,
      storeName,
      logic.key,
      differenceMode,
      options,
    ),
  );
  const snapshotMaps = snapshotGroups.map(buildSnapshotDateMap);
  const baseSnapshots = snapshotGroups[0] ?? [];

  return baseSnapshots
    .map((baseSnapshot) => {
      const baseDate = String(baseSnapshot?.baseDate ?? "").trim();
      const matchingSnapshots = snapshotMaps.map((snapshotMap) => snapshotMap.get(baseDate) ?? null);
      if (!baseDate || matchingSnapshots.some((snapshot) => !snapshot)) {
        return null;
      }

      const rowMaps = matchingSnapshots.map(buildSnapshotRowMap);
      const rows = (Array.isArray(baseSnapshot.rows) ? baseSnapshot.rows : [])
        .map((baseRow) => {
          const rowKey = String(baseRow?.rowKey ?? "").trim();
          const scoreParts = rowMaps.map((rowMap, index) => {
            const partRow = rowMap.get(rowKey);
            const huntScore = Number(partRow?.huntScore);
            if (!Number.isFinite(huntScore)) {
              return null;
            }
            return {
              key: logicDetails[index].key,
              name: logicDetails[index].name,
              huntScore,
            };
          });
          if (!rowKey || scoreParts.some((part) => !part)) {
            return null;
          }

          return {
            ...baseRow,
            huntScore: scoreParts.reduce((total, part) => total + part.huntScore, 0),
            huntScoreParts: scoreParts,
          };
        })
        .filter(Boolean);

      return {
        baseDate,
        nextBusinessDate: baseSnapshot.nextBusinessDate,
        huntScoreLogicSnapshots: matchingSnapshots.map((snapshot, index) => ({
          key: logicDetails[index].key,
          name: logicDetails[index].name,
          rows: Array.isArray(snapshot.rows) ? snapshot.rows : [],
        })),
        rows: sortCombinedHuntScoreRows(rows),
      };
    })
    .filter((snapshot) => snapshot && snapshot.rows.length > 0)
    .sort((left, right) => right.baseDate.localeCompare(left.baseDate));
}

export function listHuntScoreRankingDateOptions(targetRows, allStoreRows = []) {
  const businessDates = buildBusinessDates(allStoreRows, targetRows);
  const businessDateIndexes = new Map(businessDates.map((date, index) => [date, index]));
  const targetDateSet = new Set(
    (Array.isArray(targetRows) ? targetRows : [])
      .filter(hasMeaningfulResult)
      .map((row) => String(row?.target_date ?? "").trim())
      .filter(Boolean),
  );

  return businessDates
    .filter((date) => targetDateSet.has(date))
    .map((date) => {
      const dateIndex = businessDateIndexes.get(date) ?? -1;
      return {
        date,
        nextBusinessDate: dateIndex >= 0 ? businessDates[dateIndex + 1] ?? null : null,
      };
    })
    .sort((left, right) => right.date.localeCompare(left.date));
}

export function attachHuntScores(
  targetRows,
  allStoreRows = [],
  storeName = "",
  logicKey = "",
  differenceMode = DEFAULT_DIFFERENCE_MODE,
  settingEstimateMode = undefined,
) {
  const storeConfig = resolveHuntScoreStoreConfig(storeName);
  if (!storeConfig) {
    return;
  }
  const config = buildRuntimeHuntScoreConfig(
    storeConfig,
    logicKey,
    differenceMode,
    settingEstimateMode,
  );

  const snapshots = buildHuntScoreSnapshots(
    targetRows,
    allStoreRows,
    storeName,
    logicKey,
    differenceMode,
    { settingEstimateMode },
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
