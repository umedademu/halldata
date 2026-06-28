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

const PARLOR_ASAHI_TARGET_MACHINES = [
  {
    name: "SアイムジャグラーＥＸ",
    aliases: ["SアイムジャグラーEX", "アイムジャグラーEX", "アイムジャグラーＥＸ"],
  },
];

const MEGA_BEAM_ASAKURA_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const NAKAGAWA_KING_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const KING2_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const WONDERLAND_1188_TACHIARAI_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
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

const TAMAYA_HONTEN_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const SUPER_HOLLYWOOD_1120_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const CAROL96_TSUBUKU_TARGET_MACHINES = [
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

const MJ_ARENA_KURUME_TARGET_MACHINES = [
  { name: "SアイムジャグラーＥＸ", aliases: ["SアイムジャグラーEX"] },
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
  { name: "ゴーゴージャグラー３", aliases: ["ゴーゴージャグラー3", "ゴーゴージャグラー"] },
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

const SUPER_DSTATION_CHIKUSHINO_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const ESPACE_UENO_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const MESSE_MINAMISENJU_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const MESSE_NISHIKASAI_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const MARUHAN_KOIWA_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const MARUHON_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const GAIA_HIKIFUNE_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const ONE_TWO_THREE_N_SHINONOME_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const RAKUEN_AMEYOKO_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const CONCERT_HALL_KITASENJU_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const KYUDEN_ANNEX_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const JARAN_YAZAIKE_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const NEW_GRAND_HOKIMA_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const NEW_CROWN_AYASE_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const PARK_TAKENOTSUKA_STUDIO_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const PARK_KITASENJU_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const PARK_KITASENJU_SSS_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const PARK_KITAYASE_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const MITOYA_KINSHICHO_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const MITOYA_KINSHICHO_SOUTH_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const MITOYA_JACKPOT_KINSHICHO_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const MITOYA_ASAKUSA_SENZOKU_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const EX_ARENA_TOKYO_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const KINTOKI_KAMATA_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const IIDABASHI_PRESAS_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const YASUDA_HIBARIGAOKA_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const MINOWA_UNO_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const TOYO_HALL_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
];

const GRAND_SHIP_TARGET_MACHINES = [
  { name: "ネオアイムジャグラーEX", aliases: ["ネオアイムジャグラーＥＸ"] },
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
    key: "parlor-asahi",
    storeNames: ["パーラーアサヒ", "パーラーアサヒ店", "PARLOR ASAHI", "PARLORASAHI", "Parlor Asahi"],
    targetMachines: PARLOR_ASAHI_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "SアイムジャグラーＥＸ": "parlor-asahi-aim",
      "SアイムジャグラーEX": "parlor-asahi-aim",
      "アイムジャグラーEX": "parlor-asahi-aim",
      "アイムジャグラーＥＸ": "parlor-asahi-aim",
    },
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
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "million-tobu-nerima-neo-aim",
      "ネオアイムジャグラーＥＸ": "million-tobu-nerima-neo-aim",
    },
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
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "amuse-asakusa-neo-aim",
      "ネオアイムジャグラーＥＸ": "amuse-asakusa-neo-aim",
    },
  },
  {
    key: "apark-yakatabaru",
    storeNames: ["A-PARK屋形原", "A-PARK屋形原店", "Aパーク屋形原", "Aパーク屋形原店"],
    targetMachines: APARK_YAKATABARU_TARGET_MACHINES,
    defaultLogicKey: "apark-yakatabaru-a",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "apark-yakatabaru-neo-aim",
      "ネオアイムジャグラーＥＸ": "apark-yakatabaru-neo-aim",
      "マイジャグラーV": "apark-yakatabaru-my",
      "マイジャグラーⅤ": "apark-yakatabaru-my",
      "ファンキージャグラー２ＫＴ": "apark-yakatabaru-funky",
      "ファンキージャグラー２": "apark-yakatabaru-funky",
      "ファンキージャグラー2": "apark-yakatabaru-funky",
      "ハッピージャグラーＶＩＩＩ": "apark-yakatabaru-happy",
      "ハッピージャグラーVIII": "apark-yakatabaru-happy",
      "ハッピージャグラーＶ": "apark-yakatabaru-happy",
      "ハッピージャグラーV": "apark-yakatabaru-happy",
      "ハッピージャグラー": "apark-yakatabaru-happy",
      "ウルトラミラクルジャグラー": "apark-yakatabaru-ultra-miracle",
    },
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
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "hinode-onojo-neo-aim",
      "ネオアイムジャグラーＥＸ": "hinode-onojo-neo-aim",
    },
  },
  {
    key: "super-dstation-chikushino",
    storeNames: [
      "スーパーDステーション39筑紫野店",
      "スーパーDステーション筑紫野店",
      "スーパーＤステーション３９筑紫野店",
      "スーパーＤステーション筑紫野店",
      "スーパーＤ’ステーション３９筑紫野店",
    ],
    targetMachines: SUPER_DSTATION_CHIKUSHINO_TARGET_MACHINES,
    defaultLogicKey: "apark",
    resetHistoryGapDays: 7,
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "chikushino-neo-aim",
      "ネオアイムジャグラーＥＸ": "chikushino-neo-aim",
    },
  },
  {
    key: "espace-ueno",
    storeNames: [
      "エスパス日拓上野本館",
      "エスパス日拓上野本館店",
      "エスパス上野本館",
      "エスパス上野本館店",
    ],
    targetMachines: ESPACE_UENO_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "espace-ueno-neo-aim",
      "ネオアイムジャグラーＥＸ": "espace-ueno-neo-aim",
    },
  },
  {
    key: "messe-minamisenju",
    storeNames: ["メッセ南千住店", "メッセ南千住"],
    targetMachines: MESSE_MINAMISENJU_TARGET_MACHINES,
    defaultLogicKey: "apark",
    resetHistoryDates: ["2026-06-01"],
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "messe-minamisenju-neo-aim",
      "ネオアイムジャグラーＥＸ": "messe-minamisenju-neo-aim",
    },
  },
  {
    key: "messe-nishikasai",
    storeNames: ["メッセ西葛西店", "メッセ西葛西", "メッセ西葛西駅前店", "メッセ西葛西駅前"],
    targetMachines: MESSE_NISHIKASAI_TARGET_MACHINES,
    defaultLogicKey: "apark",
    resetHistoryDates: ["2026-03-30"],
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "messe-nishikasai-neo-aim",
      "ネオアイムジャグラーＥＸ": "messe-nishikasai-neo-aim",
    },
  },
  {
    key: "maruhan-koiwa",
    storeNames: ["マルハン小岩店", "マルハン小岩"],
    targetMachines: MARUHAN_KOIWA_TARGET_MACHINES,
    defaultLogicKey: "apark",
    slotHistoryStartDates: [
      {
        machineName: "ネオアイムジャグラーEX",
        slotNumbers: ["131", "132"],
        startDate: "2026-04-06",
      },
    ],
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "maruhan-koiwa-neo-aim",
      "ネオアイムジャグラーＥＸ": "maruhan-koiwa-neo-aim",
    },
  },
  {
    key: "maruhon",
    storeNames: ["マルホン", "マルホン店", "MARUHON", "MARUHON店", "Maruhon"],
    targetMachines: MARUHON_TARGET_MACHINES,
    defaultLogicKey: "apark",
    excludedRows: [
      {
        targetDate: "2026-06-09",
        slotNumber: "635",
        machineName: "ネオアイムジャグラーEX",
      },
    ],
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "maruhon-neo-aim",
      "ネオアイムジャグラーＥＸ": "maruhon-neo-aim",
    },
  },
  {
    key: "gaia-hikifune",
    storeNames: ["ガイア曳舟", "ガイア曳舟店", "GAIA曳舟", "GAIA曳舟店", "ＧＡＩＡ曳舟", "ＧＡＩＡ曳舟店"],
    targetMachines: GAIA_HIKIFUNE_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "gaia-hikifune-neo-aim",
      "ネオアイムジャグラーＥＸ": "gaia-hikifune-neo-aim",
    },
  },
  {
    key: "123n-shinonome",
    storeNames: [
      "123+N東雲店",
      "123+N東雲",
      "123N東雲店",
      "123N東雲",
      "１２３＋Ｎ東雲店",
      "１２３＋Ｎ東雲",
      "123＋N東雲店",
      "123＋N東雲",
    ],
    targetMachines: ONE_TWO_THREE_N_SHINONOME_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "123n-shinonome-neo-aim",
      "ネオアイムジャグラーＥＸ": "123n-shinonome-neo-aim",
    },
  },
  {
    key: "rakuen-ameyoko",
    storeNames: [
      "楽園アメ横店",
      "楽園アメ横",
      "楽園アメヤ横丁店",
      "楽園アメヤ横丁",
      "RAKUENアメ横店",
      "RAKUENアメ横",
      "らくえんアメ横店",
      "らくえんアメ横",
    ],
    targetMachines: RAKUEN_AMEYOKO_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "rakuen-ameyoko-neo-aim",
      "ネオアイムジャグラーＥＸ": "rakuen-ameyoko-neo-aim",
    },
  },
  {
    key: "concert-hall-kitasenju",
    storeNames: ["コンサートホール北千住", "コンサートホール北千住店", "コンサートホール北千住駅前店"],
    targetMachines: CONCERT_HALL_KITASENJU_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "concert-hall-kitasenju-neo-aim",
      "ネオアイムジャグラーＥＸ": "concert-hall-kitasenju-neo-aim",
    },
  },
  {
    key: "kyuden-annex",
    storeNames: [
      "キューデン・アネックス",
      "キューデン・アネックス店",
      "キューデンアネックス",
      "キューデンアネックス店",
      "KYUDEN ANNEX",
    ],
    targetMachines: KYUDEN_ANNEX_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "kyuden-annex-neo-aim",
      "ネオアイムジャグラーＥＸ": "kyuden-annex-neo-aim",
    },
  },
  {
    key: "jaran-yazaike",
    storeNames: ["ジャラン谷在家店", "ジャラン谷在家", "JARAN谷在家店", "JARAN谷在家", "ＪＡＲＡＮ谷在家店"],
    targetMachines: JARAN_YAZAIKE_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "jaran-yazaike-neo-aim",
      "ネオアイムジャグラーＥＸ": "jaran-yazaike-neo-aim",
    },
  },
  {
    key: "new-grand-hokima",
    storeNames: ["ニューグランド保木間店", "ニューグランド保木間", "NEW GRAND保木間店", "NEW GRAND保木間"],
    targetMachines: NEW_GRAND_HOKIMA_TARGET_MACHINES,
    defaultLogicKey: "apark",
    excludedRows: [
      {
        targetDate: "2026-02-08",
        slotNumber: "355",
        machineName: "ネオアイムジャグラーEX",
      },
    ],
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "new-grand-hokima-neo-aim",
      "ネオアイムジャグラーＥＸ": "new-grand-hokima-neo-aim",
    },
  },
  {
    key: "new-crown-ayase",
    storeNames: ["ニュークラウン綾瀬店", "ニュークラウン綾瀬", "NEW CROWN綾瀬店", "NEW CROWN綾瀬"],
    targetMachines: NEW_CROWN_AYASE_TARGET_MACHINES,
    defaultLogicKey: "apark",
    slotHistoryStartDates: [
      {
        slotNumberMin: 737,
        slotNumberMax: 763,
        startDate: "2026-03-16",
        machineName: "ネオアイムジャグラーEX",
      },
    ],
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "new-crown-ayase-neo-aim",
      "ネオアイムジャグラーＥＸ": "new-crown-ayase-neo-aim",
    },
  },
  {
    key: "park-takenotsuka-studio",
    storeNames: [
      "ピーアーク竹ノ塚スタジオ",
      "ピーアーク竹の塚スタジオ",
      "ピーアーク竹ノ塚スタジオ店",
      "ピーアーク竹の塚スタジオ店",
      "P-ARK竹ノ塚スタジオ",
      "P ARK竹ノ塚スタジオ",
    ],
    targetMachines: PARK_TAKENOTSUKA_STUDIO_TARGET_MACHINES,
    defaultLogicKey: "apark",
    excludedRows: [
      {
        targetDate: "2026-02-07",
        machineName: "ネオアイムジャグラーEX",
      },
    ],
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "park-takenotsuka-studio-neo-aim",
      "ネオアイムジャグラーＥＸ": "park-takenotsuka-studio-neo-aim",
    },
  },
  {
    key: "park-kitasenju",
    storeNames: [
      "ピーアーク北千住",
      "ピーアーク北千住店",
      "P-ARK北千住",
      "PARK北千住",
      "P ARK北千住",
    ],
    targetMachines: PARK_KITASENJU_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "park-kitasenju-neo-aim",
      "ネオアイムジャグラーＥＸ": "park-kitasenju-neo-aim",
    },
  },
  {
    key: "park-kitasenju-sss",
    storeNames: [
      "ピーアーク北千住SSS",
      "ピーアーク北千住ＳＳＳ",
      "ピーアーク北千住SSS店",
      "ピーアーク北千住ＳＳＳ店",
      "P-ARK北千住SSS",
      "PARK北千住SSS",
      "P ARK北千住SSS",
    ],
    targetMachines: PARK_KITASENJU_SSS_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "park-kitasenju-sss-neo-aim",
      "ネオアイムジャグラーＥＸ": "park-kitasenju-sss-neo-aim",
    },
  },
  {
    key: "park-kitayase",
    storeNames: [
      "ピーアーク北綾瀬駅前",
      "ピーアーク北綾瀬駅前店",
      "P-ARK北綾瀬駅前",
      "PARK北綾瀬駅前",
      "P ARK北綾瀬駅前",
    ],
    targetMachines: PARK_KITAYASE_TARGET_MACHINES,
    defaultLogicKey: "apark",
    resetHistoryGapDays: 7,
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "park-kitayase-neo-aim",
      "ネオアイムジャグラーＥＸ": "park-kitayase-neo-aim",
    },
  },
  {
    key: "mitoya-kinshicho",
    storeNames: [
      "みとや錦糸町北口店",
      "みとや錦糸町北口",
      "MITOYA錦糸町北口店",
      "MITOYA錦糸町北口",
    ],
    targetMachines: MITOYA_KINSHICHO_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "mitoya-kinshicho-neo-aim",
      "ネオアイムジャグラーＥＸ": "mitoya-kinshicho-neo-aim",
    },
  },
  {
    key: "mitoya-kinshicho-south",
    storeNames: [
      "みとや錦糸町南口店",
      "みとや錦糸町南口",
      "MITOYA錦糸町南口店",
      "MITOYA錦糸町南口",
    ],
    targetMachines: MITOYA_KINSHICHO_SOUTH_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "mitoya-kinshicho-south-neo-aim",
      "ネオアイムジャグラーＥＸ": "mitoya-kinshicho-south-neo-aim",
    },
  },
  {
    key: "mitoya-jackpot-kinshicho",
    storeNames: [
      "みとやジャックポット錦糸町店",
      "みとやジャックポット錦糸町",
      "MITOYAジャックポット錦糸町店",
      "MITOYAジャックポット錦糸町",
      "みとやJACKPOT錦糸町店",
      "みとやJACKPOT錦糸町",
      "ジャックポット錦糸町店",
      "ジャックポット錦糸町",
    ],
    targetMachines: MITOYA_JACKPOT_KINSHICHO_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "mitoya-jackpot-kinshicho-neo-aim",
      "ネオアイムジャグラーＥＸ": "mitoya-jackpot-kinshicho-neo-aim",
    },
  },
  {
    key: "mitoya-asakusa-senzoku",
    storeNames: [
      "みとや浅草千束店",
      "みとや浅草千束",
      "MITOYA浅草千束店",
      "MITOYA浅草千束",
      "ＭＩＴＯＹＡ浅草千束店",
      "ＭＩＴＯＹＡ浅草千束",
      "みとや千束店",
      "みとや千束",
    ],
    targetMachines: MITOYA_ASAKUSA_SENZOKU_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "mitoya-asakusa-senzoku-neo-aim",
      "ネオアイムジャグラーＥＸ": "mitoya-asakusa-senzoku-neo-aim",
    },
  },
  {
    key: "ex-arena-tokyo",
    storeNames: [
      "エクスアリーナ東京",
      "エクスアリーナ東京店",
      "EXアリーナ東京",
      "EXアリーナ東京店",
      "EXARENA東京",
      "EX-ARENA東京",
    ],
    targetMachines: EX_ARENA_TOKYO_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "ex-arena-tokyo-neo-aim",
      "ネオアイムジャグラーＥＸ": "ex-arena-tokyo-neo-aim",
    },
  },
  {
    key: "kintoki-kamata",
    storeNames: ["金時蒲田東口店", "金時蒲田東口"],
    targetMachines: KINTOKI_KAMATA_TARGET_MACHINES,
    defaultLogicKey: "apark",
    adjacentSameMachineMode: "slot-number-gap",
    adjacentSlotNumberGroups: [
      [651, 664],
      [703, 708],
    ],
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "kintoki-kamata-neo-aim",
      "ネオアイムジャグラーＥＸ": "kintoki-kamata-neo-aim",
    },
  },
  {
    key: "iidabashi-presas",
    storeNames: ["飯田橋プレサス", "飯田橋プレサス店", "プレサス飯田橋", "プレサス飯田橋店"],
    targetMachines: IIDABASHI_PRESAS_TARGET_MACHINES,
    defaultLogicKey: "apark",
    adjacentSameMachineMode: "slot-number-gap",
    adjacentSlotNumberGroups: [
      [164, 168],
      [193, 197],
    ],
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "iidabashi-presas-neo-aim",
      "ネオアイムジャグラーＥＸ": "iidabashi-presas-neo-aim",
    },
  },
  {
    key: "yasuda-hibarigaoka",
    storeNames: [
      "やすだひばりヶ丘店",
      "やすだひばりヶ丘",
      "やすだひばりケ丘店",
      "やすだひばりケ丘",
      "やすだひばりが丘店",
      "やすだひばりが丘",
    ],
    targetMachines: YASUDA_HIBARIGAOKA_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "yasuda-hibarigaoka-neo-aim",
      "ネオアイムジャグラーＥＸ": "yasuda-hibarigaoka-neo-aim",
    },
  },
  {
    key: "sengawa-uno",
    storeNames: ["仙川UNO", "仙川UNO店", "仙川ＵＮＯ", "仙川ＵＮＯ店"],
    targetMachines: APARK_KASUGA_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "sengawa-uno-neo-aim",
      "ネオアイムジャグラーＥＸ": "sengawa-uno-neo-aim",
    },
  },
  {
    key: "minowa-uno",
    storeNames: [
      "三ノ輪UNO",
      "三ノ輪UNO店",
      "三ノ輪ＵＮＯ",
      "三ノ輪ＵＮＯ店",
      "三ノ輪ウノ",
      "三ノ輪ウノ店",
      "MINOWA UNO",
      "Minowa UNO",
    ],
    targetMachines: MINOWA_UNO_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "minowa-uno-neo-aim",
      "ネオアイムジャグラーＥＸ": "minowa-uno-neo-aim",
    },
  },
  {
    key: "toyo-hall",
    storeNames: [
      "TOYO HALL",
      "TOYOHALL",
      "TOYO HALL店",
      "TOYOHALL店",
      "ＴＯＹＯ ＨＡＬＬ",
      "ＴＯＹＯＨＡＬＬ",
      "トーヨーホール",
      "トーヨーホール店",
      "東洋ホール",
      "東洋ホール店",
    ],
    targetMachines: TOYO_HALL_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "toyo-hall-neo-aim",
      "ネオアイムジャグラーＥＸ": "toyo-hall-neo-aim",
    },
  },
  {
    key: "grandship",
    storeNames: [
      "グランドシップ",
      "グランドシップ店",
      "GRAND SHIP",
      "GRANDSHIP",
      "Grand Ship",
      "GrandShip",
      "グランド シップ",
    ],
    targetMachines: GRAND_SHIP_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "grandship-neo-aim",
      "ネオアイムジャグラーＥＸ": "grandship-neo-aim",
    },
  },
  {
    key: "tamaya-ohashi",
    storeNames: ["玉屋555大橋店", "玉屋555大橋", "玉屋５５５大橋店", "玉屋５５５大橋"],
    targetMachines: TAMAYA_OHASHI_TARGET_MACHINES,
    defaultLogicKey: "tamaya-ohashi",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "tamaya-ohashi-neo-aim",
      "ネオアイムジャグラーＥＸ": "tamaya-ohashi-neo-aim",
    },
  },
  {
    key: "tamaya-honten",
    storeNames: ["玉屋本店", "玉屋本店店", "TAMAYA本店", "ＴＡＭＡＹＡ本店"],
    targetMachines: TAMAYA_HONTEN_TARGET_MACHINES,
    defaultLogicKey: "apark",
    resetHistoryDates: ["2026-03-18"],
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "tamaya-honten-neo-aim",
      "ネオアイムジャグラーＥＸ": "tamaya-honten-neo-aim",
    },
  },
  {
    key: "super-hollywood-1120",
    storeNames: [
      "スーパーハリウッド1120",
      "スーパーハリウッド1120店",
      "スーパーハリウッド１１２０",
      "スーパーハリウッド１１２０店",
      "SUPER HOLLYWOOD1120",
      "SUPERHOLLYWOOD1120",
      "ＳＵＰＥＲＨＯＬＬＹＷＯＯＤ１１２０",
    ],
    targetMachines: SUPER_HOLLYWOOD_1120_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "super-hollywood-1120-neo-aim",
      "ネオアイムジャグラーＥＸ": "super-hollywood-1120-neo-aim",
    },
  },
  {
    key: "carol96-tsubuku",
    storeNames: [
      "キャロル96津福本店",
      "キャロル96津福",
      "キャロル９６津福本店",
      "キャロル９６津福",
      "CAROL96津福本店",
      "ＣＡＲＯＬ９６津福本店",
    ],
    targetMachines: CAROL96_TSUBUKU_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "carol96-tsubuku-neo-aim",
      "ネオアイムジャグラーＥＸ": "carol96-tsubuku-neo-aim",
    },
  },
  {
    key: "123-hakata",
    storeNames: ["123博多店"],
    targetMachines: HAKATA_123_TARGET_MACHINES,
    defaultLogicKey: "123-hakata",
  },
  {
    key: "boom-tenjin",
    storeNames: [
      "BOOM天神本店",
      "BOOM天神店",
      "BOOM天神",
      "ＢＯＯＭ天神本店",
      "ＢＯＯＭ天神店",
      "ＢＯＯＭ天神",
    ],
    targetMachines: BOOM_TENJIN_TARGET_MACHINES,
    defaultLogicKey: "boom-tenjin",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "boom-tenjin-neo-aim",
      "ネオアイムジャグラーＥＸ": "boom-tenjin-neo-aim",
    },
  },
  {
    key: "beam-hikari",
    storeNames: ["ビームヒカリ店", "ビームヒカリ", "BEAM HIKARI", "BEAMHIKARI", "ＢＥＡＭヒカリ店"],
    targetMachines: BEAM_HIKARI_TARGET_MACHINES,
    defaultLogicKey: "beam-hikari-a",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "beam-hikari-neo-aim-content",
      "ネオアイムジャグラーＥＸ": "beam-hikari-neo-aim-content",
      "ファンキージャグラー２ＫＴ": "beam-hikari-funky-content",
      "ファンキージャグラー２": "beam-hikari-funky-content",
      "ファンキージャグラー2": "beam-hikari-funky-content",
      "ゴーゴージャグラー３": "beam-hikari-gogo-content",
      "ゴーゴージャグラー3": "beam-hikari-gogo-content",
      "ゴーゴージャグラー": "beam-hikari-gogo-content",
      "マイジャグラーV": "beam-hikari-my-content",
      "マイジャグラーⅤ": "beam-hikari-my-content",
      "マイジャグラー": "beam-hikari-my-content",
      "ジャグラーガールズSS": "beam-hikari-girls-content",
      "ジャグラーガールズ": "beam-hikari-girls-content",
      "ハッピージャグラーＶＩＩＩ": "beam-hikari-happy-content",
      "ハッピージャグラーVIII": "beam-hikari-happy-content",
      "ハッピージャグラーＶ": "beam-hikari-happy-content",
      "ハッピージャグラーV": "beam-hikari-happy-content",
      "ハッピージャグラー": "beam-hikari-happy-content",
      "ウルトラミラクルジャグラー": "beam-hikari-ultra-miracle-content",
      "Lスマスロ北斗の拳": "beam-hikari-hokuto-base-content",
      "L スマスロ北斗の拳": "beam-hikari-hokuto-base-content",
      "スマスロ北斗の拳": "beam-hikari-hokuto-base-content",
    },
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
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "mj-arena-airport-neo-aim",
      "ネオアイムジャグラーＥＸ": "mj-arena-airport-neo-aim",
    },
  },
  {
    key: "mega-beam-asakura",
    storeNames: ["メガビーム朝倉999", "メガビーム朝倉999店", "メガビーム朝倉９９９", "メガビーム朝倉９９９店"],
    targetMachines: MEGA_BEAM_ASAKURA_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "mega-beam-asakura-neo-aim",
      "ネオアイムジャグラーＥＸ": "mega-beam-asakura-neo-aim",
    },
  },
  {
    key: "nakagawa-king",
    storeNames: ["那珂川キング本店", "那珂川キング", "キング本店", "キング本店那珂川"],
    targetMachines: NAKAGAWA_KING_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "nakagawa-king-neo-aim",
      "ネオアイムジャグラーＥＸ": "nakagawa-king-neo-aim",
    },
  },
  {
    key: "king2",
    storeNames: ["キング2", "キング２", "キング2店", "キング２店"],
    targetMachines: KING2_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "king2-neo-aim",
      "ネオアイムジャグラーＥＸ": "king2-neo-aim",
    },
  },
  {
    key: "wonderland-1188-tachiarai",
    storeNames: [
      "ワンダーランド1188大刀洗店",
      "ワンダーランド1188大刀洗",
      "ワンダーランド１１８８大刀洗店",
      "ワンダーランド１１８８大刀洗",
    ],
    targetMachines: WONDERLAND_1188_TACHIARAI_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "wonderland-1188-tachiarai-neo-aim",
      "ネオアイムジャグラーＥＸ": "wonderland-1188-tachiarai-neo-aim",
    },
  },
  {
    key: "mj-arena-kurume",
    storeNames: ["MJアリーナ久留米店", "MJアリーナ久留米", "ＭＪアリーナ久留米店", "ＭＪアリーナ久留米"],
    targetMachines: MJ_ARENA_KURUME_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "mj-arena-kurume-neo-aim",
      "SアイムジャグラーＥＸ": "mj-arena-kurume-aim",
      "SアイムジャグラーEX": "mj-arena-kurume-aim",
      "ゴーゴージャグラー３": "mj-arena-kurume-gogo",
      "ゴーゴージャグラー3": "mj-arena-kurume-gogo",
      "ゴーゴージャグラー": "mj-arena-kurume-gogo",
      "ファンキージャグラー２ＫＴ": "mj-arena-kurume-funky",
      "マイジャグラーV": "mj-arena-kurume-my",
      "マイジャグラーⅤ": "mj-arena-kurume-my",
      "マイジャグラー": "mj-arena-kurume-my",
      "ジャグラーガールズSS": "mj-arena-kurume-girls",
      "ジャグラーガールズ": "mj-arena-kurume-girls",
    },
  },
  {
    key: "slot-marumitsu-ohashi",
    storeNames: ["スロットまるみつ大橋店", "スロットまるみつ大橋", "まるみつ大橋店", "まるみつ大橋"],
    targetMachines: SLOT_MARUMITSU_OHASHI_TARGET_MACHINES,
    defaultLogicKey: "slot-marumitsu-ohashi-a",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "slot-marumitsu-ohashi-neo-aim",
      "ネオアイムジャグラーＥＸ": "slot-marumitsu-ohashi-neo-aim",
    },
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
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "wonderland-minamigaoka-neo-aim",
      "ネオアイムジャグラーＥＸ": "wonderland-minamigaoka-neo-aim",
    },
  },
  {
    key: "wonderland-sue",
    storeNames: ["ワンダーランド須恵店", "ワンダーランド須恵"],
    targetMachines: WONDERLAND_SUE_TARGET_MACHINES,
    defaultLogicKey: "wonderland-sue-a",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "wonderland-sue-neo-aim",
      "ネオアイムジャグラーＥＸ": "wonderland-sue-neo-aim",
    },
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
    key: "plaza-tenjin",
    storeNames: ["プラザ天神", "プラザ天神店", "PLAZA天神", "ＰＬＡＺＡ天神"],
    targetMachines: APARK_KASUGA_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "plaza-tenjin-neo-aim",
      "ネオアイムジャグラーＥＸ": "plaza-tenjin-neo-aim",
    },
  },
  {
    key: "plaza-honten",
    storeNames: ["プラザ本店", "プラザ本店店", "PLAZA本店", "ＰＬＡＺＡ本店"],
    targetMachines: APARK_KASUGA_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "plaza-honten-neo-aim",
      "ネオアイムジャグラーＥＸ": "plaza-honten-neo-aim",
    },
  },
  {
    key: "plaza-honten-ii",
    storeNames: [
      "プラザ本店II",
      "プラザ本店II店",
      "プラザ本店Ⅱ",
      "プラザ本店Ⅱ店",
      "プラザ本店2",
      "プラザ本店2店",
      "PLAZA本店II",
      "ＰＬＡＺＡ本店Ⅱ",
    ],
    targetMachines: APARK_KASUGA_TARGET_MACHINES,
    defaultLogicKey: "apark",
    resetHistoryDates: ["2025-12-16", "2026-01-07", "2026-04-17"],
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "plaza-honten-ii-neo-aim",
      "ネオアイムジャグラーＥＸ": "plaza-honten-ii-neo-aim",
    },
  },
  {
    key: "plaza3",
    storeNames: ["プラザ3", "プラザ３", "PLAZA3", "ＰＬＡＺＡ３"],
    targetMachines: APARK_KASUGA_TARGET_MACHINES,
    defaultLogicKey: "apark",
    machineHighContentRules: {
      "ネオアイムジャグラーEX": "plaza3-neo-aim",
      "ネオアイムジャグラーＥＸ": "plaza3-neo-aim",
    },
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

function buildTargetMachineNameLookup(targetMachines = []) {
  const lookup = new Map();
  for (const targetMachine of Array.isArray(targetMachines) ? targetMachines : []) {
    const targetName = String(targetMachine?.name ?? "").trim();
    if (!targetName) {
      continue;
    }
    for (const candidateName of listHuntScoreTargetMachineNameCandidates(targetMachine)) {
      const normalizedCandidateName = normalizeText(candidateName);
      if (normalizedCandidateName && !lookup.has(normalizedCandidateName)) {
        lookup.set(normalizedCandidateName, targetName);
      }
    }
  }
  return lookup;
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
  runtimeOptions = {},
) {
  const logicDefinition =
    findHuntScoreLogicDefinition(normalizeHuntScoreLogicKey(logicKey, config?.storeNames?.[0] ?? "")) ??
    findHuntScoreLogicDefinition(DEFAULT_HUNT_SCORE_LOGIC_KEY);
  const baseHistoryWindowDays =
    logicDefinition.historyWindowDays ?? logicDefinition.windowDays ?? DEFAULT_HUNT_SCORE_WINDOW_DAYS;
  const requestedHistoryWindowDays = Number(runtimeOptions?.historyWindowDays);
  return {
    ...config,
    logicKey: logicDefinition.key,
    logicName: logicDefinition.name,
    targetMachineNameLookup: buildTargetMachineNameLookup(config?.targetMachines),
    windowDays: logicDefinition.windowDays ?? DEFAULT_HUNT_SCORE_WINDOW_DAYS,
    historyWindowDays: Number.isFinite(requestedHistoryWindowDays)
      ? Math.max(baseHistoryWindowDays, requestedHistoryWindowDays)
      : baseHistoryWindowDays,
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
  const normalizedMachineName = normalizeText(machineName);
  if (!normalizedMachineName) {
    return "";
  }
  return config?.targetMachineNameLookup?.get(normalizedMachineName) ?? normalizedMachineName;
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

function readRawHuntScoreDifferenceValue(row) {
  return readNumber(row?.difference_value);
}

function readWindowRowDateTime(windowRow) {
  const dateText = String(windowRow?.row?.target_date ?? windowRow?.target_date ?? "").trim();
  if (!dateText) {
    return null;
  }
  const time = Date.parse(dateText);
  return Number.isFinite(time) ? time : null;
}

function filterWindowRowsAfterLargeDateGap(windowRows, resetHistoryGapDays) {
  const normalizedGapDays = Number(resetHistoryGapDays);
  if (!Array.isArray(windowRows) || !Number.isFinite(normalizedGapDays) || normalizedGapDays <= 0) {
    return windowRows;
  }

  let startIndex = 0;
  for (let index = 1; index < windowRows.length; index += 1) {
    const previousTime = readWindowRowDateTime(windowRows[index - 1]);
    const currentTime = readWindowRowDateTime(windowRows[index]);
    if (!Number.isFinite(previousTime) || !Number.isFinite(currentTime)) {
      continue;
    }
    const gapDays = (currentTime - previousTime) / 86400000;
    if (gapDays > normalizedGapDays) {
      startIndex = index;
    }
  }

  return startIndex > 0 ? windowRows.slice(startIndex) : windowRows;
}

function filterWindowRowsAfterResetHistoryDates(windowRows, resetHistoryDates) {
  if (!Array.isArray(windowRows) || !Array.isArray(resetHistoryDates) || resetHistoryDates.length === 0) {
    return windowRows;
  }

  const resetTimes = resetHistoryDates
    .map((dateText) => Date.parse(String(dateText ?? "").trim()))
    .filter(Number.isFinite)
    .sort((a, b) => a - b);
  if (resetTimes.length === 0) {
    return windowRows;
  }

  let startIndex = 0;
  for (let index = 0; index < windowRows.length; index += 1) {
    const currentTime = readWindowRowDateTime(windowRows[index]);
    if (!Number.isFinite(currentTime)) {
      continue;
    }
    const previousTime = index > 0 ? readWindowRowDateTime(windowRows[index - 1]) : null;
    if (resetTimes.some((resetTime) => currentTime >= resetTime && (!Number.isFinite(previousTime) || previousTime < resetTime))) {
      startIndex = index;
    }
  }

  return startIndex > 0 ? windowRows.slice(startIndex) : windowRows;
}

function matchesSlotHistoryStartDateRule(windowRows, rule, config = {}) {
  const referenceRow = windowRows?.at(-1)?.row ?? windowRows?.[0]?.row ?? null;
  const slotNumber = Number(referenceRow?.slot_number);
  const slotText = String(referenceRow?.slot_number ?? "").trim();
  const machineName = normalizeHuntScoreMachineName(referenceRow?.machine_name, config);
  const ruleMachineName = String(rule?.machineName ?? "").trim();
  if (ruleMachineName && normalizeText(ruleMachineName) !== normalizeText(machineName)) {
    return false;
  }

  const slotNumbers = Array.isArray(rule?.slotNumbers) ? rule.slotNumbers.map((value) => String(value).trim()) : [];
  if (slotNumbers.length > 0) {
    return slotNumbers.includes(slotText);
  }

  const minSlotNumber = Number(rule?.slotNumberMin);
  const maxSlotNumber = Number(rule?.slotNumberMax);
  if (Number.isFinite(minSlotNumber) && Number.isFinite(maxSlotNumber)) {
    return Number.isFinite(slotNumber) && slotNumber >= minSlotNumber && slotNumber <= maxSlotNumber;
  }

  return false;
}

function filterWindowRowsAfterSlotHistoryStartDates(windowRows, slotHistoryStartDates, config = {}) {
  if (
    !Array.isArray(windowRows) ||
    !Array.isArray(slotHistoryStartDates) ||
    slotHistoryStartDates.length === 0
  ) {
    return windowRows;
  }

  const matchingStartTimes = slotHistoryStartDates
    .filter((rule) => matchesSlotHistoryStartDateRule(windowRows, rule, config))
    .map((rule) => Date.parse(String(rule?.startDate ?? "").trim()))
    .filter(Number.isFinite)
    .sort((a, b) => a - b);
  if (matchingStartTimes.length === 0) {
    return windowRows;
  }

  const latestStartTime = matchingStartTimes.at(-1);
  return windowRows.filter((windowRow) => {
    const currentTime = readWindowRowDateTime(windowRow);
    return Number.isFinite(currentTime) && currentTime >= latestStartTime;
  });
}

function filterWindowRowsForHistoryReset(windowRows, config = {}) {
  return filterWindowRowsAfterSlotHistoryStartDates(
    filterWindowRowsAfterResetHistoryDates(
      filterWindowRowsAfterLargeDateGap(windowRows, config?.resetHistoryGapDays),
      config?.resetHistoryDates,
    ),
    config?.slotHistoryStartDates,
    config,
  );
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

function isExcludedHuntScoreRow(row, config = {}) {
  const excludedRows = Array.isArray(config?.excludedRows) ? config.excludedRows : [];
  if (excludedRows.length === 0) {
    return false;
  }

  const rowDate = String(row?.target_date ?? "").trim();
  const rowSlotNumber = String(row?.slot_number ?? "").trim();
  const rowMachineName = normalizeHuntScoreMachineName(row?.machine_name, config);
  return excludedRows.some((rule) => {
    const ruleDate = String(rule?.targetDate ?? rule?.date ?? "").trim();
    const ruleSlotNumber = String(rule?.slotNumber ?? rule?.slot ?? "").trim();
    const ruleMachineName = String(rule?.machineName ?? "").trim();
    if (ruleDate && rowDate !== ruleDate) {
      return false;
    }
    if (ruleSlotNumber && rowSlotNumber !== ruleSlotNumber) {
      return false;
    }
    if (ruleMachineName && normalizeText(ruleMachineName) !== normalizeText(rowMachineName)) {
      return false;
    }
    return Boolean(ruleDate || ruleSlotNumber || ruleMachineName);
  });
}

function filterExcludedHuntScoreRows(rows, config = {}) {
  if (!Array.isArray(rows) || !Array.isArray(config?.excludedRows) || config.excludedRows.length === 0) {
    return Array.isArray(rows) ? rows : [];
  }
  return rows.filter((row) => !isExcludedHuntScoreRow(row, config));
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
  const estimateCacheKey = `__estimateCache\u0000${config?.settingEstimateMode ?? ""}`;
  let estimateCache = settingDefinitionCache.get(estimateCacheKey);
  if (!estimateCache) {
    estimateCache = new WeakMap();
    settingDefinitionCache.set(estimateCacheKey, estimateCache);
  }
  if (row && typeof row === "object" && estimateCache.has(row)) {
    const cachedEstimate = estimateCache.get(row);
    return {
      estimate: cachedEstimate,
      average: cachedEstimate?.average ?? null,
    };
  }
  const definition = getSettingDefinition(
    settingDefinitionCache,
    normalizeHuntScoreMachineName(row?.machine_name, config),
  );
  const estimate = definition
    ? calculateSettingEstimate(definition, row, { mode: config?.settingEstimateMode })
    : null;
  if (row && typeof row === "object") {
    estimateCache.set(row, estimate);
  }
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

function calculateCurrentNonPositiveStreak(windowRows) {
  let streak = 0;

  for (let index = windowRows.length - 1; index >= 0; index -= 1) {
    if (windowRows[index].differenceValue > 0) {
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

function calculateCurrentMachineContentStreak(windowRows, predicate) {
  let streak = 0;

  for (let index = windowRows.length - 1; index >= 0; index -= 1) {
    if (!predicate(windowRows[index])) {
      break;
    }
    streak += 1;
  }

  return streak;
}

function sumDifferenceValues(rows) {
  return rows.reduce((total, row) => total + (readNumber(row?.differenceValue) ?? 0), 0);
}

function readWindowRawDifferenceValue(row) {
  return readRawHuntScoreDifferenceValue(row?.row);
}

function sumRawDifferenceValues(rows) {
  return rows.reduce((total, row) => {
    const differenceValue = readWindowRawDifferenceValue(row);
    return total + (Number.isFinite(differenceValue) ? differenceValue : 0);
  }, 0);
}

function countRawDifferenceValueRows(rows) {
  return rows.filter((row) => Number.isFinite(readWindowRawDifferenceValue(row))).length;
}

function calculateCurrentRawDifferenceLosingStreak(rows) {
  let streak = 0;
  for (let index = rows.length - 1; index >= 0; index -= 1) {
    const differenceValue = readWindowRawDifferenceValue(rows[index]);
    if (!Number.isFinite(differenceValue) || differenceValue >= 0) {
      break;
    }
    streak += 1;
  }
  return streak;
}

function readWindowField(row, fieldName) {
  const directValue = Number(row?.[fieldName]);
  if (Number.isFinite(directValue)) {
    return directValue;
  }
  const rawFieldName =
    fieldName === "games" ? "games_count" :
    fieldName === "bbCount" ? "bb_count" :
    fieldName === "rbCount" ? "rb_count" :
    "";
  return rawFieldName ? (readNumber(row?.row?.[rawFieldName]) ?? 0) : 0;
}

function sumWindowField(rows, fieldName) {
  return rows.reduce((total, row) => total + readWindowField(row, fieldName), 0);
}

function countBigShowRows(rows) {
  return rows.filter((row) => readWindowField(row, "games") >= 5000 && row.differenceValue >= 1000).length;
}

function countStrictHighContentRows(rows) {
  return rows.filter((row) => {
    const games = readWindowField(row, "games");
    const bbCount = readWindowField(row, "bbCount");
    const rbCount = readWindowField(row, "rbCount");
    const bonusCount = bbCount + rbCount;
    if (games < 5000 || bonusCount <= 0 || rbCount <= 0) {
      return false;
    }
    return games / bonusCount <= 145 && games / rbCount <= 315;
  }).length;
}

function calculateCombinedDenominatorFromWindowRow(row) {
  const games = readWindowField(row, "games");
  const bonusCount = readWindowField(row, "bbCount") + readWindowField(row, "rbCount");
  return games > 0 && bonusCount > 0 ? games / bonusCount : 9999;
}

function calculateRbDenominatorFromWindowRow(row) {
  const games = readWindowField(row, "games");
  const rbCount = readWindowField(row, "rbCount");
  return games > 0 && rbCount > 0 ? games / rbCount : 9999;
}

function isOkidokiDuoEncoreMachineName(machineName) {
  const normalizedMachineName = normalizeText(machineName);
  return [
    "スマスロ 沖ドキ!DUO アンコール",
    "スマスロ沖ドキ!DUOアンコール",
    "L沖ドキ!DUO アンコール",
  ].some((candidateName) => normalizedMachineName === normalizeText(candidateName));
}

function isOkidokiBlackMachineName(machineName) {
  const normalizedMachineName = normalizeText(machineName);
  return ["沖ドキ！BLACK", "沖ドキ!BLACK", "沖ドキ！ＢＬＡＣＫ"].some(
    (candidateName) => normalizedMachineName === normalizeText(candidateName),
  );
}

function isOkidokiGoldMachineName(machineName) {
  const normalizedMachineName = normalizeText(machineName);
  return ["沖ドキ！ＧＯＬＤ", "沖ドキ！ＧＯＬＤ-30", "沖ドキ!GOLD", "沖ドキ!GOLD-30"].some(
    (candidateName) => normalizedMachineName === normalizeText(candidateName),
  );
}

function isHououMachineName(machineName) {
  const normalizedMachineName = normalizeText(machineName);
  return [
    "ハナハナホウオウ",
    "ハナハナホウオウ-30",
    "ハナハナホウオウ‐30",
    "ハナハナホウオウ～天翔～-30",
    "ハナハナホウオウ～天翔～‐30",
  ].some((candidateName) => normalizedMachineName === normalizeText(candidateName));
}

function calculateOkidokiDuoHighContentScore(row) {
  const games = readWindowField(row, "games");
  const bbCount = readWindowField(row, "bbCount");
  const rbCount = readWindowField(row, "rbCount");
  const bonusCount = bbCount + rbCount;
  const combinedDenominator = calculateCombinedDenominatorFromWindowRow(row);
  const rbDenominator = calculateRbDenominatorFromWindowRow(row);
  const differenceValue = readNumber(row?.differenceValue) ?? 0;
  const rbRatio = bonusCount > 0 ? rbCount / bonusCount : 0;

  let score = 0;
  score += games >= 4500 ? 30 : games >= 3700 ? 25 : games >= 2400 ? 15 : games >= 1800 ? 8 : 0;
  score += combinedDenominator <= 95
    ? 30
    : combinedDenominator <= 105
      ? 25
      : combinedDenominator <= 115
        ? 20
        : combinedDenominator <= 125
          ? 12
          : combinedDenominator <= 140
            ? 5
            : 0;
  score += rbDenominator <= 285
    ? 20
    : rbDenominator <= 330
      ? 15
      : rbDenominator <= 400
        ? 10
        : rbDenominator <= 500
          ? 5
          : 0;
  score += differenceValue >= 3200
    ? 20
    : differenceValue >= 750
      ? 15
      : differenceValue >= 0
        ? 10
        : differenceValue >= -500
          ? 4
          : 0;
  score += rbRatio >= 0.3 ? 5 : rbRatio >= 0.25 ? 3 : 0;

  return score;
}

function calculateKurumeMyHighContentScore(row) {
  const games = readWindowField(row, "games");
  const bbCount = readWindowField(row, "bbCount");
  const rbCount = readWindowField(row, "rbCount");
  const combinedDenominator = calculateCombinedDenominatorFromWindowRow(row);
  const rbDenominator = calculateRbDenominatorFromWindowRow(row);
  const differenceValue = readNumber(row?.differenceValue) ?? 0;
  const rbToBbRatio = bbCount > 0 ? rbCount / bbCount : 0;

  let score = 0;
  score += games >= 7000 ? 15 : games >= 5000 ? 12 : games >= 3000 ? 8 : 0;
  score += combinedDenominator <= 125
    ? 30
    : combinedDenominator <= 130
      ? 26
      : combinedDenominator <= 140
        ? 18
        : combinedDenominator <= 150
          ? 10
          : 0;
  score += rbDenominator <= 260
    ? 30
    : rbDenominator <= 285
      ? 26
      : rbDenominator <= 320
        ? 18
        : rbDenominator <= 360
          ? 10
          : 0;
  score += differenceValue >= 2000
    ? 12
    : differenceValue >= 1000
      ? 9
      : differenceValue >= 0
        ? 5
        : differenceValue >= -500
          ? 2
          : 0;
  score += rbToBbRatio >= 0.65 ? 5 : 0;

  return score;
}

function readMachineContentRule(config, machineName) {
  const rules = config?.machineHighContentRules;
  if (!rules || typeof rules !== "object") {
    return "";
  }
  const normalizedMachineName = normalizeText(machineName);
  for (const [candidateName, ruleName] of Object.entries(rules)) {
    if (normalizeText(candidateName) === normalizedMachineName) {
      return String(ruleName ?? "").trim();
    }
  }
  return "";
}

const NEO_AIM_BONUS_SETTING_RATES = [
  { setting: 1, bb: 1 / 273.1, rb: 1 / 439.8 },
  { setting: 2, bb: 1 / 269.7, rb: 1 / 399.6 },
  { setting: 3, bb: 1 / 269.7, rb: 1 / 331.0 },
  { setting: 4, bb: 1 / 259.0, rb: 1 / 315.1 },
  { setting: 5, bb: 1 / 259.0, rb: 1 / 255.0 },
  { setting: 6, bb: 1 / 255.0, rb: 1 / 255.0 },
];

function calculateLogBinomialProbabilityForHuntScore(successCount, totalCount, probability) {
  if (
    totalCount < 0 ||
    successCount < 0 ||
    successCount > totalCount ||
    probability < 0 ||
    probability > 1
  ) {
    return Number.NEGATIVE_INFINITY;
  }
  if (totalCount === 0) {
    return successCount === 0 ? 0 : Number.NEGATIVE_INFINITY;
  }
  if (probability === 0) {
    return successCount === 0 ? 0 : Number.NEGATIVE_INFINITY;
  }
  if (probability === 1) {
    return successCount === totalCount ? 0 : Number.NEGATIVE_INFINITY;
  }

  const smallerSide = Math.min(successCount, totalCount - successCount);
  let logCombination = 0;
  for (let count = 1; count <= smallerSide; count += 1) {
    logCombination += Math.log(totalCount - smallerSide + count) - Math.log(count);
  }
  return (
    logCombination +
    successCount * Math.log(probability) +
    (totalCount - successCount) * Math.log(1 - probability)
  );
}

function calculateNeoAimSettingFivePlusProbability(row) {
  const games = Math.round(readWindowField(row, "games"));
  const bbCount = Math.round(readWindowField(row, "bbCount"));
  const rbCount = Math.round(readWindowField(row, "rbCount"));

  if (
    !Number.isInteger(games) ||
    !Number.isInteger(bbCount) ||
    !Number.isInteger(rbCount) ||
    games <= 0 ||
    bbCount < 0 ||
    rbCount < 0 ||
    bbCount > games ||
    rbCount > games
  ) {
    return null;
  }

  const logRows = NEO_AIM_BONUS_SETTING_RATES.map((rate) => ({
    setting: rate.setting,
    logValue:
      calculateLogBinomialProbabilityForHuntScore(bbCount, games, rate.bb) +
      calculateLogBinomialProbabilityForHuntScore(rbCount, games, rate.rb),
  }));
  const maxLogValue = Math.max(...logRows.map((rowValue) => rowValue.logValue));
  if (!Number.isFinite(maxLogValue)) {
    return null;
  }

  const weightedRows = logRows.map((rowValue) => ({
    ...rowValue,
    weight: Math.exp(rowValue.logValue - maxLogValue),
  }));
  const totalWeight = weightedRows.reduce((total, rowValue) => total + rowValue.weight, 0);
  if (!Number.isFinite(totalWeight) || totalWeight <= 0) {
    return null;
  }

  return (
    weightedRows
      .filter((rowValue) => rowValue.setting >= 5)
      .reduce((total, rowValue) => total + rowValue.weight, 0) / totalWeight
  );
}

function calculateNeoAimSettingFivePlusProbabilityAverage(rows) {
  const values = (Array.isArray(rows) ? rows : [])
    .map((row) => calculateNeoAimSettingFivePlusProbability(row))
    .filter((value) => Number.isFinite(value));
  if (values.length === 0) {
    return null;
  }
  return values.reduce((total, value) => total + value, 0) / values.length;
}

function isMachineHighContentWindowRow(row, machineName, config = null) {
  const normalizedMachineName = normalizeText(machineName);
  const games = readWindowField(row, "games");
  const combinedDenominator = calculateCombinedDenominatorFromWindowRow(row);
  const rbDenominator = calculateRbDenominatorFromWindowRow(row);
  const differenceValue = readNumber(row?.differenceValue) ?? 0;

  if (normalizedMachineName === normalizeText("ネオアイムジャグラーEX")) {
    const contentRule = readMachineContentRule(config, machineName);
    if (contentRule === "million-tobu-nerima-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "boom-tenjin-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 2500 && settingFivePlusProbability >= 0.5;
      }
      return games >= 2500 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "amuse-asakusa-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "beam-hikari-neo-aim-content") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && combinedDenominator <= 150 && rbDenominator <= 300;
    }
    if (contentRule === "apark-yakatabaru-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 150;
    }
    if (contentRule === "mj-arena-kurume-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 2500 && rbDenominator <= 300 && combinedDenominator <= 135;
    }
    if (contentRule === "mj-arena-airport-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return (
          (games >= 3500 && settingFivePlusProbability >= 0.5) ||
          (games >= 2500 &&
            settingFivePlusProbability >= 0.35 &&
            rbDenominator <= 340 &&
            combinedDenominator <= 150)
        );
      }
      return (
        (games >= 3500 && rbDenominator <= 300 && combinedDenominator <= 145) ||
        (games >= 2500 && rbDenominator <= 340 && combinedDenominator <= 150)
      );
    }
    if (contentRule === "mega-beam-asakura-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return (
          (games >= 2500 && settingFivePlusProbability >= 0.45) ||
          (games >= 3500 &&
            settingFivePlusProbability >= 0.3 &&
            rbDenominator <= 290 &&
            combinedDenominator <= 140)
        );
      }
      return games >= 3500 && rbDenominator <= 290 && combinedDenominator <= 140;
    }
    if (contentRule === "nakagawa-king-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 140;
    }
    if (contentRule === "king2-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability >= 0.5) ||
          (rbDenominator <= 300 && combinedDenominator <= 145))
      );
    }
    if (contentRule === "hinode-onojo-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 150;
    }
    if (contentRule === "chikushino-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 150;
    }
    if (contentRule === "espace-ueno-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 150;
    }
    if (contentRule === "messe-minamisenju-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 2500 && settingFivePlusProbability >= 0.5;
      }
      return games >= 2500 && rbDenominator <= 300 && combinedDenominator <= 150;
    }
    if (contentRule === "messe-nishikasai-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 310 && combinedDenominator <= 145;
    }
    if (contentRule === "maruhan-koiwa-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "maruhon-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 2500 && settingFivePlusProbability >= 0.5;
      }
      return games >= 2500 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "gaia-hikifune-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 2500 && settingFivePlusProbability >= 0.5;
      }
      return games >= 2500 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "123n-shinonome-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 2000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 2000 && rbDenominator <= 300 && combinedDenominator <= 140;
    }
    if (contentRule === "rakuen-ameyoko-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        (games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 140) ||
        (Number.isFinite(settingFivePlusProbability) &&
          games >= 4000 &&
          settingFivePlusProbability >= 0.5)
      );
    }
    if (contentRule === "concert-hall-kitasenju-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "kyuden-annex-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "jaran-yazaike-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 2500 && settingFivePlusProbability >= 0.5;
      }
      return games >= 2500 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "new-grand-hokima-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "new-crown-ayase-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 2500 && settingFivePlusProbability >= 0.5;
      }
      return games >= 2500 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "park-takenotsuka-studio-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "park-kitasenju-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 140;
    }
    if (contentRule === "park-kitasenju-sss-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 140;
    }
    if (contentRule === "park-kitayase-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 140;
    }
    if (contentRule === "mitoya-kinshicho-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 140;
    }
    if (contentRule === "mitoya-kinshicho-south-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 140;
    }
    if (contentRule === "mitoya-jackpot-kinshicho-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return (
          (games >= 1800 && settingFivePlusProbability >= 0.5) ||
          (games >= 2500 && rbDenominator <= 300 && combinedDenominator <= 145)
        );
      }
      return games >= 2500 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "mitoya-asakusa-senzoku-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 2000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 2500 && rbDenominator <= 300 && combinedDenominator <= 140;
    }
    if (contentRule === "ex-arena-tokyo-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return (
          (games >= 5000 && settingFivePlusProbability >= 0.5) ||
          (games >= 3000 && rbDenominator <= 285 && combinedDenominator <= 140)
        );
      }
      return games >= 3000 && rbDenominator <= 285 && combinedDenominator <= 140;
    }
    if (contentRule === "kintoki-kamata-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return (
          (games >= 3000 && settingFivePlusProbability >= 0.5) ||
          (games >= 4500 &&
            settingFivePlusProbability >= 0.35 &&
            rbDenominator <= 310 &&
            combinedDenominator <= 145)
        );
      }
      return (
        (games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145) ||
        (games >= 4500 && rbDenominator <= 310 && combinedDenominator <= 145)
      );
    }
    if (contentRule === "iidabashi-presas-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "yasuda-hibarigaoka-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "wonderland-minamigaoka-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return (
          (games >= 3000 && settingFivePlusProbability >= 0.5) ||
          (games >= 4000 && rbDenominator <= 300 && combinedDenominator <= 145)
        );
      }
      return (
        (games >= 3000 && rbDenominator <= 310 && combinedDenominator <= 140) ||
        (games >= 4000 && rbDenominator <= 300 && combinedDenominator <= 145)
      );
    }
    if (contentRule === "wonderland-sue-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return (
          (games >= 3000 && settingFivePlusProbability >= 0.5) ||
          (games >= 3000 &&
            settingFivePlusProbability >= 0.35 &&
            rbDenominator <= 300 &&
            combinedDenominator <= 140)
        );
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 140;
    }
    if (contentRule === "wonderland-1188-tachiarai-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 4000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 4000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "sengawa-uno-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "minowa-uno-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 140;
    }
    if (contentRule === "toyo-hall-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 2000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 2000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "grandship-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 2500 && settingFivePlusProbability >= 0.5;
      }
      return games >= 2500 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "plaza-tenjin-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 4000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 4000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "plaza-honten-ii-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "plaza-honten-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "plaza3-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "super-hollywood-1120-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "carol96-tsubuku-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "slot-marumitsu-ohashi-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3500 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3500 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "tamaya-honten-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    if (contentRule === "tamaya-ohashi-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 2500 && settingFivePlusProbability >= 0.5;
      }
      return games >= 2500 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    return games >= 6000 && rbDenominator <= 280 && combinedDenominator <= 140;
  }
  if (
    normalizedMachineName === normalizeText("SアイムジャグラーＥＸ") ||
    normalizedMachineName === normalizeText("SアイムジャグラーEX") ||
    normalizedMachineName === normalizeText("アイムジャグラーEX") ||
    normalizedMachineName === normalizeText("アイムジャグラーＥＸ")
  ) {
    const contentRule = readMachineContentRule(config, machineName);
    if (contentRule === "parlor-asahi-aim") {
      const rbCount = readWindowField(row, "rbCount");
      return (
        (games >= 3000 && rbCount > 0 && rbDenominator <= 300 && combinedDenominator <= 145) ||
        (games >= 4000 && rbCount > 0 && rbDenominator <= 270) ||
        (games >= 4000 && combinedDenominator <= 130 && rbDenominator <= 350)
      );
    }
    if (contentRule === "mj-arena-kurume-aim") {
      return games >= 2000 && rbDenominator <= 300 && combinedDenominator <= 155;
    }
    return games >= 4000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ゴーゴージャグラー３") ||
    normalizedMachineName === normalizeText("ゴーゴージャグラー3") ||
    normalizedMachineName === normalizeText("ゴーゴージャグラー")
  ) {
    const contentRule = readMachineContentRule(config, machineName);
    if (contentRule === "beam-hikari-gogo-content") {
      return games >= 3500 && combinedDenominator <= 135 && rbDenominator <= 280 && differenceValue >= -1000;
    }
    if (contentRule === "mj-arena-kurume-gogo") {
      return (
        games >= 3000 &&
        ((combinedDenominator <= 130 && rbDenominator <= 300) ||
          (combinedDenominator <= 138 && rbDenominator <= 265))
      );
    }
    const rbCount = readWindowField(row, "rbCount");
    return games >= 3000 && rbCount >= 15 && rbDenominator <= 240 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("マイジャグラーV") ||
    normalizedMachineName === normalizeText("マイジャグラーⅤ") ||
    normalizedMachineName === normalizeText("マイジャグラー")
  ) {
    const contentRule = readMachineContentRule(config, machineName);
    if (contentRule === "apark-yakatabaru-my") {
      return (
        games >= 5000 &&
        ((combinedDenominator <= 135 && rbDenominator <= 300) ||
          (combinedDenominator <= 140 && rbDenominator <= 270) ||
          (combinedDenominator <= 130 && rbDenominator <= 330))
      );
    }
    if (contentRule === "mj-arena-kurume-my") {
      return games >= 3000 && calculateKurumeMyHighContentScore(row) >= 60;
    }
    if (contentRule === "beam-hikari-my-content") {
      return games >= 5000 && combinedDenominator <= 135 && rbDenominator <= 300;
    }
    return games >= 6000 && rbDenominator <= 270 && combinedDenominator <= 135;
  }
  if (normalizedMachineName === normalizeText("スターハナハナ")) {
    return games >= 5500 && rbDenominator <= 285 && combinedDenominator <= 123;
  }
  if (normalizedMachineName === normalizeText("ニューキングハナハナ")) {
    return games >= 4000 && combinedDenominator <= 165 && rbDenominator <= 420;
  }
  if (normalizedMachineName === normalizeText("ドラゴンハナハナ～閃光～")) {
    return games >= 5000 && rbDenominator <= 450 && combinedDenominator <= 148;
  }
  if (
    normalizedMachineName === normalizeText("ファンキージャグラー２ＫＴ") ||
    normalizedMachineName === normalizeText("ファンキージャグラー２") ||
    normalizedMachineName === normalizeText("ファンキージャグラー2")
  ) {
    const contentRule = readMachineContentRule(config, machineName);
    if (contentRule === "beam-hikari-funky-content") {
      return (
        (games >= 4000 && combinedDenominator <= 138 && rbDenominator <= 300) ||
        (games >= 5500 && combinedDenominator <= 145 && rbDenominator <= 320 && differenceValue > 0)
      );
    }
    if (contentRule === "apark-yakatabaru-funky") {
      return (
        games >= 3500 &&
        ((combinedDenominator <= 138 && rbDenominator <= 330) ||
          (combinedDenominator <= 145 && rbDenominator <= 280))
      );
    }
    if (contentRule === "mj-arena-kurume-funky") {
      return (
        games >= 3000 &&
        ((combinedDenominator <= 138 && rbDenominator <= 320) ||
          (combinedDenominator <= 145 && rbDenominator <= 285) ||
          (combinedDenominator <= 125 && rbDenominator <= 360))
      );
    }
    const rbCount = readWindowField(row, "rbCount");
    return games >= 4000 && rbCount >= 20 && rbDenominator <= 300 && combinedDenominator <= 133;
  }
  if (
    normalizedMachineName === normalizeText("ハッピージャグラーＶＩＩＩ") ||
    normalizedMachineName === normalizeText("ハッピージャグラーVIII") ||
    normalizedMachineName === normalizeText("ハッピージャグラーＶ") ||
    normalizedMachineName === normalizeText("ハッピージャグラーV") ||
    normalizedMachineName === normalizeText("ハッピージャグラー")
  ) {
    if (readMachineContentRule(config, machineName) === "beam-hikari-happy-content") {
      return games >= 3000 && combinedDenominator <= 135 && rbDenominator <= 290;
    }
    if (readMachineContentRule(config, machineName) === "apark-yakatabaru-happy") {
      return games >= 3500 && combinedDenominator <= 135 && rbDenominator <= 310;
    }
    return games >= 5000 && combinedDenominator <= 145 && rbDenominator <= 315;
  }
  if (normalizedMachineName === normalizeText("ウルトラミラクルジャグラー")) {
    if (readMachineContentRule(config, machineName) === "apark-yakatabaru-ultra-miracle") {
      return games >= 3000 && combinedDenominator <= 134 && rbDenominator <= 300;
    }
    return games >= 5000 && combinedDenominator <= 145 && rbDenominator <= 315;
  }
  if (
    normalizedMachineName === normalizeText("ジャグラーガールズSS") ||
    normalizedMachineName === normalizeText("ジャグラーガールズ")
  ) {
    if (readMachineContentRule(config, machineName) === "beam-hikari-girls-content") {
      return games >= 3500 && combinedDenominator <= 132 && rbDenominator <= 278;
    }
    if (readMachineContentRule(config, machineName) === "mj-arena-kurume-girls") {
      return games >= 1500 && combinedDenominator <= 130 && rbDenominator <= 315;
    }
    const rbCount = readWindowField(row, "rbCount");
    return games >= 4000 && rbCount >= 25 && rbDenominator <= 281 && combinedDenominator <= 128;
  }
  if (normalizedMachineName === normalizeText("ミスタージャグラー")) {
    return games >= 5000 && rbDenominator <= 290 && combinedDenominator <= 135;
  }
  if (normalizedMachineName === normalizeText("スマスロモンキーターンV")) {
    return games >= 3502 && combinedDenominator <= 434 && differenceValue >= -819;
  }
  if (
    normalizedMachineName === normalizeText("Lスマスロ北斗の拳") ||
    normalizedMachineName === normalizeText("L スマスロ北斗の拳") ||
    normalizedMachineName === normalizeText("スマスロ北斗の拳")
  ) {
    if (readMachineContentRule(config, machineName) === "beam-hikari-hokuto-base-content") {
      return games >= 4000 && combinedDenominator <= 105;
    }
    return games >= 4000 && combinedDenominator <= 105;
  }
  if (
    normalizedMachineName === normalizeText("スマスロ北斗の拳 転生の章") ||
    normalizedMachineName === normalizeText("スマスロ北斗の拳 転生の章2") ||
    normalizedMachineName === normalizeText("スマスロ北斗の拳転生の章") ||
    normalizedMachineName === normalizeText("スマスロ北斗の拳転生の章2")
  ) {
    return (
      games >= 5000 &&
      ((combinedDenominator <= 425 && differenceValue >= -1500) ||
        (combinedDenominator <= 475 && differenceValue >= 2000))
    );
  }
  if (
    normalizedMachineName === normalizeText("スマスロ ハナビ") ||
    normalizedMachineName === normalizeText("スマスロハナビ")
  ) {
    return games >= 4000 && combinedDenominator <= 155 && rbDenominator <= 330;
  }
  if (
    normalizedMachineName === normalizeText("スマスロ サンダーV") ||
    normalizedMachineName === normalizeText("スマスロサンダーV") ||
    normalizedMachineName === normalizeText("LサンダーV")
  ) {
    return games >= 4000 && combinedDenominator <= 154 && rbDenominator <= 350;
  }
  if (normalizedMachineName === normalizeText("ウルトラミラクルジャグラー")) {
    if (readMachineContentRule(config, machineName) === "beam-hikari-ultra-miracle-content") {
      return games >= 3000 && combinedDenominator <= 150 && rbDenominator <= 350;
    }
  }
  if (isOkidokiDuoEncoreMachineName(machineName)) {
    return games >= 2400 && calculateOkidokiDuoHighContentScore(row) >= 65;
  }
  if (isOkidokiGoldMachineName(machineName)) {
    return games >= 4053 && combinedDenominator <= 152.4 && rbDenominator <= 566.8;
  }
  if (isOkidokiBlackMachineName(machineName)) {
    return games >= 4181 && combinedDenominator <= 142.4 && (rbDenominator <= 367.8 || differenceValue >= 1032);
  }
  if (isHououMachineName(machineName)) {
    return games >= 5000 && combinedDenominator <= 155 && rbDenominator <= 380;
  }

  return games >= 5000 && combinedDenominator <= 145 && rbDenominator <= 315;
}

function isMachineGoodContentWindowRow(row, machineName, config = null) {
  const normalizedMachineName = normalizeText(machineName);
  const games = readWindowField(row, "games");
  const combinedDenominator = calculateCombinedDenominatorFromWindowRow(row);
  const rbDenominator = calculateRbDenominatorFromWindowRow(row);
  const differenceValue = readNumber(row?.differenceValue) ?? 0;

  if (normalizedMachineName === normalizeText("ネオアイムジャグラーEX")) {
    const contentRule = readMachineContentRule(config, machineName);
    if (contentRule === "million-tobu-nerima-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.35;
      }
      return games >= 3000 && rbDenominator <= 360 && combinedDenominator <= 160;
    }
    if (contentRule === "boom-tenjin-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 2500 && settingFivePlusProbability >= 0.3 && rbDenominator <= 350 && combinedDenominator <= 160;
      }
      return games >= 2500 && rbDenominator <= 350 && combinedDenominator <= 160;
    }
    if (contentRule === "amuse-asakusa-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.35;
      }
      return games >= 3000 && rbDenominator <= 360 && combinedDenominator <= 160;
    }
    if (contentRule === "beam-hikari-neo-aim-content") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return (
          games >= 3000 &&
          settingFivePlusProbability >= 0.35 &&
          (rbDenominator <= 330 || combinedDenominator <= 135)
        );
      }
      return games >= 3000 && combinedDenominator <= 150 && rbDenominator <= 300;
    }
    if (contentRule === "apark-yakatabaru-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return (
          (games >= 3000 && settingFivePlusProbability >= 0.5) ||
          (games >= 5000 && rbDenominator <= 300 && combinedDenominator <= 140)
        );
      }
      return games >= 5000 && rbDenominator <= 300 && combinedDenominator <= 140;
    }
    if (contentRule === "mj-arena-kurume-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 2500 && settingFivePlusProbability >= 0.35;
      }
      return games >= 2500 && rbDenominator <= 300 && combinedDenominator <= 135;
    }
    if (contentRule === "mj-arena-airport-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return (
          (games >= 3500 && settingFivePlusProbability >= 0.5) ||
          (games >= 2500 &&
            settingFivePlusProbability >= 0.35 &&
            rbDenominator <= 340 &&
            combinedDenominator <= 150)
        );
      }
      return (
        (games >= 3500 && rbDenominator <= 300 && combinedDenominator <= 145) ||
        (games >= 2500 && rbDenominator <= 340 && combinedDenominator <= 150)
      );
    }
    if (contentRule === "mega-beam-asakura-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return (
          (games >= 2500 && settingFivePlusProbability >= 0.45) ||
          (games >= 3500 &&
            settingFivePlusProbability >= 0.3 &&
            rbDenominator <= 290 &&
            combinedDenominator <= 140)
        );
      }
      return games >= 3500 && rbDenominator <= 290 && combinedDenominator <= 140;
    }
    if (contentRule === "nakagawa-king-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return (
          (games >= 3000 && settingFivePlusProbability >= 0.5) ||
          (games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 140)
        );
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 140;
    }
    if (contentRule === "king2-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability >= 0.5) ||
          (rbDenominator <= 300 && combinedDenominator <= 145))
      );
    }
    if (contentRule === "hinode-onojo-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.35;
      }
      return games >= 3000 && rbDenominator <= 330 && combinedDenominator <= 150;
    }
    if (contentRule === "chikushino-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.35;
      }
      return games >= 3000 && rbDenominator <= 330 && combinedDenominator <= 150;
    }
    if (contentRule === "espace-ueno-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.35;
      }
      return games >= 3000 && rbDenominator <= 330 && combinedDenominator <= 150;
    }
    if (contentRule === "messe-minamisenju-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 2500 && settingFivePlusProbability >= 0.35;
      }
      return games >= 2500 && rbDenominator <= 330 && combinedDenominator <= 150;
    }
    if (contentRule === "messe-nishikasai-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.35;
      }
      return games >= 3000 && rbDenominator <= 350 && combinedDenominator <= 155;
    }
    if (contentRule === "maruhan-koiwa-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.35;
      }
      return games >= 3000 && rbDenominator <= 350 && combinedDenominator <= 160;
    }
    if (contentRule === "maruhon-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 2500 && settingFivePlusProbability >= 0.35;
      }
      return games >= 2500 && rbDenominator <= 350 && combinedDenominator <= 160;
    }
    if (contentRule === "gaia-hikifune-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 2500 && settingFivePlusProbability >= 0.3;
      }
      return games >= 2500 && rbDenominator <= 350 && combinedDenominator <= 160;
    }
    if (contentRule === "123n-shinonome-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return (
          (games >= 2000 && settingFivePlusProbability >= 0.35) ||
          (games >= 2000 && rbDenominator <= 300 && combinedDenominator <= 140)
        );
      }
      return games >= 2000 && rbDenominator <= 300 && combinedDenominator <= 140;
    }
    if (contentRule === "rakuen-ameyoko-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        (games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145) ||
        (Number.isFinite(settingFivePlusProbability) &&
          games >= 4000 &&
          settingFivePlusProbability >= 0.35)
      );
    }
    if (contentRule === "minowa-uno-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        (games >= 2500 && rbDenominator <= 330 && combinedDenominator <= 154) ||
        (Number.isFinite(settingFivePlusProbability) &&
          games >= 2500 &&
          settingFivePlusProbability >= 0.35)
      );
    }
    if (contentRule === "toyo-hall-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        rbDenominator <= 320 &&
        combinedDenominator <= 145 &&
        Number.isFinite(settingFivePlusProbability) &&
        settingFivePlusProbability >= 0.5
      );
    }
    if (contentRule === "grandship-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability >= 0.3) ||
          (rbDenominator <= 320 && combinedDenominator <= 145))
      );
    }
    if (contentRule === "concert-hall-kitasenju-neo-aim") {
      return games >= 3000 && rbDenominator <= 310 && combinedDenominator <= 145;
    }
    if (contentRule === "kyuden-annex-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.35;
      }
      return games >= 3000 && rbDenominator <= 350 && combinedDenominator <= 160;
    }
    if (contentRule === "jaran-yazaike-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 2500 && settingFivePlusProbability >= 0.35;
      }
      return games >= 2500 && rbDenominator <= 350 && combinedDenominator <= 160;
    }
    if (contentRule === "new-grand-hokima-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.35;
      }
      return games >= 3000 && rbDenominator <= 350 && combinedDenominator <= 160;
    }
    if (contentRule === "new-crown-ayase-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 2500 && settingFivePlusProbability >= 0.35;
      }
      return games >= 2500 && rbDenominator <= 350 && combinedDenominator <= 160;
    }
    if (contentRule === "park-takenotsuka-studio-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.35;
      }
      return games >= 3000 && rbDenominator <= 350 && combinedDenominator <= 160;
    }
    if (contentRule === "park-kitasenju-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.35;
      }
      return games >= 3000 && rbDenominator <= 350 && combinedDenominator <= 160;
    }
    if (contentRule === "park-kitasenju-sss-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.35;
      }
      return games >= 3000 && rbDenominator <= 350 && combinedDenominator <= 160;
    }
    if (contentRule === "park-kitayase-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return (
          games >= 3000 &&
          settingFivePlusProbability >= 0.5 &&
          (rbDenominator <= 310 || combinedDenominator <= 140)
        );
      }
      return games >= 3000 && (rbDenominator <= 310 || combinedDenominator <= 140);
    }
    if (contentRule === "mitoya-kinshicho-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return (
          games >= 3000 &&
          settingFivePlusProbability >= 0.5 &&
          (rbDenominator <= 310 || combinedDenominator <= 140)
        );
      }
      return games >= 3000 && (rbDenominator <= 310 || combinedDenominator <= 140);
    }
    if (contentRule === "mitoya-kinshicho-south-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        (Number.isFinite(settingFivePlusProbability) && games >= 2500 && settingFivePlusProbability >= 0.35) ||
        (games >= 2500 && rbDenominator <= 300 && combinedDenominator <= 140)
      );
    }
    if (contentRule === "mitoya-jackpot-kinshicho-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        (Number.isFinite(settingFivePlusProbability) && games >= 1800 && settingFivePlusProbability >= 0.35) ||
        (games >= 2500 && rbDenominator <= 330 && combinedDenominator <= 155)
      );
    }
    if (contentRule === "mitoya-asakusa-senzoku-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        (Number.isFinite(settingFivePlusProbability) && games >= 2500 && settingFivePlusProbability >= 0.35) ||
        (games >= 2500 && rbDenominator <= 300 && combinedDenominator <= 140)
      );
    }
    if (contentRule === "ex-arena-tokyo-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        (Number.isFinite(settingFivePlusProbability) && games >= 3000 && settingFivePlusProbability >= 0.35) ||
        (games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145)
      );
    }
    if (contentRule === "kintoki-kamata-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return (
          games >= 3000 &&
          settingFivePlusProbability >= 0.35 &&
          rbDenominator <= 350 &&
          combinedDenominator <= 160
        );
      }
      return games >= 3000 && rbDenominator <= 350 && combinedDenominator <= 160;
    }
    if (contentRule === "iidabashi-presas-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.35;
      }
      return games >= 3000 && rbDenominator <= 360 && combinedDenominator <= 160;
    }
    if (contentRule === "yasuda-hibarigaoka-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.35;
      }
      return games >= 3000 && rbDenominator <= 360 && combinedDenominator <= 160;
    }
    if (contentRule === "wonderland-minamigaoka-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return (
          (games >= 3000 && settingFivePlusProbability >= 0.35) ||
          (games >= 4000 && rbDenominator <= 330 && combinedDenominator <= 150)
        );
      }
      return games >= 3000 && rbDenominator <= 350 && combinedDenominator <= 160;
    }
    if (contentRule === "wonderland-sue-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return (
          (games >= 3000 && settingFivePlusProbability >= 0.5) ||
          (games >= 3000 &&
            settingFivePlusProbability >= 0.35 &&
            rbDenominator <= 300 &&
            combinedDenominator <= 140)
        );
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 140;
    }
    if (contentRule === "wonderland-1188-tachiarai-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return (
          (games >= 4000 && settingFivePlusProbability >= 0.5) ||
          (games >= 3000 && settingFivePlusProbability >= 0.35 && rbDenominator <= 330 && combinedDenominator <= 150)
        );
      }
      return games >= 3000 && rbDenominator <= 330 && combinedDenominator <= 150;
    }
    if (contentRule === "super-hollywood-1120-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 2500 && settingFivePlusProbability >= 0.3;
      }
      return games >= 2500 && rbDenominator <= 350 && combinedDenominator <= 155;
    }
    if (contentRule === "carol96-tsubuku-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      if (Number.isFinite(settingFivePlusProbability)) {
        return games >= 3000 && settingFivePlusProbability >= 0.5;
      }
      return games >= 3000 && rbDenominator <= 300 && combinedDenominator <= 145;
    }
    return games >= 5000 && rbDenominator <= 315 && combinedDenominator <= 145;
  }
  if (normalizedMachineName === normalizeText("スターハナハナ")) {
    return games >= 4000 && rbDenominator <= 300 && combinedDenominator <= 135;
  }
  if (normalizedMachineName === normalizeText("ドラゴンハナハナ～閃光～")) {
    return games >= 4000 && rbDenominator <= 500 && combinedDenominator <= 155;
  }
  if (
    normalizedMachineName === normalizeText("ファンキージャグラー２ＫＴ") ||
    normalizedMachineName === normalizeText("ファンキージャグラー２") ||
    normalizedMachineName === normalizeText("ファンキージャグラー2")
  ) {
    const contentRule = readMachineContentRule(config, machineName);
    if (contentRule === "beam-hikari-funky-content") {
      return (
        (games >= 4000 && combinedDenominator <= 138 && rbDenominator <= 300) ||
        (games >= 5500 && combinedDenominator <= 145 && rbDenominator <= 320 && differenceValue > 0)
      );
    }
    if (contentRule === "apark-yakatabaru-funky") {
      return (
        games >= 3500 &&
        ((combinedDenominator <= 138 && rbDenominator <= 330) ||
          (combinedDenominator <= 145 && rbDenominator <= 280))
      );
    }
    if (contentRule === "mj-arena-kurume-funky") {
      return (
        games >= 3000 &&
        ((combinedDenominator <= 138 && rbDenominator <= 320) ||
          (combinedDenominator <= 145 && rbDenominator <= 285) ||
          (combinedDenominator <= 125 && rbDenominator <= 360))
      );
    }
    const rbCount = readWindowField(row, "rbCount");
    return games >= 3500 && rbCount >= 15 && rbDenominator <= 323 && combinedDenominator <= 140;
  }
  if (normalizedMachineName === normalizeText("ウルトラミラクルジャグラー")) {
    if (readMachineContentRule(config, machineName) === "beam-hikari-ultra-miracle-content") {
      return games >= 3000 && combinedDenominator <= 134 && rbDenominator <= 300;
    }
  }
  if (
    normalizedMachineName === normalizeText("ハッピージャグラーＶＩＩＩ") ||
    normalizedMachineName === normalizeText("ハッピージャグラーVIII") ||
    normalizedMachineName === normalizeText("ハッピージャグラーＶ") ||
    normalizedMachineName === normalizeText("ハッピージャグラーV") ||
    normalizedMachineName === normalizeText("ハッピージャグラー")
  ) {
    if (readMachineContentRule(config, machineName) === "beam-hikari-happy-content") {
      return games >= 3500 && combinedDenominator <= 145 && rbDenominator <= 310;
    }
  }

  return isMachineHighContentWindowRow(row, machineName, config);
}

function isMachineLowContentWindowRow(row, machineName, config = null) {
  const normalizedMachineName = normalizeText(machineName);
  const games = readWindowField(row, "games");

  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "super-hollywood-1120-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    return Number.isFinite(settingFivePlusProbability) && games >= 3000 && settingFivePlusProbability < 0.3;
  }

  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "carol96-tsubuku-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    return Number.isFinite(settingFivePlusProbability) && games >= 3000 && settingFivePlusProbability < 0.3;
  }

  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "mj-arena-airport-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    return Number.isFinite(settingFivePlusProbability) && games >= 3000 && settingFivePlusProbability < 0.3;
  }

  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "mega-beam-asakura-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    return (
      (Number.isFinite(settingFivePlusProbability) && games >= 3000 && settingFivePlusProbability < 0.3) ||
      (games >= 3000 && calculateRbDenominatorFromWindowRow(row) > 400)
    );
  }

  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "tamaya-honten-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    return Number.isFinite(settingFivePlusProbability) && games >= 3000 && settingFivePlusProbability < 0.3;
  }

  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    [
      "million-tobu-nerima-neo-aim",
      "boom-tenjin-neo-aim",
      "chikushino-neo-aim",
      "iidabashi-presas-neo-aim",
      "wonderland-minamigaoka-neo-aim",
      "wonderland-sue-neo-aim",
      "wonderland-1188-tachiarai-neo-aim",
      "nakagawa-king-neo-aim",
      "king2-neo-aim",
      "sengawa-uno-neo-aim",
      "plaza-tenjin-neo-aim",
      "plaza-honten-ii-neo-aim",
      "plaza-honten-neo-aim",
      "plaza3-neo-aim",
      "slot-marumitsu-ohashi-neo-aim",
      "tamaya-ohashi-neo-aim",
      "maruhon-neo-aim",
      "gaia-hikifune-neo-aim",
      "123n-shinonome-neo-aim",
      "rakuen-ameyoko-neo-aim",
      "minowa-uno-neo-aim",
      "toyo-hall-neo-aim",
      "grandship-neo-aim",
      "park-takenotsuka-studio-neo-aim",
      "park-kitasenju-neo-aim",
      "park-kitasenju-sss-neo-aim",
      "park-kitayase-neo-aim",
      "mitoya-kinshicho-neo-aim",
      "mitoya-kinshicho-south-neo-aim",
      "mitoya-jackpot-kinshicho-neo-aim",
      "mitoya-asakusa-senzoku-neo-aim",
      "ex-arena-tokyo-neo-aim",
    ].includes(
      readMachineContentRule(config, machineName),
    )
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    return Number.isFinite(settingFivePlusProbability) && games >= 2000 && settingFivePlusProbability < 0.3;
  }

  return false;
}

function isMachineWeakContentWindowRow(row, machineName, config = null) {
  const normalizedMachineName = normalizeText(machineName);
  const games = readWindowField(row, "games");
  const combinedDenominator = calculateCombinedDenominatorFromWindowRow(row);
  const rbDenominator = calculateRbDenominatorFromWindowRow(row);

  if (normalizedMachineName === normalizeText("ネオアイムジャグラーEX")) {
    if (readMachineContentRule(config, machineName) === "park-kitasenju-sss-neo-aim") {
      const rbCount = readWindowField(row, "rbCount");
      return games >= 2000 && (rbCount <= 0 || rbDenominator > 400);
    }
    if (readMachineContentRule(config, machineName) === "million-tobu-nerima-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "boom-tenjin-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 1500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "chikushino-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "mj-arena-airport-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "mega-beam-asakura-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 3000 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400)
      );
    }
    if (readMachineContentRule(config, machineName) === "iidabashi-presas-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "wonderland-minamigaoka-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "wonderland-sue-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "wonderland-1188-tachiarai-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "nakagawa-king-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "king2-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "sengawa-uno-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "minowa-uno-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 3000 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "toyo-hall-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "grandship-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "mitoya-asakusa-senzoku-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 1500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "plaza-tenjin-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 3000 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.15) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "plaza-honten-ii-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "plaza-honten-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "plaza3-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "super-hollywood-1120-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 3000 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "carol96-tsubuku-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 3000 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "slot-marumitsu-ohashi-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2500 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "tamaya-honten-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 3000 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 420 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "tamaya-ohashi-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2000 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "123n-shinonome-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 2000 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    if (readMachineContentRule(config, machineName) === "rakuen-ameyoko-neo-aim") {
      const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
      return (
        games >= 3000 &&
        ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability < 0.3) ||
          rbDenominator > 400 ||
          combinedDenominator > 170)
      );
    }
    return games >= 3000 && combinedDenominator >= 170 && rbDenominator >= 400;
  }

  return false;
}

function isMachineStrongHighContentWindowRow(row, machineName, config = null) {
  const normalizedMachineName = normalizeText(machineName);
  const games = readWindowField(row, "games");
  const combinedDenominator = calculateCombinedDenominatorFromWindowRow(row);
  const rbDenominator = calculateRbDenominatorFromWindowRow(row);
  const differenceValue = readNumber(row?.differenceValue) ?? 0;

  if (normalizedMachineName === normalizeText("スマスロモンキーターンV")) {
    return games >= 4851 && combinedDenominator <= 422 && differenceValue >= -468;
  }
  if (normalizedMachineName === normalizeText("ニューキングハナハナ")) {
    return games >= 6000 && combinedDenominator <= 155 && rbDenominator <= 420;
  }
  if (normalizedMachineName === normalizeText("ミスタージャグラー")) {
    const rbCount = readWindowField(row, "rbCount");
    return rbCount >= 25 && rbDenominator <= 260 && combinedDenominator <= 140;
  }
  if (
    normalizedMachineName === normalizeText("ジャグラーガールズSS") ||
    normalizedMachineName === normalizeText("ジャグラーガールズ")
  ) {
    if (readMachineContentRule(config, machineName) === "beam-hikari-girls-content") {
      return games >= 3000 && combinedDenominator <= 145 && rbDenominator <= 320;
    }
    if (readMachineContentRule(config, machineName) === "mj-arena-kurume-girls") {
      return games >= 2000 && combinedDenominator <= 132 && rbDenominator <= 278;
    }
  }
  if (
    normalizedMachineName === normalizeText("SアイムジャグラーＥＸ") ||
    normalizedMachineName === normalizeText("SアイムジャグラーEX") ||
    normalizedMachineName === normalizeText("アイムジャグラーEX") ||
    normalizedMachineName === normalizeText("アイムジャグラーＥＸ")
  ) {
    if (readMachineContentRule(config, machineName) === "parlor-asahi-aim") {
      return games >= 4000 && rbDenominator <= 270 && combinedDenominator <= 135;
    }
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "million-tobu-nerima-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 5000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 5000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "boom-tenjin-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "amuse-asakusa-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "apark-yakatabaru-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "mj-arena-kurume-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "mj-arena-airport-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3500 && settingFivePlusProbability >= 0.5;
    }
    return games >= 3500 && rbDenominator <= 300 && combinedDenominator <= 145;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "mega-beam-asakura-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.5;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "nakagawa-king-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "king2-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    return (
      games >= 2500 &&
      ((Number.isFinite(settingFivePlusProbability) && settingFivePlusProbability >= 0.7) ||
        (rbDenominator <= 270 && combinedDenominator <= 130))
    );
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "beam-hikari-neo-aim-content"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 4000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 5000 && combinedDenominator <= 135 && rbDenominator <= 285;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "hinode-onojo-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "chikushino-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "espace-ueno-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "messe-minamisenju-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "messe-nishikasai-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "maruhan-koiwa-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "maruhon-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 2500 && settingFivePlusProbability >= 0.7;
    }
    return games >= 2500 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "gaia-hikifune-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "123n-shinonome-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 2000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 2000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "rakuen-ameyoko-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 4000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 4000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "concert-hall-kitasenju-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "kyuden-annex-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "jaran-yazaike-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 2500 && settingFivePlusProbability >= 0.7;
    }
    return games >= 2500 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "new-grand-hokima-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "new-crown-ayase-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "park-takenotsuka-studio-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "park-kitasenju-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "park-kitasenju-sss-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "park-kitayase-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "mitoya-kinshicho-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "mitoya-kinshicho-south-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "mitoya-jackpot-kinshicho-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 2500 && settingFivePlusProbability >= 0.7;
    }
    return games >= 2500 && rbDenominator <= 270 && combinedDenominator <= 135;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "mitoya-asakusa-senzoku-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "ex-arena-tokyo-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 5500 && settingFivePlusProbability >= 0.7 && combinedDenominator <= 135;
    }
    return games >= 5500 && rbDenominator <= 270 && combinedDenominator <= 135;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "kintoki-kamata-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "iidabashi-presas-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "yasuda-hibarigaoka-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "wonderland-minamigaoka-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "wonderland-sue-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 4000 && settingFivePlusProbability >= 0.7 && rbDenominator <= 270 && combinedDenominator <= 135;
    }
    return games >= 4000 && rbDenominator <= 270 && combinedDenominator <= 135;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "wonderland-1188-tachiarai-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 5000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 5000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "sengawa-uno-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 4000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 4000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "minowa-uno-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 4000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 4000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "toyo-hall-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 2000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 2000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "grandship-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 2500 && settingFivePlusProbability >= 0.7;
    }
    return games >= 2500 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "plaza-tenjin-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "plaza-honten-ii-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "plaza-honten-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "plaza3-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 4000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 4000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "slot-marumitsu-ohashi-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 5000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 5000 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "tamaya-honten-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 3500 && settingFivePlusProbability >= 0.7;
    }
    return games >= 3500 && rbDenominator <= 270 && combinedDenominator <= 135;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "carol96-tsubuku-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 4000 && settingFivePlusProbability >= 0.7;
    }
    return games >= 4000 && rbDenominator <= 270 && combinedDenominator <= 135;
  }
  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "tamaya-ohashi-neo-aim"
  ) {
    const settingFivePlusProbability = calculateNeoAimSettingFivePlusProbability(row);
    if (Number.isFinite(settingFivePlusProbability)) {
      return games >= 2500 && settingFivePlusProbability >= 0.7;
    }
    return games >= 2500 && rbDenominator <= 270 && combinedDenominator <= 130;
  }
  if (
    (normalizedMachineName === normalizeText("ファンキージャグラー２ＫＴ") ||
      normalizedMachineName === normalizeText("ファンキージャグラー２") ||
      normalizedMachineName === normalizeText("ファンキージャグラー2")) &&
    readMachineContentRule(config, machineName) === "beam-hikari-funky-content"
  ) {
    return games >= 5000 && combinedDenominator <= 132 && rbDenominator <= 285 && differenceValue > -500;
  }
  if (isOkidokiGoldMachineName(machineName)) {
    return games >= 4299 && combinedDenominator <= 140.8 && rbDenominator <= 465.7;
  }
  if (isOkidokiBlackMachineName(machineName)) {
    return games >= 4698 && combinedDenominator <= 129.6 && (rbDenominator <= 334.6 || differenceValue >= 1297);
  }
  if (isHououMachineName(machineName)) {
    return games >= 5500 && combinedDenominator <= 150 && rbDenominator <= 360;
  }

  return isMachineHighContentWindowRow(row, machineName, config) && rbDenominator <= 285;
}

function isMachineStrongBonusWindowRow(row, machineName, config = null) {
  const normalizedMachineName = normalizeText(machineName);
  const games = readWindowField(row, "games");
  const combinedDenominator = calculateCombinedDenominatorFromWindowRow(row);
  const rbDenominator = calculateRbDenominatorFromWindowRow(row);

  if (
    normalizedMachineName === normalizeText("ネオアイムジャグラーEX") &&
    readMachineContentRule(config, machineName) === "concert-hall-kitasenju-neo-aim"
  ) {
    return games >= 4000 && rbDenominator <= 280 && combinedDenominator <= 135;
  }

  return isMachineStrongHighContentWindowRow(row, machineName, config);
}

function countDifferenceAtLeastRows(rows, threshold) {
  return rows.filter((row) => row.differenceValue >= threshold).length;
}

function isMachineShowWindowRow(row, machineName, config = null) {
  const normalizedMachineName = normalizeHuntScoreMachineName(row?.row?.machine_name, config);
  const differenceValue = readNumber(row?.differenceValue) ?? 0;
  return differenceValue >= 1000 || isMachineHighContentWindowRow(row, machineName ?? normalizedMachineName, config);
}

function countMachineShowRows(rows, machineName, config = null) {
  return (Array.isArray(rows) ? rows : []).filter((row) => isMachineShowWindowRow(row, machineName, config)).length;
}

function countConsecutiveRollingNetThresholdDays(rows, windowSize, threshold) {
  if (!Array.isArray(rows) || rows.length < windowSize || windowSize <= 0) return 0;
  let count = 0;
  for (let endIndex = rows.length - 1; endIndex >= windowSize - 1; endIndex -= 1) {
    const windowRows = rows.slice(endIndex - windowSize + 1, endIndex + 1);
    if (sumDifferenceValues(windowRows) > threshold) {
      break;
    }
    count += 1;
  }
  return count;
}

function countConsecutiveRollingNetAndGamesThresholdDays(rows, windowSize, threshold, minimumGames) {
  if (!Array.isArray(rows) || rows.length < windowSize || windowSize <= 0) return 0;
  let count = 0;
  for (let endIndex = rows.length - 1; endIndex >= windowSize - 1; endIndex -= 1) {
    const windowRows = rows.slice(endIndex - windowSize + 1, endIndex + 1);
    if (sumDifferenceValues(windowRows) > threshold || sumWindowField(windowRows, "games") < minimumGames) {
      break;
    }
    count += 1;
  }
  return count;
}

function countConsecutiveRollingAngleThresholdDays(rows, windowSize, threshold) {
  if (!Array.isArray(rows) || rows.length < windowSize || windowSize <= 0) return 0;
  let count = 0;
  for (let endIndex = rows.length - 1; endIndex >= windowSize - 1; endIndex -= 1) {
    const windowRows = rows.slice(endIndex - windowSize + 1, endIndex + 1);
    const netTotal = sumDifferenceValues(windowRows);
    const gamesTotal = sumWindowField(windowRows, "games");
    const netPerThousand = gamesTotal > 0 ? (netTotal / gamesTotal) * 1000 : 0;
    if (netPerThousand > threshold) {
      break;
    }
    count += 1;
  }
  return count;
}

function findRecentDifferenceAtLeastDays(rows, threshold) {
  if (!Array.isArray(rows)) return null;
  for (let offset = 1; offset <= rows.length; offset += 1) {
    if ((rows.at(-offset)?.differenceValue ?? 0) >= threshold) {
      return offset;
    }
  }
  return null;
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

function buildIncompleteWindowRows(businessDates, dateIndex, recordMapByDate, windowDays, config) {
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

  return windowRows.length > 0 ? windowRows : null;
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

function listSameMachineRowsByOrder(dateRows, config, machineName) {
  const normalizedMachineName = normalizeText(machineName);
  if (!Array.isArray(dateRows) || !normalizedMachineName) {
    return [];
  }

  return dateRows
    .filter((dateRow) => {
      const rowMachineName = normalizeHuntScoreMachineName(dateRow?.machine_name, config);
      return normalizeText(rowMachineName) === normalizedMachineName;
    })
    .sort((left, right) => {
      const leftSlotNumber = Number(left?.slot_number);
      const rightSlotNumber = Number(right?.slot_number);
      if (Number.isFinite(leftSlotNumber) && Number.isFinite(rightSlotNumber) && leftSlotNumber !== rightSlotNumber) {
        return leftSlotNumber - rightSlotNumber;
      }
      return String(left?.slot_number ?? "").localeCompare(String(right?.slot_number ?? ""), "ja", { numeric: true });
    });
}

function listAdjacentSameMachineRowsByOrder(dateRows, row, config, machineName, distance = 2) {
  const slotText = String(row?.slot_number ?? "").trim();
  const normalizedDistance = Math.max(1, Number(distance) || 2);
  if (!slotText) {
    return [];
  }

  const sameMachineRows = listSameMachineRowsByOrder(dateRows, config, machineName);
  if (sameMachineRows.length === 0) {
    return [];
  }

  if (config?.adjacentSameMachineMode === "slot-number-gap") {
    const targetSlotNumber = Number(row?.slot_number);
    if (!Number.isFinite(targetSlotNumber)) {
      return [];
    }
    const findSlotGroup = (slotNumber) => {
      const groups = Array.isArray(config?.adjacentSlotNumberGroups) ? config.adjacentSlotNumberGroups : [];
      return groups.find((group) => {
        const start = Number(group?.[0]);
        const end = Number(group?.[1]);
        return Number.isFinite(start) && Number.isFinite(end) && slotNumber >= start && slotNumber <= end;
      });
    };
    const targetGroup = findSlotGroup(targetSlotNumber);

    return sameMachineRows.filter((dateRow) => {
      const candidateSlotNumber = Number(dateRow?.slot_number);
      if (!Number.isFinite(candidateSlotNumber) || candidateSlotNumber === targetSlotNumber) {
        return false;
      }
      if (targetGroup) {
        const candidateGroup = findSlotGroup(candidateSlotNumber);
        if (!candidateGroup || candidateGroup !== targetGroup) {
          return false;
        }
      }
      const slotDistance = Math.abs(candidateSlotNumber - targetSlotNumber);
      return slotDistance >= 1 && slotDistance <= normalizedDistance;
    });
  }

  const targetIndex = sameMachineRows.findIndex((dateRow) => String(dateRow?.slot_number ?? "").trim() === slotText);
  if (targetIndex < 0) {
    return [];
  }

  return sameMachineRows.filter((_, index) => {
    const indexDistance = Math.abs(index - targetIndex);
    return indexDistance >= 1 && indexDistance <= normalizedDistance;
  });
}

function countAdjacentMachineHighContentRows(
  businessDates,
  dateIndex,
  row,
  rowsByDate,
  config,
  windowDays,
  machineName,
  distance = 2,
) {
  if (!(rowsByDate instanceof Map)) {
    return 0;
  }

  const normalizedWindowDays = Math.max(1, Number(windowDays) || DEFAULT_HUNT_SCORE_WINDOW_DAYS);
  const startIndex = Math.max(0, dateIndex - (normalizedWindowDays - 1));
  const windowDates = businessDates.slice(startIndex, dateIndex + 1);
  let count = 0;

  for (const date of windowDates) {
    for (const dateRow of listAdjacentSameMachineRowsByOrder(rowsByDate.get(date) ?? [], row, config, machineName, distance)) {
      const rowMachineName = normalizeHuntScoreMachineName(dateRow?.machine_name, config);
      const differenceValue = readHuntScoreDifferenceValue(dateRow, config.differenceMode, rowMachineName);
      if (isMachineHighContentWindowRow({ row: dateRow, differenceValue }, machineName, config)) {
        count += 1;
      }
    }
  }

  return count;
}

function countAdjacentMachineGoodContentRows(
  businessDates,
  dateIndex,
  row,
  rowsByDate,
  config,
  windowDays,
  machineName,
  distance = 2,
) {
  if (!(rowsByDate instanceof Map)) {
    return 0;
  }

  const normalizedWindowDays = Math.max(1, Number(windowDays) || DEFAULT_HUNT_SCORE_WINDOW_DAYS);
  const startIndex = Math.max(0, dateIndex - (normalizedWindowDays - 1));
  const windowDates = businessDates.slice(startIndex, dateIndex + 1);
  let count = 0;

  for (const date of windowDates) {
    for (const dateRow of listAdjacentSameMachineRowsByOrder(rowsByDate.get(date) ?? [], row, config, machineName, distance)) {
      const rowMachineName = normalizeHuntScoreMachineName(dateRow?.machine_name, config);
      const differenceValue = readHuntScoreDifferenceValue(dateRow, config.differenceMode, rowMachineName);
      if (isMachineGoodContentWindowRow({ row: dateRow, differenceValue }, machineName, config)) {
        count += 1;
      }
    }
  }

  return count;
}

function countAdjacentMachineWeakContentRows(
  businessDates,
  dateIndex,
  row,
  rowsByDate,
  config,
  windowDays,
  machineName,
  distance = 2,
) {
  if (!(rowsByDate instanceof Map)) {
    return 0;
  }

  const normalizedWindowDays = Math.max(1, Number(windowDays) || DEFAULT_HUNT_SCORE_WINDOW_DAYS);
  const startIndex = Math.max(0, dateIndex - (normalizedWindowDays - 1));
  const windowDates = businessDates.slice(startIndex, dateIndex + 1);
  let count = 0;

  for (const date of windowDates) {
    for (const dateRow of listAdjacentSameMachineRowsByOrder(rowsByDate.get(date) ?? [], row, config, machineName, distance)) {
      const rowMachineName = normalizeHuntScoreMachineName(dateRow?.machine_name, config);
      const differenceValue = readHuntScoreDifferenceValue(dateRow, config.differenceMode, rowMachineName);
      if (isMachineWeakContentWindowRow({ row: dateRow, differenceValue }, machineName, config)) {
        count += 1;
      }
    }
  }

  return count;
}

function countOtherSameMachineHighContentRows(
  businessDates,
  dateIndex,
  row,
  rowsByDate,
  config,
  windowDays,
  machineName,
) {
  if (!(rowsByDate instanceof Map)) {
    return 0;
  }

  const normalizedMachineName = normalizeText(machineName);
  const slotText = String(row?.slot_number ?? "").trim();
  const normalizedWindowDays = Math.max(1, Number(windowDays) || DEFAULT_HUNT_SCORE_WINDOW_DAYS);
  const startIndex = Math.max(0, dateIndex - (normalizedWindowDays - 1));
  const windowDates = businessDates.slice(startIndex, dateIndex + 1);
  let count = 0;

  for (const date of windowDates) {
    for (const dateRow of rowsByDate.get(date) ?? []) {
      if (String(dateRow?.slot_number ?? "").trim() === slotText) {
        continue;
      }
      const rowMachineName = normalizeHuntScoreMachineName(dateRow?.machine_name, config);
      if (normalizeText(rowMachineName) !== normalizedMachineName) {
        continue;
      }
      const differenceValue = readHuntScoreDifferenceValue(dateRow, config.differenceMode, rowMachineName);
      if (isMachineHighContentWindowRow({ row: dateRow, differenceValue }, machineName, config)) {
        count += 1;
      }
    }
  }

  return count;
}

function countAdjacentMachineDifferenceAtLeastRows(
  businessDates,
  dateIndex,
  row,
  rowsByDate,
  config,
  windowDays,
  machineName,
  minimumDifference,
  distance = 2,
) {
  if (!(rowsByDate instanceof Map)) {
    return 0;
  }

  const normalizedWindowDays = Math.max(1, Number(windowDays) || DEFAULT_HUNT_SCORE_WINDOW_DAYS);
  const startIndex = Math.max(0, dateIndex - (normalizedWindowDays - 1));
  const windowDates = businessDates.slice(startIndex, dateIndex + 1);
  let count = 0;

  for (const date of windowDates) {
    for (const dateRow of listAdjacentSameMachineRowsByOrder(rowsByDate.get(date) ?? [], row, config, machineName, distance)) {
      const rowMachineName = normalizeHuntScoreMachineName(dateRow?.machine_name, config);
      if (readHuntScoreDifferenceValue(dateRow, config.differenceMode, rowMachineName) >= minimumDifference) {
        count += 1;
      }
    }
  }

  return count;
}

function countAdjacentMachineShowRows(
  businessDates,
  dateIndex,
  row,
  rowsByDate,
  config,
  windowDays,
  machineName,
  distance = 2,
) {
  if (!(rowsByDate instanceof Map)) {
    return 0;
  }

  const normalizedWindowDays = Math.max(1, Number(windowDays) || DEFAULT_HUNT_SCORE_WINDOW_DAYS);
  const startIndex = Math.max(0, dateIndex - (normalizedWindowDays - 1));
  const windowDates = businessDates.slice(startIndex, dateIndex + 1);
  let count = 0;

  for (const date of windowDates) {
    for (const dateRow of listAdjacentSameMachineRowsByOrder(rowsByDate.get(date) ?? [], row, config, machineName, distance)) {
      const rowMachineName = normalizeHuntScoreMachineName(dateRow?.machine_name, config);
      const differenceValue = readHuntScoreDifferenceValue(dateRow, config.differenceMode, rowMachineName);
      if (isMachineShowWindowRow({ row: dateRow, differenceValue }, machineName, config)) {
        count += 1;
      }
    }
  }

  return count;
}

function countAdjacentMachineRows(
  businessDates,
  dateIndex,
  row,
  rowsByDate,
  config,
  windowDays,
  machineName,
  distance = 2,
) {
  if (!(rowsByDate instanceof Map)) {
    return 0;
  }

  const normalizedWindowDays = Math.max(1, Number(windowDays) || DEFAULT_HUNT_SCORE_WINDOW_DAYS);
  const startIndex = Math.max(0, dateIndex - (normalizedWindowDays - 1));
  const windowDates = businessDates.slice(startIndex, dateIndex + 1);
  let count = 0;

  for (const date of windowDates) {
    count += listAdjacentSameMachineRowsByOrder(rowsByDate.get(date) ?? [], row, config, machineName, distance).length;
  }

  return count;
}

function sumAdjacentMachineDifferenceRows(
  businessDates,
  dateIndex,
  row,
  rowsByDate,
  config,
  windowDays,
  machineName,
  distance = 2,
) {
  if (!(rowsByDate instanceof Map)) {
    return 0;
  }

  const normalizedWindowDays = Math.max(1, Number(windowDays) || DEFAULT_HUNT_SCORE_WINDOW_DAYS);
  const startIndex = Math.max(0, dateIndex - (normalizedWindowDays - 1));
  const windowDates = businessDates.slice(startIndex, dateIndex + 1);
  let total = 0;

  for (const date of windowDates) {
    for (const dateRow of listAdjacentSameMachineRowsByOrder(rowsByDate.get(date) ?? [], row, config, machineName, distance)) {
      const rowMachineName = normalizeHuntScoreMachineName(dateRow?.machine_name, config);
      total += readHuntScoreDifferenceValue(dateRow, config.differenceMode, rowMachineName);
    }
  }

  return total;
}

function sumAdjacentMachineWindowFieldRows(
  businessDates,
  dateIndex,
  row,
  rowsByDate,
  config,
  windowDays,
  machineName,
  fieldName,
  distance = 2,
) {
  if (!(rowsByDate instanceof Map)) {
    return 0;
  }

  const normalizedWindowDays = Math.max(1, Number(windowDays) || DEFAULT_HUNT_SCORE_WINDOW_DAYS);
  const startIndex = Math.max(0, dateIndex - (normalizedWindowDays - 1));
  const windowDates = businessDates.slice(startIndex, dateIndex + 1);
  let total = 0;

  for (const date of windowDates) {
    for (const dateRow of listAdjacentSameMachineRowsByOrder(rowsByDate.get(date) ?? [], row, config, machineName, distance)) {
      total += readWindowField({ row: dateRow }, fieldName);
    }
  }

  return total;
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
  let windowRows = useAvailableRows
    ? buildAvailableWindowRows(businessDates, dateIndex, recordMapByDate, windowDays, config)
    : buildWindowRows(businessDates, dateIndex, recordMapByDate, windowDays, config);
  if (!windowRows && !useAvailableRows) {
    windowRows = buildIncompleteWindowRows(
      businessDates,
      dateIndex,
      recordMapByDate,
      windowDays,
      config,
    );
  }
  windowRows = filterWindowRowsForHistoryReset(windowRows, config);
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

  const historyWindowRows = filterWindowRowsForHistoryReset(
    buildAvailableWindowRows(
      businessDates,
      dateIndex,
      recordMapByDate,
      config.historyWindowDays ?? config.windowDays ?? DEFAULT_HUNT_SCORE_WINDOW_DAYS,
      config,
    ),
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
  const targetRangeStartDate = String(config?.targetRangeStartDate ?? "").trim();
  const targetRangeHistoryRowCount = targetRangeStartDate
    ? historyWindowRows.filter((historyWindowRow) => {
        const rowDate = String(historyWindowRow?.row?.target_date ?? "").trim();
        return rowDate && rowDate >= targetRangeStartDate;
      }).length
    : historyRowCount;

  const todaySetting = getSettingEstimateAverage(settingDefinitionCache, row, config).average;
  const previousWindowRow = metricWindowRows.at(-2) ?? null;
  const recentTwoRows = metricWindowRows.slice(-2);
  const recentThreeRows = metricWindowRows.slice(-3);
  const recentFourRows = metricWindowRows.slice(-4);
  const recentFiveRows = metricWindowRows.slice(-5);
  const recentSixRows = metricWindowRows.slice(-6);
  const recentSevenRows = historyWindowRows.slice(-7);
  const recentTenRows = historyWindowRows.slice(-10);
  const recentFourteenRows = historyWindowRows.slice(-14);
  const recentTwentyOneRows = historyWindowRows.slice(-21);
  const recentTwentyEightRows = historyWindowRows.slice(-28);
  const recentThirtyRows = historyWindowRows.slice(-30);
  const recentFortyTwoRows = historyWindowRows.slice(-42);
  const recentFiftySixRows = historyWindowRows.slice(-56);
  const recentTwoNetTotal = sumDifferenceValues(recentTwoRows);
  const recentThreeNetTotal = sumDifferenceValues(recentThreeRows);
  const recentFourNetTotal = sumDifferenceValues(recentFourRows);
  const recentFiveNetTotal = sumDifferenceValues(recentFiveRows);
  const recentSixNetTotal = sumDifferenceValues(recentSixRows);
  const recentSevenNetTotal = sumDifferenceValues(recentSevenRows);
  const recentTenNetTotal = sumDifferenceValues(recentTenRows);
  const recentFourteenNetTotal = sumDifferenceValues(recentFourteenRows);
  const recentTwentyOneNetTotal = sumDifferenceValues(recentTwentyOneRows);
  const recentTwentyEightNetTotal = sumDifferenceValues(recentTwentyEightRows);
  const recentThirtyNetTotal = sumDifferenceValues(recentThirtyRows);
  const recentFortyTwoNetTotal = sumDifferenceValues(recentFortyTwoRows);
  const recentFiftySixNetTotal = sumDifferenceValues(recentFiftySixRows);
  const shortSevenSinkStayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 7, -500);
  const shortThreeSinkStayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 3, -300);
  const recentSevenMinus1200StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 7, -1200);
  const recentSevenMinus2000StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 7, -2000);
  const recentSevenMinus3000StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 7, -3000);
  const recentSevenMinus1500Games9000StayDays = countConsecutiveRollingNetAndGamesThresholdDays(
    historyWindowRows,
    7,
    -1500,
    9000,
  );
  const recentThreeMinus1000StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 3, -1000);
  const recentThreeMinus1700StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 3, -1700);
  const recentFiveMinus1000StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 5, -1000);
  const recentFiveMinus1500StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 5, -1500);
  const recentFiveMinus2000StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 5, -2000);
  const recentFiveMinus3000StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 5, -3000);
  const recentFiveMinus3500StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 5, -3500);
  const recentSevenMinus1500StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 7, -1500);
  const recentThirtyMinus2700StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 30, -2700);
  const recentFiveMinus500StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 5, -500);
  const recentTenMinus3000StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 10, -3000);
  const recentTenMinus2500StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 10, -2500);
  const recentTenMinus5225StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 10, -5225);
  const recentFourteenMinus500StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 14, -500);
  const recentFourteenMinus1500StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 14, -1500);
  const recentFourteenMinus1800StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 14, -1800);
  const recentFourteenMinus2000StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 14, -2000);
  const recentFourteenMinus2500StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 14, -2500);
  const recentFourteenMinus3000StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 14, -3000);
  const recentFourteenMinus4000StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 14, -4000);
  const recentFourteenMinus5000StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 14, -5000);
  const recentFourteenMinus3218StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 14, -3218);
  const recentFourteenNegativeStayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 14, -1);
  const recentTwentyOneMinus1500StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 21, -1500);
  const recentTwentyOneMinus2000StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 21, -2000);
  const recentTwentyOneMinus3000StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 21, -3000);
  const recentTwentyOneMinus4000StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 21, -4000);
  const recentTwentyOneMinus5000StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 21, -5000);
  const recentTwentyOneMinus11333StayDays = countConsecutiveRollingNetThresholdDays(historyWindowRows, 21, -11333);
  const recentFiveAngleMinus80StayDays = countConsecutiveRollingAngleThresholdDays(historyWindowRows, 5, -80);
  const recentFourLossDays = recentFourRows.filter((windowRow) => windowRow.differenceValue < 0).length;
  const recentSevenLossDays = recentSevenRows.filter((windowRow) => windowRow.differenceValue < 0).length;
  const recentFourteenLossDays = recentFourteenRows.filter((windowRow) => windowRow.differenceValue < 0).length;
  const recentFiveWinDays = recentFiveRows.filter((windowRow) => windowRow.differenceValue > 0).length;
  const recentSevenWinDays = recentSevenRows.filter((windowRow) => windowRow.differenceValue > 0).length;
  const recentFourteenWinDays = recentFourteenRows.filter((windowRow) => windowRow.differenceValue > 0).length;
  const recentSevenNonPositiveDays = recentSevenRows.filter((windowRow) => windowRow.differenceValue <= 0).length;
  const recentFourteenNonPositiveDays = recentFourteenRows.filter((windowRow) => windowRow.differenceValue <= 0).length;
  const recentFourPositiveCount = recentFourRows.filter((windowRow) => windowRow.differenceValue > 0).length;
  const recentFiveMaxWin = Math.max(0, ...recentFiveRows.map((windowRow) => windowRow.differenceValue));
  const recentThreeLowGames1500Count = recentThreeRows.filter((windowRow) => windowRow.games < 1500).length;
  const recentSevenLowGames500Count = recentSevenRows.filter((windowRow) => readWindowField(windowRow, "games") < 500).length;
  const recentTwoGamesTotal = recentTwoRows.reduce((total, windowRow) => total + windowRow.games, 0);
  const recentThreeGamesTotal = recentThreeRows.reduce((total, windowRow) => total + windowRow.games, 0);
  const recentFiveGamesTotal = recentFiveRows.reduce((total, windowRow) => total + windowRow.games, 0);
  const recentSevenGamesTotal = sumWindowField(recentSevenRows, "games");
  const recentTenGamesTotal = sumWindowField(recentTenRows, "games");
  const recentFourteenGamesTotal = sumWindowField(recentFourteenRows, "games");
  const recentTwentyOneGamesTotal = sumWindowField(recentTwentyOneRows, "games");
  const recentTwentyEightGamesTotal = sumWindowField(recentTwentyEightRows, "games");
  const recentThirtyGamesTotal = sumWindowField(recentThirtyRows, "games");
  const recentFortyTwoGamesTotal = sumWindowField(recentFortyTwoRows, "games");
  const recentFiftySixGamesTotal = sumWindowField(recentFiftySixRows, "games");
  const recentThreeBonusTotal = recentThreeRows.reduce(
    (total, windowRow) => total + windowRow.bbCount + windowRow.rbCount,
    0,
  );
  const recentTwoBonusTotal = recentTwoRows.reduce(
    (total, windowRow) => total + windowRow.bbCount + windowRow.rbCount,
    0,
  );
  const recentFiveBonusTotal = recentFiveRows.reduce(
    (total, windowRow) => total + windowRow.bbCount + windowRow.rbCount,
    0,
  );
  const recentTenBonusTotal = recentTenRows.reduce(
    (total, windowRow) => total + windowRow.bbCount + windowRow.rbCount,
    0,
  );
  const recentTwentyOneBonusTotal = recentTwentyOneRows.reduce(
    (total, windowRow) => total + windowRow.bbCount + windowRow.rbCount,
    0,
  );
  const recentTwoRbTotal = recentTwoRows.reduce((total, windowRow) => total + windowRow.rbCount, 0);
  const recentThreeRbTotal = recentThreeRows.reduce((total, windowRow) => total + windowRow.rbCount, 0);
  const recentFiveRbTotal = recentFiveRows.reduce((total, windowRow) => total + windowRow.rbCount, 0);
  const recentSevenRbTotal = sumWindowField(recentSevenRows, "rbCount");
  const recentTenRbTotal = sumWindowField(recentTenRows, "rbCount");
  const recentFourteenRbTotal = sumWindowField(recentFourteenRows, "rbCount");
  const recentTwentyOneRbTotal = sumWindowField(recentTwentyOneRows, "rbCount");
  const recentSevenBbTotal = sumWindowField(recentSevenRows, "bbCount");
  const recentTenBbTotal = sumWindowField(recentTenRows, "bbCount");
  const recentFourteenBbTotal = sumWindowField(recentFourteenRows, "bbCount");
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
  const isHistoryRbLightWindowRow = (historyWindowRow) => {
    const games = readWindowField(historyWindowRow, "games");
    const rbDenominator = calculateRbDenominatorFromWindowRow(historyWindowRow);
    return games >= 4000 && rbDenominator <= 270;
  };
  const twoDaysAgoHighSettingCandidate = isHighSettingCandidateWindowRow(metricWindowRows.at(-2));
  const threeDaysAgoHighSettingCandidate = isHighSettingCandidateWindowRow(metricWindowRows.at(-3));
  const fourDaysAgoHighSettingCandidate = isHighSettingCandidateWindowRow(metricWindowRows.at(-4));
  const recentFiveHighSettingCandidateCount = recentFiveRows.filter(isHighSettingCandidateWindowRow).length;
  const recentSevenHighSettingCandidateCount = historyWindowRows
    .slice(-7)
    .filter(isHistoryHighSettingCandidateWindowRow).length;
  const recentSevenHighSettingEstimateCount = historyWindowRows
    .slice(-7)
    .filter(isHistoryHighSettingEstimateWindowRow).length;
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
  const recentTwentyEightRbLightCount = recentTwentyEightRows.filter(isHistoryRbLightWindowRow).length;
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
  const daysSinceHistoryRbLight = (() => {
    for (let offset = 1; offset <= historyWindowRows.length; offset += 1) {
      const historyWindowRow = historyWindowRows.at(-offset);
      if (isHistoryRbLightWindowRow(historyWindowRow)) {
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
  const currentMachineName = normalizeHuntScoreMachineName(row?.machine_name, config);
  const previousMachineSettingFivePlusProbability = calculateNeoAimSettingFivePlusProbability({
    row,
    differenceValue: readHuntScoreDifferenceValue(row, config.differenceMode, currentMachineName),
  });
  const recentThreeMachineSettingFivePlusProbabilityAverage =
    calculateNeoAimSettingFivePlusProbabilityAverage(recentThreeRows);
  const recentFiveMachineSettingFivePlusProbabilityAverage =
    calculateNeoAimSettingFivePlusProbabilityAverage(recentFiveRows);
  const recentSevenMachineSettingFivePlusProbabilityAverage =
    calculateNeoAimSettingFivePlusProbabilityAverage(recentSevenRows);
  const recentTenMachineSettingFivePlusProbabilityAverage =
    calculateNeoAimSettingFivePlusProbabilityAverage(recentTenRows);
  const recentFourteenMachineSettingFivePlusProbabilityAverage =
    calculateNeoAimSettingFivePlusProbabilityAverage(recentFourteenRows);
  const recentTwentyOneMachineSettingFivePlusProbabilityAverage =
    calculateNeoAimSettingFivePlusProbabilityAverage(recentTwentyOneRows);
  const currentDateRows = rowsByDate.get(businessDates[dateIndex]) ?? [];
  const sameMachineRowsByOrder = listSameMachineRowsByOrder(currentDateRows, config, currentMachineName);
  const sameMachineTargetSlotText = String(row?.slot_number ?? "").trim();
  const sameMachinePositionIndex = sameMachineRowsByOrder.findIndex(
    (dateRow) => String(dateRow?.slot_number ?? "").trim() === sameMachineTargetSlotText,
  );
  const sameMachineOrderCount = sameMachineRowsByOrder.length;
  const sameMachinePositionFromLeft = sameMachinePositionIndex >= 0 ? sameMachinePositionIndex + 1 : null;
  const sameMachinePositionFromRight =
    sameMachinePositionIndex >= 0 ? sameMachineOrderCount - sameMachinePositionIndex : null;
  const previousAdjacentMachineHighContentCount = listAdjacentSameMachineRowsByOrder(
    currentDateRows,
    row,
    config,
    currentMachineName,
    1,
  ).filter((dateRow) => {
    const rowMachineName = normalizeHuntScoreMachineName(dateRow?.machine_name, config);
    const differenceValue = readHuntScoreDifferenceValue(dateRow, config.differenceMode, rowMachineName);
    return isMachineHighContentWindowRow({ row: dateRow, differenceValue }, currentMachineName, config);
  }).length;
  const previousAdjacentMachineHighContentCountNear2 = listAdjacentSameMachineRowsByOrder(
    currentDateRows,
    row,
    config,
    currentMachineName,
    2,
  ).filter((dateRow) => {
    const rowMachineName = normalizeHuntScoreMachineName(dateRow?.machine_name, config);
    const differenceValue = readHuntScoreDifferenceValue(dateRow, config.differenceMode, rowMachineName);
    return isMachineHighContentWindowRow({ row: dateRow, differenceValue }, currentMachineName, config);
  }).length;
  const previousAdjacentMachineStrongHighContentCount = listAdjacentSameMachineRowsByOrder(
    currentDateRows,
    row,
    config,
    currentMachineName,
    1,
  ).filter((dateRow) => {
    const rowMachineName = normalizeHuntScoreMachineName(dateRow?.machine_name, config);
    const differenceValue = readHuntScoreDifferenceValue(dateRow, config.differenceMode, rowMachineName);
    return isMachineStrongHighContentWindowRow({ row: dateRow, differenceValue }, currentMachineName, config);
  }).length;
  const previousAdjacentMachineGoodContentCount = listAdjacentSameMachineRowsByOrder(
    currentDateRows,
    row,
    config,
    currentMachineName,
    1,
  ).filter((dateRow) => {
    const rowMachineName = normalizeHuntScoreMachineName(dateRow?.machine_name, config);
    const differenceValue = readHuntScoreDifferenceValue(dateRow, config.differenceMode, rowMachineName);
    return isMachineGoodContentWindowRow({ row: dateRow, differenceValue }, currentMachineName, config);
  }).length;
  const previousAdjacentMachineWeakContentCount = listAdjacentSameMachineRowsByOrder(
    currentDateRows,
    row,
    config,
    currentMachineName,
    2,
  ).filter((dateRow) => {
    const rowMachineName = normalizeHuntScoreMachineName(dateRow?.machine_name, config);
    const differenceValue = readHuntScoreDifferenceValue(dateRow, config.differenceMode, rowMachineName);
    return isMachineWeakContentWindowRow({ row: dateRow, differenceValue }, currentMachineName, config);
  }).length;
  const previousOtherMachineHighContentCount = currentDateRows.filter((dateRow) => {
    if (String(dateRow?.slot_number ?? "").trim() === String(row?.slot_number ?? "").trim()) {
      return false;
    }
    const rowMachineName = normalizeHuntScoreMachineName(dateRow?.machine_name, config);
    if (normalizeText(rowMachineName) !== normalizeText(currentMachineName)) {
      return false;
    }
    const differenceValue = readHuntScoreDifferenceValue(dateRow, config.differenceMode, rowMachineName);
    return isMachineHighContentWindowRow({ row: dateRow, differenceValue }, currentMachineName, config);
  }).length;
  const previousAdjacentMachineBigWin1000Count = listAdjacentSameMachineRowsByOrder(
    currentDateRows,
    row,
    config,
    currentMachineName,
    1,
  ).filter((dateRow) => {
    const rowMachineName = normalizeHuntScoreMachineName(dateRow?.machine_name, config);
    return readHuntScoreDifferenceValue(dateRow, config.differenceMode, rowMachineName) >= 1000;
  }).length;
  const previousAdjacentMachineBigWin1500Count = listAdjacentSameMachineRowsByOrder(
    currentDateRows,
    row,
    config,
    currentMachineName,
    1,
  ).filter((dateRow) => {
    const rowMachineName = normalizeHuntScoreMachineName(dateRow?.machine_name, config);
    return readHuntScoreDifferenceValue(dateRow, config.differenceMode, rowMachineName) >= 1500;
  }).length;
  const previousAdjacentMachineNetTotal = listAdjacentSameMachineRowsByOrder(
    currentDateRows,
    row,
    config,
    currentMachineName,
    1,
  ).reduce((total, dateRow) => {
    const rowMachineName = normalizeHuntScoreMachineName(dateRow?.machine_name, config);
    return total + readHuntScoreDifferenceValue(dateRow, config.differenceMode, rowMachineName);
  }, 0);
  const previousAdjacentMachineRowCount = listAdjacentSameMachineRowsByOrder(
    currentDateRows,
    row,
    config,
    currentMachineName,
    1,
  ).length;
  const previousAdjacentMachineAverageDifference =
    previousAdjacentMachineRowCount > 0 ? previousAdjacentMachineNetTotal / previousAdjacentMachineRowCount : 0;
  const previousAdjacentMachineNetTotalNear2 = listAdjacentSameMachineRowsByOrder(
    currentDateRows,
    row,
    config,
    currentMachineName,
    2,
  ).reduce((total, dateRow) => {
    const rowMachineName = normalizeHuntScoreMachineName(dateRow?.machine_name, config);
    return total + readHuntScoreDifferenceValue(dateRow, config.differenceMode, rowMachineName);
  }, 0);
  const sameMachinePreviousNetTotal = currentDateRows.reduce((total, dateRow) => {
    const rowMachineName = normalizeHuntScoreMachineName(dateRow?.machine_name, config);
    if (normalizeText(rowMachineName) !== normalizeText(currentMachineName)) {
      return total;
    }
    return total + readHuntScoreDifferenceValue(dateRow, config.differenceMode, rowMachineName);
  }, 0);
  const isHistoryMachineHighContentWindowRow = (historyWindowRow) =>
    isMachineHighContentWindowRow(historyWindowRow, currentMachineName, config);
  const isHistoryMachineGoodContentWindowRow = (historyWindowRow) =>
    isMachineGoodContentWindowRow(historyWindowRow, currentMachineName, config);
  const isHistoryMachineLowContentWindowRow = (historyWindowRow) =>
    isMachineLowContentWindowRow(historyWindowRow, currentMachineName, config);
  const isHistoryMachineWeakContentWindowRow = (historyWindowRow) =>
    isMachineWeakContentWindowRow(historyWindowRow, currentMachineName, config);
  const isHistoryMachineStrongHighContentWindowRow = (historyWindowRow) =>
    isMachineStrongHighContentWindowRow(historyWindowRow, currentMachineName, config);
  const isHistoryMachineStrongBonusWindowRow = (historyWindowRow) =>
    isMachineStrongBonusWindowRow(historyWindowRow, currentMachineName, config);
  const recentThreeMachineHighContentCount = recentThreeRows.filter((windowRow) =>
    isMachineHighContentWindowRow(windowRow, currentMachineName, config),
  ).length;
  const recentThreeMachineGoodContentCount = recentThreeRows.filter((windowRow) =>
    isMachineGoodContentWindowRow(windowRow, currentMachineName, config),
  ).length;
  const recentFiveMachineHighContentCount = recentFiveRows.filter((windowRow) =>
    isMachineHighContentWindowRow(windowRow, currentMachineName, config),
  ).length;
  const recentSevenMachineHighContentCount = historyWindowRows
    .slice(-7)
    .filter(isHistoryMachineHighContentWindowRow).length;
  const recentTenMachineHighContentCount = historyWindowRows
    .slice(-10)
    .filter(isHistoryMachineHighContentWindowRow).length;
  const recentFourteenMachineHighContentCount = historyWindowRows
    .slice(-14)
    .filter(isHistoryMachineHighContentWindowRow).length;
  const recentTwentyOneMachineHighContentCount = historyWindowRows
    .slice(-21)
    .filter(isHistoryMachineHighContentWindowRow).length;
  const recentThirtyMachineHighContentCount = historyWindowRows
    .slice(-30)
    .filter(isHistoryMachineHighContentWindowRow).length;
  const recentSevenMachineGoodContentCount = historyWindowRows
    .slice(-7)
    .filter(isHistoryMachineGoodContentWindowRow).length;
  const recentSevenMachineShowCount = countMachineShowRows(historyWindowRows.slice(-7), currentMachineName, config);
  const recentThreeMachineLowContentCount = recentThreeRows.filter((windowRow) =>
    isMachineLowContentWindowRow(windowRow, currentMachineName, config),
  ).length;
  const recentFiveMachineLowContentCount = recentFiveRows.filter((windowRow) =>
    isMachineLowContentWindowRow(windowRow, currentMachineName, config),
  ).length;
  const recentSevenMachineLowContentCount = historyWindowRows
    .slice(-7)
    .filter(isHistoryMachineLowContentWindowRow).length;
  const recentFiveMachineWeakContentCount = recentFiveRows.filter((windowRow) =>
    isMachineWeakContentWindowRow(windowRow, currentMachineName, config),
  ).length;
  const recentSevenMachineWeakContentCount = historyWindowRows
    .slice(-7)
    .filter(isHistoryMachineWeakContentWindowRow).length;
  const recentFourteenMachineGoodContentCount = historyWindowRows
    .slice(-14)
    .filter(isHistoryMachineGoodContentWindowRow).length;
  const recentTwentyOneMachineGoodContentCount = historyWindowRows
    .slice(-21)
    .filter(isHistoryMachineGoodContentWindowRow).length;
  const recentThreeMachineStrongHighContentCount = recentThreeRows.filter((windowRow) =>
    isMachineStrongHighContentWindowRow(windowRow, currentMachineName, config),
  ).length;
  const recentSevenMachineStrongBonusCount = historyWindowRows
    .slice(-7)
    .filter(isHistoryMachineStrongBonusWindowRow).length;
  const recentFourteenMachineStrongHighContentCount = historyWindowRows
    .slice(-14)
    .filter(isHistoryMachineStrongHighContentWindowRow).length;
  const previousMachineHighContent = isMachineHighContentWindowRow(metricWindowRows.at(-1), currentMachineName, config);
  const previousMachineGoodContent = isMachineGoodContentWindowRow(metricWindowRows.at(-1), currentMachineName, config);
  const previousMachineWeakContent = isMachineWeakContentWindowRow(metricWindowRows.at(-1), currentMachineName, config);
  const previousMachineStrongHighContent = isMachineStrongHighContentWindowRow(
    metricWindowRows.at(-1),
    currentMachineName,
    config,
  );
  const daysSinceMachineHighContent = (() => {
    for (let offset = 1; offset <= historyWindowRows.length; offset += 1) {
      const historyWindowRow = historyWindowRows.at(-offset);
      if (isHistoryMachineHighContentWindowRow(historyWindowRow)) {
        return offset;
      }
    }
    return null;
  })();
  const daysSinceMachineGoodContent = (() => {
    for (let offset = 1; offset <= historyWindowRows.length; offset += 1) {
      const historyWindowRow = historyWindowRows.at(-offset);
      if (isHistoryMachineGoodContentWindowRow(historyWindowRow)) {
        return offset;
      }
    }
    return null;
  })();
  const daysSinceMachineStrongHighContent = (() => {
    for (let offset = 1; offset <= historyWindowRows.length; offset += 1) {
      const historyWindowRow = historyWindowRows.at(-offset);
      if (isHistoryMachineStrongHighContentWindowRow(historyWindowRow)) {
        return offset;
      }
    }
    return null;
  })();
  const daysSinceMachineBigWin1000 = findRecentDifferenceAtLeastDays(historyWindowRows, 1000);
  const daysSinceMachineBigWin1500 = findRecentDifferenceAtLeastDays(historyWindowRows, 1500);
  const daysSinceMachineRb300 = (() => {
    for (let offset = 1; offset <= historyWindowRows.length; offset += 1) {
      const historyWindowRow = historyWindowRows.at(-offset);
      const games = readWindowField(historyWindowRow, "games");
      const rbDenominator = calculateRbDenominatorFromWindowRow(historyWindowRow);
      if (games >= 1000 && rbDenominator <= 300) {
        return offset;
      }
    }
    return null;
  })();
  const adjacentMachineHighContentCount7 = countAdjacentMachineHighContentRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    windowDays,
    currentMachineName,
  );
  const adjacentMachineHighContentCount3 = countAdjacentMachineHighContentRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    3,
    currentMachineName,
  );
  const adjacentMachineHighContentCount3Near2 = countAdjacentMachineHighContentRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    3,
    currentMachineName,
    1,
  );
  const adjacentMachineHighContentCount14 = countAdjacentMachineHighContentRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    14,
    currentMachineName,
  );
  const adjacentMachineHighContentCount7Near2 = countAdjacentMachineHighContentRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    7,
    currentMachineName,
    1,
  );
  const adjacentMachineGoodContentCount7Near2 = countAdjacentMachineGoodContentRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    7,
    currentMachineName,
    1,
  );
  const adjacentMachineHighContentCount14Near2 = countAdjacentMachineHighContentRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    14,
    currentMachineName,
    1,
  );
  const otherSameMachineHighContentCount7 = countOtherSameMachineHighContentRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    7,
    currentMachineName,
  );
  const adjacentMachineBigWin1000Count7Near2 = countAdjacentMachineDifferenceAtLeastRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    7,
    currentMachineName,
    1000,
    2,
  );
  const adjacentMachineBigWin1000Count14 = countAdjacentMachineDifferenceAtLeastRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    14,
    currentMachineName,
    1000,
    1,
  );
  const adjacentMachineGamesTotal7 = sumAdjacentMachineWindowFieldRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    7,
    currentMachineName,
    "games",
    1,
  );
  const adjacentMachineBbTotal7 = sumAdjacentMachineWindowFieldRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    7,
    currentMachineName,
    "bbCount",
    1,
  );
  const adjacentMachineRbTotal7 = sumAdjacentMachineWindowFieldRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    7,
    currentMachineName,
    "rbCount",
    1,
  );
  const adjacentMachineRbDenominator7 =
    adjacentMachineGamesTotal7 > 0 && adjacentMachineRbTotal7 > 0
      ? adjacentMachineGamesTotal7 / adjacentMachineRbTotal7
      : 9999;
  const adjacentMachineCombinedDenominator7 =
    adjacentMachineGamesTotal7 > 0 && adjacentMachineBbTotal7 + adjacentMachineRbTotal7 > 0
      ? adjacentMachineGamesTotal7 / (adjacentMachineBbTotal7 + adjacentMachineRbTotal7)
      : 9999;
  const adjacentMachineShowCount3Near2 = countAdjacentMachineShowRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    3,
    currentMachineName,
    2,
  );
  const adjacentMachineNetTotal7 = sumAdjacentMachineDifferenceRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    7,
    currentMachineName,
  );
  const adjacentMachineNetTotal7Near2 = sumAdjacentMachineDifferenceRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    7,
    currentMachineName,
    1,
  );
  const adjacentMachineNetTotal14 = sumAdjacentMachineDifferenceRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    14,
    currentMachineName,
  );
  const adjacentMachineNetTotal14Near2 = sumAdjacentMachineDifferenceRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    14,
    currentMachineName,
    1,
  );
  const adjacentMachineNetTotal5 = sumAdjacentMachineDifferenceRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    5,
    currentMachineName,
  );
  const adjacentMachineNetTotal5Near2 = sumAdjacentMachineDifferenceRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    5,
    currentMachineName,
    1,
  );
  const adjacentMachineNetTotal3 = sumAdjacentMachineDifferenceRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    3,
    currentMachineName,
  );
  const adjacentMachineNetTotal3Near2 = sumAdjacentMachineDifferenceRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    3,
    currentMachineName,
    1,
  );
  const adjacentMachineNetTotal3Distance2 = sumAdjacentMachineDifferenceRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    3,
    currentMachineName,
    2,
  );
  const adjacentMachineRowCount3Near2 = countAdjacentMachineRows(
    businessDates,
    dateIndex,
    row,
    rowsByDate,
    config,
    3,
    currentMachineName,
    2,
  );
  const adjacentMachineAverageDifference3Near2 =
    adjacentMachineRowCount3Near2 > 0 ? adjacentMachineNetTotal3Distance2 / adjacentMachineRowCount3Near2 : 0;
  const todayDifference = readHuntScoreDifferenceValue(row, config.differenceMode, currentMachineName);
  const previousGames = readNumber(row?.games_count) ?? 0;
  const previousRbCount = readNumber(row?.rb_count) ?? 0;
  const previousBonusTotal = (readNumber(row?.bb_count) ?? 0) + previousRbCount;
  const recentThreeBigShowDays = countBigShowRows(recentThreeRows);
  const recentSevenBigShowDays = countBigShowRows(recentSevenRows);
  const recentThreeStrictHighContentDays = countStrictHighContentRows(recentThreeRows);
  const recentSevenStrictHighContentDays = countStrictHighContentRows(recentSevenRows);
  const recentThreeBigWin1200Count = countDifferenceAtLeastRows(recentThreeRows, 1200);
  const recentFiveBigWin1000Count = countDifferenceAtLeastRows(recentFiveRows, 1000);
  const recentFiveBigWin1200Count = countDifferenceAtLeastRows(recentFiveRows, 1200);
  const recentFiveBadMinus800Count = recentFiveRows.filter(
    (windowRow) => readNumber(windowRow?.differenceValue) <= -800,
  ).length;
  const recentSevenBigWin2500Count = countDifferenceAtLeastRows(recentSevenRows, 2500);
  const recentFourteenCombinedLe140Count = recentFourteenRows.filter(
    (windowRow) => calculateCombinedDenominatorFromWindowRow(windowRow) <= 140,
  ).length;
  const recentTwentyOneMinDifference =
    recentTwentyOneRows.length > 0
      ? Math.min(...recentTwentyOneRows.map((windowRow) => readNumber(windowRow?.differenceValue)))
      : 0;
  const recentTwentyOneMaxDifference =
    recentTwentyOneRows.length > 0
      ? Math.max(...recentTwentyOneRows.map((windowRow) => readNumber(windowRow?.differenceValue)))
      : 0;
  const machineNetTotalSinceBigWin1000 = (() => {
    let lastBigWinIndex = -1;
    for (let index = historyWindowRows.length - 1; index >= 0; index -= 1) {
      if (readNumber(historyWindowRows[index]?.differenceValue) >= 1000) {
        lastBigWinIndex = index;
        break;
      }
    }
    return historyWindowRows
      .slice(lastBigWinIndex + 1)
      .reduce((total, windowRow) => total + readNumber(windowRow?.differenceValue), 0);
  })();
  const recentThreeRawDifferenceTotal = sumRawDifferenceValues(recentThreeRows);
  const recentFiveRawDifferenceTotal = sumRawDifferenceValues(recentFiveRows);
  const recentThreeRawDifferenceCount = countRawDifferenceValueRows(recentThreeRows);
  const recentFiveRawDifferenceCount = countRawDifferenceValueRows(recentFiveRows);
  const previousRawDifferenceValue = readWindowRawDifferenceValue(metricWindowRows.at(-1));
  const rawDifferenceLosingStreak = calculateCurrentRawDifferenceLosingStreak(historyWindowRows);
  const recentSevenGoldShowDays = countDifferenceAtLeastRows(recentSevenRows, 1500);
  const recentFourteenGoldShowDays = countDifferenceAtLeastRows(recentFourteenRows, 1341);
  const recentSevenBigShow1500Games2000Count = recentSevenRows.filter(
    (windowRow) => readWindowField(windowRow, "games") >= 2000 && readNumber(windowRow?.differenceValue) >= 1500,
  ).length;
  const recentThreeShow1000Games1500Count = recentThreeRows.filter(
    (windowRow) => readWindowField(windowRow, "games") >= 1500 && readNumber(windowRow?.differenceValue) >= 1000,
  ).length;
  const previousBigShow = previousGames >= 5000 && todayDifference >= 1000;

  return {
    machineName: currentMachineName,
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
    recentSevenNetTotal,
    recentTenNetTotal,
    recentFourteenNetTotal,
    recentTwentyOneNetTotal,
    recentTwentyEightNetTotal,
    recentFortyTwoNetTotal,
    recentFiftySixNetTotal,
    shortSevenSinkStayDays,
    shortThreeSinkStayDays,
    recentSevenMinus1200StayDays,
    recentSevenMinus2000StayDays,
    recentSevenMinus3000StayDays,
    recentSevenMinus1500Games9000StayDays,
    recentThreeMinus1000StayDays,
    recentThreeMinus1700StayDays,
    recentFiveMinus1000StayDays,
    recentFiveMinus1500StayDays,
    recentFiveMinus2000StayDays,
    recentFiveMinus3000StayDays,
    recentFiveMinus3500StayDays,
    recentSevenMinus1500StayDays,
    recentFiveMinus500StayDays,
    recentTenMinus3000StayDays,
    recentTenMinus2500StayDays,
    recentTenMinus5225StayDays,
    recentFourteenMinus500StayDays,
    recentFourteenMinus1500StayDays,
    recentFourteenMinus1800StayDays,
    recentFourteenMinus2000StayDays,
    recentFourteenMinus2500StayDays,
    recentFourteenMinus3000StayDays,
    recentFourteenMinus4000StayDays,
    recentFourteenMinus5000StayDays,
    recentFourteenMinus3218StayDays,
    recentFourteenNegativeStayDays,
    recentTwentyOneMinus1500StayDays,
    recentTwentyOneMinus2000StayDays,
    recentTwentyOneMinus3000StayDays,
    recentTwentyOneMinus4000StayDays,
    recentTwentyOneMinus5000StayDays,
    recentTwentyOneMinus11333StayDays,
    recentFiveAngleMinus80StayDays,
    recentFourLossDays,
    recentSevenLossDays,
    recentFourteenLossDays,
    recentFiveWinDays,
    recentSevenWinDays,
    recentFourteenWinDays,
    recentSevenNonPositiveDays,
    recentFourteenNonPositiveDays,
    recentFourPositiveCount,
    recentThreeLowGames1500Count,
    recentSevenLowGames500Count,
    recentFiveMaxWin,
    recentFiveBadMinus800Count,
    recentFourteenCombinedLe140Count,
    recentTwentyOneMinDifference,
    recentTwentyOneMaxDifference,
    machineNetTotalSinceBigWin1000,
    compensationRate: lossAbsTotal === 0 ? 999 : winAbsTotal / lossAbsTotal,
    maxWin,
    todayDifference,
    previousDifference: previousWindowRow?.differenceValue ?? 0,
    previousGames,
    previousBbCount: readNumber(row?.bb_count) ?? 0,
    previousRbCount,
    previousBonusTotal,
    todaySetting,
    settingSampleCount,
    lowSettingCount,
    highSettingCount,
    highSettingEstimateCount,
    highSettingCandidateCount,
    settingFiveCount,
    strongHighSettingCandidateCount,
    recentFiveHighSettingCandidateCount,
    recentSevenHighSettingCandidateCount,
    recentSevenHighSettingEstimateCount,
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
    daysSinceHistoryRbLight,
    highSettingStreak: calculateCurrentHighSettingStreak(metricWindowRows),
    highSettingEstimateStreak: calculateCurrentHighSettingEstimateStreak(metricWindowRows),
    highSettingCandidateStreak: calculateCurrentHighSettingCandidateStreak(metricWindowRows),
    historyLosingStreak: calculateCurrentLosingStreak(historyWindowRows),
    nonPositiveStreak: calculateCurrentNonPositiveStreak(historyWindowRows),
    machineHighContentStreak: calculateCurrentMachineContentStreak(
      metricWindowRows,
      isHistoryMachineHighContentWindowRow,
    ),
    machineGoodContentStreak: calculateCurrentMachineContentStreak(
      metricWindowRows,
      isHistoryMachineGoodContentWindowRow,
    ),
    machineLowContentStreak: calculateCurrentMachineContentStreak(
      metricWindowRows,
      isHistoryMachineLowContentWindowRow,
    ),
    machineWeakContentStreak: calculateCurrentMachineContentStreak(
      metricWindowRows,
      isHistoryMachineWeakContentWindowRow,
    ),
    recentThreeHighSettingCount,
    recentThreeHighSettingEstimateCount,
    recentThreeSettingFiveCount,
    recentThreeMachineHighContentCount,
    recentThreeMachineGoodContentCount,
    recentFiveMachineHighContentCount,
    recentSevenMachineHighContentCount,
    recentSevenMachineShowCount,
    recentTenMachineHighContentCount,
    recentFourteenMachineHighContentCount,
    recentTwentyOneMachineHighContentCount,
    recentThirtyMachineHighContentCount,
    recentTwentyEightRbLightCount,
    adjacentMachineHighContentCount3,
    adjacentMachineHighContentCount3Near2,
    adjacentMachineShowCount3Near2,
    adjacentMachineAverageDifference3Near2,
    recentSevenMachineGoodContentCount,
    recentThreeMachineLowContentCount,
    recentFiveMachineLowContentCount,
    recentSevenMachineLowContentCount,
    recentFiveMachineWeakContentCount,
    recentSevenMachineWeakContentCount,
    recentFourteenMachineGoodContentCount,
    recentTwentyOneMachineGoodContentCount,
    recentThreeMachineStrongHighContentCount,
    recentSevenMachineStrongBonusCount,
    recentFourteenMachineStrongHighContentCount,
    previousMachineHighContent,
    previousMachineGoodContent,
    previousMachineWeakContent,
    previousMachineStrongHighContent,
    previousMachineSettingFivePlusProbability,
    recentThreeMachineSettingFivePlusProbabilityAverage,
    recentFiveMachineSettingFivePlusProbabilityAverage,
    recentSevenMachineSettingFivePlusProbabilityAverage,
    recentTenMachineSettingFivePlusProbabilityAverage,
    recentFourteenMachineSettingFivePlusProbabilityAverage,
    recentTwentyOneMachineSettingFivePlusProbabilityAverage,
    recentThreeBigWin1200Count,
    recentFiveBigWin1000Count,
    daysSinceMachineHighContent,
    daysSinceMachineGoodContent,
    daysSinceMachineStrongHighContent,
    daysSinceMachineBigWin1000,
    daysSinceMachineBigWin1500,
    daysSinceMachineRb300,
    gamesTotal,
    averageGames: metricWindowRows.length > 0 ? gamesTotal / metricWindowRows.length : 0,
    recentTwoGamesTotal,
    recentThreeGamesTotal,
    recentFiveGamesTotal,
    recentSevenGamesTotal,
    recentTenGamesTotal,
    recentFourteenGamesTotal,
    recentTwentyOneGamesTotal,
    recentTwentyEightGamesTotal,
    recentThirtyGamesTotal,
    recentFortyTwoGamesTotal,
    recentFiftySixGamesTotal,
    recentTwoBonusTotal,
    recentThreeBonusTotal,
    recentFiveBonusTotal,
    recentTenBonusTotal,
    recentTwentyOneBonusTotal,
    recentTwoRbTotal,
    recentThreeRbTotal,
    recentFiveRbTotal,
    recentSevenRbTotal,
    recentTenRbTotal,
    recentFourteenRbTotal,
    recentTwentyOneRbTotal,
    recentSevenBbTotal,
    recentTenBbTotal,
    recentFourteenBbTotal,
    recentTwoSettingAverage,
    recentFiveSettingAverage,
    windowSettingAverage,
    recentSevenBigWin2500Count,
    historyRowCount,
    targetRangeHistoryRowCount,
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
    previousAdjacentMachineHighContentCount,
    previousAdjacentMachineHighContentCountNear2,
    previousAdjacentMachineStrongHighContentCount,
    previousAdjacentMachineGoodContentCount,
    previousAdjacentMachineWeakContentCount,
    previousAdjacentMachineBigWin1000Count,
    previousAdjacentMachineBigWin1500Count,
    previousAdjacentMachineNetTotal,
    previousAdjacentMachineRowCount,
    previousAdjacentMachineAverageDifference,
    previousAdjacentMachineNetTotalNear2,
    previousOtherMachineHighContentCount,
    sameMachineOrderCount,
    sameMachinePositionFromLeft,
    sameMachinePositionFromRight,
    sameMachinePreviousNetTotal,
    adjacentMachineHighContentCount7,
    adjacentMachineHighContentCount14,
    adjacentMachineHighContentCount7Near2,
    adjacentMachineGoodContentCount7Near2,
    adjacentMachineHighContentCount14Near2,
    otherSameMachineHighContentCount7,
    adjacentMachineBigWin1000Count7Near2,
    adjacentMachineBigWin1000Count14,
    adjacentMachineRbDenominator7,
    adjacentMachineCombinedDenominator7,
    adjacentMachineNetTotal3,
    adjacentMachineNetTotal3Near2,
    adjacentMachineNetTotal5,
    adjacentMachineNetTotal5Near2,
    adjacentMachineNetTotal7,
    adjacentMachineNetTotal7Near2,
    adjacentMachineNetTotal14,
    adjacentMachineNetTotal14Near2,
    historyNetTotal,
    historyPositiveDays,
    recentThirtyNetTotal,
    recentThirtyMinus2700StayDays,
    recentThreeBigShowDays,
    recentSevenBigShowDays,
    recentFiveBigWin1200Count,
    recentThreeRawDifferenceTotal,
    recentFiveRawDifferenceTotal,
    recentThreeRawDifferenceCount,
    recentFiveRawDifferenceCount,
    previousRawDifferenceValue,
    rawDifferenceLosingStreak,
    recentThreeStrictHighContentDays,
    recentSevenStrictHighContentDays,
    recentSevenGoldShowDays,
    recentFourteenGoldShowDays,
    recentSevenBigShow1500Games2000Count,
    recentThreeShow1000Games1500Count,
    previousBigShow,
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
  const metricsList = validCandidates.map((candidate) => candidate.metrics);
  let machineActiveSlotCountByName = null;
  let machineHighSettingCandidateRateByName = null;
  const context = {
    baseDate,
    nextBusinessDate,
    windowDays: config.windowDays ?? DEFAULT_HUNT_SCORE_WINDOW_DAYS,
    metricsList,
    get machineActiveSlotCountByName() {
      if (machineActiveSlotCountByName === null) {
        machineActiveSlotCountByName = buildMachineActiveSlotCountMap(dateRows, config);
      }
      return machineActiveSlotCountByName;
    },
    get machineHighSettingCandidateRateByName() {
      if (machineHighSettingCandidateRateByName === null) {
        machineHighSettingCandidateRateByName = buildMachineHighSettingCandidateRateMap(
          businessDates,
          dateIndex,
          rowsByDate,
          settingDefinitionCache,
          config,
        );
      }
      return machineHighSettingCandidateRateByName;
    },
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
        machineEvaluationMetrics: candidate.metrics,
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
    { historyWindowDays: options?.machineEvaluationHistoryWindowDays },
  );
  const effectiveTargetRows = filterExcludedHuntScoreRows(targetRows, config);
  const effectiveAllStoreRows = filterExcludedHuntScoreRows(allStoreRows, config);

  const businessDates = buildBusinessDates(effectiveAllStoreRows, effectiveTargetRows);
  if (businessDates.length === 0) {
    return [];
  }

  const businessDateSet = new Set(businessDates);
  const { rowsByCandidateKey, rowsByDate } = buildSourceMaps(effectiveTargetRows, businessDateSet, config);
  const settingDefinitionCache = new Map();
  const targetDate = String(options?.targetDate ?? "").trim();
  const targetDateRange = options?.targetDateRange ?? null;
  const targetStartDate = String(targetDateRange?.startDate ?? "").trim();
  const targetEndDate = String(targetDateRange?.endDate ?? "").trim();
  if (targetStartDate) {
    config.targetRangeStartDate = targetStartDate;
  }
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
