import Link from "next/link";

export const metadata = {
  title: "更新情報",
};

const UPDATE_ITEMS = [
  {
    date: "2026-05-18",
    title: "その他機種を台数帯で分類",
    body: "店舗別バックテスト、狙い度ランキング、台データ比較の機種選択欄で、その他機種を設置台数に応じて10台以上、3~9台、2台以下に分けました。",
  },
  {
    date: "2026-05-18",
    title: "機種選択に設置台数を表示",
    body: "店舗別バックテスト、狙い度ランキング、台データ比較の機種選択欄で、略称の後ろにその店舗での設置台数を表示するようにしました。",
  },
  {
    date: "2026-05-18",
    title: "バックテスト結果の機種名を略称表示に変更",
    body: "店舗別バックテスト結果の機種名列を、機種選択欄と同じ略称表示にして横幅を抑えました。正式な機種名はホバーで確認できます。",
  },
  {
    date: "2026-05-18",
    title: "特定日合算のバックテスト結果を追加",
    body: "店舗別バックテストで、翌営業日の末尾条件と曜日条件に当たる日をまとめた特定日合算の結果表と差枚グラフ切替を追加しました。",
  },
  {
    date: "2026-05-18",
    title: "バックテスト結果の見出しを整理",
    body: "店舗別バックテストと店舗横断バックテストで、重複していたロジック名や説明文を外し、累積差枚のグラフ名を差枚グラフにしました。",
  },
  {
    date: "2026-05-18",
    title: "ミリオン東武練馬式を追加",
    body: "ミリオン東武練馬店スロット館向けに、Aパーク春日式・改を土台としてREGの重さ、凹みすぎ、低設定寄りの履歴を減点する狙い度ロジックを追加し、同店の初期ロジックにしました。",
  },
  {
    date: "2026-05-18",
    title: "バックテスト結果の台数表示を整理",
    body: "バックテストと店舗横断バックテストから条件一致台数を外し、翌営業日実績まで集計できた台数を集計台数として表示するようにしました。",
  },
  {
    date: "2026-05-17",
    title: "店舗横断バックテストにBB率とRB率を追加",
    body: "店舗横断バックテスト結果で、BBとRBの回数だけでなく、合計G数から見たBB率とRB率も確認できるようにしました。",
  },
  {
    date: "2026-05-17",
    title: "Aパーク春日式・改を100点満点化",
    body: "追加補正の重みは保ったまま、補正後の点数を0〜100へ変換し、画面に見える点数でそのまま順位付けするようにしました。",
  },
  {
    date: "2026-05-17",
    title: "Aパーク春日式・改を追加",
    body: "現行のAパーク春日式を土台に、直近の戻し圧力、浅い候補の減点、少台数機種の構造補正を加えた新しい狙い度ロジックを選べるようにしました。",
  },
  {
    date: "2026-05-17",
    title: "狙い度まわりの表示を整理",
    body: "狙い度ランキング、バックテスト、機種別、店舗横断の条件と結果を、順位・狙い度・次点差中心の表示に整理しました。",
  },
  {
    date: "2026-05-13",
    title: "バックテストを軽量化",
    body: "バックテストでも全機種基準の選択肢を外し、チェックした機種だけを読み込んで集計するようにしました。",
  },
  {
    date: "2026-05-13",
    title: "狙い度ランキングを軽量化",
    body: "全機種内ランキングを外し、機種名でチェックした対象だけを読み込んで計算するようにしました。全機種で見たい場合は全てのチェックをONにします。",
  },
  {
    date: "2026-05-13",
    title: "マイホールの並び替えに対応",
    body: "お気に入り店舗をドラッグして、使いやすい順番に並び替えられるようにしました。並び順は使っている端末のブラウザーに保存されます。",
  },
  {
    date: "2026-05-13",
    title: "マイホールを追加",
    body: "店舗一覧で星を押した店舗を、店舗一覧の上にまとめて表示できるようにしました。保存先は使っている端末のブラウザーです。",
  },
  {
    date: "2026-05-13",
    title: "上部ボタンを追加",
    body: "店舗選択、店舗横断バックテスト、更新情報を上部から切り替えられるようにしました。",
  },
  {
    date: "2026-05-13",
    title: "店舗横断バックテストへの導線を整理",
    body: "トップ画面内の個別リンクに頼らず、上部ボタンから主要画面へ移動できる形にしました。",
  },
];

export default function UpdatesPage() {
  return (
    <main className="pageStack">
      <section className="heroPanel">
        <div className="heroCopy">
          <h1 className="pageTitle pageTitleCompact">更新情報</h1>
          <p className="leadText">
            画面の変更点や使い勝手に関わる追加内容を確認できます。
          </p>
          <div className="heroLinks simpleHeroLinks">
            <Link href="/" className="externalLink">
              店舗選択へ戻る
            </Link>
          </div>
        </div>
      </section>

      <section className="tablePanel updatesPanel">
        <div className="tablePanelHeader">
          <div>
            <p className="sectionLabel">更新情報</p>
            <h2 className="tablePanelTitle">最近の変更</h2>
          </div>
        </div>
        <div className="updatesList">
          {UPDATE_ITEMS.map((item) => (
            <article className="updateItem" key={`${item.date}-${item.title}`}>
              <time dateTime={item.date} className="updateDate">
                {item.date}
              </time>
              <div className="updateBody">
                <h3>{item.title}</h3>
                <p>{item.body}</p>
              </div>
            </article>
          ))}
        </div>
      </section>
    </main>
  );
}
