import Link from "next/link";

export const metadata = {
  title: "更新情報",
};

const UPDATE_ITEMS = [
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
