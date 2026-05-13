import Link from "next/link";

export const metadata = {
  title: "更新情報",
};

const UPDATE_ITEMS = [
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
