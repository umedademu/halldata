import Link from "next/link";

export const metadata = {
  title: "ページが見つかりません",
};

export default function NotFound() {
  return (
    <main className="pageStack">
      <section className="heroPanel">
        <div className="heroCopy">
          <p className="eyebrow">Supabase Viewer</p>
          <h1 className="pageTitle">ページが見つかりません</h1>
          <p className="leadText">
            店舗または機種の指定が見つからないため、一覧ページへ戻ってください。
          </p>
        </div>
      </section>
      <section className="statusPanel">
        <Link href="/" className="inlineAction">
          店舗一覧へ戻る
        </Link>
      </section>
    </main>
  );
}
