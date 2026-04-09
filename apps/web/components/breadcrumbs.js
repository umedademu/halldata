import Link from "next/link";

export function Breadcrumbs({ items }) {
  return (
    <nav aria-label="パンくず" className="breadcrumbs">
      {items.map((item) => (
        <span key={`${item.label}-${item.href ?? "current"}`} className="breadcrumbItem">
          {item.href ? <Link href={item.href}>{item.label}</Link> : <span>{item.label}</span>}
        </span>
      ))}
    </nav>
  );
}
