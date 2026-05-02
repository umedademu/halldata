function formatDataSourceLabel(source) {
  return "Cloudflare R2 JSON";
}

export function DataSourceLabel({ source }) {
  return (
    <p className="dataSourceLabel">
      表示元: {formatDataSourceLabel(source)}
    </p>
  );
}
