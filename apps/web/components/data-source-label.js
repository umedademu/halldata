function formatDataSourceLabel(source) {
  return source === "json" ? "JSON" : "Supabase";
}

export function DataSourceLabel({ source }) {
  return (
    <p className="dataSourceLabel">
      表示元: {formatDataSourceLabel(source)}
    </p>
  );
}
