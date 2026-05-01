function formatDataSourceLabel(source) {
  return "JSON";
}

export function DataSourceLabel({ source }) {
  return (
    <p className="dataSourceLabel">
      表示元: {formatDataSourceLabel(source)}
    </p>
  );
}
