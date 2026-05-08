export function SortableTableHeader({
  children,
  columnIndex,
  type = "number",
  initialDirection = "desc",
  className = "",
}) {
  return (
    <th
      className={className || undefined}
      data-sort-index={columnIndex}
      data-sort-type={type}
      data-sort-initial-direction={initialDirection}
      aria-sort="none"
    >
      <button type="button" className="sortableTableHeaderButton">
        <span>{children}</span>
        <span className="sortableTableHeaderIndicator" aria-hidden="true" />
      </button>
    </th>
  );
}
