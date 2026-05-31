export function SortableTableHeader({
  children,
  columnIndex,
  type = "number",
  initialDirection = "desc",
  className = "",
  activeDirection = null,
  onSort,
  title = undefined,
}) {
  const direction = activeDirection === "asc" || activeDirection === "desc"
    ? activeDirection
    : null;

  return (
    <th
      className={className || undefined}
      data-sort-index={columnIndex}
      data-sort-type={type}
      data-sort-initial-direction={initialDirection}
      data-sort-direction={direction || undefined}
      aria-sort={direction ? (direction === "asc" ? "ascending" : "descending") : "none"}
      title={title}
    >
      <button type="button" className="sortableTableHeaderButton" onClick={onSort}>
        <span>{children}</span>
        <span className="sortableTableHeaderIndicator" aria-hidden="true">
          {direction ? (direction === "asc" ? "↑" : "↓") : ""}
        </span>
      </button>
    </th>
  );
}
