import {
  formatAverageGames,
  formatCompactDate,
  formatPercent,
  formatRatio,
  formatSignedNumber,
  valueToneClass,
} from "../lib/format";
import { CsvExportButton } from "./csv-export-button";

const METRICS = [
  { key: "difference_value", label: "差枚", render: formatSignedNumber, tone: true },
  { key: "games_count", label: "G数", render: formatAverageGames },
  { key: "payout_rate", label: "出率", render: formatPercent, tone: true },
  { key: "bb_count", label: "BB", render: formatAverageGames },
  { key: "rb_count", label: "RB", render: formatAverageGames },
  { key: "combined_ratio_text", label: "合成", render: formatRatio },
  { key: "bb_ratio_text", label: "BB率", render: formatRatio },
  { key: "rb_ratio_text", label: "RB率", render: formatRatio },
];

function buildCsvRows(slotNumbers, dateRows) {
  const headerRow1 = ["日付"];
  const headerRow2 = [""];

  for (const slotNumber of slotNumbers) {
    for (let i = 0; i < METRICS.length; i++) {
      headerRow1.push(i === 0 ? `${slotNumber}番台` : "");
      headerRow2.push(METRICS[i].label);
    }
  }

  const dataRows = dateRows.map((row) => {
    const cells = [row.date];
    for (const slotNumber of slotNumbers) {
      const record = row.recordsBySlot[slotNumber] ?? null;
      for (const metric of METRICS) {
        const value = record?.[metric.key];
        cells.push(metric.render(value));
      }
    }
    return cells;
  });

  return [headerRow1, headerRow2, ...dataRows];
}

export function MachineComparison({ machineName, slotNumbers, dateRows }) {
  if (dateRows.length === 0) {
    return (
      <section className="statusPanel">
        <h2>条件に合う日付がありません</h2>
        <p>別の末尾に切り替えるか、すべて表示へ戻してください。</p>
      </section>
    );
  }

  const csvRows = buildCsvRows(slotNumbers, dateRows);

  return (
    <section className="tablePanel">
      <div className="tablePanelHeader">
        <div>
          <p className="sectionLabel">台データ比較</p>
          <h2 className="tablePanelTitle">{machineName}</h2>
        </div>
        <CsvExportButton
          machineName={machineName}
          csvRows={csvRows}
        />
      </div>
      <div className="tableScroller">
        <table className="matrixTable">
          <thead>
            <tr>
              <th rowSpan={2} className="dateHeaderCell">
                日付
              </th>
              {slotNumbers.map((slotNumber) => (
                <th key={slotNumber} colSpan={METRICS.length} className="slotHeader">
                  {slotNumber}番台
                </th>
              ))}
            </tr>
            <tr>
              {slotNumbers.flatMap((slotNumber) =>
                METRICS.map((metric) => (
                  <th key={`${slotNumber}-${metric.key}`} className="metricHeader">
                    {metric.label}
                  </th>
                )),
              )}
            </tr>
          </thead>
          <tbody>
            {dateRows.map((row) => (
              <tr key={row.date}>
                <th className="dateCell">{formatCompactDate(row.date)}</th>
                {slotNumbers.flatMap((slotNumber) =>
                  METRICS.map((metric) => {
                    const record = row.recordsBySlot[slotNumber] ?? null;
                    const value = record?.[metric.key];
                    const toneClass = metric.tone ? valueToneClass(metric.key, value) : "";
                    return (
                      <td key={`${row.date}-${slotNumber}-${metric.key}`} className={toneClass}>
                        {metric.render(value)}
                      </td>
                    );
                  }),
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
