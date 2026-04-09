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

export function MachineComparison({ machineName, slotNumbers, dateRows }) {
  if (dateRows.length === 0) {
    return (
      <section className="statusPanel">
        <h2>条件に合う日付がありません</h2>
        <p>別の末尾に切り替えるか、すべて表示へ戻してください。</p>
      </section>
    );
  }

  return (
    <>
      <section className="tablePanel desktopOnly">
        <div className="tablePanelHeader">
          <div>
            <p className="sectionLabel">台データ比較</p>
            <h2 className="tablePanelTitle">{machineName}</h2>
          </div>
          <CsvExportButton
            machineName={machineName}
            slotNumbers={slotNumbers}
            dateRows={dateRows}
            metrics={METRICS}
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

      <section className="mobileOnly mobileStack">
        {dateRows.map((row) => (
          <article key={row.date} className="dayCard">
            <div className="dayCardHead">
              <h2 className="dayCardTitle">{formatCompactDate(row.date)}</h2>
              <span className="cardBadge">{slotNumbers.length}台</span>
            </div>
            <div className="slotCardGrid">
              {slotNumbers.map((slotNumber) => {
                const record = row.recordsBySlot[slotNumber] ?? null;
                return (
                  <section key={`${row.date}-${slotNumber}`} className="slotCard">
                    <h3 className="slotCardTitle">{slotNumber}番台</h3>
                    <dl className="metricList">
                      {METRICS.map((metric) => {
                        const value = record?.[metric.key];
                        const toneClass = metric.tone ? valueToneClass(metric.key, value) : "";
                        return (
                          <div key={`${row.date}-${slotNumber}-${metric.key}`}>
                            <dt>{metric.label}</dt>
                            <dd className={toneClass}>{metric.render(value)}</dd>
                          </div>
                        );
                      })}
                    </dl>
                  </section>
                );
              })}
            </div>
          </article>
        ))}
      </section>
    </>
  );
}
