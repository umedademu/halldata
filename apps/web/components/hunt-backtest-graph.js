import { formatShortDate, formatSignedNumber } from "../lib/format";

const CHART_HEIGHT = 360;
const CHART_PADDING = {
  top: 24,
  right: 24,
  bottom: 56,
  left: 72,
};
const MIN_CHART_WIDTH = 720;
const POINT_GAP = 18;
const Y_TICK_COUNT = 5;

function formatPlainSignedNumber(value) {
  if (!Number.isFinite(value)) {
    return "-";
  }

  const roundedValue = Math.round(value);
  return `${roundedValue >= 0 ? "+" : ""}${roundedValue}`;
}

function calculateRange(values) {
  const rawMin = Math.min(...values, 0);
  const rawMax = Math.max(...values, 0);
  const spread = rawMax - rawMin;
  const padding = spread === 0 ? Math.max(Math.abs(rawMax) * 0.2, 500) : Math.max(spread * 0.12, 300);

  return {
    min: rawMin - padding,
    max: rawMax + padding,
  };
}

function buildYAxisTicks(minValue, maxValue) {
  return Array.from({ length: Y_TICK_COUNT }, (_, index) => {
    const ratio = index / (Y_TICK_COUNT - 1);
    return maxValue - (maxValue - minValue) * ratio;
  });
}

function buildPathText(points) {
  if (points.length === 0) {
    return "";
  }

  return points
    .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`)
    .join(" ");
}

export function HuntBacktestGraph({ points }) {
  if (!Array.isArray(points) || points.length === 0) {
    return null;
  }

  let cumulativeDifferenceTotal = 0;
  const cumulativePoints = points.map((point) => {
    cumulativeDifferenceTotal += point.differenceTotal;
    return {
      ...point,
      cumulativeDifferenceTotal,
    };
  });
  const chartWidth = Math.max(
    MIN_CHART_WIDTH,
    CHART_PADDING.left + CHART_PADDING.right + Math.max(points.length - 1, 1) * POINT_GAP,
  );
  const innerWidth = chartWidth - CHART_PADDING.left - CHART_PADDING.right;
  const innerHeight = CHART_HEIGHT - CHART_PADDING.top - CHART_PADDING.bottom;
  const values = cumulativePoints.map((point) => point.cumulativeDifferenceTotal);
  const range = calculateRange(values);
  const yTicks = buildYAxisTicks(range.min, range.max);
  const labelStep = Math.max(1, Math.ceil(cumulativePoints.length / 8));
  const plotPoints = cumulativePoints.map((point, index) => {
    const x =
      cumulativePoints.length === 1
        ? CHART_PADDING.left + innerWidth / 2
        : CHART_PADDING.left + (innerWidth * index) / (cumulativePoints.length - 1);
    const y =
      CHART_PADDING.top +
      ((range.max - point.cumulativeDifferenceTotal) / (range.max - range.min)) * innerHeight;

    return {
      ...point,
      x,
      y,
    };
  });
  const linePath = buildPathText(plotPoints);
  const zeroLineY =
    range.min <= 0 && range.max >= 0
      ? CHART_PADDING.top + ((range.max - 0) / (range.max - range.min)) * innerHeight
      : null;

  return (
    <section className="tablePanel">
      <div className="tablePanelHeader">
        <div>
          <p className="sectionLabel">差枚推移</p>
          <h2 className="tablePanelTitle">累積差枚折れ線</h2>
        </div>
      </div>
      <p className="backtestGraphLead">
        横軸は翌営業日の日付、縦軸はその時点までの累積差枚です。条件の期間指定自体は狙い度を出した日を基準にしています。
      </p>
      <div className="tableScroller backtestGraphScroller">
        <svg
          viewBox={`0 0 ${chartWidth} ${CHART_HEIGHT}`}
          className="backtestGraphSvg"
          role="img"
          aria-label="バックテストの累積差枚推移"
        >
          <rect
            x={CHART_PADDING.left}
            y={CHART_PADDING.top}
            width={innerWidth}
            height={innerHeight}
            className="backtestGraphPlot"
          />

          {yTicks.map((tickValue) => {
            const y =
              CHART_PADDING.top + ((range.max - tickValue) / (range.max - range.min)) * innerHeight;

            return (
              <g key={`tick-${tickValue}`}>
                <line
                  x1={CHART_PADDING.left}
                  y1={y}
                  x2={chartWidth - CHART_PADDING.right}
                  y2={y}
                  className="backtestGraphGrid"
                />
                <text x={CHART_PADDING.left - 10} y={y + 4} className="backtestGraphAxisText">
                  {formatSignedNumber(tickValue)}
                </text>
              </g>
            );
          })}

          {zeroLineY !== null ? (
            <line
              x1={CHART_PADDING.left}
              y1={zeroLineY}
              x2={chartWidth - CHART_PADDING.right}
              y2={zeroLineY}
              className="backtestGraphZeroLine"
            />
          ) : null}

          <path d={linePath} className="backtestGraphLine" />

          {plotPoints.map((point, index) => {
            const shouldShowLabel =
              index === 0 || index === plotPoints.length - 1 || index % labelStep === 0;

            return (
              <g key={point.date}>
                <circle cx={point.x} cy={point.y} r="4.5" className="backtestGraphPoint">
                  <title>
                    {`${point.date} 当日差枚 ${formatPlainSignedNumber(point.differenceTotal)} 累積差枚 ${formatPlainSignedNumber(point.cumulativeDifferenceTotal)} 条件一致 ${point.matchedRowCount}台 実績 ${point.actualRowCount}台`}
                  </title>
                </circle>
                {shouldShowLabel ? (
                  <>
                    <line
                      x1={point.x}
                      y1={CHART_HEIGHT - CHART_PADDING.bottom}
                      x2={point.x}
                      y2={CHART_HEIGHT - CHART_PADDING.bottom + 6}
                      className="backtestGraphTick"
                    />
                    <text
                      x={point.x}
                      y={CHART_HEIGHT - 16}
                      className="backtestGraphAxisText backtestGraphDateText"
                    >
                      {formatShortDate(point.date)}
                    </text>
                  </>
                ) : null}
              </g>
            );
          })}
        </svg>
      </div>
    </section>
  );
}
