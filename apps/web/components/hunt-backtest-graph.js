"use client";

import { useEffect, useMemo, useState } from "react";

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

function normalizeGraphGroups(groups, fallbackPoints) {
  const normalizedGroups = Array.isArray(groups)
    ? groups
        .map((group) => ({
          key: String(group?.key ?? "").trim(),
          title: String(group?.title ?? "").trim(),
          points: Array.isArray(group?.points) ? group.points : [],
        }))
        .filter((group) => group.key && group.title)
    : [];

  if (normalizedGroups.length > 0) {
    return normalizedGroups;
  }

  return Array.isArray(fallbackPoints)
    ? [{ key: "all", title: "全合算", points: fallbackPoints }]
    : [];
}

function GraphGroupButtons({ groups, selectedKey, onSelect }) {
  if (groups.length <= 1) {
    return null;
  }

  return (
    <div className="backtestGraphToggleRow" aria-label="グラフの集計切替">
      {groups.map((group) => {
        const isActive = group.key === selectedKey;

        return (
          <button
            key={group.key}
            type="button"
            className={`metricToggleChip backtestGraphToggleButton ${
              isActive ? "metricToggleChipActive" : ""
            }`}
            aria-pressed={isActive}
            onClick={() => onSelect(group.key)}
          >
            {group.title}
          </button>
        );
      })}
    </div>
  );
}

export function HuntBacktestGraph({ points, groups }) {
  const graphGroups = useMemo(() => normalizeGraphGroups(groups, points), [groups, points]);
  const [selectedGroupKey, setSelectedGroupKey] = useState(graphGroups[0]?.key ?? "");

  useEffect(() => {
    if (graphGroups.length === 0) {
      setSelectedGroupKey("");
      return;
    }
    if (!graphGroups.some((group) => group.key === selectedGroupKey)) {
      setSelectedGroupKey(graphGroups[0].key);
    }
  }, [graphGroups, selectedGroupKey]);

  if (graphGroups.length === 0) {
    return null;
  }

  const selectedGroup =
    graphGroups.find((group) => group.key === selectedGroupKey) ?? graphGroups[0];
  const selectedPoints = selectedGroup.points;

  if (!Array.isArray(selectedPoints) || selectedPoints.length === 0) {
    return (
      <section className="tablePanel">
        <div className="tablePanelHeader">
          <div>
            <h2 className="tablePanelTitle">差枚グラフ</h2>
          </div>
        </div>
        <GraphGroupButtons
          groups={graphGroups}
          selectedKey={selectedGroup.key}
          onSelect={setSelectedGroupKey}
        />
        <p className="filterPanelStatus">
          {selectedGroup.title}には、グラフに表示できる翌営業日実績がありません。
        </p>
      </section>
    );
  }

  let cumulativeDifferenceTotal = 0;
  const cumulativePoints = selectedPoints.map((point) => {
    cumulativeDifferenceTotal += point.differenceTotal;
    return {
      ...point,
      cumulativeDifferenceTotal,
    };
  });
  const chartWidth = Math.max(
    MIN_CHART_WIDTH,
    CHART_PADDING.left + CHART_PADDING.right + Math.max(selectedPoints.length - 1, 1) * POINT_GAP,
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
          <h2 className="tablePanelTitle">差枚グラフ</h2>
        </div>
      </div>
      <GraphGroupButtons
        groups={graphGroups}
        selectedKey={selectedGroup.key}
        onSelect={setSelectedGroupKey}
      />
      <div className="tableScroller backtestGraphScroller">
        <svg
          viewBox={`0 0 ${chartWidth} ${CHART_HEIGHT}`}
          className="backtestGraphSvg"
          role="img"
          aria-label="バックテストの差枚グラフ"
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
                <circle cx={point.x} cy={point.y} r="3" className="backtestGraphPoint">
                  <title>
                    {`${point.date} 当日差枚 ${formatPlainSignedNumber(point.differenceTotal)} 累積差枚 ${formatPlainSignedNumber(point.cumulativeDifferenceTotal)} 集計 ${point.actualRowCount}台`}
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
