"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import {
  buildMachineEvaluationCookieOverrides,
  encodeMachineEvaluationSettingsCookieValue,
  getMachineEvaluationCookieName,
} from "../lib/machine-evaluation";
import {
  formatCompactDate,
  formatNumber,
  formatPercent,
  formatSignedNumber,
} from "../lib/format";

const COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24 * 365;

function normalizeText(value) {
  return String(value ?? "").trim();
}

function normalizeRows(rows) {
  return (Array.isArray(rows) ? rows : []).map((row) => ({
    machineKey: normalizeText(row?.machineKey),
    machineName: normalizeText(row?.machineName),
    hasDefinition: Boolean(row?.hasDefinition),
    logicKey: normalizeText(row?.logicKey),
    conditionKey: normalizeText(row?.conditionKey),
    defaultLogicKey: normalizeText(row?.defaultLogicKey),
    defaultConditionKey: normalizeText(row?.defaultConditionKey),
    logicOptions: Array.isArray(row?.logicOptions) ? row.logicOptions : [],
    conditionOptions: Array.isArray(row?.conditionOptions) ? row.conditionOptions : [],
  }));
}

function normalizeMachines(machines) {
  return (Array.isArray(machines) ? machines : []).map((machine) => ({
    machineName: normalizeText(machine?.machineName),
    latestDate: machine?.latestDate ?? null,
    slotCount: machine?.slotCount ?? null,
    latestAverageDifference: machine?.latestAverageDifference ?? null,
    latestAveragePayout: machine?.latestAveragePayout ?? null,
    canVerify: Boolean(machine?.canVerify),
    isCombinedMachineGroup: Boolean(machine?.isCombinedMachineGroup),
    isCombinedMachineChild: Boolean(machine?.isCombinedMachineChild),
  }));
}

function findOption(options, optionKey) {
  return options.find((option) => option.key === optionKey) ?? null;
}

function buildOptionTitle(option) {
  return [option?.name, option?.backtestLabel].filter(Boolean).join(" / ");
}

function buildNextRows(rows, machineKey, values) {
  return rows.map((row) => {
    if (row.machineKey !== machineKey) {
      return row;
    }
    const nextRow = {
      ...row,
      ...values,
    };
    if (!nextRow.logicKey) {
      return {
        ...nextRow,
        conditionKey: "",
      };
    }
    const conditionExists = nextRow.conditionOptions.some(
      (option) => option.key === nextRow.conditionKey,
    );
    return {
      ...nextRow,
      conditionKey: conditionExists ? nextRow.conditionKey : "",
    };
  });
}

function LogicSelect({ row, onChange }) {
  return (
    <select
      className="machineEvaluationTableSelect"
      value={row.logicKey}
      disabled={!row.hasDefinition}
      title={findOption(row.logicOptions, row.logicKey)?.name || "未設定"}
      onChange={(event) => onChange(event.target.value)}
    >
      {row.logicOptions.map((option) => (
        <option key={option.key || "none"} value={option.key}>
          {option.name}
        </option>
      ))}
    </select>
  );
}

function ConditionPicker({ row, onChange }) {
  const selectedCondition = findOption(row.conditionOptions, row.conditionKey);
  const isDisabled = !row.logicKey || !row.hasDefinition;
  const title = buildOptionTitle(selectedCondition) || "未設定";
  const label = selectedCondition?.name || "未設定";

  if (isDisabled) {
    return (
      <button
        type="button"
        className="machineEvaluationConditionSummary"
        title="未設定"
        disabled
      >
        <span>未設定</span>
      </button>
    );
  }

  return (
    <details className="machineEvaluationConditionPicker">
      <summary className="machineEvaluationConditionSummary" title={title}>
        <span>{label}</span>
      </summary>
      <div className="machineEvaluationConditionMenu">
        {row.conditionOptions.map((option) => (
          <button
            key={option.key || "none"}
            type="button"
            className={
              option.key === row.conditionKey
                ? "machineEvaluationConditionOption machineEvaluationConditionOptionActive"
                : "machineEvaluationConditionOption"
            }
            title={buildOptionTitle(option) || option.name}
            onClick={(event) => {
              event.currentTarget.closest("details")?.removeAttribute("open");
              onChange(option.key);
            }}
          >
            <span>{option.name}</span>
          </button>
        ))}
      </div>
    </details>
  );
}

export function MachineDirectoryTable({
  storeId,
  machines,
  machineEvaluationSettings,
  showHuntScoreColumns = false,
}) {
  const machineRows = useMemo(() => normalizeMachines(machines), [machines]);
  const initialSettings = useMemo(
    () => normalizeRows(machineEvaluationSettings),
    [machineEvaluationSettings],
  );
  const [settingsRows, setSettingsRows] = useState(initialSettings);

  useEffect(() => {
    setSettingsRows(initialSettings);
  }, [initialSettings]);

  const settingsByMachineName = useMemo(() => {
    const map = new Map();
    for (const row of settingsRows) {
      map.set(normalizeText(row.machineName), row);
    }
    return map;
  }, [settingsRows]);

  const persistSettings = (nextRows) => {
    if (!storeId) {
      return;
    }
    const cookieName = getMachineEvaluationCookieName(storeId);
    const cookieValue = encodeMachineEvaluationSettingsCookieValue(
      buildMachineEvaluationCookieOverrides(nextRows),
    );
    document.cookie = `${cookieName}=${cookieValue}; Max-Age=${COOKIE_MAX_AGE_SECONDS}; Path=/; SameSite=Lax`;
  };

  const updateSetting = (machineKey, values) => {
    const nextRows = buildNextRows(settingsRows, machineKey, values);
    setSettingsRows(nextRows);
    persistSettings(nextRows);
  };

  return (
    <section className="tablePanel directoryPanel">
      <div className="tableScroller directoryScroller">
        <table className="directoryTable">
          <thead>
            <tr>
              <th className="directoryNameHeader">機種</th>
              <th>最新日</th>
              <th>台数</th>
              {showHuntScoreColumns ? <th>検証</th> : null}
              <th>平均差枚</th>
              <th>平均出率</th>
              <th>機種別ロジック</th>
              <th>採用条件</th>
            </tr>
          </thead>
          <tbody>
            {machineRows.map((machine) => {
              const setting = settingsByMachineName.get(normalizeText(machine.machineName));
              const machineHref = `/stores/${storeId}/machines/${encodeURIComponent(machine.machineName)}`;
              const verificationHref = `${machineHref}/hunt-score-verification`;

              return (
                <tr
                  key={`${machine.machineName}-${machine.isCombinedMachineGroup ? "group" : "machine"}`}
                  className={
                    machine.isCombinedMachineGroup
                      ? "combinedMachineGroupRow"
                      : machine.isCombinedMachineChild
                        ? "combinedMachineChildRow"
                        : undefined
                  }
                >
                  <th
                    className={`directoryNameCell ${
                      machine.isCombinedMachineChild ? "directoryNameCellIndented" : ""
                    }`}
                  >
                    <Link href={machineHref} className="directoryPrimaryLink">
                      {machine.machineName}
                    </Link>
                  </th>
                  <td>{machine.latestDate ? formatCompactDate(machine.latestDate) : "-"}</td>
                  <td>{formatNumber(machine.slotCount)}</td>
                  {showHuntScoreColumns ? (
                    <td>
                      {machine.canVerify ? (
                        <Link
                          href={verificationHref}
                          className="machineVerificationLink"
                          title="ロジック検証"
                          aria-label={`${machine.machineName}のロジック検証`}
                        >
                          <span aria-hidden="true">検</span>
                        </Link>
                      ) : (
                        "-"
                      )}
                    </td>
                  ) : null}
                  <td>{formatSignedNumber(machine.latestAverageDifference)}</td>
                  <td>{formatPercent(machine.latestAveragePayout)}</td>
                  <td className="machineEvaluationControlCell">
                    {setting ? (
                      <LogicSelect
                        row={setting}
                        onChange={(logicKey) =>
                          updateSetting(setting.machineKey, {
                            logicKey,
                          })
                        }
                      />
                    ) : (
                      "-"
                    )}
                  </td>
                  <td className="machineEvaluationControlCell">
                    {setting ? (
                      <ConditionPicker
                        row={setting}
                        onChange={(conditionKey) =>
                          updateSetting(setting.machineKey, {
                            conditionKey,
                          })
                        }
                      />
                    ) : (
                      "-"
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
