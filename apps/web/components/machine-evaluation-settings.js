"use client";

import { useEffect, useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";

import {
  buildMachineEvaluationCookieOverrides,
  encodeMachineEvaluationSettingsCookieValue,
  getMachineEvaluationCookieName,
} from "../lib/machine-evaluation";

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

function findOption(options, optionKey) {
  return options.find((option) => option.key === optionKey) ?? null;
}

function rowHasChanged(row) {
  return row.logicKey !== row.defaultLogicKey || row.conditionKey !== row.defaultConditionKey;
}

export function MachineEvaluationSettings({ storeId, settings }) {
  const router = useRouter();
  const [isPending, startTransition] = useTransition();
  const initialRows = useMemo(() => normalizeRows(settings), [settings]);
  const [rows, setRows] = useState(initialRows);
  const hasChanged = rows.some(rowHasChanged);
  const configuredCount = rows.filter((row) => row.logicKey && row.conditionKey).length;

  useEffect(() => {
    setRows(initialRows);
  }, [initialRows]);

  if (!storeId || initialRows.length === 0) {
    return null;
  }

  const updateRow = (machineKey, values) => {
    setRows((currentRows) =>
      currentRows.map((row) => {
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
      }),
    );
  };

  const saveSettings = () => {
    const cookieName = getMachineEvaluationCookieName(storeId);
    const cookieValue = encodeMachineEvaluationSettingsCookieValue(
      buildMachineEvaluationCookieOverrides(rows),
    );
    document.cookie = `${cookieName}=${cookieValue}; Max-Age=${COOKIE_MAX_AGE_SECONDS}; Path=/; SameSite=Lax`;
    startTransition(() => {
      router.refresh();
    });
  };

  const resetSettings = () => {
    setRows(initialRows.map((row) => ({
      ...row,
      logicKey: row.defaultLogicKey,
      conditionKey: row.defaultConditionKey,
    })));
  };

  return (
    <section className="tablePanel directoryPanel machineEvaluationPanel">
      <div className="tablePanelHeader">
        <div>
          <p className="sectionLabel">機種別評価</p>
          <h2 className="tablePanelTitle">機種別ロジックと採用条件</h2>
          <p className="filterLead">
            設定済み: {configuredCount}機種。未設定の機種は共通の狙い度だけで扱います。
          </p>
        </div>
        <div className="machineEvaluationActions">
          <button
            type="button"
            className="huntLogicSaveButton machineEvaluationSubButton"
            disabled={!hasChanged || isPending}
            onClick={resetSettings}
          >
            既定に戻す
          </button>
          <button
            type="button"
            className="huntLogicSaveButton"
            disabled={!hasChanged || isPending}
            onClick={saveSettings}
          >
            {isPending ? "保存中" : "保存"}
          </button>
        </div>
      </div>
      <div className="tableScroller directoryScroller">
        <table className="directoryTable machineEvaluationSettingsTable">
          <thead>
            <tr>
              <th className="directoryNameHeader">機種</th>
              <th>機種別ロジック</th>
              <th>採用条件</th>
              <th>目安</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const selectedCondition = findOption(row.conditionOptions, row.conditionKey);
              return (
                <tr key={row.machineKey}>
                  <th className="directoryNameCell" title={row.machineName}>
                    {row.machineName}
                  </th>
                  <td>
                    <select
                      className="storeReserveInput machineEvaluationSelect"
                      value={row.logicKey}
                      disabled={!row.hasDefinition}
                      onChange={(event) =>
                        updateRow(row.machineKey, {
                          logicKey: event.target.value,
                        })
                      }
                    >
                      {row.logicOptions.map((option) => (
                        <option key={option.key || "none"} value={option.key}>
                          {option.name}
                        </option>
                      ))}
                    </select>
                  </td>
                  <td>
                    <select
                      className="storeReserveInput machineEvaluationSelect machineEvaluationConditionSelect"
                      value={row.conditionKey}
                      disabled={!row.logicKey || !row.hasDefinition}
                      onChange={(event) =>
                        updateRow(row.machineKey, {
                          conditionKey: event.target.value,
                        })
                      }
                    >
                      {row.conditionOptions.map((option) => (
                        <option key={option.key || "none"} value={option.key}>
                          {option.name}
                        </option>
                      ))}
                    </select>
                  </td>
                  <td className="machineEvaluationEstimateCell">
                    {selectedCondition?.backtestLabel || "-"}
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
