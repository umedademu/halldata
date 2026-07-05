"use client";

import { useEffect } from "react";

import {
  encodeFormStateEntries,
  getFormStateCookieChunkName,
  getFormStateCookieCountName,
  getFormStateCookieName,
  getResultDisplayCookieName,
  splitFormStateCookieValue,
} from "../lib/result-display-state";

const RESULT_DISPLAY_COOKIE_MAX_AGE = 60 * 60 * 24 * 30;
const FORM_STATE_COOKIE_MAX_CHUNKS = 20;

function writeResultDisplayCookie(stateKey) {
  if (!stateKey || typeof document === "undefined") {
    return;
  }

  document.cookie = `${getResultDisplayCookieName(stateKey)}=1; Path=/; Max-Age=${RESULT_DISPLAY_COOKIE_MAX_AGE}; SameSite=Lax`;
}

function writeCookie(name, value, maxAge = RESULT_DISPLAY_COOKIE_MAX_AGE) {
  document.cookie = `${name}=${value}; Path=/; Max-Age=${maxAge}; SameSite=Lax`;
}

function readFormEntries(form, paramKeys) {
  if (!form || !Array.isArray(paramKeys) || paramKeys.length === 0) {
    return [];
  }

  const formData = new FormData(form);
  const entries = [];
  for (const key of paramKeys) {
    for (const value of formData.getAll(key)) {
      entries.push([key, String(value ?? "")]);
    }
  }
  return entries;
}

function clearOldFormStateChunks(baseName) {
  writeCookie(getFormStateCookieCountName(baseName), "", 0);
  for (let index = 0; index < FORM_STATE_COOKIE_MAX_CHUNKS; index += 1) {
    writeCookie(getFormStateCookieChunkName(baseName, index), "", 0);
  }
}

function writeFormStateCookie(stateKey, form, paramKeys) {
  if (!stateKey || !form || typeof document === "undefined") {
    return;
  }

  const entries = readFormEntries(form, paramKeys);
  if (entries.length === 0) {
    return;
  }

  const baseName = getFormStateCookieName(stateKey);
  const chunks = splitFormStateCookieValue(encodeFormStateEntries(entries));
  clearOldFormStateChunks(baseName);
  writeCookie(getFormStateCookieCountName(baseName), String(chunks.length));
  chunks.forEach((chunk, index) => {
    writeCookie(getFormStateCookieChunkName(baseName, index), chunk);
  });
}

export function ResultDisplayStateSync({
  formId,
  stateKey,
  conditionStateKey = "",
  conditionParamKeys = [],
  active = false,
}) {
  useEffect(() => {
    if (active) {
      writeResultDisplayCookie(stateKey);
      const form = formId ? document.getElementById(formId) : null;
      writeFormStateCookie(conditionStateKey, form, conditionParamKeys);
    }
  }, [active, conditionParamKeys, conditionStateKey, formId, stateKey]);

  useEffect(() => {
    if (!formId || !stateKey) {
      return undefined;
    }

    const form = document.getElementById(formId);
    if (!form) {
      return undefined;
    }

    const handleSubmit = () => {
      writeResultDisplayCookie(stateKey);
      writeFormStateCookie(conditionStateKey, form, conditionParamKeys);
    };

    form.addEventListener("submit", handleSubmit);

    return () => {
      form.removeEventListener("submit", handleSubmit);
    };
  }, [conditionParamKeys, conditionStateKey, formId, stateKey]);

  return null;
}
