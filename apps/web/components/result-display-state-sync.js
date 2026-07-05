"use client";

import { useEffect } from "react";

import { getResultDisplayCookieName } from "../lib/result-display-state";

const RESULT_DISPLAY_COOKIE_MAX_AGE = 60 * 60 * 24 * 30;

function writeResultDisplayCookie(stateKey) {
  if (!stateKey || typeof document === "undefined") {
    return;
  }

  document.cookie = `${getResultDisplayCookieName(stateKey)}=1; Path=/; Max-Age=${RESULT_DISPLAY_COOKIE_MAX_AGE}; SameSite=Lax`;
}

export function ResultDisplayStateSync({ formId, stateKey, active = false }) {
  useEffect(() => {
    if (active) {
      writeResultDisplayCookie(stateKey);
    }
  }, [active, stateKey]);

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
    };

    form.addEventListener("submit", handleSubmit);

    return () => {
      form.removeEventListener("submit", handleSubmit);
    };
  }, [formId, stateKey]);

  return null;
}
