"use client";

import { useEffect } from "react";

const MACHINE_EVALUATION_ADOPTION_ONLY = "machine";

export function HuntBacktestAdoptionModeControl({ formId }) {
  useEffect(() => {
    if (!formId) {
      return undefined;
    }

    const form = document.getElementById(formId);
    if (!form) {
      return undefined;
    }

    const syncDisabledState = () => {
      const adoptionOnly =
        form.querySelector('input[name="machineEvaluationMode"]:checked')?.value ===
        MACHINE_EVALUATION_ADOPTION_ONLY;
      for (const panel of form.querySelectorAll("[data-backtest-numeric-condition-panel]")) {
        panel.disabled = adoptionOnly;
        panel.setAttribute("aria-disabled", adoptionOnly ? "true" : "false");
      }
    };

    syncDisabledState();
    form.addEventListener("change", syncDisabledState);

    return () => {
      form.removeEventListener("change", syncDisabledState);
    };
  }, [formId]);

  return null;
}
