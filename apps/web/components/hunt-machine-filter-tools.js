"use client";

function setMachineFilterChecks(form, predicate) {
  if (!form) {
    return;
  }

  for (const input of form.querySelectorAll('input[data-machine-filter-option="1"]')) {
    input.checked = predicate(input);
    input.dispatchEvent(new Event("change", { bubbles: true }));
  }
}

export function AllMachineFilterButtons() {
  const selectAll = (event) => {
    setMachineFilterChecks(event.currentTarget.closest("form"), () => true);
  };

  const clearAll = (event) => {
    setMachineFilterChecks(event.currentTarget.closest("form"), () => false);
  };

  return (
    <div className="machineFilterActionRow">
      <button
        type="button"
        className="storeReserveButton storeReserveButtonSecondary machineFilterAction"
        onClick={selectAll}
      >
        全てのチェックをON
      </button>
      <button
        type="button"
        className="storeReserveButton storeReserveButtonSecondary machineFilterAction"
        onClick={clearAll}
      >
        全てのチェックをOFF
      </button>
    </div>
  );
}

export function JugglerOnlyButton() {
  const selectJugglerOnly = (event) => {
    setMachineFilterChecks(
      event.currentTarget.closest("form"),
      (input) => input.dataset.machineCategory === "juggler",
    );
  };

  return (
    <button
      type="button"
      className="storeReserveButton storeReserveButtonSecondary machineFilterAction"
      onClick={selectJugglerOnly}
    >
      ジャグ系のみ選択
    </button>
  );
}
