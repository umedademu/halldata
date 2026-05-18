"use client";

function updateMachineFilterChecks(form, resolveChecked) {
  if (!form) {
    return;
  }

  for (const input of form.querySelectorAll('input[data-machine-filter-option="1"]')) {
    const nextChecked = Boolean(resolveChecked(input, input.checked));
    if (input.checked !== nextChecked) {
      input.checked = nextChecked;
      input.dispatchEvent(new Event("change", { bubbles: true }));
    }
  }
}

function setMachineFilterChecks(form, checked) {
  updateMachineFilterChecks(form, () => checked);
}

function clearMachineGroupChecks(form) {
  if (!form) {
    return;
  }

  for (const input of form.querySelectorAll(
    'input[type="checkbox"][name="aimMachineGroup"], input[type="checkbox"][name="hanabiMachineGroup"]',
  )) {
    if (input.checked) {
      input.checked = false;
      input.dispatchEvent(new Event("change", { bubbles: true }));
    }
  }
}

function turnMachineFilterCategoryOn(form, category) {
  updateMachineFilterChecks(form, (input, currentChecked) =>
    input.dataset.machineCategory === category ? true : currentChecked,
  );
}

export function AllMachineFilterButtons() {
  const selectAll = (event) => {
    setMachineFilterChecks(event.currentTarget.closest("form"), true);
  };

  const clearAll = (event) => {
    const form = event.currentTarget.closest("form");
    setMachineFilterChecks(form, false);
    clearMachineGroupChecks(form);
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

export function MachineFilterCategoryButton({ category, label }) {
  const selectCategory = (event) => {
    turnMachineFilterCategoryOn(event.currentTarget.closest("form"), category);
  };

  return (
    <button
      type="button"
      className="storeReserveButton storeReserveButtonSecondary machineFilterAction"
      onClick={selectCategory}
    >
      {label}
    </button>
  );
}

export function JugglerOnlyButton() {
  return <MachineFilterCategoryButton category="juggler" label="ジャグ系のみ選択" />;
}
