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

function turnMachineFilterCategoryOff(form, category) {
  updateMachineFilterChecks(form, (input, currentChecked) =>
    input.dataset.machineCategory === category ? false : currentChecked,
  );
}

function turnMachineFilterSlotCountOn(form, minSlotCount) {
  const threshold = Number(minSlotCount);
  if (!Number.isFinite(threshold) || threshold <= 0) {
    return;
  }

  updateMachineFilterChecks(form, (input, currentChecked) => {
    const slotCount = Number(input.dataset.machineSlotCount);
    return Number.isFinite(slotCount) && slotCount >= threshold ? true : currentChecked;
  });
}

function SlotCountMachineFilterAction() {
  const selectBySlotCount = (event) => {
    const root = event.currentTarget.closest(".machineFilterSlotAction");
    const input = root?.querySelector('input[data-machine-slot-threshold="1"]');
    if (!input || !input.reportValidity()) {
      return;
    }

    turnMachineFilterSlotCountOn(event.currentTarget.closest("form"), input.value);
  };

  const handleKeyDown = (event) => {
    if (event.key !== "Enter") {
      return;
    }

    event.preventDefault();
    const button = event.currentTarget
      .closest(".machineFilterSlotAction")
      ?.querySelector('button[data-machine-slot-action="1"]');
    button?.click();
  };

  return (
    <div className="machineFilterSlotAction">
      <label className="machineFilterSlotField">
        <span>設置台数</span>
        <span className="machineFilterSlotInputWrap">
          <input
            type="number"
            min="1"
            step="1"
            placeholder="8"
            inputMode="numeric"
            className="machineFilterSlotInput"
            data-machine-slot-threshold="1"
            aria-label="ONにする機種の設置台数下限"
            onKeyDown={handleKeyDown}
          />
          <span>台以上</span>
        </span>
      </label>
      <button
        type="button"
        className="storeReserveButton storeReserveButtonSecondary machineFilterAction"
        data-machine-slot-action="1"
        onClick={selectBySlotCount}
      >
        該当機種をON
      </button>
    </div>
  );
}

export function AllMachineFilterButtons({ enableSlotCountSelection = false }) {
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
      {enableSlotCountSelection ? <SlotCountMachineFilterAction /> : null}
    </div>
  );
}

export function MachineFilterCategoryButton({ category, label, action = "select" }) {
  const updateCategory = (event) => {
    const form = event.currentTarget.closest("form");
    if (action === "clear") {
      turnMachineFilterCategoryOff(form, category);
      return;
    }

    turnMachineFilterCategoryOn(form, category);
  };

  return (
    <button
      type="button"
      className="storeReserveButton storeReserveButtonSecondary machineFilterAction"
      onClick={updateCategory}
    >
      {label}
    </button>
  );
}

export function JugglerOnlyButton() {
  return <MachineFilterCategoryButton category="juggler" label="ジャグ系のみ選択" />;
}
