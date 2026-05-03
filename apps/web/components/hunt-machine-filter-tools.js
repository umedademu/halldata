"use client";

export function JugglerOnlyButton() {
  const selectJugglerOnly = (event) => {
    const form = event.currentTarget.closest("form");
    if (!form) {
      return;
    }

    for (const input of form.querySelectorAll('input[data-machine-filter-option="1"]')) {
      input.checked = input.dataset.machineCategory === "juggler";
      input.dispatchEvent(new Event("change", { bubbles: true }));
    }
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
