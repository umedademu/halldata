"use client";

import { useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";

import {
  MY_HALL_CHANGE_EVENT,
  normalizeStoreId,
  readSavedMyHallStoreIds,
} from "./store-favorite-button";

function normalizeStoreOption(store) {
  const id = normalizeStoreId(store?.id);
  const storeName = String(store?.storeName ?? "").trim();
  if (!id || !storeName) {
    return null;
  }
  return {
    id,
    storeName,
  };
}

function compareStoreOptions(left, right) {
  return left.storeName.localeCompare(right.storeName, "ja");
}

function StoreOption({ store, currentStoreId }) {
  const isCurrent = store.id === currentStoreId;

  return (
    <option value={store.id}>
      {store.storeName}
      {isCurrent ? "（表示中）" : ""}
    </option>
  );
}

export function StoreSwitcher({ stores = [], currentStoreId = "" }) {
  const router = useRouter();
  const [myHallStoreIds, setMyHallStoreIds] = useState([]);
  const normalizedCurrentStoreId = normalizeStoreId(currentStoreId);
  const storeOptions = useMemo(
    () =>
      stores
        .map(normalizeStoreOption)
        .filter(Boolean)
        .sort(compareStoreOptions),
    [stores],
  );
  const storeById = useMemo(
    () => new Map(storeOptions.map((store) => [store.id, store])),
    [storeOptions],
  );
  const myHallStoreIdSet = useMemo(() => new Set(myHallStoreIds), [myHallStoreIds]);
  const myHallStores = useMemo(
    () => myHallStoreIds.map((storeId) => storeById.get(storeId)).filter(Boolean),
    [myHallStoreIds, storeById],
  );
  const otherStores = useMemo(
    () => storeOptions.filter((store) => !myHallStoreIdSet.has(store.id)),
    [myHallStoreIdSet, storeOptions],
  );
  const hasCurrentStoreOption = storeOptions.some((store) => store.id === normalizedCurrentStoreId);

  useEffect(() => {
    const syncMyHallStoreIds = () => {
      setMyHallStoreIds(readSavedMyHallStoreIds());
    };

    syncMyHallStoreIds();
    window.addEventListener(MY_HALL_CHANGE_EVENT, syncMyHallStoreIds);
    window.addEventListener("storage", syncMyHallStoreIds);

    return () => {
      window.removeEventListener(MY_HALL_CHANGE_EVENT, syncMyHallStoreIds);
      window.removeEventListener("storage", syncMyHallStoreIds);
    };
  }, []);

  const handleStoreChange = (event) => {
    const nextStoreId = normalizeStoreId(event.target.value);
    if (!nextStoreId || nextStoreId === normalizedCurrentStoreId) {
      return;
    }

    router.push(`/stores/${encodeURIComponent(nextStoreId)}/hunt-analysis`);
  };

  return (
    <label className="storeSwitcher">
      <span className="storeSwitcherLabel">店舗切り替え</span>
      <select
        className="storeSwitcherSelect"
        value={hasCurrentStoreOption ? normalizedCurrentStoreId : ""}
        onChange={handleStoreChange}
      >
        {!hasCurrentStoreOption ? <option value="">店舗を選択</option> : null}
        {myHallStores.length > 0 ? (
          <optgroup label="マイホール">
            {myHallStores.map((store) => (
              <StoreOption key={store.id} store={store} currentStoreId={normalizedCurrentStoreId} />
            ))}
          </optgroup>
        ) : null}
        <optgroup label={myHallStores.length > 0 ? "その他の店舗" : "店舗一覧"}>
          {otherStores.map((store) => (
            <StoreOption key={store.id} store={store} currentStoreId={normalizedCurrentStoreId} />
          ))}
        </optgroup>
      </select>
    </label>
  );
}
