"use client";

import { useEffect } from "react";

import {
  MY_HALL_CHANGE_EVENT,
  syncSavedMyHallStoreIdsToCloud,
} from "./store-favorite-button";

export function MyHallCloudWriter() {
  useEffect(() => {
    syncSavedMyHallStoreIdsToCloud();
    window.addEventListener(MY_HALL_CHANGE_EVENT, syncSavedMyHallStoreIdsToCloud);

    return () => {
      window.removeEventListener(MY_HALL_CHANGE_EVENT, syncSavedMyHallStoreIdsToCloud);
    };
  }, []);

  return null;
}
