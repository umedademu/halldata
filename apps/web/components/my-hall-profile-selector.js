"use client";

import { useEffect, useState } from "react";

import {
  MY_HALL_PROFILE_CHANGE_EVENT,
  listMyHallProfiles,
  readSavedMyHallProfileId,
  saveMyHallProfileId,
  syncMyHallStoreIdsWithCloud,
} from "./store-favorite-button";

const profiles = listMyHallProfiles();

export function MyHallProfileSelector() {
  const [profileId, setProfileId] = useState("");
  const [isSyncing, setIsSyncing] = useState(false);

  useEffect(() => {
    let mounted = true;

    const syncProfile = () => {
      const savedProfileId = readSavedMyHallProfileId();
      setProfileId(savedProfileId);
      if (!savedProfileId) {
        return;
      }

      setIsSyncing(true);
      syncMyHallStoreIdsWithCloud()
        .catch((error) => {
          console.warn(error);
        })
        .finally(() => {
          if (mounted) {
            setIsSyncing(false);
          }
        });
    };

    syncProfile();
    window.addEventListener(MY_HALL_PROFILE_CHANGE_EVENT, syncProfile);
    window.addEventListener("storage", syncProfile);

    return () => {
      mounted = false;
      window.removeEventListener(MY_HALL_PROFILE_CHANGE_EVENT, syncProfile);
      window.removeEventListener("storage", syncProfile);
    };
  }, []);

  const handleProfileChange = (event) => {
    saveMyHallProfileId(event.target.value);
  };

  return (
    <label className="myHallProfileSelector" title={isSyncing ? "同期中" : undefined}>
      <span>利用者</span>
      <select value={profileId} onChange={handleProfileChange}>
        <option value="">未選択</option>
        {profiles.map((profile) => (
          <option key={profile.id} value={profile.id}>
            {profile.label}
          </option>
        ))}
      </select>
    </label>
  );
}
