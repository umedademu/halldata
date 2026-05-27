import { cookies } from "next/headers";
import { NextResponse } from "next/server";

import {
  getMachineHuntScoreHighlight,
} from "../../../../../../../lib/data";
import {
  decodeHuntScoreLogicCookieValue,
  getHuntScoreLogicCookieName,
} from "../../../../../../../lib/hunt-score-logic-selection";

export const dynamic = "force-dynamic";

async function readStoredHuntScoreLogicKey(storeId) {
  const cookieStore = await cookies();
  return decodeHuntScoreLogicCookieValue(
    cookieStore.get(getHuntScoreLogicCookieName(storeId))?.value ?? "",
  );
}

export async function GET(request, { params }) {
  const resolvedParams = await params;
  const storeId = String(resolvedParams.storeId ?? "").trim();
  const machineName = decodeURIComponent(String(resolvedParams.machineName ?? "").trim());
  const searchParams = new URL(request.url).searchParams;
  const differenceMode = searchParams.get("differenceMode") ?? undefined;
  const settingEstimateMode = searchParams.get("settingEstimateMode") ?? undefined;
  const scope = String(searchParams.get("scope") ?? "").trim();
  const dateRange = {
    startDate: searchParams.get("startDate") ?? "",
    endDate: searchParams.get("endDate") ?? "",
  };
  const huntScoreLogicKey = await readStoredHuntScoreLogicKey(storeId);
  const highlight = await getMachineHuntScoreHighlight(
    storeId,
    huntScoreLogicKey,
    differenceMode,
    settingEstimateMode,
    dateRange,
    scope === "machine" ? [machineName] : null,
  );

  if (!highlight) {
    return NextResponse.json({ error: "狙い度データがありません。" }, { status: 404 });
  }

  return NextResponse.json(highlight);
}
