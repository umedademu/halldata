import { cookies } from "next/headers";
import { NextResponse } from "next/server";

import {
  getMachineHuntScoreHighlight,
} from "../../../../../../lib/data";
import {
  decodeHuntScoreLogicCookieValue,
  getHuntScoreLogicCookieName,
} from "../../../../../../lib/hunt-score-logic-selection";

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
  const differenceMode = new URL(request.url).searchParams.get("differenceMode") ?? undefined;
  const huntScoreLogicKey = await readStoredHuntScoreLogicKey(storeId);
  const highlight = await getMachineHuntScoreHighlight(storeId, huntScoreLogicKey, differenceMode);

  if (!highlight) {
    return NextResponse.json({ error: "狙い度データがありません。" }, { status: 404 });
  }

  return NextResponse.json(highlight);
}
