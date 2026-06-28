import { NextResponse } from "next/server";

import {
  getCloudMyHallIndex,
  updateCloudMyHallClient,
} from "../../../lib/data";
import {
  normalizeMyHallClientId,
  normalizeMyHallStoreIds,
} from "../../../lib/my-hall";

export const dynamic = "force-dynamic";

function errorResponse(message, status = 400) {
  return NextResponse.json({ error: message }, { status });
}

export async function GET() {
  const payload = await getCloudMyHallIndex();
  return NextResponse.json(payload);
}

export async function PATCH(request) {
  const body = await request.json().catch(() => ({}));
  const clientId = normalizeMyHallClientId(body?.clientId);
  if (!clientId) {
    return errorResponse("ブラウザー識別子を作成できませんでした。");
  }

  const payload = await updateCloudMyHallClient(
    clientId,
    normalizeMyHallStoreIds(body?.storeIds),
    body?.updatedAt,
  );
  return NextResponse.json(payload);
}
