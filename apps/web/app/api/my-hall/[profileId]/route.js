import { NextResponse } from "next/server";

import {
  getCloudMyHallProfile,
  updateCloudMyHallProfile,
} from "../../../../lib/data";
import {
  normalizeMyHallProfileId,
  normalizeMyHallStoreIds,
} from "../../../../lib/my-hall";

export const dynamic = "force-dynamic";

function errorResponse(message, status = 400) {
  return NextResponse.json({ error: message }, { status });
}

export async function GET(_request, { params }) {
  const resolvedParams = await params;
  const profileId = normalizeMyHallProfileId(resolvedParams?.profileId);
  if (!profileId) {
    return errorResponse("利用者を選んでください。");
  }

  const payload = await getCloudMyHallProfile(profileId);
  return NextResponse.json(payload);
}

export async function PATCH(request, { params }) {
  const resolvedParams = await params;
  const profileId = normalizeMyHallProfileId(resolvedParams?.profileId);
  if (!profileId) {
    return errorResponse("利用者を選んでください。");
  }

  const body = await request.json().catch(() => ({}));
  const payload = await updateCloudMyHallProfile(
    profileId,
    normalizeMyHallStoreIds(body?.storeIds),
    body?.updatedAt,
  );
  return NextResponse.json(payload);
}
