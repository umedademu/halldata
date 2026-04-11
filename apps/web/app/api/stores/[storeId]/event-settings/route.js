import { updateStoreEventSettings } from "../../../../../lib/data";

export async function PATCH(request, { params }) {
  const { storeId } = await params;
  const body = await request.json().catch(() => ({}));
  const eventFilters = await updateStoreEventSettings(storeId, body);

  return Response.json({ eventFilters });
}
