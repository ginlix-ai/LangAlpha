/**
 * Watchlist items CRUD API.
 * Use watchlistId "default" for the user's default watchlist.
 * GET/POST /api/v1/users/me/watchlists/:id/items, PUT/DELETE .../items/:itemId
 */
import { api, type ApiError } from '@/api/client';

export interface AddWatchlistItemPayload {
  symbol: string;
  instrument_type: string;
  exchange?: string;
  name?: string;
  notes?: string;
  alert_settings?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
}

export interface UpdateWatchlistItemPayload {
  name?: string;
  notes?: string;
  alert_settings?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
}

export async function listWatchlistItems(watchlistId: string | null | undefined): Promise<unknown> {
  const id = watchlistId == null || watchlistId === '' ? 'default' : watchlistId;
  const { data } = await api.get(
    `/api/v1/users/me/watchlists/${encodeURIComponent(id)}/items`
  );
  return data;
}

export async function addWatchlistItem(watchlistId: string | null | undefined, payload: AddWatchlistItemPayload): Promise<unknown> {
  const id = watchlistId == null || watchlistId === '' ? 'default' : watchlistId;
  try {
    const { data } = await api.post(
      `/api/v1/users/me/watchlists/${encodeURIComponent(id)}/items`,
      payload
    );
    return data;
  } catch (e) {
    const err = e as ApiError;
    console.error(
      '[api] addWatchlistItem failed:',
      err.response?.status,
      err.response?.data,
      err.message
    );
    throw e;
  }
}

export async function updateWatchlistItem(watchlistId: string | null | undefined, itemId: string, payload: UpdateWatchlistItemPayload): Promise<unknown> {
  const id = watchlistId == null || watchlistId === '' ? 'default' : watchlistId;
  const { data } = await api.put(
    `/api/v1/users/me/watchlists/${encodeURIComponent(id)}/items/${encodeURIComponent(itemId)}`,
    payload
  );
  return data;
}

export async function deleteWatchlistItem(watchlistId: string | null | undefined, itemId: string): Promise<void> {
  const id = watchlistId == null || watchlistId === '' ? 'default' : watchlistId;
  await api.delete(
    `/api/v1/users/me/watchlists/${encodeURIComponent(id)}/items/${encodeURIComponent(itemId)}`
  );
}
