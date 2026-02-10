/**
 * Watchlist items CRUD API.
 * Use watchlistId "default" for the user's default watchlist.
 * GET/POST /api/v1/users/me/watchlists/:id/items, PUT/DELETE .../items/:itemId
 */
import { api } from '@/api/client';

export async function listWatchlistItems(watchlistId) {
  const id = watchlistId == null || watchlistId === '' ? 'default' : watchlistId;
  const { data } = await api.get(
    `/api/v1/users/me/watchlists/${encodeURIComponent(id)}/items`
  );
  return data;
}

/**
 * @param {string} watchlistId - "default" or UUID
 * @param {object} payload - { symbol, instrument_type, exchange?, name?, notes?, alert_settings?, metadata? }
 */
export async function addWatchlistItem(watchlistId, payload) {
  const id = watchlistId == null || watchlistId === '' ? 'default' : watchlistId;
  try {
    const { data } = await api.post(
      `/api/v1/users/me/watchlists/${encodeURIComponent(id)}/items`,
      payload
    );
    return data;
  } catch (e) {
    console.error(
      '[api] addWatchlistItem failed:',
      e.response?.status,
      e.response?.data,
      e.message
    );
    throw e;
  }
}

/**
 * @param {string} watchlistId - "default" or UUID
 * @param {string} itemId - item_id
 * @param {object} payload - { name?, notes?, alert_settings?, metadata? }
 */
export async function updateWatchlistItem(watchlistId, itemId, payload) {
  const id = watchlistId == null || watchlistId === '' ? 'default' : watchlistId;
  const { data } = await api.put(
    `/api/v1/users/me/watchlists/${encodeURIComponent(id)}/items/${encodeURIComponent(itemId)}`,
    payload
  );
  return data;
}

export async function deleteWatchlistItem(watchlistId, itemId) {
  const id = watchlistId == null || watchlistId === '' ? 'default' : watchlistId;
  await api.delete(
    `/api/v1/users/me/watchlists/${encodeURIComponent(id)}/items/${encodeURIComponent(itemId)}`
  );
}
