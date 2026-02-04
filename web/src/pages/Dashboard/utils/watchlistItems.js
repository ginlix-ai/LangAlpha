/**
 * Watchlist items CRUD API.
 * Use watchlistId "default" for the user's default watchlist.
 * GET/POST /api/v1/users/me/watchlists/:id/items, PUT/DELETE .../items/:itemId
 */
import { api, headers, DEFAULT_USER_ID } from '@/api/client';

export async function listWatchlistItems(
  watchlistId,
  userId = DEFAULT_USER_ID
) {
  const id = watchlistId == null || watchlistId === '' ? 'default' : watchlistId;
  const { data } = await api.get(
    `/api/v1/users/me/watchlists/${encodeURIComponent(id)}/items`,
    { headers: headers(userId) }
  );
  return data;
}

/**
 * @param {string} watchlistId - "default" or UUID
 * @param {object} data - { symbol, instrument_type, exchange?, name?, notes?, alert_settings?, metadata? }
 */
export async function addWatchlistItem(
  watchlistId,
  data,
  userId = DEFAULT_USER_ID
) {
  const id = watchlistId == null || watchlistId === '' ? 'default' : watchlistId;
  try {
    const { data: result } = await api.post(
      `/api/v1/users/me/watchlists/${encodeURIComponent(id)}/items`,
      data,
      { headers: { ...headers(userId), 'Content-Type': 'application/json' } }
    );
    return result;
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
 * @param {object} data - { name?, notes?, alert_settings?, metadata? }
 */
export async function updateWatchlistItem(
  watchlistId,
  itemId,
  data,
  userId = DEFAULT_USER_ID
) {
  const id = watchlistId == null || watchlistId === '' ? 'default' : watchlistId;
  const { data: result } = await api.put(
    `/api/v1/users/me/watchlists/${encodeURIComponent(id)}/items/${encodeURIComponent(itemId)}`,
    data,
    { headers: headers(userId) }
  );
  return result;
}

export async function deleteWatchlistItem(
  watchlistId,
  itemId,
  userId = DEFAULT_USER_ID
) {
  const id = watchlistId == null || watchlistId === '' ? 'default' : watchlistId;
  await api.delete(
    `/api/v1/users/me/watchlists/${encodeURIComponent(id)}/items/${encodeURIComponent(itemId)}`,
    { headers: headers(userId) }
  );
}
