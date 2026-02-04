/**
 * Watchlists CRUD API.
 * GET /api/v1/users/me/watchlists, POST, PUT /:id, DELETE /:id
 */
import { api, headers, DEFAULT_USER_ID } from '@/api/client';

export async function listWatchlists(userId = DEFAULT_USER_ID) {
  const { data } = await api.get('/api/v1/users/me/watchlists', {
    headers: headers(userId),
  });
  return data;
}

/**
 * @param {object} data - { name, description?, is_default?, display_order? }
 */
export async function createWatchlist(data, userId = DEFAULT_USER_ID) {
  const { data: result } = await api.post(
    '/api/v1/users/me/watchlists',
    data,
    { headers: headers(userId) }
  );
  return result;
}

/**
 * @param {string} id - watchlist_id
 * @param {object} data - { name?, description?, display_order? }
 */
export async function updateWatchlist(id, data, userId = DEFAULT_USER_ID) {
  const { data: result } = await api.put(
    `/api/v1/users/me/watchlists/${encodeURIComponent(id)}`,
    data,
    { headers: headers(userId) }
  );
  return result;
}

export async function deleteWatchlist(id, userId = DEFAULT_USER_ID) {
  await api.delete(`/api/v1/users/me/watchlists/${encodeURIComponent(id)}`, {
    headers: headers(userId),
  });
}
