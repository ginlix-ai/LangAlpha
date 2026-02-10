/**
 * Watchlists CRUD API.
 * GET /api/v1/users/me/watchlists, POST, PUT /:id, DELETE /:id
 */
import { api } from '@/api/client';

export async function listWatchlists() {
  const { data } = await api.get('/api/v1/users/me/watchlists');
  return data;
}

/**
 * @param {object} payload - { name, description?, is_default?, display_order? }
 */
export async function createWatchlist(payload) {
  const { data } = await api.post('/api/v1/users/me/watchlists', payload);
  return data;
}

/**
 * @param {string} id - watchlist_id
 * @param {object} payload - { name?, description?, display_order? }
 */
export async function updateWatchlist(id, payload) {
  const { data } = await api.put(
    `/api/v1/users/me/watchlists/${encodeURIComponent(id)}`,
    payload
  );
  return data;
}

export async function deleteWatchlist(id) {
  await api.delete(`/api/v1/users/me/watchlists/${encodeURIComponent(id)}`);
}
