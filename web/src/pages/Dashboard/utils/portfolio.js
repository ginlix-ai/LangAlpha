/**
 * Portfolio CRUD API.
 * GET /api/v1/users/me/portfolio, POST, PUT /:id, DELETE /:id
 */
import { api } from '@/api/client';

export async function listPortfolio() {
  const { data } = await api.get('/api/v1/users/me/portfolio');
  return data;
}

/**
 * @param {object} payload - { symbol, instrument_type, quantity, average_cost?, exchange?, currency?, account_name?, notes?, first_purchased_at? }
 */
export async function addPortfolioHolding(payload) {
  try {
    const { data } = await api.post('/api/v1/users/me/portfolio', payload);
    return data;
  } catch (e) {
    console.error(
      '[api] addPortfolioHolding failed:',
      e.response?.status,
      e.response?.data,
      e.message
    );
    throw e;
  }
}

/**
 * @param {string} id - holding_id
 * @param {object} payload - { quantity?, average_cost?, name?, currency?, notes?, first_purchased_at? }
 */
export async function updatePortfolioHolding(id, payload) {
  const { data } = await api.put(
    `/api/v1/users/me/portfolio/${encodeURIComponent(id)}`,
    payload
  );
  return data;
}

export async function deletePortfolioHolding(id) {
  await api.delete(`/api/v1/users/me/portfolio/${encodeURIComponent(id)}`);
}
