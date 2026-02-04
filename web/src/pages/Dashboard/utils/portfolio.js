/**
 * Portfolio CRUD API.
 * GET /api/v1/users/me/portfolio, POST, PUT /:id, DELETE /:id
 */
import { api, headers, DEFAULT_USER_ID } from '@/api/client';

export async function listPortfolio(userId = DEFAULT_USER_ID) {
  const { data } = await api.get('/api/v1/users/me/portfolio', {
    headers: headers(userId),
  });
  return data;
}

/**
 * @param {object} data - { symbol, instrument_type, quantity, average_cost?, exchange?, currency?, account_name?, notes?, first_purchased_at? }
 */
export async function addPortfolioHolding(data, userId = DEFAULT_USER_ID) {
  try {
    const { data: result } = await api.post(
      '/api/v1/users/me/portfolio',
      data,
      { headers: { ...headers(userId), 'Content-Type': 'application/json' } }
    );
    return result;
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
 * @param {object} data - { quantity?, average_cost?, name?, currency?, notes?, first_purchased_at? }
 */
export async function updatePortfolioHolding(id, data, userId = DEFAULT_USER_ID) {
  const { data: result } = await api.put(
    `/api/v1/users/me/portfolio/${encodeURIComponent(id)}`,
    data,
    { headers: headers(userId) }
  );
  return result;
}

export async function deletePortfolioHolding(id, userId = DEFAULT_USER_ID) {
  await api.delete(`/api/v1/users/me/portfolio/${encodeURIComponent(id)}`, {
    headers: headers(userId),
  });
}
