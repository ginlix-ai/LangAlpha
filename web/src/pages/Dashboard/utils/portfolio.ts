/**
 * Portfolio CRUD API.
 * GET /api/v1/users/me/portfolio, POST, PUT /:id, DELETE /:id
 */
import { api, type ApiError } from '@/api/client';

export interface PortfolioHoldingPayload {
  symbol: string;
  instrument_type: string;
  quantity: number;
  average_cost?: number;
  exchange?: string;
  currency?: string;
  account_name?: string;
  notes?: string;
  first_purchased_at?: string;
}

export interface PortfolioHoldingUpdatePayload {
  quantity?: number;
  average_cost?: number;
  name?: string;
  currency?: string;
  notes?: string;
  first_purchased_at?: string;
}

export async function listPortfolio(): Promise<unknown> {
  const { data } = await api.get('/api/v1/users/me/portfolio');
  return data;
}

export async function addPortfolioHolding(payload: PortfolioHoldingPayload): Promise<unknown> {
  try {
    const { data } = await api.post('/api/v1/users/me/portfolio', payload);
    return data;
  } catch (e) {
    const err = e as ApiError;
    console.error(
      '[api] addPortfolioHolding failed:',
      err.response?.status,
      err.response?.data,
      err.message
    );
    throw e;
  }
}

export async function updatePortfolioHolding(id: string, payload: PortfolioHoldingUpdatePayload): Promise<unknown> {
  const { data } = await api.put(
    `/api/v1/users/me/portfolio/${encodeURIComponent(id)}`,
    payload
  );
  return data;
}

export async function deletePortfolioHolding(id: string): Promise<void> {
  await api.delete(`/api/v1/users/me/portfolio/${encodeURIComponent(id)}`);
}
