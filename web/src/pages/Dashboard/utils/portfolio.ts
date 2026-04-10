/**
 * Portfolio API — read-only (Sharesight proxy).
 * GET /api/v1/users/me/portfolio
 */
import { api } from '@/api/client';

export async function listPortfolio(): Promise<unknown> {
  const { data } = await api.get('/api/v1/users/me/portfolio');
  return data;
}
