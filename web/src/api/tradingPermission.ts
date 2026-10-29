/**
 * Trading-permission endpoints. Shared by Settings, where the level is chosen,
 * and the Plugins page, which names it beside the brokerages it governs.
 */
import { api } from '@/api/client';
import type { TradingPermission, TradingPermissionUpdate } from '@/types/api';

export async function getTradingPermission(): Promise<TradingPermission> {
  const { data } = await api.get<TradingPermission>('/api/v1/users/me/trading-permission');
  return data;
}

export async function updateTradingPermission(body: TradingPermissionUpdate): Promise<TradingPermission> {
  const { data } = await api.put<TradingPermission>('/api/v1/users/me/trading-permission', body);
  return data;
}
