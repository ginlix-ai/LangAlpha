import { describe, expect, it } from 'vitest';
import { pagesHoldOpenOrder } from '@/hooks/useOrders';
import type { OrderAttempt, OrderPage } from '@/pages/ChatAgent/utils/api';

const row = (status: OrderAttempt['status']): OrderAttempt =>
  ({ attempt_id: `a-${status}`, status }) as OrderAttempt;

const page = (...rows: OrderAttempt[]): OrderPage => ({ items: rows, next_cursor: null });

describe('pagesHoldOpenOrder', () => {
  it('is quiet before the first page and over a settled ledger', () => {
    expect(pagesHoldOpenOrder(undefined)).toBe(false);
    expect(pagesHoldOpenOrder([page(row('filled'), row('cancelled'))])).toBe(false);
  });

  it('asks again while any loaded row can still move', () => {
    expect(pagesHoldOpenOrder([page(row('filled')), page(row('working'))])).toBe(true);
    expect(pagesHoldOpenOrder([page(row('proposed'))])).toBe(true);
    expect(pagesHoldOpenOrder([page(row('unknown'))])).toBe(true);
  });
});
