import { describe, expect, it } from 'vitest';
import type { OrderSummary } from '@/pages/ChatAgent/utils/api';
import { HIDDEN, orderAmount, orderLimitPrice, orderSize } from '../utils/format';

/**
 * An amount on an order is the tool's answer, and the page may not improve on
 * it: a limit of 12.305 was not 12.31, and a price whose broker named no
 * currency was not a price in dollars.
 */

const SHOWN = { hidden: false, locale: 'en-US' };

function limitOrder(limit_price: string, currency?: string | null): OrderSummary {
  return { order_type: 'limit', limit_price, currency };
}

describe('order amounts', () => {
  it('keeps every digit of a sub-cent price', () => {
    expect(orderLimitPrice(limitOrder('0.1234', 'USD'), SHOWN)).toBe('$0.1234');
  });

  // A broker that names no currency gets no symbol invented for it.
  it('draws a price that came with no currency as the bare number', () => {
    expect(orderLimitPrice(limitOrder('12.305'), SHOWN)).toBe('12.305');
    expect(orderLimitPrice(limitOrder('1234.5', null), SHOWN)).toBe('1,234.50');
  });

  it('leaves a two-digit dollar amount reading as it did', () => {
    expect(orderAmount('1234.50', 'USD', SHOWN)).toBe('$1,234.50');
    expect(orderSize({ notional: { amount: '250', currency: 'USD' } }, SHOWN)).toBe(
      '$250.00',
    );
  });

  // Past 2^53 a float has already rounded the cents away.
  it('rounds nothing a float would', () => {
    expect(orderAmount('12345678901234567.25', 'USD', SHOWN)).toBe(
      '$12,345,678,901,234,567.25',
    );
  });

  it('hides an amount behind the mask', () => {
    expect(orderAmount('0.1234', 'USD', { hidden: true, locale: 'en-US' })).toBe(
      HIDDEN,
    );
  });

  // Intl takes only a three-letter code, and USDT is not a dollar.
  it('writes a code Intl cannot take beside the number, as given', () => {
    expect(orderAmount('12.5', 'USDT', SHOWN)).toBe('12.50 USDT');
  });

  it('passes text that is not a plain decimal through untouched', () => {
    for (const text of ['1E-7', 'n/a']) {
      expect(orderAmount(text, 'USD', SHOWN)).toBe(text);
    }
    expect(orderSize({ notional: { amount: '', currency: 'USD' } }, SHOWN)).toBe('');
  });
});
