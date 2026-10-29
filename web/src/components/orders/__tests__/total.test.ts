import { describe, expect, it } from 'vitest';
import type { OrderSummary } from '@/types/orders';
import { orderTotal } from '../total';

/**
 * The total is the one figure on an order card nobody typed: the card works
 * it out. So it has to be exact where a float is not, and absent wherever the
 * card cannot know the answer, since a wrong total on a card asking someone to
 * approve a trade is worse than none.
 */

const EQUITY: OrderSummary = {
  asset_class: 'equity',
  instrument: { kind: 'equity', symbol: 'AAPL' },
  order_type: 'limit',
  currency: 'USD',
};

describe('orderTotal', () => {
  it('multiplies the decimal strings exactly', () => {
    // 0.1 * 3 is 0.30000000000000004 as floats.
    expect(orderTotal({ ...EQUITY, qty: '3', limit_price: '0.1' })).toEqual({
      kind: 'estimated',
      amount: '0.30',
      currency: 'USD',
    });
    // 50.0012...; rounds half up to two places.
    expect(
      orderTotal({
        asset_class: 'crypto',
        instrument: { kind: 'crypto', pair: 'BTC-USD' },
        order_type: 'limit',
        qty: '0.00071012',
        limit_price: '70412.33',
      })?.amount,
    ).toBe('50.00');
    expect(orderTotal({ ...EQUITY, qty: '1', limit_price: '0.125' })?.amount).toBe('0.13');
  });

  // A premium is quoted per share and an option is sold per contract.
  it('prices an option by its contract multiplier', () => {
    const option: OrderSummary = {
      asset_class: 'option',
      instrument: { kind: 'option', underlying: 'AAPL', right: 'C', multiplier: 100 },
      order_type: 'limit',
      qty: '1',
      limit_price: '3.40',
    };
    expect(orderTotal(option)?.amount).toBe('340.00');
    // Guessing 1 would state $3.40 for a $340 order.
    expect(
      orderTotal({ ...option, instrument: { kind: 'option', underlying: 'AAPL', right: 'C' } }),
    ).toBeNull();
  });

  it('states none before a fill when no price was set', () => {
    expect(orderTotal({ ...EQUITY, qty: '10', order_type: 'market' })).toBeNull();
    expect(
      orderTotal({ ...EQUITY, notional: { amount: '250', currency: 'USD' }, order_type: 'market' }),
    ).toBeNull();
  });

  // moomoo sends a price on every order and ignores it on a market order, so
  // the price is a limit only where the type says it is.
  it('estimates only an order its limit bounds', () => {
    expect(orderTotal({ ...EQUITY, order_type: 'market', qty: '10', limit_price: '100' })).toBeNull();
    expect(orderTotal({ ...EQUITY, order_type: 'stop', qty: '10', limit_price: '100' })).toBeNull();
    expect(orderTotal({ ...EQUITY, order_type: undefined, qty: '10', limit_price: '100' })).toBeNull();
    expect(
      orderTotal({ ...EQUITY, order_type: 'stop_limit', qty: '10', stop_price: '99', limit_price: '100' })?.amount,
    ).toBe('1000.00');
    // A placeholder zero is not a price anyone would trade at.
    expect(orderTotal({ ...EQUITY, qty: '10', limit_price: '0' })).toBeNull();
    // An order sent by amount is for that amount.
    expect(
      orderTotal({ ...EQUITY, qty: '10', limit_price: '100', notional: { amount: '500', currency: 'USD' } }),
    ).toBeNull();
  });

  it('reads a price written in exponent form', () => {
    const order: OrderSummary = {
      asset_class: 'crypto',
      instrument: { kind: 'crypto', pair: 'SHIB-USD' },
      order_type: 'limit',
      qty: '1000000',
      limit_price: '0.000011',
      currency: 'USD',
    };
    expect(orderTotal(order)?.amount).toBe('11.00');
    // 1.2E-7 is how the server writes 0.00000012.
    expect(
      orderTotal(order, { status: 'filled', filled_qty: '1000000', avg_fill_price: '1.2E-7' }),
    ).toEqual({ kind: 'filled', amount: '0.12', currency: 'USD' });
    expect(orderTotal({ ...order, qty: '1E+2', limit_price: '2.5' })?.amount).toBe('250.00');
  });

  it('settles on what filled, and says whether it is still filling', () => {
    const order: OrderSummary = { ...EQUITY, qty: '100', limit_price: '252' };
    expect(
      orderTotal(order, { status: 'partially_filled', filled_qty: '40', avg_fill_price: '251.12' }),
    ).toEqual({ kind: 'partial', amount: '10044.80', currency: 'USD' });
    expect(
      orderTotal(order, { status: 'cancelled', filled_qty: '40', avg_fill_price: '251.12' })?.kind,
    ).toBe('filled');
    // The zeros a vendor reports for a working order are not a fill.
    expect(
      orderTotal(order, { status: 'working', filled_qty: '0', avg_fill_price: '0' })?.kind,
    ).toBe('estimated');
    // A fill with no price is not the estimate either.
    expect(orderTotal(order, { status: 'filled', filled_qty: '100' })).toBeNull();
    expect(orderTotal(order, { status: 'partially_filled', filled_qty: '40', avg_fill_price: '0' })).toBeNull();
  });

  // Points, net premiums across legs: units this card cannot price.
  it('states none for a future or a combo', () => {
    expect(
      orderTotal({
        asset_class: 'future',
        instrument: { kind: 'future', symbol: 'ES' },
        qty: '1',
        limit_price: '5000',
      }),
    ).toBeNull();
    expect(
      orderTotal({
        asset_class: 'option_combo',
        instrument: { kind: 'combo', strategy: 'vertical' },
        qty: '1',
        limit_price: '1.20',
      }),
    ).toBeNull();
  });
});
