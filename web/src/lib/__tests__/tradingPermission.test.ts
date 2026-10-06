// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { TRADING_LEVELS, skipsApproval } from '@/lib/tradingPermission';

describe('skipsApproval', () => {
  it('is exactly the two levels behind the agreement', () => {
    expect(TRADING_LEVELS.filter(skipsApproval)).toEqual(['plan_first', 'autonomous']);
  });
});

// The order comes from how the table is written, and it is the order Settings
// lists the levels in: least autonomy first.
describe('TRADING_LEVELS', () => {
  it('reads the table least autonomy first', () => {
    expect(TRADING_LEVELS).toEqual(['no_trading', 'approve_each', 'plan_first', 'autonomous']);
  });
});
