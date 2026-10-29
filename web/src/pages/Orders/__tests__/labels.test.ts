import { describe, expect, it } from 'vitest';
import enUS from '@/locales/en-US.json';
import zhCN from '@/locales/zh-CN.json';
import { ORDER_MODE_KEY } from '@/components/orders/mode';
import {
  ORDER_STATUS_KEY,
  ORDER_STATUS_SHORT_KEY,
  ORDER_STATUS_STAGED_KEYS,
  orderStatusLabel,
  orderStatusShortLabel,
  vendorNamedEnd,
} from '@/components/orders/status';
import {
  OPTION_RIGHT_KEY,
  ORDER_ASSET_CLASS_KEY,
  ORDER_SESSION_KEY,
  ORDER_SIDE_KEY,
  ORDER_TIME_IN_FORCE_KEY,
  ORDER_TYPE_KEY,
} from '@/components/orders/labels';
import { ORDER_ACTION_KEY } from '../utils/labels';
import { ORDER_STATUSES } from '@/pages/ChatAgent/utils/api';

/**
 * Most of these maps come from the shared order module rather than from the
 * page: the order's own words are the chat card's too, the status words are
 * the chat receipt's and the mode words are the Plugins badge's, on purpose.
 * The locale sweep only reads bare keys under `orders.`, so nothing else would
 * notice if one of the others were renamed under us, and a missing key renders
 * as the key itself on a row about an order somebody placed.
 */

function lookup(catalog: unknown, key: string): unknown {
  return key
    .split('.')
    .reduce<unknown>(
      (cur, part) =>
        cur && typeof cur === 'object'
          ? (cur as Record<string, unknown>)[part]
          : undefined,
      catalog,
    );
}

const MAPS: Record<string, Record<string, string>> = {
  status: ORDER_STATUS_KEY,
  statusShort: ORDER_STATUS_SHORT_KEY,
  action: ORDER_ACTION_KEY,
  side: ORDER_SIDE_KEY,
  orderType: ORDER_TYPE_KEY,
  assetClass: ORDER_ASSET_CLASS_KEY,
  timeInForce: ORDER_TIME_IN_FORCE_KEY,
  session: ORDER_SESSION_KEY,
  optionRight: OPTION_RIGHT_KEY,
  mode: ORDER_MODE_KEY,
};

describe('order label keys', () => {
  for (const [name, map] of Object.entries(MAPS)) {
    it(`resolves every ${name} in both catalogs`, () => {
      const entries = Object.entries(map);
      expect(entries.length).toBeGreaterThan(0);
      for (const [value, key] of entries) {
        expect(typeof lookup(enUS, key), `${name}.${value} -> ${key} (en-US)`).toBe(
          'string',
        );
        expect(typeof lookup(zhCN, key), `${name}.${value} -> ${key} (zh-CN)`).toBe(
          'string',
        );
      }
    });
  }

  it('covers all sixteen attempt statuses', () => {
    expect(Object.keys(ORDER_STATUS_KEY)).toHaveLength(16);
  });

  // Three of the five are durations vendors added after the ledger shipped,
  // and an untranslated one renders as its own wire value on an order row.
  it('covers every duration an order can stand for', () => {
    expect(Object.keys(ORDER_TIME_IN_FORCE_KEY).sort()).toEqual([
      'at_the_open',
      'day',
      'gtc',
      'overnight',
      'overnight_next_day',
    ]);
  });
});

/**
 * A staged order that reached the vendor reached no market: the instruction is
 * waiting in the broker's own client for the user to confirm, and expires in
 * seven days. Both pills ask this one function, so the chat receipt and the
 * Orders row can never disagree about what that state is called.
 */
describe('what a staged order is called', () => {
  it('names the broker to confirm in', () => {
    expect(orderStatusLabel('submitted', 'staged', 'Interactive Brokers')).toEqual({
      key: 'toolArtifact.directTool.orderStatus.submittedStaged',
      params: { vendor: 'Interactive Brokers' },
    });
  });

  // A surface that could not resolve the broker still must not say the order
  // was sent, so it says the true half of the sentence and stops.
  it('drops the broker rather than the meaning', () => {
    for (const label of [undefined, null, '', '   ']) {
      expect(orderStatusLabel('submitted', 'staged', label)).toEqual({
        key: 'toolArtifact.directTool.orderStatus.submittedStagedUnnamed',
      });
    }
  });

  it('names the broker a handed-off instruction left, or drops it', () => {
    expect(orderStatusLabel('handed_off', 'staged', 'Interactive Brokers')).toEqual({
      key: 'toolArtifact.directTool.orderStatus.handedOffStaged',
      params: { vendor: 'Interactive Brokers' },
    });
    expect(orderStatusLabel('handed_off', 'staged', null)).toEqual({
      key: 'toolArtifact.directTool.orderStatus.handedOffStagedUnnamed',
    });
  });

  it('says a handed-off instruction was confirmed only when the broker said so', () => {
    expect(orderStatusLabel('handed_off', 'staged', 'Webull', true)).toEqual({
      key: 'toolArtifact.directTool.orderStatus.handedOffConfirmedStaged',
      params: { vendor: 'Webull' },
    });
    expect(orderStatusLabel('handed_off', 'staged', null, true)).toEqual({
      key: 'toolArtifact.directTool.orderStatus.handedOffConfirmedStagedUnnamed',
    });
    expect(orderStatusShortLabel('handed_off', 'staged', true)).toBe(
      'toolArtifact.directTool.orderStatusShort.handedOffConfirmed',
    );
    // A broker that only stopped listing it keeps the sentence that admits it.
    expect(orderStatusLabel('handed_off', 'staged', 'Webull', false).key).toBe(
      'toolArtifact.directTool.orderStatus.handedOffStaged',
    );
    // Naming the end changes no other state.
    expect(orderStatusLabel('submitted', 'staged', 'Webull', true).key).toBe(
      'toolArtifact.directTool.orderStatus.submittedStaged',
    );
  });

  it('says a live order the user approved at the broker was confirmed there', () => {
    expect(orderStatusLabel('handed_off', 'live', 'Robinhood', true)).toEqual({
      key: 'toolArtifact.directTool.orderStatus.handedOffConfirmedStaged',
      params: { vendor: 'Robinhood' },
    });
    expect(orderStatusShortLabel('handed_off', 'live', true)).toBe(
      'toolArtifact.directTool.orderStatusShort.handedOffConfirmed',
    );
  });

  // Nobody cancelled it, so it must never read as cancelled.
  it('says an expired instruction expired before it was confirmed', () => {
    expect(orderStatusLabel('expired', 'staged', 'Webull')).toEqual({
      key: 'toolArtifact.directTool.orderStatus.expiredStaged',
      params: { vendor: 'Webull' },
    });
    expect(orderStatusLabel('expired', 'staged', null)).toEqual({
      key: 'toolArtifact.directTool.orderStatus.expiredStagedUnnamed',
    });
    expect(orderStatusLabel('expired', 'live', 'Webull')).toEqual({
      key: 'toolArtifact.directTool.orderStatus.expired',
    });
    expect(orderStatusShortLabel('expired', 'staged')).toBe(
      'toolArtifact.directTool.orderStatusShort.expired',
    );
  });

  it('reads the broker as having named the end only on an explicit null', () => {
    expect(vendorNamedEnd(null)).toBe(true);
    expect(vendorNamedEnd('2026-10-26T01:00:00+00:00')).toBe(false);
    // A record from before the ledger sent the field says nothing either way.
    expect(vendorNamedEnd(undefined)).toBe(false);
  });

  it('shortens to "Staged" on a row, and only there', () => {
    expect(orderStatusShortLabel('submitted', 'staged')).toBe(
      'toolArtifact.directTool.orderStatusShort.submittedStaged',
    );
    expect(orderStatusShortLabel('submitted', 'paper')).toBe(
      'toolArtifact.directTool.orderStatusShort.submitted',
    );
  });

  it('leaves every other mode on the word it had', () => {
    for (const mode of ['live', 'paper', null, undefined] as const) {
      expect(orderStatusLabel('submitted', mode, 'moomoo')).toEqual({
        key: ORDER_STATUS_KEY.submitted,
      });
    }
  });

  it('leaves every other staged state on the word it had', () => {
    for (const status of ORDER_STATUSES) {
      if (status === 'submitted' || status === 'handed_off' || status === 'expired') continue;
      expect(orderStatusLabel(status, 'staged', 'Interactive Brokers')).toEqual({
        key: ORDER_STATUS_KEY[status],
      });
    }
  });

  it('resolves both staged sentences in both catalogs', () => {
    for (const key of ORDER_STATUS_STAGED_KEYS) {
      expect(typeof lookup(enUS, key), `${key} (en-US)`).toBe('string');
      expect(typeof lookup(zhCN, key), `${key} (zh-CN)`).toBe('string');
    }
    expect(lookup(enUS, ORDER_STATUS_STAGED_KEYS[0])).toContain('{{vendor}}');
    expect(lookup(zhCN, ORDER_STATUS_STAGED_KEYS[0])).toContain('{{vendor}}');
  });
});
