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
} from '@/components/orders/status';
import {
  ORDER_ACTION_KEY,
  ORDER_ASSET_CLASS_KEY,
  ORDER_SIDE_KEY,
  ORDER_TIME_IN_FORCE_KEY,
  ORDER_TYPE_KEY,
} from '../utils/labels';
import { ORDER_STATUSES } from '@/pages/ChatAgent/utils/api';

/**
 * Two of these maps come from the shared order module rather than from the
 * page: the status words are the chat receipt's and the mode words are the
 * Plugins badge's, on purpose. The locale sweep only reads bare keys under
 * `orders.`, so nothing else would notice if one of those were renamed under
 * us, and a missing key renders as the key itself on a row about an order
 * somebody placed.
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

  it('covers all fourteen attempt statuses', () => {
    expect(Object.keys(ORDER_STATUS_KEY)).toHaveLength(14);
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
      if (status === 'submitted') continue;
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
