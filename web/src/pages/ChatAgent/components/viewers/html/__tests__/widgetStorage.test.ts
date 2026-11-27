import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { storageUser } from '@/lib/userStorage';
import {
  readWidgetStorage,
  resetWidgetStorage,
  subscribeWidgetStorage,
  writeWidgetStorage,
  WIDGET_STORAGE_MAX,
} from '../widgetStorage';

// Outside platform mode the storage user never changes, so a switch is the mock's.
const account = vi.hoisted(() => ({ switchedTo: null as string | null }));
vi.mock('@/lib/userStorage', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/userStorage')>();
  return { ...actual, storageUser: () => account.switchedTo ?? actual.storageUser() };
});

const KEY = (id: string) => `user:${storageUser()}:widgetStorage:${id}`;
const INDEX = `user:${storageUser()}:widgetStorage.index`;
/** A burst's writes reach localStorage a microtask later. */
const flushed = () => Promise.resolve();

describe('widgetStorage', () => {
  beforeEach(() => {
    localStorage.clear();
    resetWidgetStorage();
  });
  afterEach(() => vi.restoreAllMocks());

  it('applies sets, removals and clears', async () => {
    writeWidgetStorage('w1', { key: 'a', value: '1' });
    writeWidgetStorage('w1', { key: 'b', value: '2' });
    writeWidgetStorage('w1', { key: 'a', value: null });
    expect(readWidgetStorage('w1')).toEqual({ b: '2' });
    await flushed();
    resetWidgetStorage();
    expect(readWidgetStorage('w1')).toEqual({ b: '2' });
    writeWidgetStorage('w1', { clear: true });
    expect(readWidgetStorage('w1')).toEqual({});
    await flushed();
    expect(localStorage.getItem(KEY('w1'))).toBeNull();
    expect(JSON.parse(localStorage.getItem(INDEX)!)).toEqual([]);
  });

  it("drops a write from a frame opened under the account before a switch", async () => {
    const oldFrame = vi.fn();
    const offOld = subscribeWidgetStorage('w1', oldFrame);
    account.switchedTo = 'someone-else';
    try {
      const newFrame = vi.fn();
      const offNew = subscribeWidgetStorage('w1', newFrame);
      writeWidgetStorage('w1', { key: 'a', value: 'old account' }, oldFrame);
      expect(newFrame).not.toHaveBeenCalled();
      expect(readWidgetStorage('w1')).toEqual({});
      writeWidgetStorage('w1', { key: 'a', value: 'new account' }, newFrame);
      expect(oldFrame).not.toHaveBeenCalled();
      expect(readWidgetStorage('w1')).toEqual({ a: 'new account' });
      offNew();
    } finally {
      offOld();
      account.switchedTo = null;
      await flushed();
    }
  });

  it('writes a burst once', async () => {
    const setItem = vi.spyOn(Storage.prototype, 'setItem');
    for (let i = 0; i < 50; i++) writeWidgetStorage('w1', { key: 'n', value: String(i) });
    expect(setItem).not.toHaveBeenCalled();
    await flushed();
    // The record and the index.
    expect(setItem).toHaveBeenCalledTimes(2);
    expect(localStorage.getItem(KEY('w1'))).toBe('{"n":"49"}');
    expect(JSON.parse(localStorage.getItem(INDEX)!)).toEqual([{ id: 'w1', size: '{"n":"49"}'.length }]);
  });

  it('drops a write that takes one widget past its cap', () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    writeWidgetStorage('w1', { key: 'a', value: 'kept' });
    writeWidgetStorage('w1', { key: 'big', value: 'x'.repeat(WIDGET_STORAGE_MAX) });
    expect(readWidgetStorage('w1')).toEqual({ a: 'kept' });
    expect(warn).toHaveBeenCalledTimes(1);
  });

  it('measures a record as it serializes, escapes included', async () => {
    const value = '"\n'.repeat(1000);
    writeWidgetStorage('w1', { key: 'q"', value });
    writeWidgetStorage('w1', { key: 'b', value: 'x' });
    await flushed();
    const [entry] = JSON.parse(localStorage.getItem(INDEX)!) as Array<{ size: number }>;
    expect(entry.size).toBe(localStorage.getItem(KEY('w1'))!.length);
  });

  // Widgets never fill the quota the app's own settings are written into.
  it('evicts the least recently written widgets past the total', async () => {
    const value = 'x'.repeat(WIDGET_STORAGE_MAX - 100);
    for (const id of ['w1', 'w2', 'w3', 'w4', 'w5']) writeWidgetStorage(id, { key: 'v', value });
    await flushed();
    expect(readWidgetStorage('w1')).toEqual({});
    expect(readWidgetStorage('w5')).toEqual({ v: value });
    expect(localStorage.getItem(KEY('w1'))).toBeNull();
  });

  // A refused write must cost nothing: no other widget's record evicted to
  // make room it never took, and no index naming a record that is not there.
  it('evicts nothing when its own write is refused', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const value = 'x'.repeat(WIDGET_STORAGE_MAX - 100);
    for (const id of ['w1', 'w2', 'w3', 'w4']) writeWidgetStorage(id, { key: 'v', value });
    await flushed();
    const index = localStorage.getItem(INDEX);

    const real = Storage.prototype.setItem;
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(function (this: Storage, key: string, v: string) {
      if (key === KEY('w5')) throw new DOMException('full', 'QuotaExceededError');
      real.call(this, key, v);
    });
    writeWidgetStorage('w5', { key: 'v', value });
    await flushed();

    expect(localStorage.getItem(KEY('w1'))).not.toBeNull();
    expect(localStorage.getItem(KEY('w5'))).toBeNull();
    expect(localStorage.getItem(INDEX)).toBe(index);
    expect(warn).toHaveBeenCalledWith('[widget] localStorage write not kept: the browser refused it');
  });

  it('reads a record that does not parse as none', () => {
    localStorage.setItem(KEY('w1'), '{not json');
    expect(readWidgetStorage('w1')).toEqual({});
  });
});
