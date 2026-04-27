import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ChatInputRegistry, ContextBus } from '../contextBus';
import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';

const SNAP: WidgetContextSnapshot = {
  widget_type: 'markets.chart',
  widget_id: 'w1',
  label: 'NVDA',
  captured_at: '2026-04-26T11:42:08Z',
  text: '<widget-context>x</widget-context>',
  data: {},
};

describe('ContextBus', () => {
  beforeEach(() => {
    ContextBus.__resetForTests();
  });
  afterEach(() => {
    ContextBus.__resetForTests();
  });

  it('subscribers receive attach events', () => {
    const handler = vi.fn();
    ContextBus.subscribe(handler);
    ContextBus.attach(SNAP);
    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler).toHaveBeenCalledWith({ type: 'attach', snapshot: SNAP });
  });

  it('subscribers receive detach events', () => {
    const handler = vi.fn();
    ContextBus.subscribe(handler);
    ContextBus.detach('w1');
    expect(handler).toHaveBeenCalledWith({ type: 'detach', widgetId: 'w1' });
  });

  it('subscribers receive clear events', () => {
    const handler = vi.fn();
    ContextBus.subscribe(handler);
    ContextBus.clear();
    expect(handler).toHaveBeenCalledWith({ type: 'clear' });
  });

  it('multiple subscribers all receive events', () => {
    const a = vi.fn();
    const b = vi.fn();
    ContextBus.subscribe(a);
    ContextBus.subscribe(b);
    ContextBus.attach(SNAP);
    expect(a).toHaveBeenCalledTimes(1);
    expect(b).toHaveBeenCalledTimes(1);
  });

  it('unsubscribe stops further events', () => {
    const handler = vi.fn();
    const unsub = ContextBus.subscribe(handler);
    ContextBus.attach(SNAP);
    unsub();
    ContextBus.attach(SNAP);
    expect(handler).toHaveBeenCalledTimes(1);
  });

  it('throwing subscribers do not break siblings', () => {
    const consoleErr = vi.spyOn(console, 'error').mockImplementation(() => {});
    const bad = vi.fn(() => {
      throw new Error('boom');
    });
    const good = vi.fn();
    ContextBus.subscribe(bad);
    ContextBus.subscribe(good);
    ContextBus.attach(SNAP);
    expect(bad).toHaveBeenCalledTimes(1);
    expect(good).toHaveBeenCalledTimes(1);
    expect(consoleErr).toHaveBeenCalled();
    consoleErr.mockRestore();
  });

  it('a subscriber that unsubscribes during dispatch does not crash', () => {
    const a = vi.fn();
    let unsubB: (() => void) | null = null;
    const b = vi.fn(() => {
      unsubB?.();
    });
    ContextBus.subscribe(a);
    unsubB = ContextBus.subscribe(b);
    ContextBus.attach(SNAP);
    expect(a).toHaveBeenCalledTimes(1);
    expect(b).toHaveBeenCalledTimes(1);
  });
});

describe('ChatInputRegistry', () => {
  beforeEach(() => {
    ChatInputRegistry.__resetForTests();
  });
  afterEach(() => {
    ChatInputRegistry.__resetForTests();
  });

  it('register / unregister tracks elements', () => {
    const a = document.createElement('div');
    const b = document.createElement('div');
    expect(ChatInputRegistry.size()).toBe(0);
    const offA = ChatInputRegistry.register(a);
    const offB = ChatInputRegistry.register(b);
    expect(ChatInputRegistry.size()).toBe(2);
    offA();
    expect(ChatInputRegistry.size()).toBe(1);
    expect(ChatInputRegistry.list()).toEqual([b]);
    offB();
    expect(ChatInputRegistry.size()).toBe(0);
  });

  it('listeners fire on register and unregister', () => {
    const listener = vi.fn();
    ChatInputRegistry.subscribe(listener);
    const off = ChatInputRegistry.register(document.createElement('div'));
    expect(listener).toHaveBeenCalledTimes(1);
    off();
    expect(listener).toHaveBeenCalledTimes(2);
  });
});
