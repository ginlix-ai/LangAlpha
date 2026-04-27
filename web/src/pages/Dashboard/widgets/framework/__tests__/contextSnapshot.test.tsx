import { describe, expect, it, beforeEach, vi } from 'vitest';
import { renderHook } from '@testing-library/react';
import {
  __resetWidgetContextRegistryForTests,
  getWidgetContextSnapshot,
  hasWidgetContextExporter,
  useWidgetContextExport,
  type WidgetContextSnapshot,
} from '../contextSnapshot';

const sampleSnapshot = (id: string, label = 'X'): WidgetContextSnapshot => ({
  widget_type: 'markets.chart',
  widget_id: id,
  label,
  captured_at: '2026-04-26T11:42:08Z',
  text: '<widget-context>x</widget-context>',
  data: {},
});

describe('useWidgetContextExport / registry', () => {
  beforeEach(() => {
    __resetWidgetContextRegistryForTests();
  });

  it('register on mount, unregister on unmount', async () => {
    const exporters = { full: () => sampleSnapshot('w1') };
    const { unmount } = renderHook(() => useWidgetContextExport('w1', exporters));
    expect(hasWidgetContextExporter('w1')).toBe(true);
    expect((await getWidgetContextSnapshot('w1'))?.label).toBe('X');
    unmount();
    expect(hasWidgetContextExporter('w1')).toBe(false);
  });

  it('returns null for unregistered instance', async () => {
    expect(await getWidgetContextSnapshot('does-not-exist')).toBeNull();
  });

  it('re-registers when instanceId changes', async () => {
    const exportersA = { full: () => sampleSnapshot('a', 'A') };
    const exportersB = { full: () => sampleSnapshot('b', 'B') };
    const { rerender, unmount } = renderHook(
      ({ id, exporters }: { id: string; exporters: { full: () => WidgetContextSnapshot } }) =>
        useWidgetContextExport(id, exporters),
      { initialProps: { id: 'a', exporters: exportersA } },
    );
    expect(hasWidgetContextExporter('a')).toBe(true);
    rerender({ id: 'b', exporters: exportersB });
    expect(hasWidgetContextExporter('a')).toBe(false);
    expect(hasWidgetContextExporter('b')).toBe(true);
    expect((await getWidgetContextSnapshot('b'))?.label).toBe('B');
    unmount();
  });

  it('row exporter returns null for missing rows', async () => {
    const exporters = {
      full: () => sampleSnapshot('w1'),
      rows: (rowId: string) => (rowId === 'r1' ? sampleSnapshot('w1', 'row-1') : null),
    };
    renderHook(() => useWidgetContextExport('w1', exporters));
    expect((await getWidgetContextSnapshot('w1', 'r1'))?.label).toBe('row-1');
    expect(await getWidgetContextSnapshot('w1', 'r-missing')).toBeNull();
  });

  it('async row exporter resolves to a snapshot', async () => {
    const exporters = {
      full: () => sampleSnapshot('w1'),
      rows: async (rowId: string) =>
        rowId === 'r1' ? sampleSnapshot('w1', 'async-row') : null,
    };
    renderHook(() => useWidgetContextExport('w1', exporters));
    expect((await getWidgetContextSnapshot('w1', 'r1'))?.label).toBe('async-row');
    expect(await getWidgetContextSnapshot('w1', 'r-missing')).toBeNull();
  });

  it('row request on a widget without rows returns null', async () => {
    const exporters = { full: () => sampleSnapshot('w1') };
    renderHook(() => useWidgetContextExport('w1', exporters));
    expect(await getWidgetContextSnapshot('w1', 'r1')).toBeNull();
  });

  it('exporter that throws is caught and returns null', async () => {
    const consoleErr = vi.spyOn(console, 'error').mockImplementation(() => {});
    const exporters = {
      full: () => {
        throw new Error('boom');
      },
    };
    renderHook(() => useWidgetContextExport('w1', exporters));
    expect(await getWidgetContextSnapshot('w1')).toBeNull();
    expect(consoleErr).toHaveBeenCalled();
    consoleErr.mockRestore();
  });

  it('async row exporter that rejects is caught and returns null', async () => {
    const consoleErr = vi.spyOn(console, 'error').mockImplementation(() => {});
    const exporters = {
      full: () => sampleSnapshot('w1'),
      rows: async () => {
        throw new Error('fetch boom');
      },
    };
    renderHook(() => useWidgetContextExport('w1', exporters));
    expect(await getWidgetContextSnapshot('w1', 'r1')).toBeNull();
    expect(consoleErr).toHaveBeenCalled();
    consoleErr.mockRestore();
  });
});
