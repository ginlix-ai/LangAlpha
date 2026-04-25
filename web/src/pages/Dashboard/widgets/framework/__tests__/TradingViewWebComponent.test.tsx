import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, act, screen } from '@testing-library/react';
import { TradingViewWebComponent } from '../TradingViewWebComponent';

vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'dark' }),
}));

describe('TradingViewWebComponent', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    // Pretend the element is already registered in customElements so the
    // loader skips its `import()` (jsdom can't fetch external URLs anyway).
    if (window.customElements && !window.customElements.get('tv-ticker-tape')) {
      class TVStub extends HTMLElement {}
      window.customElements.define('tv-ticker-tape', TVStub);
    }
  });
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('mounts the custom element with kebab-cased attributes once loaded', async () => {
    const { container } = render(
      <TradingViewWebComponent
        element="tv-ticker-tape"
        config={{ displayMode: 'compact', isTransparent: true }}
      />,
    );

    // Loader resolves on a microtask + customElements.whenDefined → flush.
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    const el = container.querySelector('tv-ticker-tape');
    expect(el).not.toBeNull();
    // camelCase → kebab-case
    expect(el?.getAttribute('display-mode')).toBe('compact');
    // Boolean true → presence (empty string)
    expect(el?.getAttribute('is-transparent')).toBe('');
    // Theme stamped reactively from useTheme()
    expect(el?.getAttribute('theme')).toBe('dark');
    expect(el?.getAttribute('color-theme')).toBe('dark');
  });

  it('skips false / null / undefined attribute values (presence trap)', async () => {
    const { container } = render(
      <TradingViewWebComponent
        element="tv-ticker-tape"
        config={{ hideLegend: false, optional: null, alsoOptional: undefined, hideTopToolbar: true }}
      />,
    );
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    const el = container.querySelector('tv-ticker-tape');
    expect(el).not.toBeNull();
    // false → SHOULD NOT be present (otherwise TV reads as truthy)
    expect(el?.hasAttribute('hide-legend')).toBe(false);
    expect(el?.hasAttribute('optional')).toBe(false);
    expect(el?.hasAttribute('also-optional')).toBe(false);
    // true booleans are kept as presence
    expect(el?.getAttribute('hide-top-toolbar')).toBe('');
  });

  it('updates attributes IN PLACE on config change without remount', async () => {
    const { container, rerender } = render(
      <TradingViewWebComponent element="tv-ticker-tape" config={{ displayMode: 'compact' }} />,
    );
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    const initialEl = container.querySelector('tv-ticker-tape');
    rerender(
      <TradingViewWebComponent element="tv-ticker-tape" config={{ displayMode: 'regular' }} />,
    );
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    const sameEl = container.querySelector('tv-ticker-tape');
    expect(sameEl).toBe(initialEl);
    expect(sameEl?.getAttribute('display-mode')).toBe('regular');
  });

  it('shows the fallback when the element fails to register inside the timeout', async () => {
    // Use an element name that's never been defined. jsdom can't fetch the
    // external URL — `import()` rejects on next tick. The component's error
    // handler then flips status to 'error'. Drain enough microtasks to let
    // the rejection settle, then assert the fallback rendered.
    render(<TradingViewWebComponent element="tv-not-a-real-element-xyz-zzz" config={{}} />);
    // Promise.reject from import() unwinds across several microtask ticks +
    // the React state update + the re-render. Advance the 15s timeout in
    // case the import simply never resolves in jsdom.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(20_000);
    });
    expect(screen.getByText(/widget unavailable/i)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /retry/i })).toBeInTheDocument();
  });
});
