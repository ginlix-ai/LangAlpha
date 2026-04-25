import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, act } from '@testing-library/react';
import { TradingViewEmbed } from '../TradingViewEmbed';

// ThemeContext is consumed via useTheme() — stub it so the component mounts
// cleanly in jsdom without the full provider tree.
vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'dark' }),
}));

describe('TradingViewEmbed', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('injects a TV embed-widget script with the correct src + config', () => {
    const { container } = render(
      <TradingViewEmbed scriptKey="ticker-tape" config={{ symbols: [{ proName: 'NVDA', description: 'NVDA' }] }} />,
    );
    const script = container.querySelector('script[src*="embed-widget-ticker-tape"]');
    expect(script).toBeTruthy();
    const payload = JSON.parse((script as HTMLScriptElement).innerHTML);
    expect(payload.autosize).toBe(true);
    expect(payload.colorTheme).toBe('dark');
    expect(payload.symbols).toBeTruthy();
  });

  it('shows the fallback state when no iframe materialises within 10s', () => {
    render(<TradingViewEmbed scriptKey="ticker-tape" config={{}} />);
    // No iframe — the timeout should trip and the fallback appears.
    act(() => {
      vi.advanceTimersByTime(10_050);
    });
    expect(screen.getByText(/widget unavailable/i)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /retry/i })).toBeInTheDocument();
  });

  it('throws via console.error for an unknown script key (no silent failure)', () => {
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    render(<TradingViewEmbed scriptKey="not-a-real-widget" config={{}} />);
    // The component setState's to error synchronously via the try/catch
    // inside the effect; the fallback UI confirms the caught error.
    expect(screen.getByText(/widget unavailable/i)).toBeInTheDocument();
    expect(consoleSpy).toHaveBeenCalled();
  });

  it('clears a stuck error overlay when config changes trigger a rebuild', () => {
    const { rerender } = render(
      <TradingViewEmbed scriptKey="ticker-tape" config={{ symbols: [{ proName: 'NVDA' }] }} />,
    );
    // Trip the 10s no-iframe timeout to land in the error state.
    act(() => {
      vi.advanceTimersByTime(10_050);
    });
    expect(screen.getByText(/widget unavailable/i)).toBeInTheDocument();

    // Config change (e.g., user swaps symbol in settings) should reset the
    // error overlay back to loading on the next rebuild — without waiting
    // for the new iframe (or the next 10s timeout) to arrive.
    act(() => {
      rerender(
        <TradingViewEmbed scriptKey="ticker-tape" config={{ symbols: [{ proName: 'AAPL' }] }} />,
      );
    });
    expect(screen.queryByText(/widget unavailable/i)).not.toBeInTheDocument();
  });
});
