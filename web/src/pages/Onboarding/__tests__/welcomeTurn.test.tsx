import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, render, screen } from '@testing-library/react';

// jsdom gives framer no frames, so the typewriter never moves on its own. This
// one runs each chain to its end on the next timer, which is enough to see
// where a chain stops: before the line's last word, or at its end.
type Opts = { onUpdate: (v: number) => void; onComplete: () => void };
vi.mock('@/lib/framer', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/framer')>()),
  useReducedMotion: () => false,
  animate: (_from: number, to: number, opts: Opts) => {
    const timer = setTimeout(() => {
      opts.onUpdate(to);
      opts.onComplete();
    }, 0);
    return { stop: () => clearTimeout(timer) };
  },
}));

import { REVEAL_SETTLE_MS } from '@/pages/ChatAgent/utils/revealFade';
import { AgentTurn } from '../welcome/conversation';

const LINE = 'How much risk are you comfortable with? It guides how I balance potential gains against losses.';

describe('AgentTurn', () => {
  beforeEach(() => {
    vi.useFakeTimers({
      toFake: ['setTimeout', 'clearTimeout', 'requestAnimationFrame', 'cancelAnimationFrame', 'Date'],
    });
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('writes its whole line without waiting out the stream quiet, then brings up its card once it settles', () => {
    render(
      <AgentTurn text={LINE} typing testId="turn">
        <div data-testid="card" />
      </AgentTurn>,
    );
    // Past the lead the line waits out, and well short of the second a live
    // stream's last word is held through after it.
    for (let i = 0; i < 20; i++) {
      act(() => {
        vi.advanceTimersByTime(20);
      });
    }
    expect(screen.getByTestId('turn')).toHaveTextContent(LINE);
    // The text first: its last word is still fading in.
    expect(screen.queryByTestId('card')).not.toBeInTheDocument();
    act(() => {
      vi.advanceTimersByTime(REVEAL_SETTLE_MS);
    });
    expect(screen.getByTestId('card')).toBeInTheDocument();
  });

  it('holds its line back while the answer above it lands', () => {
    render(
      <AgentTurn text={LINE} typing testId="turn">
        <div data-testid="card" />
      </AgentTurn>,
    );
    act(() => {
      vi.advanceTimersByTime(120);
    });
    expect(screen.getByTestId('turn')).toHaveTextContent('');
    expect(screen.queryByTestId('card')).not.toBeInTheDocument();
  });

  it('shows a turn that does not type at once, card included', () => {
    render(
      <AgentTurn text={LINE} testId="turn">
        <div data-testid="card" />
      </AgentTurn>,
    );
    expect(screen.getByTestId('turn')).toHaveTextContent(LINE);
    expect(screen.getByTestId('card')).toBeInTheDocument();
  });
});
