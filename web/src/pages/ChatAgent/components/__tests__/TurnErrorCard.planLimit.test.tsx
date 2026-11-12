/**
 * A turn a Claude plan's limit ended reads under its own headline, with the
 * sentence that says until when under it, on the failed turn's card and on
 * the composer's banner a refused send shows: never the length-picked
 * "Something went wrong" a long message would otherwise get.
 */
import { describe, it, expect, vi } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';

vi.mock('@/hooks/useClaudeCodePlan', () => ({
  usePlanClock: () => ({ locale: 'en-US', zone: 'America/New_York', now: Date.parse('2026-11-06T19:05:00Z') }),
}));
vi.mock('../Markdown', () => ({ default: () => null }));

import { TurnErrorCard } from '../TextMessageContent';
import { ErrorBanner } from '@/components/ui/error-banner';
import type { StructuredError } from '@/utils/rateLimitError';

const stopped: StructuredError = {
  message: 'Server words.',
  planRefusal: { reason: 'plan_stopped', window: 'five_hour', until: '2026-11-06T20:40:00Z', creditsBilled: false },
};
const SENTENCE = "Your Claude plan's 5-hour limit is used up, so this turn stopped. It resets at 3:40 PM. Nothing ran on another model.";

describe('a Claude plan limit', () => {
  it("heads a failed turn's card with the limit, over its sentence", () => {
    renderWithProviders(<TurnErrorCard err={stopped} />);
    expect(screen.getByText('Claude plan limit reached')).toBeInTheDocument();
    expect(screen.getByText(SENTENCE)).toBeInTheDocument();
    expect(screen.queryByText('Server words.')).toBeNull();
  });

  it('heads the banner of a send refused before it started', () => {
    const refused: StructuredError = {
      message: 'Server words.',
      planRefusal: { reason: 'plan_limited', window: 'seven_day', until: '2026-11-12T14:00:00Z', creditsBilled: false },
    };
    renderWithProviders(<ErrorBanner error={refused} />);
    expect(screen.getByText('Claude plan limit reached')).toBeInTheDocument();
    expect(screen.getByText(
      "Your Claude plan's weekly limit is used up until Thu, Nov 12, 9:00 AM. Pick another model, or send this again after it resets.",
    )).toBeInTheDocument();
  });
});

describe("a subscription's limit", () => {
  const limited = (planLimit: StructuredError['planLimit']): StructuredError => ({
    message: "Error code: 429 - {'error': {'type': 'usage_limit_reached'}}",
    kind: 'upstream',
    statusCode: 429,
    payer: 'subscription',
    account: 'ChatGPT',
    reason: 'plan_limit',
    hints: ['try_another_model'],
    planLimit,
  });

  it('names the window and the reset when both are known', () => {
    renderWithProviders(<TurnErrorCard err={limited({ plan: 'ChatGPT Plus', window: 'five_hour', until: '2026-11-06T20:40:00Z' })} />);
    expect(screen.getByText('ChatGPT Plus plan limit reached')).toBeInTheDocument();
    expect(screen.getByText(
      "Your ChatGPT Plus plan's 5-hour limit is used up. It resets at 3:40 PM. Pick another model, or send this again after it resets.",
    )).toBeInTheDocument();
    // The plan working is not the provider's words, an account line or hints.
    expect(screen.queryByText(/usage_limit_reached/)).toBeNull();
    expect(screen.queryByText(/connected subscription/)).toBeNull();
    expect(screen.queryByRole('listitem')).toBeNull();
  });

  it('names the reset alone when the window is unknown', () => {
    renderWithProviders(<TurnErrorCard err={limited({ plan: 'Grok', window: null, until: '2026-11-12T14:00:00Z' })} />);
    expect(screen.getByText(
      "Your Grok plan's limit is used up. It resets at Thu, Nov 12, 9:00 AM. Pick another model, or send this again after it resets.",
    )).toBeInTheDocument();
  });

  it('says only that it is used up when the reset is unknown', () => {
    renderWithProviders(<TurnErrorCard err={limited({ plan: 'Claude', window: 'seven_day', until: null })} />);
    expect(screen.getByText('Claude plan limit reached')).toBeInTheDocument();
    expect(screen.getByText("Your Claude plan's limit is used up. Pick another model, or send this again later.")).toBeInTheDocument();
  });
});
