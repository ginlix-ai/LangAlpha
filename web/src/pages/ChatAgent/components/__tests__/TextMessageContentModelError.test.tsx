/**
 * Model-aware error display: when the backend attributes an upstream failure to
 * a specific model, StructuredErrorDisplay names it in the headline and — when
 * the resilience middleware tried more than one model — lists the others under
 * an "Also tried" line (per-model error text in a title tooltip, not inline).
 *
 * Mocks react-i18next with an interpolating identity `t` (mirrors
 * TextMessageContentError.test.tsx) so we can assert which key path + params
 * were used without wiring the full i18next runtime.
 *
 * Neutral placeholder model names only — no production data.
 */
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import React from 'react';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string, opts?: Record<string, unknown>) => {
      if (!opts) return key;
      if (opts.model !== undefined && opts.status !== undefined) return `${key}:${opts.model}:${opts.status}`;
      if (opts.model !== undefined) return `${key}:${opts.model}`;
      if (opts.status !== undefined) return `${key}:${opts.status}`;
      return key;
    },
  }),
}));

vi.mock('../Markdown', () => ({ default: () => null }));
vi.mock('@/components/ui/animated-text', () => ({
  useAnimatedText: (text: string) => text,
}));

import TextMessageContent from '../TextMessageContent';
import type { StructuredError } from '@/utils/rateLimitError';

function renderErr(structured: StructuredError) {
  return render(
    <TextMessageContent
      content={structured.message}
      isStreaming={false}
      hasError={true}
      structuredError={structured}
    />,
  );
}

describe('StructuredErrorDisplay — model-aware headline + "Also tried"', () => {
  it('renders a model-aware headline with status when model + statusCode present', () => {
    renderErr({
      message: 'Error code: 500 - upstream unavailable',
      kind: 'upstream',
      statusCode: 500,
      model: 'model-alpha',
    });
    expect(screen.getByText('chat.errorUpstreamHeadlineModelStatus:model-alpha:500')).toBeInTheDocument();
  });

  it('renders a model-aware headline without status when statusCode is absent', () => {
    renderErr({
      message: 'connection reset',
      kind: 'upstream',
      model: 'model-alpha',
    });
    expect(screen.getByText('chat.errorUpstreamHeadlineModel:model-alpha')).toBeInTheDocument();
  });

  it('falls back to the generic provider headline when no model is attributed', () => {
    renderErr({
      message: 'connection reset',
      kind: 'upstream',
      statusCode: 500,
    });
    expect(screen.getByText('chat.errorUpstreamHeadlineStatus:500')).toBeInTheDocument();
  });

  it('shows an "Also tried" line (primary excluded) with per-model error in a tooltip', () => {
    renderErr({
      message: 'all models failed',
      kind: 'upstream',
      model: 'model-alpha',
      attemptedModels: [
        { model: 'model-alpha', error: '500 upstream unavailable' },
        { model: 'model-beta', error: 'timed out' },
      ],
    });

    expect(screen.getByText('chat.errorAttemptedModels')).toBeInTheDocument();
    // The non-primary model is listed; its error rides a title tooltip.
    const beta = screen.getByText('model-beta');
    expect(beta).toHaveAttribute('title', 'timed out');
    // The primary is filtered out of the "Also tried" list, so its error text
    // is not exposed as a tooltip anywhere.
    expect(screen.queryByTitle('500 upstream unavailable')).not.toBeInTheDocument();
  });

  it('omits the "Also tried" line when only one model was attempted', () => {
    renderErr({
      message: 'model failed',
      kind: 'upstream',
      model: 'model-alpha',
      attemptedModels: [{ model: 'model-alpha', error: '500 upstream unavailable' }],
    });
    expect(screen.queryByText('chat.errorAttemptedModels')).not.toBeInTheDocument();
  });
});
