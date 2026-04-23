/**
 * Regression: when the backend classified a failure as ``upstream``, the
 * assistant bubble renders the rich ``StructuredErrorDisplay`` (headline +
 * message + hints) instead of the regex-based ``ErrorDisplay`` that reparses
 * the raw text.
 *
 * Mocks ``react-i18next`` with an identity ``t`` (same pattern as
 * FileErrorDisplay.test.tsx) so headline / hint lookups render the i18n key
 * back — we only need to confirm the right key path was taken.
 */
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import React from 'react';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string, opts?: Record<string, unknown>) => (opts?.status ? `${key}:${opts.status}` : key) }),
}));

// Avoid pulling Markdown's full ESM dep chain into jsdom — we never render
// it in these tests because content is always under the error branch.
vi.mock('../Markdown', () => ({ default: () => null }));
// Animated text in streaming mode just passes text through — no-op for tests.
vi.mock('@/components/ui/animated-text', () => ({
  useAnimatedText: (text: string) => text,
}));

import TextMessageContent from '../TextMessageContent';
import type { StructuredError } from '@/utils/rateLimitError';

describe('TextMessageContent error routing', () => {
  it('renders StructuredErrorDisplay when structuredError.kind is upstream', () => {
    const structured: StructuredError = {
      message: "Error code: 401 - login fail",
      kind: 'upstream',
      statusCode: 401,
      hints: ['api_key', 'model_access', 'provider_status', 'try_another_model'],
    };
    render(
      <TextMessageContent
        content="Error code: 401 - login fail"
        isStreaming={false}
        hasError={true}
        structuredError={structured}
      />,
    );

    // Headline comes from the status-aware key.
    expect(screen.getByText('chat.errorUpstreamHeadlineStatus:401')).toBeInTheDocument();
    // Each hint renders its i18n key (identity mock).
    expect(screen.getByText('chat.errorHintApiKey')).toBeInTheDocument();
    expect(screen.getByText('chat.errorHintModelAccess')).toBeInTheDocument();
    expect(screen.getByText('chat.errorHintProviderStatus')).toBeInTheDocument();
    expect(screen.getByText('chat.errorHintTryAnotherModel')).toBeInTheDocument();
  });

  it('falls back to legacy ErrorDisplay when structuredError is missing', () => {
    render(
      <TextMessageContent
        content="Error calling model 'gpt-5' (Bad Request): 400 Bad Request."
        isStreaming={false}
        hasError={true}
      />,
    );

    // parseErrorMessage maps "Bad Request" → title; headline keys should NOT appear.
    expect(screen.queryByText('chat.errorUpstreamHeadline')).not.toBeInTheDocument();
    expect(screen.getByText('Bad Request')).toBeInTheDocument();
  });

  it('omits structured display when kind is internal (banner handles internal)', () => {
    // Internal errors are routed to the chat-input banner in useChatMessages,
    // so the assistant bubble should NOT render the structured card even if
    // the prop is passed. Only the upstream branch triggers inline structured.
    const structured: StructuredError = {
      message: 'workspace state corrupted',
      kind: 'internal',
    };
    render(
      <TextMessageContent
        content="workspace state corrupted"
        isStreaming={false}
        hasError={true}
        structuredError={structured}
      />,
    );
    expect(screen.queryByText('chat.errorInternalHeadline')).not.toBeInTheDocument();
  });
});
