/**
 * The card a failed turn shows for its failure. A failure the server
 * classified takes its headline from the kind; only an unclassified one is
 * read by ``parseErrorMessage``, whose keyword titles would otherwise call a
 * service fault a rate limit.
 *
 * Mocks ``react-i18next`` with an identity ``t`` (same pattern as
 * FileErrorDisplay.test.tsx) so headline / hint lookups render the i18n key
 * back — we only need to confirm the right key path was taken.
 */
import { describe, it, expect, vi } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import React from 'react';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string, opts?: Record<string, unknown>) => (opts?.status ? `${key}:${opts.status}` : key) }),
}));

vi.mock('../Markdown', () => ({ default: () => null }));
vi.mock('@/components/ui/animated-text', () => ({
  useAnimatedText: (text: string) => text,
}));

import { TurnErrorCard } from '../TextMessageContent';
import type { StructuredError } from '@/utils/rateLimitError';

describe('TurnErrorCard routing', () => {
  it('renders the structured card when the failure is upstream', () => {
    const structured: StructuredError = {
      message: "Error code: 401 - login fail",
      kind: 'upstream',
      statusCode: 401,
      hints: ['api_key', 'model_access', 'provider_status', 'try_another_model'],
    };
    renderWithProviders(<TurnErrorCard err={structured} />);

    // Headline comes from the status-aware key.
    expect(screen.getByText('chat.errorUpstreamHeadlineStatus:401')).toBeInTheDocument();
    expect(screen.getByText('Error code: 401 - login fail')).toBeInTheDocument();
    // Each hint renders its i18n key (identity mock).
    expect(screen.getByText('chat.errorHintApiKey')).toBeInTheDocument();
    expect(screen.getByText('chat.errorHintModelAccess')).toBeInTheDocument();
    expect(screen.getByText('chat.errorHintProviderStatus')).toBeInTheDocument();
    expect(screen.getByText('chat.errorHintTryAnotherModel')).toBeInTheDocument();
  });

  it('parses an unclassified failure as before', () => {
    renderWithProviders(<TurnErrorCard err={{ message: "Error calling model 'gpt-5' (Bad Request): 400 Bad Request." }} />);

    // parseErrorMessage maps "Bad Request" → title; headline keys should NOT appear.
    expect(screen.queryByText('chat.errorUpstreamHeadline')).not.toBeInTheDocument();
    expect(screen.getByText('Bad Request')).toBeInTheDocument();
  });

  it('titles an internal failure by its kind, never by keywords in its text', () => {
    renderWithProviders(<TurnErrorCard err={{ message: '', kind: 'internal' }} />);
    expect(screen.getByText('chat.errorInternalHeadline')).toBeInTheDocument();
    expect(screen.queryByText('Rate limit exceeded')).not.toBeInTheDocument();
  });

  it('renders a notice as the quiet status line, not an error card', () => {
    const notice = 'Another reply was still running in this chat, so this message wasn\'t sent. Retry once it finishes.';
    renderWithProviders(<TurnErrorCard err={{ message: notice, kind: 'notice' }} />);
    expect(screen.getByRole('status')).toHaveTextContent(notice);
    // parseErrorMessage's headline would title it as a failure.
    expect(screen.queryByText('Error')).not.toBeInTheDocument();
  });
});
