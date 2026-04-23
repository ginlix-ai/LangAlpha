/**
 * Tests the ChatView error banner rendering logic (type guard for structured vs string errors).
 *
 * We extract the rendering logic into a minimal test component rather than mounting
 * the full ChatView (which has ~30 transitive dependencies). This verifies:
 * - Structured errors render message + link without going through parseErrorMessage
 * - String errors pass through parseErrorMessage as before
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { parseErrorMessage } from '../../utils/parseErrorMessage';
import type { StructuredError } from '@/utils/rateLimitError';

// Fallback i18n strings (kept in sync with chat.* keys in en-US.json) so we
// can assert on headline/hint copy without wiring the full i18next runtime.
const EN = {
  errorUpstreamHeadline: 'The model provider returned an error',
  errorUpstreamHeadlineStatus: 'The model provider returned an error ({{status}})',
  errorInternalHeadline: 'Something went wrong on our end',
  errorHintApiKey: "Check that your API key is correct and hasn't been revoked.",
  errorHintModelAccess: 'Confirm your plan or subscription allows access to this model.',
  errorHintProviderStatus: "Check the provider's status page for ongoing incidents.",
  errorHintTryAnotherModel: 'Try switching to a different model in the model picker.',
};
const HINT_COPY: Record<string, string> = {
  api_key: EN.errorHintApiKey,
  model_access: EN.errorHintModelAccess,
  provider_status: EN.errorHintProviderStatus,
  try_another_model: EN.errorHintTryAnotherModel,
};

/**
 * Minimal reproduction of the ChatView error banner logic.
 * Mirrors the IIFE in ChatView.tsx — structured branch now also renders a
 * kind-based headline and (for upstream) a hint list.
 */
function ErrorBanner({ messageError }: { messageError: string | StructuredError | null }) {
  if (!messageError) return null;

  if (typeof messageError === 'object' && 'message' in messageError) {
    const err = messageError as StructuredError;
    const isUpstream = err.kind === 'upstream';
    const isInternal = err.kind === 'internal';
    const headline = isUpstream
      ? (err.statusCode
          ? EN.errorUpstreamHeadlineStatus.replace('{{status}}', String(err.statusCode))
          : EN.errorUpstreamHeadline)
      : isInternal
        ? EN.errorInternalHeadline
        : null;
    const hasHints = isUpstream && err.hints && err.hints.length > 0;
    return (
      <div data-testid="error-banner" role="alert">
        {headline && <span data-testid="error-headline">{headline}</span>}
        <span>
          {err.message}
          {err.link && (
            <>
              {' '}
              <a
                href={err.link.url}
                target="_blank"
                rel="noopener noreferrer"
              >
                {err.link.label}
              </a>
            </>
          )}
        </span>
        {hasHints && (
          <ul data-testid="error-hints">
            {err.hints!.map((h) => (
              <li key={h}>{HINT_COPY[h] ?? h}</li>
            ))}
          </ul>
        )}
      </div>
    );
  }

  const parsed = parseErrorMessage(messageError as string);
  return (
    <div data-testid="error-banner" role="alert">
      <span>{parsed.detail ? `${parsed.title}: ${parsed.detail}` : parsed.title}</span>
    </div>
  );
}

describe('ChatView error banner type guard', () => {
  it('renders structured error with message and link', () => {
    const error: StructuredError = {
      message: 'Daily credit limit reached (80/100 credits). Resets at midnight UTC.',
      link: { url: 'https://ginlix.ai/account/usage', label: 'View Usage' },
    };
    render(<ErrorBanner messageError={error} />);

    expect(screen.getByText(/Daily credit limit reached/)).toBeInTheDocument();
    const link = screen.getByRole('link', { name: 'View Usage' });
    expect(link).toHaveAttribute('href', 'https://ginlix.ai/account/usage');
    expect(link).toHaveAttribute('target', '_blank');
  });

  it('renders structured error without link', () => {
    const error: StructuredError = {
      message: 'Too many concurrent requests. Please wait a moment.',
    };
    render(<ErrorBanner messageError={error} />);

    expect(screen.getByText(/Too many concurrent requests/)).toBeInTheDocument();
    expect(screen.queryByRole('link')).not.toBeInTheDocument();
  });

  it('renders string error through parseErrorMessage', () => {
    render(<ErrorBanner messageError="Something went wrong on the server" />);
    // parseErrorMessage returns { title: raw, detail: null } for short plain strings
    expect(screen.getByText('Something went wrong on the server')).toBeInTheDocument();
  });

  it('renders string rate-limit error with descriptive passthrough', () => {
    render(<ErrorBanner messageError="Daily credit limit reached (50/50 credits). Resets at midnight UTC." />);
    // parseErrorMessage now passes through descriptive rate-limit messages as title-only
    expect(screen.getByText(/Daily credit limit reached/)).toBeInTheDocument();
    // Should NOT have redundant "Rate limit exceeded:" prefix
    expect(screen.queryByText(/Rate limit exceeded:/)).not.toBeInTheDocument();
  });

  it('renders null for no error', () => {
    const { container } = render(<ErrorBanner messageError={null} />);
    expect(container.firstChild).toBeNull();
  });

  it('renders upstream error with headline (including status) and hint list', () => {
    const error: StructuredError = {
      message: "Error code: 500 - {'error': {'message': 'Internal service error'}}",
      kind: 'upstream',
      statusCode: 500,
      hints: ['api_key', 'model_access', 'provider_status', 'try_another_model'],
    };
    render(<ErrorBanner messageError={error} />);

    expect(screen.getByTestId('error-headline')).toHaveTextContent(
      'The model provider returned an error (500)',
    );
    const hints = screen.getByTestId('error-hints');
    expect(hints).toHaveTextContent(/API key/);
    expect(hints).toHaveTextContent(/plan or subscription/);
    expect(hints).toHaveTextContent(/status page/);
    expect(hints).toHaveTextContent(/different model/);
  });

  it('renders upstream error without status when backend omits it', () => {
    const error: StructuredError = {
      message: 'Connection reset by peer',
      kind: 'upstream',
      hints: ['provider_status'],
    };
    render(<ErrorBanner messageError={error} />);

    expect(screen.getByTestId('error-headline')).toHaveTextContent(
      'The model provider returned an error',
    );
    expect(screen.getByTestId('error-headline')).not.toHaveTextContent(/\(/);
    expect(screen.getByTestId('error-hints')).toBeInTheDocument();
  });

  it('renders internal error with generic headline and no hints', () => {
    const error: StructuredError = {
      message: 'workspace state corrupted',
      kind: 'internal',
    };
    render(<ErrorBanner messageError={error} />);

    expect(screen.getByTestId('error-headline')).toHaveTextContent(
      'Something went wrong on our end',
    );
    expect(screen.queryByTestId('error-hints')).not.toBeInTheDocument();
  });
});
