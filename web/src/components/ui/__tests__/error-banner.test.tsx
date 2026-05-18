import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom';
import { ErrorBanner } from '../error-banner';
import type { StructuredError } from '@/utils/rateLimitError';

function LocationProbe({ onPath }: { onPath: (path: string) => void }) {
  const loc = useLocation();
  onPath(loc.pathname);
  return null;
}

function renderInRouter(ui: React.ReactNode, onPath?: (p: string) => void) {
  return render(
    <MemoryRouter initialEntries={['/start']}>
      <Routes>
        <Route path="*" element={
          <>
            {ui}
            {onPath && <LocationProbe onPath={onPath} />}
          </>
        } />
      </Routes>
    </MemoryRouter>
  );
}

describe('ErrorBanner', () => {
  it('returns nothing when error is null', () => {
    const { container } = renderInRouter(<ErrorBanner error={null} />);
    expect(container).toBeEmptyDOMElement();
  });

  it('returns nothing when error is undefined', () => {
    const { container } = renderInRouter(<ErrorBanner error={undefined} />);
    expect(container).toBeEmptyDOMElement();
  });

  it('renders a plain string error', () => {
    renderInRouter(<ErrorBanner error="Something went wrong" />);
    expect(screen.getByText(/Something went wrong/)).toBeInTheDocument();
  });

  it('renders a StructuredError message', () => {
    const err: StructuredError = {
      message: 'Daily credit limit reached',
    };
    renderInRouter(<ErrorBanner error={err} />);
    expect(screen.getByText('Daily credit limit reached')).toBeInTheDocument();
  });

  it('renders an internal link that navigates via react-router', () => {
    const err: StructuredError = {
      message: 'You hit the credit limit.',
      link: { url: '/settings/billing', label: 'View Usage' },
    };
    let currentPath = '';
    renderInRouter(<ErrorBanner error={err} />, (p) => { currentPath = p; });
    const link = screen.getByText('View Usage');
    expect(link).toBeInTheDocument();
    expect(link.tagName).toBe('A');
    expect(link).not.toHaveAttribute('target');
    fireEvent.click(link);
    expect(currentPath).toBe('/settings/billing');
  });

  it('renders an external link with target=_blank', () => {
    const err: StructuredError = {
      message: 'Upstream provider failed.',
      link: { url: 'https://status.anthropic.com', label: 'Status' },
    };
    renderInRouter(<ErrorBanner error={err} />);
    const link = screen.getByText('Status');
    expect(link).toHaveAttribute('target', '_blank');
    expect(link).toHaveAttribute('rel', 'noopener noreferrer');
  });

  it('renders upstream hints as a bulleted list', () => {
    const err: StructuredError = {
      message: 'Upstream provider error',
      kind: 'upstream',
      statusCode: 503,
      hints: ['api_key', 'provider_status'],
    };
    renderInRouter(<ErrorBanner error={err} />);
    // Two list items rendered for two hints
    const items = screen.getAllByRole('listitem');
    expect(items).toHaveLength(2);
  });

  it('renders an internal-error headline when kind is internal', () => {
    const consoleErr = vi.spyOn(console, 'error').mockImplementation(() => {});
    const err: StructuredError = {
      message: 'Database unavailable',
      kind: 'internal',
    };
    renderInRouter(<ErrorBanner error={err} />);
    expect(screen.getByText('Database unavailable')).toBeInTheDocument();
    consoleErr.mockRestore();
  });
});
