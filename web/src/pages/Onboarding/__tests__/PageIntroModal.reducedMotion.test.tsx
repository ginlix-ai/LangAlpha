import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import type { PageIntroDef } from '../registry';

// framer-motion's useReducedMotion initializes a module-level matchMedia
// subscription on FIRST use, so the reduced-motion override must be in place
// before the engine (and framer-motion) is imported — hence the separate test
// file and the dynamic import below.
window.matchMedia = ((query: string) => ({
  matches: query.includes('prefers-reduced-motion'),
  media: query,
  onchange: null,
  addListener: vi.fn(),
  removeListener: vi.fn(),
  addEventListener: vi.fn(),
  removeEventListener: vi.fn(),
  dispatchEvent: vi.fn(),
})) as unknown as typeof window.matchMedia;

const { PageIntroModal } = await import('../engine/PageIntroModal');

const intro: PageIntroDef = {
  id: 'chat',
  matchRoute: () => true,
  steps: [
    { id: 's1', titleKey: 'title-1', bodyKey: 'body-1', visual: 'workspaceGrid' },
    { id: 's2', titleKey: 'title-2', bodyKey: 'body-2', visual: 'flashAnswer' },
  ],
};

describe('PageIntroModal under prefers-reduced-motion', () => {
  it('renders and navigates via the instant-crossfade path', async () => {
    render(<PageIntroModal intro={intro} onClose={vi.fn()} />);
    expect(screen.getByText('title-1')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(await screen.findByText('title-2')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Back' }));
    expect(await screen.findByText('title-1')).toBeInTheDocument();
  });
});
