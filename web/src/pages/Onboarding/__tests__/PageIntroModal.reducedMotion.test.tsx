import { Suspense } from 'react';
import { describe, it, expect, vi, beforeAll } from 'vitest';
import { act, render, screen, fireEvent } from '@testing-library/react';
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
const { INTRO_VISUALS, loadIntroScenes } = await import('../engine/introScenes');

beforeAll(() => loadIntroScenes(INTRO_VISUALS));

const intro: PageIntroDef = {
  id: 'chat',
  matchRoute: () => true,
  steps: [
    { id: 's1', visual: 'workspaceGrid' },
    { id: 's2', visual: 'chiefOfStaffAnswer' },
  ],
};

describe('PageIntroModal under prefers-reduced-motion', () => {
  it('renders and navigates via the instant-crossfade path', async () => {
    // Held in an awaited act: the modal suspends on its load, then retries.
    await act(async () => {
      render(
        <Suspense fallback={null}>
          <PageIntroModal intro={intro} onClose={vi.fn()} onAbandon={vi.fn()} />
        </Suspense>
      );
    });
    expect(screen.getByText('chat.s1.title')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(await screen.findByText('chat.s2.title')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Back' }));
    expect(await screen.findByText('chat.s1.title')).toBeInTheDocument();
  });
});
