/**
 * The offer of the welcome flow that the dashboard banner, Settings, the empty
 * gallery and the end of setup all make: gone once the profile is saved, words
 * that say whether it resumes this browser's draft, and a way in that can
 * leave the screen behind.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import type { ReactNode } from 'react';
import { MemoryRouter, useLocation, useNavigationType } from 'react-router';
import { WELCOME_PATH } from '../welcome/paths';
import { useWelcomeOffer } from '../welcome/useWelcomeOffer';

const h = vi.hoisted(() => ({
  user: null as Record<string, unknown> | null,
  /** The users this browser holds a draft for; what a draft keeps is the
   *  draft module's own to test. */
  drafts: new Set<string>(),
}));

vi.mock('@/hooks/useUser', () => ({
  useUser: () => ({ user: h.user, isLoading: false, isError: false }),
}));
vi.mock('../welcome/draft', () => ({
  readWelcomeDraft: (userId: string | null | undefined) => (userId && h.drafts.has(userId) ? {} : null),
}));

function renderOffer() {
  const wrapper = ({ children }: { children: ReactNode }) => (
    <MemoryRouter initialEntries={['/dashboard']}>{children}</MemoryRouter>
  );
  return renderHook(() => ({ offer: useWelcomeOffer(), location: useLocation(), action: useNavigationType() }), {
    wrapper,
  });
}

beforeEach(() => {
  h.drafts.clear();
  h.user = { user_id: 'u1', email: 'tester@example.com', onboarding_completed: false };
});

describe('useWelcomeOffer', () => {
  it('makes no offer once the profile is saved', () => {
    h.user = { ...h.user, onboarding_completed: true };
    expect(renderOffer().result.current.offer).toBeNull();
  });

  it('offers to set up when this browser holds no draft', () => {
    expect(renderOffer().result.current.offer?.label).toBe('Set up');
  });

  it("offers to resume when this browser holds the user's draft, and not another user's", () => {
    h.drafts.add('u2');
    expect(renderOffer().result.current.offer?.label).toBe('Set up');

    h.drafts.add('u1');
    expect(renderOffer().result.current.offer?.label).toBe('Resume');
  });

  it('opens the flow on top of the screen it was offered from', () => {
    const { result } = renderOffer();
    act(() => result.current.offer?.open());
    expect(result.current.location.pathname).toBe(WELCOME_PATH);
    expect(result.current.action).toBe('PUSH');
  });

  it('opens the flow in place of the screen when asked to replace it', () => {
    const { result } = renderOffer();
    act(() => result.current.offer?.open({ replace: true }));
    expect(result.current.location.pathname).toBe(WELCOME_PATH);
    expect(result.current.action).toBe('REPLACE');
  });
});
