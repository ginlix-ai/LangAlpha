import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, act, waitFor } from '@testing-library/react';
import { MemoryRouter, useLocation, useNavigate } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { emptyOnboardingPrefs } from '../onboardingPrefsSchema';
import type { OnboardingPrefs } from '../types';

let mockPrefs: OnboardingPrefs = emptyOnboardingPrefs();
let mockLoading = false;
const markPageIntroSeen = vi.fn();
const markTaskDone = vi.fn();
const dismissGettingStarted = vi.fn();
// replayGuides/resetAll return whether the persist write was issued (true) or
// refused on a cold cache (false); the provider only clears local state on true.
const replayGuides = vi.fn(() => true);
const setLastSeenReleaseVersion = vi.fn();
const ensureFirstRun = vi.fn();
const markWelcomeSkipped = vi.fn();
const resetAll = vi.fn(() => true);
vi.mock('../useOnboardingPrefs', () => ({
  useOnboardingPrefs: () => ({
    prefs: mockPrefs,
    isLoading: mockLoading,
    markPageIntroSeen,
    markTaskDone,
    dismissGettingStarted,
    replayGuides,
    setLastSeenReleaseVersion,
    ensureFirstRun,
    markWelcomeSkipped,
    resetAll,
  }),
}));

// A returning user with a saved investing profile, unless a test says otherwise.
let mockUser: Record<string, unknown> | null = { user_id: 'u1', onboarding_completed: true };
vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: mockUser }) }));

let mockSnoozed = false;
vi.mock('@/pages/Dashboard/hooks/useOnboarding', () => ({
  isPersonalizationSnoozed: () => mockSnoozed,
  subscribePersonalizationSnooze: () => () => {},
}));

let mockIsMobile = false;
vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => mockIsMobile }));

// Deterministic regardless of the .env loaded by vitest: provider tests run in
// OSS mode (platform-only tasks filtered out).
vi.mock('@/config/hostMode', () => ({ HOST_MODE: 'oss', isPlatformMode: false }));

let mockWorkspaces: unknown[] = [];
const useWorkspacesSpy = vi.fn((opts: { enabled?: boolean } = {}) => ({
  data: opts.enabled === false ? undefined : { workspaces: mockWorkspaces },
}));
vi.mock('@/hooks/useWorkspaces', () => ({
  useWorkspaces: (opts?: { enabled?: boolean }) => useWorkspacesSpy(opts),
}));

// The onboarding chunk, behind which the intros' scenes and copy load: the
// factory runs only if the host gate imports it.
const hostChunkLoaded = vi.hoisted(() => vi.fn());
vi.mock('../OnboardingHost', () => {
  hostChunkLoaded();
  return { OnboardingHost: () => null };
});

import { OnboardingProvider, useOnboarding } from '../OnboardingProvider';
import { OnboardingHostGate } from '../OnboardingHostGate';
import { PAGE_INTROS } from '../registry';

function Harness() {
  const {
    phase,
    activeIntro,
    gettingStarted,
    dismissPageIntro,
    abandonPageIntro,
    acknowledgeWhatsNew,
    replayGuides: replay,
    resetOnboarding,
  } = useOnboarding();
  return (
    <div>
      <span data-testid="phase">{phase}</span>
      <span data-testid="intro">{activeIntro?.id ?? 'none'}</span>
      <span data-testid="introSteps">{activeIntro?.steps.map((s) => s.id).join(',')}</span>
      <span data-testid="card">{gettingStarted.visible ? 'visible' : 'hidden'}</span>
      <span data-testid="done">{gettingStarted.doneCount}</span>
      <span data-testid="taskIds">{gettingStarted.tasks.map((t) => t.def.id).join(',')}</span>
      <button onClick={dismissPageIntro}>dismiss</button>
      <button onClick={abandonPageIntro}>abandon</button>
      <button onClick={acknowledgeWhatsNew}>ack</button>
      <button onClick={replay}>replay</button>
      <button onClick={resetOnboarding}>reset</button>
      <NavButtons />
      <LocationProbe />
    </div>
  );
}

function NavButtons() {
  const navigate = useNavigate();
  const { pathname } = useLocation();
  return (
    <>
      <button onClick={() => navigate('/chat/t/__default__')}>go-new-chat</button>
      {/* What the chat does once it has read its route state. */}
      <button onClick={() => navigate(pathname, { replace: true, state: {} })}>clear-state</button>
      <button onClick={() => navigate('/chat/t/resolved-guid', { replace: true })}>
        go-resolved
      </button>
      <button onClick={() => navigate('/chat')}>go-gallery</button>
      <button onClick={() => navigate('/chat/t/another-thread')}>go-thread</button>
      <button onClick={() => navigate('/dashboard')}>go-dashboard</button>
    </>
  );
}

function LocationProbe() {
  const { pathname, search } = useLocation();
  return <span data-testid="location">{pathname + search}</span>;
}

/** The welcome flow's hand-off into the chat it opens. */
const WELCOME_HANDOFF = { pathname: '/chat/t/__default__', state: { handoff: true } };

function renderAt(route: string | { pathname: string; state: unknown }, { withHost = false } = {}) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[route]}>
        <OnboardingProvider>
          <Harness />
          {withHost && <OnboardingHostGate />}
        </OnboardingProvider>
      </MemoryRouter>
    </QueryClientProvider>
  );
}

const phase = () => screen.getByTestId('phase').textContent;
const intro = () => screen.getByTestId('intro').textContent;
const card = () => screen.getByTestId('card').textContent;
const location = () => screen.getByTestId('location').textContent;

describe('OnboardingProvider', () => {
  beforeEach(() => {
    mockPrefs = emptyOnboardingPrefs();
    mockLoading = false;
    mockUser = { user_id: 'u1', onboarding_completed: true };
    mockWorkspaces = [];
    mockSnoozed = false;
    mockIsMobile = false;
    vi.clearAllMocks();
    localStorage.clear();
    sessionStorage.clear();
  });

  it('opens the chat intro for a fresh user on /chat and stamps first-run once', () => {
    renderAt('/chat');
    expect(phase()).toBe('pageIntro');
    expect(intro()).toBe('chat');
    expect(ensureFirstRun).toHaveBeenCalledTimes(1);
  });

  it('opens the thread intro on a real thread — pages get their own intro', () => {
    renderAt('/chat/t/abc123');
    expect(intro()).toBe('thread');
  });

  it('shows the dashboard intro even after the chat intro was seen (scattered, per page)', () => {
    mockPrefs = { ...emptyOnboardingPrefs(), pageIntrosSeen: { chat: 1, thread: 2 } };
    renderAt('/dashboard');
    expect(phase()).toBe('pageIntro');
    expect(intro()).toBe('dashboard');
  });

  it('drops platform-only intro steps from a self-hosted build', () => {
    renderAt('/automations');
    expect(intro()).toBe('automations');
    // Delivery goes to chat apps, which link through the hosted platform.
    expect(screen.getByTestId('introSteps').textContent).toBe('trigger,ask,feed');
  });

  it('drops the Chief of Staff step, which answers from iMessage, from a self-hosted build', () => {
    renderAt('/chat');
    expect(intro()).toBe('chat');
    expect(screen.getByTestId('introSteps').textContent).toBe('modes,ptc,workspace,create');
  });

  it('shows nothing on pages with no matching intro', () => {
    renderAt('/settings');
    expect(phase()).toBe('idle');
  });

  it('dismissPageIntro stamps that intro seen and does not re-open this session', () => {
    renderAt('/chat');
    fireEvent.click(screen.getByText('dismiss'));
    expect(markPageIntroSeen).toHaveBeenCalledWith('chat');
    // prefs are still "unseen" (write is async + non-optimistic) — the session
    // guard alone must prevent the orchestrator from re-opening it.
    expect(phase()).toBe('idle');
  });

  it('does not open an intro the localStorage mirror marks seen', () => {
    localStorage.setItem(
      'langalpha-onboarding-v1:u1',
      JSON.stringify({ pageIntrosSeen: { chat: 5 }, lastSeenReleaseVersion: null, firstRunAt: 1 })
    );
    renderAt('/chat');
    expect(phase()).toBe('idle');
  });

  it('suppresses intros on the dashboard while the welcome banner owns it', () => {
    mockUser = { user_id: 'u1' }; // no investing profile yet
    renderAt('/dashboard');
    expect(phase()).toBe('idle');
  });

  it('a key of their own does not stand in for a saved profile', () => {
    // `personalization_completed` says the user has a key, so the banner (and
    // the hold on the dashboard intro) stays until the profile is saved.
    mockUser = { user_id: 'u1', personalization_completed: true };
    renderAt('/dashboard');
    expect(phase()).toBe('idle');
  });

  it("never pops an intro or What's New over the welcome flow", () => {
    mockPrefs = { ...emptyOnboardingPrefs(), firstRunAt: 1, lastSeenReleaseVersion: '2026.1' };
    renderAt('/welcome');
    expect(phase()).toBe('idle');
  });

  it('sends the old connect-sheet link to the welcome flow, keeping the rest of its query', () => {
    renderAt('/chat?onboarding=connect&mcp_oauth=done');
    expect(location()).toBe('/welcome?mcp_oauth=done&step=brokerage');
    expect(phase()).toBe('idle');
  });

  it('shows the dashboard intro once the banner is snoozed', () => {
    mockUser = { user_id: 'u1' };
    mockSnoozed = true;
    renderAt('/dashboard');
    expect(intro()).toBe('dashboard');
  });

  it('never pops an intro over the personalization chat', () => {
    mockUser = { user_id: 'u1' };
    mockSnoozed = true;
    renderAt('/chat/t/__default__');
    expect(phase()).toBe('idle');
  });

  it('keeps suppressing after __default__ resolves to a real thread id mid-interview', () => {
    mockPrefs = { ...emptyOnboardingPrefs(), pageIntrosSeen: { chat: 1 } };
    renderAt('/chat/t/__default__');
    expect(phase()).toBe('idle');

    // ChatView renames the URL on the first message and drops the
    // personalization route state — the thread intro must not pop here.
    fireEvent.click(screen.getByText('go-resolved'));
    expect(phase()).toBe('idle');

    // Leaving the thread view ends the interview; the intro is still unseen
    // and fires on the next real thread.
    fireEvent.click(screen.getByText('go-gallery'));
    fireEvent.click(screen.getByText('go-thread'));
    expect(intro()).toBe('thread');
  });

  it('shows the thread intro on a different thread opened right after the interview', () => {
    mockPrefs = { ...emptyOnboardingPrefs(), pageIntrosSeen: { chat: 1 } };
    renderAt('/chat/t/__default__');
    fireEvent.click(screen.getByText('go-resolved')); // interview renamed → suppressed
    expect(phase()).toBe('idle');
    // Switching directly to ANOTHER thread (never leaving the thread view)
    // must show the intro — only the interview's own thread stays suppressed.
    fireEvent.click(screen.getByText('go-thread'));
    expect(intro()).toBe('thread');
  });

  it('keeps the getting-started list out of the welcome flow conversation', () => {
    mockPrefs = { ...emptyOnboardingPrefs(), pageIntrosSeen: { chat: 1 } };
    renderAt(WELCOME_HANDOFF);
    expect(card()).toBe('hidden');
    // The chat clears its route state as it sends, and the rename on the first
    // message drops it too: the hand-off is carried across both.
    fireEvent.click(screen.getByText('clear-state'));
    expect(card()).toBe('hidden');
    fireEvent.click(screen.getByText('go-resolved'));
    expect(card()).toBe('hidden');
    // Back on any other page, the list returns.
    fireEvent.click(screen.getByText('go-dashboard'));
    expect(card()).toBe('visible');
  });

  it('keeps the getting-started list on every other new chat', () => {
    mockPrefs = { ...emptyOnboardingPrefs(), pageIntrosSeen: { chat: 1 } };
    renderAt('/chat/t/__default__');
    expect(card()).toBe('visible');
    fireEvent.click(screen.getByText('go-resolved'));
    expect(card()).toBe('visible');
    // Its first turn still holds the intros off.
    expect(phase()).toBe('idle');
  });

  it('shows the getting-started list on a new chat started after the welcome flow', () => {
    mockPrefs = { ...emptyOnboardingPrefs(), pageIntrosSeen: { chat: 1 } };
    renderAt(WELCOME_HANDOFF);
    fireEvent.click(screen.getByText('go-resolved'));
    expect(card()).toBe('hidden');
    fireEvent.click(screen.getByText('go-new-chat'));
    expect(card()).toBe('visible');
    fireEvent.click(screen.getByText('go-thread')); // the new chat's rename
    expect(card()).toBe('visible');
    expect(phase()).toBe('idle');
  });

  it('still knows the welcome flow conversation after a reload', () => {
    mockPrefs = { ...emptyOnboardingPrefs(), pageIntrosSeen: { chat: 1 } };
    const first = renderAt(WELCOME_HANDOFF);
    fireEvent.click(screen.getByText('go-resolved'));
    first.unmount();

    // A fresh provider on the renamed thread, as a reload mounts it.
    renderAt('/chat/t/resolved-guid');
    expect(phase()).toBe('idle');
    expect(card()).toBe('hidden');
  });

  it("still knows another new chat's first turn after a reload, and shows the list there", () => {
    mockPrefs = { ...emptyOnboardingPrefs(), pageIntrosSeen: { chat: 1 } };
    const first = renderAt('/chat/t/__default__');
    fireEvent.click(screen.getByText('go-resolved'));
    first.unmount();

    renderAt('/chat/t/resolved-guid');
    expect(phase()).toBe('idle');
    expect(card()).toBe('visible');
  });

  it('an open intro closes unseen when navigation leaves its page, and re-offers later', () => {
    renderAt('/chat');
    expect(intro()).toBe('chat');
    // Browser back / redirect mid-intro: the chat intro must not float over
    // the thread page. It closes WITHOUT being stamped seen.
    fireEvent.click(screen.getByText('go-thread'));
    expect(markPageIntroSeen).not.toHaveBeenCalled();
    expect(intro()).toBe('thread');
    // Returning to /chat re-offers the unfinished chat intro.
    fireEvent.click(screen.getByText('go-gallery'));
    expect(intro()).toBe('chat');
  });

  it('abandonPageIntro closes the intro unseen, for this session only', () => {
    const first = renderAt('/chat');
    expect(intro()).toBe('chat');
    fireEvent.click(screen.getByText('abandon'));
    expect(markPageIntroSeen).not.toHaveBeenCalled();
    expect(phase()).toBe('idle');
    // A later render does not reopen it.
    fireEvent.click(screen.getByText('go-gallery'));
    expect(intro()).toBe('none');
    first.unmount();

    // A reload offers it again.
    renderAt('/chat');
    expect(intro()).toBe('chat');
  });

  it("abandonPageIntro hands the screen to What's-New", () => {
    mockPrefs = { ...emptyOnboardingPrefs(), firstRunAt: 1, lastSeenReleaseVersion: '2026.1' };
    renderAt('/chat');
    expect(intro()).toBe('chat');
    fireEvent.click(screen.getByText('abandon'));
    expect(phase()).toBe('whatsNew');
  });

  it("resets a stranded What's-New phase when another tab acknowledges it", () => {
    mockPrefs = { ...emptyOnboardingPrefs(), firstRunAt: 1, lastSeenReleaseVersion: '2026.1' };
    renderAt('/settings');
    expect(phase()).toBe('whatsNew');
    // Cross-tab acknowledge lands: prefs refresh, unseen recomputes to [] while
    // this tab's (now-invisible) modal is still "open".
    mockPrefs = {
      ...mockPrefs,
      lastSeenReleaseVersion: '9999.1',
      pageIntrosSeen: { chat: 1, thread: 1, dashboard: 1 },
    };
    fireEvent.click(screen.getByText('go-gallery')); // any re-render vehicle
    expect(phase()).toBe('idle');
  });

  it('never opens a page intro on mobile — and never marks it seen', () => {
    mockIsMobile = true;
    renderAt('/chat');
    expect(phase()).toBe('idle');
    expect(markPageIntroSeen).not.toHaveBeenCalled();
  });

  it("shows What's-New when no intro matches and acknowledge stamps the latest version", () => {
    mockPrefs = {
      ...emptyOnboardingPrefs(),
      firstRunAt: 1,
      lastSeenReleaseVersion: '2026.1', // older than the registry's announcement
    };
    renderAt('/settings');
    expect(phase()).toBe('whatsNew');
    fireEvent.click(screen.getByText('ack'));
    expect(setLastSeenReleaseVersion).toHaveBeenCalledTimes(1);
    expect(setLastSeenReleaseVersion.mock.calls[0][0]).toMatch(/^\d{4}\.\d+/);
    expect(phase()).toBe('idle');
  });

  it('replayGuides clears state so tips re-show without a reload', () => {
    // Returning user with a stale mirror — the replay must clear BOTH the
    // session guard and the mount-time mirror snapshot, or it would suppress
    // the very tips it just cleared until reload.
    localStorage.setItem(
      'langalpha-onboarding-v1:u1',
      JSON.stringify({ pageIntrosSeen: { chat: 5 }, lastSeenReleaseVersion: null, firstRunAt: 1 })
    );
    mockPrefs = { ...emptyOnboardingPrefs(), pageIntrosSeen: { chat: 5 }, firstRunAt: 1 };
    const { rerender } = renderAt('/chat');
    expect(phase()).toBe('idle');

    fireEvent.click(screen.getByText('replay'));
    expect(replayGuides).toHaveBeenCalledTimes(1);

    // Simulate the server write landing: seen map empties, provider re-renders.
    mockPrefs = { ...emptyOnboardingPrefs(), firstRunAt: 1 };
    rerender(
      <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
        <MemoryRouter initialEntries={['/chat']}>
          <OnboardingProvider>
            <Harness />
          </OnboardingProvider>
        </MemoryRouter>
      </QueryClientProvider>
    );
    expect(intro()).toBe('chat');
  });

  it('replayGuides leaves tips suppressed when the persist write is refused', () => {
    // Cold-cache refusal: the prefs write returns false, so the provider must
    // NOT clear local state — otherwise tips would re-show this session while
    // the server kept them seen, then snap back on the next login (and Settings
    // would have toasted "done" for a change that never persisted).
    replayGuides.mockReturnValueOnce(false);
    localStorage.setItem(
      'langalpha-onboarding-v1:u1',
      JSON.stringify({ pageIntrosSeen: { chat: 5 }, lastSeenReleaseVersion: null, firstRunAt: 1 })
    );
    mockPrefs = { ...emptyOnboardingPrefs(), pageIntrosSeen: { chat: 5 }, firstRunAt: 1 };
    const { rerender } = renderAt('/chat');
    expect(phase()).toBe('idle');

    fireEvent.click(screen.getByText('replay'));
    expect(replayGuides).toHaveBeenCalledTimes(1);

    // Even with server prefs cleared, the un-cleared mirror still suppresses:
    // the refusal left local state intact, so the intro must NOT re-show.
    mockPrefs = { ...emptyOnboardingPrefs(), firstRunAt: 1 };
    rerender(
      <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
        <MemoryRouter initialEntries={['/chat']}>
          <OnboardingProvider>
            <Harness />
          </OnboardingProvider>
        </MemoryRouter>
      </QueryClientProvider>
    );
    expect(intro()).toBe('none');
  });

  it('resetOnboarding clears everything via resetAll', () => {
    renderAt('/settings');
    fireEvent.click(screen.getByText('reset'));
    expect(resetAll).toHaveBeenCalledTimes(1);
    expect(phase()).toBe('idle');
  });

  it('getting-started: visible for a fresh user, platform-only and in-app tasks filtered out in an OSS browser', () => {
    mockUser = { user_id: 'u1' };
    renderAt('/settings');
    expect(card()).toBe('visible');
    expect(screen.getByTestId('done').textContent).toBe('0');
    expect(screen.getByTestId('taskIds').textContent).toBe(
      'dashboard,market,profile,createWorkspace,firstChat,models,desktopApp'
    );
  });

  it('getting-started: the BYOK personalization flag does NOT complete any task', () => {
    mockUser = { user_id: 'u1', personalization_completed: true, onboarding_completed: false };
    renderAt('/settings');
    expect(screen.getByTestId('done').textContent).toBe('0');
  });

  it('getting-started: the profile task reads the user row live, so a reset un-checks it', () => {
    const { unmount } = renderAt('/settings');
    expect(screen.getByTestId('done').textContent).toBe('1');
    expect(markTaskDone).not.toHaveBeenCalledWith('profile');
    unmount();

    mockUser = { user_id: 'u1', onboarding_completed: false };
    renderAt('/settings');
    expect(screen.getByTestId('done').textContent).toBe('0');
  });

  it('getting-started: createWorkspace stamps when a workspace other than Home exists', () => {
    mockWorkspaces = [{ workspace_id: 'ws1' }];
    renderAt('/settings');
    expect(markTaskDone).toHaveBeenCalledWith('createWorkspace');
  });

  it('getting-started: workspace lookup is disabled once stamped', () => {
    mockPrefs = { ...emptyOnboardingPrefs(), gettingStartedDoneAt: { createWorkspace: 1 } };
    renderAt('/settings');
    expect(useWorkspacesSpy).toHaveBeenCalledWith(expect.objectContaining({ enabled: false }));
  });

  it('getting-started: stamps route-visit tasks when their page is visited', () => {
    renderAt('/dashboard');
    expect(markTaskDone).toHaveBeenCalledWith('dashboard');
    expect(markTaskDone).not.toHaveBeenCalledWith('market');
  });

  it('getting-started: model configuration stamps on the settings model tab only', () => {
    const { unmount } = renderAt('/settings');
    expect(markTaskDone).not.toHaveBeenCalledWith('models');
    unmount();

    renderAt('/settings?tab=model');
    expect(markTaskDone).toHaveBeenCalledWith('models');
  });

  it('getting-started: hidden once dismissed, and hidden when every task is done', () => {
    mockPrefs = { ...emptyOnboardingPrefs(), gettingStartedDismissedAt: 1 };
    const { unmount } = renderAt('/settings');
    expect(card()).toBe('hidden');
    unmount();

    mockPrefs = {
      ...emptyOnboardingPrefs(),
      gettingStartedDoneAt: {
        firstChat: 2,
        dashboard: 3,
        market: 4,
        models: 5,
        createWorkspace: 6,
        desktopApp: 7,
      },
    };
    renderAt('/settings'); // profile done from the user row → all 7 complete
    expect(card()).toBe('hidden');
  });

  describe('welcome auto-open', () => {
    const fresh = (userId = 'u1') => ({
      user_id: userId,
      onboarding_completed: false,
      created_at: new Date(Date.now() - 60_000).toISOString(),
    });

    it("opens the flow over a new account's first dashboard visit, once", () => {
      mockUser = fresh();
      renderAt('/dashboard');
      expect(location()).toBe('/welcome');
      // Leaving the flow for the dashboard (Skip, before the stamp lands) does
      // not reopen it this session.
      fireEvent.click(screen.getByText('go-dashboard'));
      expect(location()).toBe('/dashboard');
    });

    it('leaves a deep link where it landed', () => {
      mockUser = fresh();
      renderAt('/chat');
      expect(location()).toBe('/chat');
    });

    it('leaves a returning visit, a skipped flow and an old account on the dashboard', () => {
      mockUser = fresh();
      mockPrefs = { ...emptyOnboardingPrefs(), firstRunAt: 1 };
      const { unmount } = renderAt('/dashboard');
      expect(location()).toBe('/dashboard');
      unmount();

      mockPrefs = { ...emptyOnboardingPrefs(), welcomeSkippedAt: 1 };
      const second = renderAt('/dashboard');
      expect(location()).toBe('/dashboard');
      second.unmount();

      mockPrefs = emptyOnboardingPrefs();
      mockUser = { ...fresh(), created_at: '2025-01-01T00:00:00Z' };
      renderAt('/dashboard');
      expect(location()).toBe('/dashboard');
    });

    describe('after a switch to another account in place', () => {
      // Signing in to another account without signing out clears the cache
      // in place (AuthContext), so the provider outlives the first account.
      afterEach(() => {
        ensureFirstRun.mockReset();
      });

      it('opens the flow for a new account that follows a returning one', () => {
        mockUser = fresh('a');
        mockPrefs = { ...emptyOnboardingPrefs(), firstRunAt: 1 };
        renderAt('/dashboard');
        expect(location()).toBe('/dashboard');

        mockUser = fresh('b');
        mockPrefs = emptyOnboardingPrefs();
        fireEvent.click(screen.getByText('go-dashboard'));
        expect(location()).toBe('/welcome');
      });

      it('opens the flow for a new account after the last one visited it', () => {
        mockUser = fresh('a');
        renderAt('/welcome');

        mockUser = fresh('b');
        fireEvent.click(screen.getByText('go-dashboard'));
        expect(location()).toBe('/welcome');
      });

      it('reads the first visit from the new account when its prefs land before its user row', () => {
        // Sending the stamp moves the cached prefs at once, as the optimistic
        // preferences write does.
        ensureFirstRun.mockImplementation(() => {
          mockPrefs = { ...mockPrefs, firstRunAt: Date.now() };
          return true;
        });
        mockUser = fresh('a');
        mockPrefs = { ...emptyOnboardingPrefs(), firstRunAt: 1 };
        renderAt('/dashboard');

        // The cleared cache: no user row yet, prefs loading.
        mockUser = null;
        mockLoading = true;
        mockPrefs = emptyOnboardingPrefs();
        fireEvent.click(screen.getByText('go-dashboard'));
        // The sign-in sync seeds the new account's prefs before its user row loads.
        mockLoading = false;
        fireEvent.click(screen.getByText('go-dashboard'));

        mockUser = fresh('b');
        fireEvent.click(screen.getByText('go-dashboard'));
        expect(location()).toBe('/welcome');
      });
    });
  });

  it('shows nothing while prefs are loading', () => {
    mockLoading = true;
    renderAt('/chat');
    expect(phase()).toBe('idle');
    expect(card()).toBe('hidden');
  });

  it('downloads no intro code for a user who has seen every intro, on any page', async () => {
    mockPrefs = {
      ...emptyOnboardingPrefs(),
      firstRunAt: 1,
      lastSeenReleaseVersion: '9999.1',
      pageIntrosSeen: Object.fromEntries(PAGE_INTROS.map((i) => [i.id, 1])),
    };
    const routes = ['/chat', '/chat/t/a-thread', '/dashboard', '/market', '/automations'];
    // One route per intro, so an intro added later is covered or fails here.
    expect(PAGE_INTROS.filter((i) => !routes.some((r) => i.matchRoute(r)))).toEqual([]);
    for (const route of routes) {
      const { unmount } = renderAt(route, { withHost: true });
      await act(async () => {});
      expect(phase()).toBe('idle');
      unmount();
    }
    expect(hostChunkLoaded).not.toHaveBeenCalled();

    // The same page with its intro unseen does load it, so the spy can tell.
    mockPrefs = { ...mockPrefs, pageIntrosSeen: {} };
    renderAt('/market', { withHost: true });
    await waitFor(() => expect(hostChunkLoaded).toHaveBeenCalled());
  });
});
