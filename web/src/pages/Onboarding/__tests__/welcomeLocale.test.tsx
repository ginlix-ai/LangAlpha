/**
 * The language the welcome flow is gone through in. The bar's toggle switches
 * it mid-conversation, and everything already on screen follows: the agent's
 * lines, the answers given, the card being asked, the composer. When the flow
 * ends in a new account, the account is made to hold that language, so the
 * agent writes in it from its first reply; editing from Settings changes
 * nothing of the kind.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ChatInputProps } from '@/components/ui/chat-input';
import i18n from '@/i18n';
import { getLocaleCookie, setLocaleCookie } from '@/lib/locale';
import type { InvestingProfile } from '../welcome/profile';

vi.mock('@/lib/framer', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/framer')>()),
  useReducedMotion: () => true,
}));
// The composer has its own tests; here only the line it shows when empty.
vi.mock('@/components/ui/chat-input', () => ({
  default: ({ placeholder, disabled }: Pick<ChatInputProps, 'placeholder' | 'disabled'>) => (
    <textarea data-testid="composer" aria-label="Message" placeholder={placeholder} disabled={disabled} />
  ),
}));
const mutateAsync = vi.fn(async (_patch: unknown) => ({}));
vi.mock('@/hooks/useUpdatePreferences', () => ({ useUpdatePreferences: () => ({ mutateAsync }) }));
vi.mock('../welcome/useBrokers', () => ({ useBrokers: () => ({ connected: [], rows: [], ready: true }) }));
vi.mock('../welcome/steps/BrokerageStep', () => ({ BrokerageStep: () => <div data-testid="brokerage-step" /> }));
vi.mock('../welcome/steps/ScreenshotDrop', () => ({ ScreenshotDrop: () => null }));
vi.mock('../welcome/steps/HoldingsEntry', () => ({ HoldingsEntry: () => null }));
vi.mock('@/pages/ChatAgent/utils/warmWorkspace', () => ({ warmWorkspace: () => {} }));
vi.mock('@/hooks/useMcpServers', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useMcpServers')>()),
  waitForServerTools: async () => true,
}));
vi.mock('@/hooks/useHomeWorkspace', () => ({
  homeWorkspaceQuery: () => ({ queryKey: ['test-home'], queryFn: async () => ({ workspace_id: 'home-1' }) }),
  resolveChatWorkspace: async () => 'home-1',
}));
vi.mock('../welcome/marketBrief', () => ({
  marketBriefQuery: { queryKey: ['test-brief'], queryFn: async () => null, retry: false, staleTime: Infinity },
}));
vi.mock('@/contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'light' }) }));
vi.mock('@/components/ui/use-toast', () => ({ toast: () => {} }));

const updateCurrentUser = vi.fn(async (_data: Record<string, unknown>): Promise<Record<string, unknown>> => ({}));
vi.mock('@/pages/Dashboard/utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/Dashboard/utils/api')>()),
  updateCurrentUser: (data: Record<string, unknown>) => updateCurrentUser(data),
}));
/** The account as /users/me returns it; null while it is still loading. */
let mockUser: { user_id: string; locale?: string | null } | null = null;
vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: mockUser }) }));

import { WelcomeFlow } from '../welcome/Welcome';
import { welcomeSteps } from '../welcome/paths';
import { emptyProfile } from '../welcome/profile';

const answered: InvestingProfile = {
  ...emptyProfile(),
  investorType: 'individual',
  experience: '3_10y',
  riskLevel: 'growth',
  horizon: 'years',
  markets: ['us'],
  approaches: ['fundamental'],
  styles: ['growth'],
  depth: 'thorough',
  deliverable: 'markdown',
  proactivity: 'when_needed',
};

function LocationProbe() {
  const { pathname } = useLocation();
  return <span data-testid="location">{pathname}</span>;
}

function renderFlow({
  mode = 'new' as 'new' | 'edit',
  initialStep = 'markets' as Parameters<typeof WelcomeFlow>[0]['initialStep'],
  profile = answered,
} = {}) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const onSkip = vi.fn();
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={['/welcome']}>
        <Routes>
          <Route
            path="/welcome"
            element={
              <WelcomeFlow
                mode={mode}
                steps={welcomeSteps(mode)}
                initialStep={initialStep}
                initialProfile={profile}
                savedProfile={profile}
                userId="u1"
                userName="Ada Lovelace"
                onSkip={onSkip}
              />
            }
          />
          <Route path="*" element={null} />
        </Routes>
        <LocationProbe />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  return { onSkip };
}

const zh = i18n.getFixedT('zh-CN');
const en = i18n.getFixedT('en-US');
const location = () => screen.getByTestId('location').textContent;

async function pickLanguage(name: string) {
  const user = userEvent.setup();
  await user.click(screen.getByTestId('language-toggle'));
  await user.click(await screen.findByRole('menuitemradio', { name }));
  await waitFor(() => expect(i18n.language).toBe(name === 'English' ? 'en-US' : 'zh-CN'));
}

function clearLocaleCookie() {
  document.cookie = 'locale=; path=/; expires=Thu, 01 Jan 1970 00:00:00 GMT';
}

beforeEach(async () => {
  mockUser = { user_id: 'u1', locale: null };
  localStorage.clear();
  clearLocaleCookie();
  await act(() => i18n.changeLanguage('en-US'));
});

afterEach(async () => {
  vi.clearAllMocks();
  clearLocaleCookie();
  await act(() => i18n.changeLanguage('en-US'));
});

describe('the welcome flow in another language', () => {
  it('the bar offers the language from the first screen on', () => {
    renderFlow({ initialStep: 'brokerage', profile: emptyProfile() });
    const bar = screen.getByRole('banner');
    expect(within(bar).getByTestId('language-toggle')).toHaveAccessibleName('Language: English');
    // Before the way out, in the bar's right-hand cell.
    const right = within(bar).getByTestId('welcome-exit').parentElement!;
    expect(right.firstElementChild).toBe(within(bar).getByTestId('language-toggle'));
  });

  it('a switch mid-flow rewrites everything already said, the card being asked and the composer', async () => {
    renderFlow();
    expect(screen.getByTestId('welcome-turn-markets')).toHaveTextContent(en('onboarding.welcome.chat.lead.markets'));
    expect(screen.getByTestId('welcome-answer-experience')).toHaveTextContent('Individual investor');

    await pickLanguage('简体中文');

    // The line being asked, and the ones answered above it.
    await waitFor(() =>
      expect(screen.getByTestId('welcome-turn-markets')).toHaveTextContent(zh('onboarding.welcome.chat.lead.markets')),
    );
    expect(screen.getByText(zh('onboarding.welcome.chat.lead.experience'))).toBeInTheDocument();
    expect(screen.getByText(zh('onboarding.welcome.chat.lead.risk'))).toBeInTheDocument();
    expect(
      screen.getByRole('heading', { name: zh('onboarding.welcome.chat.greeting', { name: 'Ada' }) }),
    ).toBeVisible();
    // What the user answered, put back in the new language.
    expect(screen.getByTestId('welcome-answer-brokerage')).toHaveTextContent(
      zh('onboarding.welcome.chat.summary.skipped'),
    );
    expect(screen.getByTestId('welcome-answer-experience')).toHaveTextContent(
      zh('onboarding.welcome.experience.types.individual.title'),
    );
    expect(screen.getByTestId('welcome-answer-risk')).toHaveTextContent(zh('onboarding.welcome.risk.horizons.years'));
    // The card being asked, its way on, the composer and the way out.
    expect(screen.getByRole('checkbox', { name: zh('onboarding.welcome.markets.names.us') })).toBeChecked();
    expect(screen.getByTestId('welcome-continue')).toHaveTextContent(zh('onboarding.welcome.continue'));
    expect(screen.getByTestId('composer')).toHaveAttribute(
      'placeholder',
      zh('onboarding.welcome.chat.composer.waiting'),
    );
    expect(screen.getByTestId('welcome-exit')).toHaveTextContent(zh('onboarding.welcome.skip'));
    expect(screen.queryByText(en('onboarding.welcome.chat.lead.experience'))).not.toBeInTheDocument();

    // And back again.
    await pickLanguage('English');
    await waitFor(() =>
      expect(screen.getByTestId('welcome-turn-markets')).toHaveTextContent(en('onboarding.welcome.chat.lead.markets')),
    );
    expect(screen.getByTestId('welcome-answer-experience')).toHaveTextContent('Individual investor');
  });

  it("Enter inside the open menu is the menu's, and does not move the flow on", async () => {
    const user = userEvent.setup();
    renderFlow();
    await user.click(screen.getByTestId('language-toggle'));
    const menu = await screen.findByRole('menu');
    // Opened by a click, the menu itself holds focus until an arrow moves it.
    expect(menu).toHaveFocus();
    await user.keyboard('{Enter}');
    expect(screen.getByTestId('welcome-turn-markets')).toBeInTheDocument();
    expect(screen.queryByTestId('welcome-turn-style')).not.toBeInTheDocument();
    // Picking with Enter switches the language and still leaves the flow where it was.
    await user.keyboard('{ArrowDown}{ArrowDown}{Enter}');
    await waitFor(() => expect(i18n.language).toBe('zh-CN'));
    expect(screen.getByTestId('welcome-turn-markets')).toBeInTheDocument();
    expect(screen.queryByTestId('welcome-turn-style')).not.toBeInTheDocument();
  });

  it('a switch on the last question rewrites how the agent should work, its picks kept', async () => {
    renderFlow({ initialStep: 'agent' });
    await pickLanguage('简体中文');
    await waitFor(() =>
      expect(screen.getByTestId('welcome-turn-agent')).toHaveTextContent(zh('onboarding.welcome.chat.lead.agent')),
    );
    const picked = (key: string) => screen.getByRole('radio', { name: new RegExp(`^${zh(key)}`) });
    expect(screen.getByRole('heading', { name: zh('onboarding.welcome.agent.deliverableTitle') })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: zh('onboarding.welcome.agent.proactivityTitle') })).toBeInTheDocument();
    expect(picked('onboarding.welcome.agent.depth.thorough.title')).toBeChecked();
    expect(picked('onboarding.welcome.agent.depth.light.title')).not.toBeChecked();
    expect(picked('onboarding.welcome.agent.deliverable.markdown.title')).toBeChecked();
    expect(picked('onboarding.welcome.agent.proactivity.when_needed.title')).toBeChecked();
    // The last question's way on is the start, in the new language too.
    expect(screen.getByTestId('welcome-continue')).toHaveTextContent(zh('onboarding.welcome.start.submit'));
  });

  it('the answers carried on after a switch are the ones given before it', async () => {
    renderFlow({ initialStep: 'markets' });
    await pickLanguage('简体中文');
    fireEvent.click(screen.getByTestId('welcome-continue'));
    expect(screen.getByTestId('welcome-turn-style')).toBeInTheDocument();
    expect(screen.getByTestId('welcome-answer-markets')).toHaveTextContent(zh('onboarding.welcome.markets.names.us'));
  });
});

describe('the language the account keeps when the flow ends', () => {
  it('Skip saves the language the flow was in when the account holds none', async () => {
    const { onSkip } = renderFlow();
    fireEvent.click(screen.getByTestId('welcome-exit'));
    expect(onSkip).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(updateCurrentUser).toHaveBeenCalledWith({ locale: 'en-US' }));
    expect(updateCurrentUser).toHaveBeenCalledTimes(1);
  });

  it('Skip saves it over a different one', async () => {
    mockUser = { user_id: 'u1', locale: 'en-US' };
    setLocaleCookie('zh-CN');
    await act(() => i18n.changeLanguage('zh-CN'));
    renderFlow();
    fireEvent.click(screen.getByTestId('welcome-exit'));
    await waitFor(() => expect(updateCurrentUser).toHaveBeenCalledWith({ locale: 'zh-CN' }));
  });

  it('Skip saves nothing when the account already holds that language', async () => {
    mockUser = { user_id: 'u1', locale: 'en-US' };
    const { onSkip } = renderFlow();
    fireEvent.click(screen.getByTestId('welcome-exit'));
    expect(onSkip).toHaveBeenCalledTimes(1);
    await act(async () => {});
    expect(updateCurrentUser).not.toHaveBeenCalled();
  });

  it('saves nothing while the account has not loaded, since what it holds is not known', async () => {
    mockUser = null;
    renderFlow();
    fireEvent.click(screen.getByTestId('welcome-exit'));
    await act(async () => {});
    expect(updateCurrentUser).not.toHaveBeenCalled();
  });

  it('Start saves it before the chat opens', async () => {
    renderFlow({ initialStep: 'agent' });
    fireEvent.click(screen.getByTestId('welcome-continue'));
    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    expect(updateCurrentUser).toHaveBeenCalledTimes(1);
    expect(updateCurrentUser).toHaveBeenCalledWith({ locale: 'en-US' });
  });

  it('Start saves nothing when the account already holds that language', async () => {
    mockUser = { user_id: 'u1', locale: 'en-US' };
    renderFlow({ initialStep: 'agent' });
    fireEvent.click(screen.getByTestId('welcome-continue'));
    await waitFor(() => expect(location()).toBe('/chat/t/__default__'));
    expect(updateCurrentUser).not.toHaveBeenCalled();
  });

  it('a language picked in the flow is the one kept, not the one it opened in', async () => {
    renderFlow();
    await pickLanguage('简体中文');
    expect(updateCurrentUser).toHaveBeenLastCalledWith({ locale: 'zh-CN' });
    expect(getLocaleCookie()).toBe('zh-CN');
    // The account row has not been read back yet, so it still holds none.
    fireEvent.click(screen.getByTestId('welcome-exit'));
    await waitFor(() => expect(updateCurrentUser).toHaveBeenCalledTimes(2));
    expect(updateCurrentUser.mock.calls.map(([data]) => data)).toEqual([{ locale: 'zh-CN' }, { locale: 'zh-CN' }]);
  });

  it('editing from Settings and cancelling keeps nothing', async () => {
    renderFlow({ mode: 'edit', initialStep: 'experience' });
    fireEvent.click(screen.getByTestId('welcome-exit'));
    await waitFor(() => expect(location()).toBe('/settings'));
    expect(updateCurrentUser).not.toHaveBeenCalled();
  });

  it('editing from Settings and saving keeps nothing either', async () => {
    renderFlow({ mode: 'edit', initialStep: 'agent' });
    fireEvent.click(screen.getByTestId('welcome-continue'));
    await waitFor(() => expect(location()).toBe('/settings'));
    expect(updateCurrentUser).not.toHaveBeenCalled();
  });
});
