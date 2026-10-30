/* eslint-disable react-refresh/only-export-components -- a test harness, which nothing hot-reloads */
/**
 * What the welcome flow's tests share: the stand-ins for what the flow reaches
 * (the composer, the brokers, the network), the answers a user gives, and how
 * a test mounts the flow and reads where it went.
 *
 * Each test file declares its own `vi.mock` calls, which delegate here, since a
 * mock is hoisted only in the file that declares it. This module imports none
 * of the modules those calls replace, nor the flow itself, which reaches them:
 * a test hands the flow to the renderers below, so a mock's factory can import
 * this module while the flow is still loading.
 */
import { forwardRef, useImperativeHandle, useState, type ComponentType } from 'react';
import { act, render, screen, within, fireEvent } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { vi } from 'vitest';
import type { ChatInputHandle, ChatInputProps } from '@/components/ui/chat-input';
import type { ModelOptions, ReadyAttachment } from '@/components/ui/chat-input.types';
import { queryKeys } from '@/lib/queryKeys';
import type { WelcomeFlow } from '../welcome/Welcome';
import { welcomeSteps } from '../welcome/paths';
import { emptyProfile, type InvestingProfile } from '../welcome/profile';

// --- What the stand-ins hand the flow, set per test ---

interface FakeBroker {
  label: string;
  b: { name: string };
  row: { tool_count?: number } | null;
}

export const mocks = {
  /** The brokerages connected. */
  connected: [] as FakeBroker[],
  /** The cards the brokers' screen offers, which the elsewhere screen reads to
   *  point a picked brokerage back at its card. */
  rows: [] as { b: { name: string }; label: string; connected: boolean }[],
  /** Today's brief, read from the network as the flow opens; here it is what
   *  each test sets, read at once and kept for the visit. */
  brief: null as string | null,
  /** Whether the broker lists are in, and whether one failed. */
  brokersReady: true,
  brokersError: null as Error | null,
};

/** An image picked in the composer. */
export const CHART: ReadyAttachment = {
  file: new File(['abcd'], 'chart.png', { type: 'image/png' }),
  type: 'image/png',
  dataUrl: 'data:image/png;base64,AAAA',
  preview: null,
};

/** What the composer sends beside the text, and the files handed back to it. */
export const composerSend: { attachments: ReadyAttachment[]; options: ModelOptions; givenBack: ReadyAttachment[] } = {
  attachments: [],
  options: { model: null, reasoningEffort: null, fastMode: false },
  givenBack: [],
};

export const mutateAsync = vi.fn(async (_patch: unknown) => ({}));
export const warmWorkspace = vi.fn();
export const waitForServerTools = vi.fn(async (..._a: unknown[]) => true);
export const resolveChatWorkspace = vi.fn(async (..._a: unknown[]): Promise<string> => 'home-1');
export const readBrief = vi.fn(async () => mocks.brief);
export const toast = vi.fn();
/** What the route reads before it opens the flow. */
export const getPreferences = vi.fn(async (): Promise<unknown> => null);

/** Every stand-in as a test starts: nothing connected, no brief, the composer
 *  empty, every save and lookup answering at once. */
export function resetWelcomeMocks() {
  mocks.connected = [];
  mocks.rows = [];
  mocks.brokersReady = true;
  mocks.brokersError = null;
  mocks.brief = null;
  readBrief.mockClear();
  mutateAsync.mockReset();
  mutateAsync.mockResolvedValue({});
  waitForServerTools.mockReset();
  waitForServerTools.mockResolvedValue(true);
  resolveChatWorkspace.mockReset();
  resolveChatWorkspace.mockResolvedValue('home-1');
  getPreferences.mockReset();
  composerSend.attachments = [];
  composerSend.options = { model: null, reasoningEffort: null, fastMode: false };
  composerSend.givenBack = [];
  warmWorkspace.mockClear();
  toast.mockClear();
  localStorage.clear();
}

// --- The mocks' factories ---

/** A turn arriving types its line in and holds its card back until the line
 *  is written. With reduced motion both are there at once, which is how the
 *  flow is driven here. */
export async function framerMock(importOriginal: () => Promise<typeof import('@/lib/framer')>) {
  return { ...(await importOriginal()), useReducedMotion: () => true };
}

/** The composer has its own tests; the flow needs what it sends and the text
 *  it can be handed back. */
export function composerMock() {
  const StubComposer = forwardRef<
    Pick<ChatInputHandle, 'giveBack' | 'setValue' | 'send' | 'getModelOptions'>,
    Pick<ChatInputProps, 'onSend' | 'disabled' | 'placeholder' | 'inHome'>
  >(function StubComposer({ onSend, disabled, placeholder, inHome }, ref) {
    const [value, setValue] = useState('');
    // As the real one does, the box empties on a send, and sends nothing
    // while it is shut or holds nothing.
    const send = () => {
      if (disabled || (!value.trim() && composerSend.attachments.length === 0)) return false;
      onSend(value, composerSend.attachments, [], composerSend.options);
      setValue('');
      return true;
    };
    useImperativeHandle(ref, () => ({
      setValue,
      giveBack: (text: string, files: ReadyAttachment[] = []) => {
        if (text) setValue((v) => (v ? `${v}\n\n${text}` : text));
        composerSend.givenBack.push(...files);
      },
      send,
      getModelOptions: () => composerSend.options,
    }));
    return (
      <div>
        <textarea
          data-testid="composer"
          data-in-home={inHome ? '' : undefined}
          aria-label="Message"
          value={value}
          disabled={disabled}
          placeholder={placeholder}
          onChange={(e) => setValue(e.target.value)}
        />
        <button type="button" disabled={disabled} onClick={send}>
          Send
        </button>
      </div>
    );
  });
  return { default: StubComposer };
}

export const updatePreferencesMock = () => ({ useUpdatePreferences: () => ({ mutateAsync }) });

export const brokersMock = () => ({
  useBrokers: () => ({
    connected: mocks.connected,
    rows: mocks.rows,
    ready: mocks.brokersReady,
    offersError: mocks.brokersError,
    catalogError: null,
  }),
});

/** The brokerage screen has its own surface tests; the flow only needs a
 *  stand-in. */
export const brokerageStepMock = () => ({
  BrokerageStep: ({ onElsewhere }: { onElsewhere: () => void }) => (
    <div data-testid="brokerage-step">
      <button type="button" onClick={onElsewhere}>
        stand-in elsewhere
      </button>
    </div>
  ),
});

/** The drop zone and the search have their own tests; the flow needs what
 *  they hand back. */
export const screenshotDropMock = () => ({
  ScreenshotDrop: ({ onChange }: { onChange: (shots: unknown[]) => void }) => (
    <button
      type="button"
      onClick={() => onChange([{ id: 's1', name: 'positions.png', dataUrl: 'data:image/jpeg;base64,AAAA', bytes: 4 }])}
    >
      stand-in add screenshot
    </button>
  ),
});

export const holdingsEntryMock = () => ({
  HoldingsEntry: () => <p data-testid="holdings-entry">holdings entry</p>,
});

export const warmWorkspaceMock = () => ({ warmWorkspace: (...a: unknown[]) => warmWorkspace(...a) });

export async function mcpServersMock(importOriginal: () => Promise<typeof import('@/hooks/useMcpServers')>) {
  return { ...(await importOriginal()), waitForServerTools: (...a: unknown[]) => waitForServerTools(...a) };
}

export const homeWorkspaceMock = () => ({
  homeWorkspaceQuery: () => ({
    queryKey: ['test-home'],
    queryFn: async () => ({ workspace_id: 'home-1' }),
  }),
  resolveChatWorkspace: (...a: unknown[]) => resolveChatWorkspace(...a),
});

export const marketBriefMock = () => ({
  fetchMarketBrief: () => readBrief(),
  marketBriefQuery: { queryKey: ['test-brief'], queryFn: () => readBrief(), retry: false, staleTime: Infinity },
});

export const themeMock = () => ({ useTheme: () => ({ theme: 'light' }) });

export const toastMock = () => ({ toast: (...a: unknown[]) => toast(...a) });

export async function dashboardApiMock(importOriginal: () => Promise<typeof import('@/pages/Dashboard/utils/api')>) {
  return {
    ...(await importOriginal()),
    getPreferences: () => getPreferences(),
    // Skip and Start save the language the flow was in to the account; its
    // own tests are welcomeLocale.test.tsx.
    updateCurrentUser: async () => ({}),
  };
}

export const syncUserLocaleMock = () => ({ useSyncUserLocale: () => {} });
export const userMock = () => ({ useUser: () => ({ user: { user_id: 'u1' } }) });
export const onboardingProviderMock = () => ({ useOnboarding: () => ({ skipWelcome: () => {} }) });

// --- The answers a user gives ---

/** Every question answered, and nothing optional: what a user who went
 *  through the screens without the optional lists has saved. A full report
 *  for the answer, which is saved as Settings' output format rather than
 *  beside it. */
export const answered: InvestingProfile = {
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

export const DRAFT = 'langalpha-welcome-draft:u1';

// --- Mounting, and reading where the flow went ---

function LocationProbe() {
  const { pathname, search, state } = useLocation();
  return (
    <>
      <span data-testid="location">{pathname + search}</span>
      <span data-testid="state">{JSON.stringify(state ?? null)}</span>
    </>
  );
}

type Flow = typeof WelcomeFlow;

interface FlowOptions {
  mode?: 'new' | 'edit';
  initialStep?: Parameters<Flow>[0]['initialStep'];
  profile?: InvestingProfile;
  /** What the server holds; by default the answers shown. */
  saved?: InvestingProfile;
  userName?: string | null;
}

/** Mounts `Flow` at /welcome, as the route opens it with what it read. */
export function flowRenderer(Flow: Flow) {
  return function renderFlow({
    mode = 'new',
    initialStep = 'experience',
    profile = emptyProfile(),
    saved = profile,
    userName = null,
  }: FlowOptions = {}) {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const onSkip = vi.fn();
    render(
      <QueryClientProvider client={client}>
        <MemoryRouter initialEntries={['/welcome']}>
          <Routes>
            <Route
              path="/welcome"
              element={
                <Flow
                  mode={mode}
                  steps={welcomeSteps(mode)}
                  initialStep={initialStep}
                  initialProfile={profile}
                  savedProfile={saved}
                  userId="u1"
                  userName={userName}
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
    return { onSkip, client };
  };
}

/** Mounts the route at `url`, with the preferences cache filled when given. */
export function routeRenderer(Welcome: ComponentType) {
  return function renderRoute(url: string, { cached }: { cached?: unknown } = {}) {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    if (cached !== undefined) client.setQueryData(queryKeys.user.preferences(), cached);
    render(
      <QueryClientProvider client={client}>
        <MemoryRouter initialEntries={[url]}>
          <Routes>
            <Route path="/welcome" element={<Welcome />} />
            <Route path="*" element={null} />
          </Routes>
          <LocationProbe />
        </MemoryRouter>
      </QueryClientProvider>,
    );
  };
}

export const location = () => screen.getByTestId('location').textContent;
/** The state the flow navigated with: what the chat sends first. */
export const routeState = () => JSON.parse(screen.getByTestId('state').textContent ?? 'null');
/** What the opening turn tells the agent, on the onboarding skill. */
export const instruction = (): string => routeState().additionalContext[0].instruction;
/** Today's brief as the opening turn carries it, or null without one. */
export const briefSent = (): string | null =>
  (routeState().additionalContext as { type: string; content?: string }[]).find((c) => c.type === 'directive')
    ?.content ?? null;
/** The way on, in the foot of the card being asked. */
export const primary = () => screen.getByTestId('welcome-continue');
/** The turn being asked: the one agent turn with its card open. */
export const turn = (id: string) => screen.getByTestId(`welcome-turn-${id}`);
/** The user's bubble answering a turn already passed. */
export const answer = (id: string) => screen.getByTestId(`welcome-answer-${id}`);
export const pencil = (id: string) => within(answer(id)).getByRole('button', { name: 'Change answer' });
export const revisit = (id: string) => fireEvent.click(pencil(id));
export const composer = () => screen.getByTestId('composer');
export const send = () => fireEvent.click(screen.getByRole('button', { name: 'Send' }));

/** Holds the hand-off at the workspace it opens the chat in, so what it shows
 *  while it starts can be read; the call returned lets it go on. */
export function holdWorkspace() {
  let release: (id: string) => void = () => {};
  resolveChatWorkspace.mockImplementation(() => new Promise((resolve) => (release = resolve)));
  return () => act(async () => release('home-1'));
}
