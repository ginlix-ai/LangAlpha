/* eslint-disable react-refresh/only-export-components -- entry file, not a module */
import React, { StrictMode, useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { useTranslation } from 'react-i18next';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router';
import i18n, { initI18n } from '@/i18n';
import { switchLocale } from '@/lib/locale';
import { ThemeProvider, useTheme } from '@/contexts/ThemeContext';
import '@/index.css';
import ChatInput from '@/components/ui/chat-input';
import { ChatInputModelMenu } from '@/components/ui/chat-input.modelMenu';
import type { ModelTuning } from '@/components/ui/chat-input.modelTuning';
import type { ModelMetadataEntry } from '@/hooks/useFilteredModels';
import { modelGroups } from '@/lib/modelGroups';
import type { Payer } from '@/lib/modelPayer';
import { resolveEffort } from '@/lib/modelTuning';
import { CheckNowFor } from '@/components/modelAccess/PlanCheck';
import {
  NoticeAction, NoticeBody, type ComposerNotice, type ComposerNotices,
} from '@/components/ui/chat-input.notices';
import type { TokenUsage } from '@/pages/ChatAgent/session/tokenUsage';
import type { DueSummary } from '../../../hooks/useNextSummaryHint';
import { DefaultModelOfferNotice, LapsedSignInNotice, SubagentsOfferNotice } from '../modelNotices';

/**
 * Dev-only harness for the notices a thread host says on its composer's
 * controls: the REAL ChatInput over fixture notices, a transcript stand-in
 * above it so a notice can be seen not to move anything. Open
 * /composer-preview.html on the dev server. Not bundled in production.
 *
 * Knobs are query parameters, so a screenshot is one URL:
 *   ?scenario=lapsed&width=720&theme=dark&locale=zh-CN&style=soft
 * The `picker-*` scenarios render the model pill over the catalog's real
 * figures (task costs, prices, ladders) and open its picker on load.
 * `scenario` is one of SCENARIOS' ids; with none the list of them is shown.
 * Session storage is cleared on load, so a warning opens on its own each time.
 */

const params = new URLSearchParams(window.location.search);
const knobs = {
  theme: params.get('theme') === 'dark' ? 'dark' : 'light',
  locale: params.get('locale') === 'zh-CN' ? 'zh-CN' : 'en-US',
  scenario: params.get('scenario'),
  width: Number(params.get('width')) || 720,
  // The card style the app stamps on <html> before it paints (lib/cardStyle).
  style: params.get('style') ?? 'balanced',
} as const;

document.documentElement.dataset.cardStyle = knobs.style;

try { sessionStorage.clear(); } catch { /* a page without storage opens them anyway */ }

// No request leaves the page: every fixture is here.
window.fetch = async (input) => {
  const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
  console.warn('[composer-preview] blocked fetch', url);
  return new Response(JSON.stringify({ detail: 'preview' }), { status: 503, headers: { 'content-type': 'application/json' } });
};

const THREAD = '3f9a1c2e-7d10-4b2a-9c55-0e1f2a3b4c5d';

const USAGE: TokenUsage = {
  totalInput: 410_000, totalOutput: 21_000, totalCached: 302_000, fromServer: true,
  lastInput: 118_000, lastCached: 96_000, total: 121_400, threshold: 160_000, contextLimit: 200_000,
};

type Scenario = {
  id: string;
  label: string;
  model?: (t: ReturnType<typeof useTranslation>['t']) => ComposerNotice;
  subagents?: (t: ReturnType<typeof useTranslation>['t']) => ComposerNotice;
  summary?: DueSummary['reason'];
  usage?: boolean;
  /** Renders the model pill alone, its picker open, on this model. */
  picker?: { model: string; fast?: boolean; effort?: string | null; warning?: boolean };
};

const log = (what: string) => () => console.info('[composer-preview]', what);

/** A fallback's switch, drawn as `useComposerNotices` draws it. */
const fallbackSwitch = (action: string) => (
  <NoticeBody><NoticeAction onClick={log('switch')} closes>{action}</NoticeAction></NoticeBody>
);

const SCENARIOS: Scenario[] = [
  {
    id: 'lapsed',
    label: 'Lapsed sign-in: a warning on the pill, opened on its own',
    model: (t) => {
      const text = t('chat.threadModel.lapsed', { account: t('modelAccess.codex.name'), model: 'GPT-5.5' });
      return { key: 'lapsed:gpt-5.5', tone: 'warning', text, content: <LapsedSignInNotice reason="expired" /> };
    },
  },
  {
    id: 'not-connected',
    label: 'Plan not connected: a warning on the pill, with Connect',
    model: (t) => {
      const text = t('chat.threadModel.notConnected', { account: t('modelAccess.codex.name'), model: 'GPT-5.5' });
      return { key: 'missing:gpt-5.5', tone: 'warning', text, content: <LapsedSignInNotice reason="missing" /> };
    },
  },
  {
    id: 'plan',
    label: 'Claude plan at its limit, with Check now',
    model: () => {
      const text = "Your Claude plan's 5-hour limit is used up until 3:40 PM. Pick another model to keep going.";
      return {
        key: 'plan:claude-opus:held', tone: 'warning', text,
        content: <NoticeBody><CheckNowFor target={{ credential: { kind: 'saved' }, checkAfter: null }} /></NoticeBody>,
      };
    },
  },
  {
    id: 'fallback',
    label: 'A fallback answered: switch',
    model: (t) => {
      const text = t('chat.modelTroubleSuggestion', { from: 'GPT-5.5', to: 'Claude Sonnet 5' });
      return { key: 'fallback:x', tone: 'warning', text, content: fallbackSwitch(t('chat.switchToModel', { model: 'Claude Sonnet 5' })) };
    },
  },
  {
    id: 'offer',
    label: 'Make this model the default: an offer, no mark',
    model: (t) => {
      const text = t('chat.threadModel.offer', { model: 'GPT-5.5', defaultModel: 'Claude Sonnet 5' });
      return {
        key: 'offer:gpt-5.5', tone: 'offer', text,
        content: <DefaultModelOfferNotice model="gpt-5.5" />,
        onDismiss: log('offer dismissed'), onExpire: log('offer expired'),
      };
    },
  },
  {
    id: 'subagents',
    label: 'Subagents offer on the toggle',
    subagents: (t) => {
      const text = t('chat.threadSubagents.offerOff');
      return {
        key: 'subagents:false', tone: 'offer', text,
        content: <SubagentsOfferNotice allowed={false} />,
        onDismiss: log('subagents dismissed'), onExpire: log('subagents expired'),
      };
    },
  },
  { id: 'summary', label: 'Summary due after a break: dot on the ring, Skip in its panel', summary: 'cold', usage: true },
  { id: 'summary-uncounted', label: 'Summary due with no window counted: the ring still shows', summary: 'rebuild', usage: false },
  { id: 'quiet', label: 'Nothing to say', usage: true },
  { id: 'picker-claude', label: 'Picker: Claude on credits, pane beside the list', picker: { model: 'claude-sonnet-5-5', effort: 'high' } },
  { id: 'picker-codex', label: 'Picker: a Codex model on a plan, with speed', picker: { model: 'gpt-6.2-sol-oauth', fast: true } },
  { id: 'picker-subscription', label: 'Picker: Claude on a Claude plan, no marks or price', picker: { model: 'claude-opus-5-5-oauth' } },
  { id: 'picker-long', label: 'Picker: a custom model with a long name', picker: { model: 'nousresearch/hermes-5-405b-instruct-2026-10-preview' } },
  { id: 'picker-estimate', label: 'Picker: a model with no measured figure, estimated marks', picker: { model: 'qwen-coder-next' } },
  { id: 'picker-bound', label: 'Picker: a default level carried from the one above (an upper bound)', picker: { model: 'claude-fable-5-2' } },
  { id: 'picker-warning', label: 'Picker: warning header over the pane', picker: { model: 'claude-sonnet-5-5', warning: true } },
];

// Figures shaped like the models API's (task costs against a Claude Sonnet 5.5
// at Medium baseline), so the marks and bars have realistic spreads to draw.
// They were copied once and are not kept in step with the catalog.
const tc = (o: Record<string, number | [number, false]>) => Object.fromEntries(
  Object.entries(o).map(([k, v]) => [k, Array.isArray(v) ? { value: v[0], measured: false } : { value: v, measured: true }]),
);
const FIVE = ['low', 'medium', 'high', 'xhigh', 'max'];
const NO_EFFORTS: string[] = [];
const SIX = ['none', ...FIVE];
const MODELS: Record<string, ModelMetadataEntry> = {
  'claude-sonnet-5-5': {
    display_name: 'Claude Sonnet 5.5', provider: 'anthropic', reasoning_efforts: FIVE, reasoning_effort_default: 'medium',
    task_cost: tc({ low: 0.71, medium: 1, high: 1.83, xhigh: 4.16, max: 11.3 }), pricing: { input: 2, output: 10 },
  },
  'claude-opus-5-5': {
    display_name: 'Claude Opus 5.5', provider: 'anthropic', reasoning_efforts: FIVE, reasoning_effort_default: 'medium',
    task_cost: tc({ low: 1.14, medium: 2.76, high: 3.77, xhigh: 7.16, max: 12.38 }), pricing: { input: 4, output: 20 },
  },
  'claude-haiku-5-5': {
    display_name: 'Claude Haiku 5.5', provider: 'anthropic', reasoning_efforts: FIVE, reasoning_effort_default: 'medium',
    task_cost: tc({ low: 0.05, medium: 0.1, high: 0.16, xhigh: 0.26, max: 0.44 }), pricing: { input: 0.1, output: 0.5 },
  },
  'claude-fable-5-2': {
    display_name: 'Claude Fable 5.2', provider: 'anthropic', reasoning_efforts: FIVE, reasoning_effort_default: 'medium',
    task_cost: tc({ low: 3.77, medium: [11.04, false], high: 11.04, xhigh: 18.02, max: 32.26 }), pricing: { input: 8, output: 40 },
  },
  'claude-opus-5-5-oauth': {
    display_name: 'Claude Opus 5.5', provider: 'claude-oauth', reasoning_efforts: FIVE, reasoning_effort_default: 'medium',
    task_cost: tc({ low: 1.14, medium: 2.76, high: 3.77, xhigh: 7.16, max: 12.38 }), pricing: { input: 4, output: 20 },
  },
  'gpt-6.2-sol-oauth': {
    display_name: 'GPT-6.2 Sol', provider: 'codex-oauth', sdk: 'codex', reasoning_efforts: FIVE, reasoning_effort_default: 'medium',
    task_cost: tc({ low: 0.22, medium: 0.49, high: 0.68, xhigh: 0.79, max: 1.23 }), pricing: { input: 2, output: 10 },
  },
  'gpt-6-luna-oauth': {
    display_name: 'GPT-6 Luna', provider: 'codex-oauth', sdk: 'codex', reasoning_efforts: SIX, reasoning_effort_default: 'medium',
    task_cost: tc({ none: 0.02, low: 0.01, medium: 0.04, high: 0.06, xhigh: 0.09, max: 0.14 }), pricing: { input: 0.1, output: 0.5 },
  },
  'deepseek-flash': {
    display_name: 'DeepSeek-V4.1-Flash', provider: 'deepseek', reasoning_efforts: ['none', 'low', 'high', 'max'], reasoning_effort_default: 'high',
    task_cost: tc({ none: [0.31, false], low: 0.31, high: 0.41, max: 0.54 }), pricing: { input: 0.3, output: 1.2 },
  },
  'glm-5.3': {
    display_name: 'GLM-5.3', provider: 'z-ai', reasoning_efforts: ['low', 'high', 'max'], reasoning_effort_default: 'high',
    task_cost: tc({ low: 1.76, high: [4.15, false], max: 4.15 }), pricing: { input: 1.4, output: 4.4 },
  },
  'nousresearch/hermes-5-405b-instruct-2026-10-preview': {
    // A custom model, named by its long id, for how a long name is cut.
    display_name: 'nousresearch/hermes-5-405b-instruct-2026-10-preview', provider: 'openrouter', is_custom_model: true,
  },
  'qwen-coder-next': {
    display_name: 'Qwen Coder Next', provider: 'qwen', reasoning_efforts: ['low', 'high'], reasoning_effort_default: 'high',
    task_cost_estimate: 0.6, pricing: { input: 0.45, output: 1.8 },
  },
};
const PAYERS: Record<string, Payer> = {
  'claude-sonnet-5-5': 'credits', 'claude-opus-5-5': 'credits', 'claude-haiku-5-5': 'credits', 'claude-fable-5-2': 'credits',
  'claude-opus-5-5-oauth': 'subscription:claude-oauth',
  'gpt-6.2-sol-oauth': 'subscription:codex-oauth', 'gpt-6-luna-oauth': 'subscription:codex-oauth',
  'deepseek-flash': 'own_key', 'glm-5.3': 'credits', 'qwen-coder-next': 'own_key',
  'nousresearch/hermes-5-405b-instruct-2026-10-preview': 'own_key',
};
const GROUPS = modelGroups({
  models: {
    anthropic: { display_name: 'Anthropic', models: ['claude-sonnet-5-5', 'claude-opus-5-5', 'claude-haiku-5-5', 'claude-fable-5-2', 'claude-opus-5-5-oauth'] },
    openai: { display_name: 'OpenAI', models: ['gpt-6.2-sol-oauth', 'gpt-6-luna-oauth'] },
    deepseek: { display_name: 'DeepSeek', models: ['deepseek-flash'] },
    'z-ai': { display_name: 'Z.ai', models: ['glm-5.3'] },
    qwen: { display_name: 'Qwen', models: ['qwen-coder-next'] },
    openrouter: { display_name: 'OpenRouter', models: ['nousresearch/hermes-5-405b-instruct-2026-10-preview'] },
  },
  payers: PAYERS,
});
const FAVORITES = ['claude-sonnet-5-5', 'claude-opus-5-5', 'gpt-6.2-sol-oauth', 'claude-haiku-5-5', 'deepseek-flash', 'glm-5.3', 'qwen-coder-next', 'nousresearch/hermes-5-405b-instruct-2026-10-preview', 'claude-fable-5-2', 'claude-opus-5-5-oauth'];

/** The model pill alone in a composer-shaped box, its picker opened on load,
 *  with the composer's own rules for what a pick does to the tuning. */
function PickerComposer({ picker }: { picker: NonNullable<Scenario['picker']> }): React.ReactElement {
  const { t } = useTranslation();
  const [model, setModel] = useState(picker.model);
  const [effort, setEffort] = useState<string | null>(picker.effort ?? null);
  const [fast, setFast] = useState<boolean | null>(picker.fast ?? null);
  const [box, setBox] = useState<HTMLDivElement | null>(null);
  const meta = MODELS[model];
  const efforts = meta?.reasoning_efforts ?? NO_EFFORTS;
  const defaultEffort = meta?.reasoning_effort_default ?? null;
  const shown = effort === null ? defaultEffort : resolveEffort(effort, efforts);
  const tuning = useMemo<ModelTuning>(() => ({
    reasoningEffort: shown,
    defaultEffort,
    onReasoningEffortChange: setEffort,
    fastMode: fast ?? false,
    onFastModeChange: setFast,
  }), [shown, defaultEffort, fast]);
  const notice = useMemo<ComposerNotice | null>(() => {
    if (!picker.warning) return null;
    const text = t('chat.modelTroubleSuggestion', { from: 'Claude Sonnet 5.5', to: 'GPT-6.2 Sol' });
    return { key: 'fallback:preview', tone: 'warning', text, content: fallbackSwitch(t('chat.switchToModel', { model: 'GPT-6.2 Sol' })) };
  }, [picker.warning, t]);
  useEffect(() => {
    const frame = requestAnimationFrame(() => box?.querySelector<HTMLButtonElement>('[data-pill="model"]')?.click());
    return () => cancelAnimationFrame(frame);
  }, [box]);
  return (
    <div
      ref={setBox}
      style={{ minHeight: '6.5rem' }}
      className="flex flex-col justify-between rounded-[var(--cs-composer-radius,16px)] border border-[color:var(--color-shell-edge,var(--color-border-input))] bg-(--color-bg-card) px-3 py-2"
    >
      <span className="pt-1 text-sm text-(--color-text-tertiary)">Ask anything</span>
      <div className="flex items-center justify-end gap-1">
        <ChatInputModelMenu
          selectedModel={model}
          metadata={MODELS}
          payers={PAYERS}
          groups={GROUPS}
          favorites={FAVORITES}
          recent={[]}
          onToggleFavorite={log('favorite')}
          onSelectModel={(m) => { setModel(m); setEffort(null); setFast(null); }}
          rowStatus={() => ({})}
          dropdownDirection="up"
          container={box}
          notice={notice}
          tuning={tuning}
          taskCostBaseline={{ model: 'claude-sonnet-5-5', effort: 'medium' }}
        />
        <span className="h-8 w-8 rounded-lg bg-(--color-btn-primary-bg)" />
      </div>
    </div>
  );
}

function Composer({ scenario }: { scenario: Scenario }): React.ReactElement {
  const { t } = useTranslation();
  const [skipped, setSkipped] = useState(false);
  const [subagentsOn, setSubagentsOn] = useState(false);
  const notices = useMemo<ComposerNotices>(() => ({
    model: scenario.model?.(t) ?? null,
    subagents: scenario.subagents?.(t) ?? null,
    summary: scenario.summary
      ? { reason: scenario.summary, skipped, keepFullContext: skipped, toggleSkip: () => setSkipped((s) => !s) }
      : null,
  }), [scenario, t, skipped]);
  return (
    <ChatInput
      onSend={log('send')}
      model="gpt-5.5"
      onPickModel={log('pick')}
      subagentsAllowed={subagentsOn}
      onToggleSubagents={setSubagentsOn}
      tokenUsage={scenario.usage === false ? null : USAGE}
      threadId={THREAD}
      notices={notices}
    />
  );
}

/** A scenario's address, carrying the other knobs so the list keeps them. */
function scenarioLink(id: string): string {
  const next = new URLSearchParams(params);
  next.set('scenario', id);
  return next.toString();
}

function Stage(): React.ReactElement {
  const scenario = SCENARIOS.find((s) => s.id === knobs.scenario);
  if (!scenario) {
    return (
      <ul className="space-y-1 p-6 font-mono text-xs">
        {SCENARIOS.map((s) => (
          <li key={s.id}><a className="underline" href={`?${scenarioLink(s.id)}`}>{s.id}</a>: {s.label}</li>
        ))}
      </ul>
    );
  }
  return (
    <div className="flex min-h-screen flex-col items-center justify-end px-4 pb-6">
      {/* Tailwind skips this folder (index.css), so a utility used nowhere
          else in the app goes inline. */}
      <div style={{ marginBottom: 'auto' }} className="pt-4 font-mono text-[0.625rem] text-(--color-text-quaternary)">{scenario.id}: {scenario.label}</div>
      {/* A stand-in for the end of the transcript, so a notice can be seen
          to leave everything above the composer where it was. */}
      <div style={{ width: knobs.width }} className="mb-4 space-y-2 text-sm text-(--color-text-secondary)">
        <p>Revenue grew 14% year over year, with services carrying most of the margin expansion.</p>
        <p>The next step is to reconcile the segment figures against the filing.</p>
      </div>
      <div style={{ width: knobs.width }} data-composer-stage>
        {scenario.picker ? <PickerComposer picker={scenario.picker} /> : <Composer scenario={scenario} />}
      </div>
    </div>
  );
}

const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, enabled: false } } });

function Preview(): React.ReactElement {
  const { setTheme } = useTheme();
  useEffect(() => setTheme(knobs.theme), [setTheme]);
  return (
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <main className="font-content min-h-screen bg-(--color-bg-page) text-(--color-text-primary)">
          <Stage />
        </main>
      </MemoryRouter>
    </QueryClientProvider>
  );
}

void initI18n()
  .then(() => switchLocale(i18n, knobs.locale))
  .then(() =>
    createRoot(document.getElementById('root')!).render(
      <StrictMode>
        <ThemeProvider>
          <Preview />
        </ThemeProvider>
      </StrictMode>,
    ),
  );
