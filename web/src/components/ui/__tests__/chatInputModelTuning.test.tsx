/**
 * One pill and one picker for the model and its tuning: what the pill says of
 * the level, what the tuning lists beside the models (a pane) or under them (a
 * strip), what a pick writes, and which picks leave the picker open.
 *
 * The load-bearing rule is that picking the level marked "Default", the one
 * Settings gives, clears the thread's own instead of pinning today's default as
 * a permanent choice: that is the only way back to following Settings.
 */
import type { ComponentProps } from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, render, screen, fireEvent, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { ChatInputModelMenu } from '../chat-input.modelMenu';
import type { ModelTuning } from '../chat-input.modelTuning';
import { modelGroups } from '@/lib/modelGroups';
import type { Payer } from '@/lib/modelPayer';

vi.mock('@/hooks/usePayerLabel', async () => {
  const { payerLabel } = await import('@/lib/modelPayer');
  const { useTranslation } = await import('react-i18next');
  return { usePayerLabel: () => { const { t } = useTranslation(); return (payer: Payer) => payerLabel(t, payer); } };
});
vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => false }));
vi.mock('@/hooks/useAccount', () => ({
  useAccount: () => ({
    organization: null, policy: null, unknown: false, suspended: false, organizationName: null,
    setterName: () => ({ start: '', mid: '' }),
  }),
}));

const metadata = {
  sonnet: {
    display_name: 'Claude Sonnet', provider: 'anthropic',
    reasoning_efforts: ['none', 'low', 'medium', 'high'], reasoning_effort_default: 'medium',
    task_cost: {
      none: { value: 0.4, measured: true },
      low: { value: 0.8, measured: false },
      medium: { value: 1, measured: true },
      high: { value: 1.8, measured: true },
    },
    pricing: { input: 2, output: 10 },
  },
  'sonnet-plan': {
    display_name: 'Claude Sonnet', provider: 'claude-oauth',
    reasoning_efforts: ['low', 'medium', 'high'], reasoning_effort_default: 'medium',
    task_cost: { medium: { value: 1, measured: true } },
    pricing: { input: 2, output: 10 },
  },
  haiku: { display_name: 'Claude Haiku', provider: 'anthropic', task_cost: { any: { value: 0.1, measured: true } } },
  luna: { display_name: 'GPT Luna', provider: 'openai', sdk: 'codex', reasoning_efforts: ['low', 'high'], reasoning_effort_default: 'high' },
  local: { display_name: 'Local Llama', provider: 'ollama', task_cost_estimate: 0.3 },
  // Selected only, never listed: a ladder of levels with unusual names.
  wide: { display_name: 'Claude Wide', provider: 'anthropic', reasoning_efforts: ['none', 'xhigh', 'max'], reasoning_effort_default: 'xhigh' },
};
const payers: Record<string, Payer> = {
  sonnet: 'credits', 'sonnet-plan': 'subscription:claude-oauth', haiku: 'credits', luna: 'own_key', local: 'server_key',
};
const groups = modelGroups({
  models: {
    anthropic: { display_name: 'Anthropic', models: ['sonnet', 'sonnet-plan', 'haiku'] },
    openai: { display_name: 'OpenAI', models: ['luna'] },
    ollama: { display_name: 'Ollama', models: ['local'] },
  },
  payers,
});

const onReasoningEffortChange = vi.fn();
const onFastModeChange = vi.fn();
const onSelectModel = vi.fn();

function tuningFor(over: Partial<ModelTuning> = {}): ModelTuning {
  return {
    reasoningEffort: 'high',
    defaultEffort: 'medium',
    onReasoningEffortChange,
    fastMode: false,
    onFastModeChange,
    ...over,
  };
}

function props(over: Partial<ComponentProps<typeof ChatInputModelMenu>> = {}): ComponentProps<typeof ChatInputModelMenu> {
  return {
    selectedModel: 'sonnet',
    metadata,
    payers,
    groups,
    favorites: ['sonnet', 'haiku', 'luna', 'local', 'sonnet-plan'],
    recent: [],
    onToggleFavorite: vi.fn(),
    onSelectModel,
    rowStatus: () => ({}),
    dropdownDirection: 'up',
    container: null,
    tuning: tuningFor(),
    taskCostBaseline: { model: 'sonnet', effort: 'medium' },
    ...over,
  };
}

const pill = () => screen.getByTitle('Select model');

function open(over: Partial<ComponentProps<typeof ChatInputModelMenu>> = {}) {
  const view = render(<MemoryRouter><ChatInputModelMenu {...props(over)} /></MemoryRouter>);
  fireEvent.click(pill());
  return view;
}

const pane = () => screen.getByRole('menu', { name: 'Reasoning' });

/** The pill shows the same word as a level, so look among the levels. */
function level(label: string) {
  const found = within(pane()).getAllByRole('menuitemradio').find((el) => within(el).queryByText(label));
  if (!found) throw new Error(`no level labelled ${label}`);
  return found;
}

const row = (model: string) => screen.getAllByRole('row').find((r) => r.dataset.key === model)!;

beforeEach(() => {
  onReasoningEffortChange.mockClear();
  onFastModeChange.mockClear();
  onSelectModel.mockClear();
});

describe('the pill', () => {
  it('names the model and, quieter, its level', () => {
    render(<MemoryRouter><ChatInputModelMenu {...props()} /></MemoryRouter>);
    expect(pill()).toHaveTextContent('Claude SonnetHigh');
    expect(document.querySelector('[data-pill="effort"]')).toBeNull();
  });

  it('names the default level too', () => {
    render(<MemoryRouter><ChatInputModelMenu {...props({ tuning: tuningFor({ reasoningEffort: 'medium' }) })} /></MemoryRouter>);
    expect(within(pill()).getByText('Medium')).toBeInTheDocument();
  });

  it('drops the level where the toolbar has no room for it', () => {
    render(<MemoryRouter><ChatInputModelMenu {...props({ levelHidden: true })} /></MemoryRouter>);
    expect(within(pill()).queryByText('High')).toBeNull();
  });

  it('says nothing of a level on a model with no ladder', () => {
    render(<MemoryRouter><ChatInputModelMenu {...props({ selectedModel: 'haiku' })} /></MemoryRouter>);
    expect(within(pill()).queryByText('High')).toBeNull();
  });
});

describe('the pane', () => {
  it('lists the current model\'s levels, checks the current one and badges the Settings level alone', () => {
    open();
    expect(level('High')).toHaveAttribute('aria-checked', 'true');
    expect(within(level('Medium')).getByText('Default')).toBeInTheDocument();
    expect(within(pane()).getAllByText('Default')).toHaveLength(1);
  });

  it('checks nothing while the level is not known', () => {
    open({ tuning: tuningFor({ reasoningEffort: null }) });
    expect(within(pane()).getAllByRole('menuitemradio').every((el) => el.getAttribute('aria-checked') === 'false')).toBe(true);
  });

  it('renders words, never a bare level name', () => {
    open({ selectedModel: 'wide', tuning: tuningFor({ reasoningEffort: 'xhigh' }) });
    expect(within(pane()).getAllByRole('menuitemradio').map((el) => el.textContent)).toEqual(['Off', 'Extra High', 'Max']);
  });

  it('clears the override when the default level is picked, checked or not', () => {
    open();
    fireEvent.click(level('Medium'));
    expect(onReasoningEffortChange).toHaveBeenLastCalledWith(null);
    fireEvent.click(level('Low'));
    expect(onReasoningEffortChange).toHaveBeenLastCalledWith('low');
  });

  it('stays open after a level pick, since a level is a setting and not a send', () => {
    open();
    fireEvent.click(level('Low'));
    expect(screen.getByRole('dialog')).toBeInTheDocument();
  });

  it('prints each level as a multiple of the default, an unmeasured one as a bound', () => {
    open();
    expect(within(level('Medium')).getByText('1×')).toBeInTheDocument();
    expect(within(level('Off')).getByText('0.4×')).toBeInTheDocument();
    expect(within(level('Low')).getByText('≤0.8×')).toBeInTheDocument();
    expect(within(level('High')).getByText('1.8×')).toBeInTheDocument();
    expect(pane()).toHaveAccessibleDescription(/how a typical task's cost changes with effort/);
  });

  it('lists the speeds on a Codex model and picks one', () => {
    open({ selectedModel: 'luna' });
    const fast = within(pane()).getAllByRole('menuitemradio').find((el) => el.textContent?.startsWith('Fast'))!;
    fireEvent.click(fast);
    expect(onFastModeChange).toHaveBeenCalledWith(true);
    expect(screen.getByRole('dialog')).toBeInTheDocument();
  });

  it('says a model has nothing to tune rather than leaving a hole', () => {
    open({ selectedModel: 'haiku', tuning: tuningFor({ reasoningEffort: null }) });
    expect(screen.queryByRole('menu', { name: 'Reasoning' })).toBeNull();
    expect(screen.getByText('Nothing to tune on this model')).toBeInTheDocument();
  });

  it('focuses the checked level once a pick brings the menu in place of "Nothing to tune"', () => {
    const view = open({ selectedModel: 'haiku', tuning: tuningFor({ reasoningEffort: null }) });
    expect(screen.queryByRole('menu', { name: 'Reasoning' })).toBeNull();
    view.rerender(<MemoryRouter><ChatInputModelMenu {...props({ selectedModel: 'sonnet' })} /></MemoryRouter>);
    act(() => pane().focus());
    expect(document.activeElement).toBe(level('High'));
  });

  it('gives the API price where the user pays per token, and none on a subscription', () => {
    const view = open();
    expect(screen.getByText('$2 in · $10 out')).toBeInTheDocument();
    view.unmount();
    open({ selectedModel: 'sonnet-plan' });
    expect(screen.queryByText('API price per 1M tokens')).toBeNull();
  });

  it('still weighs the levels against each other on a subscription', () => {
    open({ selectedModel: 'sonnet-plan', tuning: tuningFor({ reasoningEffort: 'medium' }) });
    expect(within(level('Medium')).getByText('1×')).toBeInTheDocument();
  });
});

describe('a model pick', () => {
  it('keeps the picker open for a model with something to tune', () => {
    open();
    fireEvent.click(row('luna'));
    expect(onSelectModel).toHaveBeenCalledWith('luna');
    expect(screen.getByRole('dialog')).toBeInTheDocument();
  });

  it('closes it for a model with nothing to tune', async () => {
    open();
    fireEvent.click(row('haiku'));
    expect(onSelectModel).toHaveBeenCalledWith('haiku');
    await vi.waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
  });

  it('closes it on the current model, a pick confirmed', async () => {
    open();
    fireEvent.click(row('sonnet'));
    await vi.waitFor(() => expect(screen.queryByRole('dialog')).toBeNull());
  });
});

describe('cost marks', () => {
  const marks = (model: string) => row(model).querySelector('[data-cost-marks]');

  it('marks a model from its figure at its default level, and says against what', () => {
    open();
    expect(marks('sonnet')).toHaveAttribute('data-cost-marks', 'measured');
    expect(row('sonnet')).toHaveAccessibleName(/About 1× Claude Sonnet at Medium/);
    expect(row('haiku')).toHaveAccessibleName(/About 0.1× Claude Sonnet at Medium/);
  });

  it('marks a model priced but never measured as an estimate', () => {
    open();
    expect(marks('local')).toHaveAttribute('data-cost-marks', 'estimate');
    expect(row('local')).toHaveAccessibleName(/Estimated from the API price/);
  });

  it('marks nothing on a subscription or with neither figure nor price', () => {
    open();
    expect(marks('sonnet-plan')).toBeNull();
    expect(marks('luna')).toBeNull();
  });

  it('heads their column, as the pane heads its own', () => {
    open();
    expect(screen.getAllByText('Cost per task')).toHaveLength(2);
    expect(within(pane()).getByText('Cost per task')).toBeInTheDocument();
  });
});

describe('the strip, where the composer has no room for the pane', () => {
  const wide = window.innerWidth;
  beforeEach(() => { window.innerWidth = 400; });
  afterEach(() => { window.innerWidth = wide; });

  const segment = (name: string) =>
    within(screen.getByRole('radiogroup', { name: 'Reasoning' })).getByRole('radio', { name });

  it('lists the levels as segments under the list, with no pane', () => {
    open();
    expect(screen.queryByRole('menu', { name: 'Reasoning' })).toBeNull();
    expect(segment('High')).toHaveAttribute('aria-checked', 'true');
    expect(screen.getByText('Default: Medium')).toBeInTheDocument();
  });

  it("describes each level's cost, which the segments have no room to print", () => {
    open();
    expect(segment('High')).toHaveAccessibleDescription("About 1.8× the default level's cost per task");
    expect(segment('Low')).toHaveAccessibleDescription("At most 0.8× the default level's cost per task");
  });

  it('clears the override on the default level and writes any other', () => {
    open();
    fireEvent.click(segment('Medium'));
    expect(onReasoningEffortChange).toHaveBeenLastCalledWith(null);
    fireEvent.click(segment('Low'));
    expect(onReasoningEffortChange).toHaveBeenLastCalledWith('low');
    expect(screen.getByRole('dialog')).toBeInTheDocument();
  });

  it('adds the speeds on a Codex model', () => {
    open({ selectedModel: 'luna' });
    fireEvent.click(within(screen.getByRole('radiogroup', { name: 'Speed' })).getByRole('radio', { name: 'Fast' }));
    expect(onFastModeChange).toHaveBeenCalledWith(true);
  });
});
