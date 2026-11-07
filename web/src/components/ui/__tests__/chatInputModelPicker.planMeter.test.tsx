/**
 * The picker's Claude plan meter: it heads a brand tab that holds Claude Code
 * models, and Favorites only while the current model is one of them; its
 * Check now icon shows with the credential the composer gives it.
 */
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ComponentProps } from 'react';
import { ChatInputModelMenu } from '../chat-input.modelMenu';
import { modelGroups } from '@/lib/modelGroups';
import type { Payer } from '@/lib/modelPayer';
import type { ClaudeCodePlan } from '@/api/model';
import type { ModelRowStatus } from '../chat-input.modelPicker';

vi.mock('@/hooks/usePayerLabel', async () => {
  const { payerLabel } = await import('@/lib/modelPayer');
  const { useTranslation } = await import('react-i18next');
  return { usePayerLabel: () => { const { t } = useTranslation(); return (payer: Payer) => payerLabel(t, payer); } };
});
vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => false }));
const checkClaudeCodePlan = vi.hoisted(() => vi.fn(() => Promise.resolve()));
vi.mock('@/api/model', async (original) => ({ ...(await original<typeof import('@/api/model')>()), checkClaudeCodePlan }));

const metadata = {
  'claude-opus': { display_name: 'Claude Opus 9', provider: 'anthropic', access_type: 'api_key' },
  'claude-opus-code': { display_name: 'Claude Opus 9 Code', provider: 'claude-code', sdk: 'claude-code' },
  'gpt-x': { display_name: 'GPT X', provider: 'openai', access_type: 'api_key' },
};
const payers: Record<string, Payer> = {
  'claude-opus': 'credits',
  'claude-opus-code': 'subscription:claude-code',
  'gpt-x': 'own_key',
};
const groups = modelGroups({
  models: {
    'claude-code': { display_name: 'Claude Code', models: ['claude-opus-code'] },
    anthropic: { display_name: 'Anthropic', models: ['claude-opus'] },
    openai: { display_name: 'OpenAI', models: ['gpt-x'] },
  },
  payers,
});

const plan: ClaudeCodePlan = {
  five_hour: { used: 0.4, resets_at: '2099-01-01T00:00:00Z' },
  seven_day: { used: 0.9, resets_at: '2099-01-02T00:00:00Z' },
  limits: [],
  credits: null,
  observed_at: new Date().toISOString(),
};
const meter = { name: 'Studio Mac', plan, credential: { kind: 'computer', computerId: 'cmp-mac' }, checkAfter: null } as const;

function open(overrides: Partial<ComponentProps<typeof ChatInputModelMenu>> = {}) {
  render(
    <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
      <MemoryRouter>
        <ChatInputModelMenu
          selectedModel="gpt-x"
          metadata={metadata}
          payers={payers}
          groups={groups}
          favorites={['gpt-x', 'claude-opus-code']}
          recent={[]}
          onToggleFavorite={vi.fn()}
          onSelectModel={vi.fn()}
          rowStatus={() => ({})}
          planMeter={meter}
          dropdownDirection="up"
          container={null}
          {...overrides}
        />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  fireEvent.click(screen.getByTitle('Select model'));
  return screen.getByRole('dialog');
}

const tab = (dialog: HTMLElement, name: string) =>
  within(within(dialog).getByRole('radiogroup', { name: 'Model groups' })).getByRole('radio', { name });
const meterOf = (dialog: HTMLElement) => dialog.querySelector('[data-plan-meter]');

describe('the picker\'s plan meter', () => {
  it('heads the tab of a brand that holds Claude Code models, with each window and the account it reads', () => {
    const dialog = open();
    fireEvent.click(tab(dialog, 'Claude Code'));
    const header = meterOf(dialog) as HTMLElement;
    expect(header).toHaveTextContent('Claude plan on Studio Mac');
    expect(header.querySelector('[data-plan-window="five_hour"]')).toHaveTextContent('5h');
    expect(header.querySelector('[data-plan-window="five_hour"]')).toHaveTextContent('40%');
    expect(header.querySelector('[data-plan-window="seven_day"]')).toHaveTextContent('90%');
  });

  it.each(['Anthropic', 'OpenAI'])('leaves the %s tab with its name, since it holds no Claude Code model', (name) => {
    const dialog = open();
    fireEvent.click(tab(dialog, name));
    expect(meterOf(dialog)).toBeNull();
    expect(within(dialog).getByText(name, { selector: 'span.truncate' })).toBeInTheDocument();
  });

  it('heads Favorites while the current model is a Claude Code model', () => {
    const dialog = open({ selectedModel: 'claude-opus-code' });
    expect(tab(dialog, 'Favorites')).toHaveAttribute('aria-checked', 'true');
    expect(meterOf(dialog)).toHaveTextContent('Claude plan on Studio Mac');
  });

  it('keeps Favorites headed by its name while the current model is not one, whatever the favorites hold', () => {
    const dialog = open({ selectedModel: 'gpt-x' });
    expect(tab(dialog, 'Favorites')).toHaveAttribute('aria-checked', 'true');
    expect(meterOf(dialog)).toBeNull();
    expect(within(dialog).getByText('Favorites', { selector: 'span.truncate' })).toBeInTheDocument();
  });

  it('shows the Check now icon beside the meter where the composer gave it a credential', () => {
    const dialog = open({ selectedModel: 'claude-opus-code' });
    expect(within(meterOf(dialog) as HTMLElement).getByRole('button', { name: 'Check now' })).toBeInTheDocument();
  });

  it('shows the meter alone where it has no credential', () => {
    const dialog = open({ selectedModel: 'claude-opus-code', planMeter: { ...meter, credential: null } });
    expect(meterOf(dialog)).toHaveTextContent('Claude plan on Studio Mac');
    expect(within(dialog).queryByRole('button', { name: 'Check now' })).toBeNull();
  });

  it('says the limit the plan is held at where no window reported, and no reset Claude did not give', () => {
    const held: ClaudeCodePlan = {
      ...plan, five_hour: null, seven_day: null,
      limits: [{ window: null, until: '2099-01-01T00:00:00Z', family: null, reset_known: false }],
    };
    const dialog = open({ selectedModel: 'claude-opus-code', planMeter: { ...meter, plan: held } });
    expect(meterOf(dialog)).toHaveTextContent('Plan limit used up');
    expect(meterOf(dialog)).not.toHaveTextContent('Usage shows after the first turn');
  });
});

describe('Check now on a held row', () => {
  const HELD = 'Plan limit used up until 3:40 PM';
  const codeRow = (dialog: HTMLElement) => within(dialog).getAllByRole('row').find((r) => r.dataset.key === 'claude-opus-code') as HTMLElement;

  it('checks the credential the row names from the held row, which a press neither picks nor closes', async () => {
    const onSelectModel = vi.fn();
    const dialog = open({
      onSelectModel,
      rowStatus: (m): ModelRowStatus => (m === 'claude-opus-code' ? { unavailable: HELD, check: { credential: meter.credential, checkAfter: null } } : {}),
    });
    fireEvent.click(tab(dialog, 'Claude Code'));
    const row = codeRow(dialog);
    expect(row).not.toHaveAttribute('aria-disabled');
    expect(row).toHaveAccessibleName(`Claude Opus 9 Code, ${HELD}`);
    const check = within(row).getByRole('button', { name: 'Check now' });
    expect(check).toBeEnabled();
    fireEvent.click(check);
    await waitFor(() => expect(checkClaudeCodePlan).toHaveBeenCalledWith(meter.credential));
    fireEvent.click(row);
    expect(onSelectModel).not.toHaveBeenCalled();
    expect(screen.getByRole('dialog')).toBeInTheDocument();
  });

  it.each([['Enter', '{Enter}'], ['Space', ' ']])('checks from the keyboard with %s, as the star beside it takes its keys', async (_, key) => {
    const user = userEvent.setup();
    checkClaudeCodePlan.mockClear();
    const onSelectModel = vi.fn();
    const dialog = open({
      onSelectModel,
      rowStatus: (m): ModelRowStatus => (m === 'claude-opus-code' ? { unavailable: HELD, check: { credential: meter.credential, checkAfter: null } } : {}),
    });
    fireEvent.click(tab(dialog, 'Claude Code'));
    within(codeRow(dialog)).getByRole('button', { name: 'Check now' }).focus();
    await user.keyboard(key);
    await waitFor(() => expect(checkClaudeCodePlan).toHaveBeenCalledWith(meter.credential));
    expect(onSelectModel).not.toHaveBeenCalled();
  });

  it('offers none on a row held with no check to make', () => {
    const dialog = open({ rowStatus: (m): ModelRowStatus => (m === 'claude-opus-code' ? { unavailable: HELD } : {}) });
    fireEvent.click(tab(dialog, 'Claude Code'));
    expect(within(codeRow(dialog)).queryByRole('button', { name: 'Check now' })).toBeNull();
  });
});
