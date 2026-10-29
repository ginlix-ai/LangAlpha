/**
 * The composer's model picker: which models each tab lists, how a brand's
 * ways to pay switch, and what a pick, a search and a star do.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import type { ComponentProps } from 'react';
import { ChatInputModelMenu } from '../chat-input.modelMenu';
import { modelGroups } from '@/lib/modelGroups';
import type { Payer } from '@/lib/modelPayer';

vi.mock('@/hooks/usePayerLabel', async () => {
  const { payerLabel } = await import('@/lib/modelPayer');
  const { useTranslation } = await import('react-i18next');
  return { usePayerLabel: () => { const { t } = useTranslation(); return (payer: Payer) => payerLabel(t, payer); } };
});

vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => false }));

const metadata = {
  'claude-opus': { display_name: 'Claude Opus 9', provider: 'anthropic', access_type: 'api_key' },
  'claude-opus-oauth': { display_name: 'Claude Opus 9', provider: 'claude-oauth', access_type: 'oauth' },
  'gpt-x': {
    display_name: 'GPT X', provider: 'openai', access_type: 'api_key',
    reasoning_efforts: ['low', 'high'], reasoning_effort_default: 'high',
  },
};
const payers: Record<string, Payer> = {
  'claude-opus': 'credits',
  'claude-opus-oauth': 'subscription:claude-oauth',
  'gpt-x': 'own_key',
};
const groups = modelGroups({
  models: {
    anthropic: { display_name: 'Anthropic', models: ['claude-opus', 'claude-opus-oauth'] },
    openai: { display_name: 'OpenAI', models: ['gpt-x'] },
  },
  payers,
});

const onSelectModel = vi.fn();
const onToggleFavorite = vi.fn();

function open(overrides: Partial<ComponentProps<typeof ChatInputModelMenu>> = {}) {
  render(
    <MemoryRouter>
      <ChatInputModelMenu
        selectedModel="gpt-x"
        metadata={metadata}
        payers={payers}
        groups={groups}
        favorites={['gpt-x']}
        recent={[]}
        onToggleFavorite={onToggleFavorite}
        onSelectModel={onSelectModel}
        needsSignIn={() => false}
        dropdownDirection="up"
        container={null}
        {...overrides}
      />
    </MemoryRouter>,
  );
  fireEvent.click(screen.getByTitle('Select model'));
  return screen.getByRole('dialog');
}

// A section heading is a row with no model.
const rows = (dialog: HTMLElement) => within(dialog).queryAllByRole('row').map((r) => r.dataset.key).filter(Boolean);
const tab = (dialog: HTMLElement, name: string) =>
  within(within(dialog).getByRole('radiogroup', { name: 'Model groups' })).getByRole('radio', { name });

beforeEach(() => {
  onSelectModel.mockClear();
  onToggleFavorite.mockClear();
});

describe('the rail', () => {
  it('opens on the favorites, and lists one tab per brand', () => {
    const dialog = open();
    expect(tab(dialog, 'Favorites')).toHaveAttribute('aria-checked', 'true');
    expect(rows(dialog)).toEqual(['gpt-x']);
    // A subscription ranks Anthropic above the brand on the user's key.
    const names = within(within(dialog).getByRole('radiogroup', { name: 'Model groups' }))
      .getAllByRole('radio').map((r) => r.getAttribute('aria-label'));
    expect(names).toEqual(['Favorites', 'Anthropic', 'OpenAI']);
  });

  it("lists a brand's models by who pays, the user's own first, and switches", () => {
    const dialog = open();
    fireEvent.click(tab(dialog, 'Anthropic'));
    const paidBy = within(dialog).getByRole('radiogroup', { name: 'Paid by' });
    // A subscription is named by its plan, credits by whose they are, so
    // neither reads as the user's LangAlpha plan.
    expect(within(paidBy).getByRole('radio', { name: 'Claude plan' })).toHaveAttribute('aria-checked', 'true');
    expect(rows(dialog)).toEqual(['claude-opus-oauth']);

    fireEvent.click(within(paidBy).getByRole('radio', { name: 'LangAlpha credits' }));
    expect(rows(dialog)).toEqual(['claude-opus']);
  });

  it("opens a brand on the way the current model is paid for", () => {
    const dialog = open({ selectedModel: 'claude-opus', favorites: [] , recent: ['claude-opus'] });
    fireEvent.click(tab(dialog, 'Anthropic'));
    expect(rows(dialog)).toEqual(['claude-opus']);
  });

  it("tags a tab's models once, above them, rather than on each row", () => {
    const dialog = open();
    fireEvent.click(tab(dialog, 'OpenAI'));
    const [row] = within(dialog).getAllByRole('row');
    expect(within(row).queryByText('Your key')).toBeNull();
    expect(within(dialog).getByText('Your key')).toBeInTheDocument();
  });
});

describe('the favorites tab', () => {
  it("tags each row, since its models come from any brand", () => {
    const dialog = open();
    const [row] = within(dialog).getAllByRole('row');
    expect(within(row).getByText('Your key')).toBeInTheDocument();
  });

  it("lists the thread's other models under the favorites", () => {
    const dialog = open({ recent: ['claude-opus'] });
    expect(rows(dialog)).toEqual(['gpt-x', 'claude-opus']);
    expect(within(dialog).getByText('In this thread')).toBeInTheDocument();
  });

  it('says how to fill it when it is empty', () => {
    const dialog = open({ favorites: [], recent: [], selectedModel: null });
    fireEvent.click(tab(dialog, 'Favorites'));
    expect(within(dialog).getByText('Star a model to keep it here')).toBeInTheDocument();
  });
});

describe('search', () => {
  it('searches every model, brand word included, whatever the tab', () => {
    const dialog = open();
    fireEvent.change(within(dialog).getByRole('searchbox'), { target: { value: 'claude opus' } });
    expect(rows(dialog)).toEqual(['claude-opus-oauth', 'claude-opus']);
    // Two rows of one name, told apart by their tags.
    expect(within(dialog).getByText('Claude plan')).toBeInTheDocument();
    expect(within(dialog).getByText('LangAlpha credits')).toBeInTheDocument();
  });

  it('hands Down to the top row', () => {
    const dialog = open();
    const box = within(dialog).getByRole('searchbox');
    fireEvent.change(box, { target: { value: 'opus' } });
    fireEvent.keyDown(box, { key: 'ArrowDown' });
    expect(document.activeElement).toBe(within(dialog).getAllByRole('row')[0]);
  });

  it('keeps focus in the search when a tab is clicked', () => {
    const dialog = open();
    fireEvent.click(tab(dialog, 'OpenAI'));
    expect(document.activeElement).toBe(within(dialog).getByRole('searchbox'));
  });

  it('starts over on the next opening', () => {
    const dialog = open();
    fireEvent.change(within(dialog).getByRole('searchbox'), { target: { value: 'opus' } });
    fireEvent.click(screen.getByTitle('Select model'));
    fireEvent.click(screen.getByTitle('Select model'));
    const reopened = screen.getByRole('dialog');
    expect(within(reopened).getByRole('searchbox')).toHaveValue('');
    expect(rows(reopened)).toEqual(['gpt-x']);
  });

  it('picks the top result on Enter', () => {
    const dialog = open();
    const box = within(dialog).getByRole('searchbox');
    fireEvent.change(box, { target: { value: 'opus' } });
    fireEvent.keyDown(box, { key: 'Enter' });
    expect(onSelectModel).toHaveBeenCalledWith('claude-opus-oauth');
    expect(screen.queryByRole('dialog')).toBeNull();
  });
});

describe('a row', () => {
  it('picks its model and closes', () => {
    const dialog = open();
    fireEvent.click(tab(dialog, 'Anthropic'));
    fireEvent.click(within(dialog).getByRole('row'));
    expect(onSelectModel).toHaveBeenCalledWith('claude-opus-oauth');
    expect(screen.queryByRole('dialog')).toBeNull();
  });

  it('stars and unstars without picking or closing', () => {
    const dialog = open();
    fireEvent.click(within(dialog).getByRole('button', { name: 'Remove GPT X from favorites' }));
    expect(onToggleFavorite).toHaveBeenCalledWith('gpt-x', false);
    fireEvent.click(tab(dialog, 'Anthropic'));
    fireEvent.click(within(dialog).getByRole('button', { name: 'Add Claude Opus 9 to favorites' }));
    expect(onToggleFavorite).toHaveBeenCalledWith('claude-opus-oauth', true);
    expect(onSelectModel).not.toHaveBeenCalled();
    expect(screen.getByRole('dialog')).toBeInTheDocument();
  });

  // Still pickable, since the user may reconnect at any moment, but named as
  // needing sign-in before a send on it is refused.
  it('says a lapsed sign-in beside the name', () => {
    const dialog = open({ needsSignIn: (m) => m === 'claude-opus-oauth' });
    fireEvent.click(tab(dialog, 'Anthropic'));
    expect(within(within(dialog).getByRole('row')).getByText('Needs sign-in')).toBeInTheDocument();
  });

  it('checks the current model', () => {
    const dialog = open();
    expect(within(within(dialog).getByRole('row')).getByText('Current model')).toBeInTheDocument();
  });
});

describe('the pill', () => {
  it('drops the brand word the mark already says', () => {
    open({ selectedModel: 'claude-opus' });
    expect(screen.getByTitle('Select model')).toHaveTextContent('Claude Opus 9');
    expect(within(screen.getByTitle('Select model')).getByText('Opus 9')).toBeInTheDocument();
  });

  it('is disabled while the model list loads', () => {
    render(
      <MemoryRouter>
        <ChatInputModelMenu
          selectedModel="gpt-x" metadata={metadata} payers={payers} groups={groups} favorites={[]} recent={[]}
          onToggleFavorite={onToggleFavorite} onSelectModel={onSelectModel} needsSignIn={() => false}
          dropdownDirection="up" container={null} disabled
        />
      </MemoryRouter>,
    );
    expect(screen.getByTitle('Select model')).toBeDisabled();
  });

  it('offers to set a model up when there is nothing to pick', () => {
    render(
      <MemoryRouter>
        <ChatInputModelMenu
          selectedModel={null} payers={{}} groups={[]} favorites={[]} recent={[]}
          onToggleFavorite={onToggleFavorite} onSelectModel={onSelectModel} needsSignIn={() => false}
          dropdownDirection="up" container={null}
        />
      </MemoryRouter>,
    );
    expect(screen.getByRole('button', { name: 'Set up a model' })).toBeInTheDocument();
  });
});

