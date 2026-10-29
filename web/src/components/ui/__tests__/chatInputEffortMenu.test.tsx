/**
 * The effort and speed options: what they show, and what a pick writes.
 *
 * The load-bearing rule is that picking the level marked "Default", the one
 * Settings gives, clears the thread's own instead of pinning today's default as
 * a permanent choice: that is the only way back to following Settings.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { ChatInputEffortMenu } from '../chat-input.modelMenu';

vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => false }));

const onReasoningEffortChange = vi.fn();
const onFastModeChange = vi.fn();

/** Renders the pill and opens its menu, as every case below reads the menu. */
function renderMenu(overrides: Record<string, unknown> = {}) {
  const props = {
    effortLabel: 'High',
    reasoningEffort: 'high',
    defaultEffort: 'medium',
    onReasoningEffortChange,
    fastMode: false,
    onFastModeChange,
    isCodexModel: false,
    reasoningEfforts: ['none', 'low', 'medium', 'high'],
    dropdownDirection: 'up' as const,
    container: null,
    ...overrides,
  };
  const view = render(
    <MemoryRouter>
      <ChatInputEffortMenu {...(props as React.ComponentProps<typeof ChatInputEffortMenu>)} />
    </MemoryRouter>,
  );
  const pill = screen.queryByTitle('Effort');
  if (pill && !(pill as HTMLButtonElement).disabled) fireEvent.click(pill);
  return view;
}

/** The pill shows the same word as an option, so look among the options. */
function option(label: string) {
  const found = screen.getAllByRole('menuitemradio').find((el) => within(el).queryByText(label));
  if (!found) throw new Error(`no option labelled ${label}`);
  return found;
}

beforeEach(() => {
  onReasoningEffortChange.mockClear();
  onFastModeChange.mockClear();
});

describe('effort', () => {
  it('shows the level it is handed on the pill and checks it', () => {
    renderMenu();
    expect(within(screen.getByTitle('Effort')).getByText('High')).toBeInTheDocument();
    expect(option('High')).toHaveAttribute('aria-checked', 'true');
  });

  it('checks nothing while the level is not known', () => {
    renderMenu({ reasoningEffort: null });
    expect(screen.getAllByRole('menuitemradio').every((el) => el.getAttribute('aria-checked') === 'false')).toBe(true);
  });

  it('badges the Settings level and nothing else', () => {
    renderMenu();
    expect(within(option('Medium')).getByText('Default')).toBeInTheDocument();
    expect(screen.getAllByText('Default')).toHaveLength(1);
  });

  it('clears the override when the default level is picked', () => {
    renderMenu();
    fireEvent.click(option('Medium'));
    expect(onReasoningEffortChange).toHaveBeenCalledWith(null);
  });

  // A thread pinned to the level Settings has since made its default.
  it('clears the override when the default level is picked while already checked', () => {
    renderMenu({ reasoningEffort: 'medium' });
    fireEvent.click(option('Medium'));
    expect(onReasoningEffortChange).toHaveBeenCalledWith(null);
  });

  it('writes the level when any other one is picked', () => {
    renderMenu();
    fireEvent.click(option('Low'));
    expect(onReasoningEffortChange).toHaveBeenCalledWith('low');
  });

  it('renders words, never a bare level name', () => {
    renderMenu({ reasoningEfforts: ['none', 'xhigh', 'max'], reasoningEffort: 'xhigh' });
    expect(screen.getAllByRole('menuitemradio').map((el) => el.textContent))
      .toEqual(['Off', 'Extra High', 'Max']);
    expect(screen.queryByText('xhigh')).not.toBeInTheDocument();
  });

  it('renders no pill for a model with no ladder and no speed', () => {
    renderMenu({ reasoningEfforts: [] });
    expect(screen.queryByTitle('Effort')).not.toBeInTheDocument();
  });

  it('keeps the pill for a Codex model with no ladder, for its speed', () => {
    renderMenu({ reasoningEfforts: [], isCodexModel: true, effortLabel: null });
    expect(screen.getByTitle('Effort')).toBeInTheDocument();
    expect(screen.getAllByRole('menuitemradio').map((el) => el.textContent)).toEqual(['Standard', 'Fast2× usage']);
  });

  it('is disabled while the model list loads', () => {
    renderMenu({ disabled: true });
    expect(screen.getByTitle('Effort')).toBeDisabled();
  });

  // A model with nothing to tune takes the pill away mid-open; the next one
  // with levels must not bring the menu back with it.
  it('comes back shut after a model with nothing to tune', () => {
    const view = renderMenu();
    expect(screen.getByRole('menu')).toBeInTheDocument();
    const props = {
      effortLabel: 'High', reasoningEffort: 'high', defaultEffort: 'medium', onReasoningEffortChange,
      fastMode: false, onFastModeChange, isCodexModel: false, dropdownDirection: 'up' as const, container: null,
    };
    view.rerender(<MemoryRouter><ChatInputEffortMenu {...props} reasoningEfforts={[]} /></MemoryRouter>);
    view.rerender(<MemoryRouter><ChatInputEffortMenu {...props} reasoningEfforts={['low', 'high']} /></MemoryRouter>);
    expect(screen.getByTitle('Effort')).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByRole('menu')).toBeNull();
  });

  it('stays open after a pick, since an effort is a setting and not a send', () => {
    renderMenu();
    fireEvent.click(option('Low'));
    expect(screen.getByRole('menu')).toBeInTheDocument();
  });
});

describe('speed row', () => {
  it('is absent on a non-Codex model', () => {
    renderMenu();
    expect(screen.queryByText('Speed')).not.toBeInTheDocument();
  });

  it('picks fast', () => {
    renderMenu({ isCodexModel: true });
    fireEvent.click(option('Fast'));
    expect(onFastModeChange).toHaveBeenCalledWith(true);
  });

  it('picks standard', () => {
    renderMenu({ isCodexModel: true, fastMode: true });
    fireEvent.click(option('Standard'));
    expect(onFastModeChange).toHaveBeenCalledWith(false);
  });
});

describe('task cost', () => {
  const taskCost = {
    none: { value: 0.1, measured: true },
    low: { value: 0.2, measured: false },
    medium: { value: 0.4, measured: true },
    high: { value: 0.72, measured: true },
  };

  it('shows each level as a multiple of the default level, an unmeasured one as a bound', () => {
    renderMenu({ taskCost });
    expect(within(option('Medium')).getByText('1×')).toBeInTheDocument();
    expect(within(option('Off')).getByText('0.3×')).toBeInTheDocument();
    expect(within(option('Low')).getByText('≤0.5×')).toBeInTheDocument();
    expect(within(option('High')).getByText('1.8×')).toBeInTheDocument();
    expect(screen.getByText('Cost per task')).toBeInTheDocument();
  });

  // The tooltip is for a pointer; the arrow keys reach only the rows, so the
  // menu itself says what the figures mean.
  it('describes the figures to assistive tech, from outside the tab order', () => {
    renderMenu({ taskCost });
    expect(screen.getByRole('menu')).toHaveAccessibleDescription(
      "A rough estimate of how a typical task's cost changes with effort, compared with the default level.",
    );
    expect(screen.getByText('Cost per task')).not.toHaveAttribute('tabindex');
  });

  it('shows no figures when the default level has no measured cost', () => {
    renderMenu({ taskCost: { ...taskCost, medium: { value: 0.4, measured: false } } });
    expect(screen.queryByText(/×/)).toBeNull();
    expect(screen.queryByText('Cost per task')).toBeNull();
  });
});


