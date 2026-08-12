/**
 * Slash-command menu behavior:
 *  1. Matching is on command name + aliases ONLY (not description), applied
 *     uniformly to skills and builtins — so a command never surfaces just because
 *     the query appears in its description text.
 *  2. Ranking: prefix (char-by-char from the start) matches rank ABOVE
 *     substring-only matches; within a tier, system/service commands
 *     (/compact, /offload, /subagent) rank above skills.
 *  3. Action commands are deferred to send — selecting one inserts a pill and
 *     does NOT fire onAction; the dispatch happens when the user sends.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, fireEvent, screen, waitFor, act } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import ChatInput from '../chat-input';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getSkills: vi.fn().mockResolvedValue([
    { name: 'Comps Analysis', command: 'comps-analysis', description: 'Comparable company analysis' },
    { name: 'DCF Model', command: 'dcf-model', description: 'DCF valuation' },
  ]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
}));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ data: undefined, isLoading: false }),
}));

vi.mock('@/lib/modelCapabilities', () => ({
  supportsXhighEffort: () => false,
}));

vi.mock('../use-toast', () => ({
  useToast: () => ({ toast: vi.fn() }),
}));

function renderInput(props: { onSend?: ReturnType<typeof vi.fn>; onAction?: ReturnType<typeof vi.fn> } = {}) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const onSend = props.onSend ?? vi.fn();
  const onAction = props.onAction ?? vi.fn();
  const utils = render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <ChatInput onSend={onSend} onAction={onAction} />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  return { ...utils, onSend, onAction };
}

function typeSlash(value: string) {
  const textarea = document.querySelector('textarea')!;
  fireEvent.change(textarea, { target: { value, selectionStart: value.length } });
  return textarea;
}

function menuNames(): string[] {
  return Array.from(
    document.querySelectorAll('.mention-autocomplete-item .slash-cmd-name'),
  ).map((e) => e.textContent || '');
}

describe('ChatInput — slash command menu', () => {
  beforeEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
    // jsdom doesn't implement scrollIntoView; the slash menu's active-item
    // scroll effect calls it.
    Element.prototype.scrollIntoView = vi.fn();
  });
  afterEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
  });

  it('fuzzy-matches by name and ranks system commands above skills (excludes description-only matches)', async () => {
    renderInput();
    // '/comp' matches the /compact builtin and the comps-analysis skill by NAME.
    // /subagent must NOT appear: it contains "comp" only in its description
    // ("Use subagents to complete this task"), and matching is name/alias-only.
    typeSlash('/comp');

    // Wait for the async skills load so the skill match is in the menu too.
    await waitFor(() => expect(menuNames()).toContain('/comps-analysis'));

    const names = menuNames();
    expect(names).not.toContain('/subagent');
    expect(names[0]).toBe('/compact');
    expect(names.indexOf('/compact')).toBeLessThan(names.indexOf('/comps-analysis'));
  });

  it('ranks prefix (char-by-char) matches above substring matches, even system commands', async () => {
    renderInput();
    // '/d': only /dcf-model STARTS with "d". /offload is a system command but
    // matches "d" only mid-word ("offloa-d-"), so the prefix skill must win.
    // /compact and /subagent have no "d" at all and are excluded entirely.
    typeSlash('/d');

    await waitFor(() => expect(menuNames()).toContain('/dcf-model'));

    const names = menuNames();
    expect(names).not.toContain('/compact');
    expect(names).not.toContain('/subagent');
    expect(names[0]).toBe('/dcf-model');
    expect(names.indexOf('/dcf-model')).toBeLessThan(names.indexOf('/offload'));
  });

  it('defers /compact to send: selecting adds a pill (no onAction), sending dispatches it', async () => {
    const { onSend, onAction } = renderInput();
    typeSlash('/comp');

    await waitFor(() => {
      expect(menuNames()).toContain('/compact');
    });

    const compactItem = Array.from(
      document.querySelectorAll('.mention-autocomplete-item'),
    ).find((el) => el.querySelector('.slash-cmd-name')?.textContent === '/compact')!;
    fireEvent.mouseDown(compactItem);

    // Selection must NOT fire the action — it only stages a pill.
    expect(onAction).not.toHaveBeenCalled();
    await waitFor(() => expect(document.querySelector('textarea')!.value).toContain('/compact'));

    // Sending dispatches the action and does NOT start a chat turn.
    fireEvent.click(screen.getByLabelText('Send message'));
    expect(onAction).toHaveBeenCalledTimes(1);
    expect(onAction.mock.calls[0][0].name).toBe('compact');
    expect(onAction.mock.calls[0][0].type).toBe('action');
    expect(onSend).not.toHaveBeenCalled();
  });

  it('clears the widget-context deck when an action command is sent', async () => {
    // Sending /compact must not leave the user's staged draft context behind:
    // the widget deck (and other draft state) clears, mirroring a normal send.
    const { onAction } = renderInput();

    const snapshot = {
      widget_type: 'markets.chart',
      widget_id: 'w1',
      label: 'NVDA · 1d',
      captured_at: '2026-01-01T00:00:00Z',
      text: '<widget-context></widget-context>',
      data: {},
    };

    const events: string[] = [];
    const off = ContextBus.subscribe((e) => events.push(e.type));

    await act(async () => {
      ContextBus.attach(snapshot as never);
    });

    // Stage a /compact action pill alongside the staged widget context.
    typeSlash('/comp');
    await waitFor(() => expect(menuNames()).toContain('/compact'));
    const compactItem = Array.from(
      document.querySelectorAll('.mention-autocomplete-item'),
    ).find((el) => el.querySelector('.slash-cmd-name')?.textContent === '/compact')!;
    fireEvent.mouseDown(compactItem);
    await waitFor(() =>
      expect(document.querySelector('textarea')!.value).toContain('/compact'),
    );

    // Sending the action fires it AND clears the staged widget deck.
    fireEvent.click(screen.getByLabelText('Send message'));
    expect(onAction).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(events).toContain('clear'));

    off();
  });
});
