/**
 * A widget's prompt calls as every chat host answers them: the data rides as
 * a widget-context card beside the text, a draft lands in the composer, and a
 * new thread keeps the settings of the one it came from. The hosts add only
 * their own contexts and where a new thread opens.
 */
import { describe, expect, it, vi } from 'vitest';
import { renderHook } from '@testing-library/react';
import type { ChatInputHandle } from '@/components/ui/chat-input';
import { useWidgetPromptActions } from '../useWidgetPromptActions';
import { newThreadCarry } from '../newThreadCarry';
import { widgetSendParts, type WidgetSent } from '../../../utils/widgetPrompt';

const SENT: WidgetSent = { data: { scenario: 'bull' }, widgetId: 'widget-call_w1', title: 'Scenarios' };

describe('widgetSendParts', () => {
  it("pairs the card's context with the snapshot the bubble shows", () => {
    const { contexts, options } = widgetSendParts(SENT);
    expect(contexts).toEqual([expect.objectContaining({
      type: 'widget', widget_type: 'chat.show_widget', widget_id: 'widget-call_w1', data: { scenario: 'bull' },
    })]);
    expect(options.widgetSnapshots).toEqual([expect.objectContaining({ widget_id: 'widget-call_w1', label: 'Scenarios' })]);
  });

  it('is nothing for a send no widget made', () => {
    expect(widgetSendParts()).toEqual({ contexts: [], options: {} });
  });

  // The server says the widget sent it and writes no data block.
  it('names the widget of a call that sent no data, with empty data and no text', () => {
    const { contexts, options } = widgetSendParts({ widgetId: 'widget-call_w1', title: 'Scenarios' });
    expect(contexts).toEqual([expect.objectContaining({
      type: 'widget', widget_type: 'chat.show_widget', widget_id: 'widget-call_w1', label: 'Scenarios', data: {}, text: '',
    })]);
    expect(options.widgetSnapshots?.[0].description).toBeUndefined();
  });
});

describe('newThreadCarry', () => {
  it("keeps the composer's picks and this thread's settings, and leaves out what is unset", () => {
    expect(newThreadCarry(
      { model: 'm-box', reasoningEffort: 'high', fastMode: false },
      { allowed: false },
      { reasoningEffort: 'low', fastMode: null },
    )).toEqual({ model: 'm-box', reasoningEffort: 'high', fastMode: false, subagentsAllowed: false, reasoningEffortOverride: 'low' });
    expect(newThreadCarry(undefined, { allowed: null }, { reasoningEffort: undefined, fastMode: true }))
      .toEqual({ fastModeOverride: true });
  });
});

function host() {
  const composer = { addWidgetSnapshot: vi.fn(), addContext: vi.fn() };
  const chatInputRef = { current: composer as unknown as ChatInputHandle };
  const send = vi.fn();
  const openThread = vi.fn();
  const carry = { model: 'm-box' };
  const hook = renderHook(
    ({ opens }: { opens: boolean }) => useWidgetPromptActions({
      chatInputRef, send: (...a) => send(...a), openThread: opens ? (...a) => openThread(...a) : undefined, readCarry: () => carry,
    }),
    { initialProps: { opens: true } },
  );
  return { ...hook, composer, send, openThread, carry };
}

describe('useWidgetPromptActions', () => {
  it('sends the text with the card, through the host', () => {
    const { result, send } = host();
    result.current.sendPrompt!('Model the bull case', SENT);
    const [text, parts] = send.mock.calls[0];
    expect(text).toBe('Model the bull case');
    expect(parts.contexts).toEqual([expect.objectContaining({ type: 'widget', widget_id: 'widget-call_w1' })]);
    expect(parts.options.widgetSnapshots).toHaveLength(1);
  });

  it('drafts into the box: the text after what it holds, and a card for its data, sending nothing', () => {
    const { result, composer, send } = host();
    result.current.draftPrompt!('Model the bull case', SENT);
    expect(composer.addContext).toHaveBeenCalledWith({ kind: 'attachment', files: [], text: 'Model the bull case' });
    expect(composer.addWidgetSnapshot).toHaveBeenCalledWith(expect.objectContaining({
      widget_type: 'chat.show_widget', label: 'Scenarios', data: { scenario: 'bull' },
    }));
    expect(send).not.toHaveBeenCalled();

    // The same data again is the card already there; other data is a second card.
    result.current.draftPrompt!('again', SENT);
    result.current.draftPrompt!('bear', { ...SENT, data: { scenario: 'bear' } });
    const ids = composer.addWidgetSnapshot.mock.calls.map(([s]) => s.widget_id);
    expect(ids[0]).toBe(ids[1]);
    expect(ids[2]).not.toBe(ids[0]);
  });

  it('drafts a call with no data with a card for its widget', () => {
    const { result, composer } = host();
    result.current.draftPrompt!('note', { widgetId: 'widget-call_w1', title: 'Scenarios' });
    expect(composer.addWidgetSnapshot).toHaveBeenCalledWith(expect.objectContaining({
      widget_id: 'widget-call_w1:none', label: 'Scenarios', data: {},
    }));
  });

  it('opens a new thread with the card and the carry read at the click', () => {
    const { result, openThread, carry } = host();
    result.current.startThread!('Explore', SENT);
    const [text, parts, carried] = openThread.mock.calls[0];
    expect(text).toBe('Explore');
    expect(parts.options.widgetSnapshots).toEqual([expect.objectContaining({ widget_id: 'widget-call_w1' })]);
    expect(carried).toBe(carry);
  });

  it('keeps its identity across renders, and answers no startThread where the host opens none', () => {
    const { result, rerender } = host();
    const first = result.current;
    rerender({ opens: true });
    expect(result.current).toBe(first);
    rerender({ opens: false });
    expect(result.current.startThread).toBeUndefined();
    expect(result.current.sendPrompt).toBe(first.sendPrompt);
  });
});
