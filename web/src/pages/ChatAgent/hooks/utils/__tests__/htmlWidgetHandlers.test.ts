import { describe, it, expect } from 'vitest';
import type { MessageRecord, SetMessages } from '../types';
import { handleHtmlWidget } from '../streamEventHandlers';
import { handleHistoryHtmlWidget } from '../historyEventHandlers';

// ---- helpers ----------------------------------------------------------------

/** Build a minimal assistant message with empty widget state. */
function makeAssistantMessage(
  id: string,
  overrides: Partial<MessageRecord> = {},
): MessageRecord {
  return {
    id,
    role: 'assistant',
    content: '',
    contentSegments: [],
    htmlWidgetProcesses: {},
    ...overrides,
  };
}

/** Build a minimal non-target message to verify it stays untouched. */
function makeOtherMessage(id: string): MessageRecord {
  return {
    id,
    role: 'assistant',
    content: 'other',
    contentSegments: [],
    htmlWidgetProcesses: {},
  };
}

/**
 * Creates a mock setMessages that captures the updater and applies it
 * to the provided initial messages array, returning the result.
 */
function applySetMessages(
  initial: MessageRecord[],
  callHandler: (setMessages: SetMessages) => void,
): MessageRecord[] {
  let result: MessageRecord[] = initial;
  const setMessages: SetMessages = (updater) => {
    result = updater(result);
  };
  callHandler(setMessages);
  return result;
}

/** Minimal StreamRefs shape needed by handleHtmlWidget. */
function makeStreamRefs(counterStart = 0) {
  return {
    contentOrderCounterRef: { current: counterStart },
    currentReasoningIdRef: { current: null },
    currentToolCallIdRef: { current: null },
  };
}

/** Minimal PairState shape needed by handleHistoryHtmlWidget. */
function makePairState(counterStart = 0) {
  return {
    contentOrderCounter: counterStart,
    reasoningId: null,
    toolCallId: null,
  };
}

// ---- handleHtmlWidget (streaming) -------------------------------------------

describe('handleHtmlWidget (streaming)', () => {
  const assistantMessageId = 'ast-1';
  const artifactId = 'w-abc';

  it('returns false when artifactType is not html_widget', () => {
    const refs = makeStreamRefs();
    let called = false;
    const setMessages: SetMessages = () => { called = true; };

    const result = handleHtmlWidget({
      assistantMessageId,
      artifactType: 'code',
      artifactId,
      payload: { html: '<p>hi</p>', title: 'Test' },
      refs,
      setMessages,
    });

    expect(result).toBe(false);
    expect(called).toBe(false);
  });

  it('returns false when payload is null', () => {
    const refs = makeStreamRefs();
    let called = false;
    const setMessages: SetMessages = () => { called = true; };

    const result = handleHtmlWidget({
      assistantMessageId,
      artifactType: 'html_widget',
      artifactId,
      payload: null,
      refs,
      setMessages,
    });

    expect(result).toBe(false);
    expect(called).toBe(false);
  });

  it('creates a content segment with type html_widget and correct widgetId', () => {
    const refs = makeStreamRefs();
    const initial = [makeAssistantMessage(assistantMessageId)];

    const messages = applySetMessages(initial, (setMessages) => {
      handleHtmlWidget({
        assistantMessageId,
        artifactType: 'html_widget',
        artifactId,
        payload: { html: '<p>chart</p>', title: 'Chart' },
        refs,
        setMessages,
      });
    });

    const msg = messages[0];
    const segments = msg.contentSegments as Record<string, unknown>[];
    expect(segments).toHaveLength(1);
    expect(segments[0]).toMatchObject({
      type: 'html_widget',
      widgetId: `widget-${artifactId}`,
    });
  });

  it('populates htmlWidgetProcesses with html and title', () => {
    const refs = makeStreamRefs();
    const initial = [makeAssistantMessage(assistantMessageId)];

    const messages = applySetMessages(initial, (setMessages) => {
      handleHtmlWidget({
        assistantMessageId,
        artifactType: 'html_widget',
        artifactId,
        payload: { html: '<div>Hello</div>', title: 'My Widget' },
        refs,
        setMessages,
      });
    });

    const msg = messages[0];
    const processes = msg.htmlWidgetProcesses as Record<string, { html: string; title: string }>;
    const segmentId = `widget-${artifactId}`;
    expect(processes[segmentId]).toEqual({
      html: '<div>Hello</div>',
      title: 'My Widget',
    });
  });

  it('uses eventId for ordering when provided', () => {
    const refs = makeStreamRefs(10);
    const initial = [makeAssistantMessage(assistantMessageId)];

    const messages = applySetMessages(initial, (setMessages) => {
      handleHtmlWidget({
        assistantMessageId,
        artifactType: 'html_widget',
        artifactId,
        payload: { html: '<p>x</p>', title: 'T' },
        refs,
        setMessages,
        eventId: 42,
      });
    });

    const segments = messages[0].contentSegments as Record<string, unknown>[];
    expect(segments[0].order).toBe(42);
    // Counter should not have been incremented
    expect(refs.contentOrderCounterRef.current).toBe(10);
  });

  it('falls back to contentOrderCounterRef when eventId is null', () => {
    const refs = makeStreamRefs(5);
    const initial = [makeAssistantMessage(assistantMessageId)];

    const messages = applySetMessages(initial, (setMessages) => {
      handleHtmlWidget({
        assistantMessageId,
        artifactType: 'html_widget',
        artifactId,
        payload: { html: '<p>x</p>', title: 'T' },
        refs,
        setMessages,
        eventId: null,
      });
    });

    const segments = messages[0].contentSegments as Record<string, unknown>[];
    expect(segments[0].order).toBe(6); // incremented from 5 to 6
    expect(refs.contentOrderCounterRef.current).toBe(6);
  });

  it('prevents duplicate segments with same widgetId', () => {
    const refs = makeStreamRefs();
    const segmentId = `widget-${artifactId}`;
    const initial = [
      makeAssistantMessage(assistantMessageId, {
        contentSegments: [{ type: 'html_widget', widgetId: segmentId, order: 1 }],
        htmlWidgetProcesses: { [segmentId]: { html: '<p>old</p>', title: 'Old' } },
      }),
    ];

    const messages = applySetMessages(initial, (setMessages) => {
      handleHtmlWidget({
        assistantMessageId,
        artifactType: 'html_widget',
        artifactId,
        payload: { html: '<p>new</p>', title: 'New' },
        refs,
        setMessages,
      });
    });

    const msg = messages[0];
    const segments = msg.contentSegments as Record<string, unknown>[];
    expect(segments).toHaveLength(1);
    // Should be the original message object (identity unchanged)
    expect(msg).toBe(initial[0]);
  });

  it('only updates the matching assistant message (leaves others untouched)', () => {
    const refs = makeStreamRefs();
    const other = makeOtherMessage('other-1');
    const initial = [other, makeAssistantMessage(assistantMessageId)];

    const messages = applySetMessages(initial, (setMessages) => {
      handleHtmlWidget({
        assistantMessageId,
        artifactType: 'html_widget',
        artifactId,
        payload: { html: '<p>hi</p>', title: 'W' },
        refs,
        setMessages,
      });
    });

    // The other message should be the exact same reference
    expect(messages[0]).toBe(other);
    // The target message should have been updated
    const target = messages[1];
    const segments = target.contentSegments as Record<string, unknown>[];
    expect(segments).toHaveLength(1);
    expect(segments[0]).toMatchObject({ type: 'html_widget' });
  });

  it('passes data field through to htmlWidgetProcesses when present in payload', () => {
    const refs = makeStreamRefs();
    const initial = [makeAssistantMessage(assistantMessageId)];

    const messages = applySetMessages(initial, (setMessages) => {
      handleHtmlWidget({
        assistantMessageId,
        artifactType: 'html_widget',
        artifactId,
        payload: { html: '<p>chart</p>', title: 'Chart', data: { 'prices.json': '{"AAPL": 150}' } },
        refs,
        setMessages,
      });
    });

    const msg = messages[0];
    const segmentId = `widget-${artifactId}`;
    const processes = msg.htmlWidgetProcesses as Record<string, { html: string; title: string; data?: Record<string, string> }>;
    expect(processes[segmentId].data).toEqual({ 'prices.json': '{"AAPL": 150}' });
  });
});

// ---- handleHistoryHtmlWidget (history) --------------------------------------

describe('handleHistoryHtmlWidget (history)', () => {
  const assistantMessageId = 'ast-2';
  const artifactId = 'w-xyz';

  it('returns false when artifactType is not html_widget', () => {
    const pairState = makePairState();
    let called = false;
    const setMessages: SetMessages = () => { called = true; };

    const result = handleHistoryHtmlWidget({
      assistantMessageId,
      artifactType: 'file',
      artifactId,
      payload: { html: '<p>hi</p>', title: 'Test' },
      pairState,
      setMessages,
    });

    expect(result).toBe(false);
    expect(called).toBe(false);
  });

  it('returns false when payload is null', () => {
    const pairState = makePairState();
    let called = false;
    const setMessages: SetMessages = () => { called = true; };

    const result = handleHistoryHtmlWidget({
      assistantMessageId,
      artifactType: 'html_widget',
      artifactId,
      payload: null,
      pairState,
      setMessages,
    });

    expect(result).toBe(false);
    expect(called).toBe(false);
  });

  it('creates a content segment with type html_widget and correct widgetId', () => {
    const pairState = makePairState();
    const initial = [makeAssistantMessage(assistantMessageId)];

    const messages = applySetMessages(initial, (setMessages) => {
      handleHistoryHtmlWidget({
        assistantMessageId,
        artifactType: 'html_widget',
        artifactId,
        payload: { html: '<p>chart</p>', title: 'Chart' },
        pairState,
        setMessages,
      });
    });

    const msg = messages[0];
    const segments = msg.contentSegments as Record<string, unknown>[];
    expect(segments).toHaveLength(1);
    expect(segments[0]).toMatchObject({
      type: 'html_widget',
      widgetId: `widget-${artifactId}`,
    });
  });

  it('populates htmlWidgetProcesses with html and title', () => {
    const pairState = makePairState();
    const initial = [makeAssistantMessage(assistantMessageId)];

    const messages = applySetMessages(initial, (setMessages) => {
      handleHistoryHtmlWidget({
        assistantMessageId,
        artifactType: 'html_widget',
        artifactId,
        payload: { html: '<section>Data</section>', title: 'Data Widget' },
        pairState,
        setMessages,
      });
    });

    const msg = messages[0];
    const processes = msg.htmlWidgetProcesses as Record<string, { html: string; title: string }>;
    const segmentId = `widget-${artifactId}`;
    expect(processes[segmentId]).toEqual({
      html: '<section>Data</section>',
      title: 'Data Widget',
    });
  });

  it('uses eventId for ordering when provided', () => {
    const pairState = makePairState(10);
    const initial = [makeAssistantMessage(assistantMessageId)];

    const messages = applySetMessages(initial, (setMessages) => {
      handleHistoryHtmlWidget({
        assistantMessageId,
        artifactType: 'html_widget',
        artifactId,
        payload: { html: '<p>x</p>', title: 'T' },
        pairState,
        setMessages,
        eventId: 99,
      });
    });

    const segments = messages[0].contentSegments as Record<string, unknown>[];
    expect(segments[0].order).toBe(99);
    // Counter should not have been incremented
    expect(pairState.contentOrderCounter).toBe(10);
  });

  it('falls back to pairState.contentOrderCounter when eventId is null', () => {
    const pairState = makePairState(3);
    const initial = [makeAssistantMessage(assistantMessageId)];

    const messages = applySetMessages(initial, (setMessages) => {
      handleHistoryHtmlWidget({
        assistantMessageId,
        artifactType: 'html_widget',
        artifactId,
        payload: { html: '<p>x</p>', title: 'T' },
        pairState,
        setMessages,
        eventId: null,
      });
    });

    const segments = messages[0].contentSegments as Record<string, unknown>[];
    expect(segments[0].order).toBe(4); // incremented from 3 to 4
    expect(pairState.contentOrderCounter).toBe(4);
  });

  it('prevents duplicate segments with same widgetId', () => {
    const pairState = makePairState();
    const segmentId = `widget-${artifactId}`;
    const initial = [
      makeAssistantMessage(assistantMessageId, {
        contentSegments: [{ type: 'html_widget', widgetId: segmentId, order: 1 }],
        htmlWidgetProcesses: { [segmentId]: { html: '<p>old</p>', title: 'Old' } },
      }),
    ];

    const messages = applySetMessages(initial, (setMessages) => {
      handleHistoryHtmlWidget({
        assistantMessageId,
        artifactType: 'html_widget',
        artifactId,
        payload: { html: '<p>new</p>', title: 'New' },
        pairState,
        setMessages,
      });
    });

    const msg = messages[0];
    const segments = msg.contentSegments as Record<string, unknown>[];
    expect(segments).toHaveLength(1);
    // Should be the original message object (identity unchanged)
    expect(msg).toBe(initial[0]);
  });

  it('only updates the matching assistant message (leaves others untouched)', () => {
    const pairState = makePairState();
    const other = makeOtherMessage('other-2');
    const initial = [other, makeAssistantMessage(assistantMessageId)];

    const messages = applySetMessages(initial, (setMessages) => {
      handleHistoryHtmlWidget({
        assistantMessageId,
        artifactType: 'html_widget',
        artifactId,
        payload: { html: '<p>hi</p>', title: 'W' },
        pairState,
        setMessages,
      });
    });

    // The other message should be the exact same reference
    expect(messages[0]).toBe(other);
    // The target message should have been updated
    const target = messages[1];
    const segments = target.contentSegments as Record<string, unknown>[];
    expect(segments).toHaveLength(1);
    expect(segments[0]).toMatchObject({ type: 'html_widget' });
  });

  it('passes data field through to htmlWidgetProcesses when present in payload', () => {
    const pairState = makePairState();
    const initial = [makeAssistantMessage(assistantMessageId)];

    const messages = applySetMessages(initial, (setMessages) => {
      handleHistoryHtmlWidget({
        assistantMessageId,
        artifactType: 'html_widget',
        artifactId,
        payload: { html: '<p>chart</p>', title: 'Chart', data: { 'data.csv': 'a,b\n1,2' } },
        pairState,
        setMessages,
      });
    });

    const msg = messages[0];
    const segmentId = `widget-${artifactId}`;
    const processes = msg.htmlWidgetProcesses as Record<string, { html: string; title: string; data?: Record<string, string> }>;
    expect(processes[segmentId].data).toEqual({ 'data.csv': 'a,b\n1,2' });
  });
});
