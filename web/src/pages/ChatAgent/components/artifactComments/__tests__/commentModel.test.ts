// @vitest-environment node
import type { TFunction } from 'i18next';
import { describe, expect, it } from 'vitest';
import type { ArtifactCommentMessage } from '@/types/api';
import { flattenMarkdown } from '@/lib/plainLine';
import {
  cardActions, describeComment, draftHeading, filterCounts, filterShowing, firstWords, inFilter, isWorking,
  laterMessages, newestUnread, oneLine, pinState, reachedAgent, showsDraft,
} from '../commentModel';

function message(author: ArtifactCommentMessage['author'], body: string, seq = 1): ArtifactCommentMessage {
  return { message_id: `${author}-${body}`, seq, author, body, sent_at: null, request_key: null, thread_id: null, created_at: '2026-01-01T00:00:00Z' };
}

describe('pinState', () => {
  it('reads resolved over any delivery', () => {
    expect(pinState({ status: 'resolved', delivery: 'sent' })).toBe('resolved');
    expect(pinState({ status: 'open', delivery: 'queued' })).toBe('queued');
    expect(pinState({ status: 'open', delivery: 'note' })).toBe('note');
  });
});

describe('describeComment', () => {
  const idle = { sending: false, pending: false, refusal: null };
  const sending = { ...idle, sending: true };
  const note = { status: 'open' as const, delivery: 'note' as const, messages: [message('user', 'fix it')] };

  it('reads a send in flight as sent, on the pin and the card alike', () => {
    expect(describeComment(note, sending, true)).toEqual({
      state: 'sent', working: true, reached: true, canReply: true, pending: false, refusal: null,
    });
    expect(describeComment(note, idle, true)).toEqual({
      state: 'note', working: false, reached: false, canReply: false, pending: false, refusal: null,
    });
  });

  it('keeps a resolved comment resolved and idle whatever is sending', () => {
    const look = describeComment({ ...note, status: 'resolved' }, sending, true);
    expect(look.state).toBe('resolved');
    expect(look.working).toBe(false);
  });

  it('carries the queued send\'s progress and refusal through', () => {
    const queued = { ...note, delivery: 'queued' as const };
    expect(describeComment(queued, { ...idle, pending: true }, false).pending).toBe(true);
    expect(describeComment(queued, { ...idle, refusal: 'busy' }, false)).toMatchObject({ state: 'queued', refusal: 'busy' });
  });
});

describe('isWorking', () => {
  const sent = { status: 'open' as const, delivery: 'sent' as const, messages: [message('user', 'fix it')] };

  it('is on while a turn runs and the owner spoke last', () => {
    expect(isWorking(sent, true)).toBe(true);
    expect(isWorking(sent, false)).toBe(false);
    expect(isWorking({ ...sent, messages: [...sent.messages, message('agent', 'done')] }, true)).toBe(false);
  });

  it('is never on for a note or a resolved comment', () => {
    expect(isWorking({ ...sent, delivery: 'note' }, true)).toBe(false);
    expect(isWorking({ ...sent, status: 'resolved' }, true)).toBe(false);
  });
});

describe('firstWords and oneLine', () => {
  it('reads the owner, not the agent', () => {
    expect(firstWords({ messages: [message('agent', 'hello'), message('user', 'check EPS')] })).toBe('check EPS');
    expect(firstWords({ messages: [] })).toBe('');
  });

  it('squashes whitespace and cuts with an ellipsis', () => {
    expect(oneLine('  Revenue\n by   segment ', 80)).toBe('Revenue by segment');
    expect(oneLine('Revenue by segment', 10)).toBe('Revenue b…');
    expect(oneLine(undefined, 10)).toBe('');
  });
});

describe('the list filter', () => {
  const open = { status: 'open' as const, unread: 0 };
  const resolved = { status: 'resolved' as const, unread: 0 };
  const answered = { status: 'resolved' as const, unread: 1 };
  const comments = [open, resolved, { ...open, unread: 2 }, answered];

  it('counts what each filter shows', () => {
    expect(filterCounts(comments)).toEqual({ open: 3, unread: 2, resolved: 2, all: 4 });
    expect(filterCounts([])).toEqual({ open: 0, unread: 0, resolved: 0, all: 0 });
  });

  it('shows all under All, else the status asked for, and Open keeps a resolved comment until its reply is read', () => {
    expect(comments.filter((c) => inFilter(c, 'all'))).toHaveLength(4);
    expect(inFilter(resolved, 'open')).toBe(false);
    expect(inFilter(answered, 'open')).toBe(true);
    expect(inFilter(resolved, 'resolved')).toBe(true);
    expect(inFilter(answered, 'unread')).toBe(true);
    expect(inFilter(open, 'unread')).toBe(false);
  });

  it('keeps All when a comment is opened from elsewhere, and moves to its status otherwise', () => {
    expect(filterShowing(resolved, 'all')).toBe('all');
    expect(filterShowing(resolved, 'open')).toBe('resolved');
    expect(filterShowing(answered, 'open')).toBe('open');
    expect(filterShowing(open, 'unread')).toBe('open');
  });

  it('shows a comment being written under Open and All only', () => {
    expect(['open', 'unread', 'resolved', 'all'].filter((f) => showsDraft(f as Parameters<typeof showsDraft>[0]))).toEqual(['open', 'all']);
  });
});

describe('newestUnread', () => {
  it('is the agent\'s newest message past the read cursor, never the owner\'s', () => {
    const messages = [message('user', 'why', 1), message('agent', 'first', 2), message('agent', 'second', 3), message('user', 'ok', 4)];
    expect(newestUnread({ messages, read_through: 1 })?.body).toBe('second');
    expect(newestUnread({ messages, read_through: 3 })).toBeNull();
    expect(newestUnread({ messages: [], read_through: 0 })).toBeNull();
  });
});

// A shut card previews its newest reply through the shared one-line reading.
describe('a reply preview', () => {
  it('keeps the words markdown shows and drops its marks', () => {
    expect(flattenMarkdown('**Fiscal.** See [the filing](https://example.com) and `Q3`.')).toBe('Fiscal. See the filing and Q3.');
    expect(flattenMarkdown('## Margin\n- *lower* mix\n> quoted')).toBe('Margin lower mix quoted');
  });

  it('leaves underscores in names and a star between spaces', () => {
    expect(flattenMarkdown('read_through is 2 * 3')).toBe('read_through is 2 * 3');
  });
});

describe('draftHeading', () => {
  const t = ((key: string, values?: Record<string, unknown>) => (values ? `${key} ${JSON.stringify(values)}` : key)) as unknown as TFunction;

  it('names words by their place and quotes them, and an element by its own name alone', () => {
    expect(draftHeading(t, { kind: 'text', quote: 'Q3 revenue', label: 'Revenue' })).toEqual({
      label: 'pageViewer.comments.onLabel {"label":"Revenue"}',
      quote: 'chat.artifactComments.message.quote {"quote":"Q3 revenue"}',
    });
    expect(draftHeading(t, { kind: 'text' })).toEqual({ label: 'pageViewer.comments.onText', quote: null });
    expect(draftHeading(t, { kind: 'element', path: '#a', quote: 'Total' })).toEqual({ label: 'pageViewer.comments.onElement', quote: null });
  });

  it('names an area by its size and lists its parts under it', () => {
    const area = { kind: 'area' as const, elements: [{ path: '#a' }, { path: '#b' }], count: 2, label: 'Chart, Table' };
    expect(draftHeading(t, area)).toEqual({ label: 'pageViewer.comments.areaTitle {"count":2}', quote: 'Chart, Table' });
  });
});

describe('cardActions', () => {
  const both = { canSend: true, canQueue: true };

  it('offers the moves that fit each state, Delete always last', () => {
    expect(cardActions('queued', both)).toEqual(['keepNote', 'delete']);
    expect(cardActions('note', both)).toEqual(['sendNow', 'addToQueue', 'delete']);
    expect(cardActions('resolved', both)).toEqual(['reopen', 'delete']);
    expect(cardActions('sent', both)).toEqual(['delete']);
  });

  it('offers a note no queue where no thread would take it, and no send while its destination is unknown', () => {
    expect(cardActions('note', { canSend: true, canQueue: false })).toEqual(['sendNow', 'delete']);
    expect(cardActions('note', { canSend: false, canQueue: false })).toEqual(['delete']);
  });

  it('leaves Send now out of a queued card\'s menu, which has it on its face', () => {
    expect(cardActions('queued', { canSend: true, canQueue: false })).toEqual(['keepNote', 'delete']);
  });
});

describe('reachedAgent and laterMessages', () => {
  const sentAt = (body: string) => ({ ...message('user', body), sent_at: '2026-01-01T00:01:00Z' });

  it('marks a comment the agent has had, even once a follow-up waits again', () => {
    expect(reachedAgent({ delivery: 'sent', messages: [message('user', 'a')] })).toBe(true);
    expect(reachedAgent({ delivery: 'queued', messages: [sentAt('a'), message('user', 'b')] })).toBe(true);
    expect(reachedAgent({ delivery: 'note', messages: [message('user', 'a')] })).toBe(false);
  });

  it("leaves out the owner's first words, which the card shows as the comment", () => {
    const messages = [message('user', 'why?'), message('agent', 'because'), message('user', 'thanks')];
    expect(laterMessages({ messages }).map((m) => m.body)).toEqual(['because', 'thanks']);
  });
});
