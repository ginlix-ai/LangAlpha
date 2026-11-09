/**
 * A bot messaging another bot reads the way ds-bot's activity row does:
 * "Messaging Scout · request" while the call runs, then "Messaged Scout" with
 * what became of it. The tool row is the outgoing half of the bot-to-bot
 * display; the received half is `BotMessageRow`.
 */
import { describe, it, expect } from 'vitest';
import i18n from '@/i18n';
import {
  getActiveLabel,
  getCompletedRowTitle,
  getCompletedSummary,
  getDisplayName,
} from '../toolDisplayConfig';

const t = i18n.getFixedT('en-US');

const call = (args: Record<string, unknown>) => ({ args });

describe('a bot messaging another bot, while it runs', () => {
  it('names the target and the kind it asked for', () => {
    expect(getActiveLabel('send_bot_message', call({ to: 'Scout', kind: 'request' }), t))
      .toBe('Messaging Scout · request');
    expect(getActiveLabel('send_bot_message', call({ to: 'Scout', kind: 'reply' }), t))
      .toBe('Messaging Scout · reply');
  });

  it('falls back to the default kind and an unnamed target', () => {
    expect(getActiveLabel('send_bot_message', call({ to: 'Scout' }), t)).toBe('Messaging Scout · message');
    expect(getActiveLabel('send_bot_message', call({ kind: 'request' }), t))
      .toBe('Messaging another bot · request');
    expect(getActiveLabel('send_bot_message', call({ to: 'Scout', kind: 'shout' }), t))
      .toBe('Messaging Scout · message');
  });

  it('cannot name the requester of a report before its answer', () => {
    expect(getActiveLabel('report_to', call({ assignment_id: 'a-1', outcome: 'done' }), t))
      .toBe('Reporting back · report');
  });
});

describe('a settled bot-message call', () => {
  const settled = (tool: string, args: Record<string, unknown>, result: string) => ({
    title: getCompletedRowTitle(tool, call(args), t, { result }),
    summary: getCompletedSummary(tool, call(args), t, null, result),
  });

  it('says it messaged the target, and what became of it', () => {
    const delivered = settled('send_bot_message', { to: 'Scout', kind: 'request' },
      'delivered to Scout (new message); it answers on its own thread');
    expect(delivered.title).toBe('Messaged Scout');
    expect(delivered.summary).toBe('delivered');

    const queued = settled('send_bot_message', { to: 'Scout' },
      'queued for Scout (it is busy); the message is kept and delivered as its next turn');
    expect(queued.title).toBe('Messaged Scout');
    expect(queued.summary).toBe('queued');

    const refused = settled('send_bot_message', { to: 'Scout' },
      "refused: no bot named or identified by 'Scout' in this workspace");
    expect(refused.title).toBe('Messaged Scout');
    expect(refused.summary).toBe('refused');
  });

  it('names the requester its report reached, from the answer', () => {
    const reported = settled('report_to', { assignment_id: 'a-1', outcome: 'done' },
      'reported to Scout (delivered)');
    expect(reported.title).toBe('Reported to Scout');
    expect(reported.summary).toBe('delivered');
    expect(settled('report_to', { assignment_id: 'a-1' }, 'reported to Scout (queued)').summary)
      .toBe('queued');
    // A report whose own notification was refused still closed the assignment:
    // the pill says the message did not go, not that the report failed.
    expect(settled('report_to', { assignment_id: 'a-1' },
      'reported, but the report message was refused: the target is paused').summary)
      .toBe('refused');
  });

  it('maps the other report outcomes the server can state', () => {
    const at = (result: string) => settled('report_to', { assignment_id: 'a-1' }, result).summary;
    expect(at('reported to Scout (failed)')).toBe('failed');
    expect(at('reported to Scout (missing)')).toBe('missing');
    expect(at('reported to Scout (already)')).toBe('already');
    expect(at('reported (no requester to notify)')).toBe('missing');
    expect(at('reported (the requester bot is gone)')).toBe('missing');
    expect(at('refused: that assignment is already done')).toBe('already');
    expect(at('refused: no assignment you were given has that id')).toBe('missing');
  });

  it('says no more than it knows when the answer did not state an outcome', () => {
    expect(settled('send_bot_message', { to: 'Scout' }, '').summary).toBeNull();
    expect(settled('report_to', { assignment_id: 'a-1' }, '').title).toBe('Reported back');
  });
});

describe('the rest of the tool naming', () => {
  it('still names the tools by their display names', () => {
    expect(getDisplayName('send_bot_message', t)).toBe('Message Bot');
    expect(getDisplayName('report_to', t)).toBe('Report Back');
  });

  it('leaves every other tool\'s label as it was', () => {
    expect(getActiveLabel('Bash', call({ command: 'ls' }), t)).toContain('Bash');
    expect(getCompletedRowTitle('Read', call({ file_path: '/workspace/notes.md' }), t))
      .toBe('Read');
  });
});
