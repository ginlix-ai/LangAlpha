// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { threadByline, threadStanding } from '../sharedThreads';

const MAYA = { user_id: 'u-maya', name: 'Maya Okafor' };
const LEE = { user_id: 'u-lee', name: 'Lee Chen' };
const thread = (over: { shared_with_workspace?: boolean; others_sent?: boolean; last_sender?: typeof LEE } = {}) => ({
  created_by: MAYA, shared_with_workspace: true, others_sent: false, ...over,
});

describe('threadByline', () => {
  it('names the starter, and the last sender when someone else wrote last', () => {
    expect(threadByline(thread({ last_sender: LEE }), 'u-lee')).toEqual({
      starter: { name: 'Maya Okafor', mine: false },
      last: { name: 'Lee Chen', mine: true },
    });
    expect(threadByline(thread({ last_sender: MAYA }), 'u-lee')).toEqual({
      starter: { name: 'Maya Okafor', mine: false },
      last: null,
    });
  });

  it('says nothing for a personal thread', () => {
    expect(threadByline(undefined, 'u-lee')).toEqual({ starter: null, last: null });
  });
});

describe('threadStanding', () => {
  it('offers delete to the starter and to managers only', () => {
    expect(threadStanding(thread(), 'u-maya', false).canDelete).toBe(true);
    expect(threadStanding(thread(), 'u-lee', false).canDelete).toBe(false);
    expect(threadStanding(thread(), 'u-lee', true).canDelete).toBe(true);
  });

  it('lets only the starter change who sees it, and not make it private once someone else sent', () => {
    expect(threadStanding(thread(), 'u-maya', false)).toMatchObject({ mine: true, sharedWithWorkspace: true, canChangePrivacy: true });
    expect(threadStanding(thread({ others_sent: true }), 'u-maya', false).canChangePrivacy).toBe(false);
    expect(threadStanding(thread({ shared_with_workspace: false, others_sent: true }), 'u-maya', false).canChangePrivacy).toBe(true);
    expect(threadStanding(thread(), 'u-lee', true)).toMatchObject({ mine: false, canChangePrivacy: false });
    expect(threadStanding(thread(), null, false).mine).toBe(false);
  });

  it('reads a thread outside a shared workspace as the viewer\'s own and private', () => {
    expect(threadStanding(undefined, undefined, false)).toEqual({
      sharedWithWorkspace: false, mine: true, canDelete: true, canChangePrivacy: false,
    });
  });
});

describe('parseEmails', () => {
  it('reads addresses however they were separated, each once', async () => {
    const { parseEmails } = await import('../emailList');
    expect(parseEmails('lee@n.test, sam@n.test\nlee@n.test;  ana@n.test ')).toEqual(['lee@n.test', 'sam@n.test', 'ana@n.test']);
    expect(parseEmails('  ')).toEqual([]);
  });
});
