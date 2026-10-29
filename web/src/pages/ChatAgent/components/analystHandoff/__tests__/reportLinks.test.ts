// @vitest-environment node
/**
 * Which report answers which hand-off. A thread handed work twice reports
 * twice, so the thread alone cannot say; the run each side names can, and the
 * order on screen settles what predates run ids.
 */
import { describe, it, expect } from 'vitest';
import { placeHandoffs, type HandoffPlace } from '../reportLinks';

type Loose = Record<string, unknown>;

const AT = new Date('2026-10-21T10:00:00Z');

/** A reply holding the given approved hand-offs, as approval proposals. */
function reply(id: string, ...handoffs: Loose[]): { message: Loose } {
  return {
    message: {
      id,
      role: 'assistant',
      timestamp: AT,
      contentSegments: handoffs.map((_, i) => ({ type: 'ptc_agent', proposalId: `${id}-p${i}`, order: i })),
      ptcAgentProposals: Object.fromEntries(handoffs.map((h, i) => [`${id}-p${i}`, { status: 'approved', question: 'Q', ...h }])),
    },
  };
}

/** A report row; `runIds` are the runs it names, the reported run last. */
function report(id: string, threadId: string, ...runIds: string[]): { message: Loose } {
  return {
    message: {
      id,
      role: 'user',
      content: '',
      provenance: { kind: 'analyst_report', threadId, workspaceId: null, workspaceName: null, ...(runIds.length ? { runIds } : {}) },
    },
  };
}

/** A report that lists the hand-off steers its runs took in, possibly none.
 *  One without the list (`report`) is a report whose steers are unknown: from
 *  before the list was kept, or one the server could not read it for. */
function steeredReport(id: string, threadId: string, runIds: string[], steerIds: string[]): { message: Loose } {
  const row = report(id, threadId, ...runIds);
  (row.message.provenance as Loose).steerIds = steerIds;
  return row;
}

const placesOf = (messages: Parameters<typeof placeHandoffs>[0]) => new Map(placeHandoffs(messages));
const reportOf = (places: ReadonlyMap<string, HandoffPlace>, key: string) => places.get(key)?.reportId;

describe('placeHandoffs', () => {
  it('matches each hand-off to the report naming its run, whatever order they report in', () => {
    const places = placesOf([
      reply('m1', { thread_id: 'th', run_id: 'run-1', tool_call_id: 'tc-1' }),
      reply('m2', { thread_id: 'th', run_id: 'run-2', tool_call_id: 'tc-2' }),
      report('r2', 'th', 'run-2'),
      report('r1', 'th', 'run-1'),
    ]);
    expect(reportOf(places, 'tc-1')).toBe('r1');
    expect(reportOf(places, 'tc-2')).toBe('r2');
  });

  it('gives hand-offs steered into one run that run\'s one report', () => {
    const places = placesOf([
      reply('m1', { thread_id: 'th', run_id: 'run-1', tool_call_id: 'tc-1' }),
      reply('m2', { thread_id: 'th', run_id: 'run-1', tool_call_id: 'tc-2' }),
      report('r1', 'th', 'run-1'),
    ]);
    expect(reportOf(places, 'tc-1')).toBe('r1');
    expect(reportOf(places, 'tc-2')).toBe('r1');
  });

  it('matches a hand-off to the report on the run that continued its own past an approval', () => {
    const places = placesOf([
      reply('m1', { thread_id: 'th', run_id: 'run-1', tool_call_id: 'tc-1' }),
      reply('m2', { thread_id: 'th', run_id: 'run-2', tool_call_id: 'tc-2' }),
      report('r1', 'th', 'run-1', 'run-1b'),
      report('r2', 'th', 'run-2'),
    ]);
    expect(reportOf(places, 'tc-1')).toBe('r1');
    expect(reportOf(places, 'tc-2')).toBe('r2');
  });

  it('takes the first report after it that no other hand-off has, when a side names no run', () => {
    const places = placesOf([
      reply('m1', { thread_id: 'th', interruptId: 'i-1' }),
      reply('m2', { thread_id: 'th', run_id: 'run-2', tool_call_id: 'tc-2' }),
      report('r1', 'th'),
      report('r2', 'th', 'run-2'),
    ]);
    // r2 names tc-2's run, so the older hand-off takes the report left over.
    expect(reportOf(places, 'tc-2')).toBe('r2');
    expect(reportOf(places, 'i-1')).toBe('r1');
  });

  it('carries the run its report is on, the last it names, and how that run ended', () => {
    const r1 = report('r1', 'th', 'run-1', 'run-1b');
    (r1.message.provenance as Loose).outcome = 'error';
    const places = placesOf([reply('m1', { thread_id: 'th', run_id: 'run-1', tool_call_id: 'tc-1' }), r1]);
    expect(places.get('tc-1')).toEqual({
      madeAt: AT.getTime(), reportId: 'r1', reportRunId: 'run-1b', outcome: 'error', reportFollows: true,
    });
  });

  it('says a report follows its hand-off straight on through replies and other reports', () => {
    const places = placesOf([
      reply('m1', { thread_id: 'th-1', run_id: 'run-1', tool_call_id: 'tc-1' }, { thread_id: 'th-2', run_id: 'run-2', tool_call_id: 'tc-2' }),
      reply('m1b'),
      report('r1', 'th-1', 'run-1'),
      reply('m2'),
      report('r2', 'th-2', 'run-2'),
    ]);
    expect(places.get('tc-1')?.reportFollows).toBe(true);
    expect(places.get('tc-2')?.reportFollows).toBe(true);
  });

  it('says the conversation moved on when a message of the user\'s comes between', () => {
    const user = { message: { id: 'u1', role: 'user', content: 'And the margins?' } };
    const places = placesOf([
      reply('m1', { thread_id: 'th-1', run_id: 'run-1', tool_call_id: 'tc-1' }, { thread_id: 'th-2', run_id: 'run-2', tool_call_id: 'tc-2' }),
      report('r1', 'th-1', 'run-1'),
      reply('m2'),
      user,
      reply('m3'),
      report('r2', 'th-2', 'run-2'),
    ]);
    expect(places.get('tc-1')?.reportFollows).toBe(true);
    expect(places.get('tc-2')?.reportFollows).toBe(false);
  });

  it('takes no report that came before it', () => {
    const places = placesOf([
      report('r0', 'th'),
      reply('m1', { thread_id: 'th', run_id: 'run-1', tool_call_id: 'tc-1' }),
    ]);
    expect(reportOf(places, 'tc-1')).toBeUndefined();
  });

  it('takes the report a newer run took over, which names only that run', () => {
    const places = placesOf([
      reply('m1', { thread_id: 'th', run_id: 'run-1', tool_call_id: 'tc-1' }),
      reply('m2', { thread_id: 'th', run_id: 'run-2', tool_call_id: 'tc-2' }),
      report('r2', 'th', 'run-2'),
      report('r3', 'th', 'run-3'),
    ]);
    expect(reportOf(places, 'tc-2')).toBe('r2');
    expect(reportOf(places, 'tc-1')).toBe('r3');
  });

  it('leaves a report naming another hand-off\'s run to that hand-off', () => {
    const places = placesOf([
      reply('m1', { thread_id: 'th', run_id: 'run-1', tool_call_id: 'tc-1' }),
      report('r2', 'th', 'run-2'),
      reply('m2', { thread_id: 'th', run_id: 'run-2', tool_call_id: 'tc-2' }),
    ]);
    expect(reportOf(places, 'tc-1')).toBeUndefined();
  });

  it('leaves out a hand-off whose dispatch definitely failed, but not one of unknown outcome', () => {
    const places = placesOf([
      reply('m1', { thread_id: 'th', tool_call_id: 'tc-failed', dispatch_failed: true }),
      reply('m2', { thread_id: 'th', tool_call_id: 'tc-unknown', dispatch_failed: true, dispatch_unknown: true }),
      report('r1', 'th'),
    ]);
    expect(places.has('tc-failed')).toBe(false);
    expect(places.get('tc-unknown')).toEqual({ madeAt: AT.getTime(), reportId: 'r1', reportFollows: true });
  });

  it('places a hand-off made without asking by its tool call, from the call\'s result', () => {
    const places = placesOf([
      {
        message: {
          id: 'm1',
          role: 'assistant',
          timestamp: AT,
          contentSegments: [{ type: 'tool_call', toolCallId: 'tc-9', order: 0 }],
          toolCallProcesses: {
            'tc-9': {
              toolName: 'delegate_to_analyst',
              toolCall: { id: 'tc-9', args: { question: 'Q' } },
              toolCallResult: {
                content: JSON.stringify({ preapproved: true, success: true, status: 'dispatched', thread_id: 'th', run_id: 'run-9' }),
              },
            },
          },
        },
      },
      report('r9', 'th', 'run-9'),
    ]);
    expect(places.get('tc-9')).toEqual({ madeAt: AT.getTime(), reportId: 'r9', reportRunId: 'run-9', reportFollows: true });
  });

  it('answers a steered hand-off with the report naming its steer, not the one on the run it was steered into', () => {
    // The run it was steered into ended before reading it; the next run took it in.
    const places = placesOf([
      reply('m1', { thread_id: 'th', run_id: 'run-1', tool_call_id: 'tc-1' }),
      reply('m2', { thread_id: 'th', run_id: 'run-1', steer_id: 'steer-2', tool_call_id: 'tc-2' }),
      steeredReport('r1', 'th', ['run-1'], []),
      steeredReport('r2', 'th', ['run-2'], ['steer-2']),
    ]);
    expect(reportOf(places, 'tc-1')).toBe('r1');
    expect(places.get('tc-2')).toMatchObject({ reportId: 'r2', reportRunId: 'run-2' });
  });

  it('gives hand-offs whose steers one run read that run\'s one report', () => {
    const places = placesOf([
      reply('m1', { thread_id: 'th', run_id: 'run-1', tool_call_id: 'tc-1' }),
      reply('m2', { thread_id: 'th', run_id: 'run-1', steer_id: 'steer-2', tool_call_id: 'tc-2' }),
      reply('m3', { thread_id: 'th', run_id: 'run-1', steer_id: 'steer-3', tool_call_id: 'tc-3' }),
      steeredReport('r1', 'th', ['run-1'], ['steer-2', 'steer-3']),
    ]);
    expect(['tc-1', 'tc-2', 'tc-3'].map((key) => reportOf(places, key))).toEqual(['r1', 'r1', 'r1']);
  });

  const steered = reply('m1', { thread_id: 'th', run_id: 'run-1', steer_id: 'steer-1', tool_call_id: 'tc-1' });

  it('lets a report whose steers are unknown answer a steered hand-off by order', () => {
    expect(reportOf(placesOf([steered, report('r1', 'th', 'run-1')]), 'tc-1')).toBe('r1');
  });

  it('keeps a steered hand-off from a report whose list names none', () => {
    expect(reportOf(placesOf([steered, steeredReport('r1', 'th', ['run-1'], [])]), 'tc-1')).toBeUndefined();
  });

  it('does not take the report on the run a steer went into for one a newer run took over', () => {
    // That run ended without reading the steer; its report answers neither hand-off.
    const places = placesOf([
      reply('m1', { thread_id: 'th', run_id: 'run-1', tool_call_id: 'tc-1' }),
      reply('m2', { thread_id: 'th', run_id: 'run-2', steer_id: 'steer-2', tool_call_id: 'tc-2' }),
      steeredReport('r2', 'th', ['run-2'], []),
    ]);
    expect(reportOf(places, 'tc-1')).toBeUndefined();
    expect(reportOf(places, 'tc-2')).toBeUndefined();
  });
});
