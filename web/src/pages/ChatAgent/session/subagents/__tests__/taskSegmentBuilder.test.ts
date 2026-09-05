import { describe, expect, it } from 'vitest';

import { applyTaskSegment, deriveTaskSegment } from '../taskSegmentBuilder';
import type { SubagentTaskRecord } from '@/types/chat';
import { WORKFLOW_TASK_TYPE } from '../workflowRunState';

type Segment = Record<string, unknown>;
type TaskMap = Record<string, SubagentTaskRecord>;

function apply(
  calls: { toolCall: Parameters<typeof deriveTaskSegment>[0]; id: string }[]
): { segments: Segment[]; tasks: TaskMap } {
  const segments: Segment[] = [];
  const tasks: TaskMap = {};
  calls.forEach(({ toolCall, id }, order) => {
    const derived = deriveTaskSegment(toolCall, id, order);
    if (derived) applyTaskSegment(derived, id, segments, tasks);
  });
  return { segments, tasks };
}

describe('deriveTaskSegment', () => {
  it('renders no card for a tool call that has none', () => {
    expect(deriveTaskSegment({ name: 'Read', args: { path: 'a.ts' } }, 'tc-1', 0)).toBeNull();
  });

  it('accepts both the PascalCase and lowercase task names', () => {
    // The backend sends "Task"; older payloads send "task". A silent null here
    // would drop the card entirely rather than render it wrong.
    const lower = deriveTaskSegment({ name: 'task', args: { description: 'd' } }, 'tc-1', 0);
    const upper = deriveTaskSegment({ name: 'Task', args: { description: 'd' } }, 'tc-1', 0);
    expect(lower).not.toBeNull();
    expect(upper).toEqual(lower);
  });

  it('marks a spawn as merging, not stacking', () => {
    const derived = deriveTaskSegment(
      { name: 'Task', args: { description: 'research AAPL', subagent_type: 'research' } },
      'tc-1',
      3
    );

    expect(derived).toEqual({
      segment: { type: 'subagent_task', subagentId: 'tc-1', order: 3 },
      record: {
        subagentId: 'tc-1',
        description: 'research AAPL',
        prompt: 'research AAPL',
        type: 'research',
        action: 'init',
        status: 'running',
      },
      stacks: false,
    });
  });

  it('marks a resume as stacking and normalizes its target id', () => {
    const derived = deriveTaskSegment(
      { name: 'Task', args: { action: 'resumed', task_id: 'abc', prompt: 'keep going' } },
      'tc-2',
      1
    );

    expect(derived?.stacks).toBe(true);
    expect(derived?.segment.resumeTargetId).toBe('task:abc');
    expect(derived?.record.action).toBe('resume');
  });

  it('leaves an already-prefixed target id alone', () => {
    // Double-prefixing yields "task:task:abc", which matches no floating card.
    const derived = deriveTaskSegment(
      { name: 'Task', args: { action: 'resume', task_id: 'task:abc' } },
      'tc-2',
      1
    );
    expect(derived?.segment.resumeTargetId).toBe('task:abc');
  });

  it('infers resume from a bare task_id with no action', () => {
    const derived = deriveTaskSegment({ name: 'Task', args: { task_id: 'abc' } }, 'tc-2', 1);
    expect(derived?.stacks).toBe(true);
  });

  it('gives a workflow run the workflow type and merges like a spawn', () => {
    const derived = deriveTaskSegment(
      { name: 'RunWorkflow', args: { workflow: 'ticker-briefs' } },
      'tc-3',
      0
    );

    expect(derived?.stacks).toBe(false);
    expect(derived?.record.type).toBe(WORKFLOW_TASK_TYPE);
    // Named runs pass no description, so the name is what the card can show.
    expect(derived?.record.description).toBe('ticker-briefs');
  });
});

describe('deriveTaskSegment — legacy action vocabulary', () => {
  // These three spellings are the entire backward-compat surface for replayed
  // checkpoints. A regression here changes how *history* renders while live
  // traffic looks perfect, which is the one failure mode a visual pass cannot
  // see — so each is pinned to the stack-vs-merge decision it must land on.

  it('normalizes a persisted "spawned" to init and keeps it merging', () => {
    const derived = deriveTaskSegment(
      { name: 'Task', args: { action: 'spawned', description: 'research AAPL' } },
      'tc-1',
      0
    );
    expect(derived?.record.action).toBe('init');
    expect(derived?.stacks).toBe(false);
    expect(derived?.segment.resumeTargetId).toBeUndefined();
  });

  it('normalizes "steering_accepted" to update and gives it its own card', () => {
    // `update` is not `init`, so a steering follow-up takes the resume branch:
    // its own card pointing back at the task it steered.
    const derived = deriveTaskSegment(
      { name: 'Task', args: { action: 'steering_accepted', task_id: 'abc' } },
      'tc-2',
      1
    );
    expect(derived?.record.action).toBe('update');
    expect(derived?.stacks).toBe(true);
    expect(derived?.record.resumeTargetId).toBe('task:abc');
  });

  it('passes an unrecognized action through rather than forcing it to init', () => {
    const derived = deriveTaskSegment(
      { name: 'Task', args: { action: 'teleported', task_id: 'abc' } },
      'tc-2',
      1
    );
    expect(derived?.record.action).toBe('teleported');
    expect(derived?.stacks).toBe(true);
  });

  it('yields a bare "task:" target when a resume carries no task_id', () => {
    // Degenerate but non-throwing: the sentinel matches no floating card, so
    // the card renders with a dangling target rather than crashing the message.
    // Pinned as current behaviour, not endorsed — see the note in the build log.
    const derived = deriveTaskSegment({ name: 'Task', args: { action: 'resume' } }, 'tc-2', 1);
    expect(derived?.record.resumeTargetId).toBe('task:');
    expect(derived?.stacks).toBe(true);
  });
});

describe('applyTaskSegment', () => {
  it('merges a repeated spawn in place instead of duplicating its card', () => {
    // Live streaming re-derives the same tool call as the args fill in; each
    // pass must land on the one card, not add another.
    const { segments, tasks } = apply([
      { toolCall: { name: 'Task', args: { description: 'first' } }, id: 'tc-1' },
      { toolCall: { name: 'Task', args: { description: 'first', prompt: 'full' } }, id: 'tc-1' },
    ]);

    expect(segments).toHaveLength(1);
    expect(tasks['tc-1'].prompt).toBe('full');
  });

  it('leaves a resume its own card below the spawn it follows', () => {
    // A resume arrives under its own tool call id, so the spawn's card stays
    // visible above it rather than being mutated into the resume.
    const { segments } = apply([
      { toolCall: { name: 'Task', args: { description: 'go' } }, id: 'tc-1' },
      { toolCall: { name: 'Task', args: { action: 'resume', task_id: 'abc' } }, id: 'tc-2' },
    ]);

    expect(segments).toHaveLength(2);
    expect(segments[0].resumeTargetId).toBeUndefined();
    expect(segments[1].resumeTargetId).toBe('task:abc');
  });

  it('replaces a resume record rather than merging over the spawn it follows', () => {
    // Merging would leave the spawn's fields on a resume that never sent them —
    // the record replaces so a stale description cannot survive onto the card.
    const segments: Segment[] = [];
    const tasks: TaskMap = {};
    const spawn = deriveTaskSegment(
      { name: 'Task', args: { description: 'original', subagent_type: 'research' } },
      'tc-1',
      0
    )!;
    applyTaskSegment(spawn, 'tc-1', segments, tasks);
    const resume = deriveTaskSegment(
      { name: 'Task', args: { action: 'resume', task_id: 'abc' } },
      'tc-1',
      1
    )!;
    applyTaskSegment(resume, 'tc-1', segments, tasks);

    expect(tasks['tc-1'].description).toBe('');
    expect(tasks['tc-1'].resumeTargetId).toBe('task:abc');
  });

  // Deliberately unasserted: a re-derived resume currently appends a duplicate
  // card, because `stacks` bypasses the exists-check. Whether that is a bug is
  // a product decision pending a live turn, and pinning it either way now would
  // either bless the duplication or fail against shipped behaviour.

  it('merges a re-derived workflow run in place', () => {
    const { segments, tasks } = apply([
      { toolCall: { name: 'RunWorkflow', args: { workflow: 'ticker-briefs' } }, id: 'tc-3' },
      { toolCall: { name: 'RunWorkflow', args: { workflow: 'ticker-briefs' } }, id: 'tc-3' },
    ]);

    expect(segments).toHaveLength(1);
    expect(tasks['tc-3'].type).toBe(WORKFLOW_TASK_TYPE);
  });
});
