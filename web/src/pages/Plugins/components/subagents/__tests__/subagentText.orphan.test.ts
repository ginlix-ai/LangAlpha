import { describe, expect, it } from 'vitest';
import type { TFunction } from 'i18next';
import type { SubagentBaseValues, SubagentView } from '@/pages/ChatAgent/utils/api';
import { isOrphan, isResettable, removalCopy } from '../subagentText';

const t = ((key: string) => key) as unknown as TFunction;

const values: SubagentBaseValues = {
  model: null,
  effort: null,
  tools: ['web'],
  search_provider: null,
  search_depth: null,
  connectors: true,
  brokerages: 'on',
  memory: false,
  preferred_connectors: [],
  preferred_skills: [],
  enabled: true,
};

const view = (over: Partial<SubagentView>): SubagentView =>
  ({
    ...values,
    name: 'research',
    kind: 'base',
    source: 'builtin',
    plugin: null,
    edited: false,
    user_fields: [],
    description: '',
    instructions: '',
    available_to: ['analyst', 'chief_of_staff'],
    base: values,
    problems: [],
    updated_by: 'user',
    updated_thread_id: null,
    updated_thread_title: null,
    updated_at: null,
    file: '.agents/user/agents/research.md',
    version: 'v',
    ...over,
  }) as SubagentView;

// The name now resolves to another base (a deployment definition replaced
// the built-in, or took a plugin's name): the server flags the user's old
// row but serves the current base, which runs every turn and takes a save.
const stale = view({ problems: [{ code: 'base_removed', detail: 'deployment:research' }] });

// Nothing defines the name any more: the server sends no base.
const orphan = view({
  name: 'ghost',
  source: 'plugin',
  plugin: { name: 'gone-kit', display_name: 'gone-kit', installed: false },
  edited: true,
  user_fields: ['effort'],
  effort: 'low',
  tools: [],
  base: null,
  file: null,
  problems: [{ code: 'base_removed', detail: 'plugin:gone-kit/ghost' }],
});

describe('isOrphan', () => {
  it('is a base with no base values, as the server decides it', () => {
    expect(isOrphan(orphan)).toBe(true);
    expect(isOrphan(stale)).toBe(false);
  });

  it('does not tell the user a running base stops when its stale row goes', () => {
    expect(removalCopy(t, orphan, null, false).message).toBe('plugins.subagents.removeConfirm');
    expect(removalCopy(t, stale, null, false).message).toBe('plugins.subagents.resetConfirmPlain');
  });
});

describe('isResettable', () => {
  it("offers a reset for the user's changes and for a row no turn reads", () => {
    expect(isResettable(view({ edited: true, user_fields: ['model'] }))).toBe(true);
    // Saving starts over from the current base, so a reset is the one way
    // to drop the row without changing anything.
    expect(isResettable(stale)).toBe(true);
  });

  it('not for an untouched base, a custom subagent or an orphan, which goes whole', () => {
    expect(isResettable(view({}))).toBe(false);
    expect(isResettable(view({ kind: 'custom', base: null }))).toBe(false);
    expect(isResettable(orphan)).toBe(false);
  });
});
