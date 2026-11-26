import type { ThreadTuning } from '@/components/ui/chat-input.types';

/** What a new thread opened from this one keeps as its own: the composer's
 *  model and level, and this thread's subagents, effort and fast mode. */
export interface NewThreadCarry {
  model?: string;
  reasoningEffort?: string;
  fastMode?: boolean;
  subagentsAllowed?: boolean;
  reasoningEffortOverride?: string;
  fastModeOverride?: boolean;
}

/**
 * The carry of a thread opened the way a new chat's composer opens one. A
 * value this thread leaves unset is left out, so the new thread follows
 * Settings for it rather than a null the composer would not have sent.
 */
export function newThreadCarry(
  composer: { model?: string | null; reasoningEffort?: string | null; fastMode?: boolean | null } | null | undefined,
  subagents: { allowed?: boolean | null },
  tuning: ThreadTuning,
): NewThreadCarry {
  return {
    ...(composer?.model ? { model: composer.model } : {}),
    ...(composer?.reasoningEffort ? { reasoningEffort: composer.reasoningEffort } : {}),
    ...(typeof composer?.fastMode === 'boolean' ? { fastMode: composer.fastMode } : {}),
    ...(typeof subagents.allowed === 'boolean' ? { subagentsAllowed: subagents.allowed } : {}),
    ...(typeof tuning.reasoningEffort === 'string' ? { reasoningEffortOverride: tuning.reasoningEffort } : {}),
    ...(typeof tuning.fastMode === 'boolean' ? { fastModeOverride: tuning.fastMode } : {}),
  };
}
