/**
 * Why the server would not run or take a turn, read from a refused request's
 * structured detail (`errorInfo`, or an API error's body) or a stream's error
 * frame. The shared workspace's refusal codes are spelled only here.
 */
import { readRuleRefusal, type RuleRefusal } from '@/lib/ruleRefusal';
import { readSuspension } from '@/lib/accountSuspension';
import { personLabel } from '@/lib/sharedThreads';

/** The admission code of a send into a shared thread while another member's
 *  turn runs there. */
const OTHERS_TURN_RUNNING = 'another_member';
/** A rewrite (edit, regenerate, retry, answer) that would replace another
 *  member's turn. */
export const OTHERS_TURN_REWRITE = 'another_members_turn';
/** A steer into a task another member's turn started. */
export const OTHERS_TASK = 'another_members_task';
/** A thread sent under a workspace that is not its own, across a shared
 *  workspace's line. */
const OTHER_WORKSPACE = 'thread_in_another_workspace';

export type TurnRefusal =
  | { kind: 'rule'; refusal: RuleRefusal }
  | { kind: 'suspended'; organization: string | null }
  /** `member` is the name of whoever sent the running turn, when known. */
  | { kind: 'others_turn'; member: string | null }
  | { kind: 'other_workspace' };

/**
 * A turn the account's standing kept from running or ended: a rule, the
 * account's suspension, rules that could not be read, or a limit the
 * organization set (worded by the server, in the error's `message`). Drawn
 * as a neutral row, never as a failure, and never offering a Retry.
 */
export type RuleStop =
  | Extract<TurnRefusal, { kind: 'rule' | 'suspended' }>
  | { kind: 'rules_unknown' }
  | { kind: 'limit' };

function memberName(member: unknown): string | null {
  if (!member || typeof member !== 'object') return null;
  const { name } = member as { name?: unknown };
  return typeof name === 'string' && name ? name : null;
}

/** The refusal `detail` carries, or null when it is none of these. */
export function readTurnRefusal(detail: unknown): TurnRefusal | null {
  if (!detail || typeof detail !== 'object') return null;
  const rule = readRuleRefusal(detail);
  if (rule) return { kind: 'rule', refusal: rule };
  const suspension = readSuspension(detail);
  if (suspension) return { kind: 'suspended', organization: suspension.organization };
  const d = detail as Record<string, unknown>;
  if (d.code === OTHERS_TURN_RUNNING) return { kind: 'others_turn', member: memberName(d.member) };
  return d.type === OTHER_WORKSPACE ? { kind: 'other_workspace' } : null;
}

type Translate = (key: string, options?: Record<string, unknown>) => string;

/** Another member's turn is running: nothing failed, the composer holds the
 *  draft until it ends, and the view reloads to show that turn. */
export function holdsDraft(refusal: TurnRefusal | null): boolean {
  return refusal?.kind === 'others_turn';
}

/** The calm words for a send refused because another member's turn is
 *  running, or `waiting` on their answer to a question or an approval, naming
 *  that member when the refusal did. */
export function othersTurnWords(t: Translate, member: string | null, waiting = false): string {
  const name = personLabel(t, { name: member, mine: false });
  return t(waiting ? 'sharedWorkspace.othersTurn.waiting' : 'sharedWorkspace.othersTurn.running', { name });
}
