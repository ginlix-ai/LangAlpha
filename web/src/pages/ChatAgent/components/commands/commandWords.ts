/**
 * How a background command reads to the user: by what the agent said it
 * does, never by its id, and where it stands in the words its tool card, the
 * tray and its report share. Pure, so the copy is pinned without rendering.
 */
import type { CommandFailureReason, CommandRun, CommandStatus, CommandTool } from '@/types/commands';
import { epochMs, formatRunLength, type TFn } from '@/lib/elapsed';
import { getDisplayName } from '../toolDisplayConfig';
import { summarize } from '../messageList/subagentRun/text';

/** The two tools whose calls can go on running after they return. */
export function isCommandTool(toolName: string | undefined): toolName is CommandTool {
  return toolName === 'Bash' || toolName === 'ExecuteCode';
}

/** A command's name: the agent's description, else the tool's own name. */
export function commandName(description: string | null | undefined, tool: string, t: TFn): string {
  return summarize(description ?? undefined) || getDisplayName(tool, t);
}

type Ended = Exclude<CommandStatus, 'running'>;

/**
 * How a command ended. One stopped an hour after its turn, with nothing
 * checking on it, failed on the wire, but it ran out of time rather than
 * failed, so it reads as stopped early.
 */
export function commandOutcome<S extends Ended>(status: S, failureReason: CommandFailureReason | null): S | 'stoppedEarly' {
  return status === 'failed' && failureReason === 'after_turn_limit' ? 'stoppedEarly' : status;
}

/**
 * Each outcome's words: on its tool card with how long it took, on the card
 * alone when its row's two clocks give it no length, and on its report. Of
 * the stops, only the user's reports.
 */
export const OUTCOME_KEYS = {
  completed: { took: 'chat.commands.completedIn', bare: 'chat.commands.completed', report: 'chat.provenance.taskFinished' },
  failed: { took: 'chat.commands.failedAfter', bare: 'chat.commands.failed', report: 'chat.provenance.taskFailed' },
  stoppedEarly: { took: 'chat.commands.stoppedEarlyAfter', bare: 'chat.commands.stoppedEarly', report: 'chat.provenance.commandStoppedEarly' },
  stopped: { took: 'chat.commands.stoppedAfter', bare: 'chat.commands.stopped', report: 'chat.provenance.stopped' },
} as const satisfies Record<Ended | 'stoppedEarly', { took: string; bare: string; report: string }>;

/** How long a running command has run. The time runs from its start, so the
 *  minutes the tool waited count. */
export function commandRunningFor(command: CommandRun, now: number, t: TFn): string {
  return formatRunLength(now - Date.parse(command.started_at), t);
}

/** Where a command stands, in words: how long it has run in the background,
 *  or how it ended and how long it took. */
export function commandStateWords(command: CommandRun, now: number, t: TFn): string {
  if (command.status === 'running') return t('chat.commands.inBackground', { duration: commandRunningFor(command, now, t) });
  const keys = OUTCOME_KEYS[commandOutcome(command.status, command.failure_reason)];
  // The start is the app's clock and the end the database's, so a command
  // stopped as it began can end a moment before it started.
  const took = Date.parse(command.ended_at) - Date.parse(command.started_at);
  return took >= 0 ? t(keys.took, { duration: formatRunLength(took, t) }) : t(keys.bare);
}

/** The commands still running, oldest first, as the tray lists them. */
export function runningCommands(rows: readonly CommandRun[]): CommandRun[] {
  return rows
    .filter((row) => row.status === 'running')
    .sort((a, b) => (epochMs(a.started_at) ?? 0) - (epochMs(b.started_at) ?? 0));
}
