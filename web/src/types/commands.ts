/**
 * A Bash or ExecuteCode call that outlasted its tool's wait and went on
 * running on the computer after the tool returned. Only such a call has a
 * row: one that ends within the wait returns as any tool does. Mirrors the
 * thread's commands listing.
 */
export type CommandTool = 'Bash' | 'ExecuteCode';
/** Why a command failed: its own exit code, its computer going away, or the
 *  hour after its turn running out with nothing checking on it. */
export type CommandFailureReason = 'exit' | 'lost' | 'vanished' | 'after_turn_limit';

/** Only an ended command has an end, and only a failed one a reason. */
export type CommandRun = {
  command_id: string;
  tool: CommandTool;
  /** What the agent said the command does; null when it said nothing. */
  description: string | null;
  exit_code: number | null;
  /** The tool call it started from, which is how its tool card finds it. */
  tool_call_id: string | null;
  log_path: string;
  started_at: string;
  handed_off_at: string;
  /** A stop is under way that the server finishes on its own, as when it
   *  could not reach the computer yet; only ever true while it runs. */
  stopping?: boolean;
} & (
  | { status: 'running'; failure_reason: null; ended_at: null }
  | { status: 'completed' | 'stopped'; failure_reason: null; ended_at: string }
  | { status: 'failed'; failure_reason: CommandFailureReason; ended_at: string }
);
export type CommandStatus = CommandRun['status'];

export interface CommandsResponse {
  thread_id: string;
  commands: CommandRun[];
}

export interface CommandStopResponse {
  /** Whether it was still running when the stop reached it. */
  stopped: boolean;
  /** The row after the stop; absent or null when it could not be read. */
  command?: CommandRun | null;
}
