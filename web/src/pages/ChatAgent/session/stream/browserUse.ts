import type { ToolCallData } from '@/types/sse';
import { drivesBrowser } from '../../components/filePanel/browserProtocol';

/** The fields of a stream frame that say a tool call was made or answered. */
interface ToolFrame {
  tool_calls?: readonly ToolCallData[];
  tool_call_id?: unknown;
}

/**
 * A shell call that ran the browser CLI on this thread's own computer. One that
 * names a `computer` ran on another of the user's machines, whose browser is not
 * the one the panel shows: the argument is left out for the turn's own, so any
 * name in it is read as elsewhere, as the server reads it.
 */
function drivesThisBrowser({ name, args }: ToolCallData): boolean {
  if (name !== 'Bash' || typeof args?.command !== 'string') return false;
  const computer = args.computer;
  if (typeof computer === 'string' && computer.trim()) return false;
  return drivesBrowser(args.command);
}

/** One of the agent's browser calls, as it starts and again as it ends. */
export interface BrowserUse {
  id: string;
  command: string;
  done: boolean;
  /** The command of the latest call still running once this one is seen, which
   *  is what the view captions; null once none is. */
  running: string | null;
}

/**
 * Which events show the agent using the browser. Each such call is seen twice:
 * when it is made, before its command has run, so a browser it opens is not up
 * yet, and again when its result comes back, by which time it is. The calls
 * still running are kept here and nowhere else.
 */
export class BrowserUseWatch {
  /** The command of each call still running, by call id, oldest first. */
  private readonly running = new Map<string, string>();

  private latest(): string | null {
    return [...this.running.values()].at(-1) ?? null;
  }

  seen(eventType: string, event: ToolFrame): BrowserUse | null {
    if (eventType === 'tool_calls') {
      const calls = (event.tool_calls ?? []).filter(drivesThisBrowser);
      for (const { id, args } of calls) if (id) this.running.set(id, String(args?.command));
      // The last call made is the one the agent is on.
      const last = calls.at(-1);
      if (!last) return null;
      const command = String(last.args?.command);
      // A call with no id cannot be heard to end, but is still what the agent is on.
      return { id: last.id ?? '', command, done: false, running: last.id ? this.latest() : command };
    }
    if (eventType === 'tool_call_result' && typeof event.tool_call_id === 'string') {
      const command = this.running.get(event.tool_call_id);
      if (command === undefined) return null;
      this.running.delete(event.tool_call_id);
      return { id: event.tool_call_id, command, done: true, running: this.latest() };
    }
    return null;
  }
}
