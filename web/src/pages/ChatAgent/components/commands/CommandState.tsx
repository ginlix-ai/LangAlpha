import { useContext, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Check, Square, X } from 'lucide-react';
import { useNow } from '@/hooks/useNow';
import { cn } from '@/lib/utils';
import type { CommandRun, CommandStatus } from '@/types/commands';
import { RunGlyph } from '../messageList/runCard';
import { COMMAND_TICK_MS, useStopCommand } from '../../hooks/useCommands';
import { commandName, commandStateWords } from './commandWords';
import { ThreadCommandsContext, useCommandForToolCall } from './threadCommandsContext';

const STATE = 'inline-flex shrink-0 items-center gap-1.5 whitespace-nowrap font-mono text-[0.71875rem] tabular-nums text-(--color-text-tertiary)';

// Quiet and neutral, as every button is: the amber stays on the glyph.
const STOP = cn(
  'inline-flex h-5 shrink-0 cursor-pointer items-center gap-1 rounded-md border border-(--color-border-default) px-1.5',
  'text-[0.71875rem] leading-none font-medium text-(--color-text-secondary) outline-hidden transition-colors',
  'hover:bg-(--color-bg-hover) hover:text-(--color-text-primary) focus-visible:ring-2 focus-visible:ring-(--color-focus-ring)',
  'disabled:cursor-default disabled:opacity-60 pointer-coarse:h-7 pointer-coarse:px-2.5',
);

/**
 * A command's state as a glyph: the liveness glyph while it runs, a check
 * and an X for how it ended, as the approved card draws them, and the stop
 * for one the user or the agent stopped. Decorative: the words say the same.
 * Not `DispatchStatusGlyph`, which marks a failure with an alert and names
 * its status to a screen reader.
 */
function CommandGlyph({ status }: { status: CommandStatus }) {
  switch (status) {
    case 'running':
      return <RunGlyph kind="live" />;
    case 'completed':
      return <Check aria-hidden className="h-3 w-3 shrink-0 stroke-[2.5] text-(--color-success)" />;
    case 'failed':
      return <X aria-hidden className="h-3 w-3 shrink-0 stroke-[2.5] text-(--color-icon-danger)" />;
    case 'stopped':
      return <RunGlyph kind="stop" />;
  }
}

/**
 * Where the command a tool call handed off to stands, on the call's row or
 * card, and nothing for a call that returned as any call does. Mount it only
 * for the tools that hand off, since each one watches the thread's listing;
 * the state mounts with its command, so its clock starts then. `className`
 * sets it in a block of its own.
 */
export function ToolCallCommandState({ toolCallId, className }: { toolCallId: string; className?: string }) {
  const command = useCommandForToolCall(toolCallId);
  if (!command) return null;
  return className ? <div className={className}><CommandState command={command} /></div> : <CommandState command={command} />;
}

/**
 * Where a handed-off command stands: in the background, with how long and a
 * Stop, then how it ended and how long it took. The tool's own result only
 * says it moved to the background, which stays true long after it stops
 * being the news.
 */
function CommandState({ command }: { command: CommandRun }) {
  const { t } = useTranslation();
  const running = command.status === 'running';
  const now = useNow(COMMAND_TICK_MS, running);
  return (
    <span className="inline-flex min-w-0 shrink-0 items-center gap-2 self-center" data-command-state={command.status}>
      <span className={STATE}>
        <CommandGlyph status={command.status} />
        {commandStateWords(command, now, t)}
      </span>
      {running && <CommandStopButton command={command} />}
    </span>
  );
}

/**
 * Stops a running command. No confirmation, as a wakeup's Cancel has none:
 * the command's row says it stopped, and the agent can run it again. A stop
 * that fails says so beside the button, which stays for another try; one the
 * server took but has yet to finish reads as stopping until the row ends.
 */
export function CommandStopButton({ command }: { command: CommandRun }) {
  const { t } = useTranslation();
  const { mutate, isPending } = useStopCommand(useContext(ThreadCommandsContext));
  const [failed, setFailed] = useState(false);
  const name = commandName(command.description, command.tool, t);
  const stopping = isPending || !!command.stopping;
  return (
    <>
      <button
        type="button"
        className={STOP}
        disabled={stopping}
        aria-label={t('chat.commands.stopLabel', { name })}
        onClick={(event) => {
          // A row that opens something holds this button; the stop is all it does.
          event.stopPropagation();
          setFailed(false);
          mutate(command.command_id, { onError: () => setFailed(true) });
        }}
      >
        <Square aria-hidden className="h-2 w-2 shrink-0" fill="currentColor" strokeWidth={0} />
        {stopping ? t('chat.stopping') : t('chat.stop')}
      </button>
      {failed && (
        <span role="alert" className="shrink-0 text-[0.71875rem] text-(--color-icon-danger)">
          {t('chat.commands.stopFailed')}
        </span>
      )}
    </>
  );
}
