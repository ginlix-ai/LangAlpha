import { useTranslation } from 'react-i18next';
import { PopoverClose } from '@/components/ui/popover';
import { useNow } from '@/hooks/useNow';
import { cn } from '@/lib/utils';
import type { CommandRun } from '@/types/commands';
import { COMMAND_TICK_MS } from '../../hooks/useCommands';
import { TRAY_ROW } from '../chatView/trayRow';
import { RunGlyph } from '../messageList/runCard';
import { CommandStopButton } from './CommandState';
import { commandName, commandRunningFor } from './commandWords';

/**
 * The tray's list of the commands the agent left running in the background:
 * what each does, how long it has run, and a Stop. A row opens the tool card
 * its command started from. The agent hears of each one when it ends, which
 * the list says, so nobody has to watch it.
 */
export function CommandListRows({ rows, onOpen }: { rows: readonly CommandRun[]; onOpen: (toolCallId: string) => void }) {
  const { t } = useTranslation();
  const now = useNow(COMMAND_TICK_MS);
  return (
    <>
      {rows.map((row) => {
        const name = commandName(row.description, row.tool, t);
        const toolCallId = row.tool_call_id;
        const body = (
          <>
            <span className="flex w-3.5 shrink-0 justify-center">
              <RunGlyph kind="live" />
            </span>
            <span className="min-w-0 flex-1 truncate text-[0.8125rem]" style={{ color: 'var(--color-text-primary)' }} title={name}>
              {name}
            </span>
            <span className="shrink-0 font-mono text-[0.71875rem] tabular-nums" style={{ color: 'var(--color-text-tertiary)' }}>
              {commandRunningFor(row, now, t)}
            </span>
          </>
        );
        return (
          <li key={row.command_id} className="flex min-w-0 items-center gap-1.5 pr-1.5">
            {toolCallId ? (
              <PopoverClose asChild>
                <button type="button" className={cn(TRAY_ROW, 'min-w-0 flex-1')} onClick={() => onOpen(toolCallId)}>
                  {body}
                </button>
              </PopoverClose>
            ) : (
              <div className="flex min-w-0 flex-1 items-center gap-2.5 px-2.5 py-2">{body}</div>
            )}
            <CommandStopButton command={row} />
          </li>
        );
      })}
      <li className="px-2.5 pt-1 pb-1.5 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('chat.commands.pickedUp', { count: rows.length })}
      </li>
    </>
  );
}
