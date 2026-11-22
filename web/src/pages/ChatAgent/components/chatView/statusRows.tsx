/**
 * The rows a tray chip's list and the Status panel share: a task, an Analyst,
 * and the row shape they both take, so the two surfaces list one thing alike.
 */
import { createContext, useContext, type ReactElement, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { ArrowUpRight, Folder } from 'lucide-react';
import { PopoverClose } from '@/components/ui/popover';
import { DispatchStatusIndicator } from '../dispatchStatusUi';
import { SubagentGlyph } from '../mcp/subagentGlyph';
import type { PTCDispatchStatus } from '../../hooks/usePTCDispatchStatus';
import { isTerminalStatus, sidebarAgentRowLabel, type SubagentTerminalStatus } from '../../session/subagents/subagentStatus';
import type { RosterTask } from './delegatedWork';
import type { RosterAnalyst } from './useAnalystRoster';

/** A row in one of the tray's lists that opens what it names. */
export const TRAY_ROW = 'flex w-full cursor-pointer items-center gap-2 rounded-md px-2 row-inset text-left outline-hidden transition-colors hover:bg-(--color-bg-hover) focus-visible:bg-(--color-bg-hover)';

/** True inside a tray chip's popover, where a row closes the list as it
 *  opens what it names. The Status panel draws the same rows with no list
 *  to close. */
// eslint-disable-next-line react-refresh/only-export-components
export const InTrayList = createContext(false);

/** A row's button, closing the tray's list when it sits in one. */
export function RowClose({ children }: { children: ReactElement }): ReactElement {
  return useContext(InTrayList) ? <PopoverClose asChild>{children}</PopoverClose> : children;
}

/** A settled task in the Analysts' glyphs, so the two lists read alike. */
const SETTLED_TASK_LOOK: Record<SubagentTerminalStatus, PTCDispatchStatus> = {
  completed: 'completed',
  error: 'failed',
  cancelled: 'stopped',
};

/**
 * How a roster task stands, in the Analysts' terms: running only while it
 * runs. A status alone can read live after the run ended unseen, so a stopped
 * task without a final status has none.
 */
// eslint-disable-next-line react-refresh/only-export-components
export function taskDispatchStatus(row: RosterTask): PTCDispatchStatus | null {
  if (row.running) return 'running';
  return isTerminalStatus(row.status) ? SETTLED_TASK_LOOK[row.status] : null;
}

interface WorkRowProps {
  icon: ReactNode;
  title: string;
  /** The title's tooltip, where it says more than the title. */
  tooltip?: string;
  /** What kind of work it is, before its status. */
  kind: string;
  status: PTCDispatchStatus | null;
  /** When the work started, for the status's clock; without it, no clock. */
  startedAt?: number;
  clock?: boolean;
  onOpen: () => void;
}

/** A piece of handed-off work: its kind's glyph, its title, what it is and
 *  how it stands, opening it. */
function WorkRow({ icon, title, tooltip, kind, status, startedAt, clock, onOpen }: WorkRowProps) {
  return (
    <RowClose>
      <button type="button" className={TRAY_ROW} onClick={onOpen}>
        <span
          aria-hidden="true"
          className="flex h-6 w-6 shrink-0 items-center justify-center rounded-md"
          style={{ background: 'var(--color-bg-hover)', color: 'var(--color-text-tertiary)' }}
        >
          {icon}
        </span>
        <span className="flex min-w-0 flex-1 flex-col gap-0.5">
          <span className="truncate text-[0.8125rem]" style={{ color: 'var(--color-text-primary)' }} title={tooltip ?? title}>
            {title}
          </span>
          <span className="flex min-w-0 items-center gap-1.5 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            <span className="truncate">{kind}</span>
            {status && <span aria-hidden="true">·</span>}
            {status && <DispatchStatusIndicator status={status} startedAt={startedAt} clock={clock} />}
          </span>
        </span>
        <ArrowUpRight aria-hidden="true" className="h-3 w-3 shrink-0" style={{ color: 'var(--color-text-quaternary)' }} />
      </button>
    </RowClose>
  );
}

/** How a task reads in a list: a subagent, by its role, beside the Analysts
 *  and commands it shares the Working and Done lists with. */
export function TaskRow({ row, onOpen }: { row: RosterTask; onOpen: (agentId: string) => void }) {
  const { t } = useTranslation();
  const label = sidebarAgentRowLabel(row);
  return (
    <WorkRow
      icon={<SubagentGlyph type={row.type} className="h-3 w-3" />}
      title={label}
      // The agent's id only tells two tasks with the same description apart.
      tooltip={label !== row.name ? `${label} · ${row.name}` : label}
      kind={row.type ? t('chat.status.subagentOfType', { type: row.type }) : t('chat.status.subagent')}
      status={taskDispatchStatus(row)}
      clock={false}
      onOpen={() => onOpen(row.id)}
    />
  );
}

export function AnalystRow({ thread, onOpen }: { thread: RosterAnalyst; onOpen: () => void }) {
  const { t } = useTranslation();
  return (
    <WorkRow
      icon={<Folder className="h-3 w-3" />}
      title={thread.title || t('nav.untitledThread')}
      kind={thread.workspaceName ? t('chat.status.analystIn', { workspace: thread.workspaceName }) : t('chat.status.analyst')}
      status={thread.status}
      startedAt={thread.startedAt}
      clock={thread.startedAt != null}
      onOpen={onOpen}
    />
  );
}
