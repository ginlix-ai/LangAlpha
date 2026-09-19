import { MoreHorizontal, Zap, Pin, Cpu, Server, Infinity as InfinityIcon } from 'lucide-react';
import { useTranslation } from 'react-i18next';

import { DropdownMenu, DropdownMenuContent, DropdownMenuTrigger } from '@/components/ui/dropdown-menu';
import { useIsMobile } from '@/hooks/useIsMobile';
import type { Computer } from '@/types/api';

import { ComputerStatusIndicator } from '../computerStatusUi';
import { normalizeTier, tierLabel } from '../tierUi';
import { WorkspaceMenuItems } from '../workspaceActions';
import { WORKSPACE_CARD_HEIGHT } from './cardMetrics';
import type { WorkspaceRecord } from './types';

interface CardMenuProps {
  workspace: WorkspaceRecord;
  onTogglePin: (workspace: WorkspaceRecord) => void;
  onRename: (workspace: WorkspaceRecord) => void;
  onUpgrade: (workspace: WorkspaceRecord) => void;
  onToggleAlwaysOn: (workspace: WorkspaceRecord) => void;
  onDuplicate: (workspace: WorkspaceRecord) => void;
  onDelete: (workspace: WorkspaceRecord) => void;
}

/** Card menu dropdown (Pin / Rename / Spec / Always-on / Duplicate / Delete). */
function CardMenu({ workspace, onTogglePin, onRename, onUpgrade, onToggleAlwaysOn, onDuplicate, onDelete }: CardMenuProps) {
  return (
    <DropdownMenu modal={false}>
      <DropdownMenuTrigger asChild>
        <button
          onPointerDown={(e) => e.stopPropagation()}
          className="h-8 w-8 rounded-md transition-colors flex items-center justify-center hover:bg-[var(--color-border-muted)]"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          <MoreHorizontal className="h-5 w-5" />
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" sideOffset={4}>
        <WorkspaceMenuItems
          workspace={workspace}
          onTogglePin={onTogglePin}
          onRename={onRename}
          onUpgrade={onUpgrade}
          onToggleAlwaysOn={onToggleAlwaysOn}
          onDuplicate={onDuplicate}
          onDelete={onDelete}
        />
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

interface WorkspaceCardProps {
  workspace: WorkspaceRecord;
  /** The machine this workspace lives on, when the user's list names one. */
  computer?: Computer | null;
  onSelect: (wsId: string, name?: string, status?: string) => void;
  onTogglePin: (workspace: WorkspaceRecord) => void;
  onRenameStart: (workspace: WorkspaceRecord) => void;
  onUpgrade: (workspace: WorkspaceRecord) => void;
  onToggleAlwaysOn: (workspace: WorkspaceRecord) => void;
  onDuplicate: (workspace: WorkspaceRecord) => void;
  onDelete: (workspace: WorkspaceRecord) => void;
  prefetchThreads?: (wsId: string) => void;
  index?: number;
}

/** Workspace card for the normal gallery grid (no DnD). */
export function WorkspaceCard({ workspace, computer, onSelect, onTogglePin, onRenameStart, onUpgrade, onToggleAlwaysOn, onDuplicate, onDelete, prefetchThreads, index }: WorkspaceCardProps) {
  const { t, i18n } = useTranslation();
  const isMobile = useIsMobile();
  const isFlash = workspace.status === 'flash';

  const tier = normalizeTier(workspace.resource_tier);
  const showTierBadge = !isFlash && tier !== 'standard';
  const showAlwaysOn = !isFlash && workspace.is_always_on === true;
  // Several workspaces share one machine, so the state worth showing is the
  // machine's. The workspace row mirrors it, and is the only source for a
  // workspace that names no computer. The folder is the card title spelled as
  // a path, so the row does not repeat it.
  const machineStatus = computer?.status ?? workspace.status;
  const machineName = computer?.name;
  const showMachine = !isFlash && !!machineName;

  return (
    <div
      className="enter-fade-up"
      style={{ height: WORKSPACE_CARD_HEIGHT, animationDelay: `${(index || 0) * 50}ms` }}
    >
      <div
        className="relative group h-full"
        data-testid="workspace-card"
        onMouseEnter={!isMobile ? () => prefetchThreads?.(workspace.workspace_id) : undefined}
      >
        <div
          onClick={() => onSelect(workspace.workspace_id, workspace.name, workspace.status)}
          className="relative flex cursor-pointer flex-col overflow-hidden rounded-xl py-4 pl-5 pr-4 transition-all ease-in-out hover:shadow-sm active:scale-[0.98] h-full w-full"
          style={{
            // Flash is a system card: flat elevated surface + crisp hairline,
            // a different material from user cards; the amber Zap glyph is the
            // only accent.
            background: isFlash
              ? 'var(--color-bg-elevated)'
              : 'var(--color-bg-card-gradient, linear-gradient(to bottom, var(--color-border-muted), var(--color-border-muted)))',
            border: isFlash
              ? '1px solid var(--color-border-default)'
              : '1px solid var(--color-bg-card-border, var(--color-border-muted))',
            backdropFilter: 'blur(8px)',
            WebkitBackdropFilter: 'blur(8px)',
          }}
        >
          {/* Four rows at gap-3 come to 156px inside the 160px card; the
              machine row pushed the same rows past it at gap-4 and the footer
              was what overflow-hidden clipped. */}
          <div className="flex flex-col flex-grow gap-3">
            <div className="flex items-center pr-10 overflow-hidden gap-2">
              {isFlash && (
                <Zap className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
              )}
              {!isFlash && workspace.is_pinned && (
                <Pin className="h-3.5 w-3.5 flex-shrink-0 rotate-45" style={{ color: 'var(--color-text-tertiary)' }} />
              )}
              <div className="font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
                {workspace.name}
              </div>
            </div>
            <div className="text-sm line-clamp-1 flex-grow" style={{ color: 'var(--color-text-tertiary)' }}>
              {workspace.description || ''}
            </div>
            {showMachine && (
              <div className="text-xs flex items-center gap-1.5 min-w-0" style={{ color: 'var(--color-text-tertiary)' }}>
                <Server className="h-3 w-3 flex-shrink-0" aria-hidden="true" />
                <span className="truncate" title={t('computer.onComputer', { name: machineName })}>
                  {machineName}
                </span>
                <ComputerStatusIndicator status={machineStatus} glyphSize={10} className="flex-shrink-0" />
              </div>
            )}
            <div className="text-xs mt-auto pt-3 flex items-center justify-between gap-2" style={{ color: 'var(--color-text-tertiary)' }}>
              <span className="truncate">
                {t('workspace.updated', { time: workspace.updated_at ? new Date(workspace.updated_at).toLocaleDateString(i18n.language, { month: 'short', day: 'numeric' }) : t('workspace.recently') })}
              </span>
              {(showTierBadge || showAlwaysOn) && (
                <div className="flex items-center gap-1.5 flex-shrink-0">
                  {showTierBadge && (
                    <span
                      className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[0.625rem] font-medium"
                      style={{ backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }}
                      title={t('workspace.tierBadgeTitle', { tier: tierLabel(t, tier) })}
                    >
                      <Cpu className="h-3 w-3" />
                      {tierLabel(t, tier)}
                    </span>
                  )}
                  {showAlwaysOn && (
                    <span
                      className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[0.625rem] font-medium"
                      style={{ backgroundColor: 'var(--color-border-muted)', color: 'var(--color-text-secondary)' }}
                      title={t('workspace.alwaysOnBadgeTitle', 'Always-on, sandbox stays running')}
                    >
                      <InfinityIcon className="h-3 w-3" />
                      {t('workspace.alwaysOnBadge', 'Always-on')}
                    </span>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Menu (no drag handle in normal mode) */}
        {!isFlash && (
          <div className={`absolute top-3 right-3 z-10 transition-opacity ${isMobile ? 'opacity-60' : 'opacity-0 group-focus-within:opacity-100 group-hover:opacity-100'}`}>
            <CardMenu
              workspace={workspace}
              onTogglePin={onTogglePin}
              onRename={onRenameStart}
              onUpgrade={onUpgrade}
              onToggleAlwaysOn={onToggleAlwaysOn}
              onDuplicate={onDuplicate}
              onDelete={onDelete}
            />
          </div>
        )}
      </div>
    </div>
  );
}

export default WorkspaceCard;
