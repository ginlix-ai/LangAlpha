import { useTranslation } from 'react-i18next';
import { ArrowRightLeft, Check, ChevronDown, FolderOpen, Globe } from 'lucide-react';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuSub,
  DropdownMenuSubTrigger,
  DropdownMenuSubContent,
} from '@/components/ui/dropdown-menu';

/**
 * The scope control on a Plugins row: where a skill / MCP server lives and
 * where it is active. Shared by the Skills and MCP tabs.
 *
 * Tier is a property (all workspaces vs one workspace) changed only by an
 * explicit Move action. The per-workspace checklist on an all-workspaces row
 * drives the deny-list (per-workspace disables / tombstones) and deliberately
 * never changes tier — deny-list semantics mean a workspace created later
 * starts with the item enabled.
 */

export interface ScopeWorkspace {
  id: string;
  name: string;
}

/**
 * Whether a row's checklist has to lock, from its *effective* state.
 *
 * A workspace can add a deny-marker for anything, but removing one goes
 * through a re-enable that 409s whenever the account tier already subtracts
 * the row -- by its own switch, or by the package that ships it being off.
 * Reading only `enabled` leaves an interactive control that can make a change
 * it cannot take back, so the two conditions live together here rather than
 * being restated at each call site, where one of them kept being forgotten.
 */
export function scopeLocked(row: {
  enabled?: boolean | null;
  plugin_enabled?: boolean | null;
}): boolean {
  return !row.enabled || row.plugin_enabled === false;
}

export function ScopeControl({
  workspaces,
  scopeWorkspaceId,
  disabledWorkspaceIds = [],
  checklistLocked = false,
  allowWorkspaceTargets = true,
  moveBlockedReason = null,
  moveToAllBlockedReason = null,
  busy = false,
  onSetWorkspaceDisabled,
  onMove,
}: {
  workspaces: ScopeWorkspace[];
  /** null = all-workspaces tier; a workspace id = scoped to that workspace. */
  scopeWorkspaceId: string | null;
  disabledWorkspaceIds?: string[];
  /** Lock the checklist, e.g. when the row itself is disabled account-wide
   * (a workspace re-enable would 409 against the asymmetry rule). */
  checklistLocked?: boolean;
  /** Offer workspace destinations for the move (false = only the user tier,
   * for items without a cross-workspace move path). */
  allowWorkspaceTargets?: boolean;
  /** Shown instead of the move actions (e.g. the OAuth guard). */
  moveBlockedReason?: string | null;
  /** Blocks only the move-to-all-workspaces destination (e.g. a shadowing
   * row whose name is known to collide there); workspace targets stay
   * offered. */
  moveToAllBlockedReason?: string | null;
  busy?: boolean;
  onSetWorkspaceDisabled?: (workspaceId: string, disabled: boolean) => void;
  onMove?: (toWorkspaceId: string | null) => void;
}) {
  const { t } = useTranslation();
  const isUserTier = scopeWorkspaceId === null;
  const scopeWorkspace = workspaces.find((w) => w.id === scopeWorkspaceId);
  const disabledSet = new Set(disabledWorkspaceIds);
  // Only disables that name a live workspace count toward the label — stale
  // rows for deleted workspaces would otherwise inflate "except N".
  const activeDisables = workspaces.filter((w) => disabledSet.has(w.id));

  const label = isUserTier
    ? activeDisables.length > 0
      ? t('plugins.scope.allExcept', { count: activeDisables.length })
      : t('plugins.scope.allWorkspaces')
    : scopeWorkspace?.name ?? t('plugins.scope.unknownWorkspace');
  const Icon = isUserTier ? Globe : FolderOpen;

  const hasChecklist =
    isUserTier && !!onSetWorkspaceDisabled && workspaces.length > 0;
  const moveTargets = workspaces.filter((w) => w.id !== scopeWorkspaceId);
  const hasMove =
    !!onMove &&
    (moveBlockedReason !== null ||
      !isUserTier ||
      (allowWorkspaceTargets && moveTargets.length > 0));

  const badge = (
    <span
      className="inline-flex items-center gap-1 px-2 py-1 text-[0.6875rem] rounded-md whitespace-nowrap"
      style={{ color: 'var(--color-text-secondary)', border: '1px solid var(--color-border-muted)' }}
    >
      <Icon className="h-3 w-3" />
      {label}
    </span>
  );

  if (!hasChecklist && !hasMove) return badge;

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <button
          type="button"
          disabled={busy}
          aria-label={t('plugins.scope.triggerAria', { scope: label })}
          className="inline-flex items-center gap-1 px-2 py-1 text-[0.6875rem] rounded-md transition-colors hover:bg-[var(--color-bg-hover)] disabled:opacity-50 disabled:hover:bg-transparent whitespace-nowrap"
          style={{ color: 'var(--color-text-secondary)', border: '1px solid var(--color-border-muted)' }}
        >
          <Icon className="h-3 w-3" />
          {label}
          <ChevronDown className="h-3 w-3" />
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        {hasChecklist && (
          <>
            <DropdownMenuLabel>{t('plugins.scope.activeIn')}</DropdownMenuLabel>
            {workspaces.map((ws) => {
              const active = !disabledSet.has(ws.id);
              return (
                <DropdownMenuItem
                  key={ws.id}
                  disabled={checklistLocked || busy}
                  onSelect={(e) => {
                    // Keep the menu open: the checklist is a multi-toggle.
                    e.preventDefault();
                    onSetWorkspaceDisabled?.(ws.id, active);
                  }}
                >
                  <Check
                    className="h-3.5 w-3.5 mr-2"
                    style={{ opacity: active ? 1 : 0 }}
                  />
                  <span className="truncate">{ws.name}</span>
                </DropdownMenuItem>
              );
            })}
            <DropdownMenuLabel
              className="font-normal"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {t('plugins.scope.futureHint')}
            </DropdownMenuLabel>
          </>
        )}
        {hasChecklist && hasMove && <DropdownMenuSeparator />}
        {hasMove && moveBlockedReason !== null ? (
          <DropdownMenuItem disabled>
            <ArrowRightLeft className="h-3.5 w-3.5 mr-2" />
            <span className="max-w-[16rem] whitespace-normal">
              {moveBlockedReason}
            </span>
          </DropdownMenuItem>
        ) : hasMove ? (
          <>
            {!isUserTier &&
              (moveToAllBlockedReason !== null ? (
                <DropdownMenuItem disabled>
                  <Globe className="h-3.5 w-3.5 mr-2" />
                  <span className="max-w-[16rem] whitespace-normal">
                    {moveToAllBlockedReason}
                  </span>
                </DropdownMenuItem>
              ) : (
                <DropdownMenuItem disabled={busy} onSelect={() => onMove?.(null)}>
                  <Globe className="h-3.5 w-3.5 mr-2" />
                  {t('plugins.scope.moveToAll')}
                </DropdownMenuItem>
              ))}
            {allowWorkspaceTargets && moveTargets.length > 0 && (
              <DropdownMenuSub>
                <DropdownMenuSubTrigger>
                  <ArrowRightLeft className="h-3.5 w-3.5 mr-2" />
                  {isUserTier
                    ? t('plugins.scope.moveToWorkspace')
                    : t('plugins.scope.moveToAnother')}
                </DropdownMenuSubTrigger>
                <DropdownMenuSubContent>
                  {moveTargets.map((ws) => (
                    <DropdownMenuItem
                      key={ws.id}
                      disabled={busy}
                      onSelect={() => onMove?.(ws.id)}
                    >
                      <span className="truncate">{ws.name}</span>
                    </DropdownMenuItem>
                  ))}
                </DropdownMenuSubContent>
              </DropdownMenuSub>
            )}
          </>
        ) : null}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
