import { useId } from 'react';
import type { TFunction } from 'i18next';
import { useTranslation } from 'react-i18next';
import { ArrowRightLeft, Check, ChevronDown, FolderOpen, Globe, House, PowerOff } from 'lucide-react';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuCheckboxItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuSub,
  DropdownMenuSubTrigger,
  DropdownMenuSubContent,
} from '@/components/ui/dropdown-menu';
import { scopeOff, type ScopeOff } from '../utils/scopeOff';

/**
 * The scope control on a Plugins row: where a skill / MCP server lives and
 * where it is active. Shared by the Skills and MCP tabs.
 *
 * Tier is a property (all workspaces vs one workspace) changed only by an
 * explicit Move action. The per-workspace checklist on an all-workspaces row
 * drives the deny-list (per-workspace disables / tombstones) and deliberately
 * never changes tier. A checklist switch changes today's workspaces only:
 * what a workspace created later starts with is the row's own setting where
 * it has one (a user MCP server, "On in new workspaces"), and on for
 * everything else.
 */

export interface ScopeWorkspace {
  id: string;
  name: string;
}

/**
 * Whether a row's per-workspace switches are locked, from its *effective*
 * state.
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
  return scopeOff(row) !== null;
}

export type ScopeReach =
  | { kind: 'all' }
  | { kind: 'allExcept'; count: number }
  | { kind: 'none' }
  | { kind: 'only'; name: string }
  | { kind: 'some'; count: number };

/**
 * The checklist a user-tier row offers, the workspaces in it that switched the
 * row off, and the reach that follows, in one pass so the badge, the menu and
 * every cell that names a reach cannot read them differently. Only listed
 * workspaces count as off: a stale disable for a deleted one would otherwise
 * inflate "except N".
 *
 * A row new workspaces start with reads as the deny-list it is: all
 * workspaces, less the ones that switched it off. A row they start without
 * reads as the list of workspaces it is actually on in, Home included when
 * the checklist offers it, because "All workspaces" there would promise the
 * next workspace a server it will not have. `undefined` is a backend that
 * predates the setting, where every row still inherited.
 */
export function readChecklist(
  workspaces: ScopeWorkspace[],
  disabledWorkspaceIds: string[] = [],
  homeWorkspace?: ScopeWorkspace,
  newWorkspacesOn?: boolean,
): {
  checklist: ScopeWorkspace[];
  disabled: ReadonlySet<string>;
  active: ScopeWorkspace[];
  reach: ScopeReach;
} {
  const checklist = homeWorkspace ? [...workspaces, homeWorkspace] : workspaces;
  const disabled = new Set(disabledWorkspaceIds);
  const active = checklist.filter((w) => !disabled.has(w.id));
  const off = checklist.length - active.length;
  let reach: ScopeReach;
  if (newWorkspacesOn !== false) {
    reach = off > 0 ? { kind: 'allExcept', count: off } : { kind: 'all' };
  } else if (active.length === 0) {
    reach = { kind: 'none' };
  } else if (active.length === 1) {
    reach = { kind: 'only', name: active[0].name };
  } else {
    reach = { kind: 'some', count: active.length };
  }
  return { checklist, disabled, active, reach };
}

/** A reach in the words the badge uses, which every cell naming one repeats. */
export function reachText(t: TFunction, reach: ScopeReach): string {
  switch (reach.kind) {
    case 'all':
      return t('plugins.scope.allWorkspaces');
    case 'allExcept':
      return t('plugins.scope.allExcept', { count: reach.count });
    case 'none':
      return t('plugins.scope.noWorkspaces');
    case 'only':
      return t('plugins.scope.onlyIn', { name: reach.name });
    case 'some':
      return t('plugins.scope.someWorkspaces', { count: reach.count });
  }
}

export function ScopeControl({
  workspaces,
  scopeWorkspaceId,
  disabledWorkspaceIds = [],
  off = null,
  moveBlockedReason = null,
  moveToAllBlockedReason = null,
  busy = false,
  homeWorkspace,
  newWorkspacesOn,
  loading = false,
  onSetWorkspaceDisabled,
  onSetNewWorkspacesOn,
  onMove,
}: {
  workspaces: ScopeWorkspace[];
  /** null = all-workspaces tier; a workspace id = scoped to that workspace. */
  scopeWorkspaceId: string | null;
  disabledWorkspaceIds?: string[];
  /** The row is off account-wide (`scopeOff`). An all-workspaces row then
   * says so instead of a reach that does not apply, and its menu leads with
   * the step that comes first in place of a checklist that could change
   * nothing yet: a workspace re-enable would 409 until the row is back on. */
  off?: ScopeOff;
  /** Shown instead of the move actions (e.g. a plugin-owned skill, which
   * stays at the account level with its package). */
  moveBlockedReason?: string | null;
  /** Blocks only the move-to-all-workspaces destination (e.g. a shadowing
   * row whose name is known to collide there); workspace targets stay
   * offered. */
  moveToAllBlockedReason?: string | null;
  busy?: boolean;
  /** Adds Home to the checklist, and to nothing else: a row can be switched
   * off there, but never moved there. */
  homeWorkspace?: ScopeWorkspace;
  /** What a workspace created later starts with, on rows that have their own
   * setting. `false` also switches the badge from the deny-list wording to
   * the workspaces the row is on in. */
  newWorkspacesOn?: boolean;
  /** `workspaces` has not arrived yet: the pill waits rather than name a reach
   * counted against an empty list. */
  loading?: boolean;
  onSetWorkspaceDisabled?: (workspaceId: string, disabled: boolean) => void;
  /** Offers "On in new workspaces" in place of the static hint. Rows whose
   * new workspaces always start on (builtins, skills) leave this unset. */
  onSetNewWorkspacesOn?: (on: boolean) => void;
  onMove?: (toWorkspaceId: string | null) => void;
}) {
  const { t } = useTranslation();
  const useInId = useId();
  const moveId = useId();
  if (loading) return null;
  const isUserTier = scopeWorkspaceId === null;
  const scopeWorkspace = workspaces.find((w) => w.id === scopeWorkspaceId);
  const { checklist, disabled, reach } = readChecklist(
    workspaces,
    disabledWorkspaceIds,
    homeWorkspace,
    newWorkspacesOn,
  );
  const inherits = reach.kind === 'all' || reach.kind === 'allExcept';
  // A workspace's own row is off in the one place it lives; its pill names
  // that place either way, so only the all-workspaces reach gives way.
  const offAll = isUserTier ? off : null;
  const workspaceName = scopeWorkspace?.name ?? t('plugins.scope.unknownWorkspace');

  const label =
    offAll === 'plugin'
      ? t('plugins.scope.pluginOff')
      : offAll === 'self'
        ? t('plugins.scope.offEverywhere')
        : isUserTier
          ? reachText(t, reach)
          : workspaceName;
  const Icon = offAll ? PowerOff : isUserTier && inherits ? Globe : FolderOpen;
  // Only a label that carries a workspace name can outgrow the pill, so only
  // that one needs the full text on hover.
  const labelTitle = offAll || (isUserTier && reach.kind !== 'only') ? undefined : label;

  const hasChecklist =
    isUserTier && !!onSetWorkspaceDisabled && checklist.length > 0;
  // Not gated on the checklist: with no workspace yet to list, what the next
  // one starts with is the only scope a row has.
  const hasNewWorkspaces = isUserTier && !!onSetNewWorkspacesOn;
  const newOn = newWorkspacesOn !== false;
  const moveTargets = workspaces.filter((w) => w.id !== scopeWorkspaceId);
  const hasMove =
    !!onMove &&
    (moveBlockedReason !== null || !isUserTier || moveTargets.length > 0);

  const badge = (
    <span
      title={labelTitle}
      className="inline-flex items-center gap-1 px-2 py-1 text-[0.6875rem] rounded-md whitespace-nowrap max-w-[12rem]"
      style={{ color: 'var(--color-text-secondary)', border: '1px solid var(--color-border-muted)' }}
    >
      <Icon className="h-3 w-3 shrink-0" />
      <span className="truncate">{label}</span>
    </span>
  );

  if (!hasChecklist && !hasNewWorkspaces && !hasMove) return badge;

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <button
          type="button"
          disabled={busy}
          aria-label={t('plugins.scope.triggerAria', { scope: label })}
          title={labelTitle}
          className="inline-flex items-center gap-1 px-2 py-1 text-[0.6875rem] rounded-md transition-colors hover:bg-(--color-bg-hover) disabled:opacity-50 disabled:hover:bg-transparent whitespace-nowrap max-w-[12rem]"
          style={{ color: 'var(--color-text-secondary)', border: '1px solid var(--color-border-muted)' }}
        >
          <Icon className="h-3 w-3 shrink-0" />
          <span className="truncate">{label}</span>
          <ChevronDown className="h-3 w-3 shrink-0" />
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        {hasChecklist && offAll ? (
          // Locked boxes would read as a choice still on offer; until the row
          // is back on, the step that comes first is the whole answer.
          <DropdownMenuLabel
            className="font-normal max-w-[16rem] whitespace-normal"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {offAll === 'plugin' ? t('plugins.scope.pluginOffHint') : t('plugins.scope.offHint')}
          </DropdownMenuLabel>
        ) : hasChecklist ? (
          // A named group, so a screen reader hears which half an item is in.
          <div role="group" aria-labelledby={useInId}>
            <DropdownMenuLabel id={useInId}>{t('plugins.scope.activeIn')}</DropdownMenuLabel>
            {checklist.map((ws) => {
              const active = !disabled.has(ws.id);
              return (
                <DropdownMenuCheckboxItem
                  key={ws.id}
                  checked={active}
                  disabled={busy}
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
                  {ws === homeWorkspace ? (
                    <House className="h-3.5 w-3.5 mr-1.5 shrink-0" />
                  ) : null}
                  <span className="truncate">{ws.name}</span>
                </DropdownMenuCheckboxItem>
              );
            })}
            {!hasNewWorkspaces && (
              <DropdownMenuLabel
                className="font-normal"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {t('plugins.scope.futureHint')}
              </DropdownMenuLabel>
            )}
          </div>
        ) : null}
        {hasNewWorkspaces && (
          <>
            {/* Apart from the checklist: it is not a workspace, and a flip
                here changes none of the ones listed above. */}
            {hasChecklist && <DropdownMenuSeparator />}
            <DropdownMenuCheckboxItem
              checked={newOn}
              // Live while the row is off account-wide: the setting is stored
              // on the row and no workspace re-enable is involved, so nothing
              // 409s.
              disabled={busy}
              onSelect={(e) => {
                // Keep the menu open, as the checklist does.
                e.preventDefault();
                onSetNewWorkspacesOn?.(!newOn);
              }}
            >
              <Check className="h-3.5 w-3.5 mr-2" style={{ opacity: newOn ? 1 : 0 }} />
              <span>{t('plugins.scope.newWorkspacesOn')}</span>
            </DropdownMenuCheckboxItem>
          </>
        )}
        {(hasChecklist || hasNewWorkspaces) && hasMove && <DropdownMenuSeparator />}
        {hasMove && (
          <div role="group" aria-labelledby={moveId}>
            {/* A move changes where the row lives, not where it is on, and
                the checklist above is easy to mistake for it. So the half
                opens on what a move means from here: into one workspace at
                the cost of every other, or, for a workspace's own row, the
                one place it lives now, which each move below widens or
                trades. */}
            <DropdownMenuLabel id={moveId} className="max-w-[16rem] whitespace-normal">
              {isUserTier
                ? t('plugins.scope.moveIntoOne')
                : t('plugins.scope.onlyIn', { name: workspaceName })}
            </DropdownMenuLabel>
            {moveBlockedReason !== null ? (
              <DropdownMenuItem disabled>
                <ArrowRightLeft className="h-3.5 w-3.5 mr-2" />
                <span className="max-w-[16rem] whitespace-normal">
                  {moveBlockedReason}
                </span>
              </DropdownMenuItem>
            ) : (
              <>
                {isUserTier && (
                  <DropdownMenuLabel
                    className="font-normal pt-0 max-w-[16rem] whitespace-normal"
                    style={{ color: 'var(--color-text-tertiary)' }}
                  >
                    {t('plugins.scope.moveIntoOneNote')}
                  </DropdownMenuLabel>
                )}
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
                {moveTargets.length > 0 && (
                  <DropdownMenuSub>
                    <DropdownMenuSubTrigger>
                      <ArrowRightLeft className="h-3.5 w-3.5 mr-2" />
                      {isUserTier
                        ? t('plugins.scope.chooseWorkspace')
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
            )}
          </div>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
