import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { Check, Users, X } from 'lucide-react';
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import type { AccountTeam } from '@/types/api';
import type { AudienceTeam } from '@/hooks/useSharedWorkspaces';

/** A section's small label in the People dialog, in the data face the app's
 *  kickers use. */
export function SectionLabel({ children }: { children: ReactNode }) {
  return (
    <div
      className="font-['JetBrains_Mono','Menlo',monospace] text-[11px] font-medium uppercase tracking-[0.06em]"
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      {children}
    </div>
  );
}

/** The people mark in the avatar column, so the team's row starts where each
 *  person's does. */
function TeamMark({ size }: { size: number }) {
  return (
    <span
      aria-hidden="true"
      className="inline-flex shrink-0 items-center justify-center rounded-full"
      style={{
        width: size,
        height: size,
        background: 'linear-gradient(var(--color-accent-soft), var(--color-accent-soft)), var(--color-bg-page)',
        color: 'var(--color-accent-light)',
      }}
    >
      <Users style={{ width: size * 0.5, height: size * 0.5 }} />
    </span>
  );
}

/**
 * The team a shared workspace names, as the People dialog's first section:
 * its name and how many people it brings, read from the auth service when
 * the dialog opens, and for whoever may manage the workspace, a menu to
 * name another of their own teams or none. A workspace naming no team shows
 * the row only to a manager who has a team to name.
 */
export function TeamSection({ team, canManage, myTeams, busy, onChange }: {
  team: AudienceTeam | null;
  canManage: boolean;
  /** The caller's own teams: the only ones they may name. */
  myTeams: AccountTeam[];
  busy: boolean;
  onChange: (teamId: string | null) => void;
}) {
  const { t } = useTranslation();
  const name = team ? team.name ?? t('sharedWorkspace.team.unnamed') : t('sharedWorkspace.team.none');
  const count = team?.people?.length ?? 0;
  const sub = !team
    ? t('sharedWorkspace.team.noneBody')
    : team.failed
      ? t('sharedWorkspace.team.loadFailed')
      : team.loading && !team.people
        ? ' '
        : t('sharedWorkspace.team.people', { count });
  const offered = canManage && (myTeams.length > 0 || !!team);

  return (
    <div className="flex min-w-0 flex-col gap-1">
      <SectionLabel>{t('sharedWorkspace.team.label')}</SectionLabel>
      <div className="flex min-w-0 items-center gap-3 py-2.5" data-testid="shared-team-row">
        <TeamMark size={28} />
        <div className="flex min-w-0 flex-1 flex-col">
          <span className="truncate text-sm font-medium" style={{ color: team ? 'var(--color-text-primary)' : 'var(--color-text-secondary)' }}>{name}</span>
          <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }} aria-live="polite">{sub}</span>
        </div>
        {offered && (
          <DropdownMenu modal={false}>
            <DropdownMenuTrigger asChild>
              <button
                type="button"
                className="tap-target shrink-0 rounded-md px-2 py-1 text-xs font-medium hover:bg-(--color-bg-hover) focus-visible:ring-2 focus-visible:ring-ring"
                style={{ color: 'var(--color-text-secondary)' }}
                disabled={busy}
                aria-label={t('sharedWorkspace.team.optionsLabel')}
              >
                {team ? t('sharedWorkspace.team.change') : t('sharedWorkspace.team.choose')}
              </button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" sideOffset={4} className="min-w-48">
              {myTeams.map((mine) => (
                <DropdownMenuItem key={mine.id} onSelect={() => { if (mine.id !== team?.id) onChange(mine.id); }}>
                  <Users className="h-4 w-4" />
                  <span className="min-w-0 flex-1 truncate">{mine.name}</span>
                  {mine.id === team?.id && <Check aria-label={t('sharedWorkspace.team.current')} className="ml-auto h-4 w-4" />}
                </DropdownMenuItem>
              ))}
              {team && myTeams.length > 0 && <DropdownMenuSeparator />}
              {team && (
                <DropdownMenuItem onSelect={() => onChange(null)}>
                  <X className="h-4 w-4" />
                  {t('sharedWorkspace.team.remove')}
                </DropdownMenuItem>
              )}
            </DropdownMenuContent>
          </DropdownMenu>
        )}
      </div>
    </div>
  );
}
