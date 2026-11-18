import { useState, useId } from 'react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import { useQueryClient } from '@tanstack/react-query';
import { MoreHorizontal } from 'lucide-react';
import {
  Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle,
} from '@/components/ui/dialog';
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuTrigger, DESTRUCTIVE_ITEM,
} from '@/components/ui/dropdown-menu';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { ToggleSwitch } from '@/components/ui/switch';
import { PersonAvatar } from '@/components/organization/PersonAvatar';
import { useAccount } from '@/hooks/useAccount';
import { useUser } from '@/hooks/useUser';
import { useChangeMembers, useSharedEntry, useWorkspaceAudience, useWorkspaceMembers } from '@/hooks/useSharedWorkspaces';
import { queryKeys } from '@/lib/queryKeys';
import { sharedRefusal, type SharedRefusal } from '@/lib/sharedWorkspaceRefusal';
import { deleteWorkspace } from '@/pages/ChatAgent/utils/api';
import type { SharedMember } from '@/types/sharedWorkspaces';
import { SharedRefusalRow } from './SharedRefusalRow';
import { ShareOutSetting } from './ShareOutSetting';
import { SectionLabel, TeamSection } from './TeamSection';
import { personLabel } from '@/lib/sharedThreads';

interface Props {
  workspaceId: string;
  workspaceName: string;
  open: boolean;
  onClose: () => void;
}

type Confirm = { kind: 'leave' } | { kind: 'remove'; member: SharedMember } | { kind: 'delete' } | null;

/**
 * Who is in a shared workspace and what each may do. Everyone sees the list,
 * and anyone added to it may leave; a manager adds people by email, opens it
 * to the whole organization or names one of their teams, chooses who may
 * share its work outside it, changes roles and removes people, and deletes
 * the workspace. A workspace that names a team shows it first, then the
 * people added individually: someone reaches it through either, so a person
 * in both is marked, and taking them off the list keeps them in.
 * Each refusal the server can give reads as a calm row under the list.
 */
export function MembersDialog({ workspaceId, workspaceName, open, onClose }: Props) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { organizationLabel: org, teams: myTeams } = useAccount();
  const { user } = useUser();
  const me = user?.user_id ?? null;
  const members = useWorkspaceMembers(workspaceId, open);
  const change = useChangeMembers(workspaceId);
  const [email, setEmail] = useState('');
  const [refusal, setRefusal] = useState<SharedRefusal | null>(null);
  const [confirm, setConfirm] = useState<Confirm>(null);
  const [deleting, setDeleting] = useState(false);
  const ids = { email: useId(), everyone: useId() };
  const view = members.data;
  const canManage = !!view?.you.can_manage;
  const busy = change.isPending || deleting;
  const { team, inTeam } = useWorkspaceAudience(useSharedEntry(workspaceId), { people: open, roster: open });
  // Shown where a team is named, and to a manager who has one to name.
  const showTeam = !!view && (!!team || (canManage && myTeams.length > 0));
  const teamName = team?.name ?? null;
  const listed = view?.you.listed !== false;

  const run = async (work: () => Promise<unknown>, address?: string): Promise<boolean> => {
    setRefusal(null);
    try {
      await work();
      return true;
    } catch (err) {
      setRefusal(sharedRefusal(err, address));
      return false;
    }
  };

  /** The workspace is no longer the caller's to open: leave it. */
  const goneFromHere = () => {
    onClose();
    void queryClient.invalidateQueries({ queryKey: queryKeys.sharedWorkspaces.list() });
    navigate('/chat', { replace: true });
  };

  const add = async () => {
    const address = email.trim();
    if (!address) return;
    if (await run(() => change.mutateAsync({ kind: 'add', email: address }), address)) setEmail('');
  };

  const confirmed = async () => {
    if (!confirm) return;
    if (confirm.kind === 'leave' && me) {
      if (await run(() => change.mutateAsync({ kind: 'remove', userId: me }))) goneFromHere();
    } else if (confirm.kind === 'remove') {
      await run(() => change.mutateAsync({ kind: 'remove', userId: confirm.member.user_id }));
    } else if (confirm.kind === 'delete') {
      setDeleting(true);
      const ok = await run(() => deleteWorkspace(workspaceId));
      setDeleting(false);
      if (ok) {
        void queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.all });
        goneFromHere();
      }
    }
    setConfirm(null);
  };

  const addForm = canManage && (
    <form className="flex gap-2" onSubmit={(e) => { e.preventDefault(); void add(); }}>
      <label htmlFor={ids.email} className="sr-only">{t('sharedWorkspace.members.addLabel')}</label>
      <Input
        id={ids.email}
        type="email"
        value={email}
        onChange={(e) => setEmail(e.target.value)}
        placeholder={t('sharedWorkspace.members.addPlaceholder')}
        className="min-w-0 flex-1"
      />
      <Button type="submit" variant="outline" disabled={!email.trim() || busy}>{t('sharedWorkspace.members.add')}</Button>
    </form>
  );

  const roleLabel = (m: SharedMember) => (m.role === 'manager' ? t('sharedWorkspace.role.manager') : t('sharedWorkspace.role.member'));

  return (
    <Dialog open={open} onOpenChange={(next) => { if (!next && !busy) { setConfirm(null); setRefusal(null); onClose(); } }}>
      <DialogContent
        className="sm:max-w-lg sm:max-h-[calc(100dvh-4rem)] sm:overflow-y-auto"
        style={{ backgroundColor: 'var(--color-shell-float, var(--color-bg-page))', borderColor: 'var(--color-shell-edge, var(--color-border-muted))' }}
      >
        <DialogHeader>
          <DialogTitle>{t('sharedWorkspace.members.title')}</DialogTitle>
          <DialogDescription>{t('sharedWorkspace.members.description', { workspace: workspaceName })}</DialogDescription>
        </DialogHeader>

        {members.isError && !view ? (
          <SharedRefusalRow refusal={sharedRefusal(members.error)} />
        ) : (
          <div className="flex min-w-0 flex-col gap-4">
            <div className="flex items-start justify-between gap-3">
              <label htmlFor={ids.everyone} className="flex min-w-0 flex-col gap-0.5 text-sm">
                <span className="font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('sharedWorkspace.everyone', { organization: org })}</span>
                <span style={{ color: 'var(--color-text-secondary)' }}>
                  {canManage
                    ? (team ? t('sharedWorkspace.team.everyoneClears') : t('sharedWorkspace.members.everyoneBody'))
                    : view?.everyone
                      ? t('sharedWorkspace.members.everyoneOn')
                      : team
                        ? t('sharedWorkspace.members.everyoneOffTeam', { team: teamName ?? t('sharedWorkspace.team.unnamedLower') })
                        : t('sharedWorkspace.members.everyoneOff')}
                </span>
              </label>
              {canManage && (
                <ToggleSwitch
                  id={ids.everyone}
                  checked={!!view?.everyone}
                  disabled={busy}
                  onChange={() => void run(() => change.mutateAsync({ kind: 'everyone', everyone: !view?.everyone }))}
                  className="mt-0.5"
                />
              )}
            </div>

            {view && !view.everyone && (
              <ShareOutSetting
                value={view.sharing}
                canManage={canManage}
                disabled={busy}
                organization={org}
                onChange={(sharing) => void run(() => change.mutateAsync({ kind: 'sharing', sharing }))}
              />
            )}

            {showTeam && (
              <div className="flex min-w-0 flex-col gap-3 border-t pt-4" style={{ borderColor: 'var(--color-border-muted)' }}>
                <TeamSection
                  team={team}
                  canManage={canManage}
                  myTeams={myTeams}
                  busy={busy}
                  onChange={(teamId) => void run(() => change.mutateAsync({ kind: 'team', teamId }))}
                />
                <SectionLabel>{t('sharedWorkspace.team.individually')}</SectionLabel>
              </div>
            )}

            {!showTeam && addForm}

            <ul className="flex max-h-[min(50vh,22rem)] flex-col overflow-y-auto" aria-label={t('sharedWorkspace.members.title')} aria-busy={members.isLoading}>
              {(view?.members ?? []).map((m) => {
                const mine = m.user_id === me;
                return (
                  <li key={m.user_id} className="flex min-w-0 items-center gap-3 border-t py-2.5 first:border-t-0" style={{ borderColor: 'var(--color-border-muted)' }}>
                    <PersonAvatar name={m.name} email={m.email} avatarUrl={m.avatar_url} size={28} />
                    <div className="flex min-w-0 flex-1 flex-col">
                      <span className="truncate text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                        {personLabel(t, { name: m.name || m.email, mine: false })}
                        {mine && <span className="ml-1.5 font-normal" style={{ color: 'var(--color-text-tertiary)' }}>{t('sharedWorkspace.you')}</span>}
                      </span>
                      <span className="truncate text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                        {m.active === false
                          ? t('sharedWorkspace.members.noLongerActive')
                          : inTeam(m.user_id)
                            ? t('sharedWorkspace.team.alsoIn', { team: teamName ?? '' })
                            : m.email}
                      </span>
                    </div>
                    <span className="shrink-0 text-xs" style={{ color: 'var(--color-text-secondary)' }}>{roleLabel(m)}</span>
                    {(canManage || mine) && (
                      <DropdownMenu modal={false}>
                        <DropdownMenuTrigger asChild>
                          <button
                            type="button"
                            className="tap-target flex h-7 w-7 shrink-0 items-center justify-center rounded-md hover:bg-(--color-bg-hover) focus-visible:ring-2 focus-visible:ring-ring"
                            style={{ color: 'var(--color-text-tertiary)' }}
                            aria-label={t('sharedWorkspace.members.optionsFor', { name: m.name || m.email || '' })}
                            disabled={busy}
                          >
                            <MoreHorizontal className="h-4 w-4" />
                          </button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent align="end" sideOffset={4}>
                          {canManage && (m.role === 'member' ? (
                            <DropdownMenuItem onSelect={() => void run(() => change.mutateAsync({ kind: 'role', userId: m.user_id, role: 'manager' }))}>
                              {t('sharedWorkspace.members.makeManager')}
                            </DropdownMenuItem>
                          ) : (
                            <DropdownMenuItem onSelect={() => void run(() => change.mutateAsync({ kind: 'role', userId: m.user_id, role: 'member' }))}>
                              {t('sharedWorkspace.members.makeMember')}
                            </DropdownMenuItem>
                          ))}
                          {canManage && <DropdownMenuSeparator />}
                          {mine ? (
                            <DropdownMenuItem className={DESTRUCTIVE_ITEM} onSelect={() => setConfirm({ kind: 'leave' })}>
                              {t('sharedWorkspace.members.leave')}
                            </DropdownMenuItem>
                          ) : (
                            <DropdownMenuItem className={DESTRUCTIVE_ITEM} onSelect={() => setConfirm({ kind: 'remove', member: m })}>
                              {t('sharedWorkspace.members.remove')}
                            </DropdownMenuItem>
                          )}
                        </DropdownMenuContent>
                      </DropdownMenu>
                    )}
                  </li>
                );
              })}
            </ul>

            {showTeam && addForm}

            {refusal && <SharedRefusalRow refusal={refusal} />}

            {confirm ? (
              <div className="flex flex-col gap-3 rounded-lg border px-3.5 py-3 text-sm" style={{ borderColor: 'var(--color-border-muted)', backgroundColor: 'var(--color-bg-card)' }} role="group" aria-label={t('sharedWorkspace.members.confirmLabel')}>
                <span style={{ color: 'var(--color-text-primary)' }}>
                  {confirm.kind === 'leave'
                    ? t('sharedWorkspace.members.leaveConfirm', { workspace: workspaceName })
                    : confirm.kind === 'remove'
                      ? inTeam(confirm.member.user_id)
                        ? t('sharedWorkspace.team.removeConfirmInTeam', { name: confirm.member.name || confirm.member.email || '', team: teamName ?? '' })
                        : t('sharedWorkspace.members.removeConfirm', { name: confirm.member.name || confirm.member.email || '' })
                      : t('sharedWorkspace.members.deleteConfirm', { workspace: workspaceName })}
                </span>
                <div className="flex justify-end gap-2">
                  <Button variant="ghost" size="sm" onClick={() => setConfirm(null)} disabled={busy}>{t('common.cancel')}</Button>
                  <Button variant="destructive" size="sm" onClick={() => void confirmed()} disabled={busy}>
                    {confirm.kind === 'leave' ? t('sharedWorkspace.members.leave') : confirm.kind === 'remove' ? t('sharedWorkspace.members.remove') : t('common.delete')}
                  </Button>
                </div>
              </div>
            ) : (listed || canManage) && (
              <div className="flex flex-wrap justify-between gap-2 border-t pt-3" style={{ borderColor: 'var(--color-border-muted)' }}>
                {listed ? (
                  <Button variant="ghost" size="sm" onClick={() => setConfirm({ kind: 'leave' })} disabled={busy || !view}>
                    {t('sharedWorkspace.members.leaveWorkspace')}
                  </Button>
                ) : <span />}
                {canManage && (
                  <Button variant="ghost" size="sm" className="text-destructive" onClick={() => setConfirm({ kind: 'delete' })} disabled={busy}>
                    {t('sharedWorkspace.members.deleteWorkspace')}
                  </Button>
                )}
              </div>
            )}
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
