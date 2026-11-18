import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Users } from 'lucide-react';
import { AvatarStack } from '@/components/organization/PersonAvatar';
import { useAccount } from '@/hooks/useAccount';
import { useWorkspaceAudience, useWorkspaceMembers } from '@/hooks/useSharedWorkspaces';
import type { SharedWorkspaceEntry } from '@/types/sharedWorkspaces';
import { MembersDialog } from './MembersDialog';
import { managesIt } from '@/lib/sharedTeams';

/** What says a workspace is shared, under its name: the organization's name,
 *  the team it names once its name is read, and the viewer's managing it;
 *  its people open the members panel. */
export function SharedWorkspaceKind({ entry }: { entry: SharedWorkspaceEntry }) {
  const { t } = useTranslation();
  const { organizationName } = useAccount();
  const teamName = useWorkspaceAudience(entry, { people: false, roster: 'to name' }).team?.name;
  return (
    <span>
      {t('sharedWorkspace.badge')}
      {organizationName ? ` · ${organizationName}` : ''}
      {teamName ? ` · ${teamName}` : ''}
      {managesIt(entry) ? ` · ${t('sharedWorkspace.role.manager')}` : ''}
    </span>
  );
}

export function SharedPeopleButton({ entry, className }: { entry: SharedWorkspaceEntry; className?: string }) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const { data } = useWorkspaceMembers(entry.workspace.workspace_id);
  const people = data?.members ?? [];
  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        className={`tap-target inline-flex items-center gap-2 rounded-full border px-2.5 py-1 text-xs font-medium press hover:bg-(--color-bg-hover) focus-visible:ring-2 focus-visible:ring-ring ${className ?? ''}`}
        style={{ borderColor: 'var(--color-border-muted)', color: 'var(--color-text-secondary)' }}
        aria-label={t('sharedWorkspace.members.openLabel')}
        data-testid="shared-people-button"
      >
        {people.length > 0 ? <AvatarStack people={people} size={18} max={5} /> : <Users aria-hidden="true" className="h-3.5 w-3.5" />}
        <span>{t('sharedWorkspace.members.open')}</span>
      </button>
      <MembersDialog
        workspaceId={entry.workspace.workspace_id}
        workspaceName={entry.workspace.name}
        open={open}
        onClose={() => setOpen(false)}
      />
    </>
  );
}
