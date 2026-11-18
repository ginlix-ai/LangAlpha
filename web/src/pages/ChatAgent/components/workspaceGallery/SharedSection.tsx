import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Plus, Users } from 'lucide-react';
import { AvatarStack } from '@/components/organization/PersonAvatar';
import { RuleRow } from '@/components/organization/RuleRow';
import { CreateSharedWorkspaceDialog } from '@/components/sharedWorkspaces/CreateSharedWorkspaceDialog';
import { useAccount } from '@/hooks/useAccount';
import { useLocale } from '@/hooks/useLocale';
import { useSharedWorkspaces, useWorkspaceAudience } from '@/hooks/useSharedWorkspaces';
import { relativeTime } from '@/lib/format';
import { SHARED_WORKSPACES_CREATE, ruleRefusalCopy } from '@/lib/ruleRefusal';
import type { SharedWorkspaceEntry } from '@/types/sharedWorkspaces';
import { managesIt } from '@/lib/sharedTeams';
import { Kicker } from './Kicker';
import { GALLERY_GRID } from './layout';
import { CARD_BODY, CARD_SURFACE } from './cardSurface';

/** The rule that keeps "New shared workspace" away, worded as a refusal of
 *  it would be, where the button would be. */
function CreateRuleRow({ organizationName }: { organizationName: string | null }) {
  const { t } = useTranslation();
  const refusal = { rule: SHARED_WORKSPACES_CREATE, setBy: 'organization', message: '' } as const;
  const copy = ruleRefusalCopy(t, refusal, organizationName, (model) => model);
  return <RuleRow title={copy.title} body={copy.body} className="mt-3" />;
}

/** Who is in it, read per card: a shared workspace's list carries no people. */
function SharedCardPeople({ entry, organization }: { entry: SharedWorkspaceEntry; organization: string }) {
  const { t } = useTranslation();
  const { team, listed } = useWorkspaceAudience(entry, { people: true, roster: 'to name' });
  const people = listed ?? [];
  const label = entry.shared.everyone
    ? t('sharedWorkspace.everyone', { organization })
    : team
      ? t('sharedWorkspace.teamAndPeople', { team: team.name ?? t('sharedWorkspace.team.unnamed'), count: people.length })
      : t('sharedWorkspace.peopleCount', { count: people.length });
  return (
    <div className="mt-auto flex min-w-0 items-center gap-2 pt-2.5 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
      {people.length > 0 ? <AvatarStack people={people} size={18} /> : <Users aria-hidden="true" className="h-3.5 w-3.5" />}
      <span className="min-w-0 truncate">{listed || entry.shared.everyone ? label : ' '}</span>
      {managesIt(entry) && (
        <span className="ml-auto shrink-0">{t('sharedWorkspace.role.manager')}</span>
      )}
    </div>
  );
}

function SharedWorkspaceCard({ entry, now, organization, onSelect }: {
  entry: SharedWorkspaceEntry;
  now: number;
  organization: string;
  onSelect: (wsId: string, name?: string, status?: string) => void;
}) {
  const locale = useLocale();
  const ws = entry.workspace;
  const when = relativeTime(ws.last_activity_at ?? ws.updated_at, locale, now);
  const open = () => onSelect(ws.workspace_id, ws.name, ws.status);
  return (
    <div className="relative h-full rounded-xl hover-lift" data-testid="shared-workspace-card">
      <div
        role="button"
        tabIndex={0}
        aria-label={ws.name}
        onClick={open}
        onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); open(); } }}
        className={`${CARD_BODY} cursor-pointer press`}
        style={CARD_SURFACE}
      >
        <div className="pf-card-head flex items-start gap-2 min-w-0">
          <div data-bracket="title" className="pf-card-title font-medium flex-1 min-w-0 line-clamp-1 break-words" style={{ color: 'var(--color-text-primary)' }} title={ws.name}>
            <span className="pf-card-name">{ws.name}</span>
          </div>
          {when && (
            <span className="pf-card-meta shrink-0 pt-0.5 text-xs whitespace-nowrap" style={{ color: 'var(--color-text-tertiary)' }}>{when}</span>
          )}
        </div>
        <div className="pf-card-body text-sm line-clamp-2 min-h-[2lh]" style={{ color: 'var(--color-text-tertiary)' }}>
          {ws.description}
        </div>
        <SharedCardPeople entry={entry} organization={organization} />
      </div>
    </div>
  );
}

/**
 * The organization's workspaces the caller is in, below their own: a section
 * of its own, labeled with the organization, so a personal workspace never
 * reads as shared or the other way round. Absent unless shared workspaces
 * are on for the account.
 */
export function SharedSection({ now, query, onWorkspaceSelect }: {
  now: number;
  /** The gallery's search, which narrows these cards too. */
  query: string;
  onWorkspaceSelect: (wsId: string, name?: string, status?: string) => void;
}) {
  const { t } = useTranslation();
  const { organizationName, organizationLabel: organization } = useAccount();
  const { data, enabled } = useSharedWorkspaces();
  const [creating, setCreating] = useState(false);
  if (!enabled || !data) return null;

  const q = query.trim().toLowerCase();
  const entries = data.workspaces.filter((e) => !q || e.workspace.name.toLowerCase().includes(q));
  // Nothing to show under a search that matched none of them, nor for a
  // member in none who may not make one: the section would only say no.
  if (q && entries.length === 0) return null;
  if (data.workspaces.length === 0 && !data.can_create) return null;

  return (
    <section className="mt-6 md:mt-7" aria-label={t('sharedWorkspace.sectionLabel', { organization })} data-testid="shared-workspaces-section">
      <div className="mb-2.5 flex min-h-7 items-center justify-between gap-3">
        <Kicker className="min-w-0 truncate">{t('sharedWorkspace.kicker', { organization })}{entries.length > 0 ? ` · ${entries.length}` : ''}</Kicker>
        {data.can_create && entries.length > 0 && (
          <button
            type="button"
            onClick={() => setCreating(true)}
            className="tap-target -my-1 inline-flex shrink-0 items-center gap-1.5 whitespace-nowrap rounded-md px-2 py-1 text-xs font-medium hover:bg-(--color-bg-hover) focus-visible:ring-2 focus-visible:ring-ring"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            <Plus aria-hidden="true" className="h-3.5 w-3.5" />
            {t('sharedWorkspace.new')}
          </button>
        )}
      </div>
      {entries.length > 0 ? (
        <>
          <div className={GALLERY_GRID}>
            {entries.map((entry) => (
              <SharedWorkspaceCard key={entry.workspace.workspace_id} entry={entry} now={now} organization={organization} onSelect={onWorkspaceSelect} />
            ))}
          </div>
          {/* Where "New shared workspace" would be, the rule that keeps it
              away, as any rule-refused action says. */}
          {!data.can_create && <CreateRuleRow organizationName={organizationName} />}
        </>
      ) : (
        <div
          className="flex flex-col items-start gap-3 rounded-xl border border-dashed px-5 py-4 text-sm sm:flex-row sm:items-center sm:justify-between"
          style={{ borderColor: 'var(--color-border-default)', color: 'var(--color-text-secondary)' }}
        >
          <span>{t('sharedWorkspace.empty', { organization })}</span>
          <button
            type="button"
            onClick={() => setCreating(true)}
            className="flex h-9 shrink-0 items-center gap-2 rounded-lg border px-4 text-sm font-medium press hover:bg-foreground/5"
            style={{ borderColor: 'var(--color-border-default)', color: 'var(--color-text-primary)', backgroundColor: 'var(--color-bg-page)' }}
          >
            <Plus aria-hidden="true" className="h-4 w-4" />
            {t('sharedWorkspace.new')}
          </button>
        </div>
      )}
      <CreateSharedWorkspaceDialog
        open={creating}
        onClose={() => setCreating(false)}
        onCreated={(entry) => {
          setCreating(false);
          onWorkspaceSelect(entry.workspace.workspace_id, entry.workspace.name, entry.workspace.status);
        }}
      />
    </section>
  );
}
