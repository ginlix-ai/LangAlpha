import { useState, useId } from 'react';
import { useTranslation } from 'react-i18next';
import {
  Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Button } from '@/components/ui/button';
import { Select } from '@/components/ui/select';
import { ToggleSwitch } from '@/components/ui/switch';
import { useAccount } from '@/hooks/useAccount';
import { useCreateSharedWorkspace } from '@/hooks/useSharedWorkspaces';
import { parseEmails } from '@/lib/emailList';
import { sharedRefusal, type SharedRefusal } from '@/lib/sharedWorkspaceRefusal';
import { clampWorkspaceName } from '@/pages/ChatAgent/utils/workspaceName';
import type { SharedWorkspaceEntry } from '@/types/sharedWorkspaces';
import { SharedRefusalRow } from './SharedRefusalRow';

interface Props {
  open: boolean;
  onClose: () => void;
  onCreated: (entry: SharedWorkspaceEntry) => void;
}

/** A new shared workspace: its name, what it is for, and who is in it: one of
 *  the creator's teams if they choose one, people by email, or everyone in
 *  the organization instead of a team. The creator is its first manager. */
export function CreateSharedWorkspaceDialog({ open, onClose, onCreated }: Props) {
  const { t } = useTranslation();
  const { organizationLabel: org, teams } = useAccount();
  const create = useCreateSharedWorkspace();
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [emails, setEmails] = useState('');
  const [everyone, setEveryone] = useState(false);
  const [teamId, setTeamId] = useState('');
  const [refusal, setRefusal] = useState<SharedRefusal | null>(null);
  const ids = { name: useId(), description: useId(), emails: useId(), everyone: useId(), team: useId() };
  // A team chosen and then gone from the account (it was left meanwhile) is not sent.
  const team = teams.some((x) => x.id === teamId) ? teamId : '';
  const busy = create.isPending;
  const trimmed = name.trim();

  const reset = () => {
    setName(''); setDescription(''); setEmails(''); setEveryone(false); setTeamId(''); setRefusal(null);
  };
  const close = () => { if (!busy) { reset(); onClose(); } };

  const submit = async () => {
    if (!trimmed || busy) return;
    setRefusal(null);
    const members = parseEmails(emails);
    try {
      const entry = await create.mutateAsync({
        name: trimmed,
        ...(description.trim() ? { description: description.trim() } : {}),
        everyone,
        members,
        ...(team && !everyone ? { team_id: team } : {}),
      });
      reset();
      onCreated(entry);
    } catch (err) {
      setRefusal(sharedRefusal(err));
    }
  };

  return (
    <Dialog open={open} onOpenChange={(next) => { if (!next) close(); }}>
      <DialogContent style={{ backgroundColor: 'var(--color-shell-float, var(--color-bg-page))', borderColor: 'var(--color-shell-edge, var(--color-border-muted))' }}>
        <DialogHeader>
          <DialogTitle>{t('sharedWorkspace.create.title')}</DialogTitle>
          <DialogDescription>{t('sharedWorkspace.create.description', { organization: org })}</DialogDescription>
        </DialogHeader>
        <form
          className="flex flex-col gap-4"
          onSubmit={(e) => { e.preventDefault(); void submit(); }}
        >
          <div className="flex flex-col gap-1.5">
            <label htmlFor={ids.name} className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
              {t('sharedWorkspace.create.name')}
            </label>
            <Input
              id={ids.name}
              value={name}
              onChange={(e) => setName(clampWorkspaceName(e.target.value))}
              placeholder={t('sharedWorkspace.create.namePlaceholder')}
              autoFocus
            />
          </div>
          <div className="flex flex-col gap-1.5">
            <label htmlFor={ids.description} className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
              {t('sharedWorkspace.create.purpose')}
              <span className="ml-1.5 font-normal" style={{ color: 'var(--color-text-tertiary)' }}>{t('common.optional')}</span>
            </label>
            <Textarea
              id={ids.description}
              value={description}
              onChange={(e) => setDescription(e.target.value.slice(0, 1000))}
              rows={2}
              className="min-h-[64px]"
            />
          </div>
          <div className="flex items-start justify-between gap-3">
            <label htmlFor={ids.everyone} className="flex flex-col gap-0.5 text-sm">
              <span className="font-medium" style={{ color: 'var(--color-text-primary)' }}>
                {t('sharedWorkspace.everyone', { organization: org })}
              </span>
              <span style={{ color: 'var(--color-text-secondary)' }}>{t('sharedWorkspace.create.everyoneBody')}</span>
            </label>
            <ToggleSwitch
              id={ids.everyone}
              checked={everyone}
              onChange={() => { setEveryone((v) => !v); setTeamId(''); }}
              className="mt-0.5"
            />
          </div>
          {teams.length > 0 && !everyone && (
            <div className="flex flex-col gap-1.5">
              <label htmlFor={ids.team} className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                {t('sharedWorkspace.team.label')}
                <span className="ml-1.5 font-normal" style={{ color: 'var(--color-text-tertiary)' }}>{t('common.optional')}</span>
              </label>
              <Select
                id={ids.team}
                value={team}
                onChange={(e) => setTeamId(e.target.value)}
                aria-describedby={`${ids.team}-hint`}
                data-testid="create-team-picker"
              >
                <option value="">{t('sharedWorkspace.team.none')}</option>
                {teams.map((x) => <option key={x.id} value={x.id}>{x.name}</option>)}
              </Select>
              <span id={`${ids.team}-hint`} className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('sharedWorkspace.create.teamHint')}
              </span>
            </div>
          )}
          <div className="flex flex-col gap-1.5">
            <label htmlFor={ids.emails} className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
              {everyone ? t('sharedWorkspace.create.alsoManagers') : t('sharedWorkspace.create.people')}
            </label>
            <Textarea
              id={ids.emails}
              value={emails}
              onChange={(e) => setEmails(e.target.value)}
              placeholder={t('sharedWorkspace.create.peoplePlaceholder')}
              rows={2}
              className="min-h-[64px]"
              aria-describedby={`${ids.emails}-hint`}
            />
            <span id={`${ids.emails}-hint`} className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('sharedWorkspace.create.peopleHint', { organization: org })}
            </span>
          </div>
          {refusal && <SharedRefusalRow refusal={refusal} />}
          <DialogFooter>
            <Button type="button" variant="ghost" onClick={close} disabled={busy}>{t('common.cancel')}</Button>
            <Button type="submit" disabled={!trimmed || busy}>
              {busy ? t('sharedWorkspace.create.creating') : t('sharedWorkspace.create.submit')}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
