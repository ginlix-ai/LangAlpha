/**
 * Create a room: name it, pick its workspace and bots, and choose who answers.
 *
 * A room belongs to one workspace and every member must belong to it too, so
 * the bot list follows the workspace picker. The host defaults to the first
 * member (the server lets a host-less room exist, but a host speaks first in
 * every round, so starting without one only makes the order arbitrary).
 */
import { useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import type { RoomView } from '@/api/rooms';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { ErrorBanner } from '@/components/ui/error-banner';
import { Input } from '@/components/ui/input';
import { Select } from '@/components/ui/select';
import { Textarea } from '@/components/ui/textarea';
import { useBots } from '@/hooks/useBots';
import { useCreateRoom } from '@/hooks/useRooms';
import type { Workspace } from '@/types/api';
import { failureText } from '@/lib/failureText';

function FormField({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label className="flex flex-col gap-1.5">
      <span className="text-xs font-medium" style={{ color: 'var(--color-text-secondary)' }}>
        {label}
      </span>
      {children}
    </label>
  );
}

interface CreateRoomDialogProps {
  workspaces: readonly Workspace[];
  /** The workspace the page is showing; the dialog opens on it. */
  initialWorkspaceId: string | null;
  maxBots: number;
  onClose: () => void;
  onCreated: (room: RoomView) => void;
}

export default function CreateRoomDialog({
  workspaces,
  initialWorkspaceId,
  maxBots,
  onClose,
  onCreated,
}: CreateRoomDialogProps) {
  const { t } = useTranslation();
  const createRoom = useCreateRoom();
  const [workspaceId, setWorkspaceId] = useState(
    initialWorkspaceId && workspaces.some((ws) => ws.workspace_id === initialWorkspaceId)
      ? initialWorkspaceId
      : (workspaces[0]?.workspace_id ?? ''),
  );
  const [name, setName] = useState('');
  const [memberIds, setMemberIds] = useState<string[]>([]);
  const [hostChoice, setHostChoice] = useState('');
  const [responder, setResponder] = useState<'everyone' | 'mentions'>('everyone');
  const [bulletin, setBulletin] = useState('');
  const botsQuery = useBots(workspaceId || null, !!workspaceId);
  const bots = botsQuery.data?.bots ?? [];
  // The host is a member; with no explicit pick the first member hosts.
  const hostId = hostChoice && memberIds.includes(hostChoice) ? hostChoice : (memberIds[0] ?? '');
  const busy = createRoom.isPending;
  const canSubmit = !!name.trim() && memberIds.length > 0 && !!workspaceId && !busy;

  const changeWorkspace = (next: string) => {
    setWorkspaceId(next);
    setMemberIds([]);
    setHostChoice('');
  };

  const toggleMember = (botId: string) => {
    // Compute the next member list in the handler: a host fix inside a
    // setState updater is a side effect React may run twice.
    const next = memberIds.includes(botId)
      ? memberIds.filter((id) => id !== botId)
      : memberIds.length >= maxBots
        ? memberIds
        : [...memberIds, botId];
    setMemberIds(next);
    if (hostChoice === botId && !next.includes(botId)) setHostChoice(next[0] ?? '');
  };

  const submit = async () => {
    if (!canSubmit) return;
    try {
      const room = await createRoom.mutateAsync({
        workspace_id: workspaceId,
        name: name.trim(),
        member_bot_ids: memberIds,
        host_bot_id: hostId || null,
        responder,
        bulletin: bulletin.trim(),
      });
      onCreated(room);
    } catch {
      // The refusal is drawn under the fields; nothing typed is thrown away.
    }
  };

  const failure = failureText(createRoom.error, t('rooms.create.failed'));

  return (
    <Dialog open onOpenChange={(open) => { if (!open && !busy) onClose(); }}>
      <DialogContent style={{ backgroundColor: 'var(--color-bg-page)', borderColor: 'var(--color-border-muted)' }}>
        <DialogHeader>
          <DialogTitle>{t('rooms.create.title')}</DialogTitle>
          <DialogDescription>
            {t('rooms.create.description', { max: maxBots })}
          </DialogDescription>
        </DialogHeader>

        <div className="flex flex-col gap-3">
          <FormField label={t('rooms.create.name')}>
            <Input
              value={name}
              onChange={(event) => setName(event.target.value)}
              placeholder={t('rooms.create.namePlaceholder')}
              maxLength={64}
              autoFocus
            />
          </FormField>

          <FormField label={t('rooms.create.workspace')}>
            <Select
              value={workspaceId}
              onChange={(event) => changeWorkspace(event.target.value)}
              disabled={workspaces.length <= 1}
            >
              {workspaces.map((workspace) => (
                <option key={workspace.workspace_id} value={workspace.workspace_id}>
                  {workspace.name}
                </option>
              ))}
            </Select>
          </FormField>

          <div className="flex flex-col gap-1.5">
            <div className="flex items-baseline justify-between gap-2">
              <span className="text-xs font-medium" style={{ color: 'var(--color-text-secondary)' }}>
                {t('rooms.create.members')}
              </span>
              <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('rooms.create.membersHint', { n: memberIds.length, max: maxBots })}
              </span>
            </div>
            {botsQuery.isLoading ? (
              <span className="text-xs" role="status" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('common.loading')}
              </span>
            ) : bots.length === 0 ? (
              <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('rooms.create.noBots')}
              </span>
            ) : (
              <div
                role="group"
                aria-label={t('rooms.create.members')}
                className="flex max-h-44 flex-col gap-1 overflow-y-auto rounded-md p-2"
                style={{ border: '1px solid var(--color-border-muted)' }}
              >
                {bots.map((bot) => {
                  const checked = memberIds.includes(bot.bot_id);
                  return (
                    <label
                      key={bot.bot_id}
                      className="flex items-center gap-2 text-sm"
                      style={{ color: 'var(--color-text-primary)' }}
                    >
                      <input
                        type="checkbox"
                        aria-label={bot.name}
                        checked={checked}
                        disabled={!checked && memberIds.length >= maxBots}
                        onChange={() => toggleMember(bot.bot_id)}
                      />
                      <span className="truncate">{bot.name}</span>
                      <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                        {t(bot.role === 'chief_of_staff' ? 'agents.chiefOfStaff' : 'agents.analyst')}
                      </span>
                      {bot.status === 'paused' && (
                        <Badge variant="warning" className="ml-auto">{t('rooms.pausedBadge')}</Badge>
                      )}
                    </label>
                  );
                })}
              </div>
            )}
          </div>

          <FormField label={t('rooms.create.host')}>
            <Select
              value={hostId}
              onChange={(event) => setHostChoice(event.target.value)}
              disabled={memberIds.length === 0}
            >
              {memberIds.length === 0 && <option value="">{t('rooms.create.hostNone')}</option>}
              {memberIds.map((id) => (
                <option key={id} value={id}>
                  {bots.find((bot) => bot.bot_id === id)?.name ?? id}
                </option>
              ))}
            </Select>
          </FormField>

          <FormField label={t('rooms.create.responder')}>
            <Select
              value={responder}
              onChange={(event) => setResponder(event.target.value === 'mentions' ? 'mentions' : 'everyone')}
            >
              <option value="everyone">{t('rooms.responder.everyone')}</option>
              <option value="mentions">{t('rooms.responder.mentions')}</option>
            </Select>
          </FormField>

          <FormField label={t('rooms.create.bulletin')}>
            <Textarea
              value={bulletin}
              onChange={(event) => setBulletin(event.target.value)}
              placeholder={t('rooms.create.bulletinPlaceholder')}
              rows={2}
              maxLength={2000}
            />
          </FormField>
        </div>

        {failure && <ErrorBanner error={failure} />}

        <DialogFooter>
          <Button variant="ghost" onClick={onClose} disabled={busy}>{t('common.cancel')}</Button>
          <Button onClick={() => { void submit(); }} disabled={!canSubmit}>
            {busy ? t('common.saving') : t('rooms.create.create')}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
