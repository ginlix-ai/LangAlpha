import { useId } from 'react';
import { FolderPlus } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import { cn } from '@/lib/utils';
import { useRouteLeaveGuard } from '../../contexts/RouteLeaveGuardContext';
import { useCreatedWorkspace } from '../../hooks/useCreatedWorkspace';
import { RUN_DETAIL, RUN_FOCUS, RUN_HOVER, RUN_OPEN_BUTTON, RUN_ROW, RUN_TITLE, RunAvatar, RunPart, RunStatus } from './runCard';

/**
 * A workspace the agent created without a card in the chat (with `la`, or
 * approved in advance), on the line a hand-off uses, since the hand-off to it
 * usually follows: its name, then that it was created and what it is for,
 * cut to the line with the whole of it on hover. The whole row opens it once
 * the app has found it by the id the create printed; until then, once it is
 * gone, or for a create that printed none, the row is only words.
 */
export function WorkspaceCreatedSlip({
  name,
  workspaceId: createdId,
  description,
}: {
  name: string;
  /** The id the create printed; a rename keeps it, a name may move on. */
  workspaceId?: string;
  description?: string;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const guardLeave = useRouteLeaveGuard();
  const workspaceId = useCreatedWorkspace(createdId);
  const metaId = useId();
  const title = <span className={cn(RUN_TITLE, 'block')}>{name}</span>;

  return (
    <div data-testid="workspace-created" className={cn(RUN_ROW, RUN_FOCUS, workspaceId && RUN_HOVER)}>
      <RunAvatar className="text-(--color-text-primary)">
        <FolderPlus className="h-[15px] w-[15px]" />
      </RunAvatar>
      <span className="min-w-0">
        {workspaceId ? (
          <button
            type="button"
            title={name}
            aria-describedby={metaId}
            onClick={() => guardLeave(() => navigate(`/chat/${workspaceId}`, { state: { workspaceName: name, workspaceStatus: null } }))}
            className={RUN_OPEN_BUTTON}
          >
            {title}
          </button>
        ) : title}
        <span id={metaId} className={RUN_DETAIL}>
          <RunStatus words={t('chat.createWorkspaceCard.createdStatus')} />
          {description && <RunPart><span title={description} className="min-w-0 truncate">{description}</span></RunPart>}
        </span>
      </span>
      <span />
    </div>
  );
}
