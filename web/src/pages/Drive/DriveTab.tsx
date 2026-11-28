import { useRef } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { HardDrive } from 'lucide-react';
import { useNarrowContainer } from '@/hooks/useNarrowContainer';
import { driveHref, fillLocation, type DriveLocation } from '@/lib/driveLinks';
import type { DriveTabSpec } from '@/pages/ChatAgent/components/filePanel/types';
import { useRouteLeaveGuard } from '@/pages/ChatAgent/contexts/RouteLeaveGuardContext';
import { PaneAction } from './components/PaneFrame';
import { DriveView } from './DriveView';

/** Past this width the panel shows the list and the open file side by side, as the page does. */
const SPLIT_WIDTH = 760;

export interface DriveTabProps {
  tab: DriveTabSpec;
  /** Moves the tab to another place; `replace` corrects where it is without a step back. */
  onGo: (to: DriveLocation, opts: { replace?: boolean }) => void;
  /** The open file's id, once read, kept on the tab to find the file again after a move. */
  onTrack: (entryId: string) => void;
  /** Keeps the tab once the reader starts editing in it. */
  onPin: () => void;
}

/**
 * The person's Drive in a tab of the chat's panel: the Drive page itself,
 * its folders, memo list, trash and files, browsed in place so reading it
 * never leaves the chat. Each move is a step the panel's back and forward
 * take, and "Open in Drive" takes the same place to the page.
 */
export default function DriveTab({ tab, onGo, onTrack, onPin }: DriveTabProps) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const guardLeave = useRouteLeaveGuard();
  const boxRef = useRef<HTMLDivElement>(null);
  const narrow = useNarrowContainer(boxRef, SPLIT_WIDTH);
  const loc = fillLocation(tab.loc);
  const pageAction = (
    <PaneAction
      variant="ghost"
      icon={HardDrive}
      label={t('drive:openInDrive')}
      compact
      onClick={() => guardLeave(() => navigate(driveHref(loc)))}
    />
  );

  return (
    <div className="drive-panel-tab" ref={boxRef}>
      {narrow !== null && (
        <DriveView
          loc={loc}
          navigate={onGo}
          host="panel"
          single={narrow}
          pageAction={pageAction}
          entryId={tab.entryId}
          onTrack={onTrack}
          onEditing={onPin}
        />
      )}
    </div>
  );
}
