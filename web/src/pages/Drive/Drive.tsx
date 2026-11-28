import { useNavigate, useSearchParams } from 'react-router';
import { useIsMobile } from '@/hooks/useIsMobile';
import { driveHref, readDriveLocation, type DriveLocation } from '@/lib/driveLinks';
import { DriveView } from './DriveView';
import { useDriveMove } from './useDriveMove';
import { useDriveSpace } from './useOpenDriveFile';

/**
 * /drive: the person's window onto the files their agent works in. The place
 * in view is the URL, so a link from a chat, a tool row or another tab opens
 * the same place; a move to another place is drawn as one motion
 * (useDriveMove), and a correction replaces the address it corrects.
 */
function Drive() {
  const routerNavigate = useNavigate();
  const [params] = useSearchParams();
  const mobile = useIsMobile();
  const loc = readDriveLocation(params);
  const { space } = useDriveSpace(loc.space);
  const move = useDriveMove({ space, at: loc, mobile });
  const navigate = (to: DriveLocation, { replace = false }: { replace?: boolean }) => {
    const href = driveHref(to);
    if (replace) routerNavigate(href, { replace: true });
    else move(to, href);
  };
  return (
    <DriveView
      loc={loc}
      navigate={navigate}
      host="page"
      single={mobile}
      stale={params.has('history')}
    />
  );
}

export default Drive;
