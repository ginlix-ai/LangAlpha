import { useLayoutEffect, useRef } from 'react';
import { useLocation, useNavigate } from 'react-router';
import { usePrefetchDriveList } from '@/hooks/useDrive';
import type { DriveLocation } from '@/lib/driveLinks';
import { markDriveLaidOut, motionBetween, runDriveMotion } from './driveMotion';
import { drivePathKey, parentOf } from './drivePaths';

/**
 * The longest a move to another folder waits for that folder's listing. Past
 * it the page moves and the list's placeholder shows the rest of the wait.
 */
const FOLDER_HOLD_MS = 250;

/** The folder a place is in, whose listing the page reads: its file's, or the one it names. */
function folderOf(at: DriveLocation): string {
  return at.file ? parentOf(at.file) : (at.dir ?? '');
}

/**
 * Lands the page on another place as one change rather than a run of them. A
 * move to another folder waits briefly for its listing, so its crumbs and rows
 * arrive in the same frame; a move that opens or closes a file is drawn as one
 * motion between the two layouts (driveMotion.ts).
 *
 * A move lands only while it is still the last thing asked for: a newer move,
 * any other change of address (back, a link, the page left) or the page going
 * away drops one still waiting, whether on its folder or on the motion's
 * first frame.
 */
export function useDriveMove({ space, at, mobile }: { space: string | null; at: DriveLocation; mobile: boolean }) {
  const navigate = useNavigate();
  const location = useLocation();
  const prefetchList = usePrefetchDriveList();
  const latest = useRef(0);
  const fileOpen = !at.trash && !!at.file;

  // Each new address, whoever made it, supersedes every move still waiting:
  // in the commit itself, so no wait that ends before effects run slips past.
  useLayoutEffect(() => {
    latest.current++;
  }, [location.key]);
  useLayoutEffect(() => () => {
    latest.current++;
  }, []);
  // The layout the page just committed lets a motion waiting on it run.
  useLayoutEffect(() => {
    markDriveLaidOut();
  }, [fileOpen]);

  return (to: DriveLocation, href: string, opts: { replace?: boolean } = {}) => {
    const seq = ++latest.current;
    // Already there (a second Escape while the first lands): nothing to add to
    // history, though a move still waiting elsewhere is dropped all the same.
    if (href === location.pathname + location.search) return;
    const motion = motionBetween(fileOpen, !to.trash && !!to.file, mobile);
    const land = () => runDriveMotion(motion, () => {
      if (latest.current === seq) navigate(href, opts);
    });
    const folder = folderOf(to);
    // The trash and the memo list show no folder, so there is no listing to wait for.
    if (!space || to.trash || to.memos || drivePathKey(folder) === drivePathKey(folderOf(at))) {
      land();
      return;
    }
    const hold = new Promise<void>((resolve) => setTimeout(resolve, FOLDER_HOLD_MS));
    void Promise.race([prefetchList(space, folder), hold]).then(() => {
      if (latest.current === seq) land();
    });
  };
}
