import { useEffect } from 'react';

import { useQueryClient } from '@tanstack/react-query';

import { useAllWorkspacesAgent } from '@/hooks/useAllWorkspacesAgent';
import { flashWorkspaceQuery } from '@/hooks/useFlashWorkspace';
import { warmWorkspace } from '@/pages/ChatAgent/utils/warmWorkspace';

/** How long after a warm the next interaction warms again. A computer the
 *  server stopped for idling (50 minutes by default) is started again by the
 *  first interaction past this, and clicking around asks for at most six
 *  starts an hour. */
export const REWARM_AFTER_MS = 10 * 60 * 1000;

/** A modifier alone is no sign of work here: ⌘-Tab and Alt-Tab hand the page
 *  the modifier on their way to another app. */
const MODIFIER_KEYS = new Set(['Alt', 'AltGraph', 'CapsLock', 'Control', 'Fn', 'Meta', 'OS', 'Shift']);

/** Marks a control that starts, stops or archives a computer. Using one is the
 *  user setting the computer's state, which a warm would race: a start sent
 *  with a Stop click can undo it. So it warms nothing, and the next warm waits
 *  as long as after a warm, or the click after a stop would restart it. */
const POWER_CONTROL = '[data-computer-power]';

/**
 * Starts the user's computer on their first click, tap or key press in the app,
 * and on the first one 10 minutes after each warm, so the Chief of Staff's first
 * chat lands on a running machine. A computer archived after a week away can
 * take a minute or more to restore, which the user would otherwise wait out on
 * their first message. Opening or returning to the app does not count, so a tab
 * left open or only glanced at starts nothing; neither does the click that
 * brings a background window forward on macOS, which never reaches the page.
 * The start puts Home on the computer as a turn would, creating one for a user
 * who has none.
 */
export function WarmHome(): null {
  const queryClient = useQueryClient();
  const allWorkspaces = useAllWorkspacesAgent();

  useEffect(() => {
    if (!allWorkspaces) return;
    // Bumped by a power control and by cleanup, which drops a Home lookup
    // still pending from before: a warm after a Stop would undo it, and one
    // after the flag turns off would start a bound Home's computer anyway,
    // since its row carries the computer's status.
    let generation = 0;
    let warmedAt = -Infinity;
    const warm = (event: Event) => {
      if (event.target instanceof Element && event.target.closest(POWER_CONTROL)) {
        generation += 1;
        warmedAt = Date.now();
        return;
      }
      if (Date.now() - warmedAt < REWARM_AFTER_MS) return;
      warmedAt = Date.now();
      // Home's id is read here rather than before listening, so a click made
      // while it would still be loading counts. Its request upserts the row,
      // which is why only with the flag on. Best effort, like the warm: a send
      // still starts the computer and shows what failed.
      const current = generation;
      void queryClient.ensureQueryData(flashWorkspaceQuery(queryClient)).then(
        (home) => (current === generation ? warmWorkspace(home.workspace_id, queryClient, { home: true }) : undefined),
        // A lookup that failed started nothing, so the next interaction
        // retries, unless a power control has since begun its own wait.
        () => {
          if (current === generation) warmedAt = -Infinity;
        },
      );
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (!MODIFIER_KEYS.has(event.key)) warm(event);
    };
    // Capture, so a click a component stops from bubbling still counts.
    const options = { capture: true, passive: true };
    window.addEventListener('pointerdown', warm, options);
    window.addEventListener('keydown', onKeyDown, options);
    return () => {
      generation += 1;
      window.removeEventListener('pointerdown', warm, options);
      window.removeEventListener('keydown', onKeyDown, options);
    };
  }, [allWorkspaces, queryClient]);

  return null;
}
