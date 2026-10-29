import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useNow } from '@/hooks/useNow';
import { useWorkspaceFolders } from '../../contexts/WorkspaceContext';
import { useSubagentTelemetries, useTelemetryFollowsRuns } from '../SubagentTelemetryContext';
import type { SubagentInfo } from './types';
import { SubagentRow } from './subagentRun/SubagentRow';
import { SubagentGroup } from './subagentRun/SubagentGroup';
import {
  describeMember,
  type SubagentRowView,
  type SubagentRunMember,
} from './subagentRun/model';

export type { SubagentRunMember } from './subagentRun/model';

/**
 * Subagents launched back to back in a reply, with nothing between them. One
 * component draws a lone subagent and a group alike, keyed by the first, so a
 * second launch landing later grows the run without remounting the first.
 *
 * The fold lives here for that reason. A group is open while any member
 * works and folds itself once all have finished, so a run replayed already
 * settled starts folded; a toggle by hand overrides that for as long as the
 * run is on screen.
 */
export function SubagentRun({
  members,
  pausing = false,
  onOpen,
}: {
  members: SubagentRunMember[];
  /** The turn holds an unanswered credit pause, so a running task is
   *  finishing its step rather than doing more. */
  pausing?: boolean;
  /** Absent on read-only surfaces (shared links): every row renders inert. */
  onOpen?: (info: SubagentInfo) => void;
}): React.ReactElement | null {
  const { t } = useTranslation();
  const root = useWorkspaceFolders()?.rootDir;
  // Telemetry is read here, at the leaf, so live token ticks re-render the
  // run without busting the memoized bubble above it.
  const telemetries = useSubagentTelemetries(members.map((m) => m.subagentId));
  const followed = useTelemetryFollowsRuns();
  const views = members
    .map((m, i) => describeMember(m, telemetries[i], { pausing, followed, root, t }))
    .filter((v): v is SubagentRowView => v !== null);
  const live = views.some((v) => v.live);
  const now = useNow(1000, live);
  const [openByHand, setOpenByHand] = useState<boolean | null>(null);
  const open = openByHand ?? live;

  if (views.length === 0) return null;
  return (
    <div data-testid="subagent-run">
      {views.length === 1 ? (
        <SubagentRow view={views[0]} now={now} onOpen={onOpen} />
      ) : (
        <SubagentGroup
          views={views}
          now={now}
          open={open}
          onToggle={() => setOpenByHand(!open)}
          onOpen={onOpen}
        />
      )}
    </div>
  );
}

export default SubagentRun;
