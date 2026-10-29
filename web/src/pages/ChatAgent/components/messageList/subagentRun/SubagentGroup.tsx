import React from 'react';
import { useTranslation } from 'react-i18next';
import { SubagentsRings } from '@/components/ui/subagents-rings';
import { RunGroup } from '../runCard';
import type { SubagentInfo } from '../types';
import { SubagentRow } from './SubagentRow';
import { groupSummary, type SubagentRowView } from './model';

/**
 * Two or more subagents launched back to back: a one-line header that says how
 * many and how they are doing, over a row each. No frame: the header is a line
 * of words with its chevron right after them, as the turn's own fold is.
 * Whether the rows are open is the caller's state.
 */
export function SubagentGroup({
  views,
  now,
  open,
  onToggle,
  onOpen,
}: {
  views: readonly SubagentRowView[];
  now: number;
  open: boolean;
  onToggle: () => void;
  onOpen?: (info: SubagentInfo) => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const { lead, parts, clock } = groupSummary(views, now, t);
  return (
    <div data-testid="subagent-group">
      <RunGroup
        icon={<SubagentsRings className="h-3.5 w-3.5" />}
        title={t('chat.subagentRun.count', { count: views.length })}
        lead={lead}
        parts={parts}
        clock={clock}
        open={open}
        onToggle={onToggle}
        // Whole when it mounts, so what it says then is history, not news.
        announce={{ read: true }}
      >
        <ul className="pt-0.5">
          {views.map((view) => (
            <li key={view.id}>
              <SubagentRow view={view} now={now} onOpen={onOpen} />
            </li>
          ))}
        </ul>
      </RunGroup>
    </div>
  );
}
