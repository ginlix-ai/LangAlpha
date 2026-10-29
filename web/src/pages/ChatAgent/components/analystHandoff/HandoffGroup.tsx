import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { RunGroup } from '../messageList/runCard';
import { AnalystGroupMark } from './AnalystMark';
import { ArrivingRow } from './handoffMotion';
import { handoffTally, type HandoffView } from './handoffModel';
import { HandoffSlip } from './HandoffSlip';

/**
 * Hand-offs made side by side, as one: a header line with the mark once, how
 * many, and where they stand, over a slip per hand-off with where it stands
 * in its avatar's place, since the same mark stacked would only repeat it.
 * Open until the reader folds it: a row is where its report link lives. A
 * hand-off that joins it once drawn (one approved beside it, one arriving in
 * the stream) unfolds into its row; the ones it was drawn with are there, as
 * is one that has arrived, when the reader folds the group and opens it again.
 */
export function HandoffGroup({ views }: { views: readonly HandoffView[] }) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(true);
  const [drawn, setDrawn] = useState<ReadonlySet<string>>(() => new Set(views.map((v) => v.id)));
  const { lead, parts } = handoffTally(views.map((v) => v.phase), t);

  return (
    // Narrow, the tally is cut before the header outgrows the reply. Names its
    // hand-offs so focus can find the group when its rows are folded away.
    <div data-handoff-group data-handoff-members={views.map((v) => v.id).join(' ')} className="@container">
      <RunGroup
        icon={<AnalystGroupMark />}
        title={t('chat.ptcCard.groupTitle', { count: views.length })}
        lead={lead}
        parts={parts}
        open={open}
        onToggle={() => setOpen((v) => !v)}
        // Until every row has read its run, the tally is the default.
        announce={{ read: views.every((v) => v.read) }}
      >
        <ul>
          {views.map((view) => (
            <ArrivingRow
              key={view.id}
              arrives={!drawn.has(view.id)}
              onDrawn={() => setDrawn((prev) => new Set(prev).add(view.id))}
            >
              <HandoffSlip view={view} grouped />
            </ArrivingRow>
          ))}
        </ul>
      </RunGroup>
    </div>
  );
}
