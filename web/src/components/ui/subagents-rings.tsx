/**
 * The subagents figure: the agent with three seats around it. The composer's
 * toggle animates agents into the seats (`SubagentsGlyph`); everywhere else
 * the figure names the kind of thing, so it is drawn as the rings alone.
 *
 * Its own module so a page that only needs the mark does not pull in the
 * composer's parts. Seats take a lighter stroke than the core, and the figure
 * is centered on its box rather than on the core, to sit level with the
 * lucide icons beside it (24 box, 2 stroke).
 */

import { SUBAGENT_CORE, SUBAGENT_SEATS } from './subagents-rings.geometry';

export function SubagentsRings({ className }: { className?: string }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      aria-hidden="true"
    >
      <circle cx={SUBAGENT_CORE.x} cy={SUBAGENT_CORE.y} r="3.5" />
      {SUBAGENT_SEATS.map(([x, y]) => (
        <circle key={x} cx={x} cy={y} r="2.25" strokeWidth={1.5} />
      ))}
    </svg>
  );
}
