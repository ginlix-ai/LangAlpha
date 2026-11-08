import { SkeletonBar } from '@/components/ui/skeleton';
import { cn } from '@/lib/utils';

// Line widths per placeholder turn, so the stack reads as text rather than a grid.
const TURNS: { ask: string; lines: string[] }[] = [
  { ask: '34%', lines: ['92%', '86%', '58%'] },
  { ask: '46%', lines: ['95%', '72%'] },
  { ask: '28%', lines: ['90%', '94%', '81%', '40%'] },
  { ask: '40%', lines: ['88%', '64%'] },
];

/**
 * Stands in for the older turns still on their way above a transcript that
 * opened shorter than the screen and sits at its bottom (historyWindow
 * `aligned`). It takes only the room the transcript leaves (a zero basis that
 * grows, clipped from the top: `flex-1`'s 0% basis is read as the content's
 * height in a column with no definite height), so it never moves the transcript,
 * and it shrinks to nothing as pages land and fill the screen. A pin holding
 * room for a fresh send hides it, as that room belongs below the send.
 */
export function OlderHistorySkeleton({ isMobile }: { isMobile: boolean }) {
  return (
    <div
      aria-hidden="true"
      className={cn(
        'flex min-h-0 grow basis-0 justify-center overflow-hidden group-data-[pinned]/floor:hidden',
        isMobile ? 'px-3' : 'px-6',
      )}
    >
      <div className="flex w-full max-w-3xl flex-col justify-end gap-8 pb-6 opacity-70">
        {TURNS.map((turn, i) => (
          <div key={i} className="flex flex-col gap-3">
            <div className="flex justify-end">
              <SkeletonBar className="h-9 rounded-2xl" style={{ width: turn.ask }} />
            </div>
            <div className="flex flex-col gap-1 text-[15px] leading-7">
              {turn.lines.map((width, j) => (
                <div key={j}>
                  <SkeletonBar style={{ width }} />
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
