import { useTranslation } from 'react-i18next';
import { motion } from '@/lib/framer';
import { TWEEN } from '@/lib/motion';
import { percent } from '@/lib/format';
import { useLocale } from '@/hooks/useLocale';
import type { TokenUsage } from '@/pages/ChatAgent/session/tokenUsage';
import type { DueSummary } from '@/pages/ChatAgent/hooks/useNextSummaryHint';
import { Popover, PopoverTrigger, PopoverContent } from './popover';
import { ContextPanel } from './context-panel';
import { share } from './context-panel.model';

const SIZE = 16;
const STROKE_WIDTH = 1.5;
const RADIUS = (SIZE - STROKE_WIDTH) / 2;
const CIRCUMFERENCE = 2 * Math.PI * RADIUS;

/**
 * The arc's stroke. Neutral while there is room, since a fill is not news
 * until it nears the threshold: color is kept for the two states that are.
 */
function getColor(pct: number): string {
  if (pct > 0.85) return 'var(--color-loss)';
  if (pct > 0.60) return 'var(--color-warning)';
  return 'var(--color-text-tertiary)';
}

interface TokenUsageRingProps {
  /** Null while the thread's history loads or the window has not been
   *  counted: the ring holds its slot as a bare track, so the toolbar does not
   *  shift when the count lands and the arc fills in from empty rather than
   *  popping in. A summary due meanwhile, or skipped, still opens it. */
  tokenUsage: TokenUsage | null;
  /** The thread the panel counts the parts of; absent for a composer with
   *  no thread yet. */
  threadId?: string | null;
  /** The summary the next send writes first, while one is due: the ring
   *  carries a dot and its panel says so, with the skip where there is one. */
  summary?: DueSummary | null;
}

export function TokenUsageRing({ tokenUsage, threadId, summary = null }: TokenUsageRingProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const total = tokenUsage?.total ?? 0;
  const threshold = tokenUsage?.threshold ?? 0;
  // A skipped summary will not run, so the ring stops saying it will, but it
  // still opens: its panel holds the Undo.
  const due = !!summary && !summary.skipped;
  // Nothing to count and nothing to say: a placeholder, out of the tab order
  // and out of a screen reader's way.
  const pending = tokenUsage === null && !summary;

  const pct = tokenUsage ? share(total, threshold) : 0;
  const color = getColor(pct);
  const dashOffset = CIRCUMFERENCE * (1 - pct);
  const usedPercent = percent(threshold > 0 ? total / threshold : 0, locale);
  const label = tokenUsage
    ? t(due ? 'chat.contextPanel.triggerDue' : 'chat.contextPanel.trigger', { percent: usedPercent })
    : t(due ? 'chat.contextPanel.triggerDueUncounted' : 'chat.contextPanel.title');

  return (
    <Popover>
      {/* An icon button's slot, so the ring sits as far from its neighbours
          as the toolbar's icons sit from each other. */}
      <PopoverTrigger asChild>
        <button
          type="button"
          disabled={pending}
          aria-hidden={pending || undefined}
          aria-label={pending ? undefined : label}
          className="press tap-target relative inline-flex flex-none items-center justify-center h-8 w-8 rounded-lg cursor-pointer hover:bg-foreground/5 disabled:cursor-default disabled:hover:bg-transparent"
        >
          <svg width={SIZE} height={SIZE} viewBox={`0 0 ${SIZE} ${SIZE}`} aria-hidden="true">
            <circle
              cx={SIZE / 2}
              cy={SIZE / 2}
              r={RADIUS}
              fill="none"
              stroke="var(--color-border-muted)"
              strokeWidth={STROKE_WIDTH}
            />
            <motion.circle
              cx={SIZE / 2}
              cy={SIZE / 2}
              r={RADIUS}
              fill="none"
              stroke={color}
              strokeWidth={STROKE_WIDTH}
              strokeLinecap="round"
              strokeDasharray={CIRCUMFERENCE}
              initial={false}
              animate={{ strokeDashoffset: dashOffset, opacity: tokenUsage ? 1 : 0 }}
              transition={TWEEN.fold}
              style={{ transform: 'rotate(-90deg)', transformOrigin: '50% 50%' }}
            />
          </svg>
          {/* On the arc's shoulder, ringed by the composer's surface so it
              reads over the stroke whatever the fill. */}
          {due && (
            <span
              aria-hidden
              data-summary-due
              className="absolute right-[7px] top-[7px] h-1.5 w-1.5 rounded-full bg-(--color-warning) ring-[1.5px] ring-(--color-bg-card)"
            />
          )}
        </button>
      </PopoverTrigger>
      <PopoverContent
        side="top"
        align="start"
        sideOffset={8}
        aria-label={t('chat.contextPanel.title')}
        // Focus lands on the panel rather than its first control: the summary's
        // Skip would otherwise open with its tooltip already over the panel.
        onOpenAutoFocus={(e) => {
          e.preventDefault();
          (e.currentTarget as HTMLElement).focus();
        }}
        className="w-80 max-w-[calc(100vw-2rem)] rounded-xl border-(--color-border-elevated) bg-(--color-bg-elevated) p-3 text-xs text-(--color-text-primary) shadow-lg tabular-nums"
      >
        <ContextPanel tokenUsage={tokenUsage} threadId={threadId} summary={summary} />
      </PopoverContent>
    </Popover>
  );
}
