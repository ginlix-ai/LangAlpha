import { useTranslation } from 'react-i18next';
import { motion } from '@/lib/framer';
import { TWEEN } from '@/lib/motion';
import { percent } from '@/lib/format';
import { useLocale } from '@/hooks/useLocale';
import type { TokenUsage } from '@/pages/ChatAgent/session/tokenUsage';
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
  tokenUsage: TokenUsage;
  /** The thread the panel counts the parts of; absent for a composer with
   *  no thread yet. */
  threadId?: string | null;
}

export function TokenUsageRing({ tokenUsage, threadId }: TokenUsageRingProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const { total, threshold } = tokenUsage;

  const pct = share(total, threshold);
  const color = getColor(pct);
  const dashOffset = CIRCUMFERENCE * (1 - pct);
  const usedPercent = percent(threshold > 0 ? total / threshold : 0, locale);

  return (
    <Popover>
      {/* An icon button's slot, so the ring sits as far from its neighbours
          as the toolbar's icons sit from each other. */}
      <PopoverTrigger asChild>
        <button
          type="button"
          aria-label={t('chat.contextPanel.trigger', { percent: usedPercent })}
          className="press tap-target inline-flex flex-none items-center justify-center h-8 w-8 rounded-lg cursor-pointer hover:bg-foreground/5"
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
              animate={{ strokeDashoffset: dashOffset }}
              transition={TWEEN.fold}
              style={{ transform: 'rotate(-90deg)', transformOrigin: '50% 50%' }}
            />
          </svg>
        </button>
      </PopoverTrigger>
      <PopoverContent
        side="top"
        align="start"
        sideOffset={8}
        aria-label={t('chat.contextPanel.title')}
        className="w-80 max-w-[calc(100vw-2rem)] rounded-xl border-(--color-border-elevated) bg-(--color-bg-elevated) p-3 text-xs text-(--color-text-primary) shadow-lg tabular-nums"
      >
        <ContextPanel tokenUsage={tokenUsage} threadId={threadId} />
      </PopoverContent>
    </Popover>
  );
}
