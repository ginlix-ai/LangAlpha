import { useTranslation } from 'react-i18next';
import { ArrowDown } from 'lucide-react';
import './JumpToLatestPill.css';

interface JumpToLatestPillProps {
  /** Whether the user is scrolled up far enough to warrant the affordance. */
  visible: boolean;
  /** New messages arrived below the fold while scrolled up. */
  hasNew: boolean;
  /** Count of new messages since the user last scrolled up (only shown when hasNew). */
  newCount?: number;
  onJump: () => void;
}

/**
 * Floating "jump to latest" affordance shown when the user has scrolled up —
 * a compact icon circle that grows a count when messages arrive below the
 * fold. Purely presentational — all scroll logic lives in ChatView.
 */
export default function JumpToLatestPill({ visible, hasNew, newCount = 0, onJump }: JumpToLatestPillProps) {
  const { t } = useTranslation();
  if (!visible) return null;

  const showCount = hasNew && newCount > 0;
  const label = t('chat.jumpToLatest.aria', { defaultValue: 'Scroll to latest message' });

  return (
    <button
      type="button"
      className={`jump-to-latest-pill${showCount ? ' jump-to-latest-pill--count' : ''}`}
      onClick={onJump}
      aria-label={label}
      title={label}
    >
      {showCount && <span className="jump-to-latest-pill__count">{newCount}</span>}
      <ArrowDown className="h-4 w-4" />
    </button>
  );
}
