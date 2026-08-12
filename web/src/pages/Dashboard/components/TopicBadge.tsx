import React from 'react';

type Trend = 'up' | 'down' | 'neutral';

const trendStyles: Record<Trend, React.CSSProperties> = {
  up: {
    backgroundColor: 'var(--color-profit-soft)',
    color: 'var(--color-profit)',
    borderColor: 'var(--color-profit-soft)',
  },
  down: {
    backgroundColor: 'var(--color-loss-soft)',
    color: 'var(--color-loss)',
    borderColor: 'var(--color-loss-soft)',
  },
  neutral: {
    backgroundColor: 'var(--color-bg-tag)',
    color: 'var(--color-text-secondary)',
    borderColor: 'var(--color-bg-tag)',
  },
};

interface TopicBadgeProps {
  text: string;
  trend: Trend;
}

function TopicBadge({ text, trend }: TopicBadgeProps) {
  return (
    <span
      className="px-1.5 py-0.5 sm:px-3 sm:py-1.5 rounded sm:rounded-lg border text-[0.625rem] sm:text-xs font-medium"
      style={trendStyles[trend] || trendStyles.neutral}
    >
      #{text}
    </span>
  );
}

export default TopicBadge;
