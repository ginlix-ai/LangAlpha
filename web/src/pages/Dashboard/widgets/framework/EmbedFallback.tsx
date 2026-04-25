import { AlertTriangle, RotateCw, ExternalLink } from 'lucide-react';

export function EmbedFallback({ onRetry }: { onRetry: () => void }) {
  return (
    <div
      className="absolute inset-0 flex flex-col items-center justify-center gap-2 p-4 text-center"
      style={{
        backgroundColor: 'var(--color-bg-card)',
        color: 'var(--color-text-secondary)',
      }}
    >
      <AlertTriangle size={20} style={{ color: 'var(--color-icon-warning, var(--color-text-tertiary))' }} />
      <div className="text-xs font-medium" style={{ color: 'var(--color-text-primary)' }}>
        Widget unavailable
      </div>
      <div className="text-[11px] max-w-[28ch]" style={{ color: 'var(--color-text-tertiary)' }}>
        TradingView didn&apos;t load. Check your network or ad-blocker.
      </div>
      <div className="flex items-center gap-2 mt-1">
        <button
          type="button"
          onClick={onRetry}
          className="inline-flex items-center gap-1 px-2.5 py-1 rounded text-[11px] border widget-drag-cancel"
          style={{
            borderColor: 'var(--color-border-default)',
            backgroundColor: 'var(--color-bg-elevated)',
            color: 'var(--color-text-primary)',
          }}
        >
          <RotateCw size={11} /> Retry
        </button>
        <a
          href="https://status.tradingview.com/"
          target="_blank"
          rel="noopener noreferrer"
          className="tv-attribution inline-flex items-center gap-1 px-2.5 py-1 rounded text-[11px] widget-drag-cancel"
          style={{ color: 'var(--color-text-tertiary)', padding: '4px 10px' }}
        >
          Status <ExternalLink size={10} />
        </a>
      </div>
    </div>
  );
}
