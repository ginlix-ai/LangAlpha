import React from 'react';
import { Card } from '../../../components/ui/card';

function IndexMovementCard({ indices = [], loading = false }) {
  return (
    <Card
      className="w-full fin-card flex-shrink-0"
      style={{ backgroundColor: 'transparent', border: 'none', boxShadow: 'none' }}
    >
      <div className="grid grid-cols-2 sm:flex gap-2.5 p-0">
        {loading
          ? Array.from({ length: 4 }).map((_, i) => (
              <div
                key={i}
                className="flex-1 flex flex-col gap-2 p-3 rounded-lg min-w-0 animate-pulse dashboard-index-pill"
                style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-default)' }}
              >
                <div className="h-3 rounded bg-foreground/10" style={{ width: '60%' }} />
                <div className="h-4 rounded bg-foreground/10" style={{ width: '80%' }} />
                <div className="h-3 rounded bg-foreground/10" style={{ width: '50%' }} />
              </div>
            ))
          : indices.map((index) => {
              const pos = index.isPositive;
              const ch = Number(index.change);
              const pct = Number(index.changePercent);
              const changeStr = (pos ? '+' : '') + ch.toFixed(2);
              const pctStr = (pos ? '+' : '') + pct.toFixed(2) + '%';
              return (
                <div
                  key={index.symbol}
                  className={`flex-1 flex flex-col gap-1.5 p-3 transition-all dashboard-index-pill ${pos ? 'dashboard-index-pill--up' : 'dashboard-index-pill--down'}`}
                  style={{
                    backgroundColor: 'var(--color-bg-card)',
                    border: '1px solid var(--color-border-default)',
                    borderRadius: '8px',
                    minWidth: '0',
                  }}
                >
                  <p
                    className="text-xs leading-tight truncate"
                    style={{ color: 'var(--color-text-secondary)' }}
                  >
                    {index.name}
                  </p>
                  <p
                    className="text-sm dashboard-mono leading-none"
                    style={{ color: 'var(--color-text-primary)' }}
                  >
                    {Number(index.price).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                  </p>
                  <div className="flex items-center gap-1.5 flex-wrap">
                    <span className={`text-xs dashboard-mono ${pos ? 'text-up' : 'text-down'}`}>
                      {changeStr}
                    </span>
                    <span className={`text-xs dashboard-mono ${pos ? 'dashboard-change-up' : 'dashboard-change-down'}`} style={{ color: pos ? 'var(--color-profit)' : 'var(--color-loss)' }}>
                      {pctStr}
                    </span>
                  </div>
                </div>
              );
            })}
      </div>
    </Card>
  );
}

export default IndexMovementCard;
