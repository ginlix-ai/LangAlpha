import React from 'react';
import { AlertTriangle } from 'lucide-react';
import { Loader } from '@/components/ui/loader';

export function DocumentLoadingFallback(): React.ReactElement {
  return (
    <div className="flex items-center justify-center py-12">
      <Loader size={20} className="text-[color:var(--color-text-tertiary)]" />
    </div>
  );
}

interface DocumentErrorFallbackProps {
  onDownload: () => void;
}

export function DocumentErrorFallback({ onDownload }: DocumentErrorFallbackProps): React.ReactElement {
  return (
    <div className="flex flex-col items-center justify-center gap-3 py-12">
      <AlertTriangle className="h-6 w-6" style={{ color: 'var(--color-text-tertiary)' }} />
      <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>Unable to preview this file</p>
      <button
        className="text-xs px-3 py-1.5 rounded"
        style={{ background: 'var(--color-bg-elevated)', color: 'var(--color-text-primary)', border: '1px solid var(--color-border-elevated)' }}
        onClick={onDownload}
      >
        Download instead
      </button>
    </div>
  );
}
