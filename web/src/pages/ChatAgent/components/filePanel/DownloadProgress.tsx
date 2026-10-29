import React from 'react';
import { CircleCheck } from 'lucide-react';

/** A save's notice body; ``fraction`` null means nothing reports progress yet. */
export function DownloadProgress({ text, fraction }: { text: string; fraction: number | null }): React.ReactElement {
  return (
    <div className="grid gap-2">
      <span>{text}</span>
      <div
        className="download-progress-track"
        role="progressbar"
        aria-label={text}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={fraction === null ? undefined : Math.round(fraction * 100)}
      >
        {fraction === null ? (
          <div className="download-progress-fill" data-indeterminate="" />
        ) : (
          <div className="download-progress-fill" style={{ width: `${fraction * 100}%` }} />
        )}
      </div>
    </div>
  );
}

/** The notice once a save is handed to the browser, which takes it from there. */
export function DownloadStarted({ text }: { text: string }): React.ReactElement {
  return (
    <div className="flex items-center gap-2" role="status">
      <CircleCheck className="h-4 w-4 shrink-0" style={{ color: 'var(--color-success)' }} aria-hidden />
      <span>{text}</span>
    </div>
  );
}
