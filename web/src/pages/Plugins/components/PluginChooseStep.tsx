import { useTranslation } from 'react-i18next';
import { ChevronRight } from 'lucide-react';
import { TagBadge } from '@/pages/ChatAgent/components/mcp/McpPrimitives';
import type { PluginCandidate } from '@/pages/ChatAgent/utils/api';

/**
 * The chooser between source and installing: shown when the source turned
 * out to be a marketplace repo holding several plugins. Picking one re-runs
 * the install with its subdirectory.
 */

export function PluginChooseStep({
  candidates,
  onPick,
  busy,
}: {
  candidates: PluginCandidate[];
  onPick: (candidate: PluginCandidate) => void;
  busy: boolean;
}) {
  const { t } = useTranslation();

  /** In-repo rows show their subtree path; external ones show where the
   * plugin actually lives. */
  function location(candidate: PluginCandidate): string {
    if (!candidate.source_url) return candidate.path || './';
    return candidate.source_url.replace(/^https:\/\//, '');
  }

  return (
    <div className="flex flex-col gap-3">
      <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('plugins.install.chooseHint', { count: candidates.length })}
      </p>

      <div className="flex flex-col gap-1.5 max-h-80 overflow-y-auto">
        {candidates.map((candidate) => (
          <button
            key={candidate.source_url ?? candidate.path}
            type="button"
            disabled={busy}
            onClick={() => onPick(candidate)}
            className="flex items-center gap-2 px-3 py-2 rounded-md text-left transition-colors hover:bg-foreground/5 disabled:opacity-50"
            style={{ border: '1px solid var(--color-border-muted)' }}
            data-testid={`plugin-candidate-${candidate.path}`}
          >
            <div className="flex flex-col gap-0.5 min-w-0 flex-1">
              <div className="flex items-center gap-2">
                <span
                  className="text-xs font-medium truncate"
                  style={{ color: 'var(--color-text-primary)' }}
                >
                  {candidate.name || candidate.path.split('/').pop()}
                </span>
                {candidate.version && (
                  <TagBadge>v{candidate.version}</TagBadge>
                )}
              </div>
              {candidate.description && (
                <span
                  className="text-[0.6875rem] line-clamp-1"
                  style={{ color: 'var(--color-text-tertiary)' }}
                >
                  {candidate.description}
                </span>
              )}
              <span
                className="text-[0.6875rem] font-mono truncate"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {location(candidate)}
              </span>
            </div>
            <ChevronRight
              className="h-3.5 w-3.5 shrink-0"
              style={{ color: 'var(--color-text-tertiary)' }}
            />
          </button>
        ))}
      </div>
    </div>
  );
}
