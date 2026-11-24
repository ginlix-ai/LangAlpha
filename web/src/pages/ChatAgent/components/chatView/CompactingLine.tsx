import type React from 'react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { compactNumber } from '@/lib/format';
import { useLocale } from '@/hooks/useLocale';
import type { Compaction } from './compaction';

/**
 * A compaction under way, drawn as the divider its end leaves in the
 * transcript ("Compacted from 120 messages") and in the same box, so the one
 * becomes the other in place.
 */
export function CompactingLine({ kind, tokens }: Compaction): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const rule = <div className="flex-1" style={{ borderTop: '1px solid var(--color-border-muted)' }} />;
  return (
    <div className="my-1">
      <div className="flex items-center gap-3 py-2 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {rule}
        <span role="status" aria-live="polite" className="inline-flex items-center gap-2 whitespace-nowrap">
          <span aria-hidden="true" className="inline-flex">
            <Loader size={12} className="text-(--color-accent-primary)" />
          </span>
          {t(kind === 'offload' ? 'chat.offloading' : 'chat.compacting')}
        </span>
        {/* Beside the live region rather than in it, which would read the
            count out every time it moves; a screen reader still reaches it. */}
        {tokens != null && (
          <span className="tabular-nums whitespace-nowrap" style={{ color: 'var(--color-text-quaternary)' }}>
            {t('chat.compactingTokens', { value: compactNumber(tokens, locale) })}
          </span>
        )}
        {rule}
      </div>
    </div>
  );
}
