import type React from 'react';
import { useTranslation } from 'react-i18next';
import { Info } from 'lucide-react';
import { StatusLine } from './StatusLine';
import { useComposerModelLabel } from './useComposerModelLabel';

/* The thread's own model has left the catalog, so the composer fell back to
   the account default; the next send stores that one on the thread and this row
   goes with it. A status line, not a banner: there is nothing to decide. */
export function RetiredModelNotice({
  model,
  fallback,
  className,
}: {
  model: string;
  fallback: string;
  className?: string;
}): React.ReactElement {
  const { t } = useTranslation();
  const label = useComposerModelLabel();
  return (
    <StatusLine glyph={<Info className="h-3.5 w-3.5" />} className={className}>
      <span className="min-w-0">
        {t('chat.threadModel.retired', { model: label(model), fallback: label(fallback) })}
      </span>
    </StatusLine>
  );
}
