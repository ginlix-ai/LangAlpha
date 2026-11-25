import { PencilLine } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { DraftMark } from '@/components/ui/draft-mark';
import { draftKeys, useWaitingDraft } from '@/lib/composerDrafts';

/** A new thread written in a workspace and not sent, listed atop its threads
 *  by its first line; opening it opens the composer that holds it. */
export function NewThreadDraftRow({ preview, onOpen }: { preview: string; onOpen: () => void }) {
  const { t } = useTranslation();
  return (
    <div
      role="button"
      tabIndex={0}
      data-nav-key="new-draft"
      className="nav-panel-row group relative"
      style={{ paddingLeft: 28 }}
      onClick={onOpen}
      onKeyDown={(e) => {
        if (e.key !== 'Enter' && e.key !== ' ') return;
        e.preventDefault();
        onOpen();
      }}
    >
      <span className="text-sm truncate" style={{ color: 'var(--color-text-secondary)' }}>
        <DraftMark prefix />{' '}{preview || t('nav.newThread')}
      </span>
    </div>
  );
}

/** The start of a thread's unsent draft, under its facts in the hover card. */
export function ThreadDraftLine({ tid }: { tid: string }) {
  const draft = useWaitingDraft(draftKeys.thread(tid));
  if (!draft) return null;
  return (
    <div
      className="mt-2 flex gap-1.5 border-t pt-2 text-xs leading-snug"
      style={{ borderColor: 'var(--color-border-muted)', color: 'var(--color-text-secondary)' }}
    >
      <PencilLine className="mt-[2px] h-[11px] w-[11px] shrink-0" strokeWidth={2.2} style={{ color: 'var(--color-accent-primary)' }} aria-hidden />
      <span className="line-clamp-2 break-words">{draft}</span>
    </div>
  );
}
