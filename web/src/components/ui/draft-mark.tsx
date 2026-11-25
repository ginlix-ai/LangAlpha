import { PencilLine } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useWaitingDraft } from '@/lib/composerDrafts';

const PENCIL = 'h-[11px] w-[11px] shrink-0';

/**
 * Says a thread holds words its composer kept and nobody sent
 * (`lib/composerDrafts`): an amber pencil and "Draft", leading the title in
 * the sidebar and the line under it on the workspace page. It sits in the text
 * rather than the row's status gutter so it never displaces the running,
 * waiting or unread mark there.
 */
export function DraftMark({ prefix = false }: { prefix?: boolean }) {
  const { t } = useTranslation();
  return (
    <span
      // Top-aligned so the pencil never lifts the line box: a marked row
      // stays the height of the rest.
      className="inline-flex shrink-0 items-center gap-[3px] align-top font-medium"
      style={{ color: 'var(--color-accent-primary)' }}
    >
      <PencilLine className={PENCIL} strokeWidth={2.2} aria-hidden />
      {t(prefix ? 'nav.draftPrefix' : 'nav.draft')}
    </span>
  );
}

/** The pencil alone, where a draft waits under a name with no room for the
 *  word: a closed workspace, Dashboard, Market View. Nothing while none does. */
export function DraftPencil({ draftKey }: { draftKey: string }) {
  const { t } = useTranslation();
  const preview = useWaitingDraft(draftKey);
  if (preview === null) return null;
  return (
    <span role="img" aria-label={t('nav.draftWaiting')} className="inline-flex shrink-0 items-center">
      <PencilLine className={PENCIL} strokeWidth={2.2} style={{ color: 'var(--color-accent-primary)' }} aria-hidden />
    </span>
  );
}
