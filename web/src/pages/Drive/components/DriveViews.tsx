import { useTranslation } from 'react-i18next';
import { cn } from '@/lib/utils';
import { useDriveContext } from '../driveContext';

/**
 * The switch between the Drive's two views at the top of the page: its files
 * folder by folder, and its memos wherever they sit. The current one carries
 * the memo tabs' neutral underline.
 */
export function DriveViews({ current }: { current: 'files' | 'memos' }) {
  const { t } = useTranslation();
  const { go } = useDriveContext();
  const views = [
    { key: 'files', label: t('drive:views.files'), to: {} },
    { key: 'memos', label: t('drive:memos.title'), to: { memos: true } },
  ] as const;
  return (
    <nav className="drive-views" aria-label={t('drive:views.label')}>
      {views.map((v) => (
        <button
          key={v.key}
          type="button"
          className={cn('drive-tab', current === v.key && 'is-active')}
          aria-current={current === v.key ? 'page' : undefined}
          onClick={() => {
            if (current !== v.key) go(v.to);
          }}
        >
          {v.label}
        </button>
      ))}
    </nav>
  );
}
