import { ScrollText } from 'lucide-react';
import { useTranslation } from 'react-i18next';

export default function MemoPanel() {
  const { t } = useTranslation();
  return (
    <div
      className="flex flex-col h-full items-center justify-center px-6 text-center"
      style={{ backgroundColor: 'var(--color-bg-page)' }}
    >
      <ScrollText
        className="h-10 w-10 mb-4"
        style={{ color: 'var(--color-text-tertiary)', opacity: 0.5 }}
      />
      <div
        className="text-sm font-semibold mb-2"
        style={{ color: 'var(--color-text-primary)' }}
      >
        {t('memoPanel.title')}
      </div>
      <div
        className="text-xs max-w-[20rem]"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {t('memoPanel.description')}
      </div>
    </div>
  );
}
