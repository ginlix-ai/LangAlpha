import { useTranslation } from 'react-i18next';
import { HeaderButton } from '@/components/mcp/McpPrimitives';

/** A view that cannot show: why, and the way on (ask again, or back to the current version). */
export function PaneProblem({ line, onRetry, onBack }: { line: string; onRetry?: () => void; onBack?: () => void }) {
  const { t } = useTranslation();
  return (
    <div className="drive-file-empty">
      <p>{line}</p>
      {onRetry && <HeaderButton onClick={onRetry}>{t('common.retry')}</HeaderButton>}
      {onBack && <HeaderButton onClick={onBack}>{t('drive:versions.backToCurrent')}</HeaderButton>}
    </div>
  );
}
