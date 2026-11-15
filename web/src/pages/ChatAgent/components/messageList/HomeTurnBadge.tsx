/**
 * Under a Home turn that ran on the user's cloud computer because the machine
 * Home lives on couldn't be reached: one quiet line naming where it ran, so a
 * file it wrote is not looked for on the machine.
 */
import { useTranslation } from 'react-i18next';
import { Cloud } from 'lucide-react';

import type { HomeFallback } from '@/types/sse';

export function HomeTurnBadge({ fallback }: { fallback: HomeFallback }) {
  const { t } = useTranslation();
  return (
    <div
      data-testid="home-turn-on-cloud"
      className="inline-flex items-center gap-1.5 self-end text-xs"
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      <Cloud aria-hidden="true" className="h-3.5 w-3.5 shrink-0" />
      <span>{t('homePlacement.ranOn', { cloud: fallback.cloud_name })}</span>
    </div>
  );
}
