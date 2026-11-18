import { useId } from 'react';
import { useTranslation } from 'react-i18next';

import { SegmentedControl } from '@/components/ui/segmented-control';
import type { Computer } from '@/types/api';

import { QUALITY_CHOICES, useBrowserQuality, type QualityChoice } from './browserQuality';

/**
 * How sharply the Browser tab draws this computer's browser. Auto follows the
 * computer's size; a choice reaches the next browser it opens, since the open
 * one keeps what it started with.
 */
export function BrowserQualitySetting({ computer }: { computer: Computer }) {
  const { t } = useTranslation();
  const labelId = useId();
  const { value, label, retinaHeld, autoIs, busy, set } = useBrowserQuality(computer);
  return (
    <section className="computer-details-group">
      <h3 id={labelId} className="computer-details-title">
        {t('computer.browserQuality.label', 'Live view quality')}
      </h3>
      <SegmentedControl<QualityChoice>
        value={value}
        labelledBy={labelId}
        size="compact"
        disabled={busy || computer.status === 'deleted'}
        onChange={set}
        options={QUALITY_CHOICES.map((choice) => ({
          value: choice,
          label: label(choice),
          disabled: choice === 'retina' && retinaHeld,
        }))}
      />
      <p className="mt-1.5 text-[12px]" style={{ color: 'var(--color-text-secondary)' }}>
        {value === 'auto' && autoIs
          ? t('computer.browserQuality.autoIs', 'Auto uses {{quality}} on this computer.', { quality: label(autoIs) }) + ' '
          : ''}
        {value === 'retina' && !retinaHeld
          ? t('computer.browserQuality.retinaCost', 'Retina takes about a whole CPU while someone watches.') + ' '
          : ''}
        {/* The computer itself falls back to Sharp below two CPUs. */}
        {retinaHeld
          ? (value === 'retina'
              ? t('computer.browserQuality.retinaFallback', 'Retina needs 2 or more CPUs, so this computer uses Sharp.')
              : t('computer.browserQuality.retinaHeld', 'Retina needs 2 or more CPUs.')) + ' '
          : ''}
        {t(
          'computer.browserQuality.hint',
          'Sharper is clearer and uses more bandwidth. It applies the next time the browser opens.',
        )}
      </p>
    </section>
  );
}
