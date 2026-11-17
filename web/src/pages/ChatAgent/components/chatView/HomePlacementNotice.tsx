/**
 * Above Home's composer: where the Chief of Staff works while the machine
 * Home lives on can't be reached, and what came back from there.
 *
 * Home's turns then run on the user's cloud computer, so they still work and
 * the user is told so quietly. With no cloud computer they wait for the
 * machine, which reads as a warning, since a message sent then won't run.
 * Once the machine is back the files written on the cloud are copied into a
 * folder of Home, and one line says how many and where until dismissed, and
 * which stayed on the cloud; or, when the copy kept failing, that they all
 * did.
 *
 * Drawn only on the placement's word: while it loads or failed, a turn that
 * cannot reach the machine says so on its own card.
 */
import type { ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { Cloud, FolderInput, WifiOff, X } from 'lucide-react';

import { useLocale } from '@/hooks/useLocale';

import { homePlacementLines, machineName, namedFiles, useDismissHomeHandback, useHomePlacement } from '../../hooks/useHomePlacement';

export function HomePlacementNotice() {
  const { t } = useTranslation();
  const locale = useLocale();
  const { data: placement } = useHomePlacement();
  const dismiss = useDismissHomeHandback();
  const { away, handback } = homePlacementLines(placement);
  if (!placement || (!away && !handback)) return null;

  const mac = machineName(t, placement.computer_name);
  const left: string[] = [];
  if (handback && handback.left_on_cloud.length > 0) {
    left.push(t('homePlacement.leftOnCloud', { cloud: handback.cloud_name, files: namedFiles(t, handback.left_on_cloud, locale) }));
  }
  if (handback?.unreadable.length) {
    left.push(t('homePlacement.unreadable', { cloud: handback.cloud_name, files: namedFiles(t, handback.unreadable, locale) }));
  }
  const handbackMac = machineName(t, handback?.computer_name);

  return (
    <>
      {away === 'onCloud' && placement.stand_in && (
        <Line testId="home-on-cloud" icon={<Cloud aria-hidden="true" className="mt-0.5 h-4 w-4 shrink-0" />}>
          {t('homePlacement.onCloud', { mac, cloud: placement.stand_in.name })}
        </Line>
      )}
      {away === 'waiting' && (
        <Line testId="home-waiting" warning icon={<WifiOff aria-hidden="true" className="mt-0.5 h-4 w-4 shrink-0" />}>
          {t('homePlacement.waiting', { mac })}
        </Line>
      )}
      {handback && (
        <Line
          testId="home-handback"
          icon={<FolderInput aria-hidden="true" className="mt-0.5 h-4 w-4 shrink-0" />}
          onDismiss={() => dismiss.mutate(handback.handback_id)}
          dismissLabel={t('homePlacement.dismiss')}
        >
          {handback.failed
            ? t('homePlacement.handbackFailed', { cloud: handback.cloud_name, mac: handbackMac })
            : handback.folder && t('homePlacement.handedBack', {
              count: handback.files,
              cloud: handback.cloud_name,
              folder: handback.folder,
              mac: handbackMac,
            })}
          {left.map((line) => (
            <span key={line} className={`${handback.folder ? 'mt-0.5 text-xs' : ''} block`} style={handback.folder ? { color: 'var(--color-text-tertiary)' } : undefined}>{line}</span>
          ))}
        </Line>
      )}
    </>
  );
}

/**
 * One row of the composer stack, with a surface of its own: the transcript
 * scrolls beneath the stack, so it is opaque, and the stack sets its corner
 * and text inset (ChatView).
 */
function Line({
  icon,
  children,
  testId,
  warning = false,
  onDismiss,
  dismissLabel,
}: {
  icon: ReactNode;
  children: ReactNode;
  testId: string;
  warning?: boolean;
  onDismiss?: () => void;
  dismissLabel?: string;
}) {
  return (
    <div
      role="status"
      data-testid={testId}
      className="flex items-start gap-2 rounded-[var(--stack-row-radius,var(--radius-lg))] border py-2.5 pr-3 pl-[var(--stack-row-pad,0.75rem)] text-sm"
      style={{
        backgroundColor: 'var(--color-bg-page)',
        backgroundImage: warning ? 'linear-gradient(var(--color-warning-soft), var(--color-warning-soft))' : undefined,
        borderColor: warning ? 'var(--color-warning)' : 'var(--color-border-muted)',
        color: 'var(--color-text-secondary)',
      }}
    >
      <span className="contents" style={{ color: warning ? 'var(--color-warning)' : 'var(--color-text-tertiary)' }}>
        {icon}
      </span>
      <div className="min-w-0 flex-1 wrap-break-word">{children}</div>
      {onDismiss && (
        <button
          type="button"
          onClick={onDismiss}
          aria-label={dismissLabel}
          className="tap-target shrink-0 rounded p-0.5 hover:bg-(--color-bg-hover)"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          <X aria-hidden="true" className="h-3.5 w-3.5" />
        </button>
      )}
    </div>
  );
}
