import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog';
import type { AnnouncementDef } from '../registry/types';
import { compareReleaseVersions } from './whatsNew';

/**
 * Versioned "What's New" modal. Lists unseen announcements grouped by release
 * (newest first). Acknowledge → caller stamps `lastSeenReleaseVersion`, which
 * is what marks them seen.
 */
export function WhatsNewModal({
  announcements,
  onAcknowledge,
}: {
  announcements: AnnouncementDef[];
  onAcknowledge: () => void;
}) {
  const { t } = useTranslation();

  const groups = useMemo(() => {
    const byVersion = new Map<string, AnnouncementDef[]>();
    for (const a of announcements) {
      const list = byVersion.get(a.releaseVersion) ?? [];
      list.push(a);
      byVersion.set(a.releaseVersion, list);
    }
    return [...byVersion.entries()].sort((a, b) => compareReleaseVersions(b[0], a[0]));
  }, [announcements]);

  if (announcements.length === 0) return null;

  return (
    <Dialog
      open
      onOpenChange={(next) => {
        if (!next) onAcknowledge();
      }}
    >
      <DialogContent variant="centered" className="max-w-md">
        <DialogHeader>
          <DialogTitle>{t('onboarding.whatsNew.title', "What's new")}</DialogTitle>
          <DialogDescription>
            {t('onboarding.whatsNew.subtitle', 'A few updates since you were last here.')}
          </DialogDescription>
        </DialogHeader>

        <div className="flex flex-col gap-4">
          {groups.map(([version, items]) => (
            <div key={version} className="flex flex-col gap-3">
              <span
                className="text-xs font-medium uppercase tracking-wide"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {version}
              </span>
              {items.map((a) => (
                <div key={a.key} className="rounded-lg border p-3" style={{ borderColor: 'var(--color-border-default)' }}>
                  <h4 className="text-sm font-semibold" style={{ color: 'var(--color-text-primary)' }}>
                    {t(a.modalTitleKey)}
                  </h4>
                  <p className="mt-1 text-sm leading-relaxed" style={{ color: 'var(--color-text-secondary)' }}>
                    {t(a.modalBodyKey)}
                  </p>
                </div>
              ))}
            </div>
          ))}
        </div>

        <DialogFooter>
          <button
            type="button"
            onClick={onAcknowledge}
            className="rounded-md px-4 py-2 text-sm font-semibold transition-opacity hover:opacity-90 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-accent-primary)] focus-visible:ring-offset-2"
            style={{ background: 'var(--color-accent-primary)', color: 'var(--color-text-on-accent)' }}
          >
            {t('onboarding.whatsNew.gotIt', 'Got it')}
          </button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
