/**
 * Why a backup could not take a file, in words. Shared by the panel's backup
 * notice and the spec-change refusal, which list the same server reasons.
 */
import type { useTranslation } from 'react-i18next';

import { formatBytes } from '@/lib/format';

import type { UnsavedFile } from './types';

type Translate = ReturnType<typeof useTranslation>['t'];

const SHOWN_PATH_CHARS = 120;

/** A path short enough for a sentence; the ends are what identify it. */
export function shownPath(path: string): string {
  if (path.length <= SHOWN_PATH_CHARS) return path;
  const half = Math.floor((SHOWN_PATH_CHARS - 1) / 2);
  return `${path.slice(0, half)}…${path.slice(-half)}`;
}

export function unsavedReasonLabel(t: Translate, file: Pick<UnsavedFile, 'reason' | 'size'>, locale: string): string {
  return file.reason === 'too_large' && file.size
    ? t('filePanel.unsavedReason.too_large_sized', { size: formatBytes(file.size, locale) })
    : t(`filePanel.unsavedReason.${file.reason}`, { defaultValue: t('filePanel.unsavedReason.failed') });
}
