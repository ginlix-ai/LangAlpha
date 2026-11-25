import type { TFunction } from 'i18next';
import type { DriveModifiedBy } from '@/types/drive';

/**
 * Who made a change, as the page names them. A person in a personal space is
 * its owner, so "You"; the agent is "Agent", or a subagent by its name.
 */
export function whoLabel(t: TFunction, by: Pick<DriveModifiedBy, 'kind' | 'agent'> | null | undefined): string {
  if (!by) return '';
  if (by.kind === 'person') return t('drive:who.you');
  if (by.kind === 'system') return t('drive:who.system');
  return by.agent && by.agent !== 'main' ? t('drive:who.subagent', { name: by.agent }) : t('drive:who.agent');
}

/** Where the thread a change was made in opens. */
export function threadHref(threadId: string): string {
  return `/chat/t/${encodeURIComponent(threadId)}`;
}
