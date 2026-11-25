/**
 * A fork still being made, in the sidebar: the running glyph, its title-to-be
 * and a word on what is happening, with nothing to open until the fork lands
 * and the real row takes its place.
 */
import React from 'react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import type { PendingFork } from '../hooks/forkMutation';

export function PendingForkRow({ fork }: { fork: PendingFork }): React.ReactElement {
  const { t } = useTranslation();
  return (
    <div
      className="nav-panel-row relative cursor-default"
      style={{ paddingLeft: 28 }}
      title={t('nav.forkPreparingHint')}
      aria-busy
    >
      <span className="absolute left-2 top-1/2 flex h-4 w-4 -translate-y-1/2 items-center justify-center">
        <Loader size={13} label={t('nav.forkPreparing')} className="text-(--color-accent-primary)" />
      </span>
      <span className="text-sm truncate" style={{ color: 'var(--color-text-tertiary)' }}>
        {fork.title ?? t('nav.untitledThread')}
      </span>
      <span className="ml-auto shrink-0 text-xs" style={{ color: 'var(--color-text-quaternary)' }}>
        {t('nav.forkPreparing')}
      </span>
    </div>
  );
}
