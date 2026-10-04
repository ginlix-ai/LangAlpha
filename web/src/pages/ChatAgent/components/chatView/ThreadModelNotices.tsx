import type React from 'react';
import type { ComposerMode } from '@/lib/modelPreferences';
import type { RetiredModel, DefaultModelOffer } from '../../hooks/useThreadModel';
import { RetiredModelNotice, ThreadModelBanner } from './ThreadModelBanner';

/* The rows `useThreadModel` raises above a composer: why the thread moved off
   a retired model, and the offer to make a pick the default. Both hosts render
   them from here, so the pair stays the same in the chat view and the market
   panel. */
export function ThreadModelNotices({
  retired,
  offer,
  mode,
  onDismiss,
}: {
  retired: RetiredModel | null;
  offer: DefaultModelOffer | null;
  mode: ComposerMode;
  onDismiss: () => void;
}): React.ReactElement {
  return (
    <>
      {retired && <RetiredModelNotice {...retired} />}
      {offer && <ThreadModelBanner {...offer} mode={mode} onDismiss={onDismiss} />}
    </>
  );
}
