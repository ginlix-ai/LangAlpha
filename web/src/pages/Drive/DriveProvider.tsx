import { useState } from 'react';
import type { DriveEntry } from '@/types/drive';
import { MoveDialog, NameDialog, type NameAsk } from './components/DriveDialogs';
import { DriveContext, type DriveContextValue } from './driveContext';

/**
 * What every part of the page reaches for: the space, the actions, the way
 * around the page, and the questions it asks (a name, a folder to move into),
 * whose dialogs live here.
 */
export function DriveProvider({ children, ...value }: Omit<DriveContextValue, 'ask'> & { children: React.ReactNode }) {
  const [nameAsk, setNameAsk] = useState<NameAsk | null>(null);
  const [nameKey, setNameKey] = useState(0);
  const [moving, setMoving] = useState<DriveEntry | null>(null);
  const askName = (next: NameAsk) => {
    setNameKey((k) => k + 1);
    setNameAsk(next);
  };
  const { actions } = value;
  const ctx: DriveContextValue = {
    ...value,
    ask: {
      rename: (entry) => askName({ kind: 'rename', entry }),
      move: setMoving,
      newFolder: (dir) => askName({ kind: 'folder', dir }),
    },
  };
  return (
    <DriveContext value={ctx}>
      {children}
      <NameDialog
        key={nameKey}
        ask={nameAsk}
        onClose={() => setNameAsk(null)}
        onSubmit={(name) => {
          if (nameAsk?.kind === 'rename') return actions.rename(nameAsk.entry, name);
          return actions.mkdir(nameAsk?.dir ?? '', name);
        }}
      />
      <MoveDialog
        key={moving?.id ?? 'none'}
        space={value.space}
        entry={moving}
        onClose={() => setMoving(null)}
        onMove={actions.moveInto}
      />
    </DriveContext>
  );
}
