/**
 * The file cards of a Home turn that ran on the cloud stand-in, pointed at
 * where each file is now (`placeCloudTurns`): the copy a hand-back brought
 * into Home, else the stand-in's own. Asks the server only in a thread with
 * such a turn, and again while one of their files has not come back.
 *
 * The read lives in a reader element the list mounts only then, so a list
 * drawn with no query client (a transcript with no such turn) needs none.
 */
import { createElement, useMemo, useState, type ReactElement } from 'react';

import type { HomeFallback } from '@/types/sse';

import { HandbacksReader } from '../components/messageList/HandbacksReader';
import type { MessageRecord } from '../components/messageList/types';
import type { HomeHandbacks } from '../utils/api/workspaces';
import { placeCloudTurns } from '../utils/cloudTurnFiles';
import type { TurnFile } from '../utils/turnFiles';

export function useCloudTurnFiles(
  turned: readonly { message: MessageRecord; turnIndex: number }[],
  files: Map<number, TurnFile[]>,
  { enabled }: { enabled: boolean },
): { files: Map<number, TurnFile[]>; reader: ReactElement | null } {
  const cloudTurns = useMemo(() => {
    const byTurn = new Map<number, HomeFallback>();
    for (const { message, turnIndex } of turned) {
      const fallback = message.role === 'user' ? (message.homeFallback as HomeFallback | undefined) : undefined;
      if (fallback) byTurn.set(turnIndex, fallback);
    }
    return byTurn;
  }, [turned]);
  const [handbacks, setHandbacks] = useState<HomeHandbacks | null>(null);
  const placement = useMemo(
    () => (handbacks && cloudTurns.size > 0 ? placeCloudTurns(files, cloudTurns, handbacks) : null),
    [handbacks, cloudTurns, files],
  );
  const reading = enabled && cloudTurns.size > 0;
  return {
    files: placement?.placed ?? files,
    reader: reading ? createElement(HandbacksReader, { poll: placement?.unplaced ?? false, onData: setHandbacks }) : null,
  };
}
