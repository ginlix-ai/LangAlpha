import { useCallback, useEffect, useMemo, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { useComputer } from '../../hooks/useComputers';
import type { FilesAway } from '../../hooks/useWorkspaceFiles';
import {
  awayMachineOf, awayRetryMessage, awayRetryText, backupCode, backupShownText,
} from '../../utils/computerAway';
import type { ReadFrom } from '../../utils/api/readFrom';
import type { FileError } from './fileErrors';

/** What the hooks that change files need to know of a computer that is away. */
export interface ComputerAway {
  /** Why no change can be made now; null while the computer answers. */
  blockedReason: string | null;
  /** Words for a computer that did not answer, from a request it refused or
   *  the connect code a backup read carries; null for anything else. */
  describe: (cause: unknown) => string | null;
}

/** A computer that always answers, for a hook used where none can be away. */
export const NOT_AWAY: ComputerAway = { blockedReason: null, describe: () => null };

/** The panel's view of its computer being away, worded for that machine. */
export interface PanelAway extends ComputerAway {
  /** The open file's error, a computer that did not answer worded for it. */
  fileError: FileError | null;
  /** The listing's error: the machine's words when it failed with no backup. */
  filesError: string | null;
  /** Says the files shown are the backup; null for live files. */
  backupNotice: string | null;
}

/**
 * Everything the file panel derives from its computer not answering: the
 * listing's word (`filesAway`), or the open file's body read from the backup
 * since, which stands for it until the listing catches up.
 */
export function useComputerAway({ filesAway, bodyFrom, fileError, filesError, computerId, invalidate }: {
  filesAway: FilesAway | null;
  /** Where the open file's body came from. */
  bodyFrom: ReadFrom | undefined;
  fileError: FileError | null;
  filesError: string | null;
  computerId: string | null;
  /** Drops what the panel read, all of it or one path, so it is read again. */
  invalidate: (path?: string) => void;
}): PanelAway {
  const { t } = useTranslation();
  const bodyBackup = backupCode(bodyFrom);
  const away = useMemo<FilesAway | null>(
    () => filesAway ?? (bodyBackup ? { code: bodyBackup, backup: true } : null),
    [filesAway, bodyBackup],
  );
  const { data: computer } = useComputer(computerId);
  const machine = useMemo(() => awayMachineOf(computer), [computer]);
  const shownFileError = useMemo(
    () => (fileError?.category === 'computer_unreachable' && fileError.code
      ? { ...fileError, message: awayRetryText(t, fileError.code, machine) }
      : fileError),
    [fileError, machine, t],
  );
  // The backup stands in for reading only: a change made to it would be lost.
  const blockedReason = away ? awayRetryText(t, away.code, machine) : null;
  const describe = useCallback(
    (cause: unknown) => (typeof cause === 'string' ? awayRetryText(t, cause, machine) : awayRetryMessage(cause, t, machine)),
    [t, machine],
  );
  // What was read from the backup is read again live once the machine answers.
  const shownCode = away?.backup ? away.code : null;
  const shownBackup = useRef(shownCode);
  useEffect(() => {
    if (shownBackup.current && !shownCode) invalidate();
    shownBackup.current = shownCode;
  }, [shownCode, invalidate]);

  return useMemo(() => ({
    blockedReason,
    describe,
    fileError: shownFileError,
    filesError: filesAway && !filesAway.backup ? blockedReason : filesError,
    backupNotice: away?.backup ? backupShownText(t, away.code, machine) : null,
  }), [blockedReason, describe, shownFileError, filesAway, filesError, away, machine, t]);
}
