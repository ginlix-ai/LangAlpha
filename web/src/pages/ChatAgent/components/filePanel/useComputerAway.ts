import { useCallback, useEffect, useMemo, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';
import { useComputer } from '../../hooks/useComputers';
import type { FilesAway } from '../../hooks/useWorkspaceFiles';
import {
  awayMachineOf, awayRetryMessage, awayRetryText, backupShownText, cachedComputer,
} from '../../utils/computerAway';
import type { FileError } from './fileErrors';

/** What the hooks that change files need to know of a computer that is away. */
export interface ComputerAway {
  /** Why no change can be made now; null while the computer answers. */
  blockedReason: string | null;
  /** Words a refusal from a computer that did not answer; null for any other failure. */
  message: (err: unknown) => string | null;
  /** Words for a computer that did not answer, by its connect code. */
  text: (code: string) => string;
  /** A read of `path` was answered from the backup: the body on screen is
   *  read again, so it carries the same notice and edit block. */
  onBackupRead: (path: string) => void;
}

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
export function useComputerAway({ filesAway, bodyBackup, fileError, filesError, computerId, invalidate }: {
  filesAway: FilesAway | null;
  /** The open file's connect code when its body is the backup's. */
  bodyBackup: string | undefined;
  fileError: FileError | null;
  filesError: string | null;
  computerId: string | null;
  /** Drops what the panel read, all of it or one path, so it is read again. */
  invalidate: (path?: string) => void;
}): PanelAway {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const away = useMemo<FilesAway | null>(
    () => filesAway ?? (bodyBackup ? { code: bodyBackup, backup: true } : null),
    [filesAway, bodyBackup],
  );
  // The machine is looked up only once it is what is down, so a panel that
  // never meets that pays no read of the computer list for it.
  const machineDown = !!away || fileError?.category === 'computer_unreachable';
  const { data: downComputer } = useComputer(machineDown ? computerId : null);
  const machine = useMemo(() => awayMachineOf(downComputer), [downComputer]);
  const shownFileError = useMemo(
    () => (fileError?.category === 'computer_unreachable' && fileError.code
      ? { ...fileError, message: awayRetryText(t, fileError.code, machine) }
      : fileError),
    [fileError, machine, t],
  );
  // The backup stands in for reading only: a change made to it would be lost.
  const blockedReason = away ? awayRetryText(t, away.code, machine) : null;
  // A change the machine refused as it went away names it from the lists
  // already read: no fetch for a failure.
  const machineNow = useCallback(
    () => (downComputer ? machine : awayMachineOf(cachedComputer(queryClient, computerId))),
    [downComputer, machine, queryClient, computerId],
  );
  const message = useCallback((err: unknown) => awayRetryMessage(err, t, machineNow()), [t, machineNow]);
  const text = useCallback((code: string) => awayRetryText(t, code, machineNow()), [t, machineNow]);
  const onBackupRead = useCallback((path: string) => invalidate(path), [invalidate]);
  // What was read from the backup is read again live once the machine answers.
  const backupCode = away?.backup ? away.code : null;
  const shownBackup = useRef(backupCode);
  useEffect(() => {
    if (shownBackup.current && !backupCode) invalidate();
    shownBackup.current = backupCode;
  }, [backupCode, invalidate]);

  return useMemo(() => ({
    blockedReason,
    message,
    text,
    onBackupRead,
    fileError: shownFileError,
    filesError: filesAway && !filesAway.backup ? blockedReason : filesError,
    backupNotice: away?.backup ? backupShownText(t, away.code, machine) : null,
  }), [blockedReason, message, text, onBackupRead, shownFileError, filesAway, filesError, away, machine, t]);
}
