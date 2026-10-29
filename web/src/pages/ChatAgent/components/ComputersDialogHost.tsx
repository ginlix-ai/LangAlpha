/**
 * Mounts the computer dialogs once, at the page root, driven by
 * `computerPanelStore`. The gallery, a card's machine line and the disk
 * warnings in a chat all open them, and none of those share a parent below
 * the root.
 *
 * Being always mounted and reading the list, it is also where list-wide
 * bookkeeping lives: announcing spec-change and preparation outcomes and
 * forgetting disk warning dismissals once a disk recovers.
 */
import { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useQueryClient } from '@tanstack/react-query';

import { toast } from '@/components/ui/use-toast';
import { useLocale } from '@/hooks/useLocale';
import type { ResourceTier } from '@/types/api';

import {
  closeComputerSpec,
  closeComputersPanel,
  useComputersPanel,
} from '../hooks/computerPanelStore';
import { computerOutcomeWatch, reduceComputerOutcomes } from '../hooks/computerOutcomes';
import { takePrepareFailures } from '../hooks/computerPreparation';
import { useComputerActions } from '../hooks/useComputerActions';
import { invalidateMachine, invalidateMachineStorage, useComputerProviders, useComputers } from '../hooks/useComputers';
import { useTierQuota } from '../hooks/useTierQuota';
import ChangeSpecDialog from './ChangeSpecDialog';
import ComputersDialog from './ComputersDialog';
import { useForgetRecoveredDismissals } from './DiskWarning';
import { specErrorFiles, specErrorFrom, specErrorFromOutcome, type SpecError } from './specErrors';
import { normalizeTier, tierLabel } from './tierUi';
import { WorkspaceMoveOutcomes } from './WorkspaceMoveOutcomes';

/** `currentWorkspaceId` is the workspace the page shows, which a computer's removal can delete. */
function ComputersDialogHost({ currentWorkspaceId = null }: { currentWorkspaceId?: string | null }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const queryClient = useQueryClient();
  const { panel, opening, specComputerId } = useComputersPanel();
  // Always on: the fan-out at the same root reads this list anyway, and the
  // outcome watch below needs every refetch, whoever triggered it.
  const { data } = useComputers();
  const computers = data?.computers;
  const specTarget = specComputerId
    ? computers?.find((c) => c.computer_id === specComputerId) ?? null
    : null;
  const { spec } = useComputerActions();
  const [specError, setSpecError] = useState<SpecError | null>(null);
  const { data: quota } = useTierQuota({ enabled: !!specTarget });
  const { data: providerData } = useComputerProviders({ enabled: !!specTarget });
  const specProvider = specTarget ? providerData?.providers.find((p) => p.kind === specTarget.kind) : undefined;

  useForgetRecoveredDismissals(computers);

  useEffect(() => {
    setSpecError(null);
  }, [specComputerId]);

  // Any list read can be the one that sees an outcome land, so outcomes are
  // read off every new list rather than off a poll of our own. The watch is
  // set before anything is announced, so a second run over the same list
  // (StrictMode, a remount) finds these outcomes already handed out.
  useEffect(() => {
    if (!computers) return;
    const outcomes = reduceComputerOutcomes(computerOutcomeWatch.get(), computers);
    computerOutcomeWatch.set(outcomes.watch);
    const failures = takePrepareFailures(outcomes.idle);

    // The dialog may be closed long before a spec change settles, so the
    // outcome is reported wherever the user is: inline when the dialog is
    // still open on that computer, as a toast otherwise.
    for (const computer of outcomes.specChanges) {
      const change = computer.spec_change;
      if (!change) continue;
      // A new tier is a new disk, so the breakdown is re-read here and
      // nowhere else on this path.
      invalidateMachine(queryClient);
      invalidateMachineStorage(queryClient, computer.computer_id);
      const dialogOpen = specComputerId === computer.computer_id;
      if (change.state === 'succeeded') {
        toast({
          title: t('workspace.specUpdated', 'Computer spec updated'),
          description: t('computer.spec.succeeded', '{{name}} now runs at {{tier}}.', {
            name: computer.name,
            tier: tierLabel(t, normalizeTier(change.target_tier)),
          }),
        });
        if (dialogOpen) closeComputerSpec();
        continue;
      }
      const error = specErrorFromOutcome(change, t);
      if (dialogOpen) {
        setSpecError(error);
        continue;
      }
      const files = specErrorFiles(error.files, t, locale);
      toast({
        variant: 'destructive',
        title: t('computer.spec.failedTitle', 'Could not change the spec of {{name}}', { name: computer.name }),
        description: files.length > 0 ? `${error.message} ${files.join(', ')}` : error.message,
      });
    }

    // A new computer is prepared in the background and the user has usually
    // closed the dialog by the time it lands, so its outcome is a toast.
    if (outcomes.preparations.length === 0) return;
    // The row has its sandbox now, or is back at stopped after a failure:
    // either way the cached copy is out of date.
    invalidateMachine(queryClient);
    for (const { computer, ready } of outcomes.preparations) {
      if (ready) {
        toast({
          title: t('computer.prepare.readyTitle', '{{name}} is ready', { name: computer.name }),
          description: t('computer.prepare.readyDesc', 'You can create workspaces on it now.'),
        });
        continue;
      }
      const reason = failures.get(computer.computer_id);
      const retry = t('computer.prepare.retryHint', 'Start it to try again.');
      toast({
        variant: 'destructive',
        title: t('computer.prepare.failedTitle', "Couldn't prepare {{name}}", { name: computer.name }),
        // The server's reason is its own sentence, so the hint gets a line of its own.
        description: reason ? <>{reason}<br />{retry}</> : retry,
      });
    }
  }, [computers, specComputerId, queryClient, t, locale]);

  const submitSpec = (tier: ResourceTier) => {
    if (!specComputerId) return;
    setSpecError(null);
    spec.mutate(
      { computerId: specComputerId, tier },
      { onError: (err) => setSpecError(specErrorFrom(err, t, tier)) },
    );
  };

  return (
    <>
      <ComputersDialog
        panel={panel}
        opening={opening}
        onClose={closeComputersPanel}
        // One panel at a time: Change spec opened from a row takes the
        // Computers panel's place until it closes.
        covered={!!specTarget}
        currentWorkspaceId={currentWorkspaceId}
      />
      <ChangeSpecDialog
        target={specTarget}
        onClose={closeComputerSpec}
        onSubmit={submitSpec}
        busy={spec.isPending}
        quota={quota}
        provider={specProvider}
        error={specError}
        // Opened from a disk warning there is no Computers panel to dim the page.
        stacked={!!panel}
      />
      {/* The workspace menu hosts tell how a move ended too; this one covers
          a page with none of them mounted, such as a chat on a phone, which
          is where make-primary's move of Home can end. */}
      <WorkspaceMoveOutcomes />
    </>
  );
}

export default ComputersDialogHost;
