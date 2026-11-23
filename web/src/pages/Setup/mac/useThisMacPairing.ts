/**
 * Pairing this Mac from setup: the existing remote-control flow
 * (`useAddThisMac`: a pairing minted for the helper, the shell's own
 * confirmation, Login Items, the helper's deadline), then the computer made
 * from what the helper found, with no form between, as the stage the page
 * shows. Home goes where the answers say (`useAddOwnComputer`).
 */
import { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { OnboardingRunsOn } from '@/api/onboarding';
import { useNow } from '@/hooks/useNow';
import { createRefusalMessage } from '@/pages/ChatAgent/components/addComputer';
import { useAddThisMac } from '@/pages/ChatAgent/components/addThisMac';
import { folderMarkedOwner } from '@/pages/ChatAgent/utils/sshLimit';
import type { Computer } from '@/types/api';
import { useAddOwnComputer, type HomeOutcome } from '../answers/addOwnComputer';

/**
 * `ask` before the user said yes; `confirm` while the shell's confirmation
 * (and then the helper's start) is up; `approval` while macOS holds the
 * helper back in Login Items; `pairing` until the computer is made;
 * `failed` with the reason; `paired` once this Mac is a computer.
 */
export type PairStage = 'ask' | 'confirm' | 'approval' | 'pairing' | 'failed' | 'paired';

export function useThisMacPairing({ runsOn, onPaired, held = false }: {
  runsOn: OnboardingRunsOn | null | undefined;
  onPaired?: (computer: Computer) => void;
  /** The user said no meanwhile: a machine the helper reaches makes no computer. */
  held?: boolean;
}) {
  const { t } = useTranslation();
  const addThisMac = useAddThisMac(true);
  const mac = addThisMac.setup;
  const add = useAddOwnComputer(runsOn);
  const { phase, setAcknowledged, start } = mac;
  const [startedAt, setStartedAt] = useState<number | null>(null);
  // The prompt is the disclosure the Computers form asks to be ticked, and
  // nothing starts before Pair this Mac, which is the yes.
  useEffect(() => {
    setAcknowledged(true);
  }, [setAcknowledged]);

  const send = (takeOver: string | null) => {
    const body = addThisMac.body({ takeOver });
    if (!body) return;
    add.mutate(body, { onSuccess: ({ computer }) => onPaired?.(computer) });
  };
  // Ready is a machine the helper reached; the computer is made from it once.
  const adding = useRef(false);
  const ready = phase.kind === 'ready' && !!mac.create;
  useEffect(() => {
    if (!ready || adding.current || held) return;
    adding.current = true;
    send(null);
  });
  // The folder already marks another computer (another account's, or an
  // earlier install): taking it over is the user's explicit yes, as on the
  // SSH page.
  const takeOver = add.isError ? folderMarkedOwner(add.error) : null;

  // A machine the helper reached that cannot run as a computer makes none.
  const unsupported = phase.kind === 'ready' && !mac.create;
  let stage: PairStage;
  if (add.isSuccess) stage = 'paired';
  else if (add.isError || unsupported) stage = 'failed';
  else if (phase.kind === 'disclosure') stage = 'ask';
  else if (phase.kind === 'working') stage = phase.stage === 'turningOn' ? 'confirm' : 'pairing';
  else if (phase.kind === 'approval') stage = 'approval';
  else if (phase.kind === 'ready') stage = 'pairing';
  else stage = 'failed';

  const now = useNow(1000, stage === 'pairing');
  const elapsedSeconds = startedAt === null ? 0 : Math.max(0, Math.floor((now - startedAt) / 1000));

  let error: string | null = null;
  if (add.isError) {
    error = createRefusalMessage(add.error, t, 'standard');
  } else if (phase.kind === 'failed') {
    error = phase.timedOut ? t('setup:thisMac.timeout') : phase.message ?? t('computer.remoteControl.failed');
  } else if (phase.kind === 'insecure') {
    error = t('computer.remoteControl.insecure');
  } else if (unsupported) {
    error = [t('computer.ssh.error.unsupported'), ...(mac.ready?.probe.problems ?? [])].join(' ');
  }

  return {
    stage,
    error,
    elapsedSeconds,
    /** The computer this Mac became. */
    computer: add.data?.computer ?? null,
    /** Where Home is now that this Mac is a computer. */
    home: add.data?.home ?? null,
    /** The folder's other owner, where the create was refused for it. */
    takeOver,
    takeOverFolder: () => {
      if (!takeOver) return;
      add.reset();
      send(takeOver);
    },
    pair: () => {
      setStartedAt(Date.now());
      start();
    },
    /** Login Items' Open (the shell opens System Settings again while macOS
     *  still holds the helper back) and Try again: a new pairing, or the
     *  computer made again where only that failed. */
    retry: () => {
      if (add.isError) {
        add.reset();
        adding.current = false;
        return;
      }
      setStartedAt(Date.now());
      start();
    },
  };
}

export type ThisMacPairing = ReturnType<typeof useThisMacPairing>;

/** The page's title for a stage. */
export function pairingTitleKey(stage: PairStage): string {
  if (stage === 'paired') return 'setup:thisMac.paired.title';
  if (stage === 'ask' || stage === 'confirm') return 'setup:thisMac.ask.title';
  return 'setup:thisMac.pairing.title';
}

/**
 * The line under it: what pairing does to Home, from the answers before it
 * and from where Home went after; none once paired where Home stayed put.
 */
export function pairingLead(
  stage: PairStage,
  runsOn: OnboardingRunsOn | null | undefined,
  home: HomeOutcome | null,
): { key: string; name?: string } | null {
  if (stage === 'paired') {
    if (home?.kind === 'here') return { key: 'setup:thisMac.paired.home' };
    if (home?.kind === 'moving') {
      return home.fromMachine
        ? { key: 'setup:thisMac.paired.homeMovingFromMachine', name: home.fromMachine }
        : { key: 'setup:thisMac.paired.homeMoving' };
    }
    return null;
  }
  if (stage === 'ask' || stage === 'confirm') return { key: runsOn === 'cloud' ? 'setup:thisMac.ask.lead' : 'setup:thisMac.ask.leadHome' };
  return { key: 'setup:thisMac.pairing.lead' };
}
