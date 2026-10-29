/**
 * The state behind adding this Mac by remote control: the disclosure and its
 * acknowledgment, a pairing the desktop helper takes up while the page polls
 * it, then the same ready step the SSH pairing ends in. The view is
 * `remoteControlSetup.tsx`; the dialog owns this hook, as it owns the SSH one.
 */
import { useEffect, useRef, useState } from 'react';
import { useQueryClient, type QueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';

import { enableRemoteControl, type RemoteControlError, type RemoteControlResult } from '@/lib/desktop';
import type { SshPairingStatus } from '@/types/api';

import { useSshPairingStatus } from '../hooks/useComputers';
import { releaseRemoteControl, setRemoteControlStatus, withRemoteControl } from '../hooks/useRemoteControl';
import { createSshPairing } from '../utils/api';
import { sshLimitMessage } from '../utils/sshLimit';
import { useSshReadyFields } from './sshSetupState';
import { pairingReady, type SshReady } from './sshTarget';

/**
 * How far the latest try went. The pairing it polls is named by its token,
 * once minted; `message` is the sentence to show, null for our own.
 * `insecure` is the app refusing a pairing address that is not https, which
 * no retry changes.
 */
export type RemoteControlAttempt =
  | { step: 'idle' }
  | { step: 'starting'; token: string | null }
  | { step: 'enabled'; token: string }
  | { step: 'approval'; token: string }
  | { step: 'failed'; message: string | null }
  | { step: 'insecure' };

export type RemoteControlPhase =
  | { kind: 'disclosure' }
  | { kind: 'working'; stage: 'turningOn' | 'connecting' }
  | { kind: 'approval' }
  | { kind: 'failed'; message: string | null }
  | { kind: 'insecure' }
  | { kind: 'ready'; ready: SshReady };

/** The footer's action in a phase that has one; the others show Add computer, closed until ready. */
export type RemoteControlAction = 'turnOn' | 'continue' | 'tryAgain';

/** What the shell answers having changed nothing, so the helper is as this try found it. */
const UNTOUCHED = new Set<RemoteControlError>(['cancelled', 'bad_url', 'unsupported', 'unavailable', 'not_replaced']);

/**
 * What the user sees, from the try and what its pairing says. The pairing
 * wins once it is ready, whatever the try last heard: allowing the helper in
 * System Settings starts it on the pairing it was given, with no Continue.
 */
export function remoteControlPhase(attempt: RemoteControlAttempt, pairing: SshPairingStatus | undefined): RemoteControlPhase {
  if (attempt.step === 'idle') return { kind: 'disclosure' };
  if (attempt.step === 'failed') return { kind: 'failed', message: attempt.message };
  if (attempt.step === 'insecure') return { kind: 'insecure' };
  const ready = pairingReady(pairing);
  if (ready) return { kind: 'ready', ready };
  // An expired pairing is what Continue replaces, so waiting on approval
  // outlasts it.
  if (pairing?.status === 'failed' && pairing.error?.code !== 'expired') {
    return { kind: 'failed', message: pairing.error?.message || null };
  }
  if (pairing?.status === 'checking' || pairing?.status === 'tunneling') return { kind: 'working', stage: 'connecting' };
  if (attempt.step === 'approval') return { kind: 'approval' };
  return { kind: 'working', stage: attempt.step === 'enabled' ? 'connecting' : 'turningOn' };
}

/** The try after the shell answered. A cancel in its own confirmation is the user's no, so it goes back quietly. */
export function attemptAfter(result: RemoteControlResult, token: string): RemoteControlAttempt {
  if (result.ok) return { step: 'enabled', token };
  switch (result.error) {
    case 'cancelled':
      return { step: 'idle' };
    case 'needs_approval':
      return { step: 'approval', token };
    case 'failed':
      return { step: 'failed', message: result.message || null };
    case 'bad_url':
      return { step: 'insecure' };
    default:
      return { step: 'failed', message: null };
  }
}

/**
 * Turns the helper off again if a try from this dialog touched it and no
 * computer is this Mac by then, once: whichever of moving away and closing
 * comes first owes it. Queued behind a try still in flight and asked only
 * then, so it never lands before the enable it undoes, and a try that ends
 * without touching the helper owes nothing.
 */
function releaseTouched(queryClient: QueryClient, touched: { current: boolean }): Promise<void> {
  return releaseRemoteControl(queryClient, {
    when: () => {
      const owed = touched.current;
      touched.current = false;
      return owed;
    },
  });
}

function actionFor(phase: RemoteControlPhase): RemoteControlAction | null {
  switch (phase.kind) {
    case 'disclosure':
      return 'turnOn';
    case 'approval':
      return 'continue';
    case 'failed':
      return 'tryAgain';
    default:
      return null;
  }
}

/**
 * `computerId` pairs the helper to reconnect that computer rather than to
 * add this Mac as a new one.
 */
export function useRemoteControlSetup(active: boolean, { computerId = null }: { computerId?: string | null } = {}) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  // Kept through a cancel or a failure: the user read it once and said yes.
  const [acknowledged, setAcknowledged] = useState(false);
  const [attempt, setAttempt] = useState<RemoteControlAttempt>({ step: 'idle' });

  const token = attempt.step === 'starting' || attempt.step === 'enabled' || attempt.step === 'approval' ? attempt.token : null;
  const pairingStatus = useSshPairingStatus(token, { enabled: active, paused: false });
  const pairing = token ? pairingStatus.data : undefined;
  const phase = remoteControlPhase(attempt, pairing);
  const fields = useSshReadyFields(phase.kind === 'ready' ? phase.ready : null);

  // A try is numbered, so the answer to one the user moved on from changes
  // nothing. `touched` is whether the helper took a pairing from this dialog,
  // which is what closing it must take back.
  const attemptId = useRef(0);
  const disposed = useRef(false);
  const touched = useRef(false);

  // Closing after the helper took a pairing turns it off again unless a
  // computer is this Mac by then: the one this dialog just created, which
  // the list holds once its create has landed.
  useEffect(() => {
    disposed.current = false;
    return () => {
      disposed.current = true;
      void releaseTouched(queryClient, touched);
    };
  }, [queryClient]);

  // Picking another provider abandons this setup as closing does, so the Mac
  // is not left paired behind a dialog showing something else. Coming back
  // starts again from the disclosure.
  const [wasActive, setWasActive] = useState(active);
  if (wasActive !== active) {
    setWasActive(active);
    if (!active) setAttempt({ step: 'idle' });
  }
  useEffect(() => {
    if (active) return;
    attemptId.current += 1;
    void releaseTouched(queryClient, touched);
  }, [active, queryClient]);

  const action = actionFor(phase);

  const start = () => {
    if (!acknowledged || !action) return;
    const id = ++attemptId.current;
    const current = () => attemptId.current === id && !disposed.current;
    setAttempt({ step: 'starting', token: null });
    // A helper already paired, to an abandoned setup or to another account
    // signed in on this Mac, is the app's to replace, after its own
    // confirmation says so: turning it off here would break that account's
    // computer even when the user then said no.
    void withRemoteControl(async () => {
      if (!current()) return;
      const minted = await createSshPairing(computerId ? { computerId } : undefined);
      if (!current()) return;
      setAttempt({ step: 'starting', token: minted.token });
      const result = await enableRemoteControl(minted.script_url);
      if (result.ok || !UNTOUCHED.has(result.error)) touched.current = true;
      setRemoteControlStatus(queryClient, result.status);
      if (current()) setAttempt(attemptAfter(result, minted.token));
    }).catch((err: unknown) => {
      // Only the mint rejects: every answer from the shell is a result.
      if (current()) setAttempt({ step: 'failed', message: sshLimitMessage(err, t) });
    });
  };

  return {
    acknowledged,
    setAcknowledged,
    phase,
    action,
    /** Turn on, Continue and Try again alike: each mints a pairing and hands it to the helper. */
    start,
    readyStep: fields.step,
    /** What the helper found on this Mac, once it is ready. */
    ready: phase.kind === 'ready' ? phase.ready : null,
    /** The machine's part of the create body, once a supported Mac is ready. */
    create: fields.create,
    defaultName: fields.defaultName,
  };
}

export type RemoteControlSetupState = ReturnType<typeof useRemoteControlSetup>;
