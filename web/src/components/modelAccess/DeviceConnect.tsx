import { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { Loader } from '@/components/ui/loader';
import { useNow } from '@/hooks/useNow';
import { useRefreshAfterAccessChange } from '@/hooks/useModelAccessActions';
import {
  initiateDevice,
  pollDevice,
  readOAuthError,
  subscriptionKey,
  type DeviceCode,
  type DevicePollResult,
  type DeviceProvider,
} from '@/api/oauth';
import { failureFromError, nextPollStep, type DeviceFailure, type PollOutcome } from './devicePoll';
import { SubscriptionBefore } from './SubscriptionBefore';
import { CopyButton, FlowFailure, OpenProviderLink, StepRow } from './parts';
import { countdown } from './countdown';
import { copyText } from '@/lib/clipboard';

/** A code with no expiry from the server is given this long, as before. */
const FALLBACK_LIFETIME_MS = 15 * 60_000;

/** Failures that asking again fixes, so the button says "Try again". */
const TRANSIENT = new Set<DeviceFailure>(['redis_unavailable', 'upstream_unavailable', 'device_flow_unavailable']);


/**
 * Whether the copy under `modelAccess.<key>` words the variant where the
 * provider's page arrives with the code filled in; a provider without it keeps
 * the paste steps whatever the server says.
 */
const PREFILL: Record<DeviceProvider, boolean> = { 'codex-oauth': false, 'grok-oauth': true };

type Phase =
  | { kind: 'before' }
  | { kind: 'starting' }
  | { kind: 'waiting'; device: DeviceCode; copied: boolean; opened: boolean; deadline: number; retrying: boolean }
  | { kind: 'finishing'; device: DeviceCode }
  | { kind: 'failed'; failure: DeviceFailure };

/**
 * Puts the code on the clipboard as soon as it arrives, so the user only has to
 * paste it on the provider's page. A browser that refuses (no permission, an
 * unfocused page) leaves the Copy button, which is a gesture, to do it.
 */
async function copyToClipboard(text: string): Promise<boolean> {
  try {
    await copyText(text);
    return true;
  } catch {
    return false;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Connects a subscription through the provider's device sign-in (ChatGPT,
 * Grok): the user approves a short code on the provider's page while this
 * polls at the interval the server gives, backing off while the provider or
 * the network is unhappy. Settings and the setup wizard both render this;
 * `onConnected` is where they differ.
 */
export function DeviceConnect({
  provider,
  onConnected,
  onCancel,
}: {
  provider: DeviceProvider;
  onConnected: (result: DevicePollResult) => void;
  /** Leaves the flow; absent where the surrounding page has its own way back. */
  onCancel?: () => void;
}) {
  const { t } = useTranslation();
  const refresh = useRefreshAfterAccessChange();
  const [agreed, setAgreed] = useState(false);
  const [phase, setPhase] = useState<Phase>({ kind: 'before' });
  // Each start bumps the run; a loop that sees a newer run (or an unmount) stops.
  const run = useRef(0);
  useEffect(() => () => { run.current += 1; }, []);

  const ns = `modelAccess.${subscriptionKey(provider)}`;
  const now = useNow(1000, phase.kind === 'waiting');

  // Getting the code leaves the user here: they read it (already on their
  // clipboard, unless the provider's page fills it in) and the steps, then
  // open the provider themselves from the button. Polling starts at once, so
  // an approval made before this page catches up still lands.
  const start = async () => {
    const mine = ++run.current;
    setPhase({ kind: 'starting' });

    let device: DeviceCode;
    try {
      device = await initiateDevice(provider);
    } catch (err) {
      if (run.current === mine) setPhase({ kind: 'failed', failure: failureFromError(readOAuthError(err)) });
      return;
    }
    if (run.current !== mine) return;
    const prefilled = PREFILL[provider] && device.verification_url_has_code;
    // A page that fills the code in needs nothing pasted, so the clipboard is left alone.
    const copied = prefilled ? false : await copyToClipboard(device.user_code);
    if (run.current !== mine) return;
    const intervalMs = device.interval * 1000;
    const deadline = device.expires_at ? Date.parse(device.expires_at) : Date.now() + FALLBACK_LIFETIME_MS;
    setPhase({ kind: 'waiting', device, copied, opened: false, deadline, retrying: false });

    let retries = 0;
    let delay = intervalMs;
    for (;;) {
      await sleep(delay);
      if (run.current !== mine) return;
      let outcome: PollOutcome;
      try {
        outcome = { kind: 'result', result: await pollDevice(provider) };
      } catch (err) {
        outcome = { kind: 'error', error: readOAuthError(err) };
      }
      if (run.current !== mine) return;
      const step = nextPollStep(outcome, { intervalMs, retries, now: Date.now(), deadline });
      if (step.next === 'connected') {
        setPhase({ kind: 'finishing', device });
        await refresh(provider);
        if (run.current === mine) onConnected(step.result);
        return;
      }
      if (step.next === 'failed') {
        setPhase({ kind: 'failed', failure: step.failure });
        return;
      }
      retries = step.retries;
      delay = step.delayMs;
      const retrying = retries > 0;
      setPhase((p) => (p.kind === 'waiting' && p.retrying !== retrying ? { ...p, retrying } : p));
    }
  };

  const markWaiting = (patch: { copied?: boolean; opened?: boolean }) =>
    setPhase((p) => (p.kind === 'waiting' ? { ...p, ...patch } : p));

  const cancel = () => {
    run.current += 1;
    setPhase({ kind: 'before' });
    onCancel?.();
  };

  if (phase.kind === 'before' || phase.kind === 'starting') {
    const starting = phase.kind === 'starting';
    return (
      <div className="flex flex-col gap-4">
        <SubscriptionBefore provider={provider} agreed={agreed} onAgreedChange={setAgreed} />
        <div className="flex flex-wrap items-center gap-2">
          <Button type="button" onClick={start} disabled={!agreed || starting} aria-busy={starting} className="gap-1.5">
            {starting && <Loader size={14} className="text-current" />}
            {starting ? t(`${ns}.starting`) : t(`${ns}.start`)}
          </Button>
          {onCancel && (
            <Button type="button" variant="ghost" onClick={cancel} disabled={starting}>
              {t('common.cancel')}
            </Button>
          )}
        </div>
      </div>
    );
  }

  if (phase.kind === 'failed') {
    const transient = TRANSIENT.has(phase.failure);
    const actions = (
      <>
        <Button type="button" size="sm" onClick={start}>
          {t(transient ? 'modelAccess.tryAgain' : `${ns}.newCode`)}
        </Button>
        {onCancel && (
          <Button type="button" size="sm" variant="ghost" onClick={cancel}>
            {t('common.cancel')}
          </Button>
        )}
      </>
    );
    return (
      <FlowFailure
        title={t(`${ns}.fail.${phase.failure}`)}
        detail={t(transient ? `${ns}.failNextTransient` : `${ns}.failNext`)}
        action={actions}
      />
    );
  }

  const { device } = phase;
  const prefilled = PREFILL[provider] && device.verification_url_has_code;
  const finishing = phase.kind === 'finishing';
  const waiting = phase.kind === 'waiting' ? phase : null;
  const copied = finishing || !!waiting?.copied;
  const opened = finishing || !!waiting?.opened;
  const remaining = waiting ? waiting.deadline - now : 0;
  const onProviderPage = finishing ? 'done' : opened ? 'active' : 'todo';

  return (
    <div className="flex flex-col gap-5">
      <div className="flex flex-col items-center gap-3 rounded-lg p-5" style={{ background: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}>
        <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          {t(prefilled ? `${ns}.codeToMatch` : `${ns}.yourCode`)}
        </span>
        <code
          className="select-all rounded-md px-4 py-2 font-mono text-2xl font-semibold tracking-[0.2em]"
          style={{ color: 'var(--color-text-primary)', background: 'var(--color-bg-input)', border: '1px solid var(--color-border-default)' }}
        >
          {device.user_code}
        </code>
        <CopyButton text={device.user_code} onCopied={() => markWaiting({ copied: true })} />
        {waiting && (
          <span className="text-xs" style={{ color: 'var(--color-text-tertiary)', fontVariantNumeric: 'tabular-nums' }}>
            {waiting.retrying ? t(`${ns}.retrying`) : t(`${ns}.expiresIn`, { time: countdown(remaining) })}
          </span>
        )}
      </div>

      <ol className="flex flex-col gap-2.5" aria-live="polite">
        {prefilled ? (
          <>
            <StepRow state={opened ? 'done' : 'active'}>{t(`${ns}.stepOpenPrefilled`)}</StepRow>
            <StepRow state={onProviderPage}>{t(`${ns}.stepConfirm`)}</StepRow>
          </>
        ) : (
          <>
            <StepRow state={copied ? 'done' : 'active'}>
              {t(copied ? `${ns}.stepCopied` : `${ns}.stepCopy`)}
            </StepRow>
            <StepRow state={opened ? 'done' : 'active'}>{t(`${ns}.stepOpen`)}</StepRow>
          </>
        )}
        <StepRow state={onProviderPage}>{t(`${ns}.stepApprove`)}</StepRow>
        <StepRow state={finishing ? 'active' : 'todo'}>{t(`${ns}.stepPlan`)}</StepRow>
      </ol>

      <div className="flex flex-wrap items-center gap-2">
        <OpenProviderLink href={device.verification_url} used={opened} onOpen={() => markWaiting({ opened: true })}>
          {t(opened ? `${ns}.openAgain` : `${ns}.open`)}
        </OpenProviderLink>
        <Button type="button" variant="ghost" size="sm" onClick={cancel} disabled={finishing} className="ml-auto">
          {t('common.cancel')}
        </Button>
      </div>
    </div>
  );
}
