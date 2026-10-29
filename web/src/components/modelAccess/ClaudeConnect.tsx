import { useEffect, useId, useRef, useState } from 'react';
import { ClipboardPaste } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Loader } from '@/components/ui/loader';
import { useNow } from '@/hooks/useNow';
import { useRefreshAfterAccessChange } from '@/hooks/useModelAccessActions';
import { initiateClaudeOAuth, readOAuthError, submitClaudeCallback } from '@/api/oauth';
import { checkClaudePaste, type PasteShape } from './claudePaste';
import { SubscriptionBefore } from './SubscriptionBefore';
import { FlowFailure, OpenProviderLink, StepRow } from './parts';
import { countdown } from './countdown';

/** Refusals worded here; any other code reads as `unknown`. */
const CODES = new Set([
  'invalid_callback_input',
  'state_mismatch',
  'no_pending_flow',
  'exchange_failed',
  'upstream_unavailable',
  'save_failed',
  'redis_unavailable',
]);

/** Refusals after which the same sign-in can still finish: fix the paste, or send it again. */
const STAYS_OPEN = new Set(['invalid_callback_input', 'upstream_unavailable', 'redis_unavailable']);

/** The server keeps a sign-in this long when it does not say. */
const FALLBACK_LIFETIME_MS = 10 * 60_000;

type Phase =
  | { kind: 'before' }
  | { kind: 'starting' }
  | { kind: 'pasting'; authorizeUrl: string; deadline: number; opened: boolean }
  | { kind: 'failed'; code: string };

/** What to say under the field for a paste that is not the code yet. */
const SHAPE_HINT: Partial<Record<PasteShape, string>> = {
  bare_page: 'modelAccess.claude.pasteBarePage',
  code_only: 'modelAccess.claude.pasteCodeOnly',
  unknown: 'modelAccess.claude.pasteUnknown',
};

/**
 * Connects a Claude subscription by paste-back. Claude's page shows a code
 * after the user approves, and they bring it here. Like DeviceConnect, getting
 * ready never sends the user away: they read the steps, open claude.ai from
 * the button, and a paste that is the code connects without another click.
 * No Claude-only extras, so it can be withdrawn on its own.
 */
export function ClaudeConnect({
  onConnected,
  onCancel,
}: {
  onConnected: () => void;
  onCancel?: () => void;
}) {
  const { t } = useTranslation();
  const refresh = useRefreshAfterAccessChange();
  const fieldId = useId();
  const hintId = useId();
  const field = useRef<HTMLInputElement>(null);
  const [agreed, setAgreed] = useState(false);
  const [phase, setPhase] = useState<Phase>({ kind: 'before' });
  const [paste, setPaste] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [refusal, setRefusal] = useState<string | null>(null);
  const run = useRef(0);
  useEffect(() => () => { run.current += 1; }, []);

  const pasting = phase.kind === 'pasting' ? phase : null;
  const now = useNow(1000, !!pasting && !submitting);
  const expired = !!pasting && !submitting && now >= pasting.deadline;

  // Coming back from claude.ai lands the cursor in the field, so the code is
  // one keystroke away.
  const opened = !!pasting?.opened;
  useEffect(() => {
    if (!opened) return;
    const focus = () => field.current?.focus();
    window.addEventListener('focus', focus);
    return () => window.removeEventListener('focus', focus);
  }, [opened]);

  const start = async () => {
    const mine = ++run.current;
    setPhase({ kind: 'starting' });
    setPaste('');
    setRefusal(null);
    let flow: { authorize_url: string; expires_at: string | null };
    try {
      flow = await initiateClaudeOAuth();
    } catch (err) {
      if (run.current === mine) setPhase({ kind: 'failed', code: readOAuthError(err)?.code ?? 'unknown' });
      return;
    }
    if (run.current !== mine) return;
    const deadline = flow.expires_at ? Date.parse(flow.expires_at) : Date.now() + FALLBACK_LIFETIME_MS;
    setPhase({ kind: 'pasting', authorizeUrl: flow.authorize_url, deadline, opened: false });
  };

  const submit = async (value: string) => {
    if (!checkClaudePaste(value).ok || submitting) return;
    const mine = run.current;
    setSubmitting(true);
    setRefusal(null);
    try {
      await submitClaudeCallback(value.trim());
    } catch (err) {
      if (run.current !== mine) return;
      setSubmitting(false);
      const code = readOAuthError(err)?.code ?? 'unknown';
      if (STAYS_OPEN.has(code)) setRefusal(code);
      else setPhase({ kind: 'failed', code });
      return;
    }
    await refresh('claude-oauth');
    if (run.current === mine) onConnected();
  };

  // A paste that is the code goes straight through; anything else waits in
  // the field with a hint, so a wrong paste never spends the single-use code.
  const take = (value: string) => {
    setPaste(value);
    setRefusal(null);
    if (checkClaudePaste(value).ok) void submit(value);
  };

  const pasteFromClipboard = async () => {
    try {
      take(await navigator.clipboard.readText());
    } catch {
      // Clipboard refused: the field is focused for a manual paste.
      field.current?.focus();
    }
  };

  const cancel = () => {
    run.current += 1;
    setSubmitting(false);
    setPhase({ kind: 'before' });
    onCancel?.();
  };

  if (phase.kind === 'before' || phase.kind === 'starting') {
    const starting = phase.kind === 'starting';
    return (
      <div className="flex flex-col gap-4">
        <SubscriptionBefore provider="claude-oauth" agreed={agreed} onAgreedChange={setAgreed} />
        <div className="flex flex-wrap items-center gap-2">
          <Button type="button" onClick={start} disabled={!agreed || starting} aria-busy={starting} className="gap-1.5">
            {starting && <Loader size={14} className="text-current" />}
            {starting ? t('modelAccess.claude.starting') : t('modelAccess.claude.start')}
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

  if (phase.kind === 'failed' || expired) {
    const raw = phase.kind === 'failed' ? phase.code : 'no_pending_flow';
    const code = CODES.has(raw) ? raw : 'unknown';
    const transient = code === 'redis_unavailable' || code === 'upstream_unavailable';
    return (
      <FlowFailure
        title={t(`modelAccess.claude.fail.${code}`)}
        detail={t(transient ? 'modelAccess.claude.failNextTransient' : 'modelAccess.claude.failNext')}
        action={
          <>
            <Button type="button" size="sm" onClick={start}>
              {t(transient ? 'modelAccess.tryAgain' : 'modelAccess.claude.restart')}
            </Button>
            {onCancel && (
              <Button type="button" size="sm" variant="ghost" onClick={cancel}>
                {t('common.cancel')}
              </Button>
            )}
          </>
        }
      />
    );
  }

  const check = checkClaudePaste(paste);
  const hintKey = refusal
    ? `modelAccess.claude.fail.${CODES.has(refusal) ? refusal : 'unknown'}`
    : paste.trim() ? SHAPE_HINT[check.shape] : undefined;

  return (
    <form
      className="flex flex-col gap-5"
      onSubmit={(e) => {
        e.preventDefault();
        void submit(paste);
      }}
    >
      <div className="flex flex-col items-center gap-3 rounded-lg p-5" style={{ background: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}>
        <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('modelAccess.claude.signInLabel')}</span>
        <OpenProviderLink
          href={phase.authorizeUrl}
          used={phase.opened}
          onOpen={() => setPhase((p) => (p.kind === 'pasting' ? { ...p, opened: true } : p))}
        >
          {t(phase.opened ? 'modelAccess.claude.openAgain' : 'modelAccess.claude.open')}
        </OpenProviderLink>
        <span className="text-xs" style={{ color: 'var(--color-text-tertiary)', fontVariantNumeric: 'tabular-nums' }}>
          {t('modelAccess.claude.expiresIn', { time: countdown(phase.deadline - now) })}
        </span>
      </div>

      <ol className="flex flex-col gap-2.5" aria-live="polite">
        <StepRow state={phase.opened ? 'done' : 'active'}>{t('modelAccess.claude.stepOpen')}</StepRow>
        <StepRow state={submitting ? 'done' : phase.opened ? 'active' : 'todo'}>{t('modelAccess.claude.stepPaste')}</StepRow>
        <StepRow state={submitting ? 'active' : 'todo'}>{t('modelAccess.claude.stepConnect')}</StepRow>
      </ol>

      <div className="flex flex-col gap-1.5">
        <label htmlFor={fieldId} className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {t('modelAccess.claude.pasteLabel')}
        </label>
        <div className="flex gap-2">
          <Input
            ref={field}
            id={fieldId}
            value={paste}
            onChange={(e) => {
              setPaste(e.target.value);
              setRefusal(null);
            }}
            onPaste={(e) => {
              const text = e.clipboardData.getData('text');
              if (!text) return;
              e.preventDefault();
              take(text);
            }}
            placeholder={t('modelAccess.claude.pastePlaceholder')}
            aria-invalid={!!hintKey || undefined}
            aria-describedby={hintKey ? hintId : undefined}
            className="flex-1 font-mono text-sm"
            autoComplete="off"
            spellCheck={false}
            disabled={submitting}
          />
          {check.ok || submitting ? (
            <Button type="submit" disabled={submitting} aria-busy={submitting} className="shrink-0 gap-1.5">
              {submitting && <Loader size={14} className="text-current" />}
              {submitting ? t('modelAccess.claude.connecting') : t('modelAccess.claude.submit')}
            </Button>
          ) : (
            <Button type="button" variant="outline" onClick={pasteFromClipboard} className="shrink-0 gap-1.5">
              <ClipboardPaste aria-hidden="true" className="h-4 w-4" />
              {t('modelAccess.claude.paste')}
            </Button>
          )}
        </div>
        {hintKey && (
          <p id={hintId} role={refusal ? 'alert' : undefined} className="text-xs" style={{ color: refusal ? 'var(--color-loss)' : 'var(--color-warning)' }}>
            {t(hintKey)}
          </p>
        )}
      </div>

      <div className="flex justify-end">
        <Button type="button" variant="ghost" size="sm" onClick={cancel} disabled={submitting}>
          {t('common.cancel')}
        </Button>
      </div>
    </form>
  );
}
