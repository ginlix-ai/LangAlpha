import { Fragment, useEffect, useId, useRef, useState } from 'react';
import { ExternalLink, Shield } from 'lucide-react';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Loader } from '@/components/ui/loader';
import { ModalShell } from '@/components/ui/ModalShell';
import { useNow } from '@/hooks/useNow';
import {
  removeClaudeCodeToken, saveClaudeCodeToken, startClaudeCodeSignIn, submitClaudeCodeSignInCode, type ClaudeCodeStatus,
} from '@/api/model';
import { readOAuthError } from '@/api/oauth';
import { queryKeys } from '@/lib/queryKeys';
import { countdown } from './countdown';
import { CopyButton, OpenProviderLink } from './parts';

/** Claude Code's install page, where step one sends anyone without it. */
const INSTALL_URL = 'https://code.claude.com/docs/en/setup';
const COMMAND = 'claude setup-token';

/** The refusals each section words itself, by its copy's prefix; any other
 *  code shows the server's sentence. A `<code>_spent` entry words the same
 *  refusal once it is no longer retryable. */
const WORDED = {
  'modelAccess.claudeCode': new Set(['token_missing', 'token_rejected', 'check_unavailable', 'check_in_progress', 'unavailable']),
  'modelAccess.claudeCode.signIn': new Set([
    'unavailable', 'redis_unavailable', 'code_missing', 'invalid_callback_input', 'state_mismatch', 'no_pending_flow',
    'code_rejected', 'check_unavailable', 'check_unavailable_spent', 'token_rejected', 'unexpected_grant', 'check_in_progress',
  ]),
};

type Refusal = { code: string; message: string; retryable: boolean };

const refusalOf = (err: unknown): Refusal => {
  const detail = readOAuthError(err);
  return { code: detail?.code ?? 'unknown', message: detail?.message ?? '', retryable: detail?.retryable ?? false };
};

function say(t: (key: string) => string, scope: keyof typeof WORDED, r: Refusal): string {
  const worded = WORDED[scope];
  const code = !r.retryable && worded.has(`${r.code}_spent`) ? `${r.code}_spent` : r.code;
  if (worded.has(code)) return t(`${scope}.fail.${code}`);
  return r.message || t(`${scope}.fail.unknown`);
}

/** Keeps the status a save answered with, which is the status itself: the
 *  row and the picker read it with no second request. */
function useStoreStatus() {
  const queryClient = useQueryClient();
  return (status: ClaudeCodeStatus) => queryClient.setQueryData(queryKeys.features.claudeCodeStatus(), status);
}

/** The server keeps a sign-in this long when its answer has no usable time. */
const FALLBACK_LIFETIME_MS = 10 * 60_000;

/**
 * Adds or replaces the setup token Claude Code signs in with: what the token
 * is, then each way to get one in, top to bottom: signing in to Claude, which
 * makes the token here, or pasting one made with the CLI. Each way is a
 * section that saves on its own and calls `onSaved`.
 */
export function ClaudeCodeTokenSheet({ onSaved, onCancel }: { onSaved: () => void; onCancel: () => void }) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col gap-5">
      <div className="flex flex-col gap-2">
        <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{t('modelAccess.claudeCode.intro')}</p>
        <p className="flex items-start gap-1.5 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          <Shield aria-hidden="true" className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          <span>{t('modelAccess.claudeCode.separate')}</span>
        </p>
      </div>
      <SignInSection onSaved={onSaved} />
      <section className="flex flex-col gap-3">
        <h3 className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('modelAccess.claudeCode.pasteHeading')}</h3>
        <PastedTokenForm onSaved={onSaved} onCancel={onCancel} />
      </section>
    </div>
  );
}

type SignIn =
  | { kind: 'start'; failure: Refusal | null }
  | { kind: 'starting' }
  | { kind: 'code'; authorizeUrl: string; deadline: number; opened: boolean };

/** A sign-in that ran out of time, read as the server words one it no longer holds. */
const EXPIRED: Refusal = { code: 'no_pending_flow', message: '', retryable: false };

/**
 * Signs in to Claude in a tab the user opens from here, as ClaudeConnect
 * does, so no popup blocker stands in the way, then takes the code Claude's
 * page shows, which the server turns into a setup token of its own and
 * checks before saving. A refusal the same code cannot get past starts the sign-in over,
 * since the code is spent; any other leaves the code to fix or send again.
 */
function SignInSection({ onSaved }: { onSaved: () => void }) {
  const { t } = useTranslation();
  const storeStatus = useStoreStatus();
  const fieldId = useId();
  const errorId = useId();
  const field = useRef<HTMLInputElement>(null);
  const [phase, setPhase] = useState<SignIn>({ kind: 'start', failure: null });
  const [code, setCode] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [refusal, setRefusal] = useState<Refusal | null>(null);
  const run = useRef(0);
  useEffect(() => () => { run.current += 1; }, []);

  const pending = phase.kind === 'code' ? phase : null;
  const now = useNow(1000, !!pending && !submitting);

  // Coming back from claude.ai lands the cursor in the field.
  const opened = !!pending?.opened;
  useEffect(() => {
    if (!opened) return;
    const focus = () => field.current?.focus();
    window.addEventListener('focus', focus);
    return () => window.removeEventListener('focus', focus);
  }, [opened]);
  if (pending && !submitting && now >= pending.deadline) {
    setPhase({ kind: 'start', failure: EXPIRED });
    setCode('');
    setRefusal(null);
  }

  const start = async () => {
    const mine = ++run.current;
    setPhase({ kind: 'starting' });
    setCode('');
    setRefusal(null);
    let flow: { authorize_url: string; expires_at: string };
    try {
      flow = await startClaudeCodeSignIn();
    } catch (err) {
      if (run.current === mine) setPhase({ kind: 'start', failure: refusalOf(err) });
      return;
    }
    if (run.current !== mine) return;
    const deadline = Date.parse(flow.expires_at) || Date.now() + FALLBACK_LIFETIME_MS;
    setPhase({ kind: 'code', authorizeUrl: flow.authorize_url, deadline, opened: false });
  };

  // A code copied off a narrow page can carry a line break.
  const value = code.replace(/\s+/g, '');

  const submit = async () => {
    if (!value || submitting) return;
    const mine = run.current;
    setSubmitting(true);
    setRefusal(null);
    let status: ClaudeCodeStatus;
    try {
      status = await submitClaudeCodeSignInCode(value);
    } catch (err) {
      if (run.current !== mine) return;
      const r = refusalOf(err);
      setSubmitting(false);
      if (r.retryable) {
        setRefusal(r);
      } else {
        setPhase({ kind: 'start', failure: r });
        setCode('');
      }
      return;
    }
    // Whether or not the sheet is still open: the save landed.
    storeStatus(status);
    if (run.current !== mine) return;
    setCode('');
    onSaved();
  };

  const card = { background: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' };

  if (!pending) {
    const starting = phase.kind === 'starting';
    const failure = phase.kind === 'start' ? phase.failure : null;
    return (
      <section className="flex flex-col items-start gap-3 rounded-lg p-4" style={card}>
        <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{t('modelAccess.claudeCode.signIn.intro')}</p>
        {failure && (
          <p role="alert" className="text-xs" style={{ color: 'var(--color-loss)' }}>{say(t, 'modelAccess.claudeCode.signIn', failure)}</p>
        )}
        <Button type="button" onClick={start} disabled={starting} aria-busy={starting} className="gap-1.5">
          {starting && <Loader size={14} className="text-current" />}
          {starting ? t('modelAccess.claude.starting') : t('modelAccess.claudeCode.signIn.start')}
        </Button>
      </section>
    );
  }

  return (
    <form
      className="flex flex-col gap-3 rounded-lg p-4"
      style={card}
      onSubmit={(e) => {
        e.preventDefault();
        void submit();
      }}
    >
      <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{t('modelAccess.claudeCode.signIn.codeHint')}</p>
      <div className="flex flex-wrap items-center gap-3">
        <OpenProviderLink
          href={pending.authorizeUrl}
          used={pending.opened}
          onOpen={() => setPhase((p) => (p.kind === 'code' ? { ...p, opened: true } : p))}
        >
          {t(pending.opened ? 'modelAccess.claude.openAgain' : 'modelAccess.claude.open')}
        </OpenProviderLink>
        <span className="text-xs" style={{ color: 'var(--color-text-tertiary)', fontVariantNumeric: 'tabular-nums' }}>
          {t('modelAccess.claude.expiresIn', { time: countdown(pending.deadline - now) })}
        </span>
      </div>
      <div className="flex flex-col gap-1.5">
        <label htmlFor={fieldId} className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {t('modelAccess.claudeCode.signIn.fieldLabel')}
        </label>
        <div className="flex gap-2">
          <Input
            ref={field}
            id={fieldId}
            type="password"
            value={code}
            onChange={(e) => {
              setCode(e.target.value);
              setRefusal(null);
            }}
            placeholder={t('modelAccess.claudeCode.signIn.fieldPlaceholder')}
            aria-invalid={!!refusal || undefined}
            aria-describedby={refusal ? errorId : undefined}
            className="flex-1 font-mono text-sm"
            autoComplete="off"
            autoCapitalize="off"
            autoCorrect="off"
            spellCheck={false}
            disabled={submitting}
          />
          <Button type="submit" disabled={!value || submitting} aria-busy={submitting} className="shrink-0 gap-1.5">
            {submitting && <Loader size={14} className="text-current" />}
            {submitting ? t('modelAccess.claudeCode.checking') : t('modelAccess.claude.submit')}
          </Button>
        </div>
        {refusal && (
          <p id={errorId} role="alert" className="text-xs" style={{ color: 'var(--color-loss)' }}>{say(t, 'modelAccess.claudeCode.signIn', refusal)}</p>
        )}
      </div>
    </form>
  );
}

/**
 * A token the user made with `claude setup-token`, on this computer or any
 * other: how to make one, then the field it is pasted into. Saving waits on
 * Anthropic taking the token, so a refused one is never saved. The token
 * lives in this form's field alone, and leaves with it.
 */
function PastedTokenForm({ onSaved, onCancel }: { onSaved: () => void; onCancel: () => void }) {
  const { t } = useTranslation();
  const storeStatus = useStoreStatus();
  const fieldId = useId();
  const errorId = useId();
  const [token, setToken] = useState('');
  const [saving, setSaving] = useState(false);
  const [refusal, setRefusal] = useState<Refusal | null>(null);
  const run = useRef(0);
  useEffect(() => () => { run.current += 1; }, []);

  // The command prints a token wide enough to wrap, and a copy of it can
  // carry the line break.
  const value = token.replace(/\s+/g, '');

  const save = async () => {
    if (!value || saving) return;
    const mine = run.current;
    setSaving(true);
    setRefusal(null);
    let status: ClaudeCodeStatus;
    try {
      status = await saveClaudeCodeToken(value);
    } catch (err) {
      if (run.current !== mine) return;
      const r = refusalOf(err);
      setSaving(false);
      // A refused token is no use to anyone, so it goes; one the check never
      // reached stays for another try.
      if (r.code === 'token_rejected') setToken('');
      setRefusal(r);
      return;
    }
    // Whether or not the form is still open: the save landed.
    storeStatus(status);
    if (run.current !== mine) return;
    setToken('');
    onSaved();
  };

  const refusalText = refusal ? say(t, 'modelAccess.claudeCode', refusal) : null;

  const steps = [
    <Fragment key="install">
      {t('modelAccess.claudeCode.stepInstall')}{' '}
      <a
        href={INSTALL_URL}
        target="_blank"
        rel="noopener noreferrer"
        className="inline-flex items-center gap-1 underline underline-offset-2 hover:opacity-80"
      >
        {t('modelAccess.claudeCode.installLink')}
        <ExternalLink aria-hidden="true" className="h-3 w-3" />
      </a>
    </Fragment>,
    <span key="run" className="flex flex-col items-start gap-1.5">
      {t('modelAccess.claudeCode.stepRun')}
      <span className="flex items-center gap-2">
        <code
          className="rounded-md px-2 py-1 font-mono text-xs"
          style={{ background: 'var(--color-bg-tag)', color: 'var(--color-text-primary)' }}
        >
          {COMMAND}
        </code>
        <CopyButton text={COMMAND} />
      </span>
    </span>,
    t('modelAccess.claudeCode.stepSignIn'),
    t('modelAccess.claudeCode.stepCopy'),
    t('modelAccess.claudeCode.stepPaste'),
  ];

  return (
    <form
      className="flex flex-col gap-5"
      onSubmit={(e) => {
        e.preventDefault();
        void save();
      }}
    >
      <ol className="flex flex-col gap-3">
        {steps.map((step, i) => (
          <li key={i} className="flex items-start gap-2.5 text-sm" style={{ color: 'var(--color-text-primary)' }}>
            <span
              aria-hidden="true"
              className="flex h-5 w-5 shrink-0 items-center justify-center rounded-full text-xs font-medium"
              style={{ background: 'var(--color-bg-tag)', color: 'var(--color-text-secondary)', fontVariantNumeric: 'tabular-nums' }}
            >
              {i + 1}
            </span>
            <span className="min-w-0 pt-px">{step}</span>
          </li>
        ))}
      </ol>

      <div className="flex flex-col gap-1.5">
        <label htmlFor={fieldId} className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {t('modelAccess.claudeCode.fieldLabel')}
        </label>
        <Input
          id={fieldId}
          type="password"
          value={token}
          onChange={(e) => {
            setToken(e.target.value);
            setRefusal(null);
          }}
          placeholder={t('modelAccess.claudeCode.fieldPlaceholder')}
          aria-invalid={!!refusal || undefined}
          aria-describedby={refusal ? errorId : undefined}
          className="font-mono text-sm"
          autoComplete="off"
          autoCapitalize="off"
          autoCorrect="off"
          spellCheck={false}
          disabled={saving}
        />
        {refusalText && (
          <p id={errorId} role="alert" className="text-xs" style={{ color: 'var(--color-loss)' }}>
            {refusalText}
          </p>
        )}
      </div>

      <div className="flex items-center justify-end gap-2">
        <Button type="button" variant="ghost" onClick={onCancel} disabled={saving}>
          {t('common.cancel')}
        </Button>
        <Button type="submit" disabled={!value || saving} aria-busy={saving} className="gap-1.5">
          {saving && <Loader size={14} className="text-current" />}
          {saving ? t('modelAccess.claudeCode.checking') : t('modelAccess.claudeCode.save')}
        </Button>
      </div>
    </form>
  );
}

/** Confirms removing the setup token, runs it, and keeps the dialog open with
 *  the reason when it fails. Mounted only while open. */
export function RemoveClaudeCodeTokenDialog({ onClose }: { onClose: () => void }) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const titleId = useId();
  const [busy, setBusy] = useState(false);
  const [failed, setFailed] = useState(false);

  const confirm = async () => {
    setBusy(true);
    setFailed(false);
    try {
      await removeClaudeCodeToken();
    } catch {
      setFailed(true);
      setBusy(false);
      return;
    }
    await queryClient.invalidateQueries({ queryKey: queryKeys.features.claudeCodeStatus() });
    onClose();
  };

  return (
    <ModalShell
      labelId={titleId}
      title={t('modelAccess.claudeCode.removeTitle')}
      onClose={onClose}
      closeDisabled={busy}
      width="narrow"
      footer={
        <div className="flex items-center justify-end gap-2">
          <Button type="button" variant="ghost" onClick={onClose} disabled={busy}>
            {t('common.cancel')}
          </Button>
          <Button type="button" variant="destructive" onClick={confirm} disabled={busy} aria-busy={busy}>
            {busy ? t('modelAccess.remove.removing') : t('modelAccess.claudeCode.removeConfirm')}
          </Button>
        </div>
      }
    >
      <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
        {t('modelAccess.claudeCode.removeBody')}
      </p>
      {failed && !busy && (
        <p role="alert" className="text-sm" style={{ color: 'var(--color-loss)' }}>
          {t('modelAccess.claudeCode.removeFailed')}
        </p>
      )}
    </ModalShell>
  );
}
