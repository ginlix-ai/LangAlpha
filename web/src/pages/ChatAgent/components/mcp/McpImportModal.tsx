import React, { useDeferredValue, useEffect, useId, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { TFunction } from 'i18next';
import { Download, CheckCircle2, AlertTriangle, KeyRound, LogIn, Power, Wrench } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { ModalShell } from '@/components/ui/ModalShell';
import { McpQuietButton } from './McpQuietButton';
import { normalizeMcpServers, parseMcpServersJson, type ParsedImportServer } from './mcpImport';
import {
  findPlaceholders,
  placeholderLabel,
  substitutePlaceholders,
  type Placeholder,
} from './mcpImportPlaceholders';
import {
  formatApiErrorDetail,
  type McpImportResult,
  type McpImportResultRow,
  type McpServerInput,
} from '../../utils/api';
import { refusalSaying, type Refusal } from '../../utils/api/refusal';

/**
 * Bulk-import modal: paste a standard `{ "mcpServers": { … } }` blob and create
 * every server at once. The textarea is parsed client-side for an instant
 * count preview. When the config carries placeholder credentials
 * (`<your-api-key>`), a second step asks each server that wrote one for its own
 * value and substitutes it into that server before the POST; the backend then
 * vaults the literal. Every placeholder needs its value: each server is checked
 * with what it will be saved with, and a stand-in fails that check.
 *
 * Per-server outcomes are shown after import: every entry that was not added
 * says why under its name, one that failed can be opened in the add form as it
 * was sent to be fixed there, one that signs in offers its sign-in (which
 * saves it), and where the caller offers one, a created row can be switched
 * on, since imported rows start off.
 */

const PLACEHOLDER = `{
  "mcpServers": {
    "my-server": {
      "type": "http",
      "url": "https://api.example.com/mcp",
      "headers": { "Authorization": "<token>" }
    }
  }
}`;

export interface McpImportModalProps {
  onClose: () => void;
  onImport: (payload: unknown) => Promise<McpImportResult>;
  /** Switch one created row on from the result view. Absent where rows start on. */
  onEnable?: (name: string) => Promise<void>;
  /** Start the sign-in that saves an entry the import could not save without
   *  one. The page leaves for the vendor's when it starts. */
  onSignIn?: (server: McpServerInput) => void;
  /** The entry whose sign-in is starting, if any. */
  signingIn?: string | null;
  /** Open the add form filled with an entry that was not saved, to fix it
   *  there, with the reason its row gave, which the form has to go on saying:
   *  this dialog closes, and it was the only place that said it. */
  onFix?: (server: McpServerInput, refusal: Refusal | null) => void;
}

// Explicit map so the i18n parity test sees each key as a literal.
const STATUS_KEYS: Record<McpImportResultRow['status'], string> = {
  created: 'mcp.import.status.created',
  exists: 'mcp.import.status.exists',
  skipped: 'mcp.import.status.skipped',
  invalid: 'mcp.import.status.invalid',
  error: 'mcp.import.status.error',
  check_failed: 'mcp.import.status.check_failed',
  sign_in_required: 'mcp.import.status.sign_in_required',
};

/**
 * An entry as it was sent, placeholders filled, under the name the import gave
 * it: for the add form to open on to fix it, or for the sign-in that saves it.
 * Headers travel as written; a check that found one to be a credential would
 * have reported the key refused rather than asked for a sign-in.
 */
function entryBody(row: McpImportResultRow, parsed: ParsedImportServer | undefined): McpServerInput | null {
  if (!parsed) return row.url ? { name: row.name, transport: 'http', url: row.url } : null;
  const common = {
    name: row.name,
    ...(parsed.description ? { description: parsed.description } : {}),
    ...(parsed.instruction ? { instruction: parsed.instruction } : {}),
    ...(parsed.toolExposureMode ? { tool_exposure_mode: parsed.toolExposureMode } : {}),
  };
  if (parsed.transport === 'stdio') {
    return { ...common, transport: 'stdio', command: parsed.command, args: parsed.args, env: parsed.env };
  }
  return { ...common, transport: parsed.transport, url: row.url || parsed.url, headers: parsed.headers };
}

/**
 * Why an entry was not added, as the add form says a refusal of its own save:
 * a check that failed is that same check's failure, placed beside what fixes
 * it, and anything else is said as the import's own sentence.
 */
function notAddedBecause(row: McpImportResultRow, t: TFunction): Refusal | null {
  const why = row.error || row.reason;
  if (!why) return null;
  if (row.verdict) {
    const failure = {
      server: row.name,
      verdict: row.verdict,
      reason: why,
      missing_secrets: row.missing_secrets ?? [],
    };
    return { message: why, failures: [failure], diagnostics: [] };
  }
  return refusalSaying(t('plugins.signin.joined', { first: t('mcp.modal.importNotAdded'), second: why }));
}

// What the add form can do something about. An entry that already exists, or
// that the import skipped as a repeat, has nothing to fix.
const FIXABLE = new Set<McpImportResultRow['status']>(['invalid', 'error', 'check_failed']);

const NOOP = () => {};

type Step = 'paste' | 'credentials';

export function McpImportModal({
  onClose,
  onImport,
  onEnable,
  onSignIn,
  signingIn = null,
  onFix,
}: McpImportModalProps) {
  const { t } = useTranslation();
  const [text, setText] = useState('');
  const [step, setStep] = useState<Step>('paste');
  const [values, setValues] = useState<Record<string, string>>({});
  const [importing, setImporting] = useState(false);
  // Every dismissal route waits out an import: its report is the only record of
  // which servers were created and which secrets went into the vault.
  const close = importing ? NOOP : onClose;
  const titleId = useId();
  const [result, setResult] = useState<McpImportResult | null>(null);
  // The servers as they were sent, placeholders filled, so a sign-in offered
  // for one of them carries what the user typed rather than the stand-in.
  const [sent, setSent] = useState<ParsedImportServer[]>([]);
  const sentFor = (row: McpImportResultRow) => sent.find((p) => p.originalName === row.original_name);
  const [error, setError] = useState<string | null>(null);

  // Defer the parse so a fast typer doesn't pay full JSON.parse + normalization
  // on every keystroke; the count preview lags a frame behind the textarea.
  const deferredText = useDeferredValue(text);
  const preview = useMemo(() => parseMcpServersJson(deferredText), [deferredText]);
  const parseableCount = preview.servers.filter((s) => !s.error).length;
  const placeholders = useMemo(() => findPlaceholders(preview.servers), [preview.servers]);
  // A blank placeholder would be checked as written and fail, so every one has
  // to be answered before the import is sent.
  const unanswered = placeholders.some((p) => !(values[p.id] ?? '').trim());
  const canImport =
    !importing && parseableCount > 0 && !(step === 'credentials' && unanswered);

  // An answer belongs to the config that asked for it. Going Back and pasting a
  // different vendor's file is a different set of questions, and one that
  // happens to share a server name and a placeholder spelling with the last
  // paste would otherwise arrive with the previous vendor's key already in it.
  useEffect(() => {
    setValues((v) => (Object.keys(v).length === 0 ? v : {}));
  }, [text]);

  async function handleImport() {
    let payload: unknown;
    try {
      payload = JSON.parse(text.trim());
    } catch {
      setError(t('mcp.import.notJson'));
      return;
    }
    setError(null);
    setImporting(true);
    setResult(null);
    try {
      // Re-read the servers off the payload being sent rather than the deferred
      // preview: the substitution has to name the same servers the POST carries.
      const servers = normalizeMcpServers(payload).servers;
      const filled = substitutePlaceholders(payload, values, servers);
      setSent(normalizeMcpServers(filled).servers);
      setResult(await onImport(filled));
    } catch (err) {
      setError(formatApiErrorDetail(err));
    } finally {
      setImporting(false);
    }
  }

  function handleNext() {
    if (step === 'paste' && placeholders.length > 0) {
      setStep('credentials');
      return;
    }
    void handleImport();
  }

  const primaryLabel =
    step === 'paste' && placeholders.length > 0
      ? t('mcp.import.next')
      : parseableCount > 0
        ? t('mcp.import.importCount', { count: parseableCount })
        : t('mcp.import.import');

  const footer = (
    <div className="flex items-center justify-end gap-2">
      {!result && step === 'credentials' && (
        <button
          type="button"
          onClick={() => setStep('paste')}
          disabled={importing}
          className="px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10 disabled:opacity-50 disabled:pointer-events-none mr-auto"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {t('mcp.import.back')}
        </button>
      )}
      <button
        type="button"
        onClick={close}
        disabled={importing}
        className="px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10 disabled:opacity-50 disabled:pointer-events-none"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {result ? t('mcp.import.done') : t('mcp.import.cancel')}
      </button>
      {!result && (
        <button
          type="button"
          onClick={handleNext}
          disabled={!canImport}
          data-testid="mcp-import-primary"
          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50"
          style={{ color: 'var(--color-btn-primary-text)', backgroundColor: 'var(--color-btn-primary-bg)' }}
        >
          {importing ? <Loader size={14} className="text-current" /> : <Download className="h-3.5 w-3.5" />}
          {primaryLabel}
        </button>
      )}
    </div>
  );

  return (
    <ModalShell
      labelId={titleId}
      title={step === 'credentials' && !result ? t('mcp.import.credentialsTitle') : t('mcp.import.title')}
      subtitle={step === 'credentials' && !result ? undefined : t('mcp.import.hint')}
      onClose={onClose}
      closeDisabled={importing}
      footer={footer}
    >
          {!result && step === 'paste' && (
            <>
              <textarea
                value={text}
                onChange={(e) => setText(e.target.value)}
                placeholder={PLACEHOLDER}
                rows={12}
                spellCheck={false}
                data-testid="mcp-import-text"
                className="w-full px-3 py-2 text-xs rounded-md bg-transparent font-mono resize-none"
                style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
              />
              {text.trim() && (
                <div className="text-[0.6875rem]" style={{ color: preview.error ? 'var(--color-loss)' : 'var(--color-text-tertiary)' }}>
                  {preview.error
                    ? preview.error
                    : t('mcp.import.found', { count: preview.servers.length }) +
                      (parseableCount !== preview.servers.length
                        ? ' ' + t('mcp.import.unparseable', { count: preview.servers.length - parseableCount })
                        : '')}
                </div>
              )}
            </>
          )}

          {!result && step === 'credentials' && (
            <CredentialsStep placeholders={placeholders} values={values} onChange={setValues} />
          )}

          {result && (
            <ImportResultView
              result={result}
              onEnable={onEnable}
              onSignIn={
                onSignIn &&
                ((row) => {
                  const body = entryBody(row, sentFor(row));
                  if (body) onSignIn(body);
                })
              }
              signingIn={signingIn}
              onFix={
                onFix &&
                ((row) => {
                  const body = entryBody(row, sentFor(row));
                  if (body) onFix(body, notAddedBecause(row, t));
                })
              }
            />
          )}

          {error && (
            <div className="text-xs p-2 rounded" style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-loss)' }}>
              {error}
            </div>
          )}
    </ModalShell>
  );
}

function CredentialsStep({
  placeholders,
  values,
  onChange,
}: {
  placeholders: Placeholder[];
  values: Record<string, string>;
  onChange: (next: Record<string, string>) => void;
}) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col gap-3" data-testid="mcp-import-credentials">
      {placeholders.map((p) => (
        <div key={p.id} className="flex flex-col gap-1">
          <label className="text-xs font-mono" style={{ color: 'var(--color-text-secondary)' }}>
            {placeholderLabel(p)}
          </label>
          <input
            type="password"
            autoComplete="off"
            value={values[p.id] ?? ''}
            onChange={(e) => onChange({ ...values, [p.id]: e.target.value })}
            placeholder={t('mcp.import.credentialPlaceholder')}
            aria-label={placeholderLabel(p)}
            className="w-full px-3 py-2 text-sm rounded-md bg-transparent font-mono"
            style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
          />
        </div>
      ))}
      <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('mcp.import.fillAllHint')}
      </p>
    </div>
  );
}

type EnableState = 'idle' | 'busy' | 'on' | 'failed';

function ImportResultView({
  result,
  onEnable,
  onSignIn,
  signingIn,
  onFix,
}: {
  result: McpImportResult;
  onEnable?: (name: string) => Promise<void>;
  onSignIn?: (row: McpImportResultRow) => void;
  signingIn: string | null;
  onFix?: (row: McpImportResultRow) => void;
}) {
  const { t } = useTranslation();
  const [enableState, setEnableState] = useState<Record<string, EnableState>>({});
  // Why a switch-on was refused, said under its row: in a tooltip it was the
  // one thing the user needed and the one thing they could not see.
  const [enableReason, setEnableReason] = useState<Record<string, string>>({});
  const ok = (s: McpImportResultRow['status']) => s === 'created';
  const created = result.results.filter((r) => ok(r.status)).map((r) => r.name);
  const pending = created.filter((n) => (enableState[n] ?? 'idle') !== 'on');

  async function enable(name: string) {
    if (!onEnable) return;
    setEnableState((s) => ({ ...s, [name]: 'busy' }));
    setEnableReason(({ [name]: _gone, ...rest }) => rest);
    try {
      await onEnable(name);
      setEnableState((s) => ({ ...s, [name]: 'on' }));
    } catch (err) {
      setEnableState((s) => ({ ...s, [name]: 'failed' }));
      setEnableReason((r) => ({ ...r, [name]: formatApiErrorDetail(err) }));
    }
  }

  async function enableAll() {
    for (const name of pending) await enable(name);
  }

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center gap-1.5 text-sm" style={{ color: 'var(--color-text-primary)' }}>
        {result.created > 0 ? (
          <CheckCircle2 className="h-4 w-4" style={{ color: 'var(--color-profit)' }} />
        ) : (
          <AlertTriangle className="h-4 w-4" style={{ color: 'var(--color-warning)' }} />
        )}
        {t('mcp.import.imported', { created: result.created, total: result.results.length })}
      </div>

      {result.secrets_created.length > 0 && (
        <div
          className="flex items-start gap-1.5 text-[0.6875rem] p-2 rounded"
          style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-secondary)' }}
        >
          <KeyRound className="h-3.5 w-3.5 mt-0.5 shrink-0" />
          <span>
            {t('mcp.import.secretsSaved', { count: result.secrets_created.length })}{' '}
            <span className="font-mono">{result.secrets_created.join(', ')}</span>
          </span>
        </div>
      )}

      {onEnable && created.length > 0 && (
        <div className="flex items-center justify-between gap-2 text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
          <span>{t('mcp.import.enableHint')}</span>
          {pending.length > 1 && (
            <McpQuietButton
              onClick={() => void enableAll()}
              className="shrink-0"
              icon={<Power className="h-3 w-3" />}
              data-testid="mcp-import-enable-all"
            >
              {t('mcp.import.enableAll')}
            </McpQuietButton>
          )}
        </div>
      )}

      <div className="flex flex-col gap-1">
        {result.results.map((r, i) => {
          const state = enableState[r.name] ?? 'idle';
          // Why nothing was saved for this entry, said in place: the reason is
          // the one thing the user can act on, and it used to sit in a tooltip
          // on the status word, where nobody looked for it.
          const why = ok(r.status) ? enableReason[r.name] : r.error || r.reason;
          return (
            <div
              key={`${r.name}-${i}`}
              className="flex flex-col gap-0.5 px-2 py-1.5 rounded text-xs"
              style={{ backgroundColor: 'var(--color-bg-card)' }}
              data-testid={`mcp-import-row-${r.name}`}
            >
              <div className="flex flex-wrap items-center justify-between gap-x-2 gap-y-1">
                <div className="flex items-center gap-1.5 min-w-0">
                  {ok(r.status) ? (
                    <CheckCircle2 className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-profit)' }} />
                  ) : (
                    <AlertTriangle className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-warning)' }} />
                  )}
                  <span className="font-mono truncate" style={{ color: 'var(--color-text-primary)' }}>
                    {r.name}
                  </span>
                  {r.renamed && (
                    <span className="text-[0.625rem]" style={{ color: 'var(--color-text-tertiary)' }}>
                      {t('mcp.import.from', { name: r.original_name })}
                    </span>
                  )}
                </div>
                <div className="flex items-center gap-2 shrink-0">
                  <span
                    className="text-[0.625rem]"
                    style={{ color: ok(r.status) ? 'var(--color-profit)' : 'var(--color-warning)' }}
                  >
                    {t(STATUS_KEYS[r.status] ?? 'mcp.import.status.error')}
                  </span>
                  {onSignIn && r.status === 'sign_in_required' && (
                    <McpQuietButton
                      onClick={() => onSignIn(r)}
                      disabled={signingIn !== null}
                      icon={
                        signingIn === r.name ? (
                          <Loader size={12} className="text-current" />
                        ) : (
                          <LogIn className="h-3 w-3" />
                        )
                      }
                      data-testid={`mcp-import-signin-${r.name}`}
                    >
                      {t('mcp.import.signIn')}
                    </McpQuietButton>
                  )}
                  {onFix && FIXABLE.has(r.status) && (
                    <McpQuietButton
                      onClick={() => onFix(r)}
                      icon={<Wrench className="h-3 w-3" />}
                      data-testid={`mcp-import-fix-${r.name}`}
                    >
                      {t('mcp.import.fix')}
                    </McpQuietButton>
                  )}
                  {onEnable && ok(r.status) && (
                    state === 'on' ? (
                      <span className="text-[0.625rem]" style={{ color: 'var(--color-text-tertiary)' }}>
                        {t('mcp.import.enabled')}
                      </span>
                    ) : (
                      <McpQuietButton
                        onClick={() => void enable(r.name)}
                        disabled={state === 'busy'}
                        icon={
                          state === 'busy' ? <Loader size={12} className="text-current" /> : <Power className="h-3 w-3" />
                        }
                        data-testid={`mcp-import-enable-${r.name}`}
                      >
                        {t('mcp.import.enable')}
                      </McpQuietButton>
                    )
                  )}
                </div>
              </div>
              {why && (
                <p
                  className="text-[0.6875rem] pl-5 wrap-break-word"
                  style={{ color: state === 'failed' ? 'var(--color-loss)' : 'var(--color-text-secondary)' }}
                  data-testid={`mcp-import-why-${r.name}`}
                >
                  {why}
                </p>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
