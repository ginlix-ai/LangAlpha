import React, { useDeferredValue, useEffect, useId, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Download, CheckCircle2, AlertTriangle, KeyRound, Power } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { ModalShell } from '@/components/ui/ModalShell';
import { normalizeMcpServers, parseMcpServersJson } from './mcpImport';
import {
  findPlaceholders,
  placeholderLabel,
  substitutePlaceholders,
  type Placeholder,
} from './mcpImportPlaceholders';
import { formatApiErrorDetail, type McpImportResult, type McpImportResultRow } from '../../utils/api';

/**
 * Bulk-import modal: paste a standard `{ "mcpServers": { … } }` blob and create
 * every server at once. The textarea is parsed client-side for an instant
 * count preview. When the config carries placeholder credentials
 * (`<your-api-key>`), a second step asks each server that wrote one for its own
 * value and substitutes it into that server before the POST; the backend then
 * vaults the literal. Per-server outcomes are shown after import, with a
 * switch-on affordance where the caller offers one, since imported rows
 * start off.
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
}

const NOOP = () => {};

type Step = 'paste' | 'credentials';

export function McpImportModal({ onClose, onImport, onEnable }: McpImportModalProps) {
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
  const [error, setError] = useState<string | null>(null);

  // Defer the parse so a fast typer doesn't pay full JSON.parse + normalization
  // on every keystroke; the count preview lags a frame behind the textarea.
  const deferredText = useDeferredValue(text);
  const preview = useMemo(() => parseMcpServersJson(deferredText), [deferredText]);
  const parseableCount = preview.servers.filter((s) => !s.error).length;
  const placeholders = useMemo(() => findPlaceholders(preview.servers), [preview.servers]);
  const canImport = !importing && parseableCount > 0;

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
      setResult(await onImport(substitutePlaceholders(payload, values, servers)));
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
      subtitle={step === 'credentials' && !result ? t('mcp.import.credentialsHint') : t('mcp.import.hint')}
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

          {result && <ImportResultView result={result} onEnable={onEnable} />}

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
        {t('mcp.import.leaveBlankHint')}
      </p>
    </div>
  );
}

type EnableState = 'idle' | 'busy' | 'on' | 'failed';

function ImportResultView({
  result,
  onEnable,
}: {
  result: McpImportResult;
  onEnable?: (name: string) => Promise<void>;
}) {
  const { t } = useTranslation();
  const [enableState, setEnableState] = useState<Record<string, EnableState>>({});
  const ok = (s: McpImportResultRow['status']) => s === 'created';
  const created = result.results.filter((r) => ok(r.status)).map((r) => r.name);
  const pending = created.filter((n) => (enableState[n] ?? 'idle') !== 'on');

  async function enable(name: string) {
    if (!onEnable) return;
    setEnableState((s) => ({ ...s, [name]: 'busy' }));
    try {
      await onEnable(name);
      setEnableState((s) => ({ ...s, [name]: 'on' }));
    } catch {
      setEnableState((s) => ({ ...s, [name]: 'failed' }));
    }
  }

  async function enableAll() {
    for (const name of pending) await enable(name);
  }

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center gap-1.5 text-sm" style={{ color: 'var(--color-text-primary)' }}>
        <CheckCircle2 className="h-4 w-4" style={{ color: 'var(--color-profit)' }} />
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
            <button
              type="button"
              onClick={() => void enableAll()}
              className="inline-flex items-center gap-1 shrink-0"
              style={{ color: 'var(--color-accent-primary)' }}
              data-testid="mcp-import-enable-all"
            >
              <Power className="h-3 w-3" />
              {t('mcp.import.enableAll')}
            </button>
          )}
        </div>
      )}

      <div className="flex flex-col gap-1">
        {result.results.map((r, i) => {
          const state = enableState[r.name] ?? 'idle';
          return (
            <div
              key={`${r.name}-${i}`}
              className="flex items-center justify-between gap-2 px-2 py-1.5 rounded text-xs"
              style={{ backgroundColor: 'var(--color-bg-card)' }}
            >
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
                  title={r.error || r.reason || ''}
                >
                  {t(`mcp.import.status.${r.status}`)}
                </span>
                {onEnable && ok(r.status) && (
                  state === 'on' ? (
                    <span className="text-[0.625rem]" style={{ color: 'var(--color-text-tertiary)' }}>
                      {t('mcp.import.enabled')}
                    </span>
                  ) : (
                    <button
                      type="button"
                      onClick={() => void enable(r.name)}
                      disabled={state === 'busy'}
                      title={state === 'failed' ? t('mcp.import.enableFailed', { name: r.name }) : undefined}
                      className="inline-flex items-center gap-1 text-[0.625rem] disabled:opacity-50"
                      style={{ color: state === 'failed' ? 'var(--color-loss)' : 'var(--color-accent-primary)' }}
                      data-testid={`mcp-import-enable-${r.name}`}
                    >
                      {state === 'busy' ? <Loader size={12} className="text-current" /> : <Power className="h-3 w-3" />}
                      {t('mcp.import.enable')}
                    </button>
                  )
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
