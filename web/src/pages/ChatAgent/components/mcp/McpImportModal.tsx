import React, { useDeferredValue, useMemo, useState } from 'react';
import { X, Download, CheckCircle2, AlertTriangle, KeyRound } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { parseMcpServersJson } from './mcpImport';
import { formatApiErrorDetail, type McpImportResult, type McpImportResultRow } from '../../utils/api';

/**
 * Bulk-import modal: paste a standard `{ "mcpServers": { … } }` blob and create
 * every server at once. The textarea is parsed client-side for an instant count
 * preview; on submit the parsed JSON object is sent to the backend, which
 * coerces names, maps transports, and auto-extracts inline literal secrets into
 * the vault. Per-server outcomes are shown after import.
 */

const PLACEHOLDER = `{
  "mcpServers": {
    "my-server": {
      "type": "streamablehttp",
      "url": "https://api.example.com/mcp",
      "headers": { "Authorization": "<token>" }
    }
  }
}`;

export interface McpImportModalProps {
  onClose: () => void;
  onImport: (payload: unknown) => Promise<McpImportResult>;
  /** Called after a successful import with the names that were created. */
  onImported?: (createdNames: string[], secretsCreated: string[]) => void;
}

export function McpImportModal({ onClose, onImport, onImported }: McpImportModalProps) {
  const [text, setText] = useState('');
  const [importing, setImporting] = useState(false);
  const [result, setResult] = useState<McpImportResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Defer the parse so a fast typer doesn't pay full JSON.parse + normalization
  // on every keystroke; the count preview lags a frame behind the textarea.
  const deferredText = useDeferredValue(text);
  const preview = useMemo(() => parseMcpServersJson(deferredText), [deferredText]);
  const parseableCount = preview.servers.filter((s) => !s.error).length;
  const canImport = !importing && parseableCount > 0;

  async function handleImport() {
    let payload: unknown;
    try {
      payload = JSON.parse(text.trim());
    } catch {
      setError('Not valid JSON.');
      return;
    }
    setError(null);
    setImporting(true);
    setResult(null);
    try {
      const res = await onImport(payload);
      setResult(res);
      const created = res.results.filter((r) => r.status === 'created').map((r) => r.name);
      onImported?.(created, res.secrets_created);
    } catch (err) {
      setError(formatApiErrorDetail(err));
    } finally {
      setImporting(false);
    }
  }

  return (
    <div
      className="fixed inset-0 z-[60] flex items-center justify-center p-4"
      style={{ backgroundColor: 'var(--color-bg-overlay-strong)' }}
      onClick={onClose}
    >
      <div
        className="relative w-full max-w-lg rounded-lg p-5"
        style={{
          backgroundColor: 'var(--color-bg-elevated)',
          border: '1px solid var(--color-border-muted)',
          maxHeight: '85vh',
          display: 'flex',
          flexDirection: 'column',
          overflow: 'hidden',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <button
          onClick={onClose}
          className="absolute top-3 right-3 p-1 rounded-full transition-colors hover:bg-foreground/10"
          style={{ color: 'var(--color-text-primary)' }}
          aria-label="Close"
        >
          <X className="h-4 w-4" />
        </button>

        <h3 className="text-lg font-semibold mb-1" style={{ color: 'var(--color-text-primary)' }}>
          Import MCP servers
        </h3>
        <p className="text-xs mb-4" style={{ color: 'var(--color-text-tertiary)' }}>
          Paste a standard <code>mcpServers</code> config. Inline secrets (auth tokens) are saved to
          your vault automatically.
        </p>

        <div className="flex flex-col gap-3 overflow-y-auto" style={{ flex: 1, minHeight: 0 }}>
          {!result && (
            <>
              <textarea
                value={text}
                onChange={(e) => setText(e.target.value)}
                placeholder={PLACEHOLDER}
                rows={12}
                spellCheck={false}
                className="w-full px-3 py-2 text-xs rounded-md bg-transparent font-mono resize-none"
                style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
              />
              {text.trim() && (
                <div className="text-[0.6875rem]" style={{ color: preview.error ? 'var(--color-loss)' : 'var(--color-text-tertiary)' }}>
                  {preview.error
                    ? preview.error
                    : `Found ${preview.servers.length} server${preview.servers.length === 1 ? '' : 's'}` +
                      (parseableCount !== preview.servers.length
                        ? ` (${preview.servers.length - parseableCount} can't be parsed)`
                        : '')}
                </div>
              )}
            </>
          )}

          {result && <ImportResultView result={result} />}

          {error && (
            <div className="text-xs p-2 rounded" style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-loss)' }}>
              {error}
            </div>
          )}
        </div>

        <div className="flex items-center justify-end gap-2 pt-4 mt-2 border-t" style={{ borderColor: 'var(--color-border-muted)' }}>
          <button
            type="button"
            onClick={onClose}
            className="px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {result ? 'Done' : 'Cancel'}
          </button>
          {!result && (
            <button
              type="button"
              onClick={handleImport}
              disabled={!canImport}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50"
              style={{ color: 'var(--color-btn-primary-text)', backgroundColor: 'var(--color-btn-primary-bg)' }}
            >
              {importing ? <Loader size={14} className="text-current" /> : <Download className="h-3.5 w-3.5" />}
              Import{parseableCount > 0 ? ` ${parseableCount}` : ''}
            </button>
          )}
        </div>
      </div>
    </div>
  );
}

const STATUS_LABEL: Record<McpImportResultRow['status'], string> = {
  created: 'Added',
  exists: 'Already present',
  skipped: 'Skipped',
  invalid: 'Invalid',
  error: 'Failed',
};

function ImportResultView({ result }: { result: McpImportResult }) {
  const ok = (s: McpImportResultRow['status']) => s === 'created';
  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center gap-1.5 text-sm" style={{ color: 'var(--color-text-primary)' }}>
        <CheckCircle2 className="h-4 w-4" style={{ color: 'var(--color-accent-primary)' }} />
        Imported {result.created} of {result.results.length} server{result.results.length === 1 ? '' : 's'}.
      </div>

      {result.secrets_created.length > 0 && (
        <div
          className="flex items-start gap-1.5 text-[0.6875rem] p-2 rounded"
          style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-secondary)' }}
        >
          <KeyRound className="h-3.5 w-3.5 mt-0.5 shrink-0" />
          <span>
            Saved {result.secrets_created.length} secret{result.secrets_created.length === 1 ? '' : 's'} to your
            vault: <span className="font-mono">{result.secrets_created.join(', ')}</span>
          </span>
        </div>
      )}

      <div className="flex flex-col gap-1">
        {result.results.map((r, i) => (
          <div
            key={`${r.name}-${i}`}
            className="flex items-center justify-between gap-2 px-2 py-1.5 rounded text-xs"
            style={{ backgroundColor: 'var(--color-bg-card)' }}
          >
            <div className="flex items-center gap-1.5 min-w-0">
              {ok(r.status) ? (
                <CheckCircle2 className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
              ) : (
                <AlertTriangle className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
              )}
              <span className="font-mono truncate" style={{ color: 'var(--color-text-primary)' }}>
                {r.name}
              </span>
              {r.renamed && (
                <span className="text-[0.625rem]" style={{ color: 'var(--color-text-tertiary)' }}>
                  (from {r.original_name})
                </span>
              )}
            </div>
            <span
              className="shrink-0 text-[0.625rem]"
              style={{ color: ok(r.status) ? 'var(--color-accent-primary)' : 'var(--color-text-tertiary)' }}
              title={r.error || r.reason || ''}
            >
              {STATUS_LABEL[r.status]}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
