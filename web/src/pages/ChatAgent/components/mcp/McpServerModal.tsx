import React, { useCallback, useDeferredValue, useMemo, useState } from 'react';
import { X, Plus, Trash2, Loader2, Zap, ClipboardPaste } from 'lucide-react';
import { VaultSecretPicker } from './VaultSecretPicker';
import { McpDiscoverResult } from './McpDiscoverResult';
import { parseMcpServersJson } from './mcpImport';
import {
  ALLOWED_COMMANDS,
  EXPOSURE_MODES,
  TRANSPORTS,
  collectVaultRefs,
  validateMcpServer,
} from './mcpSchemas';
import { formatApiErrorDetail, type McpServerInput, type McpDiscoveryResult, type EffectiveServer } from '../../utils/api';

/**
 * Create/edit modal for a workspace (or catalog) MCP server.
 *
 * Transport selector drives conditional fields:
 *   - stdio → command (allowlist), args, env key/value editor
 *   - sse/http → url, headers key/value editor
 *
 * env/header values use `VaultSecretPicker` (emits `${vault:NAME}`).
 * `description` + `instruction` carry helper text marking them as untrusted,
 * user-provided context shown to the agent. An exposure-mode toggle picks
 * summary/detailed. "Test connection" runs the discovery probe.
 */

type Transport = (typeof TRANSPORTS)[number];
type Exposure = (typeof EXPOSURE_MODES)[number];

interface KV {
  /** Stable React key — rows hold stateful children (VaultSecretPicker), so we
   *  must key on identity, not array index, or deleting a middle row leaks the
   *  picker's draft/mode onto its neighbor. */
  id: string;
  key: string;
  value: string;
}

let _kvSeq = 0;
function nextKvId(): string {
  _kvSeq += 1;
  return `kv-${_kvSeq}`;
}

function kvsToMap(kvs: KV[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (const { key, value } of kvs) {
    if (key.trim()) out[key.trim()] = value;
  }
  return out;
}

function mapToKVs(m: Record<string, string>): KV[] {
  return Object.entries(m).map(([key, value]) => ({ id: nextKvId(), key, value }));
}

export interface McpServerModalProps {
  workspaceId: string;
  /** Existing vault secret names for the picker. */
  secretNames: string[];
  /** When editing, the server being edited (its name field is locked). */
  initial?: EffectiveServer | null;
  /** Hide the "Test connection" button (e.g. in the catalog where there's no sandbox). */
  allowDiscover?: boolean;
  onClose: () => void;
  onSubmit: (body: McpServerInput) => Promise<void>;
  onDiscover?: (body: McpServerInput) => Promise<McpDiscoveryResult>;
  onSecretCreated?: (name: string) => void;
  saving?: boolean;
  submitError?: string | null;
}

export function McpServerModal({
  workspaceId,
  secretNames,
  initial,
  allowDiscover = true,
  onClose,
  onSubmit,
  onDiscover,
  onSecretCreated,
  saving = false,
  submitError = null,
}: McpServerModalProps) {
  const isEdit = !!initial;
  const [name, setName] = useState(initial?.name ?? '');
  const [transport, setTransport] = useState<Transport>(
    (initial?.transport as Transport) ?? 'stdio',
  );
  const [command, setCommand] = useState(initial?.command ?? 'npx');
  const [args, setArgs] = useState<string[]>(initial?.args ?? []);
  const [url, setUrl] = useState(initial?.url ?? '');
  // On edit, prefer the stored env/header REFERENCE maps (real keys + their
  // `${vault:NAME}` ref / literal values) so an unrelated edit re-saves the
  // existing config intact. A PUT replaces the full config, and `kvsToMap` drops
  // blank-key rows — so the legacy refs-only hydration (blank keys) silently
  // erased every entry on save. Fall back to `refsToKVs` only when the maps are
  // absent (older backend that returns just `env_refs`/`header_refs`).
  const [env, setEnv] = useState<KV[]>(
    initial ? initialKVs(initial.env, initial.env_refs) : [],
  );
  const [headers, setHeaders] = useState<KV[]>(
    initial ? initialKVs(initial.headers, initial.header_refs) : [],
  );
  const [description, setDescription] = useState(initial?.description ?? '');
  const [instruction, setInstruction] = useState(initial?.instruction ?? '');
  const [exposure, setExposure] = useState<Exposure>(
    (initial?.tool_exposure_mode as Exposure) ?? 'summary',
  );
  const [discoveryUsesSecrets, setDiscoveryUsesSecrets] = useState<boolean>(
    initial?.discovery_uses_secrets ?? false,
  );

  const [errors, setErrors] = useState<Array<{ path: string; message: string }>>([]);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<McpDiscoveryResult | null>(null);

  // Paste-to-fill: parse a standard mcpServers blob and fill the form from its
  // first server. Inline secrets land as literal values the user can vault via
  // the picker (bulk "Import JSON" auto-extracts them instead).
  const [pasteOpen, setPasteOpen] = useState(false);
  const [pasteText, setPasteText] = useState('');
  const [pasteNote, setPasteNote] = useState<string | null>(null);

  function applyPaste() {
    const res = parseMcpServersJson(pasteText);
    const first = res.servers.find((s) => !s.error) ?? res.servers[0];
    if (res.error || !first || first.error) {
      setPasteNote(res.error ?? first?.error ?? 'No server found in the pasted config.');
      return;
    }
    setName(first.name);
    setTransport(first.transport);
    setCommand(first.command || 'npx');
    setArgs(first.args);
    setUrl(first.url);
    setEnv(mapToKVs(first.env));
    setHeaders(mapToKVs(first.headers));
    if (first.description) setDescription(first.description);
    if (first.instruction) setInstruction(first.instruction);
    setExposure(first.toolExposureMode);
    setErrors([]);
    const extra = res.servers.length - 1;
    setPasteNote(
      extra > 0
        ? `Filled "${first.name}". ${extra} more in the blob — use Import JSON to add all.`
        : `Filled from "${first.name}".`,
    );
    setPasteOpen(false);
  }

  // An authenticated remote server needs its header even to list tools, so
  // discovery must resolve secrets — the toggle is forced on for it (the backend
  // enforces the same; this just keeps the UI honest).
  const remoteAuthForcesDiscoverySecrets =
    transport !== 'stdio' && collectVaultRefs(kvsToMap(headers)).length > 0;
  const effectiveDiscoverySecrets = discoveryUsesSecrets || remoteAuthForcesDiscoverySecrets;

  const buildPayload = useCallback((): McpServerInput => {
    const base: McpServerInput = {
      name: name.trim(),
      transport,
      description,
      instruction,
      tool_exposure_mode: exposure,
      discovery_uses_secrets: effectiveDiscoverySecrets,
    };
    if (transport === 'stdio') {
      return { ...base, command, args, env: kvsToMap(env) };
    }
    return { ...base, url: url.trim(), headers: kvsToMap(headers) };
  }, [name, transport, command, args, url, env, headers, description, instruction, exposure, effectiveDiscoverySecrets]);

  // Defer the validated payload so a fast typer doesn't pay a full Zod safeParse
  // + URL canonicalization on every keystroke; `validation` only gates the
  // disabled state of the Add/Test buttons, and submit re-validates the CURRENT
  // payload below — so a deferred (slightly-stale) gate can never let stale data
  // through.
  const payload = useMemo(buildPayload, [buildPayload]);
  const deferredPayload = useDeferredValue(payload);
  const validation = useMemo(() => validateMcpServer(deferredPayload), [deferredPayload]);

  // `kvsToMap` silently drops blank-key rows, so the Zod schema never sees
  // them — guard on the raw KV state or a legacy refs-only hydration (blank
  // keys, `${vault:NAME}` values) would silently erase entries on save.
  function blankKeyErrors(): Array<{ path: string; message: string }> {
    const rows = transport === 'stdio' ? env : headers;
    const path = transport === 'stdio' ? 'env' : 'headers';
    return rows.some((kv) => !kv.key.trim() && kv.value.trim())
      ? [{ path, message: 'Key is required — entries without a key are not saved' }]
      : [];
  }

  async function handleSubmit() {
    const result = validateMcpServer(buildPayload());
    const blanks = blankKeyErrors();
    if (!result.ok || blanks.length > 0) {
      setErrors([...(result.ok ? [] : result.errors), ...blanks]);
      return;
    }
    setErrors([]);
    await onSubmit(buildPayload());
  }

  async function handleTest() {
    if (!onDiscover) return;
    const result = validateMcpServer(buildPayload());
    if (!result.ok) {
      setErrors(result.errors);
      return;
    }
    setErrors([]);
    setTesting(true);
    setTestResult(null);
    try {
      const res = await onDiscover(buildPayload());
      setTestResult(res);
    } catch (err) {
      setTestResult({
        status: 'error',
        tools: [],
        error: formatApiErrorDetail(err),
      });
    } finally {
      setTesting(false);
    }
  }

  const errorFor = (path: string) => errors.find((e) => e.path === path || e.path.startsWith(`${path}.`));

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

        <h3 className="text-lg font-semibold mb-4" style={{ color: 'var(--color-text-primary)' }}>
          {isEdit ? 'Edit MCP server' : 'Add MCP server'}
        </h3>

        <div className="flex flex-col gap-4 overflow-y-auto" style={{ flex: 1, minHeight: 0 }}>
          {/* Paste-to-fill (add mode only) */}
          {!isEdit && (
            <div className="flex flex-col gap-2 p-2 rounded" style={{ backgroundColor: 'var(--color-bg-card)' }}>
              {!pasteOpen ? (
                <button
                  type="button"
                  onClick={() => { setPasteOpen(true); setPasteNote(null); }}
                  className="inline-flex items-center gap-1.5 text-[11px] self-start"
                  style={{ color: 'var(--color-accent-primary)' }}
                >
                  <ClipboardPaste className="h-3.5 w-3.5" />
                  Paste from JSON config
                </button>
              ) : (
                <>
                  <textarea
                    value={pasteText}
                    onChange={(e) => setPasteText(e.target.value)}
                    placeholder={'{ "mcpServers": { "my-server": { "command": "npx", "args": ["-y", "pkg"] } } }'}
                    rows={4}
                    spellCheck={false}
                    className="w-full px-2 py-1.5 text-[11px] rounded bg-transparent outline-none font-mono resize-none"
                    style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
                  />
                  <div className="flex items-center gap-2">
                    <button
                      type="button"
                      onClick={applyPaste}
                      disabled={!pasteText.trim()}
                      className="px-2.5 py-1 text-[11px] rounded transition-colors disabled:opacity-50"
                      style={{ color: 'var(--color-text-on-accent)', backgroundColor: 'var(--color-accent-primary)' }}
                    >
                      Fill form
                    </button>
                    <button
                      type="button"
                      onClick={() => { setPasteOpen(false); setPasteText(''); setPasteNote(null); }}
                      className="px-2.5 py-1 text-[11px] rounded transition-colors hover:bg-foreground/10"
                      style={{ color: 'var(--color-text-tertiary)' }}
                    >
                      Cancel
                    </button>
                  </div>
                </>
              )}
              {pasteNote && (
                <p className="text-[11px]" style={{ color: 'var(--color-text-tertiary)' }}>{pasteNote}</p>
              )}
            </div>
          )}

          {/* Name */}
          <Field label="Name">
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              disabled={isEdit}
              placeholder="my_server"
              className="w-full px-3 py-2 text-sm rounded-md bg-transparent outline-none font-mono disabled:opacity-60"
              style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
              maxLength={64}
            />
            <FieldError error={errorFor('name')} />
          </Field>

          {/* Transport */}
          <Field label="Transport">
            <div className="flex gap-1">
              {TRANSPORTS.map((t) => (
                <button
                  key={t}
                  type="button"
                  onClick={() => setTransport(t)}
                  className="px-3 py-1.5 text-xs rounded-md uppercase"
                  style={{
                    color: transport === t ? 'var(--color-text-on-accent)' : 'var(--color-text-tertiary)',
                    backgroundColor: transport === t ? 'var(--color-accent-primary)' : 'var(--color-bg-card)',
                  }}
                >
                  {t}
                </button>
              ))}
            </div>
          </Field>

          {/* stdio fields */}
          {transport === 'stdio' ? (
            <>
              <Field label="Command">
                <select
                  value={command ?? ''}
                  onChange={(e) => setCommand(e.target.value)}
                  className="w-full px-3 py-2 text-sm rounded-md outline-none"
                  style={{
                    color: 'var(--color-text-primary)',
                    backgroundColor: 'var(--color-bg-card)',
                    border: '1px solid var(--color-border-muted)',
                  }}
                >
                  {ALLOWED_COMMANDS.map((c) => (
                    <option key={c} value={c}>{c}</option>
                  ))}
                </select>
                <FieldError error={errorFor('command')} />
              </Field>

              <Field label="Arguments">
                <ArgsEditor args={args} onChange={setArgs} />
              </Field>

              <Field label="Environment variables" hint="Use a vault secret for credentials.">
                <KeyValueEditor
                  kvs={env}
                  onChange={setEnv}
                  workspaceId={workspaceId}
                  secretNames={secretNames}
                  onSecretCreated={onSecretCreated}
                  keyPlaceholder="ENV_VAR"
                />
                <FieldError error={errorFor('env')} />
              </Field>
            </>
          ) : (
            <>
              <Field label="URL" hint="https only. No localhost or private IPs. Put credentials in headers.">
                <input
                  type="text"
                  value={url}
                  onChange={(e) => setUrl(e.target.value)}
                  placeholder="https://example.com/mcp"
                  className="w-full px-3 py-2 text-sm rounded-md bg-transparent outline-none"
                  style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
                />
                <FieldError error={errorFor('url')} />
              </Field>

              <Field label="Headers" hint="Use a vault secret for auth tokens.">
                <KeyValueEditor
                  kvs={headers}
                  onChange={setHeaders}
                  workspaceId={workspaceId}
                  secretNames={secretNames}
                  onSecretCreated={onSecretCreated}
                  keyPlaceholder="Authorization"
                />
                <FieldError error={errorFor('headers')} />
              </Field>
            </>
          )}

          {/* Description + instruction */}
          <Field
            label="Description"
            hint="Shown to the agent as untrusted, user-provided context."
          >
            <textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="What this server does"
              rows={2}
              className="w-full px-3 py-2 text-sm rounded-md bg-transparent outline-none resize-none"
              style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
              maxLength={512}
            />
            <FieldError error={errorFor('description')} />
          </Field>

          <Field
            label="Instruction"
            hint="Shown to the agent as untrusted, user-provided context."
          >
            <textarea
              value={instruction}
              onChange={(e) => setInstruction(e.target.value)}
              placeholder="How the agent should use this server"
              rows={2}
              className="w-full px-3 py-2 text-sm rounded-md bg-transparent outline-none resize-none"
              style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
              maxLength={1024}
            />
            <FieldError error={errorFor('instruction')} />
          </Field>

          {/* Exposure mode */}
          <Field label="Tool exposure" hint="Detailed lists full tool schemas in the prompt (bounded by caps).">
            <div className="flex gap-1">
              {EXPOSURE_MODES.map((m) => (
                <button
                  key={m}
                  type="button"
                  onClick={() => setExposure(m)}
                  className="px-3 py-1.5 text-xs rounded-md capitalize"
                  style={{
                    color: exposure === m ? 'var(--color-text-on-accent)' : 'var(--color-text-tertiary)',
                    backgroundColor: exposure === m ? 'var(--color-accent-primary)' : 'var(--color-bg-card)',
                  }}
                >
                  {m}
                </button>
              ))}
            </div>
          </Field>

          {/* Discovery secret usage */}
          <Field
            label="Tool discovery"
            hint={
              remoteAuthForcesDiscoverySecrets
                ? 'This server has an authenticated header, so discovery must use your secrets to list its tools.'
                : 'Off (default): tool discovery runs without your vault secrets. Turn on only if this server needs authentication to list its tools.'
            }
          >
            <label
              className="flex items-center gap-2 text-sm"
              style={{
                color: 'var(--color-text-primary)',
                cursor: remoteAuthForcesDiscoverySecrets ? 'not-allowed' : 'pointer',
                opacity: remoteAuthForcesDiscoverySecrets ? 0.7 : 1,
              }}
            >
              <input
                type="checkbox"
                checked={effectiveDiscoverySecrets}
                disabled={remoteAuthForcesDiscoverySecrets}
                onChange={(e) => setDiscoveryUsesSecrets(e.target.checked)}
                className="h-4 w-4 rounded"
                style={{ accentColor: 'var(--color-accent-primary)' }}
              />
              Use my secrets during discovery
            </label>
          </Field>

          {/* Test connection result */}
          {testResult && <McpDiscoverResult result={testResult} />}

          {submitError && (
            <div className="text-xs p-2 rounded" style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-loss)' }}>
              {submitError}
            </div>
          )}
        </div>

        {/* Footer actions */}
        <div className="flex items-center justify-between gap-2 pt-4 mt-2 border-t" style={{ borderColor: 'var(--color-border-muted)' }}>
          {/* Discovery runs against the PERSISTED server, so it's only offered
              when editing an existing row — and labelled to make clear it tests
              the saved config, not unsaved edits in this form. */}
          {allowDiscover && onDiscover && isEdit ? (
            <button
              type="button"
              onClick={handleTest}
              disabled={testing || saving || !validation.ok}
              title="Runs discovery against the saved server. Save first to test edits."
              className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50"
              style={{ color: 'var(--color-text-secondary)', border: '1px solid var(--color-border-muted)' }}
            >
              {testing ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Zap className="h-3.5 w-3.5" />}
              Test saved config
            </button>
          ) : <span />}

          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={onClose}
              className="px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={handleSubmit}
              disabled={saving || !validation.ok}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50"
              style={{ color: 'var(--color-text-on-accent)', backgroundColor: 'var(--color-accent-primary)' }}
            >
              {saving && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              {isEdit ? 'Save' : 'Add'}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

/**
 * Hydrate the env/header editor on edit. Prefer the stored reference `map`
 * (real keys + `${vault:NAME}`/literal values) — it round-trips the existing
 * config so an unrelated edit doesn't drop entries on save. Fall back to the
 * refs-only form for older backends that don't return the map.
 */
function initialKVs(map: Record<string, string> | undefined, refs: string[]): KV[] {
  if (map && Object.keys(map).length > 0) return mapToKVs(map);
  return refsToKVs(refs);
}

/** On edit the wire returns only masked vault names, so pre-fill keys with refs. */
function refsToKVs(refs: string[]): KV[] {
  // We can recover the ref form `${vault:NAME}` but not the original key name,
  // so seed each ref under a blank key the user re-labels. Most servers use a
  // single secret, so this is acceptable for v1.
  return (refs ?? []).map((name) => ({ id: nextKvId(), key: '', value: `\${vault:${name}}` }));
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function Field({ label, hint, children }: { label: string; hint?: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1">
      <label className="text-xs font-medium" style={{ color: 'var(--color-text-secondary)' }}>{label}</label>
      {children}
      {hint && <p className="text-[11px]" style={{ color: 'var(--color-text-tertiary)' }}>{hint}</p>}
    </div>
  );
}

function FieldError({ error }: { error?: { message: string } }) {
  if (!error) return null;
  return <p className="text-[11px]" style={{ color: 'var(--color-loss)' }}>{error.message}</p>;
}

function ArgsEditor({ args, onChange }: { args: string[]; onChange: (a: string[]) => void }) {
  return (
    <div className="flex flex-col gap-1.5">
      {args.map((a, i) => (
        <div key={i} className="flex gap-1.5">
          <input
            type="text"
            value={a}
            onChange={(e) => onChange(args.map((x, j) => (j === i ? e.target.value : x)))}
            placeholder="argument"
            className="flex-1 px-2 py-1 text-xs rounded bg-transparent outline-none font-mono"
            style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
          />
          <button
            type="button"
            onClick={() => onChange(args.filter((_, j) => j !== i))}
            className="p-1.5 rounded hover:bg-foreground/10"
            style={{ color: 'var(--color-text-tertiary)' }}
            aria-label="Remove argument"
          >
            <Trash2 className="h-3.5 w-3.5" />
          </button>
        </div>
      ))}
      <button
        type="button"
        onClick={() => onChange([...args, ''])}
        className="inline-flex items-center gap-1 text-[11px] self-start"
        style={{ color: 'var(--color-accent-primary)' }}
      >
        <Plus className="h-3 w-3" />
        Add argument
      </button>
    </div>
  );
}

interface KeyValueEditorProps {
  kvs: KV[];
  onChange: (kvs: KV[]) => void;
  workspaceId: string;
  secretNames: string[];
  onSecretCreated?: (name: string) => void;
  keyPlaceholder: string;
}

function KeyValueEditor({ kvs, onChange, workspaceId, secretNames, onSecretCreated, keyPlaceholder }: KeyValueEditorProps) {
  return (
    <div className="flex flex-col gap-2">
      {kvs.map((kv, i) => (
        <div
          key={kv.id}
          className="flex flex-col gap-1.5 p-2 rounded"
          style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
        >
          <div className="flex gap-1.5">
            <input
              type="text"
              value={kv.key}
              onChange={(e) => onChange(kvs.map((x, j) => (j === i ? { ...x, key: e.target.value } : x)))}
              placeholder={keyPlaceholder}
              className="flex-1 px-2 py-1 text-xs rounded bg-transparent outline-none font-mono"
              style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
            />
            <button
              type="button"
              onClick={() => onChange(kvs.filter((_, j) => j !== i))}
              className="p-1.5 rounded hover:bg-foreground/10"
              style={{ color: 'var(--color-text-tertiary)' }}
              aria-label="Remove entry"
            >
              <Trash2 className="h-3.5 w-3.5" />
            </button>
          </div>
          <VaultSecretPicker
            workspaceId={workspaceId}
            value={kv.value}
            onChange={(value) => onChange(kvs.map((x, j) => (j === i ? { ...x, value } : x)))}
            secretNames={secretNames}
            onSecretCreated={onSecretCreated}
          />
        </div>
      ))}
      <button
        type="button"
        onClick={() => onChange([...kvs, { id: nextKvId(), key: '', value: '' }])}
        className="inline-flex items-center gap-1 text-[11px] self-start"
        style={{ color: 'var(--color-accent-primary)' }}
      >
        <Plus className="h-3 w-3" />
        Add entry
      </button>
    </div>
  );
}
