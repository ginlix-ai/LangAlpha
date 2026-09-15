import React, { useCallback, useDeferredValue, useEffect, useId, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { X, Plus, Trash2, Zap, ChevronRight, Globe, Terminal } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { useBackdropDismiss, useDialogA11y } from '@/hooks/useDialogA11y';
import { VaultSecretPicker } from './VaultSecretPicker';
import { McpDiscoverResult } from './McpDiscoverResult';
import { McpProbePanel } from './McpProbePanel';
import { commandLine, parseEntry, suggestName } from './mcpEntry';
import { HEADER_CHOICES, choiceLabel, joinHeader, splitHeader, type HeaderChoiceId } from './mcpHeaderNames';
import {
  EXPOSURE_MODES,
  collectVaultRefs,
  validateMcpServer,
  validateRemoteUrl,
} from './mcpSchemas';
import {
  formatApiErrorDetail,
  type McpDiscoveryResult,
  type McpProbeInput,
  type McpProbeResult,
  type McpServerDraft,
  type McpServerInput,
  type McpTransport,
} from '../../utils/api';

/**
 * Create/edit modal for a workspace (or catalog) MCP server.
 *
 * One field carries the whole definition: a URL makes a remote server, a
 * command line makes a local one, a pasted JSON config fills everything. The
 * transport is read off it rather than asked for, the name is suggested from
 * it, and a remote address is checked from the host as the user types so the
 * verdict (open, needs a credential, wants OAuth, unreachable) is on screen
 * before Save rather than after the next sandbox turn. A local command has no
 * host-side check; it is probed in the workspace after the save.
 *
 * env/header values use `VaultSecretPicker` (emits `${vault:NAME}`). The
 * fields that only tune the prompt sit under Advanced.
 */

type Exposure = (typeof EXPOSURE_MODES)[number];

interface KV {
  /** Stable React key: rows hold stateful children (VaultSecretPicker), so we
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

/**
 * Blank-key rows are dropped. A header row with a name but no value is dropped
 * too: a fresh header row already carries `Authorization`, and an empty
 * `Authorization:` is a malformed request, not an absent credential. An env
 * var set to the empty string is a real setting and stays.
 */
function kvsToMap(kvs: KV[], kind: 'headers' | 'env' = 'env'): Record<string, string> {
  const out: Record<string, string> = {};
  for (const { key, value } of kvs) {
    if (!key.trim()) continue;
    if (kind === 'headers' && value === '') continue;
    out[key.trim()] = value;
  }
  return out;
}

function mapToKVs(m: Record<string, string>): KV[] {
  return Object.entries(m).map(([key, value]) => ({ id: nextKvId(), key, value }));
}

/** How long the form rests before a remote address is checked. */
const PROBE_DEBOUNCE_MS = 700;

export interface McpServerModalProps {
  /** Existing vault secret names for the picker. */
  secretNames: string[];
  /** When editing, the server being edited (its name field is locked). */
  initial?: McpServerDraft | null;
  /** Hide the "Test saved config" button (e.g. in the catalog where there's no sandbox). */
  allowDiscover?: boolean;
  onClose: () => void;
  onSubmit: (body: McpServerInput) => Promise<void>;
  onDiscover?: (body: McpServerInput) => Promise<McpDiscoveryResult>;
  /** The host-side check of a remote address, before anything is saved. */
  onProbe?: (body: McpProbeInput) => Promise<McpProbeResult>;
  /** Inline secret-create for the picker, into the tier this modal edits. */
  createSecret: (body: { name: string; value: string }) => Promise<unknown>;
  saving?: boolean;
  submitError?: string | null;
}

const NOOP = () => {};

const CATCH_ALL_PROBE_ERROR = (error: string): McpProbeResult => ({
  status: 'error',
  auth: 'unknown',
  tool_count: null,
  tools: [],
  server_info: null,
  error,
  http_status: null,
  missing_secrets: [],
});

export function McpServerModal({
  secretNames,
  initial,
  allowDiscover = true,
  onClose,
  onSubmit,
  onDiscover,
  onProbe,
  createSecret,
  saving = false,
  submitError = null,
}: McpServerModalProps) {
  const { t } = useTranslation();
  // Every dismissal route waits out a save. Closing mid-flight leaves the
  // outcome nowhere to land: a failure has no form left to show it in, and a
  // success closes whatever the user opened next.
  const close = saving ? NOOP : onClose;
  const titleId = useId();
  const dialogRef = useDialogA11y<HTMLDivElement>(close);
  const backdrop = useBackdropDismiss<HTMLDivElement>(close);
  const isEdit = !!initial;

  const [entry, setEntry] = useState(() =>
    initial
      ? initial.transport === 'stdio'
        ? commandLine(initial.command, initial.args ?? [])
        : (initial.url ?? '')
      : '',
  );
  const [name, setName] = useState(initial?.name ?? '');
  // The name follows the field until the user edits it by hand.
  const [nameTouched, setNameTouched] = useState(isEdit);
  const [transport, setTransport] = useState<McpTransport | null>(initial?.transport ?? null);
  // A transport picked under Advanced holds against what the field looks like.
  const [transportPinned, setTransportPinned] = useState(false);
  const [command, setCommand] = useState(initial?.command ?? '');
  const [args, setArgs] = useState<string[]>(initial?.args ?? []);
  const [url, setUrl] = useState(initial?.url ?? '');
  // On edit, prefer the stored env/header REFERENCE maps (real keys + their
  // `${vault:NAME}` ref / literal values) so an unrelated edit re-saves the
  // existing config intact. A PUT replaces the full config, and `kvsToMap` drops
  // blank-key rows, so the legacy refs-only hydration (blank keys) silently
  // erased every entry on save. Fall back to `refsToKVs` only when the maps are
  // absent (older backend that returns just `env_refs`/`header_refs`).
  const [env, setEnv] = useState<KV[]>(initial ? initialKVs(initial.env, initial.env_refs) : []);
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
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [entryNote, setEntryNote] = useState<string | null>(null);

  const [errors, setErrors] = useState<Array<{ path: string; message: string }>>([]);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<McpDiscoveryResult | null>(null);

  const remote = transport === 'http' || transport === 'sse';

  // The field is the source; command/args/url/transport are read off it. A
  // pasted config fills every field and rewrites the entry as the line the
  // config amounts to, so the field never shows JSON the user has to edit.
  function applyEntry(raw: string) {
    setEntry(raw);
    setEntryNote(null);
    const parsed = parseEntry(raw);
    if (parsed.kind === 'json') {
      const s = parsed.server;
      setTransport(s.transport);
      setTransportPinned(false);
      setCommand(s.command);
      setArgs(s.args);
      setUrl(s.url);
      setEnv(mapToKVs(s.env));
      setHeaders(mapToKVs(s.headers));
      if (s.description) setDescription(s.description);
      if (s.instruction) setInstruction(s.instruction);
      setExposure(s.toolExposureMode);
      if (!isEdit) {
        setName(s.name);
        setNameTouched(true);
      }
      setEntry(s.transport === 'stdio' ? commandLine(s.command, s.args) : s.url);
      setEntryNote(
        parsed.more > 0
          ? t('mcp.modal.filledFromMore', { name: s.originalName, count: parsed.more })
          : t('mcp.modal.filledFrom', { name: s.originalName }),
      );
      setErrors([]);
      return;
    }
    if (parsed.kind === 'json-error') {
      setEntryNote(parsed.error || t('mcp.modal.pasteNoServer'));
      return;
    }
    if (parsed.kind === 'empty') {
      if (!transportPinned) setTransport(null);
      setUrl('');
      setCommand('');
      setArgs([]);
      return;
    }
    if (parsed.kind === 'remote') {
      // A pinned sse stays sse; anything else that reads as an address is http.
      if (!transportPinned || transport === 'stdio') setTransport('http');
      setUrl(parsed.url);
      setCommand('');
      setArgs([]);
      return;
    }
    if (!transportPinned || transport !== 'stdio') setTransport('stdio');
    setCommand(parsed.command);
    setArgs(parsed.args);
    setUrl('');
  }

  // Suggested name: follows the field until the user types one.
  useEffect(() => {
    if (nameTouched) return;
    setName(suggestName(transport, url, command, args));
  }, [nameTouched, transport, url, command, args]);

  function pinTransport(next: McpTransport) {
    setTransport(next);
    setTransportPinned(true);
    // Moving a line between kinds keeps what can carry over: an address stays
    // an address, a command stays a command.
    if (next === 'stdio' && !command && url) {
      setCommand(url);
      setUrl('');
      setEntry(url);
    } else if (next !== 'stdio' && !url && command) {
      const line = commandLine(command, args);
      setUrl(line);
      setCommand('');
      setArgs([]);
      setEntry(line);
    }
  }

  // An authenticated remote server needs its header even to list tools, so
  // discovery must resolve secrets; the toggle is forced on for it (the backend
  // enforces the same; this just keeps the UI honest).
  const remoteAuthForcesDiscoverySecrets = remote && collectVaultRefs(kvsToMap(headers, 'headers')).length > 0;
  const effectiveDiscoverySecrets = discoveryUsesSecrets || remoteAuthForcesDiscoverySecrets;

  const buildPayload = useCallback((): McpServerInput => {
    const tr: McpTransport = transport ?? 'stdio';
    const base: McpServerInput = {
      name: name.trim(),
      transport: tr,
      description,
      instruction,
      tool_exposure_mode: exposure,
      discovery_uses_secrets: effectiveDiscoverySecrets,
    };
    if (tr === 'stdio') {
      return { ...base, command, args, env: kvsToMap(env) };
    }
    return { ...base, url: url.trim(), headers: kvsToMap(headers, 'headers') };
  }, [name, transport, command, args, url, env, headers, description, instruction, exposure, effectiveDiscoverySecrets]);

  // Defer the validated payload so a fast typer doesn't pay a full Zod safeParse
  // + URL canonicalization on every keystroke; `validation` only gates the
  // disabled state of the Add/Test buttons, and submit re-validates the CURRENT
  // payload below, so a deferred (slightly-stale) gate can never let stale data
  // through.
  const payload = useMemo(buildPayload, [buildPayload]);
  const deferredPayload = useDeferredValue(payload);
  const validation = useMemo(() => validateMcpServer(deferredPayload), [deferredPayload]);

  // --- The live check of a remote address -------------------------------
  const headersMap = useMemo(() => kvsToMap(headers, 'headers'), [headers]);
  const headersKey = JSON.stringify(headersMap);
  const trimmedUrl = url.trim();
  const probeable = remote && !!onProbe && validateRemoteUrl(trimmedUrl) === null;
  const [probing, setProbing] = useState(false);
  // A result is shown only for the exact (transport, url, headers) it answered.
  const [probe, setProbe] = useState<{ key: string; result: McpProbeResult } | null>(null);
  const probeSeq = useRef(0);
  const probeKey = `${transport}|${trimmedUrl}|${headersKey}`;

  const runProbe = useCallback(async () => {
    if (!onProbe || !remote) return;
    const key = probeKey;
    const seq = ++probeSeq.current;
    setProbing(true);
    try {
      const result = await onProbe({
        transport: transport === 'sse' ? 'sse' : 'http',
        url: trimmedUrl,
        headers: headersMap,
      });
      if (seq === probeSeq.current) setProbe({ key, result });
    } catch (err) {
      if (seq === probeSeq.current) setProbe({ key, result: CATCH_ALL_PROBE_ERROR(formatApiErrorDetail(err)) });
    } finally {
      if (seq === probeSeq.current) setProbing(false);
    }
  }, [onProbe, remote, transport, trimmedUrl, headersMap, probeKey]);

  useEffect(() => {
    if (!probeable) return;
    if (probe?.key === probeKey) return;
    const timer = setTimeout(() => void runProbe(), PROBE_DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [probeable, probeKey, probe?.key, runProbe]);

  const currentProbe = probe?.key === probeKey ? probe.result : null;

  // `kvsToMap` silently drops blank-key rows, so the Zod schema never sees
  // them. Guard on the raw KV state or a legacy refs-only hydration (blank
  // keys, `${vault:NAME}` values) would silently erase entries on save.
  function blankKeyErrors(): Array<{ path: string; message: string }> {
    const rows = remote ? headers : env;
    const path = remote ? 'headers' : 'env';
    return rows.some((kv) => !kv.key.trim() && kv.value.trim())
      ? [{ path, message: t('mcp.modal.blankKey') }]
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
      setTestResult(await onDiscover(buildPayload()));
    } catch (err) {
      setTestResult({ status: 'error', tools: [], error: formatApiErrorDetail(err) });
    } finally {
      setTesting(false);
    }
  }

  const errorFor = (path: string) => errors.find((e) => e.path === path || e.path.startsWith(`${path}.`));
  const entryError = errorFor('url') ?? errorFor('command') ?? errorFor('args') ?? errorFor('transport');

  const inputStyle = {
    color: 'var(--color-text-primary)',
    border: '1px solid var(--color-border-muted)',
  } as const;

  const transportChoices: McpTransport[] =
    initial?.transport === 'sse' || transport === 'sse' ? ['http', 'sse', 'stdio'] : ['http', 'stdio'];

  return (
    <div
      className="fixed inset-0 z-[1010] flex items-center justify-center p-4"
      style={{ backgroundColor: 'var(--color-bg-overlay-strong)' }}
      {...backdrop}
    >
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        tabIndex={-1}
        className="relative w-full max-w-lg rounded-lg p-5"
        style={{
          backgroundColor: 'var(--color-bg-elevated)',
          border: '1px solid var(--color-border-muted)',
          maxHeight: '85vh',
          display: 'flex',
          flexDirection: 'column',
          overflow: 'hidden',
        }}
      >
        <button
          onClick={close}
          disabled={saving}
          className="absolute top-3 right-3 p-1 rounded-full transition-colors hover:bg-foreground/10 disabled:opacity-40 disabled:pointer-events-none"
          style={{ color: 'var(--color-text-primary)' }}
          aria-label={t('mcp.modal.close')}
        >
          <X className="h-4 w-4" />
        </button>

        <h3 id={titleId} className="text-lg font-semibold mb-4" style={{ color: 'var(--color-text-primary)' }}>
          {isEdit ? t('mcp.modal.editTitle') : t('mcp.modal.addTitle')}
        </h3>

        <div className="flex flex-col gap-4 overflow-y-auto" style={{ flex: 1, minHeight: 0 }}>
          <Field label={t('mcp.modal.entryLabel')} hint={t('mcp.modal.entryHint')}>
            <input
              type="text"
              value={entry}
              onChange={(e) => applyEntry(e.target.value)}
              placeholder={t('mcp.modal.entryPlaceholder')}
              spellCheck={false}
              autoCapitalize="off"
              autoCorrect="off"
              autoFocus={!isEdit}
              data-testid="mcp-entry"
              className="w-full px-3 py-2 text-sm rounded-md bg-transparent font-mono"
              style={inputStyle}
            />
            {transport && (
              <p
                className="inline-flex items-center gap-1.5 text-[0.6875rem]"
                style={{ color: 'var(--color-text-secondary)' }}
                data-testid="mcp-entry-kind"
              >
                {transport === 'stdio' ? <Terminal className="h-3 w-3" /> : <Globe className="h-3 w-3" />}
                {transport === 'stdio'
                  ? t('mcp.modal.detectedStdio')
                  : transport === 'sse'
                    ? t('mcp.modal.detectedSse')
                    : t('mcp.modal.detectedHttp')}
              </p>
            )}
            {entryNote && (
              <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>{entryNote}</p>
            )}
            <FieldError error={entryError} />
          </Field>

          <Field label={t('mcp.modal.nameLabel')} hint={isEdit ? undefined : t('mcp.modal.nameHint')}>
            <input
              type="text"
              value={name}
              onChange={(e) => {
                setNameTouched(true);
                setName(e.target.value);
              }}
              disabled={isEdit}
              placeholder={t('mcp.modal.namePlaceholder')}
              className="w-full px-3 py-2 text-sm rounded-md bg-transparent font-mono disabled:opacity-60"
              style={inputStyle}
              maxLength={64}
              data-testid="mcp-name"
            />
            <FieldError error={errorFor('name')} />
          </Field>

          {remote && (
            <>
              <Field label={t('mcp.modal.headersLabel')} hint={t('mcp.modal.headersHint')}>
                <KeyValueEditor
                  kind="headers"
                  serverName={name}
                  kvs={headers}
                  onChange={setHeaders}
                  secretNames={secretNames}
                  createSecret={createSecret}
                  keyPlaceholder={t('mcp.modal.headerCustomPlaceholder')}
                />
                <FieldError error={errorFor('headers')} />
              </Field>
              {onProbe && (
                <McpProbePanel
                  result={currentProbe}
                  probing={probing}
                  canCheck={probeable}
                  hasHeaders={Object.keys(headersMap).length > 0}
                  onCheck={() => void runProbe()}
                />
              )}
            </>
          )}

          {transport === 'stdio' && (
            <>
              <Field label={t('mcp.modal.argsLabel')}>
                <ArgsEditor
                  args={args}
                  onChange={(next) => {
                    setArgs(next);
                    setEntry(commandLine(command, next));
                  }}
                />
              </Field>
              <Field label={t('mcp.modal.envLabel')} hint={t('mcp.modal.envHint')}>
                <KeyValueEditor
                  kind="env"
                  serverName={name}
                  kvs={env}
                  onChange={setEnv}
                  secretNames={secretNames}
                  createSecret={createSecret}
                  keyPlaceholder={t('mcp.modal.envKeyPlaceholder')}
                />
                <FieldError error={errorFor('env')} />
              </Field>
              <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('mcp.modal.stdioCheckNote')}
              </p>
            </>
          )}

          <div className="flex flex-col gap-3">
            <button
              type="button"
              onClick={() => setAdvancedOpen((v) => !v)}
              aria-expanded={advancedOpen}
              className="inline-flex items-center gap-1 text-xs self-start"
              style={{ color: 'var(--color-text-secondary)' }}
              data-testid="mcp-advanced-toggle"
            >
              <ChevronRight
                className="h-3.5 w-3.5 transition-transform"
                style={{ transform: advancedOpen ? 'rotate(90deg)' : undefined }}
              />
              {t('mcp.modal.advanced')}
            </button>

            {advancedOpen && (
              <div className="flex flex-col gap-4 pl-1">
                <Field label={t('mcp.modal.transportLabel')} hint={t('mcp.modal.transportHint')}>
                  <div className="flex gap-1">
                    {transportChoices.map((tr) => (
                      <button
                        key={tr}
                        type="button"
                        onClick={() => pinTransport(tr)}
                        className="px-3 py-1.5 text-xs rounded-md uppercase"
                        style={{
                          color: transport === tr ? 'var(--color-btn-primary-text)' : 'var(--color-text-tertiary)',
                          backgroundColor: transport === tr ? 'var(--color-btn-primary-bg)' : 'var(--color-bg-card)',
                        }}
                      >
                        {tr}
                      </button>
                    ))}
                  </div>
                </Field>

                <Field label={t('mcp.modal.descriptionLabel')} hint={t('mcp.modal.descriptionHint')}>
                  <textarea
                    value={description}
                    onChange={(e) => setDescription(e.target.value)}
                    placeholder={t('mcp.modal.descriptionPlaceholder')}
                    rows={2}
                    className="w-full px-3 py-2 text-sm rounded-md bg-transparent resize-none"
                    style={inputStyle}
                    maxLength={512}
                  />
                  <FieldError error={errorFor('description')} />
                </Field>

                <Field label={t('mcp.modal.instructionLabel')} hint={t('mcp.modal.instructionHint')}>
                  <textarea
                    value={instruction}
                    onChange={(e) => setInstruction(e.target.value)}
                    placeholder={t('mcp.modal.instructionPlaceholder')}
                    rows={2}
                    className="w-full px-3 py-2 text-sm rounded-md bg-transparent resize-none"
                    style={inputStyle}
                    maxLength={1024}
                  />
                  <FieldError error={errorFor('instruction')} />
                </Field>

                <Field label={t('mcp.modal.exposureLabel')} hint={t('mcp.modal.exposureHint')}>
                  <div className="flex gap-1">
                    {EXPOSURE_MODES.map((m) => (
                      <button
                        key={m}
                        type="button"
                        onClick={() => setExposure(m)}
                        className="px-3 py-1.5 text-xs rounded-md"
                        style={{
                          color: exposure === m ? 'var(--color-btn-primary-text)' : 'var(--color-text-tertiary)',
                          backgroundColor: exposure === m ? 'var(--color-btn-primary-bg)' : 'var(--color-bg-card)',
                        }}
                      >
                        {m === 'summary' ? t('mcp.modal.exposureSummary') : t('mcp.modal.exposureDetailed')}
                      </button>
                    ))}
                  </div>
                </Field>

                <Field
                  label={t('mcp.modal.discoveryLabel')}
                  hint={remoteAuthForcesDiscoverySecrets ? t('mcp.modal.discoveryForced') : t('mcp.modal.discoveryHint')}
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
                    {t('mcp.modal.discoveryToggle')}
                  </label>
                </Field>
              </div>
            )}
          </div>

          {testResult && <McpDiscoverResult result={testResult} />}

          {submitError && (
            <div className="text-xs p-2 rounded" style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-loss)' }}>
              {submitError}
            </div>
          )}
        </div>

        <div className="flex items-center justify-between gap-2 pt-4 mt-2 border-t" style={{ borderColor: 'var(--color-border-muted)' }}>
          {/* Sandbox discovery runs against the PERSISTED server, so it's only
              offered when editing an existing row, and labelled to make clear
              it tests the saved config, not unsaved edits in this form. A
              remote row has the live check above instead. */}
          {allowDiscover && onDiscover && isEdit && !remote ? (
            <button
              type="button"
              onClick={handleTest}
              disabled={testing || saving || !validation.ok}
              title={t('mcp.modal.testSavedHint')}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50"
              style={{ color: 'var(--color-text-secondary)', border: '1px solid var(--color-border-muted)' }}
            >
              {testing ? <Loader size={14} className="text-current" /> : <Zap className="h-3.5 w-3.5" />}
              {t('mcp.modal.testSaved')}
            </button>
          ) : (
            <span />
          )}

          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={close}
              disabled={saving}
              className="px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10 disabled:opacity-50 disabled:pointer-events-none"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {t('mcp.modal.cancel')}
            </button>
            <button
              type="button"
              onClick={handleSubmit}
              disabled={saving || !validation.ok}
              data-testid="mcp-submit"
              className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50"
              style={{ color: 'var(--color-btn-primary-text)', backgroundColor: 'var(--color-btn-primary-bg)' }}
            >
              {saving && <Loader size={14} className="text-current" />}
              {isEdit ? t('mcp.modal.save') : t('mcp.modal.add')}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

/**
 * Hydrate the env/header editor on edit. Prefer the stored reference `map`
 * (real keys + `${vault:NAME}`/literal values): it round-trips the existing
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
  // so seed each ref under a blank key the user re-labels.
  return (refs ?? []).map((name) => ({ id: nextKvId(), key: '', value: `\${vault:${name}}` }));
}

/**
 * The header line a row will send, with a literal hidden and a vault ref shown
 * by name, so the row reads as `Authorization: Bearer ${vault:TOKEN}` and the
 * scheme composition is visible without revealing a pasted key.
 */
function headerPreview(key: string, value: string, hidden: string): string {
  const parts = splitHeader(key, value);
  const inner = parts.inner === '' || !/^\$\{vault:[A-Za-z_][A-Za-z0-9_]*\}$/.test(parts.inner) ? hidden : parts.inner;
  const shown = parts.scheme ? `${parts.scheme} ${inner}` : inner;
  return `${key || '?'}: ${shown}`;
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function Field({ label, hint, children }: { label: string; hint?: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1">
      <label className="text-xs font-medium" style={{ color: 'var(--color-text-secondary)' }}>{label}</label>
      {children}
      {hint && <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>{hint}</p>}
    </div>
  );
}

function FieldError({ error }: { error?: { message: string } }) {
  if (!error) return null;
  return <p className="text-[0.6875rem]" style={{ color: 'var(--color-loss)' }}>{error.message}</p>;
}

function ArgsEditor({ args, onChange }: { args: string[]; onChange: (a: string[]) => void }) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col gap-1.5">
      {args.map((a, i) => (
        <div key={i} className="flex gap-1.5">
          <input
            type="text"
            value={a}
            onChange={(e) => onChange(args.map((x, j) => (j === i ? e.target.value : x)))}
            placeholder={t('mcp.modal.argPlaceholder')}
            className="flex-1 px-2 py-1 text-xs rounded bg-transparent font-mono"
            style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
          />
          <button
            type="button"
            onClick={() => onChange(args.filter((_, j) => j !== i))}
            className="p-1.5 rounded hover:bg-foreground/10"
            style={{ color: 'var(--color-text-tertiary)' }}
            aria-label={t('mcp.modal.removeArg')}
          >
            <Trash2 className="h-3.5 w-3.5" />
          </button>
        </div>
      ))}
      <button
        type="button"
        onClick={() => onChange([...args, ''])}
        className="inline-flex items-center gap-1 text-[0.6875rem] self-start"
        style={{ color: 'var(--color-accent-primary)' }}
      >
        <Plus className="h-3 w-3" />
        {t('mcp.modal.addArg')}
      </button>
    </div>
  );
}

interface KeyValueEditorProps {
  /**
   * Headers get a pick list of the names remote servers actually want and a
   * scheme word composed in front of the value; env names have no common set,
   * so that side stays a plain text field.
   */
  kind: 'headers' | 'env';
  kvs: KV[];
  onChange: (kvs: KV[]) => void;
  secretNames: string[];
  createSecret: (body: { name: string; value: string }) => Promise<unknown>;
  keyPlaceholder: string;
  /** Prefix for the name offered when a typed value is saved to the vault. */
  serverName: string;
}

function KeyValueEditor({ kind, kvs, onChange, secretNames, createSecret, keyPlaceholder, serverName }: KeyValueEditorProps) {
  const { t } = useTranslation();
  const update = (i: number, patch: Partial<KV>) => onChange(kvs.map((x, j) => (j === i ? { ...x, ...patch } : x)));
  const newRow = (): KV =>
    kind === 'headers'
      ? { id: nextKvId(), ...joinHeader('bearer', '', '') }
      : { id: nextKvId(), key: '', value: '' };
  const fieldClass = 'min-w-0 px-2 py-1 text-xs rounded bg-transparent font-mono';
  const fieldStyle = { color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' };
  return (
    <div className="flex flex-col gap-1.5">
      {kvs.map((kv, i) => {
        const removeButton = (
          <button
            type="button"
            onClick={() => onChange(kvs.filter((_, j) => j !== i))}
            className="p-1.5 rounded hover:bg-foreground/10 self-start"
            style={{ color: 'var(--color-text-tertiary)' }}
            aria-label={t('mcp.modal.removeEntry')}
          >
            <Trash2 className="h-3.5 w-3.5" />
          </button>
        );
        // Suggested vault name for a typed value, e.g. FUYAO_FUND_X_API_KEY.
        const parts = kind === 'headers' ? splitHeader(kv.key, kv.value) : null;
        const suggestedName = `${serverName}_${parts ? (parts.scheme ? parts.scheme.toUpperCase() : parts.name) : kv.key}`;
        if (parts) {
          const pick = (choice: HeaderChoiceId) =>
            update(i, joinHeader(choice, choice === 'custom' ? '' : parts.name, parts.inner));
          return (
            <div key={kv.id} className="flex flex-col gap-1">
              <div className="flex gap-1.5 items-start">
                <select
                  value={parts.choice}
                  onChange={(e) => pick(e.target.value as HeaderChoiceId)}
                  aria-label={t('mcp.modal.headerChoiceLabel')}
                  data-testid={`mcp-header-choice-${i}`}
                  className={`${fieldClass} w-[12.75rem] shrink-0`}
                  style={fieldStyle}
                >
                  {HEADER_CHOICES.map((c) => (
                    <option key={c.id} value={c.id}>
                      {c.id === 'custom' ? t('mcp.modal.headerCustom') : choiceLabel(c)}
                    </option>
                  ))}
                </select>
                {parts.choice === 'custom' && (
                  <input
                    type="text"
                    value={parts.name}
                    onChange={(e) => update(i, { key: e.target.value })}
                    placeholder={keyPlaceholder}
                    aria-label={t('mcp.modal.headerCustomPlaceholder')}
                    data-testid={`mcp-header-name-${i}`}
                    className={`${fieldClass} w-[8rem] shrink-0`}
                    style={fieldStyle}
                  />
                )}
                <VaultSecretPicker
                  value={parts.inner}
                  onChange={(inner) => update(i, joinHeader(parts.choice, parts.name, inner))}
                  secretNames={secretNames}
                  createSecret={createSecret}
                  suggestedName={suggestedName}
                />
                {removeButton}
              </div>
              {parts.scheme && (
                <p
                  className="text-[0.6875rem] font-mono truncate"
                  style={{ color: 'var(--color-text-tertiary)' }}
                  data-testid={`mcp-header-preview-${i}`}
                >
                  {t('mcp.modal.headerSends', { line: headerPreview(kv.key, kv.value, t('mcp.modal.headerHiddenValue')) })}
                </p>
              )}
            </div>
          );
        }
        return (
          <div key={kv.id} className="flex gap-1.5 items-start">
            <input
              type="text"
              value={kv.key}
              onChange={(e) => update(i, { key: e.target.value })}
              placeholder={keyPlaceholder}
              className={`${fieldClass} w-[12.75rem] shrink-0`}
              style={fieldStyle}
            />
            <VaultSecretPicker
              value={kv.value}
              onChange={(value) => update(i, { value })}
              secretNames={secretNames}
              createSecret={createSecret}
              suggestedName={suggestedName}
            />
            {removeButton}
          </div>
        );
      })}
      <button
        type="button"
        onClick={() => onChange([...kvs, newRow()])}
        className="inline-flex items-center gap-1 text-[0.6875rem] self-start"
        style={{ color: 'var(--color-accent-primary)' }}
      >
        <Plus className="h-3 w-3" />
        {t('mcp.modal.addEntry')}
      </button>
    </div>
  );
}
