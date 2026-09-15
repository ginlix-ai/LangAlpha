import { useDeferredValue, useEffect, useId, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { ChevronRight, Globe, Terminal, Zap } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { Disclosure } from '@/components/ui/Disclosure';
import { ModalShell } from '@/components/ui/ModalShell';
import { Field, FieldError } from '@/components/mcp/McpPrimitives';
import { cn } from '@/lib/utils';
import { queryKeys } from '@/lib/queryKeys';
import { useDebouncedValue } from '@/lib/useDebouncedValue';
import { McpDiscoverResult } from './McpDiscoverResult';
import { headersFingerprint } from './mcpHeadersFingerprint';
import { McpProbePanel } from './McpProbePanel';
import { ArgsEditor, KeyValueEditor } from './McpKeyValueEditor';
import {
  argsChanged,
  discoverySecretsForced,
  draftArgv,
  draftBlankKeyPath,
  draftDuplicateKeyPath,
  draftPayload,
  draftSuggestedName,
  draftTransport,
  entryChanged,
  envChanged,
  headersChanged,
  initialDraft,
  probeTarget,
  transportPinned,
  type Draft,
  type DraftMeta,
} from './mcpServerDraft';
import { DESCRIPTION_MAX, EXPOSURE_MODES, INSTRUCTION_MAX, validateMcpServer } from './mcpSchemas';
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
 * verdict is on screen before Save rather than after the next sandbox turn. A
 * local command has no host-side check; it is probed in the workspace after
 * the save.
 *
 * The form holds one typed `Draft` (`mcpServerDraft.ts`) and every input is a
 * transition on it; the payload, the suggested name and the address to check
 * are derived from it rather than stored beside it. env/header values use
 * `VaultSecretPicker` (emits `${vault:NAME}`), and the fields that only tune
 * the prompt sit under Advanced.
 */

/** How long the form rests before a remote address is checked. */
const PROBE_DEBOUNCE_MS = 700;

export interface McpServerModalProps {
  /** Existing vault secret names for the picker. */
  secretNames: string[];
  /** When editing, the server being edited (its name field is locked). */
  initial?: McpServerDraft | null;
  /** Whether the sandbox is in a state that can run a discovery of the saved
   *  config. False where there is no sandbox to run one in, which takes the
   *  "Test saved config" button away. */
  allowDiscover?: boolean;
  onClose: () => void;
  onSubmit: (body: McpServerInput) => Promise<void>;
  onDiscover?: (body: McpServerInput) => Promise<McpDiscoveryResult>;
  /** The host-side check of a remote address, before anything is saved. The
   *  caller supplies it because only it knows which vault the refs in the
   *  headers should resolve against. The signal is the query's: an address the
   *  user has typed past stops holding a host-side probe slot. */
  onProbe?: (body: McpProbeInput, signal?: AbortSignal) => Promise<McpProbeResult>;
  /** Which vault `onProbe` resolves refs against, as the cache's scope: a
   *  workspace id, or `''` for the user vault. A verdict is only about the
   *  vault that produced it, so the two must not share a cache entry. */
  probeScope?: string;
  /** Inline secret-create for the picker, into the tier this modal edits. */
  createSecret: (body: { name: string; value: string }) => Promise<unknown>;
  saving?: boolean;
  submitError?: string | null;
}

const NOOP = () => {};

/** Whether two addresses name the same host. An address that will not parse is
 *  never the one already saved. */
function sameOrigin(a: string, b: string): boolean {
  try {
    return new URL(a).origin === new URL(b).origin;
  } catch {
    return false;
  }
}

/** A check that never reached the server reads as unreachable: the form's job
 *  is to say whether this address answers, and from here it did not. */
function unreachable(error: string): McpProbeResult {
  return {
    verdict: 'unreachable',
    tools: [],
    server_info: null,
    error,
    http_status: null,
    missing_secrets: [],
    probed_at: null,
  };
}

export function McpServerModal({
  secretNames,
  initial,
  allowDiscover = true,
  onClose,
  onSubmit,
  onDiscover,
  onProbe,
  probeScope = '',
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
  const isEdit = !!initial;

  const [draft, setDraft] = useState<Draft>(() => initialDraft(initial));
  // Null until the user types one: the name follows the field until then.
  const [typedName, setTypedName] = useState<string | null>(initial?.name ?? null);
  const [meta, setMeta] = useState<Omit<DraftMeta, 'name'>>({
    description: initial?.description ?? '',
    instruction: initial?.instruction ?? '',
    exposure: (initial?.tool_exposure_mode as DraftMeta['exposure']) ?? 'summary',
    discoveryUsesSecrets: initial?.discovery_uses_secrets ?? false,
  });
  // The address the user has said is finished, by leaving the field or pressing
  // Enter in it, and whether a credential was already in the form when they
  // said it. An edit starts on the saved pairing: that one has been made.
  const [committed, setCommitted] = useState<{ url: string; withHeaders: boolean } | null>(
    initial?.url ? { url: initial.url, withHeaders: true } : null,
  );
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [errors, setErrors] = useState<Array<{ path: string; message: string }>>([]);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<McpDiscoveryResult | null>(null);

  const transport = draftTransport(draft);
  const remote = draft.kind === 'remote';
  const name = typedName ?? draftSuggestedName(draft);
  const discoveryForced = discoverySecretsForced(draft);
  const fullMeta: DraftMeta = { ...meta, name };
  const payload = useMemo(
    () => draftPayload(draft, fullMeta),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [draft, name, meta],
  );
  // Defer the validated payload so a fast typer doesn't pay a full Zod
  // safeParse + URL canonicalization on every keystroke; this only gates the
  // disabled state of the Add/Test buttons, and both handlers re-validate the
  // CURRENT payload, so a slightly-stale gate can never let stale data through.
  const deferredPayload = useDeferredValue(payload);
  const canSubmit = useMemo(
    () => !!deferredPayload && validateMcpServer(deferredPayload).ok,
    [deferredPayload],
  );

  // --- The live check of a remote address -------------------------------
  // React Query owns it: the key IS the question (this address, which
  // credential), so an answer can never be shown against a different one, and
  // typing back to an address already checked shows its verdict at once. The
  // credential rides in the key as a digest, never as itself: the key outlives
  // this form in the cache, and naming the credential is all the key needs.
  const queryClient = useQueryClient();
  const target = useMemo(() => probeTarget(draft), [draft]);
  const headersKey = target ? headersFingerprint(target.headers) : '';
  const probeKey = target ? `${target.url}\n${headersKey}` : '';
  const restedKey = useDebouncedValue(probeKey, PROBE_DEBOUNCE_MS);
  // When editing, the headers hold the server's own credential (or the refs the
  // host resolves into it), so re-pointing an existing server would hand that
  // credential to every hostname typed on the way to the intended one. A new
  // host waits for Save, which is the point the user says it is the right one;
  // a new path on the same host is the address the credential already went to.
  const probeHeld = isEdit && !!target && !sameOrigin(target.url, initial?.url ?? '');
  // A draft that carries a header carries a credential, and `https://acme.co`
  // is a complete, reachable host on the way to `https://acme.corp.example`.
  // Resting for 700ms is not the user saying the address is finished, so with a
  // header in hand the automatic check waits for the field to be committed
  // instead. A draft with no header has nothing to hand over and keeps the rest.
  const hasHeaders = !!target && Object.keys(target.headers).length > 0;
  const commitUrl = () => setCommitted(target ? { url: target.url, withHeaders: hasHeaders } : null);
  // A commit made before any credential existed is not consent to send one:
  // clicking "Add entry" under Headers is itself what leaves the address field,
  // so the host the user is still halfway through typing gets committed a beat
  // before the key is pasted in. The commit has to have been made with the
  // credential already in hand, which is the user saying this address gets it.
  const urlCommitted =
    !hasHeaders || (!!committed && committed.withHeaders && committed.url === target?.url);
  const probeQuery = useQuery({
    queryKey: queryKeys.mcp.probe(probeScope, target?.url ?? '', headersKey),
    queryFn: ({ signal }) => onProbe!({ url: target!.url, headers: target!.headers }, signal),
    enabled:
      !!onProbe && !!target && !probeHeld && urlCommitted && restedKey === probeKey,
    staleTime: Infinity,
    retry: false,
  });
  const probeResult =
    probeQuery.data ??
    (probeQuery.error ? unreachable(formatApiErrorDetail(probeQuery.error)) : null);
  // A verdict is only of use to the form that asked for it, so it leaves with
  // the form rather than resting in the cache for its gcTime.
  useEffect(
    () => () => {
      queryClient.removeQueries({ queryKey: queryKeys.mcp.probes(probeScope) });
    },
    [queryClient, probeScope],
  );

  function applyEntry(raw: string) {
    const next = entryChanged(draft, raw);
    setDraft(next);
    // A pasted config also carries the fields that live outside the draft.
    const filled = next.kind === 'remote' || next.kind === 'stdio' ? next.filled : undefined;
    if (filled) setMeta((m) => ({ ...m, ...filled.meta }));
    setErrors([]);
  }

  async function handleSubmit() {
    const body = draftPayload(draft, fullMeta);
    if (!body) return;
    const result = validateMcpServer(body);
    // Two rows the schema never gets to see: one map drops a blank key, both
    // maps are last-wins on a repeated one, so the payload is already short an
    // entry by the time it is validated.
    const blankPath = draftBlankKeyPath(draft);
    const duplicatePath = draftDuplicateKeyPath(draft);
    const rowErrors = [
      ...(blankPath ? [{ path: blankPath, message: t('mcp.modal.blankKey') }] : []),
      ...(duplicatePath
        ? [
            {
              path: duplicatePath,
              message:
                duplicatePath === 'headers'
                  ? t('mcp.modal.duplicateHeader')
                  : t('mcp.modal.duplicateEnvKey'),
            },
          ]
        : []),
    ];
    if (!result.ok || rowErrors.length > 0) {
      setErrors([...(result.ok ? [] : result.errors), ...rowErrors]);
      return;
    }
    setErrors([]);
    await onSubmit(body);
  }

  async function handleTest() {
    const body = draftPayload(draft, fullMeta);
    if (!onDiscover || !body) return;
    const result = validateMcpServer(body);
    if (!result.ok) {
      setErrors(result.errors);
      return;
    }
    setErrors([]);
    setTesting(true);
    setTestResult(null);
    try {
      setTestResult(await onDiscover(body));
    } catch (err) {
      setTestResult({ status: 'error', tools: [], error: formatApiErrorDetail(err) });
    } finally {
      setTesting(false);
    }
  }

  const errorFor = (path: string) =>
    errors.find((e) => e.path === path || e.path.startsWith(`${path}.`));
  const entryError =
    errorFor('url') ?? errorFor('command') ?? errorFor('args') ?? errorFor('transport');

  const filled = draft.kind === 'remote' || draft.kind === 'stdio' ? draft.filled : undefined;
  const entryNote =
    draft.kind === 'invalid'
      ? draft.note || t('mcp.modal.pasteNoServer')
      : filled
        ? filled.more > 0
          ? t('mcp.modal.filledFromMore', { name: filled.from, count: filled.more })
          : t('mcp.modal.filledFrom', { name: filled.from })
        : null;

  const inputStyle = {
    color: 'var(--color-text-primary)',
    border: '1px solid var(--color-border-muted)',
  } as const;

  const transportChoices: McpTransport[] =
    initial?.transport === 'sse' || transport === 'sse' ? ['http', 'sse', 'stdio'] : ['http', 'stdio'];

  const footer = (
    <div className="flex items-center justify-between gap-2">
      {/* Sandbox discovery runs against the PERSISTED server, so it's only
          offered when editing an existing row, and labelled to make clear
          it tests the saved config, not unsaved edits in this form. A
          remote row has the live check above instead. */}
      {allowDiscover && onDiscover && isEdit && !remote ? (
        <button
          type="button"
          onClick={handleTest}
          disabled={testing || saving || !canSubmit}
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
          disabled={saving || !canSubmit}
          data-testid="mcp-submit"
          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50"
          style={{ color: 'var(--color-btn-primary-text)', backgroundColor: 'var(--color-btn-primary-bg)' }}
        >
          {saving && <Loader size={14} className="text-current" />}
          {isEdit ? t('mcp.modal.save') : t('mcp.modal.add')}
        </button>
      </div>
    </div>
  );

  return (
    <ModalShell
      labelId={titleId}
      title={isEdit ? t('mcp.modal.editTitle') : t('mcp.modal.addTitle')}
      onClose={onClose}
      closeDisabled={saving}
      footer={footer}
    >
      <Field label={t('mcp.modal.entryLabel')} hint={t('mcp.modal.entryHint')}>
        <input
          type="text"
          value={draft.entry}
          onChange={(e) => applyEntry(e.target.value)}
          onBlur={commitUrl}
          onKeyDown={(e) => {
            if (e.key === 'Enter') commitUrl();
          }}
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
          onChange={(e) => setTypedName(e.target.value)}
          disabled={isEdit}
          placeholder={t('mcp.modal.namePlaceholder')}
          className="w-full px-3 py-2 text-sm rounded-md bg-transparent font-mono disabled:opacity-60"
          style={inputStyle}
          maxLength={64}
          data-testid="mcp-name"
        />
        <FieldError error={errorFor('name')} />
      </Field>

      {draft.kind === 'remote' && (
        <>
          <Field label={t('mcp.modal.headersLabel')} hint={t('mcp.modal.headersHint')}>
            <KeyValueEditor
              kind="headers"
              serverName={name}
              rows={draft.headers}
              onChange={(rows) => setDraft(headersChanged(draft, rows))}
              secretNames={secretNames}
              createSecret={createSecret}
              keyPlaceholder={t('mcp.modal.headerCustomPlaceholder')}
            />
            <FieldError error={errorFor('headers')} />
          </Field>
          {onProbe &&
            (probeHeld ? (
              <p
                className="text-[0.6875rem]"
                style={{ color: 'var(--color-text-tertiary)' }}
                data-testid="mcp-probe-held"
              >
                {t('mcp.probe.saveToCheck')}
              </p>
            ) : (
              <McpProbePanel
                result={probeResult}
                probing={probeQuery.isFetching}
                canCheck={!!target}
                onCheck={() => void probeQuery.refetch()}
              />
            ))}
        </>
      )}

      {draft.kind === 'stdio' && (
        <>
          <Field label={t('mcp.modal.argsLabel')}>
            <ArgsEditor
              args={draftArgv(draft).args}
              onChange={(args) => setDraft(argsChanged(draft, args))}
            />
          </Field>
          <Field label={t('mcp.modal.envLabel')} hint={t('mcp.modal.envHint')}>
            <KeyValueEditor
              kind="env"
              serverName={name}
              rows={draft.env}
              onChange={(rows) => setDraft(envChanged(draft, rows))}
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

        <Disclosure open={advancedOpen} className="flex flex-col gap-4 pl-1">
          <Field label={t('mcp.modal.transportLabel')} hint={t('mcp.modal.transportHint')}>
            <div className="flex gap-1">
              {transportChoices.map((tr) => (
                <button
                  key={tr}
                  type="button"
                  onClick={() => setDraft(transportPinned(draft, tr))}
                  className={cn(
                    'px-3 py-1.5 text-xs rounded-md uppercase transition-colors',
                    transport !== tr &&
                      'bg-[var(--color-bg-card)] hover:bg-[var(--color-bg-card-hover)]',
                  )}
                  style={
                    transport === tr
                      ? {
                          color: 'var(--color-btn-primary-text)',
                          backgroundColor: 'var(--color-btn-primary-bg)',
                        }
                      : { color: 'var(--color-text-tertiary)' }
                  }
                >
                  {tr}
                </button>
              ))}
            </div>
          </Field>

          <Field label={t('mcp.modal.descriptionLabel')} hint={t('mcp.modal.descriptionHint')}>
            <textarea
              value={meta.description}
              onChange={(e) => setMeta((m) => ({ ...m, description: e.target.value }))}
              placeholder={t('mcp.modal.descriptionPlaceholder')}
              rows={2}
              className="w-full px-3 py-2 text-sm rounded-md bg-transparent resize-none"
              style={inputStyle}
              maxLength={DESCRIPTION_MAX}
            />
            <FieldError error={errorFor('description')} />
          </Field>

          <Field label={t('mcp.modal.instructionLabel')} hint={t('mcp.modal.instructionHint')}>
            <textarea
              value={meta.instruction}
              onChange={(e) => setMeta((m) => ({ ...m, instruction: e.target.value }))}
              placeholder={t('mcp.modal.instructionPlaceholder')}
              rows={2}
              className="w-full px-3 py-2 text-sm rounded-md bg-transparent resize-none"
              style={inputStyle}
              maxLength={INSTRUCTION_MAX}
            />
            <FieldError error={errorFor('instruction')} />
          </Field>

          <Field label={t('mcp.modal.exposureLabel')} hint={t('mcp.modal.exposureHint')}>
            <div className="flex gap-1">
              {EXPOSURE_MODES.map((m) => (
                <button
                  key={m}
                  type="button"
                  onClick={() => setMeta((prev) => ({ ...prev, exposure: m }))}
                  className={cn(
                    'px-3 py-1.5 text-xs rounded-md transition-colors',
                    meta.exposure !== m &&
                      'bg-[var(--color-bg-card)] hover:bg-[var(--color-bg-card-hover)]',
                  )}
                  style={
                    meta.exposure === m
                      ? {
                          color: 'var(--color-btn-primary-text)',
                          backgroundColor: 'var(--color-btn-primary-bg)',
                        }
                      : { color: 'var(--color-text-tertiary)' }
                  }
                >
                  {m === 'summary' ? t('mcp.modal.exposureSummary') : t('mcp.modal.exposureDetailed')}
                </button>
              ))}
            </div>
          </Field>

          <Field
            label={t('mcp.modal.discoveryLabel')}
            hint={discoveryForced ? t('mcp.modal.discoveryForced') : t('mcp.modal.discoveryHint')}
          >
            <label
              className="flex items-center gap-2 text-sm"
              style={{
                color: 'var(--color-text-primary)',
                cursor: discoveryForced ? 'not-allowed' : 'pointer',
                opacity: discoveryForced ? 0.7 : 1,
              }}
            >
              <input
                type="checkbox"
                checked={meta.discoveryUsesSecrets || discoveryForced}
                disabled={discoveryForced}
                onChange={(e) => setMeta((m) => ({ ...m, discoveryUsesSecrets: e.target.checked }))}
                className="h-4 w-4 rounded"
                style={{ accentColor: 'var(--color-accent-primary)' }}
              />
              {t('mcp.modal.discoveryToggle')}
            </label>
          </Field>
        </Disclosure>
      </div>

      {testResult && <McpDiscoverResult result={testResult} />}

      {submitError && (
        <div className="text-xs p-2 rounded" style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-loss)' }}>
          {submitError}
        </div>
      )}
    </ModalShell>
  );
}
