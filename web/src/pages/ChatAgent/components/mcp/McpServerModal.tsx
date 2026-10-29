import { useDeferredValue, useId, useMemo, useReducer, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AlertCircle, Globe, Terminal, Zap } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { ModalShell } from '@/components/ui/ModalShell';
import { Field, FieldError } from '@/components/mcp/McpPrimitives';
import { cn } from '@/lib/utils';
import { useDebouncedValue } from '@/lib/useDebouncedValue';
import { RefusalBox } from '@/pages/Plugins/components/RefusalView';
import { McpAdvancedFields } from './McpAdvancedFields';
import { McpDiscoverResult } from './McpDiscoverResult';
import { McpProbePanel } from './McpProbePanel';
import { McpQuietButton } from './McpQuietButton';
import { McpStdioCheck } from './McpStdioCheck';
import { ArgsEditor, KeyValueEditor } from './McpKeyValueEditor';
import { useTakenNames } from './useMcpFormFacts';
import { PROBE_DEBOUNCE_MS, useRemoteProbe } from './useRemoteProbe';
import type { FormSaid } from './useMcpServerList';
import {
  argsChanged,
  commandChanged,
  discoverySecretsForced,
  draftArgv,
  draftBlankKeyPath,
  draftDuplicateKeyPath,
  draftPayload,
  draftStandIns,
  draftSuggestedName,
  draftTransport,
  draftUrl,
  envChanged,
  formReducer,
  freeName,
  hasStandIns,
  initialDraft,
  initialForm,
  placeRefusal,
  remoteUrlProblem,
  transportPinned,
  type DraftMeta,
  type RefusalSpot,
  type RemoteUrlProblem,
} from './mcpServerDraft';
import { serverNameError, validateMcpServer, type ServerNameError } from './mcpSchemas';
import {
  formatApiErrorDetail,
  type McpDiscoveryResult,
  type McpProbeInput,
  type McpProbeResult,
  type McpServerDraft,
  type McpServerInput,
} from '../../utils/api';

/**
 * Create/edit modal for a workspace (or catalog) MCP server.
 *
 * One field carries the whole definition: a URL makes a remote server, a
 * command line makes a local one, a pasted JSON config fills everything. The
 * transport is read off it rather than asked for, the name is suggested from
 * it, and a remote address is checked from the host as the user types so the
 * verdict is on screen before Save rather than after the next sandbox turn. A
 * local command has no check before Save; the save itself starts it on the
 * user's computer and keeps it only if it lists its tools.
 *
 * The form holds one typed state (`mcpServerDraft.ts`) and every input is a
 * transition on it; the payload, the suggested name and the address to check
 * are derived from it rather than stored beside it. env/header values use
 * `VaultSecretPicker` (emits `${vault:NAME}`), and the fields that only tune
 * the prompt sit under Advanced.
 */

export interface McpServerModalProps {
  /** When editing, the server being edited (its name field is locked). */
  initial?: McpServerDraft | null;
  /** For an add, a server already written out to start from: an import entry
   *  being fixed, or a save being tried again. Ignored when editing. */
  prefill?: McpServerInput | null;
  /** Whether the sandbox is in a state that can run a discovery of the saved
   *  config. False where there is no sandbox to run one in, which takes the
   *  "Test saved config" button away. */
  allowDiscover?: boolean;
  onClose: () => void;
  /** `toolCount` is what the form's own check listed for exactly this server,
   *  for the confirmation; null where it has no such answer. */
  onSubmit: (body: McpServerInput, facts?: { toolCount: number | null }) => Promise<void>;
  onDiscover?: (body: McpServerInput) => Promise<McpDiscoveryResult>;
  /** The host-side check of a remote address, before anything is saved. The
   *  signal is the query's: an address the user has typed past stops holding
   *  a host-side probe slot. */
  onProbe?: (body: McpProbeInput, signal?: AbortSignal) => Promise<McpProbeResult>;
  /** A helper line under the title, for where the save lands when the surface
   *  alone does not make it obvious. */
  note?: string;
  /** What the surface says about the server the form opened on: a sign-in
   *  cancelled at the vendor, said instead of `note`, and why the server was
   *  not saved (the form's last save, an import entry's reason, a sign-in
   *  that came back failed), said until the form changes from what was sent. */
  said?: FormSaid | null;
  /** Title and save label for an edit that reaches past the surface it was
   *  opened from, such as an account server edited from one workspace. */
  editLabels?: { title: string; save: string };
  saving?: boolean;
  /** The save turned out to need a sign-in, which is starting; the form holds
   *  until the page leaves for the vendor. */
  signingIn?: boolean;
}

const NOOP = () => {};

const NAME_ERROR_KEY: Record<ServerNameError, string> = {
  shape: 'mcp.modal.nameShape',
  runtimeModule: 'mcp.modal.nameReservedRuntime',
  dunder: 'mcp.modal.nameReservedDunder',
  keyword: 'mcp.modal.nameReservedKeyword',
};

// The address policy's own words name the rule, in English; the field says the
// fix. Explicit so the i18n parity test sees each key as a literal.
const URL_PROBLEM_KEY: Record<RemoteUrlProblem, string> = {
  scheme: 'mcp.modal.urlScheme',
  local: 'mcp.modal.urlLocal',
  userinfo: 'mcp.modal.urlUserinfo',
  secret: 'mcp.modal.urlSecret',
  malformed: 'mcp.modal.urlMalformed',
};

export function McpServerModal({
  initial,
  prefill = null,
  allowDiscover = true,
  onClose,
  onSubmit,
  onDiscover,
  onProbe,
  note,
  said = null,
  editLabels,
  saving: savingProp = false,
  signingIn = false,
}: McpServerModalProps) {
  const { t } = useTranslation();
  const saving = savingProp || signingIn;
  // Every dismissal route waits out a save. Closing mid-flight leaves the
  // outcome nowhere to land: a failure has no form left to show it in, and a
  // success closes whatever the user opened next.
  const close = saving ? NOOP : onClose;
  const titleId = useId();
  const isEdit = !!initial;
  // What an edited row is saved under. It keeps that name even if the sandbox
  // has reserved it since, as the backend's edit routes do.
  const keepName = initial?.name ?? null;

  const [form, dispatch] = useReducer(formReducer, null, () => initialForm(initial, prefill));
  const { draft, meta } = form;
  // The edited connector as it opened: what it already holds is never read as
  // a stand-in that blocks the save.
  const [saved] = useState(() => (initial ? initialDraft(initial) : null));
  // Null until the user types one: the name follows the field until then.
  const [typedName, setTypedName] = useState<string | null>(initial?.name ?? null);
  const [errors, setErrors] = useState<Array<{ path: string; message: string }>>([]);
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<McpDiscoveryResult | null>(null);
  const takenNames = useTakenNames();

  const transport = draftTransport(draft);
  const remote = draft.kind === 'remote';
  // A new server's suggested name steps aside for one already in use; one the
  // user typed is theirs to change, so a clash there is said, not fixed.
  const suggestedName = draftSuggestedName(draft);
  const autoName = isEdit ? suggestedName : freeName(suggestedName, takenNames);
  const name = typedName ?? autoName;
  const suffixedFrom = !isEdit && typedName === null && autoName !== suggestedName ? suggestedName : null;
  const takenBy = !isEdit && typedName !== null ? takenNames.get(typedName.trim()) : undefined;
  const discoveryForced = discoverySecretsForced(draft);
  const standIns = useMemo(() => draftStandIns(draft, saved), [draft, saved]);
  const blockedByStandIns = hasStandIns(standIns);
  // Legacy SSE is refused by every save's check. A new server is told so at
  // detection and cannot be added; an existing one keeps its edit, since a
  // save that leaves its address alone is not checked.
  const isSse = transport === 'sse';
  const sseRefused = !isEdit && isSse;
  const fullMeta: DraftMeta = { ...meta, name };
  const payload = useMemo(
    () => draftPayload(draft, fullMeta),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [draft, name, meta],
  );
  const payloadKey = payload ? JSON.stringify(payload) : '';
  // Defer the validated payload so a fast typer doesn't pay a full Zod
  // safeParse + URL canonicalization on every keystroke; this only gates the
  // disabled state of the Add/Test buttons, and both handlers re-validate the
  // CURRENT payload, so a slightly-stale gate can never let stale data through.
  const deferredPayload = useDeferredValue(payload);
  const liveCheck = useMemo(
    () => (deferredPayload ? validateMcpServer(deferredPayload, { keepName }) : null),
    [deferredPayload, keepName],
  );
  const payloadValid = !!liveCheck?.ok;
  // That gate leaves Add disabled with no submit to explain it, so each field
  // says its own problem as the form stands, once it has rested: an address
  // is "incomplete" at every keystroke on the way to a good one.
  const restedPayloadKey = useDebouncedValue(payloadKey, PROBE_DEBOUNCE_MS);
  const liveErrors =
    liveCheck && !liveCheck.ok && restedPayloadKey === payloadKey ? liveCheck.errors : [];
  const canSubmit = payloadValid && !blockedByStandIns && !takenBy && !sseRefused;
  // What the last save sent. A refusal speaks to that payload alone: kept up
  // once the form said something else, a 401 stayed red beside the key that
  // fixed it, and a pasted config went on naming the server it replaced.
  // A form opened with a refusal already in hand (a sign-in that came back
  // failed, reopening what it was saving, or an import entry opened to fix)
  // holds it for what it opened on.
  const refusal = said?.refusal ?? null;
  const [sentPayload, setSentPayload] = useState<string | null>(() =>
    refusal && payload ? payloadKey : null,
  );
  // An empty form sent nothing, so no refusal is its to show.
  const refusalStands = !!payload && sentPayload === payloadKey;

  const probe = useRemoteProbe({
    form,
    dispatch,
    onProbe,
    savedUrl: initial ? (initial.url ?? '') : null,
    standInHeaders: standIns.headers.size > 0,
  });

  async function handleSubmit() {
    const body = draftPayload(draft, fullMeta);
    if (!body) return;
    const result = validateMcpServer(body, { keepName });
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
    setSentPayload(JSON.stringify(body));
    await onSubmit(body, { toolCount: probe.toolCount });
  }

  async function handleTest() {
    const body = draftPayload(draft, fullMeta);
    if (!onDiscover || !body) return;
    const result = validateMcpServer(body, { keepName });
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

  // What a submit found first, then what the form as it stands would be
  // refused for.
  const errorFor = (path: string) => {
    const at = (e: { path: string }) => e.path === path || e.path.startsWith(`${path}.`);
    return errors.find(at) ?? liveErrors.find(at);
  };
  // Checked as the name is typed: a refused name disables Save, so waiting
  // for a submit to explain it would leave the button disabled with no reason.
  // An empty one waits for the submit, as the other fields do.
  const nameReason = name || errorFor('name') ? serverNameError(name, keepName) : null;
  const nameError = nameReason
    ? t(NAME_ERROR_KEY[nameReason], { name })
    : takenBy === 'yours'
      ? t('mcp.modal.nameTakenYours', { name: name.trim() })
      : takenBy === 'reserved'
        ? t('mcp.modal.nameTakenReserved', { name: name.trim() })
        : null;
  const nameNote = suffixedFrom
    ? t(
        takenNames.get(suffixedFrom) === 'reserved'
          ? 'mcp.modal.nameSuffixedReserved'
          : 'mcp.modal.nameSuffixed',
        { taken: suffixedFrom, name },
      )
    : null;
  // Only the edited command is started again by its save.
  const stdioChecks = draft.kind === 'stdio' && (!isEdit || commandChanged(payload, initial));
  const entryFault =
    errorFor('url') ?? errorFor('command') ?? errorFor('args') ?? errorFor('transport');
  const urlProblem = remoteUrlProblem(draftUrl(draft));
  const entryError =
    entryFault?.path === 'url' && urlProblem ? { message: t(URL_PROBLEM_KEY[urlProblem]) } : entryFault;

  // --- A refusal, said once, where it is fixed ---------------------------
  // The save's check is about this server; an API error is about the request
  // and keeps its own line.
  const standing = refusalStands ? refusal : null;
  const refusedFailures = standing?.failures?.length ? standing.failures : null;
  const refusalSpot: RefusalSpot | null =
    refusedFailures &&
    placeRefusal({
      probe: probe.canCheck && (probe.probing || probe.refuses),
      sse: isSse,
      entry: !!entryError,
      name: !!nameError,
      headers: !!errorFor('headers'),
      env: !!errorFor('env'),
    });
  const refusalDetail = (spot: RefusalSpot) =>
    refusalSpot === spot && (
      <p
        className="text-[0.625rem] wrap-break-word"
        style={{ color: 'var(--color-text-tertiary)' }}
        data-testid="mcp-refusal-detail"
      >
        {t('mcp.probe.wireDetail', { detail: refusedFailures?.map((f) => f.reason).join(' · ') })}
      </p>
    );

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
          data-emphasis={probe.refuses ? 'secondary' : 'primary'}
          className={cn(
            'inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50',
            probe.refuses && 'hover:bg-(--color-bg-hover)',
          )}
          style={
            probe.refuses
              ? // Inset, so the button keeps its size when the verdict flips it.
                { color: 'var(--color-text-primary)', boxShadow: 'inset 0 0 0 1px var(--color-border-default)' }
              : { color: 'var(--color-btn-primary-text)', backgroundColor: 'var(--color-btn-primary-bg)' }
          }
        >
          {saving && <Loader size={14} className="text-current" />}
          {signingIn
            ? t('mcp.modal.signingIn')
            : isEdit
              ? (editLabels?.save ?? t('mcp.modal.save'))
              : t('mcp.modal.add')}
        </button>
      </div>
    </div>
  );

  return (
    <ModalShell
      labelId={titleId}
      title={isEdit ? (editLabels?.title ?? t('mcp.modal.editTitle')) : t('mcp.modal.addTitle')}
      onClose={onClose}
      closeDisabled={saving}
      footer={footer}
    >
      {(said?.note || note) && (
        <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }} data-testid="mcp-modal-note">
          {said?.note || note}
        </p>
      )}
      <Field label={t('mcp.modal.entryLabel')} hint={t('mcp.modal.entryHint')}>
        <input
          type="text"
          value={draft.entry}
          onChange={(e) => {
            dispatch({ type: 'entry', raw: e.target.value });
            setErrors([]);
          }}
          onBlur={() => dispatch({ type: 'commit' })}
          onKeyDown={(e) => {
            if (e.key === 'Enter') dispatch({ type: 'commit' });
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
        {isSse && (
          <div
            className="flex flex-col gap-1 p-2 rounded-md text-xs"
            style={{ backgroundColor: 'var(--color-bg-card)' }}
            data-testid="mcp-sse-refused"
          >
            <div className="flex items-start justify-between gap-2">
              <span
                className="inline-flex items-start gap-2 min-w-0"
                style={{ color: sseRefused ? 'var(--color-loss)' : 'var(--color-text-secondary)' }}
              >
                <AlertCircle className="h-3.5 w-3.5 shrink-0 mt-0.5" />
                <span className="wrap-break-word">
                  {sseRefused ? t('mcp.modal.sseRefused') : t('mcp.modal.sseEditNote')}
                </span>
              </span>
              <McpQuietButton
                className="shrink-0"
                onClick={() => dispatch({ type: 'draft', change: (d) => transportPinned(d, 'http') })}
                data-testid="mcp-sse-to-http"
              >
                {t('mcp.modal.sseUseHttp')}
              </McpQuietButton>
            </div>
            {refusalSpot === 'sse' && <div className="pl-5">{refusalDetail('sse')}</div>}
          </div>
        )}
        {entryNote && (
          <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>{entryNote}</p>
        )}
        <FieldError error={entryError} />
        {refusalDetail('entry')}
      </Field>

      <Field
        label={t('mcp.modal.nameLabel')}
        hint={isEdit || nameNote ? undefined : t('mcp.modal.nameHint')}
      >
        <input
          type="text"
          value={name}
          onChange={(e) => setTypedName(e.target.value)}
          disabled={isEdit}
          placeholder={t('mcp.modal.namePlaceholder')}
          aria-invalid={nameError ? true : undefined}
          className="w-full px-3 py-2 text-sm rounded-md bg-transparent font-mono disabled:opacity-60"
          style={inputStyle}
          maxLength={64}
          data-testid="mcp-name"
        />
        {nameNote && (
          <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-secondary)' }} data-testid="mcp-name-suffixed">
            {nameNote}
          </p>
        )}
        <FieldError error={nameError ? { message: nameError } : undefined} />
        {refusalDetail('name')}
      </Field>

      {draft.kind === 'remote' && (
        <>
          {/* The hint speaks to a row's value field, so it waits for a row:
              under a bare Add header it pointed at nothing. */}
          <Field
            label={t('mcp.modal.headersLabel')}
            hint={draft.headers.length ? t('mcp.modal.headersHint') : undefined}
          >
            <KeyValueEditor
              kind="headers"
              serverName={name}
              rows={draft.headers}
              onChange={(rows) => dispatch({ type: 'headers', rows })}
              onSettle={() => dispatch({ type: 'settle' })}
              standIns={standIns.headers}
              keyPlaceholder={t('mcp.modal.headerCustomPlaceholder')}
            />
            <FieldError error={errorFor('headers')} />
            {refusalDetail('headers')}
          </Field>
          {onProbe &&
            (probe.held ? (
              <p
                className="text-[0.6875rem]"
                style={{ color: 'var(--color-text-tertiary)' }}
                data-testid="mcp-probe-held"
              >
                {t('mcp.probe.saveToCheck')}
              </p>
            ) : (
              <McpProbePanel
                result={probe.result}
                probing={probe.probing}
                canCheck={probe.canCheck}
                onCheck={probe.check}
                pending={probe.pending}
              />
            ))}
        </>
      )}

      {draft.kind === 'stdio' && (
        <>
          <Field label={t('mcp.modal.argsLabel')}>
            <ArgsEditor
              args={draftArgv(draft).args}
              onChange={(args) => dispatch({ type: 'draft', change: (d) => argsChanged(d, args) })}
              standIns={standIns.args}
            />
          </Field>
          <Field
            label={t('mcp.modal.envLabel')}
            hint={draft.env.length ? t('mcp.modal.envHint') : undefined}
          >
            <KeyValueEditor
              kind="env"
              serverName={name}
              rows={draft.env}
              onChange={(rows) => dispatch({ type: 'draft', change: (d) => envChanged(d, rows) })}
              standIns={standIns.env}
              keyPlaceholder={t('mcp.modal.envKeyPlaceholder')}
            />
            <FieldError error={errorFor('env')} />
            {refusalDetail('env')}
          </Field>
          {saving && stdioChecks ? (
            <McpStdioCheck />
          ) : (
            stdioChecks && (
              <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('mcp.modal.stdioCheckNote')}
              </p>
            )
          )}
        </>
      )}

      {blockedByStandIns && (
        <p className="text-[0.6875rem]" style={{ color: 'var(--color-loss)' }} data-testid="mcp-standins">
          {t('mcp.modal.placeholdersBlock')}
        </p>
      )}

      {/* Under the fields the refusal is about, above what only tunes the
          prompt, so the reason sits beside what fixes it. */}
      {standing && (!refusedFailures || refusalSpot === 'box') && <RefusalBox refusal={standing} />}

      <McpAdvancedFields
        transport={transport}
        savedTransport={initial?.transport}
        meta={meta}
        dispatch={dispatch}
        discoveryForced={discoveryForced}
        errorFor={errorFor}
      />

      {testResult && <McpDiscoverResult result={testResult} />}
    </ModalShell>
  );
}
