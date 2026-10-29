import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { TFunction } from 'i18next';
import { ChevronLeft } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { MAX_SECRET_VALUE_LENGTH } from '@/lib/secretNames';
import { cn } from '@/lib/utils';
import { verdictIs } from '@/pages/ChatAgent/components/mcp/mcpProbeVerdict';
import type { McpCheckFailure, PluginSecretRequest } from '@/pages/ChatAgent/utils/api';
import type { Refusal } from '@/pages/ChatAgent/utils/api/refusal';
import type { KeysAsk, UndeclaredKey } from '../utils/installRefusal';
import { keyMismatch, type KeyMismatch } from '../utils/pluginSchemas';
import { FailureLine, RefusalBox } from './RefusalView';
import { SecretValueInput } from './secrets/SecretValueInput';

/**
 * The keys a plugin declares, asked for before it is installed.
 *
 * Every field is required. The plugin's servers are checked with these values
 * before anything is written, and a server with no key fails that check, so
 * an install with a blank field would only come back refused. A refusal lands
 * here too, with each server's reason, and nothing was installed.
 *
 * The label and description come with the request, not from the vault's
 * blueprint catalog. That catalog is keyed by name and holds one entry per
 * credential, so on a name this plugin shares with a builtin or with another
 * plugin it would describe somebody else's request on this plugin's form.
 *
 * Every value is masked: a declaration has no way to say a value is not a
 * secret, so none is shown in the clear until the user asks.
 *
 * A key a connector reads without the plugin declaring it gets a field once
 * the check reports it missing, and its value goes with the install like any
 * other, so it too is saved only once every check passes.
 */

/** The hint under a value its pattern refuses: what to check, not the
 *  pattern. */
function mismatchNote(t: TFunction, mismatch: KeyMismatch): string {
  switch (mismatch.kind) {
    case 'prefix':
      return t('plugins.install.keyFormatPrefix', { prefix: mismatch.prefix });
    case 'length': {
      const { actual: count, min, max } = mismatch;
      if (max === null) return t('plugins.install.keyFormatLengthAtLeast', { count, min });
      if (max !== min) return t('plugins.install.keyFormatLengthRange', { count, min, max });
      return t('plugins.install.keyFormatLength', { count, expected: min });
    }
    case 'characters':
      return t('plugins.install.keyFormatCharacters');
    default:
      return t('plugins.install.keyFormat');
  }
}

/**
 * Which field each refusal is about. A failure that names the keys it found
 * missing places itself. A key the server rejected is named nowhere, so it
 * goes under the undeclared keys that server once asked for by name, the
 * only keys known to be its own; failing that, under the declared key when
 * the plugin declares just one. The rest stay listed under the form.
 */
function placeFailures(
  required: PluginSecretRequest[],
  undeclared: readonly UndeclaredKey[],
  failures: McpCheckFailure[],
) {
  const fieldsOf = (failure: McpCheckFailure): string[] => {
    if (failure.missing_secrets.length > 0) return failure.missing_secrets;
    // About the server rather than the key (unreachable, a command that
    // would not start): no field fixes it.
    if (!verdictIs(failure.verdict, 'keyVerdict')) return [];
    const reads = undeclared.filter((u) => u.servers.includes(failure.server));
    if (reads.length > 0) return reads.map((u) => u.name);
    return required.length === 1 ? [required[0].name] : [];
  };
  const byField = new Map<string, McpCheckFailure[]>();
  const unplaced: McpCheckFailure[] = [];
  for (const failure of failures) {
    const fields = fieldsOf(failure);
    if (fields.length === 0) {
      unplaced.push(failure);
      continue;
    }
    for (const name of fields) {
      byField.set(name, [...(byField.get(name) ?? []), failure]);
    }
  }
  return { byField, unplaced };
}

function KeyField({
  name,
  label,
  description,
  docsUrl,
  regex,
  value,
  onChange,
  disabled,
  rejections,
}: {
  name: string;
  label: string;
  description: string;
  docsUrl: string | null;
  regex: string | null;
  value: string;
  onChange: (value: string) => void;
  disabled: boolean;
  /** The servers that refused this key on the last attempt. */
  rejections: McpCheckFailure[];
}) {
  const { t } = useTranslation();
  const [revealed, setRevealed] = useState(false);
  const id = `plugin-key-${name}`;
  const noteId = `${id}-note`;
  const mismatch = keyMismatch(value.trim(), regex);
  const offPattern = mismatch !== null;
  const refused = rejections.length > 0;
  return (
    <div className="flex flex-col gap-1">
      <label
        className="flex items-center gap-2 text-xs"
        style={{ color: 'var(--color-text-secondary)' }}
        htmlFor={id}
      >
        <span className="font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {label}
        </span>
        {/* The variable name is shown for matching against a vendor's docs,
            which a label that already is the name does by itself. */}
        {label !== name && (
          <span
            className="font-mono px-1.5 py-0.5 rounded text-[0.6875rem]"
            style={{
              color: 'var(--color-text-tertiary)',
              backgroundColor: 'var(--color-bg-tag)',
            }}
          >
            {name}
          </span>
        )}
      </label>
      {description && (
        <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
          {description}
        </p>
      )}
      <SecretValueInput
        id={id}
        value={value}
        onChange={onChange}
        visible={revealed}
        onToggleVisible={() => setRevealed((r) => !r)}
        labels={{ show: t('plugins.install.showKey'), hide: t('plugins.install.hideKey') }}
        required
        disabled={disabled}
        maxLength={MAX_SECRET_VALUE_LENGTH}
        placeholder={t('plugins.install.keyPlaceholder')}
        aria-invalid={refused || offPattern || undefined}
        aria-describedby={refused || offPattern ? noteId : undefined}
        className="w-full text-xs pl-2 pr-8 py-1.5 rounded-md placeholder:text-(--color-text-quaternary)"
        style={{
          color: 'var(--color-text-primary)',
          backgroundColor: 'var(--color-bg-input)',
          border: `1px solid ${refused ? 'var(--color-border-loss)' : 'var(--color-border-muted)'}`,
        }}
      />
      {(refused || offPattern) && (
        <div id={noteId} className="flex flex-col gap-0.5 text-[0.6875rem]">
          {rejections.map((f) => (
            <p key={f.server} style={{ color: 'var(--color-loss)' }}>
              <FailureLine failure={f} />
            </p>
          ))}
          {mismatch && !refused && (
            <p style={{ color: 'var(--color-warning)' }}>{mismatchNote(t, mismatch)}</p>
          )}
        </div>
      )}
      {docsUrl && (
        <a
          href={docsUrl}
          target="_blank"
          rel="noreferrer"
          className="self-start text-[0.6875rem] underline underline-offset-2"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {t('plugins.install.keyDocs')}
        </a>
      )}
    </div>
  );
}

/** Back to the step before this one, or Cancel where there is none. */
type KeysExit = { onBack: () => void; onCancel?: never } | { onCancel: () => void; onBack?: never };

const NO_EDITS: ReadonlySet<string> = new Set();

export function PluginKeysStep({
  ask,
  values,
  onChange,
  onSubmit,
  submitting,
  submitLabel,
  onBack,
  onCancel,
}: {
  ask: KeysAsk;
  values: Record<string, string>;
  onChange: (next: Record<string, string>) => void;
  /** Every key on the form, declared or not, in one map: the install takes
   *  both and saves them only once its checks pass. */
  onSubmit: (secrets: Record<string, string>) => void;
  submitting: boolean;
  submitLabel: string;
} & KeysExit) {
  const { t } = useTranslation();
  const { required, undeclared, refusal } = ask;
  const failures = refusal?.failures ?? null;

  // A refusal under a field stands until that field is edited; the next
  // attempt's refusal starts with nothing edited.
  const [edits, setEdits] = useState<{ of: Refusal | null; names: ReadonlySet<string> }>({
    of: refusal,
    names: NO_EDITS,
  });
  const edited = edits.of === refusal ? edits.names : NO_EDITS;
  const { byField, unplaced } = placeFailures(required, undeclared, failures ?? []);

  const busy = submitting;
  const names = [...required.map((r) => r.name), ...undeclared.map((u) => u.name)];
  const complete = names.every((name) => (values[name] ?? '').trim() !== '');

  function change(name: string, value: string) {
    if (!edited.has(name)) setEdits({ of: refusal, names: new Set(edited).add(name) });
    onChange({ ...values, [name]: value });
  }

  function submit() {
    if (!complete || busy) return;
    onSubmit(Object.fromEntries(names.map((name) => [name, (values[name] ?? '').trim()])));
  }

  const field = (name: string) => ({
    value: values[name] ?? '',
    onChange: (value: string) => change(name, value),
    disabled: busy,
    rejections: edited.has(name) ? [] : (byField.get(name) ?? []),
  });

  return (
    <form
      className="flex flex-col gap-3"
      onSubmit={(e) => {
        e.preventDefault();
        submit();
      }}
    >
      <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('plugins.install.keysHint')}
      </p>

      {required.map(({ name, label, description, docs_url, regex }) => (
        <KeyField
          key={name}
          name={name}
          label={label}
          description={description}
          docsUrl={docs_url}
          regex={regex}
          {...field(name)}
        />
      ))}

      {undeclared.map(({ name, servers }) => {
        const props = field(name);
        return (
          <KeyField
            key={name}
            name={name}
            label={name}
            description={t('plugins.install.keyUndeclared', { server: servers[0] })}
            docsUrl={null}
            regex={null}
            {...props}
            // The report that the key is missing is why the field exists, and
            // its description already names the connector; repeating it in
            // red under an empty field reads as a second failure. A key the
            // connector rejected is news, and is said here.
            rejections={props.rejections.filter((f) => f.verdict !== 'missing_secrets')}
          />
        );
      })}

      {/* Once a failure is said under its field, saying it again here is
          the same news twice, so only the ones no field holds stay. */}
      {refusal && (!failures?.length || unplaced.length > 0) && (
        <RefusalBox refusal={failures?.length ? { ...refusal, failures: unplaced } : refusal} />
      )}

      <div className={cn('flex gap-2', onBack ? 'justify-between' : 'justify-end')}>
        {onBack ? (
          <button
            type="button"
            onClick={onBack}
            disabled={busy}
            className="inline-flex items-center gap-1 px-2 py-1 text-xs rounded-md transition-colors hover:bg-foreground/5 disabled:opacity-50"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            <ChevronLeft className="h-3.5 w-3.5" />
            {t('common.back')}
          </button>
        ) : (
          <button
            type="button"
            onClick={onCancel}
            // Held while the keys are out being checked, as the dialog's own
            // close is: the answer to that attempt is said in this form.
            disabled={busy}
            className="px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10 disabled:opacity-50"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {t('plugins.install.keysCancel')}
          </button>
        )}
        <button
          type="submit"
          disabled={!complete || busy}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md disabled:opacity-50"
          style={{
            color: 'var(--color-btn-primary-text)',
            backgroundColor: 'var(--color-btn-primary-bg)',
          }}
        >
          {busy && <Loader size={12} className="text-current" />}
          {submitLabel}
        </button>
      </div>
    </form>
  );
}
