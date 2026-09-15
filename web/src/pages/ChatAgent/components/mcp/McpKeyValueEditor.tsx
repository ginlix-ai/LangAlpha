import { useTranslation } from 'react-i18next';
import { Plus, Trash2 } from 'lucide-react';
import { VaultSecretPicker } from './VaultSecretPicker';
import {
  HEADER_CHOICES,
  choiceLabel,
  joinHeader,
  splitHeader,
  type HeaderChoice,
  type HeaderChoiceId,
} from './mcpHeaderNames';

/**
 * The two key/value tables the server form holds: environment variables for a
 * local command, headers for a remote address, plus the argument list beside
 * them.
 *
 * Headers keep a typed row rather than the `{key, value}` pair they serialize
 * to. The pair could not tell an empty Basic selection from an empty
 * Authorization header, so a row the user had chosen Basic on and not yet
 * filled serialized to `{Authorization: ''}` and read back as Bearer on the
 * next render. The choice the user made is now stored as the choice they made,
 * and the pair is computed at the moment it is sent.
 */

/** Stable React key: rows hold stateful children (the vault picker), so
 *  identity has to be the key or deleting a middle row leaks its neighbour's
 *  draft onto it. */
let rowSeq = 0;
export function nextRowId(): string {
  rowSeq += 1;
  return `kv-${rowSeq}`;
}

export interface KvRow {
  id: string;
  key: string;
  value: string;
}

export interface HeaderRow {
  id: string;
  choice: HeaderChoiceId;
  /** The name typed beside a `custom` choice; unused by the listed ones. */
  customName: string;
  /** The value without its scheme word. */
  inner: string;
}

/** Blank-key rows are dropped; an env var set to the empty string is a real
 *  setting and stays. */
export function kvsToMap(rows: KvRow[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (const { key, value } of rows) {
    if (!key.trim()) continue;
    out[key.trim()] = value;
  }
  return out;
}

export function mapToKVs(map: Record<string, string>): KvRow[] {
  return Object.entries(map).map(([key, value]) => ({ id: nextRowId(), key, value }));
}

function choiceOf(id: HeaderChoiceId): HeaderChoice {
  return HEADER_CHOICES.find((c) => c.id === id) ?? HEADER_CHOICES[0];
}

/** The header name a row will send. */
export function headerRowName(row: HeaderRow): string {
  const choice = choiceOf(row.choice);
  return choice.id === 'custom' ? row.customName : choice.name;
}

/**
 * The `{name: value}` map these rows send. A row with a name but no value is
 * dropped: a fresh header row already carries `Authorization`, and an empty
 * `Authorization:` is a malformed request, not an absent credential.
 */
export function headersToMap(rows: HeaderRow[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (const row of rows) {
    const { key, value } = joinHeader(row.choice, row.customName, row.inner);
    if (!key.trim() || value === '') continue;
    out[key.trim()] = value;
  }
  return out;
}

export function mapToHeaderRows(map: Record<string, string>): HeaderRow[] {
  return Object.entries(map).map(([key, value]) => {
    const parts = splitHeader(key, value);
    return {
      id: nextRowId(),
      choice: parts.choice,
      customName: parts.choice === 'custom' ? parts.name : '',
      inner: parts.inner,
    };
  });
}

export function newHeaderRow(): HeaderRow {
  return { id: nextRowId(), choice: 'bearer', customName: '', inner: '' };
}

/**
 * The header line a row will send, with a literal hidden and a vault ref shown
 * by name, so the row reads as `Authorization: Bearer ${vault:TOKEN}` and the
 * scheme composition is visible without revealing a pasted key.
 */
export function headerPreview(row: HeaderRow, hidden: string): string {
  const choice = choiceOf(row.choice);
  const bare = /^\$\{vault:[A-Za-z_][A-Za-z0-9_]*\}$/.test(row.inner) ? row.inner : hidden;
  const shown = choice.scheme ? `${choice.scheme} ${bare}` : bare;
  return `${headerRowName(row) || '?'}: ${shown}`;
}

/** The vault name offered when a typed value on this row is saved. */
function suggestedSecretName(serverName: string, leaf: string): string {
  return `${serverName}_${leaf}`;
}

const fieldClass = 'min-w-0 px-2 py-1 text-xs rounded bg-transparent font-mono';
const fieldStyle = {
  color: 'var(--color-text-primary)',
  border: '1px solid var(--color-border-muted)',
} as const;

function RemoveRowButton({ onClick }: { onClick: () => void }) {
  const { t } = useTranslation();
  return (
    <button
      type="button"
      onClick={onClick}
      className="p-1.5 rounded hover:bg-foreground/10 self-start"
      style={{ color: 'var(--color-text-tertiary)' }}
      aria-label={t('mcp.modal.removeEntry')}
    >
      <Trash2 className="h-3.5 w-3.5" />
    </button>
  );
}

function AddRowButton({ label, onClick }: { label: string; onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="inline-flex items-center gap-1 text-[0.6875rem] self-start"
      style={{ color: 'var(--color-accent-primary)' }}
    >
      <Plus className="h-3 w-3" />
      {label}
    </button>
  );
}

export function ArgsEditor({
  args,
  onChange,
}: {
  args: string[];
  onChange: (args: string[]) => void;
}) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col gap-1.5">
      {args.map((arg, i) => (
        <div key={i} className="flex gap-1.5">
          <input
            type="text"
            value={arg}
            onChange={(e) => onChange(args.map((x, j) => (j === i ? e.target.value : x)))}
            placeholder={t('mcp.modal.argPlaceholder')}
            className="flex-1 px-2 py-1 text-xs rounded bg-transparent font-mono"
            style={fieldStyle}
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
      <AddRowButton label={t('mcp.modal.addArg')} onClick={() => onChange([...args, ''])} />
    </div>
  );
}

interface EditorCommon {
  secretNames: string[];
  /** Inline-create into the caller's vault tier, so a ref always resolves
   *  where the server it belongs to runs. */
  createSecret: (body: { name: string; value: string }) => Promise<unknown>;
  keyPlaceholder: string;
  /** Prefix for the name offered when a typed value is saved to the vault. */
  serverName: string;
}

export type KeyValueEditorProps =
  | (EditorCommon & { kind: 'env'; rows: KvRow[]; onChange: (rows: KvRow[]) => void })
  | (EditorCommon & { kind: 'headers'; rows: HeaderRow[]; onChange: (rows: HeaderRow[]) => void });

/**
 * Headers get a pick list of the names remote servers actually want and a
 * scheme word composed in front of the value; env names have no common set, so
 * that side stays a plain text field.
 */
export function KeyValueEditor(props: KeyValueEditorProps) {
  const { t } = useTranslation();
  const { secretNames, createSecret, keyPlaceholder, serverName } = props;

  if (props.kind === 'headers') {
    const { rows, onChange } = props;
    const update = (i: number, patch: Partial<HeaderRow>) =>
      onChange(rows.map((x, j) => (j === i ? { ...x, ...patch } : x)));
    return (
      <div className="flex flex-col gap-1.5">
        {rows.map((row, i) => {
          const choice = choiceOf(row.choice);
          return (
            <div key={row.id} className="flex flex-col gap-1">
              <div className="flex gap-1.5 items-start">
                <select
                  value={row.choice}
                  onChange={(e) =>
                    update(i, { choice: e.target.value as HeaderChoiceId })
                  }
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
                {row.choice === 'custom' && (
                  <input
                    type="text"
                    value={row.customName}
                    onChange={(e) => update(i, { customName: e.target.value })}
                    placeholder={keyPlaceholder}
                    aria-label={t('mcp.modal.headerCustomPlaceholder')}
                    data-testid={`mcp-header-name-${i}`}
                    className={`${fieldClass} w-[8rem] shrink-0`}
                    style={fieldStyle}
                  />
                )}
                <VaultSecretPicker
                  value={row.inner}
                  onChange={(inner) => update(i, { inner })}
                  secretNames={secretNames}
                  createSecret={createSecret}
                  suggestedName={suggestedSecretName(
                    serverName,
                    choice.scheme ? choice.scheme.toUpperCase() : headerRowName(row),
                  )}
                />
                <RemoveRowButton onClick={() => onChange(rows.filter((_, j) => j !== i))} />
              </div>
              {choice.scheme && (
                <p
                  className="text-[0.6875rem] font-mono truncate"
                  style={{ color: 'var(--color-text-tertiary)' }}
                  data-testid={`mcp-header-preview-${i}`}
                >
                  {t('mcp.modal.headerSends', {
                    line: headerPreview(row, t('mcp.modal.headerHiddenValue')),
                  })}
                </p>
              )}
            </div>
          );
        })}
        <AddRowButton
          label={t('mcp.modal.addEntry')}
          onClick={() => onChange([...rows, newHeaderRow()])}
        />
      </div>
    );
  }

  const { rows, onChange } = props;
  const update = (i: number, patch: Partial<KvRow>) =>
    onChange(rows.map((x, j) => (j === i ? { ...x, ...patch } : x)));
  return (
    <div className="flex flex-col gap-1.5">
      {rows.map((row, i) => (
        <div key={row.id} className="flex gap-1.5 items-start">
          <input
            type="text"
            value={row.key}
            onChange={(e) => update(i, { key: e.target.value })}
            placeholder={keyPlaceholder}
            className={`${fieldClass} w-[12.75rem] shrink-0`}
            style={fieldStyle}
          />
          <VaultSecretPicker
            value={row.value}
            onChange={(value) => update(i, { value })}
            secretNames={secretNames}
            createSecret={createSecret}
            suggestedName={suggestedSecretName(serverName, row.key)}
          />
          <RemoveRowButton onClick={() => onChange(rows.filter((_, j) => j !== i))} />
        </div>
      ))}
      <AddRowButton
        label={t('mcp.modal.addEntry')}
        onClick={() => onChange([...rows, { id: nextRowId(), key: '', value: '' }])}
      />
    </div>
  );
}
