import React, { useId, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { KeyRound, Plus, X } from 'lucide-react';
import { Disclosure } from '@/components/ui/Disclosure';
import { Loader } from '@/components/ui/loader';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
} from '@/components/ui/dropdown-menu';
import { readersSentence } from '@/hooks/useSecretUsage';
import { useSecretNameInput } from '@/hooks/useSecretNameInput';
import { normalizeSecretName } from '@/lib/secretNames';
import { SecretValueInput } from '@/pages/Plugins/components/secrets/SecretValueInput';
import { formatApiErrorDetail } from '../../utils/api';
import { McpQuietButton } from './McpQuietButton';
import { useVaultKeys } from './useMcpFormFacts';

/**
 * One value field for an env var or header. It takes a plain literal by
 * default, because that is what a user has in their clipboard, masked as the
 * Secrets form masks one; a key button beside it lists the stored keys, and a
 * typed literal earns an offer to keep it in Secrets under a suggested name,
 * which swaps the `${vault:NAME}` ref in.
 *
 * The picker never reveals a stored key: a chosen ref renders as a chip with
 * only the name.
 *
 * The picker lays out the whole row, the header name before it and the remove
 * button after, so what it opens underneath (the save row, a note) spans the
 * row rather than the narrow column the value field gets beside a header
 * name. Squeezed into that column, a suggested name read `:SEARCH_BEARER`.
 */

/** Past this many stored keys the menu grows a filter. */
const FILTER_AT = 7;

function vaultRef(name: string): string {
  return `\${vault:${name}}`;
}

/** Extract the vault name from a `${vault:NAME}` value, or null for a literal. */
function refName(value: string): string | null {
  const m = value.match(/^\$\{vault:([A-Za-z_][A-Za-z0-9_]{0,127})\}$/);
  return m ? m[1] : null;
}

interface VaultSecretPickerProps {
  /** Current value (a `${vault:NAME}` ref or a literal). */
  value: string;
  onChange: (value: string) => void;
  /** The name offered when saving a typed literal, e.g. `FUYAO_FUND_X_API_KEY`. */
  suggestedName?: string;
  /** Accessible name of the value field, e.g. the header it fills. */
  label?: string;
  /** What sits before the value field on its line: a header's name. */
  lead?: React.ReactNode;
  /** What sits after it: the row's remove button. */
  trail?: React.ReactNode;
  /** What the row says about itself under its line, before the save offer. */
  below?: React.ReactNode;
  /** Why this value cannot be saved as it stands, said under the row. */
  problem?: string | null;
  /** The value is final for now: a key chosen or kept, or the field left. */
  onSettle?: () => void;
}

export function VaultSecretPicker({
  value,
  onChange,
  suggestedName = '',
  label,
  lead,
  trail,
  below,
  problem = null,
  onSettle,
}: VaultSecretPickerProps) {
  const { t } = useTranslation();
  const keys = useVaultKeys();
  const selectedRef = refName(value);
  const [saving, setSaving] = useState(false);
  const [saveOpen, setSaveOpen] = useState(false);
  const [name, setName] = useState('');
  const [saveError, setSaveError] = useState<string | null>(null);
  const [revealed, setRevealed] = useState(false);
  const [filter, setFilter] = useState('');
  const valueRef = useRef<HTMLInputElement>(null);
  const saveNameRef = useRef<HTMLInputElement>(null);
  const onSaveNameInput = useSecretNameInput(saveNameRef, name, setName);
  const saveNameId = useId();
  // "New key…" closes the menu and hands focus to the value field. Radix
  // returns focus to the trigger as the menu closes, so the request waits here
  // for that moment rather than racing it.
  const focusValueOnClose = useRef(false);

  const suggested = normalizeSecretName(suggestedName);
  const canOfferSave = selectedRef === null && value.trim() !== '' && !problem;
  // A taken name is offered as a replacement of what it holds, which is what
  // rotating a key is.
  const nameTaken = name !== '' && keys.facts.has(name);
  const takenUsers = keys.facts.get(name)?.usedBy;

  function openSave() {
    setName(suggested);
    setSaveError(null);
    setSaveOpen(true);
  }

  function startNew() {
    if (selectedRef !== null) onChange('');
    openSave();
    focusValueOnClose.current = true;
  }

  function pick(next: string) {
    onChange(vaultRef(next));
    setSaveOpen(false);
    onSettle?.();
  }

  async function handleSave() {
    if (!name || !value) return;
    setSaving(true);
    setSaveError(null);
    try {
      if (nameTaken) await keys.update(name, value);
      else await keys.create({ name, value });
      onChange(vaultRef(name));
      setSaveOpen(false);
      onSettle?.();
    } catch (err) {
      setSaveError(formatApiErrorDetail(err));
    } finally {
      setSaving(false);
    }
  }

  // The suggested key first, where the row has one stored; the rest as stored.
  const ordered = keys.facts.has(suggested)
    ? [suggested, ...keys.names.filter((n) => n !== suggested)]
    : keys.names;
  const needle = filter.trim().toLowerCase();
  const shown = needle
    ? ordered.filter(
        (n) =>
          n.toLowerCase().includes(needle) ||
          (keys.facts.get(n)?.description ?? '').toLowerCase().includes(needle),
      )
    : ordered;

  // Nothing while the connectors are unknown: "not used yet" would be a
  // guess, and a reason to pick that key over another.
  const usageLine = (users: readonly string[] | undefined) => {
    if (!users) return '';
    const { key, values } = readersSentence(users, {
      none: 'mcp.secret.unused',
      one: 'mcp.secret.usedByOne',
      many: 'mcp.secret.usedBy',
    });
    return t(key, values);
  };

  const fieldStyle = {
    color: 'var(--color-text-primary)',
    border: `1px solid ${problem ? 'var(--color-loss)' : 'var(--color-border-muted)'}`,
  };

  const keyMenu = (
    <DropdownMenu onOpenChange={(open) => !open && setFilter('')}>
      <DropdownMenuTrigger asChild>
        <button
          type="button"
          className="p-1.5 rounded hover:bg-foreground/10 shrink-0"
          style={{
            color: selectedRef !== null ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
          }}
          aria-label={t('mcp.secret.fromVault')}
          title={t('mcp.secret.fromVault')}
          data-testid="vault-key-menu"
        >
          <KeyRound className="h-3.5 w-3.5" />
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent
        align="end"
        className="w-72 max-w-[calc(100vw-2rem)]"
        onCloseAutoFocus={(e) => {
          if (!focusValueOnClose.current) return;
          focusValueOnClose.current = false;
          e.preventDefault();
          requestAnimationFrame(() => valueRef.current?.focus());
        }}
      >
        {keys.names.length >= FILTER_AT && (
          <input
            type="text"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            // The menu reads typed letters as a jump to the item that starts
            // with them, which would take every keystroke away from the field.
            onKeyDown={(e) => {
              if (e.key !== 'Escape' && e.key !== 'ArrowDown' && e.key !== 'ArrowUp') e.stopPropagation();
            }}
            placeholder={t('mcp.secret.filter')}
            aria-label={t('mcp.secret.filter')}
            className="w-full mb-1 px-2 py-1 text-xs rounded bg-transparent"
            style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
            autoComplete="off"
            spellCheck={false}
          />
        )}
        {shown.map((n) => {
          const facts = keys.facts.get(n);
          return (
            <DropdownMenuItem
              key={n}
              onSelect={() => pick(n)}
              className="flex-col items-start gap-0.5"
              data-testid={`vault-key-option-${n}`}
            >
              <span className="flex w-full items-center gap-2 min-w-0">
                <span className="font-mono text-xs truncate" style={{ color: 'var(--color-text-primary)' }}>
                  {n}
                </span>
                {n === suggested && (
                  <span className="ml-auto shrink-0 text-[0.625rem]" style={{ color: 'var(--color-text-tertiary)' }}>
                    {t('mcp.secret.suggested')}
                  </span>
                )}
              </span>
              <span className="text-[0.6875rem] line-clamp-2" style={{ color: 'var(--color-text-tertiary)' }}>
                {[facts?.description, usageLine(facts?.usedBy)].filter(Boolean).join(' · ')}
              </span>
            </DropdownMenuItem>
          );
        })}
        {needle && shown.length === 0 && (
          <p className="px-2.5 py-1.5 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('mcp.secret.noMatch')}
          </p>
        )}
        {keys.names.length > 0 && <DropdownMenuSeparator />}
        <DropdownMenuItem onSelect={startNew} className="text-xs" data-testid="vault-key-new">
          <Plus className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
          {t('mcp.secret.newKey')}
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );

  return (
    <div className="flex flex-col gap-1 min-w-0 flex-1">
      <div className="flex flex-col sm:flex-row gap-1.5 min-w-0 sm:items-start">
        {lead && <div className="flex gap-1.5 min-w-0 sm:shrink-0">{lead}</div>}
        <div className="flex gap-1.5 min-w-0 flex-1 items-start">
          {selectedRef !== null ? (
            <div
              className="flex-1 min-w-0 inline-flex items-center gap-1.5 px-2 py-1 text-xs rounded font-mono"
              style={fieldStyle}
              data-testid="vault-ref-chip"
            >
              <KeyRound className="h-3 w-3 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
              <span className="truncate">{selectedRef}</span>
              <button
                type="button"
                onClick={() => onChange('')}
                className="ml-auto p-0.5 rounded hover:bg-foreground/10"
                style={{ color: 'var(--color-text-tertiary)' }}
                aria-label={t('mcp.secret.clear')}
              >
                <X className="h-3 w-3" />
              </button>
            </div>
          ) : (
            <SecretValueInput
              ref={valueRef}
              value={value}
              onChange={onChange}
              visible={revealed}
              onToggleVisible={() => setRevealed((v) => !v)}
              labels={{ show: t('mcp.secret.showValue'), hide: t('mcp.secret.hideValue') }}
              onBlur={onSettle}
              onKeyDown={(e) => {
                if (e.key === 'Enter') onSettle?.();
              }}
              placeholder={t('mcp.secret.valuePlaceholder')}
              aria-label={label}
              aria-invalid={problem ? true : undefined}
              wrapperClassName="flex-1 min-w-0"
              className="w-full min-w-0 pl-2 pr-7 py-1 text-xs rounded bg-transparent font-mono"
              style={fieldStyle}
            />
          )}
          {keyMenu}
          {trail}
        </div>
      </div>

      {below}

      {problem && (
        <p className="text-[0.6875rem]" style={{ color: 'var(--color-loss)' }} data-testid="vault-value-problem">
          {problem}
        </p>
      )}

      {canOfferSave && !saveOpen && (
        <McpQuietButton
          onClick={openSave}
          className="self-start"
          icon={<KeyRound className="h-3 w-3" />}
        >
          {t('mcp.secret.saveOffer')}
        </McpQuietButton>
      )}

      <Disclosure open={saveOpen && selectedRef === null} className="flex flex-col gap-1 pt-1">
        <div className="flex flex-col gap-1">
          <label htmlFor={saveNameId} className="text-[0.6875rem]" style={{ color: 'var(--color-text-secondary)' }}>
            {t('mcp.secret.saveAs')}
          </label>
          <div className="flex flex-wrap gap-1.5 items-center">
            <input
              id={saveNameId}
              ref={saveNameRef}
              type="text"
              value={name}
              onChange={onSaveNameInput}
              placeholder="SECRET_NAME"
              // A row of its own on a phone: beside "Replace stored value" the
              // name was cut short, and the name is what the user checks here.
              className="grow basis-full sm:basis-48 px-2 py-1 text-xs rounded bg-transparent font-mono"
              style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
              maxLength={64}
              autoFocus
              data-testid="vault-save-name"
            />
            <McpQuietButton
              tone="primary"
              size="md"
              onClick={() => void handleSave()}
              disabled={saving || !name || !value}
              icon={saving ? <Loader size={12} className="text-current" /> : undefined}
              data-testid="vault-save"
            >
              {nameTaken ? t('mcp.secret.replace') : t('mcp.secret.save')}
            </McpQuietButton>
            <McpQuietButton size="md" onClick={() => setSaveOpen(false)}>
              {t('common.cancel')}
            </McpQuietButton>
          </div>
          {nameTaken && (
            <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }} data-testid="vault-save-taken">
              {takenUsers?.length
                ? t('mcp.secret.replaceUsed', { name, count: takenUsers.length })
                : t('mcp.secret.replaceUnused', { name })}
            </p>
          )}
          {!value && (
            <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('mcp.secret.pasteFirst')}
            </p>
          )}
          {saveError && (
            <div className="text-[0.6875rem]" style={{ color: 'var(--color-loss)' }}>{saveError}</div>
          )}
        </div>
      </Disclosure>
    </div>
  );
}
