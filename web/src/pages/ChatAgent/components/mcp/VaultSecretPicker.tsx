import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { KeyRound, X } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
} from '@/components/ui/dropdown-menu';
import { formatApiErrorDetail } from '../../utils/api';

/**
 * One value field for an env var or header. It takes a plain literal by
 * default, because that is what a user has in their clipboard; a key button
 * beside it lists the vault, and a typed literal earns an offer to save it to
 * the vault under a suggested name, which swaps the `${vault:NAME}` ref in.
 * Which vault the save lands in is the caller's to decide.
 *
 * The picker never reveals a stored secret: a chosen ref renders as a chip
 * with only the name.
 */

function vaultRef(name: string): string {
  return `\${vault:${name}}`;
}

/** Extract the vault name from a `${vault:NAME}` value, or null for a literal. */
function refName(value: string): string | null {
  const m = value.match(/^\$\{vault:([A-Za-z_][A-Za-z0-9_]{0,127})\}$/);
  return m ? m[1] : null;
}

function normalizeSecretName(raw: string): string {
  return raw.toUpperCase().replace(/[^A-Z0-9_]/g, '_').replace(/_+/g, '_').replace(/^[0-9_]+|_+$/g, '');
}

interface VaultSecretPickerProps {
  /** Current value (a `${vault:NAME}` ref or a literal). */
  value: string;
  onChange: (value: string) => void;
  /** Existing secret names in the vault this picker writes to. */
  secretNames: string[];
  /**
   * Inline-create into the caller's vault tier, the workspace vault in the
   * settings panel and the user vault on /plugins, so a ref always resolves
   * where the server it belongs to runs.
   */
  createSecret: (body: { name: string; value: string }) => Promise<unknown>;
  /** The name offered when saving a typed literal, e.g. `FUYAO_FUND_X_API_KEY`. */
  suggestedName?: string;
  placeholder?: string;
}

export function VaultSecretPicker({
  value,
  onChange,
  secretNames,
  createSecret,
  suggestedName = '',
  placeholder,
}: VaultSecretPickerProps) {
  const { t } = useTranslation();
  const selectedRef = refName(value);
  const [saving, setSaving] = useState(false);
  const [saveOpen, setSaveOpen] = useState(false);
  const [saveName, setSaveName] = useState('');
  const [saveError, setSaveError] = useState<string | null>(null);

  const canOfferSave = selectedRef === null && value.trim() !== '';

  function openSave() {
    setSaveName(normalizeSecretName(suggestedName));
    setSaveError(null);
    setSaveOpen(true);
  }

  async function handleSave() {
    const name = normalizeSecretName(saveName);
    if (!name || !value) return;
    setSaving(true);
    setSaveError(null);
    try {
      await createSecret({ name, value });
      onChange(vaultRef(name));
      setSaveOpen(false);
    } catch (err) {
      setSaveError(formatApiErrorDetail(err));
    } finally {
      setSaving(false);
    }
  }

  const fieldStyle = { color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' };

  return (
    <div className="flex flex-col gap-1 flex-1 min-w-0">
      <div className="flex gap-1.5 min-w-0">
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
          <input
            type="text"
            value={value}
            onChange={(e) => onChange(e.target.value)}
            placeholder={placeholder ?? t('mcp.secret.valuePlaceholder')}
            className="flex-1 min-w-0 px-2 py-1 text-xs rounded bg-transparent font-mono"
            style={fieldStyle}
            autoComplete="off"
            spellCheck={false}
          />
        )}
        {secretNames.length > 0 && (
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <button
                type="button"
                className="p-1.5 rounded hover:bg-foreground/10"
                style={{ color: selectedRef !== null ? 'var(--color-accent-primary)' : 'var(--color-text-tertiary)' }}
                aria-label={t('mcp.secret.fromVault')}
                title={t('mcp.secret.fromVault')}
              >
                <KeyRound className="h-3.5 w-3.5" />
              </button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              {secretNames.map((name) => (
                <DropdownMenuItem key={name} onSelect={() => onChange(vaultRef(name))} className="font-mono text-xs">
                  {name}
                </DropdownMenuItem>
              ))}
            </DropdownMenuContent>
          </DropdownMenu>
        )}
      </div>

      {canOfferSave && !saveOpen && (
        <button
          type="button"
          onClick={openSave}
          className="text-[0.6875rem] self-start"
          style={{ color: 'var(--color-accent-primary)' }}
        >
          {t('mcp.secret.saveOffer')}
        </button>
      )}

      {canOfferSave && saveOpen && (
        <div className="flex flex-col gap-1">
          <div className="flex gap-1.5 items-center">
            <input
              type="text"
              value={saveName}
              aria-label={t('mcp.secret.saveAs')}
              onChange={(e) => setSaveName(e.target.value.toUpperCase().replace(/[^A-Z0-9_]/g, ''))}
              placeholder="SECRET_NAME"
              className="flex-1 min-w-0 px-2 py-0.5 text-[0.6875rem] rounded bg-transparent font-mono"
              style={fieldStyle}
              maxLength={64}
              autoFocus
            />
            <button
              type="button"
              onClick={() => void handleSave()}
              disabled={saving || !saveName}
              className="inline-flex items-center gap-1 px-2 py-0.5 text-[0.6875rem] rounded disabled:opacity-50"
              style={{ color: 'var(--color-btn-primary-text)', backgroundColor: 'var(--color-btn-primary-bg)' }}
            >
              {saving && <Loader size={12} className="text-current" />}
              {t('mcp.secret.save')}
            </button>
            <button
              type="button"
              onClick={() => setSaveOpen(false)}
              className="px-1.5 py-0.5 text-[0.6875rem] rounded hover:bg-foreground/10"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {t('common.cancel')}
            </button>
          </div>
          {saveError && (
            <div className="text-[0.6875rem]" style={{ color: 'var(--color-loss)' }}>{saveError}</div>
          )}
        </div>
      )}
    </div>
  );
}
