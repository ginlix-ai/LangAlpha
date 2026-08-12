import React, { useState } from 'react';
import { KeyRound, Plus, Check } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { createVaultSecret, formatApiErrorDetail } from '../../utils/api';

/**
 * Picks an existing workspace vault secret (emitting a `${vault:NAME}`
 * reference) or lets the user type a plain literal value. Includes an inline
 * "create a new secret" affordance so a user can provision a credential
 * without leaving the MCP modal.
 *
 * The picker NEVER reveals a secret value — it only deals in names. The chosen
 * value is the literal `${vault:NAME}` string (resolved server-side inside the
 * sandbox at run time).
 */

type Mode = 'vault' | 'literal';

function vaultRef(name: string): string {
  return `\${vault:${name}}`;
}

/** Extract the vault name from a `${vault:NAME}` value, or null for a literal. */
function refName(value: string): string | null {
  const m = value.match(/^\$\{vault:([A-Za-z_][A-Za-z0-9_]{0,127})\}$/);
  return m ? m[1] : null;
}

interface VaultSecretPickerProps {
  workspaceId: string;
  /** Current value (a `${vault:NAME}` ref or a literal). */
  value: string;
  onChange: (value: string) => void;
  /** Existing vault secret names in this workspace. */
  secretNames: string[];
  /** Called after a successful inline create so the parent can refetch names. */
  onSecretCreated?: (name: string) => void;
}

export function VaultSecretPicker({
  workspaceId,
  value,
  onChange,
  secretNames,
  onSecretCreated,
}: VaultSecretPickerProps) {
  const initialRef = refName(value);
  const [mode, setMode] = useState<Mode>(initialRef !== null || value === '' ? 'vault' : 'literal');

  // Inline-create state
  const [creating, setCreating] = useState(false);
  const [newName, setNewName] = useState('');
  const [newValue, setNewValue] = useState('');
  const [saving, setSaving] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);

  const selectedRef = refName(value);

  async function handleCreate() {
    const name = newName.trim().toUpperCase();
    if (!name || !newValue) return;
    setSaving(true);
    setCreateError(null);
    try {
      await createVaultSecret(workspaceId, { name, value: newValue });
      onChange(vaultRef(name));
      onSecretCreated?.(name);
      setCreating(false);
      setNewName('');
      setNewValue('');
    } catch (err) {
      setCreateError(formatApiErrorDetail(err));
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="flex flex-col gap-1.5">
      {/* Mode toggle */}
      <div className="flex gap-1 text-[0.6875rem]">
        <button
          type="button"
          onClick={() => { setMode('vault'); if (refName(value) === null) onChange(''); }}
          className="px-2 py-0.5 rounded"
          style={{
            color: mode === 'vault' ? 'var(--color-btn-primary-text)' : 'var(--color-text-tertiary)',
            backgroundColor: mode === 'vault' ? 'var(--color-btn-primary-bg)' : 'var(--color-bg-card)',
          }}
        >
          From vault
        </button>
        <button
          type="button"
          onClick={() => { setMode('literal'); if (refName(value) !== null) onChange(''); }}
          className="px-2 py-0.5 rounded"
          style={{
            color: mode === 'literal' ? 'var(--color-btn-primary-text)' : 'var(--color-text-tertiary)',
            backgroundColor: mode === 'literal' ? 'var(--color-btn-primary-bg)' : 'var(--color-bg-card)',
          }}
        >
          Literal
        </button>
      </div>

      {mode === 'vault' ? (
        <div className="flex flex-col gap-1.5">
          {secretNames.length > 0 && (
            <div className="flex flex-wrap gap-1">
              {secretNames.map((name) => {
                const active = selectedRef === name;
                return (
                  <button
                    key={name}
                    type="button"
                    onClick={() => onChange(vaultRef(name))}
                    className="inline-flex items-center gap-1 px-2 py-0.5 text-[0.6875rem] font-mono rounded"
                    style={{
                      color: active ? 'var(--color-btn-primary-text)' : 'var(--color-text-secondary)',
                      backgroundColor: active ? 'var(--color-btn-primary-bg)' : 'var(--color-bg-card)',
                      border: '1px solid var(--color-border-muted)',
                    }}
                  >
                    {active && <Check className="h-3 w-3" />}
                    <KeyRound className="h-3 w-3" />
                    {name}
                  </button>
                );
              })}
            </div>
          )}

          {creating ? (
            <div
              className="flex flex-col gap-1.5 p-2 rounded"
              style={{ backgroundColor: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}
            >
              <input
                type="text"
                value={newName}
                onChange={(e) => setNewName(e.target.value.toUpperCase().replace(/[^A-Z0-9_]/g, '').replace(/^[0-9]+/, ''))}
                placeholder="SECRET_NAME"
                className="w-full px-2 py-1 text-xs rounded bg-transparent outline-none font-mono"
                style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
                maxLength={64}
              />
              <input
                type="password"
                value={newValue}
                onChange={(e) => setNewValue(e.target.value)}
                placeholder="Secret value"
                className="w-full px-2 py-1 text-xs rounded bg-transparent outline-none"
                style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
                maxLength={4096}
              />
              {createError && (
                <div className="text-[0.6875rem]" style={{ color: 'var(--color-loss)' }}>{createError}</div>
              )}
              <div className="flex justify-end gap-1.5">
                <button
                  type="button"
                  onClick={() => { setCreating(false); setCreateError(null); }}
                  className="px-2 py-0.5 text-[0.6875rem] rounded hover:bg-foreground/10"
                  style={{ color: 'var(--color-text-tertiary)' }}
                >
                  Cancel
                </button>
                <button
                  type="button"
                  onClick={handleCreate}
                  disabled={saving || !newName || !newValue}
                  className="inline-flex items-center gap-1 px-2 py-0.5 text-[0.6875rem] rounded disabled:opacity-50"
                  style={{ color: 'var(--color-btn-primary-text)', backgroundColor: 'var(--color-btn-primary-bg)' }}
                >
                  {saving && <Loader size={12} className="text-current" />}
                  Create &amp; use
                </button>
              </div>
            </div>
          ) : (
            <button
              type="button"
              onClick={() => { setCreating(true); setCreateError(null); }}
              className="inline-flex items-center gap-1 text-[0.6875rem] self-start"
              style={{ color: 'var(--color-accent-primary)' }}
            >
              <Plus className="h-3 w-3" />
              New secret
            </button>
          )}
        </div>
      ) : (
        <input
          type="text"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder="Literal value"
          className="w-full px-2 py-1 text-xs rounded bg-transparent outline-none"
          style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-muted)' }}
        />
      )}
    </div>
  );
}
