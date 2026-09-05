import React, { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { KeyRound, Plus } from 'lucide-react';
import { ListEmpty, ListError, ListHeader, ListSkeleton } from '../mcp/McpPrimitives';
import { formatApiErrorDetail, type VaultBlueprint } from '../../utils/api';
import { BlueprintCards } from './BlueprintCards';
import { EMPTY_DRAFT, SecretAddForm, SecretEditForm, type SecretDraft } from './SecretEditor';
import { SecretRow } from './SecretRow';

/**
 * The one vault-secrets manager, shared by the two scopes: the workspace Vault
 * tab and the Plugins → Secrets page. It owns the entire add/edit/reveal/
 * delete state machine; callers supply the data and the four async operations
 * (React Query mutations on both sides) plus the scope-specific extras —
 * blueprints, prefill deep-link, hint copy, footer.
 */

const NAME_RE = /^[A-Za-z_][A-Za-z0-9_]{0,63}$/;

/**
 * At most one editing surface is open at a time, and its in-flight flag lives
 * with it — an `add` cannot be mid-save while an `edit` is also armed, so the
 * flags can't contradict each other the way parallel booleans could.
 * Reveal is deliberately outside: it is per-row display state that coexists
 * with any mode.
 */
type SecretsMode =
  | { kind: 'idle' }
  | { kind: 'add'; draft: SecretDraft; blueprint: VaultBlueprint | null; saving: boolean }
  | { kind: 'edit'; name: string; draft: SecretDraft; saving: boolean }
  | { kind: 'confirmDelete'; name: string; pending: boolean };

const IDLE: SecretsMode = { kind: 'idle' };

export interface SecretItem {
  id: string;
  name: string;
  description: string;
  masked_value: string;
}

export interface SecretsManagerProps {
  title: string;
  secrets: SecretItem[];
  maxSecrets: number;
  loading: boolean;
  loadError?: string | null;
  /** Scope explainer rendered under the header. */
  hint?: React.ReactNode;
  emptyText: string;
  /** "Recommended credentials" cards (declared by enabled MCP servers). */
  blueprints?: VaultBlueprint[];
  /** Deep-link (e.g. an MCP "Set up NAME" affordance): opens the add form prefilled. */
  prefillSecretName?: string | null;
  /** Must be referentially stable — it fires from the prefill effect. */
  onPrefillConsumed?: () => void;
  onCreate: (body: { name: string; value: string; description?: string }) => Promise<unknown>;
  onUpdate: (name: string, body: { value?: string; description?: string }) => Promise<unknown>;
  onDelete: (name: string) => Promise<unknown>;
  onReveal: (name: string) => Promise<string>;
  /** Scope-specific trailing content (e.g. the workspace usage/security card). */
  footer?: React.ReactNode;
}

export function SecretsManager({
  title,
  secrets,
  maxSecrets,
  loading,
  loadError,
  hint,
  emptyText,
  blueprints = [],
  prefillSecretName,
  onPrefillConsumed,
  onCreate,
  onUpdate,
  onDelete,
  onReveal,
  footer,
}: SecretsManagerProps) {
  const { t } = useTranslation();

  const [mode, setMode] = useState<SecretsMode>(IDLE);
  const [revealing, setRevealing] = useState<string | null>(null);
  const [revealed, setRevealed] = useState<Record<string, string>>({});
  // Bumped on every successful delete or update; a reveal resolving under an
  // older epoch discards its value instead of caching it.
  const revealEpoch = useRef(0);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!prefillSecretName) return;
    setError(null);
    setMode({
      kind: 'add',
      draft: { ...EMPTY_DRAFT, name: prefillSecretName },
      blueprint: null,
      saving: false,
    });
    onPrefillConsumed?.();
  }, [prefillSecretName, onPrefillConsumed]);

  function patchDraft(patch: Partial<SecretDraft>) {
    setMode((m) =>
      m.kind === 'add' || m.kind === 'edit' ? { ...m, draft: { ...m.draft, ...patch } } : m,
    );
  }

  function forget(name: string) {
    setRevealed((prev) => {
      const next = { ...prev };
      delete next[name];
      return next;
    });
  }

  async function handleCreate() {
    if (mode.kind !== 'add') return;
    const { name, value, description } = mode.draft;
    if (!name || !value) return;
    if (!NAME_RE.test(name)) {
      setError(t('vault.nameInvalid'));
      return;
    }
    setMode({ ...mode, saving: true });
    setError(null);
    try {
      await onCreate({ name, value, description: description || undefined });
      setMode(IDLE);
    } catch (err) {
      setError(formatApiErrorDetail(err));
    } finally {
      setMode((m) => (m.kind === 'add' ? { ...m, saving: false } : m));
    }
  }

  async function handleUpdate() {
    if (mode.kind !== 'edit') return;
    const { name, draft } = mode;
    setMode({ ...mode, saving: true });
    setError(null);
    try {
      await onUpdate(name, { ...(draft.value ? { value: draft.value } : {}), description: draft.description });
      setMode(IDLE);
      // forget() alone is not enough: a reveal already in flight would
      // re-cache the pre-edit plaintext when it resolves.
      revealEpoch.current += 1;
      forget(name);
    } catch (err) {
      setError(formatApiErrorDetail(err));
    } finally {
      setMode((m) => (m.kind === 'edit' ? { ...m, saving: false } : m));
    }
  }

  async function handleDelete() {
    if (mode.kind !== 'confirmDelete') return;
    setMode({ ...mode, pending: true });
    setError(null);
    try {
      await onDelete(mode.name);
      setMode(IDLE);
      // The reveal cache is keyed by name — left in place, a recreated
      // same-name secret would display the deleted one's plaintext.
      revealEpoch.current += 1;
      forget(mode.name);
    } catch (err) {
      setError(formatApiErrorDetail(err));
    } finally {
      setMode((m) => (m.kind === 'confirmDelete' ? { ...m, pending: false } : m));
    }
  }

  async function handleRevealToggle(name: string) {
    if (revealed[name] !== undefined) {
      forget(name);
      return;
    }
    setRevealing(name);
    setError(null);
    const epoch = revealEpoch.current;
    try {
      const value = await onReveal(name);
      // A delete that landed mid-reveal bumped the epoch — caching now would
      // resurrect the deleted secret's plaintext under a recreated name.
      if (revealEpoch.current === epoch) {
        setRevealed((prev) => ({ ...prev, [name]: value }));
      }
    } catch (err) {
      setError(formatApiErrorDetail(err));
    } finally {
      setRevealing(null);
    }
  }

  if (loading) {
    return <ListSkeleton rows={2} />;
  }

  const adding = mode.kind === 'add';

  return (
    <div className="flex flex-col gap-4">
      <ListHeader icon={KeyRound} title={title} count={secrets.length} max={maxSecrets}>
        {secrets.length < maxSecrets && (
          <button
            type="button"
            onClick={() => {
              setMode(adding ? IDLE : { kind: 'add', draft: EMPTY_DRAFT, blueprint: null, saving: false });
              setError(null);
            }}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors"
            style={{ color: 'var(--color-btn-primary-text)', backgroundColor: 'var(--color-btn-primary-bg)' }}
          >
            <Plus className="h-3 w-3" />
            {t('vault.addSecret')}
          </button>
        )}
      </ListHeader>

      {hint && (
        <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
          {hint}
        </p>
      )}

      {(error || loadError) && <ListError>{error || loadError}</ListError>}

      {blueprints.length > 0 && !adding && (
        <BlueprintCards
          blueprints={blueprints}
          atCap={secrets.length >= maxSecrets}
          maxSecrets={maxSecrets}
          onSelect={(bp) => {
            setError(null);
            setMode({
              kind: 'add',
              draft: { ...EMPTY_DRAFT, name: bp.name, description: bp.description || '' },
              blueprint: bp,
              saving: false,
            });
          }}
        />
      )}

      {mode.kind === 'add' && (
        <SecretAddForm
          draft={mode.draft}
          blueprint={mode.blueprint}
          saving={mode.saving}
          onChange={patchDraft}
          onCancel={() => { setMode(IDLE); setError(null); }}
          onSave={handleCreate}
        />
      )}

      {/* Secret list */}
      {secrets.length === 0 && !adding ? (
        <ListEmpty>{emptyText}</ListEmpty>
      ) : (
        <div className="flex flex-col gap-1">
          {secrets.map((secret) => (
            <div key={secret.id}>
              {mode.kind === 'edit' && mode.name === secret.name ? (
                <SecretEditForm
                  name={secret.name}
                  draft={mode.draft}
                  saving={mode.saving}
                  onChange={patchDraft}
                  onCancel={() => setMode(IDLE)}
                  onSave={handleUpdate}
                />
              ) : (
                <SecretRow
                  secret={secret}
                  revealedValue={revealed[secret.name]}
                  revealing={revealing === secret.name}
                  confirmingDelete={mode.kind === 'confirmDelete' && mode.name === secret.name}
                  deletePending={mode.kind === 'confirmDelete' && mode.pending}
                  onToggleReveal={() => handleRevealToggle(secret.name)}
                  onEdit={() => {
                    setMode({
                      kind: 'edit',
                      name: secret.name,
                      draft: { ...EMPTY_DRAFT, description: secret.description },
                      saving: false,
                    });
                    setError(null);
                  }}
                  onRequestDelete={() => setMode({ kind: 'confirmDelete', name: secret.name, pending: false })}
                  onCancelDelete={() => setMode(IDLE)}
                  onConfirmDelete={handleDelete}
                />
              )}
            </div>
          ))}
        </div>
      )}

      {footer}
    </div>
  );
}
