import React, { useCallback, useEffect, useId, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { KeyRound, Plus, X } from 'lucide-react';
import {
  HeaderButton,
  ListEmpty,
  ListError,
  ListHeader,
  ListSkeleton,
} from '@/components/mcp/McpPrimitives';
import { Disclosure } from '@/components/ui/Disclosure';
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

/**
 * Keeps the last non-null value so a form that is folding closed still has
 * something to render on the way out. Without it the mode flips to idle in one
 * frame, the fields vanish, and the fold animates an empty box. What is kept is
 * a draft holding plaintext, so `release` gives it back the moment the fold
 * that paints it is gone.
 */
function useLastPresent<T>(value: T | null): [T | null, () => void] {
  const last = useRef<T | null>(null);
  if (value !== null) last.current = value;
  const release = useCallback(() => {
    last.current = null;
  }, []);
  return [value ?? last.current, release];
}

/**
 * The same, remembered per row. The list folds one editor closed as it opens
 * another, so a single slot hands the closing row the incoming row's value and
 * it folds on an empty box anyway, which is the thing being prevented. Entries
 * leave with their row's fold, so the map holds the folds on screen rather than
 * every row the session has edited.
 */
function useLastPresentByKey<T>(
  value: T | null,
  key: string | null,
): [Map<string, T>, (key: string) => void] {
  const remembered = useRef(new Map<string, T>());
  if (value !== null && key !== null) remembered.current.set(key, value);
  const release = useCallback((released: string) => {
    remembered.current.delete(released);
  }, []);
  return [remembered.current, release];
}

/**
 * Calls `onGone` once the folded content has actually left the tree.
 * `AnimatePresence` holds an exiting child mounted until its animation
 * finishes, so this unmount *is* the end of the fold, with no duration constant
 * to keep in step with the one the animation is really running.
 */
function OnFoldGone({ onGone, children }: { onGone: () => void; children: React.ReactNode }) {
  const latest = useRef(onGone);
  latest.current = onGone;
  useEffect(() => () => latest.current(), []);
  return <>{children}</>;
}

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
  // Read from the fold-gone callbacks, which fire during an unmount and so
  // cannot trust the render that scheduled them: reopening a form mid-exit
  // unmounts the old content after the new draft is already remembered.
  const modeRef = useRef(mode);
  modeRef.current = mode;
  const [revealing, setRevealing] = useState<string | null>(null);
  const [revealed, setRevealed] = useState<Record<string, string>>({});
  // Bumped on every successful delete or update; a reveal resolving under an
  // older epoch discards its value instead of caching it.
  const revealEpoch = useRef(0);
  const [error, setError] = useState<string | null>(null);
  const addFormId = useId();

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

  function closeForm() {
    setMode(IDLE);
    setError(null);
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

  const addMode = mode.kind === 'add' ? mode : null;
  const editMode = mode.kind === 'edit' ? mode : null;
  // The forms fold rather than pop, so each needs its content for one more
  // beat after the mode has already moved on.
  const [foldingAdd, releaseFoldingAdd] = useLastPresent(addMode);
  const [foldingEditByName, releaseFoldingEdit] = useLastPresentByKey(
    editMode,
    editMode?.name ?? null,
  );
  const adding = addMode !== null;

  function handleAddFoldGone() {
    if (modeRef.current.kind !== 'add') releaseFoldingAdd();
  }

  function handleEditFoldGone(name: string) {
    const current = modeRef.current;
    if (current.kind !== 'edit' || current.name !== name) releaseFoldingEdit(name);
  }

  if (loading) {
    return <ListSkeleton rows={2} />;
  }

  return (
    <div className="flex flex-col gap-4">
      <ListHeader icon={KeyRound} title={title} count={secrets.length} max={maxSecrets}>
        {secrets.length < maxSecrets && (
          // The button is the form's toggle, so it says which way it points:
          // a primary Add while the form is closed, a quiet Cancel while it is
          // open. A primary button that re-opens what is already open reads as
          // the action having failed.
          <HeaderButton
            variant={adding ? 'secondary' : 'primary'}
            icon={adding ? X : Plus}
            aria-expanded={adding}
            aria-controls={addFormId}
            onClick={() => {
              setMode(adding ? IDLE : { kind: 'add', draft: EMPTY_DRAFT, blueprint: null, saving: false });
              setError(null);
            }}
          >
            {adding ? t('common.cancel') : t('vault.addSecret')}
          </HeaderButton>
        )}
      </ListHeader>

      {hint && (
        <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
          {hint}
        </p>
      )}

      {(error || loadError) && <ListError>{error || loadError}</ListError>}

      {/* Stays put while the form is open: these cards are what tell the user
          which name the server is looking for, and hiding them the moment the
          form needs that name is hiding the answer to the question on screen. */}
      {blueprints.length > 0 && (
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

      <Disclosure open={adding} id={addFormId}>
        {foldingAdd && (
          <OnFoldGone onGone={handleAddFoldGone}>
            {/* Keyed by the blueprint so picking a different card while the
                form is open remounts it, and the cursor lands on the field
                that card left empty. */}
            <SecretAddForm
              key={foldingAdd.blueprint?.name ?? 'blank'}
              draft={foldingAdd.draft}
              blueprint={foldingAdd.blueprint}
              saving={foldingAdd.saving}
              onChange={patchDraft}
              onCancel={closeForm}
              onSave={handleCreate}
            />
          </OnFoldGone>
        )}
      </Disclosure>

      {/* Secret list */}
      {secrets.length === 0 && !adding ? (
        <ListEmpty>{emptyText}</ListEmpty>
      ) : (
        <div className="flex flex-col gap-1">
          {secrets.map((secret) => {
            const editingThis = editMode?.name === secret.name;
            const foldingThis = foldingEditByName.get(secret.name) ?? null;
            return (
              <div key={secret.id}>
                {/* Two folds, one row: the editor grows as the resting row
                    collapses, so the list never jumps by a row height. */}
                <Disclosure open={!!editingThis}>
                  {foldingThis && (
                    <OnFoldGone onGone={() => handleEditFoldGone(secret.name)}>
                      <SecretEditForm
                        name={foldingThis.name}
                        draft={foldingThis.draft}
                        saving={foldingThis.saving}
                        onChange={patchDraft}
                        onCancel={closeForm}
                        onSave={handleUpdate}
                      />
                    </OnFoldGone>
                  )}
                </Disclosure>
                <Disclosure open={!editingThis}>
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
                    onCancelDelete={closeForm}
                    onConfirmDelete={handleDelete}
                  />
                </Disclosure>
              </div>
            );
          })}
        </div>
      )}

      {footer}
    </div>
  );
}
