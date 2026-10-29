import { useReducer } from 'react';
import type { UserVaultBlueprint, UserVaultSecret } from '@/pages/ChatAgent/utils/api';
import { EMPTY_DRAFT, type FormError, type Landing, type SecretDraft } from './secretDraft';
import type { SecretDeepLink } from './useSecretDeepLink';

/** Why the add form is open, which it says first when it is not a plain add. */
export type AddReason =
  | { kind: 'new' }
  /** It took over an edit whose key was deleted elsewhere; saving adds it again. */
  | { kind: 'readd'; name: string }
  /** An edit link (`?edit=NAME`) named a key that is not stored; saving adds it. */
  | { kind: 'notStored'; name: string };

interface Submission {
  saving: boolean;
  error: FormError | null;
}

/**
 * At most one editing surface is open at a time, and its in-flight flag and
 * refusal live with it: an `add` cannot be mid-save while an `edit` is also
 * armed, and a refusal cannot outlive the form it was about. Reveal is
 * deliberately outside: it is per-row display state that coexists with any
 * mode.
 */
export type SecretsMode =
  | { kind: 'idle' }
  | (Submission & {
      kind: 'add';
      draft: SecretDraft;
      blueprint: UserVaultBlueprint | null;
      reason: AddReason;
      landing: Landing | null;
    })
  | (Submission & { kind: 'edit'; name: string; draft: SecretDraft; landing: Landing | null })
  | (Submission & { kind: 'confirmDelete'; name: string });

export type EditMode = Extract<SecretsMode, { kind: 'edit' }>;
export type ConfirmDeleteMode = Extract<SecretsMode, { kind: 'confirmDelete' }>;

export type ModeAction =
  | { type: 'open'; mode: SecretsMode }
  | { type: 'patch'; patch: Partial<SecretDraft> }
  | { type: 'submit' }
  | { type: 'saved' }
  | { type: 'refused'; error: FormError }
  /** The key left the vault: settles whatever was open on it. */
  | { type: 'gone'; name: string };

export const IDLE: SecretsMode = { kind: 'idle' };

const NEW: AddReason = { kind: 'new' };

/** The add form, for NAME when one is given, prefilled from what recommends it. */
export function addMode({
  name,
  blueprint = null,
  landing = null,
  reason = NEW,
}: {
  name?: string;
  blueprint?: UserVaultBlueprint | null;
  landing?: Landing | null;
  reason?: AddReason;
}): SecretsMode {
  return {
    kind: 'add',
    draft: { ...EMPTY_DRAFT, name: name ?? blueprint?.name ?? '', description: blueprint?.description ?? '' },
    blueprint,
    reason,
    landing,
    saving: false,
    error: null,
  };
}

export function editMode(
  secret: Pick<UserVaultSecret, 'name' | 'description'>,
  landing: Landing | null,
  carry?: Partial<SecretDraft>,
): SecretsMode {
  return {
    kind: 'edit',
    name: secret.name,
    draft: { ...EMPTY_DRAFT, description: secret.description, ...carry },
    landing,
    saving: false,
    error: null,
  };
}

export function confirmDeleteMode(name: string): SecretsMode {
  return { kind: 'confirmDelete', name, saving: false, error: null };
}

/**
 * What a link opens: NAME's own edit form when it is stored, otherwise the add
 * form for it. An edit link that finds nothing to edit says why, since the
 * user asked to change a key, not to add one.
 */
export function linkMode(
  link: SecretDeepLink,
  secrets: readonly UserVaultSecret[],
  blueprints: readonly UserVaultBlueprint[],
): SecretsMode {
  const stored = secrets.find((s) => s.name === link.name);
  if (stored) return editMode(stored, 'center');
  return addMode({
    name: link.name,
    blueprint: blueprints.find((bp) => bp.name === link.name) ?? null,
    landing: 'center',
    reason: link.kind === 'edit' ? { kind: 'notStored', name: link.name } : NEW,
  });
}

function reduce(mode: SecretsMode, action: ModeAction): SecretsMode {
  switch (action.type) {
    case 'open':
      return action.mode;
    case 'patch': {
      if (mode.kind !== 'add' && mode.kind !== 'edit') return mode;
      // An edit to the field a refusal was about answers it; toggling the
      // value's visibility does not.
      const { patch } = action;
      const answers = 'name' in patch || 'value' in patch || 'description' in patch;
      return { ...mode, draft: { ...mode.draft, ...patch }, error: answers ? null : mode.error };
    }
    case 'submit':
      return mode.kind === 'idle' ? mode : { ...mode, saving: true, error: null };
    // Only the form in flight is settled: one opened while the save was on the
    // wire is not the one it answers.
    case 'saved':
      return mode.kind !== 'idle' && mode.saving ? IDLE : mode;
    case 'refused':
      return mode.kind !== 'idle' && mode.saving ? { ...mode, saving: false, error: action.error } : mode;
    case 'gone':
      // An edit can only be saved now by adding the key again, so it becomes
      // the add form for that name and keeps what was typed, rather than
      // folding away with the row. A confirm closes: what it asked for is done.
      if (mode.kind === 'edit' && mode.name === action.name) {
        return {
          kind: 'add',
          draft: { ...mode.draft, name: mode.name },
          blueprint: null,
          reason: { kind: 'readd', name: mode.name },
          landing: 'center',
          saving: false,
          error: null,
        };
      }
      return mode.kind === 'confirmDelete' && mode.name === action.name ? IDLE : mode;
  }
}

/**
 * The open form, settled against the list in render: a key deleted elsewhere
 * leaves `secrets` on the next read (a refetch on focus, or the one a refused
 * save starts), and what was open on it is settled before a frame paints its
 * row gone.
 */
export function useSecretsMode(secrets: readonly UserVaultSecret[]) {
  const [mode, dispatch] = useReducer(reduce, IDLE);
  if ((mode.kind === 'edit' || mode.kind === 'confirmDelete') && !secrets.some((s) => s.name === mode.name)) {
    dispatch({ type: 'gone', name: mode.name });
  }
  return [mode, dispatch] as const;
}
