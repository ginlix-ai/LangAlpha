import { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { Info, Plus, X } from 'lucide-react';
import { HeaderButton, ListEmpty } from '@/components/mcp/McpPrimitives';
import { Disclosure } from '@/components/ui/Disclosure';
import { savedSecretName } from '@/lib/secretNames';
import type { UserVaultBlueprint, UserVaultSecret } from '@/pages/ChatAgent/utils/api';
import { markJustAdded, useJustAdded } from '../../hooks/useJustAdded';
import { SurfaceSection } from '../surface/Surface';
import { BlueprintCards } from './BlueprintCards';
import { KeyFilter, KeyFilterEmpty } from './KeyFilter';
import { SecretAddForm } from './SecretEditor';
import type { SecretDraft } from './secretDraft';
import { SecretListItem } from './SecretListItem';
import { createError, deleteError, isAlreadyGone, updateError } from './secretErrors';
import { useKeyFilter } from './useKeyFilter';
import { useRevealCache } from './useRevealCache';
import { useSecretDeepLink, type SecretDeepLink } from './useSecretDeepLink';
import { addMode, editMode, IDLE, linkMode, useSecretsMode, type SecretsMode } from './useSecretsMode';
import './vault.css';

export interface SecretsManagerProps {
  secrets: readonly UserVaultSecret[];
  maxSecrets: number;
  /** Recommended keys (declared by connectors and plugins, not yet stored).
   *  Undefined while they load, which holds a deep link until they are in. */
  blueprints: readonly UserVaultBlueprint[] | undefined;
  /** The connectors that read each stored key, by key name. Absent while that
   *  is unknown, when no row claims that nothing reads it. */
  readers: ReadonlyMap<string, readonly string[]> | undefined;
  deepLink: SecretDeepLink | null;
  onDeepLinkConsumed: () => void;
  onCreate: (body: { name: string; value: string; description?: string }) => Promise<unknown>;
  onUpdate: (name: string, body: { value?: string; description?: string }) => Promise<unknown>;
  onDelete: (name: string) => Promise<unknown>;
  onReveal: (name: string) => Promise<string>;
}

/**
 * The account's keys (Plugins → Secrets): the keys connectors ask for, then
 * the stored ones, each added, edited, revealed and deleted in place. Mounted
 * once the list has loaded; the four operations are the caller's mutations.
 */
export function SecretsManager({
  secrets,
  maxSecrets,
  blueprints,
  readers,
  deepLink,
  onDeepLinkConsumed,
  onCreate,
  onUpdate,
  onDelete,
  onReveal,
}: SecretsManagerProps) {
  const { t } = useTranslation();
  const [mode, dispatch] = useSecretsMode(secrets);
  const reveal = useRevealCache(onReveal, secrets);
  const filter = useKeyFilter(
    secrets,
    readers,
    mode.kind === 'edit' || mode.kind === 'confirmDelete' ? mode.name : null,
  );
  const isJustAdded = useJustAdded();
  const addFormId = useId();
  const capLineId = useId();

  const open = (next: SecretsMode) => dispatch({ type: 'open', mode: next });
  const close = () => open(IDLE);
  const patch = (draft: Partial<SecretDraft>) => dispatch({ type: 'patch', patch: draft });

  // What a link opens depends on whether NAME is stored and what recommends
  // it, so it waits for the recommended keys.
  const link = useSecretDeepLink({ deepLink, ready: blueprints !== undefined, onConsumed: onDeepLinkConsumed });
  if (link) {
    open(linkMode(link, secrets, blueprints ?? []));
    // A filter that hides the row the link is about would leave its form
    // nowhere to open.
    filter.clear();
  }

  /** "Edit NAME instead": the add form's value carries over, since it was
   *  typed for that name. */
  function switchToEdit(name: string) {
    const stored = secrets.find((s) => s.name === name);
    if (mode.kind !== 'add' || !stored) return;
    filter.clear();
    open(editMode(stored, 'nearest', { value: mode.draft.value, valueVisible: mode.draft.valueVisible }));
  }

  async function handleCreate() {
    if (mode.kind !== 'add') return;
    const { value, description } = mode.draft;
    const name = savedSecretName(mode.draft.name);
    if (!name || !value) return;
    dispatch({ type: 'submit' });
    try {
      await onCreate({ name, value, description: description || undefined });
      markJustAdded('secret', name);
      dispatch({ type: 'saved' });
    } catch (err) {
      dispatch({ type: 'refused', error: createError(err, t, { name, max: maxSecrets }) });
    }
  }

  async function handleUpdate() {
    if (mode.kind !== 'edit') return;
    const { name, draft } = mode;
    dispatch({ type: 'submit' });
    try {
      await onUpdate(name, { ...(draft.value ? { value: draft.value } : {}), description: draft.description });
      dispatch({ type: 'saved' });
      reveal.invalidate(name);
    } catch (err) {
      // Deleted elsewhere since the form opened. The row is still listed until
      // the refetch the refusal started lands, so this does not wait for it.
      dispatch(isAlreadyGone(err) ? { type: 'gone', name } : { type: 'refused', error: updateError(err, t) });
    }
  }

  async function handleDelete() {
    if (mode.kind !== 'confirmDelete') return;
    const { name } = mode;
    dispatch({ type: 'submit' });
    try {
      await onDelete(name);
    } catch (err) {
      // Deleted elsewhere already: what was asked for is true.
      if (!isAlreadyGone(err)) {
        dispatch({ type: 'refused', error: { field: 'form', message: deleteError(err, t) } });
        return;
      }
    }
    dispatch({ type: 'gone', name });
    reveal.invalidate(name);
  }

  const adding = mode.kind === 'add';
  const atCap = secrets.length >= maxSecrets;
  const capMessage = atCap ? t('vault.atCapHint', { max: maxSecrets }) : null;
  const pendingAdd = mode.kind === 'add' ? savedSecretName(mode.draft.name) : '';
  // After a refused save the list still holds the key the server just said is
  // gone, until the refetch lands; it is not a clash with itself.
  const readding = mode.kind === 'add' && mode.reason.kind === 'readd' ? mode.reason.name : null;
  const clash =
    pendingAdd && pendingAdd !== readding && secrets.some((s) => s.name === pendingAdd) ? pendingAdd : null;

  return (
    <>
      {/* Stays put while the form is open: these cells are what tell the user
          which name the server is looking for, and hiding them the moment the
          form needs that name is hiding the answer to the question on screen. */}
      {blueprints && blueprints.length > 0 && (
        <BlueprintCards
          blueprints={blueprints}
          atCap={atCap}
          onSelect={(bp) => open(addMode({ blueprint: bp, landing: 'nearest' }))}
        />
      )}

      <SurfaceSection
        title={t('plugins.secrets.title')}
        note={<span className="surface-count">{`${secrets.length} / ${maxSecrets}`}</span>}
        action={
          // The button is the form's toggle, so it says which way it points:
          // a primary Add while the form is closed, a quiet Cancel while it is
          // open. A primary button that re-opens what is already open reads as
          // the action having failed. At the cap it stays where it is, held,
          // with the reason on the line under the header rather than a hover
          // tooltip a touch screen never shows.
          <HeaderButton
            variant={adding ? 'secondary' : 'primary'}
            icon={adding ? X : Plus}
            aria-expanded={adding}
            aria-controls={addFormId}
            disabled={atCap && !adding}
            aria-describedby={atCap ? capLineId : undefined}
            onClick={() => open(adding ? IDLE : addMode({}))}
          >
            {adding ? t('common.cancel') : t('vault.addSecret')}
          </HeaderButton>
        }
      >
        {atCap && (
          <p id={capLineId} className="vault-cap-line">
            <Info className="vault-cap-icon" aria-hidden />
            <span>
              {capMessage}
              {!!filter.unreadCount && !filter.unreadOnly && (
                <>
                  {' '}
                  {t('vault.cap.unread', { count: filter.unreadCount })}{' '}
                  <button type="button" className="vault-inline-btn" onClick={filter.showUnread}>
                    {t('vault.cap.showUnread', { count: filter.unreadCount })}
                  </button>
                </>
              )}
            </span>
          </p>
        )}

        {/* A fold that closes keeps painting what it last held until it is
            gone, then lets the typed value go with it, so a form renders only
            while its mode is open, here and on each row. */}
        <Disclosure open={adding} id={addFormId}>
          {mode.kind === 'add' && (
            <div className="vault-form">
              <SecretAddForm
                // Keyed by what opened it, so picking a different cell while
                // the form is open remounts it and the cursor lands on the
                // field that cell left empty; an edit taken over the same way,
                // so the form lands on screen from wherever the row was.
                key={
                  mode.blueprint?.name ??
                  (mode.reason.kind === 'new' ? 'blank' : `${mode.reason.kind}:${mode.reason.name}`)
                }
                draft={mode.draft}
                blueprint={mode.blueprint}
                reason={mode.reason}
                saving={mode.saving}
                error={mode.error}
                existingName={clash}
                capMessage={capMessage}
                landing={mode.landing}
                onChange={patch}
                onEditExisting={switchToEdit}
                onCancel={close}
                onSave={handleCreate}
              />
            </div>
          )}
        </Disclosure>

        {filter.offered && <KeyFilter filter={filter} />}

        {secrets.length === 0 && !adding ? (
          <ListEmpty>{t('plugins.secrets.empty')}</ListEmpty>
        ) : filter.visible.length === 0 && filter.filtering ? (
          <KeyFilterEmpty filter={filter} />
        ) : (
          <div className="vault-list">
            {filter.visible.map((secret) => (
              <SecretListItem
                key={secret.user_vault_secret_id}
                secret={secret}
                readers={readers ? (readers.get(secret.name) ?? []) : undefined}
                editing={mode.kind === 'edit' && mode.name === secret.name ? mode : null}
                confirming={mode.kind === 'confirmDelete' && mode.name === secret.name ? mode : null}
                reveal={reveal}
                justAdded={isJustAdded('secret', secret.name)}
                dispatch={dispatch}
                onSave={handleUpdate}
                onDelete={handleDelete}
              />
            ))}
          </div>
        )}
      </SurfaceSection>
    </>
  );
}
