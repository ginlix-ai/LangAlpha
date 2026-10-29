import type { Dispatch } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { ConfirmStrip } from '@/components/mcp/McpPrimitives';
import { Disclosure } from '@/components/ui/Disclosure';
import { readersSentence } from '@/hooks/useSecretUsage';
import type { UserVaultSecret } from '@/pages/ChatAgent/utils/api';
import { SecretEditForm } from './SecretEditor';
import { SecretRow } from './SecretRow';
import type { RevealCache } from './useRevealCache';
import {
  confirmDeleteMode,
  editMode,
  IDLE,
  type ConfirmDeleteMode,
  type EditMode,
  type ModeAction,
} from './useSecretsMode';

/**
 * One stored key: its resting row, the editor that folds open in its place,
 * and the delete confirm under it. Two folds, one row: the editor grows as the
 * resting row collapses, so the list never jumps by a row height.
 */
export function SecretListItem({
  secret,
  readers,
  editing,
  confirming,
  reveal,
  justAdded,
  dispatch,
  onSave,
  onDelete,
}: {
  secret: UserVaultSecret;
  /** The connectors that read this key; undefined while that is unknown. */
  readers: readonly string[] | undefined;
  /** The open form when it is this row's. */
  editing: EditMode | null;
  confirming: ConfirmDeleteMode | null;
  reveal: RevealCache;
  justAdded: boolean;
  dispatch: Dispatch<ModeAction>;
  onSave: () => void;
  onDelete: () => void;
}) {
  const { t } = useTranslation();
  const close = () => dispatch({ type: 'open', mode: IDLE });
  return (
    <div className="vault-list-item">
      <Disclosure open={!!editing}>
        {editing && (
          <div className="vault-form">
            <SecretEditForm
              name={editing.name}
              draft={editing.draft}
              saving={editing.saving}
              error={editing.error}
              readers={readers}
              landing={editing.landing}
              onChange={(patch) => dispatch({ type: 'patch', patch })}
              onCancel={close}
              onSave={onSave}
            />
          </div>
        )}
      </Disclosure>
      <Disclosure open={!editing}>
        <SecretRow
          secret={secret}
          revealedValue={reveal.revealed[secret.name]}
          revealing={reveal.revealing === secret.name}
          readers={readers}
          confirmingDelete={!!confirming}
          justAdded={justAdded}
          onToggleReveal={() => reveal.toggle(secret.name)}
          onEdit={() => dispatch({ type: 'open', mode: editMode(secret, null) })}
          onRequestDelete={() =>
            dispatch({ type: 'open', mode: confirming ? IDLE : confirmDeleteMode(secret.name) })
          }
        />
        {reveal.failure?.name === secret.name && <p className="vault-row-error">{reveal.failure.message}</p>}
      </Disclosure>
      <Disclosure open={!!confirming}>
        {confirming && (
          <div className="vault-confirm">
            <ConfirmStrip
              message={<DeleteConsequence name={secret.name} readers={readers} />}
              confirmLabel={confirming.saving ? t('vault.deleting') : t('vault.delete')}
              cancelLabel={t('common.cancel')}
              pending={confirming.saving}
              onConfirm={onDelete}
              onCancel={close}
            />
            {confirming.error && <p className="vault-row-error">{confirming.error.message}</p>}
          </div>
        )}
      </Disclosure>
    </div>
  );
}

/**
 * What deleting the key breaks, asked before it happens: a connector that
 * reads a missing key stops starting, so the sentence names how many (or
 * which) rather than asking a bare "Delete?". When nothing in the list reads
 * it, code may still, and that is said too.
 */
function DeleteConsequence({ name, readers }: { name: string; readers: readonly string[] | undefined }) {
  const { key, values } = readers
    ? readersSentence(readers, {
        none: 'vault.deleteConfirm.none',
        one: 'vault.deleteConfirm.one',
        many: 'vault.deleteConfirm.many',
      })
    : { key: 'vault.deleteConfirm.unknown', values: {} };
  return (
    <Trans i18nKey={key} values={{ ...values, name }} components={{ code: <code className="vault-inline-name" /> }} />
  );
}
