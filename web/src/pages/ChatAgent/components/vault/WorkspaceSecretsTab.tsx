import { useTranslation } from 'react-i18next';
import {
  useCreateWorkspaceVaultSecret,
  useDeleteWorkspaceVaultSecret,
  useUpdateWorkspaceVaultSecret,
  useVaultBlueprints,
  useWorkspaceVaultSecrets,
} from '@/hooks/useWorkspaceVault';
import { formatApiErrorDetail, revealVaultSecret } from '../../utils/api';
import { SecretsManager } from './SecretsManager';

/**
 * The workspace port of {@link SecretsManager}: the sandbox settings → Vault
 * tab. Blueprints are secondary — when that query fails the Recommended section
 * simply disappears and the cap falls back to the static limit, so a broken
 * recommendation feed never blocks the secrets list.
 */

const MAX_SECRETS = 20;

interface WorkspaceSecretsTabProps {
  workspaceId: string;
  /** When set (e.g. via an MCP "Set up NAME" deep-link), opens the add form prefilled. */
  prefillSecretName?: string | null;
  onPrefillConsumed?: () => void;
}

export function WorkspaceSecretsTab({
  workspaceId,
  prefillSecretName,
  onPrefillConsumed,
}: WorkspaceSecretsTabProps) {
  const { t } = useTranslation();
  const secretsQuery = useWorkspaceVaultSecrets(workspaceId);
  const blueprintsQuery = useVaultBlueprints(workspaceId);
  const createSecret = useCreateWorkspaceVaultSecret(workspaceId);
  const updateSecret = useUpdateWorkspaceVaultSecret(workspaceId);
  const deleteSecret = useDeleteWorkspaceVaultSecret(workspaceId);

  const secrets = secretsQuery.data ?? [];
  const blueprints = blueprintsQuery.data?.blueprints ?? [];
  const remainingSlots = blueprintsQuery.data?.remaining_slots ?? MAX_SECRETS - secrets.length;

  return (
    <SecretsManager
      // Remount on workspace switch: a half-filled add form from the previous
      // workspace must not carry over and silently target the new one.
      key={workspaceId}
      title={t('vault.workspace.title')}
      secrets={secrets.map((s) => ({
        id: s.workspace_vault_secret_id,
        name: s.name,
        description: s.description,
        masked_value: s.masked_value,
      }))}
      maxSecrets={secrets.length + remainingSlots}
      loading={secretsQuery.isLoading || blueprintsQuery.isLoading}
      loadError={secretsQuery.error ? formatApiErrorDetail(secretsQuery.error) : null}
      emptyText={t('vault.workspace.empty')}
      blueprints={blueprints}
      prefillSecretName={prefillSecretName}
      onPrefillConsumed={onPrefillConsumed}
      onCreate={(body) => createSecret.mutateAsync(body)}
      onUpdate={(name, body) => updateSecret.mutateAsync({ name, body })}
      onDelete={(name) => deleteSecret.mutateAsync(name)}
      onReveal={(name) => revealVaultSecret(workspaceId, name)}
      footer={<VaultInfoFooter />}
    />
  );
}

/** Usage + security explainer under the workspace vault list. */
function VaultInfoFooter() {
  const { t } = useTranslation();
  const code = (text: string) => (
    <code className="font-mono" style={{ color: 'var(--color-text-secondary)' }}>{text}</code>
  );
  return (
    <div
      className="flex flex-col gap-2.5 text-xs p-3 rounded-lg mt-1"
      style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-tertiary)' }}
    >
      <div>
        <span className="font-medium" style={{ color: 'var(--color-text-secondary)' }}>{t('vault.workspace.usageTitle')}</span>
        <div className="mt-1">
          {t('vault.workspace.usageAccess')} {code('from vault import get; key = get("SECRET_NAME")')}
        </div>
      </div>
      <div
        className="pt-2 flex flex-col gap-1.5"
        style={{ borderTop: '1px solid var(--color-border-muted)' }}
      >
        <span className="font-medium" style={{ color: 'var(--color-text-secondary)' }}>{t('vault.workspace.securityTitle')}</span>
        <ul className="flex flex-col gap-1 pl-3" style={{ listStyleType: 'disc' }}>
          <li>{t('vault.workspace.security1')}</li>
          <li>{t('vault.workspace.security2a')} {code('vault.get()')} {t('vault.workspace.security2b')}</li>
          <li>{t('vault.workspace.security3')}</li>
          <li>{t('vault.workspace.security4a')} {code('vault')} {t('vault.workspace.security4b')}</li>
        </ul>
      </div>
    </div>
  );
}
