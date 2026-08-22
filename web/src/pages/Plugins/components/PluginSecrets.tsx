import { useTranslation } from 'react-i18next';
import {
  useUserVaultSecrets,
  useUserVaultBlueprints,
  useCreateUserVaultSecret,
  useUpdateUserVaultSecret,
  useDeleteUserVaultSecret,
} from '@/hooks/useUserVault';
import { SecretsManager } from '@/pages/ChatAgent/components/vault/SecretsManager';
import { revealUserVaultSecret } from '@/pages/ChatAgent/utils/api';

/**
 * The Plugins → Secrets tab: user-level vault CRUD. These secrets back
 * `${vault:NAME}` refs on inherited (user-level) MCP servers and are merged
 * into every sandbox push — a same-named workspace secret wins, so a
 * workspace can always override a user default.
 */

export function PluginSecrets() {
  const { t } = useTranslation();
  const { data, isLoading, error: loadError } = useUserVaultSecrets();
  const { data: blueprintData } = useUserVaultBlueprints();
  const createMutation = useCreateUserVaultSecret();
  const updateMutation = useUpdateUserVaultSecret();
  const deleteMutation = useDeleteUserVaultSecret();

  const secrets = data?.secrets ?? [];
  const maxSecrets = secrets.length + (data?.remaining_slots ?? 0);

  return (
    <SecretsManager
      title={t('plugins.secrets.title')}
      blueprints={blueprintData?.blueprints ?? []}
      secrets={secrets.map((s) => ({
        id: s.user_vault_secret_id,
        name: s.name,
        description: s.description,
        masked_value: s.masked_value,
      }))}
      maxSecrets={maxSecrets}
      loading={isLoading}
      loadError={loadError ? (loadError as { message?: string })?.message || t('vault.loadFailed') : null}
      hint={t('plugins.secrets.scopeHint')}
      emptyText={t('plugins.secrets.empty')}
      onCreate={(body) => createMutation.mutateAsync(body)}
      onUpdate={(name, body) => updateMutation.mutateAsync({ name, body })}
      onDelete={(name) => deleteMutation.mutateAsync(name)}
      onReveal={(name) => revealUserVaultSecret(name)}
    />
  );
}
