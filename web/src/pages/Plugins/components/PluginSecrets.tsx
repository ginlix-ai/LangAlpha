import { useSearchParams } from 'react-router';
import { useTranslation } from 'react-i18next';
import { ListSkeleton } from '@/components/mcp/McpPrimitives';
import {
  useUserVaultSecrets,
  useUserVaultBlueprints,
  useCreateUserVaultSecret,
  useUpdateUserVaultSecret,
  useDeleteUserVaultSecret,
} from '@/hooks/useUserVault';
import { useSecretReaders } from '@/hooks/useSecretUsage';
import { revealUserVaultSecret } from '@/pages/ChatAgent/utils/api';
import { EDIT_SECRET_PARAM, SECRET_PARAM } from '../utils/secretParam';
import { LoadFailed } from './LoadFailed';
import { SecretsManager } from './secrets/SecretsManager';
import type { SecretDeepLink } from './secrets/useSecretDeepLink';
import { VaultUsageNote } from './secrets/VaultUsageNote';
import { AccountsLayout } from './surface/AccountsLayout';

/**
 * The Plugins → Secrets tab: the account's keys. They back `${vault:NAME}`
 * refs on the account's connectors and reach sandbox code in every workspace.
 */
export function PluginSecrets() {
  const { t } = useTranslation();
  const [searchParams, setSearchParams] = useSearchParams();
  const { data, error: loadError, refetch } = useUserVaultSecrets();
  const { data: blueprintData, isLoading: blueprintsLoading } = useUserVaultBlueprints();
  const readers = useSecretReaders();
  const createMutation = useCreateUserVaultSecret();
  const updateMutation = useUpdateUserVaultSecret();
  const deleteMutation = useDeleteUserVaultSecret();

  const editName = searchParams.get(EDIT_SECRET_PARAM);
  const prefillName = searchParams.get(SECRET_PARAM);
  const deepLink: SecretDeepLink | null = editName
    ? { kind: 'edit', name: editName }
    : prefillName
      ? { kind: 'prefill', name: prefillName }
      : null;
  const consumeDeepLink = () =>
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        next.delete(SECRET_PARAM);
        next.delete(EDIT_SECRET_PARAM);
        return next;
      },
      { replace: true },
    );

  return (
    <AccountsLayout lead={t('plugins.secrets.scopeHint')} aside={<VaultUsageNote />}>
      {/* Nothing in the list is true without it: read as empty, a failed list
          said "0 / 0" and no keys stored to a user who has some, and a link
          opened the add form for a stored key, spent before a Retry could
          land it. So the manager waits for the list. */}
      {data ? (
        <SecretsManager
          secrets={data.secrets}
          maxSecrets={data.secrets.length + data.remaining_slots}
          blueprints={blueprintsLoading ? undefined : (blueprintData?.blueprints ?? [])}
          readers={readers}
          deepLink={deepLink}
          onDeepLinkConsumed={consumeDeepLink}
          onCreate={(body) => createMutation.mutateAsync(body)}
          onUpdate={(name, body) => updateMutation.mutateAsync({ name, body })}
          onDelete={(name) => deleteMutation.mutateAsync(name)}
          onReveal={revealUserVaultSecret}
        />
      ) : loadError ? (
        <LoadFailed what={t('plugins.loadFailed.secrets')} error={loadError} onRetry={() => void refetch()} />
      ) : (
        <ListSkeleton rows={2} />
      )}
    </AccountsLayout>
  );
}
