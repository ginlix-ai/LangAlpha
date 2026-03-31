import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { useApiKeys } from './useApiKeys';
import { getCodexOAuthStatus, getClaudeOAuthStatus } from '@/pages/Dashboard/utils/api';

export interface ConfiguredProvider {
  provider: string;
  displayName: string;
  type: 'api_key' | 'oauth';
}

/**
 * Returns a unified list of all configured providers (BYOK keys + OAuth connections).
 * Used by the wizard to show what's already connected and to mark provider cards.
 */
export function useConfiguredProviders() {
  const { apiKeys, isLoading: keysLoading } = useApiKeys();

  const { data: codexStatus, isLoading: codexLoading } = useQuery({
    queryKey: queryKeys.oauth.codex(),
    queryFn: getCodexOAuthStatus,
    staleTime: 60_000,
    retry: false,
  });

  const { data: claudeStatus, isLoading: claudeLoading } = useQuery({
    queryKey: queryKeys.oauth.claude(),
    queryFn: getClaudeOAuthStatus,
    staleTime: 60_000,
    retry: false,
  });

  const providers = useMemo<ConfiguredProvider[]>(() => {
    const result: ConfiguredProvider[] = [];

    // BYOK providers with keys
    if (apiKeys) {
      const keys = apiKeys as Record<string, unknown>;
      if (Array.isArray(keys.providers)) {
        for (const p of keys.providers as Array<{
          provider: string;
          display_name?: string;
          has_key?: boolean;
        }>) {
          if (p.has_key) {
            result.push({
              provider: p.provider,
              displayName: p.display_name ?? p.provider,
              type: 'api_key',
            });
          }
        }
      }
    }

    // OAuth providers
    if (codexStatus?.connected) {
      result.push({
        provider: 'codex-oauth',
        displayName: 'ChatGPT Codex',
        type: 'oauth',
      });
    }
    if (claudeStatus?.connected) {
      result.push({
        provider: 'claude-oauth',
        displayName: 'Claude (OAuth)',
        type: 'oauth',
      });
    }

    return result;
  }, [apiKeys, codexStatus, claudeStatus]);

  /** Set of configured provider keys for fast lookup */
  const configuredSet = useMemo(
    () => new Set(providers.map((p) => p.provider)),
    [providers],
  );

  return {
    providers,
    configuredSet,
    hasAny: providers.length > 0,
    isLoading: keysLoading || codexLoading || claudeLoading,
  };
}
