import { useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import { payerLabel, subscriptionPayer, type Payer } from '@/lib/modelPayer';
import { useClaudeSignInLifecycle } from './useClaudeSignInEnd';
import { useProviderCatalog } from './useProviderCatalog';

const CLAUDE_SIGN_IN = subscriptionPayer('claude-oauth');

/** A payer's badge text, a coding plan named as the catalog names it, and the
 *  Claude sign-in marked legacy once the deployment set it an end day, unless
 *  the caller asks for the bare name (`markLegacy: false`). */
export function usePayerLabel(): (payer: Payer, options?: { markLegacy?: boolean }) => string {
  const { t } = useTranslation();
  const { entryFor } = useProviderCatalog();
  const legacy = useClaudeSignInLifecycle() !== 'active';
  return useCallback(
    (payer: Payer, { markLegacy = true }: { markLegacy?: boolean } = {}) => {
      const label = payerLabel(t, payer, (provider) => entryFor(provider)?.display_name);
      return markLegacy && legacy && payer === CLAUDE_SIGN_IN ? t('modelAccess.claude.legacyName', { name: label }) : label;
    },
    [t, entryFor, legacy],
  );
}
