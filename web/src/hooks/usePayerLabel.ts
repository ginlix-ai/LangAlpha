import { useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import { payerLabel, type Payer } from '@/lib/modelPayer';
import { useProviderCatalog } from './useProviderCatalog';

/** A payer's badge text, a coding plan named as the catalog names it. */
export function usePayerLabel(): (payer: Payer) => string {
  const { t } = useTranslation();
  const { entryFor } = useProviderCatalog();
  return useCallback(
    (payer: Payer) => payerLabel(t, payer, (provider) => entryFor(provider)?.display_name),
    [t, entryFor],
  );
}
