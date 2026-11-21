/**
 * The web search provider the agent uses, a preference of the user's. One
 * control for the Agent section and the server's own setup, so both read the
 * stored value against the catalog the same way (`readSearchPreference`) and a
 * pick clears a depth the new provider lacks (`searchProviderPatch`).
 */
import { useTranslation } from 'react-i18next';
import { Select } from '@/components/ui/select';
import { useSearchPreference } from '@/hooks/useSearchPreference';
import { searchProviderPatch } from '@/lib/searchPreferences';
import { usePreferenceWrite } from '../panels/usePreferenceWrite';

export function SearchProviderSelect({ id, defaultLabel, optionLabel }: {
  id: string;
  /** What the empty choice, the deployment's own default, reads as. */
  defaultLabel?: string;
  /** What a provider's option reads as; its catalog name otherwise. */
  optionLabel?: (provider: string, name: string) => string;
}) {
  const { t } = useTranslation();
  const write = usePreferenceWrite();
  const { provider, catalog, providers, tierAllows } = useSearchPreference();
  return (
    <Select
      id={id}
      value={provider}
      onChange={(e) => { void write(searchProviderPatch(e.target.value, catalog)); }}
      disabled={!providers.some(([, p]) => tierAllows(p.min_tier))}
    >
      <option value="">{defaultLabel ?? t('settings.searchProviderDefault')}</option>
      {providers.map(([value, p]) => (
        <option key={value} value={value} disabled={!tierAllows(p.min_tier)}>
          {optionLabel ? optionLabel(value, p.display_name) : p.display_name}
        </option>
      ))}
    </Select>
  );
}
