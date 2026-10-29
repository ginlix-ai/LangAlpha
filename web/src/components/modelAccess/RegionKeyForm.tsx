import { useState, type ReactNode } from 'react';
import type { ConfiguredProvider } from '@/hooks/useConfiguredProviders';
import { ApiKeyFormFields, useApiKeyForm, type ApiKeyFormState } from './ApiKeyForm';
import { RegionChoice } from './KeyProviderPicker';
import type { KeyOption, ProviderGroup } from './providerOptions';

interface RegionKeyFormProps {
  /** The region the form starts on, or the only place the key goes. */
  start: KeyOption;
  /** The provider whose regions are offered above the form. */
  group?: ProviderGroup;
  configured: readonly ConfiguredProvider[];
  /** Lays out the region choice and key fields (`fields`) with the caller's
   *  own heading and buttons, which act on `form`. */
  children: (props: { form: ApiKeyFormState; fields: ReactNode; option: KeyOption }) => ReactNode;
}

/** A key form with the provider's regions above it. A region is its own key
 *  and base URL, so the form starts over on each. */
export function RegionKeyForm({ start, ...props }: RegionKeyFormProps) {
  const [option, setOption] = useState(start);
  return <KeyFields key={option.provider} {...props} option={option} onRegion={setOption} />;
}

function KeyFields({
  option,
  group,
  configured,
  onRegion,
  children,
}: Omit<RegionKeyFormProps, 'start'> & { option: KeyOption; onRegion: (option: KeyOption) => void }) {
  const form = useApiKeyForm(option, configured);
  const fields = (
    <>
      {group && <RegionChoice group={group} option={option} onChange={onRegion} />}
      <ApiKeyFormFields form={form} />
    </>
  );
  return children({ form, fields, option });
}
