import { useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { ChevronDown, CircleCheck } from 'lucide-react';
import { Disclosure } from '@/components/ui/Disclosure';
import { cn } from '@/lib/utils';
import type { SetupKey, SetupStatus } from '@/api/setup';
import { useSearchPreference } from '@/hooks/useSearchPreference';
import { SearchProviderSelect } from '@/pages/Settings/components/SearchProviderSelect';
import type { ServiceKeysDraft } from '../hooks/useSetupDrafts';
import { keyFieldKey, vendorName } from '../utils/vendors';
import { KeyField } from './KeyField';

interface Props {
  status: SetupStatus;
  draft: ServiceKeysDraft;
}

const keysIn = (status: SetupStatus, group: SetupKey['group']) => status.keys.filter((key) => key.group === group);

/** Yahoo Finance, which needs nothing, then the optional key that adds to it. */
export function MarketDataKeys({ status, draft }: Props) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col gap-4">
      <div className="flex items-start gap-2.5">
        <CircleCheck aria-hidden="true" className="mt-0.5 h-4 w-4 shrink-0 text-(--color-success)" />
        <div className="min-w-0">
          <p className="text-sm font-medium text-(--color-text-primary)">{t('setup.services.yahooTitle')}</p>
          <p className="mt-0.5 text-xs leading-snug text-(--color-text-tertiary)">{t('setup.services.yahooDesc')}</p>
        </div>
      </div>
      {keysIn(status, 'data').map((key) => (
        <KeyField
          key={keyFieldKey(key)}
          setupKey={key}
          description={t('setup.services.fmpDesc')}
          draft={draft.drafts[key.name] ?? ''}
          onDraft={(value) => draft.setDraft(key.name, value)}
        />
      ))}
    </div>
  );
}

const hasKey = (key: SetupKey | undefined) => key !== undefined && key.state !== 'unset';

/** One provider at a time: it is the agent's single search engine, so there is
 *  no call for a key from each. The provider is the user's own preference,
 *  the one the Agent section sets, saved as it is picked; the key under it is
 *  the server's, saved with the form. */
export function WebSearchKeys({ status, draft }: Props) {
  const { t } = useTranslation();
  const selectId = useId();
  const keys = keysIn(status, 'search');
  const { provider, catalog } = useSearchPreference();
  const chosen = keys.find((key) => key.provider === provider);
  const chosenName = provider ? (catalog?.[provider]?.display_name ?? vendorName(provider, t)) : '';
  const typed = chosen ? Boolean(draft.drafts[chosen.name]?.trim()) : false;
  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-col gap-1.5">
        <label htmlFor={selectId} className="text-sm font-medium text-(--color-text-primary)">
          {t('setup.services.searchProvider')}
        </label>
        <SearchProviderSelect
          id={selectId}
          defaultLabel={t('setup.services.searchNone')}
          optionLabel={(id, name) => hasKey(keys.find((key) => key.provider === id))
            ? t('setup.services.searchProviderHasKey', { provider: name })
            : name}
        />
      </div>
      {chosen && (
        <KeyField
          key={keyFieldKey(chosen)}
          setupKey={chosen}
          label={t('setup.services.searchKey', { provider: chosenName })}
          draft={draft.drafts[chosen.name] ?? ''}
          onDraft={(value) => draft.setDraft(chosen.name, value)}
        />
      )}
      {!typed && !keys.some(hasKey) && (
        <p className="text-xs leading-snug text-(--color-text-tertiary)">{t('setup.services.searchOff')}</p>
      )}
    </div>
  );
}

/** Optional on top of the reader that needs no key, so folded until it is in use. */
export function PageReadingKeys({ status, draft }: Props) {
  const { t } = useTranslation();
  const keys = keysIn(status, 'fetch');
  const panelId = useId();
  const [open, setOpen] = useState(() => keys.some((key) => key.state !== 'unset'));
  if (keys.length === 0) return null;
  return (
    <div>
      <button
        type="button"
        aria-expanded={open}
        aria-controls={panelId}
        onClick={() => setOpen((v) => !v)}
        className="inline-flex items-center gap-1.5 text-sm text-(--color-text-secondary) hover:text-(--color-text-primary)"
      >
        {t('setup.services.fetchToggle')}
        <ChevronDown aria-hidden="true" className={cn('h-3.5 w-3.5 transition-transform', open && 'rotate-180')} />
      </button>
      <Disclosure open={open} id={panelId}>
        <div className="flex flex-col gap-3 pt-3">
          <p className="text-xs leading-snug text-(--color-text-tertiary)">{t('setup.services.fetchDesc')}</p>
          {keys.map((key) => (
            <KeyField
              key={keyFieldKey(key)}
              setupKey={key}
              draft={draft.drafts[key.name] ?? ''}
              onDraft={(value) => draft.setDraft(key.name, value)}
            />
          ))}
        </div>
      </Disclosure>
    </div>
  );
}
