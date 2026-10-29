import { useId, useState } from 'react';
import { Plus } from 'lucide-react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { ModalShell } from '@/components/ui/ModalShell';
import { isPlatformMode } from '@/config/hostMode';
import { useConfiguredProviders, type ConfiguredProvider } from '@/hooks/useConfiguredProviders';
import { useProviderCatalog } from '@/hooks/useProviderCatalog';
import type { ProviderCatalogEntry } from '@/components/model/types';
import { isSubscriptionProvider, subscriptionKey, type SubscriptionProvider } from '@/api/oauth';
import { RemoveAccessDialog } from '@/components/modelAccess/RemoveAccessDialog';
import { planLabel } from '@/components/modelAccess/planLabel';
import { regionTag } from '@/components/modelAccess/optionLabels';
import { StatusLabel, type AccessStatus } from '@/components/modelAccess/parts';
import { ProviderChoices } from '@/components/modelAccess/ProviderChoices';
import { SubscriptionChoices } from '@/components/modelAccess/SubscriptionChoices';
import { KeyProviderList, KeyStepFrame, ProviderTitle } from '@/components/modelAccess/KeyProviderPicker';
import { NO_FILTER, optionsOf, startingOption, type KeyFilter, type KeyOption, type ProviderGroup } from '@/components/modelAccess/providerOptions';
import { AccessRow, Group, KeyRow } from './modelAccess/AccessRows';
import { KeySheet } from './modelAccess/KeySheet';
import { ManageSheet, SubscriptionFlow } from './modelAccess/ManageSheet';

const subscriptionName = (t: (k: string) => string, p: SubscriptionProvider) =>
  t(`modelAccess.${subscriptionKey(p)}.name`);

/** The list a key flow was opened from, which Back returns to: the API keys,
 *  or the subscriptions for a coding plan. */
type KeyOrigin = 'pick-key' | 'pick-subscription';

/** What the sheet is showing. */
type Sheet =
  | { kind: 'pick-subscription' }
  | { kind: 'connect-subscription'; provider: SubscriptionProvider }
  | { kind: 'pick-key' }
  | { kind: 'pick-local' }
  | { kind: 'key'; group: ProviderGroup; option: KeyOption; origin: KeyOrigin }
  | { kind: 'local-key'; entry: ProviderCatalogEntry }
  /** `from` the add-key flow keeps that flow's width for the key it just saved.
   *  `justConnected`: the flow before it made the connection, so its models
   *  start picked. */
  | { kind: 'manage'; provider: string; from: 'list' | 'key-flow'; justConnected?: boolean };

/** The add-key flow is one size from the list to the saved key's models; see KeyStepFrame. */
function isWide(sheet: Sheet): boolean {
  switch (sheet.kind) {
    case 'pick-key':
    case 'key':
      return true;
    case 'manage':
      return sheet.from === 'key-flow';
    default:
      return false;
  }
}

/** The step Back returns to, for the steps that have one. */
function backFrom(sheet: Sheet): Sheet | null {
  switch (sheet.kind) {
    case 'connect-subscription': return { kind: 'pick-subscription' };
    case 'key': return { kind: sheet.origin };
    case 'local-key': return { kind: 'pick-local' };
    default: return null;
  }
}

/**
 * Settings → Model → Model access: every way the account reaches a model
 * (subscriptions, API keys, local servers), each with its live status, and
 * one sheet where each is connected and managed. The connect flows are the
 * ones the setup wizard renders.
 */
export function ModelAccessSection() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const sheetTitleId = useId();
  const { keyGroups, planGroups, localEntries, placeOf, optionFor } = useProviderCatalog();
  const { providers, withdrawn, configuredSet, statusLoading, statusError } = useConfiguredProviders();
  const [sheet, setSheet] = useState<Sheet | null>(null);
  const [removing, setRemoving] = useState<ConfiguredProvider | null>(null);
  // Kept here, not in the list, so Back from a provider returns to the same narrowed list.
  const [keyFilter, setKeyFilter] = useState<KeyFilter>(NO_FILTER);

  const subscriptionRows = providers.filter((p) => p.access_type === 'oauth' || p.access_type === 'coding_plan');
  const keyRows = providers.filter((p) => p.access_type === 'api_key');
  const localRows = providers.filter((p) => p.access_type === 'local');

  // The key form opens on the region a key is saved for, and offers the rest.
  const pickKeyGroup = (origin: KeyOrigin) => (group: ProviderGroup) =>
    setSheet({ kind: 'key', group, option: startingOption(group, configuredSet), origin });
  const manage = (provider: string) => setSheet({ kind: 'manage', provider, from: 'list' });
  const back = (from: Sheet) => () => setSheet(backFrom(from));

  const managed = sheet?.kind === 'manage' ? providers.find((p) => p.provider === sheet.provider) ?? null : null;
  /** How a row and its dialogs name a connection: a coding plan offered in
   *  several regions says which one, since each is its own connection. */
  const nameOf = (p: ConfiguredProvider) => {
    if (isSubscriptionProvider(p.provider)) return subscriptionName(t, p.provider);
    const region = p.access_type === 'coding_plan' ? regionTag(t, placeOf(p.provider)) : null;
    return region ? `${p.display_name} · ${region}` : p.display_name;
  };

  const sheetTitle = (() => {
    if (!sheet) return null;
    const providerTitle = (name: string, markProvider: string) => (
      <ProviderTitle name={name} markProvider={markProvider} title={t('setup.connectTitle', { provider: name })} />
    );
    switch (sheet.kind) {
      case 'pick-subscription': return t('modelAccess.connectSubscription');
      case 'connect-subscription': return t('setup.connectTitle', { provider: subscriptionName(t, sheet.provider) });
      case 'pick-key': return t('modelAccess.addKey');
      case 'pick-local': return t('modelAccess.addLocal');
      case 'key': return providerTitle(sheet.group.name, sheet.group.markProvider);
      case 'local-key': return providerTitle(sheet.entry.display_name, sheet.entry.provider);
      case 'manage': return managed ? nameOf(managed) : null;
    }
  })();

  const subscriptionDetail = (p: ConfiguredProvider) =>
    [p.email || p.account_id, planLabel(p.provider, p.plan_type)].filter(Boolean).join(' · ') || null;

  const rowStatus = (p: ConfiguredProvider): AccessStatus =>
    p.state === 'needs_reauth' ? 'needs_reauth' : 'connected';

  return (
    <div className="flex flex-col gap-4">
      <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('modelAccess.description')}</p>

      <Group title={t('modelAccess.subscriptions')} empty={statusLoading ? '' : statusError ? t('modelAccess.status.unknownLong') : t('modelAccess.noneSubscriptions')}>
        {subscriptionRows.map((p) => p.access_type === 'coding_plan' ? (
          <KeyRow key={p.provider} p={p} place={placeOf(p.provider)} onManage={() => manage(p.provider)} />
        ) : (
          <AccessRow
            key={p.provider}
            provider={p.provider}
            name={nameOf(p)}
            detail={subscriptionDetail(p)}
            status={rowStatus(p)}
            action={
              <Button type="button" variant={p.state === 'needs_reauth' ? 'outline' : 'ghost'} size="sm" onClick={() => manage(p.provider)}>
                {p.state === 'needs_reauth' ? t('modelAccess.reconnect') : t('modelAccess.manage')}
              </Button>
            }
          />
        ))}
      </Group>

      <Group title={t('modelAccess.apiKeys')} empty={t('modelAccess.noneKeys')}>
        {[
          ...keyRows.map((p) => (
            <KeyRow key={p.provider} p={p} place={placeOf(p.provider)} onManage={() => manage(p.provider)} />
          )),
          // Nothing to manage on a key no provider takes any more: only removing it.
          ...withdrawn.map((p) => (
            <AccessRow
              key={p.provider}
              provider={p.provider}
              name={nameOf(p)}
              detail={p.masked_key}
              status="withdrawn"
              action={
                <Button type="button" variant="outline" size="sm" onClick={() => setRemoving(p)}>
                  {t('modelAccess.remove.remove')}
                </Button>
              }
            />
          )),
        ]}
      </Group>

      {!isPlatformMode && (
        <Group title={t('modelAccess.localServers')} empty={t('modelAccess.noneLocal')}>
          {localRows.map((p) => (
            <KeyRow key={p.provider} p={p} onManage={() => manage(p.provider)} />
          ))}
        </Group>
      )}

      <div className="flex flex-wrap gap-2">
        <Button type="button" variant="outline" size="sm" className="gap-1.5" onClick={() => setSheet({ kind: 'pick-subscription' })}>
          <Plus aria-hidden="true" className="h-3.5 w-3.5" />
          {t('modelAccess.connectSubscription')}
        </Button>
        <Button type="button" variant="outline" size="sm" className="gap-1.5" onClick={() => {
          setKeyFilter(NO_FILTER);
          setSheet({ kind: 'pick-key' });
        }}>
          <Plus aria-hidden="true" className="h-3.5 w-3.5" />
          {t('modelAccess.addKey')}
        </Button>
        {!isPlatformMode && (
          <Button type="button" variant="outline" size="sm" className="gap-1.5" onClick={() => setSheet({ kind: 'pick-local' })}>
            <Plus aria-hidden="true" className="h-3.5 w-3.5" />
            {t('modelAccess.addLocal')}
          </Button>
        )}
      </div>

      {sheet && (
        <ModalShell
          labelId={sheetTitleId}
          title={sheetTitle}
          onClose={() => setSheet(null)}
          width={isWide(sheet) ? 'wide' : 'standard'}
          covered={!!removing}
        >
          {sheet.kind === 'pick-subscription' && (
            <SubscriptionChoices
              planGroups={planGroups}
              configured={configuredSet}
              onPickSignIn={(p) => setSheet(configuredSet.has(p) ? { kind: 'manage', provider: p, from: 'list' } : { kind: 'connect-subscription', provider: p })}
              onPickPlan={pickKeyGroup('pick-subscription')}
            />
          )}
          {sheet.kind === 'connect-subscription' && (
            <SubscriptionFlow
              key={sheet.provider}
              provider={sheet.provider}
              onConnected={() => setSheet({ kind: 'manage', provider: sheet.provider, from: 'list', justConnected: true })}
              onCancel={back(sheet)}
            />
          )}
          {sheet.kind === 'pick-local' && (
            <ProviderChoices
              entries={localEntries.map((e) => ({ provider: e.provider, name: e.display_name }))}
              configured={configuredSet}
              onPick={(provider) => {
                const entry = localEntries.find((e) => e.provider === provider);
                if (entry) setSheet({ kind: 'local-key', entry });
              }}
            />
          )}
          {sheet.kind === 'pick-key' && (
            <KeyStepFrame
              footer={(
                <button
                  type="button"
                  onClick={() => navigate('/setup/connect', { state: { method: 'api_key', isCustom: true } })}
                  className="text-xs underline underline-offset-2 hover:opacity-80"
                  style={{ color: 'var(--color-text-secondary)' }}
                >
                  {t('modelAccess.customProvider')}
                </button>
              )}
            >
              <KeyProviderList
                groups={keyGroups}
                filter={keyFilter}
                onFilterChange={setKeyFilter}
                configured={configuredSet}
                onPick={pickKeyGroup('pick-key')}
              />
            </KeyStepFrame>
          )}
          {sheet.kind === 'key' && (
            <KeySheet
              key={sheet.group.group}
              option={sheet.option}
              group={sheet.group}
              configured={providers}
              onSaved={(provider, fresh) => setSheet({ kind: 'manage', provider, from: 'key-flow', justConnected: fresh })}
              onBack={back(sheet)}
            />
          )}
          {sheet.kind === 'local-key' && (
            <KeySheet
              key={sheet.entry.provider}
              option={optionsOf(sheet.entry)[0]}
              configured={providers}
              onSaved={(provider, fresh) => setSheet({ kind: 'manage', provider, from: 'list', justConnected: fresh })}
              onBack={back(sheet)}
            />
          )}
          {sheet.kind === 'manage' && managed && (
            <ManageSheet
              key={managed.provider}
              p={managed}
              option={optionFor(managed.provider)}
              configured={providers}
              onClose={() => setSheet(null)}
              onRemove={() => setRemoving(managed)}
              justConnected={sheet.justConnected === true}
            />
          )}
          {sheet.kind === 'manage' && !managed && <StatusLabel status="working" />}
        </ModalShell>
      )}

      {removing && (
        <RemoveAccessDialog
          provider={removing}
          name={nameOf(removing)}
          stacked={!!sheet}
          onClose={() => setRemoving(null)}
          onRemoved={() => setSheet(null)}
        />
      )}
    </div>
  );
}
