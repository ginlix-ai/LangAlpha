import { useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { SERVER_KEY, type ConfiguredProvider } from '@/hooks/useConfiguredProviders';
import { isSubscriptionProvider, type SubscriptionProvider } from '@/api/oauth';
import { ClaudeConnect } from '@/components/modelAccess/ClaudeConnect';
import { DeviceConnect } from '@/components/modelAccess/DeviceConnect';
import { ModelPickerList } from '@/components/modelAccess/ModelPicker';
import { useModelPicker } from '@/components/modelAccess/useModelPicker';
import { SubscriptionSummary } from '@/components/modelAccess/SubscriptionSummary';
import { QuietNote } from '@/components/modelAccess/parts';
import type { KeyOption } from '@/components/modelAccess/providerOptions';
import { KeySheet } from './KeySheet';
import { keyDetail } from './keyDetail';

/** The sign-in for a subscription: a code approved on the provider's page, or Claude's paste-back. */
export function SubscriptionFlow({
  provider,
  onConnected,
  onCancel,
}: {
  provider: SubscriptionProvider;
  onConnected: () => void;
  onCancel?: () => void;
}) {
  return provider === 'claude-oauth'
    ? <ClaudeConnect onConnected={onConnected} onCancel={onCancel} />
    : <DeviceConnect provider={provider} onConnected={onConnected} onCancel={onCancel} />;
}

/** A connection's own view: its account or key, then which of its models are on the list. */
export function ManageSheet({
  p,
  option,
  configured,
  onClose,
  onRemove,
  justConnected = false,
}: {
  p: ConfiguredProvider;
  /** The catalog's place for this connection: for a key, the plan and region
   *  it is for, so changing it edits this key and no sibling's. Null for a
   *  provider of the user's own, whose key is not changed here. */
  option: KeyOption | null;
  configured: ConfiguredProvider[];
  onClose: () => void;
  onRemove: () => void;
  /** The sheet before this one made the connection, so its models start picked. */
  justConnected?: boolean;
}) {
  const { t } = useTranslation();
  const modelsHeadingId = useId();
  const subscription = isSubscriptionProvider(p.provider) ? p.provider : null;
  const picker = useModelPicker({ provider: p.provider, modelsFrom: option?.models_from ?? p.brand_key, justConnected });
  const [reconnecting, setReconnecting] = useState(false);
  const [editingKey, setEditingKey] = useState(false);
  const [saved, setSaved] = useState(false);
  const needsReauth = !!subscription && p.state === 'needs_reauth';
  const local = p.access_type === 'local';
  // The deployment's own key has no row of the user's to delete.
  const removable = p.masked_key !== SERVER_KEY;

  if (subscription && (needsReauth || reconnecting)) {
    const done = () => setReconnecting(false);
    return (
      <div className="flex flex-col gap-4">
        {needsReauth && <QuietNote>{t('modelAccess.reconnectNote')}</QuietNote>}
        <SubscriptionFlow provider={subscription} onConnected={done} onCancel={needsReauth ? undefined : done} />
        {needsReauth && (
          <div className="border-t pt-4" style={{ borderColor: 'var(--color-border-muted)' }}>
            <Button type="button" variant="ghost" size="sm" onClick={onRemove} style={{ color: 'var(--color-loss)' }}>
              {t('modelAccess.remove.disconnect')}
            </Button>
          </div>
        )}
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-5">
      {subscription ? (
        <SubscriptionSummary provider={subscription} />
      ) : option && editingKey ? (
        <KeySheet option={option} configured={configured} onSaved={() => setEditingKey(false)} onCancel={() => setEditingKey(false)} />
      ) : (
        <div className="flex items-center justify-between gap-3 rounded-lg p-4" style={{ background: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)' }}>
          <div className="flex min-w-0 flex-col gap-0.5">
            <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {local ? t('setup.baseUrlLabel') : t('modelAccess.apiKey.savedKey')}
            </span>
            <span className="truncate font-mono text-sm" style={{ color: 'var(--color-text-primary)' }}>
              {keyDetail(p, t)}
            </span>
            {!local && p.base_url && (
              <span className="truncate font-mono text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                {p.base_url}
              </span>
            )}
          </div>
          {option && (
            <Button type="button" variant="outline" size="sm" onClick={() => setEditingKey(true)}>
              {local ? t('modelAccess.apiKey.edit') : t('modelAccess.apiKey.change')}
            </Button>
          )}
        </div>
      )}

      <section className="flex flex-col gap-2">
        <h3 id={modelsHeadingId} className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {t('modelAccess.models.title')}
        </h3>
        <ModelPickerList picker={picker} labelledBy={modelsHeadingId} />
      </section>

      <div className="flex flex-wrap items-center gap-2 border-t pt-4" style={{ borderColor: 'var(--color-border-muted)' }}>
        {removable && (
          <Button type="button" variant="ghost" size="sm" onClick={onRemove} style={{ color: 'var(--color-loss)' }}>
            {subscription ? t('modelAccess.remove.disconnect') : t('modelAccess.remove.remove')}
          </Button>
        )}
        {subscription && (
          <Button type="button" variant="ghost" size="sm" onClick={() => setReconnecting(true)}>
            {t('modelAccess.reconnect')}
          </Button>
        )}
        <span className="ml-auto text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          {saved && !picker.saving ? t('common.saved') : null}
        </span>
        <Button
          type="button"
          disabled={picker.loading || picker.saving}
          aria-busy={picker.saving}
          onClick={async () => {
            setSaved(false);
            if (await picker.save()) {
              setSaved(true);
              onClose();
            }
          }}
        >
          {picker.saving ? t('setup.saving') : t('modelAccess.models.save')}
        </Button>
      </div>
    </div>
  );
}
