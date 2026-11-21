import { useId, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import type { SetupStatus } from '@/api/setup';
import { Button } from '@/components/ui/button';
import { WizardSection } from '@/components/ui/wizard';
import { SaveStatus } from '@/pages/Settings/components/SaveStatus';
import { SettingsCardBody, SettingsGroup } from '@/pages/Settings/components/SettingsLayout';
import { SETTINGS } from '@/pages/Settings/sections';
import { StepError } from '../answers/parts';
import {
  saveInOrder,
  useCompleteServerSetup,
  useSandboxDraft,
  useSaveErrorCopy,
  useServiceKeysDraft,
} from '../hooks/useSetupDrafts';
import { SandboxChoice } from './SandboxChoice';
import { MarketDataKeys, PageReadingKeys, WebSearchKeys } from './ServiceKeys';
import { SetupStatusGate } from './SetupStatusGate';

/** What the step hands its footer: its writes in order, and whether they can run. */
export interface ServerStepContinue {
  run: () => Promise<boolean>;
  saving: boolean;
  ready: boolean;
}

interface ServerSetupFormProps {
  /** The setup step, saved by one Continue, or Settings, saved a group at a time. */
  layout: 'step' | 'panel';
  /** Whether where agent code runs is asked; the step asks it only of an
   *  answer that runs tasks on a computer the server runs. */
  choosesComputer?: boolean;
  /** The step's way on, under the form. */
  footer?: (step: ServerStepContinue) => ReactNode;
  /** Beside a status that could not be read, for the way past it. */
  onFailed?: ReactNode;
}

/**
 * Where the server runs agent code and the vendor keys its tools use: what the
 * setup step asks once, and Settings shows to change later. Drawn once the
 * status is in, so its drafts start from it.
 */
export function ServerSetupForm({ onFailed, ...props }: ServerSetupFormProps) {
  return (
    <SetupStatusGate onFailed={onFailed}>
      {(status) => <Form status={status} {...props} />}
    </SetupStatusGate>
  );
}

/** The step's sections sit in a box of the rail's own. */
const STEP_BOX =
  'rounded-[var(--cs-radius-box,0.5rem)] border border-[var(--color-shell-edge,var(--color-border-muted))] ' +
  'bg-[var(--color-shell-fill,var(--color-bg-card))] p-4';

function Form({ status, layout, choosesComputer = true, footer }: Omit<ServerSetupFormProps, 'onFailed'> & { status: SetupStatus }) {
  const { t } = useTranslation();
  const computerId = useId();
  const marketId = useId();
  const searchId = useId();
  const sandbox = useSandboxDraft(status);
  const keys = useServiceKeysDraft();
  const complete = useCompleteServerSetup();
  const saveCopy = useSaveErrorCopy();
  // A provider set on the server is the operator's, not something to pick here.
  const fixed = status.sandbox.provider_source === 'environment';

  const computer = <SandboxChoice status={status} draft={sandbox} labelledBy={computerId} />;
  const market = <MarketDataKeys status={status} draft={keys} />;
  const pageReading = <PageReadingKeys status={status} draft={keys} />;
  const search = <WebSearchKeys status={status} draft={keys} />;

  if (layout === 'panel') {
    // Both key groups save the one draft, and each says so where it is edited.
    const saveKeys = (
      <>
        <SaveError error={keys.error} />
        <SaveRow
          onSave={() => void keys.save()}
          disabled={!keys.dirty || keys.saving}
          status={saveState(keys.saving, keys.error != null, keys.saved)}
        />
      </>
    );
    return (
      <>
        <SettingsGroup title={t(SETTINGS.agentLocation.labelKey)} description={t(SETTINGS.agentLocation.descKey)}>
          <SettingsCardBody id={SETTINGS.agentLocation.anchor}>
            <span id={computerId} className="sr-only">{t(SETTINGS.agentLocation.labelKey)}</span>
            {computer}
            <SaveError error={sandbox.error} />
            {!fixed && (
              <SaveRow
                onSave={() => void sandbox.save()}
                disabled={!sandbox.ready || !sandbox.dirty || sandbox.saving}
                status={saveState(sandbox.saving, sandbox.error != null, sandbox.saved)}
              />
            )}
          </SettingsCardBody>
        </SettingsGroup>
        <SettingsGroup title={t(SETTINGS.marketDataKeys.labelKey)} description={t(SETTINGS.marketDataKeys.descKey)}>
          <SettingsCardBody id={SETTINGS.marketDataKeys.anchor}>
            {market}
            {saveKeys}
          </SettingsCardBody>
        </SettingsGroup>
        <SettingsGroup title={t(SETTINGS.webSearchKeys.labelKey)} description={t(SETTINGS.webSearchKeys.descKey)}>
          <SettingsCardBody id={SETTINGS.webSearchKeys.anchor}>
            {search}
            <div className="mt-4">{pageReading}</div>
            <p className="mt-4 text-xs leading-snug text-(--color-text-tertiary)">{t('settings.server.keysReach')}</p>
            {saveKeys}
          </SettingsCardBody>
        </SettingsGroup>
      </>
    );
  }

  const picks = choosesComputer && !fixed;
  // Continue's writes, in the order the server takes them, ending at the first
  // that fails: where code runs (its cloud key first), the vendor keys, then
  // the mark that the step was answered, which no account is sent back past.
  const run = () => saveInOrder([...(picks ? [sandbox.save] : []), keys.save, complete.save]);
  const error = (picks ? sandbox.error : null) ?? keys.error ?? complete.error;

  return (
    <>
      {choosesComputer && (
        <WizardSection className="mt-0" title={t('setup.computer.title')} titleId={computerId}>
          {computer}
        </WizardSection>
      )}
      <WizardSection className={choosesComputer ? undefined : 'mt-0'} title={t('setup.services.marketData')} titleId={marketId}>
        <div className={STEP_BOX}>{market}</div>
      </WizardSection>
      <WizardSection title={t('setup.services.webSearch')} titleId={searchId} hint={t('setup.services.optional')}>
        <div className={STEP_BOX}>{search}</div>
        <div className="mt-4">{pageReading}</div>
      </WizardSection>
      {error != null && <StepError>{saveCopy(error)}</StepError>}
      {footer?.({
        run,
        saving: sandbox.saving || keys.saving || complete.saving,
        ready: !picks || sandbox.ready,
      })}
    </>
  );
}

function SaveError({ error }: { error: unknown }) {
  const saveCopy = useSaveErrorCopy();
  if (error == null) return null;
  return (
    <p role="alert" className="mt-3 text-sm text-(--color-loss)">
      {saveCopy(error)}
    </p>
  );
}

function saveState(saving: boolean, failed: boolean, saved: boolean) {
  if (saving) return 'saving';
  if (failed) return 'error';
  return saved ? 'saved' : 'idle';
}

function SaveRow({ onSave, disabled, status }: { onSave: () => void; disabled: boolean; status: 'idle' | 'saving' | 'saved' | 'error' }) {
  const { t } = useTranslation();
  return (
    <div className="mt-4 flex items-center justify-end gap-3">
      <SaveStatus status={status} />
      <Button type="button" size="sm" disabled={disabled} onClick={onSave}>
        {t('common.save')}
      </Button>
    </div>
  );
}
