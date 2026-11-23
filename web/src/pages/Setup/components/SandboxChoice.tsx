import { useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Box, Cloud, CircleCheck, ExternalLink, Laptop, TriangleAlert } from 'lucide-react';
import type { InstallGroup, SetupStatus } from '@/api/setup';
import { Button } from '@/components/ui/button';
import { Disclosure } from '@/components/ui/Disclosure';
import { Loader } from '@/components/ui/loader';
import { SegmentedControl } from '@/components/ui/segmented-control';
import { ToggleSwitch } from '@/components/ui/switch';
import { useLocale } from '@/hooks/useLocale';
import { useSetupStatus } from '@/hooks/useServerSetup';
import { formatBytes } from '@/lib/format';
import { cn } from '@/lib/utils';
import { providerLabel } from '@/pages/ChatAgent/components/providerUi';
import { AnswerOption, AnswerTile } from '../answers/parts';
import type { SandboxDraft } from '../hooks/useSetupDrafts';
import { CLOUD_KINDS, cardAvailable, optionOf, selectedGroupsSize, shownCards, type CloudKind, type SandboxCard } from '../utils/sandboxChoice';
import { DOCKER_INSTALL_URL, keyFieldKey, vendorName } from '../utils/vendors';
import { KeyField } from './KeyField';

const GROUP_COPY: Readonly<Record<string, { labelKey: string; descKey: string }>> = {
  core: { labelKey: 'setup:computer.groupCore', descKey: 'setup:computer.groupCoreDesc' },
  browsing: { labelKey: 'setup:computer.groupBrowsing', descKey: 'setup:computer.groupBrowsingDesc' },
  documents: { labelKey: 'setup:computer.groupDocuments', descKey: 'setup:computer.groupDocumentsDesc' },
  chromium: { labelKey: 'setup:computer.groupChromium', descKey: 'setup:computer.groupChromiumDesc' },
};

function InstallGroups({ groups, onToggle }: { groups: readonly InstallGroup[]; onToggle: (id: string) => void }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const size = (mb: number) => formatBytes(mb * 1024 * 1024, locale);
  const total = selectedGroupsSize(groups);
  return (
    <div className="flex flex-col gap-3">
      <div>
        <p className="text-[0.8125rem] font-medium text-(--color-text-primary)">{t('setup:computer.installTitle')}</p>
        <p className="mt-0.5 text-xs leading-snug text-(--color-text-tertiary)">{t('setup:computer.installHelp')}</p>
      </div>
      <ul className="flex flex-col gap-3">
        {groups.map((group) => {
          const copy = Object.hasOwn(GROUP_COPY, group.id) ? GROUP_COPY[group.id] : null;
          const name = copy ? t(copy.labelKey) : group.id;
          return (
            <li key={group.id} className="flex items-start justify-between gap-4">
              <span className="min-w-0">
                <span className="block text-[0.8125rem] text-(--color-text-primary)">
                  {name}
                  {group.required && (
                    <span className="ml-2 text-xs text-(--color-text-tertiary)">{t('setup:computer.required')}</span>
                  )}
                </span>
                {copy && (
                  <span className="mt-0.5 block text-xs leading-snug text-(--color-text-tertiary)">{t(copy.descKey)}</span>
                )}
                {group.download_mb != null && group.disk_mb != null && (
                  <span className="mt-0.5 block text-xs tabular-nums text-(--color-text-tertiary)">
                    {t('setup:computer.groupSize', { download: size(group.download_mb), disk: size(group.disk_mb) })}
                  </span>
                )}
              </span>
              <ToggleSwitch
                checked={group.selected}
                disabled={group.required}
                onChange={() => onToggle(group.id)}
                ariaLabel={name}
                className={group.required ? 'mt-0.5 cursor-not-allowed opacity-60' : 'mt-0.5'}
              />
            </li>
          );
        })}
      </ul>
      {total && (
        <p className="text-xs tabular-nums text-(--color-text-secondary)">
          {t('setup:computer.installTotal', { download: size(total.downloadMb), disk: size(total.diskMb) })}
        </p>
      )}
    </div>
  );
}

/** Below a card's label: a hairline, then what belongs to the choice. */
const PANEL = 'mx-4 mb-3.5 border-t border-[var(--color-shell-edge,var(--color-border-muted))] pt-3';

/** A line under a card's description, inside its label. */
const NOTE = 'mt-1.5 flex items-start gap-1.5 text-xs leading-snug';

/** Starting Docker would not change this, so nothing here asks the user to. */
function DockerNotOffered() {
  const { t } = useTranslation();
  return (
    <div className={PANEL}>
      <p className="text-xs leading-snug text-(--color-text-secondary)">{t('setup:computer.dockerNotOffered')}</p>
    </div>
  );
}

/** Reads the status again, which says whether Docker now answers. */
function DockerMissing() {
  const { t } = useTranslation();
  const { refetch, isFetching: rechecking } = useSetupStatus();
  // Answered in words, or a check that finds nothing looks as if it did nothing.
  const [asked, setAsked] = useState(false);
  return (
    <div className={PANEL}>
      <p role="status" className="text-xs leading-snug text-(--color-text-secondary)">
        {asked && !rechecking ? t('setup:computer.dockerStillDown') : t('setup:computer.dockerMissing')}
      </p>
      <div className="mt-2 flex flex-wrap items-center gap-2">
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={rechecking}
          aria-busy={rechecking || undefined}
          onClick={() => {
            setAsked(true);
            void refetch();
          }}
        >
          {rechecking && (
            <span aria-hidden="true" className="mr-1.5 shrink-0">
              <Loader size={14} className="text-current" />
            </span>
          )}
          {t('setup:computer.dockerCheck')}
        </Button>
        <a
          href={DOCKER_INSTALL_URL}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-1 px-2 text-[0.8125rem] text-(--color-text-secondary) hover:text-(--color-text-primary)"
        >
          {t('setup:computer.dockerInstall')}
          <ExternalLink aria-hidden="true" className="h-3 w-3" />
        </a>
      </div>
    </div>
  );
}

interface SandboxChoiceProps {
  status: SetupStatus;
  draft: SandboxDraft;
  /** The question that names the group. */
  labelledBy: string;
}

/**
 * Where the agent runs, as a card per place, each saying what it means before
 * it is picked. The step and Settings share it; each says itself what a save
 * that failed means, beside the button that saves.
 */
export function SandboxChoice({ status, draft, labelledBy }: SandboxChoiceProps) {
  const { t } = useTranslation();
  const name = useId();
  const cards = shownCards(status);
  const cloudKey = status.keys.find((key) => key.group === 'sandbox' && key.provider === draft.cloudKind);
  const cloudName = vendorName(draft.cloudKind, t);
  const dockerAvailable = cardAvailable(status, 'docker');

  // Set outside the app, this choice cannot be changed from here.
  if (status.sandbox.provider_source === 'environment') {
    return (
      <p className="text-sm text-(--color-text-secondary)">
        {t('setup:computer.environment', { provider: providerLabel(status.sandbox.provider, t) })}
      </p>
    );
  }

  return (
    <div role="radiogroup" aria-labelledby={labelledBy} className="flex flex-col gap-2">
      {cards.includes('local') && (
        <AnswerOption<SandboxCard>
          name={name}
          value="local"
          checked={draft.card === 'local'}
          onSelect={draft.pickCard}
          mark={<AnswerTile><Laptop aria-hidden="true" strokeWidth={1.5} /></AnswerTile>}
          title={t('setup:computer.localTitle')}
          detail={
            <>
              {t('setup:computer.localDesc')}
              <span className={cn(NOTE, 'text-(--color-warning)')}>
                <TriangleAlert aria-hidden="true" className="mt-px h-3.5 w-3.5 shrink-0" />
                {t('setup:computer.localWarning')}
              </span>
            </>
          }
          panel={
            <Disclosure open={draft.card === 'local'}>
              <div className={PANEL}>
                <InstallGroups groups={draft.groups} onToggle={draft.toggleGroup} />
              </div>
            </Disclosure>
          }
        />
      )}

      <AnswerOption<SandboxCard>
        name={name}
        value="docker"
        checked={draft.card === 'docker'}
        onSelect={draft.pickCard}
        disabled={!dockerAvailable}
        mark={<AnswerTile><Box aria-hidden="true" strokeWidth={1.5} /></AnswerTile>}
        title={t('setup:computer.dockerTitle')}
        detail={
          <>
            {t('setup:computer.dockerDesc')}
            {dockerAvailable && (
              <span className={cn(NOTE, 'items-center text-(--color-text-secondary)')}>
                <CircleCheck aria-hidden="true" className="h-3.5 w-3.5 shrink-0 text-(--color-success)" />
                {t('setup:computer.dockerFound')}
              </span>
            )}
          </>
        }
        panel={dockerAvailable ? null : optionOf(status, 'docker')?.detail === 'not_offered' ? <DockerNotOffered /> : <DockerMissing />}
      />

      <AnswerOption<SandboxCard>
        name={name}
        value="cloud"
        checked={draft.card === 'cloud'}
        onSelect={draft.pickCard}
        mark={<AnswerTile><Cloud aria-hidden="true" strokeWidth={1.5} /></AnswerTile>}
        title={t('setup:computer.cloudTitle')}
        detail={
          <>
            {t('setup:computer.cloudDesc')}
            <span className={NOTE}>{t('setup:computer.cloudReach')}</span>
          </>
        }
        panel={
          <Disclosure open={draft.card === 'cloud'}>
            <div className={cn(PANEL, 'flex flex-col gap-3')}>
              <div className="self-start">
                <SegmentedControl<CloudKind>
                  label={t('setup:computer.cloudVendor')}
                  value={draft.cloudKind}
                  onChange={draft.pickCloudKind}
                  options={CLOUD_KINDS.map((kind) => ({ value: kind, label: vendorName(kind, t) }))}
                />
              </div>
              {cloudKey && (
                <KeyField
                  key={keyFieldKey(cloudKey)}
                  setupKey={cloudKey}
                  label={t('setup:computer.cloudKey', { vendor: cloudName })}
                  draft={draft.keyDrafts[cloudKey.name] ?? ''}
                  onDraft={(value) => draft.setKeyDraft(cloudKey.name, value)}
                  error={draft.keyMissing ? t('setup:computer.cloudKeyMissing', { vendor: cloudName }) : null}
                />
              )}
            </div>
          </Disclosure>
        }
      />
    </div>
  );
}
