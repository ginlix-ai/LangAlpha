import { useQueryClient } from '@tanstack/react-query';
import { ChevronRight, Plus } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { Loader } from '@/components/ui/loader';
import type { Computer } from '@/types/api';
import { computerIdentity, ComputerBadge } from '@/pages/ChatAgent/components/ComputerBadge';
import { RolePills } from '@/pages/ChatAgent/components/computerParts';
import { ComputerStatusIndicator } from '@/pages/ChatAgent/components/computerStatusUi';
import { newComputerNote } from '@/pages/ChatAgent/components/providerUi';
import { DiskPercent } from '@/pages/ChatAgent/components/workspaceGallery/DiskPercent';
import { openComputersPanel } from '@/pages/ChatAgent/hooks/computerPanelStore';
import { prefetchComputerProviders, useComputerProviders, useComputers } from '@/pages/ChatAgent/hooks/useComputers';
import { useThisMac } from '@/pages/ChatAgent/hooks/useRemoteControl';
import { prefetchTierQuota } from '@/pages/ChatAgent/hooks/useTierQuota';
import { isComputerPreparing, liveComputers } from '@/pages/ChatAgent/utils/computerStatus';
import { SettingsGroup } from '../components/SettingsLayout';
import { useSettingCopy } from '../components/useSettingCopy';
import { SETTINGS } from '../sections';

/**
 * Computers: every machine as a row that opens its details in the Computers
 * panel, over this page, and the way to add one. The panel stays the one
 * place a machine is changed; this page is the list it returns to.
 */
export function ComputersPanel() {
  const { t } = useTranslation();
  const copy = useSettingCopy();
  const queryClient = useQueryClient();
  const { data, isLoading, isError, refetch } = useComputers();
  const thisMac = useThisMac({ enabled: true });
  const { data: providerData } = useComputerProviders({ enabled: true });
  const computers = liveComputers(data?.computers);
  const list = copy(SETTINGS.computers);
  const add = copy(SETTINGS.newComputer);
  // The New computer form waits on the providers and the plan's quota.
  const prefetch = () => {
    void prefetchComputerProviders(queryClient);
    void prefetchTierQuota(queryClient);
  };

  return (
    // The anchor is the group, as the rows can be none at all: the search
    // then lands on its first control, a machine or New computer.
    <SettingsGroup id={list.id} title={list.label} description={list.description}>
      {isLoading ? (
        <div className="settings-machine is-static" aria-busy="true">
          <Loader size={14} style={{ color: 'var(--color-accent-primary)' }} />
          <span className="settings-machine-desc">{t('computer.loading', 'Loading computers')}</span>
        </div>
      ) : isError && !data ? (
        <div className="settings-machine is-static">
          <span className="settings-machine-desc flex-1" role="alert">{t('settings.computers.loadFailed')}</span>
          <Button variant="outline" size="sm" onClick={() => void refetch()}>{t('common.retry')}</Button>
        </div>
      ) : (
        computers.map((computer) => (
          <MachineRow
            key={computer.computer_id}
            computer={computer}
            isThisMac={thisMac.of(computer) !== null}
          />
        ))
      )}
      <button
        type="button"
        id={add.id}
        className="settings-machine"
        onClick={() => openComputersPanel({ view: 'create', returnTo: 'settings' })}
        onPointerEnter={prefetch}
        onFocus={prefetch}
        data-testid="settings-new-computer"
      >
        <span className="settings-machine-icon is-add" aria-hidden="true"><Plus /></span>
        <span className="settings-machine-text">
          <span className="settings-machine-label">{add.label}</span>
          <span className="settings-machine-desc">{newComputerNote(t, providerData?.providers)}</span>
        </span>
      </button>
    </SettingsGroup>
  );
}

/** One machine: its kind's glyph, its name and pills, and what it is doing. */
function MachineRow({ computer, isThisMac }: { computer: Computer; isThisMac: boolean }) {
  const { t } = useTranslation();
  const Icon = computerIdentity(t, computer, isThisMac).icon;
  const workspaces = computer.workspace_count;
  return (
    <button
      type="button"
      className="settings-machine"
      onClick={() => openComputersPanel({ view: 'details', computerId: computer.computer_id, returnTo: 'settings' })}
      data-testid="settings-computer"
      data-computer-id={computer.computer_id}
    >
      <span className="settings-machine-icon" aria-hidden="true"><Icon /></span>
      <span className="settings-machine-text">
        <span className="settings-machine-name">
          <span className="settings-machine-label truncate">{computer.name}</span>
          <RolePills computer={computer} />
        </span>
        <span className="settings-machine-desc settings-machine-facts">
          <ComputerStatusIndicator status={computer.status} preparing={isComputerPreparing(computer)} glyphSize={10} />
          <ComputerBadge computer={computer} isThisMac={isThisMac} name={computer.name} variant="plain" />
          {/* The dot goes with its fact, so a wrap never leaves one at a line's end. */}
          {typeof workspaces === 'number' && (
            <span className="inline-flex gap-1.5 whitespace-nowrap">
              <span aria-hidden="true">·</span>
              {t('computer.workspaceCount', '{{count}} workspaces', { count: workspaces })}
            </span>
          )}
        </span>
      </span>
      <DiskPercent computer={computer} className="text-xs" />
      <ChevronRight className="settings-machine-chevron" aria-hidden="true" />
    </button>
  );
}
