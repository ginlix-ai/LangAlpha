import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { SegmentedControl } from '@/components/ui/segmented-control';
import type { Automation } from '@/types/automation';
import { automationCensus } from '../utils/status';
import type { TemplateId } from '../utils/templates';
import { NewAutomationMenu } from './NewAutomationMenu';

export type AutomationsView = 'feed' | 'manage';

interface AutomationsHeaderProps {
  automations: Automation[];
  view: AutomationsView;
  onViewChange: (view: AutomationsView) => void;
  onNew: (template: TemplateId) => void;
}

/** Title, a one-line census of what is set up, and the two ways to look at it. */
export default function AutomationsHeader({ automations, view, onViewChange, onNew }: AutomationsHeaderProps) {
  const { t } = useTranslation();

  // What is set up, so a finished automation is left out. Paused and switched
  // off are counted apart, which the groups below, keeping them under their
  // kind, do not do.
  const census = useMemo(() => {
    const { running, scheduled, watching, paused, off, attention } = automationCensus(automations);
    const parts: string[] = [];
    if (running) parts.push(t('automation.censusRunning', { count: running }));
    if (scheduled) parts.push(t('automation.censusScheduled', { count: scheduled }));
    if (watching) parts.push(t('automation.censusWatching', { count: watching }));
    if (paused) parts.push(t('automation.censusPaused', { count: paused }));
    if (off) parts.push(t('automation.censusOff', { count: off }));
    if (attention) parts.push(t('automation.censusAttention', { count: attention }));
    return parts;
  }, [automations, t]);

  // With nothing set up the page below is the invitation, so the header
  // carries no actions of its own (the workspace gallery does the same).
  const empty = automations.length === 0;

  return (
    <header className="automations-header">
      <div className="min-w-0">
        <h1 className="title-font automations-title">{t('automation.automations')}</h1>
        {census.length > 0 ? (
          <p className="automation-mono automations-census">{census.join(' · ')}</p>
        ) : (
          <p className="automations-tagline">{t('automation.tagline')}</p>
        )}
      </div>
      {!empty && (
        <div className="flex items-center gap-2.5">
          <SegmentedControl<AutomationsView>
            value={view}
            onChange={onViewChange}
            label={t('automation.viewLabel')}
            options={[
              { value: 'feed', label: t('automation.viewFeed') },
              { value: 'manage', label: t('automation.viewManage') },
            ]}
          />
          <NewAutomationMenu onPick={onNew} />
        </div>
      )}
    </header>
  );
}
