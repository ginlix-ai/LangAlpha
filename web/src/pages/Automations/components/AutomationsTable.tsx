import React from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from 'framer-motion';
import AutomationRow from './AutomationRow';
import AutomationDetailOverlay from './AutomationDetailOverlay';
import type { Automation } from '@/types/automation';

interface AutomationsTableProps {
  automations: Automation[];
  loading: boolean;
  selectedAutomation: Automation | null;
  onSelectAutomation: (automation: Automation) => void;
  onCloseOverlay: () => void;
  onEdit: (automation: Automation) => void;
  onDelete: (automation: Automation) => void;
  onPause: (id: string) => void;
  onResume: (id: string) => void;
  onTrigger: (id: string) => void;
  mutationsLoading: boolean;
}

export default function AutomationsTable({
  automations,
  loading,
  selectedAutomation,
  onSelectAutomation,
  onCloseOverlay,
  onEdit,
  onDelete,
  onPause,
  onResume,
  onTrigger,
  mutationsLoading,
}: AutomationsTableProps) {
  const { t } = useTranslation();

  return (
    <div className="relative flex-1 min-h-0">
      {/* Column Headers */}
      <div
        className="hidden sm:grid grid-cols-[1fr_1fr_0.6fr_0.8fr_0.5fr] gap-4 px-4 py-2 text-xs uppercase tracking-wider mb-2"
        style={{ color: 'var(--color-text-secondary)' }}
      >
        <span>{t('common.name')}</span>
        <span>{t('automation.schedule')}</span>
        <span>{t('automation.mode')}</span>
        <span>{t('automation.nextRun')}</span>
        <span className="text-right">{t('automation.status')}</span>
      </div>

      {/* Rows */}
      <div className="flex flex-col gap-2">
        {!loading && automations.length === 0 && (
          <div className="flex items-center justify-center py-12">
            <span className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('automation.noAutomationsHint')}
            </span>
          </div>
        )}
        <AnimatePresence>
          {automations.map((automation, index) => (
            <AutomationRow
              key={automation.automation_id as string}
              automation={automation}
              index={index}
              onClick={onSelectAutomation}
            />
          ))}
        </AnimatePresence>
      </div>

      {/* Detail Overlay */}
      <AnimatePresence>
        {selectedAutomation && (
          <AutomationDetailOverlay
            automation={selectedAutomation}
            onClose={onCloseOverlay}
            onEdit={onEdit}
            onDelete={onDelete}
            onPause={onPause}
            onResume={onResume}
            onTrigger={onTrigger}
            mutationsLoading={mutationsLoading}
          />
        )}
      </AnimatePresence>
    </div>
  );
}
