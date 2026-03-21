import React from 'react';
import { useTranslation } from 'react-i18next';
import { motion } from 'framer-motion';
import { AUTOMATION_TEMPLATES, type TemplateId } from '../utils/templates';

const TEMPLATE_ACCENT: Record<TemplateId, { color: string; bg: string }> = {
  price_alert: { color: 'var(--color-accent-primary)', bg: 'var(--color-accent-soft)' },
  morning_briefing: { color: 'var(--color-info)', bg: 'var(--color-info-soft)' },
  weekly_review: { color: 'var(--color-profit)', bg: 'var(--color-profit-soft)' },
  earnings_watch: { color: 'var(--color-warning)', bg: 'var(--color-warning-soft)' },
  custom: { color: 'var(--color-text-secondary)', bg: 'var(--color-bg-surface)' },
};

interface AutomationTemplateCardsProps {
  selectedTemplate: TemplateId | null;
  onSelectTemplate: (id: TemplateId) => void;
}

export default function AutomationTemplateCards({
  selectedTemplate,
  onSelectTemplate,
}: AutomationTemplateCardsProps) {
  const { t } = useTranslation();

  return (
    <div className="flex gap-2 sm:gap-3 overflow-x-auto sm:flex-wrap pb-1 snap-x snap-mandatory sm:snap-none">
      {AUTOMATION_TEMPLATES.map((template, i) => {
        const Icon = template.icon;
        const isSelected = selectedTemplate === template.id;
        const isCustom = template.id === 'custom';
        const accent = TEMPLATE_ACCENT[template.id];

        return (
          <motion.div
            key={template.id}
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.3, delay: i * 0.05 }}
            whileTap={{ scale: 0.97 }}
            onClick={() => onSelectTemplate(template.id)}
            className="snap-start shrink-0 cursor-pointer rounded-xl p-3 sm:p-4 transition-shadow w-[148px] sm:w-[176px]"
            style={{
              border: isSelected
                ? '1.5px solid var(--color-accent-primary)'
                : isCustom
                  ? '1.5px dashed var(--color-border-elevated)'
                  : '1px solid var(--color-border-default)',
              backgroundColor: isSelected
                ? 'var(--color-accent-soft)'
                : 'var(--color-bg-card)',
              boxShadow: isSelected
                ? '0 0 0 3px var(--color-accent-soft), var(--shadow-card)'
                : 'var(--shadow-card)',
            }}
          >
            <div
              className="w-9 h-9 rounded-lg flex items-center justify-center mb-3"
              style={{
                backgroundColor: isSelected ? 'var(--color-accent-soft)' : accent.bg,
              }}
            >
              <Icon
                className="w-[18px] h-[18px]"
                style={{
                  color: isSelected ? 'var(--color-accent-primary)' : accent.color,
                }}
              />
            </div>
            <div
              className="text-[13px] font-semibold leading-tight"
              style={{ color: 'var(--color-text-primary)' }}
            >
              {t(template.nameKey)}
            </div>
            <div
              className="text-[11px] mt-1.5 leading-snug"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {t(template.descriptionKey)}
            </div>
          </motion.div>
        );
      })}
    </div>
  );
}
