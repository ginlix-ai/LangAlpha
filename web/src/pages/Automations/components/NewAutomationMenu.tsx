import React from 'react';
import { useTranslation } from 'react-i18next';
import { ChevronDown, Plus } from 'lucide-react';
import { HeaderButton } from '@/components/mcp/McpPrimitives';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { AUTOMATION_TEMPLATES, type TemplateId } from '../utils/templates';

/**
 * The page's one primary action. Templates are starting points for the form,
 * not a gallery to browse, so they live behind the button that creates rather
 * than as a row of cards above everything the user already has.
 */
export function NewAutomationMenu({ onPick }: { onPick: (id: TemplateId) => void }) {
  const { t } = useTranslation();
  const templates = AUTOMATION_TEMPLATES.filter((tpl) => tpl.id !== 'custom');
  return (
    <DropdownMenu modal={false}>
      {/* The Plugins and Orders headers' primary action. It takes the ref the
          trigger hands it as a prop, which React 19 passes through. */}
      <DropdownMenuTrigger asChild>
        <HeaderButton variant="primary" icon={Plus}>
          {t('automation.newAutomation')}
          <ChevronDown className="h-3 w-3 opacity-70" />
        </HeaderButton>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-[300px] p-1.5">
        <DropdownMenuItem onSelect={() => onPick('custom')} className="items-start gap-3 py-2">
          <Plus className="mt-0.5 h-4 w-4 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
          <span className="flex min-w-0 flex-col">
            <span style={{ color: 'var(--color-text-primary)' }}>{t('automation.blankAutomation')}</span>
            <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('automation.blankAutomationDesc')}
            </span>
          </span>
        </DropdownMenuItem>
        <DropdownMenuSeparator />
        <div className="automations-kicker px-2.5 pb-1 pt-1.5">{t('automation.fromTemplate')}</div>
        {templates.map((tpl) => {
          const Icon = tpl.icon;
          return (
            <DropdownMenuItem key={tpl.id} onSelect={() => onPick(tpl.id)} className="items-start gap-3 py-2">
              <Icon className="mt-0.5 h-4 w-4 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
              <span className="flex min-w-0 flex-col">
                <span style={{ color: 'var(--color-text-primary)' }}>{t(tpl.nameKey)}</span>
                <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                  {t(tpl.descriptionKey)}
                </span>
              </span>
            </DropdownMenuItem>
          );
        })}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
