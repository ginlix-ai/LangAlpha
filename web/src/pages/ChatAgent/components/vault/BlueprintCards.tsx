import { useTranslation } from 'react-i18next';
import { Plus, Sparkles } from 'lucide-react';
import type { VaultBlueprint } from '../../utils/api';

/**
 * "Recommended credentials": the credentials the workspace's enabled MCP
 * servers declare but the vault doesn't hold yet. Dashed cards, not rows —
 * they are an invitation to create, not a listing of what exists.
 */

interface BlueprintCardsProps {
  /** User-tier blueprints additionally carry the declaring plugin's name. */
  blueprints: (VaultBlueprint & { plugin_name?: string | null })[];
  /** At the secret cap: cards stay visible but explain why they're inert. */
  atCap: boolean;
  maxSecrets: number;
  onSelect: (blueprint: VaultBlueprint) => void;
}

export function BlueprintCards({ blueprints, atCap, maxSecrets, onSelect }: BlueprintCardsProps) {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center gap-1.5 text-xs font-medium" style={{ color: 'var(--color-text-secondary)' }}>
        <Sparkles className="h-3 w-3" />
        {t('vault.recommended')}
      </div>
      {blueprints.map((bp) => (
        <button
          key={bp.name}
          type="button"
          onClick={() => onSelect(bp)}
          disabled={atCap}
          className="flex flex-col items-start gap-0.5 p-3 rounded-lg text-left transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          style={{
            backgroundColor: 'var(--color-bg-card)',
            border: '1px dashed var(--color-border-default)',
          }}
          title={atCap ? t('vault.atCapHint', { max: maxSecrets }) : undefined}
        >
          <div className="flex items-center justify-between w-full gap-2">
            <div className="flex items-center gap-2 min-w-0">
              <span className="text-sm font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
                {bp.label}
              </span>
              <span className="text-xs font-mono px-1.5 py-0.5 rounded flex-shrink-0" style={{ color: 'var(--color-text-tertiary)', backgroundColor: 'var(--color-bg-tag)' }}>
                {bp.name}
              </span>
              {bp.plugin_name && (
                <span
                  className="text-xs px-1.5 py-0.5 rounded flex-shrink-0"
                  style={{ color: 'var(--color-text-tertiary)', backgroundColor: 'var(--color-bg-tag)' }}
                  title={t('plugins.component.fromPlugin', { plugin: bp.plugin_name })}
                >
                  {bp.plugin_name}
                </span>
              )}
            </div>
            <span className="text-xs flex items-center gap-1 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }}>
              <Plus className="h-3 w-3" />
              {t('vault.setUp')}
            </span>
          </div>
          {bp.description && (
            <div className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {bp.description}
            </div>
          )}
        </button>
      ))}
    </div>
  );
}
