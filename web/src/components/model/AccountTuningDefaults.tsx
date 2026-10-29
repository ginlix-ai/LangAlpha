import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { Select } from '@/components/ui/select';
import { CompactionProfilePicker } from './CompactionProfilePicker';
import { usePreferences } from '@/hooks/usePreferences';
import { useModelProfileWriter } from '@/hooks/useModelProfile';
import { modelPrefs } from '@/lib/modelPreferences';
import {
  EFFORT_LABELS,
  EFFORT_ORDER,
  GUIDANCE_LABELS,
  GUIDANCE_LEVELS,
} from '@/lib/modelTuning';
import type { CompactionProfileCatalog } from '@/hooks/useAllModels';
import type { ModelMetadataEntry } from '@/hooks/useFilteredModels';

export interface AccountTuningDefaultsProps {
  metadata: Record<string, ModelMetadataEntry>;
  /** The models the user can actually select. `metadata` is the whole catalog,
   *  so the union below has to be narrowed to these or it offers levels no
   *  reachable model honors. */
  validModelNames: Set<string>;
  compactionProfiles: CompactionProfileCatalog | null;
}

/**
 * What a model falls back to when it carries no override of its own.
 *
 * Sits above the per-model table on purpose: this is where each setting is
 * described, and the table below is bare columns that assume you have read
 * them. An empty cell down there resolves to whatever is set here.
 */
export function AccountTuningDefaults({
  metadata,
  validModelNames,
  compactionProfiles,
}: AccountTuningDefaultsProps) {
  const { t } = useTranslation();
  const { preferences } = usePreferences();
  const { writeAccountDefault } = useModelProfileWriter();
  const account = modelPrefs(preferences);

  // The union of what the visible models actually honor — no model honors all
  // seven levels, and a hardcoded ladder here would offer levels that silently
  // fall back on every model the user owns. Keyed on the selectable names
  // rather than every entry in `metadata`: a level only a gated model declares
  // is one this account saves and every reachable model then clamps away.
  const effortLevels = useMemo(() => {
    const seen = new Set<string>();
    for (const name of validModelNames) {
      for (const level of metadata[name]?.reasoning_efforts ?? []) seen.add(level);
    }
    return EFFORT_ORDER.filter((lv) => seen.has(lv) && EFFORT_LABELS[lv]);
  }, [metadata, validModelNames]);

  const guidance = account.prompt_guidance ?? '';

  return (
    <div className="flex flex-col gap-4">
      {effortLevels.length > 0 && (
        <div className="flex flex-col gap-1.5">
          <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {t('settings.modelTuning.colEffort')}
          </label>
          <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('settings.modelTuning.effortDesc')}
          </p>
          <Select
            value={account.reasoning_effort ?? ''}
            onChange={(e) => writeAccountDefault({ reasoning_effort: e.target.value || null })}
            aria-label={t('settings.modelTuning.colEffort')}
          >
            <option value="">{t('settings.modelTuning.modelDefault')}</option>
            {effortLevels.map((lv) => (
              <option key={lv} value={lv}>{t(EFFORT_LABELS[lv])}</option>
            ))}
          </Select>
        </div>
      )}

      <CompactionProfilePicker
        value={account.compaction_profile ?? ''}
        onChange={(v) => writeAccountDefault({ compaction_profile: v || null })}
        profiles={compactionProfiles}
      />

      <div className="flex flex-col gap-1.5">
        <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {t('settings.modelTuning.colGuidance')}
        </label>
        <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('settings.modelTuning.guidanceDesc')}
        </p>
        <div
          role="radiogroup"
          aria-label={t('settings.modelTuning.colGuidance')}
          className="flex flex-wrap gap-2"
        >
          {GUIDANCE_LEVELS.map((level) => {
            const selected = guidance === level;
            return (
              <button
                key={level}
                type="button"
                role="radio"
                aria-checked={selected}
                onClick={() => writeAccountDefault({ prompt_guidance: selected ? null : level })}
                className="rounded-md px-3 py-1.5 text-xs font-medium transition-colors"
                style={{
                  background: selected ? 'var(--color-bg-tag)' : 'var(--color-bg-surface)',
                  border: `1px solid ${selected ? 'var(--color-text-primary)' : 'var(--color-border-default)'}`,
                  color: 'var(--color-text-primary)',
                }}
              >
                {t(GUIDANCE_LABELS[level])}
              </button>
            );
          })}
        </div>
        {guidance !== '' && (
          <button
            type="button"
            onClick={() => writeAccountDefault({ prompt_guidance: null })}
            className="self-start text-xs underline-offset-2 hover:underline"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {t('settings.modelTuning.useModelDefault')}
          </button>
        )}
      </div>
    </div>
  );
}
