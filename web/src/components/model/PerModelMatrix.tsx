import { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { X } from 'lucide-react';
import { Select } from '@/components/ui/select';
import { usePreferences } from '@/hooks/usePreferences';
import { useModelProfileWriter } from '@/hooks/useModelProfile';
import { modelPrefs } from '@/lib/modelPreferences';
import {
  COMPACTION_PROFILE_ORDER,
  EFFORT_LABELS,
  EFFORT_ORDER,
  GUIDANCE_LABELS,
  GUIDANCE_LEVELS,
  SPEED_LABELS,
  resolveTuning,
} from '@/lib/modelTuning';
import type { GuidanceLevel } from '@/lib/modelTuning';
import type { CompactionProfileCatalog, CompactionProfileName } from '@/hooks/useAllModels';
import type { ModelMetadataEntry } from '@/hooks/useFilteredModels';

export interface PerModelMatrixProps {
  metadata: Record<string, ModelMetadataEntry>;
  validModelNames: Set<string>;
  compactionProfiles: CompactionProfileCatalog | null;
  /** Deployment-pinned `prompt.guidance`; null on the `auto` default. */
  pinnedGuidance?: string | null;
}

/** A cell whose setting the model has no control for — stated, not left blank,
 *  so an empty cell always means "inherits" and never "impossible". */
function NotSupported({ label }: { label: string }) {
  return (
    <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
      {label}
    </span>
  );
}

function CellSelect({
  value,
  onChange,
  ariaLabel,
  defaultLabel,
  options,
}: {
  value: string;
  onChange: (v: string) => void;
  ariaLabel: string;
  defaultLabel: string;
  options: Array<{ value: string; label: string }>;
}) {
  return (
    <Select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      aria-label={ariaLabel}
      className="min-w-[7.5rem]"
      // className lands on the wrapper, so the compact type has to come through
      // style — the select itself hardcodes text-sm.
      style={{ fontSize: '0.75rem', paddingTop: '0.25rem', paddingBottom: '0.25rem' }}
    >
      <option value="">{defaultLabel}</option>
      {options.map((o) => (
        <option key={o.value} value={o.value}>{o.label}</option>
      ))}
    </Select>
  );
}

/**
 * Every model the user has tuned, one row each.
 *
 * The table is the only place that answers "what have I overridden, and where?"
 * — the composer menu can set a model's effort but never shows that five other
 * models carry settings of their own. An empty cell reads as the account
 * default, so the inheritance is on screen rather than implied.
 */
export function PerModelMatrix({
  metadata,
  validModelNames,
  compactionProfiles,
  pinnedGuidance = null,
}: PerModelMatrixProps) {
  const { t } = useTranslation();
  const { preferences } = usePreferences();
  const { writeProfile, clearProfile } = useModelProfileWriter();
  // Rows the user opened but has not set anything on yet. They carry no server
  // state — a profile only exists once a field is picked.
  const [pendingRows, setPendingRows] = useState<string[]>([]);

  const account = useMemo(() => modelPrefs(preferences), [preferences]);
  const profiles = useMemo(() => account.profiles ?? {}, [account]);

  const rows = useMemo(() => {
    const configured = Object.keys(profiles).filter(
      (m) => profiles[m] && Object.keys(profiles[m]).length > 0,
    );
    return [...new Set([...configured, ...pendingRows])].sort();
  }, [profiles, pendingRows]);

  // A profile whose model the user cannot currently reach still gets a row.
  // Its metadata is gone, so the cells have no ladder to offer and render
  // inert -- but the settings are live the moment the model comes back, and a
  // hidden row is one the user can neither see nor clear. Nothing scrubs these
  // either: the server-side sweep only fires on an unresolvable *selected*
  // model, so a profile alone never reaches it.
  const unreachable = useMemo(
    () => new Set(rows.filter((m) => !validModelNames.has(m))),
    [rows, validModelNames],
  );

  const addable = useMemo(
    () => [...validModelNames].filter((m) => !rows.includes(m)).sort(),
    [validModelNames, rows],
  );

  const profileOptions = useMemo(
    () =>
      COMPACTION_PROFILE_ORDER.filter((name) => compactionProfiles?.[name]).map((name) => ({
        value: name,
        label: t(`settings.compactionProfiles.${name}.label`),
      })),
    [compactionProfiles, t],
  );

  const guidanceOptions = useMemo(
    () => GUIDANCE_LEVELS.map((level) => ({ value: level, label: t(GUIDANCE_LABELS[level]) })),
    [t],
  );

  const plainDefault = t('settings.modelTuning.default');
  /** "Default" alone says nothing about what will actually happen, so name the
   *  value it resolves to whenever the account or the model declares one. */
  const defaultLabelFor = (value: string | null) =>
    value ? t('settings.modelTuning.defaultNamed', { value }) : plainDefault;

  const drop = (model: string) => {
    setPendingRows((prev) => prev.filter((m) => m !== model));
    clearProfile(model);
  };

  return (
    <div className="flex flex-col gap-2">
      {/* No label of its own: the tab gives this a section heading, the same
          as the account defaults it overrides. */}
      <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('settings.modelTuning.perModelDesc')}
      </p>

      {rows.length > 0 && (
        <div className="overflow-x-auto">
          {/* Below this width the cells squeeze their labels into ellipses
              instead of the container scrolling, which is the whole point of
              the overflow wrapper. */}
          <table className="w-full text-xs" style={{ borderCollapse: 'collapse', minWidth: '660px' }}>
            <thead>
              <tr style={{ color: 'var(--color-text-tertiary)' }}>
                <th className="text-left font-medium py-1 pr-3">{t('settings.modelTuning.colModel')}</th>
                <th className="text-left font-medium py-1 pr-3">{t('settings.modelTuning.colEffort')}</th>
                <th className="text-left font-medium py-1 pr-3">{t('settings.modelTuning.colContext')}</th>
                <th className="text-left font-medium py-1 pr-3">{t('settings.modelTuning.colGuidance')}</th>
                <th className="text-left font-medium py-1 pr-3">{t('settings.modelTuning.colSpeed')}</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {rows.map((model) => {
                if (unreachable.has(model)) {
                  return (
                    <tr key={model} style={{ borderTop: '1px solid var(--color-border-muted)' }}>
                      <td className="py-2 pr-3 align-middle" style={{ color: 'var(--color-text-tertiary)' }}>
                        {model}
                      </td>
                      <td className="py-2 pr-3 align-middle" colSpan={4}>
                        <NotSupported label={t('settings.modelTuning.modelUnavailable')} />
                      </td>
                      <td className="py-2 align-middle">
                        <button
                          type="button"
                          onClick={() => drop(model)}
                          aria-label={`${t('settings.modelTuning.resetModel')} ${model}`}
                          title={t('settings.modelTuning.resetModel')}
                          className="hover:opacity-70"
                          style={{ color: 'var(--color-text-tertiary)' }}
                        >
                          <X className="h-3.5 w-3.5" />
                        </button>
                      </td>
                    </tr>
                  );
                }
                const meta = metadata[model];
                const { profile, efforts, inherited } = resolveTuning(
                  account,
                  profiles[model] ?? {},
                  meta,
                  pinnedGuidance,
                );
                const storedEffort = profile.reasoning_effort ?? '';
                // A stored value the model no longer offers still has to be
                // selectable, or the row shows a setting with no way to clear it.
                const orderedEfforts = EFFORT_ORDER.filter(
                  (lv) => efforts.includes(lv) || lv === storedEffort,
                );
                const storedSpeed = profile.fast_mode ?? null;
                const supportsSpeed = meta?.sdk === 'codex' || storedSpeed !== null;
                return (
                  <tr key={model} style={{ borderTop: '1px solid var(--color-border-muted)' }}>
                    <td className="py-2 pr-3 align-middle" style={{ color: 'var(--color-text-primary)' }}>
                      {model}
                    </td>
                    <td className="py-2 pr-3 align-middle">
                      {orderedEfforts.length > 0 ? (
                        <CellSelect
                          value={storedEffort}
                          onChange={(v) => writeProfile(model, { reasoning_effort: v || null })}
                          ariaLabel={`${model} ${t('settings.modelTuning.colEffort')}`}
                          defaultLabel={defaultLabelFor(
                            inherited.reasoning_effort && EFFORT_LABELS[inherited.reasoning_effort]
                              ? t(EFFORT_LABELS[inherited.reasoning_effort])
                              : null,
                          )}
                          options={orderedEfforts.map((lv) => ({ value: lv, label: t(EFFORT_LABELS[lv]) }))}
                        />
                      ) : (
                        <NotSupported label={t('settings.modelTuning.notSupported')} />
                      )}
                    </td>
                    <td className="py-2 pr-3 align-middle">
                      <CellSelect
                        value={profile.compaction_profile ?? ''}
                        onChange={(v) =>
                          writeProfile(model, { compaction_profile: (v as CompactionProfileName) || null })
                        }
                        ariaLabel={`${model} ${t('settings.modelTuning.colContext')}`}
                        defaultLabel={defaultLabelFor(
                          inherited.compaction_profile
                            ? t(`settings.compactionProfiles.${inherited.compaction_profile}.label`)
                            : null,
                        )}
                        options={profileOptions}
                      />
                    </td>
                    <td className="py-2 pr-3 align-middle">
                      <CellSelect
                        value={profile.prompt_guidance ?? ''}
                        onChange={(v) => writeProfile(model, { prompt_guidance: (v as GuidanceLevel) || null })}
                        ariaLabel={`${model} ${t('settings.modelTuning.colGuidance')}`}
                        defaultLabel={defaultLabelFor(t(GUIDANCE_LABELS[inherited.prompt_guidance]))}
                        options={guidanceOptions}
                      />
                    </td>
                    <td className="py-2 pr-3 align-middle">
                      {supportsSpeed ? (
                        <CellSelect
                          value={storedSpeed === true ? 'fast' : storedSpeed === false ? 'standard' : ''}
                          onChange={(v) => writeProfile(model, { fast_mode: v === '' ? null : v === 'fast' })}
                          ariaLabel={`${model} ${t('settings.modelTuning.colSpeed')}`}
                          defaultLabel={defaultLabelFor(
                            t(SPEED_LABELS[inherited.fast_mode ? 'fast' : 'standard']),
                          )}
                          options={[
                            { value: 'standard', label: t(SPEED_LABELS.standard) },
                            { value: 'fast', label: t(SPEED_LABELS.fast) },
                          ]}
                        />
                      ) : (
                        <NotSupported label={t('settings.modelTuning.notSupported')} />
                      )}
                    </td>
                    <td className="py-2 align-middle">
                      <button
                        type="button"
                        onClick={() => drop(model)}
                        aria-label={`${t('settings.modelTuning.resetModel')} ${model}`}
                        title={t('settings.modelTuning.resetModel')}
                        className="hover:opacity-70"
                        style={{ color: 'var(--color-text-tertiary)' }}
                      >
                        <X className="h-3.5 w-3.5" />
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {addable.length > 0 && (
        <Select
          value=""
          onChange={(e) => {
            if (e.target.value) setPendingRows((prev) => [...prev, e.target.value]);
          }}
          aria-label={t('settings.modelTuning.addModel')}
          className="self-start text-xs"
        >
          <option value="">+ {t('settings.modelTuning.addModel')}</option>
          {addable.map((m) => (
            <option key={m} value={m}>{m}</option>
          ))}
        </Select>
      )}
    </div>
  );
}
