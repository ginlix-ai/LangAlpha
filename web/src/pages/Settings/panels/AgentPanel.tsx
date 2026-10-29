import { useId, useState } from 'react';
import { X } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Input } from '@/components/ui/input';
import { isImeComposing } from '@/lib/ime';
import { Select } from '@/components/ui/select';
import { usePreferences } from '@/hooks/usePreferences';
import { useAllModels } from '@/hooks/useAllModels';
import { useSearchTierAllows } from '@/hooks/useSearchTierAllows';
import { readSubagentsDefault, subagentsDefaultPatch } from '@/lib/subagentsDefault';
import { readSearchPreference, searchDepthPatch, searchProviderPatch } from '@/lib/searchPreferences';
import { OUTPUT_FORMATS, readOutputFormats } from '@/lib/outputFormat';
import { PROACTIVITY, RESEARCH_DEPTHS, isOneOf } from '@/pages/Onboarding/welcome/profile';
import { SettingRow, SettingsCardBody, SettingsGroup, ToggleRow } from '../components/SettingsLayout';
import { useSettingCopy } from '../components/useSettingCopy';
import { SETTINGS } from '../sections';
import { usePreferenceWrite } from './usePreferenceWrite';
import { AutoApproveSettings } from './AutoApproveSettings';
import { OutputFormatRanking } from './OutputFormatRanking';
import type { Preferences } from './types';

const isRanking = (value: unknown) => Array.isArray(value) && value.length > 0
  && value.every((item) => isOneOf(OUTPUT_FORMATS, item));

/** Whether a key of `agent_preference` holds a value that selects the
 *  agent's guidance rather than reaching it as the user's words, as the
 *  server splits them (`prompt_preferences`). `deliverable` and the string
 *  output formats are the keys the ranking replaced; an old `deliverable`
 *  is read into the ranking unless `output_format` holds the user's words,
 *  and then it stays theirs too (`upgraded_agent_preference`). */
const STEERS: Record<string, (value: unknown, prefs: Record<string, unknown>) => boolean> = {
  research_depth: (value) => isOneOf(RESEARCH_DEPTHS, value),
  proactivity: (value) => isOneOf(PROACTIVITY, value),
  output_format: (value) => value === 'html' || value === 'markdown' || isRanking(value),
  deliverable: (value, { output_format: form }) => (value === 'text' || value === 'visual')
    && (form == null || form === 'html' || form === 'markdown' || isRanking(form)),
};

const holdsNothing = (value: unknown) => value == null
  || (typeof value === 'string' && !value.trim())
  || (Array.isArray(value) && value.length === 0)
  || (typeof value === 'object' && !Array.isArray(value) && Object.keys(value as object).length === 0);

/** The keys the agent reads as the user's own words, each as it holds it. */
function instructionsOf(agentPreference: Record<string, unknown>): [string, unknown][] {
  return Object.entries(agentPreference).filter(([key, value]) => !holdsNothing(value) && !STEERS[key]?.(value, agentPreference));
}

const keyLabel = (key: string) => {
  const words = key.replaceAll('_', ' ');
  return words.charAt(0).toUpperCase() + words.slice(1);
};

/** Behavior: how the agent answers, whether it delegates, what it may do
 *  without asking, and how it searches the web. */
export function AgentPanel() {
  const { t } = useTranslation();
  const copy = useSettingCopy();
  const { preferences: prefsData } = usePreferences();
  const write = usePreferenceWrite();
  const tierAllows = useSearchTierAllows();
  const { searchProviders } = useAllModels();
  const depthId = useId();
  const outputFormatId = useId();

  const agentPreference = (prefsData as Preferences | null)?.agent_preference ?? {};
  const writeAgent = (patch: Record<string, unknown>) => { void write({ agent_preference: patch }); };
  const researchDepth = isOneOf(RESEARCH_DEPTHS, agentPreference.research_depth) ? agentPreference.research_depth : '';
  const proactivity = isOneOf(PROACTIVITY, agentPreference.proactivity) ? agentPreference.proactivity : '';
  const instructions = instructionsOf(agentPreference);
  const subagentsDefault = readSubagentsDefault(prefsData);
  const search = readSearchPreference(prefsData, searchProviders);
  const providers = Object.entries(searchProviders ?? {});
  const providerCopy = copy(SETTINGS.searchProvider);

  return (
    <>
      <SettingsGroup title={t('settings.answersGroup')}>
        <SettingsCardBody id={SETTINGS.outputFormat.anchor} className="flex flex-col gap-3">
          <div>
            <p id={outputFormatId} className="settings-row-label">{t(SETTINGS.outputFormat.labelKey)}</p>
            <p className="settings-row-desc">{t(SETTINGS.outputFormat.descKey)}</p>
          </div>
          <OutputFormatRanking
            ranking={readOutputFormats(agentPreference)}
            onChange={(ranking) => writeAgent({ output_format: ranking, deliverable: null })}
            labelledBy={outputFormatId}
          />
        </SettingsCardBody>
        <SettingRow
          {...copy(SETTINGS.researchDepth)}
          description={researchDepth
            ? t(`onboarding.welcome.agent.depth.${researchDepth}.desc`)
            : t(SETTINGS.researchDepth.descKey)}
          htmlFor="research-depth-select"
          wide
        >
          <Select
            id="research-depth-select"
            value={researchDepth}
            onChange={(e) => writeAgent({ research_depth: e.target.value || null })}
          >
            <option value="">{t('settings.agentDecides')}</option>
            {RESEARCH_DEPTHS.map((depth) => (
              <option key={depth} value={depth}>{t(`onboarding.welcome.agent.depth.${depth}.title`)}</option>
            ))}
          </Select>
        </SettingRow>
        <SettingRow
          {...copy(SETTINGS.reachingOut)}
          description={proactivity
            ? t(`onboarding.welcome.agent.proactivity.${proactivity}.desc`)
            : t(SETTINGS.reachingOut.descKey)}
          htmlFor="reaching-out-select"
          wide
        >
          <Select
            id="reaching-out-select"
            value={proactivity}
            onChange={(e) => writeAgent({ proactivity: e.target.value || null })}
          >
            <option value="">{t('settings.agentDecides')}</option>
            {PROACTIVITY.map((level) => (
              <option key={level} value={level}>{t(`onboarding.welcome.agent.proactivity.${level}.title`)}</option>
            ))}
          </Select>
        </SettingRow>
        <ToggleRow
          {...copy(SETTINGS.subagents)}
          checked={subagentsDefault}
          onChange={() => { void write(subagentsDefaultPatch(!subagentsDefault)); }}
        />
      </SettingsGroup>

      {instructions.length > 0 && (
        <SettingsGroup title={t('settings.instructions')} description={t('settings.instructionsDesc')}>
          {instructions.map(([key, value]) => (
            <Instruction
              key={`${key}:${JSON.stringify(value)}`}
              name={keyLabel(key)}
              value={value}
              onSave={key in STEERS ? undefined : (text) => writeAgent({ [key]: text })}
              onRemove={() => writeAgent({ [key]: null })}
            />
          ))}
        </SettingsGroup>
      )}

      <AutoApproveSettings />

      <SettingsGroup title={t('settings.modelTuning.sectionSearch')}>
        <SettingRow
          {...providerCopy}
          description={providers.some(([, p]) => !tierAllows(p.min_tier))
            ? <>{providerCopy.description} <span>{t('settings.searchProviderUpgradeHint')}</span></>
            : providerCopy.description}
          htmlFor={`${providerCopy.id}-select`}
          wide
        >
          <Select
            id={`${providerCopy.id}-select`}
            value={search.provider}
            onChange={(e) => { void write(searchProviderPatch(e.target.value, searchProviders)); }}
            disabled={!providers.some(([, p]) => tierAllows(p.min_tier))}
          >
            <option value="">{t('settings.searchProviderDefault')}</option>
            {providers.map(([value, p]) => (
              <option key={value} value={value} disabled={!tierAllows(p.min_tier)}>
                {p.display_name}
              </option>
            ))}
          </Select>
        </SettingRow>
        {search.depths.length > 0 && (
          <SettingRow
            label={t('settings.searchDepth')}
            description={search.depths.some((d) => !tierAllows(d.min_tier))
              ? <>{t('settings.searchDepthDesc')} <span>{t('settings.searchDepthUpgradeHint')}</span></>
              : t('settings.searchDepthDesc')}
            htmlFor={depthId}
            wide
          >
            <Select
              id={depthId}
              value={search.depth}
              onChange={(e) => { void write(searchDepthPatch(e.target.value)); }}
              disabled={!search.depths.some((d) => tierAllows(d.min_tier))}
            >
              <option value="">{t('settings.searchDepthDefault')}</option>
              {search.depths.map((d) => (
                <option key={d.name} value={d.name} disabled={!tierAllows(d.min_tier)}>
                  {t(`settings.searchDepthLevel.${d.name}`, d.display_name)}
                </option>
              ))}
            </Select>
          </SettingRow>
        )}
      </SettingsGroup>
    </>
  );
}

/** One of the user's own instructions: text edits in place, and anything
 *  else is shown as it reads, to remove only. That includes the agent's
 *  other shapes and words under a key with choices, which the server keeps
 *  as they were but would refuse as an edit. */
function Instruction({ name, value, onSave, onRemove }: {
  name: string;
  value: unknown;
  onSave?: (text: string) => void;
  onRemove: () => void;
}) {
  const { t } = useTranslation();
  const fieldId = useId();
  const [draft, setDraft] = useState(typeof value === 'string' ? value : '');
  const editable = onSave && typeof value === 'string';
  const save = () => {
    const text = draft.trim();
    if (!text) setDraft(value as string);
    else if (text !== value) onSave?.(text);
  };
  return (
    <SettingRow label={name} htmlFor={editable ? fieldId : undefined} wide>
      <div className="flex items-center gap-1.5">
        {editable ? (
          <Input
            id={fieldId}
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onBlur={save}
            onKeyDown={(e) => { if (e.key === 'Enter' && !isImeComposing(e)) e.currentTarget.blur(); }}
            className="h-8"
          />
        ) : (
          <span className="min-w-0 flex-1 text-[0.8125rem]" style={{ color: 'var(--color-text-secondary)', wordBreak: 'break-word' }}>
            {typeof value === 'string' ? value : Array.isArray(value) ? value.join(', ') : JSON.stringify(value)}
          </span>
        )}
        <button
          type="button"
          onClick={onRemove}
          aria-label={t('settings.instructionsRemove', { name })}
          className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md text-(--color-text-tertiary) hover:bg-(--color-bg-hover) hover:text-(--color-text-primary)"
        >
          <X className="h-4 w-4" />
        </button>
      </div>
    </SettingRow>
  );
}
