import { useId } from 'react';
import type { TFunction } from 'i18next';
import { AlertTriangle, ChevronRight, Info } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useFeatures, useSetFeatureOverride } from '@/hooks/useFeatures';
import { useAccount } from '@/hooks/useAccount';
import { Button } from '@/components/ui/button';
import { Dialog, DialogClose, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog';
import { ToggleSwitch } from '@/components/ui/switch';
import { NameTag } from '@/components/model/PayerBadge';
import { cn } from '@/lib/utils';
import type { FeatureGuide, FeatureState } from '@/types/api';
import { SettingRow, SettingsCardBody, SettingsGroup } from '../components/SettingsLayout';
import { SETTINGS } from '../sections';
import { useSaveFailureToast } from './usePreferenceWrite';
import { experimentGroups, type ExperimentFeature, type ExperimentRow } from './featureGroups';

/** Experiments: user-overridable feature flags, data-driven from the
 *  feature-flag API, so a new flag needs no change here. Each feature says
 *  what it does, its default and what it costs, and opens the case for each
 *  side in a dialog, so the choice is the user's to make from what they do
 *  rather than a warning to heed. */
export function ExperimentsPanel() {
  const { data, isLoading } = useFeatures();
  const { t } = useTranslation();
  const groups = experimentGroups(data ?? []);
  // Wait out the features query before committing to the empty state, so it
  // never flashes before the real list lands.
  if (groups.length === 0 && isLoading) return null;

  if (groups.length === 0) {
    return (
      <SettingsGroup title={t('settings:experimentsGroup')} description={t(SETTINGS.experiments.descKey)}>
        <SettingsCardBody id={SETTINGS.experiments.anchor} className="py-6 text-center">
          <p className="text-[0.8125rem]" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('settings:experimentsEmpty')}
          </p>
        </SettingsCardBody>
      </SettingsGroup>
    );
  }

  return groups.map((group, index) => (
    <SettingsGroup
      key={group.key}
      id={index === 0 ? SETTINGS.experiments.anchor : undefined}
      title={t(`settings:features.groups.${group.key}.title`)}
      description={t(`settings:features.groups.${group.key}.description`)}
    >
      {group.rows.map((row) => row.feature.set_by_organization
        ? <OrgSetRow key={row.feature.key} feature={row.feature} nested={row.nested} />
        : <FeatureRow key={row.feature.key} row={row} />)}
    </SettingsGroup>
  ));
}

/** A feature's copy in the reader's language. The API serves the catalog's
 *  English; a locale translates a feature under `settings:featureCatalog.<key>`,
 *  and one it has not translated shows the English. */
function localize(t: TFunction, feature: FeatureState) {
  const text = (field: string, english: string) =>
    t(`settings:featureCatalog.${feature.key}.${field}`, { defaultValue: english });
  const { guide } = feature;
  return {
    label: text('label', feature.label),
    description: text('description', feature.description),
    guide: guide ? {
      on_if: text('on_if', guide.on_if),
      off_if: text('off_if', guide.off_if),
      cost: guide.cost && text('cost', guide.cost),
      good_to_know: guide.good_to_know && text('good_to_know', guide.good_to_know),
      caution: guide.caution && text('caution', guide.caution),
    } satisfies FeatureGuide : null,
  };
}

type FeatureCopy = ReturnType<typeof localize>;

/** The organization decides it and an override is refused, so the row says
 *  the state and who set it, with nothing to press or weigh. */
function OrgSetRow({ feature, nested }: { feature: ExperimentFeature; nested: boolean }) {
  const { t } = useTranslation();
  const { organizationName } = useAccount();
  const copy = localize(t, feature);
  return (
    <SettingRow
      className={cn(nested && 'is-nested')}
      label={<FeatureName label={copy.label} preview={!!feature.preview} />}
      description={t('organization.twoSentences', {
        first: copy.description,
        next: t(feature.enabled ? 'organization.features.keptOn' : 'organization.features.turnedOff', {
          who: organizationName ?? t('organization.who.yourOrganizationStart'),
        }),
      })}
      inline
    >
      <span className="text-[0.8125rem]" style={{ color: 'var(--color-text-secondary)' }}>
        {feature.enabled ? t('organization.features.on') : t('organization.features.off')}
      </span>
    </SettingRow>
  );
}

/** One feature. The whole row opens the case for each side: the name is the
 *  dialog's trigger, its hit area stretched over the row, and the switch sits
 *  above it, so a quick toggle needs no dialog. The chevron says the row opens
 *  something, as a computer's row in Computers does. Being the trigger, the
 *  name is where Radix returns focus when the dialog closes from the keyboard. */
function FeatureRow({ row: { feature, nested, blockedBy } }: { row: ExperimentRow }) {
  const { t } = useTranslation();
  const descId = useId();
  const cautionId = useId();
  const setOverride = useSetFeatureOverride();
  const reportFailure = useSaveFailureToast();
  const toggle = () => { void reportFailure(setOverride.mutateAsync({ key: feature.key, enabled: !feature.enabled })); };
  const copy = localize(t, feature);
  const needs = blockedBy && localize(t, blockedBy).label;

  return (
    <Dialog>
      <div className={cn('settings-row is-feature', nested && 'is-nested')}>
        <div className="settings-row-text">
          <DialogTrigger asChild>
            <button type="button" className="settings-row-label feature-open" aria-describedby={descId}>
              <FeatureName label={copy.label} preview={!!feature.preview} />
            </button>
          </DialogTrigger>
          <div className="settings-row-desc">
            <p id={descId}>{copy.description}</p>
            <p className="feature-facts">
              <span>
                {needs
                  ? t('settings:features.needs', { name: needs })
                  : t(feature.gate === 'opt_out' ? 'settings:features.onByDefault' : 'settings:features.offByDefault')}
              </span>
              {copy.guide?.cost && (
                <>
                  <span aria-hidden="true" className="feature-facts-dot">·</span>
                  <span>{copy.guide.cost}</span>
                </>
              )}
            </p>
            {copy.guide?.caution && <Caution id={cautionId} text={copy.guide.caution} />}
          </div>
        </div>
        <div className="settings-row-control">
          <ToggleSwitch
            checked={feature.enabled}
            disabled={!!needs}
            onChange={toggle}
            ariaLabel={copy.label}
            ariaDescribedBy={copy.guide?.caution ? `${descId} ${cautionId}` : descId}
          />
          <ChevronRight className="settings-row-chevron" aria-hidden="true" />
        </div>
      </div>
      <FeatureDecideDialog feature={feature} copy={copy} needs={needs} onToggle={toggle} />
    </Dialog>
  );
}

function FeatureName({ label, preview }: { label: string; preview: boolean }) {
  const { t } = useTranslation();
  return (
    <>
      {label}
      {preview && <NameTag className="ml-1.5 inline-block align-[1px]">{t('settings:features.preview')}</NameTag>}
    </>
  );
}

/** The case for each side of one feature, laid out to be read rather than
 *  scanned: the two sides as cards side by side, what holds either way under
 *  them, and the switch at the foot, so the reader decides where they read. */
function FeatureDecideDialog({ feature, copy, needs, onToggle }: {
  feature: ExperimentFeature;
  copy: FeatureCopy;
  /** The name of the feature holding this one off, if one is. */
  needs: string | null;
  onToggle: () => void;
}) {
  const { t } = useTranslation();
  const switchId = useId();
  const cautionId = useId();
  const { guide } = copy;
  return (
    <DialogContent className="feature-dialog sm:max-w-xl">
      {/* Left-aligned on a phone too: a centered paragraph is hard to read. */}
      <DialogHeader className="text-left">
        <DialogTitle className="text-base leading-snug">
          <FeatureName label={copy.label} preview={!!feature.preview} />
        </DialogTitle>
        <DialogDescription className="feature-dialog-lede">{copy.description}</DialogDescription>
      </DialogHeader>

      {guide && (
        <div className="grid gap-3 sm:grid-cols-2">
          <FeatureSide on heading={t(`settings:features.onIf.${feature.gate}`)} text={guide.on_if} />
          <FeatureSide on={false} heading={t(`settings:features.offIf.${feature.gate}`)} text={guide.off_if} />
        </div>
      )}

      <dl className="feature-dialog-facts">
        <dt>{t('settings:features.defaultLabel')}</dt>
        <dd>{t(feature.gate === 'opt_out' ? 'settings:features.on' : 'settings:features.off')}</dd>
        {guide?.cost && (
          <>
            <dt>{t('settings:features.costLabel')}</dt>
            <dd>{guide.cost}</dd>
          </>
        )}
      </dl>

      {guide?.caution && <Caution id={cautionId} text={guide.caution} />}

      {guide?.good_to_know && (
        <p className="feature-dialog-know">
          <Info aria-hidden="true" className="mt-[3px] h-3.5 w-3.5 shrink-0" />
          <span>{guide.good_to_know}</span>
        </p>
      )}

      <div className="feature-dialog-foot">
        <div className="flex min-w-0 items-center gap-2.5">
          <ToggleSwitch
            id={switchId}
            checked={feature.enabled}
            disabled={!!needs}
            onChange={onToggle}
            ariaLabel={copy.label}
            ariaDescribedBy={guide?.caution ? cautionId : undefined}
          />
          <label htmlFor={switchId} className="min-w-0 text-sm" style={{ color: 'var(--color-text-primary)' }}>
            {needs
              ? t('settings:features.needs', { name: needs })
              : t(feature.enabled ? 'settings:features.isOn' : 'settings:features.isOff', { name: copy.label })}
          </label>
        </div>
        <DialogClose asChild>
          <Button variant="outline" size="sm">{t('common.done')}</Button>
        </DialogClose>
      </div>
    </DialogContent>
  );
}

/** One side of the case, headed by a switch drawn small in that position, so
 *  "on" and "off" read at a glance before the words. */
function FeatureSide({ on, heading, text }: { on: boolean; heading: string; text: string }) {
  return (
    <section className="feature-side">
      <h3 className="feature-side-head">
        <span aria-hidden="true" className={cn('feature-switch-glyph', on && 'is-on')} />
        {heading}
      </h3>
      <p>{text}</p>
    </section>
  );
}

/** What switching a feature loses: the one line in the warning voice, since
 *  it is the one thing on the page that is a warning. Each switch that can do
 *  it names this line as its description, so a screen reader says it too. */
function Caution({ id, text }: { id: string; text: string }) {
  return (
    <p id={id} className="feature-caution">
      <AlertTriangle aria-hidden="true" className="mt-[2px] h-3.5 w-3.5 shrink-0" />
      <span>{text}</span>
    </p>
  );
}
