import { useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { useUpgradePluginSseEntries } from '@/hooks/usePlugins';
import {
  formatApiErrorDetail,
  type PluginDiagnostic,
  type PluginInstallReport,
  type PluginInstallResponse,
} from '@/pages/ChatAgent/utils/api';
import { PluginDialog } from './PluginDialog';
import { PluginReportStep } from './PluginReportStep';

/**
 * The tail of every install-shaped response: the report. Install and update
 * return the same `PluginInstallResponse`, so both show it through
 * `usePluginOutcome` rather than each deciding for itself how much of the
 * response is worth showing; an update reaches the sse-upgrade consent because
 * the response carries it, not because a second surface was built to carry it.
 *
 * The keys a plugin declares are asked for before it installs, never after:
 * its servers are checked with them first, and nothing installs until they
 * pass.
 */

const unique = (values: string[]) => [...new Set(values)];

/** Same job as `unique`, for the declaration lists, which are objects. */
const uniqueByName = <T extends { name: string }>(values: T[]) =>
  values.filter((v, i, all) => all.findIndex((o) => o.name === v.name) === i);

/** Identity of a finding, for the dedupe below. */
const diagnosticKey = (d: PluginDiagnostic) =>
  `${d.level}|${d.scope}|${d.target}|${d.code}|${d.message}`;

/**
 * Fold an sse-upgrade follow-up into the report on screen.
 *
 * The follow-up reports only the entries just consented, never a fresh
 * statement about the whole plugin, so every field accumulates rather than
 * replaces: components merge by key because the follow-up restates those,
 * the name lists union because each side saw only its own half, and the
 * counts sum because each counted what it created. Replacing `secrets_required`
 * would drop what the first half reported.
 *
 * Diagnostics dedupe because an upgrade re-derives its plans from the stored
 * manifest, so a finding from install time comes back verbatim.
 *
 * Every field is named rather than spread from `base`: a field added to the
 * report later should fail this merge to compile, not silently keep the
 * pre-upgrade value.
 */
function mergeReport(
  base: PluginInstallReport,
  followUp: PluginInstallReport,
): PluginInstallReport {
  const byKey = new Map(followUp.components.map((c) => [`${c.kind}:${c.key}`, c]));
  const restated = base.components.map((c) => byKey.get(`${c.kind}:${c.key}`) ?? c);
  // A follow-up row for a key the base never carried (a key the plugin no
  // longer holds back comes home as an error row) still has to be seen.
  const added = followUp.components.filter(
    (c) => !base.components.some((b) => b.kind === c.kind && b.key === c.key),
  );
  return {
    components: [...restated, ...added],
    diagnostics: [...base.diagnostics, ...followUp.diagnostics].filter(
      (d, i, all) => all.findIndex((o) => diagnosticKey(o) === diagnosticKey(d)) === i,
    ),
    secrets_created: unique([...base.secrets_created, ...followUp.secrets_created]),
    secrets_required: uniqueByName([
      ...base.secrets_required,
      ...followUp.secrets_required,
    ]),
    dropped_files: unique([...base.dropped_files, ...followUp.dropped_files]),
    servers_created: base.servers_created + followUp.servers_created,
    skills_created: base.skills_created + followUp.skills_created,
    subagents_created: (base.subagents_created ?? 0) + (followUp.subagents_created ?? 0),
  };
}

/** An install or update that went through, and the keys typed for it. */
export interface PluginOutcomeOf {
  response: PluginInstallResponse;
  /** The keys the user typed for this attempt, saved to Secrets with it. */
  savedKeys: string[];
}

interface OutcomeActions {
  onDone: () => void;
  /** Given where the plugin is somewhere else to go (an install); absent on
   *  an update, which was opened from the plugin itself. */
  onOpenPlugin?: (name: string) => void;
}

export interface PluginOutcomeView {
  /** An sse upgrade is in flight; leaving would drop its report. */
  upgrading: boolean;
  body: ReactNode;
  footer: ReactNode;
}

/**
 * The report for `outcome`, and the buttons under it, for whichever dialog
 * shows it. Consenting to an sse upgrade rewrites the report in place, so the
 * report on screen is held here, against the outcome it grew from: a new
 * outcome starts from its own response.
 */
export function usePluginOutcome(outcome: PluginOutcomeOf, actions: OutcomeActions): PluginOutcomeView;
export function usePluginOutcome(
  outcome: PluginOutcomeOf | null,
  actions: OutcomeActions,
): PluginOutcomeView | null;
export function usePluginOutcome(
  outcome: PluginOutcomeOf | null,
  { onDone, onOpenPlugin }: OutcomeActions,
): PluginOutcomeView | null {
  const { t } = useTranslation();
  const upgradeSse = useUpgradePluginSseEntries();
  const [upgraded, setUpgraded] = useState<{
    of: PluginOutcomeOf;
    result: PluginInstallResponse | null;
    error: string | null;
  } | null>(null);
  if (!outcome) return null;
  const mine = upgraded?.of === outcome ? upgraded : null;
  const result = mine?.result ?? outcome.response;
  const upgrading = upgradeSse.isPending;

  async function upgrade(keys: string[]) {
    if (!outcome) return;
    setUpgraded({ of: outcome, result, error: null });
    try {
      const followUp = await upgradeSse.mutateAsync({ name: result.plugin.name, keys });
      const next = { plugin: followUp.plugin, report: mergeReport(result.report, followUp.report) };
      setUpgraded({ of: outcome, result: next, error: null });
    } catch (err) {
      setUpgraded({ of: outcome, result, error: formatApiErrorDetail(err) });
    }
  }

  const primary = {
    color: 'var(--color-btn-primary-text)',
    backgroundColor: 'var(--color-btn-primary-bg)',
  };
  return {
    upgrading,
    body: (
      <PluginReportStep
        report={result.report}
        plugin={result.plugin.name}
        savedKeys={outcome.savedKeys}
        onUpgradeSse={(keys) => void upgrade(keys)}
        upgrading={upgrading}
        upgradeError={mine?.error ?? null}
        onLeave={onDone}
      />
    ),
    // Where the plugin can be opened, opening it is the next thing to do, and
    // Done steps back to a quiet second place. Both are held mid-upgrade:
    // leaving unmounts the step that is going to report what landed.
    footer: (
      <div className="flex justify-end gap-2">
        <button
          type="button"
          onClick={onDone}
          disabled={upgrading}
          className={
            onOpenPlugin
              ? 'px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10 disabled:opacity-50'
              : 'px-3 py-1.5 text-xs rounded-md disabled:opacity-50'
          }
          style={onOpenPlugin ? { color: 'var(--color-text-secondary)' } : primary}
        >
          {t('common.done')}
        </button>
        {onOpenPlugin && (
          <button
            type="button"
            onClick={() => onOpenPlugin(result.plugin.name)}
            disabled={upgrading}
            className="px-3 py-1.5 text-xs rounded-md disabled:opacity-50"
            style={primary}
          >
            {t('plugins.install.openPlugin')}
          </button>
        )}
      </div>
    ),
  };
}

/** The report in a dialog of its own, for an install or update started
 *  outside the install wizard. */
export function PluginOutcome({
  outcome,
  title,
  ...actions
}: {
  outcome: PluginOutcomeOf;
  /** Names the action that produced the report (installed / updated). */
  title: string;
} & OutcomeActions) {
  const { t } = useTranslation();
  const view = usePluginOutcome(outcome, actions);
  return (
    <PluginDialog
      title={title}
      subtitle={t('plugins.install.stepReport')}
      dismissable={!view.upgrading}
      onClose={actions.onDone}
      footer={view.footer}
    >
      {view.body}
    </PluginDialog>
  );
}
