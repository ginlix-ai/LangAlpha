import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import {
  useBindPluginSecrets,
  useUpgradePluginSseEntries,
} from '@/hooks/usePlugins';
import {
  formatApiErrorDetail,
  type PluginDiagnostic,
  type PluginInstallReport,
  type PluginInstallResponse,
} from '@/pages/ChatAgent/utils/api';
import { PluginDialog } from './PluginDialog';
import { PluginReportStep } from './PluginReportStep';
import { PluginBindingsStep } from './PluginBindingsStep';

/**
 * The tail of every install-shaped response: the report, then the credentials
 * the package declared. Install and update return the same
 * `PluginInstallResponse`, so both mount this rather than each deciding for
 * itself how much of the response is worth showing — an update reaches the
 * sse-upgrade consent and the secret bindings because the response carries
 * them, not because a second surface was built to carry them too.
 */

type OutcomePhase =
  | { kind: 'report'; result: PluginInstallResponse }
  | { kind: 'bindings'; result: PluginInstallResponse; error: string | null };

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
 * would empty it outright and skip the bindings step.
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
  };
}

export function PluginOutcome({
  response,
  title,
  onDone,
}: {
  response: PluginInstallResponse;
  /** Names the action that produced the report (installed / updated). */
  title: string;
  onDone: () => void;
}) {
  const { t } = useTranslation();
  // Seeded from the response and then owned here: consenting to an sse upgrade
  // rewrites the report in place, so what is on screen outlives the mutation.
  const [phase, setPhase] = useState<OutcomePhase>({
    kind: 'report',
    result: response,
  });

  const upgradeSse = useUpgradePluginSseEntries();
  const bindSecrets = useBindPluginSecrets();

  async function handleUpgradeSse(keys: string[]) {
    if (phase.kind !== 'report') return;
    try {
      const followUp = await upgradeSse.mutateAsync({
        name: phase.result.plugin.name,
        keys,
      });
      setPhase({
        kind: 'report',
        result: {
          plugin: followUp.plugin,
          report: mergeReport(phase.result.report, followUp.report),
        },
      });
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.install.sseUpgradeFailed'),
        description: formatApiErrorDetail(err),
      });
    }
  }

  function continueFromReport() {
    if (phase.kind !== 'report') return;
    if (phase.result.report.secrets_required.length > 0) {
      setPhase({ kind: 'bindings', result: phase.result, error: null });
    } else {
      onDone();
    }
  }

  async function handleBind(secrets: Record<string, string>) {
    if (phase.kind !== 'bindings') return;
    try {
      await bindSecrets.mutateAsync({ name: phase.result.plugin.name, secrets });
      toast({
        title: t('plugins.install.boundTitle'),
        description: t('plugins.install.boundDesc', {
          count: Object.keys(secrets).length,
        }),
      });
      onDone();
    } catch (err) {
      setPhase({ ...phase, error: formatApiErrorDetail(err) });
    }
  }

  return (
    <PluginDialog
      title={title}
      subtitle={
        phase.kind === 'report'
          ? t('plugins.install.stepReport')
          : t('plugins.install.stepBindings')
      }
      onClose={onDone}
    >
      {phase.kind === 'report' ? (
        <PluginReportStep
          report={phase.result.report}
          onUpgradeSse={handleUpgradeSse}
          upgrading={upgradeSse.isPending}
          onContinue={continueFromReport}
          continueLabel={
            phase.result.report.secrets_required.length > 0
              ? t('plugins.install.continueToBindings')
              : t('common.done')
          }
        />
      ) : (
        <PluginBindingsStep
          required={phase.result.report.secrets_required}
          onBind={handleBind}
          binding={bindSecrets.isPending}
          error={phase.error}
          onSkip={onDone}
        />
      )}
    </PluginDialog>
  );
}
