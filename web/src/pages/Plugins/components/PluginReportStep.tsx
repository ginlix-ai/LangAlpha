import { useState } from 'react';
import type { TFunction } from 'i18next';
import { Trans, useTranslation } from 'react-i18next';
import { Link } from 'react-router';
import { ChevronRight, KeyRound } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { useLocale } from '@/hooks/useLocale';
import { formatList } from '@/lib/format';
import type {
  PluginComponentResult,
  PluginComponentStatus,
  PluginDiagnostic,
  PluginInstallReport,
} from '@/pages/ChatAgent/utils/api';
import { detailHref } from '../utils/detailParam';
import { secretEditHref } from '../utils/secretParam';
import { capitalizeFirst } from '../utils/sentence';
import { DiagnosticLine } from './RefusalView';

/**
 * What an install or update did, in the order a user asks it: did it work
 * (one sentence, counting what landed), what was saved where, then each
 * connector and skill with what happened to it in a word. Rows that ended
 * up in the user's lists link to them.
 *
 * The findings that matter to whoever wrote the package (spec warnings,
 * files that were not imported) sit in a closed disclosure: they are true,
 * and they say nothing a user installing someone else's plugin can act on.
 * What a user does have to know stays out in the open: a finding about
 * credentials the plugin reaches for, and an error no row already says.
 *
 * sse entries the probe found upgradable render with a consent checkbox:
 * installing them as streamable-http is the user's call, made here, never
 * silently.
 */

/** In the user's lists now, from this plugin. */
const IN_PLACE: ReadonlySet<PluginComponentStatus> = new Set(['created', 'updated', 'unchanged']);
/** Only an update reports these: they describe a component that was there. */
const UPDATE_ONLY: ReadonlySet<PluginComponentStatus> = new Set([
  'updated',
  'unchanged',
  'deleted',
  'detached',
]);
/** There is a row by this name to open, this plugin's or the user's own. */
const HAS_ROW: ReadonlySet<PluginComponentStatus> = new Set([
  'created',
  'updated',
  'unchanged',
  'exists',
  'detached',
]);
/** What the user has to see even when installing someone else's package:
 *  which of their credentials the plugin reaches for, and where. */
const USER_NOTICES: ReadonlySet<string> = new Set([
  'secret_preexisting',
  'secret_undeclared',
  'secret_not_carried',
]);

const STATUS_WORD: Record<Exclude<PluginComponentStatus, 'upgradable'>, string> = {
  created: 'statusAdded',
  updated: 'statusUpdated',
  unchanged: 'statusUnchanged',
  exists: 'statusExists',
  skipped: 'statusSkipped',
  invalid: 'statusFailed',
  error: 'statusFailed',
  deleted: 'statusDeleted',
  detached: 'statusDetached',
};

const HREF_KIND = { mcp: 'server', skill: 'skill', subagent: 'subagent' } as const;

function componentHref(result: PluginComponentResult): string {
  return detailHref({ kind: HREF_KIND[result.kind], name: result.name });
}

function ComponentRow({
  result,
  onLeave,
}: {
  result: PluginComponentResult;
  onLeave: () => void;
}) {
  const { t } = useTranslation();
  const label = result.name || result.key;
  const failed = result.status === 'invalid' || result.status === 'error';
  const status = result.status === 'upgradable' ? null : STATUS_WORD[result.status];
  // "Already had one" is the whole story; the server's sentence for it only
  // says the same thing longer.
  const reason = result.status === 'exists' ? '' : capitalizeFirst(result.reason);
  return (
    <li className="flex flex-col gap-0.5 py-1.5">
      <div className="flex items-baseline gap-3 min-w-0">
        {HAS_ROW.has(result.status) && result.name ? (
          <Link
            to={componentHref(result)}
            onClick={onLeave}
            className="text-xs font-medium truncate underline-offset-2 hover:underline"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {label}
          </Link>
        ) : (
          <span className="text-xs font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
            {label}
          </span>
        )}
        {status && (
          <span
            className="ml-auto shrink-0 text-[0.6875rem]"
            style={{ color: failed ? 'var(--color-loss)' : 'var(--color-text-tertiary)' }}
          >
            {t(`plugins.install.${status}`)}
          </span>
        )}
      </div>
      {reason && (
        <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
          {reason}
        </p>
      )}
    </li>
  );
}

function Group({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section className="flex flex-col gap-1">
      <h4 className="text-[0.6875rem] font-medium" style={{ color: 'var(--color-text-tertiary)' }}>
        {title}
      </h4>
      <div
        className="flex flex-col rounded-md px-3 py-0.5"
        style={{ backgroundColor: 'var(--color-bg-card)' }}
      >
        {children}
      </div>
    </section>
  );
}

const diagnosticKey = (d: PluginDiagnostic, i: number) => `${d.code}-${d.target}-${i}`;

/** Rows read top down from what the user got to what they did not. */
const ROW_ORDER: Record<PluginComponentStatus, number> = {
  created: 0,
  updated: 0,
  unchanged: 0,
  exists: 1,
  detached: 1,
  skipped: 2,
  invalid: 3,
  error: 3,
  deleted: 4,
  upgradable: 5,
};

/** How a package with no plugin.json was read: a folder of skills, of
 *  subagents (a top-level agents/), or of both. */
function packagedAs(
  t: TFunction,
  components: PluginComponentResult[],
  plugin: string,
  locale: string,
): string {
  // A deleted row is what an update took away, not something the folder
  // carries: counting it named two subagents for a folder of one.
  const carried = components.filter((c) => c.status !== 'deleted');
  const skills = carried.filter((c) => c.kind === 'skill').length;
  const subagents = carried.filter((c) => c.kind === 'subagent').length;
  if (subagents === 0) return t('plugins.install.packagedAs', { count: skills, plugin });
  if (skills === 0) return t('plugins.install.packagedAsSubagents', { count: subagents, plugin });
  const parts = formatList(
    [
      t('plugins.install.skillCount', { count: skills }),
      t('plugins.install.subagentCount', { count: subagents }),
    ],
    locale,
  );
  return t('plugins.install.packagedAsBoth', { parts, plugin });
}

export function PluginReportStep({
  report,
  plugin,
  savedKeys = [],
  onUpgradeSse,
  upgrading,
  upgradeError = null,
  onLeave,
}: {
  report: PluginInstallReport;
  /** The plugin the report is about, named in its first sentence. */
  plugin: string;
  /** Keys the user typed for this install, saved to Secrets with it. */
  savedKeys?: string[];
  /** Present when held-back sse entries can be consented into http. */
  onUpgradeSse?: (keys: string[]) => void;
  upgrading?: boolean;
  /** Why the last sse upgrade was refused, said beside its button. */
  upgradeError?: string | null;
  /** A link out of the report was followed: the dialog closes behind it. */
  onLeave: () => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const [checkedKeys, setCheckedKeys] = useState<Set<string>>(new Set());

  const { components, diagnostics } = report;
  // An update is told by what it did to components that were already there.
  // An update that only added would read as an install, which is what it did.
  const update = components.some((c) => UPDATE_ONLY.has(c.status));
  const inPlace = components.filter((c) => IN_PLACE.has(c.status));
  const connectorCount = inPlace.filter((c) => c.kind === 'mcp').length;
  const skillCount = inPlace.filter((c) => c.kind === 'skill').length;
  const subagentCount = inPlace.filter((c) => c.kind === 'subagent').length;
  const parts = [
    connectorCount > 0 ? t('plugins.install.connectorCount', { count: connectorCount }) : null,
    skillCount > 0 ? t('plugins.install.skillCount', { count: skillCount }) : null,
    subagentCount > 0 ? t('plugins.install.subagentCount', { count: subagentCount }) : null,
  ].filter((part): part is string => part !== null);
  const lead =
    parts.length > 0
      ? t(update ? 'plugins.install.leadUpdated' : 'plugins.install.leadInstalled', {
          plugin,
          parts: formatList(parts, locale),
        })
      : t(update ? 'plugins.install.leadUpdatedNothing' : 'plugins.install.leadInstalledNothing', {
          plugin,
        });

  const rows = (kind: PluginComponentResult['kind']) =>
    components
      .filter((c) => c.kind === kind && c.status !== 'upgradable')
      .sort((a, b) => ROW_ORDER[a.status] - ROW_ORDER[b.status]);
  const connectorRows = rows('mcp');
  const skillRows = rows('skill');
  const subagentRows = rows('subagent');
  const upgradable = components.filter((c) => c.status === 'upgradable');

  const packaged = diagnostics.find((d) => d.code === 'synthesized_manifest');
  const saved = [...new Set([...savedKeys, ...report.secrets_created])];
  const notices = diagnostics.filter((d) => USER_NOTICES.has(d.code));
  // An error a row already says (a skill that would not validate) is not said
  // twice; one no row carries stays in the open, never with the authors'
  // notes, since what it reports is something the user does not have.
  const reasons = new Set(components.map((c) => c.reason));
  const errors = diagnostics.filter((d) => d.level === 'error' && !reasons.has(d.message));
  const forAuthors = diagnostics.filter(
    (d) => d !== packaged && d.level !== 'error' && !USER_NOTICES.has(d.code),
  );
  const componentWarnings = components.flatMap((c) =>
    c.warnings.map((warning) => ({ name: c.name || c.key, warning })),
  );
  const authorNotes =
    forAuthors.length + componentWarnings.length + report.dropped_files.length > 0;
  // An update that found nothing to do reports nothing: say so, rather than
  // showing a panel that is empty for a reason the reader has to guess.
  const empty =
    components.length === 0 &&
    diagnostics.length === 0 &&
    report.dropped_files.length === 0 &&
    saved.length === 0;

  const componentList = (list: PluginComponentResult[]) => (
    <ul className="flex flex-col divide-y divide-(--color-border-muted)">
      {list.map((c) => (
        <ComponentRow key={`${c.kind}:${c.key}`} result={c} onLeave={onLeave} />
      ))}
    </ul>
  );

  return (
    <div className="flex flex-col gap-4">
      {empty ? (
        <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('plugins.install.reportNoChanges')}
        </p>
      ) : (
        <div className="flex flex-col gap-1">
          <p className="text-sm" style={{ color: 'var(--color-text-primary)' }}>
            {lead}
          </p>
          {packaged && (
            <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {packagedAs(t, components, plugin, locale)}
            </p>
          )}
        </div>
      )}

      {saved.length > 0 && (
        <ul className="flex flex-col gap-1">
          {saved.map((name) => (
            <li
              key={name}
              className="flex items-center gap-1.5 text-xs min-w-0"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              <KeyRound
                className="h-3.5 w-3.5 shrink-0"
                style={{ color: 'var(--color-text-tertiary)' }}
              />
              <span className="min-w-0 truncate">
                <Trans
                  i18nKey="plugins.install.savedKey"
                  values={{ name }}
                  components={{ name: <span className="font-mono" /> }}
                />
              </span>
              <span aria-hidden="true" style={{ color: 'var(--color-text-tertiary)' }}>
                ·
              </span>
              <Link
                to={secretEditHref(name)}
                onClick={onLeave}
                aria-label={t('plugins.install.manageKeyLabel', { name })}
                className="shrink-0 underline underline-offset-2 transition-colors hover:text-(--color-text-primary)"
              >
                {t('plugins.install.manageKey')}
              </Link>
            </li>
          ))}
        </ul>
      )}

      {errors.length + notices.length > 0 && (
        <div className="flex flex-col">
          {errors.map((d, i) => (
            <DiagnosticLine key={diagnosticKey(d, i)} diagnostic={d} color="var(--color-loss)" />
          ))}
          {notices.map((d, i) => (
            <DiagnosticLine
              key={diagnosticKey(d, i)}
              diagnostic={d}
              color="var(--color-text-secondary)"
            />
          ))}
        </div>
      )}

      {connectorRows.length > 0 && (
        <Group title={t('plugins.install.groupConnectors')}>{componentList(connectorRows)}</Group>
      )}
      {skillRows.length > 0 && (
        <Group title={t('plugins.install.groupSkills')}>{componentList(skillRows)}</Group>
      )}
      {subagentRows.length > 0 && (
        <Group title={t('plugins.install.groupSubagents')}>{componentList(subagentRows)}</Group>
      )}

      {upgradable.length > 0 && (
        <Group title={t('plugins.install.reportUpgradable')}>
          <p
            className="text-[0.6875rem] py-1"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {t('plugins.install.sseUpgradeHint')}
          </p>
          {upgradable.map((c) => (
            <label
              key={c.key}
              className="flex items-center gap-2 py-1 cursor-pointer"
            >
              <input
                type="checkbox"
                checked={checkedKeys.has(c.key)}
                onChange={(e) => {
                  const next = new Set(checkedKeys);
                  if (e.target.checked) next.add(c.key);
                  else next.delete(c.key);
                  setCheckedKeys(next);
                }}
              />
              <span className="text-xs" style={{ color: 'var(--color-text-primary)' }}>
                {c.name || c.key}
              </span>
            </label>
          ))}
          {onUpgradeSse && (
            <div className="flex justify-end py-1">
              <button
                type="button"
                onClick={() => {
                  // Clear before handing off: the list re-renders from the new
                  // report, and keys that just landed would otherwise stay
                  // ticked and be re-submitted by a second confirm.
                  const keys = [...checkedKeys];
                  setCheckedKeys(new Set());
                  onUpgradeSse(keys);
                }}
                disabled={checkedKeys.size === 0 || upgrading}
                className="inline-flex items-center gap-1.5 px-2 py-1 text-[0.6875rem] rounded-md disabled:opacity-50"
                style={{
                  color: 'var(--color-text-primary)',
                  border: '1px solid var(--color-border-muted)',
                }}
              >
                {upgrading && <Loader size={12} className="text-current" />}
                {t('plugins.install.sseUpgradeConfirm')}
              </button>
            </div>
          )}
          {upgradeError && (
            <p className="text-[0.6875rem] pb-1.5" style={{ color: 'var(--color-loss)' }}>
              {t('plugins.install.sseUpgradeFailed')}
              {t('mcp.check.separator')}
              {upgradeError}
            </p>
          )}
        </Group>
      )}

      {authorNotes && (
        <details className="group">
          <summary
            className="inline-flex items-center gap-1 text-xs cursor-pointer select-none list-none rounded transition-colors hover:text-(--color-text-secondary) [&::-webkit-details-marker]:hidden"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            <ChevronRight className="h-3.5 w-3.5 transition-transform group-open:rotate-90" />
            {t('plugins.install.authorDetails')}
          </summary>
          <div className="flex flex-col pt-1 pl-[1.125rem]">
            {forAuthors.map((d, i) => (
              <DiagnosticLine key={diagnosticKey(d, i)} diagnostic={d} />
            ))}
            {componentWarnings.map(({ name, warning }) => (
              <p
                key={`${name}:${warning}`}
                className="text-[0.6875rem] py-1"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {warning.startsWith(name) ? warning : `${name}: ${warning}`}
              </p>
            ))}
            {report.dropped_files.length > 0 && (
              <p className="text-[0.6875rem] py-1" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('plugins.install.droppedFiles', {
                  files: report.dropped_files.join(', '),
                })}
              </p>
            )}
          </div>
        </details>
      )}
    </div>
  );
}
