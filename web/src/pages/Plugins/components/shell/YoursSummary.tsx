import { Fragment, type ReactNode } from 'react';
import { Link } from 'react-router';
import { Trans, useTranslation } from 'react-i18next';
import type { TFunction } from 'i18next';
import { useAllModels } from '@/hooks/useAllModels';
import { effortLabelFor } from '@/lib/modelTuning';
import type { PluginInfo, SubagentsResponse } from '@/pages/ChatAgent/utils/api';
import { failureBucket, type FailureBucket, type PluginProblem } from '../../utils/connectorState';
import { StatusDot } from '../surface/Surface';
import { modelName } from '../subagents/subagentText';
import type { Section } from './sections';
import type { ShellData } from './useShellData';

/** Plugins named before the rest fold into "N more". */
const NAMED_PROBLEMS = 3;

/**
 * The status line under a section's title on Yours: how much there is, then
 * what needs the user, behind an attention dot. With nothing to fix it says
 * what is switched off instead, quietly, or nothing more.
 *
 * Read off `useShellData`, the same tallies the rail's dots come from, so the
 * line and the dot beside the section never disagree.
 */
export function YoursSummary({ section, data }: { section: Section; data: ShellData }) {
  const { t } = useTranslation();
  const line = summaryParts(t, section, data);
  if (!line) return <p className="plugins-summary" aria-hidden>&nbsp;</p>;
  const sep = t('plugins.shell.summary.sep');
  return (
    <p className="plugins-summary">
      <span>{line.lead}</span>
      {line.attention.length > 0 ? (
        <>
          <span className="plugins-summary-sep" aria-hidden>·</span>
          <StatusDot tone="attn" />
          <b>
            {line.attention.map((clause, i) => (
              <Fragment key={i}>
                {i > 0 && sep}
                <ClauseText clause={clause} />
              </Fragment>
            ))}
          </b>
        </>
      ) : line.quiet ? (
        <>
          <span className="plugins-summary-sep" aria-hidden>·</span>
          <span className="plugins-summary-quiet">{line.quiet}</span>
        </>
      ) : null}
    </p>
  );
}

/** One thing that needs the user, and the plugin it names, if any. */
interface Clause {
  text: string;
  name?: string;
}

interface SummaryLine {
  lead: ReactNode;
  /** What needs the user, each a clause; joined with the locale's comma. */
  attention: Clause[];
  /** Said only when nothing needs attention. */
  quiet?: ReactNode;
}

/**
 * A clause with its plugin's name held together: on a phone the line wraps as
 * a sentence, and a name like `financial-datasets` otherwise breaks at its
 * hyphen into two words that read as neither.
 */
function ClauseText({ clause }: { clause: Clause }) {
  const at = clause.name ? clause.text.indexOf(clause.name) : -1;
  if (!clause.name || at < 0) return <>{clause.text}</>;
  return (
    <>
      {clause.text.slice(0, at)}
      <span className="plugins-summary-name">{clause.name}</span>
      {clause.text.slice(at + clause.name.length)}
    </>
  );
}

// Bare literals so the locale parity test sees every key.
const PLUGIN_CLAUSE: Record<FailureBucket, string> = {
  key: 'plugins.shell.summary.pluginKey',
  rejected: 'plugins.shell.summary.pluginKeyRejected',
  unreachable: 'plugins.shell.summary.pluginUnreachable',
  signIn: 'plugins.shell.summary.pluginSignIn',
};

function pluginClause(
  t: TFunction,
  plugin: PluginInfo,
  problem: PluginProblem,
  keyMissing: boolean,
): Clause {
  const bucket: FailureBucket =
    problem.kind === 'failing' ? failureBucket(problem.noteKey) : keyMissing ? 'key' : 'signIn';
  return { text: t(PLUGIN_CLAUSE[bucket], { name: plugin.name }), name: plugin.name };
}


function summaryParts(t: TFunction, section: Section, data: ShellData): SummaryLine | null {
  switch (section) {
    case 'plugins': {
      const p = data.plugins;
      if (!p) return null;
      // One count: the groups below already split it into catalog and built in.
      const lead = t('plugins.shell.summary.plugins', {
        count: p.installed.length + p.builtIn.length,
      });
      const troubled = [...p.installed, ...p.builtIn].flatMap((plugin) => {
        const problem = p.problems.get(plugin.name);
        return problem ? [pluginClause(t, plugin, problem, p.keyMissing.has(plugin.name))] : [];
      });
      const attention = troubled.slice(0, NAMED_PROBLEMS);
      if (troubled.length > NAMED_PROBLEMS) {
        attention.push({
          text: t('plugins.shell.summary.andMore', { count: troubled.length - NAMED_PROBLEMS }),
        });
      }
      return {
        lead,
        attention,
        quiet: p.off > 0 ? t('plugins.shell.summary.off', { count: p.off }) : undefined,
      };
    }
    case 'mcp': {
      const c = data.connectors;
      if (!c) return null;
      const attention = [
        c.signIn > 0 && t('plugins.shell.summary.needSignIn', { count: c.signIn }),
        c.key > 0 && t('plugins.shell.summary.needKey', { count: c.key }),
        c.rejected > 0 && t('plugins.shell.summary.keyRejected', { count: c.rejected }),
        c.unreachable > 0 && t('plugins.shell.summary.unreachable', { count: c.unreachable }),
      ]
        .filter((s): s is string => !!s)
        .map((text) => ({ text }));
      return {
        // The rail's number, with the part the app ships named, so a count
        // larger than what the user added explains itself.
        lead: c.builtIn
          ? t('plugins.shell.summary.connectorsWithBuiltIn', { count: c.total, builtIn: c.builtIn })
          : t('plugins.shell.summary.connectors', { count: c.total }),
        attention,
        quiet: c.off > 0 ? t('plugins.shell.summary.off', { count: c.off }) : undefined,
      };
    }
    case 'skills': {
      const s = data.skills;
      if (!s) return null;
      return {
        lead: t('plugins.shell.summary.skills', { count: s.total }),
        attention: [],
        quiet: s.off > 0 ? t('plugins.shell.summary.off', { count: s.off }) : undefined,
      };
    }
    case 'subagents': {
      const s = data.subagents;
      if (!s) return null;
      // A row's problem is on the row and lights the rail; the line says what
      // the agent handing them work runs on, the one thing no row shows.
      return {
        lead: t('plugins.shell.summary.subagents', { count: s.total }),
        attention: [],
        quiet: <MainAgentClause main={s.main} />,
      };
    }
    case 'brokerages': {
      const b = data.brokerages;
      if (!b) return null;
      return {
        lead:
          b.connected > 0
            ? t('plugins.shell.summary.connected', { count: b.connected })
            : t('plugins.shell.summary.noneConnected'),
        attention:
          b.signIn > 0 ? [{ text: t('plugins.shell.summary.needSignIn', { count: b.signIn }) }] : [],
      };
    }
    case 'secrets': {
      if (data.secrets === null) return null;
      return {
        lead:
          data.secrets > 0
            ? t('plugins.shell.summary.secrets', { count: data.secrets })
            : t('plugins.shell.summary.noSecrets'),
        attention: [],
      };
    }
  }
}

/**
 * "The agent itself runs on Sonnet 5.5 at Medium, set in Settings": the
 * model and effort a subagent left on Thread's model or Thread's setting
 * starts from on a new thread.
 */
function MainAgentClause({ main }: { main: SubagentsResponse['main'] }) {
  const { t } = useTranslation();
  const { metadata } = useAllModels();
  const settings = <Link to="/settings?tab=model" className="plugins-summary-link" />;
  const model = main.model ? modelName(main.model, metadata) : null;
  const effort = effortLabelFor(t, main.effort);
  if (!model) {
    return <Trans i18nKey="plugins.shell.summary.mainDefault" components={{ settings }} />;
  }
  return effort ? (
    <Trans
      i18nKey="plugins.shell.summary.mainRunsOn"
      values={{ model, effort }}
      components={{ b: <b />, settings }}
    />
  ) : (
    <Trans
      i18nKey="plugins.shell.summary.mainRunsOnModel"
      values={{ model }}
      components={{ b: <b />, settings }}
    />
  );
}
