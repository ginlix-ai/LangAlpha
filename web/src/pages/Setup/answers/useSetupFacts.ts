/**
 * What setup knows about the user, read from the record and the user's
 * computers, tokens and providers, as the step rule takes it, and where it
 * resumes (`next`, `nextPath`). Any setup page reads it, the Mac route's
 * included; `refetchInterval` is for a page waiting on another surface to
 * move the record.
 *
 * The record is read first and alone. The rest (computers, providers,
 * Claude Code) is read only while setup runs on the answers, so an account
 * that finished setup, or never answered, waits on one cheap read.
 */
import { useQueryClient } from '@tanstack/react-query';
import { useConfiguredProviders, SERVER_KEY } from '@/hooks/useConfiguredProviders';
import { useAccount } from '@/hooks/useAccount';
import { allows, RULE } from '@/lib/accountRules';
import type { ClaudeCodeComputer } from '@/api/model';
import { useClaudeCodeComputers, useClaudeCodeStatus } from '@/hooks/useClaudeCode';
import { useClaudeCodeCustody } from '@/hooks/useClaudeCodeCustody';
import { SAVED_TOKEN, TOKEN_WORKS, computerTokenTarget, computerTokenWorks, type TokenTarget } from '@/lib/claudeCode';
import { providerIsOwnMachine } from '@/pages/ChatAgent/components/providerUi';
import { useComputers } from '@/pages/ChatAgent/hooks/useComputers';
import type { Computer } from '@/types/api';
import { useOnboardingProgress } from '@/hooks/useOnboardingProgress';
import { isPlatformMode } from '@/config/hostMode';
import { nextSetupPath, nextSetupStep, readAnswers, setupRail, type SetupFacts } from './setupSteps';

/** A machine the user brought (SSH, this Mac through Remote control) that is
 *  still there to run tasks. */
export const isOwnComputer = (c: Computer): boolean => providerIsOwnMachine(c.kind) && c.status !== 'error' && c.status !== 'deleted';

/** The machine of the user's own Home runs on: the primary, while it is one.
 *  A cloud answer leaves it there (this Mac paired before the questions). */
export const homeMachine = (computers: readonly Computer[]): Computer | null =>
  computers.find((c) => c.is_primary && c.status !== 'deleted' && providerIsOwnMachine(c.kind)) ?? null;

export function useSetupFacts({ refetchInterval = false }: { refetchInterval?: number | false } = {}) {
  const { progress, isLoading: progressLoading, isError: progressFailed, refetch: refetchProgress } = useOnboardingProgress({ refetchInterval });
  const answers = readAnswers(progress);
  const running = !!answers && !progress?.setup_completed_at;
  const ownAnswer = answers?.runsOn === 'own_computer';
  const computers = useComputers({ enabled: running });
  const listed = computers.data?.computers ?? [];
  const own = listed.filter(isOwnComputer);
  const home = homeMachine(listed);
  const claudeCodeAnswer = answers?.models === 'claude_code';
  const ccComputers = useClaudeCodeComputers({ enabled: running && claudeCodeAnswer });
  const ccStatus = useClaudeCodeStatus({ enabled: running && claudeCodeAnswer });

  // A token counts only while it runs a turn, as Settings reads it: one that
  // works by its last check (`TOKEN_WORKS`), a computer's also before its
  // expiry, as admission refuses one past it (`computerTokenWorks`), and the
  // cloud's only where this server runs Claude Code on a cloud computer.
  const machineToken = (c: Computer | null | undefined) =>
    c ? ccComputers.computers.find((e) => e.computer_id === c.computer_id && e.source === 'machine') : undefined;
  const runs = (e: ClaudeCodeComputer | undefined) => !!e && computerTokenWorks(e, ccComputers.readAt);

  // The computer Claude Code's token goes on: the machine of the user's own
  // Home runs on, whatever the answer, where Home's turns sign in with its
  // token: always on the platform; on a self-hosted server once it holds a
  // token of its own, which admission signs in with rather than relaying the
  // saved one, an expired one included, and while whether it holds one is
  // unknown (being checked, or the list unread), so the saved token is
  // neither offered nor counted until it is known. Else, for an
  // own-computer answer on the platform, one of the user's own whose token
  // runs, else one with a token, else the newest; else the saved token,
  // which a self-hosted server relays to the user's own computers that have
  // none of their own.
  const homeUnknown = !!home && (ccComputers.unread
    || ccComputers.computers.some((e) => e.computer_id === home.computer_id && e.state === 'checking'));
  const homeTarget = home && (isPlatformMode || machineToken(home) || homeUnknown) ? home : null;
  const newest = [...own].sort((a, b) => (b.created_at ?? '').localeCompare(a.created_at ?? ''))[0];
  const targetComputer = homeTarget ?? (ownAnswer && isPlatformMode
    ? own.find((c) => runs(machineToken(c))) ?? own.find((c) => machineToken(c)) ?? newest ?? null
    : null);
  const savedTarget = !targetComputer && !(ownAnswer && isPlatformMode);
  const custody = useClaudeCodeCustody({ enabled: running && claudeCodeAnswer && savedTarget });
  const { providers, byokEnabled, isLoading: providersLoading, unread: providersUnread } = useConfiguredProviders({ enabled: running });
  const queryClient = useQueryClient();
  const tokenTarget: TokenTarget | null = !claudeCodeAnswer ? null
    : savedTarget ? SAVED_TOKEN
    : targetComputer ? computerTokenTarget(targetComputer) : null;
  const claudeCodeToken = !!tokenTarget && (tokenTarget.kind === 'saved'
    ? ccStatus.saved && TOKEN_WORKS.has(ccStatus.state) && custody.custody?.refusal !== 'needs_own_computer'
    : runs(machineToken(targetComputer)));
  // A connection that runs a turn, as the model filters count one: a
  // subscription signed in, a key only while BYOK is on where it can be off,
  // either only while the account's rules allow it.
  const { policy } = useAccount();
  const providerConnected = providers.some((p) => p.masked_key !== SERVER_KEY
    && (p.access_type === 'oauth'
      ? allows(policy, RULE.ownSubscriptions) && p.state === 'connected'
      : allows(policy, RULE.ownApiKeys) && (!isPlatformMode || byokEnabled)));

  const isLoading = progressLoading || (running && (
    computers.isLoading || providersLoading || (claudeCodeAnswer && (ccComputers.isLoading || ccStatus.isLoading || (savedTarget && custody.isLoading)))
  ));
  // A fact whose read failed is unknown, not false: setup resumes on no step
  // until it is read (`factsFailed`), rather than on one it may be past. The
  // record itself first, whose answers every step reads.
  const computersUnread = (ownAnswer || claudeCodeAnswer) && computers.isError && !computers.data;
  const factsFailed = (progressFailed && !progress) || (running && (
    computersUnread
    || (claudeCodeAnswer && (savedTarget ? ccStatus.unread || custody.unread : computersUnread || ccComputers.unread))
    || (answers?.models === 'subscription_or_key' && !providerConnected && providersUnread)
  ));

  const facts: SetupFacts | null = answers && {
    answers,
    ownComputer: own.length > 0,
    homeOnCloud: listed.some((c) => c.is_primary && c.status !== 'deleted' && !providerIsOwnMachine(c.kind)),
    claudeCodeToken,
    providerConnected,
    lastStep: progress?.setup_step ?? null,
  };

  const next = facts && !isLoading && !factsFailed ? (progress?.setup_completed_at ? 'done' as const : nextSetupStep(facts)) : null;
  return {
    progress,
    progressFailed,
    /** Read the record again after a failed read. */
    retryProgress: () => void refetchProgress(),
    factsFailed,
    /** Read again whatever failed. */
    retryFacts: () => void queryClient.refetchQueries({ type: 'active', predicate: (q) => q.state.status === 'error' }),
    answers,
    facts,
    isLoading,
    ownComputers: own,
    tokenTarget,
    rail: answers ? setupRail(answers) : [],
    next,
    /** Where setup goes from here, /welcome once it is done; null while unknown. */
    nextPath: next ? nextSetupPath(next) : null,
  };
}
