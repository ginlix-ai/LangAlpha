import type { TFunction } from 'i18next';
import { useTranslation } from 'react-i18next';
import { useMutation, useMutationState, useQueryClient, type QueryKey } from '@tanstack/react-query';
import { checkClaudeCodePlan, type ClaudeCodeComputer, type ClaudeCodeStatus, type PlanCredential } from '@/api/model';
import { apiErrorStatus } from '@/api/errors';
import { readOAuthError } from '@/api/oauth';
import { useToast } from '@/components/ui/use-toast';
import { updateComputers } from '@/hooks/useClaudeCode';
import { useNow } from '@/hooks/useNow';
import { credentialKey, planCredentialOf } from '@/lib/claudeCodePlan';
import { queryKeys } from '@/lib/queryKeys';

/** A credential Check now can read, and until when a check made in the last
 *  5 minutes holds it, by this browser's clock (epoch ms); null while none
 *  does. */
export interface CheckTarget {
  credential: PlanCredential;
  checkAfter: number | null;
}

/** What a Check now control shows: ready, a check running, or held until
 *  `until` (epoch ms) by one made in the last 5 minutes. */
export interface PlanCheck {
  state: 'idle' | 'running' | 'held';
  until: number | null;
  run: () => void;
}

/** How long the server holds a credential after a check spent a request (`CHECK_EVERY`). */
const CHECK_HOLD_S = 300;

/** Whether a check made in the last 5 minutes holds the target at `at`. */
const holding = (target: CheckTarget, at: number) => target.checkAfter !== null && target.checkAfter > at;

/** How long Check now waits after the server would not run a check (409):
 *  a limit that holds, a check already running, or a credential it cannot
 *  check. The read after it says which, so this only bounds one that failed. */
const REFUSED_HOLD_S = 60;

/** How long a refused check holds its credential, in seconds: as long as
 *  the server says for `check_held`, another tab's or a worker's check
 *  having taken it, or briefly for one it would not run. Null for a refusal
 *  that says why in a toast (`failure`). */
const heldFor = (err: unknown): number | null => {
  const status = apiErrorStatus(err);
  if (status === 409) return REFUSED_HOLD_S;
  const checkIn = (err as { response?: { data?: { error?: { check_in?: unknown } } } })?.response?.data?.error?.check_in;
  return status === 429 && typeof checkIn === 'number' && checkIn > 0 ? checkIn : null;
};

/** What a failed check says, as the server's answer tells it: that the
 *  computer could not be reached, that Claude answered but what it said of
 *  the usage was not kept (`plan_unsaved`), that Claude could not be
 *  reached, or, for a refusal the check never ran past (a policy, a store
 *  down, Claude Code turned off), the server's own sentence. A hold, a
 *  check already running, or a credential no longer checkable is no
 *  failure, since the read after it shows where the credential stands. */
const failure = (t: TFunction, err: unknown): string | null => {
  if ([409, 429].includes(apiErrorStatus(err) ?? 0)) return null;
  const refusal = readOAuthError(err);
  if (refusal?.code === 'computer_unreachable') return t('claudeCodePlan.check.computerUnreached');
  if (refusal?.code === 'plan_unsaved') return t('claudeCodePlan.check.unsaved');
  if (refusal && refusal.code !== 'check_unavailable' && refusal.message) return refusal.message;
  return t('claudeCodePlan.check.failed');
};

/**
 * Check now over `targets`: one minimal request on each credential's plan
 * that no check holds, run at once. A check runs until the plan is read
 * again, so it reads held, not ready, once it lands. Each runs as a
 * mutation carrying its credential, so the view's header, a row's menu and
 * the picker agree on which are running. Held while every target is; until
 * the first of them frees, by a clock that turns only until then. A check
 * that failed says why in a toast, once for each reason (`failure`).
 */
export function usePlanCheck(targets: CheckTarget[]): PlanCheck {
  const { t } = useTranslation();
  const { toast } = useToast();
  const queryClient = useQueryClient();
  const mutationKey = queryKeys.features.claudeCodePlanCheck();
  const reread = (...keys: QueryKey[]) => Promise.all(keys.map((queryKey) => queryClient.invalidateQueries({ queryKey })));
  const hold = (credential: PlanCredential, checkIn: number) => {
    const key = credentialKey(credential);
    if (key === 'saved') {
      queryClient.setQueryData<ClaudeCodeStatus>(queryKeys.features.claudeCodeStatus(), (s) => s && { ...s, check_in: checkIn });
    }
    const holds = (c: ClaudeCodeComputer) => {
      const of = planCredentialOf(c);
      return of !== null && credentialKey(of) === key;
    };
    updateComputers(queryClient, holds, (c) => ({ ...c, check_in: checkIn }));
    setTimeout(() => void reread(queryKeys.features.claudeCodeStatus(), queryKeys.features.claudeCodeComputers()), checkIn * 1_000);
  };
  const check = useMutation({
    mutationKey,
    mutationFn: (credential: PlanCredential) => checkClaudeCodePlan(credential),
    // An answer with no toast leaves the reads after it to say where the
    // credential stands, but a read that fails keeps the data from before
    // the check, which would offer it again. So the answer holds the
    // credential, as a hold the server gives does, until a read replaces it,
    // and the reads go again as it ends.
    onSuccess: (_, credential) => hold(credential, CHECK_HOLD_S),
    onError: (err, credential) => {
      const checkIn = heldFor(err);
      if (checkIn !== null) hold(credential, checkIn);
    },
    onSettled: () => reread(queryKeys.features.claudeCodeStatus(), queryKeys.features.claudeCodeComputers(), queryKeys.features.claudeCodeSetup()),
  });
  // The credentials checks are running on, matched to the targets as they
  // render: a filter's predicate is read again only when the mutations change.
  const pending = useMutationState({
    filters: { mutationKey, status: 'pending' },
    select: (mutation) => credentialKey(mutation.state.variables as PlanCredential),
  });
  const running = targets.some((target) => pending.includes(credentialKey(target.credential)));
  const now = useNow(1_000, (at) => targets.some((target) => holding(target, at)));
  const holds = targets.filter((target) => holding(target, now)).flatMap((target) => target.checkAfter ?? []);
  const free = targets.filter((target) => !holding(target, now));
  const state = targets.length === 0 ? 'held' : running ? 'running' : free.length > 0 ? 'idle' : 'held';
  const run = () => {
    if (state !== 'idle') return;
    void Promise.allSettled(free.map(({ credential }) => check.mutateAsync(credential))).then((results) => {
      const said = new Set(results.flatMap((result) => (result.status === 'rejected' ? failure(t, result.reason) ?? [] : [])));
      said.forEach((description) => toast({ variant: 'destructive', description }));
    });
  };
  return { state, until: state === 'held' && holds.length > 0 ? Math.min(...holds) : null, run };
}
