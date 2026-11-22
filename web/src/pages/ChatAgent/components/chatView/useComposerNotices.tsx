import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { subscriptionKey } from '@/api/oauth';
import { CheckNowFor } from '@/components/modelAccess/PlanCheck';
import { NoticeAction, NoticeBody, type ComposerNotice, type ComposerNotices } from '@/components/ui/chat-input.notices';
import { useAllModels } from '@/hooks/useAllModels';
import { usePlanClock, usePlanOut } from '@/hooks/useClaudeCodePlan';
import { useClaudeCodeSteps } from '@/hooks/useClaudeCodeSteps';
import { useModelName, usePaidByLabel } from '@/hooks/useModelName';
import { useTurnClaudeCode } from '@/hooks/useWorkspaceClaudeCode';
import { isClaudeCodeModel } from '@/lib/claudeCode';
import { composerPlanLine, creditsRefused, pickerPlanHold } from '@/lib/claudeCodePlan';
import type { DueSummary } from '../../hooks/useNextSummaryHint';
import type { useThreadModel } from '../../hooks/useThreadModel';
import type { useThreadSubagents } from '../../hooks/useThreadSubagents';
import type { FallbackSuggestion } from '../../session/types';
import {
  DefaultModelOfferNotice, HeldDefaultNotice, LapsedSignInNotice, SubagentsOfferNotice,
} from './modelNotices';
import { useComposerModelLabel } from './useComposerModelLabel';

type ThreadModel = Pick<
  ReturnType<typeof useThreadModel>,
  'model' | 'lapsed' | 'heldDefault' | 'offer' | 'dismissOffer' | 'expireOffer' | 'pickModel'
>;
type ThreadSubagents = Pick<ReturnType<typeof useThreadSubagents>, 'offer' | 'closeOffer' | 'expireOffer'>;

interface ChatFallback {
  fallbackSuggestion: FallbackSuggestion | null;
  clearFallbackSuggestion: () => void;
  isLoading: boolean;
}

interface UseComposerNoticesOptions {
  /** The view's thread, which an offer or a fallback belongs to. */
  threadId: string | null | undefined;
  threadModel: ThreadModel;
  subagents: ThreadSubagents;
  /** The chat's last fallback, for a host that runs turns. */
  chat?: ChatFallback | null;
  /** The summary the next send writes first (`useNextSummary`). */
  summary?: DueSummary | null;
  /** Where the thread's turns run, whose Claude plan a Claude Code model uses. */
  workspaceId: string | null | undefined;
  chiefOfStaff?: boolean;
}

/**
 * What a thread host says on its composer's controls about the next send,
 * for `ChatInput`'s `notices`. Every host builds them here, so the chat view
 * and the market panels say the same things in the same places.
 *
 * The model pill takes one notice at a time, the most urgent first: a lapsed
 * sign-in or a Claude plan at its limit, which would refuse or bill the send,
 * then a held Default, a fallback that answered, and last the offer to make
 * the thread's pick the default. Each is keyed by what it is about, so a
 * notice for another model or thread opens anew. The host re-renders with
 * every streamed token, so each notice is memoized over the words and ids it
 * says, never over the objects they come from, which are rebuilt with reads.
 */
export function useComposerNotices({
  threadId,
  threadModel,
  subagents,
  chat = null,
  summary = null,
  workspaceId,
  chiefOfStaff = false,
}: UseComposerNoticesOptions): ComposerNotices {
  const { model } = threadModel;
  const lapsed = useLapsedNotice(threadModel.lapsed);
  const plan = usePlanNotice(model, chiefOfStaff, workspaceId);
  const held = useHeldDefaultNotice(threadModel.heldDefault);
  const fallback = useFallbackNotice(threadId, model, chat, threadModel.pickModel);
  const offer = useDefaultOfferNotice(threadId, threadModel);
  const modelNotice = lapsed ?? plan ?? held ?? fallback ?? offer;
  const subagentsNotice = useSubagentsOfferNotice(threadId, subagents);

  return useMemo(
    () => ({ model: modelNotice, subagents: subagentsNotice, summary }),
    [modelNotice, subagentsNotice, summary],
  );
}

function useLapsedNotice(lapsed: ThreadModel['lapsed']): ComposerNotice | null {
  const { t } = useTranslation();
  const label = useComposerModelLabel();
  const reason = lapsed?.reason ?? null;
  const text = lapsed
    ? t(reason === 'missing' ? 'chat.threadModel.notConnected' : 'chat.threadModel.lapsed', {
      account: t(`modelAccess.${subscriptionKey(lapsed.provider)}.name`),
      model: label(lapsed.model),
    })
    : null;
  // The reason is in the key, so a sign-in that goes from expired to removed
  // is a new notice and opens again.
  const key = lapsed ? `lapsed:${lapsed.model}:${lapsed.provider}:${lapsed.reason}` : null;
  return useMemo<ComposerNotice | null>(
    () => (text && key && reason
      ? { key, tone: 'warning', text, content: <LapsedSignInNotice reason={reason} /> }
      : null),
    [text, key, reason],
  );
}

/* A Claude Code thread whose Claude plan, the one behind the token the
   workspace's turns run on (the Chief of Staff's in Home), is at or near its
   limit. The picker is where another model is picked, so the notice acts only
   where the user chose usage credits and the account has them off: Check now
   then costs nothing, and reads them once they are on. */
function usePlanNotice(
  model: string | null, chiefOfStaff: boolean, workspaceId: string | null | undefined,
): ComposerNotice | null {
  const { t } = useTranslation();
  const { metadata } = useAllModels();
  const clock = usePlanClock();
  const { plan, meter } = useTurnClaudeCode(chiefOfStaff, workspaceId);
  const { planOut } = usePlanOut();
  const claudeCode = model && isClaudeCodeModel(metadata[model]) ? model : null;
  const text = claudeCode ? composerPlanLine(t, plan, planOut, claudeCode, clock) : null;
  // Opens again once the limit holds the model, after a nearing that was seen.
  const held = !!text && !!claudeCode && !!pickerPlanHold(t, plan, planOut, claudeCode, clock);
  const credential = meter?.credential ?? null;
  const checkAfter = meter?.checkAfter ?? null;
  const checkable = !!text && !!claudeCode && !!credential && creditsRefused(plan, planOut, claudeCode, clock.now);
  // The meter is rebuilt with every read, so the credential goes into the
  // notice by what it names rather than by the object.
  const checkKind = checkable ? credential?.kind ?? null : null;
  const checkComputer = credential?.kind === 'computer' ? credential.computerId : null;
  return useMemo<ComposerNotice | null>(() => {
    if (!text || !claudeCode) return null;
    const check = checkKind === 'saved'
      ? { credential: { kind: 'saved' as const }, checkAfter }
      : checkKind === 'computer' && checkComputer
        ? { credential: { kind: 'computer' as const, computerId: checkComputer }, checkAfter }
        : null;
    return {
      key: `plan:${claudeCode}:${held ? 'held' : 'near'}`,
      tone: 'warning',
      text,
      content: <NoticeBody>{check && <CheckNowFor target={check} />}</NoticeBody>,
    };
  }, [text, claudeCode, held, checkKind, checkComputer, checkAfter]);
}

function useHeldDefaultNotice(heldDefault: ThreadModel['heldDefault']): ComposerNotice | null {
  const { t } = useTranslation();
  const label = useComposerModelLabel();
  const steps = useClaudeCodeSteps();
  // Only a Default the cloud steps would let run is one to set up here; held
  // for another reason (an own computer offline), the composer's model says it.
  const text = heldDefault && steps.status === 'pending'
    ? t('modelAccess.claudeCode.steps.composerHeld', { model: label(heldDefault.model), fallback: label(heldDefault.fallback) })
    : null;
  const heldModel = heldDefault?.model ?? null;
  return useMemo<ComposerNotice | null>(
    () => (text && heldModel ? { key: `held:${heldModel}`, tone: 'warning', text, content: <HeldDefaultNotice /> } : null),
    [text, heldModel],
  );
}

/* The model the user sent with had trouble and a fallback answered the last
   turn: the offer to keep the working one for this thread. Switching is a
   pick, so it shuts the picker when made from there. */
function useFallbackNotice(
  threadId: string | null | undefined,
  model: string | null,
  chat: ChatFallback | null,
  pickModel: ThreadModel['pickModel'],
): ComposerNotice | null {
  const { t } = useTranslation();
  const name = useModelName();
  const paidBy = usePaidByLabel();
  const suggestion = chat?.fallbackSuggestion ?? null;
  // Said only while the composer still names the troubled model: it is about
  // a send on that model, and a pick of any other one (the working one or a
  // third) has answered it.
  const fallback = suggestion && !chat?.isLoading && suggestion.fromModel === model ? suggestion : null;
  const text = fallback ? t('chat.modelTroubleSuggestion', { from: name(fallback.fromModel), to: name(fallback.toModel) }) : null;
  // A switch can move the thread onto another payer, so the button says who
  // pays: two models can share a name, one on a plan and one on credits.
  const payer = fallback ? paidBy(fallback.paidBy, fallback.toModel, fallback.provider) : undefined;
  const action = !fallback
    ? null
    : payer
      ? t('chat.switchToModelPaid', { model: name(fallback.toModel), payer })
      : t('chat.switchToModel', { model: name(fallback.toModel) });
  const from = fallback?.fromModel ?? null;
  const to = fallback?.toModel ?? null;
  const clear = chat?.clearFallbackSuggestion;
  return useMemo<ComposerNotice | null>(() => {
    if (!text || !action || !from || !to) return null;
    const onSwitch = () => {
      void pickModel(to).then((saved) => {
        if (saved) clear?.();
      });
    };
    return {
      key: `fallback:${threadId}:${from}:${to}`,
      tone: 'warning',
      text,
      content: <NoticeBody><NoticeAction onClick={onSwitch} closes>{action}</NoticeAction></NoticeBody>,
    };
  }, [text, action, from, to, threadId, pickModel, clear]);
}

function useDefaultOfferNotice(
  threadId: string | null | undefined,
  { offer, dismissOffer, expireOffer }: Pick<ThreadModel, 'offer' | 'dismissOffer' | 'expireOffer'>,
): ComposerNotice | null {
  const { t } = useTranslation();
  const label = useComposerModelLabel();
  const text = offer ? t('chat.threadModel.offer', { model: label(offer.model), defaultModel: label(offer.defaultModel) }) : null;
  const offerModel = offer?.model ?? null;
  return useMemo<ComposerNotice | null>(() => (text && offerModel ? {
    key: `offer:${threadId}:${offerModel}`,
    tone: 'offer',
    text,
    content: <DefaultModelOfferNotice model={offerModel} />,
    onDismiss: dismissOffer,
    onExpire: expireOffer,
  } : null), [text, offerModel, threadId, dismissOffer, expireOffer]);
}

function useSubagentsOfferNotice(
  threadId: string | null | undefined,
  { offer, closeOffer, expireOffer }: ThreadSubagents,
): ComposerNotice | null {
  const { t } = useTranslation();
  const text = offer === null ? null : t(offer ? 'chat.threadSubagents.offerOn' : 'chat.threadSubagents.offerOff');
  return useMemo<ComposerNotice | null>(() => (offer === null || !text ? null : {
    // Keyed by the value, so an offer for the other side opens anew.
    key: `subagents:${threadId}:${offer}`,
    tone: 'offer',
    text,
    content: <SubagentsOfferNotice allowed={offer} />,
    onDismiss: closeOffer,
    onExpire: expireOffer,
  }), [offer, text, threadId, closeOffer, expireOffer]);
}
