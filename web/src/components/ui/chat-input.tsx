import React, { useState, useRef, useEffect, useCallback, useImperativeHandle, useMemo } from 'react';
import { Plus, ChartCandlestick } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { TokenUsageRing } from './token-usage-ring';
import type { TokenUsage } from '@/pages/ChatAgent/session/tokenUsage';
import { usePreferences } from '@/hooks/usePreferences';
import { useTurnModels } from '@/hooks/useTurnModels';
import { useEffectiveTuning, useModelProfileWriter } from '@/hooks/useModelProfile';
import { useFeatureEnabled } from '@/hooks/useFeatures';
import { homeWorkspaceQuery } from '@/hooks/useHomeWorkspace';
import { useAllModels } from '@/hooks/useAllModels';
import { useProviderCatalog } from '@/hooks/useProviderCatalog';
import { useLapsedSignIn } from '@/hooks/useLapsedSignIn';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useLatestRef } from '@/hooks/useLatestRef';
import { onKeyboardFit, onScreenKeyboardUp, roomAboveKeyboard } from '@/lib/visualViewport';
import { useFavoriteModels } from '@/hooks/useFavoriteModels';
import { useSeededModel } from '@/hooks/useSeededModel';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';
import { AnimatePresence } from '@/lib/framer';
import { isImeComposing } from '@/lib/ime';
import { focusWithoutRing } from '@/lib/inputModality';
import { cn } from '@/lib/utils';
import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';
import './chat-input.css';
import type { ComposerDraft, ComposerScope, ComposerSend, ContextPayload, ModelOptions, ReadyAttachment, SlashCommand, ThreadTuning, Workspace } from './chat-input.types';
import { isLargePaste, getModelDisplayName } from './chat-input.helpers';
import { effortLabelFor, resolveEffort } from '@/lib/modelTuning';
import { ChatInputWidgetDeck, MentionAutocompleteList, SlashCommandList } from './chat-input.parts';
import {
  ComposerAttachments, ComposerChips, DraftTokenLayer, DropOverlay, FilesHoldHint, SendStopButton, ToolbarOverflowMenu, VoiceButton, VoiceOverlay,
} from './chat-input.composerParts';
import { ChatInputModelMenu, ModelTriggerMeasure, type EffortMenuProps } from './chat-input.modelMenu';
import type { ModelRowStatus } from './chat-input.modelPicker';
import { claudeCodeHold, isClaudeCodeModel } from '@/lib/claudeCode';
import { usePlanReader } from '@/hooks/useClaudeCodePlan';
import { creditsRefused, pickerPlanHold, planOutOf } from '@/lib/claudeCodePlan';
import { deriveRecentModels } from './chat-input.models';
import { modelGroups } from '@/lib/modelGroups';
import { useEffortToolbarItem, useToolbarItems } from './chat-input.toolbar';
import {
  useToolbarFold, CONTAINER_BORDER, CONTAINER_PX, GROUP_GAP, ICON_BUTTON_W, TOOLBAR_GAP,
} from './chat-input.useToolbarFold';
import { useMentions } from './chat-input.useMentions';
import { useQuoteTokens } from './chat-input.useQuoteTokens';
import { useSlashCommands } from './chat-input.useSlashCommands';
import { speechSupported, useVoiceInput } from './chat-input.useVoiceInput';
import { useFileAttachments } from './chat-input.useFileAttachments';
import { useDeviceTuningLift } from './chat-input.useDeviceTuningLift';
import { readSubagentsDefault } from '@/lib/subagentsDefault';
import { readGivenBack, spellOutQuotes } from './chat-input.quoteTokens';
import { formatContextBlock } from './chat-input.wire';

/** Autosize cap for the composer textarea; past this the box scrolls. */
const MAX_TEXTAREA_HEIGHT = 200;

/** What the textarea keeps, two lines of the phone's text, however little room the keyboard leaves. */
const MIN_TEXTAREA_HEIGHT_OVER_KEYBOARD = 52;

/** A thread that follows Settings for both. */
const FOLLOWS_SETTINGS = { reasoningEffort: null, fastMode: null } as const;

/** The box with `text` put back: into it when empty, else after what is
 *  typed, a blank line between, and never twice. */
function withGivenBack(typed: string, text: string): string {
  const kept = typed.trimEnd();
  if (!kept) return text;
  if (kept === text || kept.endsWith(`\n\n${text}`)) return typed;
  return `${kept}\n\n${text}`;
}

export interface ChatInputHandle {
  getModelOptions: () => ModelOptions;
  addContext: (ctx: ContextPayload) => void;
  /**
   * Imperatively add a widget context snapshot to the deck. Local-only —
   * this does NOT publish to ContextBus. Use this when re-seeding the deck
   * from `location.state` after a navigate (e.g. dashboard → chat handoff).
   * Use `ContextBus.attach` when the user clicks "+" on a widget so every
   * mounted chat input sees it.
   */
  addWidgetSnapshot: (snapshot: WidgetContextSnapshot) => void;
  /** Replaces the text: pills it no longer names drop, and `commands` are picked beside it. */
  setValue: (text: string, commands?: SlashCommand[]) => void;
  /**
   * Puts back what was written elsewhere, or sent and handed back, without
   * losing what is typed here: the text into an empty box, else after it, a
   * blank line between, and the files and pills beside what is there now.
   * `focus: false` leaves focus where it is, for a draft that comes back
   * while the person may have moved on to something else.
   */
  giveBack: (
    text: string,
    files?: ReadyAttachment[],
    rest?: Partial<Pick<ComposerDraft, 'mentions' | 'commands'>> & { focus?: boolean },
  ) => void;
  /** Sends what is written, as Enter does. Returns whether it went: false
   *  when there was nothing to send, or files hold it (`holdsSend`). */
  send: () => boolean;
  /** Whether files still uploading, or failed, hold the send. The hint
   *  beside Send says which; the draft waits in the box. */
  holdsSend: () => boolean;
  focus: () => void;
}


export interface ChatInputProps {
  /** The send is also the box as the person wrote it, for `giveBack` should it fail. */
  onSend: (send: ComposerSend) => void;
  disabled?: boolean;
  isLoading?: boolean;
  /**
   * A compaction is in progress with no streaming turn (manual /compact|/offload
   * runs with isLoading=false). Surfaces the Stop button so the user can
   * interrupt it, just like a running turn. The Enter-key send path stays open
   * so a message typed now is queued (mirrors steering during a running turn).
   */
  isCompacting?: boolean;
  onStop?: () => void;
  placeholder?: string;
  files?: string[];
  /** Where the send goes. With `onScopeChange` the toolbar offers the scope
   *  picker. */
  scope?: ComposerScope;
  onScopeChange?: ((scope: ComposerScope) => void) | null;
  /** Shown in the scope picker while there is no workspace to pick. */
  emptyWorkspacesHint?: string | null;
  /** The send goes to Home with no scope picker to say so: a Home thread's,
   *  or the welcome's, whose first message opens one there. Its turns are the
   *  Chief of Staff's, as a send scoped to all workspaces is. */
  inHome?: boolean;
  /**
   * The Subagents switch, owned by a thread host (`useThreadSubagents`),
   * which also receives each flip through `onToggleSubagents`. Null while the
   * host has not read its thread's value, which hides the toggle rather than
   * claim one. A composer with no thread behind it leaves both unset: it then
   * holds the pick itself, shown over the user's default, and a PTC send
   * carries it in its options.
   */
  subagentsAllowed?: boolean | null;
  onToggleSubagents?: ((next: boolean) => void) | null;
  workspaces?: Workspace[] | null;
  selectedWorkspaceId?: string | null;
  onWorkspaceChange?: ((wsId: string) => void) | null;
  onCaptureChart?: (() => void) | null;
  chartImage?: string | null;
  onRemoveChartImage?: (() => void) | null;
  prefillMessage?: string;
  onClearPrefill?: (() => void) | null;
  /**
   * External context attached by the host (e.g. a confirmed chart selection
   * rendered outside this input). When true, an empty text box still counts as
   * sendable — the selection carries its own instruction.
   */
  hasExternalContext?: boolean;
  tokenUsage?: TokenUsage | null;
  /** The thread the context panel breaks down; absent for a chat with no
   *  thread yet. */
  threadId?: string | null;
  onAction?: ((cmd: SlashCommand) => void) | null;
  /**
   * The model the pill shows and a send names, owned by the host's thread
   * (`useThreadModel`), which also receives each pick through `onPickModel`.
   * A composer with no thread behind it leaves both unset: it then holds the
   * pick itself, seeded from the account default, and the pick rides the next
   * send, which is how such a composer starts its thread on the chosen model.
   */
  model?: string | null;
  onPickModel?: ((model: string) => void) | null;
  /** The model a send names where the host's thread names another than the
   *  pill shows (`useThreadModel`'s `sendModel`); unset, the pill's. */
  sendModel?: string | null;
  /**
   * The thread's own reasoning effort and fast mode, over what Settings gives
   * its model, owned by a thread host (`useThreadTuning`), which also receives
   * each pick: the level, or null to follow Settings again. A composer with no
   * thread behind it leaves all three unset: it then holds the picks itself,
   * and a send hands them to the thread it starts. Either way a pick never
   * writes Settings, which is the only place a default is set.
   */
  tuning?: ThreadTuning;
  onPickReasoningEffort?: ((effort: string | null) => void) | null;
  onPickFastMode?: ((fast: boolean | null) => void) | null;
  threadModels?: string[];
  dropdownDirection?: 'up' | 'down';
  minRows?: number;
  /**
   * A status item the host shows at the end of the left tools (the chat view
   * docks its folded tray here). Outside the fold budget on purpose: a host's
   * status comes and goes on its own clock, and the toggles must not hop into
   * the overflow menu with it, so the model label gives up the room instead.
   * Only once the label is down to its floor does the status cost a slot.
   */
  toolbarStatus?: React.ReactNode;
  /**
   * Context the host gathers above the text box, ruled off from it (the chat
   * view's queued file comments). The host owns what it holds and what a send
   * does with it; pair it with `hasExternalContext` when it makes an empty box
   * sendable. A press on it is its own and does not focus the text box.
   */
  deck?: React.ReactNode;
}

/* --- MAIN COMPONENT --- */

function settleAfterSend(field: HTMLTextAreaElement | null): void {
  if (!field) return;
  field.style.height = 'auto';
  // The answer lands where the on-screen keyboard is.
  if (onScreenKeyboardUp()) field.blur();
}

function ChatInput({
  onSend,
  disabled = false,
  isLoading = false,
  isCompacting = false,
  onStop,
  placeholder,
  files: workspaceFiles = [],
  // Scope picker
  scope,
  onScopeChange = null,
  emptyWorkspacesHint = null,
  inHome = false,
  // Subagents switch
  subagentsAllowed: hostSubagents,
  onToggleSubagents = null,
  // Workspace selector
  workspaces = null,
  selectedWorkspaceId = null,
  onWorkspaceChange = null,
  // Chart capture (trading)
  onCaptureChart = null,
  chartImage = null,
  onRemoveChartImage = null,
  // Prefill (trading)
  prefillMessage = '',
  onClearPrefill = null,
  // Host-attached context (e.g. a chart selection pill) that makes an empty box sendable
  hasExternalContext = false,
  // Token usage (context window progress)
  tokenUsage = null,
  threadId,
  // Action commands (e.g. /compact) — dispatched on send, not on selection
  onAction = null,
  // Model selector
  model: hostModel,
  onPickModel = null,
  sendModel: hostSendModel,
  // Thread effort and fast mode
  tuning: hostTuning,
  onPickReasoningEffort = null,
  onPickFastMode = null,
  // All models used in this thread (listed under the favorites)
  threadModels: threadModelsProp = [],
  // Dropdown direction: 'up' (default, for bottom-positioned inputs) or 'down' (for mid-page inputs like ThreadGallery)
  dropdownDirection = 'up',
  // Minimum visible rows for the textarea (default 1 = compact; pass 2 for roomier non-ChatView callsites)
  minRows = 1,
  toolbarStatus = null,
  deck = null,
  ref,
}: ChatInputProps & { ref?: React.Ref<ChatInputHandle> }) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const isMobile = useIsMobile();
  const { preferences, isLoaded: preferencesRead } = usePreferences();
  const marketWatchEnabled = useFeatureEnabled('market_watch');
  const { models: visibleModels, validModelNames, metadata: modelMetadata, payers, isLoading: modelsLoading } = useAllModels();
  const lapsedSignIn = useLapsedSignIn();
  const otherPref = (preferences as Record<string, Record<string, unknown>> | null)?.other_preference;
  const [message, setMessage] = useState('');
  // Where the message is going: Home for the Chief of Staff, else the
  // selected workspace. File uploads and the slash menu both aim at it.
  const queryClient = useQueryClient();
  const { data: homeWorkspaceId = null } = useQuery({
    ...homeWorkspaceQuery(queryClient),
    enabled: scope === 'all',
    select: (workspace) => workspace.workspace_id,
  });
  const slashWorkspaceId = scope === 'all' ? homeWorkspaceId : selectedWorkspaceId;
  // Read when a file starts uploading, which is outside any render.
  const targetWorkspaceRef = useRef<string | null>(null);
  const {
    attachedFiles, uploadingCount, failedCount, isDragging,
    handleFiles, retryFile, restoreFiles, removeFile, discardFiles, takeFiles, holdsStale,
    onDragEnter, onDragOver, onDragLeave, onDrop, handlePaste,
  } = useFileAttachments({ disabled, workspaceIdRef: targetWorkspaceRef, workspaceId: slashWorkspaceId ?? null });
  const [watchMode, setWatchMode] = useState(false);
  // The toggle state survives the feature going off (the pill just hides), so
  // every payload derives from this gated value.
  const effectiveMarketWatch = watchMode && marketWatchEnabled;

  // The model a send names. A thread host owns it. Without one the composer
  // holds it, starting on the account default, which with nothing saved is
  // the deployment's (see useSlotDefaultModels for why it waits on the
  // preference), and following the default when it changes. A turn in Home
  // starts on the deployment's over a Claude Code one the server does not
  // run there (see useTurnDefaultModel).
  const chiefOfStaff = inHome || scope === 'all';
  const turn = useTurnModels(chiefOfStaff, selectedWorkspaceId);
  const [ownModel, setOwnModel] = useSeededModel(turn.defaultModel);
  const pillModel = hostModel === undefined ? ownModel : hostModel;
  const sentModel = hostSendModel === undefined ? pillModel : hostSendModel;

  // Per-model tuning is an account preference, not a device one: the server
  // resolves ``profiles[<model>]`` for turns this input never starts (schedules,
  // automations, subagent role models). Only the device lift below writes it
  // from here; a pick in the menu is the thread's own.
  const { writeProfile: writeModelProfile } = useModelProfileWriter();

  // What Settings gives this model: its profile, else the account default,
  // else what the model declares. The level is clamped onto the model's
  // ladder, as the server runs it, so the menu's Default names one it lists.
  const { efforts: reasoningEfforts, effective: settingsTuning } = useEffectiveTuning(pillModel);
  const settingsEffort = settingsTuning.reasoning_effort && reasoningEfforts.length > 0
    ? resolveEffort(settingsTuning.reasoning_effort, reasoningEfforts)
    : settingsTuning.reasoning_effort;
  const settingsFast = settingsTuning.fast_mode;

  // The thread's own picks over Settings. A thread host owns them. Without
  // one the composer holds them, following Settings until touched.
  const [ownTuning, setOwnTuning] = useState<{ reasoningEffort: string | null; fastMode: boolean | null }>(FOLLOWS_SETTINGS);
  const tuning = hostTuning === undefined ? ownTuning : hostTuning;
  // What a send carries as the turn's level, and what the pill shows of it.
  // Saves and Settings writes are optimistic and not awaited, so between a
  // pick and its write landing the composer already shows the pick while the
  // row the server reads still holds the old level. Sending nothing would let
  // the server resolve against the row the write is still replacing, so the
  // turn runs a setting the screen no longer shows. Sending what is displayed
  // cannot disagree with it. The level goes out unclamped, as the row holds
  // it: every client of the turn, a fallback model included, clamps it onto
  // its own ladder, and a turn no composer started reads the same level from
  // the row. Only the pill clamps, onto this model's ladder, which names none
  // until the ladder is known. A thread whose row is unread shows and sends
  // none, since Settings may not be what it runs, and the server runs the
  // thread's own.
  const sentEffort = tuning.reasoningEffort === undefined
    ? null
    : tuning.reasoningEffort ?? settingsTuning.reasoning_effort;
  const resolvedEffort = tuning.reasoningEffort === undefined
    ? null
    : tuning.reasoningEffort === null
      ? settingsEffort
      : resolveEffort(tuning.reasoningEffort, reasoningEfforts);
  const resolvedFastMode = tuning.fastMode === undefined ? null : tuning.fastMode ?? settingsFast;
  // A composer with no thread behind it hands its picks to the thread its
  // send starts, which keeps them as its own.
  const ownOverrides = useMemo(() => (hostTuning === undefined ? {
    ...(ownTuning.reasoningEffort !== null ? { reasoningEffortOverride: ownTuning.reasoningEffort } : {}),
    ...(ownTuning.fastMode !== null ? { fastModeOverride: ownTuning.fastMode } : {}),
  } : {}), [hostTuning, ownTuning]);

  // The Subagents switch a send names. A thread host owns it. Without one the
  // composer holds a pick, null until touched so the toggle shows the user's
  // default, and a send names only that pick, leaving the default to the
  // server. Until the default is read the toggle shows what no preference
  // means, which a stored default may contradict, so a send names what it
  // shows.
  const [ownSubagents, setOwnSubagents] = useState<boolean | null>(null);
  const ownShown = ownSubagents ?? readSubagentsDefault(preferences);
  const shownSubagents = hostSubagents === undefined ? ownShown : hostSubagents;
  const draftSubagents = hostSubagents === undefined
    ? ownSubagents ?? (preferencesRead ? undefined : ownShown)
    : undefined;
  const handleToggleSubagents = useCallback((next: boolean) => {
    if (hostSubagents === undefined) setOwnSubagents(next);
    else onToggleSubagents?.(next);
  }, [hostSubagents, onToggleSubagents]);

  const handlePickModel = useCallback((model: string) => {
    // The checked row is the model the composer already runs.
    if (model === pillModel) return;
    if (hostModel === undefined) setOwnModel(model);
    else onPickModel?.(model);
  }, [pillModel, hostModel, setOwnModel, onPickModel]);

  const isCodexModel = pillModel ? modelMetadata[pillModel]?.sdk === 'codex' : false;

  // Widget context deck state. Snapshots arrive via ContextBus.attach (when
  // the user clicks "+" on any widget on the page) or via addWidgetSnapshot
  // (when re-seeding from location.state after a navigate). Each input
  // mirrors the same global ContextBus state so two visible inputs always
  // show the same deck.
  const [widgetSnapshots, setWidgetSnapshots] = useState<WidgetContextSnapshot[]>([]);
  const [deckFanned, setDeckFanned] = useState(false);

  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const chatContainerRef = useRef<HTMLDivElement>(null);
  // The composer box as state too: the mobile menus portal into it, and render
  // may only read it from state. The callback ref keeps the ref filled for the
  // effects and hooks that read it after commit.
  const [chatContainer, setChatContainer] = useState<HTMLDivElement | null>(null);
  const attachChatContainer = useCallback((el: HTMLDivElement | null) => {
    chatContainerRef.current = el;
    setChatContainer(el);
  }, []);

  // @file mentions and /slash commands — trigger detection, filtering,
  // selection and menu keyboard navigation live in these two machines. Both
  // return fresh objects each render, so hooks below depend on the individual
  // (memoized) members rather than the machine itself.
  const mentions = useMentions({ textareaRef, message, setMessage, workspaceFiles });
  // The workspace context scopes the slash menu: inside a workspace the
  // enabled-skill list is the workspace-effective one (workspace-scoped
  // skills included, its disables applied). The Chief of Staff runs in Home,
  // which loads its skills as any workspace does, so the menu lists Home's
  // rather than those of the workspace the picker remembers.
  useEffect(() => {
    targetWorkspaceRef.current = slashWorkspaceId ?? null;
  }, [slashWorkspaceId]);
  const slash = useSlashCommands({ textareaRef, message, setMessage, workspaceId: slashWorkspaceId });
  const {
    mentionedFiles, setMentionedFiles, pruneRemoved: pruneMentions, detectTrigger: detectMention,
    close: closeMentions, reset: resetMentions, handleKeyDown: mentionKeyDown, open: mentionsOpen, addMention, removeMention,
  } = mentions;
  const {
    slashCommands, pruneRemoved: pruneSlash, detectTrigger: detectSlash,
    close: closeSlash, reset: resetSlash, restore: restoreSlash, handleKeyDown: slashKeyDown,
  } = slash;

  // Pills whose /{command} or @mention text was deleted go with it
  const pruneDraft = useCallback((draft: string) => {
    pruneSlash(draft);
    pruneMentions(draft);
  }, [pruneSlash, pruneMentions]);
  // The words of the draft that stand for a skill or a file.
  const draftTokens = useMemo(() => [
    ...slashCommands.map((c) => `/${c.name}`),
    ...mentionedFiles.filter((f) => f.kind === 'file').map((f) => `@${f.path}`),
  ], [slashCommands, mentionedFiles]);
  const {
    heldMentions, quotes, chips, addQuote, handleSelect, handleQuotePress, handleKeyDown: tokenKeyDown, keepWhole,
  } = useQuoteTokens({ textareaRef, message, setMessage, mentions, draftTokens, pruneDraft });

  // Voice input (speech recognition) — see chat-input.useVoiceInput.
  const { isListening, toggleListening, stopListening, abandonListening } = useVoiceInput({ message, setMessage, isLoading });

  const givenBackBesideRef = useLatestRef(mentionedFiles);

  // The handle is built before the send below, so it reaches it through here.
  const sendRef = useRef<() => boolean>(() => false);
  const holdsRef = useRef(false);

  // Expose addContext method for external callers (e.g. FilePanel, message selection)
  useImperativeHandle(ref, () => ({
    getModelOptions() {
      return {
        model: sentModel,
        reasoningEffort: sentEffort,
        fastMode: resolvedFastMode,
        marketWatch: effectiveMarketWatch,
        subagentsAllowed: draftSubagents,
        ...ownOverrides,
      };
    },
    addContext(ctx) {
      // Dictation rewrites the draft from what it held when it started.
      if (ctx.kind !== 'chip') abandonListening();
      switch (ctx.kind) {
        case 'file':
          // Same behavior as selectFile via @mention
          addMention(ctx);
          setMessage((prev) => {
            if (prev.includes('@' + ctx.path)) return prev;
            const prefix = prev && !prev.endsWith(' ') ? prev + ' ' : prev;
            return prefix + '@' + ctx.path + ' ';
          });
          break;
        case 'quote':
          addQuote(ctx);
          break;
        case 'chip':
          addMention(ctx);
          break;
        case 'attachment': {
          const { files, text } = ctx;
          if (files.length) handleFiles(files);
          if (text) setMessage((prev) => (prev.trim() ? `${prev.trimEnd()}\n${text}` : text));
          break;
        }
      }
      // Focus the textarea
      setTimeout(() => focusWithoutRing(textareaRef.current), 0);
    },
    addWidgetSnapshot(snapshot) {
      setWidgetSnapshots((prev) => {
        if (prev.some((s) => s.widget_id === snapshot.widget_id)) return prev;
        return [...prev, snapshot];
      });
    },
    setValue(text, commands = []) {
      // Dictation rewrites the draft from what it held when it started.
      abandonListening();
      setMessage(text);
      // Pills follow the text, as typing prunes them: one the new text no
      // longer names would otherwise ride the next send unseen.
      pruneDraft(text);
      restoreSlash(commands);
      // A draft handed in by another surface (a palette's Enter, a starter's
      // click) carries on that action, so it draws no keyboard ring.
      setTimeout(() => focusWithoutRing(textareaRef.current), 0);
    },
    giveBack(text, files = [], { mentions = [], commands = [], focus = true } = {}) {
      // Through a ref, for what else comes back in the same commit to number against.
      const back = readGivenBack(text, mentions, givenBackBesideRef.current);
      givenBackBesideRef.current = [...givenBackBesideRef.current, ...back.mentions];
      if (back.text) setMessage((prev) => withGivenBack(prev, back.text));
      restoreFiles(files);
      if (back.mentions.length > 0) setMentionedFiles((prev) => [...prev, ...back.mentions]);
      restoreSlash(commands);
      // Handed back by the app, not reached by a key: no keyboard ring.
      if (focus) setTimeout(() => focusWithoutRing(textareaRef.current), 0);
    },
    send() {
      return sendRef.current();
    },
    holdsSend() {
      return holdsRef.current || holdsStale();
    },
    focus() {
      textareaRef.current?.focus();
    },
  }), [sentModel, sentEffort, resolvedFastMode, effectiveMarketWatch, draftSubagents, ownOverrides, mentionedFiles, setMentionedFiles, addMention, addQuote, restoreFiles, holdsStale, restoreSlash, pruneDraft, handleFiles, abandonListening]);

  // Subscribe to ContextBus so this input mirrors the global widget-context
  // deck. Multiple chat inputs can be visible simultaneously (hero card +
  // ConversationWidget): they all subscribe and stay in sync. Detach events
  // come from another input's deck card "×" — we filter our own state to
  // match.
  useEffect(() => {
    const off = ContextBus.subscribe((event) => {
      if (event.type === 'attach') {
        setWidgetSnapshots((prev) => {
          if (prev.some((s) => s.widget_id === event.snapshot.widget_id)) return prev;
          return [...prev, event.snapshot];
        });
      } else if (event.type === 'detach') {
        setWidgetSnapshots((prev) => prev.filter((s) => s.widget_id !== event.widgetId));
      } else if (event.type === 'clear') {
        setWidgetSnapshots([]);
      }
    });
    return off;
  }, []);

  // Register this chat input's root element so the global ContextOverflowPill
  // can probe visibility via IntersectionObserver and only show when zero
  // chat inputs are in the viewport.
  useEffect(() => {
    const el = chatContainerRef.current;
    if (!el) return;
    const off = ChatInputRegistry.register(el);
    return off;
  }, []);

  // Prefill support
  useEffect(() => {
    if (prefillMessage) {
      setMessage(prefillMessage);
      onClearPrefill?.();
    }
  }, [prefillMessage, onClearPrefill]);

  // Effort and fast-mode picks this device still holds go up to the account profile.
  useDeviceTuningLift({
    pillModel, preferences, modelMetadata, reasoningEfforts, writeProfile: writeModelProfile,
  });

  // The menu names null for the level Settings gives, which returns the
  // thread to following Settings rather than pinning today's value.
  const handleReasoningEffortChange = useCallback((effort: string | null) => {
    if (effort === tuning.reasoningEffort) return;
    if (hostTuning === undefined) setOwnTuning((prev) => ({ ...prev, reasoningEffort: effort }));
    else onPickReasoningEffort?.(effort);
  }, [tuning.reasoningEffort, hostTuning, onPickReasoningEffort]);

  // Speed has no Default of its own in the menu, so the side Settings is on
  // stands for it: flipping there follows Settings again.
  const handleFastModeChange = useCallback((fast: boolean) => {
    const next = fast === settingsFast ? null : fast;
    if (next === tuning.fastMode) return;
    if (hostTuning === undefined) setOwnTuning((prev) => ({ ...prev, fastMode: next }));
    else onPickFastMode?.(next);
  }, [settingsFast, tuning.fastMode, hostTuning, onPickFastMode]);

  const tokenLayerRef = useRef<HTMLDivElement>(null);
  const sizeTokenLayer = useCallback(() => {
    if (!textareaRef.current || !tokenLayerRef.current) return;
    tokenLayerRef.current.style.width = `${textareaRef.current.clientWidth}px`;
    tokenLayerRef.current.style.height = `${textareaRef.current.clientHeight}px`;
    tokenLayerRef.current.scrollTop = textareaRef.current.scrollTop;
  }, []);

  // Auto-resize textarea; past the cap the box scrolls (overflow-y-auto). A
  // chip beside it changes its width, and so where the text wraps, and the
  // layer has to wrap with it, a scrollbar taken out of the width included.
  const fitTextarea = useCallback(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
      // Rounded up: `scrollHeight` rounds a fractional line height down as often as
      // not, and what fits exactly then comes up short and shows a scrollbar.
      textareaRef.current.style.height = Math.min(textareaRef.current.scrollHeight + 1, MAX_TEXTAREA_HEIGHT) + 'px';
      sizeTokenLayer();
    }
  }, [sizeTokenLayer]);
  useEffect(fitTextarea, [fitTextarea, message, chips.length, draftTokens.length]);
  // A narrower window wraps the text too. Fitted a frame on, since the fit itself
  // resizes what is being observed.
  useEffect(() => {
    const textarea = textareaRef.current;
    if (!textarea) return;
    let width = textarea.clientWidth;
    let frame = 0;
    const observer = new ResizeObserver(() => {
      if (textarea.clientWidth === width) return;
      width = textarea.clientWidth;
      cancelAnimationFrame(frame);
      frame = requestAnimationFrame(fitTextarea);
    });
    observer.observe(textarea);
    return () => {
      observer.disconnect();
      cancelAnimationFrame(frame);
    };
  }, [fitTextarea]);

  // A composer in a scrolling route grows downward, so with a keyboard up the
  // text gives up what would push the send row under it. A limit apart from the
  // height above, measured from the box as it stands: nothing collapses, so the
  // text keeps its scroll, and a composer hidden with its cached thread keeps its size.
  const fitToKeyboard = useCallback(() => {
    const textarea = textareaRef.current;
    const box = chatContainerRef.current;
    if (!textarea || !box) return;
    const active = document.activeElement;
    // Its picker is anchored to the box as it stands.
    if (active?.closest('[data-composer-panel]')) return;
    const room = active === textarea ? roomAboveKeyboard(box) : null;
    if (room === null) {
      textarea.style.maxHeight = '';
      sizeTokenLayer();
      return;
    }
    const shownBefore = textarea.clientHeight;
    // Not `offsetHeight`: rounded, the limit never settles and the observer below
    // refits it every frame.
    textarea.style.maxHeight = Math.max(textarea.getBoundingClientRect().height + room, MIN_TEXTAREA_HEIGHT_OVER_KEYBOARD) + 'px';
    // The box gives up its foot, and a caret at the end of the text is there.
    if (textarea.clientHeight < shownBefore && textarea.selectionEnd === textarea.value.length) {
      textarea.scrollTop = textarea.scrollHeight;
    }
    sizeTokenLayer();
    if ((roomAboveKeyboard(box) ?? 0) < 0) box.scrollIntoView({ block: 'nearest' });
  }, [sizeTokenLayer]);

  useEffect(fitToKeyboard, [message, fitToKeyboard]);
  useEffect(() => {
    if (!chatContainer) return;
    // Whatever else sizes the box: a pill, an attachment leaving, a deck folding,
    // with or without motion. A frame later, since changing the box inside its
    // own callback is a ResizeObserver loop error.
    let frame = 0;
    const observer = new ResizeObserver(() => {
      cancelAnimationFrame(frame);
      frame = requestAnimationFrame(fitToKeyboard);
    });
    observer.observe(chatContainer);
    const stopListening = onKeyboardFit(fitToKeyboard);
    return () => {
      observer.disconnect();
      cancelAnimationFrame(frame);
      stopListening();
    };
  }, [chatContainer, fitToKeyboard]);

  // Large text pastes condense into a pill (snippet-mention mechanics) instead
  // of flooding the draft. File pastes keep priority — handlePaste prevents
  // default when it takes the clipboard.
  const handleTextareaPaste = useCallback((e: React.ClipboardEvent<HTMLTextAreaElement>) => {
    handlePaste(e);
    if (e.defaultPrevented) return;
    const text = e.clipboardData.getData('text/plain');
    if (!isLargePaste(text)) return;
    e.preventDefault();
    addMention({ kind: 'chip', source: 'paste', snippet: text, label: t('context.pastedLines', { count: text.split('\n').length }) });
  }, [handlePaste, addMention, t]);

  // Detect @ and / triggers on input change
  const handleChange = useCallback((e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const val = keepWhole(e.target.value, e.target.selectionStart);
    setMessage(val);
    const cursorPos = e.target.selectionStart;

    pruneDraft(val);

    // A live /slash trigger wins: the two menus never show at once.
    if (detectSlash(val, cursorPos)) closeMentions();
    else detectMention(val, cursorPos);
  }, [keepWhole, pruneDraft, detectSlash, detectMention, closeMentions]);

  // Close both autocompletes on blur
  const handleBlur = useCallback(() => {
    setTimeout(() => {
      closeMentions();
      closeSlash();
    }, 200);
  }, [closeMentions, closeSlash]);

  // --- Send ---
  const hasContent = message.trim() || attachedFiles.length > 0 || !!chartImage || chips.length > 0 || widgetSnapshots.length > 0 || hasExternalContext;
  // A file still uploading has nothing to send yet, and a failed one is never
  // dropped unseen: either holds the send until it is ready or removed.
  const filesHold = uploadingCount > 0 || failedCount > 0;

  /** Whether it sent. */
  const handleSend = useCallback((): boolean => {
    if (!hasContent || disabled || filesHold) return false;

    // System action commands (/compact, /offload) are standalone: they run on
    // send rather than starting a chat turn. If any are present, fire them and
    // clear the input — don't dispatch a message.
    const actionCommands = slashCommands.filter((c) => c.type === 'action');
    if (actionCommands.length > 0) {
      actionCommands.forEach((c) => onAction?.(c));
      // Fully reset the draft, same as a normal send — an action must not leave
      // staged attachments / mentions / widget context behind for the next turn.
      setMessage('');
      resetSlash();
      discardFiles();
      resetMentions();
      if (widgetSnapshots.length > 0) {
        ContextBus.clear();
      }
      setDeckFanned(false);
      settleAfterSend(textareaRef.current);
      return true;
    }

    const readyAttachments = takeFiles();
    // A file too old to name uploads again, and the send waits for it as for
    // any upload; the hold shows before the render that counts it.
    if (!readyAttachments) {
      holdsRef.current = true;
      return false;
    }
    // Quotes go where they sit among the typed words. Chips' blocks trail.
    let finalMessage = spellOutQuotes(message, quotes);
    if (chips.length > 0) {
      const blocks = chips.map((f) => formatContextBlock(f, t));
      finalMessage = finalMessage.trimEnd() + '\n' + blocks.join('\n');
    }
    onSend({
      text: message,
      files: readyAttachments,
      mentions: heldMentions,
      commands: slashCommands,
      message: finalMessage,
      modelOptions: {
        model: sentModel,
        reasoningEffort: sentEffort,
        fastMode: resolvedFastMode,
        marketWatch: effectiveMarketWatch,
        subagentsAllowed: draftSubagents,
        ...ownOverrides,
        widgetSnapshots: widgetSnapshots.length ? widgetSnapshots : undefined,
      },
    });
    setMessage('');
    resetMentions();
    resetSlash();
    // Clear widget deck on send. Other mounted chat inputs hear the same
    // clear via ContextBus so the deck empties everywhere — single source
    // of truth.
    if (widgetSnapshots.length > 0) {
      ContextBus.clear();
    }
    setDeckFanned(false);
    settleAfterSend(textareaRef.current);
    return true;
  }, [hasContent, disabled, filesHold, message, effectiveMarketWatch, draftSubagents, discardFiles, takeFiles, onSend, onAction,
    heldMentions, quotes, chips, resetMentions, slashCommands, resetSlash,
    sentModel, sentEffort, resolvedFastMode, ownOverrides, widgetSnapshots, t]);
  useEffect(() => {
    holdsRef.current = filesHold;
    sendRef.current = () => {
      // Held by its files, the draft is still there to send, and the hint
      // beside Send says what it waits on; nothing went.
      if (!hasContent || disabled || filesHold) return false;
      return handleSend();
    };
  }, [hasContent, disabled, filesHold, handleSend]);

  // --- Keyboard & Language Detection ---
  const handleKeyDown = useCallback((e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    // If user starts typing while voice input is active, terminate the voice input.
    // We treat any single character key or delete/backspace as a signal to stop.
    if (isListening && (e.key.length === 1 || e.key === 'Backspace' || e.key === 'Delete')) {
      stopListening();
    }

    if (isImeComposing(e)) return;

    // An open autocomplete owns the navigation keys (slash menu first — it
    // suppresses the mention menu while active).
    if (slashKeyDown(e)) return;
    if (mentionKeyDown(e)) return;
    if (tokenKeyDown(e)) return;

    if (e.key === 'Enter' && !mentionsOpen) {
      // Cmd/Ctrl+Enter inserts a newline at the cursor (browsers suppress this by default)
      if (e.metaKey || e.ctrlKey) {
        e.preventDefault();
        const ta = textareaRef.current;
        if (ta) {
          const start = ta.selectionStart;
          const end = ta.selectionEnd;
          const next = message.slice(0, start) + '\n' + message.slice(end);
          setMessage(next);
          requestAnimationFrame(() => {
            ta.setSelectionRange(start + 1, start + 1);
          });
        }
        return;
      }
      if (!e.shiftKey && !onScreenKeyboardUp()) {
        e.preventDefault();
        handleSend();
      }
    }
  }, [slashKeyDown, mentionKeyDown, tokenKeyDown, mentionsOpen, handleSend, isListening, stopListening, message]);

  /* --- Progressive toolbar folding ---
   *
   * The labeled toggles fold into a ⋯ overflow menu only when the measured row
   * can't hold them — whatever fits stays inline, the rest overflows in
   * declaration (priority) order. Widths come from a hidden measure row that
   * renders the SAME components, so any locale / workspace name / model label
   * is measured, never guessed. */
  const toolbarItems = useToolbarItems({
    scope, onScopeChange, emptyWorkspacesHint,
    subagentsAllowed: shownSubagents, onToggleSubagents: handleToggleSubagents,
    watchMode, setWatchMode, marketWatchEnabled,
    workspaces, selectedWorkspaceId, onWorkspaceChange,
    menuDirection: dropdownDirection,
    menuContainer: chatContainer,
    draftRef: textareaRef,
  });
  // The effort pill sits beside the model pill and folds last (`slot: 'right'`).
  const effortLabel = effortLabelFor(t, resolvedEffort);
  const taskCost = pillModel ? modelMetadata[pillModel]?.task_cost : undefined;
  const effortProps = useMemo<EffortMenuProps>(() => ({
    effortLabel,
    reasoningEffort: resolvedEffort,
    defaultEffort: settingsEffort,
    reasoningEfforts,
    onReasoningEffortChange: handleReasoningEffortChange,
    fastMode: resolvedFastMode,
    onFastModeChange: handleFastModeChange,
    isCodexModel,
    dropdownDirection,
    container: chatContainer,
    taskCost,
    disabled: modelsLoading,
  }), [
    effortLabel, resolvedEffort, settingsEffort, reasoningEfforts, handleReasoningEffortChange, resolvedFastMode,
    handleFastModeChange, isCodexModel, dropdownDirection, chatContainer, taskCost, modelsLoading,
  ]);
  const effortItem = useEffortToolbarItem(effortProps, isMobile);
  const foldItems = useMemo(() => [...toolbarItems, effortItem], [toolbarItems, effortItem]);

  const ringVisible = !!(tokenUsage && tokenUsage.threshold > 0 && tokenUsage.total > 0);
  const micReserved = speechSupported && !!otherPref?.voice_input_enabled;
  // Right group: the model pill (measured), the mic, and send/stop. A turn
  // hides the mic but keeps its room, so the fold set holds through it.
  const rightGroup = (model: number) => GROUP_GAP + model
    + (micReserved ? TOOLBAR_GAP + ICON_BUTTON_W : 0)
    + TOOLBAR_GAP + ICON_BUTTON_W;
  const { measureRowRef, statusRef, fold } = useToolbarFold({
    containerRef: chatContainerRef,
    items: foldItems,
    // The model pill is measured too, but it isn't foldable. Its label joins
    // the key because an authored display_name comes with the models request,
    // which can land after the pill was first measured under the key's label.
    measureKey: `${pillModel}|${getModelDisplayName(pillModel, modelMetadata)}|${resolvedEffort}|${resolvedFastMode && isCodexModel}`,
    // Left of the first pill: container border + px-3, the attach button, and
    // the optional ring / chart-capture button (see the action bar below).
    fixedLeft: CONTAINER_BORDER + CONTAINER_PX + ICON_BUTTON_W
      + (ringVisible ? TOOLBAR_GAP + ICON_BUTTON_W : 0)
      + (onCaptureChart ? TOOLBAR_GAP + ICON_BUTTON_W : 0),
    rightReserve: (widths) => rightGroup(widths.model ?? 0),
    rightFloor: (widths) => rightGroup(widths.modelFloor ?? 0),
  });
  const inlineItems = toolbarItems.filter((i) => fold.inline.has(i.id));
  const foldedItems = foldItems.filter((i) => fold.folded.includes(i.id));

  const favorites = useFavoriteModels();
  const { entryFor } = useProviderCatalog();
  // A Claude Code model runs a turn while the token where the turn's
  // workspace (Home's for the Chief of Staff) runs it works or could not be
  // checked, as admission lets it;
  // otherwise its row says why, and where the user can fix it: that
  // computer's token, or the saved one. One whose Claude plan is used up for
  // it waits for the reset, unless usage credits carry it, told by the
  // picker's own minute clock (`now`), so the composer does not turn with the
  // minute.
  const { claudeCode } = turn;
  const planReader = usePlanReader();
  const modelGroupList = useMemo(() => modelGroups({
    models: turn.models,
    payers,
    brandOf: (group) => {
      const entry = entryFor(group);
      if (!entry?.group || entry.group === group) return undefined;
      return {
        id: entry.group,
        label: visibleModels[entry.group]?.display_name ?? entry.group_display_name ?? entry.display_name,
      };
    },
  }), [turn.models, visibleModels, payers, entryFor]);
  const recentModels = deriveRecentModels({ selectedModel: pillModel, threadModels: threadModelsProp, validModelNames })
    .filter((m) => !favorites.models.includes(m));
  const modelRowStatus = (m: string, now: number): ModelRowStatus => {
    if (isClaudeCodeModel(modelMetadata[m])) {
      const hold = claudeCodeHold(claudeCode.state, turn.workspaceId, {
        home: chiefOfStaff, cliComputerName: claudeCode.cliComputerName,
      });
      if (hold) {
        const { link } = hold;
        return {
          unavailable: t(hold.messageKey, hold.messageValues),
          fix: link ? () => (link.external ? window.open(link.url, '_blank', 'noopener,noreferrer') : navigate(link.url)) : undefined,
        };
      }
      const planOut = planOutOf(preferences?.other_preference);
      const planHold = pickerPlanHold(t, claudeCode.plan, planOut, m, { ...planReader, now });
      const credential = claudeCode.meter?.credential;
      if (planHold && credential && creditsRefused(claudeCode.plan, planOut, m, now)) {
        return { unavailable: planHold, check: { credential, checkAfter: claudeCode.meter?.checkAfter ?? null } };
      }
      if (planHold) return { unavailable: planHold };
    }
    return { tag: lapsedSignIn(m) ? t('modelAccess.status.needsReauth') : undefined };
  };

  return (
    <div
      className="relative w-full"
      onDragEnter={onDragEnter}
      onDragOver={onDragOver}
      onDragLeave={onDragLeave}
      onDrop={onDrop}
    >
      {/* Main Container. 16px, concentric with the 8px send button set 8px
          into the corner (DESIGN.md, Radius). Terminal and Soft state
          their own corners for the composer, its send button and the rows
          stacked on it (--cs-composer-*, styles/cardStyle.css); the drag and
          recording overlays repeat the corner, and the open styles draw the
          edge in theirs. */}
      <div
        ref={attachChatContainer}
        className={cn('chat-input-container owns-its-edge scroll-mb-2 flex flex-col items-stretch relative z-10 rounded-[var(--cs-composer-radius,16px)] cursor-text border border-[color:var(--color-shell-edge,var(--color-border-input))] bg-(--color-bg-card)', isListening && 'recording')}
        onClick={(event) => {
          // A press on a control in the box is that control's, and the deck
          // above the text is its own surface: focusing the text there would
          // raise a phone's keyboard over the page the press was about.
          if (event.target instanceof Element && event.target.closest('button, a, input, select, [role="button"], [data-composer-deck]')) return;
          textareaRef.current?.focus();
        }}
      >
        {isListening && <VoiceOverlay />}
        <div className="flex flex-col px-3 pt-3 pb-2 gap-2">

          {/* The deck folds, and the column's gap below it cannot: a gap lands
              whole the frame the deck mounts and vanishes the frame it hides.
              The deck carries the gap as its own bottom margin, inside the
              fold, and this cancels the column's. */}
          {deck && <div data-composer-deck className="-mb-2">{deck}</div>}

          {/* Widget context deck — top card visible, others peek behind */}
          {widgetSnapshots.length > 0 && (
            <ChatInputWidgetDeck
              snapshots={widgetSnapshots}
              fanned={deckFanned}
              boundaryRef={chatContainerRef}
              onToggle={() => setDeckFanned((p) => !p)}
              onCollapse={() => setDeckFanned(false)}
              onRemove={(widgetId) => {
                // Local + bus: remove from this input AND publish detach so
                // every other mounted input drops the same card.
                setWidgetSnapshots((prev) => prev.filter((s) => s.widget_id !== widgetId));
                ContextBus.detach(widgetId);
              }}
              onClear={() => {
                ContextBus.clear();
                setDeckFanned(false);
              }}
            />
          )}

          <ComposerAttachments
            chartImage={chartImage}
            onRemoveChartImage={onRemoveChartImage}
            files={attachedFiles}
            onRemoveFile={removeFile}
            onRetryFile={retryFile}
            isMobile={isMobile}
          />

          {/* Autocomplete dropdowns (above textarea). A closing menu keeps
              the list it last showed through its exit: the machines empty
              their lists the moment they close. */}
          <AnimatePresence>
            {mentionsOpen && (
              <MentionAutocompleteList
                key="mentions"
                listRef={mentions.listRef}
                files={mentions.filtered}
                activeIndex={mentions.activeIndex}
                hasWorkspaceFiles={workspaceFiles.length > 0}
                onSelect={mentions.selectFile}
                onHover={mentions.setActiveIndex}
              />
            )}
            {slash.open && (
              <SlashCommandList
                key="slash"
                listRef={slash.listRef}
                commands={slash.filtered}
                activeIndex={slash.activeIndex}
                onSelect={slash.selectCommand}
                onHover={slash.setActiveIndex}
              />
            )}
          </AnimatePresence>

          {/* Textarea, starting on the line the chips end on while it fits
              there: its basis grows with the draft until it takes a line of
              its own, where the text has the whole width to wrap in. */}
          <div className="relative flex flex-wrap items-start gap-x-2">
            <ComposerChips chips={chips} onRemove={removeMention} />
            <div
              className="relative min-w-0 flex-[1_1_5rem]"
              style={chips.length > 0 ? { flexBasis: message.includes('\n') ? '100%' : `max(5rem, ${message.length + 1}ch)` } : undefined}
            >
            <DraftTokenLayer
              text={message}
              tokens={draftTokens}
              quotes={quotes}
              layerRef={tokenLayerRef}
              onQuotePress={handleQuotePress}
              onWheel={(e) => { if (textareaRef.current) textareaRef.current.scrollTop += e.deltaY; }}
              className={`font-content ${isMobile ? 'text-base' : 'text-sm'} leading-relaxed transition-opacity ${isListening ? 'opacity-20' : 'opacity-100'}`}
            />
            <textarea
              ref={textareaRef}
              value={message}
              onChange={handleChange}
              onPaste={handleTextareaPaste}
              onKeyDown={handleKeyDown}
              onSelect={handleSelect}
              onBlur={handleBlur}
              onScroll={(e) => { if (tokenLayerRef.current) tokenLayerRef.current.scrollTop = e.currentTarget.scrollTop; }}
              onCompositionStart={() => {
                // Terminate voice input if IME starts
                if (isListening) {
                  stopListening();
                }
              }}
              // Named apart from the placeholder, which says what state the
              // thread is in and is empty while listening.
              aria-label={t("chat.composer.messageLabel")}
              placeholder={isListening || chips.length > 0 ? "" : (placeholder ?? t("chat.placeholderDefault"))}
              className={`relative font-content w-full bg-transparent border-0 text-(--color-text-primary) ${isMobile ? 'text-base' : 'text-sm'} placeholder:text-(--color-text-tertiary) resize-none overflow-y-auto leading-relaxed block transition-opacity ${isListening ? 'opacity-20' : 'opacity-100'}`}
              rows={minRows}
              disabled={disabled}
              style={{ minHeight: `${minRows * 1.5}em` }}
            />
            </div>
          </div>

          {/* Action Bar */}
          <div className="flex gap-2 w-full items-center">
            {/* Left Tools — min-width:auto (no min-w-0) on purpose: the group's
                min-content floor makes the RIGHT group absorb any over-squeeze
                (model label truncates) instead of the groups overlapping. */}
            <div className="relative flex-1 flex items-center shrink gap-1">
              {/* Attach Button */}
              <button
                onClick={(e) => { e.stopPropagation(); fileInputRef.current?.click(); }}
                disabled={disabled}
                className="press inline-flex flex-none items-center justify-center h-8 w-8 rounded-lg text-(--color-icon-muted) hover:text-(--color-text-muted) hover:bg-foreground/5"
                type="button"
                aria-label={t('chat.composer.attachFile')}
              >
                <Plus className="w-4 h-4" />
              </button>

              {/* Token Usage Ring */}
              {ringVisible && tokenUsage && (
                <TokenUsageRing tokenUsage={tokenUsage} threadId={threadId} />
              )}

              {/* Chart Capture (trading only) */}
              {onCaptureChart && (
                <button
                  onClick={(e) => { e.stopPropagation(); onCaptureChart(); }}
                  className="press inline-flex flex-none items-center justify-center h-8 w-8 rounded-lg text-(--color-icon-muted) hover:text-(--color-text-muted) hover:bg-foreground/5"
                  type="button"
                  title={t('chat.composer.attachChart')}
                  aria-label={t('chat.composer.captureChart')}
                >
                  <ChartCandlestick className="w-4 h-4" />
                </button>
              )}

              {/* Foldable toggles — whatever fits, in priority order */}
              {inlineItems.map((item) => (
                <React.Fragment key={item.id}>{item.inline({})}</React.Fragment>
              ))}

              {foldedItems.length > 0 && (
                <ToolbarOverflowMenu
                  items={foldedItems}
                  side={dropdownDirection === 'down' ? 'bottom' : 'top'}
                  container={isMobile ? chatContainer : undefined}
                />
              )}

              {/* Measured where it paints, for the fold's last resort. The
                  gap before it moves inside (its own cancelled, its child's
                  margin in its place), so an absent or hidden status takes
                  no room and the measured width is the whole cost. */}
              <span ref={statusRef} className="-ml-1 flex flex-none items-center *:ml-1">{toolbarStatus}</span>

              {/* Hidden measure row — the SAME components rendered geometry-only
                  (no handlers, menus, or aria state), so the fold budget can
                  never drift from what actually paints. */}
              <div
                ref={measureRowRef}
                aria-hidden
                className="invisible pointer-events-none absolute top-0 left-[-9999px] flex items-center gap-1 whitespace-nowrap"
              >
                {foldItems.filter((i) => i.visible).map((item) => (
                  <span key={item.id} data-measure={item.id}>{item.inline({ measureOnly: true })}</span>
                ))}
                <span data-measure="model">
                  <ModelTriggerMeasure pill="model" selectedModel={pillModel} metadata={modelMetadata} />
                </span>
                <span data-measure="modelFloor">
                  <ModelTriggerMeasure pill="model" selectedModel={pillModel} metadata={modelMetadata} floor />
                </span>
              </div>
            </div>

            {/* Right Tools */}
            <div className="flex flex-row items-center min-w-0 gap-1">
              <ChatInputModelMenu
                selectedModel={pillModel}
                metadata={modelMetadata}
                payers={payers}
                groups={modelGroupList}
                favorites={favorites.models}
                recent={recentModels}
                onToggleFavorite={(m, on) => (on ? favorites.add(m) : favorites.remove(m))}
                onSelectModel={handlePickModel}
                rowStatus={modelRowStatus}
                planMeter={claudeCode.meter}
                dropdownDirection={dropdownDirection}
                container={chatContainer}
                draftRef={textareaRef}
                disabled={modelsLoading}
                nameHidden={fold.nameHidden}
              />
              {fold.inline.has(effortItem.id) && effortItem.inline({})}
              {/* Voice input, when enabled in settings. */}
              {micReserved && (
                <VoiceButton
                  listening={isListening}
                  hidden={isLoading}
                  disabled={disabled}
                  onToggle={toggleListening}
                />
              )}
              {/* Also beside Stop: Enter steering a running turn waits on the files too. */}
              {filesHold && <FilesHoldHint uploading={uploadingCount} failed={failedCount} />}
              <SendStopButton
                running={isLoading || isCompacting}
                canSend={!!hasContent && !disabled && !filesHold}
                onSend={handleSend}
                onStop={onStop}
              />
            </div>
          </div>
        </div>
      </div>

      <DropOverlay active={isDragging} />

      {/* Hidden File Input */}
      <input
        ref={fileInputRef}
        type="file"
        multiple
        className="hidden"
        onChange={(e) => {
          if (e.target.files) handleFiles(e.target.files);
          e.target.value = '';
        }}
      />
    </div>
  );
}

// Memoized: ChatView re-renders on every streamed chunk, and without the memo
// the whole composer subtree (model menu, attachment deck, autocomplete) rides
// along. Effective only while ChatView keeps the callback props identity-stable
// (latest-ref pattern there) — keystrokes stay internal state either way.
export default React.memo(ChatInput);
