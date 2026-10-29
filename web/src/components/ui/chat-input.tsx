import React, { useState, useRef, useEffect, useCallback, useImperativeHandle, useMemo } from 'react';
import { Plus, ChartCandlestick } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { TokenUsageRing, type TokenUsageData } from './token-usage-ring';
import { usePreferences } from '@/hooks/usePreferences';
import { useTurnModels } from '@/hooks/useTurnModels';
import { useEffectiveTuning, useModelProfileWriter } from '@/hooks/useModelProfile';
import { useFeatureEnabled } from '@/hooks/useFeatures';
import { homeWorkspaceQuery } from '@/hooks/useHomeWorkspace';
import { useAllModels } from '@/hooks/useAllModels';
import { useProviderCatalog } from '@/hooks/useProviderCatalog';
import { useLapsedSignIn } from '@/hooks/useLapsedSignIn';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useFavoriteModels } from '@/hooks/useFavoriteModels';
import { useSeededModel } from '@/hooks/useSeededModel';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';
import { AnimatePresence } from '@/lib/framer';
import { isImeComposing } from '@/lib/ime';
import { cn } from '@/lib/utils';
import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';
import './chat-input.css';
import type { ComposerScope, ModelOptions, ReadyAttachment, SlashCommand, ThreadTuning, Workspace } from './chat-input.types';
import { isLargePaste, getModelDisplayName } from './chat-input.helpers';
import { effortLabelFor, hasTuningMenu, resolveEffort } from '@/lib/modelTuning';
import { ChatInputWidgetDeck, MentionAutocompleteList, SlashCommandList } from './chat-input.parts';
import {
  ComposerAttachments, ComposerPills, DropOverlay, SendStopButton, ToolbarOverflowMenu, VoiceButton, VoiceOverlay,
} from './chat-input.composerParts';
import { ChatInputEffortMenu, ChatInputModelMenu, ModelTriggerMeasure } from './chat-input.modelMenu';
import type { ModelRowStatus } from './chat-input.modelPicker';
import { CLAUDE_CODE_TOKEN_URL, isClaudeCodeModel } from '@/lib/claudeCode';
import { deriveRecentModels } from './chat-input.models';
import { modelGroups } from '@/lib/modelGroups';
import { useToolbarItems } from './chat-input.toolbar';
import {
  useToolbarFold, CONTAINER_BORDER, CONTAINER_PX, GROUP_GAP, ICON_BUTTON_W, TOOLBAR_GAP,
} from './chat-input.useToolbarFold';
import { useMentions } from './chat-input.useMentions';
import { useSlashCommands } from './chat-input.useSlashCommands';
import { speechSupported, useVoiceInput } from './chat-input.useVoiceInput';
import { useFileAttachments } from './chat-input.useFileAttachments';
import { useDeviceTuningLift } from './chat-input.useDeviceTuningLift';
import { readSubagentsDefault } from '@/lib/subagentsDefault';
import { formatContextBlock } from './chat-input.contextBlocks';

/** Autosize cap for the composer textarea; past this the box scrolls. */
const MAX_TEXTAREA_HEIGHT = 200;

/** A thread that follows Settings for both. */
const FOLLOWS_SETTINGS = { reasoningEffort: null, fastMode: null } as const;

export interface ChatInputHandle {
  getModelOptions: () => ModelOptions;
  addContext: (ctx: { path?: string; snippet?: string; label?: string; locator?: string; lineStart?: number; lineEnd?: number; lineCount?: number; source?: string }) => void;
  /**
   * Imperatively add a widget context snapshot to the deck. Local-only —
   * this does NOT publish to ContextBus. Use this when re-seeding the deck
   * from `location.state` after a navigate (e.g. dashboard → chat handoff).
   * Use `ContextBus.attach` when the user clicks "+" on a widget so every
   * mounted chat input sees it.
   */
  addWidgetSnapshot: (snapshot: WidgetContextSnapshot) => void;
  setValue: (text: string) => void;
  /** Puts back text the user wrote elsewhere without losing what is typed
   *  here: into an empty box, else after it, a blank line between. Files a
   *  send took come back beside what is attached now. */
  giveBack: (text: string, files?: ReadyAttachment[]) => void;
  /** Sends what is written, as Enter does. Returns whether there was
   *  anything to send. */
  send: () => boolean;
}


export interface ChatInputProps {
  onSend: (message: string, attachments: ReadyAttachment[], slashCommands: SlashCommand[], modelOptions: ModelOptions) => void;
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
  tokenUsage?: TokenUsageData | null;
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
   */
  toolbarStatus?: React.ReactNode;
}

/* --- MAIN COMPONENT --- */

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
  // Action commands (e.g. /compact) — dispatched on send, not on selection
  onAction = null,
  // Model selector
  model: hostModel,
  onPickModel = null,
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
  ref,
}: ChatInputProps & { ref?: React.Ref<ChatInputHandle> }) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const isMobile = useIsMobile();
  const { preferences, isLoaded: preferencesRead } = usePreferences();
  const marketWatchEnabled = useFeatureEnabled('market_watch');
  const { models: visibleModels, validModelNames, metadata: modelMetadata, payers, claudeCodeState, isLoading: modelsLoading } = useAllModels();
  const lapsedSignIn = useLapsedSignIn();
  const otherPref = (preferences as Record<string, Record<string, unknown>> | null)?.other_preference;
  const [message, setMessage] = useState('');
  const { attachedFiles, setAttachedFiles, isDragging, handleFiles, restoreFiles, removeFile, onDragEnter, onDragOver, onDragLeave, onDrop, handlePaste } = useFileAttachments({ disabled });
  const [watchMode, setWatchMode] = useState(false);
  // The toggle state survives the feature going off (the pill just hides), so
  // every payload derives from this gated value.
  const effectiveMarketWatch = watchMode && marketWatchEnabled;

  // The model a send names. A thread host owns it. Without one the composer
  // holds it, starting on the account default, which with nothing saved is
  // the deployment's (see useSlotDefaultModels for why it waits on the
  // preference), and following the default when it changes. A turn in Home
  // swaps a model it never runs for the default (see useTurnModels).
  const turn = useTurnModels(inHome || scope === 'all');
  const [ownModel, setOwnModel] = useSeededModel(turn.defaultModel);
  const pillModel = turn.resolve(hostModel === undefined ? ownModel : hostModel);

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
  const queryClient = useQueryClient();
  const { data: homeWorkspaceId = null } = useQuery({
    ...homeWorkspaceQuery(queryClient),
    enabled: scope === 'all',
    select: (workspace) => workspace.workspace_id,
  });
  const slashWorkspaceId = scope === 'all' ? homeWorkspaceId : selectedWorkspaceId;
  const slash = useSlashCommands({ textareaRef, message, setMessage, workspaceId: slashWorkspaceId });
  const {
    mentionedFiles, setMentionedFiles, pruneRemoved: pruneMentions, detectTrigger: detectMention,
    close: closeMentions, reset: resetMentions, handleKeyDown: mentionKeyDown, open: mentionsOpen,
  } = mentions;
  const {
    slashCommands, pruneRemoved: pruneSlash, detectTrigger: detectSlash,
    close: closeSlash, reset: resetSlash, handleKeyDown: slashKeyDown,
  } = slash;

  // Voice input (speech recognition) — see chat-input.useVoiceInput.
  const { isListening, toggleListening, stopListening } = useVoiceInput({ message, setMessage, isLoading });

  // The handle is built before the send below, so it reaches it through here.
  const sendRef = useRef<() => boolean>(() => false);

  // Expose addContext method for external callers (e.g. FilePanel, message selection)
  useImperativeHandle(ref, () => ({
    getModelOptions() {
      return {
        model: pillModel,
        reasoningEffort: sentEffort,
        fastMode: resolvedFastMode,
        marketWatch: effectiveMarketWatch,
        subagentsAllowed: draftSubagents,
        ...ownOverrides,
      };
    },
    addContext({ path, snippet, label, locator, lineStart, lineEnd, lineCount, source }) {
      if (snippet) {
        // Snippet context — add pill with snippet data, don't modify textarea
        setMentionedFiles((prev) => {
          // Deduplicate by exact snippet content (handles multiple selections)
          if (prev.some((f) => f.snippet === snippet && f.path === (path || '') && f.locator === locator && f.source === (source || undefined))) return prev;
          return [...prev, { path: path || '', snippet, label, locator, lineStart, lineEnd, lineCount, source }];
        });
      } else if (path) {
        // Whole file context — same behavior as selectFile via @mention
        setMentionedFiles((prev) => {
          if (prev.some((f) => f.path === path && !f.snippet)) return prev;
          return [...prev, { path }];
        });
        // Insert @path into message text
        setMessage((prev) => {
          if (prev.includes('@' + path)) return prev;
          const prefix = prev && !prev.endsWith(' ') ? prev + ' ' : prev;
          return prefix + '@' + path + ' ';
        });
      }
      // Focus the textarea
      setTimeout(() => textareaRef.current?.focus(), 0);
    },
    addWidgetSnapshot(snapshot) {
      setWidgetSnapshots((prev) => {
        if (prev.some((s) => s.widget_id === snapshot.widget_id)) return prev;
        return [...prev, snapshot];
      });
    },
    setValue(text) {
      setMessage(text);
      setTimeout(() => textareaRef.current?.focus(), 0);
    },
    giveBack(text, files = []) {
      if (text) {
        setMessage((prev) => {
          const typed = prev.trimEnd();
          if (!typed) return text;
          if (typed === text || typed.endsWith(`\n\n${text}`)) return prev;
          return `${typed}\n\n${text}`;
        });
      }
      restoreFiles(files);
      setTimeout(() => textareaRef.current?.focus(), 0);
    },
    send() {
      return sendRef.current();
    },
  }), [pillModel, sentEffort, resolvedFastMode, effectiveMarketWatch, draftSubagents, ownOverrides, setMentionedFiles, restoreFiles]);

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

  // Auto-resize textarea; past the cap the box scrolls (overflow-y-auto).
  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
      textareaRef.current.style.height = Math.min(textareaRef.current.scrollHeight, MAX_TEXTAREA_HEIGHT) + 'px';
    }
  }, [message]);

  // Large text pastes condense into a pill (snippet-mention mechanics) instead
  // of flooding the draft. File pastes keep priority — handlePaste prevents
  // default when it takes the clipboard.
  const handleTextareaPaste = useCallback((e: React.ClipboardEvent<HTMLTextAreaElement>) => {
    handlePaste(e);
    if (e.defaultPrevented) return;
    const text = e.clipboardData.getData('text/plain');
    if (!isLargePaste(text)) return;
    e.preventDefault();
    const lineCount = text.split('\n').length;
    setMentionedFiles((prev) => {
      if (prev.some((f) => f.source === 'paste' && f.snippet === text)) return prev;
      return [...prev, { path: '', snippet: text, label: t('context.pastedLines', { count: lineCount }), lineCount, source: 'paste' }];
    });
  }, [handlePaste, setMentionedFiles, t]);

  // Detect @ and / triggers on input change
  const handleChange = useCallback((e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const val = e.target.value;
    setMessage(val);
    const cursorPos = e.target.selectionStart;

    // Pills whose /{command} or @mention text was deleted go with it
    pruneSlash(val);
    pruneMentions(val);

    // A live /slash trigger wins: the two menus never show at once.
    if (detectSlash(val, cursorPos)) closeMentions();
    else detectMention(val, cursorPos);
  }, [pruneSlash, pruneMentions, detectSlash, detectMention, closeMentions]);

  // Close both autocompletes on blur
  const handleBlur = useCallback(() => {
    setTimeout(() => {
      closeMentions();
      closeSlash();
    }, 200);
  }, [closeMentions, closeSlash]);

  // --- Send ---
  const hasContent = message.trim() || attachedFiles.length > 0 || !!chartImage || mentionedFiles.some((f) => f.snippet) || widgetSnapshots.length > 0 || hasExternalContext;

  const handleSend = useCallback(() => {
    if (!hasContent || disabled) return;

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
      attachedFiles.forEach((f) => { if (f.preview) URL.revokeObjectURL(f.preview); });
      setAttachedFiles([]);
      resetMentions();
      if (widgetSnapshots.length > 0) {
        ContextBus.clear();
      }
      setDeckFanned(false);
      if (textareaRef.current) {
        textareaRef.current.style.height = 'auto';
      }
      return;
    }

    const readyAttachments = attachedFiles
      .filter((f) => f.dataUrl)
      .map((f) => ({
        file: f.file,
        dataUrl: f.dataUrl,
        type: f.type,
        preview: f.preview,
      }));
    // Append snippet blocks to message for mentions that have snippets
    let finalMessage = message;
    const snippetMentions = mentionedFiles.filter((f) => f.snippet);
    if (snippetMentions.length > 0) {
      const blocks = snippetMentions.map((f) => formatContextBlock(f, t));
      finalMessage = finalMessage.trimEnd() + '\n' + blocks.join('\n');
    }
    onSend(finalMessage, readyAttachments, slashCommands, {
      model: pillModel,
      reasoningEffort: sentEffort,
      fastMode: resolvedFastMode,
      marketWatch: effectiveMarketWatch,
      subagentsAllowed: draftSubagents,
      ...ownOverrides,
      widgetSnapshots: widgetSnapshots.length ? widgetSnapshots : undefined,
    });
    setMessage('');
    attachedFiles.forEach(f => { if (f.preview) URL.revokeObjectURL(f.preview); });
    setAttachedFiles([]);
    resetMentions();
    resetSlash();
    // Clear widget deck on send. Other mounted chat inputs hear the same
    // clear via ContextBus so the deck empties everywhere — single source
    // of truth.
    if (widgetSnapshots.length > 0) {
      ContextBus.clear();
    }
    setDeckFanned(false);
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
    }
  }, [hasContent, disabled, message, effectiveMarketWatch, draftSubagents, attachedFiles, setAttachedFiles, onSend, onAction,
    mentionedFiles, resetMentions, slashCommands, resetSlash,
    pillModel, sentEffort, resolvedFastMode, ownOverrides, widgetSnapshots, t]);
  useEffect(() => {
    sendRef.current = () => {
      if (!hasContent || disabled) return false;
      handleSend();
      return true;
    };
  }, [hasContent, disabled, handleSend]);

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
      if (!e.shiftKey) {
        e.preventDefault();
        handleSend();
      }
    }
  }, [slashKeyDown, mentionKeyDown, mentionsOpen, handleSend, isListening, stopListening, message]);

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

  const ringVisible = !!(tokenUsage && tokenUsage.threshold > 0 && tokenUsage.total > 0);
  const micReserved = speechSupported && !!otherPref?.voice_input_enabled;
  const { measureRowRef, fold } = useToolbarFold({
    containerRef: chatContainerRef,
    items: toolbarItems,
    // The model pill is measured too, but it isn't foldable. Its label joins
    // the key because an authored display_name comes with the models request,
    // which can land after the pill was first measured under the key's label.
    measureKey: `${pillModel}|${getModelDisplayName(pillModel, modelMetadata)}|${resolvedEffort}|${resolvedFastMode && isCodexModel}`,
    // Left of the first pill: container border + px-3, the attach button, and
    // the optional ring / chart-capture button (see the action bar below).
    fixedLeft: CONTAINER_BORDER + CONTAINER_PX + ICON_BUTTON_W
      + (ringVisible ? TOOLBAR_GAP + ICON_BUTTON_W : 0)
      + (onCaptureChart ? TOOLBAR_GAP + ICON_BUTTON_W : 0),
    // Right group: the model pill (measured), the mic, and send/stop. A turn
    // hides the mic but keeps its room, so the fold set holds through it.
    rightReserve: (widths) => GROUP_GAP + (widths.model ?? 0)
      + (micReserved ? TOOLBAR_GAP + ICON_BUTTON_W : 0)
      + TOOLBAR_GAP + ICON_BUTTON_W,
  });
  const inlineItems = toolbarItems.filter((i) => fold.inline.has(i.id));
  const foldedItems = toolbarItems.filter((i) => fold.folded.includes(i.id));

  const favorites = useFavoriteModels();
  const { entryFor } = useProviderCatalog();
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
    .filter((m) => !favorites.models.includes(m) && turn.offers(m));
  // A Claude Code model runs a turn while its token works or could not be
  // checked, as admission lets it; otherwise its row says why, and where the
  // user can fix it.
  const modelRowStatus = (m: string): ModelRowStatus => {
    if (isClaudeCodeModel(modelMetadata[m])) {
      switch (claudeCodeState) {
        case 'checking': return { unavailable: t('modelAccess.status.working') };
        case 'no_token': return { unavailable: t('chat.modelSelector.claudeCodeSignedOut'), fix: () => navigate(CLAUDE_CODE_TOKEN_URL) };
        case 'rejected': return { unavailable: t('chat.modelSelector.claudeCodeRejected'), fix: () => navigate(CLAUDE_CODE_TOKEN_URL) };
      }
    }
    return { tag: lapsedSignIn(m) ? t('modelAccess.status.needsReauth') : undefined };
  };
  const showEffort = hasTuningMenu(reasoningEfforts, isCodexModel);

  return (
    <div
      className="relative w-full"
      onDragEnter={onDragEnter}
      onDragOver={onDragOver}
      onDragLeave={onDragLeave}
      onDrop={onDrop}
    >
      {/* Main Container. 22px is concentric with the 12px send button 10px in
          from the bottom edge (DESIGN.md, Radius); the drag and recording
          overlays repeat it. */}
      <div
        ref={attachChatContainer}
        className={cn('chat-input-container owns-its-edge flex flex-col items-stretch relative z-10 rounded-[22px] cursor-text border border-(--color-border-input) bg-(--color-bg-card)', isListening && 'recording')}
        onClick={() => textareaRef.current?.focus()}
      >
        {isListening && <VoiceOverlay />}
        <div className="flex flex-col px-3 pt-3 pb-2 gap-2">

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

          <ComposerPills
            slashCommands={slashCommands}
            onRemoveCommand={slash.removeCommand}
            mentionedFiles={mentionedFiles}
            onRemoveMention={mentions.removeMention}
          />

          <ComposerAttachments
            chartImage={chartImage}
            onRemoveChartImage={onRemoveChartImage}
            files={attachedFiles}
            onRemoveFile={removeFile}
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

          {/* Textarea */}
          <div className="relative">
            <textarea
              ref={textareaRef}
              value={message}
              onChange={handleChange}
              onPaste={handleTextareaPaste}
              onKeyDown={handleKeyDown}
              onBlur={handleBlur}
              onCompositionStart={() => {
                // Terminate voice input if IME starts
                if (isListening) {
                  stopListening();
                }
              }}
              placeholder={isListening ? "" : (placeholder ?? t("chat.placeholderDefault"))}
              className={`font-content w-full bg-transparent border-0 text-(--color-text-primary) ${isMobile ? 'text-base' : 'text-sm'} placeholder:text-(--color-text-tertiary) resize-none overflow-y-auto leading-relaxed block transition-opacity ${isListening ? 'opacity-20' : 'opacity-100'}`}
              rows={minRows}
              disabled={disabled}
              style={{ minHeight: `${minRows * 1.5}em` }}
            />
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
                <TokenUsageRing tokenUsage={tokenUsage} />
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

              {toolbarStatus}

              {/* Hidden measure row — the SAME components rendered geometry-only
                  (no handlers, menus, or aria state), so the fold budget can
                  never drift from what actually paints. */}
              <div
                ref={measureRowRef}
                aria-hidden
                className="invisible pointer-events-none absolute top-0 left-[-9999px] flex items-center gap-1 whitespace-nowrap"
              >
                {toolbarItems.filter((i) => i.visible).map((item) => (
                  <span key={item.id} data-measure={item.id}>{item.inline({ measureOnly: true })}</span>
                ))}
                <span data-measure="model">
                  <ModelTriggerMeasure selectedModel={pillModel} metadata={modelMetadata} effortLabel={effortLabelFor(t, resolvedEffort)} fast={resolvedFastMode === true && isCodexModel} showEffort={showEffort} />
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
                favorites={favorites.models.filter(turn.offers)}
                recent={recentModels}
                onToggleFavorite={(m, on) => (on ? favorites.add(m) : favorites.remove(m))}
                onSelectModel={handlePickModel}
                rowStatus={modelRowStatus}
                dropdownDirection={dropdownDirection}
                container={chatContainer}
                draftRef={textareaRef}
                disabled={modelsLoading}
              />
              <ChatInputEffortMenu
                effortLabel={effortLabelFor(t, resolvedEffort)}
                reasoningEffort={resolvedEffort}
                defaultEffort={settingsEffort}
                reasoningEfforts={reasoningEfforts}
                onReasoningEffortChange={handleReasoningEffortChange}
                fastMode={resolvedFastMode}
                onFastModeChange={handleFastModeChange}
                isCodexModel={isCodexModel}
                dropdownDirection={dropdownDirection}
                container={chatContainer}
                taskCost={pillModel ? modelMetadata[pillModel]?.task_cost : undefined}
                disabled={modelsLoading}
              />
              {/* Voice input, when enabled in settings. */}
              {micReserved && (
                <VoiceButton
                  listening={isListening}
                  hidden={isLoading}
                  disabled={disabled}
                  onToggle={toggleListening}
                />
              )}
              <SendStopButton
                running={isLoading || isCompacting}
                canSend={!!hasContent && !disabled}
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
