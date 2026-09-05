import React, { useState, useRef, useEffect, useCallback, useMemo, forwardRef, useImperativeHandle } from 'react';
import {
  Plus, ArrowUp, X, FileText, Archive, Square, ClipboardList,
  ChartCandlestick, TextSelect, MoreHorizontal, Mic, MicOff,
} from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { TokenUsageRing, type TokenUsageData } from './token-usage-ring';
import {
  DropdownMenu, DropdownMenuTrigger, DropdownMenuContent, DropdownMenuSeparator,
} from './dropdown-menu';
import { Loader } from './loader';
import { usePreferences } from '@/hooks/usePreferences';
import { useEffectiveTuning, useModelProfileWriter } from '@/hooks/useModelProfile';
import { useFeatureEnabled } from '@/hooks/useFeatures';
import { useAllModels } from '@/hooks/useAllModels';
import { useIsMobile } from '@/hooks/useIsMobile';
import { deriveQuickAccessModels } from './chat-input.models';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';
import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';
import './chat-input.css';
import type { ModelOptions, ReadyAttachment, SlashCommand, Workspace } from './chat-input.types';
import { getSlashCommandIcon, isLargePaste } from './chat-input.helpers';
import { effortLabelFor } from '@/lib/modelTuning';
import type { ModelProfile } from '@/lib/modelTuning';
import {
  ChatInputWidgetDeck, FilePreviewCard, MentionAutocompleteList, SlashCommandList,
} from './chat-input.parts';
import { ChatInputModelMenu, ModelTriggerMeasure } from './chat-input.modelMenu';
import { useToolbarItems } from './chat-input.toolbar';
import {
  useToolbarFold, CONTAINER_BORDER, CONTAINER_PX, GROUP_GAP, ICON_BUTTON_W, TOKEN_RING_W, TOOLBAR_GAP,
} from './chat-input.useToolbarFold';
import { useMentions } from './chat-input.useMentions';
import { useSlashCommands } from './chat-input.useSlashCommands';
import { speechSupported, useVoiceInput } from './chat-input.useVoiceInput';
import { useFileAttachments } from './chat-input.useFileAttachments';
import { modelPrefs, modelProfile } from '@/lib/modelPreferences';

/** Autosize cap for the composer textarea; past this the box scrolls. */
const MAX_TEXTAREA_HEIGHT = 200;


export interface ChatInputHandle {
  getModelOptions: () => ModelOptions;
  addContext: (ctx: { path?: string; snippet?: string; label?: string; lineStart?: number; lineEnd?: number; lineCount?: number; source?: string }) => void;
  /**
   * Imperatively add a widget context snapshot to the deck. Local-only —
   * this does NOT publish to ContextBus. Use this when re-seeding the deck
   * from `location.state` after a navigate (e.g. dashboard → chat handoff).
   * Use `ContextBus.attach` when the user clicks "+" on a widget so every
   * mounted chat input sees it.
   */
  addWidgetSnapshot: (snapshot: WidgetContextSnapshot) => void;
  setValue: (text: string) => void;
  /** Imperatively change the selected model (e.g. the model-fallback
   *  "Switch to X" action) so the next send uses it. */
  setModel: (model: string) => void;
}


export interface ChatInputProps {
  onSend: (message: string, planMode: boolean, attachments: ReadyAttachment[], slashCommands: SlashCommand[], modelOptions: ModelOptions) => void;
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
  mode?: 'fast' | 'ptc';
  onModeChange?: (mode: 'fast' | 'ptc') => void;
  /** When set, disables switching into PTC mode and shows this reason as a tooltip. */
  ptcDisabledReason?: string | null;
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
  initialModel?: string | null;
  /** Reports the live model selection (fires on mount and every change). */
  onModelChange?: ((model: string | null) => void) | null;
  threadModels?: string[];
  dropdownDirection?: 'up' | 'down';
  minRows?: number;
}

/* --- MAIN COMPONENT --- */

const ChatInput = forwardRef<ChatInputHandle, ChatInputProps>(function ChatInput({
  onSend,
  disabled = false,
  isLoading = false,
  isCompacting = false,
  onStop,
  placeholder = 'Type / for skills, @ for files',
  files: workspaceFiles = [],
  // Mode toggle
  mode,
  onModeChange,
  ptcDisabledReason = null,
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
  initialModel = null,
  onModelChange = null,
  // All models used in this thread (shown in primary menu)
  threadModels: threadModelsProp = [],
  // Dropdown direction: 'up' (default, for bottom-positioned inputs) or 'down' (for mid-page inputs like ThreadGallery)
  dropdownDirection = 'up',
  // Minimum visible rows for the textarea (default 1 = compact; pass 2 for roomier non-ChatView callsites)
  minRows = 1,
}, ref) {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const { preferences } = usePreferences();
  const marketWatchEnabled = useFeatureEnabled('market_watch');
  const { validModelNames, metadata: modelMetadata } = useAllModels();
  const otherPref = (preferences as Record<string, Record<string, unknown>> | null)?.other_preference;
  const starredModels = Array.isArray(otherPref?.starred_models)
    ? (otherPref.starred_models as unknown[]).filter((m): m is string => typeof m === 'string')
    : [];
  // Model routing moved to its own bucket; starred_models above did not,
  // so the two reads deliberately differ.
  const mPref = modelPrefs(preferences);
  const preferredModel = mPref.preferred_model || null;
  const preferredFlashModel = mPref.preferred_flash_model || null;
  const [message, setMessage] = useState('');
  const { attachedFiles, setAttachedFiles, isDragging, handleFiles, removeFile, onDragOver, onDragLeave, onDrop, handlePaste } = useFileAttachments({ mode });
  const [planMode, setPlanMode] = useState(false);
  const [watchMode, setWatchMode] = useState(false);
  // Market watch is a PTC capability. The toggle state survives a switch to
  // flash (the pill just hides), so every payload derives from this gated
  // value — a watch flipped on in PTC must never ride a flash send.
  const effectiveMarketWatch = watchMode && marketWatchEnabled && mode !== 'fast';

  // Model selector state — use flash model preference when in flash mode
  const modePreferredModel = mode === 'fast' ? (preferredFlashModel || preferredModel) : preferredModel;
  const effectiveInitialModel = initialModel || modePreferredModel;
  const [selectedModel, setSelectedModel] = useState<string | null>(effectiveInitialModel);

  // Per-model tuning is an account preference, not a device one: the server
  // resolves ``profiles[<model>]`` for turns this input never starts (schedules,
  // automations, subagent role models). ``null`` clears a field; the server
  // merges per model, so siblings and this model's other fields survive.
  const { writeProfile: writeModelProfile } = useModelProfileWriter();

  // Both controls read the account profile straight through, with no local
  // copy: a failed write rolls the cache back and the pill follows it, instead
  // of naming a level the next scheduled turn will not run.
  const { profile: modelTuningProfile, efforts: reasoningEfforts, inherited, effective } =
    useEffectiveTuning(selectedModel);
  // The raw override, which is what the effort menu needs: it renders "Default"
  // from the absence of one. The fast-mode toggle has no such state, so it
  // reads the resolved value below.
  const reasoningEffort = modelTuningProfile.reasoning_effort ?? null;
  const inheritedEffort = inherited.reasoning_effort;
  // The resolved value, which is what a send carries. Tuning writes are
  // optimistic and not awaited, so between picking Default and the PUT landing
  // the composer already reads as cleared while the row still holds the old
  // level. Sending the raw override omits a cleared level and lets the server
  // resolve against the row the write is still replacing, so the turn runs a
  // setting the screen no longer shows. Sending what is displayed cannot
  // disagree with it.
  const resolvedEffort = effective.reasoning_effort;
  const resolvedFastMode = effective.fast_mode;

  // Sync selectedModel when initialModel or preferredModel changes
  useEffect(() => {
    if (effectiveInitialModel) setSelectedModel(effectiveInitialModel);
  }, [effectiveInitialModel]);

  // Mirror the live selection to the host (ChatView gates the fallback
  // suggestion pill on the model the next send will actually use).
  useEffect(() => {
    onModelChange?.(selectedModel);
  }, [selectedModel, onModelChange]);

  const isCodexModel = selectedModel ? modelMetadata[selectedModel]?.sdk === 'codex' : false;

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

  // @file mentions and /slash commands — trigger detection, filtering,
  // selection and menu keyboard navigation live in these two machines. Both
  // return fresh objects each render, so hooks below depend on the individual
  // (memoized) members rather than the machine itself.
  const mentions = useMentions({ textareaRef, message, setMessage, workspaceFiles });
  // The workspace context scopes the slash menu: inside a workspace the
  // enabled-skill list is the workspace-effective one (workspace-scoped
  // skills included, its disables applied).
  const slash = useSlashCommands({ textareaRef, message, setMessage, mode, workspaceId: selectedWorkspaceId });
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

  // Stop button state
  const [isStopping, setIsStopping] = useState(false);

  // Expose addContext method for external callers (e.g. FilePanel, message selection)
  useImperativeHandle(ref, () => ({
    getModelOptions() {
      return {
        model: selectedModel,
        reasoningEffort: resolvedEffort,
        fastMode: resolvedFastMode,
        marketWatch: effectiveMarketWatch,
      };
    },
    addContext({ path, snippet, label, lineStart, lineEnd, lineCount, source }) {
      if (snippet) {
        // Snippet context — add pill with snippet data, don't modify textarea
        setMentionedFiles((prev) => {
          // Deduplicate by exact snippet content (handles multiple selections)
          if (prev.some((f) => f.snippet === snippet && f.path === (path || '') && f.source === (source || undefined))) return prev;
          return [...prev, { path: path || '', snippet, label, lineStart, lineEnd, lineCount, source }];
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
    setModel(model) {
      if (model) setSelectedModel(model);
    },
  }), [selectedModel, resolvedEffort, resolvedFastMode, effectiveMarketWatch, setMentionedFiles]);

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

  // These picks used to be device-local, which meant server-started turns never
  // saw them. Lift what this device still holds into the account profile, then
  // drop the keys. Per field, not per record: a profile that already carries one
  // of the two must not strand the other. And the keys are this value's only
  // copy until the write lands, so they go only once it has.
  const liftedModelRef = useRef<string | null>(null);
  useEffect(() => {
    // The model's own row has to be in hand first: `reasoningEfforts` reads
    // empty both while the catalog loads and for a model that declares no
    // ladder, and the clamp below wants opposite answers for the two.
    const meta = selectedModel ? modelMetadata[selectedModel] : undefined;
    if (!selectedModel || !preferences || !meta || liftedModelRef.current === selectedModel) return;
    liftedModelRef.current = selectedModel;

    const profile = modelProfile(preferences, selectedModel);
    const storedEffort = localStorage.getItem(`reasoning_effort:${selectedModel}`);
    const storedFast = localStorage.getItem(`fast_mode:${selectedModel}`) === 'true';
    const lift: ModelProfile = {};
    // These keys predate per-model ladders, when one fixed list of levels was
    // offered for every model, so a device may hold a level this model never
    // declared. The account rejects it and a run would drop it anyway, so it
    // is discarded with the keys rather than lifted.
    if (storedEffort && reasoningEfforts.includes(storedEffort) && profile.reasoning_effort === undefined) {
      lift.reasoning_effort = storedEffort;
    }
    if (storedFast && profile.fast_mode === undefined) lift.fast_mode = true;

    const dropDeviceKeys = () => {
      localStorage.removeItem(`reasoning_effort:${selectedModel}`);
      localStorage.removeItem(`fast_mode:${selectedModel}`);
    };
    if (Object.keys(lift).length > 0) {
      // No release of the guard on failure: the write moves `preferences`
      // twice, so re-entry is immediate and a rejection would resubmit itself
      // without bound. A failed lift keeps its device keys and is retried on
      // the next mount or model switch.
      writeModelProfile(selectedModel, lift, { onSuccess: dropDeviceKeys });
    } else {
      dropDeviceKeys();
    }
  }, [selectedModel, preferences, writeModelProfile, modelMetadata, reasoningEfforts]);

  const handleReasoningEffortChange = useCallback((effort: string | null) => {
    if (selectedModel) writeModelProfile(selectedModel, { reasoning_effort: effort });
  }, [selectedModel, writeModelProfile]);

  const handleFastModeChange = useCallback((fast: boolean) => {
    // Both states are explicit. `null` here would mean "inherit", which this
    // toggle cannot express — and on an account defaulting to fast it would
    // delete the override and snap straight back to fast. Clearing is the
    // per-model matrix's job, where Default is its own option.
    if (selectedModel) writeModelProfile(selectedModel, { fast_mode: fast });
  }, [selectedModel, writeModelProfile]);

  // Reset isStopping once both a running turn and a compaction have finished.
  useEffect(() => {
    if (!isLoading && !isCompacting) setIsStopping(false);
  }, [isLoading, isCompacting]);

  const handleStop = useCallback(() => {
    if (isStopping) return;
    setIsStopping(true);
    onStop?.();
  }, [isStopping, onStop]);

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
      const blocks = snippetMentions.map((f) => {
        if (f.source === 'chat') {
          return `\n<details>\n<summary>[${t('context.fromAgentResponse')}]</summary>\n\n\`\`\`\n${f.snippet}\n\`\`\`\n</details>`;
        }
        if (f.source === 'paste') {
          return `\n<details>\n<summary>[${t('context.pastedText')}]</summary>\n\n\`\`\`\n${f.snippet}\n\`\`\`\n</details>`;
        }
        const lineInfo = f.lineStart != null
          ? ` (lines ${f.lineStart}-${f.lineEnd}, ${f.lineCount} line${f.lineCount !== 1 ? 's' : ''})`
          : '';
        return `\n<details>\n<summary>@${f.path}${lineInfo}</summary>\n\n\`\`\`\n${f.snippet}\n\`\`\`\n</details>`;
      });
      finalMessage = finalMessage.trimEnd() + '\n' + blocks.join('\n');
    }
    onSend(finalMessage, planMode, readyAttachments, slashCommands, {
      model: selectedModel,
      reasoningEffort: resolvedEffort,
      fastMode: resolvedFastMode,
      marketWatch: effectiveMarketWatch,
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
  }, [hasContent, disabled, message, planMode, effectiveMarketWatch, attachedFiles, setAttachedFiles, onSend, onAction,
    mentionedFiles, resetMentions, slashCommands, resetSlash,
    selectedModel, resolvedEffort, resolvedFastMode, widgetSnapshots, t]);

  // --- Keyboard & Language Detection ---
  const handleKeyDown = useCallback((e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    // If user starts typing while voice input is active, terminate the voice input.
    // We treat any single character key or delete/backspace as a signal to stop.
    if (isListening && (e.key.length === 1 || e.key === 'Backspace' || e.key === 'Delete')) {
      stopListening();
    }

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
    mode, onModeChange, ptcDisabledReason,
    planMode, setPlanMode, watchMode, setWatchMode, marketWatchEnabled,
    workspaces, selectedWorkspaceId, onWorkspaceChange,
  });

  const ringVisible = !!(tokenUsage && tokenUsage.threshold > 0 && tokenUsage.total > 0);
  const micReserved = speechSupported && !!otherPref?.voice_input_enabled;
  const { measureRowRef, fold } = useToolbarFold({
    containerRef: chatContainerRef,
    items: toolbarItems,
    // The model pill is measured too, but it isn't foldable.
    measureKey: `${selectedModel}|${resolvedEffort}|${resolvedFastMode && isCodexModel}`,
    // Left of the first pill: container border + px-3, the attach button, and
    // the optional ring / chart-capture button (see the action bar below).
    fixedLeft: CONTAINER_BORDER + CONTAINER_PX + ICON_BUTTON_W
      + (ringVisible ? TOOLBAR_GAP + TOKEN_RING_W : 0)
      + (onCaptureChart ? TOOLBAR_GAP + ICON_BUTTON_W : 0),
    // Right group: the model pill (measured), the mic, and send/stop. Mic
    // reserve ignores isLoading on purpose — the fold set must not jump when a
    // turn starts and the mic hides.
    rightReserve: (widths) => GROUP_GAP + (widths.model ?? 0)
      + (micReserved ? TOOLBAR_GAP + ICON_BUTTON_W : 0)
      + TOOLBAR_GAP + ICON_BUTTON_W,
  });
  const inlineItems = toolbarItems.filter((i) => fold.inline.has(i.id));
  const foldedItems = toolbarItems.filter((i) => fold.folded.includes(i.id));

  /** Quick-access models for both the desktop submenu and mobile inline expand */
  const moreModelsItems = useMemo(
    () => deriveQuickAccessModels({
      preferredModel, preferredFlashModel, starredModels, validModelNames,
      // Don't repeat models already shown in the primary (selected + thread) section.
      excludeModels: [selectedModel, ...threadModelsProp].filter((m): m is string => !!m),
    }),
    [preferredModel, preferredFlashModel, starredModels, validModelNames, selectedModel, threadModelsProp],
  );

  return (
    <div
      className="relative w-full"
      onDragOver={onDragOver}
      onDragLeave={onDragLeave}
      onDrop={onDrop}
    >
      {/* Main Container */}
      <div
        ref={chatContainerRef}
        className={`chat-input-container flex flex-col items-stretch transition-all duration-200 relative z-10 rounded-2xl cursor-text border border-[var(--color-border-input)] bg-[var(--color-bg-card)] ${isListening ? 'recording' : ''}`}
        onClick={() => textareaRef.current?.focus()}
      >
        {isListening && (
          <>
            <div className="recording-overlay" />
            <div className="voice-status-hint">
              <div className="waveform">
                <div className="waveform-bar" />
                <div className="waveform-bar" />
                <div className="waveform-bar" />
                <div className="waveform-bar" />
                <div className="waveform-bar" />
                <div className="waveform-bar" />
                <div className="waveform-bar" />
                <div className="waveform-bar" />
              </div>
              <span className="animate-pulse">{t('chat.voice.listening', { defaultValue: 'Listening... (Type to cancel)' })}</span>
            </div>
          </>
        )}
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

          {/* Slash command pills + Mention pills */}
          {(slashCommands.length > 0 || mentionedFiles.length > 0) && (
            <div className="mention-pills">
              {slashCommands.map((cmd) => (
                <div
                  key={`slash-${cmd.name}`}
                  className="mention-pill mention-pill-slash"
                  title={cmd.description}
                >
                  {getSlashCommandIcon(cmd, "h-3 w-3 flex-shrink-0 mention-pill-icon")}
                  <span>/{cmd.name}</span>
                  <button
                    className="mention-pill-remove"
                    onClick={(e) => { e.stopPropagation(); slash.removeCommand(cmd.name); }}
                    title="Remove"
                  >
                    <X className="h-2.5 w-2.5" />
                  </button>
                </div>
              ))}
              {mentionedFiles.map((f, idx) => {
                const isSnippet = !!f.snippet;
                const isPaste = f.source === 'paste';
                const name = isSnippet ? f.label : f.path.split('/').pop();
                const pillKey = (f.path || '') + '::' + (f.label || '') + '::' + idx;
                // Snippet tooltips preview, not mirror — a condensed paste can
                // be tens of KB and title= renders it all.
                const tooltip = isSnippet
                  ? (f.snippet!.length > 500 ? f.snippet!.slice(0, 500) + '…' : f.snippet)
                  : f.path;
                return (
                  <div
                    key={pillKey}
                    className={`mention-pill ${isSnippet ? 'mention-pill-snippet' : ''}`}
                    title={tooltip}
                  >
                    {isPaste
                      ? <ClipboardList className="h-3 w-3 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                      : isSnippet
                        ? <TextSelect className="h-3 w-3 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                        : <FileText className="h-3 w-3 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                    }
                    <span>{name}</span>
                    <button
                      className="mention-pill-remove"
                      onClick={(e) => { e.stopPropagation(); mentions.removeMention(f.path, f.label, f.snippet); }}
                      title="Remove"
                    >
                      <X className="h-2.5 w-2.5" />
                    </button>
                  </div>
                );
              })}
            </div>
          )}

          {/* Chart image + File Preview Cards */}
          {(chartImage || attachedFiles.length > 0) && (
            <div className="flex gap-3 overflow-x-auto pb-2 px-1">
              {chartImage && (
                <div className="relative group flex-shrink-0 w-24 h-24 rounded-xl overflow-hidden border border-[var(--color-border-muted)] bg-[var(--color-bg-elevated)] animate-fade-in transition-all hover:border-[var(--color-border-default)]">
                  <img src={chartImage} alt="Chart" className="w-full h-full object-cover" />
                  <button
                    onClick={(e) => { e.stopPropagation(); onRemoveChartImage?.(); }}
                    className={`absolute top-1 right-1 p-1 bg-black/50 hover:bg-black/70 rounded-full text-white transition-opacity ${isMobile ? 'opacity-60' : 'opacity-0 group-hover:opacity-100'}`}
                  >
                    <X className="w-3 h-3" />
                  </button>
                </div>
              )}
              {attachedFiles.map((file) => (
                <FilePreviewCard
                  key={file.id}
                  file={file}
                  onRemove={removeFile}
                />
              ))}
            </div>
          )}

          {/* Autocomplete dropdowns (above textarea) */}
          {mentionsOpen && (
            <MentionAutocompleteList
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
              listRef={slash.listRef}
              commands={slash.filtered}
              activeIndex={slash.activeIndex}
              onSelect={slash.selectCommand}
              onHover={slash.setActiveIndex}
            />
          )}

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
              placeholder={isListening ? "" : placeholder}
              className={`font-content w-full bg-transparent border-0 text-[var(--color-text-primary)] ${isMobile ? 'text-base' : 'text-sm'} placeholder:text-[var(--color-text-tertiary)] resize-none overflow-y-auto leading-relaxed block transition-opacity duration-300 ${isListening ? 'opacity-20' : 'opacity-100'}`}
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
                className="inline-flex flex-none items-center justify-center h-8 w-8 rounded-lg transition-colors text-[var(--color-icon-muted)] hover:text-[var(--color-text-muted)] hover:bg-foreground/5 active:scale-95"
                type="button"
                aria-label="Attach file"
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
                  className="inline-flex flex-none items-center justify-center h-8 w-8 rounded-lg transition-colors text-[var(--color-icon-muted)] hover:text-[var(--color-text-muted)] hover:bg-foreground/5 active:scale-95"
                  type="button"
                  title="Attach chart screenshot"
                  aria-label="Capture chart"
                >
                  <ChartCandlestick className="w-4 h-4" />
                </button>
              )}

              {/* Foldable toggles — whatever fits, in priority order */}
              {inlineItems.map((item) => (
                <React.Fragment key={item.id}>{item.inline({})}</React.Fragment>
              ))}

              {/* ⋯ overflow — only the toggles that didn't fit inline. The
                  amber dot marks a folded-but-active toggle. */}
              {foldedItems.length > 0 && (
                <DropdownMenu modal={false}>
                  <DropdownMenuTrigger asChild>
                    <button
                      className="relative inline-flex flex-none items-center justify-center h-8 w-8 rounded-lg transition-colors text-[var(--color-icon-muted)] hover:text-[var(--color-text-muted)] hover:bg-foreground/5 data-[state=open]:bg-foreground/5 data-[state=open]:text-[var(--color-text-muted)]"
                      onClick={(e) => e.stopPropagation()}
                      type="button"
                      title={t('chat.moreTools', { defaultValue: 'More tools' })}
                      aria-label={t('chat.moreTools', { defaultValue: 'More tools' })}
                    >
                      <MoreHorizontal className="w-4 h-4" />
                      {foldedItems.some((item) => item.active) && (
                        <span
                          className="absolute top-1 right-1 h-1.5 w-1.5 rounded-full"
                          style={{ background: 'var(--color-accent-primary)' }}
                        />
                      )}
                    </button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent
                    align="start"
                    side={dropdownDirection === 'down' ? 'bottom' : 'top'}
                    className="w-52"
                    container={isMobile ? chatContainerRef.current : undefined}
                  >
                    {foldedItems.map((item, idx) => (
                      <React.Fragment key={item.id}>
                        {idx > 0 && foldedItems[idx - 1].group !== item.group && <DropdownMenuSeparator />}
                        {item.menu()}
                      </React.Fragment>
                    ))}
                  </DropdownMenuContent>
                </DropdownMenu>
              )}

              {/* Hidden measure row — the SAME components rendered geometry-only
                  (no handlers, menus, or aria state), so the fold budget can
                  never drift from what actually paints. */}
              <div
                ref={measureRowRef}
                aria-hidden
                className="invisible pointer-events-none absolute top-0 -left-[9999px] flex items-center gap-1 whitespace-nowrap"
              >
                {toolbarItems.filter((i) => i.visible).map((item) => (
                  <span key={item.id} data-measure={item.id}>{item.inline({ measureOnly: true })}</span>
                ))}
                <span data-measure="model">
                  <ModelTriggerMeasure selectedModel={selectedModel} effortLabel={effortLabelFor(t, resolvedEffort)} fastMode={resolvedFastMode} isCodexModel={isCodexModel} />
                </span>
              </div>
            </div>

            {/* Right Tools */}
            <div className="flex flex-row items-center min-w-0 gap-1">
              <ChatInputModelMenu
                selectedModel={selectedModel}
                onSelectModel={setSelectedModel}
                threadModels={threadModelsProp}
                validModelNames={validModelNames}
                moreModelsItems={moreModelsItems}
                hasStarredModels={starredModels.length > 0}
                reasoningEffort={reasoningEffort}
                inheritedEffort={inheritedEffort}
                onReasoningEffortChange={handleReasoningEffortChange}
                fastMode={resolvedFastMode}
                onFastModeChange={handleFastModeChange}
                isCodexModel={isCodexModel}
                reasoningEfforts={reasoningEfforts}
                dropdownDirection={dropdownDirection}
                containerRef={chatContainerRef}
              />
              {/* Voice Input Button (Show only if enabled in user settings) */}
              {speechSupported && !isLoading && !!otherPref?.voice_input_enabled && (
                <div className="flex items-center">
                  <button
                    onClick={(e) => { e.stopPropagation(); toggleListening(); }}
                    disabled={disabled}
                    className={`inline-flex flex-none items-center justify-center h-8 w-8 rounded-xl transition-all active:scale-95 mic-button ${isListening ? 'recording' : 'text-[var(--color-icon-muted)] hover:text-[var(--color-text-muted)] hover:bg-foreground/5'}`}
                    type="button"
                    title={isListening ? t('chat.voice.stop') : t('chat.voice.start')}
                    aria-label={isListening ? t('chat.voice.stop') : t('chat.voice.start')}
                  >
                    {isListening ? (
                      <MicOff className="w-4 h-4" />
                    ) : (
                      <Mic className="w-4 h-4" />
                    )}
                  </button>
                </div>
              )}
              {/* Send / Stop Button. Stop also shows during a compaction with no
                  streaming turn (manual /compact|/offload) so the user can
                  interrupt it; the Enter-key send path stays open for queuing. */}
              {(isLoading || isCompacting) && onStop ? (
                <button
                  className="stop-button w-8 h-8 flex-none rounded-xl flex items-center justify-center transition-colors disabled:cursor-not-allowed"
                  onClick={(e) => { e.stopPropagation(); handleStop(); }}
                  disabled={isStopping}
                  title={isStopping ? t('chat.stopping') : t('chat.stop')}
                  aria-label={isStopping ? t('chat.stopping') : t('chat.stop')}
                  type="button"
                >
                  {isStopping ? (
                    <Loader size={16} label={t('chat.stopping')} className="text-current" />
                  ) : (
                    <Square className="h-4 w-4" fill="currentColor" />
                  )}
                </button>
              ) : (
                <button
                  onClick={(e) => { e.stopPropagation(); handleSend(); }}
                  disabled={!hasContent || disabled}
                  className="inline-flex flex-none items-center justify-center h-8 w-8 rounded-xl transition-colors active:scale-95 disabled:cursor-default"
                  style={{
                    backgroundColor: !hasContent || disabled ? 'var(--color-bg-elevated)' : 'var(--color-btn-primary-bg)',
                    color: !hasContent || disabled ? 'var(--color-text-quaternary)' : 'var(--color-btn-primary-text)',
                  }}
                  type="button"
                  aria-label="Send message"
                >
                  <ArrowUp className="w-4 h-4" />
                </button>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Drag Overlay */}
      {
        isDragging && (
          <div className="absolute inset-0 bg-[var(--color-accent-soft)] border-2 border-dashed border-[hsl(var(--primary))] rounded-2xl z-50 flex flex-col items-center justify-center backdrop-blur-sm pointer-events-none">
            <Archive className="w-10 h-10 text-[hsl(var(--primary))] mb-2 animate-bounce" />
            <p className="text-[hsl(var(--primary))] font-medium">Drop files to upload</p>
          </div>
        )
      }

      {/* Hidden File Input */}
      <input
        ref={fileInputRef}
        type="file"
        multiple
        accept={mode === 'fast' ? "image/png,image/jpeg,image/gif,image/webp,application/pdf" : undefined}
        className="hidden"
        onChange={(e) => {
          if (e.target.files) handleFiles(e.target.files);
          e.target.value = '';
        }}
      />
    </div>
  );
});

// Memoized: ChatView re-renders on every streamed chunk, and without the memo
// the whole composer subtree (model menu, attachment deck, autocomplete) rides
// along. Effective only while ChatView keeps the callback props identity-stable
// (latest-ref pattern there) — keystrokes stay internal state either way.
export default React.memo(ChatInput);
