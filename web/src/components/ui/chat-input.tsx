import React, { useState, useRef, useEffect, useCallback, useMemo, forwardRef, useImperativeHandle } from 'react';
import {
  Plus, ArrowUp, X, FileText, Loader2, Archive, Square,
  ScrollText, ChartCandlestick, Zap, FileStack, ChevronDown, ChevronRight, FolderOpen, TextSelect,
  Terminal, Bot, Shrink, HardDriveDownload, Check, Brain, Flame, Rocket, CircleHelp,
  Mic, MicOff, Sparkles,
} from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router-dom';
import { TokenUsageRing, type TokenUsageData } from './token-usage-ring';
import {
  DropdownMenu, DropdownMenuTrigger, DropdownMenuContent, DropdownMenuItem,
  DropdownMenuSeparator, DropdownMenuSub, DropdownMenuSubTrigger, DropdownMenuSubContent,
} from './dropdown-menu';
import { usePreferences } from '@/hooks/usePreferences';
import { useIsMobile } from '@/hooks/useIsMobile';
import { supportsXhighEffort } from '@/lib/modelCapabilities';
import { getSkills, getModelMetadata } from '../../pages/ChatAgent/utils/api';
import { useToast } from './use-toast';
import './chat-input.css';

/* --- TYPES --- */

interface FileAttachment {
  id: string;
  file: File;
  type: string;
  preview: string | null;
  uploadStatus: 'pending' | 'uploading' | 'complete';
  dataUrl: string | null;
}

interface MentionedFile {
  path: string;
  snippet?: string;
  label?: string;
  lineStart?: number;
  lineEnd?: number;
  lineCount?: number;
  source?: string;
}

interface SlashCommand {
  type: string;
  name: string;
  skillName?: string;
  description?: string;
  aliases?: string[];
}

interface ModelOptions {
  model: string | null;
  reasoningEffort: string | null;
  fastMode: boolean;
}

interface ReadyAttachment {
  file: File;
  dataUrl: string | null;
  type: string;
  preview: string | null;
}

export interface ChatInputHandle {
  getModelOptions: () => ModelOptions;
  addContext: (ctx: { path?: string; snippet?: string; label?: string; lineStart?: number; lineEnd?: number; lineCount?: number; source?: string }) => void;
  setValue: (text: string) => void;
}

interface Workspace {
  workspace_id: string;
  name: string;
  [key: string]: unknown;
}

export interface ChatInputProps {
  onSend: (message: string, planMode: boolean, attachments: ReadyAttachment[], slashCommands: SlashCommand[], modelOptions: ModelOptions) => void;
  disabled?: boolean;
  isLoading?: boolean;
  onStop?: () => void;
  placeholder?: string;
  files?: string[];
  mode?: 'fast' | 'deep';
  onModeChange?: (mode: 'fast' | 'deep') => void;
  workspaces?: Workspace[] | null;
  selectedWorkspaceId?: string | null;
  onWorkspaceChange?: ((wsId: string) => void) | null;
  onCaptureChart?: (() => void) | null;
  chartImage?: string | null;
  onRemoveChartImage?: (() => void) | null;
  prefillMessage?: string;
  onClearPrefill?: (() => void) | null;
  tokenUsage?: TokenUsageData | null;
  onAction?: ((cmd: SlashCommand) => void) | null;
  initialModel?: string | null;
  threadModels?: string[];
  dropdownDirection?: 'up' | 'down';
}

/* --- UTILS --- */

/** Return the appropriate icon for a slash command. */
function getSlashCommandIcon(cmd: SlashCommand, className: string) {
  if (cmd.type === 'subagent') return <Bot className={className} />;
  if (cmd.name === 'offload') return <HardDriveDownload className={className} />;
  if (cmd.type === 'action') return <Shrink className={className} />;
  return <Terminal className={className} />;
}

const formatFileSize = (bytes: number): string => {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
};

/* --- FILE PREVIEW CARD --- */
const FilePreviewCard = ({ file, onRemove }: { file: FileAttachment; onRemove: (id: string) => void }) => {
  const isMobilePreview = useIsMobile();
  const isImage = file.type.startsWith('image/') && file.preview;

  return (
    <div className="relative group flex-shrink-0 w-24 h-24 rounded-xl overflow-hidden border border-[var(--color-border-muted)] bg-[var(--color-bg-elevated)] animate-fade-in transition-all hover:border-[var(--color-border-default)]">
      {isImage ? (
        <div className="w-full h-full relative">
          <img src={file.preview!} alt={file.file.name} className="w-full h-full object-cover" />
          <div className="absolute inset-0 bg-black/20 group-hover:bg-black/0 transition-colors" />
        </div>
      ) : (
        <div className="w-full h-full p-3 flex flex-col justify-between">
          <div className="flex items-center gap-2">
            <div className="p-1.5 rounded" style={{ background: 'var(--color-border-muted)' }}>
              <FileText className="w-4 h-4" style={{ color: 'var(--color-text-tertiary)' }} />
            </div>
            <span className="text-[10px] font-medium uppercase tracking-wider truncate" style={{ color: 'var(--color-text-tertiary)' }}>
              {file.file.name.split('.').pop()}
            </span>
          </div>
          <div className="space-y-0.5">
            <p className="text-xs font-medium truncate" style={{ color: 'var(--color-text-muted)' }} title={file.file.name}>
              {file.file.name}
            </p>
            <p className="text-[10px]" style={{ color: 'var(--color-text-tertiary)' }}>
              {formatFileSize(file.file.size)}
            </p>
          </div>
        </div>
      )}

      {/* Remove Button Overlay */}
      <button
        onClick={() => onRemove(file.id)}
        className={`absolute top-1 right-1 p-1 bg-black/50 hover:bg-black/70 rounded-full text-white transition-opacity ${isMobilePreview ? 'opacity-60' : 'opacity-0 group-hover:opacity-100'}`}
      >
        <X className="w-3 h-3" />
      </button>

      {/* Upload Status */}
      {file.uploadStatus === 'uploading' && (
        <div className="absolute inset-0 bg-black/40 flex items-center justify-center">
          <Loader2 className="w-5 h-5 text-white animate-spin" />
        </div>
      )}
    </div>
  );
};

/* --- CONSTANTS --- */
const MAX_FILE_SIZE = 10 * 1024 * 1024; // 10MB
const MAX_FILES = 5;
const BUILTIN_SLASH_COMMANDS = [
  { type: 'subagent', name: 'subagent' },
  { type: 'action', name: 'compact', aliases: ['compaction', 'summarize'] },
  { type: 'action', name: 'offload', aliases: ['truncate'] },
];

/** Derive a short display name from a model key string. */
function getModelDisplayName(key: string | null): string {
  if (!key) return '';
  let name = key;
  // Strip common provider prefixes
  for (const prefix of ['claude-', 'gpt-', 'chatgpt-', 'o1-', 'o3-', 'o4-']) {
    if (name.startsWith(prefix)) { name = name.slice(prefix.length); break; }
  }
  // Convert version-like patterns: "opus-4-6" → "Opus 4.6", "sonnet-4-6" → "Sonnet 4.6"
  name = name
    .replace(/-(\d+)-(\d+)/, ' $1.$2')
    .replace(/-(\d+\.\d+)/, ' $1')
    .replace(/-/g, ' ')
    .replace(/\b\w/g, (c: string) => c.toUpperCase());
  return name;
}

/**
 * Check if two models are compatible for mid-session switching.
 * - Different SDKs → incompatible
 * - openai/codex SDK → must be same provider (sub-provider)
 * - Other SDKs (anthropic, gemini, etc.) → compatible if same SDK
 */
function areModelsCompatible(modelA: string | null, modelB: string | null, metadata: Record<string, { sdk?: string; provider?: string }>): boolean {
  if (!modelA || !modelB) return true;
  const a = metadata[modelA], b = metadata[modelB];
  if (!a || !b) return true; // unknown models → allow
  if (a.sdk !== b.sdk) return false;
  if (a.sdk === 'openai' || a.sdk === 'codex') {
    return a.provider === b.provider;
  }
  return true;
}

/* --- MAIN COMPONENT --- */

/**
 * ChatInput — unified chat input component used across the entire app.
 *
 * @param {Function}  onSend              - (message, planMode, attachments, slashCommands) => void
 * @param {boolean}   disabled
 * @param {boolean}   isLoading
 * @param {Function}  onStop
 * @param {string}    placeholder
 * @param {string[]}  files               - workspace file paths for @mention autocomplete
 * @param {string}    mode                - 'fast' | 'deep' — undefined = no toggle shown
 * @param {Function}  onModeChange        - (newMode) => void
 * @param {Array}     workspaces          - [{ workspace_id, name }] — null = hidden
 * @param {string}    selectedWorkspaceId
 * @param {Function}  onWorkspaceChange   - (wsId) => void
 * @param {Function}  onCaptureChart      - triggers chart screenshot capture (trading only)
 * @param {string}    chartImage          - base64 data URL of captured chart
 * @param {Function}  onRemoveChartImage
 * @param {string}    prefillMessage
 * @param {Function}  onClearPrefill
 */
const speechSupported = typeof window !== 'undefined' && !!(window.SpeechRecognition || window.webkitSpeechRecognition);

const ChatInput = forwardRef<ChatInputHandle, ChatInputProps>(function ChatInput({
  onSend,
  disabled = false,
  isLoading = false,
  onStop,
  placeholder = 'Type / for skills, @ for files',
  files: workspaceFiles = [],
  // Mode toggle
  mode,
  onModeChange,
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
  // Token usage (context window progress)
  tokenUsage = null,
  // Action commands (e.g. /compact) — fired immediately on selection
  onAction = null,
  // Model selector
  initialModel = null,
  // All models used in this thread (shown in primary menu)
  threadModels: threadModelsProp = [],
  // Dropdown direction: 'up' (default, for bottom-positioned inputs) or 'down' (for mid-page inputs like ThreadGallery)
  dropdownDirection = 'up',
}, ref) {
  const { t, i18n } = useTranslation();
  const { toast } = useToast();
  const isMobile = useIsMobile();
  const { preferences } = usePreferences();
  const otherPref = (preferences as Record<string, Record<string, unknown>> | null)?.other_preference;
  const starredModels = (otherPref?.starred_models as string[] | undefined) || [];
  const preferredModel = (otherPref?.preferred_model as string | undefined) || null;
  const preferredFlashModel = (otherPref?.preferred_flash_model as string | undefined) || null;
  const [message, setMessage] = useState('');
  const [attachedFiles, setAttachedFiles] = useState<FileAttachment[]>([]);
  const [isDragging, setIsDragging] = useState(false);
  const [planMode, setPlanMode] = useState(false);

  // Model selector state — use flash model preference when in flash mode
  const modePreferredModel = mode === 'fast' ? (preferredFlashModel || preferredModel) : preferredModel;
  const effectiveInitialModel = initialModel || modePreferredModel;
  const [selectedModel, setSelectedModel] = useState<string | null>(effectiveInitialModel);
  const [reasoningEffort, setReasoningEffort] = useState<string | null>(null);
  const [fastMode, setFastMode] = useState(false);
  const [showMoreModels, setShowMoreModels] = useState(false);
  const [modelMenuOpen, setModelMenuOpen] = useState(false);

  const [modelMetadata, setModelMetadata] = useState<Record<string, { sdk?: string; provider?: string }>>({});
  const navigate = useNavigate();

  // Sync selectedModel when initialModel or preferredModel changes
  useEffect(() => {
    if (effectiveInitialModel) setSelectedModel(effectiveInitialModel);
  }, [effectiveInitialModel]);

  // Fetch model metadata for compatibility checking (eager prefetch, resolves instantly after first load)
  useEffect(() => { getModelMetadata().then((d: Record<string, unknown>) => setModelMetadata(d as typeof modelMetadata)).catch(() => { }); }, []);

  const isCodexModel = selectedModel ? modelMetadata[selectedModel]?.sdk === 'codex' : false;
  const supportsXhigh = supportsXhighEffort(selectedModel);

  // @file mention state
  const [mentionedFiles, setMentionedFiles] = useState<MentionedFile[]>([]);
  const [showAutocomplete, setShowAutocomplete] = useState(false);
  const [autocompleteQuery, setAutocompleteQuery] = useState('');
  const [activeIndex, setActiveIndex] = useState(0);
  const [mentionStart, setMentionStart] = useState(-1);

  // /slash command state
  const [slashCommands, setSlashCommands] = useState<SlashCommand[]>([]);
  const [showSlashMenu, setShowSlashMenu] = useState(false);
  const [slashQuery, setSlashQuery] = useState('');
  const [slashActiveIndex, setSlashActiveIndex] = useState(0);
  const [slashStart, setSlashStart] = useState(-1);
  const [skills, setSkills] = useState<Array<{ command?: string; name: string; description?: string }>>([]);

  // Fetch skills filtered by agent mode (re-fetches when mode changes; cached per mode in api.js)
  const skillsMode = mode === 'fast' ? 'flash' : 'ptc';
  useEffect(() => {
    getSkills(skillsMode).then((s: unknown[]) => setSkills(s as typeof skills)).catch(() => { });
  }, [skillsMode]);

  // Voice Input (Speech Recognition)
  const [isListening, setIsListening] = useState(false);
  const recognitionRef = useRef<SpeechRecognition | null>(null);
  const isStartingRef = useRef(false);
  const finalTranscriptRef = useRef('');
  const baseMessageRef = useRef('');
  const messageRef = useRef(message);

  // Sync message ref
  useEffect(() => { messageRef.current = message; }, [message]);

  // Stop recognition when loading starts (ghost text fix)
  useEffect(() => {
    if (isLoading && recognitionRef.current) {
      recognitionRef.current.stop();
      recognitionRef.current = null;
      setIsListening(false);
    }
  }, [isLoading]);

  const toggleListening = useCallback(() => {
    if (isStartingRef.current) return;

    // ALWAYS stop existing instance if it exists (prevents orphaned instances on rapid clicks)
    if (recognitionRef.current) {
      recognitionRef.current.stop();
      recognitionRef.current = null;
    }

    if (isListening) {
      setIsListening(false);
      return;
    }

    const SpeechRecognitionAPI = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!SpeechRecognitionAPI) {
      console.warn("Speech recognition is not supported in this browser.");
      return;
    }

    try {
      const recognition = new SpeechRecognitionAPI();
      recognition.continuous = true;
      recognition.interimResults = true;
      // Derived purely from current UI locale
      recognition.lang = i18n.language.startsWith('zh') ? 'zh-CN' : 'en-US';

      // Capture message BEFORE starting recognition
      const startMessage = messageRef.current.trim();
      baseMessageRef.current = startMessage ? startMessage + ' ' : '';

      recognition.onstart = () => {
        setIsListening(true);
        isStartingRef.current = false;
        finalTranscriptRef.current = ''; // Reset for new session
      };

      recognition.onresult = (event: SpeechRecognitionEvent) => {
        if (recognitionRef.current !== recognition) return;
        let interimTranscript = '';
        // Start from resultIndex to avoid re-processing or duplicating old results
        for (let i = event.resultIndex; i < event.results.length; ++i) {
          const result = event.results[i];
          if (result.isFinal) {
            finalTranscriptRef.current += result[0].transcript;
          } else {
            interimTranscript += result[0].transcript;
          }
        }
        setMessage(baseMessageRef.current + finalTranscriptRef.current + interimTranscript);
      };

      recognition.onerror = (event: SpeechRecognitionErrorEvent) => {
        if (recognitionRef.current !== recognition) return;
        if (event.error !== 'no-speech' && event.error !== 'aborted') {
          console.error('Speech recognition error:', event.error);
          if (event.error === 'not-allowed') {
            toast({
              title: t('chat.voice.permissionDenied'),
              variant: 'destructive',
            });
          } else if (event.error === 'service-not-allowed' || event.error === 'network') {
            toast({
              title: t('chat.voice.serviceError'),
              variant: 'destructive',
            });
          }
        }
        setIsListening(false);
        isStartingRef.current = false;
        recognitionRef.current = null;
      };

      recognition.onend = () => {
        if (recognitionRef.current !== recognition) return;
        isStartingRef.current = false; // Release lock in case onstart never fired
        setIsListening(false);
        recognitionRef.current = null;
      };

      recognitionRef.current = recognition;
      isStartingRef.current = true;
      recognition.start();
    } catch (err) {
      console.error('Failed to start speech recognition:', err);
      isStartingRef.current = false;
      recognitionRef.current = null; // Prevent stale ref if start() throws
      setIsListening(false);
    }
  }, [isListening, toast, t, i18n.language]);

  // Clean up recognition on unmount
  useEffect(() => {
    return () => {
      if (recognitionRef.current) {
        recognitionRef.current.abort(); // Terminate immediately without firing callbacks
        recognitionRef.current = null;
      }
    };
  }, []);

  // Stop button state
  const [isStopping, setIsStopping] = useState(false);

  // Expose addContext method for external callers (e.g. FilePanel, message selection)
  useImperativeHandle(ref, () => ({
    getModelOptions() {
      return { model: selectedModel, reasoningEffort, fastMode };
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
    setValue(text) {
      setMessage(text);
      setTimeout(() => textareaRef.current?.focus(), 0);
    },
  }), [selectedModel, reasoningEffort, fastMode]);

  // Workspace dropdown
  const [showWorkspaceMenu, setShowWorkspaceMenu] = useState(false);
  const workspaceMenuRef = useRef<HTMLDivElement>(null);
  const workspaceBtnRef = useRef<HTMLButtonElement>(null);

  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const autocompleteRef = useRef<HTMLDivElement>(null);
  const slashMenuRef = useRef<HTMLDivElement>(null);
  const chatContainerRef = useRef<HTMLDivElement>(null);

  const hasModeToggle = mode !== undefined && onModeChange !== undefined;

  // Prefill support
  useEffect(() => {
    if (prefillMessage) {
      setMessage(prefillMessage);
      onClearPrefill?.();
    }
  }, [prefillMessage, onClearPrefill]);

  // Load per-model reasoning effort from localStorage
  useEffect(() => {
    if (!selectedModel) { setReasoningEffort(null); return; }
    const saved = localStorage.getItem(`reasoning_effort:${selectedModel}`);
    setReasoningEffort(saved || null);
  }, [selectedModel]);

  // Persist reasoning effort per model
  useEffect(() => {
    if (!selectedModel) return;
    if (reasoningEffort) localStorage.setItem(`reasoning_effort:${selectedModel}`, reasoningEffort);
    else localStorage.removeItem(`reasoning_effort:${selectedModel}`);
  }, [reasoningEffort, selectedModel]);

  // Load per-model fast mode from localStorage
  useEffect(() => {
    if (!selectedModel) { setFastMode(false); return; }
    const saved = localStorage.getItem(`fast_mode:${selectedModel}`);
    setFastMode(saved === 'true');
  }, [selectedModel]);

  // Persist fast mode per model
  useEffect(() => {
    if (!selectedModel) return;
    if (fastMode) localStorage.setItem(`fast_mode:${selectedModel}`, 'true');
    else localStorage.removeItem(`fast_mode:${selectedModel}`);
  }, [fastMode, selectedModel]);

  // Reset isStopping when loading finishes
  useEffect(() => {
    if (!isLoading) setIsStopping(false);
  }, [isLoading]);

  const handleStop = useCallback(() => {
    if (isStopping) return;
    setIsStopping(true);
    onStop?.();
  }, [isStopping, onStop]);

  // Auto-resize textarea
  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
      textareaRef.current.style.height = Math.min(textareaRef.current.scrollHeight, 200) + 'px';
    }
  }, [message]);

  // Close workspace menu on click outside
  useEffect(() => {
    if (!showWorkspaceMenu) return;
    const handleClickOutside = (e: MouseEvent) => {
      if (workspaceBtnRef.current?.contains(e.target as Node)) return;
      if (workspaceMenuRef.current && !workspaceMenuRef.current.contains(e.target as Node)) {
        setShowWorkspaceMenu(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [showWorkspaceMenu]);

  // --- File Upload Handling ---
  const handleFiles = useCallback((newFilesList: FileList | File[]) => {
    const currentCount = attachedFiles.length;
    const fileArray = Array.from(newFilesList);
    const isFlashMode = mode === 'fast';

    const validFiles = [];
    for (const file of fileArray) {
      if (currentCount + validFiles.length >= MAX_FILES) break;
      if (isFlashMode) {
        // Flash: only images and PDFs
        const isImage = file.type.startsWith('image/') || /\.(jpg|jpeg|png|gif|webp)$/i.test(file.name);
        const isPdf = file.type === 'application/pdf' || /\.pdf$/i.test(file.name);
        if (!isImage && !isPdf) continue;
      }
      // PTC/deep: accept any file
      if (file.size > MAX_FILE_SIZE) continue;
      validFiles.push(file);
    }

    const newFiles: FileAttachment[] = validFiles.map((file) => {
      const isImage = file.type.startsWith('image/') || /\.(jpg|jpeg|png|gif|webp)$/i.test(file.name);
      return {
        id: Math.random().toString(36).substr(2, 9),
        file,
        type: file.type || (isImage ? 'image/png' : 'application/octet-stream'),
        preview: isImage ? URL.createObjectURL(file) : null,
        uploadStatus: 'pending' as const,
        dataUrl: null,
      };
    });

    if (newFiles.length === 0) return;

    setAttachedFiles((prev) => [...prev, ...newFiles]);

    newFiles.forEach((f) => {
      const reader = new FileReader();
      reader.onload = () => {
        setAttachedFiles((prev) =>
          prev.map((p) =>
            p.id === f.id ? { ...p, uploadStatus: 'complete' as const, dataUrl: reader.result as string } : p
          )
        );
      };
      reader.onerror = () => {
        setAttachedFiles((prev) => prev.filter((p) => p.id !== f.id));
      };
      reader.readAsDataURL(f.file);
    });
  }, [attachedFiles.length, mode]);

  const removeFile = useCallback((id: string) => {
    setAttachedFiles((prev) => {
      const file = prev.find((f) => f.id === id);
      if (file?.preview) URL.revokeObjectURL(file.preview);
      return prev.filter((f) => f.id !== id);
    });
  }, []);

  // Drag & Drop
  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  }, []);
  const onDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
  }, []);
  const onDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
    if (e.dataTransfer.files) handleFiles(e.dataTransfer.files);
  }, [handleFiles]);

  // Paste Handling
  const handlePaste = useCallback((e: React.ClipboardEvent) => {
    const items = e.clipboardData.items;
    const pastedFiles = [];
    for (let i = 0; i < items.length; i++) {
      if (items[i].kind === 'file') {
        const file = items[i].getAsFile();
        if (file) pastedFiles.push(file);
      }
    }
    if (pastedFiles.length > 0) {
      e.preventDefault();
      handleFiles(pastedFiles);
    }
  }, [handleFiles]);

  // --- @File Mention Autocomplete ---
  const filteredMentionFiles = useMemo(() => {
    if (!showAutocomplete) return [];
    const query = autocompleteQuery.toLowerCase();
    const dirPriority: Record<string, number> = { '': 0, results: 1, data: 2 };
    return workspaceFiles
      .filter((f) => f.toLowerCase().includes(query))
      .sort((a, b) => {
        const da = a.includes('/') ? a.slice(0, a.indexOf('/')) : '';
        const db = b.includes('/') ? b.slice(0, b.indexOf('/')) : '';
        const pa = dirPriority[da] ?? 3;
        const pb = dirPriority[db] ?? 3;
        if (pa !== pb) return pa - pb;
        return a.localeCompare(b);
      })
      .slice(0, 10);
  }, [workspaceFiles, autocompleteQuery, showAutocomplete]);

  useEffect(() => {
    setActiveIndex(0);
  }, [filteredMentionFiles.length]);

  // --- /Slash Command Autocomplete ---
  const filteredSlashCommands = useMemo(() => {
    if (!showSlashMenu) return [];
    const query = slashQuery.toLowerCase();
    const items: SlashCommand[] = [
      ...skills.filter((s) => s.command).map((s) => ({ type: 'skill', name: s.command!, skillName: s.name, description: t(`chat.slashCommand.${s.command}Desc`, { defaultValue: s.description }) })),
      ...BUILTIN_SLASH_COMMANDS.map((c) => ({ ...c, description: t(`chat.slashCommand.${c.name}Desc`) })),
    ];
    return items
      .filter((item) => !slashCommands.some((c) => c.name === item.name))
      .filter((item) => {
        if (!query) return true;
        if (item.name.toLowerCase().includes(query)) return true;
        if (item.description?.toLowerCase().includes(query)) return true;
        if (item.aliases?.some((a: string) => a.toLowerCase().includes(query))) return true;
        return false;
      })
      .slice(0, 10);
  }, [skills, showSlashMenu, slashQuery, slashCommands, t]);

  useEffect(() => {
    setSlashActiveIndex(0);
  }, [filteredSlashCommands.length]);

  // Detect @ and / triggers on input change
  const handleChange = useCallback((e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const val = e.target.value;
    setMessage(val);

    // Remove pills whose /{command} or @mention text was deleted from the textarea
    setSlashCommands((prev) => prev.filter((cmd) => val.includes(`/${cmd.name}`)));
    setMentionedFiles((prev) => prev.filter((f) => f.snippet || val.includes(`@${f.path}`)));

    const cursorPos = e.target.selectionStart;

    // Detect / trigger for slash commands
    let slashIdx = -1;
    for (let i = cursorPos - 1; i >= 0; i--) {
      const ch = val[i];
      if (ch === '/') {
        if (i === 0 || /\s/.test(val[i - 1])) {
          slashIdx = i;
        }
        break;
      }
      if (/\s/.test(ch)) break;
    }

    if (slashIdx >= 0) {
      const partial = val.slice(slashIdx + 1, cursorPos);
      setSlashStart(slashIdx);
      setSlashQuery(partial);
      setShowSlashMenu(true);
      // Close @mention when /slash is active
      setShowAutocomplete(false);
      setMentionStart(-1);
      setAutocompleteQuery('');
      return;
    } else {
      setShowSlashMenu(false);
      setSlashStart(-1);
      setSlashQuery('');
    }

    // Detect @ trigger for file mentions
    let atIdx = -1;
    for (let i = cursorPos - 1; i >= 0; i--) {
      const ch = val[i];
      if (ch === '@') {
        if (i === 0 || /\s/.test(val[i - 1])) {
          atIdx = i;
        }
        break;
      }
      if (/\s/.test(ch)) break;
    }

    if (atIdx >= 0) {
      const partial = val.slice(atIdx + 1, cursorPos);
      setMentionStart(atIdx);
      setAutocompleteQuery(partial);
      setShowAutocomplete(true);
    } else {
      setShowAutocomplete(false);
      setMentionStart(-1);
      setAutocompleteQuery('');
    }
  }, []);

  const selectFile = useCallback((filePath: string) => {
    if (mentionStart < 0) return;
    const cursorPos = textareaRef.current?.selectionStart ?? message.length;
    const before = message.slice(0, mentionStart);
    const after = message.slice(cursorPos);
    const newMessage = before + '@' + filePath + ' ' + after;
    setMessage(newMessage);

    setMentionedFiles((prev) => {
      if (prev.some((f) => f.path === filePath)) return prev;
      return [...prev, { path: filePath }];
    });

    setShowAutocomplete(false);
    setMentionStart(-1);
    setAutocompleteQuery('');

    setTimeout(() => {
      if (textareaRef.current) {
        const newCursorPos = before.length + 1 + filePath.length + 1;
        textareaRef.current.focus();
        textareaRef.current.setSelectionRange(newCursorPos, newCursorPos);
      }
    }, 0);
  }, [mentionStart, message]);

  const removeMention = useCallback((path: string, label?: string, snippet?: string) => {
    setMentionedFiles((prev) => prev.filter((f) => {
      if (snippet) return !(f.snippet === snippet && f.path === path);
      if (label) return !(f.path === path && f.label === label);
      return !(f.path === path && !f.label);
    }));
    // Also remove @path text from textarea
    setMessage((prev) => prev.replace(new RegExp(`(^|\\s)@${path.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(\\s|$)`), '$1$2').trim());
  }, []);

  const selectSlashCommand = useCallback((cmd: SlashCommand) => {
    if (slashStart < 0) return;
    const cursorPos = textareaRef.current?.selectionStart ?? message.length;
    const before = message.slice(0, slashStart);
    const after = message.slice(cursorPos);

    // Action commands fire immediately — no pill, no send required
    if (cmd.type === 'action') {
      setMessage(before + after);
      setShowSlashMenu(false);
      setSlashStart(-1);
      setSlashQuery('');
      onAction?.(cmd);
      setTimeout(() => {
        if (textareaRef.current) {
          textareaRef.current.focus();
          textareaRef.current.setSelectionRange(before.length, before.length);
        }
      }, 0);
    } else {
      // Retain /{command} in textarea and add pill
      const inserted = `/${cmd.name} `;
      const newMsg = before + inserted + after;
      setMessage(newMsg);
      setShowSlashMenu(false);
      setSlashStart(-1);
      setSlashQuery('');
      setSlashCommands((prev) => {
        if (prev.some((c) => c.name === cmd.name)) return prev;
        return [...prev, cmd];
      });
      const newCursor = before.length + inserted.length;
      setTimeout(() => {
        if (textareaRef.current) {
          textareaRef.current.focus();
          textareaRef.current.setSelectionRange(newCursor, newCursor);
        }
      }, 0);
    }
  }, [slashStart, message, onAction]);

  const removeSlashCommand = useCallback((name: string) => {
    setSlashCommands((prev) => prev.filter((c) => c.name !== name));
    // Also remove /{command} text from textarea
    setMessage((prev) => prev.replace(new RegExp(`(^|\\s)/${name}(\\s|$)`), '$1$2').trim());
  }, []);

  // Scroll active autocomplete item into view
  useEffect(() => {
    if (!showAutocomplete || !autocompleteRef.current) return;
    const items = autocompleteRef.current.querySelectorAll('.mention-autocomplete-item');
    if (items[activeIndex]) {
      items[activeIndex].scrollIntoView({ block: 'nearest' });
    }
  }, [activeIndex, showAutocomplete]);

  // Scroll active slash menu item into view
  useEffect(() => {
    if (!showSlashMenu || !slashMenuRef.current) return;
    const items = slashMenuRef.current.querySelectorAll('.mention-autocomplete-item');
    if (items[slashActiveIndex]) {
      items[slashActiveIndex].scrollIntoView({ block: 'nearest' });
    }
  }, [slashActiveIndex, showSlashMenu]);

  // Close autocomplete on blur
  const handleBlur = useCallback(() => {
    setTimeout(() => {
      setShowAutocomplete(false);
      setShowSlashMenu(false);
    }, 200);
  }, []);

  // --- Send ---
  const hasContent = message.trim() || attachedFiles.length > 0 || !!chartImage || mentionedFiles.some((f) => f.snippet);

  const handleSend = useCallback(() => {
    if (!hasContent || disabled) return;
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
        const lineInfo = f.lineStart != null
          ? ` (lines ${f.lineStart}-${f.lineEnd}, ${f.lineCount} line${f.lineCount !== 1 ? 's' : ''})`
          : '';
        return `\n<details>\n<summary>@${f.path}${lineInfo}</summary>\n\n\`\`\`\n${f.snippet}\n\`\`\`\n</details>`;
      });
      finalMessage = finalMessage.trimEnd() + '\n' + blocks.join('\n');
    }
    onSend(finalMessage, planMode, readyAttachments, slashCommands, { model: selectedModel, reasoningEffort, fastMode });
    setMessage('');
    attachedFiles.forEach(f => { if (f.preview) URL.revokeObjectURL(f.preview); });
    setAttachedFiles([]);
    setMentionedFiles([]);
    setSlashCommands([]);
    setShowAutocomplete(false);
    setShowSlashMenu(false);
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
    }
  }, [hasContent, disabled, message, planMode, attachedFiles, onSend, mentionedFiles, slashCommands, selectedModel, reasoningEffort, fastMode, t]);

  // --- Keyboard & Language Detection ---
  const handleKeyDown = useCallback((e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    // If user starts typing while voice input is active, terminate the voice input.
    // We treat any single character key or delete/backspace as a signal to stop.
    if (isListening && recognitionRef.current && (e.key.length === 1 || e.key === 'Backspace' || e.key === 'Delete')) {
      recognitionRef.current.stop();
    }

    // Slash command menu keyboard navigation
    if (showSlashMenu && filteredSlashCommands.length > 0) {
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        setSlashActiveIndex((prev) => (prev + 1) % filteredSlashCommands.length);
        return;
      }
      if (e.key === 'ArrowUp') {
        e.preventDefault();
        setSlashActiveIndex((prev) => (prev - 1 + filteredSlashCommands.length) % filteredSlashCommands.length);
        return;
      }
      if (e.key === 'Enter' || e.key === 'Tab') {
        e.preventDefault();
        selectSlashCommand(filteredSlashCommands[slashActiveIndex]);
        return;
      }
      if (e.key === 'Escape') {
        e.preventDefault();
        setShowSlashMenu(false);
        return;
      }
    }

    // @mention autocomplete keyboard navigation
    if (showAutocomplete && filteredMentionFiles.length > 0) {
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        setActiveIndex((prev) => (prev + 1) % filteredMentionFiles.length);
        return;
      }
      if (e.key === 'ArrowUp') {
        e.preventDefault();
        setActiveIndex((prev) => (prev - 1 + filteredMentionFiles.length) % filteredMentionFiles.length);
        return;
      }
      if (e.key === 'Enter' || e.key === 'Tab') {
        e.preventDefault();
        selectFile(filteredMentionFiles[activeIndex]);
        return;
      }
      if (e.key === 'Escape') {
        e.preventDefault();
        setShowAutocomplete(false);
        return;
      }
    }

    if (e.key === 'Enter' && !e.shiftKey && !showAutocomplete) {
      e.preventDefault();
      handleSend();
    }

    if (e.key === 'Escape' && showAutocomplete) {
      setShowAutocomplete(false);
    }
  }, [showSlashMenu, filteredSlashCommands, slashActiveIndex, selectSlashCommand, showAutocomplete, filteredMentionFiles, activeIndex, selectFile, handleSend, isListening]);

  // Workspace selector helpers
  const selectedWorkspaceName = useMemo(() => {
    if (!workspaces || !selectedWorkspaceId) return 'Workspace';
    const ws = workspaces.find((w) => w.workspace_id === selectedWorkspaceId);
    return ws?.name || 'Workspace';
  }, [workspaces, selectedWorkspaceId]);

  const showWorkspaceSelector = hasModeToggle && mode === 'deep' && workspaces && workspaces.length > 0;

  /** Render starred model items — used by both desktop submenu and mobile inline expand */
  const moreModelsItems = useMemo(() => {
    return starredModels.filter((m) => !initialModel || areModelsCompatible(selectedModel, m, modelMetadata));
  }, [starredModels, initialModel, selectedModel, modelMetadata]);

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
        className={`chat-input-container flex flex-col items-stretch transition-all duration-200 relative z-10 rounded-2xl cursor-text border border-[hsl(var(--primary))] bg-[var(--color-bg-card)] ${isListening ? 'recording' : ''}`}
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
                    onClick={(e) => { e.stopPropagation(); removeSlashCommand(cmd.name); }}
                    title="Remove"
                  >
                    <X className="h-2.5 w-2.5" />
                  </button>
                </div>
              ))}
              {mentionedFiles.map((f, idx) => {
                const isSnippet = !!f.snippet;
                const name = isSnippet ? f.label : f.path.split('/').pop();
                const pillKey = (f.path || '') + '::' + (f.label || '') + '::' + idx;
                return (
                  <div
                    key={pillKey}
                    className={`mention-pill ${isSnippet ? 'mention-pill-snippet' : ''}`}
                    title={isSnippet ? f.snippet : f.path}
                  >
                    {isSnippet
                      ? <TextSelect className="h-3 w-3 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                      : <FileText className="h-3 w-3 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                    }
                    <span>{name}</span>
                    <button
                      className="mention-pill-remove"
                      onClick={(e) => { e.stopPropagation(); removeMention(f.path, f.label, f.snippet); }}
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

          {/* Autocomplete dropdown (above textarea) */}
          {showAutocomplete && (
            <div className="mention-autocomplete" ref={autocompleteRef}>
              {filteredMentionFiles.length === 0 ? (
                <div className="mention-autocomplete-empty">
                  {workspaceFiles.length === 0 ? 'No files available' : 'No matching files'}
                </div>
              ) : (
                filteredMentionFiles.map((filePath, idx) => {
                  const name = filePath.split('/').pop();
                  const dir = filePath.includes('/') ? filePath.slice(0, filePath.lastIndexOf('/')) : '';
                  return (
                    <div
                      key={filePath}
                      className={`mention-autocomplete-item ${idx === activeIndex ? 'active' : ''}`}
                      onMouseDown={(e) => {
                        e.preventDefault();
                        selectFile(filePath);
                      }}
                      onMouseEnter={() => setActiveIndex(idx)}
                    >
                      <FileText className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                      <span className="file-name">{name}</span>
                      {dir && <span className="file-path">{dir}/</span>}
                    </div>
                  );
                })
              )}
            </div>
          )}

          {/* Slash command dropdown */}
          {showSlashMenu && (
            <div className="mention-autocomplete" ref={slashMenuRef}>
              {filteredSlashCommands.length === 0 ? (
                <div className="mention-autocomplete-empty">
                  {t('chat.slashCommand.noMatching')}
                </div>
              ) : (
                filteredSlashCommands.map((cmd, idx) => (
                  <div
                    key={cmd.name}
                    className={`mention-autocomplete-item ${idx === slashActiveIndex ? 'active' : ''}`}
                    onMouseDown={(e) => {
                      e.preventDefault();
                      selectSlashCommand(cmd);
                    }}
                    onMouseEnter={() => setSlashActiveIndex(idx)}
                  >
                    {getSlashCommandIcon(cmd, "h-4 w-4 flex-shrink-0 slash-cmd-icon")}
                    <span className="slash-cmd-name">/{cmd.name}</span>
                    <span className="slash-cmd-desc">{cmd.description}</span>
                  </div>
                ))
              )}
            </div>
          )}

          {/* Textarea */}
          <div className="relative">
            <textarea
              ref={textareaRef}
              value={message}
              onChange={handleChange}
              onPaste={handlePaste}
              onKeyDown={handleKeyDown}
              onBlur={handleBlur}
              onCompositionStart={() => {
                // Terminate voice input if IME starts
                if (isListening && recognitionRef.current) {
                  recognitionRef.current.stop();
                }
              }}
              placeholder={isListening ? "" : placeholder}
              className={`w-full bg-transparent border-0 outline-none text-[var(--color-text-primary)] ${isMobile ? 'text-base' : 'text-sm'} placeholder:text-[var(--color-text-tertiary)] resize-none overflow-hidden leading-relaxed block transition-opacity duration-300 ${isListening ? 'opacity-20' : 'opacity-100'}`}
              rows={1}
              disabled={disabled}
              style={{ minHeight: '1.5em' }}
            />
          </div>

          {/* Action Bar */}
          <div className="flex gap-2 w-full items-center">
            {/* Left Tools */}
            <div className="relative flex-1 flex items-center shrink min-w-0 gap-1">
              {/* Attach Button */}
              <button
                onClick={(e) => { e.stopPropagation(); fileInputRef.current?.click(); }}
                className="inline-flex items-center justify-center h-8 w-8 rounded-lg transition-colors text-[var(--color-icon-muted)] hover:text-[var(--color-text-muted)] hover:bg-foreground/5 active:scale-95"
                type="button"
                aria-label="Attach file"
              >
                <Plus className="w-4 h-4" />
              </button>

              {/* Token Usage Ring */}
              {tokenUsage && tokenUsage.threshold > 0 && tokenUsage.total > 0 && (
                <TokenUsageRing tokenUsage={tokenUsage} />
              )}

              {/* Chart Capture (trading only) */}
              {onCaptureChart && (
                <button
                  onClick={(e) => { e.stopPropagation(); onCaptureChart(); }}
                  className="inline-flex items-center justify-center h-8 w-8 rounded-lg transition-colors text-[var(--color-icon-muted)] hover:text-[var(--color-text-muted)] hover:bg-foreground/5 active:scale-95"
                  type="button"
                  title="Attach chart screenshot"
                  aria-label="Capture chart"
                >
                  <ChartCandlestick className="w-4 h-4" />
                </button>
              )}

              {/* Mode Toggle (fast/deep) */}
              {hasModeToggle && (
                <button
                  className="inline-flex items-center rounded-full border-none cursor-pointer"
                  style={{
                    gap: '6px',
                    padding: '6px 10px',
                    fontSize: '13px',
                    fontWeight: 500,
                    background: mode === 'deep' ? 'var(--color-accent-soft)' : 'transparent',
                    color: mode === 'deep' ? 'var(--color-accent-light)' : 'var(--color-text-muted, #8b8fa3)',
                    border: mode === 'deep' ? '1px solid var(--color-accent-overlay)' : '1px solid transparent',
                    transition: 'background 0.2s, color 0.2s, border-color 0.2s',
                  }}
                  onClick={(e) => { e.stopPropagation(); onModeChange(mode === 'fast' ? 'deep' : 'fast'); }}
                  onMouseEnter={(e) => {
                    if (mode !== 'deep') e.currentTarget.style.background = 'var(--color-border-muted)';
                  }}
                  onMouseLeave={(e) => {
                    if (mode !== 'deep') e.currentTarget.style.background = 'transparent';
                  }}
                  type="button"
                  title={mode === 'fast' ? 'Flash — quick answer using flash model' : 'Deep — full agent with workspace and tools'}
                >
                  {mode === 'fast' ? <Zap className="h-4 w-4" /> : <FileStack className="h-4 w-4" />}
                  <span>{mode === 'fast' ? 'Flash' : 'Deep'}</span>
                </button>
              )}

              {/* Workspace Selector */}
              {showWorkspaceSelector && (
                <div className="relative" ref={workspaceMenuRef}>
                  <button
                    ref={workspaceBtnRef}
                    className="inline-flex items-center rounded-full border-none cursor-pointer"
                    style={{
                      gap: '4px',
                      padding: '6px 10px',
                      fontSize: '13px',
                      fontWeight: 500,
                      background: showWorkspaceMenu ? 'var(--color-accent-soft)' : 'transparent',
                      color: showWorkspaceMenu ? 'var(--color-accent-light)' : 'var(--color-text-muted, #8b8fa3)',
                      border: showWorkspaceMenu ? '1px solid var(--color-accent-overlay)' : '1px solid transparent',
                      transition: 'background 0.2s, color 0.2s, border-color 0.2s',
                    }}
                    onClick={(e) => { e.stopPropagation(); setShowWorkspaceMenu((v) => !v); }}
                    onMouseEnter={(e) => {
                      if (!showWorkspaceMenu) e.currentTarget.style.background = 'var(--color-border-muted)';
                    }}
                    onMouseLeave={(e) => {
                      if (!showWorkspaceMenu) e.currentTarget.style.background = 'transparent';
                    }}
                    type="button"
                    title="Select workspace"
                  >
                    <FolderOpen className="h-4 w-4" />
                    <span className="max-w-[100px] truncate">{selectedWorkspaceName}</span>
                    <ChevronDown className="h-3 w-3" />
                  </button>
                  {showWorkspaceMenu && (
                    <div className="workspace-dropdown workspace-dropdown-up">
                      {workspaces.map((ws) => (
                        <div
                          key={ws.workspace_id}
                          className={`workspace-dropdown-item ${ws.workspace_id === selectedWorkspaceId ? 'active' : ''}`}
                          onMouseDown={(e) => {
                            e.preventDefault();
                            onWorkspaceChange?.(ws.workspace_id);
                            setShowWorkspaceMenu(false);
                          }}
                        >
                          <FolderOpen className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                          <span>{ws.name}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}

              {/* Plan Mode Toggle — shown when no mode toggle (PTC enforced) OR mode === 'deep' */}
              {(!hasModeToggle || mode === 'deep') && (
                <button
                  className={`inline-flex items-center rounded-full border-none cursor-pointer${planMode ? ' plan-mode-toggle-active' : ''}`}
                  style={{
                    gap: '6px',
                    padding: '6px 10px',
                    fontSize: '13px',
                    fontWeight: 500,
                    background: planMode ? 'var(--color-accent-soft)' : 'transparent',
                    color: planMode ? 'var(--color-accent-light)' : 'var(--color-text-muted, #8b8fa3)',
                    border: planMode ? '1px solid var(--color-accent-overlay)' : '1px solid transparent',
                    transition: 'background 0.2s, color 0.2s, border-color 0.2s',
                  }}
                  onClick={(e) => { e.stopPropagation(); setPlanMode(!planMode); }}
                  onMouseEnter={(e) => {
                    if (!planMode) e.currentTarget.style.background = 'var(--color-border-muted)';
                  }}
                  onMouseLeave={(e) => {
                    if (!planMode) e.currentTarget.style.background = 'transparent';
                  }}
                  type="button"
                  title="Plan mode — agent creates a plan for approval before executing"
                >
                  <ScrollText className="h-4 w-4" style={planMode ? { color: 'var(--color-accent-light)' } : {}} />
                  <span>Plan</span>
                </button>
              )}
            </div>

            {/* Right Tools */}
            <div className="flex flex-row items-center min-w-0 gap-1">
              {/* Model Selector */}
              {(starredModels.length > 0 || selectedModel) && (
                <DropdownMenu modal={false} open={modelMenuOpen} onOpenChange={(open) => { setModelMenuOpen(open); if (!open) setShowMoreModels(false); }}>
                  <DropdownMenuTrigger asChild>
                    <button
                      className="model-selector-trigger inline-flex items-center gap-1 rounded-full cursor-pointer py-1.5 px-2.5 text-[13px] font-medium border border-transparent transition-colors outline-none"
                      onClick={(e) => e.stopPropagation()}
                      type="button"
                      title="Select model"
                    >
                      <span className="max-w-[120px] truncate">{getModelDisplayName(selectedModel) || 'Model'}</span>
                      {fastMode && isCodexModel && <Rocket className="h-3 w-3" style={{ color: 'var(--color-accent-light)' }} />}
                      <ChevronDown className="h-3 w-3" />
                    </button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent
                    align="end"
                    side={dropdownDirection === 'down' ? 'bottom' : 'top'}
                    className="w-60"
                    container={isMobile ? chatContainerRef.current : undefined}
                    data-click-outside-ignore
                    onClick={(e) => e.stopPropagation()}
                    style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-muted)' }}
                  >
                    {/* Thread models */}
                    {(() => {
                      const primaryModels: string[] = threadModelsProp.length > 0
                        ? [...new Set([...threadModelsProp, selectedModel].filter((m): m is string => !!m))]
                        : [selectedModel].filter((m): m is string => !!m);
                      return primaryModels
                        .filter((m: string) => !initialModel || areModelsCompatible(selectedModel, m, modelMetadata))
                        .map((m: string) => (
                          <DropdownMenuItem
                            key={m}
                            onSelect={() => setSelectedModel(m)}
                            className="text-[13px] justify-between"
                            style={{ color: 'var(--color-text-primary)' }}
                          >
                            <span>{getModelDisplayName(m)}</span>
                            {m === selectedModel && <Check className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />}
                          </DropdownMenuItem>
                        ));
                    })()}
                    <DropdownMenuSeparator style={{ backgroundColor: 'var(--color-border-muted)' }} />
                    {/* Reasoning effort (custom control — not a menu item, won't close on click) */}
                    <div className="model-effort-section">
                      <span className="model-effort-label">{t('chat.modelSelector.reasoningEffort')}</span>
                      <div className="model-effort-toggle">
                        {([
                          ['low', Zap, t('chat.modelSelector.effortLow')],
                          ['medium', Brain, t('chat.modelSelector.effortMedium')],
                          ['high', Flame, t('chat.modelSelector.effortHigh')],
                          ...(supportsXhigh ? [['xhigh', Sparkles, t('chat.modelSelector.effortXhigh')]] as const : []),
                        ] as const).map(([level, Icon, label]) => (
                          <button
                            key={level}
                            className={`model-effort-btn ${level === reasoningEffort ? 'active' : ''}`}
                            onMouseDown={(e) => {
                              e.preventDefault();
                              setReasoningEffort(level === reasoningEffort ? null : level);
                            }}
                            title={label}
                          >
                            <Icon className="h-3.5 w-3.5" />
                          </button>
                        ))}
                      </div>
                    </div>
                    {/* Fast mode (Codex only — custom control, won't close on click) */}
                    {isCodexModel && (
                      <div className="model-effort-section">
                        <span className="model-effort-label">
                          <Rocket className="h-3.5 w-3.5" style={{ marginRight: 4, verticalAlign: '-2px', display: 'inline' }} />
                          {t('chat.modelSelector.fastMode')}
                          <span className="fast-mode-help-wrap">
                            <CircleHelp className="fast-mode-help-icon" />
                            <span className="fast-mode-help-tooltip">{t('chat.modelSelector.fastModeHelpLine1')}<br />{t('chat.modelSelector.fastModeHelpLine2')}</span>
                          </span>
                        </span>
                        <button
                          className={`fast-mode-switch ${fastMode ? 'active' : ''}`}
                          onMouseDown={(e) => { e.preventDefault(); setFastMode((v) => !v); }}
                          title={t('chat.modelSelector.fastModeDesc')}
                          role="switch"
                          aria-checked={fastMode}
                        >
                          <span className="fast-mode-switch-thumb" />
                        </button>
                      </div>
                    )}
                    <DropdownMenuSeparator style={{ backgroundColor: 'var(--color-border-muted)' }} />
                    {/* More models — desktop: sub-menu flyout, mobile: inline toggle with onMouseDown (same pattern as effort buttons) */}
                    {isMobile ? (
                      <>
                        {/* Toggle trigger — onMouseDown fires after pointerup on mobile, avoiding Radix's pointer sequence race */}
                        <div
                          className="flex items-center justify-between px-3 py-2 text-xs cursor-pointer rounded-sm transition-colors hover:bg-accent/15"
                          style={{ color: 'var(--color-text-tertiary)' }}
                          role="menuitem"
                          onMouseDown={(e) => {
                            e.preventDefault();
                            setShowMoreModels((v) => !v);
                          }}
                        >
                          <span>{t('chat.modelSelector.moreModels')}</span>
                          <ChevronRight className={`h-3.5 w-3.5 transition-transform ${showMoreModels ? 'rotate-90' : ''}`} />
                        </div>
                        {/* Model items — plain divs (not DropdownMenuItems) to avoid Collection registration changes */}
                        <div className={showMoreModels ? '' : 'hidden'}>
                          {moreModelsItems.length > 0 ? (
                            moreModelsItems.map((m) => (
                              <div
                                key={m}
                                className="relative flex w-full cursor-default select-none items-center justify-between rounded-sm px-3 py-2 pl-6 text-[13px] outline-none transition-colors hover:bg-accent/15"
                                style={{ color: 'var(--color-text-primary)' }}
                                role="menuitem"
                                onMouseDown={(e) => {
                                  e.preventDefault();
                                  setSelectedModel(m);
                                  setModelMenuOpen(false);
                                }}
                              >
                                <span>{getModelDisplayName(m)}</span>
                                {m === selectedModel && <Check className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />}
                              </div>
                            ))
                          ) : (
                            <div
                              className="relative flex w-full cursor-default select-none items-center rounded-sm px-3 py-2 pl-6 text-xs outline-none transition-colors hover:bg-accent/15"
                              style={{ color: 'var(--color-text-tertiary)' }}
                              role="menuitem"
                              onMouseDown={(e) => {
                                e.preventDefault();
                                navigate('/settings?tab=model');
                                setModelMenuOpen(false);
                              }}
                            >
                              {t('chat.modelSelector.configureModels')}
                            </div>
                          )}
                        </div>
                      </>
                    ) : (
                      <DropdownMenuSub>
                        <DropdownMenuSubTrigger
                          className="text-xs justify-between"
                          style={{ color: 'var(--color-text-tertiary)' }}
                        >
                          <span className="flex-1">{t('chat.modelSelector.moreModels')}</span>
                          <ChevronRight className="h-3.5 w-3.5" />
                        </DropdownMenuSubTrigger>
                        <DropdownMenuSubContent
                          className="w-60"
                          data-click-outside-ignore
                          collisionPadding={{ top: 8, right: 8, bottom: 60, left: 8 }}
                          style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-muted)' }}
                        >
                          {moreModelsItems.length > 0 ? (
                            moreModelsItems.map((m) => (
                              <DropdownMenuItem
                                key={m}
                                onSelect={() => setSelectedModel(m)}
                                className="text-[13px] justify-between"
                                style={{ color: 'var(--color-text-primary)' }}
                              >
                                <span>{getModelDisplayName(m)}</span>
                                {m === selectedModel && <Check className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />}
                              </DropdownMenuItem>
                            ))
                          ) : (
                            <DropdownMenuItem
                              onSelect={() => navigate('/settings?tab=model')}
                              className="text-xs"
                              style={{ color: 'var(--color-text-tertiary)' }}
                            >
                              {t('chat.modelSelector.configureModels')}
                            </DropdownMenuItem>
                          )}
                        </DropdownMenuSubContent>
                      </DropdownMenuSub>
                    )}
                    {isMobile ? (
                      <div
                        className="relative flex w-full cursor-default select-none items-center rounded-sm px-3 py-2 text-xs outline-none transition-colors hover:bg-accent/15"
                        style={{ color: 'var(--color-text-tertiary)' }}
                        role="menuitem"
                        onMouseDown={(e) => {
                          e.preventDefault();
                          navigate('/settings?tab=model');
                          setModelMenuOpen(false);
                        }}
                      >
                        {t('chat.modelSelector.manageModels')}
                      </div>
                    ) : (
                      <DropdownMenuItem
                        onSelect={() => navigate('/settings?tab=model')}
                        className="text-xs"
                        style={{ color: 'var(--color-text-tertiary)' }}
                      >
                        {t('chat.modelSelector.manageModels')}
                      </DropdownMenuItem>
                    )}
                  </DropdownMenuContent>
                </DropdownMenu>
              )}
              {/* Voice Input Button (Show only if enabled in user settings) */}
              {speechSupported && !isLoading && !!otherPref?.voice_input_enabled && (
                <div className="flex items-center">
                  <button
                    onClick={(e) => { e.stopPropagation(); toggleListening(); }}
                    disabled={disabled}
                    className={`inline-flex items-center justify-center h-8 w-8 rounded-xl transition-all active:scale-95 mic-button ${isListening ? 'recording' : 'text-[var(--color-icon-muted)] hover:text-[var(--color-text-muted)] hover:bg-foreground/5'}`}
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
              {/* Send / Stop Button */}
              {isLoading && onStop ? (
                <button
                  className="w-8 h-8 rounded-xl flex items-center justify-center transition-colors disabled:opacity-60 disabled:cursor-not-allowed"
                  style={{ backgroundColor: isStopping ? 'var(--color-btn-danger-pressed)' : 'var(--color-btn-danger)', color: 'var(--color-text-on-accent)' }}
                  onClick={(e) => { e.stopPropagation(); handleStop(); }}
                  disabled={isStopping}
                  title={isStopping ? 'Stopping...' : 'Stop'}
                  type="button"
                >
                  {isStopping ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : (
                    <Square className="h-4 w-4" fill="currentColor" />
                  )}
                </button>
              ) : (
                <button
                  onClick={(e) => { e.stopPropagation(); handleSend(); }}
                  disabled={!hasContent || disabled}
                  className="inline-flex items-center justify-center h-8 w-8 rounded-xl transition-colors active:scale-95 disabled:cursor-default"
                  style={{
                    backgroundColor: !hasContent || disabled ? 'var(--color-accent-disabled)' : 'var(--color-accent-primary)',
                    color: !hasContent || disabled ? 'var(--color-text-tertiary)' : 'var(--color-text-on-accent)',
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

export default ChatInput;
