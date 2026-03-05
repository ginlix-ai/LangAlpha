import React, { useState, useRef, useEffect, useCallback, useMemo, forwardRef, useImperativeHandle } from 'react';
import {
  Plus, ArrowUp, X, FileText, Loader2, Archive, Square,
  ScrollText, ChartCandlestick, Zap, FileStack, ChevronDown, FolderOpen, TextSelect,
  Terminal, Bot, Shrink, HardDriveDownload,
} from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { TokenUsageRing } from './token-usage-ring';
import { getSkills } from '../../pages/ChatAgent/utils/api';
import './chat-input.css';

/* --- UTILS --- */

/** Return the appropriate icon for a slash command. */
function getSlashCommandIcon(cmd, className) {
  if (cmd.type === 'subagent') return <Bot className={className} />;
  if (cmd.name === 'offload') return <HardDriveDownload className={className} />;
  if (cmd.type === 'action') return <Shrink className={className} />;
  return <Terminal className={className} />;
}

const formatFileSize = (bytes) => {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
};

/* --- FILE PREVIEW CARD --- */
const FilePreviewCard = ({ file, onRemove }) => {
  const isImage = file.type.startsWith('image/') && file.preview;

  return (
    <div className="relative group flex-shrink-0 w-24 h-24 rounded-xl overflow-hidden border border-[var(--color-border-muted)] bg-[var(--color-bg-elevated)] animate-fade-in transition-all hover:border-[var(--color-border-default)]">
      {isImage ? (
        <div className="w-full h-full relative">
          <img src={file.preview} alt={file.file.name} className="w-full h-full object-cover" />
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
        className="absolute top-1 right-1 p-1 bg-black/50 hover:bg-black/70 rounded-full text-white opacity-0 group-hover:opacity-100 transition-opacity"
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
  { type: 'action', name: 'summarize', aliases: ['compaction', 'compact'] },
  { type: 'action', name: 'offload', aliases: ['truncate'] },
];

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
const ChatInput = forwardRef(function ChatInput({
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
  // Action commands (e.g. /summarize) — fired immediately on selection
  onAction = null,
}, ref) {
  const { t } = useTranslation();
  const [message, setMessage] = useState('');
  const [attachedFiles, setAttachedFiles] = useState([]);
  const [isDragging, setIsDragging] = useState(false);
  const [planMode, setPlanMode] = useState(false);

  // @file mention state
  const [mentionedFiles, setMentionedFiles] = useState([]);
  const [showAutocomplete, setShowAutocomplete] = useState(false);
  const [autocompleteQuery, setAutocompleteQuery] = useState('');
  const [activeIndex, setActiveIndex] = useState(0);
  const [mentionStart, setMentionStart] = useState(-1);

  // /slash command state
  const [slashCommands, setSlashCommands] = useState([]);
  const [showSlashMenu, setShowSlashMenu] = useState(false);
  const [slashQuery, setSlashQuery] = useState('');
  const [slashActiveIndex, setSlashActiveIndex] = useState(0);
  const [slashStart, setSlashStart] = useState(-1);
  const [skills, setSkills] = useState([]);

  // Fetch skills filtered by agent mode (re-fetches when mode changes; cached per mode in api.js)
  const skillsMode = mode === 'fast' ? 'flash' : 'ptc';
  useEffect(() => {
    getSkills(skillsMode).then(setSkills).catch(() => {});
  }, [skillsMode]);

  // Stop button state
  const [isStopping, setIsStopping] = useState(false);

  // Expose addContext method for external callers (e.g. FilePanel, message selection)
  useImperativeHandle(ref, () => ({
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
  }), []);

  // Workspace dropdown
  const [showWorkspaceMenu, setShowWorkspaceMenu] = useState(false);
  const workspaceMenuRef = useRef(null);

  const textareaRef = useRef(null);
  const fileInputRef = useRef(null);
  const autocompleteRef = useRef(null);
  const slashMenuRef = useRef(null);

  const hasModeToggle = mode !== undefined && onModeChange !== undefined;

  // Prefill support
  useEffect(() => {
    if (prefillMessage) {
      setMessage(prefillMessage);
      onClearPrefill?.();
    }
  }, [prefillMessage, onClearPrefill]);

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
    const handleClickOutside = (e) => {
      if (workspaceMenuRef.current && !workspaceMenuRef.current.contains(e.target)) {
        setShowWorkspaceMenu(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [showWorkspaceMenu]);

  // --- File Upload Handling ---
  const handleFiles = useCallback((newFilesList) => {
    const currentCount = attachedFiles.length;
    const fileArray = Array.from(newFilesList);

    const validFiles = [];
    for (const file of fileArray) {
      if (currentCount + validFiles.length >= MAX_FILES) break;
      const isImage = file.type.startsWith('image/') || /\.(jpg|jpeg|png|gif|webp)$/i.test(file.name);
      const isPdf = file.type === 'application/pdf' || /\.pdf$/i.test(file.name);
      if (!isImage && !isPdf) continue;
      if (file.size > MAX_FILE_SIZE) continue;
      validFiles.push(file);
    }

    const newFiles = validFiles.map((file) => {
      const isImage = file.type.startsWith('image/') || /\.(jpg|jpeg|png|gif|webp)$/i.test(file.name);
      return {
        id: Math.random().toString(36).substr(2, 9),
        file,
        type: isImage ? (file.type || 'image/png') : (file.type || 'application/pdf'),
        preview: isImage ? URL.createObjectURL(file) : null,
        uploadStatus: 'pending',
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
            p.id === f.id ? { ...p, uploadStatus: 'complete', dataUrl: reader.result } : p
          )
        );
      };
      reader.onerror = () => {
        setAttachedFiles((prev) => prev.filter((p) => p.id !== f.id));
      };
      reader.readAsDataURL(f.file);
    });
  }, [attachedFiles.length]);

  const removeFile = useCallback((id) => {
    setAttachedFiles((prev) => {
      const file = prev.find((f) => f.id === id);
      if (file?.preview) URL.revokeObjectURL(file.preview);
      return prev.filter((f) => f.id !== id);
    });
  }, []);

  // Drag & Drop
  const onDragOver = useCallback((e) => {
    e.preventDefault();
    setIsDragging(true);
  }, []);
  const onDragLeave = useCallback((e) => {
    e.preventDefault();
    setIsDragging(false);
  }, []);
  const onDrop = useCallback((e) => {
    e.preventDefault();
    setIsDragging(false);
    if (e.dataTransfer.files) handleFiles(e.dataTransfer.files);
  }, [handleFiles]);

  // Paste Handling
  const handlePaste = useCallback((e) => {
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
    const dirPriority = { '': 0, results: 1, data: 2 };
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
    const items = [
      ...skills.filter((s) => s.command).map((s) => ({ type: 'skill', name: s.command, skillName: s.name, description: t(`chat.slashCommand.${s.command}Desc`, { defaultValue: s.description }) })),
      ...BUILTIN_SLASH_COMMANDS.map((c) => ({ ...c, description: t(`chat.slashCommand.${c.name}Desc`) })),
    ];
    return items
      .filter((item) => !slashCommands.some((c) => c.name === item.name))
      .filter((item) => {
        if (!query) return true;
        if (item.name.toLowerCase().includes(query)) return true;
        if (item.description.toLowerCase().includes(query)) return true;
        if (item.aliases?.some((a) => a.toLowerCase().includes(query))) return true;
        return false;
      })
      .slice(0, 10);
  }, [skills, showSlashMenu, slashQuery, slashCommands, t]);

  useEffect(() => {
    setSlashActiveIndex(0);
  }, [filteredSlashCommands.length]);

  // Detect @ and / triggers on input change
  const handleChange = useCallback((e) => {
    const val = e.target.value;
    setMessage(val);

    // Remove pills whose /{command} or @mention text was deleted from the textarea
    setSlashCommands((prev) => prev.filter((cmd) => val.includes(`/${cmd.name}`)));
    setMentionedFiles((prev) => prev.filter((f) => val.includes(`@${f.path}`)));

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

  const selectFile = useCallback((filePath) => {
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

  const removeMention = useCallback((path, label, snippet) => {
    setMentionedFiles((prev) => prev.filter((f) => {
      if (snippet) return !(f.snippet === snippet && f.path === path);
      if (label) return !(f.path === path && f.label === label);
      return !(f.path === path && !f.label);
    }));
    // Also remove @path text from textarea
    setMessage((prev) => prev.replace(new RegExp(`(^|\\s)@${path.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(\\s|$)`), '$1$2').trim());
  }, []);

  const selectSlashCommand = useCallback((cmd) => {
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

  const removeSlashCommand = useCallback((name) => {
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
    onSend(finalMessage, planMode, readyAttachments, slashCommands);
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
  }, [hasContent, disabled, message, planMode, attachedFiles, chartImage, onSend, mentionedFiles, slashCommands]);

  // --- Keyboard ---
  const handleKeyDown = useCallback((e) => {
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
  }, [showSlashMenu, filteredSlashCommands, slashActiveIndex, selectSlashCommand, showAutocomplete, filteredMentionFiles, activeIndex, selectFile, handleSend]);

  // Workspace selector helpers
  const selectedWorkspaceName = useMemo(() => {
    if (!workspaces || !selectedWorkspaceId) return 'Workspace';
    const ws = workspaces.find((w) => w.workspace_id === selectedWorkspaceId);
    return ws?.name || 'Workspace';
  }, [workspaces, selectedWorkspaceId]);

  const showWorkspaceSelector = hasModeToggle && mode === 'deep' && workspaces && workspaces.length > 0;

  return (
    <div
      className="relative w-full"
      onDragOver={onDragOver}
      onDragLeave={onDragLeave}
      onDrop={onDrop}
    >
      {/* Main Container */}
      <div
        className="flex flex-col items-stretch transition-all duration-200 relative z-10 rounded-2xl cursor-text border border-[hsl(var(--primary))] bg-[var(--color-bg-card)]"
        onClick={() => textareaRef.current?.focus()}
      >
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
                    className="absolute top-1 right-1 p-1 bg-black/50 hover:bg-black/70 rounded-full text-white opacity-0 group-hover:opacity-100 transition-opacity"
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

          {/* Workspace dropdown */}
          {showWorkspaceMenu && showWorkspaceSelector && (
            <div className="workspace-dropdown" ref={workspaceMenuRef}>
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

          {/* Textarea */}
          <div className="relative">
            <textarea
              ref={textareaRef}
              value={message}
              onChange={handleChange}
              onPaste={handlePaste}
              onKeyDown={handleKeyDown}
              onBlur={handleBlur}
              placeholder={placeholder}
              className="w-full bg-transparent border-0 outline-none text-[var(--color-text-primary)] text-sm placeholder:text-[var(--color-text-tertiary)] resize-none overflow-hidden leading-relaxed block"
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
                  title={mode === 'fast' ? 'Flash — quick answer using fast model' : 'Deep — full agent with workspace and tools'}
                >
                  {mode === 'fast' ? <Zap className="h-4 w-4" /> : <FileStack className="h-4 w-4" />}
                  <span>{mode === 'fast' ? 'Fast' : 'Deep'}</span>
                </button>
              )}

              {/* Workspace Selector */}
              {showWorkspaceSelector && (
                <button
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
      {isDragging && (
        <div className="absolute inset-0 bg-[var(--color-accent-soft)] border-2 border-dashed border-[hsl(var(--primary))] rounded-2xl z-50 flex flex-col items-center justify-center backdrop-blur-sm pointer-events-none">
          <Archive className="w-10 h-10 text-[hsl(var(--primary))] mb-2 animate-bounce" />
          <p className="text-[hsl(var(--primary))] font-medium">Drop files to upload</p>
        </div>
      )}

      {/* Hidden File Input */}
      <input
        ref={fileInputRef}
        type="file"
        multiple
        accept="image/png,image/jpeg,image/gif,image/webp,application/pdf"
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
