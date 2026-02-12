import React, { useState, useRef, useEffect, useCallback, useMemo } from 'react';
import {
  Plus, ArrowUp, X, FileText, Loader2, Archive, Square,
  ScrollText, ChartCandlestick, Zap, FileStack, ChevronDown, FolderOpen,
} from 'lucide-react';
import './chat-input.css';

/* --- UTILS --- */
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
    <div className="relative group flex-shrink-0 w-24 h-24 rounded-xl overflow-hidden border border-[rgba(255,255,255,0.1)] bg-[rgba(255,255,255,0.05)] animate-fade-in transition-all hover:border-[rgba(255,255,255,0.25)]">
      {isImage ? (
        <div className="w-full h-full relative">
          <img src={file.preview} alt={file.file.name} className="w-full h-full object-cover" />
          <div className="absolute inset-0 bg-black/20 group-hover:bg-black/0 transition-colors" />
        </div>
      ) : (
        <div className="w-full h-full p-3 flex flex-col justify-between">
          <div className="flex items-center gap-2">
            <div className="p-1.5 bg-[rgba(255,255,255,0.1)] rounded">
              <FileText className="w-4 h-4 text-[rgba(255,255,255,0.6)]" />
            </div>
            <span className="text-[10px] font-medium text-[rgba(255,255,255,0.45)] uppercase tracking-wider truncate">
              {file.file.name.split('.').pop()}
            </span>
          </div>
          <div className="space-y-0.5">
            <p className="text-xs font-medium text-[#BBBBBB] truncate" title={file.file.name}>
              {file.file.name}
            </p>
            <p className="text-[10px] text-[rgba(255,255,255,0.35)]">
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

/* --- MAIN COMPONENT --- */

/**
 * ChatInput — unified chat input component used across the entire app.
 *
 * @param {Function}  onSend              - (message, planMode, attachments) => void
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
function ChatInput({
  onSend,
  disabled = false,
  isLoading = false,
  onStop,
  placeholder = 'What would you like to know? Type @ to mention a file',
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
}) {
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

  // Stop button state
  const [isStopping, setIsStopping] = useState(false);

  // Workspace dropdown
  const [showWorkspaceMenu, setShowWorkspaceMenu] = useState(false);
  const workspaceMenuRef = useRef(null);

  const textareaRef = useRef(null);
  const fileInputRef = useRef(null);
  const autocompleteRef = useRef(null);

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

  // Detect @ trigger on input change
  const handleChange = useCallback((e) => {
    const val = e.target.value;
    setMessage(val);

    const cursorPos = e.target.selectionStart;
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

  const removeMention = useCallback((path) => {
    setMentionedFiles((prev) => prev.filter((f) => f.path !== path));
  }, []);

  // Scroll active autocomplete item into view
  useEffect(() => {
    if (!showAutocomplete || !autocompleteRef.current) return;
    const items = autocompleteRef.current.querySelectorAll('.mention-autocomplete-item');
    if (items[activeIndex]) {
      items[activeIndex].scrollIntoView({ block: 'nearest' });
    }
  }, [activeIndex, showAutocomplete]);

  // Close autocomplete on blur
  const handleBlur = useCallback(() => {
    setTimeout(() => {
      setShowAutocomplete(false);
    }, 200);
  }, []);

  // --- Send ---
  const hasContent = message.trim() || attachedFiles.length > 0 || !!chartImage;

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
    onSend(message, planMode, readyAttachments);
    setMessage('');
    setAttachedFiles([]);
    setMentionedFiles([]);
    setShowAutocomplete(false);
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto';
    }
  }, [hasContent, disabled, message, planMode, attachedFiles, chartImage, onSend]);

  // --- Keyboard ---
  const handleKeyDown = useCallback((e) => {
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
  }, [showAutocomplete, filteredMentionFiles, activeIndex, selectFile, handleSend]);

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
        className="flex flex-col items-stretch transition-all duration-200 relative z-10 rounded-2xl cursor-text border border-[hsl(var(--primary))] bg-[rgba(0,0,0,0.3)]"
        onClick={() => textareaRef.current?.focus()}
      >
        <div className="flex flex-col px-3 pt-3 pb-2 gap-2">

          {/* Mention pills */}
          {mentionedFiles.length > 0 && (
            <div className="mention-pills">
              {mentionedFiles.map((f) => {
                const name = f.path.split('/').pop();
                return (
                  <div key={f.path} className="mention-pill" title={f.path}>
                    <FileText className="h-3 w-3 flex-shrink-0" style={{ color: 'rgba(97, 85, 245, 0.8)' }} />
                    <span>{name}</span>
                    <button
                      className="mention-pill-remove"
                      onClick={(e) => { e.stopPropagation(); removeMention(f.path); }}
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
                <div className="relative group flex-shrink-0 w-24 h-24 rounded-xl overflow-hidden border border-[rgba(255,255,255,0.1)] bg-[rgba(255,255,255,0.05)] animate-fade-in transition-all hover:border-[rgba(255,255,255,0.25)]">
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
                      <FileText className="h-4 w-4 flex-shrink-0" style={{ color: 'rgba(255,255,255,0.45)' }} />
                      <span className="file-name">{name}</span>
                      {dir && <span className="file-path">{dir}/</span>}
                    </div>
                  );
                })
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
                  <FolderOpen className="h-4 w-4 flex-shrink-0" style={{ color: 'rgba(255,255,255,0.45)' }} />
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
              className="w-full bg-transparent border-0 outline-none text-[#BBBBBB] text-sm placeholder:text-[rgba(255,255,255,0.35)] resize-none overflow-hidden leading-relaxed block"
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
                className="inline-flex items-center justify-center h-8 w-8 rounded-lg transition-colors text-[rgba(255,255,255,0.45)] hover:text-[#BBBBBB] hover:bg-[rgba(255,255,255,0.05)] active:scale-95"
                type="button"
                aria-label="Attach file"
              >
                <Plus className="w-5 h-5" />
              </button>

              {/* Chart Capture (trading only) */}
              {onCaptureChart && (
                <button
                  onClick={(e) => { e.stopPropagation(); onCaptureChart(); }}
                  className="inline-flex items-center justify-center h-8 w-8 rounded-lg transition-colors text-[rgba(255,255,255,0.45)] hover:text-[#BBBBBB] hover:bg-[rgba(255,255,255,0.05)] active:scale-95"
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
                    background: mode === 'deep' ? 'rgba(97, 85, 245, 0.25)' : 'transparent',
                    color: mode === 'deep' ? '#a89afb' : 'var(--color-text-muted, #8b8fa3)',
                    border: mode === 'deep' ? '1px solid rgba(97, 85, 245, 0.5)' : '1px solid transparent',
                    transition: 'background 0.2s, color 0.2s, border-color 0.2s',
                  }}
                  onClick={(e) => { e.stopPropagation(); onModeChange(mode === 'fast' ? 'deep' : 'fast'); }}
                  onMouseEnter={(e) => {
                    if (mode !== 'deep') e.currentTarget.style.background = 'rgba(255, 255, 255, 0.05)';
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
                    background: showWorkspaceMenu ? 'rgba(97, 85, 245, 0.25)' : 'transparent',
                    color: showWorkspaceMenu ? '#a89afb' : 'var(--color-text-muted, #8b8fa3)',
                    border: showWorkspaceMenu ? '1px solid rgba(97, 85, 245, 0.5)' : '1px solid transparent',
                    transition: 'background 0.2s, color 0.2s, border-color 0.2s',
                  }}
                  onClick={(e) => { e.stopPropagation(); setShowWorkspaceMenu((v) => !v); }}
                  onMouseEnter={(e) => {
                    if (!showWorkspaceMenu) e.currentTarget.style.background = 'rgba(255, 255, 255, 0.05)';
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
                    background: planMode ? 'rgba(97, 85, 245, 0.25)' : 'transparent',
                    color: planMode ? '#a89afb' : 'var(--color-text-muted, #8b8fa3)',
                    border: planMode ? '1px solid rgba(97, 85, 245, 0.5)' : '1px solid transparent',
                    transition: 'background 0.2s, color 0.2s, border-color 0.2s',
                  }}
                  onClick={(e) => { e.stopPropagation(); setPlanMode(!planMode); }}
                  onMouseEnter={(e) => {
                    if (!planMode) e.currentTarget.style.background = 'rgba(255, 255, 255, 0.05)';
                  }}
                  onMouseLeave={(e) => {
                    if (!planMode) e.currentTarget.style.background = 'transparent';
                  }}
                  type="button"
                  title="Plan mode — agent creates a plan for approval before executing"
                >
                  <ScrollText className="h-4 w-4" style={planMode ? { color: '#a89afb' } : {}} />
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
                  style={{ backgroundColor: isStopping ? '#991b1b' : '#dc2626', color: '#FFFFFF' }}
                  onClick={(e) => { e.stopPropagation(); handleStop(); }}
                  disabled={isStopping}
                  title={isStopping ? 'Stopping...' : 'Stop'}
                  type="button"
                >
                  {isStopping ? (
                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  ) : (
                    <Square className="h-3.5 w-3.5" fill="currentColor" />
                  )}
                </button>
              ) : (
                <button
                  onClick={(e) => { e.stopPropagation(); handleSend(); }}
                  disabled={!hasContent || disabled}
                  className="inline-flex items-center justify-center h-8 w-8 rounded-xl transition-colors active:scale-95 disabled:cursor-default"
                  style={{
                    backgroundColor: !hasContent || disabled ? 'rgba(97, 85, 245, 0.3)' : '#6155F5',
                    color: !hasContent || disabled ? 'rgba(255,255,255,0.4)' : '#FFFFFF',
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
        <div className="absolute inset-0 bg-[rgba(255,255,255,0.05)] border-2 border-dashed border-[#6155F5] rounded-2xl z-50 flex flex-col items-center justify-center backdrop-blur-sm pointer-events-none">
          <Archive className="w-10 h-10 text-[#6155F5] mb-2 animate-bounce" />
          <p className="text-[#6155F5] font-medium">Drop files to upload</p>
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
}

export default ChatInput;
