import { useCallback, useEffect, useRef, useState } from 'react';
import type { ContextMenuData, ContextPayload, EditorTextSelectData, SelectionTooltipData } from './types';

/** "Add to context" text-selection tooltip (read-only views + Monaco) and the
 * right-click file context menu. Line numbers are recovered via data-line
 * attributes, source-text matching, then DOM range counting. */
export function useSelectionContext({ selectedFile, fileContent, onAddContext }: {
  selectedFile: string | null;
  fileContent: string | null;
  onAddContext?: ((ctx: ContextPayload) => void) | null;
}) {
  // Selection tooltip state ("Add to context")
  const [selectionTooltip, setSelectionTooltip] = useState<SelectionTooltipData | null>(null);
  const contentWrapperRef = useRef<HTMLDivElement>(null);

  // Right-click context menu state
  const [contextMenu, setContextMenu] = useState<ContextMenuData | null>(null);

  // Close context menu on outside click
  useEffect(() => {
    if (!contextMenu) return;
    const handler = () => setContextMenu(null);
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [contextMenu]);

  // Clear selection tooltip when navigating away from a file
  useEffect(() => {
    setSelectionTooltip(null);
  }, [selectedFile]);

  // Clear selection tooltip on mousedown if selection is empty
  useEffect(() => {
    if (!selectionTooltip) return;
    const handler = () => {
      setTimeout(() => {
        const sel = window.getSelection();
        if (!sel || !sel.toString().trim()) setSelectionTooltip(null);
      }, 10);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [selectionTooltip]);

  // Handle text selection in read-only views (Markdown, SyntaxHighlighter, etc.)
  const handleContentMouseUp = useCallback(() => {
    if (!onAddContext || !selectedFile) return;
    // Small delay to let the browser finalize the selection
    setTimeout(() => {
      const sel = window.getSelection();
      if (!sel || !sel.toString().trim()) {
        setSelectionTooltip(null);
        return;
      }
      const text = sel.toString();
      const range = sel.getRangeAt(0);
      const rect = range.getBoundingClientRect();
      const wrapper = contentWrapperRef.current;
      const wrapperRect = wrapper?.getBoundingClientRect();
      if (!wrapperRect) return;

      // Account for scroll position
      const scrollTop = wrapper!.scrollTop || 0;
      const scrollLeft = wrapper!.scrollLeft || 0;

      // Determine accurate SOURCE line numbers (matching what the agent sees)
      let lineStart: number | undefined | null;
      let lineEnd: number | undefined | null;

      const startNode = range.startContainer.nodeType === 3
        ? range.startContainer.parentElement
        : range.startContainer as Element;
      const endNode = range.endContainer.nodeType === 3
        ? range.endContainer.parentElement
        : range.endContainer as Element;

      // Method 1: data-line attributes from SyntaxHighlighter
      const startLineEl = startNode?.closest?.('[data-line]');
      const endLineEl = endNode?.closest?.('[data-line]');
      if (startLineEl && endLineEl) {
        lineStart = parseInt((startLineEl as HTMLElement).dataset.line!, 10);
        lineEnd = parseInt((endLineEl as HTMLElement).dataset.line!, 10);
      }

      // Method 2: Source-text matching
      if (lineStart == null && fileContent && typeof fileContent === 'string') {
        const selectedLines = text.split('\n').filter((l: string) => l.trim());
        const firstLine = (selectedLines[0] || '').trim();
        const lastLine = selectedLines.length > 1 ? (selectedLines[selectedLines.length - 1] || '').trim() : firstLine;

        const getSearchWords = (line: string) => line.split(/\s+/).filter((w: string) => w.replace(/[^a-zA-Z0-9]/g, '').length > 2);

        const findLineInSource = (searchLine: string, fromLine = 0): number | null => {
          const words = getSearchWords(searchLine);
          if (words.length < 2) {
            const fragment = searchLine.substring(0, Math.min(searchLine.length, 40));
            if (fragment.length >= 5) {
              const sourceLines = fileContent!.split('\n');
              for (let i = fromLine; i < sourceLines.length; i++) {
                if (sourceLines[i].includes(fragment)) return i + 1;
              }
            }
            return null;
          }
          try {
            const pattern = words.slice(0, 8).map((w: string) =>
              w.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
            ).join('[\\s\\S]{0,30}');
            const regex = new RegExp(pattern);
            const sourceLines = fileContent!.split('\n');
            for (let i = fromLine; i < sourceLines.length; i++) {
              if (regex.test(sourceLines[i])) return i + 1;
            }
          } catch { /* regex failed */ }
          return null;
        };

        lineStart = findLineInSource(firstLine);
        if (lineStart != null) {
          if (firstLine !== lastLine) {
            lineEnd = findLineInSource(lastLine, lineStart - 1);
          }
          if (lineEnd == null) lineEnd = lineStart + (text.match(/\n/g) || []).length;
        }
      }

      // Method 3: DOM range counting fallback
      if (lineStart == null) {
        try {
          const contentRoot = startNode?.closest?.('pre') || startNode?.closest?.('.p-4');
          if (contentRoot && !startNode?.closest?.('.markdown-print-content')) {
            const preRange = document.createRange();
            preRange.selectNodeContents(contentRoot);
            preRange.setEnd(range.startContainer, range.startOffset);
            const fragment = preRange.cloneContents();
            fragment.querySelectorAll('[style*="user-select"]').forEach((el) => el.remove());
            const textBefore = fragment.textContent;
            lineStart = (textBefore!.match(/\n/g) || []).length + 1;
            lineEnd = lineStart + (text.match(/\n/g) || []).length;
          }
        } catch {
          // Range operations can throw in edge cases
        }
      }

      setSelectionTooltip({
        x: rect.left - wrapperRect.left + scrollLeft + rect.width / 2,
        y: rect.top - wrapperRect.top + scrollTop - 8,
        text,
        lineStart,
        lineEnd,
      });
    }, 10);
  }, [onAddContext, selectedFile, fileContent]);

  // Handle text selection from Monaco editor (CodeEditor)
  const handleEditorTextSelect = useCallback((selData: EditorTextSelectData | null) => {
    if (!onAddContext || !selectedFile) return;
    if (!selData) {
      setSelectionTooltip(null);
      return;
    }
    const wrapper = contentWrapperRef.current;
    const wrapperRect = wrapper?.getBoundingClientRect();
    let x = 120, y = 8;
    if (selData.rect && wrapperRect) {
      x = selData.rect.left - wrapperRect.left + 50;
      y = selData.rect.top - wrapperRect.top - 8;
    }
    setSelectionTooltip({
      x, y,
      text: selData.text,
      lineStart: selData.startLine,
      lineEnd: selData.endLine,
    });
  }, [onAddContext, selectedFile]);

  const handleAddSelectionContext = useCallback(() => {
    if (!selectionTooltip || !selectedFile || !onAddContext) return;
    const { text, lineStart, lineEnd } = selectionTooltip;
    const fileName = selectedFile.split('/').pop()!;
    const lineCount = lineStart != null && lineEnd != null ? lineEnd - lineStart + 1 : (text.match(/\n/g) || []).length + 1;
    const label = lineStart != null
      ? (lineStart === lineEnd ? `${fileName}:L${lineStart}` : `${fileName}:L${lineStart}-${lineEnd}`)
      : fileName;
    onAddContext({ path: selectedFile, snippet: text, label, lineStart, lineEnd, lineCount });
    setSelectionTooltip(null);
    window.getSelection()?.removeAllRanges();
  }, [selectionTooltip, selectedFile, onAddContext]);

  return {
    selectionTooltip,
    setSelectionTooltip,
    contentWrapperRef,
    contextMenu,
    setContextMenu,
    handleContentMouseUp,
    handleEditorTextSelect,
    handleAddSelectionContext,
  };
}
