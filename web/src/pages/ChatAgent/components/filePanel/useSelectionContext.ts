import { useCallback, useEffect, useRef, useState } from 'react';
import type { ContextPayload } from '@/components/ui/chat-input.types';
import { useStableHandler } from '@/hooks/useStableHandler';
import { lineCountOf, makeQuoteAnchor, type RectAnchor, type SelectionAnchor } from '../selectionQuote/quoteAnchor';
import { useQuoteSession } from '../selectionQuote/useQuoteSession';
import { useTextSelectionCapture } from '../selectionQuote/useTextSelectionCapture';
import type { ContextMenuData, EditorTextSelectData, FileQuoteAnchor } from './types';

/** `report.md:L3-5`, the spot a file selection is quoted from. */
export function selectionSourceName(path: string, { lineStart, lineEnd }: Pick<FileQuoteAnchor, 'lineStart' | 'lineEnd'>): string {
  const name = path.split('/').pop()!;
  if (lineStart == null) return name;
  return lineEnd != null && lineEnd !== lineStart ? `${name}:L${lineStart}-${lineEnd}` : `${name}:L${lineStart}`;
}

/** Quoting a selection in a file (read-only views + Monaco) and the right-click
 * file context menu. Line numbers are recovered via data-line attributes,
 * source-text matching, then DOM range counting. */
export function useSelectionContext({ selectedFile, fileContent, onAddContext }: {
  selectedFile: string | null;
  fileContent: string | null;
  onAddContext?: ((ctx: ContextPayload) => void) | null;
}) {
  const { anchor: selectionTooltip, composing: selectionComposing, show, beginReply, dismiss: dismissSelection } = useQuoteSession<FileQuoteAnchor>();
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

  // Clear the quote when navigating away from a file
  useEffect(() => {
    dismissSelection();
  }, [selectedFile, dismissSelection]);

  // Handle text selection in read-only views (Markdown, SyntaxHighlighter, etc.)
  const captureContentSelection = (text: string, range: Range, backward: boolean) => {
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
        if (lineEnd == null) lineEnd = lineStart + lineCountOf(text) - 1;
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
          lineStart = lineCountOf(textBefore!);
          lineEnd = lineStart + lineCountOf(text) - 1;
        }
      } catch {
        // Range operations can throw in edge cases
      }
    }

    show({ ...makeQuoteAnchor<SelectionAnchor>({ kind: 'selection', text, range, backward }), lineStart, lineEnd });
  };

  useTextSelectionCapture({
    surfaceRef: contentWrapperRef,
    enabled: !!onAddContext && !!selectedFile && !selectionComposing,
    onSelect: captureContentSelection,
    // Monaco's selection is not the document's, and reports its own clearing.
    onClear: () => {
      if (selectionTooltip?.kind === 'selection') dismissSelection();
    },
  });

  // Handle text selection from Monaco editor (CodeEditor), which keeps the
  // callback it was first handed.
  const handleEditorTextSelect = useStableHandler((selData: EditorTextSelectData | null) => {
    if (!onAddContext || !selectedFile || selectionComposing) return;
    if (!selData) {
      dismissSelection();
      return;
    }
    const wrapperRect = contentWrapperRef.current?.getBoundingClientRect();
    const rect = selData.rect
      ? { left: selData.rect.left, top: selData.rect.top, bottom: selData.rect.top + selData.rect.height }
      : { left: (wrapperRect?.left ?? 0) + 120, top: (wrapperRect?.top ?? 0) + 8, bottom: (wrapperRect?.top ?? 0) + 8 };
    show({
      ...makeQuoteAnchor<RectAnchor>({ kind: 'rect', text: selData.text, rect }),
      lineStart: selData.startLine,
      lineEnd: selData.endLine,
    });
  });

  const handleAddSelectionContext = useCallback((reply?: string) => {
    if (!selectionTooltip || !selectedFile || !onAddContext) return;
    const { text, lineStart, lineEnd } = selectionTooltip;
    onAddContext({
      kind: 'quote',
      snippet: text,
      reply,
      from: {
        kind: 'file',
        path: selectedFile,
        label: selectionSourceName(selectedFile, selectionTooltip),
        lineStart,
        lineEnd,
        lineCount: lineStart != null && lineEnd != null ? lineEnd - lineStart + 1 : lineCountOf(text),
      },
    });
    dismissSelection();
    window.getSelection()?.removeAllRanges();
  }, [selectionTooltip, selectedFile, onAddContext, dismissSelection]);

  return {
    selectionTooltip,
    selectionComposing,
    beginReply,
    dismissSelection,
    contentWrapperRef,
    contextMenu,
    setContextMenu,
    handleEditorTextSelect,
    handleAddSelectionContext,
  };
}
