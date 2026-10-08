import React from 'react';
import Editor, { DiffEditor, loader } from '@monaco-editor/react';
import type { editor } from 'monaco-editor';
import { useTheme } from '@/contexts/ThemeContext';
import { rememberMonaco, saveViewState } from './editorModels';

// @monaco-editor/loader fetches Monaco from the CDN at a release it picks, which
// the monaco-editor package, installed only for its types, never moved. Pinned
// to the installed release, the editor that runs is the one the types describe.
loader.config({ paths: { vs: `https://cdn.jsdelivr.net/npm/monaco-editor@${__MONACO_VERSION__}/min/vs` } });

const EXT_TO_MONACO_LANG: Record<string, string> = {
  py: 'python', js: 'javascript', jsx: 'javascript', ts: 'typescript', tsx: 'typescript',
  json: 'json', md: 'markdown', yaml: 'yaml', yml: 'yaml', sql: 'sql',
  sh: 'shell', bash: 'shell', rs: 'rust', rb: 'ruby', go: 'go', java: 'java',
  xml: 'xml', css: 'css', html: 'html', htm: 'html', toml: 'ini', cfg: 'ini', ini: 'ini',
  txt: 'plaintext', csv: 'plaintext', env: 'shell', log: 'plaintext',
};

function getLanguageFromFileName(fileName: string | undefined): string {
  const ext = (fileName || '').split('.').pop()?.toLowerCase() || '';
  return EXT_TO_MONACO_LANG[ext] || 'plaintext';
}

const EDITOR_OPTIONS: editor.IStandaloneEditorConstructionOptions = {
  minimap: { enabled: false },
  lineNumbers: 'on',
  wordWrap: 'on',
  scrollBeyondLastLine: false,
  fontSize: 12,
  automaticLayout: true,
  padding: { top: 8 },
  // Monaco draws its own scrollbar, so the app's inset (styles/tokens.css) is
  // set here: a 6px slider centred in a 12px lane, and 4px arrows, hidden in
  // CSS, that Monaco's own math keeps the track clear of at both ends.
  scrollbar: {
    verticalScrollbarSize: 12,
    horizontalScrollbarSize: 12,
    verticalSliderSize: 6,
    horizontalSliderSize: 6,
    verticalHasArrows: true,
    horizontalHasArrows: true,
    arrowSize: 4,
  },
};

interface TextSelection {
  text: string;
  startLine: number;
  endLine: number;
  rect: { left: number; top: number; width: number; height: number } | null;
}

interface UndoRedoState {
  canUndo: boolean;
  canRedo: boolean;
}

interface CodeEditorProps {
  value: string | undefined;
  onChange?: (value: string) => void;
  fileName?: string;
  readOnly?: boolean;
  height?: string;
  diffMode?: boolean;
  originalValue?: string;
  editorRef?: React.MutableRefObject<editor.IStandaloneCodeEditor | null>;
  /** The Monaco model to edit. Given, the model outlives this editor, so undo
   * history and view state survive a remount, and the caller disposes it. */
  modelPath?: string;
  onUndoRedoChange?: (state: UndoRedoState) => void;
  onTextSelect?: (selection: TextSelection | null) => void;
}

export default function CodeEditor({ value, onChange, fileName, readOnly = false, height = '100%', diffMode = false, originalValue, editorRef, modelPath, onUndoRedoChange, onTextSelect }: CodeEditorProps) {
  const language = getLanguageFromFileName(fileName);
  const theme = useTheme().theme === 'light' ? 'vs' : 'vs-dark';
  const showDiff = diffMode && originalValue != null;

  // Track DiffEditor listener disposables to prevent "TextModel got disposed" race
  const diffDisposablesRef = React.useRef<{ dispose(): void }[]>([]);
  const diffDisposedRef = React.useRef(false);

  // The instance goes with this mount; a ref left pointing at it would
  // drive a disposed editor from the toolbar.
  const mountedRef = React.useRef<editor.IStandaloneCodeEditor | null>(null);
  React.useEffect(() => () => {
    if (editorRef && editorRef.current === mountedRef.current) editorRef.current = null;
  }, [editorRef]);

  // A kept model's scroll and selection are filed with its session in
  // editorModels. This is a layout cleanup because the library disposes the
  // editor in a passive one, and the state has to be read before that.
  React.useLayoutEffect(() => () => {
    if (modelPath && mountedRef.current) saveViewState(mountedRef.current);
  }, [modelPath]);

  React.useEffect(() => {
    if (showDiff) {
      diffDisposedRef.current = false;
    }
    return () => {
      diffDisposedRef.current = true;
      diffDisposablesRef.current.forEach((d) => d.dispose());
      diffDisposablesRef.current = [];
    };
  }, [showDiff]);

  return (
    <div style={{ position: 'relative', height, width: '100%' }}>
      {/* Always-mounted editor — preserves undo stack across diff toggles */}
      <div style={showDiff ? { position: 'absolute', inset: 0, visibility: 'hidden', pointerEvents: 'none' } : { height: '100%' }}>
        {/* One instance per model: onMount reports the undo state of the model
            it opens on, and a model swapped into a live instance would leave
            the toolbar showing the last one's. */}
        <Editor
          key={modelPath}
          height="100%"
          language={language}
          theme={theme}
          value={value ?? ''}
          path={modelPath}
          keepCurrentModel={modelPath != null}
          saveViewState={false}
          beforeMount={rememberMonaco}
          onMount={(monacoEditor: editor.IStandaloneCodeEditor) => {
            mountedRef.current = monacoEditor;
            if (editorRef) editorRef.current = monacoEditor;
            // Read from the model, not counted from this mount: a kept model
            // arrives with the history it had when its tab was left.
            const reportUndoRedo = () => {
              const model = monacoEditor.getModel();
              onUndoRedoChange?.({ canUndo: !!model?.canUndo(), canRedo: !!model?.canRedo() });
            };
            reportUndoRedo();
            monacoEditor.onDidChangeModelContent(() => {
              reportUndoRedo();
              onChange?.(monacoEditor.getValue());
            });
            // Text selection callback for "Add to context"
            if (onTextSelect) {
              monacoEditor.onDidChangeCursorSelection(() => {
                const sel = monacoEditor.getSelection();
                if (!sel || sel.isEmpty()) {
                  onTextSelect(null);
                  return;
                }
                const text = monacoEditor.getModel()?.getValueInRange(sel);
                if (!text?.trim()) {
                  onTextSelect(null);
                  return;
                }
                // Get visual position of selection start for tooltip placement
                const pos = monacoEditor.getScrolledVisiblePosition(sel.getStartPosition());
                const editorDom = monacoEditor.getDomNode();
                const editorRect = editorDom?.getBoundingClientRect();
                const rect = (pos && editorRect) ? {
                  left: editorRect.left + pos.left,
                  top: editorRect.top + pos.top,
                  width: 0,
                  height: pos.height || 18,
                } : null;
                onTextSelect({ text, startLine: sel.startLineNumber, endLine: sel.endLineNumber, rect });
              });
            }
          }}
          options={{ ...EDITOR_OPTIONS, readOnly }}
        />
      </div>
      {/* Diff overlay — edits here flow back to the normal editor via onChange → value prop */}
      {showDiff && (
        <div style={{ position: 'absolute', inset: 0 }}>
          <DiffEditor
            height="100%"
            language={language}
            theme={theme}
            original={originalValue}
            modified={value ?? ''}
            onMount={(diffEditor: editor.IStandaloneDiffEditor) => {
              // Dispose any stale listeners from a previous mount
              diffDisposablesRef.current.forEach((d) => d.dispose());
              diffDisposablesRef.current = [];

              const modifiedEditor = diffEditor.getModifiedEditor();
              diffDisposablesRef.current.push(
                modifiedEditor.onDidChangeModelContent(() => {
                  if (diffDisposedRef.current) return;
                  onChange?.(modifiedEditor.getValue());
                }),
              );
              if (onTextSelect) {
                diffDisposablesRef.current.push(
                  modifiedEditor.onDidChangeCursorSelection(() => {
                    if (diffDisposedRef.current) return;
                    const sel = modifiedEditor.getSelection();
                    if (!sel || sel.isEmpty()) {
                      onTextSelect(null);
                      return;
                    }
                    const text = modifiedEditor.getModel()?.getValueInRange(sel);
                    if (!text?.trim()) {
                      onTextSelect(null);
                      return;
                    }
                    const pos = modifiedEditor.getScrolledVisiblePosition(sel.getStartPosition());
                    const editorDom = modifiedEditor.getDomNode();
                    const editorRect = editorDom?.getBoundingClientRect();
                    const rect = (pos && editorRect) ? {
                      left: editorRect.left + pos.left,
                      top: editorRect.top + pos.top,
                      width: 0,
                      height: pos.height || 18,
                    } : null;
                    onTextSelect({ text, startLine: sel.startLineNumber, endLine: sel.endLineNumber, rect });
                  }),
                );
              }
            }}
            options={{ ...EDITOR_OPTIONS, readOnly, renderSideBySide: true }}
          />
        </div>
      )}
    </div>
  );
}
