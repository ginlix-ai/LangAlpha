import type * as MonacoApi from 'monaco-editor';
import { registerAuthReset } from '@/lib/authResets';

// An edit session owns one Monaco model, named by the path minted here. The
// model outlives the editor that shows it, which is what keeps a tab's undo
// history across a tab switch, and the model's scroll and selection wait here
// between mounts. The session drops both when it ends. The editor loads Monaco
// lazily; before it has, no model exists.
let monaco: typeof MonacoApi | null = null;
let seq = 0;
// Keyed by the model each state was read from. The editor library would keep
// its own copy in a module map that nothing can empty, so CodeEditor turns its
// copy off. This one empties with the rest of the account's state on sign-out.
const viewStates = new Map<string, MonacoApi.editor.ICodeEditorViewState>();
registerAuthReset(() => viewStates.clear());

export function rememberMonaco(instance: typeof MonacoApi): void {
  if (monaco === instance) return;
  monaco = instance;
  // Restored as the model attaches, inside editor.create while the editor is
  // still hidden. Restored any later, the top of the file paints for a frame
  // before the jump.
  instance.editor.onDidCreateEditor((editor) => {
    editor.onDidChangeModel(({ newModelUrl }) => {
      const state = newModelUrl && viewStates.get(newModelUrl.toString());
      if (state) editor.restoreViewState(state);
    });
  });
}

/** A model path no earlier session used, so no stale view state is restored onto it. */
export function newEditorModelPath(file: string): string {
  seq += 1;
  return `inmemory://edit/${seq}/${encodeURIComponent(file)}`;
}

export function saveViewState(editor: MonacoApi.editor.IStandaloneCodeEditor): void {
  const model = editor.getModel();
  const state = editor.saveViewState();
  if (model && state) viewStates.set(model.uri.toString(), state);
}

export function disposeEditorModel(path: string): void {
  if (!monaco) return;
  const uri = monaco.Uri.parse(path);
  viewStates.delete(uri.toString());
  monaco.editor.getModel(uri)?.dispose();
}
