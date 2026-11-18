/**
 * Asks the composer in view to open its model picker, for a refusal that says
 * "Choose a model" from outside the composer (the banner above it, a failed
 * turn's card). An event rather than a prop chain: the row and the picker sit
 * in different subtrees, and only a composer that is on screen answers.
 */
const EVENT = 'la:open-model-picker';

export function requestModelPicker(): void {
  window.dispatchEvent(new Event(EVENT));
}

export function onModelPickerRequest(handler: () => void): () => void {
  window.addEventListener(EVENT, handler);
  return () => window.removeEventListener(EVENT, handler);
}
