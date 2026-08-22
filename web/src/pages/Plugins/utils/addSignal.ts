/**
 * The page-header Add menu's message to the tab bodies. The page switches to
 * the right tab and emits one of these; the target list opens its own modal.
 * `nonce` makes repeat picks of the same action observable to an effect.
 */
export type AddAction =
  | 'install-plugin'
  | 'add-server'
  | 'import-servers'
  | 'upload-skill';

export interface AddSignal {
  action: AddAction;
  nonce: number;
}
