/** What the add and edit forms hold while open; the manager owns it. */
export interface SecretDraft {
  name: string;
  value: string;
  description: string;
  /** Plaintext toggle on the value field. */
  valueVisible: boolean;
}

export const EMPTY_DRAFT: SecretDraft = {
  name: '',
  value: '',
  description: '',
  valueVisible: false,
};

/** A refusal, placed under the field it is about; `form` sits above the buttons. */
export interface FormError {
  field: 'name' | 'value' | 'description' | 'form';
  message: string;
  /** A name refusal because that key is already stored: offers its edit form. */
  existing?: string;
}

/** How a form that opened from somewhere else brings itself on screen. */
export type Landing = 'center' | 'nearest';
