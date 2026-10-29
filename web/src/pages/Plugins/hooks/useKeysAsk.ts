import { useState } from 'react';
import { nextKeysAsk, type InstallError, type KeysAsk } from '../utils/installRefusal';

export interface KeysAskState<T> {
  /** The form, for `target` (what its submit retries); null while closed. */
  open: { target: T; ask: KeysAsk } | null;
  values: Record<string, string>;
  setValues: (values: Record<string, string>) => void;
  /** Say `error` on the form, opening it when the error asks for keys. False
   *  when it is not the form's to say. */
  answer: (target: T, error: InstallError) => boolean;
  close: () => void;
}

/**
 * The keys form a catalog install or an update opens over the page. The
 * values typed so far outlive a refused try, so a retry changes one field
 * rather than all of them.
 */
export function useKeysAsk<T>(): KeysAskState<T> {
  const [open, setOpen] = useState<{ target: T; ask: KeysAsk } | null>(null);
  const [values, setValues] = useState<Record<string, string>>({});
  return {
    open,
    values,
    setValues,
    answer(target, error) {
      const ask = nextKeysAsk(open?.target === target ? open.ask : null, error);
      if (ask) setOpen({ target, ask });
      return ask !== null;
    },
    close() {
      setOpen(null);
      setValues({});
    },
  };
}
