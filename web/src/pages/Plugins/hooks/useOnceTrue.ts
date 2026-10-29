import { useState } from 'react';

/**
 * True from the first render `value` is, for as long as the component lives.
 *
 * For a block that should not vanish under the user because of what they did
 * in it: the first-run catalog block installs into the very list whose
 * emptiness put it there, and dropping it the moment the install lands would
 * take the install's report and sign-in with it.
 */
export function useOnceTrue(value: boolean): boolean {
  const [seen, setSeen] = useState(value);
  if (value && !seen) setSeen(true);
  return value || seen;
}
