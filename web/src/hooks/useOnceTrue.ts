import { useState } from 'react';

/**
 * True from the first render `value` is, for as long as the component lives.
 *
 * For what should not vanish under the user because of what they did in it:
 * a dialog that animates out after it closes (`MountOnceOpen`), or the
 * first-run catalog block, which installs into the very list whose emptiness
 * put it there.
 */
export function useOnceTrue(value: boolean): boolean {
  const [seen, setSeen] = useState(value);
  if (value && !seen) setSeen(true);
  return value || seen;
}
