import { useState } from 'react';

/**
 * `value` while there is one, else the last there was: what something that
 * fades out on going still draws while it fades. `same` tells a new value
 * from the one kept, and must hold for a value built afresh each render, or
 * keeping it would render again forever.
 */
export function useKeptWhileFading<T>(value: T | null, same: (a: T, b: T) => boolean = Object.is): T | null {
  const [kept, setKept] = useState(value);
  if (value !== null && (kept === null || !same(kept, value))) setKept(value);
  return value ?? kept;
}

/**
 * The same for things that take one another's place, each fading where it
 * was while the next comes in: every one shown, as it was when it came,
 * until `gone` lets it go once its exit has played, `current` last. `key`
 * tells them apart, so one built afresh each render is still the one kept.
 */
export function useEachKeptWhileFading<T extends { key: string }>(current: T | null): {
  shown: T[];
  gone: (key: string) => void;
} {
  const [kept, setKept] = useState<T[]>(() => (current ? [current] : []));
  if (current && kept.at(-1)?.key !== current.key) setKept([...kept.filter((k) => k.key !== current.key), current]);
  return {
    shown: current ? [...kept.filter((k) => k.key !== current.key), current] : kept,
    gone: (key) => setKept((prev) => prev.filter((k) => k.key !== key)),
  };
}
