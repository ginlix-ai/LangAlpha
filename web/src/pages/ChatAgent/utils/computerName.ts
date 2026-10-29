/**
 * A computer's name says which of the user's computers it means, so the
 * server refuses a second one, compared trimmed and case-folded, and numbers
 * one created unnamed past the rest.
 */
import type { useTranslation } from 'react-i18next';

import { apiErrorCoded } from '@/api/errors';

type Translate = ReturnType<typeof useTranslation>['t'];

/** The server's column width for a computer's name. */
export const COMPUTER_NAME_MAX = 255;

/**
 * The name to suggest for a computer created unnamed under `base`: `base`
 * itself, or the first free "base 2", "base 3" past `taken`, compared as the
 * server's index compares names: trimmed, each run of spaces read as one, and
 * lower-cased. It is only what the field shows: a name the user did not type
 * is never sent, and the server picks its own.
 */
export function nextComputerName(base: string, taken: readonly (string | null | undefined)[]): string {
  const fold = (name: string) => name.trim().split(/\s+/).join(' ').toLowerCase();
  const stem = base.trim().slice(0, COMPUTER_NAME_MAX);
  const used = new Set(taken.flatMap((name) => (name ? [fold(name)] : [])));
  if (!used.has(fold(stem))) return stem;
  for (let number = 2; ; number += 1) {
    const suffix = ` ${number}`;
    const numbered = `${stem.slice(0, COMPUTER_NAME_MAX - suffix.length).trimEnd()}${suffix}`;
    if (!used.has(fold(numbered))) return numbered;
  }
}

/**
 * The holder's own spelling of a name another of the user's computers already
 * has, from a 409 `name_taken`, or null when `err` is any other failure.
 */
export function nameTakenBy(err: unknown): string | null {
  const name = apiErrorCoded(err, 'name_taken')?.name;
  return typeof name === 'string' ? name : null;
}

/** Copy for a name another of the user's computers has, or null for any other failure. */
export function computerNameTakenMessage(err: unknown, t: Translate): string | null {
  const name = nameTakenBy(err);
  if (name === null) return null;
  return t('computer.nameTaken', 'You already have a computer named "{{name}}". Choose another name.', { name });
}
