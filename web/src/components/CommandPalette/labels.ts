import type { Kind, Lens, Place, ViewSpec } from './types';

type T = (key: string, options?: Record<string, unknown>) => string;

/** A kind as the field names it, in its chip or at its end. */
export function kindLabel(kind: Kind, t: T): string {
  return t(`palette.scope.kinds.${kind}`);
}

/** A drilled-in view, named at the field's end: its kind, or the ticker it is about. */
export function lensLabel(lens: Lens, t: T): string {
  return lens.kind === 'ticker' ? lens.symbol : kindLabel(lens.kind, t);
}

const placeName = (place: Place) => (place.kind === 'workspace' ? place.name : place.title);

/** What the field's chips say: the place searched within, then the kind its prefix narrows it to. */
export function scopeChips(scope: ViewSpec, t: T): string[] {
  const kind = scope.lens?.via === 'prefix' ? scope.lens.kind : null;
  return [...(scope.place ? [placeName(scope.place)] : []), ...(kind ? [kindLabel(kind, t)] : [])];
}

/** The field's name for a screen reader, which the chips' words are hidden from. */
export function fieldLabel(chips: readonly string[], t: T): string {
  if (chips.length === 0) return t('palette.label');
  if (chips.length === 2) return t('palette.scope.kindIn', { kind: chips[1], place: chips[0] });
  return t('palette.scope.label', { scope: chips[0] });
}

export function placeholder({ place, lens }: ViewSpec, t: T): string {
  if (lens?.kind === 'ticker') return t('palette.narrowPlaceholder', { symbol: lens.symbol });
  if (lens) return t(`palette.scope.placeholders.${lens.kind}`);
  if (place?.kind === 'workspace') return t('palette.scope.workspacePlaceholder');
  if (place?.kind === 'thread') return t('palette.scope.threadPlaceholder');
  return t('palette.placeholder');
}
