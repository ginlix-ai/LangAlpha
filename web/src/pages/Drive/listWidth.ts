import { useState } from 'react';

const STORAGE_KEY = 'drive:list-width';
export const LIST_WIDTH_DEFAULT = 320;
export const LIST_WIDTH_MIN = 240;
export const LIST_WIDTH_MAX = 560;
export const KEY_STEP = 16;

function clampWidth(width: number): number {
  return Math.round(Math.min(LIST_WIDTH_MAX, Math.max(LIST_WIDTH_MIN, width)));
}

function readWidth(): number {
  try {
    const raw = Number(localStorage.getItem(STORAGE_KEY));
    return Number.isFinite(raw) && raw > 0 ? clampWidth(raw) : LIST_WIDTH_DEFAULT;
  } catch {
    return LIST_WIDTH_DEFAULT;
  }
}

function keepWidth(width: number) {
  try {
    if (width === LIST_WIDTH_DEFAULT) localStorage.removeItem(STORAGE_KEY);
    else localStorage.setItem(STORAGE_KEY, String(width));
  } catch {
    // Storage refused (private mode, quota): the width holds for this visit.
  }
}

/**
 * The width of the list beside an open file: the person's own, kept between
 * visits, since long names that differ late ("…_v2.xlsx", "…_v3.xlsx") need
 * more room in one Drive than another.
 */
export function useListWidth() {
  const [width, setWidth] = useState(readWidth);
  const change = (next: number, commit = false) => {
    const clamped = clampWidth(next);
    setWidth(clamped);
    if (commit) keepWidth(clamped);
  };
  return { width, change };
}
