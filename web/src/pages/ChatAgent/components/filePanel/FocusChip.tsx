import React from 'react';
import { X } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import type { TFunction } from 'i18next';
import type { FocusChipState } from './useFileFocus';

function chipLabel(state: FocusChipState, t: TFunction): string {
  if (state.kind === 'line') {
    if (state.beyond) return t('filePanel.focusLineBeyond', { line: state.line });
    if (state.unplaced) return t('filePanel.focusLineUnplaced', { line: state.line });
    if (state.missing) return t('filePanel.focusLineMissing', { line: state.line });
    if (state.near) return t('filePanel.focusLineNear', { line: state.line });
    return state.lineEnd
      ? t('filePanel.focusLines', { start: state.line, end: state.lineEnd })
      : t('filePanel.focusLine', { line: state.line });
  }
  if (state.kind === 'page') {
    return state.missing ? t('filePanel.focusPageMissing', { page: state.page }) : t('filePanel.focusPage', { page: state.page });
  }
  return state.missing ? t('filePanel.focusSectionMissing', { anchor: state.anchor }) : state.title || `#${state.anchor}`;
}

interface FocusChipProps {
  state: FocusChipState;
  onJump: () => void;
  onDismiss: () => void;
}

/** Names the spot a reference pointed at; the label scrolls back to it, the X clears it. */
export function FocusChip({ state, onJump, onDismiss }: FocusChipProps): React.ReactElement {
  const { t } = useTranslation();
  const label = chipLabel(state, t);
  return (
    <span className="file-focus-chip">
      {state.missing ? (
        <span className="file-focus-chip-label is-missing" title={label}>
          <span className="truncate">{label}</span>
        </span>
      ) : (
        <button type="button" className="file-focus-chip-label" onClick={onJump} title={t('filePanel.focusJump')}>
          <span className="file-focus-chip-dot" aria-hidden="true" />
          <span className="truncate">{label}</span>
        </button>
      )}
      <button
        type="button"
        className="file-focus-chip-close"
        onClick={onDismiss}
        aria-label={t('filePanel.focusDismiss')}
        title={t('filePanel.focusDismiss')}
      >
        <X className="h-3 w-3" />
      </button>
    </span>
  );
}
