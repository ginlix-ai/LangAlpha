import React, { useEffect, useRef, useState } from 'react';
import { CornerDownLeft, Search } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { isImeComposing } from '@/lib/ime';
import { cn } from '@/lib/utils';
import type { BrowserStartState } from '../filePanel/browserPhase';
import { visitFor } from '../filePanel/browserRefusal';
import type { BrowserViewSession } from '../filePanel/browserStream';
import { useBrowserViewState } from '../filePanel/useBrowserView';
import { isHandBackKey } from './browserInput';

const STATUS: Record<BrowserStartState, [key: string, fallback: string]> = {
  open: ['filePanel:browser.startStatusOpen', 'Open'],
  idle: ['filePanel:browser.startStatusIdle', 'Not open'],
  stopped: ['filePanel:browser.startStatusStopped', 'Computer stopped'],
};

const NOTE: Record<BrowserStartState, [key: string, fallback: string]> = {
  open: ['filePanel:browser.startNoteOpen', 'Going to a page takes control from the agent. Shift+Esc hands it back.'],
  idle: ['filePanel:browser.startNoteIdle', 'Opens the browser and takes control from the agent. Shift+Esc hands it back.'],
  stopped: [
    'filePanel:browser.startNoteStopped',
    'Starts this computer, opens its browser and takes control from the agent. Shift+Esc hands it back.',
  ],
};

/**
 * What the Browser tab shows where there is no page. One field, as a browser's
 * new tab has, under a line saying where the browser is. Going somewhere from it
 * is the person taking the browser, so it starts the computer and opens the
 * browser where they are not up and takes control on the way, with no separate
 * step to ask for any of them. The tab keeps it up while that visit is on its
 * way, and what the visit goes to is the session's, so it outlasts a remount.
 */
export function BrowserStartPage({
  view,
  state,
  computerName,
}: {
  view: BrowserViewSession;
  state: BrowserStartState;
  /** The computer the browser runs on; null where the reader is not shown it. */
  computerName: string | null;
}): React.ReactElement {
  const { t } = useTranslation();
  const { visiting } = useBrowserViewState(view);
  const going = visiting !== null;
  // A refused visit leaves what was typed to be fixed.
  const [draft, setDraft] = useState(() => visiting ?? '');
  const field = useRef<HTMLInputElement>(null);

  // Ready to type into, unless the person is already typing somewhere else.
  useEffect(() => {
    const active = document.activeElement;
    if (!active || active === document.body) field.current?.focus({ preventScroll: true });
  }, []);

  const go = async () => {
    const typed = draft.trim();
    if (!typed || going) return;
    if (await visitFor(t, view, typed)) setDraft('');
  };

  const placeholder = t('filePanel:browser.startPlaceholder', 'Search or enter an address');
  const [statusKey, statusFallback] = STATUS[state];
  const [noteKey, noteFallback] = NOTE[state];
  return (
    <div className="browser-start">
      <div className="browser-start-status">
        <span className={cn('browser-start-dot', state === 'open' && 'is-open')} aria-hidden="true" />
        <span>{t('filePanel:browser.startStatusLabel', 'Browser')}</span>
        {computerName && (
          <>
            <span aria-hidden="true">·</span>
            <span className="browser-start-computer">{computerName}</span>
          </>
        )}
        <span aria-hidden="true">·</span>
        <span>{t(statusKey, statusFallback)}</span>
      </div>
      <div className="browser-start-box">
        {going
          ? <Loader size={14} className="shrink-0 text-current" />
          : <Search className="h-4 w-4 shrink-0" aria-hidden="true" />}
        <input
          ref={field}
          type="text"
          inputMode="url"
          className="browser-start-input"
          aria-label={placeholder}
          placeholder={placeholder}
          value={draft}
          readOnly={going}
          autoCapitalize="off"
          autoComplete="off"
          autoCorrect="off"
          spellCheck={false}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => {
            e.stopPropagation();
            // Keys belong to an input method while it composes.
            if (isImeComposing(e)) return;
            if (e.key === 'Enter') {
              e.preventDefault();
              void go();
            } else if (isHandBackKey(e)) {
              // The note's promise holds here too: the person may hold a blank page.
              e.preventDefault();
              view.releaseControl();
            }
          }}
        />
        <kbd className="browser-start-key" aria-hidden="true">
          <CornerDownLeft className="h-3 w-3" />
        </kbd>
      </div>
      <p className="browser-start-note">{t(noteKey, noteFallback)}</p>
    </div>
  );
}
