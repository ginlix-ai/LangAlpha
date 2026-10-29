/**
 * Where a reader adds screenshots of their positions and watchlist, so the
 * agent can read them in its first message instead of being typed at.
 *
 * One zone takes all three routes in: a click opens the file picker, a drop
 * anywhere on it adds, and a paste anywhere on the page adds too (a screenshot
 * is usually on the clipboard already). The zone is a real button, so Tab,
 * Enter and Space work with no extra wiring. Files are prepared in parallel and
 * each shows a loader tile until it is ready. The component is controlled and
 * keeps nothing but what is still being prepared; the list belongs to the
 * flow. Adding past the cap keeps the first files that fit and says so, and a
 * file that cannot be used is named in a status line that the next add clears.
 */
import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type DragEvent,
  type Dispatch,
  type SetStateAction,
} from 'react';
import { useTranslation } from 'react-i18next';
import { ImageUp, Plus, X } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { cn } from '@/lib/utils';
import {
  ACCEPTED_TYPES,
  MAX_SCREENSHOTS,
  ScreenshotError,
  prepareScreenshot,
  type Screenshot,
} from '../screenshots';

interface ScreenshotDropProps {
  shots: Screenshot[];
  /** The flow's setter: adds finish out of step with renders, so they are
   *  applied to the list as it stands then, not to the one a render closed over. */
  onChange: Dispatch<SetStateAction<Screenshot[]>>;
  /** Handed each add as it starts, settling once its files are in the list:
   *  the flow can be started from a later step while one is still being read,
   *  and waits for it there. */
  onPreparing?: (work: Promise<void>) => void;
  /** False while the zone is kept on the page unseen: a paste is then not
   *  for it. */
  active?: boolean;
}

const FOCUS =
  'focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-(--color-focus-ring)';

const TILE =
  'relative aspect-square overflow-hidden rounded-lg border border-(--color-border-muted) bg-(--color-bg-card)';

/** A paste into a field is the field's own; only a paste with nowhere to type
 *  is taken as a screenshot. */
function isTextTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  return (
    target instanceof HTMLTextAreaElement ||
    (target instanceof HTMLInputElement && target.type !== 'file') ||
    target.isContentEditable
  );
}

export function ScreenshotDrop({ shots, onChange, onPreparing, active = true }: ScreenshotDropProps) {
  const { t } = useTranslation();
  const inputRef = useRef<HTMLInputElement>(null);
  const [pending, setPending] = useState(0);
  const [messages, setMessages] = useState<string[]>([]);
  const [dragging, setDragging] = useState(false);

  const pendingRef = useRef(0);
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const add = useCallback(
    async (files: File[]) => {
      if (files.length === 0) return;
      const room = Math.max(0, MAX_SCREENSHOTS - shots.length - pendingRef.current);
      const taken = files.slice(0, room);
      const notes: string[] = [];
      if (taken.length < files.length) {
        notes.push(t('onboarding.welcome.shots.tooMany', { max: MAX_SCREENSHOTS }));
      }
      setMessages(notes);
      if (taken.length === 0) return;

      pendingRef.current += taken.length;
      setPending(pendingRef.current);
      const results = await Promise.allSettled(taken.map((file) => prepareScreenshot(file)));
      pendingRef.current -= taken.length;

      const ready: Screenshot[] = [];
      results.forEach((result, i) => {
        if (result.status === 'fulfilled') {
          ready.push(result.value);
          return;
        }
        const reason = result.reason instanceof ScreenshotError ? result.reason.reason : 'unreadable';
        notes.push(
          t(
            reason === 'not-image'
              ? 'onboarding.welcome.shots.notImage'
              : 'onboarding.welcome.shots.unreadable',
            { name: taken[i].name },
          ),
        );
      });
      // The list is the flow's, so a file still being read when the user
      // moved on (Continue, mid-decode) joins it all the same; only the
      // zone's own loader and status line go with the zone.
      if (ready.length > 0) {
        onChange((prev) => [...prev, ...ready].slice(0, MAX_SCREENSHOTS));
      }
      if (!mounted.current) return;
      setPending(pendingRef.current);
      setMessages([...notes]);
    },
    [onChange, shots.length, t],
  );

  const take = useCallback(
    (files: File[]) => {
      const work = add(files);
      onPreparing?.(work);
    },
    [add, onPreparing],
  );

  useEffect(() => {
    if (!active) return;
    const onPaste = (event: ClipboardEvent) => {
      if (isTextTarget(event.target)) return;
      const files = Array.from(event.clipboardData?.files ?? []).filter((f) =>
        f.type.startsWith('image/'),
      );
      if (files.length === 0) return;
      event.preventDefault();
      take(files);
    };
    window.addEventListener('paste', onPaste);
    return () => window.removeEventListener('paste', onPaste);
  }, [take, active]);

  // A file let go anywhere but the zone is otherwise the browser's, and the
  // browser opens it in place of the flow. Whatever already claimed the drag
  // (the zone, or a field with its own drop) has cancelled it by the time it
  // reaches the window; the rest is refused, so the cursor says "not here".
  useEffect(() => {
    if (!active) return;
    const onWindowDragOver = (event: globalThis.DragEvent) => {
      if (event.defaultPrevented || !event.dataTransfer?.types.includes('Files')) return;
      event.preventDefault();
      event.dataTransfer.dropEffect = 'none';
    };
    const onWindowDrop = (event: globalThis.DragEvent) => {
      if (!event.defaultPrevented) event.preventDefault();
    };
    window.addEventListener('dragover', onWindowDragOver);
    window.addEventListener('drop', onWindowDrop);
    return () => {
      window.removeEventListener('dragover', onWindowDragOver);
      window.removeEventListener('drop', onWindowDrop);
    };
  }, [active]);

  const remove = (id: string) => {
    onChange((prev) => prev.filter((s) => s.id !== id));
  };

  const onDragOver = (event: DragEvent) => {
    if (!event.dataTransfer?.types.includes('Files')) return;
    event.preventDefault();
    setDragging(true);
  };
  const onDragLeave = (event: DragEvent) => {
    // Moving over a child fires a leave on the zone; only a real exit counts.
    if (event.currentTarget.contains(event.relatedTarget as Node | null)) return;
    setDragging(false);
  };
  const onDrop = (event: DragEvent) => {
    event.preventDefault();
    setDragging(false);
    take(Array.from(event.dataTransfer?.files ?? []));
  };

  const openPicker = () => inputRef.current?.click();
  const empty = shots.length === 0 && pending === 0;
  const canAdd = shots.length + pending < MAX_SCREENSHOTS;
  const addLabel = t('onboarding.welcome.shots.add');

  return (
    <div onDragOver={onDragOver} onDragLeave={onDragLeave} onDrop={onDrop}>
      <input
        ref={inputRef}
        type="file"
        accept={ACCEPTED_TYPES.join(',')}
        multiple
        hidden
        tabIndex={-1}
        onChange={(e) => {
          take(Array.from(e.target.files ?? []));
          e.target.value = '';
        }}
      />
      {empty ? (
        <button
          type="button"
          onClick={openPicker}
          data-dragging={dragging}
          className={cn(
            'flex w-full flex-col items-center justify-center gap-1 rounded-lg border border-dashed px-4 py-10 text-center press',
            dragging
              ? 'border-(--color-text-primary) bg-(--color-bg-hover)'
              : 'border-(--color-border-default) bg-(--color-bg-card) hover:bg-(--color-bg-hover)',
            FOCUS,
          )}
        >
          <ImageUp aria-hidden="true" className="mb-1 h-6 w-6 text-(--color-text-tertiary)" />
          <span className="text-sm font-medium text-(--color-text-primary)">
            {t('onboarding.welcome.shots.prompt')}
          </span>
          <span className="text-[0.8125rem] text-(--color-text-tertiary)">
            {t('onboarding.welcome.shots.hint', { max: MAX_SCREENSHOTS })}
          </span>
        </button>
      ) : (
        <ul
          data-dragging={dragging}
          className={cn(
            'grid grid-cols-3 gap-2 rounded-lg sm:grid-cols-5',
            dragging && 'outline-2 outline-dashed outline-offset-4 outline-(--color-text-primary)',
          )}
        >
          {shots.map((shot) => (
            <li key={shot.id} className={cn(TILE, 'wizard-fade-in')}>
              {/* From the top: a phone capture is tall, and its header names the screen. */}
              <img src={shot.dataUrl} alt={shot.name} className="h-full w-full object-cover object-top" />
              <button
                type="button"
                onClick={() => remove(shot.id)}
                aria-label={t('onboarding.welcome.shots.remove', { name: shot.name })}
                className={cn(
                  'absolute right-1 top-1 flex h-6 w-6 items-center justify-center rounded-full border border-(--color-border-default) bg-(--color-bg-card) text-(--color-text-primary) press hover:bg-(--color-bg-hover)',
                  FOCUS,
                )}
              >
                <X aria-hidden="true" className="h-3.5 w-3.5" />
              </button>
            </li>
          ))}
          {Array.from({ length: pending }, (_, i) => (
            <li
              key={`pending-${i}`}
              className={cn(TILE, 'flex items-center justify-center text-(--color-text-tertiary)')}
            >
              <Loader size={18} />
            </li>
          ))}
          {canAdd && (
            <li>
              <button
                type="button"
                onClick={openPicker}
                aria-label={addLabel}
                className={cn(
                  'flex aspect-square w-full items-center justify-center rounded-lg border border-dashed border-(--color-border-default) bg-(--color-bg-card) text-(--color-text-secondary) press hover:bg-(--color-bg-hover) hover:text-(--color-text-primary)',
                  FOCUS,
                )}
              >
                <Plus aria-hidden="true" className="h-5 w-5" />
              </button>
            </li>
          )}
        </ul>
      )}
      <div role="status" className="mt-2 space-y-0.5 text-[0.8125rem] text-(--color-text-secondary)">
        {messages.map((message, i) => (
          <p key={`${i}-${message}`}>{message}</p>
        ))}
      </div>
    </div>
  );
}
