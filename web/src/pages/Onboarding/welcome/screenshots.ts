import type { ReadyAttachment } from '@/components/ui/chat-input.types';
import {
  discardAttachment,
  RESTAGE_AFTER_MS,
  RESTAGE_RETRY_MS,
  stageAttachment,
  STAGED_FOR_MS,
} from '@/pages/ChatAgent/utils/attachmentUpload';

/**
 * Screenshots the welcome flow carries into the agent's first message.
 *
 * Each is staged on the server as a chat attachment as soon as it is added,
 * which needs no computer, so it goes with the first message even though a
 * new user's Home has none yet. A phone's full-size capture is far more than
 * the agent needs to read a screen, so each image is first decoded, shrunk to
 * at most 2000px on its long side and re-encoded as JPEG, which keeps a
 * brokerage screen legible while cutting it to a fraction of its size. An
 * image that is already small and would only grow on re-encoding (a crisp
 * PNG of a table, say) is kept as the file it came in.
 */

export interface Screenshot {
  id: string;
  /** The image as prepared, which is what goes up and what the tile shows. */
  file: File;
  /** The staged upload the first message names it by. */
  attachmentId: string;
  /** When that upload became ready, which the server's expiry counts from. */
  readyAt?: number;
}

export const MAX_SCREENSHOTS = 5;

/** `not-image` for a type we do not take (HEIC included, since browsers
 *  cannot draw it); `unreadable` for a file that claims an image type but will
 *  not decode; `upload-failed` for one the server could not take. */
export type ScreenshotFailure = 'not-image' | 'unreadable' | 'upload-failed';

export class ScreenshotError extends Error {
  readonly reason: ScreenshotFailure;
  constructor(reason: ScreenshotFailure) {
    super(reason);
    this.name = 'ScreenshotError';
    this.reason = reason;
  }
}

export const ACCEPTED_TYPES = ['image/png', 'image/jpeg', 'image/webp', 'image/gif'] as const;

const MAX_SIDE = 2000;
const KEEP_ORIGINAL_BYTES = 1.5 * 1024 * 1024;
const JPEG_QUALITY = 0.88;

interface Decoded {
  source: CanvasImageSource;
  width: number;
  height: number;
  release: () => void;
}

async function decode(file: File): Promise<Decoded> {
  if (typeof createImageBitmap === 'function') {
    const bitmap = await createImageBitmap(file);
    return { source: bitmap, width: bitmap.width, height: bitmap.height, release: () => bitmap.close() };
  }
  const url = URL.createObjectURL(file);
  try {
    const img = new Image();
    img.src = url;
    await img.decode();
    return {
      source: img,
      width: img.naturalWidth,
      height: img.naturalHeight,
      release: () => URL.revokeObjectURL(url),
    };
  } catch (error) {
    URL.revokeObjectURL(url);
    throw error;
  }
}

function jpeg(canvas: HTMLCanvasElement): Promise<Blob> {
  return new Promise((resolve, reject) => {
    canvas.toBlob((blob) => (blob ? resolve(blob) : reject(new ScreenshotError('unreadable'))), 'image/jpeg', JPEG_QUALITY);
  });
}

/** Shrinks and encodes the image, then stages it for the first message. */
export async function prepareScreenshot(file: File): Promise<Screenshot> {
  const encoded = await encodeScreenshot(file);
  const prepared = new File([encoded], file.name, { type: encoded.type });
  let attachmentId: string;
  try {
    attachmentId = await stageAttachment(prepared);
  } catch {
    throw new ScreenshotError('upload-failed');
  }
  return { id: crypto.randomUUID(), file: prepared, attachmentId, readyAt: Date.now() };
}

async function encodeScreenshot(file: File): Promise<Blob> {
  if (!(ACCEPTED_TYPES as readonly string[]).includes(file.type)) {
    throw new ScreenshotError('not-image');
  }
  let decoded: Decoded;
  try {
    decoded = await decode(file);
  } catch {
    throw new ScreenshotError('unreadable');
  }
  try {
    const { width, height } = decoded;
    if (!width || !height) throw new ScreenshotError('unreadable');
    const scale = Math.min(1, MAX_SIDE / Math.max(width, height));
    const w = Math.max(1, Math.round(width * scale));
    const h = Math.max(1, Math.round(height * scale));
    const canvas = document.createElement('canvas');
    canvas.width = w;
    canvas.height = h;
    const ctx = canvas.getContext('2d');
    if (!ctx) throw new ScreenshotError('unreadable');
    // JPEG has no alpha: left alone, transparent pixels would turn black.
    ctx.fillStyle = '#fff';
    ctx.fillRect(0, 0, w, h);
    ctx.drawImage(decoded.source, 0, 0, w, h);
    const encoded = await jpeg(canvas);
    return encoded.size > file.size && scale === 1 && file.size <= KEEP_ORIGINAL_BYTES ? file : encoded;
  } catch (error) {
    if (error instanceof ScreenshotError) throw error;
    throw new ScreenshotError('unreadable');
  } finally {
    decoded.release();
  }
}

/** A screenshot as the composer holds a file it attached, so it goes out with the composer's own files and the same way. */
export function screenshotAttachment(shot: Screenshot): ReadyAttachment {
  return {
    file: shot.file,
    type: shot.file.type,
    attachmentId: shot.attachmentId,
    ...(shot.readyAt !== undefined ? { readyAt: shot.readyAt } : {}),
  };
}

/**
 * Keeps the flow's screenshots staged while the flow stays open, as the
 * composer keeps its files: each stages again, unseen, before the server lets
 * it go, and the old copy is forgotten once the new one is ready. A tab asleep
 * past that point wakes with ids the server may have dropped, so Start asks
 * `freshen` to stage those again and waits for them rather than send them.
 */
export class ScreenshotStaging {
  private readonly held = new Map<string, {
    readyAt: number;
    timer?: ReturnType<typeof setTimeout>;
    going?: { ctrl: AbortController; done: Promise<void> };
  }>();

  constructor(
    /** The list as it stands. */
    private readonly shots: () => readonly Screenshot[],
    /** Puts a shot's new upload in the list. */
    private readonly replace: (id: string, attachmentId: string, readyAt: number) => void,
  ) {}

  /** Follows `shots`: a timer for each new one, nothing left for one gone. */
  track(shots: readonly Screenshot[]): void {
    const ids = new Set(shots.map((s) => s.id));
    for (const id of [...this.held.keys()]) if (!ids.has(id)) this.drop(id);
    for (const shot of shots) {
      if (this.held.has(shot.id)) continue;
      this.held.set(shot.id, { readyAt: shot.readyAt ?? Date.now() });
      this.schedule(shot.id);
    }
  }

  /**
   * Stages again now each shot past the point, and settles once they are; one
   * that fails keeps its old id. False when such an id is one the server has
   * already let go, which a send would name as not received.
   */
  freshen(): Promise<boolean> {
    const stale = this.shots().filter((s) => {
      const entry = this.held.get(s.id);
      return !!entry && Date.now() - entry.readyAt >= RESTAGE_AFTER_MS;
    });
    return Promise.all(stale.map((s) => this.restage(s.id))).then(() =>
      stale.every((s) => {
        const entry = this.held.get(s.id);
        return !entry || Date.now() < entry.readyAt + STAGED_FOR_MS;
      }));
  }

  /** Ends every timer and staging under way, as the flow goes. */
  close(): void {
    for (const id of [...this.held.keys()]) this.drop(id);
  }

  private drop(id: string): void {
    const entry = this.held.get(id);
    if (!entry) return;
    clearTimeout(entry.timer);
    entry.going?.ctrl.abort();
    this.held.delete(id);
  }

  private schedule(id: string, wait?: number): void {
    const entry = this.held.get(id);
    if (!entry) return;
    clearTimeout(entry.timer);
    const due = wait ?? entry.readyAt + RESTAGE_AFTER_MS - Date.now();
    entry.timer = setTimeout(() => void this.restage(id), Math.max(0, due));
  }

  private restage(id: string): Promise<void> {
    const entry = this.held.get(id);
    const shot = this.shots().find((s) => s.id === id);
    if (!entry || !shot) return Promise.resolve();
    if (entry.going) return entry.going.done;
    clearTimeout(entry.timer);
    const old = shot.attachmentId;
    const ctrl = new AbortController();
    const done = (async () => {
      try {
        const fresh = await stageAttachment(shot.file, { signal: ctrl.signal });
        // Removed, or the flow gone, while it went: the copy is not needed.
        if (this.held.get(id) !== entry || this.shots().find((s) => s.id === id)?.attachmentId !== old) {
          discardAttachment(fresh);
          return;
        }
        entry.readyAt = Date.now();
        this.replace(id, fresh, entry.readyAt);
        discardAttachment(old);
        this.schedule(id);
      } catch {
        if (ctrl.signal.aborted) return;
        // Tried again while there is time; past it, Start tries once more.
        if (Date.now() + RESTAGE_RETRY_MS < entry.readyAt + STAGED_FOR_MS) this.schedule(id, RESTAGE_RETRY_MS);
      } finally {
        if (entry.going?.ctrl === ctrl) entry.going = undefined;
      }
    })();
    entry.going = { ctrl, done };
    return done;
  }
}
