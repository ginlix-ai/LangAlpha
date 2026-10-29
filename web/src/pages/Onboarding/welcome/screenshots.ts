import type { ReadyAttachment } from '@/components/ui/chat-input.types';

/**
 * Screenshots the welcome flow carries into the agent's first message.
 *
 * They travel as base64 data URLs in router navigation state, which lives in
 * the history entry and is copied whenever it is read, so a phone's full-size
 * capture is far too heavy to pass along as it is. Each image is decoded,
 * shrunk to at most 2000px on its long side and re-encoded as JPEG, which keeps
 * a brokerage screen legible while cutting it to a fraction of its size. An
 * image that is already small and would only grow on re-encoding (a crisp
 * PNG of a table, say) is kept as the file it came in.
 */

export interface Screenshot {
  id: string;
  name: string;
  dataUrl: string;
  /** Length of the encoded data, roughly what the screenshot costs to carry. */
  bytes: number;
}

export const MAX_SCREENSHOTS = 5;

/** `not-image` for a type we do not take (HEIC included, since browsers
 *  cannot draw it); `unreadable` for a file that claims an image type but will
 *  not decode. */
export type ScreenshotFailure = 'not-image' | 'unreadable';

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

function toDataUrl(blob: Blob): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result));
    reader.onerror = () => reject(reader.error);
    reader.readAsDataURL(blob);
  });
}

export async function prepareScreenshot(file: File): Promise<Screenshot> {
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
    let dataUrl = canvas.toDataURL('image/jpeg', JPEG_QUALITY);
    if (
      dataUrl.length > file.size &&
      scale === 1 &&
      file.size <= KEEP_ORIGINAL_BYTES
    ) {
      dataUrl = await toDataUrl(file);
    }
    return { id: crypto.randomUUID(), name: file.name, dataUrl, bytes: dataUrl.length };
  } catch (error) {
    if (error instanceof ScreenshotError) throw error;
    throw new ScreenshotError('unreadable');
  } finally {
    decoded.release();
  }
}

/**
 * A screenshot as the composer holds a file it attached, so it goes out with
 * the composer's own files and the same way. Its bytes are the data URL's, read
 * back into a file once, at the hand-off.
 */
export function screenshotAttachment(shot: Screenshot): ReadyAttachment {
  const comma = shot.dataUrl.indexOf(',');
  const type = /^data:([^;,]+)/.exec(shot.dataUrl)?.[1] ?? 'image/jpeg';
  const binary = atob(shot.dataUrl.slice(comma + 1));
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return { file: new File([bytes], shot.name, { type }), type, dataUrl: shot.dataUrl, preview: null };
}
