/**
 * The picture of each box a comment holds, written into the workspace as the
 * comment is saved so the agent can open it: the box with a margin around
 * it, the box outlined. Written only then, so a box let go leaves no file.
 */
import type { PDFDocumentProxy } from 'pdfjs-dist';
import type { ArtifactCommentAnchor, ArtifactCommentRegionBox } from '@/types/artifacts';
import { uploadWorkspaceFile } from '../../../../utils/api/files';

/** Where pictures go, as the API takes a box's `picture`. */
export const PICTURES_DIR = '.agents/comments';
/** The longest side of a picture, in pixels. */
const MAX_SIDE = 1280;
/** How many pictures a save draws and uploads at once: each is a canvas up to `MAX_SIDE` a side, and a comment may hold 64 boxes. */
const PICTURES_AT_ONCE = 4;
/** The margin around a box, as a share of its size, and at least this share of the surface. */
const MARGIN = 0.25;
const MIN_MARGIN = 0.03;
/** The outline, over a white edge so it reads on dark pictures too; a colour charts seldom use, so it is not taken for content. */
const OUTLINE = '#e0157a';
const OUTLINE_EDGE = '#ffffff';

/** A part of a surface, as fractions of it. */
type Crop = { x: number; y: number; w: number; h: number };

/** What a picture is cut from: a surface of `width` by `height` pixels at its own size, which paints `crop` into a canvas. */
export interface PictureSource {
  /** The surface's size at scale 1: an image's pixels, a PDF page's points. */
  size(box: ArtifactCommentRegionBox): Promise<{ width: number; height: number } | null>;
  /** Paints `crop` of the surface `box` is on over the whole of `canvas`. */
  paint(canvas: HTMLCanvasElement, box: ArtifactCommentRegionBox, crop: Crop): Promise<void>;
  /** How much larger than its own size a crop may be drawn: a PDF draws sharp at any scale, an image does not. */
  maxScale: number;
}

/** The box and its margin, kept on the surface. */
export function cropOf(box: ArtifactCommentRegionBox): Crop {
  const mx = Math.max(box.w * MARGIN, MIN_MARGIN);
  const my = Math.max(box.h * MARGIN, MIN_MARGIN);
  const x = Math.max(0, box.x - mx);
  const y = Math.max(0, box.y - my);
  return { x, y, w: Math.min(1, box.x + box.w + mx) - x, h: Math.min(1, box.y + box.h + my) - y };
}

/** The picture of `box` as a PNG, or null where the surface cannot be read. */
export async function pictureOf(source: PictureSource, box: ArtifactCommentRegionBox): Promise<Blob | null> {
  const size = await source.size(box);
  if (!size?.width || !size.height) return null;
  const crop = cropOf(box);
  const own = { w: crop.w * size.width, h: crop.h * size.height };
  const scale = Math.min(MAX_SIDE / Math.max(own.w, own.h), source.maxScale);
  const canvas = document.createElement('canvas');
  canvas.width = Math.max(1, Math.round(own.w * scale));
  canvas.height = Math.max(1, Math.round(own.h * scale));
  await source.paint(canvas, box, crop);
  const ctx = canvas.getContext('2d');
  if (!ctx) return null;
  const line = Math.max(2, Math.round(Math.max(canvas.width, canvas.height) / 320));
  const at = {
    x: ((box.x - crop.x) / crop.w) * canvas.width,
    y: ((box.y - crop.y) / crop.h) * canvas.height,
    w: (box.w / crop.w) * canvas.width,
    h: (box.h / crop.h) * canvas.height,
  };
  for (const [color, width] of [[OUTLINE_EDGE, line * 2], [OUTLINE, line]] as const) {
    ctx.strokeStyle = color;
    ctx.lineWidth = width;
    ctx.strokeRect(at.x, at.y, at.w, at.h);
  }
  // A canvas read from a picture of another origin throws here; it has none to give.
  try {
    return await new Promise<Blob | null>((resolve) => canvas.toBlob(resolve, 'image/png'));
  } catch {
    return null;
  }
}

/**
 * An image already shown as `src`, whose pixels are read as they are. React
 * keeps the element when the viewer moves to another image, so a save begun
 * on this one takes no picture from the next.
 */
export function imageSource(img: HTMLImageElement, src: string): PictureSource {
  const shown = () => img.getAttribute('src') === src;
  return {
    maxScale: 2,
    size: async () => (shown() && img.complete && img.naturalWidth ? { width: img.naturalWidth, height: img.naturalHeight } : null),
    async paint(canvas, _box, crop) {
      if (!shown()) throw new Error('the image shown changed');
      const w = img.naturalWidth;
      const h = img.naturalHeight;
      canvas.getContext('2d')?.drawImage(img, crop.x * w, crop.y * h, crop.w * w, crop.h * h, 0, 0, canvas.width, canvas.height);
    },
  };
}

/** A PDF's pages, drawn afresh for the crop alone, sharp enough to read small print at any zoom. */
export function pdfSource(pdf: PDFDocumentProxy): PictureSource {
  return {
    maxScale: 4,
    async size(box) {
      if (!box.page || box.page > pdf.numPages) return null;
      const { width, height } = (await pdf.getPage(box.page)).getViewport({ scale: 1 });
      return { width, height };
    },
    async paint(canvas, box, crop) {
      const page = await pdf.getPage(box.page ?? 1);
      const base = page.getViewport({ scale: 1 });
      const viewport = page.getViewport({ scale: canvas.width / (crop.w * base.width) });
      await page.render({
        canvas,
        viewport,
        transform: [1, 0, 0, 1, -crop.x * viewport.width, -crop.y * viewport.height],
      }).promise;
    },
  };
}

/** The file's name as a picture's name may start: letters, digits, dots, dashes and underscores. */
function stemOf(path: string): string {
  const name = path.split('/').pop() ?? '';
  const stem = name.replace(/\.[^.]*$/, '').replace(/[^A-Za-z0-9._-]+/g, '-').replace(/^[^A-Za-z0-9]+|-+$/g, '').slice(0, 80);
  return stem || 'file';
}

function stampOf(at: Date): string {
  const two = (n: number) => String(n).padStart(2, '0');
  return `${at.getFullYear()}${two(at.getMonth() + 1)}${two(at.getDate())}-${two(at.getHours())}${two(at.getMinutes())}${two(at.getSeconds())}`;
}

/**
 * A picture's file name: the file's, the page, when it was saved, the save's
 * own `token` (two saves in one second, or two files of one name, never
 * share a picture), and which box of several.
 */
export function pictureName(path: string, box: ArtifactCommentRegionBox, at: Date, token: string, index: number | null): string {
  return `${stemOf(path)}${box.page ? `-p${box.page}` : ''}-${stampOf(at)}-${token}${index === null ? '' : `-${index + 1}`}.png`;
}

/**
 * The anchor with a picture written for each box that has none, as the
 * comment on `path` is saved. A box whose picture cannot be read or written
 * (the computer is stopped) is saved without one: it still says where it is.
 */
export async function withPictures(
  anchor: ArtifactCommentAnchor,
  { workspaceId, path, source }: { workspaceId: string; path: string; source: PictureSource },
): Promise<ArtifactCommentAnchor> {
  if (anchor.kind !== 'region') return anchor;
  const at = new Date();
  const token = Math.random().toString(36).slice(2, 6).padEnd(4, '0');
  const several = anchor.boxes.filter((b) => b.w > 0).length > 1;
  const boxes = await inTurns(anchor.boxes, PICTURES_AT_ONCE, async (box, i) => {
    if (box.w === 0 || box.picture) return box;
    try {
      const blob = await pictureOf(source, box);
      if (!blob) return box;
      const name = pictureName(path, box, at, token, several ? i : null);
      const picture = `${PICTURES_DIR}/${name}`;
      await uploadWorkspaceFile(workspaceId, new File([blob], name, { type: 'image/png' }), picture);
      return { ...box, picture };
    } catch {
      return box;
    }
  });
  return { ...anchor, boxes };
}

/** `run` over `items`, at most `limit` at a time, the results in their order. */
async function inTurns<T, R>(items: readonly T[], limit: number, run: (item: T, index: number) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array(items.length);
  let next = 0;
  const worker = async () => {
    while (next < items.length) {
      const i = next++;
      out[i] = await run(items[i]!, i);
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return out;
}
