import { isRangedFile, type RangedFile } from '@/lib/rangedFile';
import { getFileExtension, DOWNLOAD_ONLY_EXTENSIONS } from './fileMeta';

/**
 * How a file's bytes have to be fetched for the viewer that shows them.
 *
 * `html` is its own mode rather than a flavour of `text` because the paginated
 * read caps at 20k lines and the Source tab needs the whole document; `none`
 * is a file with no in-browser viewer, which opens as a download card without
 * a request.
 */
export type BodyMode = 'text' | 'html' | 'buffer' | 'image' | 'none';

const IMAGE_EXTENSIONS = new Set(['png', 'jpg', 'jpeg', 'gif', 'svg', 'webp', 'bmp']);
const BUFFER_EXTENSIONS = new Set(['pdf', 'xlsx', 'xlsm', 'xls']);

export function bodyMode(path: string): BodyMode {
  const ext = getFileExtension(path);
  if (DOWNLOAD_ONLY_EXTENSIONS.has(ext)) return 'none';
  if (IMAGE_EXTENSIONS.has(ext)) return 'image';
  if (BUFFER_EXTENSIONS.has(ext)) return 'buffer';
  if (ext === 'html' || ext === 'htm') return 'html';
  return 'text';
}

/** The blob type an image's bytes need to render; the server does not say. */
export function imageMime(path: string): string {
  const ext = getFileExtension(path);
  if (ext === 'svg') return 'image/svg+xml';
  if (ext === 'jpg') return 'image/jpeg';
  return `image/${ext}`;
}

/**
 * One file's bytes, in the shape its viewer wants them.
 *
 * `mime` keeps the panel's own vocabulary — `'pdf'`, `'excel'`, `'image'` —
 * beside real mime strings, because that is what the viewer switch, the
 * editable test and the focus ladder have always read.
 */
export interface FileBody {
  mode: BodyMode;
  mime: string | null;
  content: string | null;
  buffer: ArrayBuffer | null;
  /** A large PDF opened from its first slice, in place of `buffer`. */
  ranged?: RangedFile;
  /** The read stopped short of the end, so a line past it may still exist. */
  truncated: boolean;
}

/** The adapter-resolved readers — never the api imports directly, so a share keeps working. */
export interface BodyReaders {
  readFile: (workspaceId: string, path: string) => Promise<{ content?: string; mime?: string; truncated?: boolean }>;
  readFileFull: (workspaceId: string, path: string) => Promise<{ content?: string }>;
  downloadFileAsArrayBuffer: (workspaceId: string, path: string) => Promise<ArrayBuffer>;
  /** Left out, a PDF downloads whole. */
  openRanged?: (workspaceId: string, path: string) => Promise<ArrayBuffer | RangedFile>;
}

const EMPTY: Omit<FileBody, 'mode' | 'mime'> = { content: null, buffer: null, truncated: false };

export async function readFileBody(
  readers: BodyReaders,
  workspaceId: string,
  path: string,
): Promise<FileBody> {
  const mode = bodyMode(path);
  switch (mode) {
    case 'none':
      return { mode, mime: null, ...EMPTY };
    case 'image': {
      const buffer = await readers.downloadFileAsArrayBuffer(workspaceId, path);
      return { mode, mime: 'image', content: null, buffer, truncated: false };
    }
    case 'buffer': {
      if (getFileExtension(path) === 'pdf' && readers.openRanged) {
        // pdf.js reads a PDF by range, so a large one opens from its first slice.
        const opened = await readers.openRanged(workspaceId, path);
        return isRangedFile(opened)
          ? { mode, mime: 'pdf', content: null, buffer: null, ranged: opened, truncated: false }
          : { mode, mime: 'pdf', content: null, buffer: opened, truncated: false };
      }
      const buffer = await readers.downloadFileAsArrayBuffer(workspaceId, path);
      return { mode, mime: getFileExtension(path) === 'pdf' ? 'pdf' : 'excel', content: null, buffer, truncated: false };
    }
    case 'html': {
      const data = await readers.readFileFull(workspaceId, path);
      return { mode, mime: 'text/html', content: data.content || '', buffer: null, truncated: false };
    }
    default: {
      const data = await readers.readFile(workspaceId, path);
      return {
        mode,
        mime: data.mime || 'text/plain',
        content: data.content || '',
        buffer: null,
        truncated: !!data.truncated,
      };
    }
  }
}
