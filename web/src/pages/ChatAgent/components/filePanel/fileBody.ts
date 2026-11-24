import type { RangedFile } from '@/lib/rangedFile';
import { getFileExtension, DOWNLOAD_ONLY_EXTENSIONS } from './fileMeta';
import type { BytesRead, OpenedRead, TextRead } from '../../utils/api/files';
import { LIVE, type ReadFrom } from '../../utils/api/readFrom';

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
  /** Where the bytes came from: a backup may be older than the file. */
  from: ReadFrom;
  /** The token of a text body's bytes where its source names one: an edit made over it saves against it. */
  version?: string | null;
}

/** The adapter-resolved readers — never the api imports directly, so a share keeps working. */
export interface BodyReaders {
  readFile: (workspaceId: string, path: string) => Promise<TextRead>;
  readFileFull: (workspaceId: string, path: string) => Promise<Pick<TextRead, 'content' | 'from' | 'version'>>;
  downloadFileAsArrayBuffer: (workspaceId: string, path: string) => Promise<BytesRead>;
  /** Left out, a PDF downloads whole. */
  openRanged?: (workspaceId: string, path: string) => Promise<OpenedRead>;
}

/**
 * One file's readers, already bound to where it lives: a workspace path, or
 * a Drive entry and version. The body reader needs only these and the name
 * that picks the viewer.
 */
export interface FileReaders {
  /** The text, paginated where the source caps a read. */
  readText: () => Promise<TextRead>;
  /** The whole text, for an HTML document's source. */
  readFull: () => Promise<Pick<TextRead, 'content' | 'from' | 'version'>>;
  readBytes: () => Promise<BytesRead>;
  /** Left out, a PDF downloads whole. */
  openRanged?: () => Promise<OpenedRead>;
}

/** A workspace file's readers, bound to its workspace and path. */
export function bindReaders(readers: BodyReaders, workspaceId: string, path: string): FileReaders {
  const { openRanged } = readers;
  return {
    readText: () => readers.readFile(workspaceId, path),
    readFull: () => readers.readFileFull(workspaceId, path),
    readBytes: () => readers.downloadFileAsArrayBuffer(workspaceId, path),
    ...(openRanged && { openRanged: () => openRanged(workspaceId, path) }),
  };
}

const EMPTY: Omit<FileBody, 'mode' | 'mime'> = { content: null, buffer: null, truncated: false, from: LIVE };

/** Reads a file's bytes in the shape the viewer its `name` picks wants them. */
export async function readFileBody(file: FileReaders, name: string): Promise<FileBody> {
  const mode = bodyMode(name);
  switch (mode) {
    case 'none':
      return { mode, mime: null, ...EMPTY };
    case 'image': {
      const { buffer, from } = await file.readBytes();
      return { mode, mime: 'image', content: null, buffer, truncated: false, from };
    }
    case 'buffer': {
      if (getFileExtension(name) === 'pdf' && file.openRanged) {
        // pdf.js reads a PDF by range, so a large one opens from its first slice.
        const opened = await file.openRanged();
        return 'ranged' in opened
          ? { mode, mime: 'pdf', content: null, buffer: null, ranged: opened.ranged, truncated: false, from: opened.from }
          : { mode, mime: 'pdf', content: null, buffer: opened.buffer, truncated: false, from: opened.from };
      }
      const { buffer, from } = await file.readBytes();
      return { mode, mime: getFileExtension(name) === 'pdf' ? 'pdf' : 'excel', content: null, buffer, truncated: false, from };
    }
    case 'html': {
      const data = await file.readFull();
      return { mode, mime: 'text/html', content: data.content, buffer: null, truncated: false, from: data.from, version: data.version };
    }
    default: {
      const data = await file.readText();
      return {
        mode,
        mime: data.mime || 'text/plain',
        content: data.content,
        buffer: null,
        truncated: !!data.truncated,
        from: data.from,
        version: data.version,
      };
    }
  }
}
