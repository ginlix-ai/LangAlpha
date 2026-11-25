/**
 * Handles to files on the person's disk, which a browser with the File System
 * Access API (Chrome, Edge, the desktop app) lets a page keep and read again
 * after a reload, with the person's permission. Elsewhere none of this exists,
 * and every function here says so by answering null or nothing.
 *
 * Handles are kept in IndexedDB, the only store that holds them; a store that
 * is blocked (private mode) or missing loses them, and the person chooses the
 * file again instead.
 */

interface PermissionedHandle extends FileSystemFileHandle {
  queryPermission?: (d: { mode: 'read' }) => Promise<PermissionState>;
  requestPermission?: (d: { mode: 'read' }) => Promise<PermissionState>;
}

type OpenFilePicker = (opts: { multiple?: boolean }) => Promise<FileSystemFileHandle[]>;

function openFilePicker(): OpenFilePicker | null {
  const picker = (window as unknown as { showOpenFilePicker?: OpenFilePicker }).showOpenFilePicker;
  return typeof picker === 'function' ? picker.bind(window) : null;
}

/** Whether files can be chosen with handles kept, so the page can read them again later. */
export const canKeepHandles = (): boolean => openFilePicker() !== null;

/**
 * Files chosen with the browser's handle picker, each with its handle; null
 * when the person closed the picker or the browser refused to show it.
 */
export async function pickWithHandles(multiple: boolean): Promise<Array<{ file: File; handle: FileSystemFileHandle }> | null> {
  const picker = openFilePicker();
  if (!picker) return null;
  try {
    const handles = await picker({ multiple });
    return await Promise.all(handles.map(async (handle) => ({ file: await handle.getFile(), handle })));
  } catch {
    return null;
  }
}

/**
 * The handle a dropped item carries, asked for while the drop is handled:
 * the browser empties the drop's list once its handler returns.
 */
export function dropHandle(item: DataTransferItem): Promise<FileSystemFileHandle | null> {
  const get = (item as DataTransferItem & { getAsFileSystemHandle?: () => Promise<FileSystemHandle | null> }).getAsFileSystemHandle;
  if (typeof get !== 'function') return Promise.resolve(null);
  return get.call(item).then(
    (h) => (h && h.kind === 'file' ? (h as FileSystemFileHandle) : null),
    () => null,
  );
}

/**
 * The file behind `handle` now, asking the person's permission when the
 * browser no longer holds it (it asks only during a click); null when refused
 * or when the file is gone.
 */
export async function readHandle(handle: FileSystemFileHandle): Promise<File | null> {
  const h = handle as PermissionedHandle;
  try {
    if (h.queryPermission && (await h.queryPermission({ mode: 'read' })) !== 'granted') {
      if (!h.requestPermission || (await h.requestPermission({ mode: 'read' })) !== 'granted') return null;
    }
    return await handle.getFile();
  } catch {
    return null;
  }
}

const DB = 'langalpha-file-handles';
const STORE = 'handles';

function openDb(): Promise<IDBDatabase | null> {
  return new Promise((resolve) => {
    try {
      const req = indexedDB.open(DB, 1);
      req.onupgradeneeded = () => req.result.createObjectStore(STORE);
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => resolve(null);
      req.onblocked = () => resolve(null);
    } catch {
      resolve(null);
    }
  });
}

async function withStore<T>(mode: IDBTransactionMode, act: (store: IDBObjectStore) => IDBRequest<T>): Promise<T | null> {
  const db = await openDb();
  if (!db) return null;
  return new Promise((resolve) => {
    try {
      const req = act(db.transaction(STORE, mode).objectStore(STORE));
      req.onsuccess = () => resolve(req.result ?? null);
      req.onerror = () => resolve(null);
    } catch {
      resolve(null);
    }
  }).finally(() => db.close()) as Promise<T | null>;
}

export async function keepHandle(key: string, handle: FileSystemFileHandle): Promise<void> {
  await withStore('readwrite', (s) => s.put(handle, key));
}

export async function loadHandle(key: string): Promise<FileSystemFileHandle | null> {
  const found = await withStore<unknown>('readonly', (s) => s.get(key));
  return typeof FileSystemFileHandle !== 'undefined' && found instanceof FileSystemFileHandle ? found : null;
}

export async function forgetHandle(key: string): Promise<void> {
  await withStore('readwrite', (s) => s.delete(key));
}
