/**
 * The bytes of files held in unsent composer drafts, in IndexedDB, which can
 * hold a File where localStorage holds only strings. A draft's record names
 * its files; this keeps what they are, so a reload can show them and stage
 * them again once the server has let its copy go.
 *
 * Every call degrades to nothing: with no IndexedDB (a private window that
 * refuses it, an old WebView, the unit suite) a draft keeps its files only
 * while the page lives.
 */

const DB_NAME = 'langalpha-drafts';
const STORE = 'files';

let opening: Promise<IDBDatabase | null> | null = null;

function openDb(): Promise<IDBDatabase | null> {
  if (opening) return opening;
  opening = new Promise((resolve) => {
    let request: IDBOpenDBRequest;
    try {
      if (typeof indexedDB === 'undefined') {
        resolve(null);
        return;
      }
      request = indexedDB.open(DB_NAME, 1);
    } catch {
      resolve(null);
      return;
    }
    request.onupgradeneeded = () => {
      if (!request.result.objectStoreNames.contains(STORE)) request.result.createObjectStore(STORE);
    };
    request.onsuccess = () => {
      const db = request.result;
      // Another tab upgrading later must not be held up by this one.
      db.onversionchange = () => db.close();
      resolve(db);
    };
    request.onerror = () => resolve(null);
    request.onblocked = () => resolve(null);
  });
  return opening;
}

function run<T>(mode: IDBTransactionMode, act: (store: IDBObjectStore) => IDBRequest<T> | void): Promise<T | undefined> {
  return openDb().then((db) => new Promise<T | undefined>((resolve) => {
    if (!db) {
      resolve(undefined);
      return;
    }
    try {
      const tx = db.transaction(STORE, mode);
      const request = act(tx.objectStore(STORE));
      tx.oncomplete = () => resolve(request ? request.result : undefined);
      tx.onerror = () => resolve(undefined);
      tx.onabort = () => resolve(undefined);
    } catch {
      resolve(undefined);
    }
  }));
}

/** A file's bytes and when they were written, which a sweep reads. */
interface Stored {
  file: Blob;
  at: number;
}

const fileOf = (value: unknown): Blob | null => {
  const file = (value as Stored | undefined)?.file;
  return file instanceof Blob ? file : null;
};

export function putDraftFile(key: string, file: File): Promise<void> {
  const value: Stored = { file, at: Date.now() };
  return run('readwrite', (store) => store.put(value, key)).then(() => undefined);
}

export function getDraftFile(key: string): Promise<Blob | null> {
  return run<unknown>('readonly', (store) => store.get(key)).then(fileOf);
}

export function deleteDraftFiles(keys: string[]): Promise<void> {
  if (keys.length === 0) return Promise.resolve();
  return run('readwrite', (store) => {
    for (const key of keys) store.delete(key);
  }).then(() => undefined);
}

/** Every stored file's key, and when it was written. */
export function draftFileEntries(): Promise<Array<{ key: string; at: number }>> {
  const entries: Array<{ key: string; at: number }> = [];
  return run('readonly', (store) => {
    const request = store.openCursor();
    request.onsuccess = () => {
      const cursor = request.result;
      if (!cursor) return;
      if (typeof cursor.key === 'string') entries.push({ key: cursor.key, at: (cursor.value as Stored | undefined)?.at ?? 0 });
      cursor.continue();
    };
  }).then(() => entries);
}
