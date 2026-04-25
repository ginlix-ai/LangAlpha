import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { Mock } from 'vitest';

vi.mock('@/api/client', () => {
  const mockGet = vi.fn().mockResolvedValue({ data: {} });
  const mockPost = vi.fn().mockResolvedValue({ data: {} });
  const mockPut = vi.fn().mockResolvedValue({ data: {} });
  const mockDelete = vi.fn().mockResolvedValue({ data: {} });
  const mockPatch = vi.fn().mockResolvedValue({ data: {} });
  return {
    api: {
      get: mockGet,
      post: mockPost,
      put: mockPut,
      delete: mockDelete,
      patch: mockPatch,
      defaults: { baseURL: 'http://localhost:8000' },
    },
  };
});

vi.mock('@/lib/supabase', () => ({
  supabase: null,
}));

import { api } from '@/api/client';
import {
  listUserMemos,
  readUserMemo,
  uploadUserMemo,
  writeUserMemo,
  deleteUserMemo,
  regenerateUserMemo,
  downloadUserMemoBlobUrl,
  triggerUserMemoDownload,
} from '../api';

const mockGet = api.get as Mock;
const mockPost = api.post as Mock;
const mockPut = api.put as Mock;
const mockDelete = api.delete as Mock;

describe('Memo API utilities', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('listUserMemos', () => {
    it('GETs /api/v1/memo/user and returns the data', async () => {
      const payload = {
        entries: [
          {
            key: 'abc.md',
            original_filename: 'abc.md',
            mime_type: 'text/markdown',
            size_bytes: 12,
            description: null,
            metadata_status: 'ready',
            created_at: null,
            modified_at: null,
            source_kind: null,
            source_workspace_id: null,
            source_path: null,
            sha256: null,
          },
        ],
        truncated: false,
      };
      mockGet.mockResolvedValue({ data: payload });

      const result = await listUserMemos();
      expect(mockGet).toHaveBeenCalledWith('/api/v1/memo/user');
      expect(result).toEqual(payload);
    });
  });

  describe('readUserMemo', () => {
    it('GETs /api/v1/memo/user/read with the key as a query param', async () => {
      const payload = { key: 'abc.md', content: 'hello' };
      mockGet.mockResolvedValue({ data: payload });

      const result = await readUserMemo('abc.md');
      expect(mockGet).toHaveBeenCalledWith('/api/v1/memo/user/read', {
        params: { key: 'abc.md' },
      });
      expect(result).toEqual(payload);
    });
  });

  describe('uploadUserMemo', () => {
    it('POSTs multipart/form-data with the file, no source by default', async () => {
      mockPost.mockResolvedValue({
        data: { key: 'abc.md', original_filename: 'abc.md', metadata_status: 'pending' },
      });
      const file = new File(['hi'], 'abc.md', { type: 'text/markdown' });

      await uploadUserMemo(file);

      expect(mockPost).toHaveBeenCalledTimes(1);
      const [url, formData, opts] = mockPost.mock.calls[0];
      expect(url).toBe('/api/v1/memo/user/upload');
      expect(formData).toBeInstanceOf(FormData);
      expect(opts.headers['Content-Type']).toBe('multipart/form-data');
      // FormData should contain the file under "file"
      const fd = formData as FormData;
      expect(fd.get('file')).toBeInstanceOf(File);
      expect((fd.get('file') as File).name).toBe('abc.md');
      // No source fields when source is null
      expect(fd.get('source_kind')).toBeNull();
      expect(fd.get('source_workspace_id')).toBeNull();
      expect(fd.get('source_path')).toBeNull();
    });

    it('appends source fields when source is provided', async () => {
      mockPost.mockResolvedValue({
        data: { key: 'abc.md', original_filename: 'abc.md', metadata_status: 'pending' },
      });
      const file = new File(['hi'], 'abc.md', { type: 'text/markdown' });

      await uploadUserMemo(file, null, {
        source_kind: 'sandbox',
        source_workspace_id: 'ws-1',
        source_path: 'work/notes/abc.md',
      });

      const [, formData] = mockPost.mock.calls[0];
      const fd = formData as FormData;
      expect(fd.get('source_kind')).toBe('sandbox');
      expect(fd.get('source_workspace_id')).toBe('ws-1');
      expect(fd.get('source_path')).toBe('work/notes/abc.md');
    });

    it('wires onUploadProgress to the callback as a percent', async () => {
      mockPost.mockResolvedValue({ data: {} });
      const file = new File(['hi'], 'abc.md', { type: 'text/markdown' });
      const progress = vi.fn();

      await uploadUserMemo(file, progress);

      const [, , opts] = mockPost.mock.calls[0];
      expect(typeof opts.onUploadProgress).toBe('function');
      // Simulate axios firing the progress event
      opts.onUploadProgress({ loaded: 50, total: 200 });
      expect(progress).toHaveBeenCalledWith(25);
    });
  });

  describe('writeUserMemo', () => {
    it('PUTs JSON body with key + content', async () => {
      mockPut.mockResolvedValue({
        data: { key: 'abc.md', original_filename: 'abc.md', metadata_status: 'pending' },
      });

      await writeUserMemo('abc.md', '# updated');

      expect(mockPut).toHaveBeenCalledWith('/api/v1/memo/user/write', {
        key: 'abc.md',
        content: '# updated',
      });
    });
  });

  describe('deleteUserMemo', () => {
    it('DELETEs /api/v1/memo/user with key query param', async () => {
      mockDelete.mockResolvedValue({});

      await deleteUserMemo('abc.md');

      expect(mockDelete).toHaveBeenCalledWith('/api/v1/memo/user', {
        params: { key: 'abc.md' },
      });
    });
  });

  describe('regenerateUserMemo', () => {
    it('POSTs to /api/v1/memo/user/regenerate with key as query param', async () => {
      mockPost.mockResolvedValue({
        data: { key: 'abc.md', original_filename: 'abc.md', metadata_status: 'pending' },
      });

      await regenerateUserMemo('abc.md');

      expect(mockPost).toHaveBeenCalledWith(
        '/api/v1/memo/user/regenerate',
        undefined,
        { params: { key: 'abc.md' } },
      );
    });
  });

  describe('downloadUserMemoBlobUrl', () => {
    let createUrl: typeof URL.createObjectURL;
    beforeEach(() => {
      createUrl = URL.createObjectURL;
      // jsdom doesn't provide createObjectURL out of the box
      URL.createObjectURL = vi.fn(() => 'blob:fake-url');
    });
    afterEach(() => {
      URL.createObjectURL = createUrl;
    });

    it('GETs blob and returns the object URL', async () => {
      const blob = new Blob(['hello']);
      mockGet.mockResolvedValue({ data: blob });

      const url = await downloadUserMemoBlobUrl('abc.md');

      expect(mockGet).toHaveBeenCalledWith('/api/v1/memo/user/download', {
        params: { key: 'abc.md' },
        responseType: 'blob',
      });
      expect(URL.createObjectURL).toHaveBeenCalledWith(blob);
      expect(url).toBe('blob:fake-url');
    });
  });

  describe('triggerUserMemoDownload', () => {
    let createUrl: typeof URL.createObjectURL;
    let revokeUrl: typeof URL.revokeObjectURL;
    beforeEach(() => {
      createUrl = URL.createObjectURL;
      revokeUrl = URL.revokeObjectURL;
      URL.createObjectURL = vi.fn(() => 'blob:fake-url');
      URL.revokeObjectURL = vi.fn();
    });
    afterEach(() => {
      URL.createObjectURL = createUrl;
      URL.revokeObjectURL = revokeUrl;
    });

    it('creates an anchor with the blob url and clicks it', async () => {
      mockGet.mockResolvedValue({ data: new Blob(['x']) });
      const click = vi.fn();
      const realCreateElement = document.createElement.bind(document);
      const createSpy = vi.spyOn(document, 'createElement').mockImplementation(
        ((tag: string) => {
          if (tag === 'a') {
            const a = realCreateElement('a') as HTMLAnchorElement;
            a.click = click;
            return a;
          }
          return realCreateElement(tag);
        }) as typeof document.createElement,
      );

      await triggerUserMemoDownload('abc.md', 'pretty.md');

      expect(click).toHaveBeenCalled();
      expect(URL.revokeObjectURL).toHaveBeenCalledWith('blob:fake-url');
      createSpy.mockRestore();
    });

    it('falls back to key as filename when none provided', async () => {
      mockGet.mockResolvedValue({ data: new Blob(['x']) });
      const realCreateElement = document.createElement.bind(document);
      let lastAnchor: HTMLAnchorElement | null = null;
      const createSpy = vi.spyOn(document, 'createElement').mockImplementation(
        ((tag: string) => {
          if (tag === 'a') {
            const a = realCreateElement('a') as HTMLAnchorElement;
            a.click = vi.fn();
            lastAnchor = a;
            return a;
          }
          return realCreateElement(tag);
        }) as typeof document.createElement,
      );

      await triggerUserMemoDownload('abc.md');

      expect(lastAnchor).not.toBeNull();
      expect(lastAnchor!.download).toBe('abc.md');
      createSpy.mockRestore();
    });
  });
});
