// @vitest-environment node
import { describe, expect, it } from 'vitest';
import type { FileBody } from '@/pages/ChatAgent/components/filePanel/fileBody';
import { LIVE } from '@/pages/ChatAgent/utils/api/readFrom';
import type { DriveEntry } from '@/types/drive';
import { bodyTakesComments } from '../bodyComments';
import type { BodyView } from '../components/FileBody';

const file = (name: string): DriveEntry => ({
  id: 'e1', path: name, name, kind: 'file', size: 8, sha256: 'a'.repeat(64), content_type: null, is_text: true, version: 1,
  created_at: '2026-11-01T00:00:00Z', modified_at: '2026-11-01T00:00:00Z', modified_by: null, deleted_at: null, memo: null, undo_until: null,
});

const text = (content: string | null, mime = 'text/plain'): FileBody =>
  ({ mode: 'text', mime, content, buffer: null, truncated: false, from: LIVE });
const bytes = (mime: string): FileBody =>
  ({ mode: mime === 'image' ? 'image' : 'buffer', mime, content: null, buffer: new ArrayBuffer(4), truncated: false, from: LIVE });

function doc(name: string, body: FileBody | null, over: Partial<Extract<BodyView, { kind: 'document' }>> = {}): BodyView {
  return { kind: 'document', entry: file(name), body, loading: false, error: null, onRetry: () => {}, seq: null, comments: null, ...over };
}

describe('which bodies take the comment tools', () => {
  it.each<[string, BodyView]>([
    ['a Markdown file', doc('a.md', text('# hi', 'text/markdown'))],
    ['a code file', doc('a.py', text('x = 1'))],
    ['a CSV', doc('a.csv', text('a,b'))],
    ['a PDF', doc('a.pdf', bytes('pdf'))],
    ['a workbook', doc('a.xlsx', bytes('excel'))],
    ['an image', doc('a.png', bytes('image'))],
    ['an HTML page or its source', { kind: 'html', name: 'a.html', content: '<p/>', source: false, page: null, comments: null }],
  ])('%s does', (_what, view) => {
    expect(bodyTakesComments(view, false)).toBe(true);
  });

  it.each<[string, BodyView, boolean?]>([
    ['a file with no preview', doc('pricing_facts.csv', null, { error: { category: 'binary_file' } })],
    ['a file too large to preview', doc('big.pdf', null, { error: { category: 'too_large' } })],
    ['a body read as none', doc('big.pdf', { ...bytes('pdf'), mode: 'none', buffer: null })],
    ['a read that failed', doc('a.md', null, { error: { category: 'unknown' } })],
    ['a first read on its way', doc('a.md', null, { loading: true })],
    ['a viewer that failed on the bytes', doc('a.xlsx', bytes('excel')), true],
    ['a page framed with no bridge', { kind: 'html', name: 'a.html', content: '<p/>', source: false, page: null, comments: null }, true],
    ['a text file with no text', doc('a.py', text(null))],
    ['the editor', { kind: 'editor', name: 'a.md', draft: '', original: '', compare: false, modelPath: 'm', editorRef: { current: null }, onChange: () => {} }],
    ['the pane still loading', { kind: 'loading' }],
    ['a file that could not be read', { kind: 'failure', name: 'a.md', gone: false, onRetry: () => {} }],
  ])('%s does not', (_what, view, failed = false) => {
    expect(bodyTakesComments(view, failed)).toBe(false);
  });
});
