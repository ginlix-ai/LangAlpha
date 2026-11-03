// @vitest-environment node
import { describe, it, expect } from 'vitest';

import { buildServeUrl, withServeView } from '../wsfilesUrl';

// A served path is a reference like any other: a sandbox root names the
// workspace and comes off. Stripping only the leading slash sent
// `/home/workspace/charts/x.png` on as two literal segments, and the shared
// view's inline images 404 on it.
describe('wsfilesUrl strips a sandbox root', () => {
  it.each([
    ['/home/workspace/charts/x.png'],
    ['file:///home/workspace/charts/x.png'],
    ['/home/daytona/charts/x.png'],
    ['charts/x.png'],
  ])('serves %s workspace-relative', (path) => {
    expect(buildServeUrl('/api/v1/wsfiles/g/grant-1/', path)).toBe('/api/v1/wsfiles/g/grant-1/charts/x.png');
    expect(buildServeUrl('/api/v1/public/shared/tok/files/serve/', path)).toBe('/api/v1/public/shared/tok/files/serve/charts/x.png');
  });

  it('encodes each segment and keeps the slashes', () => {
    expect(buildServeUrl('/api/v1/wsfiles/g/grant-1/', '/home/workspace/results/图 表.png'))
      .toBe('/api/v1/wsfiles/g/grant-1/results/%E5%9B%BE%20%E8%A1%A8.png');
  });
});

// A script that sets location, or a refresh, takes the viewer's frame to the
// next file without the view its address carried.
describe('withServeView', () => {
  const GRANT = 'https://app.example.com/api/v1/wsfiles/g/v1.ws.1.sig/';
  const SHARE = 'https://app.example.com/api/v1/public/shared/tok/files/serve/';

  it('carries the frame\'s view to another file under its prefix', () => {
    expect(withServeView(`${GRANT}r/next.html`, `${GRANT}r/a.html?inject=comments`))
      .toBe(`${GRANT}r/next.html?inject=comments`);
    expect(withServeView(`${SHARE}r/next.html?q=1#s`, `${SHARE}r/a.html?inject=view`))
      .toBe(`${SHARE}r/next.html?q=1&inject=view#s`);
  });

  it('leaves alone what is not that frame\'s next file', () => {
    // Already viewed: its document speaks no bridge for another reason.
    expect(withServeView(`${GRANT}r/next.html?inject=comments`, `${GRANT}r/a.html?inject=comments`)).toBeNull();
    // Another grant, another host, a frame with no view, an address that is none.
    expect(withServeView('https://app.example.com/api/v1/wsfiles/g/other/r/n.html', `${GRANT}r/a.html?inject=view`)).toBeNull();
    expect(withServeView('https://evil.example/api/v1/wsfiles/g/v1.ws.1.sig/r/n.html', `${GRANT}r/a.html?inject=view`)).toBeNull();
    expect(withServeView(`${GRANT}r/n.html`, `${GRANT}r/a.html`)).toBeNull();
    expect(withServeView('not a url', `${GRANT}r/a.html?inject=view`)).toBeNull();
  });
});
