import { describe, it, expect } from 'vitest';

import { buildSharedServeUrl, buildWsfilesUrl } from '../wsfilesUrl';

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
    expect(buildWsfilesUrl('ws-1', path)).toBe('/api/v1/wsfiles/ws-1/charts/x.png');
    expect(buildSharedServeUrl('tok', path)).toBe('/api/v1/public/shared/tok/files/serve/charts/x.png');
  });

  it('encodes each segment and keeps the slashes', () => {
    expect(buildWsfilesUrl('ws-1', '/home/workspace/results/图 表.png'))
      .toBe('/api/v1/wsfiles/ws-1/results/%E5%9B%BE%20%E8%A1%A8.png');
  });
});
