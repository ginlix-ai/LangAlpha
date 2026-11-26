import { describe, expect, it, vi } from 'vitest';

import { inlineWorkspaceImages } from '../widgetExport';

const BASE = 'https://api.example.com/api/v1/wsfiles/g/tok-1/';

describe('inlineWorkspaceImages', () => {
  it('leaves markup that names nothing under the base as it is, synchronously', () => {
    const html = '<img src="https://cdn.example.com/x.png"><p a=b>text</p>';
    const fetchImpl = vi.fn();
    expect(inlineWorkspaceImages(html, BASE, fetchImpl)).toBe(html);
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  // The copy must not carry the grant: what it names under the base becomes
  // bytes, fetched once per URL, and nothing else is touched.
  it('inlines images under the base and keeps the rest', async () => {
    const fetchImpl = vi.fn(async () => new Response('png', { headers: { 'content-type': 'image/png' } }));
    const html = '<img src="memes/01.jpg" alt="a"><img src="https://cdn.example.com/x.png">'
      + '<img srcset="memes/01.jpg 1x, memes/02.jpg 2x">';
    const out = await inlineWorkspaceImages(html, BASE, fetchImpl);
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    expect(fetchImpl).toHaveBeenCalledWith(`${BASE}memes/01.jpg`, { credentials: 'omit', signal: expect.any(AbortSignal) });
    expect(out).toContain('<img src="data:image/png;base64,cG5n" alt="a">');
    expect(out).toContain('<img src="https://cdn.example.com/x.png">');
    expect(out).toContain('srcset="data:image/png;base64,cG5n 1x, data:image/png;base64,cG5n 2x"');
    expect(out).not.toContain('tok-1');
  });

  // Written absolute, a reference left as it was would carry the grant out.
  it('drops a reference under the base that it could not inline', async () => {
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => (String(input).endsWith('ok.png')
      ? new Response('png', { headers: { 'content-type': 'image/png' } })
      : new Response('', { status: 404 })));
    const html = '<img src="missing.png" alt="a">'
      + `<img src="${BASE}gone.png" alt="b">`
      + `<img srcset="${BASE}gone.png 1x, ok.png 2x, https://cdn.example.com/x.png 3x">`
      + '<picture><source srcset="missing.png"><img src="ok.png"></picture>';
    const out = await inlineWorkspaceImages(html, BASE, fetchImpl);
    expect(out).toContain('<img alt="a">');
    expect(out).toContain('<img alt="b">');
    expect(out).toContain('<img srcset="data:image/png;base64,cG5n 2x, https://cdn.example.com/x.png 3x">');
    expect(out).toContain('<picture><source><img src="data:image/png;base64,cG5n"></picture>');
    expect(out).not.toContain('tok-1');
  });

  it('drops a srcset it cannot split when it names the base', () => {
    const html = `<img src="https://cdn.example.com/x.png" srcset="${BASE}a.png 2x junk">`;
    const fetchImpl = vi.fn();
    expect(inlineWorkspaceImages(html, BASE, fetchImpl)).toBe('<img src="https://cdn.example.com/x.png">');
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it('keeps the commas of a data: URL in a srcset', async () => {
    const data = 'data:image/png;base64,AAAA';
    const fetchImpl = vi.fn(async () => new Response('png', { headers: { 'content-type': 'image/png' } }));
    const out = await inlineWorkspaceImages(`<img srcset="${data} 1x, ws.png 2x,">`, BASE, fetchImpl);
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(out).toBe(`<img srcset="${data} 1x, data:image/png;base64,cG5n 2x">`);
  });

  it('keeps a no-break space inside a srcset URL', async () => {
    const fetchImpl = vi.fn(async () => new Response('png', { headers: { 'content-type': 'image/png' } }));
    const out = await inlineWorkspaceImages('<img srcset="sales\u00a02026.png 2x">', BASE, fetchImpl);
    expect(fetchImpl).toHaveBeenCalledWith(`${BASE}sales%C2%A02026.png`, expect.anything());
    expect(out).toBe('<img srcset="data:image/png;base64,cG5n 2x">');
  });
});
