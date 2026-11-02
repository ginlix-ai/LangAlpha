// @vitest-environment node
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';
import { withFramePolicy } from '../../../scripts/vite-plugins/framePolicy';
import { EMBED_FRAME_ORIGINS, HANDOFF_SCHEMES, appFrameSrc, hostedFrameSrc } from '../framePolicy';

// The policy is the one thing standing between a tampered report and a
// self-navigation that carries what it read to another site. A bare scheme
// (`https:`), a lone `*` or a scheme-less wildcard would let a frame go
// anywhere while every frame in the app still loads, so nothing else would
// notice it. `mailto:` and `tel:` are the only schemes allowed, since they
// load no document.
function sources(policy: string): string[] {
  const [directive, ...rest] = policy.split(' ');
  expect(directive).toBe('frame-src');
  return rest;
}

function expectNoWildcardScheme(policy: string): void {
  for (const source of sources(policy)) {
    if (source === "'self'" || (HANDOFF_SCHEMES as readonly string[]).includes(source)) continue;
    expect(source).toMatch(/^https?:\/\/(\*\.)?[a-z0-9-]+(\.[a-z0-9-]+)*(:(\d+|\*))?$/);
  }
}

describe('appFrameSrc', () => {
  it('names the app, the embeds and nothing else when the API is same-origin', () => {
    for (const base of [undefined, '', '/']) {
      const policy = appFrameSrc(base);
      expect(sources(policy)).toEqual(["'self'", ...EMBED_FRAME_ORIGINS, ...HANDOFF_SCHEMES]);
      expectNoWildcardScheme(policy);
    }
  });

  it('adds an absolute API base as its origin', () => {
    const policy = appFrameSrc('https://api.example.com/base/');
    expect(sources(policy)).toContain('https://api.example.com');
    expectNoWildcardScheme(policy);
  });

  it('drops an API base that is not one origin', () => {
    for (const base of ['https:', 'https://*', 'ftp://files.example.com', 'javascript:alert(1)']) {
      const policy = appFrameSrc(base);
      expect(sources(policy)).toEqual(["'self'", ...EMBED_FRAME_ORIGINS, ...HANDOFF_SCHEMES]);
      expectNoWildcardScheme(policy);
    }
  });
});

describe('hostedFrameSrc', () => {
  it('names exactly the framed origin, port included', () => {
    expect(hostedFrameSrc('https://3000-abc.preview.example.com/g/x/?token=t')).toBe(
      'frame-src https://3000-abc.preview.example.com mailto: tel:',
    );
    expect(hostedFrameSrc('http://localhost:54321/app')).toBe('frame-src http://localhost:54321 mailto: tel:');
  });

  it('refuses anything that is not an http(s) origin', () => {
    for (const src of ['', 'about:blank', 'data:text/html,x', 'blob:https://example.com/1', 'javascript:alert(1)', 'https:', 'https://*', '/relative']) {
      expect(hostedFrameSrc(src)).toBeNull();
    }
  });
});

describe('index.html', () => {
  const html = readFileSync(resolve(__dirname, '../../../index.html'), 'utf8');

  it('carries the policy in <head> ahead of every script', () => {
    const built = withFramePolicy(html, undefined);
    const meta = built.indexOf('http-equiv="Content-Security-Policy"');
    expect(meta).toBeGreaterThan(-1);
    expect(meta).toBeLessThan(built.indexOf('<script'));
    expect(meta).toBeLessThan(built.indexOf('</head>'));
    expect(built).toContain(`content="${appFrameSrc(undefined)}"`);
  });

  it('fails the build when the marker is gone', () => {
    expect(() => withFramePolicy('<head></head>', undefined)).toThrow(/frame-policy/);
  });
});
