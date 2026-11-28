import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { act, render } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import type { ViewerCommentsTarget } from '@/pages/ChatAgent/components/viewers/comments/ViewerComments';
import type { PageBridge } from '@/pages/ChatAgent/components/viewers/html/usePageBridge';
import { DriveFrame } from '../components/DriveHtml';

interface LayerProps {
  target: { kind: string; path: string };
  bridge: PageBridge;
  view: { key: string; shown: boolean };
  fileSha?: string | null;
}

const layer = vi.hoisted(() => ({ props: null as LayerProps | null }));
vi.mock('@/pages/ChatAgent/components/viewers/html/comments/lazyCommentLayer', () => ({
  LazyCommentLayer: (props: LayerProps) => {
    layer.props = props;
    return <div data-testid="comment-layer" />;
  },
}));

/** What the layer was last drawn with; read through a call, as a render sets it. */
const seen = (): LayerProps | null => layer.props;

const ENTRY = '0b9e1c52-6f0a-4c7e-9d1e-3a2b5c4d6e7f';
const comments = {
  workspaceId: 'ws-home', kind: 'drive', path: ENTRY, tabId: 'drive-file', active: true, fileSha: 'f'.repeat(64),
  ui: {} as ViewerCommentsTarget['ui'],
} satisfies ViewerCommentsTarget;

function fromFrame(frame: HTMLIFrameElement, data: unknown, origin = 'null') {
  act(() => {
    window.dispatchEvent(new MessageEvent('message', { data, origin, source: frame.contentWindow }));
  });
}

describe('an HTML file\'s page in the Drive', () => {
  it('frames the page opaque, hears its bridge, and draws the comment layer over it on that bridge', () => {
    layer.props = null;
    const { container } = render(
      <DriveFrame docKey={`${ENTRY}:abc`} doc="<p>hi</p>" title="report.html" comments={comments} />,
    );
    const frame = container.querySelector('iframe')!;
    expect(frame.getAttribute('srcdoc')).toBe('<p>hi</p>');
    expect(frame.getAttribute('sandbox')).not.toContain('allow-same-origin');
    expect(frame.getAttribute('referrerpolicy')).toBe('no-referrer');

    expect(seen()?.target).toMatchObject({ kind: 'drive', path: ENTRY });
    expect(seen()?.fileSha).toBe(comments.fileSha);
    expect(seen()?.view).toEqual({ key: `${ENTRY}:abc`, shown: false });
    expect(seen()?.bridge.ready).toBe(false);

    const post = vi.spyOn(frame.contentWindow!, 'postMessage');
    // A page on a real origin is not this frame's.
    fromFrame(frame, { type: 'page:ready', path: null }, 'https://elsewhere.example');
    expect(seen()?.bridge.ready).toBe(false);

    fromFrame(frame, { type: 'page:ready', path: null });
    expect(seen()?.bridge.ready).toBe(true);
    // The theme goes to the opaque frame by its window, with no origin to name.
    expect(post).toHaveBeenCalledWith(expect.objectContaining({ type: 'page:theme' }), '*');

    fromFrame(frame, { type: 'page:ground', color: 'rgb(1, 2, 3)' });
    expect((container.firstElementChild as HTMLElement).style.getPropertyValue('--page-ground')).toBe('rgb(1, 2, 3)');
  });

  it('draws no layer where the file takes no comments', () => {
    layer.props = null;
    const { queryByTestId } = render(<DriveFrame docKey="k" doc="<p/>" title="x.html" comments={null} />);
    expect(queryByTestId('comment-layer')).toBeNull();
    expect(layer.props).toBeNull();
  });

  // jsdom applies no stylesheet, so the ground is pinned on the rules themselves.
  it('paints nothing until the page has loaded, then takes the bridged frame\'s ground, never paper of its own', () => {
    const css = readFileSync(resolve(__dirname, '../Drive.css'), 'utf8');
    const own = css.split('\n').filter((line) => line.includes('.drive-html-page'));
    expect(own).toContain('.drive-html-page:not(.is-shown) { background-color: transparent; }');
    // Paper under a page the bridge has set light text on for the dark theme reads as white on white.
    expect(own.join('\n')).not.toMatch(/sheet-paper|\.is-shown \{[^}]*background/);
    const frame = readFileSync(resolve(__dirname, '../../ChatAgent/components/viewers/html/BridgedFrame.css'), 'utf8');
    expect(frame).toMatch(/\.bridged-frame \{[^}]*background-color: var\(--page-ground, var\(--color-bg-page\)\)/);
  });
});
