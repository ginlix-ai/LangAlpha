/**
 * A relative link in agent output is a workspace file. Left to the browser it
 * opens the app itself in a new tab, which is what "I click and no file opens"
 * looked like for any name outside the old extension allowlist.
 */
import { describe, it, expect, vi } from 'vitest';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router';
import '@testing-library/jest-dom';

vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'dark', setTheme: () => {} }),
}));

import Markdown from '../Markdown';
import { WorkspaceProvider } from '../../contexts/WorkspaceContext';
import { RouteLeaveGuardContext, type RouteLeaveGuard } from '../../contexts/RouteLeaveGuardContext';
import type { ComputerFolders } from '../../utils/agentPaths';

function renderHtml(content: string, variant: 'chat' | 'panel' = 'chat'): string {
  return renderToStaticMarkup(<Markdown variant={variant} content={content} onOpenFile={() => {}} />);
}

describe('Markdown file links', () => {
  it.each([
    ['a deck', '[deck](results/deck.pptx)'],
    ['a macro workbook', '[model](results/model.xlsm)'],
    ['an extensionless file', '[notes](results/NOTES)'],
    ['a name with spaces', '[deck](results/Q3 deck.pptx)'],
  ])('renders %s as an in-app file link', (_label, content) => {
    const html = renderHtml(content);
    expect(html).toContain('<a class="underline hover:opacity-80');
    expect(html).not.toContain('target="_blank"');
  });

  it('renders a bare name with a line suffix as a link', () => {
    expect(renderHtml('[model](model.py:42)')).toContain('<a class="underline hover:opacity-80');
  });

  it('renders a bare name with a line suffix and a title as a link', () => {
    // The anchor is what proves it: without the `./`, react-markdown's
    // `defaultUrlTransform` reads `model.py:` as a scheme and emits an <a>
    // with a title and no href at all, which renders as unclickable text.
    // Without the `./`, react-markdown's `defaultUrlTransform` reads `model.py:`
    // as a scheme and emits no href, so the link falls through to the web-link
    // branch: a new tab with nothing to open, rather than the panel.
    const html = renderHtml('[model](model.py:42 "source")');
    expect(html).toContain('cursor-pointer');
    expect(html).not.toContain('target="_blank"');
    expect(html).toContain('title="source"');
  });

  it('makes links inside a file viewed in the panel clickable', () => {
    const html = renderHtml('[appendix](appendix)', 'panel');
    expect(html).toContain('cursor-pointer');
    expect(html).not.toContain('target="_blank"');
  });

  it('keeps web links opening in a new tab', () => {
    expect(renderHtml('[site](https://example.com/report.md)')).toContain('target="_blank"');
  });
});

/**
 * A link reads as the agent's Bash reads it from its working directory: a
 * sibling's folder opens that workspace, and a rooted path through the
 * workspace's own folder is a path in the workspace itself.
 */
describe('Markdown links through a workspace folder', () => {
  const folders: ComputerFolders = {
    dirName: 'Home',
    previousDirNames: null,
    siblings: [{ workspaceId: 'ws-nvda', dirName: 'NVDA' }],
  };

  function click(content: string) {
    const onOpenFile = vi.fn();
    const { container, unmount } = render(
      <WorkspaceProvider workspaceId="ws-home" downloadFile={null} folders={folders}>
        <Markdown variant="chat" content={content} onOpenFile={onOpenFile} />
      </WorkspaceProvider>,
    );
    fireEvent.click(container.querySelector('a')!);
    unmount();
    return onOpenFile.mock.calls[0].slice(0, 2);
  }

  it.each([
    ['climbed into', '../NVDA/dcf/report.md'],
    ['rooted', '/home/workspace/NVDA/dcf/report.md'],
  ])('opens a sibling folder %s in that workspace', (_how, href) => {
    expect(click(`[report](${href})`)).toEqual(['dcf/report.md', 'ws-nvda']);
  });

  it('reads a rooted path through the own folder as a path in the workspace', () => {
    expect(click('[notes](/home/workspace/Home/notes.md)')).toEqual(['notes.md', undefined]);
  });

  it('hands a climb into the own folder on as written, for the handler to read against its file', () => {
    expect(click('[notes](../Home/notes.md)')).toEqual(['../Home/notes.md', undefined]);
  });
});

// The agent writes the app's own pages as absolute links, so a reply read in a
// channel still works. In the app a new tab is a second copy of it that has
// lost this tab's state; a skipped setup sent that tab to the setup wizard.
describe('Markdown links to pages of this app', () => {
  function Where() {
    const { pathname, search, hash } = useLocation();
    return <output data-testid="where">{pathname + search + hash}</output>;
  }

  function renderInApp(content: string, guard: RouteLeaveGuard = (go) => go()) {
    render(
      <MemoryRouter initialEntries={['/chat/t/thread-1']}>
        <RouteLeaveGuardContext value={guard}>
          <Markdown variant="chat" content={content} />
        </RouteLeaveGuardContext>
        <Where />
      </MemoryRouter>,
    );
    return screen.getByRole('link');
  }

  it('opens a page of this app in place', () => {
    const href = `${window.location.origin}/settings?tab=preferences#trading-permission`;
    const link = renderInApp(`[Trading permission](${href})`);
    expect(link).not.toHaveAttribute('target');

    fireEvent.click(link);

    expect(screen.getByTestId('where')).toHaveTextContent('/settings?tab=preferences#trading-permission');
  });

  // Leaving the route unmounts the file panel beside the thread, and with it
  // any draft it holds, so the host is asked first and can say no.
  it('asks the host before it leaves the page', () => {
    const guard = vi.fn<RouteLeaveGuard>();
    const link = renderInApp(`[Orders](${window.location.origin}/orders)`, guard);

    fireEvent.click(link);

    expect(guard).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId('where')).toHaveTextContent('/chat/t/thread-1');
  });

  it('keeps another app on this origin a web link', () => {
    const link = renderInApp(`[Plans](${window.location.origin}/account/plans)`);
    expect(link).toHaveAttribute('target', '_blank');
  });
});
