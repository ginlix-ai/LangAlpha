/**
 * A web link in a reply can open in the computer's browser, from its right-click
 * menu, where the chat has one to show it in.
 */
import { describe, it, expect, vi } from 'vitest';
import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import '@/i18n';

vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'dark', setTheme: () => {} }),
}));

import Markdown from '../Markdown';
import { LinkActionsContext, type LinkActions } from '../../contexts/LinkActionsContext';

function show(content: string, opener: LinkActions['openInBrowser'], openShareLink: LinkActions['openShareLink'] = null) {
  render(
    <LinkActionsContext value={{ openShareLink, openInBrowser: opener }}>
      <Markdown variant="chat" content={content} onOpenFile={() => {}} />
    </LinkActionsContext>,
  );
}

describe('a web link in a reply', () => {
  it('opens in the computer\'s browser from its right-click menu', () => {
    const opener = vi.fn();
    show('See [the app](https://app.example.com/).', opener);
    fireEvent.contextMenu(screen.getByRole('link', { name: 'the app' }));
    expect(screen.getByRole('menuitem', { name: 'Open in new tab' })).toBeInTheDocument();
    fireEvent.click(screen.getByRole('menuitem', { name: 'Open in agent browser' }));
    expect(opener).toHaveBeenCalledWith('https://app.example.com/');
  });

  it('keeps the browser\'s own menu where there is no computer browser', () => {
    show('[the app](https://app.example.com/)', null);
    fireEvent.contextMenu(screen.getByRole('link', { name: 'the app' }));
    expect(screen.queryByRole('menu')).toBeNull();
  });

  it('hands a click to the chat\'s share-link opener from the same context', () => {
    const openShareLink = vi.fn(() => true);
    show('[the app](https://app.example.com/a/code1)', null, openShareLink);
    const link = screen.getByRole('link', { name: 'the app' });
    expect(fireEvent.click(link)).toBe(false);
    expect(openShareLink).toHaveBeenCalledWith('https://app.example.com/a/code1');
  });

  it('offers nothing for a mail link', () => {
    show('[write](mailto:a@example.com)', vi.fn());
    fireEvent.contextMenu(screen.getByRole('link', { name: 'write' }));
    expect(screen.queryByRole('menu')).toBeNull();
  });
});
