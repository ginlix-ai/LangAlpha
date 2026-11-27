import { describe, expect, it, vi } from 'vitest';
import { act, fireEvent, screen, waitFor } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { BrowserNavigateRefusal } from '../../../utils/api/sandbox';
import { BrowserCrumbs } from '../BrowserCrumbs';
import type { BrowserPhase } from '../browserPhase';
import type { BrowserControl, BrowserViewSession, BrowserViewState, BrowserVisitRefusal } from '../browserStream';

const toast = vi.fn();
vi.mock('@/components/ui/use-toast', () => ({ toast: (...args: unknown[]) => toast(...args) }));

function show(
  control: BrowserControl,
  pageUrl: string,
  refusal: BrowserVisitRefusal | null = null,
  phase: BrowserPhase = 'live',
) {
  const state: BrowserViewState = { phase, pageUrl, control, frame: null, agent: null, visiting: null };
  const view = {
    subscribe: () => () => {},
    getSnapshot: () => state,
    navigate: vi.fn(async (): Promise<BrowserNavigateRefusal | null> => null),
    visit: vi.fn(async (_url: string) => refusal),
    takeControl: vi.fn(),
    releaseControl: vi.fn(),
    retry: vi.fn(),
  };
  renderWithProviders(<BrowserCrumbs view={view as unknown as BrowserViewSession} computer={null} />);
  return { view, field: screen.getByRole('textbox', { name: 'Address' }) };
}

const type = (field: HTMLElement, value: string) => {
  fireEvent.focus(field);
  fireEvent.change(field, { target: { value } });
  fireEvent.keyDown(field, { key: 'Enter' });
};

describe('the browser toolbar', () => {
  it('shows the page while someone watches, and steps only for the person holding it', () => {
    const { field } = show('none', 'https://example.com/');
    expect(field).toHaveValue('https://example.com/');
    expect(field).not.toHaveAttribute('readonly');
    expect(screen.getByRole('button', { name: 'Back' })).toBeDisabled();
  });

  it('takes the person where they type, holding the page or not', async () => {
    const { view, field } = show('none', 'about:blank');
    expect(field).toHaveValue('');
    type(field, 'example.com');
    // A visit takes control on the way: typing an address is taking the browser.
    await waitFor(() => expect(view.visit).toHaveBeenCalledWith('example.com'));
    expect(view.takeControl).not.toHaveBeenCalled();
  });

  it('searches for what reads as no address', async () => {
    const { view, field } = show('held', 'about:blank');
    type(field, 'apple earnings date');
    await waitFor(() => expect(view.visit).toHaveBeenCalledWith('apple earnings date'));
  });

  it('steps back for the person holding the page', () => {
    const { view } = show('held', 'https://example.com/');
    fireEvent.click(screen.getByRole('button', { name: 'Back' }));
    expect(view.navigate).toHaveBeenLastCalledWith({ action: 'back' });
  });

  it('leaves the Enter that commits an input method\'s text to it', () => {
    const { view, field } = show('held', 'about:blank');
    fireEvent.focus(field);
    fireEvent.change(field, { target: { value: 'example' } });
    fireEvent.keyDown(field, { key: 'Enter', keyCode: 229 });
    expect(view.visit).not.toHaveBeenCalled();
    expect(field).toHaveValue('example');
  });

  it('keeps a refused address through leaving the field and coming back', async () => {
    let refuse: (refusal: BrowserVisitRefusal) => void = () => {};
    const { view, field } = show('held', 'https://example.com/');
    view.visit.mockImplementationOnce(() => new Promise((resolve) => { refuse = resolve; }));
    type(field, 'exampel.com');
    fireEvent.blur(field);
    await act(async () => { refuse('failed'); });
    fireEvent.focus(field);
    expect(field).toHaveValue('exampel.com');
  });

  it('says why an address did not open, and keeps it to fix', async () => {
    const { field } = show('held', 'about:blank', 'bad_address');
    type(field, 'file:///etc/passwd');
    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith(expect.objectContaining({ description: 'Enter a web address, like example.com.' })),
    );
    expect(field).toHaveValue('file:///etc/passwd');
  });

  it('says the browser is not open when it closed under a visit that found it live', async () => {
    const { field } = show('held', 'https://example.com/', 'not_running');
    type(field, 'example.org');
    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith(expect.objectContaining({ description: "The browser isn't open" })),
    );
  });

  it('leaves a browser that could not be opened to the tab, which says why', async () => {
    toast.mockClear();
    const { view, field } = show('none', 'about:blank', 'not_running', 'idle');
    type(field, 'example.org');
    await waitFor(() => expect(view.visit).toHaveBeenCalled());
    await act(async () => {});
    expect(toast).not.toHaveBeenCalled();
  });

  it('keeps an address refused while the field had focus once the field is left', async () => {
    const { field } = show('held', 'https://example.com/', 'failed');
    fireEvent.focus(field);
    fireEvent.change(field, { target: { value: 'exampel.com' } });
    await act(async () => { fireEvent.keyDown(field, { key: 'Enter' }); });
    fireEvent.blur(field);
    fireEvent.focus(field);
    expect(field).toHaveValue('exampel.com');
    // An edit after the refusal is an ordinary draft, which leaving drops.
    fireEvent.change(field, { target: { value: 'example.org' } });
    fireEvent.blur(field);
    expect(field).toHaveValue('https://example.com/');
  });

  it('reloads the page and hands it back for the person holding it', () => {
    const { view } = show('held', 'https://example.com/a');
    fireEvent.click(screen.getByRole('button', { name: 'Reload' }));
    expect(view.navigate).toHaveBeenLastCalledWith({ action: 'reload' });
    fireEvent.click(screen.getByRole('button', { name: 'Hand back' }));
    expect(view.releaseControl).toHaveBeenCalled();
  });

  it('draws the site apart from the rest, and opens a web page in the person\'s own browser', () => {
    show('none', 'https://news.example.com/a/b?c=1');
    expect(screen.getByText('news.example.com')).toBeInTheDocument();
    expect(screen.getByText('/a/b?c=1')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Open in your browser' })).toHaveAttribute('href', 'https://news.example.com/a/b?c=1');
  });

  it('keeps a page on the computer itself out of the person\'s browser, where it is not', () => {
    show('none', 'http://localhost:3000/');
    expect(screen.queryByRole('link', { name: 'Open in your browser' })).toBeNull();
  });
});
