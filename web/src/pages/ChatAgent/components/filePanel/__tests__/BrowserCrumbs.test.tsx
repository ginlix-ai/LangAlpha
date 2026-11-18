import { describe, expect, it, vi } from 'vitest';
import { act, fireEvent, screen, waitFor } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { BrowserNavigateRefusal } from '../../../utils/api/sandbox';
import { BrowserCrumbs } from '../BrowserCrumbs';
import type { BrowserControl, BrowserViewSession, BrowserViewState } from '../browserStream';

const toast = vi.fn();
vi.mock('@/components/ui/use-toast', () => ({ toast: (...args: unknown[]) => toast(...args) }));

function show(control: BrowserControl, pageUrl: string, refusal: BrowserNavigateRefusal | null = null) {
  const state: BrowserViewState = { phase: 'live', pageUrl, control, frame: null, agent: null };
  const view = {
    subscribe: () => () => {},
    getSnapshot: () => state,
    navigate: vi.fn(async () => refusal),
    takeControl: vi.fn(),
    releaseControl: vi.fn(),
    retry: vi.fn(),
  };
  renderWithProviders(<BrowserCrumbs view={view as unknown as BrowserViewSession} computer={null} />);
  return { view, field: screen.getByRole('textbox', { name: 'Address' }) };
}

describe('the browser toolbar', () => {
  it('shows the page while someone watches, and lets them go nowhere', () => {
    const { field } = show('none', 'https://example.com/');
    expect(field).toHaveValue('https://example.com/');
    expect(field).toHaveAttribute('readonly');
    expect(screen.getByRole('button', { name: 'Back' })).toBeDisabled();
  });

  it('opens what the person holding the page types', async () => {
    const { view, field } = show('held', 'about:blank');
    expect(field).toHaveValue('');
    fireEvent.focus(field);
    fireEvent.change(field, { target: { value: 'example.com' } });
    fireEvent.keyDown(field, { key: 'Enter' });
    await waitFor(() => expect(view.navigate).toHaveBeenCalledWith({ action: 'open', url: 'example.com' }));
    fireEvent.click(screen.getByRole('button', { name: 'Back' }));
    expect(view.navigate).toHaveBeenLastCalledWith({ action: 'back' });
  });

  it('leaves the Enter that commits an input method\'s text to it', () => {
    const { view, field } = show('held', 'about:blank');
    fireEvent.focus(field);
    fireEvent.change(field, { target: { value: 'example' } });
    fireEvent.keyDown(field, { key: 'Enter', keyCode: 229 });
    expect(view.navigate).not.toHaveBeenCalled();
    expect(field).toHaveValue('example');
  });

  it('keeps a refused address through leaving the field and coming back', async () => {
    let refuse: (refusal: BrowserNavigateRefusal) => void = () => {};
    const { view, field } = show('held', 'https://example.com/');
    view.navigate.mockImplementationOnce(() => new Promise((resolve) => { refuse = resolve; }));
    fireEvent.focus(field);
    fireEvent.change(field, { target: { value: 'exampel.com' } });
    fireEvent.keyDown(field, { key: 'Enter' });
    fireEvent.blur(field);
    await act(async () => { refuse('failed'); });
    fireEvent.focus(field);
    expect(field).toHaveValue('exampel.com');
  });

  it('says why an address did not open, and keeps it to fix', async () => {
    const { field } = show('held', 'about:blank', 'bad_address');
    fireEvent.focus(field);
    fireEvent.change(field, { target: { value: 'file:///etc/passwd' } });
    fireEvent.keyDown(field, { key: 'Enter' });
    await waitFor(() =>
      expect(toast).toHaveBeenCalledWith(expect.objectContaining({ description: 'Enter a web address, like example.com.' })),
    );
    expect(field).toHaveValue('file:///etc/passwd');
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
