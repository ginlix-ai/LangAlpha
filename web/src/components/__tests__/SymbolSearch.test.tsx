import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, render, screen } from '@testing-library/react';

const { searchStocks } = vi.hoisted(() => ({ searchStocks: vi.fn() }));
vi.mock('@/lib/marketUtils', async (importOriginal) => {
  const orig = await importOriginal<Record<string, unknown>>();
  return { ...orig, searchStocks };
});

import { SymbolSearch } from '../SymbolSearch';
import { SYMBOL_SEARCH_DEBOUNCE_MS } from '@/hooks/useSymbolSearch';

/** The rest the field waits before it searches, plus the response landing. */
const rest = () => act(async () => { await vi.advanceTimersByTimeAsync(SYMBOL_SEARCH_DEBOUNCE_MS); });

beforeEach(() => {
  vi.useFakeTimers();
  vi.clearAllMocks();
  searchStocks.mockResolvedValue({ query: 'goog', results: [{ symbol: 'GOOGL', name: 'Alphabet Inc.' }], count: 1 });
});

afterEach(() => {
  vi.useRealTimers();
});

describe('SymbolSearch', () => {
  it('picks a hit by click', async () => {
    const onPick = vi.fn();
    render(<SymbolSearch onPick={onPick} />);
    fireEvent.change(screen.getByRole('combobox'), { target: { value: 'goog' } });
    await rest();
    expect(searchStocks).toHaveBeenCalledWith('goog', 8, { signal: expect.any(AbortSignal) });
    fireEvent.click(screen.getByText('Alphabet Inc.'));
    expect(onPick).toHaveBeenCalledWith('GOOGL', expect.objectContaining({ name: 'Alphabet Inc.' }));
  });

  it('takes the highlighted hit on Enter', async () => {
    const onPick = vi.fn();
    render(<SymbolSearch onPick={onPick} />);
    const input = screen.getByRole('combobox');
    fireEvent.change(input, { target: { value: 'goog' } });
    await rest();
    screen.getByText('Alphabet Inc.');
    fireEvent.keyDown(input, { key: 'Enter' });
    expect(onPick).toHaveBeenCalledWith('GOOGL', expect.objectContaining({ name: 'Alphabet Inc.' }));
  });

  it('opens a typed ticker on Enter when the search has nothing', async () => {
    searchStocks.mockResolvedValue({ query: 'nvda', results: [], count: 0 });
    const onPick = vi.fn();
    render(<SymbolSearch onPick={onPick} />);
    const input = screen.getByRole('combobox');
    fireEvent.change(input, { target: { value: 'nvda' } });
    // Not before the search answers: a hit may still be on its way.
    fireEvent.keyDown(input, { key: 'Enter' });
    expect(onPick).not.toHaveBeenCalled();
    await rest();
    expect(searchStocks).toHaveBeenCalledWith('nvda', 8, { signal: expect.any(AbortSignal) });
    screen.getByText('Press Enter to open NVDA');
    fireEvent.keyDown(input, { key: 'Enter' });
    expect(onPick).toHaveBeenCalledWith('NVDA');
  });

  it('reads a numbered ticker as one, and hands every symbol over uppercase', async () => {
    searchStocks.mockResolvedValue({ query: '0700.hk', results: [], count: 0 });
    const onPick = vi.fn();
    render(<SymbolSearch onPick={onPick} />);
    const input = screen.getByRole('combobox');
    fireEvent.change(input, { target: { value: '0700.hk' } });
    await rest();
    fireEvent.keyDown(input, { key: 'Enter' });
    expect(onPick).toHaveBeenCalledWith('0700.HK');

    onPick.mockClear();
    searchStocks.mockResolvedValue({ query: 'brk', results: [{ symbol: 'brk.b', name: 'Berkshire' }], count: 1 });
    fireEvent.change(input, { target: { value: 'brk' } });
    await rest();
    fireEvent.click(screen.getByText('Berkshire'));
    expect(onPick).toHaveBeenCalledWith('BRK.B', expect.objectContaining({ name: 'Berkshire' }));
  });

  it('does not pick a company name typed as a ticker', async () => {
    searchStocks.mockResolvedValue({ query: 'alphabet inc', results: [], count: 0 });
    const onPick = vi.fn();
    render(<SymbolSearch onPick={onPick} />);
    const input = screen.getByRole('combobox');
    fireEvent.change(input, { target: { value: 'alphabet inc' } });
    await rest();
    expect(searchStocks).toHaveBeenCalledWith('alphabet inc', 8, { signal: expect.any(AbortSignal) });
    screen.getByText('No matches');
    fireEvent.keyDown(input, { key: 'Enter' });
    expect(onPick).not.toHaveBeenCalled();
  });

  it('does not take the outgoing query\'s hit on Enter inside the debounce', async () => {
    const onPick = vi.fn();
    render(<SymbolSearch onPick={onPick} />);
    const input = screen.getByRole('combobox');
    fireEvent.change(input, { target: { value: 'goog' } });
    await rest();
    screen.getByText('Alphabet Inc.');

    searchStocks.mockResolvedValue({ query: 'nvda', results: [{ symbol: 'NVDA', name: 'NVIDIA Corp' }], count: 1 });
    fireEvent.change(input, { target: { value: 'nvda' } });
    // The row stays up through the refetch on purpose, and it still reads GOOGL.
    screen.getByText('Alphabet Inc.');
    fireEvent.keyDown(input, { key: 'Enter' });
    expect(onPick).not.toHaveBeenCalled();

    await rest();
    fireEvent.keyDown(input, { key: 'Enter' });
    expect(onPick).toHaveBeenCalledWith('NVDA', expect.objectContaining({ name: 'NVIDIA Corp' }));
  });

  it('does not take the outgoing query\'s hit on Enter while the replacement is in flight', async () => {
    const onPick = vi.fn();
    render(<SymbolSearch onPick={onPick} />);
    const input = screen.getByRole('combobox');
    fireEvent.change(input, { target: { value: 'goog' } });
    await rest();
    screen.getByText('Alphabet Inc.');

    let answer: (res: unknown) => void = () => {};
    searchStocks.mockReturnValue(new Promise((resolve) => { answer = resolve; }));
    fireEvent.change(input, { target: { value: 'nvda' } });
    // Past the rest, so the request is out; nothing has answered it yet.
    await rest();
    expect(searchStocks).toHaveBeenCalledWith('nvda', 8, { signal: expect.any(AbortSignal) });
    screen.getByText('Alphabet Inc.');
    fireEvent.keyDown(input, { key: 'Enter' });
    expect(onPick).not.toHaveBeenCalled();

    await act(async () => {
      answer({ query: 'nvda', results: [{ symbol: 'NVDA', name: 'NVIDIA Corp' }], count: 1 });
    });
    fireEvent.keyDown(input, { key: 'Enter' });
    expect(onPick).toHaveBeenCalledWith('NVDA', expect.objectContaining({ name: 'NVIDIA Corp' }));
  });

  it('points the input at the highlighted option', async () => {
    render(<SymbolSearch onPick={() => {}} />);
    const input = screen.getByRole('combobox');
    fireEvent.change(input, { target: { value: 'goog' } });
    await rest();
    const option = screen.getByRole('option');
    expect(input).toHaveAttribute('aria-activedescendant', option.id);
    expect(input).toHaveAttribute('aria-expanded', 'true');
  });

  it('keeps the empty row out of the option list', async () => {
    searchStocks.mockResolvedValue({ query: 'alphabet inc', results: [], count: 0 });
    render(<SymbolSearch onPick={() => {}} />);
    fireEvent.change(screen.getByRole('combobox'), { target: { value: 'alphabet inc' } });
    await rest();
    expect(screen.queryAllByRole('option')).toHaveLength(0);
    expect(screen.getByText('No matches')).toHaveAttribute('role', 'presentation');
  });
});
