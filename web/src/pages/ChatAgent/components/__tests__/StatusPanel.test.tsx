import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import StatusPanel from '../StatusPanel';

// i18n runs against the real en-US catalog (initialized in test setup), so
// assert on stable substrings — symbols, invented prices, and hint copy.
// Neutral placeholder tickers and made-up prices only.
const CONTENT = ['As of 14:30:00 ET (Regular)', 'NVDA  178.34  +1.20%', 'TSLA  402.11  -0.85%'].join('\n');

describe('StatusPanel', () => {
  it('renders the watched symbols', () => {
    render(<StatusPanel marketWatch={{ symbols: ['NVDA', 'TSLA'] }} />);
    expect(screen.getByText('NVDA')).toBeInTheDocument();
    expect(screen.getByText('TSLA')).toBeInTheDocument();
  });

  it('renders the quote block — header line plus quote rows — when content is present', () => {
    render(<StatusPanel marketWatch={{ symbols: ['NVDA', 'TSLA'], content: CONTENT }} />);
    // First line is the muted "As of …" header.
    expect(screen.getByText(/As of 14:30:00 ET/)).toBeInTheDocument();
    // Remaining lines are the quote rows (invented prices).
    expect(screen.getByText(/178\.34/)).toBeInTheDocument();
    expect(screen.getByText(/402\.11/)).toBeInTheDocument();
  });

  it('shows the waiting hint (and no updated caption) when content is absent', () => {
    render(<StatusPanel marketWatch={{ symbols: ['NVDA'] }} />);
    expect(screen.getByText(/Live prices appear here/)).toBeInTheDocument();
    expect(screen.queryByText(/Updated /)).not.toBeInTheDocument();
  });

  it('shows an updated-at caption when timestamp is set', () => {
    render(<StatusPanel marketWatch={{ symbols: ['NVDA'], timestamp: 1_700_000_000 }} />);
    expect(screen.getByText(/Updated /)).toBeInTheDocument();
  });

  it('renders a defensive empty state when there are no symbols', () => {
    render(<StatusPanel marketWatch={{ symbols: [] }} />);
    expect(screen.getByText(/No tickers are being watched/)).toBeInTheDocument();
  });

  it('does not crash when marketWatch is null', () => {
    render(<StatusPanel marketWatch={null} />);
    expect(screen.getByText(/No tickers are being watched/)).toBeInTheDocument();
  });
});
