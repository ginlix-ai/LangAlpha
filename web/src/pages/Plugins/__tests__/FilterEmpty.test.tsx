import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { FilterEmpty } from '../components/FilterEmpty';

/**
 * A filtered-empty list has to say which of the two controls emptied it;
 * "No matches" leaves the user unable to tell a narrow filter from an empty
 * tab, and with nothing to click to find out.
 */

describe('FilterEmpty', () => {
  it('reads the query back', () => {
    render(<FilterEmpty noun="servers" filter="poly" stateFilter="all" onReset={vi.fn()} />);
    expect(screen.getByText('No servers match "poly".')).toBeInTheDocument();
  });

  it('names the state pill when that is what narrowed the list', () => {
    render(<FilterEmpty noun="skills" filter="" stateFilter="attention" onReset={vi.fn()} />);
    expect(screen.getByText('Nothing needs attention.')).toBeInTheDocument();
  });

  it('carries both when both are narrowing', () => {
    render(<FilterEmpty noun="plugins" filter="  acme " stateFilter="on" onReset={vi.fn()} />);
    expect(screen.getByText('No plugins that are on match "acme".')).toBeInTheDocument();
  });

  it('offers the one control that undoes it', () => {
    const onReset = vi.fn();
    render(<FilterEmpty noun="plugins" filter="acme" stateFilter="off" onReset={onReset} />);

    expect(screen.getByText('No plugins that are off match "acme".')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Clear search' }));
    expect(onReset).toHaveBeenCalled();
  });

  it('names the pill as what to undo when no search is narrowing', () => {
    const onReset = vi.fn();
    render(<FilterEmpty noun="plugins" filter="" stateFilter="off" onReset={onReset} />);

    fireEvent.click(screen.getByRole('button', { name: 'Show all' }));
    expect(onReset).toHaveBeenCalled();
  });
});
