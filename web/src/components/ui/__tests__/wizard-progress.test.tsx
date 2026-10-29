/**
 * The progress rail drops its step list on a phone and shows a bare count. A
 * screen reader has no step header to fall back on, so the count has to be in
 * the accessible tree: it is the only place the position is told.
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { WizardProgress } from '../wizard';

describe('WizardProgress', () => {
  it('keeps the phone count readable by assistive tech', () => {
    render(<WizardProgress steps={['Plan', 'Chat apps', 'Model']} current={1} label="Progress" />);
    const count = screen.getByTestId('wizard-progress-count');
    expect(count.textContent).toBe('2 / 3');
    expect(count.closest('[aria-hidden="true"]')).toBeNull();
  });

  it('marks the current step and the ones behind it in the list', () => {
    render(<WizardProgress steps={['Plan', 'Chat apps', 'Model']} current={1} label="Progress" />);
    const items = screen.getByRole('list', { name: 'Progress' }).querySelectorAll('li');
    expect(items[1].getAttribute('aria-current')).toBe('step');
    expect(items[0].getAttribute('aria-current')).toBeNull();
  });
});
