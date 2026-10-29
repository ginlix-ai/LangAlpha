import { describe, it, expect, beforeAll } from 'vitest';
import { render, screen } from '@testing-library/react';

import i18n from '@/i18n';
import type { ComputerSpecChange } from '@/types/api';

import { SpecChangeProgress } from '../specChangeUi';

const upgrading: ComputerSpecChange = {
  target_tier: 'performance',
  from_tier: 'standard',
  state: 'in_progress',
  started_at: '2026-09-25T18:00:00Z',
  error: null,
};

beforeAll(async () => {
  await i18n.changeLanguage('en-US');
});

describe('SpecChangeProgress', () => {
  it('announces the change once, without the loader glyph', () => {
    render(<SpecChangeProgress change={upgrading} />);
    const [status, ...rest] = screen.getAllByRole('status');
    expect(rest).toHaveLength(0);
    expect(status).toHaveTextContent('Upgrading to Medium…');
  });

  it('stays silent as a label, loader included', () => {
    render(<SpecChangeProgress change={upgrading} asLabel />);
    expect(screen.queryByRole('status')).toBeNull();
    expect(screen.getByText('Upgrading to Medium…')).toBeInTheDocument();
  });

  it.each([
    ['max', 'xlarge', 'Upgrading to Extra Large…'],
    ['xlarge', 'max', 'Changing spec to Large…'],
  ] as const)('reads %s to %s on the full ladder', (from_tier, target_tier, label) => {
    render(<SpecChangeProgress change={{ ...upgrading, from_tier, target_tier }} />);
    expect(screen.getByRole('status')).toHaveTextContent(label);
  });
});
