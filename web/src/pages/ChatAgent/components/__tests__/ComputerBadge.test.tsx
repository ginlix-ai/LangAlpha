/**
 * The computer badge names the kind of machine, and the size only where the
 * app sets one: a Mac or SSH box the user brings has no tier, whatever its row
 * carries.
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';

import type { Computer, ComputerSsh } from '@/types/api';

import { ComputerBadge } from '../ComputerBadge';

const SSH: ComputerSsh = {
  host: 'gpu-box.lab.internal',
  port: 22,
  username: 'alan',
  fingerprint: 'SHA256:gpubox',
  tunnel: false,
  tunnel_connected: null,
  helper: false,
};

function computer(overrides: Partial<Computer>): Computer {
  return {
    computer_id: 'c-1',
    user_id: 'u-1',
    kind: 'daytona',
    name: 'Research box',
    status: 'running',
    resource_tier: 'standard',
    is_always_on: false,
    is_primary: true,
    root_dir: '/home/workspace',
    ssh: null,
    ...overrides,
  };
}

const badge = () => screen.getByTestId('computer-badge');

describe('ComputerBadge', () => {
  it.each([
    ['daytona', 'standard', 'Daytona', 'Small'],
    ['e2b', 'max', 'E2B', 'Large'],
    ['e2b', 'xlarge', 'E2B', 'Extra Large'],
    ['docker', 'performance', 'Docker', 'Medium'],
  ] as const)('names a hosted %s machine and its %s size', (kind, tier, provider, size) => {
    render(<ComputerBadge computer={computer({ kind, resource_tier: tier })} />);
    expect(badge()).toHaveAttribute('data-kind', 'hosted');
    expect(badge()).toHaveTextContent(`${provider}·${size}`);
  });

  it('shows a provider this build does not know as the server spells it, still sized', () => {
    render(<ComputerBadge computer={computer({ kind: 'modal', resource_tier: 'max' })} />);
    expect(badge()).toHaveTextContent('modal·Large');
  });

  it('reads the size a failed, interrupted spec change left the machine on', () => {
    render(
      <ComputerBadge
        computer={computer({
          kind: 'e2b',
          resource_tier: 'max',
          spec_change: {
            target_tier: 'max',
            from_tier: 'performance',
            state: 'failed',
            error: { code: 'interrupted', message: 'Interrupted', files: [] },
            started_at: '2026-10-20T00:00:00Z',
          },
        })}
      />,
    );
    expect(badge()).toHaveTextContent('E2B·Medium');
  });

  it.each([
    ['carries no tier', { resource_tier: undefined }],
    ['carries a tier this build does not know', { resource_tier: 'giant' }],
    ['was interrupted coming from a tier this build does not know', {
      resource_tier: 'max',
      spec_change: {
        target_tier: 'max',
        from_tier: 'giant',
        state: 'failed',
        error: { code: 'interrupted', message: 'Interrupted', files: [] },
        started_at: '2026-10-20T00:00:00Z',
      },
    }],
  ])('names a hosted machine without a size when its row %s', (_, overrides) => {
    // Small would be a guess, and the machine may be any size.
    render(<ComputerBadge computer={computer({ kind: 'daytona', ...(overrides as Partial<Computer>) })} />);
    expect(badge()).toHaveAttribute('data-kind', 'hosted');
    expect(badge()).toHaveTextContent(/^Daytona$/);
  });

  it('calls a plain SSH machine SSH, with no size even when its row carries a tier', () => {
    render(<ComputerBadge computer={computer({ kind: 'ssh', resource_tier: 'max', ssh: SSH })} />);
    expect(badge()).toHaveAttribute('data-kind', 'ssh');
    expect(badge()).toHaveTextContent(/^SSH$/);
  });

  it('calls a Mac under remote control "Mac" anywhere but on that Mac', () => {
    render(<ComputerBadge computer={computer({ kind: 'ssh', ssh: { ...SSH, helper: true, tunnel: true } })} />);
    expect(badge()).toHaveAttribute('data-kind', 'mac');
    expect(badge()).toHaveTextContent(/^Mac$/);
  });

  it('calls it "This Mac" in the desktop app running on it', () => {
    render(
      <ComputerBadge computer={computer({ kind: 'ssh', ssh: { ...SSH, helper: true, tunnel: true } })} isThisMac />,
    );
    expect(badge()).toHaveAttribute('data-kind', 'thisMac');
    expect(badge()).toHaveTextContent(/^This Mac$/);
  });

  it('never calls a hosted machine This Mac', () => {
    render(<ComputerBadge computer={computer({ kind: 'daytona' })} isThisMac />);
    expect(badge()).toHaveAttribute('data-kind', 'hosted');
    expect(badge()).toHaveTextContent('Daytona·Small');
  });

  describe('beside a name', () => {
    it.each([
      ['Daytona Small', 'daytona', 'standard', null],
      ['daytona small', 'daytona', 'standard', null],
      ['Daytona box', 'daytona', 'standard', 'Small'],
      ['Large E2B rig', 'e2b', 'max', null],
      ['Backtest rig', 'e2b', 'max', 'E2B·Large'],
      // The server's own default names say neither, so they keep both.
      ['Cloud computer', 'daytona', 'standard', 'Daytona·Small'],
      // A word inside a longer one is not the word.
      ['Smallish Daytonas', 'daytona', 'standard', 'Daytona·Small'],
    ] as const)('beside "%s" says only what the name does not', (name, kind, tier, expected) => {
      render(<ComputerBadge computer={computer({ kind, resource_tier: tier })} name={name} />);
      if (expected === null) expect(screen.queryByTestId('computer-badge')).toBeNull();
      else expect(badge()).toHaveTextContent(new RegExp(`^${expected}$`));
    });

    it('keeps "Mac" beside a name that only says MacBook', () => {
      render(
        <ComputerBadge computer={computer({ kind: 'ssh', ssh: { ...SSH, helper: true, tunnel: true } })} name="Alan's MacBook Pro" />,
      );
      expect(badge()).toHaveTextContent(/^Mac$/);
    });
  });

  it('draws the plain variant as quiet words after a separator, with no pill or glyph', () => {
    render(<ComputerBadge computer={computer({ kind: 'e2b', resource_tier: 'max' })} variant="plain" />);
    expect(badge()).toHaveTextContent(/^·E2B·Large$/);
    expect(badge()).not.toHaveClass('rounded-full');
    expect(badge().querySelector('svg')).toBeNull();
  });
});
