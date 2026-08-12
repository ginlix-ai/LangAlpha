import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { PageIntroModal } from '../engine/PageIntroModal';
import type { PageIntroDef } from '../registry';

const intro: PageIntroDef = {
  id: 'chat',
  matchRoute: () => true,
  steps: [
    { id: 's1', titleKey: 'title-1', bodyKey: 'body-1', visual: 'workspaceGrid' },
    { id: 's2', titleKey: 'title-2', bodyKey: 'body-2', visual: 'flashAnswer' },
    { id: 's3', titleKey: 'title-3', bodyKey: 'body-3', visual: 'ptcSandbox' },
  ],
};

describe('PageIntroModal', () => {
  it('renders the first step with progress and the illustration panel', () => {
    render(<PageIntroModal intro={intro} onClose={vi.fn()} />);
    expect(screen.getByText('title-1')).toBeInTheDocument();
    expect(screen.getByText('body-1')).toBeInTheDocument();
    expect(screen.getByText('1 / 3')).toBeInTheDocument();
    expect(screen.getByTestId('intro-illustration')).toBeInTheDocument();
    // no Back on the first step
    expect(screen.queryByRole('button', { name: 'Back' })).toBeNull();
  });

  // step copy swaps behind an exit animation — await the incoming title
  it('Continue advances through steps and Back returns', async () => {
    render(<PageIntroModal intro={intro} onClose={vi.fn()} />);
    fireEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(await screen.findByText('title-2')).toBeInTheDocument();
    expect(screen.getByText('2 / 3')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Back' }));
    expect(await screen.findByText('title-1')).toBeInTheDocument();
  });

  it('step dots jump directly to a stage', async () => {
    render(<PageIntroModal intro={intro} onClose={vi.fn()} />);
    fireEvent.click(screen.getByRole('button', { name: 'Step 3' }));
    expect(await screen.findByText('title-3')).toBeInTheDocument();
  });

  it('intermediate steps never close — only the last CTA does', () => {
    const onClose = vi.fn();
    render(<PageIntroModal intro={intro} onClose={onClose} />);
    fireEvent.click(screen.getByRole('button', { name: 'Continue' }));
    fireEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(onClose).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: 'Start exploring' }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  // When every step anchors its visual right (the thread intro), the visual
  // panel takes the LEFT half: copy gets sm:order-2, illustration sm:order-1.
  it('a fully right-anchored intro puts the visual panel first', () => {
    const rightIntro: PageIntroDef = {
      id: 'thread',
      matchRoute: () => true,
      steps: [
        { id: 'a', titleKey: 'title-1', bodyKey: 'body-1', visual: 'filePanel' },
        { id: 'b', titleKey: 'title-2', bodyKey: 'body-2', visual: 'memory' },
      ],
    };
    render(<PageIntroModal intro={rightIntro} onClose={vi.fn()} />);
    expect(screen.getByTestId('intro-illustration').className).toMatch(/sm:order-1/);
  });

  it('left-anchored intros keep the copy panel first', () => {
    render(<PageIntroModal intro={intro} onClose={vi.fn()} />);
    expect(screen.getByTestId('intro-illustration').className).not.toMatch(/sm:order-1/);
  });

  it('the dialog X closes at any step — every close path marks seen', () => {
    const onClose = vi.fn();
    render(<PageIntroModal intro={intro} onClose={onClose} />);
    fireEvent.click(screen.getByRole('button', { name: 'Continue' }));
    fireEvent.click(screen.getByRole('button', { name: 'Close' }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
