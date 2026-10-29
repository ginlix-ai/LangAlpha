import { Suspense } from 'react';
import { describe, it, expect, vi, beforeAll } from 'vitest';
import { act, render, screen, fireEvent } from '@testing-library/react';
import i18n from '@/i18n';
import { PageIntroModal } from '../engine/PageIntroModal';
import { INTRO_VISUALS, loadIntroScenes } from '../engine/introScenes';
import { PAGE_INTROS, type PageIntroDef } from '../registry';

const intro: PageIntroDef = {
  id: 'chat',
  matchRoute: () => true,
  steps: [
    { id: 's1', visual: 'workspaceGrid' },
    { id: 's2', visual: 'chiefOfStaffAnswer' },
    { id: 's3', visual: 'ptcSandbox' },
  ],
};

// The modal waits for its intro's scenes and copy; with every scene in, it
// opens once the load it suspends on settles. Its copy comes from the
// `intros` namespace, which test/setup.ts hands over with the rest; these
// steps have none, so their keys render as written.
beforeAll(() => loadIntroScenes(INTRO_VISUALS));

/** Renders the modal, and lets it suspend on its load and retry once that is in. */
async function renderIntro(shown: PageIntroDef = intro, onClose = vi.fn(), onAbandon = vi.fn()) {
  await act(async () => {
    render(
      <Suspense fallback={null}>
        <PageIntroModal intro={shown} onClose={onClose} onAbandon={onAbandon} />
      </Suspense>
    );
  });
  return { onClose, onAbandon };
}

async function openIntro(shown: PageIntroDef = intro, onClose = vi.fn()) {
  await renderIntro(shown, onClose);
  expect(screen.getByRole('dialog')).toBeInTheDocument();
  return onClose;
}

describe('PageIntroModal', () => {
  it('renders the first step with progress and the illustration panel', async () => {
    await openIntro();
    expect(screen.getByText('chat.s1.title')).toBeInTheDocument();
    expect(screen.getByText('chat.s1.body')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Step 1' })).toHaveAttribute('aria-current', 'step');
    expect(screen.getByTestId('intro-illustration')).toBeInTheDocument();
    // no Back on the first step
    expect(screen.queryByRole('button', { name: 'Back' })).toBeNull();
    // Opening focus sits on Continue, so Enter walks the tour.
    expect(screen.getByRole('button', { name: 'Continue' })).toHaveFocus();
  });

  // step copy swaps behind an exit animation — await the incoming title
  it('Continue advances through steps and Back returns', async () => {
    await openIntro();
    fireEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(await screen.findByText('chat.s2.title')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Step 2' })).toHaveAttribute('aria-current', 'step');
    fireEvent.click(screen.getByRole('button', { name: 'Back' }));
    expect(await screen.findByText('chat.s1.title')).toBeInTheDocument();
  });

  it('step dots jump directly to a stage', async () => {
    await openIntro();
    fireEvent.click(screen.getByRole('button', { name: 'Step 3' }));
    expect(await screen.findByText('chat.s3.title')).toBeInTheDocument();
  });

  it('intermediate steps never close — only the last CTA does', async () => {
    const onClose = await openIntro();
    fireEvent.click(screen.getByRole('button', { name: 'Continue' }));
    fireEvent.click(screen.getByRole('button', { name: 'Continue' }));
    expect(onClose).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: 'Start exploring' }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  // When every step anchors its visual right (the thread intro), the visual
  // panel takes the LEFT half: copy gets sm:order-2, illustration sm:order-1.
  it('a fully right-anchored intro puts the visual panel first', async () => {
    const rightIntro: PageIntroDef = {
      id: 'thread',
      matchRoute: () => true,
      steps: [
        { id: 'a', visual: 'filePanel' },
        { id: 'b', visual: 'memory' },
      ],
    };
    await openIntro(rightIntro);
    expect(screen.getByTestId('intro-illustration').className).toMatch(/sm:order-1/);
  });

  // Every scene of the thread intro, the last one included, is drawn on a
  // thread page whose controls sit on the right; one step anchored elsewhere
  // would swap the whole intro's sides.
  it('the thread intro, as registered, puts the visual panel first', async () => {
    await openIntro(PAGE_INTROS.find((i) => i.id === 'thread')!);
    expect(screen.getByTestId('intro-illustration').className).toMatch(/sm:order-1/);
  });

  it("reads the steps' copy from the intros namespace", async () => {
    await openIntro(PAGE_INTROS.find((i) => i.id === 'market')!);
    expect(screen.getByText(i18n.t('market.ask.title', { ns: 'intros' }))).toBeInTheDocument();
    expect(screen.getByText(i18n.t('market.ask.body', { ns: 'intros' }))).toBeInTheDocument();
    expect(screen.queryByText('market.ask.title')).toBeNull();
  });

  it('waits for its copy before it opens', async () => {
    let copyIn!: () => void;
    const loadNamespaces = vi
      .spyOn(i18n, 'loadNamespaces')
      .mockReturnValueOnce(new Promise<void>((resolve) => (copyIn = resolve)));
    try {
      // An id of its own: the load is kept per intro and language.
      await renderIntro({ ...intro, id: 'awaitsCopy' });
      expect(screen.queryByRole('dialog')).toBeNull();
      await act(async () => copyIn());
      expect(screen.getByRole('dialog')).toBeInTheDocument();
    } finally {
      loadNamespaces.mockRestore();
    }
  });

  it('shows nothing when its copy did not load, and hands the screen back', async () => {
    const lng = i18n.language;
    const copy = i18n.getResourceBundle(lng, 'intros');
    i18n.removeResourceBundle(lng, 'intros');
    try {
      const market = PAGE_INTROS.find((i) => i.id === 'market')!;
      const { onClose, onAbandon } = await renderIntro({ ...market, id: 'copyMissing' });
      expect(onAbandon).toHaveBeenCalledTimes(1);
      expect(screen.queryByRole('dialog')).toBeNull();
      expect(onClose).not.toHaveBeenCalled();
    } finally {
      i18n.addResourceBundle(lng, 'intros', copy);
    }
  });

  it('left-anchored intros keep the copy panel first', async () => {
    await openIntro();
    expect(screen.getByTestId('intro-illustration').className).not.toMatch(/sm:order-1/);
  });

  it('the dialog X closes at any step — every close path marks seen', async () => {
    const onClose = await openIntro();
    fireEvent.click(screen.getByRole('button', { name: 'Continue' }));
    fireEvent.click(screen.getByRole('button', { name: 'Close' }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
