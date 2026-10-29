import { Suspense } from 'react';
import { describe, it, expect, vi } from 'vitest';
import { act, render, screen } from '@testing-library/react';
import { PageIntroModal } from '../engine/PageIntroModal';
import { loadIntroScenes } from '../engine/introScenes';
import { PAGE_INTROS } from '../registry';

// Records which page's scene module was imported. Each one is its own chunk in
// the build, so an intro that imports only its own fetches only its own.
const imported = vi.hoisted(() => new Set<string>());
vi.mock('../engine/introScenes/chat', async (load) => (imported.add('chat'), load()));
vi.mock('../engine/introScenes/share', async (load) => (imported.add('share'), load()));
vi.mock('../engine/introScenes/workspaces', async (load) => (imported.add('workspaces'), load()));
vi.mock('../engine/introScenes/threadFiles', async (load) => (imported.add('threadFiles'), load()));
vi.mock('../engine/introScenes/threadChart', async (load) => (imported.add('threadChart'), load()));
vi.mock('../engine/introScenes/thread', async (load) => (imported.add('thread'), load()));
vi.mock('../engine/introScenes/dashboard', async (load) => (imported.add('dashboard'), load()));
vi.mock('../engine/introScenes/market', async (load) => (imported.add('market'), load()));
vi.mock('../engine/introScenes/automations', async (load) => (imported.add('automations'), load()));

const byId = (id: string) => PAGE_INTROS.find((i) => i.id === id)!;

describe('intro scenes', () => {
  it('load with the intro that plays them, and no other page’s', async () => {
    expect([...imported]).toEqual([]);
    const market = byId('market');
    // One awaited act holds the render, its suspension and the retry once the
    // load it suspended on is in.
    await act(async () => {
      render(
        <Suspense fallback={null}>
          <PageIntroModal intro={market} onClose={vi.fn()} onAbandon={vi.fn()} />
        </Suspense>
      );
      await loadIntroScenes(market.steps.map((s) => s.visual));
    });
    expect(screen.getByTestId('intro-illustration')).toBeInTheDocument();
    expect([...imported]).toEqual(['market']);
  });

  // The share step opens on the chat intro's Analyst thread, and takes only
  // that thread, not the chat intro's other scenes.
  it('give the thread intro its own share scene, without the chat intro’s', async () => {
    imported.clear();
    const thread = byId('thread');
    await act(async () => {
      render(
        <Suspense fallback={null}>
          <PageIntroModal intro={thread} onClose={vi.fn()} onAbandon={vi.fn()} />
        </Suspense>
      );
      await loadIntroScenes(thread.steps.map((s) => s.visual));
    });
    expect(screen.getByTestId('intro-illustration')).toBeInTheDocument();
    expect([...imported].sort()).toEqual(['share', 'thread', 'threadChart', 'threadFiles']);
  });

  // The attach step's scene has a file of its own, but plays from the
  // dashboard record, so the intro fetches the one module.
  it('give the dashboard intro its three scenes from one module', async () => {
    imported.clear();
    const dashboard = byId('dashboard');
    await act(async () => {
      render(
        <Suspense fallback={null}>
          <PageIntroModal intro={dashboard} onClose={vi.fn()} onAbandon={vi.fn()} />
        </Suspense>
      );
      await loadIntroScenes(dashboard.steps.map((s) => s.visual));
    });
    expect(screen.getByTestId('intro-illustration')).toBeInTheDocument();
    expect([...imported]).toEqual(['dashboard']);
  });
});
