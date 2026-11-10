import { act, fireEvent, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ReactElement } from 'react';

vi.mock('../../utils/api', () => ({ deleteChartLayer: vi.fn(), fetchChartLayers: vi.fn() }));
vi.mock('@/components/ui/use-toast', () => ({ useToast: () => ({ toast: toastMock }) }));

import { renderWithProviders } from '@/test/utils';
import { ChartComposerContext, createChartComposer } from '@/components/chart/chartComposer';
import { ChartLayerLegend } from '../ChartLayerLegend';
import { layerAccent } from '../../utils/layerDisplay';
import { getHiddenLayers, resetLayerViewForTesting } from '../../stores/layerViewStore';
import { makeLayer, makeStored } from '../../stores/__tests__/layerFixture';
import { deleteChartLayer, fetchChartLayers } from '../../utils/api';
import { chartKeyFor } from '../../utils/chartInstanceKey';
import { applyChartLayerEvent } from '../../utils/chartLayerCache';
import type { StoredChartLayer } from '../../types/chartLayer';

const toastMock = vi.fn();
const del = vi.mocked(deleteChartLayer);
const fetchLayers = vi.mocked(fetchChartLayers);
const WS = 'ws-1';
const KEY = chartKeyFor(WS, 'NVDA', '1day')!;

/** What the server lists; the legend's query reads it on mount. */
let server: StoredChartLayer[] = [];
function seed(n: number) {
  server = Array.from({ length: n }, (_, i) => makeStored({ id: `l${i}`, title: `Layer ${i}` }));
}

const hidden = { has: (id: string) => getHiddenLayers(KEY).has(id) };

function mount(ui: ReactElement = <ChartLayerLegend chartKey={KEY} />) {
  const out = renderWithProviders(ui);
  return { ...out, loaded: waitFor(() => expect(fetchLayers).toHaveBeenCalled()) };
}
const open = (name: string) => fireEvent.click(screen.getByRole('button', { name: new RegExp(name) }));

beforeEach(() => {
  server = [];
  resetLayerViewForTesting();
  toastMock.mockReset();
  del.mockReset().mockResolvedValue(undefined);
  fetchLayers.mockReset().mockImplementation(async () => server);
});
afterEach(() => resetLayerViewForTesting());

describe('ChartLayerLegend', () => {
  it('draws nothing without layers', async () => {
    const { container, loaded } = mount();
    await loaded;
    expect(container).toBeEmptyDOMElement();
  });

  it('shows a chip per layer with title and as-of, oldest first', async () => {
    seed(2);
    mount();
    await screen.findByRole('button', { name: /Layer 1/ });
    expect(screen.getAllByRole('button').map((c) => c.textContent)).toEqual(['Layer 0Oct 28', 'Layer 1Oct 28']);
  });

  it('shows only the layers of its own timeframe', async () => {
    server = [makeStored({ id: 'd', title: 'Daily' }), makeStored({ id: 'h', title: 'Hourly', timeframe: '1hour' })];
    mount();
    await screen.findByRole('button', { name: /Daily/ });
    expect(screen.queryByRole('button', { name: /Hourly/ })).toBeNull();
  });

  it('folds to one count chip past three layers, listing them in its popover', async () => {
    seed(4);
    mount();
    await screen.findByRole('button', { name: /4 layers/ });
    expect(screen.getAllByRole('button')).toHaveLength(1);
    open('4 layers');
    expect(screen.getByRole('dialog')).toHaveTextContent('Layer 3');
  });

  it('opens a popover with the summary on click', async () => {
    seed(1);
    mount();
    await screen.findByRole('button', { name: /Layer 0/ });
    open('Layer 0');
    const dialog = screen.getByRole('dialog');
    expect(dialog).toHaveTextContent('Price rides the lower rail');
    expect(dialog).toHaveTextContent('NVDA · 1D · as of Oct 28, 2026');
  });

  it('pins a hover-opened popover on a click of its chip, and closes it on a second', async () => {
    // The chip anchors the popover rather than triggering it, so a press on it reads
    // as outside, and Radix dismisses on the click that follows the press.
    seed(1);
    mount();
    const chip = await screen.findByRole('button', { name: /Layer 0/ });
    const press = async () => {
      // Radix starts listening for presses outside on the tick after it opens.
      await act(() => new Promise((r) => setTimeout(r, 20)));
      fireEvent.pointerDown(chip, { pointerType: 'mouse', button: 0 });
      fireEvent.pointerUp(chip, { pointerType: 'mouse', button: 0 });
      fireEvent.click(chip, { button: 0 });
      await act(() => new Promise((r) => setTimeout(r, 20)));
    };
    fireEvent.pointerEnter(chip, { pointerType: 'mouse' });
    await screen.findByRole('dialog', {}, { timeout: 2000 });
    await press();
    expect(screen.getByRole('dialog')).toBeInTheDocument();
    await press();
    expect(screen.queryByRole('dialog')).toBeNull();
  });

  it("hides and shows a layer as the viewer's own, keeping its chip", async () => {
    seed(1);
    mount();
    await screen.findByRole('button', { name: /Layer 0/ });
    open('Layer 0');
    fireEvent.click(screen.getByRole('button', { name: 'Hide layer' }));
    expect(hidden.has('l0')).toBe(true);
    fireEvent.click(screen.getByRole('button', { name: 'Show layer' }));
    expect(hidden.has('l0')).toBe(false);
  });

  it('removes at once and asks the server to delete it', async () => {
    seed(1);
    const { container } = mount();
    await screen.findByRole('button', { name: /Layer 0/ });
    open('Layer 0');
    del.mockImplementationOnce(async () => {
      server = [];
    });
    fireEvent.click(screen.getByRole('button', { name: 'Remove layer' }));
    await waitFor(() => expect(container).toBeEmptyDOMElement());
    expect(del).toHaveBeenCalledWith(WS, 'l0', 'NVDA', '1day');
  });

  it('puts the layer back in its place, still hidden, and says so when the server refuses', async () => {
    seed(3);
    mount();
    await screen.findByRole('button', { name: /Layer 2/ });
    open('Layer 1');
    fireEvent.click(screen.getByRole('button', { name: 'Hide layer' }));
    let refuse!: (err: Error) => void;
    del.mockReturnValueOnce(new Promise((_, reject) => { refuse = reject; }));
    fireEvent.click(screen.getByRole('button', { name: 'Remove layer' }));
    await waitFor(() => expect(screen.queryByRole('button', { name: /Layer 1/ })).toBeNull());
    await act(async () => refuse(new Error('boom')));
    await waitFor(() => expect(toastMock).toHaveBeenCalledWith(expect.objectContaining({ variant: 'destructive' })));
    expect(screen.getAllByRole('button').map((c) => c.textContent)).toEqual(['Layer 0Oct 28', 'Layer 1Oct 28', 'Layer 2Oct 28']);
    expect(hidden.has('l1')).toBe(true);
  });

  it('hands "ask the agent" to the composer beside it as a request', async () => {
    seed(1);
    const composer = createChartComposer();
    const heard = vi.fn();
    composer.setAskHandler(heard);
    mount(<ChartComposerContext value={composer}><ChartLayerLegend chartKey={KEY} /></ChartComposerContext>);
    await screen.findByRole('button', { name: /Layer 0/ });
    open('Layer 0');
    fireEvent.click(screen.getByRole('button', { name: 'Ask agent about this layer' }));
    expect(heard).toHaveBeenCalledWith(expect.objectContaining({ symbol: 'NVDA', timeframe: '1day', layerId: 'l0', layerTitle: 'Layer 0' }));
    expect(toastMock).not.toHaveBeenCalled();
  });

  it('tells the user when no composer is there to take it', async () => {
    seed(1);
    mount();
    await screen.findByRole('button', { name: /Layer 0/ });
    open('Layer 0');
    fireEvent.click(screen.getByRole('button', { name: 'Ask agent about this layer' }));
    expect(toastMock).toHaveBeenCalledWith(expect.objectContaining({ title: expect.stringContaining('Open the chat') }));
  });

  it('gives every small control a touch hit area, kept off the neighbors beside it', async () => {
    seed(1);
    mount();
    const chip = await screen.findByRole('button', { name: /Layer 0/ });
    expect(chip).toHaveClass('tap-target');
    open('Layer 0');
    const hide = screen.getByRole('button', { name: 'Hide layer' });
    const ask = screen.getByRole('button', { name: 'Ask agent about this layer' });
    for (const control of [hide, ask, screen.getByRole('button', { name: 'Remove layer' })]) {
      expect(control).toHaveClass('tap-target');
    }
    // Hide and ask sit 2px apart: each keeps its box to its own side of the gap.
    expect(hide.className).toMatch(/\[--tap-target-w:/);
    expect(ask.className).toMatch(/\[--tap-target-x:/);
  });

  it('gives the rows of the folded list a touch hit area', async () => {
    seed(4);
    mount();
    const chip = await screen.findByRole('button', { name: /4 layers/ });
    expect(chip).toHaveClass('tap-target');
    open('4 layers');
    const rows = screen.getAllByRole('button', { name: /Layer \d/ });
    expect(rows).toHaveLength(4);
    for (const row of rows) expect(row).toHaveClass('tap-target');
  });

  it('follows a layer published while it is mounted', async () => {
    const { container, queryClient, loaded } = mount();
    await loaded;
    expect(container).toBeEmptyDOMElement();
    await act(() => applyChartLayerEvent(queryClient, { op: 'add', ...makeStored({ id: 'late', title: 'Late' }) }));
    expect(screen.getByRole('button', { name: /Late/ })).toBeInTheDocument();
  });
});

describe('layerAccent', () => {
  it('is the first coloured element, or none', () => {
    expect(layerAccent(makeLayer())).toBe('#e9954a');
    expect(layerAccent(makeLayer({ series: [], levels: [] }))).toBeNull();
  });
});
