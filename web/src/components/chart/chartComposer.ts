import { createContext, useContext, useEffect } from 'react';
import type { ChartView } from '@/pages/MarketView/types/chartView';
import type { LayerAskRequest } from '@/pages/MarketView/utils/layerAsk';

/**
 * What a chart can reach of the composer it sits beside. One per composer
 * host (a ChatView, the MarketView page), provided around both, so an inactive
 * thread's panel chart never reports into, or asks through, the active one.
 *
 * - Views: a chart registers a reader of what it shows and the composer reads
 *   them all at send time (`chart_views`), so nothing is tracked per frame.
 * - Asks: "ask about this layer" goes to whichever handler the host
 *   registered (a context pill, or a draft where the composer has no pill).
 *
 * Without a provider (the chart render page) a chart registers nowhere and
 * an ask is refused.
 */
export interface ChartComposer {
  registerView(read: () => ChartView | null): () => void;
  /** Every registered chart's view, skipping one with nothing to report. */
  collectViews(): ChartView[];
  /** The host's answer to an ask; returns the unregister. */
  setAskHandler(handler: (req: LayerAskRequest) => void): () => void;
  /** Hand a request to the composer. False when no handler takes it. */
  ask(req: LayerAskRequest): boolean;
}

export function createChartComposer(): ChartComposer {
  const readers = new Set<() => ChartView | null>();
  let askHandler: ((req: LayerAskRequest) => void) | null = null;
  return {
    registerView(read) {
      readers.add(read);
      return () => {
        readers.delete(read);
      };
    },
    collectViews() {
      const out: ChartView[] = [];
      for (const read of readers) {
        try {
          const view = read();
          if (view) out.push(view);
        } catch {
          /* a chart mid-teardown reports nothing */
        }
      }
      return out;
    },
    setAskHandler(handler) {
      askHandler = handler;
      return () => {
        if (askHandler === handler) askHandler = null;
      };
    },
    ask(req) {
      if (!askHandler) return false;
      askHandler(req);
      return true;
    },
  };
}

export const ChartComposerContext = createContext<ChartComposer | null>(null);

const NO_COMPOSER: Pick<ChartComposer, 'ask' | 'collectViews'> = {
  ask: () => false,
  collectViews: () => [],
};

/** The composer beside this chart, or one that takes nothing. */
export function useChartComposer(): Pick<ChartComposer, 'ask' | 'collectViews'> {
  return useContext(ChartComposerContext) ?? NO_COMPOSER;
}

/** Report this chart's view to the composer's sends while mounted. */
export function useRegisterChartView(read: () => ChartView | null): void {
  const composer = useContext(ChartComposerContext);
  useEffect(() => composer?.registerView(read), [composer, read]);
}

/** Answer the charts' asks while mounted; null registers nothing. */
export function useChartAskHandler(handler: ((req: LayerAskRequest) => void) | null): void {
  const composer = useContext(ChartComposerContext);
  useEffect(() => (composer && handler ? composer.setAskHandler(handler) : undefined), [composer, handler]);
}
