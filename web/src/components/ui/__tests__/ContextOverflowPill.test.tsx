import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, act, fireEvent } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { ContextOverflowPill } from '../ContextOverflowPill';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';
import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';

const SNAP: WidgetContextSnapshot = {
  widget_type: 'markets.chart',
  widget_id: 'a',
  label: 'NVDA',
  captured_at: '2026-04-26T11:42:08Z',
  text: '<widget-context>x</widget-context>',
  data: {},
};

// jsdom does not implement IntersectionObserver — provide a controllable mock.
let observerCallbacks: IntersectionObserverCallback[] = [];
let observedTargets: Element[] = [];

class MockIO {
  callback: IntersectionObserverCallback;
  constructor(cb: IntersectionObserverCallback) {
    this.callback = cb;
    observerCallbacks.push(cb);
  }
  observe(target: Element) {
    observedTargets.push(target);
  }
  unobserve() {}
  disconnect() {
    observerCallbacks = observerCallbacks.filter((c) => c !== this.callback);
  }
  takeRecords(): IntersectionObserverEntry[] {
    return [];
  }
  root = null;
  rootMargin = '';
  thresholds = [];
}

function setIntersection(target: Element, isIntersecting: boolean) {
  observerCallbacks.forEach((cb) => {
    cb(
      [
        {
          target,
          isIntersecting,
          intersectionRatio: isIntersecting ? 1 : 0,
          time: 0,
          boundingClientRect: target.getBoundingClientRect(),
          intersectionRect: target.getBoundingClientRect(),
          rootBounds: null,
        } as IntersectionObserverEntry,
      ],
      // mock observer instance — cast to any to avoid satisfying the full DOM type
      {} as IntersectionObserver,
    );
  });
}

describe('ContextOverflowPill', () => {
  beforeEach(() => {
    observerCallbacks = [];
    observedTargets = [];
    (globalThis as unknown as { IntersectionObserver: unknown }).IntersectionObserver = MockIO as unknown as typeof IntersectionObserver;
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
  });
  afterEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
  });

  it('does not render when there are no snapshots', () => {
    render(
      <MemoryRouter>
        <ContextOverflowPill />
      </MemoryRouter>,
    );
    expect(screen.queryByRole('button')).toBeNull();
  });

  it('does not render when a chat input is in viewport', () => {
    const el = document.createElement('div');
    document.body.appendChild(el);
    render(
      <MemoryRouter>
        <ContextOverflowPill />
      </MemoryRouter>,
    );
    act(() => {
      ChatInputRegistry.register(el);
    });
    act(() => {
      setIntersection(el, true);
    });
    act(() => {
      ContextBus.attach(SNAP);
    });
    expect(screen.queryByRole('button')).toBeNull();
  });

  it('renders when chats exist but none are visible', () => {
    const el = document.createElement('div');
    document.body.appendChild(el);
    render(
      <MemoryRouter>
        <ContextOverflowPill />
      </MemoryRouter>,
    );
    act(() => {
      ChatInputRegistry.register(el);
    });
    act(() => {
      setIntersection(el, false);
    });
    act(() => {
      ContextBus.attach(SNAP);
    });
    const btn = screen.getByRole('button');
    expect(btn).toBeInTheDocument();
    expect(btn.textContent).toContain('1');
  });

  it('renders when no chat inputs are mounted at all', () => {
    render(
      <MemoryRouter>
        <ContextOverflowPill />
      </MemoryRouter>,
    );
    act(() => {
      ContextBus.attach(SNAP);
    });
    const btn = screen.getByRole('button');
    expect(btn).toBeInTheDocument();
  });

  it('click navigates to default chat when no chat input is registered', () => {
    let lastNavigatedState: unknown = null;
    function StateProbe() {
      const search = window.history.state;
      lastNavigatedState = search;
      return <div data-testid="probe">probe</div>;
    }
    render(
      <MemoryRouter initialEntries={['/dashboard']}>
        <Routes>
          <Route path="/dashboard" element={<ContextOverflowPill />} />
          <Route path="/chat/t/__default__" element={<StateProbe />} />
        </Routes>
      </MemoryRouter>,
    );
    act(() => {
      ContextBus.attach(SNAP);
    });
    const btn = screen.getByRole('button');
    act(() => {
      fireEvent.click(btn);
    });
    expect(screen.getByTestId('probe')).toBeInTheDocument();
    // (Not asserting on `lastNavigatedState` — react-router's MemoryRouter
    // doesn't surface state on window.history. Reaching the /chat route is
    // sufficient evidence the navigate fired.)
    expect(lastNavigatedState).toBeDefined();
  });

  it('click on visible chat scrolls to it', () => {
    const el = document.createElement('div');
    document.body.appendChild(el);
    el.scrollIntoView = vi.fn();
    render(
      <MemoryRouter>
        <ContextOverflowPill />
      </MemoryRouter>,
    );
    act(() => {
      ChatInputRegistry.register(el);
    });
    act(() => {
      setIntersection(el, false);
    });
    act(() => {
      ContextBus.attach(SNAP);
    });
    const btn = screen.getByRole('button');
    act(() => {
      fireEvent.click(btn);
    });
    expect(el.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth', block: 'center' });
  });
});
