import { act, render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import i18n from '@/i18n';
import { AppBridge, type BridgeOptions } from '../AppBridge';
import type { MethodEntry } from '../methods';

const navigate = vi.fn();
const grant = vi.fn();
const socket = { subscribe: vi.fn(), unsubscribe: vi.fn() };
const news = vi.fn();
const readQuoteArgs = vi.fn();
const holdings = vi.fn();
const scopes = { granted: new Set<string>() };

vi.mock('react-router', async (importOriginal) => ({
  ...(await importOriginal<typeof import('react-router')>()),
  useNavigate: () => navigate,
}));
vi.mock('@/contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'light' }) }));
vi.mock('../useAppScopes', () => ({ useAppScopes: () => ({ granted: scopes.granted, grant }) }));
vi.mock('@/pages/MarketView/hooks/useMarketDataWS', () => ({ default: () => socket }));
vi.mock('@/lib/quotes', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/quotes')>()),
  useQuotes: () => ({ quotes: {} }),
}));
vi.mock('../methods', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../methods')>();
  const replaced: Record<string, MethodEntry> = {
    'news.list': { ...actual.METHODS['news.list'], run: () => news() },
    'portfolio.holdings': { ...actual.METHODS['portfolio.holdings'], run: () => holdings() },
    'quotes.stream': { ...actual.METHODS['quotes.stream'], first: (...args) => readQuoteArgs(...args) },
  };
  return {
    ...actual,
    methodEntry: (name: string) => (Object.hasOwn(replaced, name) ? replaced[name] : actual.methodEntry(name)),
  };
});

const PAGE = `${window.location.origin}/api/v1/files/page.html`;
// An opaque frame: served HTML without allow-same-origin.
const OPAQUE = 'allow-scripts';
// A frame whose document keeps the page's origin.
const SAME_ORIGIN = 'allow-scripts allow-same-origin';

let frame: HTMLIFrameElement;
let posted: ReturnType<typeof vi.fn>;

function mount(props: Partial<BridgeOptions> & { loaded?: boolean } = {}, sandbox = OPAQUE) {
  const queryClient = new QueryClient();
  const frameRef = { current: frame };
  const ui = (p: typeof props) => (
    <QueryClientProvider client={queryClient}>
      <AppBridge frameRef={frameRef} src={PAGE} sandbox={sandbox} {...p} grantKey={p.grantKey ?? null} />
    </QueryClientProvider>
  );
  const view = render(ui(props));
  return { ...view, update: (p: typeof props) => view.rerender(ui(p)) };
}

function send(data: Record<string, unknown>, origin = 'null') {
  act(() => {
    window.dispatchEvent(new MessageEvent('message', { data: { la: 1, ...data }, origin, source: frame.contentWindow }));
  });
}

function sent(type: string): Record<string, unknown>[] {
  return posted.mock.calls.map(([msg]) => msg as Record<string, unknown>).filter((m) => m.type === type);
}

function load() {
  act(() => { frame.dispatchEvent(new Event('load')); });
}

function setActivation(isActive: boolean | null) {
  if (isActive === null) {
    Reflect.deleteProperty(navigator, 'userActivation');
    return;
  }
  Object.defineProperty(navigator, 'userActivation', { value: { isActive }, configurable: true });
}

beforeEach(() => {
  frame = document.createElement('iframe');
  document.body.appendChild(frame);
  posted = vi.fn();
  frame.contentWindow!.postMessage = posted as unknown as Window['postMessage'];
  vi.clearAllMocks();
  news.mockReset();
  news.mockResolvedValue([]);
  scopes.granted = new Set();
});

afterEach(() => {
  frame.remove();
  setActivation(null);
});

describe('AppBridge: only the document it loaded', () => {
  it('welcomes the loaded document and answers it under its doc id', async () => {
    mount();
    send({ type: 'hello', scopes: ['news'], doc: 'd1' });
    expect(sent('welcome')).toHaveLength(1);
    expect(sent('welcome')[0].doc).toBe('d1');
    load();
    send({ type: 'req', id: 'r1', method: 'news.list', doc: 'd1' });
    await waitFor(() => expect(sent('res')).toEqual([expect.objectContaining({ id: 'r1', ok: true, doc: 'd1' })]));
  });

  it('answers nothing once the frame navigates itself to another document', async () => {
    let resolve: (v: unknown) => void = () => {};
    mount();
    send({ type: 'hello', scopes: ['news'] });
    load();
    send({ type: 'req', id: 'r1', method: 'news.list' });
    await waitFor(() => expect(sent('res')).toHaveLength(1));
    news.mockReturnValueOnce(new Promise((r) => { resolve = r; }));
    send({ type: 'req', id: 'r2', method: 'news.list' });
    // A link followed: the next document loads in the same window, as 'null' too.
    load();
    posted.mockClear();
    send({ type: 'hello', scopes: ['news'] });
    send({ type: 'req', id: 'r1', method: 'news.list' });
    await act(async () => { resolve([]); });
    expect(posted).not.toHaveBeenCalled();
  });

  it('counts the load of a document already in the frame when the bridge is mounted', () => {
    // A document put back on screen: it loaded under an earlier bridge.
    mount({ loaded: true });
    send({ type: 'hello', scopes: ['news'], doc: 'd1' });
    expect(sent('welcome')).toHaveLength(1);
    // Whatever greets next, ahead of its own load, is a document it went to.
    send({ type: 'hello', scopes: ['news'], doc: 'd2' });
    expect(sent('welcome')).toHaveLength(1);
  });

  it('refuses a hello from another document before that document has loaded', () => {
    mount();
    send({ type: 'hello', scopes: ['news'], doc: 'd1' });
    load();
    posted.mockClear();
    // The page's own repeat still answers; a stranger ends the session.
    send({ type: 'hello', scopes: ['news'], doc: 'd1' });
    expect(sent('welcome')).toHaveLength(1);
    send({ type: 'hello', scopes: ['news'], doc: 'd2' });
    send({ type: 'hello', scopes: ['news'], doc: 'd1' });
    expect(sent('welcome')).toHaveLength(1);
  });

  it('drops a message naming another document, and stops at the page\'s bye', async () => {
    mount();
    send({ type: 'hello', scopes: ['news'], doc: 'd1' });
    send({ type: 'req', id: 'r1', method: 'news.list', doc: 'd2' });
    send({ type: 'req', id: 'r2', method: 'news.list' });
    send({ type: 'bye', doc: 'd1' });
    send({ type: 'req', id: 'r3', method: 'news.list', doc: 'd1' });
    await act(async () => {});
    expect(sent('res')).toEqual([]);
  });

  it('sends nothing once unmounted, whatever frame the ref holds next', async () => {
    let resolve: (v: unknown) => void = () => {};
    news.mockReturnValueOnce(new Promise((r) => { resolve = r; }));
    const view = mount();
    send({ type: 'hello', scopes: ['news'] });
    send({ type: 'req', id: 'r1', method: 'news.list' });
    await act(async () => {});
    view.unmount();
    posted.mockClear();
    await act(async () => { resolve([]); });
    expect(posted).not.toHaveBeenCalled();
  });
});

describe('AppBridge: sessions', () => {
  it('sends the theme once, in the welcome', async () => {
    mount();
    send({ type: 'hello', scopes: ['news'], doc: 'd1' });
    await act(async () => {});
    expect(sent('welcome')).toHaveLength(1);
    expect(sent('theme')).toEqual([]);
  });

  it('answers nothing after a bye until a new hello, in a frame that keeps its origin', async () => {
    const origin = window.location.origin;
    mount({}, SAME_ORIGIN);
    send({ type: 'hello', scopes: ['news'], doc: 'd1' }, origin);
    send({ type: 'bye', doc: 'd1' }, origin);
    send({ type: 'req', id: 'r1', method: 'news.list' }, origin);
    await act(async () => {});
    expect(sent('res')).toEqual([]);
    send({ type: 'hello', scopes: ['news'], doc: 'd2' }, origin);
    send({ type: 'req', id: 'r2', method: 'news.list', doc: 'd2' }, origin);
    await waitFor(() => expect(sent('res')).toEqual([expect.objectContaining({ id: 'r2', ok: true, doc: 'd2' })]));
  });

  it('refuses a method by a name the registry does not own', async () => {
    mount();
    send({ type: 'hello', scopes: ['news'] });
    send({ type: 'req', id: 'r1', method: 'toString' });
    await waitFor(() => expect(sent('res')).toEqual([
      expect.objectContaining({ id: 'r1', ok: false, error: expect.objectContaining({ code: 'bad_request' }) }),
    ]));
  });
});

describe('AppBridge: streams', () => {
  it('stops streaming a subscription whose first read failed', async () => {
    readQuoteArgs.mockRejectedValue(new Error('down'));
    mount();
    send({ type: 'hello', scopes: ['quotes'] });
    send({ type: 'sub', id: 's1', method: 'quotes.stream', params: { symbols: ['AAPL'] } });
    await waitFor(() => expect(sent('res')).toEqual([expect.objectContaining({ id: 's1', ok: false })]));
    expect(socket.subscribe).toHaveBeenCalledWith(['AAPL']);
    expect(socket.unsubscribe).toHaveBeenCalledWith(['AAPL']);
  });

  it('holds only so many streams for one document, and takes another once one ends', async () => {
    readQuoteArgs.mockResolvedValue([]);
    mount();
    send({ type: 'hello', scopes: ['quotes'] });
    const stream = (id: string) => send({ type: 'sub', id, method: 'quotes.stream', params: { symbols: ['AAPL'] } });
    const answered = (id: string) => sent('res').find((r) => r.id === id);
    for (let i = 0; i < 33; i += 1) stream(`s${i}`);
    await waitFor(() => expect(answered('s32')).toMatchObject({ ok: false, error: { code: 'rate' } }));
    expect(answered('s31')).toMatchObject({ ok: true });
    send({ type: 'unsub', id: 's0' });
    stream('s33');
    await waitFor(() => expect(answered('s33')).toMatchObject({ ok: true }));
  });
});

describe('AppBridge: open', () => {
  it('opens only on the user\'s click in the frame', () => {
    mount();
    send({ type: 'hello', scopes: [] });
    frame.focus();
    setActivation(false);
    send({ type: 'open', target: { symbol: 'aapl' } });
    expect(navigate).not.toHaveBeenCalled();
    setActivation(true);
    send({ type: 'open', target: { symbol: 'aapl' } });
    expect(navigate).toHaveBeenCalledTimes(1);
  });

  it('opens nothing in a browser that cannot say whether the user acted', () => {
    mount();
    send({ type: 'hello', scopes: [] });
    frame.focus();
    setActivation(null);
    send({ type: 'open', target: { symbol: 'aapl' } });
    expect(navigate).not.toHaveBeenCalled();
  });

  it('opens nothing on a click outside the frame, on the page\'s own controls', () => {
    mount();
    send({ type: 'hello', scopes: [] });
    const button = document.body.appendChild(document.createElement('button'));
    button.focus();
    setActivation(true);
    send({ type: 'open', target: { symbol: 'aapl' } });
    expect(navigate).not.toHaveBeenCalled();
    button.remove();
  });

  it('opens nothing from a layer out of sight', () => {
    setActivation(true);
    frame.focus();
    const view = mount({ active: false });
    send({ type: 'hello', scopes: [] });
    send({ type: 'open', target: { symbol: 'AAPL' } });
    expect(navigate).not.toHaveBeenCalled();
    view.update({ active: true });
    send({ type: 'open', target: { symbol: 'AAPL' } });
    expect(navigate).toHaveBeenCalledTimes(1);
  });

  it('counts opens and hellos against the bucket', () => {
    vi.spyOn(Date, 'now').mockReturnValue(1_000_000);
    setActivation(true);
    frame.focus();
    mount();
    send({ type: 'hello', scopes: [] });
    for (let i = 0; i < 45; i++) send({ type: 'open', target: { symbol: 'AAPL' } });
    expect(navigate).toHaveBeenCalledTimes(39);
    vi.mocked(Date.now).mockRestore();
  });
});

describe('AppBridge: consent bar', () => {
  it('keeps Allow off for a moment after the bar appears', async () => {
    mount({ grantKey: 'code1' });
    send({ type: 'hello', scopes: ['portfolio'] });
    const allow = screen.getByRole('button', { name: 'Allow' });
    expect(allow).toBeDisabled();
    await waitFor(() => expect(allow).toBeEnabled(), { timeout: 1500 });
    act(() => allow.click());
    expect(grant).toHaveBeenCalledWith(['portfolio']);
  });
});

describe('AppBridge: what the frame may read of the user\'s own', () => {
  it('names a page as a page', () => {
    mount({ grantKey: 'code1', subject: 'page' });
    send({ type: 'hello', scopes: ['portfolio'] });
    expect(screen.getByText('This page asks to read your portfolio.')).toBeInTheDocument();
  });

  it('leaves "Not now" to an owner whose frames replace each other', () => {
    const onDismiss = vi.fn();
    const { update } = mount({ grantKey: 'code1', consent: { dismissed: false, onDismiss } });
    send({ type: 'hello', scopes: ['portfolio'] });
    act(() => screen.getByRole('button', { name: 'Not now' }).click());
    expect(onDismiss).toHaveBeenCalled();
    // Still asked for until the owner says it was dismissed.
    expect(screen.getByRole('button', { name: 'Allow' })).toBeInTheDocument();
    update({ grantKey: 'code1', consent: { dismissed: true, onDismiss } });
    expect(screen.queryByRole('button', { name: 'Allow' })).toBeNull();
  });

  it('says what the document asked to read, and that it left', () => {
    const onAsked = vi.fn();
    mount({ onAsked });
    send({ type: 'hello', scopes: ['news', 'portfolio', 'nonsense'] });
    expect(onAsked).toHaveBeenLastCalledWith(['news', 'portfolio']);
    load();
    load();
    expect(onAsked).toHaveBeenLastCalledWith(null);
  });

  it('reports nothing from a layer out of sight', () => {
    const onAsked = vi.fn();
    const { update } = mount({ onAsked, active: false });
    send({ type: 'hello', scopes: ['portfolio'] });
    expect(onAsked).not.toHaveBeenCalled();
    update({ onAsked, active: true });
    expect(onAsked).toHaveBeenLastCalledWith(['portfolio']);
  });
});

describe('AppBridge: a scope taken back', () => {
  it('refuses a read that was under way when the user took the scope back', async () => {
    scopes.granted = new Set(['portfolio']);
    let land!: (rows: unknown[]) => void;
    holdings.mockReturnValue(new Promise((resolve) => { land = resolve; }));
    const view = mount({ grantKey: 'pg1' });
    send({ type: 'hello', scopes: ['portfolio'], doc: 'd1' });
    send({ type: 'req', id: 'r1', method: 'portfolio.holdings', params: {}, doc: 'd1' });

    scopes.granted = new Set();
    act(() => view.update({ grantKey: 'pg1' }));
    expect(sent('grant')).toEqual([expect.objectContaining({ scopes: [] })]);
    await act(async () => { land([{ symbol: 'NVDA', quantity: 800 }]); });

    expect(sent('res')).toEqual([
      expect.objectContaining({ id: 'r1', ok: false, error: expect.objectContaining({ code: 'scope' }) }),
    ]);
  });
});

describe('AppBridge: the kit\'s words', () => {
  afterEach(async () => { await act(() => i18n.changeLanguage('en-US')); });

  it('sends them with the welcome, and again in a new language', async () => {
    mount();
    send({ type: 'hello', scopes: [] });
    const [welcome] = sent('welcome');
    expect(welcome.strings).toEqual(expect.objectContaining({
      offline: 'Open this page from LangAlpha to see live data.',
      'ago.minutes': '{{n}}m ago',
    }));
    await act(() => i18n.changeLanguage('zh-CN'));
    const [locale] = sent('locale');
    expect(locale.locale).toBe('zh-CN');
    expect((locale.strings as Record<string, string>)['ago.justNow']).toBe('刚刚');
  });
});
