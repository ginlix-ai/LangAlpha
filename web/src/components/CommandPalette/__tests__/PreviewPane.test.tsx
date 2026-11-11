import { afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router';
import PreviewMarkdown from '../PreviewMarkdown';
import { PreviewPane } from '../PreviewPane';

const preview = vi.hoisted(() => ({
  thread_id: 't1',
  title: 'Margins',
  workspace_id: 'ws1',
  workspace_name: 'Semis',
  turn_index: 2,
  turn_at: '2026-11-08T00:00:00Z',
  user_text: 'How did **NVDA margins** hold?',
  answer_text: 'Gross margin:\n\n- data center **margin** up\n- gaming flat\n\n```python\nmargin = 0.75\n```\n',
}));

vi.mock('../useServerSearch', () => ({
  useSearchPreview: () => ({ preview, loading: false, failure: null }),
}));
vi.mock('../useVerbs', () => ({
  useNames: () => ({ workspaceName: (_id: string, given = '') => given, isHome: () => false, when: () => 'today' }),
}));
vi.mock('@/contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'light' }) }));

// A failed assertion still puts back the layout and scroll spies.
afterEach(() => {
  vi.restoreAllMocks();
});

// Loaded up front, so the pane's lazy body resolves as it does once warm.
beforeAll(async () => {
  await import('../PreviewMarkdown');
}, 30_000);

function Path() {
  return <output aria-label="Path">{useLocation().pathname}</output>;
}

function renderMarkdown(text: string, words: string[]) {
  const onShown = vi.fn();
  const view = render(
    <MemoryRouter initialEntries={['/start']}>
      <PreviewMarkdown text={text} words={words} onShown={onShown} />
      <Path />
    </MemoryRouter>,
  );
  return { ...view, onShown };
}

const marks = (root: ParentNode) => [...root.querySelectorAll('mark')].map((mark) => mark.textContent);

describe('the preview pane', () => {
  it('renders the turn as Markdown with its matches marked, and brings the matched side into view', async () => {
    const { container } = render(
      <MemoryRouter>
        <PreviewPane target={{ kind: 'turn', threadId: 't1', turnIndex: 2, side: 'agent' }} words={['margin']} />
      </MemoryRouter>,
    );
    const agent = container.querySelector('[data-side="agent"]')!;
    // Plain text, its matches marked, until the renderer's code has loaded.
    expect(agent.querySelector('.command-palette-pane-plain mark')).toHaveTextContent('margin');
    // A match inside bold, and one inside a list item.
    expect(await screen.findAllByText('margin', { selector: 'strong mark' })).not.toHaveLength(0);
    expect(agent.querySelector('li strong mark')).toHaveTextContent('margin');
    expect(container.querySelector('[data-side="user"] strong mark')).toHaveTextContent('margin');
    // The fence keeps its code whole: CodeBlock draws it from its text.
    expect(agent.querySelector('pre mark')).toBeNull();
    expect(agent).toHaveTextContent('margin = 0.75');
    expect(container.querySelector('.command-palette-pane-plain')).toBeNull();
  });

  it('scrolls to the first match as it draws, and leaves the scroll alone when the palette renders the same words again', async () => {
    const pane = (words: string[]) => (
      <MemoryRouter>
        <PreviewPane target={{ kind: 'turn', threadId: 't1', turnIndex: 2, side: 'agent' }} words={words} />
      </MemoryRouter>
    );
    // Once the renderer has loaded, the scroller and the Markdown mount in one commit.
    const warm = render(pane(['margin']));
    await screen.findAllByText('margin', { selector: 'strong mark' });
    warm.unmount();

    const scrolled = vi.spyOn(Element.prototype, 'scrollTop', 'set');
    vi.spyOn(Element.prototype, 'getBoundingClientRect').mockImplementation(function (this: Element) {
      return { top: this.tagName === 'MARK' ? 600 : 0 } as DOMRect;
    });
    const { rerender, container } = render(pane(['margin']));
    expect(container.querySelector('.command-palette-pane-plain')).toBeNull();
    expect(scrolled).toHaveBeenCalledWith(600);
    scrolled.mockClear();
    rerender(pane(['margin']));
    expect(scrolled).not.toHaveBeenCalled();
  });

  it('scrolls to a code block when the words appear only in its code', async () => {
    const pane = (
      <MemoryRouter>
        <PreviewPane target={{ kind: 'turn', threadId: 't1', turnIndex: 2, side: 'agent' }} words={['0.75']} />
      </MemoryRouter>
    );
    const warm = render(pane);
    await screen.findAllByText('margin', { selector: 'li strong' });
    warm.unmount();

    const scrolled = vi.spyOn(Element.prototype, 'scrollTop', 'set');
    vi.spyOn(Element.prototype, 'getBoundingClientRect').mockImplementation(function (this: Element) {
      return { top: this.tagName === 'PRE' ? 400 : 0 } as DOMRect;
    });
    const { container } = render(pane);
    expect(container.querySelector('[data-side="agent"] pre mark')).toBeNull();
    expect(scrolled).toHaveBeenCalledWith(400);
  });
});

describe('PreviewMarkdown', () => {
  it('marks every match, inside bold, in a list and in inline code', () => {
    const { container, onShown } = renderMarkdown('The **gross margin** rose.\n\n1. margin of safety\n2. `margin_call`', ['margin']);
    expect(container.querySelector('strong mark')).toHaveTextContent('margin');
    expect(container.querySelector('li mark')).toHaveTextContent('margin');
    expect(container.querySelector('code mark')).toHaveTextContent('margin');
    expect(marks(container)).toEqual(['margin', 'margin', 'margin']);
    expect(onShown).toHaveBeenCalled();
  });

  it('marks each word of the query, folding case', () => {
    const { container } = renderMarkdown('# NVDA\n\nData *Center* revenue', ['nvda', 'center']);
    expect(marks(container)).toEqual(['NVDA', 'Center']);
  });

  it("tells the search's marks from a mark the text wrote", () => {
    const { container } = renderMarkdown('<mark>noted</mark> and the margin', ['margin']);
    expect([...container.querySelectorAll('mark[data-match]')].map((m) => m.textContent)).toEqual(['margin']);
    expect(marks(container)).toEqual(['noted', 'margin']);
  });

  it('shows a link inside an svg as its text too', () => {
    const { container } = renderMarkdown('<svg width="80" height="20"><a href="https://example.com"><text>go</text></a></svg>', []);
    expect(container.querySelector('a')).toBeNull();
  });

  it('shows a link as its text, which a click does not follow', () => {
    const { container } = renderMarkdown('See [the margin note](/chat/t/abc) or [docs](https://example.com).', ['margin']);
    expect(container.querySelector('a')).toBeNull();
    const link = screen.getByText('note', { exact: false });
    expect(link.closest('.command-palette-pane-link')).not.toBeNull();
    expect(link.closest('.command-palette-pane-link')!.querySelector('mark')).toHaveTextContent('margin');
    fireEvent.click(link);
    expect(screen.getByLabelText('Path')).toHaveTextContent('/start');
  });

  it('shows an image as its alt text, without fetching it', () => {
    const { container } = renderMarkdown('Chart:\n\n![margin chart](results/margin.png)\n\n<img src="x.png" alt="">', ['margin']);
    expect(container.querySelector('img')).toBeNull();
    expect(container.querySelector('.command-palette-pane-alt')).toHaveTextContent('margin chart');
    expect(container.querySelector('.command-palette-pane-alt mark')).toHaveTextContent('margin');
    expect(container.querySelectorAll('.command-palette-pane-alt')).toHaveLength(1);
  });
});
