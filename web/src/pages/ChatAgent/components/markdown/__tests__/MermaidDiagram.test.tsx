/**
 * What the model writes in a diagram reaches the page as markup, so `strict` is
 * not the only thing between a label and the app: these pin what is taken out.
 */
import { describe, expect, it, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import MermaidDiagram from '../MermaidDiagram';

vi.mock('react-i18next', () => ({ useTranslation: () => ({ t: (key: string) => key }) }));

const SVG = `<svg id="diagram"><foreignObject>
  <form action="https://example.com/c"><input name="p"><button id="appConfig">Sign in</button></form>
  <img name="authToken" src="data:,"><video autoplay src="data:,"></video><audio autoplay src="data:,"></audio>
</foreignObject></svg>`;

vi.mock('mermaid', () => ({
  default: {
    initialize: vi.fn(),
    render: vi.fn(async (_id: string, code: string) => {
      if (code === 'bad') throw new Error('parse');
      return { svg: SVG };
    }),
  },
}));

describe('MermaidDiagram', () => {
  it('draws a picture nothing in it can be reached through', async () => {
    render(<MermaidDiagram code="good" theme="dark" fallback={<pre>source</pre>} />);
    const picture = await screen.findByRole('img', { name: 'chat.diagram' });
    const drawn = picture.firstElementChild!;
    expect(drawn.hasAttribute('inert')).toBe(true);
    expect(drawn.querySelector('button')?.closest('[inert]')).toBe(drawn);
  });

  it('takes out what a label could leave on window, and media', async () => {
    render(<MermaidDiagram code="good" theme="dark" fallback={null} />);
    const picture = await screen.findByRole('img', { name: 'chat.diagram' });
    expect(picture.querySelector('foreignObject [id], foreignObject [name]')).toBeNull();
    expect(picture.querySelector('video, audio')).toBeNull();
    expect(picture.querySelector('svg')?.id).toBe('diagram');
  });

  it('draws in the theme it is handed, since an export is on paper whatever the app shows', async () => {
    const mermaid = (await import('mermaid')).default;
    render(<MermaidDiagram code="good" theme="light" fallback={null} />);
    await screen.findByRole('img', { name: 'chat.diagram' });
    expect(vi.mocked(mermaid.initialize).mock.lastCall?.[0].theme).toBe('neutral');
  });

  it('stays the source when the diagram cannot be drawn', async () => {
    const mermaid = (await import('mermaid')).default;
    const before = vi.mocked(mermaid.render).mock.calls.length;
    render(<MermaidDiagram code="bad" theme="dark" fallback={<pre>source</pre>} />);
    await waitFor(() => expect(vi.mocked(mermaid.render).mock.calls.length).toBe(before + 1));
    expect(screen.getByText('source')).toBeTruthy();
    expect(screen.queryByRole('img')).toBeNull();
  });
});
