/**
 * Text the user wrote elsewhere (an edit whose message is gone) comes back to
 * the composer without costing what is typed there.
 */
import { describe, it, expect, vi } from 'vitest';
import { render, fireEvent, act, screen } from '@testing-library/react';
import { createRef } from 'react';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import ChatInput, { type ChatInputHandle } from '../chat-input';
import type { QuoteOrigin } from '../chat-input.types';

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getSkills: vi.fn().mockResolvedValue([]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
}));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ data: undefined, isLoading: false }),
}));

vi.mock('../use-toast', () => ({
  useToast: () => ({ toast: vi.fn() }),
  toast: vi.fn(),
}));

function renderInput() {
  const ref = createRef<ChatInputHandle>();
  const onSend = vi.fn();
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <ChatInput ref={ref} onSend={onSend} />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  const box = screen.getAllByRole('textbox')[0] as HTMLTextAreaElement;
  return { ref, box, onSend };
}

const ANALYST: QuoteOrigin = { kind: 'message', speaker: 'Analyst' };

describe('ChatInput — giveBack', () => {
  it('fills an empty composer', () => {
    const { ref, box } = renderInput();
    act(() => ref.current!.giveBack('the edited question'));
    expect(box.value).toBe('the edited question');
  });

  it('takes the focus back without a keyboard ring, since no key moved it', () => {
    vi.useFakeTimers();
    try {
      const { ref, box } = renderInput();
      document.documentElement.removeAttribute('data-pointer-focus');
      act(() => ref.current!.giveBack('the edited question'));
      act(() => { vi.runAllTimers(); });
      expect(document.activeElement).toBe(box);
      expect(document.documentElement.hasAttribute('data-pointer-focus')).toBe(true);
    } finally {
      vi.useRealTimers();
    }
  });

  it('keeps what is typed and puts the text after it', () => {
    const { ref, box } = renderInput();
    fireEvent.change(box, { target: { value: 'a new question  ' } });
    act(() => ref.current!.giveBack('the edited question'));
    expect(box.value).toBe('a new question\n\nthe edited question');
  });

  it('gives a quote coming back new words when the draft has since used its own', () => {
    const { ref, box, onSend } = renderInput();
    const start = 'Revenue grew in the first quarter of the year';
    act(() => ref.current!.addContext({ kind: 'quote', snippet: `${start} by a lot`, from: ANALYST }));
    fireEvent.keyDown(box, { key: 'Enter' });
    const failed = onSend.mock.calls[0][0];
    expect(failed.mentions).toHaveLength(1);

    // Started alike, cut off alike: the same words stand for each, with nothing
    // in the empty draft to number the second against.
    act(() => ref.current!.addContext({ kind: 'quote', snippet: `${start} by a little`, from: ANALYST }));
    act(() => ref.current!.giveBack(failed.text, [], { mentions: failed.mentions }));
    fireEvent.keyDown(box, { key: 'Enter' });

    const sent = onSend.mock.calls[1][0].message;
    expect(sent).toContain(`>${start} by a lot</quote>`);
    expect(sent).toContain(`>${start} by a little</quote>`);
  });

  it('sends the quote that returned, when its words now stand for another', () => {
    const { ref, box, onSend } = renderInput();
    const start = 'Revenue grew in the first quarter of the year';
    const held = { kind: 'quote', from: ANALYST } as const;
    // The failed draft's quote is numbered: another that starts alike was beside it.
    act(() => ref.current!.addContext({ ...held, snippet: `${start} by a bit` }));
    act(() => ref.current!.addContext({ ...held, snippet: `${start} by a lot` }));
    fireEvent.keyDown(box, { key: 'Enter' });
    const failed = onSend.mock.calls[0][0];

    // The same excerpt again, on the word the failed one held, with another reply.
    act(() => ref.current!.addContext({ ...held, snippet: `${start} by a lot`, reply: 'why?' }));
    act(() => ref.current!.giveBack(failed.text, [], { mentions: failed.mentions }));
    fireEvent.keyDown(box, { key: 'Enter' });

    const sent = onSend.mock.calls[1][0].message;
    expect(sent).toContain(`>${start} by a bit</quote>`);
    expect(sent).toContain(`>${start} by a lot</quote>\n<reply>why?</reply>`);
    expect(sent.match(new RegExp(`>${start} by a lot</quote>`, 'g'))).toHaveLength(2);
    expect(sent).not.toContain('\u2060');
  });

  it('takes a returning token whose words hold replacement patterns as they are', () => {
    const { ref, box, onSend } = renderInput();
    act(() => ref.current!.addContext({ kind: 'quote', snippet: 'echo $& one', from: ANALYST }));
    fireEvent.keyDown(box, { key: 'Enter' });
    const failed = onSend.mock.calls[0][0];
    act(() => ref.current!.addContext({ kind: 'quote', snippet: 'echo $& one', from: ANALYST, reply: 'other' }));
    act(() => ref.current!.giveBack(failed.text, [], { mentions: failed.mentions }));
    fireEvent.keyDown(box, { key: 'Enter' });
    expect(onSend.mock.calls[1][0].message).not.toContain('\u2060');
  });

  describe('a message that returns spelled out', () => {
    const fileQuote = '<quote source="@work/model.py (lines 40-42, 3 lines)">def npv():\n  pass\nend</quote>';
    const chatQuote = '<quote source="Analyst">Revenue grew</quote>\n<reply>by how much?</reply>';

    it('shows each quote as a token, and sends it as it went out', () => {
      const { ref, box, onSend } = renderInput();
      const message = `first ${fileQuote} then\n${chatQuote} last`;
      act(() => ref.current!.giveBack(message));

      expect(box.value).not.toContain('<quote');
      expect(box.value).not.toContain('<reply');
      expect(box.value).toContain('first ');
      expect(box.value).toContain(' last');

      fireEvent.keyDown(box, { key: 'Enter' });
      const sent = onSend.mock.calls[0][0];
      expect(sent.message).toBe(message);
      expect(sent.mentions).toEqual([
        expect.objectContaining({
          kind: 'quote',
          snippet: 'def npv():\n  pass\nend',
          from: { kind: 'file', path: 'work/model.py', label: 'model.py:L40-42', lineStart: 40, lineEnd: 42, lineCount: 3 },
        }),
        expect.objectContaining({ kind: 'quote', snippet: 'Revenue grew', reply: 'by how much?', from: ANALYST }),
      ]);
    });

    it('takes a token apart whole, as one the composer minted', () => {
      const { ref, box } = renderInput();
      act(() => ref.current!.giveBack(`${chatQuote} last`));
      box.setSelectionRange(box.value.indexOf(' last'), box.value.indexOf(' last'));
      fireEvent.keyDown(box, { key: 'Backspace' });
      expect(box.value).toBe(' last');
    });

    it('keeps it apart from a quote that starts alike in the draft', () => {
      const { ref, box, onSend } = renderInput();
      act(() => ref.current!.addContext({ kind: 'quote', snippet: 'Revenue grew a lot', from: ANALYST }));
      act(() => ref.current!.giveBack('<quote source="Analyst">Revenue grew a lot in every single region of it</quote>'));
      act(() => ref.current!.giveBack('<quote source="Analyst">Revenue grew a lot</quote> again'));
      fireEvent.keyDown(box, { key: 'Enter' });
      const sent = onSend.mock.calls[0][0].message;
      expect(sent).toContain('>Revenue grew a lot in every single region of it</quote>');
      // Already held, so it stands on the token the draft has for it.
      expect(sent.match(/>Revenue grew a lot<\/quote>/g)).toHaveLength(2);
    });

    it('puts the caret after the token pressed, when the draft holds it twice', () => {
      const { ref, box } = renderInput();
      act(() => ref.current!.giveBack(chatQuote));
      act(() => ref.current!.giveBack(`${chatQuote} again`));
      const tokens = document.querySelectorAll('.draft-token-quote');
      expect(tokens).toHaveLength(2);
      fireEvent.mouseDown(tokens[1]);
      const after = box.value.indexOf(' again');
      expect(box.selectionEnd).toBe(after);
    });

    it('does not add the same message twice', () => {
      const { ref, box } = renderInput();
      act(() => ref.current!.giveBack(chatQuote));
      const once = box.value;
      act(() => ref.current!.giveBack(chatQuote));
      expect(box.value).toBe(once);
    });

    it('leaves a chip block as the text it is', () => {
      const { ref, box } = renderInput();
      const paste = '\n<details>\n<summary>[Pasted text]</summary>\n\n```\n<quote source="x">inside</quote>\n```\n</details>';
      act(() => ref.current!.giveBack(`asked${paste}`));
      expect(box.value).toBe(`asked${paste}`);
    });
  });

  it('does not add the same text twice', () => {
    const { ref, box } = renderInput();
    act(() => ref.current!.giveBack('the edited question'));
    act(() => ref.current!.giveBack('the edited question'));
    expect(box.value).toBe('the edited question');
  });

  it('puts back the files a send took, ready to send again', () => {
    // jsdom has no object URLs.
    vi.spyOn(URL, 'createObjectURL').mockReturnValue('blob:positions');
    vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => {});
    const { ref, box, onSend } = renderInput();
    const file = new File(['x'], 'positions.png', { type: 'image/png' });
    const sent = { file, dataUrl: 'data:image/png;base64,eA==', type: 'image/png', preview: null };
    act(() => ref.current!.giveBack('', [sent]));
    expect(box.value).toBe('');
    expect(screen.getByAltText('positions.png')).toBeTruthy();
    act(() => {
      ref.current!.send();
    });
    expect(onSend).toHaveBeenCalledTimes(1);
    expect(onSend.mock.calls[0][0].files).toEqual([expect.objectContaining({ file, dataUrl: sent.dataUrl })]);
  });
});
