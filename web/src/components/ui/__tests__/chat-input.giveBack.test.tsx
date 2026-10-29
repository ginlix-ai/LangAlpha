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

describe('ChatInput — giveBack', () => {
  it('fills an empty composer', () => {
    const { ref, box } = renderInput();
    act(() => ref.current!.giveBack('the edited question'));
    expect(box.value).toBe('the edited question');
  });

  it('keeps what is typed and puts the text after it', () => {
    const { ref, box } = renderInput();
    fireEvent.change(box, { target: { value: 'a new question  ' } });
    act(() => ref.current!.giveBack('the edited question'));
    expect(box.value).toBe('a new question\n\nthe edited question');
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
    expect(onSend.mock.calls[0][1]).toEqual([expect.objectContaining({ file, dataUrl: sent.dataUrl })]);
  });
});
