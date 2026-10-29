/**
 * A disabled composer takes no files. A file dropped on it is refused, not
 * attached to a message that cannot be sent, and the drop still stops at the
 * composer rather than reaching the page, which would open the file there.
 * Its attach button is off, and a file picked anyway is not attached either.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import ChatInput from '../chat-input';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';

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

function renderInput(disabled: boolean) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const { container } = render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <ChatInput onSend={vi.fn()} disabled={disabled} />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  return container.querySelector('.chat-input-container')!.parentElement!;
}

function dropTarget() {
  const file = new File(['notes'], 'notes.txt', { type: 'text/plain' });
  return { files: [file], types: ['Files'], dropEffect: 'copy' };
}

describe('ChatInput: a file dropped on the composer', () => {
  beforeEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
    Element.prototype.scrollIntoView = vi.fn();
  });
  afterEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
  });

  it('is attached while the composer is enabled', async () => {
    const zone = renderInput(false);
    fireEvent.drop(zone, { dataTransfer: dropTarget() });
    expect(await screen.findByText('notes.txt')).toBeTruthy();
  });

  it('is refused while the composer is disabled, and kept from the page', () => {
    const zone = renderInput(true);
    const dataTransfer = dropTarget();

    // fireEvent returns false once a handler has called preventDefault.
    expect(fireEvent.dragOver(zone, { dataTransfer })).toBe(false);
    expect(dataTransfer.dropEffect).toBe('none');
    expect(fireEvent.drop(zone, { dataTransfer })).toBe(false);
    expect(screen.queryByText('notes.txt')).toBeNull();
  });
});

// The attach button opens the hidden file input; a pick lands in its change.
function pick(zone: HTMLElement) {
  const input = zone.querySelector<HTMLInputElement>('input[type="file"]')!;
  const file = new File(['notes'], 'notes.txt', { type: 'text/plain' });
  fireEvent.change(input, { target: { files: [file] } });
}

describe('ChatInput: a file picked with the attach button', () => {
  beforeEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
    Element.prototype.scrollIntoView = vi.fn();
  });
  afterEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
  });

  it('is attached while the composer is enabled', async () => {
    const zone = renderInput(false);
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Attach file' }).disabled).toBe(false);
    pick(zone);
    expect(await screen.findByText('notes.txt')).toBeTruthy();
  });

  it('cannot be picked while the composer is disabled', () => {
    renderInput(true);
    expect(screen.getByRole<HTMLButtonElement>('button', { name: 'Attach file' }).disabled).toBe(true);
  });

  it('is not attached while the composer is disabled, should one arrive', () => {
    const zone = renderInput(true);
    pick(zone);
    expect(screen.queryByText('notes.txt')).toBeNull();
  });
});
