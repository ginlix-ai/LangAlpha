import { describe, expect, it, vi } from 'vitest';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { useRef, useState } from 'react';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { EditMessageField } from '../EditMessageField';

const QUOTE = '<quote source="Analyst">Revenue grew</quote>\n<reply>by how much?</reply>';

function renderField(value: string) {
  const onChange = vi.fn();
  let handBack: (text: string) => void = () => {};
  function Host() {
    const ref = useRef<HTMLTextAreaElement>(null);
    const [text, setText] = useState(value);
    handBack = setText;
    return (
      <EditMessageField
        textareaRef={ref}
        value={text}
        onChange={(next) => { onChange(next); setText(next); }}
        onKeyDown={vi.fn()}
      />
    );
  }
  render(<QueryClientProvider client={new QueryClient()}><MemoryRouter><Host /></MemoryRouter></QueryClientProvider>);
  return { onChange, box: screen.getByRole('textbox') as HTMLTextAreaElement, handBack: (text: string) => handBack(text) };
}

describe('EditMessageField', () => {
  it('shows a quote as its token, not its tags', () => {
    const { box } = renderField(`first ${QUOTE} last`);
    expect(box.value).not.toContain('<quote');
    expect(box.value).not.toContain('by how much');
    expect(box.value).toContain('Revenue grew');
    expect(box.value).toContain('first ');
  });

  it('hands the message back as sent, words changed and quote intact', () => {
    const { box, onChange } = renderField(`${QUOTE} last`);
    fireEvent.change(box, { target: { value: box.value.replace('last', 'later') } });
    expect(onChange).toHaveBeenLastCalledWith(`${QUOTE} later`);
  });

  it('takes a token out whole, and an undo that brings it back brings the quote', () => {
    const { box, onChange } = renderField(`${QUOTE} last`);
    const withToken = box.value;
    const end = withToken.indexOf(' last');
    box.setSelectionRange(end, end);
    fireEvent.keyDown(box, { key: 'Backspace' });
    expect(box.value).toBe(' last');
    expect(onChange).toHaveBeenLastCalledWith(' last');
    fireEvent.change(box, { target: { value: withToken } });
    expect(onChange).toHaveBeenLastCalledWith(`${QUOTE} last`);
  });

  it('reads an edit the host hands back', () => {
    const { box, handBack: giveBack } = renderField('plain');
    act(() => giveBack(`${QUOTE} again`));
    expect(box.value).not.toContain('<quote');
    expect(box.value).toContain('again');
  });

  it('leaves a message with no quote as it was, spaces included', () => {
    const { box, onChange } = renderField('plain');
    fireEvent.change(box, { target: { value: 'plain ' } });
    expect(onChange).toHaveBeenLastCalledWith('plain ');
    expect(box.value).toBe('plain ');
  });
});
