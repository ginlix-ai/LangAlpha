import { describe, it, expect, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { OptionCard, OptionGroup } from '../option-card';

function Group({
  value,
  onChange,
  onClear,
}: {
  value: 'a' | 'b' | null;
  onChange: (v: 'a' | 'b') => void;
  onClear?: () => void;
}) {
  return (
    <>
      <p id="question">Pick one</p>
      <OptionGroup<'a' | 'b'> value={value} onChange={onChange} onClear={onClear} labelledBy="question">
        <OptionCard value="a" title="Alpha" />
        <OptionCard value="b" title="Beta" />
      </OptionGroup>
    </>
  );
}

describe('OptionGroup', () => {
  it('picks an unchecked card through onChange', () => {
    const onChange = vi.fn();
    const onClear = vi.fn();
    render(<Group value="a" onChange={onChange} onClear={onClear} />);
    fireEvent.click(screen.getByRole('radio', { name: /Beta/ }));
    expect(onChange).toHaveBeenCalledWith('b');
    expect(onClear).not.toHaveBeenCalled();
  });

  it('clears when the checked card is picked again, if the group can be left unanswered', () => {
    const onChange = vi.fn();
    const onClear = vi.fn();
    render(<Group value="a" onChange={onChange} onClear={onClear} />);
    fireEvent.click(screen.getByRole('radio', { name: /Alpha/ }));
    expect(onClear).toHaveBeenCalledTimes(1);
    expect(onChange).not.toHaveBeenCalled();
  });

  it('clears from the keyboard with Space on the checked card', () => {
    const onChange = vi.fn();
    const onClear = vi.fn();
    render(<Group value="a" onChange={onChange} onClear={onClear} />);
    const alpha = screen.getByRole('radio', { name: /Alpha/ });
    const press = fireEvent.keyDown(alpha, { key: ' ' });
    expect(onClear).toHaveBeenCalledTimes(1);
    // Default prevented, so the key's own click cannot pick it straight back.
    expect(press).toBe(false);
    fireEvent.keyDown(screen.getByRole('radio', { name: /Beta/ }), { key: ' ' });
    expect(onClear).toHaveBeenCalledTimes(1);
  });

  it('is named by the visible question', () => {
    render(<Group value={null} onChange={vi.fn()} />);
    expect(screen.getByRole('radiogroup', { name: 'Pick one' })).toBeInTheDocument();
  });

  it('keeps the pick when the group has no onClear', () => {
    const onChange = vi.fn();
    render(<Group value="a" onChange={onChange} />);
    fireEvent.click(screen.getByRole('radio', { name: /Alpha/ }));
    expect(onChange).not.toHaveBeenCalled();
    expect(screen.getByRole('radio', { name: /Alpha/ })).toBeChecked();
  });
});
