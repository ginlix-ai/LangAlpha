import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { SymbolField } from '../SymbolField';
import { SymbolListField } from '../SymbolListField';
import { EnumField } from '../EnumField';

describe('SymbolField', () => {
  it('renders label and uppercases user input', () => {
    const onChange = vi.fn();
    render(<SymbolField label="Symbol" value="" onChange={onChange} />);
    const input = screen.getByRole('textbox');
    fireEvent.change(input, { target: { value: 'nvda' } });
    expect(onChange).toHaveBeenCalledWith('NVDA');
  });
});

describe('SymbolListField', () => {
  it('adds uppercase unique symbols on Enter', () => {
    const onChange = vi.fn();
    render(<SymbolListField label="Symbols" value={['AAPL']} onChange={onChange} />);
    const input = screen.getByRole('textbox');
    fireEvent.change(input, { target: { value: 'nvda' } });
    fireEvent.keyDown(input, { key: 'Enter' });
    expect(onChange).toHaveBeenCalledWith(['AAPL', 'NVDA']);
  });

  it('ignores duplicate entries', () => {
    const onChange = vi.fn();
    render(<SymbolListField label="Symbols" value={['AAPL']} onChange={onChange} />);
    const input = screen.getByRole('textbox');
    fireEvent.change(input, { target: { value: 'aapl' } });
    fireEvent.keyDown(input, { key: 'Enter' });
    expect(onChange).not.toHaveBeenCalled();
  });

  it('removes a symbol when the chip X is clicked', () => {
    const onChange = vi.fn();
    render(<SymbolListField label="Symbols" value={['AAPL', 'NVDA']} onChange={onChange} />);
    const removeBtn = screen.getByRole('button', { name: /remove aapl/i });
    fireEvent.click(removeBtn);
    expect(onChange).toHaveBeenCalledWith(['NVDA']);
  });

  it('commits a non-empty trimmed draft on blur (under cap)', () => {
    const onChange = vi.fn();
    render(<SymbolListField label="Symbols" value={['AAPL']} onChange={onChange} />);
    const input = screen.getByRole('textbox');
    fireEvent.change(input, { target: { value: 'nvda' } });
    fireEvent.blur(input);
    expect(onChange).toHaveBeenCalledWith(['AAPL', 'NVDA']);
  });

  it('does NOT commit an empty / whitespace-only draft on blur', () => {
    const onChange = vi.fn();
    render(<SymbolListField label="Symbols" value={['AAPL']} onChange={onChange} />);
    const input = screen.getByRole('textbox');
    fireEvent.change(input, { target: { value: '   ' } });
    fireEvent.blur(input);
    expect(onChange).not.toHaveBeenCalled();
  });

  it('disables the input when at cap and skips onBlur commit', () => {
    const onChange = vi.fn();
    render(
      <SymbolListField label="Symbols" value={['A', 'B', 'C']} onChange={onChange} max={3} />,
    );
    const input = screen.getByRole('textbox') as HTMLInputElement;
    expect(input.disabled).toBe(true);
    expect(input.placeholder).toMatch(/max 3 reached/i);
    // Blur shouldn't trigger any onChange even if a draft somehow exists.
    fireEvent.blur(input);
    expect(onChange).not.toHaveBeenCalled();
  });

  it('splits comma/space-separated paste into multiple chips', () => {
    const onChange = vi.fn();
    render(<SymbolListField label="Symbols" value={['AAPL']} onChange={onChange} />);
    const input = screen.getByRole('textbox');
    fireEvent.paste(input, {
      clipboardData: { getData: (t: string) => (t === 'text' ? 'nvda, msft tsla;goog' : '') },
    });
    expect(onChange).toHaveBeenCalledWith(['AAPL', 'NVDA', 'MSFT', 'TSLA', 'GOOG']);
  });

  it('paste-with-separators dedupes against existing chips', () => {
    const onChange = vi.fn();
    render(<SymbolListField label="Symbols" value={['AAPL', 'NVDA']} onChange={onChange} />);
    const input = screen.getByRole('textbox');
    fireEvent.paste(input, {
      clipboardData: { getData: (t: string) => (t === 'text' ? 'NVDA, MSFT, AAPL' : '') },
    });
    expect(onChange).toHaveBeenCalledWith(['AAPL', 'NVDA', 'MSFT']);
  });

  it('paste with no separators falls through to default Input behavior', () => {
    const onChange = vi.fn();
    render(<SymbolListField label="Symbols" value={['AAPL']} onChange={onChange} />);
    const input = screen.getByRole('textbox');
    // A single-symbol paste should NOT auto-commit — preserves edit-as-paste UX.
    fireEvent.paste(input, {
      clipboardData: { getData: (t: string) => (t === 'text' ? 'NVDA' : '') },
    });
    expect(onChange).not.toHaveBeenCalled();
  });

  it('paste respects the cap and stops adding past it', () => {
    const onChange = vi.fn();
    render(<SymbolListField label="Symbols" value={['A']} onChange={onChange} max={3} />);
    const input = screen.getByRole('textbox');
    fireEvent.paste(input, {
      clipboardData: { getData: (t: string) => (t === 'text' ? 'B, C, D, E' : '') },
    });
    // max=3, current=['A'] → can add 2 more: B + C. D and E dropped.
    expect(onChange).toHaveBeenCalledWith(['A', 'B', 'C']);
  });

  it('shows the cap label only when at cap', () => {
    const { rerender } = render(
      <SymbolListField label="Symbols" value={['A']} onChange={vi.fn()} max={2} />,
    );
    expect(screen.queryByText(/max 2 symbols/i)).toBeNull();
    rerender(<SymbolListField label="Symbols" value={['A', 'B']} onChange={vi.fn()} max={2} />);
    expect(screen.getByText(/max 2 symbols/i)).toBeInTheDocument();
  });
});

describe('EnumField', () => {
  it('emits the selected option value', () => {
    const onChange = vi.fn();
    render(
      <EnumField
        label="Interval"
        value="1D"
        onChange={onChange}
        options={[
          { value: '1D', label: '1 day' },
          { value: '1W', label: '1 week' },
        ]}
      />,
    );
    const select = screen.getByRole('combobox');
    fireEvent.change(select, { target: { value: '1W' } });
    expect(onChange).toHaveBeenCalledWith('1W');
  });
});
