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
