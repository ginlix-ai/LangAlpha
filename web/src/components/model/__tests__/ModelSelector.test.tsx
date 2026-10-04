import { render, screen, fireEvent } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import { ModelSelector } from '../ModelSelector';
import type { ProviderModelsData } from '../types';

const sampleModels: Record<string, ProviderModelsData> = {
  openai: {
    display_name: 'OpenAI',
    models: ['gpt-4o', 'gpt-4o-mini'],
  },
  anthropic: {
    display_name: 'Anthropic',
    models: ['claude-sonnet-4-20250514'],
  },
};

describe('ModelSelector', () => {
  const defaultProps = {
    label: 'Default Model',
    description: 'The primary model for analysis',
    value: '',
    onChange: vi.fn(),
    models: sampleModels,
  };

  it('renders options grouped by provider using optgroup', () => {
    render(<ModelSelector {...defaultProps} />);

    // Label and description are rendered
    expect(screen.getByText('Default Model')).toBeInTheDocument();
    expect(screen.getByText('The primary model for analysis')).toBeInTheDocument();

    // optgroups present
    const optgroups = document.querySelectorAll('optgroup');
    expect(optgroups).toHaveLength(2);
    expect(optgroups[0]).toHaveAttribute('label', 'OpenAI');
    expect(optgroups[1]).toHaveAttribute('label', 'Anthropic');

    // Model options present within groups
    expect(screen.getByRole('option', { name: 'gpt-4o' })).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'gpt-4o-mini' })).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'claude-sonnet-4-20250514' })).toBeInTheDocument();
  });

  it('shows label and description', () => {
    render(<ModelSelector {...defaultProps} />);

    expect(screen.getByText('Default Model')).toBeInTheDocument();
    expect(screen.getByText('The primary model for analysis')).toBeInTheDocument();
  });

  it('offers a choice beside the placeholder that is not a model', () => {
    const onChange = vi.fn();
    render(
      <ModelSelector
        {...defaultProps}
        onChange={onChange}
        placeholder="Same as Primary model"
        autoOption={{ value: '__auto__', label: 'Auto (gpt-4o-mini)' }}
      />,
    );

    const select = document.querySelector('select')!;
    expect([...select.options].slice(0, 2).map((o) => o.textContent)).toEqual([
      'Same as Primary model',
      'Auto (gpt-4o-mini)',
    ]);
    fireEvent.change(select, { target: { value: '__auto__' } });
    expect(onChange).toHaveBeenCalledWith('__auto__');
  });

  it('fires onChange with selected model', () => {
    const onChange = vi.fn();
    render(<ModelSelector {...defaultProps} onChange={onChange} />);

    const select = document.querySelector('select')!;
    fireEvent.change(select, { target: { value: 'gpt-4o-mini' } });

    expect(onChange).toHaveBeenCalledWith('gpt-4o-mini');
  });

  it('prints the authored display name while the key stays the value', () => {
    render(
      <ModelSelector {...defaultProps} metadata={{ 'gpt-4o': { display_name: 'GPT-4o' } }} />,
    );

    expect(screen.getByRole('option', { name: 'GPT-4o' })).toHaveValue('gpt-4o');
    expect(screen.getByRole('option', { name: 'gpt-4o-mini' })).toBeInTheDocument();
  });

  it('shows "No models available" when no models', () => {
    render(
      <ModelSelector
        {...defaultProps}
        models={{}}
      />,
    );

    expect(screen.getByText('No models available')).toBeInTheDocument();
    expect(document.querySelector('select')).toBeNull();
  });
});
