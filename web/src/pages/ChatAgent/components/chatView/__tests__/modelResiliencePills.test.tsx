/**
 * Tests the real model-resilience pill components (extracted from ChatView in
 * 5.9b; formerly pinned via local copies in ChatViewModelStatusPill.test.tsx):
 *   - ModelStatusPill: rendered only when `modelStatus && isLoading`
 *     - retrying → "<model> error — retrying ({attempt+1}/{maxRetries+1})…"
 *     - fallback → "Falling back to <toModel>…"
 *   - FallbackSuggestionPill: rendered only when `fallbackSuggestion &&
 *     !isLoading && toModel !== nextSendModel`, with a switch action and a
 *     dismiss. `nextSendModel = inputModel ?? (lastThreadModel ||
 *     activePreferredModel)` — the chat input's live selection (what the next
 *     send re-uses), NOT the durable preference, which a thread's own model
 *     overrides on every send.
 *
 * Copy is asserted through the real i18n instance (global test setup) against
 * the chat.modelRetrying / chat.modelFallingBack / chat.modelTroubleSuggestion
 * / chat.switchToModel keys in en-US.json.
 */
import { describe, it, expect, vi } from 'vitest';
import '@testing-library/jest-dom';
import { render, screen, fireEvent } from '@testing-library/react';
import React from 'react';
import type { FallbackSuggestion } from '../../../session/types';
import { ModelStatusPill } from '../ModelStatusPill';
import { FallbackSuggestionPill } from '../FallbackSuggestionPill';

describe('ModelStatusPill', () => {
  it('renders the retrying pill with 1-based counts when streaming', () => {
    render(
      <ModelStatusPill
        modelStatus={{ kind: 'retrying', model: 'model-alpha', attempt: 1, maxRetries: 3 }}
        isLoading={true}
      />,
    );
    const pill = screen.getByRole('status');
    // attempt+1 = 2, maxRetries+1 = 4
    expect(pill).toHaveTextContent('model-alpha error — retrying (2/4)…');
    expect(pill).toHaveAttribute('aria-live', 'polite');
  });

  it('renders the fallback pill naming the target model when streaming', () => {
    render(
      <ModelStatusPill
        modelStatus={{ kind: 'fallback', fromModel: 'model-alpha', toModel: 'model-beta' }}
        isLoading={true}
      />,
    );
    expect(screen.getByRole('status')).toHaveTextContent('Falling back to model-beta…');
  });

  it('renders nothing when not loading, even with a modelStatus set', () => {
    const { container } = render(
      <ModelStatusPill
        modelStatus={{ kind: 'retrying', model: 'model-alpha', attempt: 0, maxRetries: 2 }}
        isLoading={false}
      />,
    );
    expect(container.firstChild).toBeNull();
  });

  it('renders nothing when modelStatus is null', () => {
    const { container } = render(<ModelStatusPill modelStatus={null} isLoading={true} />);
    expect(container.firstChild).toBeNull();
  });
});

describe('FallbackSuggestionPill', () => {
  const suggestion: FallbackSuggestion = { fromModel: 'model-alpha', toModel: 'model-beta' };
  const noop = () => {};

  const renderPill = (overrides: Partial<React.ComponentProps<typeof FallbackSuggestionPill>> = {}) =>
    render(
      <FallbackSuggestionPill
        fallbackSuggestion={suggestion}
        isLoading={false}
        inputModel={null}
        lastThreadModel={null}
        activePreferredModel="model-alpha"
        onSwitchModel={noop}
        onDismiss={noop}
        {...overrides}
      />,
    );

  it('names the troubled model and the working model, and switches to the working one', () => {
    const onSwitchModel = vi.fn();
    renderPill({ onSwitchModel });
    const pill = screen.getByRole('status');
    expect(pill).toHaveTextContent('model-alpha is having trouble — this answer came from model-beta');

    fireEvent.click(screen.getByRole('button', { name: 'Switch to model-beta' }));
    expect(onSwitchModel).toHaveBeenCalledWith('model-beta');
  });

  it('dismisses via the close button', () => {
    const onDismiss = vi.fn();
    renderPill({ onDismiss });
    fireEvent.click(screen.getByRole('button', { name: 'Close' }));
    expect(onDismiss).toHaveBeenCalled();
  });

  it('renders nothing while a turn is streaming', () => {
    const { container } = renderPill({ isLoading: true });
    expect(container.firstChild).toBeNull();
  });

  it('renders nothing when the preference resolves to the working model and nothing overrides it', () => {
    const { container } = renderPill({ activePreferredModel: 'model-beta' });
    expect(container.firstChild).toBeNull();
  });

  it('still renders when the durable preference is the working model but the input re-sends the broken one', () => {
    // Regression: the thread's own model (input selection) overrides the
    // preference on every send — a "correct" preference must not hide the pill.
    renderPill({
      inputModel: 'model-alpha',
      lastThreadModel: 'model-alpha',
      activePreferredModel: 'model-beta',
    });
    expect(screen.getByRole('status')).toBeInTheDocument();
  });

  it('renders nothing once the input selection is already the working model', () => {
    const { container } = renderPill({
      inputModel: 'model-beta',
      lastThreadModel: 'model-alpha',
      activePreferredModel: 'model-alpha',
    });
    expect(container.firstChild).toBeNull();
  });

  it('falls back to the thread model before the preference while the input has not reported', () => {
    const { container } = renderPill({
      inputModel: null,
      lastThreadModel: 'model-beta',
      activePreferredModel: 'model-alpha',
    });
    expect(container.firstChild).toBeNull();
  });

  it('renders nothing without a suggestion', () => {
    const { container } = renderPill({ fallbackSuggestion: null, activePreferredModel: null });
    expect(container.firstChild).toBeNull();
  });
});
