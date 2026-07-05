/**
 * Tests the ChatView model-resilience pills' gating + copy.
 *
 * As with ChatViewErrorBanner.test.tsx, we reproduce the minimal pill logic in
 * test components rather than mounting the full ChatView (~30 transitive
 * dependencies). These mirror the real JSX in ChatView.tsx exactly:
 *   - status pill: rendered only when `modelStatus && isLoading`
 *     - retrying → "<model> error — retrying ({attempt+1}/{maxRetries+1})…"
 *     - fallback → "Falling back to <toModel>…"
 *   - suggestion pill: rendered only when `fallbackSuggestion && !isLoading &&
 *     toModel !== nextSendModel`, with a switch action and a dismiss.
 *     `nextSendModel = inputModel ?? (lastThreadModel || activePreferredModel)`
 *     — the chat input's live selection (what the next send re-uses), NOT the
 *     durable preference, which a thread's own model overrides on every send.
 *
 * Keep in sync with the pills in ChatView.tsx and the chat.modelRetrying /
 * chat.modelFallingBack / chat.modelTroubleSuggestion / chat.switchToModel
 * keys in the locale files.
 */
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import React from 'react';
import type { ModelStatus, FallbackSuggestion } from '../../hooks/useChatMessages';

// Fallback i18n strings (kept in sync with chat.* keys in en-US.json).
const EN = {
  modelRetrying: '{{model}} error — retrying ({{attempt}}/{{total}})…',
  modelFallingBack: 'Falling back to {{model}}…',
};

function ModelStatusPill({ modelStatus, isLoading }: { modelStatus: ModelStatus | null; isLoading: boolean }) {
  if (!(modelStatus && isLoading)) return null;
  const text = modelStatus.kind === 'retrying'
    ? EN.modelRetrying
        .replace('{{model}}', modelStatus.model)
        .replace('{{attempt}}', String(modelStatus.attempt + 1))
        .replace('{{total}}', String(modelStatus.maxRetries + 1))
    : EN.modelFallingBack.replace('{{model}}', modelStatus.toModel);
  return (
    <div data-testid="model-status-pill" role="status" aria-live="polite">
      {text}
    </div>
  );
}

describe('ChatView model-status pill', () => {
  it('renders the retrying pill with 1-based counts when streaming', () => {
    render(
      <ModelStatusPill
        modelStatus={{ kind: 'retrying', model: 'model-alpha', attempt: 1, maxRetries: 3 }}
        isLoading={true}
      />,
    );
    const pill = screen.getByTestId('model-status-pill');
    // attempt+1 = 2, maxRetries+1 = 4
    expect(pill).toHaveTextContent('model-alpha error — retrying (2/4)…');
    expect(pill).toHaveAttribute('role', 'status');
    expect(pill).toHaveAttribute('aria-live', 'polite');
  });

  it('renders the fallback pill naming the target model when streaming', () => {
    render(
      <ModelStatusPill
        modelStatus={{ kind: 'fallback', fromModel: 'model-alpha', toModel: 'model-beta' }}
        isLoading={true}
      />,
    );
    expect(screen.getByTestId('model-status-pill')).toHaveTextContent('Falling back to model-beta…');
  });

  it('renders nothing when not loading, even with a modelStatus set', () => {
    const { container } = render(
      <ModelStatusPill
        modelStatus={{ kind: 'retrying', model: 'model-alpha', attempt: 0, maxRetries: 2 }}
        isLoading={false}
      />,
    );
    expect(container.firstChild).toBeNull();
    expect(screen.queryByTestId('model-status-pill')).not.toBeInTheDocument();
  });

  it('renders nothing when modelStatus is null', () => {
    const { container } = render(<ModelStatusPill modelStatus={null} isLoading={true} />);
    expect(container.firstChild).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Fallback-suggestion pill (switch to the working model)
// ---------------------------------------------------------------------------

const EN_SUGGESTION = {
  modelTroubleSuggestion: '{{from}} is having trouble — this answer came from {{to}}',
  switchToModel: 'Switch to {{model}}',
};

function FallbackSuggestionPill({
  fallbackSuggestion,
  isLoading,
  inputModel = null,
  lastThreadModel = null,
  activePreferredModel,
  onSwitchModel,
  onDismiss,
}: {
  fallbackSuggestion: FallbackSuggestion | null;
  isLoading: boolean;
  inputModel?: string | null;
  lastThreadModel?: string | null;
  activePreferredModel: string | null;
  onSwitchModel: (model: string) => void;
  onDismiss: () => void;
}) {
  // Same resolution as ChatView: the model the next send will actually use.
  const nextSendModel = inputModel ?? (lastThreadModel || activePreferredModel);
  if (!(fallbackSuggestion && !isLoading && fallbackSuggestion.toModel !== nextSendModel)) {
    return null;
  }
  return (
    <div data-testid="fallback-suggestion-pill" role="status" aria-live="polite">
      <span>
        {EN_SUGGESTION.modelTroubleSuggestion
          .replace('{{from}}', fallbackSuggestion.fromModel)
          .replace('{{to}}', fallbackSuggestion.toModel)}
      </span>
      <button type="button" onClick={() => onSwitchModel(fallbackSuggestion.toModel)}>
        {EN_SUGGESTION.switchToModel.replace('{{model}}', fallbackSuggestion.toModel)}
      </button>
      <button type="button" aria-label="Close" onClick={onDismiss}>
        ×
      </button>
    </div>
  );
}

describe('ChatView fallback-suggestion pill', () => {
  const suggestion: FallbackSuggestion = { fromModel: 'model-alpha', toModel: 'model-beta' };
  const noop = () => {};

  it('names the troubled model and the working model, and switches to the working one', () => {
    const onSwitchModel = vi.fn();
    render(
      <FallbackSuggestionPill
        fallbackSuggestion={suggestion}
        isLoading={false}
        activePreferredModel="model-alpha"
        onSwitchModel={onSwitchModel}
        onDismiss={noop}
      />,
    );
    const pill = screen.getByTestId('fallback-suggestion-pill');
    expect(pill).toHaveTextContent('model-alpha is having trouble — this answer came from model-beta');
    expect(pill).toHaveAttribute('role', 'status');

    fireEvent.click(screen.getByRole('button', { name: 'Switch to model-beta' }));
    expect(onSwitchModel).toHaveBeenCalledWith('model-beta');
  });

  it('dismisses via the close button', () => {
    const onDismiss = vi.fn();
    render(
      <FallbackSuggestionPill
        fallbackSuggestion={suggestion}
        isLoading={false}
        activePreferredModel="model-alpha"
        onSwitchModel={noop}
        onDismiss={onDismiss}
      />,
    );
    fireEvent.click(screen.getByRole('button', { name: 'Close' }));
    expect(onDismiss).toHaveBeenCalled();
  });

  it('renders nothing while a turn is streaming', () => {
    const { container } = render(
      <FallbackSuggestionPill
        fallbackSuggestion={suggestion}
        isLoading={true}
        activePreferredModel="model-alpha"
        onSwitchModel={noop}
        onDismiss={noop}
      />,
    );
    expect(container.firstChild).toBeNull();
  });

  it('renders nothing when the preference resolves to the working model and nothing overrides it', () => {
    const { container } = render(
      <FallbackSuggestionPill
        fallbackSuggestion={suggestion}
        isLoading={false}
        activePreferredModel="model-beta"
        onSwitchModel={noop}
        onDismiss={noop}
      />,
    );
    expect(container.firstChild).toBeNull();
  });

  it('still renders when the durable preference is the working model but the input re-sends the broken one', () => {
    // Regression: the thread's own model (input selection) overrides the
    // preference on every send — a "correct" preference must not hide the pill.
    render(
      <FallbackSuggestionPill
        fallbackSuggestion={suggestion}
        isLoading={false}
        inputModel="model-alpha"
        lastThreadModel="model-alpha"
        activePreferredModel="model-beta"
        onSwitchModel={noop}
        onDismiss={noop}
      />,
    );
    expect(screen.getByTestId('fallback-suggestion-pill')).toBeInTheDocument();
  });

  it('renders nothing once the input selection is already the working model', () => {
    const { container } = render(
      <FallbackSuggestionPill
        fallbackSuggestion={suggestion}
        isLoading={false}
        inputModel="model-beta"
        lastThreadModel="model-alpha"
        activePreferredModel="model-alpha"
        onSwitchModel={noop}
        onDismiss={noop}
      />,
    );
    expect(container.firstChild).toBeNull();
  });

  it('falls back to the thread model before the preference while the input has not reported', () => {
    const { container } = render(
      <FallbackSuggestionPill
        fallbackSuggestion={suggestion}
        isLoading={false}
        inputModel={null}
        lastThreadModel="model-beta"
        activePreferredModel="model-alpha"
        onSwitchModel={noop}
        onDismiss={noop}
      />,
    );
    expect(container.firstChild).toBeNull();
  });

  it('renders nothing without a suggestion', () => {
    const { container } = render(
      <FallbackSuggestionPill
        fallbackSuggestion={null}
        isLoading={false}
        activePreferredModel={null}
        onSwitchModel={noop}
        onDismiss={noop}
      />,
    );
    expect(container.firstChild).toBeNull();
  });
});
