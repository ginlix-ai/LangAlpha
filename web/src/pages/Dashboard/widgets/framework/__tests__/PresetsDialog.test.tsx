import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import i18n from '@/i18n';
import { PresetsDialog } from '../PresetsDialog';
import { PRESETS_META } from '../../presets';
import '../../index'; // ensure registry is populated for thumbnails

function renderDialog(overrides: Partial<React.ComponentProps<typeof PresetsDialog>> = {}) {
  const props = {
    open: true,
    onOpenChange: vi.fn(),
    onApply: vi.fn(),
    ...overrides,
  };
  const utils = render(<PresetsDialog {...props} />);
  return { ...utils, props };
}

describe('PresetsDialog', () => {
  it('renders the heading + every preset card', () => {
    renderDialog();
    expect(screen.getByText(/start with a preset/i)).toBeInTheDocument();
    for (const meta of PRESETS_META) {
      expect(screen.getByText(i18n.t(meta.nameKey))).toBeInTheDocument();
    }
  });

  it('calls onApply with the preset id when a card is clicked', () => {
    const { props } = renderDialog();
    const card = screen.getByText(i18n.t(PRESETS_META[0].nameKey));
    fireEvent.click(card);
    expect(props.onApply).toHaveBeenCalledWith(PRESETS_META[0].id);
    expect(props.onOpenChange).toHaveBeenCalledWith(false);
  });

  it('renders nothing when closed', () => {
    const { queryByText } = renderDialog({ open: false });
    expect(queryByText(/start with a preset/i)).toBeNull();
  });
});
