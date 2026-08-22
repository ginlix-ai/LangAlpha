import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import type { SkillInfo } from '@/pages/ChatAgent/utils/api';
import { PluginOriginBadge, PluginSuppressedBadge } from '../components/PluginBadges';

/**
 * The two badges take different things on purpose, and the asymmetry is the
 * whole contract: origin is one field, so that badge takes a plugin name;
 * suppression is a predicate over two (`plugin_name` AND `plugin_enabled ===
 * false`), so that badge takes the ROW and evaluates it itself.
 *
 * The negative cases are the point. While the suppressed badge took a name, it
 * rendered for any plugin-owned row and every call site had to remember its own
 * `isPluginSuppressed(row) &&` guard — so a site that forgot would badge every
 * plugin row as suppressed, enabled or not.
 */

const skill = (overrides: Partial<SkillInfo>) =>
  ({ name: 'placeholder_skill', enabled: true, ...overrides }) as SkillInfo;

describe('PluginSuppressedBadge', () => {
  it('renders when the row is held down by its plugin', () => {
    renderWithProviders(
      <PluginSuppressedBadge row={skill({ plugin_name: 'acme', plugin_enabled: false })} />,
    );
    expect(screen.getByText('plugin off')).toBeInTheDocument();
    // The owning plugin is named in the tooltip, not shouted on the chip.
    expect(screen.getByTitle(/acme/)).toBeInTheDocument();
  });

  it('states it in prose where a chip would out-shout the real status', () => {
    renderWithProviders(
      <PluginSuppressedBadge
        row={skill({ plugin_name: 'acme', plugin_enabled: false })}
        variant="prose"
      />,
    );
    expect(screen.getByText(/acme/)).toBeInTheDocument();
  });

  it('renders nothing for a plugin-owned row whose plugin is ON', () => {
    const { container } = renderWithProviders(
      <PluginSuppressedBadge row={skill({ plugin_name: 'acme', plugin_enabled: true })} />,
    );
    expect(container).toBeEmptyDOMElement();
  });

  it('renders nothing for a row that never came from a plugin', () => {
    const { container } = renderWithProviders(
      <PluginSuppressedBadge row={skill({ plugin_name: null })} />,
    );
    expect(container).toBeEmptyDOMElement();
  });

  it('renders nothing for a missing row, so a deck header needs no guard', () => {
    const { container } = renderWithProviders(<PluginSuppressedBadge row={undefined} />);
    expect(container).toBeEmptyDOMElement();
  });
});

describe('PluginOriginBadge', () => {
  it('names the plugin a row came from', () => {
    renderWithProviders(<PluginOriginBadge plugin="acme" />);
    expect(screen.getByText('acme')).toBeInTheDocument();
  });

  it('renders nothing without a plugin, so call sites pass a nullable field', () => {
    const { container } = renderWithProviders(<PluginOriginBadge plugin={null} />);
    expect(container).toBeEmptyDOMElement();
  });
});
