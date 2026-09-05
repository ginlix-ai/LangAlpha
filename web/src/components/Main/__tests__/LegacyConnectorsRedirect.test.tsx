import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom';
import { LegacyConnectorsRedirect } from '../Main';

/**
 * The /connectors alias is a compat contract, not cosmetics: OAuth
 * `return_to` values parked in Redis before the Plugins rename still point at
 * /connectors with the ?mcp_connected / ?mcp_error params the landing toast
 * reads — so the redirect must carry search and hash intact.
 */

function LocationProbe() {
  const { pathname, search, hash } = useLocation();
  return <div data-testid="probe">{pathname + search + hash}</div>;
}

function renderAt(entry: string) {
  render(
    <MemoryRouter initialEntries={[entry]}>
      <Routes>
        <Route path="/connectors" element={<LegacyConnectorsRedirect />} />
        <Route path="/plugins" element={<LocationProbe />} />
      </Routes>
    </MemoryRouter>,
  );
  return screen.getByTestId('probe').textContent;
}

describe('LegacyConnectorsRedirect', () => {
  it('redirects /connectors to /plugins preserving search and hash', () => {
    expect(renderAt('/connectors?mcp_connected=srv&tab=servers#frag')).toBe(
      '/plugins?mcp_connected=srv&tab=servers#frag',
    );
  });

  it('redirects a bare /connectors to a bare /plugins', () => {
    expect(renderAt('/connectors')).toBe('/plugins');
  });
});
