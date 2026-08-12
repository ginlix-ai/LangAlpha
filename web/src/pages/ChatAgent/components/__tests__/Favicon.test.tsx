/**
 * Favicon: renders an <img> from Google's s2 service for a clearly-public
 * registrable domain and falls back to a Monogram (first character) when the
 * domain is empty, the image fails to load, or the host is non-public (so its
 * hostname is never leaked to the third-party favicon service).
 */
import { describe, it, expect } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { Favicon, Monogram, isPublicHost } from '../Favicon';

describe('Favicon', () => {
  it('renders a lazy-loaded img for a valid domain', () => {
    const { container } = render(<Favicon domain="example.com" />);
    const img = container.querySelector('img');
    expect(img).not.toBeNull();
    expect(img).toHaveAttribute('loading', 'lazy');
    expect(img!.getAttribute('src')).toContain('example.com');
  });

  it('renders a Google favicon img for public domains', () => {
    for (const domain of ['example.com', 'www.nasdaq.example', 'sub.example.org']) {
      const { container, unmount } = render(<Favicon domain={domain} />);
      const img = container.querySelector('img');
      expect(img, domain).not.toBeNull();
      expect(img!.getAttribute('src')).toContain('www.google.com/s2/favicons');
      expect(img!.getAttribute('src')).toContain(domain);
      unmount();
    }
  });

  it('renders a Monogram (no Google request) for non-public hosts', () => {
    const nonPublic = [
      'localhost',
      'intranet',
      '10.0.0.5',
      '192.168.1.10',
      '127.0.0.1',
      '172.16.4.2',
      '169.254.1.1',
      'foo.internal',
      'app.corp',
      'db.lan',
      'box.home',
      'fixtures.test',
      'host.localhost',
      '[::1]',
      '2001:db8::1',
    ];
    for (const domain of nonPublic) {
      const { container, unmount } = render(<Favicon domain={domain} />);
      // No <img> is rendered, so no favicon request is attempted at all.
      expect(container.querySelector('img'), domain).toBeNull();
      // The monogram shows the first character of the host.
      expect(screen.getByText(domain.charAt(0)), domain).toBeInTheDocument();
      // Nothing referencing the Google favicon service is present in the DOM.
      expect(container.innerHTML).not.toContain('google.com');
      unmount();
    }
  });

  it('falls back to a Monogram for an empty domain (no img, no throw)', () => {
    const { container } = render(<Favicon domain="" />);
    expect(container.querySelector('img')).toBeNull();
    // Empty domain has no first character; the Monogram shows the '?' fallback.
    expect(screen.getByText('?')).toBeInTheDocument();
  });

  it('falls back to a Monogram when the image errors', () => {
    const { container } = render(<Favicon domain="broken.example" />);
    const img = container.querySelector('img')!;
    expect(img).not.toBeNull();
    // Simulating onError must swap to the Monogram without throwing.
    fireEvent.error(img);
    expect(container.querySelector('img')).toBeNull();
    expect(screen.getByText('b')).toBeInTheDocument();
  });
});

describe('Monogram', () => {
  it('renders the provided letter', () => {
    render(<Monogram letter="N" />);
    expect(screen.getByText('N')).toBeInTheDocument();
  });
});

describe('isPublicHost', () => {
  it('accepts public registrable domains (bare host or full URL)', () => {
    expect(isPublicHost('example.com')).toBe(true);
    expect(isPublicHost('www.nasdaq.example')).toBe(true);
    expect(isPublicHost('https://example.com/path?q=1#frag')).toBe(true);
    expect(isPublicHost('http://Sub.Example.ORG:8443/foo')).toBe(true);
    expect(isPublicHost('example.com.')).toBe(true); // trailing dot tolerated
  });

  it('rejects empty, single-label, and localhost hosts', () => {
    expect(isPublicHost('')).toBe(false);
    expect(isPublicHost('localhost')).toBe(false);
    expect(isPublicHost('intranet')).toBe(false);
    expect(isPublicHost('http://localhost:5173')).toBe(false);
  });

  it('rejects IPv4 literals including private / link-local ranges', () => {
    expect(isPublicHost('10.0.0.5')).toBe(false);
    expect(isPublicHost('192.168.1.10')).toBe(false);
    expect(isPublicHost('172.16.4.2')).toBe(false);
    expect(isPublicHost('172.31.255.1')).toBe(false);
    expect(isPublicHost('169.254.1.1')).toBe(false);
    expect(isPublicHost('127.0.0.1')).toBe(false);
    expect(isPublicHost('8.8.8.8')).toBe(false); // bare IPv4 is not a registrable domain
    expect(isPublicHost('http://10.0.0.5:9000/x')).toBe(false);
  });

  it('rejects IPv6 literals', () => {
    expect(isPublicHost('[::1]')).toBe(false);
    expect(isPublicHost('2001:db8::1')).toBe(false);
    expect(isPublicHost('http://[2001:db8::1]:8080/')).toBe(false);
  });

  it('rejects internal TLD suffixes', () => {
    expect(isPublicHost('foo.internal')).toBe(false);
    expect(isPublicHost('app.corp')).toBe(false);
    expect(isPublicHost('db.lan')).toBe(false);
    expect(isPublicHost('box.home')).toBe(false);
    expect(isPublicHost('fixtures.test')).toBe(false);
    expect(isPublicHost('node.local')).toBe(false);
    expect(isPublicHost('host.localhost')).toBe(false);
  });
});
