import { describe, expect, it } from 'vitest';
import { render, screen } from '@testing-library/react';
import { DownloadProgress } from '../DownloadProgress';

describe('DownloadProgress', () => {
  it('reports no value while nothing measures the save', () => {
    render(<DownloadProgress text="Preparing big.bin for download…" fraction={null} />);
    expect(screen.getByRole('progressbar')).not.toHaveAttribute('aria-valuenow');
  });

  it('reports the measured share once the tab is fetching', () => {
    render(<DownloadProgress text="Downloading big.bin · 42%" fraction={0.42} />);
    expect(screen.getByRole('progressbar')).toHaveAttribute('aria-valuenow', '42');
  });
});
