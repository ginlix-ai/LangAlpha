/**
 * The answers strip names where tasks run in the reader's terms: in the
 * desktop app the server is their own computer.
 */
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';

const h = vi.hoisted(() => ({ desktopApp: false }));

vi.mock('@/config/hostMode', () => ({ isPlatformMode: false, HOST_MODE: 'oss', APP_ENTRY_PATH: '/' }));
vi.mock('@/hooks/useServerSetup', () => ({ useInDesktopApp: () => ({ inDesktopApp: h.desktopApp, isLoading: false }) }));
vi.mock('../flow', () => ({ useChangeAnswers: () => () => {} }));
vi.mock('react-i18next', () => ({ useTranslation: () => ({ t: (key: string) => key }) }));

import { AnswersStrip } from '../parts';

describe('AnswersStrip', () => {
  it('names this computer in the desktop app', () => {
    h.desktopApp = true;
    render(<AnswersStrip answers={{ models: 'subscription_or_key', runsOn: 'cloud' }} />);
    expect(screen.getByText('setup.answers.tag.thisComputer')).toBeTruthy();
  });

  it('names the server computer on a self-hosted server', () => {
    h.desktopApp = false;
    render(<AnswersStrip answers={{ models: 'subscription_or_key', runsOn: 'cloud' }} />);
    expect(screen.getByText('setup.answers.tag.serverComputer')).toBeTruthy();
  });
});
