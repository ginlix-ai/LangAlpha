import { useTranslation } from 'react-i18next';

import type { Computer } from '@/types/api';

import { useSetBrowserChoices } from '../hooks/useSetBrowserChoices';
import { SettingToggle } from './computerParts';

/**
 * Whether the browser also opens as a window on the Mac's screen, beside the
 * Browser tab. Off by default: a window the agent drives would otherwise take
 * the person's screen whenever a thread browses.
 */
export function BrowserWindowSetting({ computer }: { computer: Computer }) {
  const { t } = useTranslation();
  const mutation = useSetBrowserChoices();
  const shown = computer.browser?.window === true;
  return (
    <SettingToggle
      label={t('computer.browserWindow.label', 'Show as a window')}
      checked={shown}
      onChange={() => mutation.mutate({ computerId: computer.computer_id, patch: { window: !shown } })}
      disabled={mutation.busyFor(computer.computer_id) || computer.status === 'deleted'}
      hint={
        shown
          ? t(
              'computer.browserWindow.onHint',
              'The browser also opens as a window on this Mac, where you can watch and use it. It applies the next time the browser opens.',
            )
          : t(
              'computer.browserWindow.offHint',
              'The browser shows only in the Browser tab. Turn this on to also see it as a window on this Mac. It applies the next time the browser opens.',
            )
      }
    />
  );
}
