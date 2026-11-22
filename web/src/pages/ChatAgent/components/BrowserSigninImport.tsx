import { useTranslation } from 'react-i18next';

import type { Computer } from '@/types/api';

import { useBringBrowserSignins, useBrowserSigninSources } from '../hooks/useBrowserSigninImport';

/**
 * The browsers the person uses on this machine of theirs, each of which can
 * hand its sign-ins to the computer's browser. Shown only where there is one:
 * a server with no desktop lists none, so nothing appears. A list that could
 * not be read says so, since an empty one would claim there are no browsers.
 */
export function BrowserSigninImport({ computer }: { computer: Computer }) {
  const { t } = useTranslation();
  const running = computer.status === 'running';
  const { data, isError, isFetching, refetch } = useBrowserSigninSources(computer.computer_id, { enabled: running });
  const bring = useBringBrowserSignins(computer.computer_id);
  const sources = running ? (data?.sources ?? []) : [];
  // `unavailable` is the server not asking at all: not a machine of the person's own, or sign-ins not kept.
  const listFailed = running && (isError || (!!data?.reason && data.reason !== 'unavailable'));
  if (!sources.length && listFailed) {
    return (
      <p className="mt-3 text-[12px]" style={{ color: 'var(--color-text-secondary)' }}>
        {t('computer.browserSignins.import.listFailed', "Couldn't list the browsers on this computer.")}{' '}
        <button
          type="button"
          className="tap-target text-[12px] underline underline-offset-2 disabled:opacity-50"
          style={{ color: 'var(--color-text-primary)' }}
          disabled={isFetching}
          onClick={() => void refetch()}
        >
          {t('computer.remoteControl.tryAgain', 'Try again')}
        </button>
      </p>
    );
  }
  if (!sources.length) return null;
  const pending = bring.isPending ? bring.variables : undefined;
  return (
    <div className="mt-3">
      <p className="text-[12px] font-medium" style={{ color: 'var(--color-text-primary)' }}>
        {t('computer.browserSignins.import.title', 'Bring in sign-ins')}
      </p>
      <p className="mt-0.5 text-[12px]" style={{ color: 'var(--color-text-secondary)' }}>
        {t(
          'computer.browserSignins.import.hint',
          'Copy the sites you are signed in to in a browser on this computer, so the agent can use them too. That browser stays as it is.',
        )}
      </p>
      <ul className="mt-1.5 space-y-1">
        {sources.map((source) => {
          const busy = pending?.browser === source.browser && pending?.profile === source.profile;
          return (
            <li key={`${source.browser}/${source.profile}`} className="flex items-center justify-between gap-3">
              <span className="min-w-0 truncate text-[12.5px]" style={{ color: 'var(--color-text-primary)' }}>
                {source.name}
                <span style={{ color: 'var(--color-text-secondary)' }}> · {source.profile_name}</span>
              </span>
              <button
                type="button"
                className="tap-target shrink-0 text-[12px] underline underline-offset-2 disabled:opacity-50"
                style={{ color: 'var(--color-text-primary)' }}
                disabled={bring.isPending}
                onClick={() => bring.mutate(source)}
              >
                {busy
                  ? t('computer.browserSignins.import.bringing', 'Bringing in…')
                  : t('computer.browserSignins.import.bring', 'Bring in')}
              </button>
            </li>
          );
        })}
      </ul>
      {pending && (
        <p className="mt-1.5 text-[12px]" style={{ color: 'var(--color-text-secondary)' }}>
          {t(
            'computer.browserSignins.import.askHint',
            'If this computer asks to use your keychain or keyring, allow it.',
          )}
        </p>
      )}
    </div>
  );
}
