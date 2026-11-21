/**
 * Bringing the sign-ins of a browser the person uses on their own machine into
 * that computer's browser. The machine does the work; the app only lists its
 * browsers and says which one, then reports how many cookies came.
 */
import { useMutation, useQuery } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';

import { toast } from '@/components/ui/use-toast';
import { queryKeys } from '@/lib/queryKeys';
import { useLocale } from '@/hooks/useLocale';
import { createFormatter } from '@/lib/format';

import {
  bringBrowserSignins,
  listBrowserSigninSources,
  type BrowserSigninRefusal,
  type BrowserSigninSource,
} from '../utils/api/computers';

const formatCount = createFormatter({ maximumFractionDigits: 0 });

/** The browsers on the machine, read while the computer runs and keeps sign-ins. */
export function useBrowserSigninSources(computerId: string, options: { enabled: boolean }) {
  return useQuery({
    queryKey: queryKeys.computers.signinSources(computerId),
    queryFn: () => listBrowserSigninSources(computerId),
    enabled: options.enabled,
    // A browser installed or a profile added is rare, and each read reaches the
    // machine. A refusal is not kept: the computer starting or the save landing
    // changes it, and the next mount asks again.
    staleTime: (query) => (query.state.data?.reason ? 0 : 5 * 60_000),
    refetchOnWindowFocus: false,
    retry: false,
  });
}

export function useBringBrowserSignins(computerId: string) {
  const { t } = useTranslation();
  const locale = useLocale();
  const failed = (reason: BrowserSigninRefusal | null, source: BrowserSigninSource) => {
    const name = source.name;
    const why: Record<BrowserSigninRefusal, string> = {
      keychain: t(
        'computer.browserSignins.import.keychain',
        "Couldn't unlock {{name}}'s sign-ins. Allow it when this computer asks for your password, or connect it with the desktop app so it can ask.",
        { name },
      ),
      not_kept: t(
        'computer.browserSignins.import.notKept',
        'The browser is open without kept sign-ins. Try again once it closes.',
      ),
      low_memory: t(
        'computer.browserSignins.import.lowMemory',
        "This computer is short on memory, so the browser can't open. Try again later.",
      ),
      not_running: t('computer.browserSignins.import.notRunning', "The computer isn't running."),
      not_found: t('computer.browserSignins.import.notFound', '{{name}} is no longer on this computer.', { name }),
      unavailable: t('computer.browserSignins.import.failed', "Couldn't bring in sign-ins."),
      failed: t('computer.browserSignins.import.failed', "Couldn't bring in sign-ins."),
    };
    toast({ variant: 'destructive', title: why[reason ?? 'failed'] });
  };
  return useMutation({
    mutationFn: (source: BrowserSigninSource) => bringBrowserSignins(computerId, source),
    onSuccess: (result, source) => {
      if (!result.done) {
        failed(result.reason, source);
        return;
      }
      toast({
        title: t('computer.browserSignins.import.done', 'Brought in sign-ins from {{name}}', {
          name: `${source.name} · ${source.profile_name}`,
        }),
        description: result.skipped
          ? t('computer.browserSignins.import.countSkipped', '{{cookies}} cookies, {{skipped}} could not be read', {
              cookies: formatCount(result.count, locale),
              skipped: formatCount(result.skipped, locale),
            })
          : t('computer.browserSignins.import.count', '{{cookies}} cookies', {
              cookies: formatCount(result.count, locale),
            }),
      });
    },
    onError: (_err, source) => failed('failed', source),
  });
}
