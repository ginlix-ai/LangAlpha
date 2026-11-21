/**
 * Saving what a computer's browser is set to, from the computer's details and
 * the Browser tab's menu. The change shows at once on that computer's
 * `browser`, and each save answers for its own keys alone: two saves in flight
 * on one computer, or on two, never undo or overwrite each other.
 */
import { useMutation, useQueryClient, type QueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';

import { apiErrorReason } from '@/api/errors';
import { toast } from '@/components/ui/use-toast';
import { queryKeys } from '@/lib/queryKeys';
import type { ComputerBrowser, ComputersResponse } from '@/types/api';

import { setComputerBrowser, type BrowserChoices } from '../utils/api';

type BrowserChoice = keyof BrowserChoices;
const BROWSER_CHOICES: readonly BrowserChoice[] = ['quality', 'window', 'signins'];

interface BrowserChoicesVars {
  computerId: string;
  patch: BrowserChoices;
}

/** Rewrite one computer's browser in every cached list. */
function updateBrowser(
  queryClient: QueryClient,
  computerId: string,
  update: (browser: ComputerBrowser) => ComputerBrowser,
) {
  queryClient.setQueriesData<ComputersResponse | undefined>(
    { queryKey: queryKeys.computers.lists() },
    (prev) => !prev?.computers ? prev : {
      ...prev,
      computers: prev.computers.map((c) =>
        c.computer_id === computerId && c.browser ? { ...c, browser: update(c.browser) } : c),
    },
  );
}

/** The last save started of each computer's key, per cache. A key is a save's
 *  to settle only while no later save of it has started: comparing values
 *  cannot tell light, retina, light apart, so the first light's refusal would
 *  undo the last one's success. */
const latestSaves = new WeakMap<QueryClient, Map<string, number>>();
let saveCount = 0;

function savesOf(queryClient: QueryClient): Map<string, number> {
  let saves = latestSaves.get(queryClient);
  if (!saves) {
    saves = new Map();
    latestSaves.set(queryClient, saves);
  }
  return saves;
}

/** `browser` with the keys this save still owns taken from `from`. */
function settle(
  queryClient: QueryClient,
  computerId: string,
  save: number,
  browser: ComputerBrowser,
  patch: BrowserChoices,
  from: ComputerBrowser,
): ComputerBrowser {
  const saves = savesOf(queryClient);
  const owned = BROWSER_CHOICES.filter((key) => key in patch && saves.get(`${computerId}/${key}`) === save);
  return owned.reduce<ComputerBrowser>(
    (next, key) => ({ ...next, [key]: from[key] }),
    browser,
  );
}

function browserOf(queryClient: QueryClient, computerId: string): ComputerBrowser | undefined {
  return queryClient
    .getQueriesData<ComputersResponse | undefined>({ queryKey: queryKeys.computers.lists() })
    .flatMap(([, data]) => data?.computers ?? [])
    .find((c) => c.computer_id === computerId)?.browser;
}

export interface BrowserChoicesMutation {
  /** Only the keys present are saved. */
  mutate: (vars: BrowserChoicesVars) => void;
  /** A save for this computer is in flight. */
  busyFor: (computerId: string) => boolean;
}

/** A refusal puts back only what this save changed, and says why. Saving
 *  `signins` as it was signs the browser out of every site. */
export function useSetBrowserChoices(): BrowserChoicesMutation {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const failed: Record<BrowserChoice, string> = {
    quality: t('computer.browserQuality.failed', "Couldn't change the live view quality"),
    window: t('computer.browserWindow.failed', "Couldn't change the browser window"),
    signins: t('computer.browserSignins.failed', "Couldn't change kept sign-ins"),
  };
  const { mutate, isPending, variables } = useMutation({
    mutationFn: ({ computerId, patch }: BrowserChoicesVars) => setComputerBrowser(computerId, patch),
    onMutate: ({ computerId, patch }) => {
      const before = browserOf(queryClient, computerId);
      const save = ++saveCount;
      const saves = savesOf(queryClient);
      for (const key of BROWSER_CHOICES) if (key in patch) saves.set(`${computerId}/${key}`, save);
      updateBrowser(queryClient, computerId, (browser) => ({ ...browser, ...patch }));
      return { before, save };
    },
    onSuccess: (answer, { computerId, patch }, ctx) => {
      if (ctx) {
        updateBrowser(queryClient, computerId, (browser) =>
          settle(queryClient, computerId, ctx.save, browser, patch, answer.browser));
      }
      if (patch.signins === undefined) return;
      // The browsers it lists hang on kept sign-ins, and one read before this landed was refused.
      void queryClient.invalidateQueries({ queryKey: queryKeys.computers.signinSources(computerId) });
      if (!patch.signins || !ctx?.before?.signins) return;
      if (answer.applied) {
        toast({ title: t('computer.browserSignins.signedOut', 'Signed out of every site') });
        return;
      }
      toast({
        title: t('computer.browserSignins.signOutWaits', "Saved, but this computer didn't answer"),
        description: t(
          'computer.browserSignins.signOutWaitsHint',
          'Its browser signs out of every site the next time the computer is used.',
        ),
      });
    },
    onError: (err, { computerId, patch }, ctx) => {
      const before = ctx?.before;
      if (ctx && before) {
        updateBrowser(queryClient, computerId, (browser) =>
          settle(queryClient, computerId, ctx.save, browser, patch, before));
      }
      const key = BROWSER_CHOICES.find((choice) => choice in patch) ?? 'quality';
      toast({ variant: 'destructive', title: failed[key], description: apiErrorReason(err) ?? undefined });
    },
    onSettled: () => queryClient.invalidateQueries({ queryKey: queryKeys.computers.lists() }),
  });
  return {
    mutate: (vars) => mutate(vars),
    busyFor: (computerId) => isPending && variables?.computerId === computerId,
  };
}
