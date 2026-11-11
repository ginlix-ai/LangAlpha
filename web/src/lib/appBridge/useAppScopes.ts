import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import * as z from 'zod/mini';
import { toast } from '@/components/ui/use-toast';
import { usePreferenceKey } from '@/hooks/usePreferenceKey';
import { SCOPES, type Scope } from './protocol';

/** Ask-first scopes the user allowed, by grant key (`appGrantKey`). */
const KEY = 'app_scopes';

// Each entry recovers alone. Scopes this build does not know are kept, so an
// older tab writing the map back never strips what a newer one granted.
const GrantsSchema = z.pipe(
  z.catch(z.record(z.string(), z.unknown()), {}),
  z.transform((m) => Object.fromEntries(
    Object.entries(m).map(([key, list]) => [key, Array.isArray(list) ? list.filter((s): s is string => typeof s === 'string') : []]),
  ) as Record<string, string[]>),
);

const readGrants = (raw: unknown): Record<string, string[]> => {
  const parsed = GrantsSchema.safeParse(raw);
  return parsed.success ? parsed.data : {};
};

const isScope = (s: string): s is Scope => (SCOPES as readonly string[]).includes(s);

/**
 * The key an app's grants are kept under: its `/a/` code, and the digest of
 * the command that starts it when the server names one, so an app whose
 * command changed is a new app to the user and asks again.
 */
export function appGrantKey(link: { code: string; command_key?: string | null }): string {
  return link.command_key ? `${link.code}#${link.command_key}` : link.code;
}

/**
 * The scopes the user allowed one frame, kept by its `appGrantKey`: the code
 * is stable per workspace and port while the app's own origin changes with
 * every signed URL, so a grant survives the app's restarts and follows it from
 * the file panel to the dashboard. With no key nothing is stored.
 */
export function useAppScopes(grantKey: string | null): {
  granted: ReadonlySet<Scope>;
  /** Both answer whether the change was saved, having told the user when it was not. */
  grant: (scopes: Scope[]) => Promise<boolean>;
  /** Take a grant back. The frame is refused from the moment it is saved. */
  revoke: (scopes: Scope[]) => Promise<boolean>;
} {
  const { t } = useTranslation();
  const { value: all, write } = usePreferenceKey(KEY, readGrants);
  const mine = grantKey ? all[grantKey] : undefined;
  const granted = useMemo(() => new Set((mine ?? []).filter(isScope)), [mine]);

  // The preference merge is shallow, so the whole map is written back, from
  // the server's latest rather than this render's copy.
  const change = async (next: (held: string[]) => string[]): Promise<boolean> => {
    if (!grantKey) return false;
    const result = await write((latest) => ({ next: { ...latest, [grantKey]: next(latest[grantKey] ?? []) } }));
    if (result.status === 'saved') return true;
    toast({ variant: 'destructive', title: t('appBridge.saveFailed') });
    return false;
  };

  return {
    granted,
    grant: (scopes) => change((held) => [...new Set([...held, ...scopes])]),
    revoke: (scopes) => change((held) => held.filter((s) => !(scopes as string[]).includes(s))),
  };
}
