/**
 * The model picker's rail: one tab per brand, and within a brand one tab per
 * way its models are reached.
 *
 * A subscription's models carry the same names as the brand's keyed ones, so
 * one list of both would repeat every name with a different tag. Split by
 * connection inside the brand, no list repeats a name, and each connection
 * has one payer, so its tag names the tab instead of tagging every row.
 *
 * The models map files a model the user picked under a coding plan (or a
 * region of it) under that plan, so `brandOf` folds such a group into its
 * brand's: one rail tab per brand, the plan a connection inside it.
 */
import { isSubscription, type Payer } from '@/lib/modelPayer';

export interface ModelConnection {
  /** The payer, or `none`. A subscription's payer names its sign-in, so two
   *  subscriptions are two tabs, while every key the user holds pays alike,
   *  and so does the deployment. */
  id: string;
  models: string[];
  /** Names the tab. None for a model nothing bills, a local server's. */
  payer?: Payer;
}

export interface ModelGroup {
  /** The catalog group, which is also the provider whose mark the rail shows. */
  id: string;
  label: string;
  /** Best payer first. */
  connections: ModelConnection[];
}

// The user's own plans and keys come first: what they pay for themselves is
// what they most likely came to pick.
const RANK = { own_key: 1, server_key: 2, credits: 3 } as const;

function payerRank(payer: Payer | undefined): number {
  if (!payer) return 4;
  return isSubscription(payer) ? 0 : RANK[payer];
}

function rankOf(models: string[], payers: Record<string, Payer>): number {
  return Math.min(...models.map((m) => payerRank(payers[m])));
}

/** The brand a group of the models map belongs to, when it is not its own. */
export type BrandOf = (group: string) => { id: string; label: string } | undefined;

export function modelGroups({ models, payers, brandOf }: {
  models: Record<string, { models?: string[]; display_name?: string }>;
  payers: Record<string, Payer>;
  brandOf?: BrandOf;
}): ModelGroup[] {
  const brands = new Map<string, { label: string; byConnection: Map<string, ModelConnection> }>();
  for (const [group, data] of Object.entries(models)) {
    const brand = brandOf?.(group) ?? { id: group, label: data.display_name ?? group };
    const into = brands.get(brand.id) ?? { label: brand.label, byConnection: new Map() };
    brands.set(brand.id, into);
    for (const m of data.models ?? []) {
      const id = payers[m] ?? 'none';
      const connection = into.byConnection.get(id) ?? { id, models: [], payer: payers[m] };
      into.byConnection.set(id, connection);
      if (!connection.models.includes(m)) connection.models.push(m);
    }
  }
  const groups: ModelGroup[] = [];
  for (const [id, { label, byConnection }] of brands) {
    if (byConnection.size === 0) continue;
    const connections = [...byConnection.values()];
    // Array sort is stable, so the catalog order holds within a rank.
    connections.sort((a, b) => rankOf(a.models, payers) - rankOf(b.models, payers));
    groups.push({ id, label, connections });
  }
  const rank = (g: ModelGroup) => rankOf(g.connections.flatMap((c) => c.models), payers);
  return groups.sort((a, b) => rank(a) - rank(b));
}
