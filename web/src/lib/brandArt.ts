/**
 * Where a vendor's logo comes from, for every surface that draws one.
 *
 * Two sources, because the vendors differ in kind rather than in handling. An
 * LLM provider's mark is a file we ship: half of them are local runtimes with
 * no website to ask, and the art is better than any favicon. A connector's
 * mark is fetched live from the vendor's own site through the backend, because
 * the set changes without a frontend release and the good art is only
 * discoverable by reading their page. The shipped marks live in
 * `providerArt`, so the logos load only with the surfaces that draw them.
 *
 * Both answer with the same `BrandArt`, so `BrandMark` never learns which kind
 * it is drawing. What they do not share is a lookup: provider ids and server
 * names are different keyspaces, and one map over both would silently hand a
 * connector named `openai` the model provider's logo.
 */
import { baseURL } from '@/pages/ChatAgent/utils/api/transport';

export type BrandArt = {
  src: string;
  /**
   * Drawn in black for a light ground, as a monochrome mark is, so a dark
   * surface has to turn it light. An accent it carries keeps its color.
   */
  ink?: boolean;
  /** A glyph with no ground of its own, so there is no edge to round, and a
   *  rounded box would only cut off whatever reaches its corners. */
  bare?: boolean;
  /** The word a model's name opens with that the mark says already, dropped
   *  where room is short (`nameBesideMark`): "GPT-6.2 Sol" beside the OpenAI
   *  mark reads "6.2 Sol". A host whose models come from many makers
   *  (OpenRouter, a local server) has none. */
  brand?: string;
};

/**
 * The endpoint that proxies a shipped brokerage's own logo.
 *
 * Never a lookup: the backend holds the list of brokers it ships and whether
 * each one turned out to have usable art, and re-deciding that here would put
 * a second copy of both facts in the client. A broker with no mark 404s and
 * the row falls back, which is the same thing an unknown name does.
 *
 * Takes the vendor rather than its name so the three rows that resolve one off
 * an address hand it straight over. A row pointing somewhere we do not
 * recognise has no vendor and no art, and that is one answer here instead of
 * the same ternary at every call.
 */
export function brokerageArt(
  vendor: { name: string } | null | undefined,
): BrandArt | undefined {
  if (!vendor) return undefined;
  return {
    src: `${baseURL}/api/v1/mcp/brokerages/${encodeURIComponent(vendor.name)}/icon`,
  };
}

/**
 * The mark an MCP server declared for itself in the handshake.
 *
 * A fourth keyspace and the only one whose art we neither ship nor look up:
 * the server named it, the host fetched it, and what arrives here is a path
 * this origin already serves. There is nothing to decide, which is why this is
 * the same one-line shape as `bundleArt` rather than a lookup like the
 * provider map.
 */
export function mcpServerArt(server: {
  icon_url?: string | null;
}): BrandArt | undefined {
  if (!server.icon_url) return undefined;
  return { src: `${baseURL}${server.icon_url}` };
}

/**
 * A shipped bundle's mark, when it wears someone else's brand.
 *
 * Only someone else's: our own bundles draw `LangAlphaMark`, which is inline
 * and takes the tile's own color, so there is nothing here to return for them.
 * A wrapper bundle is the opposite case in every way. The brand is not ours to
 * redraw, it changes without a frontend release, and the good art is only
 * discoverable by reading the vendor's page, so the manifest names a site and
 * the backend resolves it.
 *
 * A third keyspace, deliberately not folded into any other lookup: bundle
 * names, provider ids and server names collide by design (`openai` is a
 * plausible value in all three), and one lookup over them would hand a bundle
 * the model provider's logo.
 */
export function bundleArt(bundle: { icon_url?: string | null }): BrandArt | undefined {
  // The manifest decides, not the name.
  if (!bundle.icon_url) return undefined;
  return { src: `${baseURL}${bundle.icon_url}` };
}
