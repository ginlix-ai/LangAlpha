/**
 * Where a vendor's logo comes from, for every surface that draws one.
 *
 * Two sources, because the vendors differ in kind rather than in handling. An
 * LLM provider's mark is a file we ship: half of them are local runtimes with
 * no website to ask, and the art is better than any favicon. A connector's
 * mark is fetched live from the vendor's own site through the backend, because
 * the set changes without a frontend release and the good art is only
 * discoverable by reading their page.
 *
 * Both answer with the same `BrandArt`, so `BrandMark` never learns which kind
 * it is drawing. What they do not share is a lookup: provider ids and server
 * names are different keyspaces, and one map over both would silently hand a
 * connector named `openai` the model provider's logo.
 */
import { baseURL } from '@/pages/ChatAgent/utils/api/transport';

import iconOpenai from '@/assets/providers/openai.png';
import iconAnthropic from '@/assets/providers/anthropic.png';
import iconGemini from '@/assets/providers/gemini.png';
import iconOpenrouter from '@/assets/providers/openrouter.png';
import iconZai from '@/assets/providers/z-ai.png';
import iconMinimax from '@/assets/providers/minimax.png';
import iconDashscope from '@/assets/providers/dashscope.png';
import iconVolcengine from '@/assets/providers/volcengine.png';
import iconMoonshot from '@/assets/providers/moonshot.png';
import iconDeepseek from '@/assets/providers/deepseek.png';
import iconGroq from '@/assets/providers/groq.png';
import iconCerebras from '@/assets/providers/cerebras.png';
import iconOllama from '@/assets/providers/ollama.png';
import iconLmStudio from '@/assets/providers/lmstudio.png';
import iconVllm from '@/assets/providers/vllm.png';

export type BrandArt = {
  src: string;
  /** The logo is a transparent glyph and needs a light bed to stay visible. */
  padded?: boolean;
};

/** Local runtimes ship their logo as a dark glyph on transparency. */
const PADDED = new Set(['ollama', 'lm-studio', 'vllm']);

const LLM_PROVIDER_ART: Record<string, string> = {
  openai: iconOpenai,
  'codex-oauth': iconOpenai,
  anthropic: iconAnthropic,
  'claude-oauth': iconAnthropic,
  gemini: iconGemini,
  openrouter: iconOpenrouter,
  'z-ai': iconZai,
  'z-ai-coding': iconZai,
  'z-ai-cn': iconZai,
  'z-ai-cn-coding': iconZai,
  minimax: iconMinimax,
  'minimax-coding': iconMinimax,
  dashscope: iconDashscope,
  volcengine: iconVolcengine,
  'doubao-coding': iconVolcengine,
  moonshot: iconMoonshot,
  'moonshot-coding': iconMoonshot,
  deepseek: iconDeepseek,
  groq: iconGroq,
  cerebras: iconCerebras,
  ollama: iconOllama,
  'lm-studio': iconLmStudio,
  vllm: iconVllm,
};

/** The bundled mark for an LLM provider id, or undefined for one we do not ship. */
export function llmProviderArt(provider: string): BrandArt | undefined {
  const src = LLM_PROVIDER_ART[provider];
  if (!src) return undefined;
  return { src, padded: PADDED.has(provider) };
}

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
 * the same one-line shape as `bundleArt` rather than a lookup like the two
 * maps above.
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
 * A third keyspace, deliberately not folded into either map above: bundle
 * names, provider ids and server names collide by design (`openai` is a
 * plausible value in all three), and one lookup over them would hand a bundle
 * the model provider's logo.
 */
export function bundleArt(bundle: { icon_url?: string | null }): BrandArt | undefined {
  // The manifest decides, not the name.
  if (!bundle.icon_url) return undefined;
  return { src: `${baseURL}${bundle.icon_url}` };
}
