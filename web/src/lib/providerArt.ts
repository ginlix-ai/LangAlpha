/**
 * The LLM providers' marks, which ship with the app.
 *
 * Apart from `brandArt` because the art is inlined into whatever chunk holds
 * this map, and the URL-based helpers there are reached from the sidebar on
 * every page: together they put every provider's logo on first load, for
 * surfaces (model pickers, provider setup) that all load lazily.
 */
import type { BrandArt } from '@/lib/brandArt';

import iconOpenai from '@/assets/providers/openai.svg';
import iconAnthropic from '@/assets/providers/anthropic.svg';
import iconClaude from '@/assets/providers/claude.svg';
import iconGemini from '@/assets/providers/gemini.svg';
import iconOpenrouter from '@/assets/providers/openrouter.svg';
import iconZai from '@/assets/providers/z-ai.svg';
import iconMinimax from '@/assets/providers/minimax.svg';
import iconQwen from '@/assets/providers/qwen.svg';
import iconKimi from '@/assets/providers/kimi.svg';
import iconDeepseek from '@/assets/providers/deepseek.svg';
import iconGroq from '@/assets/providers/groq.svg';
import iconCerebras from '@/assets/providers/cerebras.svg';
import iconOllama from '@/assets/providers/ollama.svg';
import iconLmStudio from '@/assets/providers/lmstudio.svg';
import iconVllm from '@/assets/providers/vllm.svg';
import iconXai from '@/assets/providers/xai.svg';
import iconOpencode from '@/assets/providers/opencode.svg';

/*
 * Each mark is the vendor's glyph without the app-icon tile it often sits on,
 * since every surface here gives it a ground of its own (the subagent badge's
 * white disc, a provider card) and a square inside that reads as a blot at
 * badge size. A family's own mark stands in where its models are what people
 * know (Gemini, Qwen, Kimi).
 */
const mark = (src: string, ink = false, brand?: string): BrandArt => ({ src, ink, bare: true, brand });

const OPENAI = mark(iconOpenai, true, 'GPT');
const ANTHROPIC = mark(iconAnthropic, true, 'Claude');
const OPENROUTER = mark(iconOpenrouter, true);
const ZAI = mark(iconZai, true, 'GLM');
const MINIMAX = mark(iconMinimax, false, 'MiniMax');
const QWEN = mark(iconQwen, false, 'Qwen');
const KIMI = mark(iconKimi, true, 'Kimi');
const XAI = mark(iconXai, true, 'Grok');
const OPENCODE = mark(iconOpencode, true);

/** Every manifest provider and its variants, which wear their parent's mark. */
const LLM_PROVIDER_ART: Record<string, BrandArt> = {
  openai: OPENAI,
  'codex-oauth': OPENAI,
  anthropic: ANTHROPIC,
  'claude-oauth': ANTHROPIC,
  'claude-code': mark(iconClaude, false, 'Claude'),
  gemini: mark(iconGemini, false, 'Gemini'),
  xai: XAI,
  'grok-oauth': XAI,
  openrouter: OPENROUTER,
  'z-ai': ZAI,
  'z-ai-coding': ZAI,
  'z-ai-cn': ZAI,
  'z-ai-cn-coding': ZAI,
  minimax: MINIMAX,
  'minimax-coding': MINIMAX,
  'minimax-cn-coding': MINIMAX,
  dashscope: QWEN,
  'dashscope-intl': QWEN,
  moonshot: KIMI,
  'moonshot-coding': KIMI,
  'moonshot-cn-coding': KIMI,
  'opencode-go': OPENCODE,
  'opencode-go-chat': OPENCODE,
  'opencode-go-responses': OPENCODE,
  'opencode-go-google': OPENCODE,
  deepseek: mark(iconDeepseek, false, 'DeepSeek'),
  groq: mark(iconGroq),
  cerebras: mark(iconCerebras, true),
  ollama: mark(iconOllama, true),
  'lm-studio': mark(iconLmStudio, true),
  vllm: mark(iconVllm),
};

/** The bundled mark for an LLM provider id, or undefined for one we do not ship. */
export function llmProviderArt(provider: string): BrandArt | undefined {
  return Object.hasOwn(LLM_PROVIDER_ART, provider) ? LLM_PROVIDER_ART[provider] : undefined;
}

/**
 * A model's name as drawn beside its maker's mark where room is short: without
 * the brand word the mark already shows. The word must end where the name's
 * next part starts (a space, a hyphen, or a version run on as in "Qwen3.8"),
 * so a name that only begins with the same letters keeps them.
 */
export function nameBesideMark(name: string, provider: string | undefined): { shown: string; brand?: string } {
  const brand = provider ? llmProviderArt(provider)?.brand : undefined;
  if (!brand || !name.startsWith(brand)) return { shown: name };
  const next = name.charAt(brand.length);
  const rest = next === ' ' || next === '-' ? name.slice(brand.length + 1) : /\d/.test(next) ? name.slice(brand.length) : '';
  return rest.trim() ? { shown: rest.trim(), brand } : { shown: name };
}
