import { useCallback, useState, memo } from "react"
import { Check } from "lucide-react"
import { cn } from "@/lib/utils"

// Provider icons — maps brand key to imported asset
import iconOpenai from "@/assets/providers/openai.png"
import iconAnthropic from "@/assets/providers/anthropic.png"
import iconGemini from "@/assets/providers/gemini.png"
import iconOpenrouter from "@/assets/providers/openrouter.png"
import iconZai from "@/assets/providers/z-ai.png"
import iconMinimax from "@/assets/providers/minimax.png"
import iconDashscope from "@/assets/providers/dashscope.png"
import iconVolcengine from "@/assets/providers/volcengine.png"
import iconMoonshot from "@/assets/providers/moonshot.png"
import iconDeepseek from "@/assets/providers/deepseek.png"
import iconGroq from "@/assets/providers/groq.png"
import iconCerebras from "@/assets/providers/cerebras.png"
import iconOllama from "@/assets/providers/ollama.png"
import iconLmStudio from "@/assets/providers/lmstudio.png"
import iconVllm from "@/assets/providers/vllm.png"

const PROVIDER_ICONS: Record<string, string> = {
  openai: iconOpenai,
  "codex-oauth": iconOpenai,
  anthropic: iconAnthropic,
  "claude-oauth": iconAnthropic,
  gemini: iconGemini,
  openrouter: iconOpenrouter,
  "z-ai": iconZai,
  "z-ai-coding": iconZai,
  "z-ai-cn": iconZai,
  "z-ai-cn-coding": iconZai,
  minimax: iconMinimax,
  "minimax-coding": iconMinimax,
  dashscope: iconDashscope,
  "dashscope-coding": iconDashscope,
  volcengine: iconVolcengine,
  "doubao-coding": iconVolcengine,
  moonshot: iconMoonshot,
  "moonshot-coding": iconMoonshot,
  deepseek: iconDeepseek,
  groq: iconGroq,
  cerebras: iconCerebras,
  ollama: iconOllama,
  "lm-studio": iconLmStudio,
  vllm: iconVllm,
}

/** Providers whose logos have transparent backgrounds — show a white circle behind them */
const NEEDS_LIGHT_BG = new Set(["ollama", "lm-studio", "vllm"])

const PROVIDER_COLORS: Record<string, string> = {
  openai: "#10A37F",
  "codex-oauth": "#10A37F",
  anthropic: "#D4A574",
  "claude-oauth": "#D4A574",
  gemini: "#4285F4",
  openrouter: "#6366F1",
  "z-ai": "#3B82F6",
  "z-ai-coding": "#3B82F6",
  "z-ai-cn": "#3B82F6",
  "z-ai-cn-coding": "#3B82F6",
  minimax: "#F59E0B",
  "minimax-coding": "#F59E0B",
  dashscope: "#FF6A00",
  "dashscope-coding": "#FF6A00",
  volcengine: "#1E88E5",
  "doubao-coding": "#1E88E5",
  moonshot: "#8B5CF6",
  "moonshot-coding": "#8B5CF6",
  deepseek: "#0EA5E9",
  "lm-studio": "#6B7280",
  vllm: "#6B7280",
  ollama: "#1A1A1A",
}

export interface ProviderCardProps {
  provider: string
  displayName: string
  selected?: boolean
  configured?: boolean
  onSelect: (provider: string) => void
}

export const ProviderCard = memo(function ProviderCard({
  provider,
  displayName,
  selected = false,
  configured = false,
  onSelect,
}: ProviderCardProps) {
  const icon = PROVIDER_ICONS[provider]
  const color = PROVIDER_COLORS[provider] ?? "var(--color-accent-primary)"
  const initial = displayName.charAt(0).toUpperCase()
  const [hovered, setHovered] = useState(false)
  const [imgError, setImgError] = useState(false)

  const handleClick = useCallback(() => {
    onSelect(provider)
  }, [onSelect, provider])

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault()
        onSelect(provider)
      }
    },
    [onSelect, provider],
  )

  return (
    <div
      role="radio"
      aria-checked={selected}
      tabIndex={0}
      onClick={handleClick}
      onKeyDown={handleKeyDown}
      className={cn(
        "relative flex flex-col items-center justify-center gap-2 cursor-pointer",
        "rounded-lg p-4 min-w-[80px] min-h-[64px] transition-colors select-none",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-background",
      )}
      style={{
        border: selected
          ? "2px solid var(--color-accent-primary)"
          : "1px solid var(--color-border-default)",
        background: selected
          ? "var(--color-accent-soft)"
          : hovered
            ? "var(--color-bg-surface)"
            : undefined,
        // Compensate for border width change to prevent layout shift
        padding: selected ? 15 : 16,
      }}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
    >
      {/* Configured indicator */}
      {configured && (
        <span
          className="absolute top-2 right-2 flex items-center justify-center w-4 h-4 rounded-full"
          style={{ background: "var(--color-success)" }}
          aria-label="API key configured"
        >
          <Check className="w-2.5 h-2.5" style={{ color: "#fff" }} strokeWidth={3} />
        </span>
      )}

      {/* Provider icon or fallback initial */}
      {icon && !imgError ? (
        <img
          src={icon}
          alt=""
          aria-hidden="true"
          className="w-10 h-10 rounded-full object-contain shrink-0"
          style={NEEDS_LIGHT_BG.has(provider) ? { background: "#fff", padding: 2 } : undefined}
          onError={() => setImgError(true)}
        />
      ) : (
        <div
          className="flex items-center justify-center w-10 h-10 rounded-full text-sm font-semibold shrink-0"
          style={{ background: color, color: "#fff" }}
          aria-hidden="true"
        >
          {initial}
        </div>
      )}

      {/* Provider name */}
      <span
        className="text-xs font-medium text-center leading-tight"
        style={{ color: "var(--color-text-primary)" }}
      >
        {displayName}
      </span>
    </div>
  )
})
