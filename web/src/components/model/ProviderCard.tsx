import { useCallback, useState, memo } from "react"
import { Check } from "lucide-react"
import { BrandMark } from "@/pages/ChatAgent/components/mcp/BrandMark"
import { llmProviderArt } from "@/lib/brandArt"
import { cn } from "@/lib/utils"

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
  const [hovered, setHovered] = useState(false)

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

      {/* Provider mark, or this provider's monogram when we ship no art */}
      <BrandMark
        name={displayName}
        art={llmProviderArt(provider)}
        size="lg"
        className="rounded-full"
      />

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
