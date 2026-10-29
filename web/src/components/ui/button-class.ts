import { cn } from "../../lib/utils"

export type ButtonVariant = "default" | "destructive" | "outline" | "secondary" | "ghost" | "link"
export type ButtonSize = "default" | "sm" | "lg" | "icon"

// Accent discipline (DESIGN.md): the amber is annotation-only — never a
// button fill, hover wash, or focus ring. Primary is the neutral
// near-white/near-charcoal pair; hovers are the neutral tint.
const variants: Record<ButtonVariant, string> = {
  default:
    "bg-(--color-btn-primary-bg) text-(--color-btn-primary-text) hover:opacity-90",
  destructive: "bg-destructive text-destructive-foreground hover:bg-destructive/90",
  outline: "border border-input bg-background hover:bg-(--color-bg-hover)",
  secondary: "bg-secondary text-secondary-foreground hover:bg-secondary/80",
  ghost: "hover:bg-(--color-bg-hover)",
  link: "underline-offset-4 hover:underline",
}

const sizes: Record<ButtonSize, string> = {
  default: "h-10 px-4 py-2",
  sm: "h-9 rounded-md px-3",
  lg: "h-11 rounded-md px-8",
  icon: "h-10 w-10",
}

/** A button's look, for an element that has to be a link but reads as a button. */
export function buttonClass({
  variant = "default",
  size = "default",
  className,
}: { variant?: ButtonVariant; size?: ButtonSize; className?: string } = {}) {
  return cn(
    // Every variant but a link is a committing control, so it presses in
    // (`press`, styles/animations.css); a link only underlines.
    "inline-flex items-center justify-center rounded-md text-sm font-medium disabled:pointer-events-none disabled:opacity-50",
    variant === "link" ? "transition-colors" : "press",
    variants[variant],
    sizes[size],
    className
  )
}
