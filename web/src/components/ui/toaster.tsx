import { CircleAlert } from "lucide-react"
import {
  Toast,
  ToastClose,
  ToastDescription,
  ToastProvider,
  ToastTitle,
  ToastViewport,
} from "@/components/ui/toast"
import { useToast } from "@/components/ui/use-toast"

/**
 * How long a toast stays when its caller did not say. A confirmation is read
 * at a glance. A failure carries a sentence the user needs, and three seconds
 * was gone before the second line was read. A toast with an action stays
 * longest, and Radix holds its timer while it is hovered or focused, so the
 * answer is not taken away mid-reach; but it still leaves, because one that
 * never does sits on whatever controls are under it. A notice that truly must
 * stay passes `duration: Infinity` itself.
 */
function defaultDuration(variant: unknown, action: unknown): number {
  if (action) return 10000
  return variant === "destructive" ? 8000 : 3000
}

export function Toaster() {
  const { toasts } = useToast()

  return (
    <ToastProvider duration={3000}>
      {/* `pinned` is store policy, not a Radix prop — the rest of props is
          spread onto the DOM node, and React warns on an unknown attribute. */}
      {toasts.map(function ({ id, title, description, action, icon, variant, pinned: _pinned, ...props }) {
        // A failure always says so with the alert glyph; any other toast shows
        // the glyph its caller gave, or none.
        const glyph =
          variant === "destructive" ? (
            <CircleAlert className="h-4 w-4 text-(--color-loss)" />
          ) : (
            icon
          )
        return (
          <Toast
            key={id}
            {...props}
            duration={
              typeof props.duration === "number"
                ? props.duration
                : defaultDuration(variant, action)
            }
          >
            {/* The glyph rides with the text, not the row, so it stays on the
                title's line however tall the description or the action is. */}
            <div className="flex min-w-0 flex-1 items-start gap-2.5">
              {glyph && (
                <span aria-hidden="true" className="flex h-5 shrink-0 items-center">
                  {glyph}
                </span>
              )}
              <div className="grid min-w-0 gap-0.5">
                {title && <ToastTitle>{title}</ToastTitle>}
                {description && (
                  <ToastDescription>{description}</ToastDescription>
                )}
              </div>
            </div>
            <div className="flex shrink-0 items-center gap-1">
              {action}
              <ToastClose />
            </div>
          </Toast>
        )
      })}
      <ToastViewport />
    </ToastProvider>
  )
}
