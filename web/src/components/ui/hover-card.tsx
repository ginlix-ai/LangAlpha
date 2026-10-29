import * as React from "react"
import * as HoverCardPrimitive from "@radix-ui/react-hover-card"

import { cn } from "@/lib/utils"

const HoverCard = HoverCardPrimitive.Root

// Radix opens the card on the trigger's focus as well as on pointerenter, and
// an open does not clear an open timer already pending. Focus bubbling up from
// a control inside the trigger (a row holding its own buttons) would arm a
// second timer beside the pointer's: leaving clears only that one, and the
// first opens the card after the pointer has gone. So only the trigger's own
// focus and blur reach Radix; a prevented event skips its handler.
function HoverCardTrigger({
  onFocus,
  onBlur,
  ...props
}: React.ComponentProps<typeof HoverCardPrimitive.Trigger>) {
  return (
    <HoverCardPrimitive.Trigger
      {...props}
      onFocus={(event) => {
        onFocus?.(event)
        if (event.target !== event.currentTarget) event.preventDefault()
      }}
      onBlur={(event) => {
        onBlur?.(event)
        if (event.target !== event.currentTarget) event.preventDefault()
      }}
    />
  )
}

function HoverCardContent({
  className,
  align = "center",
  sideOffset = 4,
  ...props
}: React.ComponentProps<typeof HoverCardPrimitive.Content>) {
  return (
    <HoverCardPrimitive.Portal>
      <HoverCardPrimitive.Content
        align={align}
        sideOffset={sideOffset}
        className={cn(
          "z-50 w-72 rounded-md border bg-popover p-3 text-sm text-popover-foreground shadow-md outline-hidden pop-in",
          className
        )}
        {...props}
      />
    </HoverCardPrimitive.Portal>
  )
}

export { HoverCard, HoverCardTrigger, HoverCardContent }
