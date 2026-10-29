import * as React from "react"
import * as TooltipPrimitive from "@radix-ui/react-tooltip"

import { cn } from "@/lib/utils"

const TooltipProvider = TooltipPrimitive.Provider

const Tooltip = TooltipPrimitive.Root

const TooltipTrigger = TooltipPrimitive.Trigger

function TooltipContent({
  className,
  sideOffset = 4,
  collisionPadding = 8,
  style,
  ...props
}: React.ComponentProps<typeof TooltipPrimitive.Content>) {
  return (
    <TooltipPrimitive.Portal>
      <TooltipPrimitive.Content
        sideOffset={sideOffset}
        collisionPadding={collisionPadding}
        className={cn(
          "z-1030 overflow-hidden rounded-md px-2.5 py-1.5 text-xs shadow-md pop-in",
          className
        )}
        style={{
          backgroundColor: "var(--color-bg-elevated)",
          border: "1px solid var(--color-border-muted)",
          color: "var(--color-text-primary)",
          ...style,
        }}
        {...props}
      />
    </TooltipPrimitive.Portal>
  )
}

export { Tooltip, TooltipTrigger, TooltipContent, TooltipProvider }
