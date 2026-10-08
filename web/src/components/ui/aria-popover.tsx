"use client"

import type { ComponentProps } from "react"
import {
  Dialog as AriaDialog,
  type DialogProps as AriaDialogProps,
  DialogTrigger as AriaDialogTrigger,
  Popover as AriaPopover,
  composeRenderProps,
} from "react-aria-components"

import { cn } from "@/lib/utils"

const PopoverTrigger = AriaDialogTrigger

// The Radix popover's layer and entrance, so the two kinds of floating panel
// stack and move alike. The layer is a style, not a class: react-aria writes
// its own z-index inline, and only a style prop is spread after it. A panel
// on its way out takes no presses, so a click made while it fades reaches
// what is under it instead of a row of a list that is already closed.
const Popover = ({ className, style, offset = 4, ...props }: ComponentProps<typeof AriaPopover>) => (
  <AriaPopover
    offset={offset}
    style={composeRenderProps(style, (style) => ({ zIndex: 1030, ...style }))}
    className={composeRenderProps(className, (className) =>
      cn(
        "rounded-md border bg-popover text-popover-foreground shadow-md outline-hidden pop-in data-exiting:pointer-events-none",
        className
      )
    )}
    {...props}
  />
)

function PopoverDialog({ className, ...props }: AriaDialogProps) {
  return (
    <AriaDialog className={cn("p-4 outline-solid outline-0", className)} {...props} />
  )
}

export { Popover, PopoverTrigger, PopoverDialog }
