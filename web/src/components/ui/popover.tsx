import * as React from "react"
import * as PopoverPrimitive from "@radix-ui/react-popover"

import { cn } from "@/lib/utils"
import { closeAutoFocus } from "@/lib/inputModality"

const Popover = PopoverPrimitive.Root

const PopoverTrigger = PopoverPrimitive.Trigger

const PopoverAnchor = PopoverPrimitive.Anchor

const PopoverClose = PopoverPrimitive.Close

function PopoverContent({
  className,
  align = "center",
  sideOffset = 4,
  onCloseAutoFocus,
  ...props
}: React.ComponentProps<typeof PopoverPrimitive.Content>) {
  return (
    <PopoverPrimitive.Portal>
      <PopoverPrimitive.Content
        align={align}
        sideOffset={sideOffset}
        onCloseAutoFocus={closeAutoFocus(onCloseAutoFocus)}
        className={cn(
          "z-1030 w-72 rounded-md border bg-popover p-4 text-popover-foreground shadow-md outline-hidden pop-in",
          className
        )}
        {...props}
      />
    </PopoverPrimitive.Portal>
  )
}

export { Popover, PopoverTrigger, PopoverContent, PopoverAnchor, PopoverClose }
