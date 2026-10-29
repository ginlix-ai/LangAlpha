import * as React from "react"
import * as ContextMenuPrimitive from "@radix-ui/react-context-menu"

import { cn } from "@/lib/utils"
import { closeAutoFocus } from "@/lib/inputModality"
import { DESTRUCTIVE_ITEM, ITEM_BOX, ITEM_HIGHLIGHT } from "./dropdown-menu"

const ContextMenu = ContextMenuPrimitive.Root

const ContextMenuTrigger = ContextMenuPrimitive.Trigger

const ContextMenuGroup = ContextMenuPrimitive.Group

function ContextMenuContent({
  className,
  collisionPadding = 8,
  onCloseAutoFocus,
  ...props
}: React.ComponentProps<typeof ContextMenuPrimitive.Content>) {
  return (
    <ContextMenuPrimitive.Portal>
      <ContextMenuPrimitive.Content
        collisionPadding={collisionPadding}
        onCloseAutoFocus={closeAutoFocus(onCloseAutoFocus)}
        className={cn(
          "z-1030 min-w-32 overflow-hidden rounded-md border bg-popover p-1 text-popover-foreground shadow-md",
          "pop-in",
          className
        )}
        {...props}
      />
    </ContextMenuPrimitive.Portal>
  )
}

// The dropdown's item box, so a row's right-click menu and its options menu
// read as the same menu.
const itemVariants = {
  default: ITEM_HIGHLIGHT,
  destructive: DESTRUCTIVE_ITEM,
} as const

function ContextMenuItem({
  className,
  variant = "default",
  ...props
}: React.ComponentProps<typeof ContextMenuPrimitive.Item> & {
  variant?: keyof typeof itemVariants
}) {
  return (
    <ContextMenuPrimitive.Item
      className={cn(ITEM_BOX, itemVariants[variant], className)}
      {...props}
    />
  )
}

function ContextMenuSeparator({
  className,
  ...props
}: React.ComponentProps<typeof ContextMenuPrimitive.Separator>) {
  return (
    <ContextMenuPrimitive.Separator
      className={cn("-mx-1 my-1 h-px bg-border", className)}
      {...props}
    />
  )
}

export {
  ContextMenu,
  ContextMenuTrigger,
  ContextMenuContent,
  ContextMenuItem,
  ContextMenuSeparator,
  ContextMenuGroup,
}
