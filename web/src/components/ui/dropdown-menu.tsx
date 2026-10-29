import * as React from "react"
import * as DropdownMenuPrimitive from "@radix-ui/react-dropdown-menu"

import { cn } from "@/lib/utils"
import { closeAutoFocus } from "@/lib/inputModality"

const DropdownMenu = DropdownMenuPrimitive.Root

const DropdownMenuTrigger = DropdownMenuPrimitive.Trigger

const DropdownMenuGroup = DropdownMenuPrimitive.Group

function DropdownMenuContent({
  className,
  sideOffset = 4,
  collisionPadding = 8,
  container,
  onCloseAutoFocus,
  ...props
}: React.ComponentProps<typeof DropdownMenuPrimitive.Content> & {
  container?: HTMLElement | null
}) {
  return (
    <DropdownMenuPrimitive.Portal container={container ?? undefined}>
      <DropdownMenuPrimitive.Content
        sideOffset={sideOffset}
        collisionPadding={collisionPadding}
        onCloseAutoFocus={closeAutoFocus(onCloseAutoFocus)}
        className={cn(
          // Radix measures the space left to the collision boundary and publishes
          // it as --radix-…-available-height; capping there is what keeps a long
          // menu (the model list) from running off-screen with no way to reach
          // the items past the fold.
          "z-1030 min-w-32 max-h-(--radix-dropdown-menu-content-available-height) overflow-y-auto overflow-x-hidden rounded-md border bg-popover p-1 text-popover-foreground shadow-md",
          "pop-in",
          className
        )}
        {...props}
      />
    </DropdownMenuPrimitive.Portal>
  )
}

// The only focus indication a menu item gets. Items wear no ring by design, so
// this tint answers pointer and keyboard alike, and it is written once: a value
// that clears contrast has to reach every shape of item at the same time.
export const ITEM_HIGHLIGHT = "data-highlighted:bg-accent/15"

// Every shape of item shares this box, so a checkbox row lines up with the
// plain rows around it.
export const ITEM_BOX = "relative flex w-full cursor-default select-none items-center gap-2 rounded-sm px-2.5 py-1.5 text-sm transition-colors data-disabled:pointer-events-none data-disabled:opacity-50"

export const DESTRUCTIVE_ITEM = "text-destructive data-highlighted:bg-destructive/10 data-highlighted:text-destructive"

const itemVariants = {
  default: ITEM_HIGHLIGHT,
  destructive: DESTRUCTIVE_ITEM,
} as const

type ItemVariant = keyof typeof itemVariants

function DropdownMenuItem({
  className,
  variant = "default",
  ...props
}: React.ComponentProps<typeof DropdownMenuPrimitive.Item> & {
  variant?: ItemVariant
}) {
  return (
    <DropdownMenuPrimitive.Item
      className={cn(ITEM_BOX, itemVariants[variant], className)}
      {...props}
    />
  )
}

// Radix gives it role="menuitemcheckbox" and aria-checked from `checked`. The
// caller draws the mark, as it draws an item's icon, so the mark sits in the
// same column as the icons on the rows beside it.
function DropdownMenuCheckboxItem({
  className,
  ...props
}: React.ComponentProps<typeof DropdownMenuPrimitive.CheckboxItem>) {
  return (
    <DropdownMenuPrimitive.CheckboxItem
      className={cn(ITEM_BOX, ITEM_HIGHLIGHT, className)}
      {...props}
    />
  )
}

const DropdownMenuRadioGroup = DropdownMenuPrimitive.RadioGroup

// One choice of several: Radix gives it role="menuitemradio" and aria-checked
// from the group's value. As with the checkbox item, the caller draws the mark.
function DropdownMenuRadioItem({
  className,
  ...props
}: React.ComponentProps<typeof DropdownMenuPrimitive.RadioItem>) {
  return (
    <DropdownMenuPrimitive.RadioItem
      className={cn(ITEM_BOX, ITEM_HIGHLIGHT, className)}
      {...props}
    />
  )
}

function DropdownMenuLabel({
  className,
  ...props
}: React.ComponentProps<typeof DropdownMenuPrimitive.Label>) {
  return (
    <DropdownMenuPrimitive.Label
      className={cn("px-2.5 py-1.5 text-sm font-medium", className)}
      {...props}
    />
  )
}

function DropdownMenuSeparator({
  className,
  ...props
}: React.ComponentProps<typeof DropdownMenuPrimitive.Separator>) {
  return (
    <DropdownMenuPrimitive.Separator
      className={cn("-mx-1 my-1 h-px bg-border", className)}
      {...props}
    />
  )
}

const DropdownMenuSub = DropdownMenuPrimitive.Sub

function DropdownMenuSubTrigger({
  className,
  children,
  ...props
}: React.ComponentProps<typeof DropdownMenuPrimitive.SubTrigger>) {
  return (
    <DropdownMenuPrimitive.SubTrigger
      className={cn(
        "flex cursor-default select-none items-center gap-2 rounded-sm px-2.5 py-1.5 text-sm data-[state=open]:bg-accent/15",
        ITEM_HIGHLIGHT,
        className
      )}
      {...props}
    >
      {children}
    </DropdownMenuPrimitive.SubTrigger>
  )
}

// Radix anchors a submenu to its trigger *row*, which the parent panel's
// border + p-1 inset 5px from the panel edge — so the stock offset of 0 opens
// the submenu 5px on top of the panel. Clear that, then add the same 4px gap
// DropdownMenuContent leaves against its own trigger.
function DropdownMenuSubContent({
  className,
  sideOffset = 9,
  collisionPadding = 8,
  ...props
}: React.ComponentProps<typeof DropdownMenuPrimitive.SubContent>) {
  return (
    <DropdownMenuPrimitive.Portal>
      <DropdownMenuPrimitive.SubContent
        sideOffset={sideOffset}
        collisionPadding={collisionPadding}
        className={cn(
          "z-1030 min-w-32 max-h-(--radix-dropdown-menu-content-available-height) overflow-y-auto overflow-x-hidden rounded-md border bg-popover p-1 text-popover-foreground shadow-lg",
          "pop-in",
          className
        )}
        {...props}
      />
    </DropdownMenuPrimitive.Portal>
  )
}

export {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuCheckboxItem,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuGroup,
  DropdownMenuSub,
  DropdownMenuSubTrigger,
  DropdownMenuSubContent,
}
