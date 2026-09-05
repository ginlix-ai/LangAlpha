import * as React from "react"
import * as DropdownMenuPrimitive from "@radix-ui/react-dropdown-menu"

import { cn } from "@/lib/utils"
import { lastInputWasPointer } from "@/lib/inputModality"

const DropdownMenu = DropdownMenuPrimitive.Root

const DropdownMenuTrigger = DropdownMenuPrimitive.Trigger

const DropdownMenuGroup = DropdownMenuPrimitive.Group

const DropdownMenuContent = React.forwardRef<
  React.ComponentRef<typeof DropdownMenuPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.Content> & {
    container?: HTMLElement | null
  }
>(({ className, sideOffset = 4, collisionPadding = 8, container, onCloseAutoFocus, ...props }, ref) => (
  <DropdownMenuPrimitive.Portal container={container ?? undefined}>
    <DropdownMenuPrimitive.Content
      ref={ref}
      sideOffset={sideOffset}
      collisionPadding={collisionPadding}
      onCloseAutoFocus={(event) => {
        onCloseAutoFocus?.(event)
        // Radix hands focus back to the trigger on close so a keyboard user
        // keeps their place. Chromium carries :focus-visible across that
        // programmatic move, so a menu opened and dismissed with the mouse
        // leaves the trigger ringed until the next click. Skip the restore
        // when no key was pressed: focus falls to <body>, which is where
        // clicking anywhere else would have put it anyway.
        if (!event.defaultPrevented && lastInputWasPointer()) event.preventDefault()
      }}
      className={cn(
        // Radix measures the space left to the collision boundary and publishes
        // it as --radix-…-available-height; capping there is what keeps a long
        // menu (the model list) from running off-screen with no way to reach
        // the items past the fold.
        "z-[1030] min-w-[8rem] max-h-[var(--radix-dropdown-menu-content-available-height)] overflow-y-auto overflow-x-hidden rounded-md border bg-popover p-1 text-popover-foreground shadow-md",
        "data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95 data-[side=bottom]:slide-in-from-top-2 data-[side=left]:slide-in-from-right-2 data-[side=right]:slide-in-from-left-2 data-[side=top]:slide-in-from-bottom-2",
        className
      )}
      {...props}
    />
  </DropdownMenuPrimitive.Portal>
))
DropdownMenuContent.displayName = DropdownMenuPrimitive.Content.displayName

// The only focus indication a menu item gets. Items wear no ring by design, so
// this tint answers pointer and keyboard alike, and it is written once: a value
// that clears contrast has to reach every shape of item at the same time.
const ITEM_HIGHLIGHT = "data-[highlighted]:bg-accent/15"

// "Label ... value >": the label at the left, whatever it currently reads
// right-aligned against the chevron. A setting row exists in two shapes, an
// item where the options expand in place and a sub-trigger where they fly out,
// so the geometry belongs here rather than restated at both call sites.
const SETTING_ROW = "justify-between text-[0.8125rem]"

const itemVariants: Record<string, string> = {
  default: ITEM_HIGHLIGHT,
  destructive: "text-destructive data-[highlighted]:bg-destructive/10 data-[highlighted]:text-destructive",
  setting: `${SETTING_ROW} ${ITEM_HIGHLIGHT}`,
}

const DropdownMenuItem = React.forwardRef<
  React.ComponentRef<typeof DropdownMenuPrimitive.Item>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.Item> & {
    variant?: "default" | "destructive" | "setting"
  }
>(({ className, variant = "default", ...props }, ref) => (
  <DropdownMenuPrimitive.Item
    ref={ref}
    className={cn(
      "relative flex w-full cursor-default select-none items-center gap-2 rounded-sm px-2.5 py-1.5 text-sm outline-none transition-colors",
      "data-[disabled]:pointer-events-none data-[disabled]:opacity-50",
      itemVariants[variant],
      className
    )}
    {...props}
  />
))
DropdownMenuItem.displayName = DropdownMenuPrimitive.Item.displayName

const DropdownMenuLabel = React.forwardRef<
  React.ComponentRef<typeof DropdownMenuPrimitive.Label>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.Label>
>(({ className, ...props }, ref) => (
  <DropdownMenuPrimitive.Label
    ref={ref}
    className={cn("px-2.5 py-1.5 text-sm font-medium", className)}
    {...props}
  />
))
DropdownMenuLabel.displayName = DropdownMenuPrimitive.Label.displayName

const DropdownMenuSeparator = React.forwardRef<
  React.ComponentRef<typeof DropdownMenuPrimitive.Separator>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.Separator>
>(({ className, ...props }, ref) => (
  <DropdownMenuPrimitive.Separator
    ref={ref}
    className={cn("-mx-1 my-1 h-px bg-border", className)}
    {...props}
  />
))
DropdownMenuSeparator.displayName = DropdownMenuPrimitive.Separator.displayName

const DropdownMenuSub = DropdownMenuPrimitive.Sub

const DropdownMenuSubTrigger = React.forwardRef<
  React.ComponentRef<typeof DropdownMenuPrimitive.SubTrigger>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.SubTrigger> & {
    variant?: "default" | "setting"
  }
>(({ className, children, variant = "default", ...props }, ref) => (
  <DropdownMenuPrimitive.SubTrigger
    ref={ref}
    className={cn(
      "flex cursor-default select-none items-center gap-2 rounded-sm px-2.5 py-1.5 text-sm outline-none data-[state=open]:bg-accent/15",
      ITEM_HIGHLIGHT,
      variant === "setting" && SETTING_ROW,
      className
    )}
    {...props}
  >
    {children}
  </DropdownMenuPrimitive.SubTrigger>
))
DropdownMenuSubTrigger.displayName = DropdownMenuPrimitive.SubTrigger.displayName

// Radix anchors a submenu to its trigger *row*, which the parent panel's
// border + p-1 inset 5px from the panel edge — so the stock offset of 0 opens
// the submenu 5px on top of the panel. Clear that, then add the same 4px gap
// DropdownMenuContent leaves against its own trigger.
const DropdownMenuSubContent = React.forwardRef<
  React.ComponentRef<typeof DropdownMenuPrimitive.SubContent>,
  React.ComponentPropsWithoutRef<typeof DropdownMenuPrimitive.SubContent>
>(({ className, sideOffset = 9, collisionPadding = 8, ...props }, ref) => (
  <DropdownMenuPrimitive.Portal>
    <DropdownMenuPrimitive.SubContent
      ref={ref}
      sideOffset={sideOffset}
      collisionPadding={collisionPadding}
      className={cn(
        "z-[1030] min-w-[8rem] max-h-[var(--radix-dropdown-menu-content-available-height)] overflow-y-auto overflow-x-hidden rounded-md border bg-popover p-1 text-popover-foreground shadow-lg",
        "data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95 data-[side=bottom]:slide-in-from-top-2 data-[side=left]:slide-in-from-right-2 data-[side=right]:slide-in-from-left-2 data-[side=top]:slide-in-from-bottom-2",
        className
      )}
      {...props}
    />
  </DropdownMenuPrimitive.Portal>
))
DropdownMenuSubContent.displayName = DropdownMenuPrimitive.SubContent.displayName

export {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuGroup,
  DropdownMenuSub,
  DropdownMenuSubTrigger,
  DropdownMenuSubContent,
}
