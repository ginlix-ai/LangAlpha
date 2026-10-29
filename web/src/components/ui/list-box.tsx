"use client"

import { Check } from "lucide-react"
import {
  Collection as AriaCollection,
  Header as AriaHeader,
  ListBox as AriaListBox,
  ListBoxItem as AriaListBoxItem,
  type ListBoxItemProps as AriaListBoxItemProps,
  type ListBoxProps as AriaListBoxProps,
  ListBoxSection as AriaListBoxSection,
  composeRenderProps,
} from "react-aria-components"

import { cn } from "@/lib/utils"

const ListBoxSection = AriaListBoxSection

const ListBoxCollection = AriaCollection

function ListBox<T extends object>({
  className,
  ...props
}: AriaListBoxProps<T>) {
  return (
    <AriaListBox
      className={composeRenderProps(className, (className) =>
        cn(
          className,
          "group overflow-auto rounded-md border bg-popover p-1 text-popover-foreground shadow-md outline-hidden",
          /* Empty */
          "data-empty:p-6 data-empty:text-center data-empty:text-sm"
        )
      )}
      {...props}
    />
  )
}

const ListBoxItem = <T extends object>({
  className,
  children,
  ...props
}: AriaListBoxItemProps<T>) => {
  return (
    <AriaListBoxItem
      textValue={
        props.textValue || (typeof children === "string" ? children : undefined)
      }
      className={composeRenderProps(className, (className) =>
        cn(
          "relative flex w-full cursor-default select-none items-center rounded-sm px-2 py-1.5 text-sm",
          /* Disabled */
          "data-disabled:pointer-events-none data-disabled:opacity-50",
          /* Focused and hovered: the tint a menu item takes (dropdown-menu's
             ITEM_HIGHLIGHT), so every floating list marks its row alike */
          "data-focused:bg-accent/15 data-hovered:bg-accent/15",
          /* Keyboard focus: the inward ring tokens.css draws on a focused
             option. Colored here as well because a list driven from a search
             field moves a virtual focus, which :focus-visible never sees, and
             the tint alone would read the same as the row under the mouse */
          "data-focus-visible:outline-current",
          /* Selection */
          "data-selection-mode:pl-8",
          className
        )
      )}
      {...props}
    >
      {composeRenderProps(children, (children, renderProps) => (
        <>
          {renderProps.isSelected && (
            <span className="absolute left-2 flex size-4 items-center justify-center">
              <Check className="size-4" />
            </span>
          )}
          {children}
        </>
      ))}
    </AriaListBoxItem>
  )
}

function ListBoxHeader({
  className,
  ...props
}: React.ComponentProps<typeof AriaHeader>) {
  return (
    <AriaHeader
      className={cn("py-1.5 pl-8 pr-2 text-sm font-semibold", className)}
      {...props}
    />
  )
}

export {
  ListBox,
  ListBoxItem,
  ListBoxHeader,
  ListBoxSection,
  ListBoxCollection,
}
