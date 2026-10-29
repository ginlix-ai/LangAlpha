"use client"

import { ChevronDown } from "lucide-react"
import {
  Button as AriaButton,
  type ButtonProps as AriaButtonProps,
  ListBox as AriaListBox,
  type ListBoxProps as AriaListBoxProps,
  type PopoverProps as AriaPopoverProps,
  Select as AriaSelect,
  type SelectProps as AriaSelectProps,
  SelectValue as AriaSelectValue,
  type SelectValueProps as AriaSelectValueProps,
  type ValidationResult as AriaValidationResult,
  composeRenderProps,
  Text,
} from "react-aria-components"

import { cn } from "@/lib/utils"

import { FieldError, Label } from "@/components/ui/field"
import {
  ListBoxCollection,
  ListBoxHeader,
  ListBoxItem,
  ListBoxSection,
} from "@/components/ui/list-box"
import { Popover } from "@/components/ui/aria-popover"

const Select = AriaSelect

const SelectItem = ListBoxItem

const SelectHeader = ListBoxHeader

const SelectSection = ListBoxSection

const SelectCollection = ListBoxCollection

const SelectValue = <T extends object>({
  className,
  ...props
}: AriaSelectValueProps<T>) => (
  <AriaSelectValue
    className={composeRenderProps(className, (className) =>
      cn(
        "line-clamp-1 data-placeholder:text-muted-foreground",
        /* Description */
        "*:[[slot=description]]:hidden",
        className
      )
    )}
    {...props}
  />
)

const SelectTrigger = ({ className, children, ...props }: AriaButtonProps) => (
  <AriaButton
    className={composeRenderProps(className, (className) =>
      cn(
        /* The field fill and hairline every text input carries */
        "flex h-9 w-full items-center justify-between gap-2 rounded-md border border-(--color-border-muted) bg-input px-3 py-2 text-left text-sm ring-offset-background",
        /* Disabled */
        "data-disabled:cursor-not-allowed data-disabled:opacity-50",
        /* Focused */
        "data-focus-visible:outline-hidden data-focus-visible:ring-2 data-focus-visible:ring-ring data-focus-visible:ring-offset-2",
        /* Resets */
        "focus-visible:outline-hidden",
        className
      )
    )}
    {...props}
  >
    {composeRenderProps(children, (children) => (
      <>
        {children}
        <ChevronDown aria-hidden="true" className="size-4 shrink-0 opacity-50" />
      </>
    ))}
  </AriaButton>
)

// Never narrower than the trigger, and wider when an option needs it, so a
// compact trigger does not fold its options onto two lines.
const SelectPopover = ({ className, ...props }: AriaPopoverProps) => (
  <Popover
    className={composeRenderProps(className, (className) =>
      cn("w-max min-w-(--trigger-width) max-w-[calc(100vw-32px)]", className)
    )}
    {...props}
  />
)

const SelectListBox = <T extends object>({
  className,
  ...props
}: AriaListBoxProps<T>) => (
  <AriaListBox
    className={composeRenderProps(className, (className) =>
      cn(
        "max-h-[inherit] overflow-auto p-1 outline-hidden [clip-path:inset(0_0_0_0_round_calc(var(--radius)-2px))]",
        className
      )
    )}
    {...props}
  />
)

interface JollySelectProps<T extends object>
  extends Omit<AriaSelectProps<T>, "children"> {
  label?: string
  description?: string
  errorMessage?: string | ((validation: AriaValidationResult) => string)
  items?: Iterable<T>
  children: React.ReactNode | ((item: T) => React.ReactNode)
}

function JollySelect<T extends object>({
  label,
  description,
  errorMessage,
  children,
  className,
  items,
  ...props
}: JollySelectProps<T>) {
  return (
    <Select
      className={composeRenderProps(className, (className) =>
        cn("group flex flex-col gap-2", className)
      )}
      {...props}
    >
      <Label>{label}</Label>
      <SelectTrigger>
        <SelectValue />
      </SelectTrigger>
      {description && (
        <Text className="text-sm text-muted-foreground" slot="description">
          {description}
        </Text>
      )}
      <FieldError>{errorMessage}</FieldError>
      <SelectPopover>
        <SelectListBox items={items}>{children}</SelectListBox>
      </SelectPopover>
    </Select>
  )
}

export {
  Select,
  SelectValue,
  SelectTrigger,
  SelectItem,
  SelectPopover,
  SelectHeader,
  SelectListBox,
  SelectSection,
  SelectCollection,
  JollySelect,
}
export type { JollySelectProps }
