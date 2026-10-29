import * as React from "react"
import * as ToastPrimitives from "@radix-ui/react-toast"
import { X } from "lucide-react"

import { cn } from "@/lib/utils"
import { aboveDialogs } from "@/hooks/useDialogA11y"

const ToastProvider = ToastPrimitives.Provider

function ToastViewport({
  className,
  ...props
}: React.ComponentProps<typeof ToastPrimitives.Viewport>) {
  // Above every modal layer, so a toast raised from inside a dialog is not
  // dimmed under its backdrop. The empty viewport still spans its padding, so it
  // passes pointer events through and only the toasts themselves take them.
  // Being above them is also why an open dialog lets a toast keep focus.
  //
  // `--toast-lift` is written by `useToastInset` for any element floating at
  // the bottom of the page, so a toast stacks above its buttons rather than
  // over them. Unset everywhere else, which leaves the edge at 0.
  return (
    <ToastPrimitives.Viewport
      {...aboveDialogs}
      className={cn(
        "pointer-events-none fixed top-0 z-1050 flex max-h-screen w-full flex-col-reverse gap-2 p-4 sm:bottom-[var(--toast-lift,0px)] sm:right-0 sm:top-auto sm:flex-col sm:transition-[bottom] motion-reduce:transition-none md:max-w-[420px]",
        className
      )}
      {...props}
    />
  )
}

// One quiet surface for every tone (DESIGN.md): an elevated card, a uniform
// neutral border, the card shadow. A failure is said by its leading glyph, not
// by repainting the card red, so the toasts read as one family and a red slab
// no longer shouts over the work. Positioning goes through `transform`
// because the enter and exit keyframes animate `transform`, which would stack
// on the separate `translate` property that translate-x-* sets instead of
// replacing it.
const toastClassName =
  "group pointer-events-auto relative flex w-full items-center gap-3 overflow-hidden rounded-lg border border-(--color-border-elevated) bg-(--color-bg-elevated) py-3 pl-3.5 pr-2.5 text-(--color-text-primary) shadow-(--shadow-card) transition-all data-[swipe=cancel]:[transform:translateX(0)] data-[swipe=end]:[transform:translateX(var(--radix-toast-swipe-end-x))] data-[swipe=move]:[transform:translateX(var(--radix-toast-swipe-move-x))] data-[swipe=move]:transition-none toast-slide"

function Toast({
  className,
  ...props
}: React.ComponentProps<typeof ToastPrimitives.Root>) {
  return (
    <ToastPrimitives.Root
      className={cn(toastClassName, className)}
      {...props}
    />
  )
}

// The toast's one way forward, so it takes the primary pair; the close button
// beside it is the way out.
function ToastAction({
  className,
  ...props
}: React.ComponentProps<typeof ToastPrimitives.Action>) {
  return (
    <ToastPrimitives.Action
      className={cn(
        "press inline-flex h-7 shrink-0 items-center justify-center rounded-md bg-(--color-btn-primary-bg) px-2.5 text-[13px] font-medium text-(--color-btn-primary-text) hover:opacity-90 focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-(--color-bg-elevated) disabled:pointer-events-none disabled:opacity-50",
        className
      )}
      {...props}
    />
  )
}

// Always shown, in the row beside the action, rather than revealed on hover
// in a corner: a hover reveal hid it from touch screens and held an empty
// gutter on every toast for a button that was usually invisible.
function ToastClose({
  className,
  ...props
}: React.ComponentProps<typeof ToastPrimitives.Close>) {
  return (
    <ToastPrimitives.Close
      className={cn(
        "shrink-0 rounded-md p-1 text-(--color-text-tertiary) transition hover:bg-(--color-bg-hover) hover:text-(--color-text-primary) focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-ring",
        className
      )}
      toast-close=""
      {...props}
    >
      <X className="h-3.5 w-3.5" />
    </ToastPrimitives.Close>
  )
}

function ToastTitle({
  className,
  ...props
}: React.ComponentProps<typeof ToastPrimitives.Title>) {
  return (
    <ToastPrimitives.Title
      className={cn("text-sm font-medium leading-5", className)}
      {...props}
    />
  )
}

function ToastDescription({
  className,
  ...props
}: React.ComponentProps<typeof ToastPrimitives.Description>) {
  return (
    <ToastPrimitives.Description
      className={cn("text-[13px] leading-[18px] text-(--color-text-secondary)", className)}
      {...props}
    />
  )
}

export {
  ToastProvider,
  ToastViewport,
  Toast,
  ToastTitle,
  ToastDescription,
  ToastClose,
  ToastAction,
}
