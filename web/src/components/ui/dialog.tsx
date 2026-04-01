import * as React from "react"
import * as DialogPrimitive from "@radix-ui/react-dialog"
import { X } from "lucide-react"

import { cn } from "@/lib/utils"
import { useIsMobile } from "@/hooks/useIsMobile"
import { useSwipeToDismiss } from "@/hooks/useSwipeToDismiss"

const Dialog = DialogPrimitive.Root

const DialogTrigger = DialogPrimitive.Trigger

const DialogPortal = DialogPrimitive.Portal

const DialogClose = DialogPrimitive.Close

const DialogOverlay = React.forwardRef<
  React.ComponentRef<typeof DialogPrimitive.Overlay>,
  React.ComponentPropsWithoutRef<typeof DialogPrimitive.Overlay>
>(({ className, ...props }, ref) => (
  <DialogPrimitive.Overlay
    ref={ref}
    className={cn(
      "fixed inset-0 z-[1030] bg-black/80 data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0",
      className
    )}
    {...props} />
))
DialogOverlay.displayName = DialogPrimitive.Overlay.displayName

// Mobile swipe variant: flex column container, no overflow (inner scroll child handles it)
const DIALOG_MOBILE_SHEET_CLASSES =
  "fixed left-0 bottom-0 z-[1030] flex flex-col w-full max-w-lg border bg-background shadow-lg duration-200 rounded-t-3xl max-h-[90dvh] data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:slide-out-to-bottom data-[state=open]:slide-in-from-bottom";

// Desktop / centered: single-element grid with native overflow scroll
const DIALOG_CENTERED_CLASSES =
  "fixed left-[50%] top-[50%] z-[1030] grid w-full max-w-lg gap-4 border bg-background p-6 shadow-lg duration-200 translate-x-[-50%] translate-y-[-50%] rounded-lg max-h-[85vh] overflow-y-auto data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 data-[state=closed]:zoom-out-95 data-[state=open]:zoom-in-95 data-[state=closed]:slide-out-to-left-1/2 data-[state=closed]:slide-out-to-top-[48%] data-[state=open]:slide-in-from-left-1/2 data-[state=open]:slide-in-from-top-[48%]";

const DialogContent = React.forwardRef<
  React.ComponentRef<typeof DialogPrimitive.Content>,
  React.ComponentPropsWithoutRef<typeof DialogPrimitive.Content> & {
    /** 'default' = bottom-sheet on mobile, centered on desktop. 'centered' = always centered. */
    variant?: 'default' | 'centered';
  }
>(({ className, children, variant = 'default', ...props }, ref) => {
  const isMobile = useIsMobile();
  const swipeEnabled = isMobile && variant === 'default';

  // Hidden close button ref — clicking it triggers Radix's onOpenChange(false)
  const closeRef = React.useRef<HTMLButtonElement>(null);

  // State-backed container node so the dragY subscriber re-attaches on portal remount
  const [containerNode, setContainerNode] = React.useState<HTMLDivElement | null>(null);

  const { contentRef, handleRef, dragY } = useSwipeToDismiss({
    onDismiss: () => closeRef.current?.click(),
    enabled: swipeEnabled,
  });

  // Merge forwarded ref + container state setter
  const containerRefCb = React.useCallback((node: HTMLDivElement | null) => {
    setContainerNode(node);
    if (typeof ref === 'function') ref(node);
    else if (ref) (ref as React.MutableRefObject<HTMLDivElement | null>).current = node;
  }, [ref]);

  // Apply dragY as CSS `translate` on the outer container (independent of CSS `transform` animations)
  React.useEffect(() => {
    if (!swipeEnabled || !containerNode) return;
    return dragY.on('change', (v) => {
      containerNode.style.translate = `0 ${v}px`;
    });
  }, [swipeEnabled, dragY, containerNode]);

  // Mobile bottom-sheet with swipe: 2-layer structure
  // Outer: flex column container for positioning + translate transform
  // Inner: flex-1 min-h-0 scroll child for content + touch handling
  if (swipeEnabled) {
    return (
      <DialogPortal>
        <DialogOverlay />
        <DialogPrimitive.Content
          aria-describedby={undefined}
          ref={containerRefCb}
          className={cn(DIALOG_MOBILE_SHEET_CLASSES, className)}
          {...props}
        >
          {/* Drag handle */}
          <div
            ref={handleRef}
            className="flex justify-center pt-3 pb-1 shrink-0 cursor-grab active:cursor-grabbing"
            style={{ touchAction: 'none' }}
          >
            <div
              className="w-10 h-1 rounded-full"
              style={{ backgroundColor: 'var(--color-border-default)' }}
            />
          </div>
          {/* Scrollable content — mirrors MobileBottomSheet inner div */}
          <div
            ref={contentRef}
            className="flex-1 min-h-0 overflow-y-auto overflow-x-hidden grid gap-4 px-6 pb-[max(1.5rem,env(safe-area-inset-bottom))] [&>*]:min-w-0"
            style={{ overscrollBehaviorY: 'contain' }}
          >
            {children}
          </div>
          {/* Hidden close button for swipe dismiss */}
          <DialogPrimitive.Close ref={closeRef} className="hidden" aria-hidden />
        </DialogPrimitive.Content>
      </DialogPortal>
    );
  }

  // Desktop bottom-sheet or centered: single-element, native scroll
  return (
    <DialogPortal>
      <DialogOverlay />
      <DialogPrimitive.Content
        aria-describedby={undefined}
        ref={ref}
        className={cn(
          DIALOG_CENTERED_CLASSES,
          className
        )}
        {...props}>
        {children}
        <DialogPrimitive.Close
          className="absolute right-4 top-4 rounded-sm opacity-70 ring-offset-background transition-opacity hover:opacity-100 focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 disabled:pointer-events-none data-[state=open]:bg-accent data-[state=open]:text-muted-foreground">
          <X className="h-4 w-4" />
          <span className="sr-only">Close</span>
        </DialogPrimitive.Close>
      </DialogPrimitive.Content>
    </DialogPortal>
  );
})
DialogContent.displayName = DialogPrimitive.Content.displayName

const DialogHeader = ({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) => (
  <div
    className={cn("flex flex-col space-y-1.5 text-center sm:text-left", className)}
    {...props} />
)
DialogHeader.displayName = "DialogHeader"

const DialogFooter = ({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) => (
  <div
    className={cn("flex flex-col-reverse sm:flex-row sm:justify-end sm:space-x-2", className)}
    {...props} />
)
DialogFooter.displayName = "DialogFooter"

const DialogTitle = React.forwardRef<
  React.ComponentRef<typeof DialogPrimitive.Title>,
  React.ComponentPropsWithoutRef<typeof DialogPrimitive.Title>
>(({ className, ...props }, ref) => (
  <DialogPrimitive.Title
    ref={ref}
    className={cn("text-lg font-semibold leading-none tracking-tight", className)}
    {...props} />
))
DialogTitle.displayName = DialogPrimitive.Title.displayName

const DialogDescription = React.forwardRef<
  React.ComponentRef<typeof DialogPrimitive.Description>,
  React.ComponentPropsWithoutRef<typeof DialogPrimitive.Description>
>(({ className, ...props }, ref) => (
  <DialogPrimitive.Description
    ref={ref}
    className={cn("text-sm text-muted-foreground", className)}
    {...props} />
))
DialogDescription.displayName = DialogPrimitive.Description.displayName

export {
  Dialog,
  DialogPortal,
  DialogOverlay,
  DialogClose,
  DialogTrigger,
  DialogContent,
  DialogHeader,
  DialogFooter,
  DialogTitle,
  DialogDescription,
}
