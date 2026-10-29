import * as React from "react"

const TOAST_LIMIT = 3
const TOAST_REMOVE_DELAY = 5000

export interface ToastProps {
  id: string
  open?: boolean
  onOpenChange?: (open: boolean) => void
  title?: React.ReactNode
  description?: React.ReactNode
  action?: React.ReactElement
  variant?: "default" | "destructive"
  /** A small glyph before the title. A destructive toast always takes the
   *  alert glyph instead. */
  icon?: React.ReactNode
  /** Milliseconds on screen. Unset, the Toaster picks by what the toast is:
   *  a confirmation leaves quickly, a failure stays longer, and one with an
   *  action stays until it is answered or closed. */
  duration?: number
  /**
   * Exempt from TOAST_LIMIT. For a notice that carries the only control out of
   * a broken state, being evicted is a silent failure — and it is the oldest
   * toast on screen by then, so it is the one the cap drops first.
   */
  pinned?: boolean
  [key: string]: unknown
}

interface ToastState {
  toasts: ToastProps[]
}

type ToastAction =
  | { type: "ADD_TOAST"; toast: ToastProps }
  | { type: "UPDATE_TOAST"; toast: Partial<ToastProps> & { id: string } }
  | { type: "DISMISS_TOAST"; toastId?: string }
  | { type: "REMOVE_TOAST"; toastId?: string }

let count = 0

function genId(): string {
  count = (count + 1) % Number.MAX_SAFE_INTEGER
  return count.toString()
}

const toastTimeouts = new Map<string, ReturnType<typeof setTimeout>>()

const addToRemoveQueue = (toastId: string) => {
  if (toastTimeouts.has(toastId)) {
    return
  }

  const timeout = setTimeout(() => {
    toastTimeouts.delete(toastId)
    dispatch({
      type: "REMOVE_TOAST",
      toastId: toastId,
    })
  }, TOAST_REMOVE_DELAY)

  toastTimeouts.set(toastId, timeout)
}

export const reducer = (state: ToastState, action: ToastAction): ToastState => {
  switch (action.type) {
    case "ADD_TOAST": {
      const next = [action.toast, ...state.toasts]
      let over = next.length - TOAST_LIMIT
      if (over <= 0) return { ...state, toasts: next }
      // Drop from the oldest end, skipping pinned notices. If every toast is
      // pinned the list is left over the limit rather than dropping one:
      // pinning is a claim that losing the toast loses something, and there is
      // nothing above this to notice it went.
      const kept: ToastProps[] = []
      for (let i = next.length - 1; i >= 0; i--) {
        if (over > 0 && !next[i].pinned) {
          over--
          continue
        }
        kept.unshift(next[i])
      }
      return { ...state, toasts: kept }
    }

    case "UPDATE_TOAST":
      return {
        ...state,
        toasts: state.toasts.map((t) =>
          t.id === action.toast.id ? { ...t, ...action.toast } : t
        ),
      }

    case "DISMISS_TOAST": {
      const { toastId } = action

      if (toastId) {
        addToRemoveQueue(toastId)
      } else {
        state.toasts.forEach((toast) => {
          addToRemoveQueue(toast.id)
        })
      }

      return {
        ...state,
        toasts: state.toasts.map((t) =>
          t.id === toastId || toastId === undefined
            ? {
                ...t,
                open: false,
              }
            : t
        ),
      }
    }
    case "REMOVE_TOAST":
      if (action.toastId === undefined) {
        return {
          ...state,
          toasts: [],
        }
      }
      return {
        ...state,
        toasts: state.toasts.filter((t) => t.id !== action.toastId),
      }
  }
}

const listeners: Array<(state: ToastState) => void> = []

let memoryState: ToastState = { toasts: [] }

function dispatch(action: ToastAction) {
  memoryState = reducer(memoryState, action)
  listeners.forEach((listener) => {
    listener(memoryState)
  })
}

type ToastInput = Omit<ToastProps, "id">

function toast({ ...props }: ToastInput) {
  const id = genId()

  const update = (props: Partial<ToastProps>) =>
    dispatch({
      type: "UPDATE_TOAST",
      toast: { ...props, id },
    })
  const dismiss = () => dispatch({ type: "DISMISS_TOAST", toastId: id })

  dispatch({
    type: "ADD_TOAST",
    toast: {
      ...props,
      id,
      open: true,
      onOpenChange: (open) => {
        if (!open) dismiss()
      },
    },
  })

  return {
    id: id,
    dismiss,
    update,
  }
}

function useToast() {
  const [state, setState] = React.useState<ToastState>(memoryState)

  React.useEffect(() => {
    listeners.push(setState)
    return () => {
      const index = listeners.indexOf(setState)
      if (index > -1) {
        listeners.splice(index, 1)
      }
    }
  }, [state])

  return {
    ...state,
    toast,
    dismiss: (toastId?: string) => dispatch({ type: "DISMISS_TOAST", toastId }),
  }
}

// Insets of the elements currently floating at the bottom of the page, by
// hook token. Several can be up at once; the toasts clear the tallest.
const insets = new Map<symbol, number>()

function applyToastLift() {
  const root = document.documentElement
  const lift = Math.max(0, ...insets.values())
  if (lift > 0) root.style.setProperty("--toast-lift", `${lift}px`)
  else root.style.removeProperty("--toast-lift")
}

/**
 * Keeps toasts clear of an element floating at the bottom of the page (a bulk
 * action bar), so a result landing in the corner does not sit on its buttons.
 * The inset is the distance from the window's bottom edge to the element's
 * top, read where the element actually is: stuck to the bottom of a column,
 * resting after the last row of a short list, or fixed above a phone's tab
 * bar. It is re-read as the element changes shape, moves (a list scrolls a
 * resting element) or the window resizes. An element resting high on the page
 * is clear of the corner, so the toasts keep it.
 */
function useToastInset(ref: React.RefObject<HTMLElement | null>) {
  React.useLayoutEffect(() => {
    const el = ref.current
    if (!el) return
    const token = Symbol("toast-inset")
    let last = 0
    let frame = 0
    const measure = () => {
      frame = 0
      const fromBottom = Math.round(window.innerHeight - el.getBoundingClientRect().top)
      const inset = fromBottom > 0 && fromBottom <= window.innerHeight / 2 ? fromBottom : 0
      // Written only on a change: a custom property on the root restyles the
      // page, and a stuck element scrolling with its list never moves.
      if (inset === last) return
      last = inset
      if (inset) insets.set(token, inset)
      else insets.delete(token)
      applyToastLift()
    }
    const schedule = () => {
      if (!frame) frame = requestAnimationFrame(measure)
    }
    measure()
    const observer = typeof ResizeObserver === "undefined" ? null : new ResizeObserver(schedule)
    observer?.observe(el)
    // An entrance animation moves the element, which no resize reports.
    el.addEventListener("animationend", schedule)
    window.addEventListener("resize", schedule)
    // Capture: a list scrolls in its own container, whose scroll does not bubble.
    window.addEventListener("scroll", schedule, { capture: true, passive: true })
    return () => {
      if (frame) cancelAnimationFrame(frame)
      observer?.disconnect()
      el.removeEventListener("animationend", schedule)
      window.removeEventListener("resize", schedule)
      window.removeEventListener("scroll", schedule, { capture: true })
      insets.delete(token)
      applyToastLift()
    }
  }, [ref])
}

export { useToast, toast, useToastInset }
