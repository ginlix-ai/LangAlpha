import * as React from "react"
import { ChevronDown } from "lucide-react"
import { cn } from "../../lib/utils"

export interface SelectProps extends React.ComponentProps<"select"> {
  /** className is applied to the outer wrapper for layout purposes; ref and style reach the <select> */
}

function Select({ className, children, style, ref, ...props }: SelectProps) {
  return (
    <div className={cn("relative", className)}>
      <select
        ref={ref}
        className="h-9 w-full appearance-none rounded-md py-0 pl-3 pr-9 text-sm disabled:cursor-not-allowed disabled:opacity-50"
        style={{
          backgroundColor: "var(--color-bg-card)",
          border: "1px solid var(--color-border-muted)",
          color: "var(--color-text-primary)",
          ...style,
        }}
        {...props}
      >
        {children}
      </select>
      <ChevronDown
        className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4"
        style={{ color: "var(--color-text-tertiary)" }}
      />
    </div>
  )
}

export { Select }
