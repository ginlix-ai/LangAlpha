"use client"

import type { Ref } from "react"
import { Search } from "lucide-react"
import {
  Autocomplete,
  Input as AriaInput,
  SearchField as AriaSearchField,
  type SearchFieldProps as AriaSearchFieldProps,
  composeRenderProps,
} from "react-aria-components"

import { cn } from "@/lib/utils"

interface SearchFieldProps extends AriaSearchFieldProps {
  placeholder?: string
  inputRef?: Ref<HTMLInputElement>
}

/**
 * The search heading a popover's list, usually inside an `Autocomplete`. It
 * holds focus the whole time the list is open, so it draws no edge or ring of
 * its own: the rule beneath it is the only line between search and results.
 *
 * It filters a list rather than taking free text, so it turns off what an
 * `Autocomplete` turns off on its input, for a list filtered without one.
 */
function SearchField({ className, placeholder, inputRef, ...props }: SearchFieldProps) {
  return (
    <AriaSearchField
      className={composeRenderProps(className, (className) =>
        cn(
          "flex shrink-0 items-center gap-2 border-b border-(--color-border-muted) px-3",
          className
        )
      )}
      {...props}
    >
      <Search
        aria-hidden="true"
        className="size-[15px] shrink-0 text-(--color-text-tertiary)"
      />
      <AriaInput
        ref={inputRef}
        placeholder={placeholder}
        autoComplete="off"
        autoCorrect="off"
        spellCheck="false"
        enterKeyHint="go"
        className={cn(
          "owns-its-edge h-10 min-w-0 flex-1 border-0 bg-transparent text-sm text-(--color-text-primary) outline-hidden",
          "placeholder:text-(--color-text-tertiary) [&::-webkit-search-cancel-button]:hidden"
        )}
      />
    </AriaSearchField>
  )
}

export { Autocomplete, SearchField }
export type { SearchFieldProps }
