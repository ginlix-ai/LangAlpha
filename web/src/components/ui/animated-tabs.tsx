"use client";

import { useEffect, useRef } from "react";
import { motion } from "@/lib/framer";
import { SPRING_INDICATOR } from "@/lib/motion";
import { useStyleLength } from "@/lib/cardStyle";

interface Tab {
  id: string;
  label: string;
}

interface AnimatedTabsProps {
  tabs: Tab[];
  value: string;
  onChange: (tabId: string) => void;
  layoutId?: string;
}

export function AnimatedTabs({
  tabs,
  value,
  onChange,
  layoutId = "bubble",
}: AnimatedTabsProps) {
  // Only animate the indicator when the selected tab actually changed,
  // not when a parent re-render shifts this component's Y position.
  const prevValue = useRef(value);
  const valueChanged = prevValue.current !== value;
  useEffect(() => { prevValue.current = value; });
  // A number, not a var(): framer corrects a px radius while the indicator
  // slides and leaves a string it cannot parse uncorrected. Soft draws
  // it as a pill, as its other choices are; Terminal hides it
  // (styles/cardStyle.css).
  const indicatorRadius = useStyleLength("--cs-radius-pill", 6);

  return (
    <div data-slot="tabs" className="flex space-x-1">
      {tabs.map((tab) => {
        const isActive = value === tab.id;
        return (
          <button
            key={tab.id}
            type="button"
            data-slot="tab"
            data-active={isActive || undefined}
            onClick={() => onChange(tab.id)}
            className={`
              relative rounded-md px-3 py-1.5 text-sm font-medium
              outline-hidden transition-colors cursor-pointer
              focus-visible:ring-2 focus-visible:ring-ring
              ${isActive ? "" : "hover:opacity-60"}
            `}
            style={{
              color: isActive
                ? 'var(--color-text-primary)'
                : 'var(--color-text-secondary)',
              WebkitTapHighlightColor: "transparent",
            }}
          >
            {isActive && (
              <motion.span
                layoutId={layoutId}
                data-slot="tab-indicator"
                className="absolute inset-0"
                style={{
                  borderRadius: indicatorRadius,
                  backgroundColor: 'var(--color-shell-fill, var(--color-bg-elevated))',
                  // The elevated fill alone is nearly invisible on a white page,
                  // so a hairline edge keeps the pill legible on both themes.
                  // The open styles leave it unfilled and edged in ink.
                  boxShadow: 'inset 0 0 0 1px var(--color-shell-ink, var(--color-border-default))',
                }}
                transition={valueChanged ? SPRING_INDICATOR : { duration: 0 }}
              />
            )}
            <span className="relative z-10">{tab.label}</span>
          </button>
        );
      })}
    </div>
  );
}
