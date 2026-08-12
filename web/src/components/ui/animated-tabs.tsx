"use client";

import { useEffect, useRef } from "react";
import { motion } from "framer-motion";

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

const springTransition = { type: "spring", bounce: 0.2, duration: 0.6 } as const;

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

  return (
    <div className="flex space-x-1">
      {tabs.map((tab) => {
        const isActive = value === tab.id;
        return (
          <button
            key={tab.id}
            type="button"
            onClick={() => onChange(tab.id)}
            className={`
              relative rounded-md px-3 py-1.5 text-sm font-medium
              outline-none transition-colors cursor-pointer
              focus-visible:ring-2 focus-visible:ring-[var(--color-accent-overlay)]
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
                className="absolute inset-0"
                style={{
                  borderRadius: '6px',
                  backgroundColor: 'var(--color-bg-elevated)',
                  // The elevated fill alone is nearly invisible on a white page,
                  // so a hairline edge keeps the pill legible on both themes.
                  boxShadow: 'inset 0 0 0 1px var(--color-border-default)',
                }}
                transition={valueChanged ? springTransition : { duration: 0 }}
              />
            )}
            <span className="relative z-10">{tab.label}</span>
          </button>
        );
      })}
    </div>
  );
}
