import { motion } from "framer-motion";
import { useTheme } from "@/contexts/ThemeContext";

export interface AgentPlanItem {
  id: string;
  label: string;
  status: "pending" | "in_progress" | "completed" | "stale";
}

// --- Theme-aware color palette (Warm Tinted / F2) ---

interface StatusColors {
  node: string;
  conn: string;
  status: string;
  glow?: string;
  size?: number;
  border?: string;
}

interface ThemePalette {
  completed: StatusColors;
  in_progress: StatusColors;
  stale: StatusColors;
  pending: StatusColors;
  indicator: string;
}

const darkPalette: ThemePalette = {
  completed: { node: "#7BAE8E", conn: "rgba(123,174,142,0.6)", status: "#7BAE8E" },
  in_progress: { node: "var(--color-accent-primary)", conn: "hsl(var(--primary) / 0.6)", status: "var(--color-accent-primary)", glow: "hsl(var(--primary) / 0.25)", size: 9 },
  stale: { node: "#B8A07A", conn: "rgba(184,160,122,0.5)", status: "#B8A07A" },
  pending: { node: "transparent", border: "var(--color-border-muted)", conn: "var(--color-border-muted)", status: "var(--color-text-quaternary)" },
  indicator: "var(--color-accent-primary)",
};

const lightPalette: ThemePalette = {
  completed: { node: "#4E8A64", conn: "rgba(78,138,100,0.5)", status: "#4E8A64" },
  in_progress: { node: "var(--color-accent-primary)", conn: "hsl(var(--primary) / 0.55)", status: "var(--color-accent-primary)", glow: "hsl(var(--primary) / 0.18)", size: 9 },
  stale: { node: "#9A7F58", conn: "rgba(154,127,88,0.45)", status: "#9A7F58" },
  pending: { node: "transparent", border: "var(--color-border-muted)", conn: "var(--color-border-muted)", status: "var(--color-text-quaternary)" },
  indicator: "var(--color-accent-primary)",
};

function usePalette(): ThemePalette {
  const { theme } = useTheme();
  return theme === "light" ? lightPalette : darkPalette;
}

function getColors(status: string, palette: ThemePalette): StatusColors {
  const map: Record<string, StatusColors> = {
    completed: palette.completed,
    in_progress: palette.in_progress,
    stale: palette.stale,
    pending: palette.pending,
  };
  return map[status] || palette.pending;
}

function connectorColor(from: string, to: string, palette: ThemePalette): string {
  const fromDone = from === "completed" || from === "stale";
  if (!fromDone) return palette.pending.conn;
  const toDone = to === "completed" || to === "stale";
  if (toDone) return getColors(from, palette).conn;
  if (to === "in_progress") return palette.in_progress.conn;
  return palette.pending.conn;
}

function textColor(status: string): string {
  switch (status) {
    case "completed":
    case "stale":
      return "var(--color-text-tertiary)";
    case "in_progress":
      return "var(--color-text-primary)";
    default:
      return "var(--color-text-secondary)";
  }
}

// --- Stepper Track ---

interface StepperTrackProps {
  items: AgentPlanItem[];
}

const MAX_VISIBLE_NODES = 12;

export function StepperTrack({ items }: StepperTrackProps) {
  const palette = usePalette();
  if (!items.length) return null;

  const overflow = items.length > MAX_VISIBLE_NODES;
  const visible = overflow ? items.slice(0, MAX_VISIBLE_NODES) : items;

  return (
    <div className="flex items-center flex-1" style={{ height: 20, gap: 0 }}>
      {visible.map((item, i) => {
        const colors = getColors(item.status, palette);
        const size = colors.size || 7;
        const nodeStyle: React.CSSProperties = {
          width: size,
          height: size,
          borderRadius: "50%",
          background: colors.node,
          flexShrink: 0,
          position: "relative",
          zIndex: 1,
        };
        if (colors.border) nodeStyle.border = `1.5px solid ${colors.border}`;
        if (colors.glow) nodeStyle.boxShadow = `0 0 0 3px ${colors.glow}`;

        const nextItem = visible[i + 1];
        const isLastVisible = i === visible.length - 1;

        return (
          <div key={item.id} className="contents">
            <div style={nodeStyle} />
            {(nextItem || (isLastVisible && overflow)) && (
              <div
                className="flex-1 relative overflow-hidden"
                style={{
                  height: 3,
                  borderRadius: 1.5,
                  background: nextItem
                    ? connectorColor(item.status, nextItem.status, palette)
                    : palette.pending.conn,
                }}
              >
                {nextItem &&
                  (item.status === "completed" || item.status === "stale") &&
                  nextItem.status === "in_progress" && (
                    <motion.div
                      className="absolute inset-0"
                      style={{
                        width: "30%",
                        background: "rgba(255,255,255,0.25)",
                      }}
                      animate={{ left: ["-30%", "100%"] }}
                      transition={{
                        duration: 1.2,
                        repeat: Infinity,
                        ease: "easeInOut",
                      }}
                    />
                  )}
              </div>
            )}
          </div>
        );
      })}
      {overflow && (
        <span
          className="flex-shrink-0 text-xs tabular-nums"
          style={{ color: "var(--color-text-quaternary)", marginLeft: 2 }}
        >
          +{items.length - MAX_VISIBLE_NODES}
        </span>
      )}
    </div>
  );
}

// --- Expanded List (Ticker-tape style) ---

export const EASING: [number, number, number, number] = [0.22, 1, 0.36, 1];

const listVariants = {
  hidden: {},
  visible: {
    transition: { staggerChildren: 0.04 },
  },
};

const itemVariants = {
  hidden: { opacity: 0, x: -6 },
  visible: {
    opacity: 1,
    x: 0,
    transition: { duration: 0.3, ease: EASING },
  },
};

function statusLabel(status: string): string {
  switch (status) {
    case "in_progress":
      return "Active";
    case "completed":
      return "Done";
    case "stale":
      return "Stale";
    default:
      return "Queue";
  }
}

export default function StepperList({ items }: { items: AgentPlanItem[] }) {
  const palette = usePalette();
  if (!items.length) return null;

  return (
    <motion.div
      className="flex flex-col"
      style={{ gap: 2 }}
      variants={listVariants}
      initial="hidden"
      animate="visible"
    >
      {items.map((item) => {
        const isDone = item.status === "completed" || item.status === "stale";
        const colors = getColors(item.status, palette);

        return (
          <motion.div
            key={item.id}
            className="flex items-center"
            style={{ gap: 8, padding: "4px 0", fontSize: 13 }}
            variants={itemVariants}
          >
            <span
              className="flex-shrink-0 text-right"
              style={{
                fontSize: 10,
                fontWeight: 600,
                textTransform: "uppercase",
                letterSpacing: "0.04em",
                width: 52,
                color: colors.status,
              }}
            >
              {statusLabel(item.status)}
            </span>

            <div
              className="flex-shrink-0"
              style={{
                width: 1,
                height: 12,
                background: "var(--color-border-muted)",
              }}
            />

            <span
              className="truncate min-w-0"
              style={{
                color: textColor(item.status),
                textDecoration: isDone ? "line-through" : "none",
              }}
            >
              {item.label}
            </span>
          </motion.div>
        );
      })}
    </motion.div>
  );
}
