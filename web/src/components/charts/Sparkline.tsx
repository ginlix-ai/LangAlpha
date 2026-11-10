import { useId, type CSSProperties, type ReactNode } from 'react';
import { sparkGeometry } from '@/lib/sparkline';

interface SparklineProps {
  values: readonly number[];
  color: string;
  height: number;
  /** The drawing's own width. A stretched line fills its box whatever this is. */
  width?: number;
  /** Fills the element's width, the stroke keeping its weight; otherwise drawn at `width`. */
  stretch?: boolean;
  /** Where each value sits across the width, 0–1; evenly spread when omitted. */
  at?: readonly number[];
  /** A dashed reference line (a previous close), inside the vertical range. */
  baseline?: number | null;
  /** A soft gradient under the line. */
  fill?: boolean;
  /** A dot on the latest value. */
  dot?: boolean;
  strokeWidth?: number;
  className?: string;
  style?: CSSProperties;
  /** Drawn behind the line, in the drawing's coordinates (`width` × `height`). */
  children?: ReactNode;
}

/** A price line in one SVG, with no axes: the one sparkline every small chart draws. */
export function Sparkline({
  values,
  color,
  height,
  width = 600,
  stretch = true,
  at,
  baseline = null,
  fill = true,
  dot = false,
  strokeWidth = 1.25,
  className,
  style,
  children,
}: SparklineProps) {
  const gradientId = useId();
  const shape = sparkGeometry(values, { width, height, at, baseline });
  if (!shape) return null;
  return (
    <svg
      className={className}
      width={stretch ? undefined : width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      preserveAspectRatio={stretch ? 'none' : undefined}
      style={stretch ? { width: '100%', ...style } : style}
      aria-hidden="true"
    >
      {fill && (
        <defs>
          <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity={0.14} />
            <stop offset="100%" stopColor={color} stopOpacity={0} />
          </linearGradient>
        </defs>
      )}
      {children}
      {fill && <polygon points={shape.area} fill={`url(#${gradientId})`} />}
      {shape.baselineY != null && (
        <line
          x1={0}
          x2={width}
          y1={shape.baselineY}
          y2={shape.baselineY}
          stroke="var(--color-text-tertiary)"
          strokeWidth={1}
          strokeDasharray="2 3"
          vectorEffect="non-scaling-stroke"
          opacity={0.6}
        />
      )}
      <polyline
        points={shape.line}
        fill="none"
        stroke={color}
        strokeWidth={strokeWidth}
        strokeLinejoin="round"
        strokeLinecap="round"
        vectorEffect="non-scaling-stroke"
      />
      {dot && <circle cx={shape.last.x} cy={shape.last.y} r={2} fill={color} />}
    </svg>
  );
}
