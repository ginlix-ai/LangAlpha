import { SkeletonBar } from '@/components/ui/skeleton';

/** Name lengths that read as a list of names rather than one block repeated. */
const WIDTHS = [58, 42, 66, 50, 36, 62, 46, 54];

/**
 * A list that is still being read, drawn as rows of its own shape: an icon, a
 * name, and beside a file the line under it. It shows only once the read
 * outlasts an entrance (Drive.css), so a folder that lands at once goes from
 * the last one straight to itself.
 */
export function RowsSkeleton({ rows = 6, compact = false }: { rows?: number; compact?: boolean }) {
  return (
    <div className="drive-rows-skeleton" aria-busy="true">
      {WIDTHS.slice(0, rows).map((w, i) => (
        <div key={i} className="drive-skeleton-row">
          <SkeletonBar className="drive-skeleton-icon" />
          <span className="drive-skeleton-text">
            <span className="drive-skeleton-line"><SkeletonBar style={{ width: `${w}%` }} /></span>
            {compact && <span className="drive-skeleton-sub"><SkeletonBar style={{ width: `${Math.round(w * 0.7)}%` }} /></span>}
          </span>
        </div>
      ))}
    </div>
  );
}
