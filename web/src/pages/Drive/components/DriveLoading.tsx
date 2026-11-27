import { Loader } from '@/components/ui/loader';

/**
 * The pane's wait. It shows only once a read outlasts an entrance, so a file
 * that arrives at once never flashes a spinner first (Drive.css).
 */
export function DriveLoading() {
  return (
    <div className="drive-loading" aria-busy="true">
      <Loader size={20} className="text-(--color-text-tertiary)" />
    </div>
  );
}
