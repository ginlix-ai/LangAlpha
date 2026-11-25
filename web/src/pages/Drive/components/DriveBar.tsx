import type React from 'react';

/**
 * A band under the file's header that says one thing about what is on
 * screen (a conflict, a change underneath, an old version) and offers what
 * to do about it.
 */
export function DriveBar({ role, children, actions }: {
  /** `alert` for a refused save, `status` for news that can wait. */
  role?: 'alert' | 'status';
  children: React.ReactNode;
  actions: React.ReactNode;
}) {
  return (
    <div className="drive-bar" role={role}>
      <span className="drive-bar-text">{children}</span>
      <span className="drive-bar-actions">{actions}</span>
    </div>
  );
}
