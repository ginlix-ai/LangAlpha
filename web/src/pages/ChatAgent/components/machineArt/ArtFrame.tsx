import type { ReactNode } from 'react';

import { cn } from '@/lib/utils';

import './machineArt.css';

interface ArtFrameProps {
  /** The mono labels in the corners, each left out when empty. */
  tl?: ReactNode;
  tr?: ReactNode;
  bl?: ReactNode;
  br?: ReactNode;
  className?: string;
  inert?: boolean;
  onClick?: () => void;
  'data-testid'?: string;
  /** The drawing, stood on the frame's floor. */
  children: ReactNode;
}

/**
 * The framed ground a machine is drawn in, with mono labels in its corners:
 * a card's machine, the one a form will make, and the rail's New computer.
 */
export function ArtFrame({ tl, tr, bl, br, className, inert, onClick, 'data-testid': testId, children }: ArtFrameProps) {
  return (
    <div className={cn('art-frame', className)} inert={inert} onClick={onClick} data-testid={testId}>
      {tl && <span className="art-frame-corner is-tl">{tl}</span>}
      {tr && <span className="art-frame-corner is-tr">{tr}</span>}
      <div className="art-frame-figure">{children}</div>
      {/* One row, so a long status wraps beside the badges rather than under them. */}
      {(bl || br) && (
        <div className="art-frame-foot">
          {bl && <span className="art-frame-corner is-bl">{bl}</span>}
          {br && <span className="art-frame-corner is-br">{br}</span>}
        </div>
      )}
    </div>
  );
}
