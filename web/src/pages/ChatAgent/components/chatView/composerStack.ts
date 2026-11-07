/**
 * A row that draws a surface in the composer stack (the error, the restore
 * offer, the disk warning) takes the composer's corner, which each card style
 * states (`--cs-composer-stack-radius`), and sets its text in where that
 * corner is round enough to need it (Balanced's `--cs-composer-row-pad`). The
 * same rows elsewhere keep their own corners.
 */
export const STACK_ROW_CLASS = 'rounded-[var(--cs-composer-stack-radius,0.5rem)] pl-[var(--cs-composer-row-pad,0.75rem)]';
