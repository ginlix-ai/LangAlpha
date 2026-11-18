// The card's box, shared with its skeleton so a placeholder is the card's size,
// and with the shared workspaces' cards, which sit in the same grid.
// The open card styles draw it unfilled inside their edge, with its title cut
// into the top edge (WorkspaceCard.css). The edge firms under the pointer
// (`.hover-lift`).
export const CARD_BODY = 'pf-card relative flex h-full w-full flex-col gap-1.5 overflow-hidden rounded-[var(--cs-radius-card,0.75rem)] py-3.5 pl-[18px] pr-4 md:px-5 md:py-4';
export const CARD_FILL = 'var(--color-bg-card-gradient, linear-gradient(to bottom, var(--color-border-muted), var(--color-border-muted)))';
export const CARD_SURFACE = {
  background: `var(--color-shell-fill, ${CARD_FILL})`,
  border: '1px solid var(--lift-edge, var(--color-shell-edge, var(--color-bg-card-border, var(--color-border-muted))))',
} as const;
