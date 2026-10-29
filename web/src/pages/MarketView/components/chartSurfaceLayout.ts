/**
 * The narrowest width the compact chart surface is laid out for: the width at
 * which the toolbar row holds a typical quote lead whole. A host that sizes
 * the surface (the workspace panel's chart tab) holds this as its floor;
 * below it the chart still lays out, with the legend ellipsized.
 *
 * Derived from a Chrome measurement with the toolbar's own stylesheet: a
 * `BRK.A 612345.00 +1234.00 (+0.20%)` lead with the after-hours pair and
 * the Closed status is 502px, of which the pair is roughly 190px, so a lead
 * without it and with a typical price width comes to about 300px. The
 * interval dropdown is 51px, the overflow menu 28px and three toolbar-sized
 * trail buttons 92px; the row's padding and gaps 44px. That is 515px inside
 * the chart's card, plus its 1px edges and the surface's 10px gutters,
 * 537px, rounded up to 20px. The long lead reads as no spare room to the
 * tier logic, so the toolbar folds to its tightest tier, which is sized for
 * exactly that, and the lead ellipsizes what is left over rather than
 * pushing the controls.
 */
export const CHART_SURFACE_MIN_WIDTH = 560;
