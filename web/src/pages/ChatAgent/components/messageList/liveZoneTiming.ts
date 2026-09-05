/**
 * Minimum time a just-completed item stays in the live zone before folding.
 * A leaf module with no imports: the e2e live-zone spec reads it from Node,
 * where the rest of the render-block module graph (i18n JSON) cannot load.
 */
export const MIN_LIVE_EXPOSURE_MS = 1800;
