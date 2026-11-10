/**
 * The dashboards a user's agent built and the user set in place of the
 * built-in one: the saved list, and the two ways onto it (a running app, an
 * HTML page kept on the server).
 */
export { MAX_DASHBOARDS, readSavedDashboards } from './saved';
export type { AppDashboard, PageDashboard, SavedDashboard, SavedDashboards } from './saved';
export { OPEN_SWITCHER, useSavedDashboards } from './useSavedDashboards';
export type { SavedDashboardsApi } from './useSavedDashboards';
export { usePinAppAsDashboard, usePinPageAsDashboard } from './usePinAsDashboard';
