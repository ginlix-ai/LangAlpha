import type { AnnouncementDef } from './types';

/**
 * Feature announcements that drive the versioned "What's New" modal. Adding one
 * = an entry here + i18n keys under `onboarding.announce.*`.
 *
 * Versioning is CalVer `YYYY.MM.DD[.N]`, reserved for large features — routine
 * releases add no entry and the version stands still. `releaseVersion` is simply
 * the date the feature ships (matching the GitHub release tag minus the `v`
 * when one exists, but no tag is required). A new entry must be strictly newer
 * than the current max across this array — that max is the app's announcement
 * version (`latestReleaseVersion`), which `ensureFirstRun` stamps on new users.
 */

const dashboardCustomizeAnnouncement: AnnouncementDef = {
  key: 'dashboard-custom-mode',
  // Custom widget layout (#167) + widget framework (#172), released v2026.04.26.
  releaseVersion: '2026.04.26',
  modalTitleKey: 'onboarding.announce.dashCustom.modalTitle',
  modalBodyKey: 'onboarding.announce.dashCustom.modalBody',
};

export const ANNOUNCEMENTS: AnnouncementDef[] = [dashboardCustomizeAnnouncement];
