/**
 * The app's own fonts, bundled. The app loads them from Google Fonts, which a
 * sandbox may not reach, and a canvas paints its labels once with whatever face
 * is loaded at that moment: a late font would leave the chart in a fallback
 * whose wider glyphs change which labels collide. So every face is in before
 * the chart mounts.
 */
import geistLatin from '@fontsource-variable/geist/files/geist-latin-wght-normal.woff2?url';
import geistLatinExt from '@fontsource-variable/geist/files/geist-latin-ext-wght-normal.woff2?url';
import monoLatin from '@fontsource-variable/jetbrains-mono/files/jetbrains-mono-latin-wght-normal.woff2?url';
import monoLatinExt from '@fontsource-variable/jetbrains-mono/files/jetbrains-mono-latin-ext-wght-normal.woff2?url';
import soraLatin from '@fontsource-variable/sora/files/sora-latin-wght-normal.woff2?url';
import soraLatinExt from '@fontsource-variable/sora/files/sora-latin-ext-wght-normal.woff2?url';

// Fontsource's ranges for the two subsets.
const LATIN = 'U+0000-00FF,U+0131,U+0152-0153,U+02BB-02BC,U+02C6,U+02DA,U+02DC,U+0304,U+0308,U+0329,U+2000-206F,U+20AC,U+2122,U+2191,U+2193,U+2212,U+2215,U+FEFF,U+FFFD';
const LATIN_EXT = 'U+0100-02BA,U+02BD-02C5,U+02C7-02CC,U+02CE-02D7,U+02DD-02FF,U+0304,U+0308,U+0329,U+1D00-1DBF,U+1E00-1E9F,U+1EF2-1EFF,U+2020,U+20A0-20AB,U+20AD-20C0,U+2113,U+2C60-2C7F,U+A720-A7FF';

const WEBFONTS: Array<[family: string, weight: string, latin: string, latinExt: string]> = [
  ['Geist', '100 900', geistLatin, geistLatinExt],
  ['Sora', '100 800', soraLatin, soraLatinExt],
  ['JetBrains Mono', '100 800', monoLatin, monoLatinExt],
];

// Names the app's stacks ask for, answered by faces a Linux sandbox has: Noto
// Sans SC ships there as Noto Sans CJK SC, and the system UI face the chart's
// axes ask for (`-apple-system, BlinkMacSystemFont, …`) is closest to Arial's
// metrics, which Liberation Sans has.
const LOCAL_ALIASES: Array<[family: string, local: string]> = [
  ['Noto Sans SC', 'Noto Sans CJK SC'],
  ['BlinkMacSystemFont', 'Liberation Sans'],
];

export async function loadFonts(): Promise<void> {
  const faces: FontFace[] = [];
  for (const [family, weight, latin, latinExt] of WEBFONTS) {
    faces.push(new FontFace(family, `url(${latin}) format('woff2')`, { weight, unicodeRange: LATIN }));
    faces.push(new FontFace(family, `url(${latinExt}) format('woff2')`, { weight, unicodeRange: LATIN_EXT }));
  }
  for (const [family, local] of LOCAL_ALIASES) {
    faces.push(new FontFace(family, `local('${local}')`));
  }
  // A local face the machine lacks rejects; the stack falls through as it would.
  await Promise.allSettled(faces.map(async (face) => {
    await face.load();
    document.fonts.add(face);
  }));
}
