/**
 * The glyphs a package may name for its skills (`skills.<name>.icon` in its
 * `plugin.json`), keyed by lucide's own name.
 *
 * A closed set rather than lucide's whole catalog: a name has to become an
 * import at build time, and resolving any name at runtime would mean shipping
 * every icon or fetching one per row. A name outside this map falls back to
 * the package's mark, so adding a glyph is an edit here and nowhere else.
 */
import {
  Activity,
  AppWindow,
  Binoculars,
  BuildingComplex,
  Calculator,
  CalendarClock,
  ChartColumnIncreasing,
  ChartPie,
  ChartSpline,
  CircleQuestionMark,
  ClipboardCheck,
  Columns3,
  DoorOpen,
  FileCode,
  FileSpreadsheet,
  FileText,
  FileType,
  Flag,
  Globe,
  Layers,
  LayoutDashboard,
  Lightbulb,
  ListChecks,
  Megaphone,
  Palette,
  Presentation,
  RefreshCw,
  Ruler,
  Scale,
  Sprout,
  Sunrise,
  Swords,
  Target,
  Telescope,
  Timer,
  Workflow,
  type LucideIcon,
} from 'lucide-react';

export const SKILL_GLYPHS: Readonly<Record<string, LucideIcon>> = {
  activity: Activity,
  'app-window': AppWindow,
  binoculars: Binoculars,
  'building-complex': BuildingComplex,
  calculator: Calculator,
  'calendar-clock': CalendarClock,
  'chart-column-increasing': ChartColumnIncreasing,
  'chart-pie': ChartPie,
  'chart-spline': ChartSpline,
  'circle-question-mark': CircleQuestionMark,
  'clipboard-check': ClipboardCheck,
  'columns-3': Columns3,
  'door-open': DoorOpen,
  'file-code': FileCode,
  'file-spreadsheet': FileSpreadsheet,
  'file-text': FileText,
  'file-type': FileType,
  flag: Flag,
  globe: Globe,
  layers: Layers,
  'layout-dashboard': LayoutDashboard,
  lightbulb: Lightbulb,
  'list-checks': ListChecks,
  megaphone: Megaphone,
  palette: Palette,
  presentation: Presentation,
  'refresh-cw': RefreshCw,
  ruler: Ruler,
  scale: Scale,
  sprout: Sprout,
  sunrise: Sunrise,
  swords: Swords,
  target: Target,
  telescope: Telescope,
  timer: Timer,
  workflow: Workflow,
};

/** The glyph a skill's package named, or undefined for none or one we do not ship. */
export function skillGlyph(name: string | null | undefined): LucideIcon | undefined {
  // `hasOwn`, so a name like `constructor` cannot reach the prototype.
  return name && Object.hasOwn(SKILL_GLYPHS, name) ? SKILL_GLYPHS[name] : undefined;
}
