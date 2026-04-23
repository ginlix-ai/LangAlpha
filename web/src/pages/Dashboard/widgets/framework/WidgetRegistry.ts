import type { WidgetDefinition } from '../types';

const registry = new Map<string, WidgetDefinition<unknown>>();

export function registerWidget<C>(def: WidgetDefinition<C>): void {
  registry.set(def.type, def as WidgetDefinition<unknown>);
}

export function getWidget(type: string): WidgetDefinition<unknown> | undefined {
  return registry.get(type);
}

export function listWidgets(): WidgetDefinition<unknown>[] {
  return Array.from(registry.values());
}

export function listWidgetsByCategory(): Record<string, WidgetDefinition<unknown>[]> {
  const grouped: Record<string, WidgetDefinition<unknown>[]> = {};
  for (const def of registry.values()) {
    if (!grouped[def.category]) grouped[def.category] = [];
    grouped[def.category].push(def);
  }
  return grouped;
}
