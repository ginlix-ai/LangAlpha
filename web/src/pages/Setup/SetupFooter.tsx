/**
 * A setup step's Back, skip and forward buttons, drawn in the wizard's pinned
 * footer rather than at the end of the step, so a long model list never pushes
 * the way on below the fold. Each step keeps its own handlers; this only moves
 * where they render.
 *
 * Mounted on its own, outside the wizard (as the step tests do), a step's
 * footer renders in place.
 *
 * A step on its way out stays mounted while it fades (WizardShell), and its
 * buttons leave the footer at once: the footer is the next step's from the
 * moment it is asked for.
 */
import { createContext, useContext } from 'react';
import { createPortal } from 'react-dom';
import { WizardFooter, type WizardFooterProps } from '@/components/ui/wizard';
import { useIsPresent } from '@/lib/framer';

/** The footer element to draw into: undefined outside the wizard, null for the
 *  one render before the wizard's footer has mounted. */
export const SetupFooterSlot = createContext<HTMLElement | null | undefined>(undefined);

export function SetupFooter(props: WizardFooterProps) {
  const slot = useContext(SetupFooterSlot);
  const present = useIsPresent();
  const footer = <WizardFooter {...props} />;
  if (slot === undefined) return <div className="pt-2">{footer}</div>;
  if (!present) return null;
  return slot ? createPortal(footer, slot) : null;
}
