import React from 'react';
import { Loader } from '@/components/ui/loader';

/**
 * The Loader on a send button, in the button's own colour, mounted only while
 * the send goes so an idle control keeps no clock running. Its cell keeps the
 * width either way. The button's label says what it is doing.
 */
export function SendingGlyph({ on }: { on: boolean }): React.ReactElement {
  return (
    <span aria-hidden className="inline-grid w-3 place-items-center">
      {on && <Loader size={11} style={{ color: 'inherit' }} />}
    </span>
  );
}
