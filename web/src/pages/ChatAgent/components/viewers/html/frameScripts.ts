import earlySource from './frameScripts/early.js?raw';
import bridgeSource from './frameScripts/bridge.js?raw';
import streamSource from './frameScripts/stream.js?raw';

/**
 * The programs a widget document carries, as it carries them. Each file keeps
 * its notes and lint directives above the program, which starts at the first
 * line opening `(function`: an uncalled function expression the template calls
 * with its arguments. Only what follows goes into every widget.
 */
function program(source: string, name: string): string {
  const at = source.search(/^\(function/m);
  if (at < 0) throw new Error(`frameScripts/${name}.js has no program`);
  const code = source.slice(at).replace(/\n$/, '');
  // The program sits in an inline <script>, which this would close.
  if (/<\/script/i.test(code)) throw new Error(`frameScripts/${name}.js would close its <script>`);
  return code;
}

/** Runs first in a widget document: `(SEED, BASE)`, the storage seed and the base. */
export const EARLY_SCRIPT = program(earlySource, 'early');

/** The parent's side of every widget document: `(MAX)`, the prompt data cap in bytes. */
export const BRIDGE_SCRIPT = program(bridgeSource, 'bridge');

/** The preview's receiver (`buildStreamSrcDoc`), called with no arguments. */
export const STREAM_RECEIVER_SCRIPT = program(streamSource, 'stream');
