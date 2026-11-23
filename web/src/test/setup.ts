/// <reference types="vitest/globals" />
import i18n, { initI18n } from '@/i18n';
import { installBrowserTraps } from './browserTraps';
import { installJsdomShims } from './jsdomShims';
import { resources } from './catalogs';

// Node's BroadcastChannel reaches every worker thread in the process, so under
// the vm pool a file hears whatever the files running beside it broadcast: one
// file's dashboard saving invalidated another's preferences mid-test. Each file
// gets its own namespace; channels within a file still talk to each other.
if (typeof BroadcastChannel !== 'undefined') {
  const NodeBroadcastChannel = BroadcastChannel;
  const scope = globalThis.crypto.randomUUID();
  globalThis.BroadcastChannel = class extends NodeBroadcastChannel {
    readonly #name: string;
    constructor(name: string) {
      super(`${scope}:${name}`);
      this.#name = name;
    }
    override get name() {
      return this.#name;
    }
  };
}

// The app's i18n config with every catalog handed over up front
// (`test/catalogs.ts`), so `t()` returns real strings and a language switch
// lands before the next line; the app loads catalogs on demand, which would
// make every switch in a test async. One setup file serves both environments:
// a test file that declares `// @vitest-environment node` has no `window`,
// and gets the traps instead of the jsdom shims.
if (typeof window === 'undefined') {
  // jsdom answers `navigator.language` with en-US; Node derives it from the
  // machine's locale. Pin it so a node file renders the same strings on every
  // machine. Done before the traps so i18n's own document probe still runs.
  await initI18n(resources);
  await i18n.changeLanguage('en-US');
  installBrowserTraps();
} else {
  void initI18n(resources);
  installJsdomShims();
}
