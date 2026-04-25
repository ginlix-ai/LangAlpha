import { Link } from 'react-router-dom';

function Legal() {
  return (
    <div
      className="h-screen overflow-y-auto py-12 px-4 sm:px-6 lg:px-8"
      style={{ backgroundColor: 'var(--color-bg-page)', color: 'var(--color-text-primary)' }}
    >
      <article className="mx-auto max-w-3xl">
        <header className="mb-10 border-b pb-6" style={{ borderColor: 'var(--color-border-subtle)' }}>
          <Link
            to="/"
            className="text-sm hover:underline"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            &larr; Back to LangAlpha
          </Link>
          <h1 className="mt-4 text-3xl font-semibold tracking-tight">Legal &amp; Attribution</h1>
          <p className="mt-2 text-sm" style={{ color: 'var(--color-text-secondary)' }}>
            Open-source licenses and third-party credits for the hosted LangAlpha service.
          </p>
        </header>

        <div className="space-y-10 text-base leading-relaxed">
          <section>
            <h2 className="text-xl font-semibold tracking-tight mb-3">TradingView</h2>
            <p>
              Charts and market widgets provided by{' '}
              <a
                href="https://www.tradingview.com/"
                target="_blank"
                rel="noopener noreferrer"
                className="underline"
              >
                TradingView
              </a>
              . The dashboard embeds the free TradingView widget library (Ticker Tape,
              Stock Heatmap, Economic Events, Technical Analysis, and related widgets).
              Each embedded widget carries its own attribution and links back to
              tradingview.com for full interactivity. The in-app real-time price chart
              is built on{' '}
              <a
                href="https://www.tradingview.com/lightweight-charts/"
                target="_blank"
                rel="noopener noreferrer"
                className="underline"
              >
                Lightweight Charts™
              </a>
              , released by TradingView under the Apache License, Version 2.0.
            </p>
          </section>

          <section>
            <h2 className="text-xl font-semibold tracking-tight mb-3">
              Lightweight Charts™ NOTICE
            </h2>
            <p className="text-sm mb-3" style={{ color: 'var(--color-text-secondary)' }}>
              Reproduced verbatim from the upstream project&apos;s NOTICE file.
            </p>
            <pre
              className="whitespace-pre-wrap text-[13px] p-4 rounded border font-mono"
              style={{
                backgroundColor: 'var(--color-bg-subtle)',
                borderColor: 'var(--color-border-muted)',
                color: 'var(--color-text-primary)',
              }}
            >{`TradingView Lightweight Charts™
Copyright (с) 2025 TradingView, Inc. https://www.tradingview.com/`}</pre>
            <p className="mt-3 text-sm" style={{ color: 'var(--color-text-secondary)' }}>
              Licensed under the Apache License, Version 2.0 — see{' '}
              <a
                href="https://www.apache.org/licenses/LICENSE-2.0"
                target="_blank"
                rel="noopener noreferrer"
                className="underline"
              >
                the full license text
              </a>
              . The Lightweight Charts™ trademark is the property of TradingView, Inc.;
              use of the TradingView name is limited to the attribution above.
            </p>
          </section>

          <section>
            <h2 className="text-xl font-semibold tracking-tight mb-3">Market data</h2>
            <p>
              Real-time and historical equity pricing in the native charts is sourced
              through data providers and is intended for research only. It is not
              investment advice and should not be treated as an execution-quality feed.
            </p>
          </section>

          <section>
            <h2 className="text-xl font-semibold tracking-tight mb-3">Other credits</h2>
            <p>
              LangAlpha is built on React, Vite, FastAPI, PostgreSQL, Redis, and many
              other open-source projects. The complete list of dependencies and their
              licenses is available in the{' '}
              <a
                href="https://github.com/ginlix/langalpha"
                target="_blank"
                rel="noopener noreferrer"
                className="underline"
              >
                langalpha repository
              </a>
              .
            </p>
            <p className="mt-3 text-sm">
              See also our{' '}
              <Link to="/privacy" className="underline">
                Privacy Policy
              </Link>
              .
            </p>
          </section>
        </div>
      </article>
    </div>
  );
}

export default Legal;
