import http from 'node:http';

// Prevent crashes from unhandled errors
process.on('uncaughtException', (err) => {
  console.error('Mock SSE server uncaught exception:', err.message);
});
process.on('unhandledRejection', (err) => {
  console.error('Mock SSE server unhandled rejection:', err);
});

let scenarios = {};
let requestLog = [];

const server = http.createServer(async (req, res) => {
  try {
  // CORS preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, PATCH, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept',
    });
    res.end();
    return;
  }

  // ── Control endpoints (called by tests) ──
  if (req.url === '/__scenario' && req.method === 'POST') {
    const body = await readBody(req);
    const { method, path, events, delay, status, errorBody, once, json } = JSON.parse(body);
    const key = `${method || 'GET'} ${path}`;
    scenarios[key] = { events: events || [], delay: delay ?? 50, status: status || 200, errorBody, once, json };
    res.writeHead(200);
    res.end('ok');
    return;
  }
  if (req.url === '/__reset' && req.method === 'POST') {
    scenarios = {};
    requestLog = [];
    res.writeHead(200);
    res.end('ok');
    return;
  }
  if (req.url === '/__requests' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(requestLog));
    return;
  }

  // ── App requests ──
  const urlPath = req.url.split('?')[0];
  const key = `${req.method} ${urlPath}`;

  // Try exact match, then pattern match
  let scenario = scenarios[key];
  if (!scenario) {
    for (const [k, v] of Object.entries(scenarios)) {
      const pattern = k.replace(/\*/g, '[^/]+');
      if (new RegExp(`^${pattern}$`).test(key)) {
        scenario = v;
        break;
      }
    }
  }

  // Log request
  const reqBody = ['POST', 'PUT', 'PATCH'].includes(req.method) ? await readBody(req) : null;
  requestLog.push({
    method: req.method,
    url: req.url,
    body: reqBody ? tryParseJSON(reqBody) : null,
  });

  if (!scenario) {
    res.writeHead(404, { 'Access-Control-Allow-Origin': '*' });
    res.end('no scenario');
    return;
  }

  // Remove if one-shot
  if (scenario.once) {
    delete scenarios[key];
    for (const k of Object.keys(scenarios)) {
      if (scenarios[k] === scenario) delete scenarios[k];
    }
  }

  // Non-200: return JSON error
  if (scenario.status !== 200) {
    res.writeHead(scenario.status, {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
    });
    res.end(JSON.stringify(scenario.errorBody || { detail: 'error' }));
    return;
  }

  // JSON response (non-SSE)
  if (scenario.json !== undefined) {
    res.writeHead(200, {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
    });
    res.end(JSON.stringify(scenario.json));
    return;
  }

  // ── SSE streaming response ──
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': '*',
  });

  for (let i = 0; i < scenario.events.length; i++) {
    const ev = scenario.events[i];
    const lines = [];
    lines.push(`id: ${ev.id ?? i + 1}`);
    if (ev.event) lines.push(`event: ${ev.event}`);
    lines.push(`data: ${JSON.stringify(ev.data)}`);
    lines.push('', '');
    res.write(lines.join('\n'));

    // An event may carry its own `delayAfter` (ms) to shape timing around
    // a single gap without slowing the whole stream.
    const wait = ev.delayAfter ?? scenario.delay;
    if (i < scenario.events.length - 1 && wait > 0) {
      await new Promise((r) => setTimeout(r, wait));
    }
  }
  res.end();
  } catch (err) {
    console.error('Mock SSE server request error:', err.message);
    if (!res.headersSent) {
      res.writeHead(500, { 'Access-Control-Allow-Origin': '*' });
    }
    res.end(JSON.stringify({ error: err.message }));
  }
});

function readBody(req) {
  return new Promise((resolve) => {
    let b = '';
    req.on('data', (c) => (b += c));
    req.on('end', () => resolve(b));
  });
}

function tryParseJSON(str) {
  try {
    return JSON.parse(str);
  } catch {
    return str;
  }
}

server.listen(4100, () => console.log('Mock SSE server on :4100'));
