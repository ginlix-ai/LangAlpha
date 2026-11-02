'use strict'

/**
 * The bundled stack's test tree: small stand-in programs that speak the same
 * contracts as the real ones, Postgres's `postmaster.pid`, a port that answers,
 * an exit code. That reaches what a real stack will not do on demand, a failed
 * migration, a child that keeps dying, a port someone else took, and runs real
 * processes with real signals, so stopping and the parent-death guard are the
 * real thing. No Electron in it, since the supervisor has none.
 */

const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')

const SRC = path.join(__dirname, '..', 'src', 'stack')
const { createSupervisor } = require(path.join(SRC, 'supervisor.js'))

const roots = []
function tempRoot() {
  const dir = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'la-stack-'))
  roots.push(dir)
  return dir
}

const node = process.execPath

/** What each stand-in reads for the test's instructions and writes what it did to. */
function program(file, source) {
  fs.mkdirSync(path.dirname(file), { recursive: true })
  fs.writeFileSync(file, `#!${node}\nconst fs=require('fs');const path=require('path');const net=require('net');const http=require('http');\nconst tree=${JSON.stringify(path.dirname(file).replace(/\/[^/]+\/bin$/, ''))};\nconst control=()=>{try{return JSON.parse(fs.readFileSync(path.join(tree,'control.json'),'utf8'))}catch{return {}}};\nconst record=(line)=>fs.appendFileSync(path.join(tree,'calls.log'),line+'\\n');\n${source}\n`, { mode: 0o755 })
}

function fakeTree({ digest = 'one', major = 18 } = {}) {
  const tree = tempRoot()
  fs.writeFileSync(path.join(tree, 'manifest.json'), JSON.stringify({
    arch: process.arch,
    python: { minor: '3.13', version: '3.13.0' },
    postgres: { major, version: `${major}.0` },
    migrationsDigest: digest,
    source: { commit: 'abc123', modified: false },
  }))
  fs.mkdirSync(path.join(tree, 'app'))
  fs.mkdirSync(path.join(tree, 'web'))

  program(path.join(tree, 'pg', 'bin', 'initdb'), `
    const dir=process.argv[process.argv.indexOf('-D')+1];
    record('initdb');
    if(control().failInitdb){console.error('initdb: refused');process.exit(1)}
    fs.mkdirSync(dir,{recursive:true});
    fs.writeFileSync(path.join(dir,'PG_VERSION'),'${major}\\n');
  `)
  program(path.join(tree, 'pg', 'bin', 'postgres'), `
    const a=process.argv; const dir=a[a.indexOf('-D')+1]; const port=Number(a[a.indexOf('-p')+1]);
    record('postgres '+process.pid);
    const pidFile=path.join(dir,'postmaster.pid');
    if(fs.existsSync(pidFile)){
      const other=Number(fs.readFileSync(pidFile,'utf8').split('\\n')[0]);
      let alive=false; try{process.kill(other,0);alive=true}catch{}
      if(alive){console.error('FATAL: lock file "postmaster.pid" already exists');process.exit(1)}
    }
    const server=net.createServer();
    server.once('error',()=>{console.error('LOG: could not bind IPv4 address: Address already in use');process.exit(1)});
    server.listen(port,'127.0.0.1',()=>{
      fs.writeFileSync(pidFile,[process.pid,dir,'',port,'','127.0.0.1','','ready',''].join('\\n'));
    });
    process.on('SIGINT',()=>{record('postgres fast shutdown');fs.rmSync(pidFile,{force:true});process.exit(0)});
  `)
  program(path.join(tree, 'valkey', 'bin', 'valkey-server'), `
    const conf=fs.readFileSync(process.argv[2],'utf8');
    const port=Number(/^port (\\d+)/m.exec(conf)[1]);
    record('valkey '+process.pid);
    const server=net.createServer();
    server.once('error',()=>{console.error('Could not create server TCP listening socket: bind: Address already in use');process.exit(1)});
    server.listen(port,'127.0.0.1');
    process.on('SIGTERM',()=>{record('valkey stop');process.exit(0)});
  `)
  program(path.join(tree, 'python', 'bin', 'python3'), `
    const a=process.argv.slice(2);
    if(a[0]==='-c'){
      record('check');
      if(control().newerData){console.log('f00dfeed');process.exit(3)}
      if(control().failCheck){console.error('could not connect');process.exit(1)}
      process.exit(0);
    }
    if(a[0]==='-m'&&a[1]==='alembic'){
      record('migrate');
      const c=control();
      if(c.scribble)fs.writeFileSync(c.scribble,'half migrated');
      if(c.failMigrate){console.error('ERROR [alembic] migration failed');process.exit(1)}
      process.exit(0);
    }
    const port=Number(a[a.indexOf('--port')+1]);
    record('server '+process.pid+' '+port);
    fs.writeFileSync(path.join(tree,'server-env.json'),JSON.stringify(process.env));
    if(control().failServer){console.error('Traceback: boom');process.exit(1)}
    const pages={'/api/v1/setup/status':'{"needs_setup":true}','/':'<div id="root"></div>'};
    const server=http.createServer((req,res)=>{
      const c=control();
      const answer=()=>{res.statusCode=c.health||200;res.end(c.blankShell&&req.url==='/'?'':pages[req.url]||'{}')};
      if(c.healthDelayMs&&req.url==='/health'){record('health');setTimeout(answer,c.healthDelayMs)}else answer();
    });
    server.once('error',()=>{console.error('ERROR: [Errno 48] error while attempting to bind on address');process.exit(1)});
    server.listen(port,'127.0.0.1');
    process.on('SIGTERM',()=>{record('server stop');const c=control();if(c.lingerMs)setTimeout(()=>process.exit(0),c.lingerMs);else process.exit(0)});
  `)
  return tree
}

const control = (tree, value) => fs.writeFileSync(path.join(tree, 'control.json'), JSON.stringify(value))
const calls = (tree) => {
  try {
    return fs.readFileSync(path.join(tree, 'calls.log'), 'utf8').trim().split('\n')
  } catch {
    return []
  }
}
const steps = (line) => line.split(' ')[0]

const running = []
function supervisor(tree, userData, { env = {}, port = null, ...options } = {}) {
  let saved = port
  const sup = createSupervisor({
    treeRoot: tree,
    userData,
    env: { PATH: process.env.PATH, HOME: userData, ...env },
    loadPort: () => saved,
    savePort: (value) => { saved = value },
    ...options,
  })
  running.push(sup)
  return sup
}

const until = async (check, ms = 10000) => {
  const deadline = Date.now() + ms
  while (Date.now() < deadline) {
    const value = await check()
    if (value) return value
    await new Promise((r) => setTimeout(r, 25))
  }
  throw new Error('condition not met in time')
}

const alive = (pid) => {
  try {
    process.kill(pid, 0)
    return true
  } catch {
    return false
  }
}

/** For each file's `after`: every supervisor stopped and every temporary folder gone. */
async function cleanup() {
  await Promise.all(running.map((sup) => sup.stop()))
  for (const dir of roots) fs.rmSync(dir, { recursive: true, force: true })
}

module.exports = { SRC, node, tempRoot, program, fakeTree, control, calls, steps, supervisor, until, alive, cleanup }
