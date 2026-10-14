#!/usr/bin/env node
// Disposable Linux native-runtime experiment. The generator runs outside the
// measured cgroup; every socket completes the Shiny protocol and renders input.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash, randomBytes } from 'node:crypto';
import { cp, mkdir, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const require = createRequire(join(root, 'package.json'));
// jsdom's dependency: npm ci at the repository root installs this driver.
const WebSocket = require('ws');
const { chromium } = createRequire(join(root, 'loadtest/render/driver/package.json'))('playwright');
const binary = resolve(process.env.LOAD_BINARY || '/tmp/shinyhub-linux-load');
const profile = process.env.LOAD_PROFILE || 'pressure';
assert.ok(['pressure', 'roomy'].includes(profile));
const sessions = Number(process.env.LOAD_SESSIONS || 1000);
const cores = Number(process.env.LOAD_CORES || (profile === 'roomy' ? 4 : 1));
const requestCores = Number(process.env.LOAD_CPU_REQUEST || (profile === 'roomy' ? 1 : 0.35));
const reserveCores = Number(process.env.LOAD_CPU_RESERVE || (profile === 'roomy' ? 0.5 : 0.1));
assert.ok(Number.isInteger(sessions) && sessions > 0 && sessions <= 1000);
const work = await mkdtemp(join(root, 'loadtest/results/update-capacity-'));
const container = `shinyhub-load-${randomBytes(6).toString('hex')}`;
const password = randomBytes(24).toString('hex');
const report = { status: 'failed', settings: { profile, sessions, cores, memory_mib: 2048,
  request_cores: requestCores, reserve_cores: reserveCores, active_cpu_ms_per_render: 50,
  background_cpu_ms_per_render: 5, active_sessions: 20, protocol_compression: false,
  session_kib: 128 }, phases: {}, events: [], samples: [] };
let host, token, phase = 'setup', cadence = 120_000, sampling = false, probing = false;
let browser, browserPage, browserTask, browserDriving = false, browserPlanned = false;
const browserSockets = [];
let sampleTask, probeTask;
let sampleFailure;
const clients = [];
const activeCommands = new Set();
const timeout = setTimeout(() => {
  for (const child of activeCommands) child.kill('SIGTERM');
  process.exitCode = 1;
}, 900_000);

async function command(args, name, expected = 0) {
  const child = spawn(args[0], args.slice(1), { cwd: root, env: process.env });
  activeCommands.add(child);
  let stdout = '', stderr = '';
  child.stdout.on('data', chunk => { stdout += chunk; });
  child.stderr.on('data', chunk => { stderr += chunk; });
  try {
    const code = await new Promise((resolve, reject) => {
      child.once('error', reject); child.once('close', resolve);
    });
    if (name) await writeFile(join(work, `${name}.log`), stdout + stderr, { mode: 0o600 });
    if (expected === 'nonzero') assert.notEqual(code, 0, `${name} unexpectedly succeeded`);
    else assert.equal(code, expected, `${name || args.join(' ')} failed: ${stderr.slice(-1500)}`);
    return stdout;
  } finally { activeCommands.delete(child); }
}
const docker = (...args) => command(['docker', ...args]);
async function poll(fn, description, limit = 120_000) {
  const end = Date.now() + limit;
  while (Date.now() < end) {
    if (sampleFailure) throw sampleFailure;
    if (await fn()) return;
    await delay(200);
  }
  throw new Error(`timeout: ${description}`);
}
const event = (name, data = {}) => {
  console.log(`==> ${name}`);
  report.events.push({ name, phase, at: new Date().toISOString(), ...data });
};
const metrics = () => report.phases[phase] ||= { latencies_ms: [], page_latencies_ms: [],
  page_errors: [], socket_errors: [], unexpected_closes: 0, planned_closes: 0, acknowledgements: 0 };
async function api(path = '', method = 'GET', body, expected = 200) {
  const res = await fetch(host + '/api/apps/load' + path, { method,
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', 'User-Agent': 'shinyhub-load-test' },
    body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(30_000) });
  const value = res.status === 204 ? null : await res.json();
  assert.equal(res.status, expected, `${method} ${path}: ${JSON.stringify(value)}`);
  return value;
}
const deploy = (name, expected = 0) => command(['docker', 'exec', '-e', `SHINYHUB_HOST=http://127.0.0.1:8080`,
  '-e', `SHINYHUB_TOKEN=${token}`, container, '/opt/shinyhub', 'deploy', '/input', '--config', '/state/client.json',
  '--slug', 'load', '--visibility', 'public', '--output', 'json'], name, expected);
const reload = name => command(['docker', 'exec', container, '/opt/shinyhub', 'reload', '--config',
  '/state/config.yaml', '--wait', '120s', '--output', 'json'], name).then(JSON.parse);
const running = state => state.replicas_status.filter(row => row.status === 'running');

class Client {
  constructor(index) { this.index = index; this.tick = 0; this.closed = false; this.planned = false; }
  async connect() {
    const page = await fetch(host + '/app/load/', { signal: AbortSignal.timeout(30_000),
      headers: { 'User-Agent': 'shinyhub-load-test' } });
    assert.equal(page.status, 200, `client ${this.index} page`);
    await page.arrayBuffer();
    const cookie = page.headers.getSetCookie().map(value => value.split(';')[0]).join('; ');
    this.socket = new WebSocket(host.replace('http:', 'ws:') + '/app/load/websocket/', {
      // Real users have separate browser decoders. A single Node process's
      // shared zlib pool otherwise queues pings behind 1,000 compressed streams.
      perMessageDeflate: false,
      headers: { Cookie: cookie, 'User-Agent': 'shinyhub-load-test' },
    });
    this.ready = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`client ${this.index} init timed out`)), 90_000);
      this.socket.on('error', error => { metrics().socket_errors.push(String(error)); reject(error); });
      this.socket.on('open', () => {
        this.sent = performance.now();
        this.socket.send(JSON.stringify({ method: 'init', data: { tick: 0, work_ms: this.index < 20 ? 50 : 5,
          '.clientdata_output_result_hidden': false, '.clientdata_url_protocol': 'http:',
          '.clientdata_url_hostname': '127.0.0.1', '.clientdata_url_pathname': '/app/load/' } }));
      });
      this.socket.on('message', raw => {
        try {
          const message = JSON.parse(raw.toString());
          if (this.closed || !message.values?.result) return;
          const result = JSON.parse(message.values.result);
          if (this.identity) assert.equal(result.session, this.identity, 'same live Shiny session');
          if (this.processPID) assert.equal(result.process_pid, this.processPID, 'same native process');
          this.identity = result.session;
          this.processPID = result.process_pid;
          assert.equal(result.tick, this.tick, 'acknowledged reactive input');
          assert.equal(result.memory_bytes, 128 * 1024, 'private session allocation exists');
          assert.equal(result.cpu_ms, this.index < 20 ? 50 : 5, 'configured computation reaches the real render');
          this.version = result.version;
          this.dataSnapshot = result.data_snapshot;
          metrics().latencies_ms.push(performance.now() - this.sent);
          metrics().acknowledgements++;
          this.acknowledgedTick = result.tick;
          this.pending = false;
          if (!this.established) {
            this.established = true; clearTimeout(timer); resolve();
            this.next = ['ramp', 'recovery'].includes(phase) ? Infinity : Date.now() + (this.index / sessions) * 120_000;
            this.loop = setInterval(() => {
              if (this.closed || this.pending || Date.now() < this.next) return;
              const interval = this.index < 20 ? cadence : 120_000;
              this.tick++; this.pending = true; this.sent = performance.now(); this.next = Date.now() + interval;
              this.socket.send(JSON.stringify({ method: 'update', data: { tick: this.tick } }));
            }, 50);
          }
        } catch (error) {
          metrics().socket_errors.push(`client ${this.index}: ${error}`);
          clearTimeout(timer); reject(error); this.socket.terminate();
        }
      });
      this.socket.on('close', (code, reason) => {
        this.closed = true; clearInterval(this.loop); clearTimeout(timer);
        if (this.planned) metrics().planned_closes++;
        else metrics().unexpected_closes++;
        if (!this.established) reject(new Error(`client ${this.index} closed before init: ${code} ${reason}`));
      });
    });
    await this.ready;
  }
  close() { this.planned = true; this.closed = true; clearInterval(this.loop); this.socket?.terminate(); }
}

try {
  await mkdir(work, { recursive: true });
  report.commit = process.env.LOAD_COMMIT || (await command(['git', 'rev-parse', 'HEAD'])).trim();
  report.node = process.version;
  report.driver_sha256 = createHash('sha256').update(await readFile(fileURLToPath(import.meta.url))).digest('hex');
  report.binary_sha256 = createHash('sha256').update(await readFile(binary)).digest('hex');
  report.image = JSON.parse(await docker('image', 'inspect', 'shinyhub-update-load:local'))[0].Id;
  await cp(join(root, 'loadtest/update-capacity/app'), join(work, 'input'), { recursive: true });
  await writeFile(join(work, 'password'), password, { mode: 0o600 });
  await writeFile(join(work, 'config.yaml'), `server:\n  host: 0.0.0.0\n  port: 8080\n  render_capacity_cores: ${cores}\n  drain_timeout: 12s\n  reload_drain_timeout: 2s\n  reload_sessions: preserve\n  reload_relay_limit: 4\n  pid_file: /state/hub.pid\n  shutdown_apps: stop\n  update_capacity:\n    worker_cpu_request_cores: ${requestCores}\n    reserved_cpu_cores: ${reserveCores}\nauth:\n  secret: ${randomBytes(32).toString('hex')}\ndatabase:\n  dsn: /state/hub.db\nstorage:\n  apps_dir: /state/apps\n  app_data_dir: /state/app-data\nruntime:\n  default_max_sessions_per_replica: 2000\nlifecycle:\n  hibernate_timeout: 30m\n`, { mode: 0o600 });
  await docker('run', '-d', '--name', container, '--label', `shinyhub.load_run=${process.env.LOAD_NETWORK || container}`, ...(process.env.LOAD_NETWORK ? ['--network', process.env.LOAD_NETWORK] : []), '--cpus', String(cores), '--memory', '2g',
    '--memory-swap', '2g', '-p', '127.0.0.1::8080', 'shinyhub-update-load:local', 'sleep', 'infinity');
  await docker('exec', container, 'mkdir', '-p', '/state');
  await docker('cp', binary, `${container}:/opt/shinyhub`);
  await docker('cp', join(work, 'config.yaml'), `${container}:/state/config.yaml`);
  await docker('cp', join(work, 'password'), `${container}:/state/password`);
  await docker('cp', join(work, 'input'), `${container}:/input`);
  report.linux_kernel = (await docker('exec', container, 'uname', '-r')).trim();
  report.cgroup_limits = (await docker('exec', container, 'sh', '-c', 'cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/memory.max')).trim();
  host = process.env.LOAD_NETWORK ? `http://${container}:8080` : 'http://' + (await docker('port', container, '8080/tcp')).trim();
  await command(['docker', 'exec', container, '/opt/shinyhub', 'init', '--config', '/state/config.yaml',
    '--admin-user', 'load-admin', '--admin-password-file', '/state/password'], 'init');
  await docker('exec', '-d', container, 'sh', '-c', 'exec /opt/shinyhub serve --config /state/config.yaml --no-browser > /state/server.log 2>&1');
  await poll(() => fetch(host + '/activez').then(res => res.ok).catch(() => false), 'server starts');
  const login = await fetch(host + '/api/auth/login', { method: 'POST',
    headers: { 'Content-Type': 'application/json', 'User-Agent': 'shinyhub-load-test' },
    body: JSON.stringify({ username: 'load-admin', password }) });
  assert.equal(login.status, 200); ({ token } = await login.json());
  await deploy('initial-deploy');
  sampling = true;
  sampleTask = (async () => {
    while (sampling) {
      const began = Date.now();
      const raw = await docker('exec', container, 'sh', '-c', 'cat /sys/fs/cgroup/cpu.stat /sys/fs/cgroup/memory.current /sys/fs/cgroup/memory.peak /sys/fs/cgroup/memory.events');
      report.samples.push({ phase, at_ms: began, raw, app: await api('/metrics') });
      await delay(Math.max(0, 1000 - (Date.now() - began)));
    }
  })();
  sampleTask.catch(error => { sampleFailure = error; });
  probing = true;
  probeTask = (async () => {
    while (probing) {
      const began = performance.now(), m = metrics();
      try {
        const res = await fetch(host + '/app/load/', { signal: AbortSignal.timeout(15_000), headers: { 'User-Agent': 'shinyhub-load-test' } });
        const html = await res.text(); m.page_latencies_ms.push(performance.now() - began);
        if (res.status !== 200) m.page_errors.push(res.status);
        else if (!html.includes('id="tick"')) m.page_errors.push('fallback page instead of app');
      } catch (error) { m.page_errors.push(String(error)); }
      await delay(200);
    }
  })();
  event('one-session negative control');
  const control = new Client(0); clients.push(control); await control.connect();
  await delay(5000); assert.ok(control.tick > 0 && !control.closed); control.close();
  clients.length = 0;
  phase = 'ramp'; event('ramp real reactive sessions');
  const protocolSessions = sessions - 1;
  for (let offset = 0; offset < protocolSessions; offset += 25) {
    const batch = Array.from({ length: Math.min(25, protocolSessions - offset) }, (_, i) => new Client(offset + i));
    clients.push(...batch); await Promise.all(batch.map(client => client.connect()));
    assert.ok(clients.every(client => !client.closed), 'negative control ramp retains every session');
    if (clients.length % 100 === 0) console.log(`    ${clients.length} sessions initialized`);
  }
  browser = await chromium.launch({ headless: true, executablePath: process.env.LOAD_CHROMIUM || undefined });
  report.chromium = browser.version();
  browserPage = await browser.newPage();
  browserPage.on('pageerror', error => metrics().socket_errors.push(`browser: ${error}`));
  browserPage.on('websocket', socket => {
    browserSockets.push(socket);
    socket.on('close', () => {
      if (browserPlanned) metrics().planned_closes++;
      else metrics().unexpected_closes++;
    });
  });
  assert.equal((await browserPage.goto(host + '/app/load/')).status(), 200);
  await browserPage.waitForFunction(() => document.querySelector('#result')?.textContent.includes('"session"'), undefined, { timeout: 60_000 });
  const browserIdentity = JSON.parse(await browserPage.locator('#result').textContent()).session;
  report.browser = { session: browserIdentity, acknowledged_inputs: 0, failures: [] };
  browserDriving = true;
  browserTask = (async () => {
    let sequence = 0;
    while (browserDriving) {
      await delay(Math.min(cadence, 5000));
      if (!browserDriving) break;
      const started = performance.now();
      await browserPage.getByRole('button', { name: 'Compute', exact: true }).click();
      sequence++;
      await browserPage.waitForFunction(tick => {
        try { return JSON.parse(document.querySelector('#result').textContent).tick === tick; }
        catch { return false; }
      }, sequence, { timeout: 30_000 });
      const value = JSON.parse(await browserPage.locator('#result').textContent());
      assert.equal(value.session, browserIdentity, 'browser session identity survives');
      metrics().latencies_ms.push(performance.now() - started);
      report.browser.acknowledged_inputs++;
    }
  })();
  browserTask.catch(error => { report.browser.failures.push(String(error)); });
  phase = 'light'; cadence = 5000;
  clients.forEach(client => { client.next = Date.now() + (client.index < 20 ? client.index * 250 : client.index / sessions * 120_000); });
  event('light steady-state traffic');
  await delay(30_000);
  assert.ok(clients.every(client => client.established && !client.closed));
  const established = await api('/metrics');
  report.established_sessions = clients.filter(client => client.established && !client.closed).length + (browserIdentity ? 1 : 0);
  report.unique_sessions = new Set([...clients.map(client => client.identity), browserIdentity]).size;
  report.observed_active_connections = established.replicas.reduce((total, row) => total + row.sessions, 0);
  assert.equal(report.established_sessions, sessions, 'every client has a live, initialized Shiny session');
  assert.equal(report.unique_sessions, sessions, 'each connection owns an independent reactive session');
  // The API counts in-flight HTTP requests alongside WebSockets.
  assert.ok(report.observed_active_connections >= sessions, 'server accounts for every initialized session');
  if (profile === 'roomy') {
    phase = 'roomy-update'; cadence = 1000;
    clients.slice(0, 20).forEach(client => { client.next = Date.now() + client.index * 50; });
    event('validate a complete replacement with one-core requests and a half-core reserve');
    await delay(20_000);
    const before = await api();
    const originalPID = running(before)[0].pid;
    browserDriving = false; await browserTask;
    assert.deepEqual(report.browser.failures, []);
    // A code handoff intentionally expires old sessions at its drain deadline.
    clients.forEach(client => { client.planned = true; }); browserPlanned = true;
    await writeFile(join(work, 'input/version.txt'), 'v2\n');
    await docker('cp', join(work, 'input/version.txt'), `${container}:/input/version.txt`);
    await deploy('roomy-live-deploy');
    const next = new Client(sessions); clients.push(next); await next.connect();
    assert.equal(next.version, 'v2');
    const handoff = await reload('roomy-update-reload');
    const tick = next.tick; next.next = Date.now();
    await poll(() => !next.closed && !next.pending && next.tick > tick, 'v2 computes after preserving reload');
    const after = await api();
    assert.equal(after.app.status, 'running');
    assert.equal(after.compatibility_quarantined, false);
    assert.notEqual(running(after)[0].pid, originalPID);
    event('live code replacement and preserving reload succeeded within the larger budget', { result: handoff, replicas: running(after) });
  } else {
    phase = 'saturated'; cadence = 1000;
    clients.slice(0, 20).forEach(client => { client.next = Date.now() + client.index * 50; });
    event('saturate with reactive computation');
    await delay(20_000);
    const before = await api();
    const oldPID = running(before)[0].pid;
    const oldPythonPID = clients[0].processPID;
    await writeFile(join(work, 'input/version.txt'), 'v2\n');
    await docker('cp', join(work, 'input/version.txt'), `${container}:/input/version.txt`);
    await deploy('blocked-deploy', 'nonzero');
    const blocked = await api();
    assert.equal(blocked.update_status?.state, 'blocked_capacity', JSON.stringify(blocked));
    assert.equal(running(blocked)[0].pid, oldPID, 'refused update retains consumer');
    assert.ok(clients.every(client => !client.closed), 'refusal preserves every live session');
    event('code update refused safely', { update: blocked.update_status });
    const schedule = await api('/schedules', 'POST', { name: 'load-roll', cron_expr: '0 0 * * *',
      command: ['/usr/local/bin/python', '-c', 'from pathlib import Path; import os; Path(os.environ["SHINYHUB_APP_DATA"]).joinpath("producer.txt").write_text("complete")'],
      enabled: false, timeout_seconds: 60, overlap_policy: 'skip', missed_policy: 'skip',
      deploy_trigger: 'first_deploy', on_success: 'roll', min_roll_interval_seconds: 0, roll_fallback: 'defer' }, 201);
    const run = await api(`/schedules/${schedule.id}/run`, 'POST', {}, 202);
    let activation;
    await poll(async () => {
      const state = await api();
      activation = state.latest_schedule_activations?.find(row => row.schedule_run_id === run.run_id);
      return activation?.status === 'deferred_capacity';
    }, 'scheduled roll defers under pressure');
    event('scheduled roll deferred safely', { activation });
    // No old-generation drain is involved: all original sessions must survive.
    const progressBeforeReload = clients.slice(0, 20).map(client => client.acknowledgedTick);
    const browserProgressBeforeReload = report.browser.acknowledged_inputs;
    const result = await reload('saturated-reload');
    await delay(4000);
    await poll(() => clients.slice(0, 20).every((client, index) => client.acknowledgedTick > progressBeforeReload[index])
      && report.browser.acknowledged_inputs > browserProgressBeforeReload,
    'frequent protocol clients and real browser continue computing across reload', 30_000);
    assert.ok(clients.every(client => !client.closed), 'preserving reload retains every saturated session');
    assert.ok(browserSockets.length > 0 && browserSockets.every(socket => !socket.isClosed()), 'actual browser socket survives saturated reload');
    assert.equal(running(await api())[0].pid, oldPID, 'successor adopts the original native consumer');
    event('saturated reload preserved all sessions', { result });
    phase = 'recovery'; cadence = 120_000; event('release compute pressure; await automatic schedule retry');
    browserDriving = false;
    await browserTask;
    assert.deepEqual(report.browser.failures, []);
    assert.ok(report.browser.acknowledged_inputs > 0, 'real browser performed reactive computations');
    browserPlanned = true;
    await browserPage.close();
    // A preserving reload's retiring relay still owns the original sockets.
    // End those sessions deliberately before attempting a serving replacement.
    clients.forEach(client => client.close());
    await delay(5000);
    clients.length = 0;
    for (let offset = 0; offset < sessions; offset += 25) {
      const batch = Array.from({ length: Math.min(25, sessions - offset) }, (_, i) => new Client(offset + i));
      clients.push(...batch); await Promise.all(batch.map(client => client.connect()));
    }
    await poll(async () => {
      const state = await api();
      activation = state.latest_schedule_activations.find(row => row.id === activation.id);
      return activation.status === 'running' && running(state).some(row => row.pid !== oldPID);
    }, 'deferred roll retries and starts its healthy replacement automatically', 240_000);
    // A successful roll intentionally retires the old generation after the
    // configured drain deadline; keep that distinct from unexpected disconnects.
    clients.forEach(client => { client.planned = true; });
    let surgeSession;
    await poll(async () => {
      const candidate = new Client(sessions);
      clients.push(candidate); await candidate.connect();
      if (candidate.processPID === oldPythonPID) { candidate.close(); return false; }
      surgeSession = candidate; return true;
    }, 'new requests reach the rolled native process', 30_000);
    // The temporary surge also has a drain deadline. Holding its session keeps
    // activation running while the canonical replacement becomes ready.
    surgeSession.planned = true;
    let replacement;
    await poll(async () => {
      const state = await api();
      activation = state.latest_schedule_activations.find(row => row.id === activation.id);
      replacement = running(state).find(row => row.index === 0 && row.pid !== oldPID);
      return !!replacement && activation.status === 'running';
    }, 'canonical replacement becomes ready while the surge drains', 60_000);
    report.roll_reload_overlap = { activation_id: activation.id, status: activation.status,
      old_native_pid: oldPythonPID, temporary_surge_native_pid: surgeSession.processPID, canonical_pid: replacement.pid };
    assert.ok(replacement, 'a ready replacement exists before reload starts');
    const rollReload = await reload('reload-after-roll');
    await poll(async () => (await api()).latest_schedule_activations.find(row => row.id === activation.id).status === 'succeeded', 'roll finishes');
    const fresh = new Client(sessions + 1); clients.push(fresh); await fresh.connect();
    assert.equal(fresh.dataSnapshot, 'complete', 'adopted process serves the producer publication');
    const previousTick = fresh.tick;
    fresh.next = Date.now();
    await poll(() => !fresh.closed && !fresh.pending && fresh.tick > previousTick, 'adopted canonical replica serves a reactive session after roll-adjacent reload', 30_000);
    const rolled = await api();
    assert.equal(rolled.app.status, 'running'); assert.equal(rolled.compatibility_quarantined, false);
    assert.equal(rolled.update_status?.deployment_id, blocked.update_status.deployment_id,
      'a data roll does not hide the refused code upload');
    assert.notEqual(running(rolled)[0].pid, oldPID);
    assert.equal(running(rolled)[0].pid, replacement.pid, 'successor adopts the exact rolled handle');
    assert.equal(running(rolled)[0].endpoint_url, replacement.endpoint_url, 'rolled endpoint survives handoff');
    assert.equal(running(rolled)[0].data_generation, activation.target_generation, 'published data generation survives handoff');
    event('automatic retry and roll-adjacent reload succeeded', { result: rollReload, replicas: running(rolled), native_process_pid: fresh.processPID });
    clients.forEach(client => client.close());
    await delay(5000);
    phase = 'deploy-recovery';
    await deploy('retry-deploy');
    const updated = new Client(0); clients.push(updated); await updated.connect();
    assert.equal(updated.version, 'v2', 'manual retry publishes blocked code version');
    assert.equal((await api()).update_status, undefined, 'successful code retry clears the capacity notice');
    event('manual deployment retry publishes v2');
    await delay(10_000);
  }
  for (const [name, data] of Object.entries(report.phases)) {
    assert.deepEqual(data.page_errors, [], `${name}: continuous page probes`);
    assert.deepEqual(data.socket_errors, [], `${name}: socket errors`);
    assert.equal(data.unexpected_closes, 0, `${name}: unexpected disconnects`);
  }
  if (sampleFailure) throw sampleFailure;
  report.status = 'passed';
} catch (error) {
  report.error = String(error.stack || error); console.error(report.error); process.exitCode = 1;
} finally {
  browserDriving = false;
  browserPlanned = true;
  await browser?.close().catch(() => {});
  await browserTask?.catch(() => {});
  clients.forEach(client => client.close());
  probing = sampling = false;
  await Promise.allSettled([sampleTask, probeTask]);
  await docker('cp', `${container}:/state/server.log`, join(work, 'server.log')).catch(() => {});
  await command(['docker', 'inspect', container], 'container-inspect').catch(() => {});
  await docker('rm', '-f', container).catch(() => {});
  clearTimeout(timeout);
  for (const data of Object.values(report.phases)) {
    const summary = values => {
      values.sort((a, b) => a - b);
      const at = q => values.length ? +values[Math.min(values.length - 1, Math.floor(values.length * q))].toFixed(2) : null;
      return { count: values.length, p50: at(0.5), p95: at(0.95), p99: at(0.99), max: at(1) };
    };
    data.reactive_latency_ms = summary(data.latencies_ms); delete data.latencies_ms;
    data.page_latency_ms = summary(data.page_latencies_ms); delete data.page_latencies_ms;
  }
  for (const [name, data] of Object.entries(report.phases)) {
    const samples = report.samples.filter(sample => sample.phase === name);
    const memory = samples.map(sample => sample.raw.split('\n').filter(line => /^\d+$/.test(line)).map(Number));
    data.max_memory_current_mib = memory.length ? Math.max(...memory.map(row => row[0])) / 1024 / 1024 : null;
    data.cgroup_memory_peak_to_date_mib = memory.length ? Math.max(...memory.map(row => row[1])) / 1024 / 1024 : null;
    const readings = samples.map(sample => ({ time: sample.at_ms / 1000,
      usage: Number(/usage_usec (\d+)/.exec(sample.raw)?.[1]) / 1e6 }));
    const usage = readings.slice(1).map((sample, index) => (sample.usage - readings[index].usage) / (sample.time - readings[index].time)).filter(Number.isFinite);
    usage.sort((a, b) => a - b);
    data.measured_cpu_cores = { samples: usage.length,
      mean: usage.length ? usage.reduce((a, b) => a + b) / usage.length : null,
      p95: usage.length ? usage[Math.floor(usage.length * 0.95)] : null };
  }
  await writeFile(join(work, 'result.json'), JSON.stringify(report, null, 2));
  console.log(`Evidence: ${work}/result.json`);
}
