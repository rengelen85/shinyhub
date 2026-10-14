#!/usr/bin/env node
// Real Linux services and Docker workers; all acceptance checks use HTTP,
// initialized Shiny sessions and the runtime's external lifecycle boundary.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomBytes, createHash } from 'node:crypto';
import { cp, mkdir, mkdtemp, readFile, writeFile, rm, access } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import net from 'node:net';

assert.equal(process.platform, 'linux', 'run on an isolated Linux host');
const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const tools = resolve(process.env.MATRIX_TOOLS);
const require = createRequire(join(tools, 'driver/package.json'));
const WS = require('ws');
const { chromium } = require('playwright');
const binary = resolve(process.env.MATRIX_BINARY);
const combinations = (process.env.MATRIX_CASES || 'python:native:multiplex,python:docker:multiplex,r:native:multiplex,r:docker:multiplex,python:native:grouped,python:docker:grouped,r:native:grouped,r:docker:grouped').split(',');
const population = Number(process.env.MATRIX_SESSIONS || 16);
assert.ok(Number.isInteger(population) && population >= 2 && population <= 32);
const evidence = await mkdtemp(join(root, 'loadtest/results/runtime-matrix-'));
const report = { status: 'failed', started_at: new Date().toISOString(), commit: process.env.MATRIX_COMMIT, binary_sha256: createHash('sha256').update(await readFile(binary)).digest('hex'), cases: [] };
const cancellation = new AbortController();
let interrupted, cleaningUp = false;
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => {
  interrupted ||= new Error(`runtime matrix interrupted by ${signal}`);
  cancellation.abort(interrupted);
});
const availablePort = () => new Promise((resolve, reject) => {
  const socket = net.createServer(); socket.on('error', reject);
  socket.listen(0, '127.0.0.1', () => { const port = socket.address().port; socket.close(() => resolve(port)); });
});
async function command(args, file, expected = 0, env = {}) {
  const child = spawn(args[0], args.slice(1), { env: { ...process.env, ...env }, timeout: 180000, signal: cleaningUp ? undefined : cancellation.signal });
  let output = '';
  child.stdout.on('data', x => { output += x; }); child.stderr.on('data', x => { output += x; });
  const code = await new Promise((resolve, reject) => { child.on('error', reject); child.on('close', resolve); });
  if (file) await writeFile(file, output);
  if (expected === 'failure') assert.notEqual(code, 0, `unexpected success: ${args[0]}`);
  else assert.equal(code, expected, output.slice(-3000));
  return output;
}
async function poll(fn, description, milliseconds = 90000) {
  const until = Date.now() + milliseconds;
  while (Date.now() < until) { if (interrupted) throw interrupted; if (await fn()) return; await delay(100); }
  throw new Error(`timeout: ${description}`);
}
async function exists(path) { return access(path).then(() => true, () => false); }

try {
  for (const combination of combinations) {
    if (interrupted) throw interrupted;
    const [language, runtime, isolation] = combination.split(':');
    assert.ok(['python', 'r'].includes(language) && ['native', 'docker'].includes(runtime) && ['multiplex', 'grouped'].includes(isolation));
    const preserving = runtime === 'native' && isolation === 'multiplex';
    const id = `matrix-${randomBytes(6).toString('hex')}`;
    const unit = `shinyhub-${id}`;
    const work = join(evidence, combination.replaceAll(':', '-'));
    await mkdir(work);
    const input = join(work, 'input'), data = join(work, 'data', id);
    await cp(join(root, 'loadtest/production-runtime', language), input, { recursive: true });
    // Both languages read the same 16 MiB binary dataset, not a lazy range.
    const dataset = Buffer.alloc(2000000 * 8);
    for (let i = 0; i < 2000000; i++) dataset.writeDoubleLE(i + 1, i * 8);
    await writeFile(join(work, 'dataset.bin'), dataset);
    await writeFile(join(input, 'shinyhub.toml'), `[app]\nreplicas = 1\nmin_warm_replicas = 1\nmax_sessions_per_replica = 64\nstartup_timeout_seconds = 15\nmemory_limit_mb = 512\ncpu_quota_percent = 100\n[app.worker]\nisolation = "${isolation}"\ngrouped_size = 64\nmax_workers = ${isolation === 'grouped' ? 1 : 2}\n${isolation === 'grouped' ? 'warm_spares = 1\n' : ''}`);
    const port = await availablePort(), chromePort = await availablePort();
    const host = `http://127.0.0.1:${port}`, url = `${host}/app/${id}/`;
    const password = randomBytes(24).toString('hex');
    const config = join(work, 'config.yaml');
    const imagePython = process.env.MATRIX_PYTHON_IMAGE || 'shinyhub-update-load:local';
    const imageR = process.env.MATRIX_R_IMAGE || 'rocker/shiny:4.5.2';
    await writeFile(config, `server:\n  host: 127.0.0.1\n  port: ${port}\n  pid_file: ${work}/hub.pid\n  reload_sessions: preserve\n  drain_timeout: 12s\n  shutdown_apps: stop\n  render_capacity_cores: 4\n  update_capacity:\n    worker_cpu_request_cores: 1\n    reserved_cpu_cores: 0.5\nauth:\n  secret: ${randomBytes(32).toString('hex')}\ndatabase:\n  dsn: ${work}/hub.db\nstorage:\n  apps_dir: ${work}/apps\n  app_data_dir: ${work}/data\nruntime:\n  mode: ${runtime}\n  docker:\n    default_memory_mb: 512\n    default_cpu_percent: 100\n    images:\n      python: ${imagePython}\n      r: ${imageR}\n`, { mode: 0o600 });
    const result = { language, runtime, isolation, reload_policy: preserving ? 'preserve' : runtime === 'native' ? 'preserve-refused-then-drain' : 'drain', capabilities: { live_code_handoff: runtime === 'native', preserving_reload: preserving, data_producer_roll: runtime === 'native' }, slug: id, status: 'failed', population, checks: [], page_errors: [], successful_page_probes: 0, planned_lifecycle_page_errors: [], session_errors: [], latencies_ms: [], planned_closes: 0, unexpected_closes: 0 };
    report.cases.push(result);
    const environment = runtime === 'docker' ? {} : { PATH: `${tools}/venv/bin:${tools}/bin:${process.env.PATH}` };
    const pw = join(work, 'password'); await writeFile(pw, password, { mode: 0o600 });
    let browser, context, probing = false, probe, token, plannedInterruption = false, browserPlanned = false;
    const chromeName = `shinyhub-chrome-${id}`;
    const clients = [];
    const api = async (path = '', method = 'GET', body, expected = 200) => {
      const response = await fetch(`${host}/api/apps/${id}${path}`, { method, headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', 'User-Agent': 'shinyhub-runtime-matrix' }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(30000) });
      const value = response.status === 204 ? null : await response.json(); assert.equal(response.status, expected, JSON.stringify(value)); return value;
    };
    const deploy = (name, expected = 0) => command([binary, 'deploy', input, '--slug', id, '--config', join(work, 'client.json'), '--output', 'json'], join(work, `${name}.log`), expected, { SHINYHUB_HOST: host, SHINYHUB_TOKEN: token });
    const reload = name => command([binary, 'reload', '--config', config, '--wait', '90s', '--output', 'json'], join(work, `${name}.log`));
    const check = name => { result.checks.push(name); console.log(`${combination}: ${name}`); };
    class Client {
      constructor(index) { this.index = index; this.tick = 0; this.multiplier = 3; this.closed = false; this.planned = false; }
      async connect(version) {
        const response = await fetch(url, { signal: AbortSignal.timeout(30000) }); assert.equal(response.status, 200); await response.arrayBuffer();
        const cookie = response.headers.getSetCookie().map(x => x.split(';')[0]).join('; ');
        this.socket = new WS(url.replace('http:', 'ws:') + 'websocket/', { perMessageDeflate: false, handshakeTimeout: 15000, headers: { Cookie: cookie, 'User-Agent': 'shinyhub-runtime-matrix' } });
        this.socket.on('error', e => { result.session_errors.push(String(e)); });
        this.socket.on('close', () => { this.closed = true; if (this.planned) result.planned_closes++; else result.unexpected_closes++; });
        this.socket.on('message', bytes => {
          try {
            const message = JSON.parse(bytes); if (!message.values?.result) return;
            const value = JSON.parse(message.values.result);
            assert.equal(value.calculation, 15000150000, 'dataset calculation matches known sum for selection 3');
            assert.equal(value.multiplier, 3, 'selected filter remains applied');
            assert.equal(value.dataset_checksum, 2000001000000, 'complete dataset checksum remains correct');
            assert.equal(value.dataset_bytes, 16000000, 'large dataset was loaded');
            assert.equal(value.memory_bytes, 262144, 'private session memory allocated');
            assert.equal(value.version, version);
            if (this.identity) assert.equal(value.session, this.identity, 'live session state preserved');
            this.identity = value.session; this.value = value;
            if (this.waiter && value.tick === this.tick) { result.latencies_ms.push(performance.now() - this.sent); this.waiter.resolve(); this.waiter = undefined; }
          } catch (e) { this.failure = e; this.waiter?.reject(e); this.waiter = undefined; }
        });
        await new Promise((resolve, reject) => { this.socket.once('open', resolve); this.socket.once('error', reject); });
        await this.compute(true);
      }
      async compute(initial = false) {
        assert.ok(!this.closed, 'session remains open'); if (this.failure) throw this.failure;
        if (!initial) this.tick++;
        this.sent = performance.now();
        const promise = new Promise((resolve, reject) => { this.waiter = { resolve, reject }; });
        this.socket.send(JSON.stringify({ method: initial ? 'init' : 'update', data: { tick: this.tick, multiplier: this.multiplier, work_ms: 5, '.clientdata_output_result_hidden': false, '.clientdata_url_protocol': 'http:', '.clientdata_url_hostname': '127.0.0.1', '.clientdata_url_pathname': `/app/${id}/` } }));
        const timer = setTimeout(() => { this.waiter?.reject(new Error('reactive computation timed out')); }, 15000);
        try { await promise; } finally { clearTimeout(timer); }
      }
      close() { this.planned = true; this.socket?.close(); }
    }
    try {
      await command([binary, 'init', '--config', config, '--admin-user', 'matrix-admin', '--admin-password-file', pw], join(work, 'init.log'));
      if (runtime === 'docker') {
        const rejection = await command([binary, 'serve', '--config', config, '--no-browser'], join(work, 'unsupported-preserve.log'), 'failure');
        assert.match(rejection, /session-preserving reloads require single-host native runtime/);
        check('Docker explicitly rejects unsupported session-preserving reloads');
        const text = await readFile(config, 'utf8');
        await writeFile(config, text.replace('reload_sessions: preserve', 'reload_sessions: drain\n  reload_drain_timeout: 2s'), { mode: 0o600 });
      }
      await command(['sudo', '-n', 'systemd-run', '--quiet', `--unit=${unit}`, '--property=Type=notify', '--property=NotifyAccess=all', '--property=Restart=on-failure', '--property=RestartSec=5s', '--property=RestartPreventExitStatus=7', `--property=User=${process.getuid()}`, '--property=Delegate=cpu memory pids', '--property=CPUQuota=400%', '--property=MemoryMax=3G', '--property=NoNewPrivileges=true', '--property=ProtectSystem=full', '--property=PrivateTmp=true', `--property=BindPaths=${dirname(tools)} ${work}`, '--property=LimitNOFILE=65536', '--', '/usr/bin/env', ...Object.entries(environment).map(([k,v]) => `${k}=${v}`), binary, 'serve', '--supervise', '--config', config, '--no-browser'], join(work, 'service-start.log'));
      await poll(() => fetch(`${host}/activez`).then(r => r.ok).catch(() => false), 'systemd service ready');
      const supervisorPID = Number((await command(['systemctl', 'show', unit, '--property=MainPID', '--value'])).trim());
      const serverPID = Number((await readFile(join(work, 'hub.pid'), 'utf8')).trim());
      assert.ok(supervisorPID > 0 && serverPID > 0);
      assert.notEqual(supervisorPID, serverPID, 'systemd tracks a stable supervisor separately from the active controller');
      result.supervisor_pid = supervisorPID;
      const response = await fetch(`${host}/api/auth/login`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: 'matrix-admin', password }) }); assert.equal(response.status, 200); ({ token } = await response.json());
      const created = await fetch(`${host}/api/apps`, { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', 'User-Agent': 'shinyhub-runtime-matrix' }, body: JSON.stringify({ slug: id, name: id, access: 'public' }) });
      assert.equal(created.status, 201, await created.text());
      await command([binary, 'data', 'push', id, join(work, 'dataset.bin'), '--config', join(work, 'client.json')], join(work, 'data-seed.log'), 0, { SHINYHUB_HOST: host, SHINYHUB_TOKEN: token });
      await deploy('v1-deploy');
      await poll(async () => (await api()).replicas_status?.some(row => row.status === 'running'), 'initial warm worker starts');
      const readinessCookies = new Map();
      const readyPage = async () => {
        const response = await fetch(url, { signal: AbortSignal.timeout(15000), headers: { 'User-Agent': 'shinyhub-runtime-matrix', Cookie: [...readinessCookies].map(([k,v]) => `${k}=${v}`).join('; ') } });
        for (const cookie of response.headers.getSetCookie()) { const pair = cookie.split(';')[0]; const split = pair.indexOf('='); readinessCookies.set(pair.slice(0, split), pair.slice(split + 1)); }
        const text = await response.text(); return response.ok && text.includes('id="tick"');
      };
      await poll(readyPage, 'initial app page becomes ready');
      const first = new Client(0); clients.push(first); await first.connect('v1');
      check('large dataset and selected calculation initialized correctly');
      const policy = await api();
      assert.equal(policy.app.worker_isolation, isolation, 'effective worker policy matches the matrix label');
      const rows = policy.replicas_status.filter(r => r.status === 'running');
      assert.ok(rows.length > 0);
      if (runtime === 'native') {
          const membership = (await readFile(`/proc/${first.value.process_pid}/cgroup`, 'utf8')).trim().split('\n').find(x => x.startsWith('0::')).slice(3);
          const directory = join('/sys/fs/cgroup', membership);
          result.worker_limits = { cpu: (await readFile(join(directory, 'cpu.max'), 'utf8')).trim(), memory: (await readFile(join(directory, 'memory.max'), 'utf8')).trim(), pids: (await readFile(join(directory, 'pids.max'), 'utf8')).trim() };
      } else {
          // Grouped apps expose elastic status rows, which need not include a
        // container ID. Inspect the actual sole worker through Docker's API.
        const containerID = rows[0].worker_id || (await command(['docker', 'ps', '-q', '--filter', `label=shinyhub.slug=${id}`])).trim();
        assert.ok(containerID && !containerID.includes('\n'), 'exactly one live Docker worker is identified');
          const details = JSON.parse(await command(['docker', 'inspect', containerID]))[0];
          assert.equal(details.Config.Labels['shinyhub.slug'], id);
        assert.equal(details.State.Running, true);
        result.worker_container = { id: details.Id, image: details.Image, network: details.HostConfig.NetworkMode, memory: details.HostConfig.Memory, nano_cpus: details.HostConfig.NanoCpus };
          const limits = (await command(['docker', 'exec', containerID, 'sh', '-c', 'cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/memory.max /sys/fs/cgroup/pids.max'])).trim().split('\n');
          result.worker_limits = { cpu: limits[0], memory: limits[1], pids: limits[2] };
      }
      const [quota, period] = result.worker_limits.cpu.split(' ').map(Number);
      assert.equal(quota / period, 1, 'kernel enforces one core per worker');
      assert.equal(Number(result.worker_limits.memory), 536870912, 'kernel enforces 512 MiB per worker');
      check('actual worker CPU and memory limits are enforced');
      for (let i = 1; i < population - 1; i++) { const client = new Client(i); clients.push(client); await client.connect('v1'); }
      await command(['docker', 'run', '-d', '--init', '--name', chromeName, '--network', 'host', '--memory=768m', '--cpus=1', 'shinyhub-update-load-driver:local', 'chromium', '--headless', '--no-sandbox', '--disable-dev-shm-usage', `--remote-debugging-port=${chromePort}`]);
      await poll(() => fetch(`http://127.0.0.1:${chromePort}/json/version`).then(r => r.ok).catch(() => false), 'browser starts');
      browser = await chromium.connectOverCDP(`http://127.0.0.1:${chromePort}`);
      context = await browser.newContext({ userAgent: 'shinyhub-runtime-matrix' });
      let page = await context.newPage();
      const browserSockets = [];
      const trackBrowserSocket = socket => {
        browserSockets.push(socket);
        socket.on('close', () => { if (browserPlanned) result.planned_closes++; else result.unexpected_closes++; });
        socket.on('socketerror', error => result.session_errors.push(String(error)));
      };
      page.on('websocket', trackBrowserSocket);
      assert.equal((await page.goto(url)).status(), 200);
      await page.waitForFunction(() => document.querySelector('#result')?.textContent.includes('"session"'));
      await page.getByLabel('Applied filter', { exact: true }).fill('3');
      let browserTick = 0, browserIdentity;
      const browserCompute = async version => {
        await page.getByRole('button', { name: 'Compute', exact: true }).click(); browserTick++;
        await page.waitForFunction(tick => { try { return JSON.parse(document.querySelector('#result').textContent).tick === tick; } catch { return false; } }, browserTick, { timeout: 15000 });
        const value = JSON.parse(await page.locator('#result').textContent());
        assert.equal(value.calculation, 15000150000); assert.equal(value.multiplier, 3); assert.equal(value.version, version);
        if (browserIdentity) assert.equal(value.session, browserIdentity);
        browserIdentity = value.session;
      };
      await browserCompute('v1');
      assert.equal(new Set([...clients.map(c => c.identity), browserIdentity]).size, population);
      result.established_sessions = population; result.browser = browser.version();
      const probeCookies = new Map();
      probing = true;
      probe = (async () => { while (probing) {
        try { const response = await fetch(url, { signal: AbortSignal.timeout(15000), headers: { 'User-Agent': 'shinyhub-runtime-matrix', Cookie: [...probeCookies].map(([k,v]) => `${k}=${v}`).join('; ') } });
          for (const cookie of response.headers.getSetCookie()) { const pair = cookie.split(';')[0]; const split = pair.indexOf('='); probeCookies.set(pair.slice(0, split), pair.slice(split + 1)); }
          const text = await response.text(); if (response.status !== 200 || !text.includes('id="tick"')) (plannedInterruption ? result.planned_lifecycle_page_errors : result.page_errors).push({ status: response.status, fallback: !text.includes('id="tick"') });
          else result.successful_page_probes++; }
        catch (e) { (plannedInterruption ? result.planned_lifecycle_page_errors : result.page_errors).push(String(e)); }
        await delay(200);
      } })();
      await Promise.all(clients.map(c => c.compute()));
      check('independent protocol sessions and a real browser preserve selected state');
      await writeFile(join(input, 'fail-startup'), 'deliberate failure\n');
      const failedDeploy = await deploy('failed-candidate', 'failure');
      if (runtime === 'docker') assert.match(failedDeploy, /handoff supports native (replicas|workers) only/);
      else assert.match(failedDeploy, /startup|readiness/, 'failure must reach the candidate startup check');
      await Promise.all(clients.map(c => c.compute())); await browserCompute('v1');
      assert.equal((await api()).released_version, policy.released_version, 'failed candidate never becomes authoritative');
      assert.ok(browserSockets.every(s => !s.isClosed()));
      await rm(join(input, 'fail-startup'));
      check(runtime === 'native' ? 'failed candidate preserves the serving generation and all sessions' : 'unsupported live code handoff is refused while all sessions keep working');
      const released = runtime === 'native' ? 'v2' : 'v1';
      if (runtime === 'native') {
        // Gate real candidate startup so reload overlaps a known transition,
        // rather than relying on a lucky sleep or internal controller hooks.
        await writeFile(join(input, 'version.txt'), 'v2\n');
        await writeFile(join(data, 'gate-v2'), 'hold readiness\n');
        const updating = deploy('v2-live-deploy'); updating.catch(() => {});
        await poll(() => exists(join(data, 'starting-v2')), 'candidate reaches its startup gate', 15000);
        const updateReload = preserving ? reload('live-update-reload') : Promise.resolve(); updateReload.catch(() => {});
        await Promise.all(clients.map(c => c.compute())); await browserCompute('v1');
        assert.ok(browserSockets.every(s => !s.isClosed()));
        await rm(join(data, 'gate-v2'));
        await updating;
        await Promise.all(clients.map(c => c.compute())); await browserCompute('v1');
        // Only after publication and a successful old-session calculation do
        // we permit the configured drain deadline to close those old sessions.
        clients.forEach(c => { c.planned = true; }); browserPlanned = true;
        const v2 = new Client(population); clients.push(v2); await v2.connect('v2');
        await updateReload;
        await v2.compute();
        assert.equal((await api()).compatibility_quarantined, false);
        check(preserving ? 'gated live deployment and overlapping reload retain serving availability' : 'gated grouped live deployment retains serving availability');
      }
      const resetPopulation = async () => {
        for (const client of clients) if (client.failure) throw client.failure;
        clients.forEach(c => c.close());
        browserPlanned = true; await page.close(); await delay(5000);
        clients.length = 0; browserSockets.length = 0; browserIdentity = undefined; browserTick = 0;
        // This population represents new visitors, not an old generation's
        // reconnect cookie, which remains valid until that generation drains.
        await context.clearCookies();
        browserPlanned = false; page = await context.newPage(); page.on('websocket', trackBrowserSocket);
        for (let i = 0; i < population - 1; i++) { const client = new Client(i); clients.push(client); await client.connect(released); }
        assert.equal((await page.goto(url)).status(), 200);
        await page.waitForFunction(() => document.querySelector('#result')?.textContent.includes('"session"'));
        await page.getByLabel('Applied filter', { exact: true }).fill('3'); await browserCompute(released);
        assert.equal(new Set([...clients.map(c => c.identity), browserIdentity]).size, population);
      };
      await resetPopulation();
      await rm(join(data, `starting-${released}`), { force: true });
      await writeFile(join(data, `gate-${released}`), 'hold scheduled replacement\n');
      const producerCommand = language === 'r'
        ? ['Rscript', '-e', 'd <- Sys.getenv("SHINYHUB_APP_DATA"); writeLines("complete", file.path(d,"producer.tmp")); stopifnot(file.rename(file.path(d,"producer.tmp"),file.path(d,"producer.txt")))']
        : ['python', '-c', 'import os; from pathlib import Path; d=Path(os.environ["SHINYHUB_APP_DATA"]); (d/"producer.tmp").write_text("complete"); os.replace(d/"producer.tmp",d/"producer.txt")'];
      const scheduleRequest = { name: 'data-refresh', cron_expr: '0 0 * * *', command: producerCommand,
        enabled: false, timeout_seconds: 30, overlap_policy: 'skip', missed_policy: 'skip', deploy_trigger: 'first_deploy',
        on_success: 'roll', min_roll_interval_seconds: 0, roll_fallback: 'defer' };
      if (runtime === 'docker') {
        const refused = await api('/schedules', 'POST', scheduleRequest, 422);
        assert.match(refused.error, /data-producing schedules need a local native tier|grouped parallel handoff supports native workers only/);
        await Promise.all(clients.map(c => c.compute())); await browserCompute(released);
        await rm(join(data, `gate-${released}`));
        check('unsupported Docker data producer is refused without disrupting serving sessions');
      } else {
        const schedule = await api('/schedules', 'POST', scheduleRequest, 201);
        const run = await api(`/schedules/${schedule.id}/run`, 'POST', {}, 202);
        await poll(() => exists(join(data, `starting-${released}`)), 'scheduled replacement reaches its startup gate within the available surge budget', 15000);
        const rollReload = preserving ? reload('producer-roll-reload') : Promise.resolve(); rollReload.catch(() => {});
        await Promise.all(clients.map(c => c.compute())); await browserCompute(released);
        assert.ok(clients.every(c => c.value.data_snapshot === 'initial'), 'serving consumers retain their last complete data snapshot');
        await rm(join(data, `gate-${released}`));
        await poll(async () => {
          const candidate = new Client(population + 1); clients.push(candidate); await candidate.connect(released);
          if (candidate.value.data_snapshot.trim() !== 'complete') { candidate.close(); return false; }
          return true;
        }, 'new consumer serves the completed producer snapshot');
        clients.forEach(c => { c.planned = true; }); browserPlanned = true;
        await rollReload;
        await poll(async () => (await api()).latest_schedule_activations.find(row => row.schedule_run_id === run.run_id)?.status === 'succeeded', 'producer activation succeeds');
        const state = await api();
        assert.equal(state.compatibility_quarantined, false);
        const activation = state.latest_schedule_activations.find(row => row.schedule_run_id === run.run_id);
        assert.equal(activation.status, 'succeeded');
        await resetPopulation();
        assert.ok(clients.every(c => c.value.data_snapshot.trim() === 'complete'));
        check(preserving ? 'real producer roll and roll-adjacent reload publish complete data and recover serving workers' : 'real grouped producer roll publishes complete data and recovers serving workers');
      }
      if (runtime === 'native' && !preserving) {
        const refused = await command([binary, 'reload', '--config', config, '--wait', '90s', '--output', 'json'], join(work, 'unsupported-elastic-preserve.log'), 'failure');
        assert.match(refused, /elastic pool/);
        await Promise.all(clients.map(c => c.compute())); await browserCompute(released);
        assert.ok(browserSockets.every(s => !s.isClosed()));
        check('grouped preserving reload is explicitly refused while all sessions keep working');
        const text = await readFile(config, 'utf8');
        await writeFile(config, text.replace('reload_sessions: preserve', 'reload_sessions: drain\n  reload_drain_timeout: 2s'), { mode: 0o600 });
      } else {
        if (!preserving) { clients.forEach(c => { c.planned = true; }); browserPlanned = true; plannedInterruption = true; }
        await reload('steady-reload');
        if (!preserving) { await poll(readyPage, 'draining reload restores a ready app page'); await resetPopulation(); plannedInterruption = false; }
        await Promise.all(clients.map(c => c.compute())); await browserCompute(released);
        assert.ok(browserSockets.every(s => !s.isClosed()));
        assert.equal((await api()).compatibility_quarantined, false);
        check(preserving ? 'preserving reload retains every session and its applied calculation' : 'draining reload recovers the complete population with correct calculations');
      }
      if (preserving) {
        const state = JSON.parse(await command([binary, 'status', '--config', config, '--output', 'json'], join(work, 'before-controller-crash.log')));
        assert.equal(Number((await command(['systemctl', 'show', unit, '--property=MainPID', '--value'])).trim()), supervisorPID,
          'every handoff keeps systemd tracking the original supervisor');
        assert.notEqual(state.pid, supervisorPID);
        assert.ok(state.retired.count > 0 && state.retired.sessions > 0,
          'controller crash must occur while a retired generation retains real sessions');
        result.retired_before_controller_crash = state.retired;
        browserPlanned = true; clients.forEach(c => { c.planned = true; }); plannedInterruption = true;
        await command(['kill', '-KILL', String(state.pid)], join(work, 'controller-crash.log'));
        await poll(async () => {
          const current = Number((await command(['systemctl', 'show', unit, '--property=MainPID', '--value'])).trim());
          return current > 0 && current !== supervisorPID
            && await fetch(`${host}/activez`).then(r => r.ok).catch(() => false);
        }, 'systemd restarts the crashed active controller despite a live retired relay');
        await poll(async () => (await api()).replicas_status?.some(row => row.status === 'running'),
          'controller crash recovers a warm worker without a demand request');
        await resetPopulation(); plannedInterruption = false;
        await Promise.all(clients.map(c => c.compute())); await browserCompute(released);
        assert.equal((await api()).compatibility_quarantined, false);
        result.restarted_supervisor_pid = Number((await command(['systemctl', 'show', unit, '--property=MainPID', '--value'])).trim());
        check('systemd recovers an active-controller crash while the retired relay remains alive');
      }
      // A complete service restart deliberately ends sessions. Require warm
      // recovery before any page request can trigger an on-demand spawn.
      browserPlanned = true; clients.forEach(c => c.close()); await page.close();
      plannedInterruption = true;
      await command(['sudo', '-n', 'systemctl', 'restart', unit], join(work, 'service-restart.log'));
      await poll(() => fetch(`${host}/activez`).then(r => r.ok).catch(() => false), 'restarted service ready');
      await poll(async () => (await api()).replicas_status?.some(row => row.status === 'running'), 'warm worker recovers without a demand request');
      await poll(readyPage, 'restarted warm worker accepts page loads');
      await resetPopulation(); plannedInterruption = false;
      await Promise.all(clients.map(c => c.compute())); await browserCompute(released);
      assert.equal((await api()).compatibility_quarantined, false);
      check('full systemd restart restores a ready warm worker without demand');
      if (runtime === 'native' && !preserving) {
        clients.forEach(c => { c.planned = true; }); browserPlanned = true; plannedInterruption = true;
        await reload('steady-draining-reload'); await poll(readyPage, 'grouped draining reload restores a ready app page'); await resetPopulation(); plannedInterruption = false;
        await Promise.all(clients.map(c => c.compute())); await browserCompute(released);
        assert.equal((await api()).compatibility_quarantined, false);
        check('grouped draining reload recovers the complete population with correct calculations');
      }
      probing = false; await probe;
      for (const client of clients) if (client.failure) throw client.failure;
      assert.ok(result.successful_page_probes >= 5, 'availability probe must actually serve successful app pages');
      assert.deepEqual(result.page_errors, []); assert.deepEqual(result.session_errors, []);
      assert.equal(result.unexpected_closes, 0);
      result.latencies_ms.sort((a,b) => a-b);
      result.reactive_p95_ms = result.latencies_ms[Math.floor(result.latencies_ms.length * 0.95)];
      result.latency_budget_ms = Number(process.env.MATRIX_LATENCY_BUDGET_MS || 3000);
      assert.ok(result.reactive_p95_ms <= result.latency_budget_ms, 'fixture reactive latency stays within its declared budget');
      result.status = 'passed';
    } catch (e) { result.error = String(e.stack || e); throw e; }
    finally {
      cleaningUp = true;
      await Promise.all(['gate-v1', 'gate-v2'].map(name => rm(join(data, name), { force: true }).catch(() => {})));
      const workerIDs = (await command(['docker', 'ps', '-aq', '--filter', `label=shinyhub.slug=${id}`]).catch(() => '')).trim().split(/\s+/).filter(Boolean);
      if (workerIDs.length) await command(['docker', 'inspect', ...workerIDs], join(work, 'worker-final-state.json')).catch(() => {});
      for (const workerID of workerIDs) await command(['docker', 'logs', '--tail', '200', workerID], join(work, `worker-${workerID}.log`)).catch(() => {});
      browserPlanned = true; clients.forEach(c => c.close()); await browser?.close();
      await command(['docker', 'rm', '-f', chromeName]).catch(() => {});
      probing = false; await probe;
      await command(['sudo', '-n', 'journalctl', '--no-pager', '-u', unit], join(work, 'server.log')).catch(() => {});
      await command(['sudo', '-n', 'systemctl', 'stop', unit]).catch(() => {});
      // Restrict cleanup to this run's unpredictable app slug, never all managed apps.
      const ids = (await command(['docker', 'ps', '-aq', '--filter', `label=shinyhub.slug=${id}`]).catch(() => '')).trim().split(/\s+/).filter(Boolean);
      for (const owned of ids) await command(['docker', 'rm', '-f', owned]).catch(() => {});
      await command(['sudo', '-n', 'systemctl', 'reset-failed', unit]).catch(() => {});
      cleaningUp = false;
    }
  }
  report.status = 'passed';
} catch (e) { report.error = String(e.stack || e); process.exitCode = 1; }
finally { report.finished_at = new Date().toISOString(); await writeFile(join(evidence, 'result.json'), JSON.stringify(report, null, 2)); console.log(`Evidence: ${evidence}/result.json`); }
