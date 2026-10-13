import assert from 'node:assert/strict';
import { access, readFile, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

// Real cron admission, native producer, scheduled roll, tableflip successor,
// and browser WebSocket. Files are checkpoints owned by the disposable fixture,
// not hooks in the server: the producer controls when its actual write finishes.
export async function checkReloadProducers({ context, page, appURL, state, work,
  binary, config, python, command, api, request, check, poll, output, sockets, report }) {
  const pidFile = join(state, 'hub.pid');
  const currentPID = async () => Number((await readFile(pidFile, 'utf8')).trim());
  const exists = path => access(path).then(() => true, () => false);
  const rollEntered = join(state, 'roll-producer-entered');
  const rollRelease = join(state, 'roll-producer-release');
  const writerEntered = join(state, 'cron-producer-entered');
  const writerRelease = join(state, 'cron-producer-release');
  const producer = (entered, release, exitCode = 0) => [python, '-c', `
import os, sys, time
from pathlib import Path
data = Path(os.environ['SHINYHUB_APP_DATA']) / 'reload-producer.txt'
data.write_text('write in progress')
Path(sys.argv[1]).touch()
while not Path(sys.argv[2]).exists():
    time.sleep(0.02)
data.write_text('${exitCode ? 'partial' : 'complete'}')
sys.exit(${exitCode})
`, entered, release];
  const schedule = (name, entered, release, onSuccess, exitCode = 0) => request('POST', '/api/apps/browser/schedules', {
    name, cron_expr: '* * * * *', command: producer(entered, release, exitCode), enabled: false,
    timeout_seconds: 180, overlap_policy: 'skip', missed_policy: 'skip',
    deploy_trigger: 'first_deploy', on_success: onSuccess, min_roll_interval_seconds: onSuccess === 'roll' ? 3600 : 0,
    roll_fallback: 'defer',
  }, 201);
  let probing = true, reloadDone = false, reloadTask, fresh;
  const failures = [];
  const probes = (async () => {
    while (probing) {
      try {
        const response = await context.request.get(appURL, { timeout: 5000 });
        if (response.status() !== 200) failures.push(`page status ${response.status()}`);
        await response.dispose();
      } catch (error) { failures.push(String(error)); }
      await delay(250);
    }
  })();
  try {
    await check('scheduled roll and admitted cron producer finish before preserving reload adopts the new replica', async () => {
      await request('PATCH', '/api/apps/browser', { min_warm_replicas: 1 });
      const before = await api('');
      const oldReplica = before.replicas_status.find(row => row.index === 0 && row.status === 'running');
      assert.ok(oldReplica?.pid, 'initial native consumer is running');
      const roll = await schedule('reload-roll', rollEntered, rollRelease, 'roll');
      const writer = await schedule('reload-writer', writerEntered, writerRelease, 'none');
      // Give both enabled producer policies valid prior provenance. Otherwise
      // the first roll is correctly refused before this regression can occur.
      await writeFile(writerRelease, 'prime');
      const primed = await request('POST', `/api/apps/browser/schedules/${writer.id}/run`, {}, 202);
      await poll(async () => (await api(`/schedules/${writer.id}/runs/${primed.run_id}`)).status === 'succeeded', 'writer policy has a successful publication');
      await rm(writerRelease);
      await rm(writerEntered);
      const started = await request('POST', `/api/apps/browser/schedules/${roll.id}/run`, {}, 202);
      await poll(() => exists(rollEntered), 'first producer reaches its real write checkpoint');

      // Release eight seconds before the next real cron minute. The fixture's
      // twenty-second drain keeps its old live WebSocket while cron admits the
      // second producer, which waits behind the roll's publication fence.
      let fireAt = Math.ceil(Date.now() / 60_000) * 60_000;
      if (fireAt - Date.now() < 15_000) fireAt += 60_000;
      while (Date.now() < fireAt - 12_000) await delay(Math.min(500, fireAt - 12_000 - Date.now()));
      await request('PATCH', `/api/apps/browser/schedules/${writer.id}`, { enabled: true });
      while (Date.now() < fireAt - 8000) await delay(Math.min(100, fireAt - 8000 - Date.now()));
      await writeFile(rollRelease, 'finish');
      let writerRun;
      await poll(async () => {
        const runs = await api(`/schedules/${writer.id}/runs`);
        writerRun = runs.items.find(run => run.trigger === 'schedule' && run.status === 'running');
        return !!writerRun;
      }, 'real cron admits the producer during the roll', 20_000);
      const inFlight = await api('');
      const activation = inFlight.latest_schedule_activations.find(item => item.schedule_run_id === started.run_id);
      assert.equal(activation?.status, 'running', 'cron writer overlaps the scheduled roll');
      assert.equal(await exists(writerEntered), false, 'producer execution waits behind the roll');
      await command([binary, 'reload', '--config', config, '--wait', '100ms'], 'producer-reload-timeout', { expectedExit: 'nonzero' });
      assert.match(await readFile(join(work, 'producer-reload-timeout.log'), 'utf8'), /reload wait timed out/);

      const previousPID = await currentPID();
      reloadTask = command([binary, 'reload', '--config', config, '--wait', '60s', '--output', 'json'], 'reload-after-schedule-roll')
        .then(text => { reloadDone = true; return JSON.parse(text); });
      // Attach a rejection handler immediately; the assertion below awaits the
      // original task and still reports a failing CLI exit.
      reloadTask.catch(() => {});
      await poll(() => exists(writerEntered), 'admitted cron producer starts after the roll finishes', 40_000);
      await delay(1500);
      const during = await api('');
      report.reload_producer = { previous_pid: previousPID, observed_pid: await currentPID(),
        cron_run_id: writerRun.id, roll_activation: activation.id, state_after_roll: during };
      assert.equal(await currentPID(), previousPID,
        `reload must wait for the writing producer: ${JSON.stringify({ status: during.app.status, quarantined: during.compatibility_quarantined, replicas: during.replicas_status })}`);
      assert.equal(reloadDone, false, 'reload has not acknowledged handoff while a producer writes');
      assert.match(await readFile(join(work, 'producer-reload-timeout.log'), 'utf8'), /browser.*reload-writer.*run \d+ in progress/);
      assert.equal(during.producer_repair_required, false, 'active writer has not created a failed producer repair obligation');
      const rolledReplica = during.replicas_status.find(row => row.index === 0 && row.status === 'running');
      assert.ok(rolledReplica?.pid, 'roll made a ready canonical replica');
      assert.notEqual(rolledReplica.pid, oldReplica.pid, 'a real process replacement occurred');
      assert.equal(during.latest_schedule_activations.find(item => item.id === activation.id)?.status, 'succeeded');
      fresh = await context.newPage();
      assert.equal((await fresh.goto(appURL)).status(), 200);
      const identity = await output(fresh, 'v1', 0);
      assert.ok(sockets.get(fresh)?.some(socket => !socket.isClosed()), 'new canonical replica has a live Shiny WebSocket');

      const paused = await request('POST', `/api/apps/browser/schedules/${writer.id}/run`, {}, 503);
      assert.match(paused.error, /retry after reload/);
      const waitingRuns = await api(`/schedules/${writer.id}/runs`);
      assert.equal(waitingRuns.items.length, 2, 'paused manual request created no additional run');

      await writeFile(writerRelease, 'finish');
      const result = await reloadTask;
      assert.equal(result.previous_pid, previousPID);
      assert.notEqual(result.pid, previousPID);
      assert.equal(result.pid, await currentPID(), 'CLI acknowledged the published successor');
      const after = await api('');
      assert.equal(after.app.status, 'running');
      assert.equal(after.compatibility_quarantined, false);
      const adopted = after.replicas_status.find(row => row.index === 0 && row.status === 'running');
      assert.equal(adopted?.pid, rolledReplica.pid, 'successor adopted the exact rolled process');
      assert.equal(adopted.endpoint_url, rolledReplica.endpoint_url, 'successor adopted its endpoint');
      assert.ok(rolledReplica.data_generation > 0, 'roll published a data generation');
      assert.equal(adopted.data_generation, rolledReplica.data_generation, 'successor adopted its generation');
      assert.equal(adopted.deployment_id, rolledReplica.deployment_id, 'successor adopted its deployment');
      process.kill(adopted.pid, 0);
      const terminal = await api(`/schedules/${writer.id}/runs/${writerRun.id}`);
      assert.equal(terminal.status, 'succeeded', 'reload did not interrupt the admitted producer');
      await request('PATCH', `/api/apps/browser/schedules/${writer.id}`, { enabled: false });
      // The fixture's reload drain deadline is two seconds. Reactive output
      // after that deadline proves real session preservation, not a short drain.
      await delay(3000);
      await fresh.getByRole('button', { name: 'Recalculate' }).click();
      await output(fresh, 'v1', 1, 1000, identity);
      assert.ok(sockets.get(fresh).every(socket => !socket.isClosed()), 'same WebSocket survives successor ownership');
      report.reload_producer = { ...report.reload_producer, successor_pid: result.pid,
        adopted_replica: adopted, producer_status: terminal.status, session: identity };
      await page.close();
    });
  } finally {
    await writeFile(rollRelease, 'cleanup').catch(() => {});
    await writeFile(writerRelease, 'cleanup').catch(() => {});
    if (reloadTask) {
      await reloadTask.catch(() => {});
      report.reload_producer = { ...report.reload_producer, state_after_handoff: await api('').catch(error => ({ error: String(error) })) };
    }
    await fresh?.close().catch(() => {});
    probing = false;
    await probes;
    report.reload_producer = { ...report.reload_producer, page_probe_failures: failures };
  }
  assert.deepEqual(failures, [], 'every app page probe succeeded during the schedule roll and reload');

  await check('acknowledged reload fails when a previously serving app has no ready successor replica', async () => {
    const entered = join(state, 'failed-producer-entered');
    const release = join(state, 'failed-producer-release');
    const failed = await schedule('failed-reload-writer', entered, release, 'none', 1);
    const started = await request('POST', `/api/apps/browser/schedules/${failed.id}/run`, {}, 202);
    await poll(() => exists(entered), 'failing producer has begun its physical write');
    const previousPID = await currentPID();
    const logName = 'reload-refuses-missing-successor-replica';
    const task = command([binary, 'reload', '--config', config, '--wait', '15s'], logName, { expectedExit: 'nonzero' });
    task.catch(() => {});
    try {
      // Public API rejection confirms the waiting reload closed admissions
      // before the fixture allows this deliberately failing writer to exit.
      await poll(async () => {
        try {
          await request('POST', `/api/apps/browser/schedules/${failed.id}/run`, {}, 503);
          return true;
        } catch (error) {
          if (error.status === 202) return false;
          throw error;
        }
      }, 'reload pauses new schedule admissions');
      await writeFile(release, 'fail the producer');
      await task;
      const diagnostic = await readFile(join(work, `${logName}.log`), 'utf8');
      assert.match(diagnostic, /browser: 0 ready replicas after reload, need 1/);
      assert.notEqual(await currentPID(), previousPID, 'a real handoff occurred before readiness failed');
      const after = await api('');
      assert.equal(after.app.status, 'failed');
      assert.equal(after.replicas_status.filter(row => row.status === 'running').length, 0);
      assert.equal(after.compatibility_quarantined, true, 'a genuinely failed physical write requires repair');
      assert.equal((await api(`/schedules/${failed.id}/runs/${started.run_id}`)).status, 'failed');
      report.failed_successor_reload = { previous_pid: previousPID, successor_pid: await currentPID(),
        diagnostic, app_status: after.app.status, ready_replicas: 0 };
    } finally {
      await writeFile(release, 'cleanup').catch(() => {});
      await task.catch(() => {});
    }
  });
}
