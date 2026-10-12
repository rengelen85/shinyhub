import assert from 'node:assert/strict';
import { readFile, readdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

// Runs inside the real lifecycle harness with a disposable native app and
// extension-free browser. All sessions carry visible random server identities.
export async function checkSessionRelays({ context, page, original, appURL, host, state, app, work,
  api, request, deploy, check, poll, output, assertDisconnected, sockets, viewerID }) {
  const pidFile = join(state, 'hub.pid');
  const currentPID = async () => Number((await readFile(pidFile, 'utf8')).trim());
  const ready = async () => fetch(host + '/activez', { signal: AbortSignal.timeout(2000),
    headers: { 'User-Agent': 'shinyhub-session-relay-test' } }).then(r => r.ok).catch(() => false);
  const reload = async () => {
    const previous = await currentPID();
    process.kill(previous, 'SIGHUP');
    await poll(async () => (await currentPID()) !== previous && await ready(), 'successor owns control plane', 30_000);
  };
  let probing = true;
  const failures = [];
  const probes = (async () => {
    while (probing) {
      try {
        const response = await context.request.get(appURL, { timeout: 5000 });
        if (response.status() !== 200) failures.push(`page status ${response.status()}`);
        await response.dispose();
      } catch (error) { failures.push(String(error)); }
      await delay(200);
    }
  })();
  const originalPID=await currentPID();
  let second, third, secondIdentity;
  try {
    await check('two reloads preserve original Shiny session past the reload deadline', async () => {
      await reload();
      await delay(6000);
      await page.getByRole('button', { name: 'Recalculate' }).click();
      await output(page, 'v1', 3, 500, original);
      second = await context.newPage();
      await second.goto(appURL);
      const identity = await output(second, 'v1', 0);
      secondIdentity=identity;
      await reload();
      await delay(6000);
      await page.getByRole('button', { name: 'Recalculate' }).click();
      await output(page, 'v1', 4, 500, original);
      await second.getByRole('button', { name: 'Recalculate' }).click();
      await output(second, 'v1', 1, 1000, identity);
      assert.ok(sockets.get(page).every(socket => !socket.isClosed()), 'original websocket remains open');
    });
    await check('relay limit refuses an upgrade without closing a working session', async () => {
      third = await context.newPage();
      await third.goto(appURL);
      await output(third, 'v1', 0);
      const previous = await currentPID();
      process.kill(previous, 'SIGHUP');
      await poll(async () => (await readFile(join(work, 'server.log'), 'utf8')).includes('reload relay limit reached'), 'explicit relay-limit refusal');
      assert.equal(await currentPID(), previous);
      await page.getByRole('button', { name: 'Recalculate' }).click();
      await output(page, 'v1', 5, 500, original);
      await page.close();
      await poll(async () => {
        // The first session lease retires after its only tab closes; retry before a
        // new child is created, then wait for takeover before another signal.
        process.kill(await currentPID(), 'SIGHUP');
        await delay(500);
        return await currentPID() !== previous;
      }, 'relay capacity reclaimed', 15_000);
      await poll(ready, 'third successor ready');
      await output(second, 'v1', 1);
      await output(third, 'v1', 0);
      // First relay's sessions are gone; its native stdout reader must stay
      // alive until the adopted backend exits. Reactive work must still log.
      process.kill(originalPID, 0);
      await second.getByRole('button', { name: 'Recalculate' }).click();
      await output(second, 'v1', 2, 1000, secondIdentity);
      await poll(async () => {
        const dir=join(state, 'apps/browser/logs');
        const texts=await Promise.all((await readdir(dir)).filter(name=>name.endsWith('.log')).map(name=>readFile(join(dir,name),'utf8')));
        return texts.some(text=>text.includes(`RELAYLOG:${secondIdentity}:2`));
      }, 'native log output survives session-relay retirement');
    });
    await check('deployment after repeated reloads protects relayed backend until app deadline', async () => {
      await writeFile(join(app, 'version.txt'), 'v2\n');
      await deploy();
      await second.getByRole('button', { name: 'Recalculate' }).click();
      await output(second, 'v1', 3);
      const fresh = await context.newPage();
      await fresh.goto(appURL);
      await output(fresh, 'v2', 0);
      await assertDisconnected(second);
      await assertDisconnected(third);
      await fresh.getByRole('button', { name: 'Recalculate' }).click();
      await output(fresh, 'v2', 1);
      await fresh.close();
    });
    probing = false;
    await probes;
    assert.deepEqual(failures, [], 'all page probes succeed throughout reloads and deployment');
    await check('access revocation reaches a session held by a retiring process', async () => {
      const retained = await context.newPage();
      await retained.goto(appURL);
      await output(retained, 'v2', 0);
      await reload();
      await request('DELETE', `/api/apps/browser/members/${viewerID}`, undefined, 204);
      await assertDisconnected(retained);
      await retained.close();
    });
  } finally {
    probing = false;
    await probes;
  }
  assert.deepEqual(failures, [], 'all page probes succeed throughout reloads and deployment');
}
