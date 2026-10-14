import assert from 'node:assert/strict';
import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

// A deliberately undersized CPU budget exercises admission without saturating
// the developer's host. HTTP and WebSockets use the actual native Shiny process.
export async function checkUpdateCapacity({ browser, context, page, original, appURL, host, app, work,
  binary, config, command, api, deploy, check, poll, output, sockets, report, username, password }) {
  const before = await api('');
  const held = [...sockets.get(page)];
  assert.ok(held.length > 0, 'a real WebSocket is held through the refused update');
  let operatorContext;
  try {
    await check('CPU-blocked update preserves the working version under concurrent traffic', async () => {
      await writeFile(join(app, 'version.txt'), 'v2\n');
      await deploy('browser', { expectedExit: 'nonzero' });
      const detail = await api('');
      assert.equal(detail.app.status, 'running');
      assert.equal(detail.released_version, before.released_version);
      assert.equal(detail.update_status.state, 'blocked_capacity');
      assert.equal(detail.update_status.resource, 'cpu');
      assert.equal(detail.update_status.automatic_retry, false);
      const responses = await Promise.all(Array.from({ length: 24 }, () => context.request.get(appURL)));
      try { assert.ok(responses.every(response => response.status() === 200), 'all concurrent page requests served'); }
      finally { await Promise.all(responses.map(response => response.dispose())); }
      await page.getByRole('button', { name: 'Recalculate' }).click();
      await output(page, 'v1', 3, 500, original);
      assert.ok(held.every(socket => !socket.isClosed()), 'blocked update keeps existing WebSockets open');
      report.capacity = { budget_cores: 0.1, concurrent_page_requests: responses.length, held_websockets: held.length };
    });
    operatorContext = await browser.newContext({ userAgent: 'shinyhub-capacity-test' });
    const operator = await operatorContext.newPage();
    await check('operator sees that the update is blocked while the app remains running', async () => {
      await operator.goto(host + '/login?next=' + encodeURIComponent('/apps/browser'));
      await operator.locator('body[data-auth="out"]').waitFor();
      await operator.locator('#login-form').getByLabel('Username', { exact: true }).fill(username);
      await operator.locator('#login-form').getByLabel('Password', { exact: true }).fill(password);
      await operator.getByRole('button', { name: 'Sign in', exact: true }).click();
      await operator.waitForURL(host + '/apps/browser');
      await operator.getByText('Update blocked by capacity', { exact: true }).waitFor();
      assert.match(await operator.locator('.capacity-banner').innerText(), /will not retry automatically/);
      await operator.screenshot({ path: join(work, 'capacity-desktop.png'), fullPage: true });
      await operator.setViewportSize({ width: 390, height: 844 });
      assert.equal(await operator.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, 'no mobile horizontal overflow');
      await operator.screenshot({ path: join(work, 'capacity-mobile.png'), fullPage: true });
    });
    await check('preserving reload retains the capacity blocker and the original session', async () => {
      await command([binary, 'reload', '--config', config, '--wait', '30s', '--output', 'json'], 'capacity-reload');
      await page.getByRole('button', { name: 'Recalculate' }).click();
      await output(page, 'v1', 4, 500, original);
      assert.ok(held.every(socket => !socket.isClosed()), 'preserving reload keeps existing WebSockets open');
      assert.equal((await api('')).update_status.state, 'blocked_capacity');
      await deploy('browser', { expectedExit: 'nonzero' });
      assert.ok(held.every(socket => !socket.isClosed()), 'explicit retry while full preserves sessions');
      await page.getByRole('button', { name: 'Recalculate' }).click();
      await output(page, 'v1', 5, 500, original);
    });
    await check('explicit stop-first deployment publishes v2 and clears the capacity blocker', async () => {
      // A full host cannot promise a parallel retry. Verify the operator's
      // explicit availability-gap choice; controlled API tests cover retry
      // after headroom returns without assuming unrelated host work will end.
      await deploy('browser', { flags: ['--allow-downtime'], expectedExit: 'nonzero' });
      assert.match(await readFile(join(work, 'deploy.log'), 'utf8'), /must wait for all previous generations to retire/);
      assert.ok(held.every(socket => !socket.isClosed()), 'stop-first guard preserves independently relayed sessions');
      await page.close();
      await poll(async () => {
        const metrics = await api('/metrics');
        return metrics.replicas.length > 0 && metrics.replicas.every(replica => replica.sessions === 0);
      }, 'closed session releases the reload relay');
      await deploy('browser', { flags: ['--allow-downtime'] });
      await poll(async () => (await api('')).update_status === undefined, 'successful retry clears blocker');
      await operator.reload();
      await operator.locator('#overview-health').waitFor();
      assert.equal(await operator.locator('.capacity-banner').count(), 0, 'obsolete warning removed');
      const fresh = await context.newPage();
      try { await fresh.goto(appURL); await output(fresh, 'v2', 0); }
      finally { await fresh.close(); }
    });
  } finally { await operatorContext?.close(); }
}
