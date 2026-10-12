import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { cp, mkdir, readFile, readdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

export async function checkBookmarkContinuity({ root, state, binary, clientConfig, host, token, env,
  command, context, check, poll, report }) {
  // The hidden-tab grace is 30s and polling is slower in a background tab.
  // Keep the old app alive long enough for that deliberate automatic-switch
  // policy; the preceding retirement check deliberately used a 5s app drain.
  const config=join(state,'shinyhub.yaml');
  await writeFile(config,(await readFile(config,'utf8')).replace('  drain_timeout: 5s\n','  drain_timeout: 120s\n  reload_drain_timeout: 5s\n'));
  const pidFile=join(state,'hub.pid');
  const before=Number((await readFile(pidFile,'utf8')).trim());
  process.kill(before,'SIGHUP');
  await poll(async()=>Number((await readFile(pidFile,'utf8')).trim())!==before &&
    await fetch(host+'/activez',{headers:{'User-Agent':'shinyhub-bookmark-continuity-test'}}).then(r=>r.ok).catch(()=>false),'longer app drain configured');
  const app = join(state, 'bookmark-input');
  await cp(join(root, 'loadtest/browser/bookmarks'), app, { recursive: true });
  const wheels = join(state, 'bookmark-wheels');
  await mkdir(wheels);
  await command(['uv', 'build', '--wheel', '--out-dir', wheels, join(root, 'packaging/python-bookmarks')], 'bookmark-wheel');
  const names = (await readdir(wheels)).filter(name => name.endsWith('.whl'));
  assert.equal(names.length, 1);
  const wheel = names[0];
  await cp(join(wheels, wheel), join(app, wheel));
  report.bookmark_wheel = { name: wheel, sha256: createHash('sha256').update(await readFile(join(wheels, wheel))).digest('hex') };
  await writeFile(join(app, 'requirements.txt'), (await readFile(join(app, 'requirements.txt'), 'utf8')) + `./${wheel}\n`);
  const deploy = () => command([binary, 'deploy', app, '--config', clientConfig, '--slug', 'bookmarks', '--visibility', 'public', '--output', 'json'], 'bookmark-deploy',
    { env: { ...env, SHINYHUB_HOST: host, SHINYHUB_TOKEN: token } });
  await check('deploy real bookmarked Shiny dashboard with automatic switching', deploy);
  const page = await context.newPage();
  // Headless Chromium does not reliably mark background pages hidden. Simulate
  // only that browser signal; timers, SDK, Shiny session and deployment are real.
  await page.addInitScript(() => {
    window.__continuityHidden = false;
    Object.defineProperty(document, 'hidden', { get: () => window.__continuityHidden });
    Object.defineProperty(document, 'visibilityState', { get: () => window.__continuityHidden ? 'hidden' : 'visible' });
  });
  let identity;
  await check('registered filters survive ordinary reload on real Shiny', async () => {
    await page.goto(host + '/app/bookmarks/');
    await poll(async () => (await page.locator('#result').textContent().catch(() => '')).includes('Samples: 1000'), 'initial filter view');
    await page.getByRole('spinbutton', { name: 'Samples' }).fill('750');
    await poll(() => new URL(page.url()).searchParams.get('samples') === '750', 'bookmark debounce commits filter URL');
    await page.reload();
    await poll(async () => (await page.locator('#result').textContent().catch(() => '')).includes('Samples: 750'), 'reload restores same filter');
    identity = /Session: ([a-f0-9]{8})/.exec(await page.locator('#result').textContent())?.[1];
    assert.ok(identity);
  });
  await check('a hidden tab updates automatically and restores registered filters', async () => {
    await page.evaluate(() => {
      window.__continuityHidden = true;
      document.dispatchEvent(new Event('visibilitychange'));
    });
    await writeFile(join(app, 'version.txt'), 'v2\n');
    await deploy();
    await poll(async () => {
      const text = await page.locator('#result').textContent().catch(() => '');
      return text.includes('Version: v2') && text.includes('Samples: 750');
    }, 'hidden automatic switch restores filters', 100_000);
    const next = /Session: ([a-f0-9]{8})/.exec(await page.locator('#result').textContent())?.[1];
    assert.ok(next);
    assert.notEqual(next, identity, 'auto-switch creates a new Shiny session');
    assert.equal(new URL(page.url()).searchParams.get('samples'), '750');
    const accessibility = await context.newCDPSession(page);
    try {
      const tree = await accessibility.send('Accessibility.getFullAXTree');
      const trigger = tree.nodes.find(node => node.role?.value === 'button' && node.name?.value?.startsWith('Switch apps'));
      assert.ok(trigger?.backendDOMNodeId, 'switcher button is accessible');
      const { object } = await accessibility.send('DOM.resolveNode', { backendNodeId: trigger.backendDOMNodeId });
      await accessibility.send('Runtime.callFunctionOn', { objectId: object.objectId, functionDeclaration: 'function(){this.click();}' });
      await poll(async () => (await accessibility.send('Accessibility.getFullAXTree')).nodes.some(node => node.name?.value === 'Updated to the latest version'), 'automatic update notice in accessibility tree');
    } finally { await accessibility.detach(); }
  });
  await page.close();
}
