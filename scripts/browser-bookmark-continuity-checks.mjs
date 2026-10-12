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
  await command([binary, 'reload', '--config', config], 'bookmark-config-reload');
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
  const pages = [];
  const frameOrder = [];
  const page = await context.newPage();
  pages.push(page);
  // Headless Chromium does not reliably mark background pages hidden. Simulate
  // only that browser signal; timers, SDK, Shiny session and deployment are real.
  const prepare = async page => {
    page.on('websocket', socket => {
      const methods = []; frameOrder.push(methods);
      socket.on('framesent', ({ payload }) => {
        try { const frame = JSON.parse(payload); if (frame.method) methods.push(frame.method); } catch {}
      });
    });
    await page.addInitScript(() => {
      window.__bookmarkSync = null;
      window.addEventListener('shinyhub:bookmark:sync-status', event => { window.__bookmarkSync = event.detail; });
      window.__continuityHidden = false;
      Object.defineProperty(document, 'hidden', { get: () => window.__continuityHidden });
      Object.defineProperty(document, 'visibilityState', { get: () => window.__continuityHidden ? 'hidden' : 'visible' });
    });
  };
  await prepare(page);
  let identity;
  await check('registered filters survive ordinary reload on real Shiny', async () => {
    await page.goto(host + '/app/bookmarks/');
    await poll(async () => (await page.locator('#result').textContent().catch(() => '')).includes('Samples: 1000'), 'initial filter view');
    await page.getByRole('spinbutton', { name: 'Samples' }).fill('750');
    await page.getByLabel('Region', { exact: true }).selectOption('US');
    await page.getByRole('button', { name: 'Apply', exact: true }).click();
    await poll(() => new URL(page.url()).searchParams.get('samples') === '750', 'bookmark debounce commits filter URL');
    await page.reload();
    await poll(async () => (await page.locator('#result').textContent().catch(() => '')).includes('Samples: 750'), 'reload restores same filter');
    identity = /Session: ([a-f0-9]{8})/.exec(await page.locator('#result').textContent())?.[1];
    assert.ok(identity);
    assert.equal(new URL(page.url()).searchParams.get('region'), '"US"');
    assert.equal(frameOrder[0][0], 'init', 'real Shiny sends init before discovery');
  });
  const draft = await context.newPage(); pages.push(draft); await prepare(draft);
  const restored = await context.newPage(); pages.push(restored); await prepare(restored);
  await check('unapplied filters remain saved and obsolete restored choices settle', async () => {
    await draft.goto(host + '/app/bookmarks/');
    await poll(async () => (await draft.locator('#result').textContent()).includes('Region: EU') &&
      await draft.evaluate(() => window.__bookmarkSync?.state === 'saved'), 'draft baseline saved');
    await draft.getByLabel('Region', { exact: true }).selectOption('APAC');
    assert.ok((await draft.locator('#result').textContent()).includes('Region: EU'));
    assert.equal(await draft.evaluate(() => window.__bookmarkSync?.state), 'saved');
    assert.equal(new URL(draft.url()).searchParams.get('region'), null);
    await restored.goto(host + '/app/bookmarks/?_inputs_&region=%22obsolete%22');
    await poll(async () => (await restored.locator('#result').textContent()).includes('Region: EU') &&
      await restored.evaluate(() => window.__bookmarkSync?.state === 'saved'), 'restore correction saved');
    assert.equal(await restored.getByLabel('Region', { exact: true }).inputValue(), 'EU');
    assert.equal(new URL(restored.url()).searchParams.get('region'), null);
  });
  await check('a hidden tab updates automatically and restores registered filters', async () => {
    await Promise.all(pages.map(page => page.evaluate(() => {
      window.__continuityHidden = true;
      document.dispatchEvent(new Event('visibilitychange'));
    })));
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
    assert.ok((await page.locator('#result').textContent()).includes('Region: US'));
    await Promise.all([draft, restored].map(async tab => {
      await poll(async () => (await tab.locator('#result').textContent()).includes('Version: v2'), 'draft/restore hidden switch');
      assert.ok((await tab.locator('#result').textContent()).includes('Region: EU'));
      assert.equal(await tab.getByLabel('Region', { exact: true }).inputValue(), 'EU');
    }));
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
  assert.ok(frameOrder.every(methods => methods[0] === 'init'), 'every real Shiny session initializes before sending updates');
  await Promise.all(pages.map(page => page.close()));
  report.bookmark_continuity = { applied_region: 'US', draft_restored_region: 'EU', corrected_region: 'EU', initial_frames: frameOrder.map(methods => methods[0]) };
}
