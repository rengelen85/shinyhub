#!/usr/bin/env node
// Real dashboard journeys against a disposable loopback hub and database.
// Build first: GOWORK=off go build -o /tmp/shinyhub-journey ./cmd/shinyhub
// Set SHINYHUB_E2E_BINARY and optionally SHINYHUB_E2E_CHROMIUM / SHINYHUB_E2E_SCREENSHOTS.
// Network delays and failures are injected only into this isolated browser.
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { once } from 'node:events';
import { createServer } from 'node:net';
import { createRequire } from 'node:module';
import { mkdtemp, writeFile, mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
const require = createRequire(new URL('../loadtest/render/driver/package.json', import.meta.url));
const { chromium } = require('playwright');
const binary = resolve(process.env.SHINYHUB_E2E_BINARY || 'bin/shinyhub');
const screenshots = process.env.SHINYHUB_E2E_SCREENSHOTS;
const audit = process.env.SHINYHUB_E2E_AUDIT === '1';
const findings = [];
function verify(condition, message) {
  if (!condition && audit) { findings.push(message); console.log('FINDING:', message); }
  else assert.ok(condition, message);
}
const work = await mkdtemp(join(tmpdir(), 'shinyhub-journey-'));
const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith('SHINYHUB_')));
const username = 'journey-admin', password = randomBytes(24).toString('hex');
let server, browser;
try {
  const reservation = createServer(); reservation.listen(0, '127.0.0.1'); await once(reservation, 'listening');
  const port = reservation.address().port; await new Promise(r => reservation.close(r));
  const hub = `http://127.0.0.1:${port}`;
  const config = join(work, 'hub.yaml'), passwordFile = join(work, 'password');
  await writeFile(passwordFile, password, { mode: 0o600 });
  await writeFile(config, `server:\n  host: 127.0.0.1\n  port: ${port}\n  base_url: ${hub}\n  shutdown_apps: stop\n  drain_timeout: 2s\nauth:\n  secret: ${randomBytes(32).toString('hex')}\ndatabase:\n  dsn: ${join(work, 'hub.db')}\nstorage:\n  apps_dir: ${join(work, 'apps')}\n  app_data_dir: ${join(work, 'app-data')}\n`, { mode: 0o600 });
  const init = spawn(binary, ['init', '--config', config, '--admin-user', username, '--admin-password-file', passwordFile], { cwd: work, env });
  let logs = ''; init.stdout.on('data', x => logs += x); init.stderr.on('data', x => logs += x);
  const [code] = await once(init, 'exit'); assert.equal(code, 0, logs);
  server = spawn(binary, ['serve', '--config', config, '--no-browser'], { cwd: work, env });
  server.stdout.on('data', x => logs += x); server.stderr.on('data', x => logs += x);
  let ready = false;
  for (let i = 0; i < 150; i++) {
    if (await fetch(hub + '/activez', { headers: { 'User-Agent': 'shinyhub-journey-test' } }).then(r => r.ok).catch(() => false)) { ready = true; break; }
    await delay(200);
  }
  assert.ok(ready, logs);
  browser = await chromium.launch({ headless: true, executablePath: process.env.SHINYHUB_E2E_CHROMIUM || undefined });
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, userAgent: 'shinyhub-journey-test' });
  context.setDefaultTimeout(15000);
  const page = await context.newPage(); const errors = []; page.on('pageerror', err => errors.push(err.message));
  await page.goto(hub + '/login');
  await page.locator('#login-username').fill(username); await page.locator('#login-password').fill(password);
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await page.locator('body[data-auth="in"]').waitFor();
  const screenshot = async name => {
    if (!screenshots) return;
    await mkdir(screenshots, { recursive: true });
    await page.screenshot({ path: join(screenshots, name + '.png'), animations: 'disabled',
      mask: [page.locator('#token-reveal-value'), page.locator('#new-user-snippet')] });
  };
  const navigate = async path => {
    await page.goto(hub + path, { waitUntil: 'domcontentloaded' });
    await page.locator('body[data-auth="in"]').waitFor();
  };
  const gate = async (path, method) => {
    let release, seen;
    const held = new Promise(resolve => { release = resolve; });
    const started = new Promise(resolve => { seen = resolve; });
    const handler = async route => {
      if (route.request().method() !== method) return route.continue();
      seen(); await held; await route.continue();
    };
    await page.route('**' + path, handler);
    return { started, release, remove: () => page.unroute('**' + path, handler) };
  };
  const fail = async (path, method, offline = false) => {
    const handler = route => route.request().method() !== method ? route.continue()
      : offline ? route.abort('failed') : route.fulfill({ status: 503, json: { error: 'Temporarily unavailable. Try again.' } });
    await page.route('**' + path, handler);
    return () => page.unroute('**' + path, handler);
  };

  if (process.env.SHINYHUB_E2E_JOURNEY_FILTER !== 'stress') {
    // Create an app from the empty account, preserve the draft on failure,
    // and keep the successful handoff reachable during a slow request.
    await navigate('/apps');
    await page.locator('#empty-state-cta').click();
    const appName = 'Forecast · 東京 · ' + 'Regional planning '.repeat(5);
    await page.locator('#new-app-name').fill(appName);
    await page.locator('#new-app-slug').fill('journey-forecast');
    const restoreCreate = await fail('/api/apps', 'POST');
    await page.locator('#new-app-submit').click();
    await page.locator('#new-app-error').waitFor();
    assert.equal(await page.locator('#new-app-name').inputValue(), appName);
    await restoreCreate();
    const createGate = await gate('/api/apps', 'POST');
    await page.locator('#new-app-submit').click(); await createGate.started;
    await page.keyboard.press('Escape');
    verify(await page.locator('#new-app-modal').isVisible(), 'App creation stays open until its deployment handoff is ready');
    createGate.release();
    await page.waitForResponse(r => r.url() === hub + '/api/apps' && r.request().method() === 'POST');
    await page.waitForFunction(() => !document.querySelector('#new-app-handoff').hidden);
    await createGate.remove();
    if (await page.locator('#new-app-modal').isVisible()) {
      await screenshot('create-handoff');
      await page.locator('#new-app-deploy-now').click();
    } else {
      await navigate('/apps/journey-forecast');
      await page.locator('#app-detail-deploy').click();
    }
    // A real invalid ZIP is rejected, with a usable retry path.
    await page.locator('#deploy-file').setInputFiles({ name: 'broken.zip', mimeType: 'application/zip', buffer: Buffer.from('invalid zip') });
    await page.locator('#deploy-submit').click();
    await page.locator('#deploy-error').waitFor();
    assert.equal(await page.locator('#deploy-submit').isEnabled(), true);
    await screenshot('deploy-error');
    // Choosing an invalid replacement must not leave the old ZIP deployable.
    await page.locator('#deploy-file').setInputFiles({ name: 'notes.txt', mimeType: 'text/plain', buffer: Buffer.from('not a bundle') });
    verify(await page.locator('#deploy-submit').isDisabled(), 'An invalid replacement cannot upload the previous bundle');
    await page.locator('#deploy-modal-close').click();
    // Closing/reopening while bundle rules load discards that old preparation.
    await navigate('/apps/journey-forecast');
    await page.locator('#app-detail-deploy').click();
    const rulesGate = await gate('/static/bundle-rules.json', 'GET');
    await page.locator('#deploy-file').setInputFiles({ name: 'old.zip', mimeType: 'application/zip', buffer: Buffer.from('old bundle') });
    await rulesGate.started;
    await page.locator('#deploy-modal-close').click();
    await page.locator('#app-detail-deploy').click();
    const rulesResponse = page.waitForResponse(r => r.url().endsWith('/static/bundle-rules.json'));
    rulesGate.release(); await rulesResponse;
    await page.waitForTimeout(100);
    verify(await page.locator('#deploy-submit').isDisabled(), 'Closed preparation cannot restore an earlier bundle into the reopened dialog');
    await rulesGate.remove();
    // Use the production ZIP writer and a real native Shiny deployment.
    const bundle = async source => Buffer.from(await page.evaluate(async app => {
      const files = { 'app.py': app, 'requirements.txt': 'shiny==1.8.0\n' };
      const zip = await buildZipNative(Object.entries(files).map(([relativePath, text]) => ({ relativePath, file: new File([text], relativePath) })));
      return [...new Uint8Array(await zip.arrayBuffer())];
    }, source));
    await page.locator('#deploy-file').setInputFiles({ name: 'startup-failure.zip', mimeType: 'application/zip',
      buffer: await bundle('from shiny import App, ui\nraise RuntimeError("Journey deliberate startup failure")\n') });
    await page.locator('#deploy-submit').click();
    await page.locator('#deploy-error').waitFor({ timeout: 180000 });
    assert.equal(await page.locator('#deploy-submit').isEnabled(), true);
    assert.equal(await page.locator('#deploy-ignored-row').isVisible(), false);
    await screenshot('startup-failure');
    await page.locator('#deploy-error-logs').click();
    await page.locator('#log-pane-body').getByText('Journey deliberate startup failure', { exact: false }).waitFor();
    await page.locator('#log-pane-close').click();
    await page.locator('#app-detail-deploy').click();
    await page.locator('#deploy-file').setInputFiles({ name: 'forecast.zip', mimeType: 'application/zip',
      buffer: await bundle('from shiny import App, ui\napp = App(ui.page_fluid(ui.h2("Forecast ready")), lambda input, output, session: None)\n') });
    await page.locator('#deploy-submit').click();
    await page.locator('#deploy-modal').waitFor({ state: 'hidden', timeout: 180000 });
    const running = await context.newPage();
    await running.goto(hub + '/app/journey-forecast/');
    await running.getByRole('heading', { name: 'Forecast ready' }).waitFor({ timeout: 60000 });
    await running.close();
    console.log('Checked app creation, long multilingual name, stale selection, real startup failure and successful redeployment.');

    // Invite a real person and accept the one-time link in a separate session.
    await navigate('/identity');
    await page.locator('#people-joining').waitFor();
    verify(await page.locator('#new-user-button').isVisible(), 'Invite person is discoverable without expanding sign-in guidance');
    if (!await page.locator('#new-user-button').isVisible()) await page.locator('#people-joining summary').click();
    await page.locator('#new-user-button').click();
    await page.locator('#new-user-username').fill('journey-viewer');
    const restoreInvite = await fail('/api/user-invitations', 'POST');
    await page.locator('#new-user-submit').click(); await page.locator('#new-user-error').waitFor();
    assert.equal(await page.locator('#new-user-username').inputValue(), 'journey-viewer');
    await restoreInvite(); await page.locator('#new-user-submit').click();
    await page.locator('#new-user-success').waitFor();
    const invitation = await page.locator('#new-user-snippet').innerText();
    await screenshot('invitation-ready'); await page.locator('#new-user-done').click();
    const viewerContext = await browser.newContext({ viewport: { width: 390, height: 844 }, userAgent: 'shinyhub-journey-test' });
    const viewer = await viewerContext.newPage();
    viewer.on('pageerror', err => errors.push(err.message));
    await viewer.goto(invitation); await viewer.locator('#invitation-form').waitFor();
    const viewerPassword = randomBytes(24).toString('hex');
    await viewer.locator('#invitation-password').fill(viewerPassword);
    await viewer.locator('#invitation-confirm').fill(viewerPassword);
    await viewer.locator('#invitation-submit').click();
    await viewer.getByRole('link', { name: 'Open Apps', exact: true }).click();
    await viewer.locator('body[data-auth="in"]').waitFor();
    assert.equal(await viewer.locator('a[href="/app/journey-forecast/"]').count(), 0);
    console.log('Checked invitation failure/retry, account acceptance, and a new viewer’s empty account.');

    await navigate('/apps/journey-forecast/access');
    await page.locator('#grant-username').fill('journey-viewer');
    const restoreGrant = await fail('/api/apps/journey-forecast/members', 'POST', true);
    await page.locator('#grant-btn').click();
    await page.waitForFunction(() => !document.querySelector('#grant-btn').disabled);
    verify(await page.locator('#grant-error').isVisible(), 'Offline member grants show an actionable error');
    await restoreGrant(); await page.locator('#grant-btn').click();
    await page.getByRole('combobox', { name: 'Role for journey-viewer' }).waitFor();
    await viewer.reload(); await viewer.locator('.lp-tile').waitFor();
    assert.match(await viewer.locator('main').innerText(), /Forecast/);
    const restoreRole = await fail('/api/apps/journey-forecast/members/*', 'PATCH');
    await page.getByRole('combobox', { name: 'Role for journey-viewer' }).selectOption('manager');
    await page.waitForFunction(() => !document.querySelector('.member-role-select').disabled);
    assert.equal(await page.getByRole('combobox', { name: 'Role for journey-viewer' }).inputValue(), 'viewer');
    verify(await page.locator('#grant-error').isVisible(), 'A rejected role change restores the prior value and explains the failure');
    await restoreRole();
    await page.getByRole('combobox', { name: 'Role for journey-viewer' }).selectOption('manager');
    await page.waitForFunction(() => !document.querySelector('.member-role-select').disabled);
    await viewer.goto(hub + '/apps/journey-forecast/configuration');
    await viewer.locator('#general-name').waitFor();
    const restoreRevoke = await fail('/api/apps/journey-forecast/members', 'DELETE');
    await page.getByRole('button', { name: 'Revoke access for journey-viewer' }).click();
    await page.waitForFunction(() => !document.querySelector('#members-list button').disabled);
    verify(await page.locator('#grant-error').isVisible(), 'A failed access revocation explains that access remains unchanged');
    await restoreRevoke(); await page.getByRole('button', { name: 'Revoke access for journey-viewer' }).click();
    await page.getByRole('combobox', { name: 'Role for journey-viewer' }).waitFor({ state: 'detached' });
    await viewer.goto(hub + '/apps'); await viewer.locator('body[data-auth="in"]').waitFor();
    await viewer.waitForFunction(() => !document.querySelector('#lp-loading') || document.querySelector('#lp-loading').hidden);
    assert.equal(await viewer.locator('.lp-tile').count(), 0);
    await viewerContext.close();
    console.log('Checked grant, manager permissions, revocation and viewer visibility.');

    // The raw token must remain reachable after a slow creation request.
    await navigate('/tokens'); await page.locator('#new-token-button').click();
    await page.locator('#new-token-name').fill('Journey workstation');
    const restoreToken = await fail('/api/tokens', 'POST');
    await page.locator('#new-token-submit').click(); await page.locator('#new-token-error').waitFor();
    assert.equal(await page.locator('#new-token-name').inputValue(), 'Journey workstation');
    assert.equal(await page.locator('#new-token-close').isEnabled(), true);
    await restoreToken();
    const tokenGate = await gate('/api/tokens', 'POST');
    await page.locator('#new-token-submit').click(); await tokenGate.started;
    await page.keyboard.press('Escape');
    verify(await page.locator('#new-token-modal').isVisible(), 'Token creation keeps the one-time secret reachable while waiting');
    const tokenResponse = page.waitForResponse(r => r.url() === hub + '/api/tokens' && r.request().method() === 'POST');
    tokenGate.release(); await tokenResponse;
    await page.waitForFunction(() => !document.querySelector('#token-reveal').hidden);
    const secret = await page.locator('#token-reveal-value').textContent();
    assert.ok(secret.length > 20);
    await tokenGate.remove();
    verify(await page.locator('#token-reveal').isVisible(), 'A created token is visible and copyable exactly once');
    if (await page.locator('#token-reveal').isVisible()) await page.locator('#token-reveal-done').click();
    else await navigate('/tokens');
    assert.equal(await page.locator('#token-reveal-value').textContent(), '');
    const authHeaders = { Authorization: 'token ' + secret, 'User-Agent': 'shinyhub-journey-test' };
    assert.equal((await fetch(hub + '/api/apps', { headers: authHeaders })).status, 200);
    page.on('dialog', dialog => dialog.accept());
    await page.getByRole('button', { name: 'Revoke token Journey workstation' }).click();
    await page.getByRole('button', { name: 'Revoke token Journey workstation' }).waitFor({ state: 'detached' });
    assert.equal((await fetch(hub + '/api/apps', { headers: authHeaders })).status, 401);
    console.log('Checked real token creation, one-time reveal, authentication and revocation.');

  }

  // Large illustrative inventories exercise production rendering and filtering
  // without writing thousands of records to the test database.
  const inventory = Array.from({ length: 1000 }, (_, index) => ({
    slug: 'stress-' + String(index).padStart(4, '0'),
    name: `Research ${index} · 東京 · ` + 'International planning '.repeat(4),
    description: 'Quarterly forecasts and capacity planning across regions.',
    status: 'stopped', can_manage: true, access: 'private', deploy_count: 0,
  }));
  await page.route(/\/api\/apps(?:\?.*)?$/, route => route.fulfill({ json: { items: inventory, total: inventory.length } }));
  for (const theme of ['light', 'dark']) for (const width of [1440, 390, 320]) {
    await page.setViewportSize({ width, height: 900 });
    await page.evaluate(value => localStorage.setItem('shinyhub-theme', value), theme);
    await navigate('/apps');
    await page.locator('#apps-search').fill('');
    await page.locator('#app-grid .app-grid-show-more').waitFor();
    const initialCards = await page.locator('#app-grid .app-card').count();
    assert.ok(initialCards > 0 && initialCards < 1000, 'large inventories render a bounded first page');
    await page.locator('#app-grid .app-grid-show-more').click();
    assert.ok(await page.locator('#app-grid .app-card').count() > initialCards, 'Show more reveals another page');
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), `${theme}/${width}: inventory overflow`);
    await page.locator('#apps-search').fill('stress-0999');
    await page.waitForFunction(() => document.querySelectorAll('#app-grid .app-card').length === 1);
    if (width <= 390) {
      const card = await page.locator('#app-grid .app-card').boundingBox();
      const heading = await page.locator('#app-grid .app-card-heading').boundingBox();
      assert.ok(heading.width > card.width * 0.75, 'narrow cards give names space instead of squeezing them beside status');
    }
    await screenshot(`search-${theme}-${width}`);
    await page.locator('#apps-search').fill('nothing-matches-this-query');
    await page.locator('#empty-state').waitFor();
    assert.equal(await page.locator('#app-grid .app-card').count(), 0);
  }
  await page.setViewportSize({ width: 390, height: 844 });
  await navigate('/identity');
  await page.locator('#new-user-button').waitFor();
  assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'Identity mobile layout');
  await screenshot('identity-mobile');
  console.log('Checked 1,000 apps, search/no results, long names and 1440/390/320px layouts in both themes.');
  // Session expiry wins over a late token response, even if creation succeeded.
  await navigate('/tokens'); await page.locator('#new-token-button').click();
  await page.locator('#new-token-name').fill('Expired session');
  let releaseToken, tokenCreated;
  const heldToken = new Promise(resolve => { releaseToken = resolve; });
  const createdToken = new Promise(resolve => { tokenCreated = resolve; });
  await page.route('**/api/tokens', async route => {
    if (route.request().method() !== 'POST') return route.continue();
    const response = await route.fetch(); tokenCreated(); await heldToken;
    await route.fulfill({ response });
  });
  await page.locator('#new-token-submit').click(); await createdToken;
  await page.route('**/api/auth/me', route => route.fulfill({ status: 401, json: { error: 'Session ended' } }));
  await page.evaluate(() => window.dispatchEvent(new Event('pageshow')));
  await page.locator('body[data-auth="out"]').waitFor();
  releaseToken();
  await page.waitForFunction(() => !document.querySelector('#new-token-submit').disabled);
  assert.equal(await page.locator('#new-token-modal').isHidden(), true);
  assert.equal(await page.locator('#token-reveal-value').textContent(), '');
  console.log('Checked that session expiry clears the dialog and ignores a late one-time token.');
  verify(errors.length === 0, 'Journeys complete without unhandled browser errors: ' + errors.join('; '));
  if (audit) console.log(JSON.stringify({ findings }, null, 2));
  else console.log('Dashboard user journeys passed.');
} catch (error) {
  console.error(error);
  throw error;
} finally {
  await browser?.close();
  if (server && server.exitCode === null) { const ended = once(server, 'exit'); server.kill('SIGTERM'); await ended; }
  await rm(work, { recursive: true, force: true });
}
