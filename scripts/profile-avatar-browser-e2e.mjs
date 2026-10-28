#!/usr/bin/env node
// Exercise the real profile flow against a disposable local database.
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
const work = await mkdtemp(join(tmpdir(), 'shinyhub-avatar-'));
const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith('SHINYHUB_')));
const username = 'avatar-tester', password = randomBytes(24).toString('hex');
let server, browser;
try {
  const reservation = createServer(); reservation.listen(0, '127.0.0.1'); await once(reservation, 'listening');
  const port = reservation.address().port; await new Promise(r => reservation.close(r));
  const hub = `http://127.0.0.1:${port}`;
  const config = join(work, 'hub.yaml'), passwordFile = join(work, 'password');
  await writeFile(passwordFile, password, { mode: 0o600 });
  await writeFile(config, `server:\n  host: 127.0.0.1\n  port: ${port}\n  base_url: ${hub}\nauth:\n  secret: ${randomBytes(32).toString('hex')}\ndatabase:\n  dsn: ${join(work, 'hub.db')}\nstorage:\n  apps_dir: ${join(work, 'apps')}\n  app_data_dir: ${join(work, 'app-data')}\n`, { mode: 0o600 });
  const init = spawn(binary, ['init', '--config', config, '--admin-user', username, '--admin-password-file', passwordFile], { cwd: work, env });
  let logs = ''; init.stdout.on('data', x => logs += x); init.stderr.on('data', x => logs += x);
  const [code] = await once(init, 'exit'); assert.equal(code, 0, logs);
  server = spawn(binary, ['serve', '--config', config, '--no-browser'], { cwd: work, env });
  server.stdout.on('data', x => logs += x); server.stderr.on('data', x => logs += x);
  let ready = false;
  for (let i = 0; i < 150; i++) {
    if (await fetch(hub + '/healthz', { headers: { 'User-Agent': 'shinyhub-avatar-test' } }).then(r => r.ok).catch(() => false)) { ready = true; break; }
    await delay(200);
  }
  assert.ok(ready, logs);
  browser = await chromium.launch({ headless: true, executablePath: process.env.SHINYHUB_E2E_CHROMIUM || undefined });
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, userAgent: 'shinyhub-avatar-test' });
  const page = await context.newPage(); const errors = []; page.on('pageerror', err => errors.push(err.message));
  await page.goto(hub + '/login');
  await page.locator('#login-username').fill(username); await page.locator('#login-password').fill(password);
  await page.getByRole('button', { name: 'Sign in', exact: true }).click();
  await page.locator('body[data-auth="in"]').waitFor();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.locator('#sidebar-toggle').click();
  await page.locator('#identity-card').click();
  assert.equal(await page.locator('#sidebar-toggle').getAttribute('aria-expanded'), 'false');
  await page.locator('#profile-cancel').click();
  assert.equal(await page.evaluate(() => document.activeElement.id), 'sidebar-toggle', 'mobile close returns focus to navigation');
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.locator('#identity-card').click();
  assert.equal(await page.locator('#profile-submit').isDisabled(), true, 'Save should wait for a change');
  assert.equal(await page.locator('#profile-password-section').getAttribute('open'), null, 'password editing starts collapsed');
  await page.locator('#profile-display-name').fill('Draft name');
  assert.equal(await page.locator('#profile-submit').isEnabled(), true);
  await page.locator('#profile-display-name').fill('');
  assert.equal(await page.locator('#profile-submit').isDisabled(), true);
  const customColor = '#7c3aed';
  await page.locator('#profile-avatar-color').fill(customColor);
  assert.equal(await page.locator('#profile-avatar').evaluate(el => el.style.backgroundColor), 'rgb(124, 58, 237)');
  assert.equal(await page.locator('#identity-avatar').evaluate(el => el.style.background), '', 'color preview must stay local');
  await page.locator('#profile-cancel').focus(); await page.keyboard.press('Enter');
  assert.equal(await page.evaluate(() => document.activeElement.id), 'identity-card');
  await page.locator('#identity-card').click();
  assert.equal(await page.locator('#profile-avatar').evaluate(el => el.style.background), '', 'closing must discard color draft');
  await page.locator('#profile-avatar-color').fill(customColor);
  if (screenshots) {
    await mkdir(screenshots, { recursive: true });
    for (const theme of ['light', 'dark']) {
      await page.locator(`input[name="theme-pref"][value="${theme}"]`).check();
      for (const [name, width, height] of [['desktop', 1440, 1000], ['mobile', 390, 844]]) {
        await page.setViewportSize({ width, height });
        await page.locator('#profile-modal .profile-body').evaluate(el => { el.scrollTop = 0; });
        await page.screenshot({ animations: 'disabled', path: join(screenshots, `avatar-color-${name}-${theme}.png`), fullPage: true });
        assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'color controls overflow');
        const placeholderContrast = await page.locator('#profile-display-name').evaluate(input => {
          const luminance = color => {
            const c = color.match(/[\d.]+/g).slice(0, 3).map(Number).map(x => x / 255)
              .map(x => x <= 0.04045 ? x / 12.92 : ((x + 0.055) / 1.055) ** 2.4);
            return c[0] * 0.2126 + c[1] * 0.7152 + c[2] * 0.0722;
          };
          const foreground = luminance(getComputedStyle(input, '::placeholder').color);
          const background = luminance(getComputedStyle(input).backgroundColor);
          return (Math.max(foreground, background) + 0.05) / (Math.min(foreground, background) + 0.05);
        });
        assert.ok(placeholderContrast >= 4.5, `${theme} placeholder contrast ${placeholderContrast}`);
        if (name === 'desktop') console.log(`${theme} placeholder contrast: ${placeholderContrast.toFixed(2)}:1`);
        const save = await page.locator('#profile-submit').boundingBox();
        assert.ok(save.y >= 0 && save.y + save.height <= height, 'Save must be visible without scrolling');
      }
    }
    await page.setViewportSize({ width: 1440, height: 1000 });
    await page.locator('input[name="theme-pref"][value="light"]').check();
  }
  await page.locator('#profile-display-name').fill('Avatar Tester');
  // Generated fixture is intentionally a geometric test image, not a user photo.
  const photo = await page.evaluate(() => {
    const c = document.createElement('canvas'); c.width = 600; c.height = 400;
    const ctx = c.getContext('2d'); ctx.fillStyle = '#1e3a5f'; ctx.fillRect(0, 0, 600, 400);
    ctx.fillStyle = '#38bdf8'; ctx.beginPath(); ctx.arc(300, 200, 110, 0, 2 * Math.PI); ctx.fill();
    return c.toDataURL('image/png').split(',')[1];
  });
  const file = { name: 'photo.png', mimeType: 'image/png', buffer: Buffer.from(photo, 'base64') };
  await page.locator('#profile-photo-file').setInputFiles(file);
  await page.waitForFunction(() => document.querySelector('#profile-avatar img')?.src.startsWith('data:') && !document.querySelector('#profile-submit').disabled);
  assert.equal(await page.locator('#identity-avatar img').count(), 0, 'preview must not change saved identity');
  if (screenshots) {
    await mkdir(screenshots, { recursive: true });
    await page.screenshot({ animations: 'disabled', path: join(screenshots, 'avatar-desktop.png'), fullPage: true });
    await page.setViewportSize({ width: 390, height: 844 });
    await page.screenshot({ animations: 'disabled', path: join(screenshots, 'avatar-mobile.png'), fullPage: true });
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'mobile horizontal overflow');
    await page.locator('#profile-submit').scrollIntoViewIfNeeded();
    await page.screenshot({ animations: 'disabled', path: join(screenshots, 'avatar-mobile-actions.png'), fullPage: true });
    await page.locator('input[name="theme-pref"][value="dark"]').check();
    await page.locator('#profile-avatar').scrollIntoViewIfNeeded();
    await page.screenshot({ animations: 'disabled', path: join(screenshots, 'avatar-mobile-dark.png'), fullPage: true });
    await page.setViewportSize({ width: 1440, height: 1000 });
    await page.screenshot({ animations: 'disabled', path: join(screenshots, 'avatar-desktop-dark.png'), fullPage: true });
  }
  // A failed save keeps the draft and leaves a visible, retryable action.
  const failSave = async route => {
    if (route.request().method() === 'PATCH') return route.fulfill({ status: 503, contentType: 'application/json', body: JSON.stringify({ error: 'Could not save. Try again.' }) });
    await route.continue();
  };
  await page.route('**/api/auth/me', failSave);
  await page.locator('#profile-submit').click();
  await page.getByText('Could not save. Try again.').waitFor();
  assert.equal(await page.locator('#profile-submit').isEnabled(), true);
  assert.equal(await page.locator('#profile-avatar img').count(), 1);
  await page.unroute('**/api/auth/me', failSave);
  // Hold the first successful save to exercise closing/reopening while it is in flight.
  let releaseSave, sawSave;
  const saveGate = new Promise(resolve => { releaseSave = resolve; });
  const saveSeen = new Promise(resolve => { sawSave = resolve; });
  const delaySave = async route => {
    if (route.request().method() === 'PATCH') { sawSave(); await saveGate; }
    await route.continue();
  };
  await page.route('**/api/auth/me', delaySave);
  await page.locator('#profile-submit').click(); await saveSeen;
  await page.locator('#profile-close').click(); await page.locator('#identity-card').click();
  releaseSave();
  await page.waitForFunction(() => document.querySelector('#identity-avatar img')?.naturalWidth === 256);
  await page.waitForFunction(() => document.querySelector('#profile-avatar img')?.naturalWidth === 256);
  assert.equal(await page.locator('#profile-photo-remove').isVisible(), true);
  assert.equal(await page.locator('#profile-avatar-color').inputValue(), customColor);
  assert.equal(await page.locator('#profile-display-name').inputValue(), 'Avatar Tester', 'reopened form receives the saved name');
  assert.equal(await page.locator('#profile-submit').isDisabled(), true);
  assert.equal(await page.locator('#identity-avatar').evaluate(el => el.style.backgroundColor), 'rgb(124, 58, 237)');
  await page.unroute('**/api/auth/me', delaySave);
  await page.reload(); await page.waitForFunction(() => document.querySelector('#identity-avatar img')?.naturalWidth === 256);
  await page.locator('#identity-card').click();
  await page.locator('#profile-photo-file').setInputFiles({ name: 'bad.svg', mimeType: 'image/svg+xml', buffer: Buffer.from('<svg/>') });
  await page.getByText('Choose a PNG, JPEG, or WebP photo.').waitFor();
  assert.equal(await page.locator('#profile-avatar img').count(), 1, 'invalid upload must preserve current photo');
  await page.locator('#profile-photo-remove').click();
  await page.locator('#profile-close').click(); await page.locator('#identity-card').click();
  assert.equal(await page.locator('#profile-avatar img').count(), 1, 'closing must discard unsaved removal');
  await page.locator('#profile-photo-remove').click(); await page.locator('#profile-submit').click();
  await page.locator('#profile-success').waitFor(); assert.equal(await page.locator('#identity-avatar img').count(), 0);
  await page.reload(); await page.locator('body[data-auth="in"]').waitFor(); assert.equal(await page.locator('#identity-avatar img').count(), 0);
  assert.equal(await page.locator('#identity-avatar').evaluate(el => el.style.backgroundColor), 'rgb(124, 58, 237)', 'removing a photo preserves color');
  await page.locator('#identity-card').click();
  await page.locator('#profile-color-reset').click();
  assert.equal(await page.locator('#profile-avatar').evaluate(el => el.style.background), '');
  await page.locator('#profile-close').click(); await page.locator('#identity-card').click();
  assert.equal(await page.locator('#profile-avatar-color').inputValue(), customColor, 'cancel reset keeps saved color');
  await page.locator('#profile-color-reset').click(); await page.locator('#profile-submit').click();
  await page.locator('#profile-success').waitFor();
  await page.reload(); await page.locator('body[data-auth="in"]').waitFor();
  assert.equal(await page.locator('#identity-avatar').evaluate(el => el.style.background), '', 'default color persists after reload');
  // Expanded password fields scroll inside the dialog; actions stay fixed,
  // including at a small phone/zoom-equivalent viewport.
  await page.locator('#identity-card').click();
  await page.setViewportSize({ width: 320, height: 568 });
  await page.locator('#profile-password-section summary').focus();
  await page.keyboard.press('Enter');
  assert.equal(await page.locator('#profile-current-password').isVisible(), true);
  const beforeScroll = await page.locator('#profile-submit').boundingBox();
  await page.locator('#profile-modal .profile-body').evaluate(el => { el.scrollTop = el.scrollHeight; });
  const afterScroll = await page.locator('#profile-submit').boundingBox();
  assert.equal(afterScroll.y, beforeScroll.y, 'footer must stay fixed while the body scrolls');
  assert.ok(afterScroll.y + afterScroll.height <= 568 && afterScroll.y >= 0, 'Save remains inside the small viewport');
  assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'small phone horizontal overflow');
  if (screenshots) await page.screenshot({ animations: 'disabled', path: join(screenshots, 'profile-password-mobile.png'), fullPage: true });
  await page.locator('#profile-current-password').fill(password);
  await page.locator('#profile-submit').click();
  await page.getByText('New password must be at least 15 characters', { exact: true }).waitFor();
  await page.locator('#profile-new-password').fill(randomBytes(24).toString('hex'));
  await page.locator('#profile-submit').click();
  await page.getByText('Profile and password updated', { exact: true }).waitFor();
  assert.equal(await page.locator('#profile-password-section').getAttribute('open'), null);
  assert.equal(await page.locator('#profile-submit').isDisabled(), true, 'successful save clears the draft');
  await page.setViewportSize({ width: 1440, height: 1000 });
  // Model SSO capabilities in the session response; backend permissions have API coverage.
  await page.route('**/api/auth/me', async route => {
    if (route.request().method() !== 'GET') return route.continue();
    const response = await route.fetch(); const body = await response.json(); body.user.can_set_password = false;
    await route.fulfill({ response, json: body });
  });
  await page.reload(); await page.locator('body[data-auth="in"]').waitFor(); await page.locator('#identity-card').click();
  assert.equal(await page.locator('#profile-name-field').isVisible(), false);
  assert.equal(await page.locator('#profile-submit').isVisible(), true);
  await page.locator('#profile-photo-file').setInputFiles(file); await page.waitForFunction(() => document.querySelector('#profile-avatar img')?.src.startsWith('data:') && !document.querySelector('#profile-submit').disabled);
  await page.locator('#profile-avatar-color').fill('#112233');
  const requestPromise = page.waitForRequest(r => r.method() === 'PATCH' && r.url().endsWith('/api/auth/me'));
  await page.locator('#profile-submit').click(); const request = await requestPromise;
  assert.deepEqual(Object.keys(request.postDataJSON()), ['avatar', 'avatar_color']); await page.locator('#profile-success').waitFor();
  assert.deepEqual(errors, []);
  console.log('Profile avatar browser checks passed: photo/color preview, fixed footer, keyboard disclosure, password change, failed-save retry, reload, validation, cancel, reset, removal, SSO, desktop and mobile.');
} finally {
  await browser?.close();
  if (server && server.exitCode === null) { const ended = once(server, 'exit'); server.kill('SIGTERM'); await ended; }
  await rm(work, { recursive: true, force: true });
}
