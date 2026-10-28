import { test } from 'node:test';
import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';
import { renderUserAvatar, prepareUserAvatar, avatarTextColor, avatarPickerColor } from '../static/views/user-avatar.js';

test('photo rendering restores initials on removal or a failed image load', () => {
  const { window } = new JSDOM('<span id="avatar"></span>');
  const el = window.document.getElementById('avatar');
  const user = { username: 'alice', avatar_url: '/api/auth/me/avatar?v=1' };
  renderUserAvatar(el, user);
  assert.equal(el.querySelector('img').getAttribute('src'), user.avatar_url);
  assert.equal(el.querySelector('img').alt, '');
  el.querySelector('img').dispatchEvent(new window.Event('error'));
  assert.equal(el.textContent, 'AL');
  renderUserAvatar(el, user);
  renderUserAvatar(el, user, '');
  assert.equal(el.textContent, 'AL');
  assert.equal(el.querySelector('img'), null);
});

test('an outdated image failure cannot clear a newer preview', () => {
  const { window } = new JSDOM('<span></span>');
  const el = window.document.querySelector('span');
  renderUserAvatar(el, { username: 'alice' }, '/old');
  const old = el.firstChild;
  renderUserAvatar(el, { username: 'alice' }, '/new');
  old.dispatchEvent(new window.Event('error'));
  assert.equal(el.firstChild.getAttribute('src'), '/new');
});

test('photo preparation rejects unsupported or oversized files before reading', async () => {
  await assert.rejects(prepareUserAvatar({ type: 'image/svg+xml', size: 10 }, {}), /PNG, JPEG, or WebP/);
  await assert.rejects(prepareUserAvatar({ type: 'image/png', size: 6 * 1024 * 1024 }, {}), /5 MB/);
});


test('custom color renders with contrasting initials and resets cleanly', () => {
  const { window } = new JSDOM('<span></span>');
  const el = window.document.querySelector('span');
  renderUserAvatar(el, { username: 'alice', avatar_color: '#112233' });
  assert.equal(el.style.background, 'rgb(17, 34, 51)');
  assert.equal(el.style.color, 'rgb(255, 255, 255)');
  renderUserAvatar(el, { username: 'alice', avatar_color: '#ffffff' });
  assert.equal(el.style.color, 'rgb(3, 5, 16)');
  renderUserAvatar(el, { username: 'alice', avatar_color: '' });
  assert.equal(el.style.background, '');
  assert.equal(el.style.color, '');
  renderUserAvatar(el, { username: 'alice', avatar_color: 'url(https://example.com)' });
  assert.equal(el.style.background, '');
});

test('color picker uses the custom color or a stable default', () => {
  assert.equal(avatarPickerColor({ avatar_color: '#123456' }), '#123456');
  assert.match(avatarPickerColor({ username: 'alice' }), /^#[a-f0-9]{6}$/);
  assert.equal(avatarPickerColor({ username: 'alice' }), avatarPickerColor({ username: 'alice', display_name: 'New name' }));
});

test('initials maintain at least 4.5:1 contrast across arbitrary RGB colors', () => {
  function luminance(hex) {
    const c = hex.slice(1).match(/../g).map(x => parseInt(x, 16) / 255)
      .map(x => x <= 0.04045 ? x / 12.92 : ((x + 0.055) / 1.055) ** 2.4);
    return 0.2126 * c[0] + 0.7152 * c[1] + 0.0722 * c[2];
  }
  for (let r = 0; r <= 255; r += 17) for (let g = 0; g <= 255; g += 17) for (let b = 0; b <= 255; b += 17) {
    const color = '#' + [r, g, b].map(x => x.toString(16).padStart(2, '0')).join('');
    const a = luminance(color), foreground = luminance(avatarTextColor(color));
    const ratio = (Math.max(a, foreground) + 0.05) / (Math.min(a, foreground) + 0.05);
    assert.ok(ratio >= 4.5, `${color}: ${ratio}`);
  }
});
