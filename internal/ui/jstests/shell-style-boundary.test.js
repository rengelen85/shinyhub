import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { JSDOM } from 'jsdom';

const root = new URL('../static/', import.meta.url);
// Shared chrome has one owner. Route styles may style page content, never the
// navigation, its descendants, or its sizing tokens (even inside @media).
const shell = /#(?:sidebar[\w-]*|primary-nav|mobile-topbar|app-shell)\b|\.(?:sidebar[\w-]*|nav-item|nav-ico|nav-label|identity-[\w-]+)\b/;
const route = /#[\w-]+-view\b/;

function violations(css, filename) {
  const dom = new JSDOM('<!doctype html><head></head><body></body>');
  const style = dom.window.document.createElement('style');
  style.textContent = css;
  dom.window.document.head.append(style);
  assert.ok(style.sheet, `${filename}: stylesheet must parse; do not silently skip the boundary check`);
  const found = [];
  function visit(rules, context = '') {
    for (const rule of rules) {
      const selector = `${context} ${rule.selectorText || ''}`;
      const ownsShell = shell.test(selector) || Array.from(rule.style || []).some(p => p.startsWith('--sidebar-'));
      if (ownsShell && (filename !== 'style.css' || route.test(selector))) {
        found.push(`${filename}: ${selector.trim()}`);
      }
      if (rule.cssRules) visit(rule.cssRules, selector);
    }
  }
  visit(style.sheet.cssRules);
  dom.window.close();
  return found;
}

function stylesheets(dir, prefix = '') {
  return readdirSync(dir, { withFileTypes: true }).flatMap(entry => {
    const path = prefix + entry.name;
    if (entry.isDirectory()) return stylesheets(new URL(entry.name + '/', dir), path + '/');
    return path.endsWith('.css') ? [path] : [];
  });
}

test('shared navigation styles belong to the shared stylesheet, independent of route', () => {
  const found = stylesheets(root).flatMap(path => violations(readFileSync(new URL(path, root), 'utf8'), path));
  assert.deepEqual(found, [], 'Move shell styling to style.css and remove route conditions. Page styles must not restyle shared navigation.');
});

test('boundary rejects the original regression and nested route overrides', () => {
  const original = 'body:where(:has(#overview-view:not([hidden]), #launchpad-view:not([hidden]))) #sidebar { padding: 20px 12px 12px; }';
  for (const file of ['workspace.css', 'style.css']) {
    assert.equal(violations(original, file).length, 1, `${file}: original regression`);
    assert.equal(violations(`@media (max-width: 860px) { ${original} }`, file).length, 1, `${file}: mobile regression`);
  }
  assert.equal(violations('.nav-item { min-height: 40px; }', 'views/new-page.css').length, 1);
  assert.equal(violations(':root { --sidebar-w: 280px; }', 'views/new-page.css').length, 1);
});

test('boundary permits shared state and responsive styling, and page content rules', () => {
  assert.deepEqual(violations('body.sidebar-collapsed .nav-item { padding: 0; } @media(max-width:860px) { #sidebar { padding-top:80px; } }', 'style.css'), []);
  assert.deepEqual(violations('body:has(#overview-view:not([hidden])) main { padding: 32px; } .ov-panel { padding: 20px; }', 'workspace.css'), []);
});
