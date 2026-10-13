import { test } from 'node:test';
import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';
import { appAttention, renderAppAttention } from '../static/views/app-attention.js';

test('producer-quarantined existing release requires operator attention', () => {
  assert.deepEqual(appAttention({
    status: 'failed', deploy_count: 1, last_deployment_status: 'succeeded',
    name: 'Dashboard', last_error: 'Producer failed after a data write',
  }), {
    severity: 'danger',
    label: 'Unavailable',
    detail: 'A producer or deployment failed. Review the app details and repair the failure.',
  });
});

test('live producer failure and repair update attention independently of release success', () => {
  const app = { status: 'running', deploy_count: 2, last_deployment_status: 'succeeded' };
  assert.equal(appAttention(app, { status: 'failed' }).severity, 'danger');
  assert.equal(appAttention({ ...app, status: 'failed' }, { status: 'running' }), null);
});

test('failed apps render a recovery entry in the attention list', () => {
  const doc = new JSDOM('<!doctype html><body></body>').window.document;
  const section = renderAppAttention(doc, [{
    slug: 'dashboard', name: 'Dashboard', status: 'failed', deploy_count: 2,
  }]);
  assert.equal(section.querySelector('.app-attention-count').textContent, '1');
  assert.equal(section.querySelector('.app-attention-problem strong').textContent, 'Unavailable');
  assert.equal(section.querySelector('.app-attention-action').getAttribute('href'), '/apps/dashboard');
});
