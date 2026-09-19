const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '../../src/sempervigil/static/admin/admin.js'), 'utf8');
const start = source.indexOf('function eventApprovalSelection(');
const end = source.indexOf('document.addEventListener("DOMContentLoaded"', start);
const context = {};
vm.createContext(context);
vm.runInContext(source.slice(start, end), context);
const data = {revision_id: 'a'.repeat(64), expected_predecessor: null,
  passages: [{id: 'one'}, {id: 'two'}], confirmation: 'APPROVE_ATTRIBUTED_QUOTES'};
test('model decisions cannot supply human approval or select quotes', () => {
  for (const [ids, confirmed] of [[[], true], [['one'], false], [['unknown'], true],
    [['one', 'one'], true], [Array(13).fill('one'), true]]) {
    assert.throws(() => context.eventApprovalSelection(data, ids, confirmed));
  }
});
test('request contains only explicit immutable selections', () => {
  const result = JSON.parse(JSON.stringify(context.eventApprovalSelection(data, ['two'], true)));
  assert.deepEqual(result, {revision_id: data.revision_id, passage_ids: ['two'],
    expected_predecessor: null, confirmation: data.confirmation});
});
test('source content is rendered as text and inputs start unselected', () => {
  const ui = source.slice(source.indexOf('async function wireEventApproval()'), end);
  assert.doesNotMatch(ui, /innerHTML|\.checked\s*=/);
  assert.match(ui, /text\.textContent = source\.text/);
  assert.match(ui, /X-SV-Event-Approval/);
  assert.match(ui, /Not published/);
});

function browser() {
  class Element {
    constructor() { this.children = []; this.listeners = {}; this.checked = false; this.dataset = {}; }
    appendChild(child) { this.children.push(child); }
    append(...children) { this.children.push(...children); }
    addEventListener(name, callback) { this.listeners[name] = callback; }
  }
  const ids = Object.fromEntries(['event-approval', 'event-approval-context', 'event-approval-status',
    'event-approval-submit', 'event-approval-confirm'].map(id => [id, new Element()]));
  ids['event-approval'].dataset.jobId = 'job-one';
  ids['event-approval-submit'].disabled = true;
  const calls = [];
  const model = {...data, documents: [{article_id: 1, title: '<img src=x>', url: 'https://example.org',
      text: 'Original source context', feed_day: '2026-09-19'}],
    passages: [{id: 'one', article_id: 1, start: 0, end: 40, quote: 'An exact attributed quotation.'}],
    scope: {focus: [{quote: 'Acme'}], anchor: {quote: 'Anchor quotation'}},
    suggestions: {one: {decision: 'include'}}};
  const local = {document: {getElementById: id => ids[id], createElement: () => new Element()},
    apiFetch: async (url, options) => { calls.push({url, options});
      return options ? {status: 'queued', job_id: 'promotion-one'} : model; }};
  vm.createContext(local);
  vm.runInContext(source.slice(start, end), local);
  function inputs(el) { return el.children.flatMap(child => [child, ...inputs(child)]).filter(el => el.type === 'checkbox'); }
  return {ids, calls, local, inputs};
}
test('review loads without approving, then submits once after selection and confirmation', async () => {
  const {ids, calls, local, inputs} = browser();
  await local.wireEventApproval();
  assert.equal(calls.length, 1);
  const quotes = inputs(ids['event-approval-context']);
  assert.equal(quotes.length, 1);
  assert.equal(quotes[0].checked, false);
  assert.equal(ids['event-approval-submit'].disabled, true);
  quotes[0].checked = true;
  quotes[0].listeners.change();
  assert.equal(ids['event-approval-submit'].disabled, true);
  ids['event-approval-confirm'].checked = true;
  ids['event-approval-confirm'].listeners.change();
  assert.equal(ids['event-approval-submit'].disabled, false);
  await ids['event-approval-submit'].listeners.click();
  assert.equal(calls.length, 2);
  assert.equal(calls[1].options.method, 'POST');
  assert.equal(calls[1].options.headers['X-SV-Event-Approval'], '1');
  assert.deepEqual(JSON.parse(calls[1].options.body).passage_ids, ['one']);
  assert.equal(ids['event-approval-submit'].disabled, true);
  assert.match(ids['event-approval-status'].textContent, /Not published/);
});
test('unavailable evidence leaves approval disabled', async () => {
  const {ids, local} = browser();
  local.apiFetch = async () => { throw new Error('stale evidence'); };
  await local.wireEventApproval();
  assert.equal(ids['event-approval-submit'].disabled, true);
  assert.match(ids['event-approval-status'].textContent, /Review unavailable/);
});
