const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const source = fs.readFileSync(path.join(__dirname, '../../src/sempervigil/static/admin/admin.js'), 'utf8');
const start = source.indexOf('function wirePrivateEventReview()');
const end = source.indexOf('function wireEventDetail()', start);
function setup(enabled) {
  const nodes = {
    'event-review-queue': {disabled:true},
    'event-review-aliases': {value:'Acme, Example Incident'},
    'event-review-status': {textContent:''},
    'event-detail': {dataset:{eventId:'event/id'}},
  };
  const calls = [];
  const context = {document:{getElementById:id => nodes[id]}, apiFetch: async (url, options) => {
    calls.push({url, options});
    return options ? {job_id:'job-test'} : {enabled};
  }};
  vm.createContext(context);
  vm.runInContext(source.slice(start, end), context);
  context.wirePrivateEventReview();
  return {nodes, calls};
}
test('disabled review cannot enqueue and never submits on page load', async () => {
  const {nodes, calls} = setup(false);
  await new Promise(setImmediate);
  await nodes['event-review-queue'].onclick();
  assert.equal(calls.length, 1);
  assert.equal(nodes['event-review-queue'].disabled, true);
  assert.match(nodes['event-review-status'].textContent, /disabled/);
});
test('enabled control submits aliases only after explicit click', async () => {
  const {nodes, calls} = setup(true);
  await new Promise(setImmediate);
  assert.equal(calls.length, 1);
  await nodes['event-review-queue'].onclick();
  assert.equal(calls[1].url, '/admin/api/events/event%2Fid/private-review');
  assert.deepEqual(JSON.parse(calls[1].options.body), {aliases:['Acme','Example Incident']});
  assert.match(nodes['event-review-status'].textContent, /Queued job-test/);
});
