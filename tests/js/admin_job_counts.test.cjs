const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../../src/sempervigil/static/admin/admin.js'), 'utf8');
const start = source.indexOf('  function renderJobCounts(');
const end = source.indexOf('  async function loadMetrics()', start);
assert.ok(start > 0 && end > start);
function render(counts, types, groups) {
  function element(tag) {
    return {tag, children:[], innerHTML:'', classList:{add() {}},
      appendChild(child) {this.children.push(child);},
      querySelector() {return this.body ||= element('tbody');}};
  }
  const container = element('main');
  const context = {jobCountsContainer:container,
    document:{createElement:element, getElementById:() => null},
    esc:value => String(value).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;'),
    formatTimestamp:String};
  vm.createContext(context);
  vm.runInContext(source.slice(start, end), context);
  context.renderJobCounts(counts, types, groups, null, {}, {});
  const sections = container.children[0].children;
  return sections.flatMap(section => section.children.filter(c => c.tag === 'table').flatMap(t => t.body.children));
}
test('daily brief, private review, control and unknown jobs are not hidden', () => {
  const counts = {build_daily_brief:{queued:1},event_review_private:{running:1},launch_llm_worker:{failed:1},historical:{succeeded:2}};
  const rows = render(counts, ['event_review_private'], [{id:'llm',title:'LLM',job_types:['event_review_private']}]);
  assert.equal(rows.length, 4);
  for (const name of Object.keys(counts)) assert.ok(rows.some(row => row.innerHTML.includes(`>${name}</a>`)));
});
test('canceled counts and encoded filtered links escape historical job names', () => {
  const name = '<script>alert(1)</script>';
  const rows = render({[name]:{canceled:7}}, [], []);
  assert.match(rows[0].innerHTML, /<td>7<\/td>/);
  assert.ok(rows[0].innerHTML.includes('job_type=' + encodeURIComponent(name)));
  assert.ok(!rows[0].innerHTML.includes('<script>'));
});

test('slow dashboard refreshes never overlap and can retry after errors', async () => {
  const begin = source.indexOf('  let metricsLoading = false;');
  const finish = source.indexOf('  async function loadQueueDiagnostics()', begin);
  let calls = 0;
  let resolve, reject;
  const context = {apiFetch: () => {
    calls++;
    return new Promise((yes, no) => {resolve = yes; reject = no;});
  }, renderJobCounts: () => {}};
  vm.createContext(context);
  vm.runInContext(source.slice(begin, finish), context);
  const first = context.loadMetrics();
  await context.loadMetrics();
  assert.equal(calls, 1);
  resolve({});
  await first;
  const second = context.loadMetrics();
  reject(new Error('request failed'));
  await assert.rejects(second, /request failed/);
  const third = context.loadMetrics();
  assert.equal(calls, 3);
  resolve({});
  await third;
});
