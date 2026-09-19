// Unit-level DOM doubles only; not a browser/layout or CSP-enforcement test.
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../../src/sempervigil/static/event_review.js'), 'utf8');

function setup(saved = null, storageFailure = false) {
  function element() {
    return {hidden: false, value: '', textContent: '', dataset: {}, handlers: {}, attrs: {},
      addEventListener(name, handler) { this.handlers[name] = handler; },
      setAttribute(name, value) { this.attrs[name] = value; }};
  }
  const passages = ['a', 'b'].map(id => {
    const p = element(), select = element();
    p.dataset.passage = id;
    select.value = 'hold';
    p.select = select;
    p.querySelector = () => select;
    return p;
  });
  const groups = passages.map(p => ({hidden: false, querySelectorAll: () => [p]}));
  const elements = Object.fromEntries(['review-note','storage-status','progress','empty-reading','mode','export'].map(id => [id, element()]));
  const body = {dataset: {packet:'snapshot', workflow:'workflow', review:'initial'}, classList:{toggle() {}}};
  const storage = new Map();
  if (saved) storage.set('sempervigil-event-review:snapshot:initial', JSON.stringify(saved));
  let downloaded;
  const document = {body, getElementById: id => elements[id],
    querySelectorAll: selector => selector === '[data-passage]' ? passages : groups,
    createElement: () => ({click() {}})};
  const context = {document, localStorage: {
    getItem(key) {if (storageFailure) throw Error('disabled'); return storage.get(key) || null;},
    setItem(key, value) {if (storageFailure) throw Error('disabled'); storage.set(key, value);}},
    URL: {createObjectURL(blob) {downloaded = blob; return 'blob:test';}, revokeObjectURL() {}},
    Blob, setTimeout(fn) {fn();}};
  vm.runInNewContext(source, context);
  return {elements, passages, groups, storage, download: () => downloaded,
    click(id) {elements[id].handlers.click({currentTarget:elements[id]});},
    choose(index, value) {passages[index].select.value = value; passages[index].select.handlers.change();}};
}

test('initial review has no implicit approvals', () => {
  const ui = setup();
  assert.equal(ui.elements.progress.textContent, '0 included / 0 excluded / 2 pending');
  assert.equal(ui.storage.size, 0);
});
test('include/exclude decisions persist for exact packet and initial review', () => {
  const ui = setup();
  ui.choose(0, 'include'); ui.choose(1, 'exclude');
  const value = JSON.parse([...ui.storage.values()][0]);
  assert.deepEqual(value.decisions, {a:'include', b:'exclude'});
  assert.equal(value.packet_version, 'snapshot');
});
test('reading view hides unselected passages and sources, return restores them', () => {
  const ui = setup();
  ui.choose(0, 'include'); ui.click('mode');
  assert.equal(ui.passages[0].hidden, false);
  assert.equal(ui.passages[1].hidden, true);
  assert.equal(ui.groups[1].hidden, true);
  assert.equal(ui.elements.mode.attrs['aria-pressed'], 'true');
  ui.click('mode');
  assert.equal(ui.passages[1].hidden, false);
});
test('empty reading view explains why it is empty', () => {
  const ui = setup(); ui.click('mode');
  assert.equal(ui.elements['empty-reading'].hidden, false);
});
test('download exports the review contract without publication fields', async () => {
  const ui = setup(); ui.choose(0, 'include'); ui.click('export');
  const data = JSON.parse(await ui.download().text());
  assert.deepEqual(Object.keys(data).sort(), ['decisions','note','packet_version','workflow']);
  assert.equal(data.decisions.a, 'include');
});
test('matching browser decisions restore', () => {
  const ui = setup({workflow:'workflow', packet_version:'snapshot', decisions:{a:'include'}, note:'Review units.'});
  assert.equal(ui.passages[0].select.value, 'include');
  assert.equal(ui.elements['review-note'].value, 'Review units.');
});
test('stale or unknown decisions are not restored', () => {
  for (const data of [
    {workflow:'workflow', packet_version:'old', decisions:{a:'include'}, note:''},
    {workflow:'workflow', packet_version:'snapshot', decisions:{unknown:'include'}, note:''},
    {workflow:'workflow', packet_version:'snapshot', decisions:{a:'publish'}, note:''}
  ]) {
    const ui = setup(data);
    assert.equal(ui.passages[0].select.value, 'hold');
    assert.match(ui.elements['storage-status'].textContent, /invalid/);
  }
});
test('storage failure retains working decisions and asks for download', async () => {
  const ui = setup(null, true); ui.choose(0, 'include'); ui.click('export');
  assert.match(ui.elements['storage-status'].textContent, /unavailable/);
  assert.equal(JSON.parse(await ui.download().text()).decisions.a, 'include');
});
