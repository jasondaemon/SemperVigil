const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../../src/sempervigil/static/admin/admin.js'), 'utf8');
const start = source.indexOf('function privateReviewResultSummary(job)');
const end = source.indexOf('function wirePrivateEventReview()', start);
const context = {};
vm.createContext(context);
vm.runInContext(source.slice(start, end), context);
function job() {
  return {job_type:'event_review_private', status:'succeeded', result:{
    status:'review_ready', public_eligible:false, passages:23, model_assessed:true,
    model_cache_hit:true, assessment_summary:{status:'proposal_only',
      assessed:12, not_assessed:11, included:6, held:4, excluded:2, scope_version:'a'.repeat(64)}}};
}
test('revision download is only offered for a completed private receipt', () => {
  const value = job();
  assert.equal(context.hasPrivateRevision(value), false);
  value.result.private_revision = {workflow:'event-private-revision-v1', version:'a'.repeat(64),
    status:'proposal_only', public_eligible:false};
  assert.equal(context.hasPrivateRevision(value), true);
  for (const [key, invalid] of [['version','../secret'], ['version','<img>'],
      ['status','approved'], ['public_eligible',true], ['workflow','other']]) {
    const bad = structuredClone(value); bad.result.private_revision[key] = invalid;
    assert.equal(context.hasPrivateRevision(bad), false);
  }
  value.status='running';
  assert.equal(context.hasPrivateRevision(value), false);
});
test('successful private job displays coverage without factual approval', () => {
  const text = context.privateReviewResultSummary(job());
  assert.match(text, /not publication approval/);
  assert.match(text, /12 of 23 candidate passages assessed; 11 not assessed/);
  assert.match(text, /6 included, 4 held, 2 excluded/);
  assert.match(text, /not independently qualified/);
  assert.match(text, /Cached assessment reused/);
});
test('historical results do not invent coverage', () => {
  const value = job();
  delete value.result.assessment_summary;
  assert.match(context.privateReviewResultSummary(value), /coverage unavailable/);
  assert.doesNotMatch(context.privateReviewResultSummary(value), /12 of/);
});
test('invalid counts and untrusted text never appear as report details', () => {
  for (const invalid of [-1, 49, '<img src=x>', 1.5, null]) {
    const value = job();
    value.result.assessment_summary.included = invalid;
    const text = context.privateReviewResultSummary(value);
    assert.match(text, /coverage unavailable/);
    assert.doesNotMatch(text, /<img|Suggestions:/);
  }
  const value = job();
  value.result.assessment_summary.assessed = 13;
  assert.match(context.privateReviewResultSummary(value), /coverage unavailable/);
});
test('nonprivate, failed or purportedly approved results are not presented as private drafts', () => {
  for (const alter of [j => j.status='failed', j => j.job_type='event_report_llm',
      j => j.result.public_eligible=true, j => j.result.status='invalid']) {
    const value = job(); alter(value);
    assert.equal(context.privateReviewResultSummary(value), '');
  }
});
test('extractive output cannot be described as model assessment', () => {
  const value = job();
  delete value.result.assessment_summary;
  value.result.model_assessed = false;
  value.result.model_cache_hit = false;
  assert.match(context.privateReviewResultSummary(value), /no model-assessed passages/);
});
test('source-level result names only a valid source id with reconciled coverage', () => {
  const value = job();
  Object.assign(value.result.assessment_summary, {workflow:'event-source-assessment-v1', article_id:21505});
  assert.match(context.privateReviewResultSummary(value), /Assessed source article 21505/);
  assert.match(context.privateReviewResultSummary(value), /other sources remain unassessed/);
  for (const invalid of [true, '21505', '<img src=x>', -1, 0, 1.5, Number.MAX_SAFE_INTEGER+1]) {
    value.result.assessment_summary.article_id = invalid;
    assert.doesNotMatch(context.privateReviewResultSummary(value), /Assessed source article|<img/);
  }
  Object.assign(value.result.assessment_summary, {article_id:21505, workflow:'event-scoped-assessment-v1'});
  assert.doesNotMatch(context.privateReviewResultSummary(value), /Assessed source article/);
});
