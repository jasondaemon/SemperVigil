"""Offline pinned catalogs and explicitly scoped successor enablement."""
import gzip,json,os
from functools import lru_cache
from pathlib import Path
from .attack_catalog import Catalog
ROOT=Path(__file__).parent/'data/attack'
RETRIEVAL_POLICY='complete-source-windowed-candidates-v2'

def scope():
 value=os.environ.get('SV_EVENT_REPORT_V2_ENABLED','0')
 if value not in {'0','1'}:raise ValueError('event_report_v2_enablement_invalid')
 if value=='0':return set()
 ids={s.strip() for s in os.environ.get('SV_EVENT_REPORT_V2_EVENT_IDS','').split(',') if s.strip()}
 if not ids:raise ValueError('event_report_v2_scope_required')
 from .event_source_report_pilot import policy
 p=policy()
 if p and ids.intersection(p['events']):raise ValueError('event_report_v2_active_pilot_overlap')
 return ids

def selected(event_id):return event_id in scope()

@lru_cache(maxsize=12)
def catalog(domain='enterprise',release=None,expected_sha256=None):
 lock=json.loads((ROOT/('lock-'+release+'.json' if release else 'lock.json')).read_text())
 if domain not in lock:raise ValueError('attack_catalog_domain_invalid')
 entry=lock[domain]
 if expected_sha256 and entry['sha256']!=expected_sha256:raise ValueError('attack_catalog_historical_digest_mismatch')
 raw=gzip.decompress((ROOT/f'{domain}-{entry["release"]}.json.gz').read_bytes())
 return Catalog(raw,domain=domain,release=entry['release'],expected_sha256=entry['sha256'])

def settings():
 domain=os.environ.get('SV_EVENT_REPORT_V2_ATTACK_DOMAIN','enterprise')
 c=catalog(domain)
 return {'contract':'event-source-report-v2','catalog':c.identity,'retrieval':RETRIEVAL_POLICY,
         'candidate_tokens':3200,'candidate_limit':10,'mapping_projection':'invalid-optional-taxonomy-only-v1',
         'baseline':'published-evidence-delta-v4-membership-baseline'}

def reference(sources):
 from .event_report_contract_v2 import tokens
 # Every body is searched, including relevant behavior beyond its opening.
 # Deterministic windows prevent long-page footers dominating a single bag of words.
 import re
 c=catalog(settings()['catalog']['domain']);ranked={};origins={}
 for source in sorted(sources,key=lambda s:s['id']):
  text=source['text'];paragraphs=re.split(r'(?<=[.!?])\s+',text)
  for passage in paragraphs:
   for start in range(0,len(passage),800):
    window=source.get('title','')+' '+passage[start:start+1000]
    ranked_window=c.rank(window)
    normalized=' '.join(re.findall(r'[a-z0-9]+',window.lower()))
    for score,tid in ranked_window[:20]:
     t=c.lookup(tid);name=' '.join(re.findall(r'[a-z0-9]+',t['name'].lower()))
     # An explicitly named behavior merits a definition, never a mapping.
     if name and name in normalized:score+=5
     ranked[tid]=max(ranked.get(tid,0),score)
     origins.setdefault(tid,set()).add(source['id'])
 result={'catalog':c.identity,'candidates':[],
         'meaning':'Taxonomy references only, not incident evidence or proven objectives.',
         'retrieval':{'policy':RETRIEVAL_POLICY,'searched_source_ids':[s['id'] for s in sources],
                      'candidate_source_ids':{},'omitted_candidates':0}}
 for tid in sorted(ranked,key=lambda t:(-ranked[t],t)):
  t=c.lookup(tid);item={k:t[k] for k in ('id','name','url','parent_id','tactics','definition','object_version')}
  candidate={**result,'candidates':result['candidates']+[item]}
  if len(result['candidates'])>=10 or tokens(json.dumps(candidate,ensure_ascii=False,separators=(',',':'))) >3200:
   result['retrieval']['omitted_candidates']+=1;continue
  result['candidates'].append(item);result['retrieval']['candidate_source_ids'][tid]=sorted(origins[tid])
 # Diagnostics can grow; keep the entire reference within the exact budget.
 while tokens(json.dumps(result,ensure_ascii=False,separators=(',',':'))) >3200 and result['candidates']:
  tid=result['candidates'].pop()['id'];result['retrieval']['candidate_source_ids'].pop(tid,None)
 return result

def catalog_for(identity):
 return catalog(identity["domain"],identity["release"],identity["sha256"])
