"""Pinned official ATT&CK taxonomy. Catalog validity never proves event behavior."""
import hashlib
import json
import math
import re
from collections import Counter

POLICY = 'pinned-attack-behavior-mapping-v1'
DOMAIN_CHAINS = {'enterprise':'mitre-attack', 'mobile':'mitre-mobile-attack', 'ics':'mitre-ics-attack'}
ID_PATTERN = re.compile(r'T\d{4}(?:\.\d{3})?')


def external_id(obj):
    ids = [r['external_id'] for r in obj.get('external_references', [])
           if r.get('source_name') in DOMAIN_CHAINS.values() and r.get('external_id')]
    if len(ids) != 1:
        raise ValueError('attack_catalog_external_id_invalid')
    return ids[0]


class Catalog:
    def __init__(self, raw, *, domain, release, expected_sha256):
        if domain not in DOMAIN_CHAINS or hashlib.sha256(raw).hexdigest() != expected_sha256:
            raise ValueError('attack_catalog_digest_or_domain_mismatch')
        data = json.loads(raw)
        collections = [o for o in data['objects'] if o['type'] == 'x-mitre-collection']
        if len(collections) != 1 or collections[0].get('x_mitre_version') != release or collections[0].get('name') != {'enterprise':'Enterprise ATT&CK','mobile':'Mobile ATT&CK','ics':'ICS ATT&CK'}[domain]:
            raise ValueError('attack_catalog_release_mismatch')
        self.identity = {'domain':domain, 'release':release, 'sha256':expected_sha256, 'policy':POLICY}
        objects = {o['id']:o for o in data['objects']}
        if len(objects) != len(data['objects']):
            raise ValueError('attack_catalog_duplicate_stix_id')
        tactics = {o['x_mitre_shortname']: {'id':external_id(o), 'name':o['name']}
                   for o in objects.values() if o['type'] == 'x-mitre-tactic'
                   and not o.get('revoked') and not o.get('x_mitre_deprecated')}
        parents = {}
        for obj in objects.values():
            if obj['type'] == 'relationship' and obj.get('relationship_type') == 'subtechnique-of' and not obj.get('revoked'):
                child, parent = obj['source_ref'], obj['target_ref']
                if child in parents or child not in objects or parent not in objects:
                    raise ValueError('attack_catalog_parent_relationship_invalid')
                parents[child] = parent
        self.techniques = {}
        for obj in objects.values():
            if obj['type'] != 'attack-pattern':
                continue
            tid = external_id(obj)
            if not ID_PATTERN.fullmatch(tid) or tid in self.techniques:
                raise ValueError('attack_catalog_technique_id_invalid')
            inactive = bool(obj.get('revoked') or obj.get('x_mitre_deprecated'))
            parent = external_id(objects[parents[obj['id']]]) if obj['id'] in parents else None
            if not inactive and (bool(obj.get('x_mitre_is_subtechnique')) != (parent is not None)
                    or (parent and ('.' not in tid or tid.split('.')[0] != parent))):
                raise ValueError('attack_catalog_parent_relationship_invalid')
            phases = []
            for phase in obj.get('kill_chain_phases', []):
                if phase['kill_chain_name'] != DOMAIN_CHAINS[domain] or phase['phase_name'] not in tactics:
                    if not inactive:
                        raise ValueError('attack_catalog_tactic_relationship_invalid')
                    continue
                phases.append(tactics[phase['phase_name']])
            self.techniques[tid] = {'id':tid, 'name':obj['name'], 'stix_id':obj['id'],
                'url':'https://attack.mitre.org/techniques/'+tid.replace('.', '/')+'/',
                'parent_id':parent, 'tactics':sorted(phases,key=lambda t:t['id']),
                'definition':obj.get('description',''), 'object_version':obj.get('x_mitre_version'),
                'revoked':bool(obj.get('revoked')), 'deprecated':bool(obj.get('x_mitre_deprecated'))}
        for technique in self.techniques.values():
            if technique['parent_id'] and technique['parent_id'] not in self.techniques:
                raise ValueError('attack_catalog_parent_missing')

    def lookup(self, tid, *, historical=False):
        if tid not in self.techniques:
            raise ValueError('attack_mapping_unknown_id')
        value = self.techniques[tid]
        if not historical and (value['revoked'] or value['deprecated']):
            raise ValueError('attack_mapping_inactive_id')
        return value

    def candidates(self, text, *, limit=6, max_tokens=1400, count_tokens):
        """Bounded lexical retrieval supplies definitions, never asserted mappings.

        No model call or omitted article. Explicit IDs plus definition/name overlap
        retrieve possibilities. Semantic fit and event support need whole-source review.
        """
        if not 0 <= limit <= 12 or not 0 <= max_tokens <= 4000:
            raise ValueError('attack_candidate_budget_invalid')
        stop = {'the','and','that','with','from','this','their','have','were','could','would','into','which','such','these','they','been','also','more','some','may','can','not','for','are','use','used','using','information','data','access','system','systems','adversaries','adversary'}
        def words(text):
            stems={'exploitation':'exploit','exploiting':'exploit','exploited':'exploit','exploits':'exploit',
                   'vulnerabilities':'vulnerability','privileges':'privilege'}
            return {stems.get(w,w) for w in re.findall(r'[a-z][a-z0-9-]{2,}',text.lower()) if w not in stop}
        query = words(text);active = [t for t in self.techniques.values() if not t['revoked'] and not t['deprecated']]
        terms = {t['id']:words(t['name']+' '+t['definition']) for t in active}
        frequency = Counter(w for value in terms.values() for w in value)
        explicit = set(ID_PATTERN.findall(text))
        scored=[]
        for t in active:
            overlap=query & terms[t['id']]
            score=sum(math.log((len(active)+1)/(frequency[w]+1)) for w in overlap)/math.sqrt(max(1,len(terms[t['id']])))
            name_words=words(t['name'])
            score+=20*len(query & name_words)**2/max(1,len(name_words))+100*(t['id'] in explicit)
            if score:scored.append((score,t['id']))
        result=[]
        reference={'catalog':self.identity,'candidates':result,
                   'meaning':'Taxonomy references only, not incident evidence or proven objectives.'}
        if not limit:
            return reference
        for _,tid in sorted(scored,key=lambda s:(-s[0],s[1])):
            t=self.lookup(tid)
            # The complete official definition is required. Never silently cut it.
            item={k:t[k] for k in ('id','name','url','parent_id','tactics','definition','object_version')}
            if count_tokens(json.dumps({**reference,'candidates':result+[item]},ensure_ascii=False,separators=(',',':'))) <= max_tokens:
                result.append(item)
            if len(result)>=limit:break
        return reference

    def validate_mapping(self, mapping, *, item, sources, allowed_ids):
        tid=mapping['technique_id'];technique=self.lookup(tid)
        if tid not in allowed_ids:
            raise ValueError('attack_mapping_not_in_supplied_definitions')
        cited={c['source_id'] for c in item['citations']}
        if not mapping['source_ids'] or len(mapping['source_ids']) != len(set(mapping['source_ids'])) or not set(mapping['source_ids']) <= cited or not set(mapping['source_ids']) <= sources.keys():
            raise ValueError('attack_mapping_evidence_binding_invalid')
        if mapping['origin'] not in {'source_supplied','analyst_applied'} or mapping['behavior_status'] not in {'reported','attempted','inferred'}:
            raise ValueError('attack_mapping_status_invalid')
        if not mapping['rationale'].strip() or (mapping['behavior_status'] in {'attempted','inferred'} and not mapping['limitations'].strip()):
            raise ValueError('attack_mapping_rationale_or_limits_missing')
        if mapping['behavior_status']=='inferred' and item['claim_type']!='assessment':
            raise ValueError('attack_mapping_inferred_behavior_requires_assessment')
        if mapping['origin']=='source_supplied' and not any(re.search(r'(?<![A-Za-z0-9])'+re.escape(tid)+r'(?![A-Za-z0-9]|\.[0-9])',sources[sid]['text']) for sid in mapping['source_ids']):
            raise ValueError('attack_mapping_source_did_not_supply_id')
        return {**mapping, **{k:technique[k] for k in ('name','url','parent_id','tactics')}, 'catalog':self.identity}


def generation_schema(source_ids, technique_ids, packet):
    """Structured mapping stays inside the existing writer response and review."""
    import copy
    from . import event_report_contract as contract
    result = copy.deepcopy(contract.generation_schema(source_ids, packet))
    mapping = contract.object_schema({
        'technique_id': {'type':'string', 'enum':list(technique_ids) or ['__unmapped__']},
        'origin': {'type':'string', 'enum':['source_supplied','analyst_applied']},
        'behavior_status': {'type':'string', 'enum':['reported','attempted','inferred']},
        'rationale': {'type':'string','minLength':12,'maxLength':600},
        'limitations': {'type':'string','maxLength':600},
        'source_ids': {'type':'array','minItems':1,'maxItems':6,
                       'items':{'type':'string','enum':source_ids}}})
    for item in result['properties']['items']['items']['anyOf']:
        item['properties']['attack_mappings'] = {'type':'array','maxItems':3 if technique_ids else 0,'items':mapping}
        item['required'].append('attack_mappings')
    return result


def validate_report(report, packet, catalog):
    import copy
    import jsonschema
    from . import event_report_contract as contract
    reference=packet['attack_reference']
    if reference['catalog'] != catalog.identity:
        raise ValueError('attack_report_catalog_identity_mismatch')
    ids=[s['id'] for s in packet['sources']]
    allowed=[t['id'] for t in reference['candidates']]
    # Candidate definitions must match the pinned taxonomy, including relationships.
    for candidate in reference['candidates']:
        value=catalog.lookup(candidate['id'])
        if candidate != {k:value[k] for k in ('id','name','url','parent_id','tactics','definition','object_version')}:
            raise ValueError('attack_report_definition_mismatch')
    jsonschema.validate(report,generation_schema(ids,allowed,packet))
    base=copy.deepcopy(report)
    for item in base['items']:item.pop('attack_mappings')
    spans=contract.validate(base,packet)
    sources={s['id']:s for s in packet['sources']};resolved={}
    for item in report['items']:
        mappings=item['attack_mappings']
        if mappings and item['section']!='attack_path':
            raise ValueError('attack_mapping_outside_attack_path')
        if len({m['technique_id'] for m in mappings}) != len(mappings):
            raise ValueError('attack_mapping_duplicate')
        resolved[item['id']]=[catalog.validate_mapping(m,item=item,sources=sources,allowed_ids=allowed) for m in mappings]
    return spans,resolved


def render_mapping(mapping):
    """Names, links and relationships are application-resolved, not model text."""
    from html import escape
    qualification={'reported':'','attempted':'Attempted behavior: ','inferred':'Analyst inference: '}[mapping['behavior_status']]
    return ('<p class="event-attack-mapping">ATT&amp;CK '
            f'<a href="{escape(mapping["url"],quote=True)}" target="_blank" rel="noopener">'
            f'{escape(mapping["technique_id"])} {escape(mapping["name"])}</a>. '
            f'{qualification}{escape(mapping["rationale"])} '
            f'{escape(mapping["limitations"])}</p>')
