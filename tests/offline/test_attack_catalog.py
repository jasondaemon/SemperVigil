import copy,hashlib,json
import pytest
from sempervigil.attack_catalog import Catalog, generation_schema, render_mapping
pytestmark=pytest.mark.offline


def dataset():
    def technique(tid,name,sub=False,**flags):
        return dict(type='attack-pattern',id='attack-pattern--'+tid,name=name,description='Official definition for '+name,
                    x_mitre_is_subtechnique=sub,kill_chain_phases=[{'kill_chain_name':'mitre-attack','phase_name':'initial-access'}],
                    external_references=[{'source_name':'mitre-attack','external_id':tid}],**flags)
    return {'objects':[
        {'type':'x-mitre-collection','id':'collection','name':'Enterprise ATT&CK','x_mitre_version':'19.2'},
        {'type':'x-mitre-tactic','id':'tactic','name':'Initial Access','x_mitre_shortname':'initial-access',
         'external_references':[{'source_name':'mitre-attack','external_id':'TA0001'}]},
        technique('T1566','Phishing'), technique('T1566.004','Spearphishing Voice',True),
        technique('T1656','Impersonation',revoked=True), technique('T9999','Old technique',x_mitre_deprecated=True),
        {'type':'relationship','id':'relationship','relationship_type':'subtechnique-of',
         'source_ref':'attack-pattern--T1566.004','target_ref':'attack-pattern--T1566'}]}


def catalog(data=None):
    raw=json.dumps(data or dataset()).encode()
    return Catalog(raw,domain='enterprise',release='19.2',expected_sha256=hashlib.sha256(raw).hexdigest())


def mapping(**values):
    return dict(technique_id='T1566.004',origin='analyst_applied',behavior_status='attempted',
                rationale='A phone call sought access to company systems.',limitations='Success is not established.',
                source_ids=['S1'],**values)


def validate(value=None,item_type='finding',text='A caller sought company access.'):
    return catalog().validate_mapping(value or mapping(),item={'claim_type':item_type,'citations':[{'source_id':'S1'}]},
                                     sources={'S1':{'text':text}},allowed_ids=['T1566.004'])


def test_pinned_id_name_parent_tactics_and_link():
    value=catalog().lookup('T1566.004')
    assert value['name']=='Spearphishing Voice' and value['parent_id']=='T1566'
    assert value['tactics']==[{'id':'TA0001','name':'Initial Access'}]
    assert value['url']=='https://attack.mitre.org/techniques/T1566/004/'


@pytest.mark.parametrize('field,value',[('domain','unknown'),('release','19.1'),('expected_sha256','0'*64)])
def test_catalog_identity_rejects_mismatch(field,value):
    raw=json.dumps(dataset()).encode();args=dict(domain='enterprise',release='19.2',expected_sha256=hashlib.sha256(raw).hexdigest());args[field]=value
    with pytest.raises(ValueError):Catalog(raw,**args)


@pytest.mark.parametrize('tid',['T1656','T9999','T0000'])
def test_new_mappings_reject_inactive_or_unknown_ids(tid):
    with pytest.raises(ValueError):catalog().lookup(tid)
    if tid!='T0000':assert catalog().lookup(tid,historical=True)


def test_invalid_parent_and_tactic_relationships_fail():
    data=dataset();data['objects'][-1]['target_ref']='missing'
    with pytest.raises(ValueError,match='parent'):catalog(data)
    data=dataset();data['objects'][3]['kill_chain_phases'][0]['phase_name']='unknown'
    with pytest.raises(ValueError,match='tactic'):catalog(data)


def test_source_supplied_id_is_distinct_from_analyst_classification():
    assert validate()['origin']=='analyst_applied'
    source=mapping();source['origin']='source_supplied'
    with pytest.raises(ValueError,match='did_not_supply'):validate(source)
    assert validate(source,text='A cited source maps attempted voice phishing to T1566.004.')['origin']=='source_supplied'


@pytest.mark.parametrize('field,value',[('source_ids',[]),('source_ids',['S2']),('limitations',''),('origin','confirmed'),('behavior_status','successful')])
def test_evidence_qualifications_and_ownership_are_bound(field,value):
    m=mapping();m[field]=value
    with pytest.raises(ValueError):validate(m)


def test_inferred_behavior_requires_assessment_and_stated_limits():
    m=mapping();m['behavior_status']='inferred'
    with pytest.raises(ValueError,match='requires_assessment'):validate(m)
    assert validate(m,item_type='assessment')['behavior_status']=='inferred'


def test_rendering_escapes_prose_and_preserves_attempt_uncertainty():
    m=validate();m['rationale']='<script>Untrusted text</script>'
    html=render_mapping(m)
    assert '<script>' not in html and '&lt;script&gt;' in html
    assert 'Attempted behavior:' in html and 'Success is not established.' in html
    assert 'https://attack.mitre.org/techniques/T1566/004/' in html
    assert 'confidence' not in html


def test_empty_mapping_schema_and_bounded_retrieval_allow_abstention():
    from sempervigil import event_report_contract as c
    reference=catalog().candidates('Voice phishing',limit=0,max_tokens=200,count_tokens=c.tokens)
    assert reference['candidates']==[]
    result=generation_schema(['S1'],[],{'update_reason':'generator_upgrade'})
    assert all(v['properties']['attack_mappings']['maxItems']==0 for v in result['properties']['items']['items']['anyOf'])
    assert catalog().candidates('voice phishing',limit=2,max_tokens=300,count_tokens=c.tokens)['candidates']
