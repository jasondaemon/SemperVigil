"""Retained final reports use fresh qualification and separated publication, no HTTP."""
import copy
import hashlib
import json
import pytest
from test_event_report_v2_policy_postgres import setup
from test_event_source_reports_postgres import database, generated, response
from sempervigil import event_source_reports_v2 as reports, event_report_final_editor as editor
from sempervigil import event_report_final_editor_import as intake, event_report_contract_v2 as contract
from sempervigil import event_source_report_publication_v2 as publication
from sempervigil.investigation import _version


@pytest.fixture
def retained(setup, monkeypatch):
    s=setup
    s.conn.execute("DELETE FROM event_articles WHERE article_id=2; DELETE FROM articles WHERE id=2")
    s.conn.commit()
    monkeypatch.setenv('SV_EVENT_REPORT_V2_AUTONOMOUS','0')
    monkeypatch.setenv('SV_EVENT_REPORT_V2_GENERATION_ENABLED','0')
    packet=reports.snapshot(s.conn,'evt_test')
    packet.update(report_contract=contract.WORKFLOW,review_contract=contract.REVIEW_CONTRACT,
                  update_reason='generator_upgrade',evidence_delta={'baseline':'known','new':[],'changed':[],'removed':[]})
    packet['coverage']={'mode':'complete','omitted_source_ids':[]}
    profile={'workflow':editor.WORKFLOW,'model':'gpt-5.6-sol','reasoning_effort':'high','max_completion_tokens':12000,'context_overrides':{}}
    original=generated();raw=editor.narrative(original)
    wq={'model':'gpt-5.6-sol','reasoning_effort':'none','max_completion_tokens':6000,
        'messages':[{'role':'system','content':contract.WRITER},{'role':'user','content':contract.encode(packet)}]}
    wr=response(original);wrsv=contract.tokens(contract.encode(wq))+512+6000
    wm={'workflow':'materiality-calibrated-report-pairs-v1','publication':False,'database_transaction':'READ ONLY',
        'systems':{'writer':_version(contract.WRITER)},'writers':{'fixture':{'event_id':'evt_test','request_sha256':_version(wq),'reservation':wrsv}}}
    wm['id']='pairs_'+_version(wm)[:24]
    w={'manifest_id':wm['id'],'case':'fixture','phase':'fixture_writer','status':'completed','request':wq,'response':wr,
       'request_sha256':_version(wq),'reservation':wrsv,'usage':wr['usage']}
    ep={**packet,'final_editor':profile};spans=contract.validate(raw,packet)
    eq={'model':'gpt-5.6-sol','reasoning_effort':'high','max_completion_tokens':12000,
        'messages':[{'role':'system','content':editor.PROMPT},{'role':'user','content':contract.encode(editor.editor_input(ep,raw,spans))}],
        'response_format':{'type':'json_schema','json_schema':{'name':'event_source_report','strict':True,'schema':editor.schema(ep)}}}
    final=copy.deepcopy(raw);final['title']='A fresh source-grounded retained report'
    result={'report':final,'review':{'ready':True,'issues':[],'locator_warnings':[],'editorial_warnings':[]}}
    er=response(result);ersv=contract.tokens(contract.encode(eq))+512+12000
    ec={'case':'fixture','event_id':'evt_test','request_version':_version(eq),'reservation':ersv,'profile':profile,
        'original_draft_version':_version(original),'draft_version':_version(raw),'source_version':packet['source_version'],
        'sources':[{'id':x['id'],'text_sha256':hashlib.sha256(x['text'].encode()).hexdigest()} for x in packet['sources']]}
    em={'workflow':'retained-narrative-editor-evaluation-v1','publication':False,'database_transaction':'READ ONLY','max_calls':2,'ceiling':49906,
        'scoped_model_policy':editor.SOURCE_CHECKING_PASSES,'cases':[ec]};em['id']='editcal_'+_version(em)[:24]
    e={'manifest_id':em['id'],'case':'fixture','status':'completed','request':eq,'response':er,'usage':er['usage'],'reservation':ersv,'request_version':_version(eq)}
    approval={'ready':True,'scope':'content_only','authority':'synthetic independent fixture authority','reviewer':'separate-fixture-reviewer',
              'source_review':'SYNTHETIC complete source review, not a live model quality assertion',
              'report_version':_version(final),'evidence_version':_version(ep),'source_version':packet['source_version'],'response_version':_version(er)}
    audit={'workflow':intake.WORKFLOW,'writer_manifest':wm,'writer_journal':w,'editor_manifest':em,'editor_journal':e,
           'content_review':approval,'predecessor':s.old}
    s.audit=audit;s.final=editor.empty_mappings(final)
    return s


def test_retained_final_editor_publishes_without_new_calls_and_exports(retained):
    s=retained;result=intake.import_reviewed(s.conn,s.audit);rid=result['run_id']
    assert result['new_model_calls']==0 and result['charged_tokens']==600 and not s.calls
    material=publication.current_material(s.conn,rid)
    assert material['report']==s.final and material['derivation']['approved_narrative_version']==s.audit['content_review']['report_version']
    assert material['derivation']['independence'].startswith('none;')
    assert intake.import_reviewed(s.conn,s.audit)['reused']
    _,promoted=s.publish(rid)
    bundle=json.loads(s.conn.execute('SELECT bundle_json FROM event_public_revisions WHERE revision_id=%s',(promoted['revision_id'],)).fetchone()[0])
    assert bundle['predecessor']==s.old and bundle['attack']['catalog'] is None
    assert bundle['revision_provenance']['update_reason']=='generator_upgrade'
    assert publication.published_material(s.conn,bundle)['report']==s.final
    from sempervigil.event_render import render
    _,html=render(bundle,event_id='evt_test',expected_revision=promoted['revision_id'])
    assert s.final['items'][1]['text'] in html and not s.calls


@pytest.mark.parametrize('change',['approval','final_content','draft','source','usage','incomplete','taxonomy','manifest'])
def test_retained_artifact_tampering_never_imports(retained,change):
    s=retained;a=copy.deepcopy(s.audit)
    if change=='approval':a['content_review']['report_version']='0'*64
    elif change=='final_content':a['editor_journal']['response']['choices'][0]['message']['content']='{}'
    elif change=='draft':a['writer_journal']['response']['choices'][0]['message']['content']='{}'
    elif change=='source':a['editor_manifest']['cases'][0]['source_version']='0'*64
    elif change=='usage':a['editor_journal']['usage']={'total_tokens':999999}
    elif change=='incomplete':a['editor_journal']['response']['choices'][0]['finish_reason']='length'
    elif change=='taxonomy':
        v=json.loads(a['editor_journal']['response']['choices'][0]['message']['content']);v['report']['items'][0]['attack_mappings']=[]
        a['editor_journal']['response']['choices'][0]['message']['content']=contract.encode(v)
    else:a['editor_manifest']['publication']=True
    with pytest.raises(Exception):intake.import_reviewed(s.conn,a)
    assert s.conn.execute("SELECT revision_id FROM event_public_pointers WHERE event_id='evt_test'").fetchone()[0]==s.old
    assert not s.calls


@pytest.mark.parametrize('change',['source','predecessor','generation'])
def test_retained_publication_rechecks_fresh_state(retained,monkeypatch,change):
    s=retained;rid=intake.import_reviewed(s.conn,s.audit)['run_id']
    if change=='source':s.conn.execute("UPDATE articles SET content_text=content_text||' New detail.' WHERE id=1");s.conn.commit()
    elif change=='predecessor':s.conn.execute("DELETE FROM event_public_pointers WHERE event_id='evt_test'");s.conn.commit()
    else:monkeypatch.setattr(reports,'configuration',lambda _:({'id':'m'},{'id':'p'},'c'*64))
    with pytest.raises(Exception):s.publish(rid)
    assert not s.calls
