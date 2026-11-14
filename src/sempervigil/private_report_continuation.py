"""Versioned one-review continuation sharing the immutable parent's call ledger."""
import json, os
from pathlib import Path
from .private_report_canary import Executor, identity
from .attack_catalog import project_optional_mappings
from .utils import atomic_write_json

class ContinuationExecutor:
    def __init__(self, parent, manifest, root, *, complete, catalog, contract):
        # Preserve and enforce the original manifest identity/budget checks.
        original=Executor(parent,root,complete=complete)
        self.root=original.root;self.parent=parent;self.manifest=manifest
        self.complete,self.catalog,self.contract=complete,catalog,contract
        if (manifest.get('workflow')!='private-report-continuation-v1' or
            manifest.get('parent_manifest_sha256')!=identity(parent) or
            manifest.get('parent_id')!=parent['id'] or
            manifest.get('max_calls')!=2 or manifest.get('ceiling')!=32000 or
            manifest.get('publication') is not False or manifest.get('database_transaction')!='READ ONLY' or
            manifest.get('id')!='continuation_'+identity({k:v for k,v in manifest.items() if k!='id'})[:24]):
            raise ValueError('continuation_manifest_invalid')
        self.directory=self.root/'continuations'/manifest['id'];self.directory.mkdir(parents=True,exist_ok=True)
        prior=self.directory/'manifest.json'
        if prior.exists() and json.loads(prior.read_text())!=manifest:
            raise ValueError('continuation_manifest_changed')
        if not prior.exists():atomic_write_json(prior,manifest)

    def call(self,payload,packet):
        c=self.contract;m=self.manifest;parent=self.parent
        writer=json.loads((self.root/'writer.json').read_text())
        if identity(writer)!=m['parent_writer_journal_sha256'] or identity(packet)!=parent['packet_sha256']:
            raise ValueError('continuation_parent_material_changed')
        if (writer['status']!='completed' or writer['manifest_id']!=parent['id'] or
            identity(writer['request'])!=parent['writer_request_sha256'] or
            writer['response']['choices'][0]['finish_reason']!='stop' or
            writer['response']['choices'][0]['message'].get('refusal')):
            raise ValueError('continuation_writer_invalid')
        raw=json.loads(writer['response']['choices'][0]['message']['content'])
        report,evidence,spans,resolved,removed=project_optional_mappings(raw,packet,self.catalog,contract_override=c)
        expected={'evidence':evidence,'report':report,'citation_provenance':spans}
        if (payload.get('model')!='gpt-5.6-luna' or payload.get('reasoning_effort')!='low' or
            payload.get('max_completion_tokens')!=2400 or len(payload.get('messages',[]))!=2 or
            [v.get('role') for v in payload['messages']]!=['system','user'] or
            identity(payload['messages'][0]['content'])!=parent['review_system_sha256'] or
            json.loads(payload['messages'][1]['content'])!=expected or identity(payload)!=m['review_request_sha256'] or
            identity({'report':report,'evidence':evidence,'spans':spans,'removed':removed})!=m['projection_sha256']):
            raise ValueError('continuation_review_material_changed')
        schema={'type':'json_schema','json_schema':{'name':'event_source_report','strict':True,
                'schema':c.review_schema(report,[s['id'] for s in evidence['sources']])}}
        if payload['response_format']!=schema:raise ValueError('continuation_review_schema_changed')
        journals=[json.loads(p.read_text()) for p in self.root.glob('*.json') if p.name in ('writer.json','review.json')]
        reserve=c.tokens(c.encode(payload))+512+2400
        if len(journals)!=1 or sum(v['reservation'] for v in journals)+reserve>32000:
            raise ValueError('continuation_reservation_limit')
        # All continuations and the old executor share this one parent phase lock.
        fd=os.open(self.root/'review.claimed',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);os.fsync(fd);os.close(fd)
        receipt={'manifest_id':parent['id'],'continuation_id':m['id'],'phase':'review',
                 'request_sha256':identity(payload),'reservation':reserve,'status':'started',
                 'request':payload,'projection':{'removed':removed,'report_sha256':identity(report),
                 'evidence_sha256':identity(evidence),'citation_provenance':spans}}
        path=self.root/'review.json';atomic_write_json(path,receipt)
        try:response=self.complete(payload)
        except Exception as exc:
            atomic_write_json(path,{**receipt,'status':'unknown_transport','error_type':type(exc).__name__});raise
        atomic_write_json(path,{**receipt,'status':'completed','response':response,'usage':response.get('usage')})
        return response
