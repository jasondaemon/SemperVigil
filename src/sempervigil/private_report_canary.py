"""Two-call file-journaled evaluator: no database writes or publication capability."""
import hashlib,json,os
from pathlib import Path
from . import event_report_contract as contract
from .utils import atomic_write_json


def identity(value):
    return hashlib.sha256(contract.encode(value).encode()).hexdigest()


class Executor:
    def __init__(self, manifest, root, *, complete):
        if (manifest.get('workflow')!='private-manifest-bound-report-canary-v1'
                or manifest.get('publication') is not False or manifest.get('database_transaction')!='READ ONLY'
                or manifest.get('max_calls')!=2 or manifest.get('ceiling')!=32000
                or manifest.get('id')!='canary_'+identity({k:v for k,v in manifest.items() if k!='id'})[:24]):
            raise ValueError('private_canary_manifest_invalid')
        self.manifest,self.complete=manifest,complete
        self.root=Path(root)/manifest['id'];self.root.mkdir(parents=True,exist_ok=True);self.root.chmod(0o700)
        prior=self.root/'manifest.json'
        if prior.exists() and json.loads(prior.read_text())!=manifest:
            raise ValueError('private_canary_manifest_changed')
        if not prior.exists():atomic_write_json(prior,manifest)

    def call(self, phase, payload):
        if phase not in ('writer','review'):
            raise ValueError('private_canary_phase_invalid')
        options={'writer':{'model':'gpt-5.6-sol','reasoning_effort':'none','max_completion_tokens':6000},
                 'review':{'model':'gpt-5.6-luna','reasoning_effort':'low','max_completion_tokens':2400}}[phase]
        if any(payload.get(k)!=v for k,v in options.items()):
            raise ValueError('private_canary_models_changed')
        if (len(payload.get('messages',[]))!=2 or
                [m.get('role') for m in payload['messages']]!=['system','user']):
            raise ValueError('private_canary_messages_changed')
        data=json.loads(payload['messages'][1]['content'])
        if identity(payload['messages'][0]['content'])!=self.manifest[phase+'_system_sha256']:
            raise ValueError('private_canary_prompt_changed')
        if phase=='writer':
            if identity(payload)!=self.manifest['writer_request_sha256'] or identity(data)!=self.manifest['packet_sha256']:
                raise ValueError('private_canary_writer_changed')
        else:
            writer=self.root/'writer.json'
            if not writer.exists():raise ValueError('private_canary_writer_required')
            original=json.loads(writer.read_text())
            if original['status']!='completed':raise ValueError('private_canary_writer_incomplete')
            choice=original['response']['choices'][0]
            if choice.get('finish_reason')!='stop' or choice['message'].get('refusal'):
                raise ValueError('private_canary_writer_incomplete')
            if (set(data)!={'evidence','report','citation_provenance'} or
                    data['report']!=json.loads(choice['message']['content']) or identity(data['evidence'])!=self.manifest['packet_sha256']):
                raise ValueError('private_canary_review_material_changed')
            base=json.loads(json.dumps(data['report']))
            for item in base['items']:item.pop('attack_mappings',None)
            if data['citation_provenance']!=contract.validate(base,data['evidence']):
                raise ValueError('private_canary_provenance_changed')
            response_schema={'type':'json_schema','json_schema':{'name':'event_source_report','strict':True,
                             'schema':contract.review_schema(data['report'],[s['id'] for s in data['evidence']['sources']])}}
            if payload['response_format']!=response_schema:
                raise ValueError('private_canary_review_schema_changed')
        reservations=[json.loads(p.read_text())['reservation'] for p in self.root.glob('*.json') if p.name in ('writer.json','review.json')]
        reservation=contract.tokens(contract.encode(payload))+512+payload['max_completion_tokens']
        if len(reservations)>=2 or sum(reservations)+reservation>self.manifest['ceiling']:
            raise ValueError('private_canary_reservation_limit')
        # Exclusive durable claim precedes transport, including ambiguous failures.
        lock=self.root/(phase+'.claimed')
        descriptor=os.open(lock,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        os.fsync(descriptor);os.close(descriptor)
        receipt={'manifest_id':self.manifest['id'],'phase':phase,'request_sha256':identity(payload),
                 'reservation':reservation,'status':'started','request':payload}
        path=self.root/(phase+'.json');atomic_write_json(path,receipt)
        try:response=self.complete(payload)
        except Exception as exc:
            atomic_write_json(path,{**receipt,'status':'unknown_transport','error_type':type(exc).__name__})
            raise
        # Preserve raw content and actual usage BEFORE parsing generated JSON.
        atomic_write_json(path,{**receipt,'status':'completed','response':response,'usage':response.get('usage')})
        return response
