"""One-time bounded executor with readiness and durable per-run response journal."""
from pathlib import Path

from . import event_report_contract as contract,event_source_reports as reports
from .investigation import _version
from .utils import atomic_write_json


class JournaledExecutor:
    def __init__(self, conn, run_id, root, *, ceiling=24000, complete=None, phases=("writer","review")):
        if not 1 <= ceiling <= 200000 or not run_id.startswith("esr_"):
            raise ValueError("event_source_report_executor_invalid")
        # Resolve client readiness in this exact execution context before calls.
        reports.ready_client(conn)
        self.conn,self.run_id,self.root = conn,run_id,Path(root)
        self.ceiling,self.complete = ceiling,complete
        if tuple(phases) not in (("writer","review"),("correction","verification")):
            raise ValueError("event_source_report_executor_invalid_phases")
        self.phases=tuple(phases)
        self.reservations=[]

    def __call__(self,payload):
        if len(self.reservations)>=2:
            raise ValueError("event_source_report_executor_call_limit")
        reservation=contract.tokens(contract.encode(payload))+512+payload["max_completion_tokens"]
        if sum(self.reservations)+reservation>self.ceiling:
            raise ValueError("event_source_report_executor_reservation_ceiling")
        phase=self.phases[len(self.reservations)]
        key=_version(payload)
        path=self.root/self.run_id/(key+".json")
        if path.exists():
            raise ValueError("event_source_report_executor_attempt_not_replayed")
        self.reservations.append(reservation)
        receipt={"run_id":self.run_id,"phase":phase,"request_version":key,
                 "reservation":reservation,"status":"started"}
        atomic_write_json(path,receipt)
        try:
            response=(self.complete or (lambda p:reports._complete(self.conn,p)))(payload)
        except reports.PreTransportFailure as exc:
            atomic_write_json(path,{**receipt,"status":"failed_pretransport","proof":exc.proof})
            raise
        except Exception as exc:
            atomic_write_json(path,{**receipt,"status":"unknown_transport","error_type":type(exc).__name__})
            raise
        atomic_write_json(path,{**receipt,"status":"completed","response":response})
        return response
