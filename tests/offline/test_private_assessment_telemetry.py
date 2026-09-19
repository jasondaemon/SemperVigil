from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from sempervigil import worker
from test_event_assessment import configured

pytestmark = pytest.mark.offline


@pytest.mark.parametrize("failure", [False, True])
def test_private_generation_records_one_attributed_timing(monkeypatch, failure):
    configured(monkeypatch)
    record = worker.insert_llm_run
    times = iter([20.0, 20.125])
    monkeypatch.setattr(worker.time, "monotonic", lambda: next(times))
    transport = Mock(return_value={"parsed":{"decisions":[]}, "schema_valid":True, "raw":"output"})
    if failure:
        transport.side_effect = TimeoutError("DO_NOT_STORE_SOURCE_OR_PROVIDER_DETAILS")
    monkeypatch.setattr(worker, "run_profile", transport)
    call = worker._private_review_completion(None, SimpleNamespace(id="private-job"), None)
    if failure:
        with pytest.raises(TimeoutError): call("private input")
    else:
        assert call("private input") == {"decisions": []}
    record.assert_called_once()
    data = record.call_args.kwargs
    assert data["job_id"] == "private-job"
    assert data["provider_id"] == "local" and data["model_id"] == "local-model"
    assert data["latency_ms"] == 125 and data["input_chars"] == 13
    assert data["output_chars"] == (0 if failure else 6)
    assert data["ok"] is not failure
    assert data["error"] == ("TimeoutError" if failure else None)
    assert "DO_NOT_STORE" not in str(data) and "private input" not in str(data)


def test_disabled_assessment_never_records_model_time(monkeypatch):
    configured(monkeypatch)
    monkeypatch.setenv("SV_EVENT_REVIEW_MODEL_ENABLED", "0")
    assert worker._private_review_completion(None, None, None) is None
    worker.insert_llm_run.assert_not_called()
