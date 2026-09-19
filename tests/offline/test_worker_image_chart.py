import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.offline


@pytest.mark.skipif(not shutil.which("helm"), reason="Helm is not installed")
@pytest.mark.parametrize("enabled", ["0", "1"])
def test_private_review_chart_configuration(enabled):
    chart = Path(__file__).resolve().parents[2] / "deploy/helm/sempervigil"
    raw = subprocess.check_output(["helm", "template", "test", str(chart),
        "--show-only", "templates/configmap-env.yaml", "--set-string",
        "env.SV_EVENT_REVIEW_ENABLED=" + enabled,
        "--set-string", "env.SV_EVENT_REVIEW_MODEL_ENABLED=" + enabled,
        "--set-string", "env.SV_EVENT_REVIEW_PROFILE_ID=private-profile"], text=True)
    data = yaml.safe_load(raw)["data"]
    assert data["SV_EVENT_REVIEW_ENABLED"] == enabled
    assert data["SV_EVENT_REVIEW_DIR"] == "/log/event-reviews"
    assert data["SV_EVENT_REVIEW_MODEL_ENABLED"] == enabled
    assert data["SV_EVENT_REVIEW_PROFILE_ID"] == "private-profile"
    assert data["SV_EVENT_REVIEW_SCOPE_ENABLED"] == "0"
    assert data["SV_EVENT_REVIEW_SCOPE_PROFILE_ID"] == ""
    assert data["SV_EVENT_REVIEW_PAIR_ENABLED"] == "0"
    assert data["SV_EVENT_REVIEW_PAIR_PROFILE_ID"] == ""


@pytest.mark.skipif(not shutil.which("helm"), reason="Helm is not installed")
@pytest.mark.parametrize("enabled", ["0", "1"])
def test_scoped_review_chart_configuration(enabled):
    chart = Path(__file__).resolve().parents[2] / "deploy/helm/sempervigil"
    raw = subprocess.check_output(["helm", "template", "test", str(chart),
        "--show-only", "templates/configmap-env.yaml", "--set-string",
        "env.SV_EVENT_REVIEW_SCOPE_ENABLED=" + enabled,
        "--set-string", "env.SV_EVENT_REVIEW_SCOPE_PROFILE_ID=scope-profile"], text=True)
    data = yaml.safe_load(raw)["data"]
    assert data["SV_EVENT_REVIEW_SCOPE_ENABLED"] == enabled
    assert data["SV_EVENT_REVIEW_SCOPE_PROFILE_ID"] == "scope-profile"
    assert data["SV_EVENT_REVIEW_MODEL_ENABLED"] == "0"


@pytest.mark.skipif(not shutil.which("helm"), reason="Helm is not installed")
@pytest.mark.parametrize("enabled", ["0", "1"])
def test_paired_review_chart_configuration(enabled):
    chart = Path(__file__).resolve().parents[2] / "deploy/helm/sempervigil"
    raw = subprocess.check_output(["helm", "template", "test", str(chart),
        "--show-only", "templates/configmap-env.yaml", "--set-string",
        "env.SV_EVENT_REVIEW_PAIR_ENABLED=" + enabled,
        "--set-string", "env.SV_EVENT_REVIEW_PAIR_PROFILE_ID=pair-profile"], text=True)
    data = yaml.safe_load(raw)["data"]
    assert data["SV_EVENT_REVIEW_PAIR_ENABLED"] == enabled
    assert data["SV_EVENT_REVIEW_PAIR_PROFILE_ID"] == "pair-profile"
    assert data["SV_EVENT_REVIEW_SCOPE_ENABLED"] == "0"


@pytest.mark.skipif(not shutil.which("helm"), reason="Helm is not installed")
@pytest.mark.parametrize("override", [False, True])
def test_worker_image_override_is_isolated(override):
    chart = Path(__file__).resolve().parents[2] / "deploy/helm/sempervigil"
    args = ["helm", "template", "test", str(chart), "--set", "image.tag=shared"]
    if override:
        args += ["--set", "workerLlm.image.tag=private"]
    docs = yaml.safe_load_all(subprocess.check_output(args, text=True))
    for doc in docs:
        if not doc or doc.get("kind") != "Deployment":
            continue
        role = doc["metadata"]["labels"]["app.kubernetes.io/component"]
        if role not in {"worker-llm", "orchestrator", "worker-fetch", "worker-openai"}:
            continue
        expected = "private" if override and role == "worker-llm" else "shared"
        spec = doc["spec"]["template"]["spec"]
        for container in spec["initContainers"] + spec["containers"]:
            if container["image"].startswith("sempervigil-ingest:"):
                assert container["image"] == "sempervigil-ingest:" + expected
