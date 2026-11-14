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
    assert data["SV_EVENT_DECONSTRUCTION_ENABLED"] == "0"
    assert data["SV_EVENT_DECONSTRUCTION_PROFILE_ID"] == ""
    assert data["SV_EVENT_CLAIM_SUPPORT_ENABLED"] == "0"
    assert data["SV_EVENT_CLAIM_SUPPORT_PROFILE_ID"] == ""


@pytest.mark.skipif(not shutil.which("helm"), reason="Helm is not installed")
@pytest.mark.parametrize("enabled", ["0", "1"])
def test_deconstruction_configuration_reaches_runtime(enabled):
    chart = Path(__file__).resolve().parents[2] / "deploy/helm/sempervigil"
    raw = subprocess.check_output(["helm", "template", "test", str(chart),
        "--show-only", "templates/configmap-env.yaml", "--set-string",
        "env.SV_EVENT_DECONSTRUCTION_ENABLED=" + enabled,
        "--set-string", "env.SV_EVENT_DECONSTRUCTION_PROFILE_ID=draft-profile"], text=True)
    data = yaml.safe_load(raw)["data"]
    assert data["SV_EVENT_DECONSTRUCTION_ENABLED"] == enabled
    assert data["SV_EVENT_DECONSTRUCTION_PROFILE_ID"] == "draft-profile"


@pytest.mark.skipif(not shutil.which("helm"), reason="Helm is not installed")
@pytest.mark.parametrize("enabled", ["0", "1"])
def test_published_composition_upgrade_configuration_reaches_runtime(enabled):
    chart = Path(__file__).resolve().parents[2] / "deploy/helm/sempervigil"
    raw = subprocess.check_output(["helm", "template", "test", str(chart),
        "--show-only", "templates/configmap-env.yaml", "--set-string",
        "env.SV_EVENT_PUBLIC_COMPOSITION_UPGRADE_ENABLED=" + enabled], text=True)
    data = yaml.safe_load(raw)["data"]
    assert data["SV_EVENT_PUBLIC_COMPOSITION_UPGRADE_ENABLED"] == enabled


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


@pytest.mark.skipif(not shutil.which("helm"), reason="Helm is not installed")
@pytest.mark.parametrize("enabled", [False, True])
def test_private_applicability_flag_is_worker_only(enabled):
    chart = Path(__file__).resolve().parents[2] / "deploy/helm/sempervigil"
    raw = subprocess.check_output([
        "helm", "template", "test", str(chart),
        "--set", f"workerLlm.articleApplicabilityReviewEnabled={str(enabled).lower()}",
    ], text=True)
    deployments = {doc["metadata"]["name"]: doc for doc in yaml.safe_load_all(raw)
                   if doc and doc.get("kind") == "Deployment"}
    worker = deployments["sempervigil-worker-llm"]
    assert worker["spec"]["strategy"]["type"] == "Recreate"
    assert worker["spec"]["template"]["spec"]["terminationGracePeriodSeconds"] == 1800
    assert "exec env PYTHONPATH=/app/src python -m sempervigil.runner" in (
        worker["spec"]["template"]["spec"]["containers"][0]["args"][0]
    )
    for name, deployment in deployments.items():
        env = deployment["spec"]["template"]["spec"]["containers"][0].get("env", [])
        flags = [item for item in env
                 if item["name"] == "SV_ARTICLE_APPLICABILITY_REVIEW_ENABLED"]
        if name == "sempervigil-worker-llm":
            assert flags == [{"name": "SV_ARTICLE_APPLICABILITY_REVIEW_ENABLED",
                              "value": "1" if enabled else "0"}]
        else:
            assert not flags


@pytest.mark.skipif(not shutil.which("helm"), reason="Helm is not installed")
def test_registry_pull_secret_is_worker_scoped():
    chart = Path(__file__).resolve().parents[2] / "deploy/helm/sempervigil"
    raw = subprocess.check_output([
        "helm", "template", "test", str(chart),
        "--set", "workerLlm.image.repository=git.example.test/owner/ingest",
        "--set", "workerLlm.image.tag=sha-abc123",
        "--set", "workerLlm.image.digest=sha256:abc123",
        "--set", "workerLlm.image.pullPolicy=IfNotPresent",
        "--set", "workerLlm.imagePullSecrets[0].name=registry-readonly",
    ], text=True)
    deployments = {doc["metadata"]["name"]: doc for doc in yaml.safe_load_all(raw)
                   if doc and doc.get("kind") == "Deployment"}
    for name, deployment in deployments.items():
        spec = deployment["spec"]["template"]["spec"]
        if name == "sempervigil-worker-llm":
            assert spec["imagePullSecrets"] == [{"name": "registry-readonly"}]
            for container in spec["containers"] + spec["initContainers"]:
                assert container["image"] == "git.example.test/owner/ingest@sha256:abc123"
                assert container["imagePullPolicy"] == "IfNotPresent"
        else:
            assert "imagePullSecrets" not in spec
