import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.offline


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
