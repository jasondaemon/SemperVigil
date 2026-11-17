import shutil
import subprocess
from pathlib import Path

import pytest
import yaml


pytestmark = [pytest.mark.offline,
              pytest.mark.skipif(not shutil.which("helm"), reason="Helm unavailable")]
CHART = Path(__file__).resolve().parents[2] / "deploy/helm/sempervigil"


def test_web_readiness_checks_site_while_liveness_checks_nginx():
    result = subprocess.run(
        ["helm", "template", "test", str(CHART)], capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    deployments = [doc for doc in yaml.safe_load_all(result.stdout)
                   if doc and doc.get("kind") == "Deployment"
                   and doc["metadata"]["name"] == "sempervigil-web"]
    assert len(deployments) == 1
    container = deployments[0]["spec"]["template"]["spec"]["containers"][0]
    assert container["readinessProbe"]["httpGet"]["path"] == "/"
    assert container["readinessProbe"]["periodSeconds"] == 2
    assert container["readinessProbe"]["failureThreshold"] == 2
    assert container["livenessProbe"]["httpGet"]["path"] == "/healthz"


def test_builder_keeps_configured_prior_releases():
    result = subprocess.run(
        ["helm", "template", "test", str(CHART), "--set", "buildWorker.keepReleases=12"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    deployment = next(doc for doc in yaml.safe_load_all(result.stdout)
                      if doc and doc.get("kind") == "Deployment"
                      and doc["metadata"]["name"] == "sempervigil-build-worker")
    env = {item["name"]: item for item in
           deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["SV_HUGO_KEEP_RELEASES"]["value"] == "12"
